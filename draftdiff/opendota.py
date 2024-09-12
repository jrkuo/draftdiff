import datetime
import json
import time
from typing import List

import boto3
import requests
from requests.exceptions import RequestException


# save data to draftdiff/opendota/matches/run_date={current_ds}/data.json.0001.gz
# time sleep for calls per min
# each file 20-50 MB
def main():
    response = requests.get("https://api.opendota.com/api/parsedMatches")
    parsed_matches = response.json()
    first_match_id = min([x["match_id"] for x in parsed_matches])
    response_pg2 = requests.get(
        f"https://api.opendota.com/api/parsedMatches?less_than_match_id={first_match_id}"
    )
    parsed_matches_pg2 = response_pg2.json()
    first_match_id_pg2 = min([x["match_id"] for x in parsed_matches_pg2])
    response_first_match_id = requests.get(
        f"https://api.opendota.com/api/matches/{first_match_id}"
    )
    first_match_id_response = response_first_match_id.json()
    return


def get_latest_parsed_match_ids() -> List[int]:
    match_id_list = []

    try:
        response = requests.get(
            "https://api.opendota.com/api/parsedMatches", timeout=10
        )
        parsed_match_ids_dict = response.json()
        parsed_match_ids_list = [x["match_id"] for x in parsed_match_ids_dict]
        match_id_list.extend(parsed_match_ids_list)
        earliest_match_id = min([x["match_id"] for x in parsed_match_ids_dict])

        for _ in range(1):
            try:
                response_next_pg = requests.get(
                    f"https://api.opendota.com/api/parsedMatches?less_than_match_id={earliest_match_id}",
                    timeout=10,
                )
                parsed_match_ids_dict_next_pg = response_next_pg.json()
                parsed_match_ids_list_next_pg = [
                    x["match_id"] for x in parsed_match_ids_dict_next_pg
                ]
                match_id_list.extend(parsed_match_ids_list_next_pg)
                earliest_match_id = min(
                    [x["match_id"] for x in parsed_match_ids_dict_next_pg]
                )
            except RequestException as e:
                print(f"Error fetching page with match_id < {earliest_match_id}: {e}")
                break  # Exit the loop if an error occurs during pagination

    except RequestException as e:
        print(f"Error fetching initial match IDs: {e}")

    return match_id_list


def get_latest_parsed_match_data(match_ids: List[int]) -> List[dict]:
    all_match_data = []
    for match_id in match_ids:
        try:
            response = requests.get(
                f"https://api.opendota.com/api/matches/{match_id}", timeout=10
            )
            response.raise_for_status()  # Check for HTTP errors

            match_data = response.json()
            if isinstance(match_data, dict):
                all_match_data.append(match_data)
            else:
                print(f"Unexpected data format for match ID {match_id}: {match_data}")

        except RequestException as e:
            print(f"Error fetching match ID {match_id} data: {e}")
            continue

        time.sleep(1)
    return all_match_data


def write_json_to_s3(
    data: List[dict], data_partition: str, bucket_name: str = "draftdiff"
) -> None:
    object_location = f"{data_partition}/data.json"
    try:
        boto3.client("s3").put_object(
            Bucket=bucket_name,
            Key=object_location,
            Body=json.dumps(data, indent=4).encode("utf-8"),
        )
        print(f"Data successfully written to s3://{bucket_name}/{object_location}")
    except Exception as e:
        print(f"Failed to write data to S3: {e}")


if __name__ == "__main__":
    match_data = get_latest_parsed_match_data([7938490194])
    ds = datetime.datetime.now().strftime("%Y-%m-%d")
    partition_path = f"opendota/matches/run_date={ds}"
    write_json_to_s3(match_data, partition_path)
    print("done")
