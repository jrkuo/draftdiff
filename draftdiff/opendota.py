import datetime
import gzip
import io
import json
import time
from typing import List

import boto3
import requests
from draftdiff.s3 import list_all_s3_files
from requests.exceptions import RequestException
from tqdm import tqdm


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

        for _ in range(18):
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


# change to only operate on one match id
def get_parsed_match_data(
    match_id: int, current_timeout: int = 0, max_timeout: int = 60 * 5
) -> dict:
    if current_timeout > max_timeout:
        raise Exception("maxmimum retries reached")
    time.sleep(current_timeout)
    try:
        response = requests.get(
            f"https://api.opendota.com/api/matches/{match_id}", timeout=10
        )
        response.raise_for_status()  # Check for HTTP errors

        match_data = response.json()

        if not isinstance(match_data, dict):
            print(f"Unexpected data format for match ID {match_id}: {match_data}")
            # raise Exception(
            #     f"Unexpected data format for match ID {match_id}: {match_data}"
            # )
            return get_parsed_match_data(
                match_id=match_id, current_timeout=current_timeout + 30
            )

    except RequestException as e:
        print(f"Error fetching match ID {match_id} data: {e}")
        raise e

    time.sleep(1)
    return match_data


def gzip_string_data(data: dict) -> bytes:
    """Compress a string (JSON data) into gzip format."""
    # Convert the JSON data to a string
    json_data = json.dumps(data, indent=4)

    # Create an in-memory bytes buffer to hold the compressed data
    buf = io.BytesIO()

    # Gzip the string data and write it to the buffer
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz_file:
        gz_file.write(json_data.encode("utf-8"))

    # Get the compressed data from the buffer
    return buf.getvalue()


def upload_gzipped_string_to_s3(
    data: dict, data_partition: str, bucket_name: str = "draftdiff"
):
    """Uploads a gzipped string (JSON) to S3."""
    s3_client = boto3.client("s3")

    # Compress the data
    compressed_data = gzip_string_data(data)
    object_location = f"{data_partition}/data.json.gz"

    try:
        # Upload the gzipped string data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_location,
            Body=compressed_data,
            ContentType="application/json",
            ContentEncoding="gzip",
        )
        print(f"Gzipped data uploaded to s3://{bucket_name}/{object_location}")
    except Exception as e:
        print(f"Failed to upload gzipped data to S3: {e}")


def write_json_to_s3(
    data: List[dict], data_partition: str, bucket_name: str = "draftdiff"
) -> None:
    object_location = f"{data_partition}/data.json.gz"
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
    ds = datetime.datetime.now().strftime("%Y-%m-%d")
    # partition_path = f"opendota/matches/run_date={ds}"
    match_ids = get_latest_parsed_match_ids()
    # match_ids = [7940186309, 7940185846, 7940185713, 7940185635, 7940185626]
    match_ids_files_in_s3 = list_all_s3_files("draftdiff", "opendota/matches/")
    match_ids_in_s3 = [
        int(s.split("id=")[1].split("/")[0]) for s in match_ids_files_in_s3
    ]

    for ii, match_id in tqdm(enumerate(match_ids), total=len(match_ids)):
        if match_id in match_ids_in_s3:
            continue
        match_data = get_parsed_match_data(match_id)
        upload_gzipped_string_to_s3(match_data, f"opendota/matches/id={match_id}")
        # if ii > 4:
        #     break
    # write_json_to_s3(match_data, partition_path)
    print("done")
