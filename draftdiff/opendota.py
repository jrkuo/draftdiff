import datetime
import gzip
import io
import json
import os
import time
from pathlib import Path
from typing import Any, TypedDict

import boto3
import diskcache as dc
import pandas as pd
import requests
from requests.exceptions import RequestException
from tqdm import tqdm

from draftdiff.models.opendota import MatchResponse
from draftdiff.s3 import list_all_s3_files

cache_dir = Path.home() / '.cache' / 'draftdiff'
cache_dir.mkdir(parents=True, exist_ok=True)
cache = dc.Cache(str(cache_dir / 'draftdiff_cache'))


async def _fetch_opendota_match_id(match_id: int) -> MatchResponse:
    time.sleep(1.05)
    response = requests.get(f'https://api.opendota.com/api/matches/{match_id}')
    response.raise_for_status()
    response_dict: dict = response.json()  # type: ignore
    match_response: MatchResponse = MatchResponse(**response_dict)  # type: ignore
    return match_response


async def opendota_match(match_id: int) -> MatchResponse:
    cache_key = f'opendota_match_{match_id}'

    cached_result = cache.get(cache_key)  # type: ignore
    if cached_result is not None:
        return cached_result  # type: ignore

    match_ids = await _fetch_opendota_match_id(match_id)
    # Cache for 30 days (1 month)
    cache.set(cache_key, match_ids, expire=2592000)  # type: ignore
    return match_ids


class MatchHero(TypedDict):
    match_id: str
    hero_name: str
    player_name: str
    team: str
    skills: list[str]
    items: list[list[str]]
    major_items: list[str]
    lane: str
    won: bool


async def parse_match_heroes(match: MatchResponse) -> list[MatchHero]:
    models_path = os.path.join(os.path.dirname(__file__), 'models', 'dotaconstants')

    ability_ids_path = os.path.join(models_path, 'ability_ids.json')
    with open(ability_ids_path) as f:
        ability_id_to_name = json.load(f)

    heroes_path = os.path.join(models_path, 'heroes.json')
    with open(heroes_path) as f:
        hero_id_to_hero = json.load(f)

    items_path = os.path.join(models_path, 'items.json')
    with open(items_path) as f:
        item_name_to_item = json.load(f)
        item_id_to_name = {v['id']: k for k, v in item_name_to_item.items()}

    if not match.players:
        raise Exception(f'No players found for match {match.match_id}')

    match_heroes: list[MatchHero] = []
    try:
        radiant_player_names = ','.join([p.name for p in match.players if p.isRadiant])  # type: ignore
        dire_player_names = ','.join([p.name for p in match.players if not p.isRadiant])  # type: ignore
    except Exception:
        radiant_player_names = ','.join([p.personaname for p in match.players if p.isRadiant])  # type: ignore
        dire_player_names = ','.join([p.personaname for p in match.players if not p.isRadiant])  # type: ignore

    for player in match.players:
        player_won: bool = (match.radiant_win and player.isRadiant) or (not match.radiant_win and not player.isRadiant)
        match_heroes += [
            MatchHero(
                {
                    'match_id': match.match_id,
                    'player_name': player.name if player.name else player.personaname,
                    'team': radiant_player_names if player.isRadiant else dire_player_names,
                    'hero_name': hero_id_to_hero[str(player.hero_id)]['localized_name'],
                    'items': [[str(i.time), str(i.key)] for i in player.purchase_log] if player.purchase_log else [],  # type: ignore
                    'major_items': [
                        item_id_to_name.get(player.item_0),
                        item_id_to_name.get(player.item_1),
                        item_id_to_name.get(player.item_2),
                        item_id_to_name.get(player.item_3),
                        item_id_to_name.get(player.item_4),
                        item_id_to_name.get(player.item_5),
                    ],
                    'skills': list(map(lambda x: ability_id_to_name[str(x)], player.ability_upgrades_arr)),  # type: ignore
                    'lane': player.lane,  # map to str
                    'won': player_won,
                }
            )
        ]
    return match_heroes


def do_hero_build_df(target_hero_builds: list[MatchHero]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    def to_item_ssv(items: list[list[str]]):
        return '\n'.join([';'.join(pair) for pair in items])

    for hero_build in target_hero_builds:
        pregame_items = [i for i in hero_build['items'] if int(i[0]) <= 0]
        laning_items = [i for i in hero_build['items'] if int(i[0]) > 0 and int(i[0]) <= 10 * 60]
        early_game_items = [i for i in hero_build['items'] if int(i[0]) > 10 * 60 and int(i[0]) <= 21 * 60]
        mid_game_items = [i for i in hero_build['items'] if int(i[0]) > 21 * 60 and int(i[0]) <= 45 * 60]
        late_game_items = [i for i in hero_build['items'] if int(i[0]) > 45 * 60 and int(i[0]) <= 60 * 60]
        super_late_game_items = [i for i in hero_build['items'] if int(i[0]) > 60 * 60]
        rows += [
            {
                'match_id': hero_build['match_id'],
                'hero_name': hero_build['hero_name'],
                'skills': '\n'.join(hero_build['skills']),
                'lane': hero_build['lane'],
                'won': hero_build['won'],
                'player_name': hero_build['player_name'],
                'team': hero_build['team'],
                'major_items': '\n'.join([i for i in hero_build['major_items'] if i]),
                'pregame_items': to_item_ssv(pregame_items),
                'laning_items': to_item_ssv(laning_items),
                'early_game_items': to_item_ssv(early_game_items),
                'mid_game_items': to_item_ssv(mid_game_items),
                'late_game_items': to_item_ssv(late_game_items),
                'super_late_game_items': to_item_ssv(super_late_game_items),
            }
        ]

    return pd.DataFrame(rows)


# save data to draftdiff/opendota/matches/run_date={current_ds}/data.json.0001.gz
# time sleep for calls per min
# each file 20-50 MB
def main():
    response = requests.get('https://api.opendota.com/api/parsedMatches')
    parsed_matches = response.json()
    first_match_id = min([x['match_id'] for x in parsed_matches])
    response_pg2 = requests.get(f'https://api.opendota.com/api/parsedMatches?less_than_match_id={first_match_id}')
    parsed_matches_pg2 = response_pg2.json()
    first_match_id_pg2 = min([x['match_id'] for x in parsed_matches_pg2])
    response_first_match_id = requests.get(f'https://api.opendota.com/api/matches/{first_match_id}')
    first_match_id_response = response_first_match_id.json()
    return


def get_latest_parsed_match_ids() -> list[int]:
    match_id_list = []

    try:
        response = requests.get('https://api.opendota.com/api/parsedMatches', timeout=10)
        parsed_match_ids_dict = response.json()
        parsed_match_ids_list = [x['match_id'] for x in parsed_match_ids_dict]
        match_id_list.extend(parsed_match_ids_list)
        earliest_match_id = min([x['match_id'] for x in parsed_match_ids_dict])

        for _ in range(18):
            try:
                response_next_pg = requests.get(
                    f'https://api.opendota.com/api/parsedMatches?less_than_match_id={earliest_match_id}',
                    timeout=10,
                )
                parsed_match_ids_dict_next_pg = response_next_pg.json()
                parsed_match_ids_list_next_pg = [x['match_id'] for x in parsed_match_ids_dict_next_pg]
                match_id_list.extend(parsed_match_ids_list_next_pg)
                earliest_match_id = min([x['match_id'] for x in parsed_match_ids_dict_next_pg])
            except RequestException as e:
                print(f'Error fetching page with match_id < {earliest_match_id}: {e}')
                break  # Exit the loop if an error occurs during pagination

    except RequestException as e:
        print(f'Error fetching initial match IDs: {e}')

    return match_id_list


# change to only operate on one match id
def get_parsed_match_data(match_id: int, current_timeout: int = 0, max_timeout: int = 60 * 5) -> dict:
    if current_timeout > max_timeout:
        raise Exception('maxmimum retries reached')
    time.sleep(current_timeout)
    try:
        response = requests.get(f'https://api.opendota.com/api/matches/{match_id}', timeout=10)
        response.raise_for_status()  # Check for HTTP errors

        match_data = response.json()

        if not isinstance(match_data, dict):
            print(f'Unexpected data format for match ID {match_id}: {match_data}')
            # raise Exception(
            #     f"Unexpected data format for match ID {match_id}: {match_data}"
            # )
            return get_parsed_match_data(match_id=match_id, current_timeout=current_timeout + 30)

    except RequestException as e:
        print(f'Error fetching match ID {match_id} data: {e}')
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
    with gzip.GzipFile(fileobj=buf, mode='wb') as gz_file:
        gz_file.write(json_data.encode('utf-8'))

    # Get the compressed data from the buffer
    return buf.getvalue()


def upload_gzipped_string_to_s3(data: dict, data_partition: str, bucket_name: str = 'draftdiff'):
    """Uploads a gzipped string (JSON) to S3."""
    s3_client = boto3.client('s3')

    # Compress the data
    compressed_data = gzip_string_data(data)
    object_location = f'{data_partition}/data.json.gz'

    try:
        # Upload the gzipped string data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_location,
            Body=compressed_data,
            ContentType='application/json',
            ContentEncoding='gzip',
        )
        print(f'Gzipped data uploaded to s3://{bucket_name}/{object_location}')
    except Exception as e:
        print(f'Failed to upload gzipped data to S3: {e}')


def write_json_to_s3(data: list[dict], data_partition: str, bucket_name: str = 'draftdiff') -> None:
    object_location = f'{data_partition}/data.json.gz'
    try:
        boto3.client('s3').put_object(
            Bucket=bucket_name,
            Key=object_location,
            Body=json.dumps(data, indent=4).encode('utf-8'),
        )
        print(f'Data successfully written to s3://{bucket_name}/{object_location}')
    except Exception as e:
        print(f'Failed to write data to S3: {e}')


if __name__ == '__main__':
    ds = datetime.datetime.now().strftime('%Y-%m-%d')
    # partition_path = f"opendota/matches/run_date={ds}"
    match_ids = get_latest_parsed_match_ids()
    # match_ids = [7940186309, 7940185846, 7940185713, 7940185635, 7940185626]
    match_ids_files_in_s3 = list_all_s3_files('draftdiff', 'opendota/matches/')
    match_ids_in_s3 = [int(s.split('id=')[1].split('/')[0]) for s in match_ids_files_in_s3]

    for ii, match_id in tqdm(enumerate(match_ids), total=len(match_ids)):
        if match_id in match_ids_in_s3:
            continue
        match_data = get_parsed_match_data(match_id)
        upload_gzipped_string_to_s3(match_data, f'opendota/matches/id={match_id}')
        # transform match_data to pandas dataframe: take out lane_pos from players?
        # write once to matches_clean upload_parquet_to_s3
        # if ii > 4:
        #     break
    # write_json_to_s3(match_data, partition_path)
    print('done')
