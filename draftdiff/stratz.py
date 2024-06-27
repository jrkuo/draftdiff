import json
import os
from datetime import datetime

import boto3
import pandas as pd
import requests
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from draftdiff import constants, io, util
from loguru import logger
from tqdm import tqdm

HERO_ID_DICT = constants.HERO_ID_DICT
ID_HERO_DICT = constants.ID_HERO_DICT
local_DS_date = constants.local_DS_date


def get_matchup_stats_for_hero_name(token, hero_name) -> dict:
    heroid = HERO_ID_DICT[hero_name]
    url = "https://api.stratz.com/graphql"

    # Define headers if needed
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    # Define your GraphQL query
    graphql_request = {
        "operationName": "GetHeroMatchUps",
        "query": """query GetHeroMatchUps($heroId: Short!, $matchLimit: Int!, $bracketBasicIds: [RankBracketBasicEnum]) {
  heroStats {
    heroVsHeroMatchup(
      heroId: $heroId
      matchLimit: $matchLimit
      bracketBasicIds: $bracketBasicIds
    ) {
      advantage {
        heroId
        matchCountWith
        matchCountVs
        with {
          heroId2
          matchCount
          winCount
          synergy
          __typename
        }
        vs {
          heroId2
          matchCount
          winCount
          synergy
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
}""",
        "variables": {
            "heroId": int(heroid),
            "matchLimit": 0,
        },
    }
    # Send the POST request with the GraphQL query
    logger.info("Sending POST request with GraphQL query to stratz API")
    response = requests.post(url, json=graphql_request, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the response JSON
        print(response.status_code)
    else:
        print("Error:", response.status_code)

    data = response.json()
    return data


def get_cached_matchup_stats(ds, token, hero_name) -> dict:
    hero_string = get_stratz_slug_for_dota_hero(hero_name)
    partition_path = f"stratz/matchups/ds={ds}/hero={hero_string}"
    try:
        match_page_data = io.read_json(partition_path)
        logger.info(
            f"[{io.get_io_location()}] loaded cached data from {partition_path}"
        )
    except Exception:
        if ds != util.get_current_ds():
            raise ValueError(
                "Cannot run webscraping functions in the past. Can only run transforms on past data"
            )
        logger.warning(
            f"[{io.get_io_location()}] Running expensive matchup stats function for {hero_name}"
        )
        match_page_data = get_matchup_stats_for_hero_name(token, hero_name)
        io.write_dictlist_to_json(match_page_data, partition_path)
    return match_page_data


def test():
    os.environ["IO_LOCATION"] = "local"

    get_cached_matchup_stats(
        ds=util.get_current_ds(),
        token=os.environ["STRATZ_API_TOKEN"],
        hero_name="Arc Warden",
    )

    os.environ["IO_LOCATION"] = "s3"

    get_cached_matchup_stats(
        ds=util.get_current_ds(),
        token=os.environ["STRATZ_API_TOKEN"],
        hero_name="Arc Warden",
    )

    return


def build_stratz_stats_df(match_page_data) -> pd.DataFrame:
    new_records = []
    for row in match_page_data["data"]["heroStats"]["heroVsHeroMatchup"]["advantage"][
        0
    ]["vs"]:
        new_records += [
            {
                "counter_hero": ID_HERO_DICT[str(row["heroId2"])],
                "target_disadvantage": float(-row["synergy"]),
                "target_winrate_vs_counter": float(
                    (row["winCount"] / row["matchCount"]) * 100
                ),
                "counter_vs_target_matches_played": int(row["matchCount"]),
                "source": "stratz",
            }
        ]
    df_output = pd.json_normalize(new_records)
    return df_output


def get_stratz_slug_for_dota_hero(hero_name) -> str:
    slug = hero_name.lower().replace(" ", "-").replace("'", "")
    return slug


def get_cached_hero_counters_for_hero_name(ds, hero_name) -> pd.DataFrame:
    hero_string = get_stratz_slug_for_dota_hero(hero_name)
    partition_path = f"stratz/matchups-df/ds={ds}/hero={hero_string}"
    try:
        match_page_df = io.read_df(partition_path)
        logger.info(f"[{io.get_io_location()}] loaded cached df from {partition_path}")
    except Exception:
        logger.info(
            f"[{io.get_io_location()}] Running making df from json for {hero_name}"
        )
        match_page_data = io.read_json(f"stratz/matchups/ds={ds}/hero={hero_string}")
        match_page_df = build_stratz_stats_df(match_page_data)
        io.write_df_to_df(match_page_df, partition_path)
    return match_page_df


def main():
    # learn about curried functions later
    ds = util.get_current_ds()
    hero_list = list(constants.HERO_ID_DICT.keys())
    for hero_name in tqdm(hero_list):
        get_cached_matchup_stats(
            ds=ds, token=os.environ["STRATZ_API_TOKEN"], hero_name=hero_name
        )
        get_cached_hero_counters_for_hero_name(ds=ds, hero_name=hero_name)


if __name__ == "__main__":
    main()
