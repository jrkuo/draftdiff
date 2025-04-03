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


def get_matchup_stats_for_hero_name(token, hero_name) -> dict:
    heroid = HERO_ID_DICT[hero_name]
    url = "https://api.stratz.com/graphql"

    # Define headers if needed
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "STRATZ_API",
    }

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


def get_all_positions_heros_winrate_for_bracket(token, bracket) -> dict:
    bracket = bracket.upper()
    url = "https://api.stratz.com/graphql"

    # Define headers if needed
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "STRATZ_API",
    }

    # Define your GraphQL query
    graphql_request = {
        "operationName": "HeroesMetaPositions",
        "query": """query HeroesMetaPositions($bracketIds: [RankBracket], $take: Int, $skip: Int, $heroIds: [Short]) {
  heroStats {
    heroesPos_1: winDay(
      take: $take
      skip: $skip
      positionIds: [POSITION_1]
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    heroesPos_2: winDay(
      take: $take
      skip: $skip
      positionIds: [POSITION_2]
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    heroesPos_3: winDay(
      take: $take
      skip: $skip
      positionIds: [POSITION_3]
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    heroesPos_4: winDay(
      take: $take
      skip: $skip
      positionIds: [POSITION_4]
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    heroesPos_5: winDay(
      take: $take
      skip: $skip
      positionIds: [POSITION_5]
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    heroes: winDay(
      take: $take
      skip: $skip
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    __typename
  }
}""",
        "variables": {
            "bracketIds": [bracket],
            "take": 7,  # past week of data
            "skip": 0,
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


# TODO:
def get_all_synergies_for_bracket(token, bracket) -> dict:
    bracket = bracket.upper()
    url = "https://api.stratz.com/graphql"

    # Define headers if needed
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "STRATZ_API",
    }

    # Define your GraphQL query
    graphql_request = {
        "operationName": "HeroesMetaPositions",
        "query": """query HeroesMetaPositions($bracketIds: [RankBracket], $take: Int, $skip: Int, $heroIds: [Short]) {
  heroStats {
    heroesPos_1: winDay(
      take: $take
      skip: $skip
      positionIds: [POSITION_1]
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    heroesPos_2: winDay(
      take: $take
      skip: $skip
      positionIds: [POSITION_2]
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    heroesPos_3: winDay(
      take: $take
      skip: $skip
      positionIds: [POSITION_3]
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    heroesPos_4: winDay(
      take: $take
      skip: $skip
      positionIds: [POSITION_4]
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    heroesPos_5: winDay(
      take: $take
      skip: $skip
      positionIds: [POSITION_5]
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    heroes: winDay(
      take: $take
      skip: $skip
      bracketIds: $bracketIds
      heroIds: $heroIds
    ) {
      heroId
      matchCount
      winCount
      timestamp: day
      __typename
    }
    __typename
  }
}""",
        "variables": {
            "bracketIds": [bracket],
            # "take": 7,
            "skip": 0,
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


# takes output from get_all_positions_heros_winrate_for_bracket and builds structured dictionary
def build_hero_position_winrate_for_bracket(stratz_api_data) -> dict:
    new_dict = {
        (
            position.replace("heroesPos_", "pos")
            if position.startswith("heroesPos_")
            else position
        ): [
            {
                ("name" if key == "heroId" else key): (
                    ID_HERO_DICT.get(str(value), "Unknown Hero")
                    if key == "heroId"
                    else value
                )
                for key, value in hero.items()
                if key in {"matchCount"}
                or key not in {"winCount", "timestamp", "__typename"}
            }
            | {
                "winrate": f"{hero['winCount'] / hero['matchCount']:.2f}"
                if hero["matchCount"] > 0
                else "N/A"
            }
            for hero in heroes
        ]
        for position, heroes in stratz_api_data["data"]["heroStats"].items()
        if position.startswith("heroesPos_")
    }
    return new_dict


def process_winrates_for_all_brackets():
    # List of rank names (brackets)
    bracket_list = list(constants.RANK_ID_DICT.keys())

    # Initialize dictionary to store winrates
    winrates = {"winrates": {}}

    # Loop through each bracket
    for bracket in tqdm(bracket_list, desc="Processing brackets"):
        tqdm.set_description(f"Processing {bracket}")

        # Fetch raw data for the current bracket
        raw_data = get_all_positions_heros_winrate_for_bracket(
            token=os.environ["STRATZ_API_TOKEN"], bracket=bracket
        )

        # Build the transformed data and store it in the winrates dictionary
        winrates["winrates"][f"bracket{constants.RANK_ID_DICT[bracket]}"] = (
            build_hero_position_winrate_for_bracket(raw_data)
        )

    # Return the final winrates dictionary
    return winrates


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


def main_test():
    ds = util.get_current_ds()
    process_winrates_for_all_brackets()


if __name__ == "__main__":
    main_test()
