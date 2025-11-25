import os
import time
from typing import Any, TypedDict

import pandas as pd
import requests
from loguru import logger
from tqdm import tqdm

from draftdiff import constants, io, util


class HeroStatsVs(TypedDict):
    heroId2: int
    matchCount: int
    winCount: int
    synergy: float
    __typename: str


class Advantage(TypedDict):
    heroId: int
    matchCountWith: int
    matchCountVs: int
    with_: list[HeroStatsVs]  # 'with' renamed because it's a keyword
    vs: list[HeroStatsVs]
    __typename: str


class HeroVsHeroMatchup(TypedDict):
    advantage: list[Advantage]
    __typename: str


class HeroStats(TypedDict):
    heroVsHeroMatchup: HeroVsHeroMatchup
    __typename: str


class MatchupData(TypedDict):
    data: HeroStats


HERO_ID_DICT = constants.HERO_ID_DICT
ID_HERO_DICT = constants.ID_HERO_DICT


def get_league_match_ids(league_id: int) -> list[int]:
    url = 'https://api.stratz.com/graphql'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJTdWJqZWN0IjoiM2Q1NjViODgtNDFjZC00ZjM1LTk1MjItNWUxN2NiOGRmODVjIiwiU3RlYW1JZCI6IjY5NTc2MDYxIiwiQVBJVXNlciI6InRydWUiLCJuYmYiOjE3NTU2MzUwMDAsImV4cCI6MTc4NzE3MTAwMCwiaWF0IjoxNzU1NjM1MDAwLCJpc3MiOiJodHRwczovL2FwaS5zdHJhdHouY29tIn0.U1S14uLg-ml0z2oOFAJ_iSl-p77MJNjJFU9sT18nENg',
        'User-Agent': 'STRATZ_API',
    }

    graphql_request: dict[str, Any] = {
        'operationName': 'GetLeagueSeries',
        'query': """
            query GetLeagueSeries($leagueId: Int!, $take: Int!, $skip: Int!, $teamId: Int, $leagueStages: [LeagueStage]) {
            league(id: $leagueId) {
                series(
                take: $take
                skip: $skip
                teamId: $teamId
                leagueStageTypeIds: $leagueStages
                ) {
                matches {
                    id
                }
                }
            }
            }
        """,
        'variables': {
            'leagueId': league_id,
            'take': 1000,
            'skip': 0,
        },
    }
    logger.info('Sending POST request with GraphQL query to stratz API')
    time.sleep(1)
    response = requests.post(url, json=graphql_request, headers=headers)
    response.raise_for_status()
    data = response.json()
    match_ids = [m['matches'][0]['id'] for m in data['data']['league']['series']]
    return match_ids


def get_matchup_stats_for_hero_name(token, hero_name) -> MatchupData:
    heroid = HERO_ID_DICT[hero_name]
    url = 'https://api.stratz.com/graphql'

    # Define headers if needed
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'User-Agent': 'STRATZ_API',
    }

    # Define your GraphQL query
    graphql_request = {
        'operationName': 'GetHeroMatchUps',
        'query': """query GetHeroMatchUps($heroId: Short!, $matchLimit: Int!, $bracketBasicIds: [RankBracketBasicEnum]) {
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
        'variables': {
            'heroId': int(heroid),
            'matchLimit': 0,
        },
    }
    # Send the POST request with the GraphQL query
    logger.info('Sending POST request with GraphQL query to stratz API')
    response = requests.post(url, json=graphql_request, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the response JSON
        print(response.status_code)
    else:
        print('Error:', response.status_code)

    data = response.json()
    return data


def get_all_positions_heros_winrate_for_bracket(token, bracket) -> dict:
    bracket = bracket.upper()
    url = 'https://api.stratz.com/graphql'

    # Define headers if needed
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'User-Agent': 'STRATZ_API',
    }

    # Define your GraphQL query
    graphql_request = {
        'operationName': 'HeroesMetaPositions',
        'query': """query HeroesMetaPositions($bracketIds: [RankBracket], $take: Int, $skip: Int, $heroIds: [Short]) {
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
        'variables': {
            'bracketIds': [bracket],
            'take': 7,  # past week of data
            'skip': 0,
        },
    }
    # Send the POST request with the GraphQL query
    logger.info('Sending POST request with GraphQL query to stratz API')
    response = requests.post(url, json=graphql_request, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the response JSON
        print(response.status_code)
    else:
        print('Error:', response.status_code)

    data = response.json()
    return data


def get_all_synergies_for_bracket_week(token, bracket, week) -> dict:
    bracket = bracket.upper()
    url = 'https://api.stratz.com/graphql'

    # Define headers if needed
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'User-Agent': 'STRATZ_API',
    }

    # Define your GraphQL query
    graphql_request = {
        'operationName': 'Synergy',
        'query': """query Synergy($bracketBasicIds: [RankBracketBasicEnum], $matchLimit: Int, $take: Int, $heroIds: [Short], $week: Long) {
  heroStats {
    matchUp_Prev_Week_1: matchUp(
      bracketBasicIds: $bracketBasicIds
      matchLimit: $matchLimit
      take: $take
      week: $week
      heroIds: $heroIds
    ) {
      heroId
      vs {
        heroId2
        synergy
        matchCount
        __typename
      }
      with {
        heroId2
        synergy
        matchCount
        __typename
      }
      __typename
    }
  }
}
""",
        'variables': {
            'matchLimit': 0,
            'take': 200,
            'bracketBasicIds': bracket,
            'week': week,
        },
    }
    # Send the POST request with the GraphQL query
    logger.info('Sending POST request with GraphQL query to stratz API')
    response = requests.post(url, json=graphql_request, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the response JSON
        print(response.status_code)
    else:
        print('Error:', response.status_code)

    data = response.json()
    return data


def get_cached_matchup_stats(ds: str, token: str, hero_name: str) -> MatchupData:
    hero_string = get_stratz_slug_for_dota_hero(hero_name)
    partition_path = f'stratz/matchups/ds={ds}/hero={hero_string}'
    try:
        match_page_data = io.read_json(partition_path)
        logger.info(f'[{io.get_io_location()}] loaded cached data from {partition_path}')
    except Exception:
        if ds != util.get_current_ds():
            raise ValueError('Cannot run webscraping functions in the past. Can only run transforms on past data')
        logger.warning(f'[{io.get_io_location()}] Running expensive matchup stats function for {hero_name}')
        match_page_data = get_matchup_stats_for_hero_name(token, hero_name)
        io.write_dictlist_to_json(match_page_data, partition_path)
    return match_page_data


def test():
    os.environ['IO_LOCATION'] = 'local'

    get_cached_matchup_stats(
        ds=util.get_current_ds(),
        token=os.environ['STRATZ_API_TOKEN'],
        hero_name='Arc Warden',
    )

    os.environ['IO_LOCATION'] = 's3'

    get_cached_matchup_stats(
        ds=util.get_current_ds(),
        token=os.environ['STRATZ_API_TOKEN'],
        hero_name='Arc Warden',
    )

    return


def build_stratz_stats_df(match_page_data) -> pd.DataFrame:
    new_records = []
    for row in match_page_data['data']['heroStats']['heroVsHeroMatchup']['advantage'][0]['vs']:
        new_records += [
            {
                'counter_hero': ID_HERO_DICT[str(row['heroId2'])],
                'target_disadvantage': float(-row['synergy']),
                'target_winrate_vs_counter': float((row['winCount'] / row['matchCount']) * 100),
                'counter_vs_target_matches_played': int(row['matchCount']),
                'source': 'stratz',
            }
        ]
    df_output = pd.json_normalize(new_records)
    return df_output


# takes output from get_all_positions_heros_winrate_for_bracket and builds structured dictionary
def build_hero_position_winrate_for_bracket(stratz_api_data) -> dict:
    winrates = {
        (position.replace('heroesPos_', 'pos') if position.startswith('heroesPos_') else position): [
            {
                ('name' if key == 'heroId' else key): (
                    ID_HERO_DICT.get(str(value), 'Unknown Hero') if key == 'heroId' else value
                )
                for key, value in hero.items()
                if key in {'matchCount'} or key not in {'winCount', 'timestamp', '__typename'}
            }
            | {'winrate': f'{hero["winCount"] / hero["matchCount"]:.3f}' if hero['matchCount'] > 0 else 'N/A'}
            for hero in heroes
        ]
        for position, heroes in stratz_api_data['data']['heroStats'].items()
        if position.startswith('heroesPos_')
    }
    return winrates


def process_winrates_for_all_brackets():
    # List of rank names (brackets)
    bracket_list = list(constants.RANK_ID_DICT.keys())

    # Initialize dictionary to store winrates
    winrates = {'winrates': {}}

    # Loop through each bracket
    pbar = tqdm(bracket_list)
    for bracket in pbar:
        pbar.set_description(f'Processing {bracket}')

        # Fetch raw data for the current bracket
        raw_data = get_all_positions_heros_winrate_for_bracket(token=os.environ['STRATZ_API_TOKEN'], bracket=bracket)

        # Build the transformed data and store it in the winrates dictionary
        winrates['winrates'][f'bracket{constants.RANK_ID_DICT[bracket]}'] = build_hero_position_winrate_for_bracket(
            raw_data
        )

    # Return the final winrates dictionary
    return winrates


# takes output from get_all_synergies_for_bracket_week and builds structured dictionary
def build_matchup_synergy_counter_for_bracket(stratz_api_data) -> dict:
    matchups = {}
    for hero_entry in stratz_api_data['data']['heroStats']['matchUp_Prev_Week_1'][1:]:
        hero_id = hero_entry['heroId']

        if hero_id == 127:
            continue
        hero_name = ID_HERO_DICT.get(str(hero_id), f'Unknown({hero_id})')

        if hero_name not in matchups:
            matchups[hero_name] = {}

        synergy_dict = {entry['heroId2']: entry for entry in hero_entry['with']}
        counter_dict = {entry['heroId2']: entry for entry in hero_entry['vs']}

        all_opponents = set(synergy_dict.keys()) | set(counter_dict.keys())

        for opp_id in all_opponents:
            opp_name = ID_HERO_DICT.get(str(opp_id), f'Unknown({opp_id})')

            synergy = round(synergy_dict.get(opp_id, {}).get('synergy', 0), 3)
            counter = round(counter_dict.get(opp_id, {}).get('synergy', 0), 3)
            match_count_with = synergy_dict.get(opp_id, {}).get('matchCount', 0)
            match_count_vs = counter_dict.get(opp_id, {}).get('matchCount', 0)

            matchups[hero_name][opp_name] = {
                'synergy': synergy,
                'counter': counter,
                'matchCountWith': match_count_with,
                'matchCountVs': match_count_vs,
            }

    return matchups


def process_matchups_for_all_brackets():
    # List of rank names (brackets)
    bracket_list = list(constants.RANK_ENUM_ID_DICT.keys())
    week = get_unix_timestamp_7_days_ago()

    # Initialize dictionary to store matchups
    matchups = {'matchups': {}}

    # Loop through each bracket
    pbar = tqdm(bracket_list)
    for bracket in pbar:
        pbar.set_description(f'Processing {bracket}')

        # Fetch raw data for the current bracket
        raw_data = get_all_synergies_for_bracket_week(token=os.environ['STRATZ_API_TOKEN'], bracket=bracket, week=week)

        # Build the transformed data and store it in the matchups dictionary
        matchups['matchups'][f'bracket{constants.RANK_ENUM_ID_DICT[bracket]}'] = (
            build_matchup_synergy_counter_for_bracket(raw_data)
        )

    # Return the final matchups dictionary
    return matchups


def get_stratz_slug_for_dota_hero(hero_name: str) -> str:
    slug = hero_name.lower().replace(' ', '-').replace("'", '')
    return slug


def get_cached_hero_counters_for_hero_name(ds: str, hero_name: str) -> pd.DataFrame:
    hero_string = get_stratz_slug_for_dota_hero(hero_name)
    partition_path = f'stratz/matchups-df/ds={ds}/hero={hero_string}'
    try:
        match_page_df = io.read_df(partition_path)
        logger.info(f'[{io.get_io_location()}] loaded cached df from {partition_path}')
    except Exception:
        logger.info(f'[{io.get_io_location()}] Running making df from json for {hero_name}')
        match_page_data = io.read_json(f'stratz/matchups/ds={ds}/hero={hero_string}')
        match_page_df = build_stratz_stats_df(match_page_data)
        io.write_df_to_df(match_page_df, partition_path)
    return match_page_df


def get_unix_timestamp_7_days_ago() -> int:
    seconds_in_seven_days = 7 * 24 * 60 * 60
    timestamp = int(time.time()) - seconds_in_seven_days
    return timestamp


def main():
    # learn about curried functions later
    ds = util.get_current_ds()
    hero_list = list(constants.HERO_ID_DICT.keys())
    for hero_name in tqdm(hero_list):
        get_cached_matchup_stats(ds=ds, token=os.environ['STRATZ_API_TOKEN'], hero_name=hero_name)
        get_cached_hero_counters_for_hero_name(ds=ds, hero_name=hero_name)


def main_test():
    ds = util.get_current_ds()
    process_winrates_for_all_brackets()
    # process_matchups_for_all_brackets()


def format_hero_winrates_all_brackets(token: str) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Fetch and format win rates for all heroes across all 8 brackets and 5 positions.

    Args:
        token: Stratz API token

    Returns:
        Dictionary with structure: {bracket_name: {position: [{hero, winrate, matches}]}}
    """
    bracket_list = list(constants.RANK_ID_DICT.keys())
    winrates: dict[str, dict[str, list[dict[str, Any]]]] = {}

    pbar = tqdm(bracket_list, desc='Fetching win rates')
    for bracket in pbar:
        pbar.set_description(f'Fetching win rates: {bracket}')

        # Fetch raw data for the current bracket
        raw_data = get_all_positions_heros_winrate_for_bracket(token=token, bracket=bracket)

        # Build the transformed data and store it with bracket name as key
        winrates[bracket] = build_hero_position_winrate_for_bracket(raw_data)

    return winrates


def format_synergies_counters_all_brackets(token: str) -> dict[str, dict[str, dict[str, dict[str, Any]]]]:
    """Fetch and format synergies/counters for all hero pairs across all 4 grouped brackets.

    Args:
        token: Stratz API token

    Returns:
        Dictionary with structure: {bracket_group: {hero1: {hero2: {synergy, counter, matchCountWith, matchCountVs}}}}
    """
    bracket_list = list(constants.RANK_ENUM_ID_DICT.keys())
    week = get_unix_timestamp_7_days_ago()
    matchups: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}

    pbar = tqdm(bracket_list, desc='Fetching synergies/counters')
    for bracket in pbar:
        pbar.set_description(f'Fetching synergies/counters: {bracket}')

        # Fetch raw data for the current bracket
        raw_data = get_all_synergies_for_bracket_week(token=token, bracket=bracket, week=week)

        # Build the transformed data and store it with bracket name as key
        matchups[bracket] = build_matchup_synergy_counter_for_bracket(raw_data)

    return matchups


if __name__ == '__main__':
    main_test()
