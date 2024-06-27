import glob
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import boto3
import pandas as pd
import requests
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from bs4 import BeautifulSoup
from draftdiff import constants, io, util
from loguru import logger
from tqdm import tqdm

local_DS = constants.local_DS
local_DS_date = constants.local_DS_date


def get_dotabuff_match_page_text_for_player_id(
    dotabuff_player_id, match_page_num
) -> str:
    # Avoid rate limiting
    headers = {"User-Agent": "Mozilla/5.0"}
    logger.warning(
        f"downloading dotabuff page {match_page_num} data for player {dotabuff_player_id}"
    )
    response = requests.get(
        f"https://dotabuff.com/players/{dotabuff_player_id}/matches?enhance=overview&page={match_page_num}",
        headers=headers,
    )
    return response.text


def get_cached_dotabuff_match_pages_for_past_n_days(ds, dotabuff_player_id, n):
    match_page_num = 1
    earliest_difference_in_days = 0

    while earliest_difference_in_days <= n:
        partition_path = f"dotabuff/ds={ds}/player_id={dotabuff_player_id}/days={n}/page-{match_page_num}"
        try:
            match_page_text = io.read_html(partition_path)
            logger.info(
                f"[{io.get_io_location()}] loaded cached data from {partition_path}"
            )
        except Exception:
            if ds != util.get_current_ds():
                raise ValueError(
                    "Cannot run webscraping functions in the past. Can only run transforms on past data"
                )
            logger.warning(
                f"[{io.get_io_location()}] Running expensive match page function for player {dotabuff_player_id}"
            )
            match_page_text = get_dotabuff_match_page_text_for_player_id(
                dotabuff_player_id, match_page_num
            )
            io.write_html_to_html(match_page_text, partition_path)
        soup = BeautifulSoup(match_page_text, "html.parser")
        tables = soup.find_all("table")
        table = tables[0]
        tr = table.find_all("tr")
        last_row = tr[-1]
        last_data_vals = last_row.find_all("td")
        hero, matchid, role, result, type, duration, kda, items = last_data_vals
        last_date_on_page = result.find("time").get_text()
        last_date_on_page = datetime.strptime(last_date_on_page, "%Y-%m-%d")
        earliest_date_difference = datetime.strptime(ds, "%Y-%m-%d") - last_date_on_page
        earliest_difference_in_days = earliest_date_difference.days
        match_page_num += 1
    logger.info(f"finished downloading match pages for player {dotabuff_player_id}")
    return


def build_dotabuff_player_stats_from_match_page_text(html_text) -> pd.DataFrame:
    new_records = []
    soup = BeautifulSoup(html_text, "html.parser")

    # Find all tables in the HTML
    tables = soup.find_all("table")
    assert len(tables) == 1
    table = tables[0]
    # Find all rows
    tr = table.find_all("tr")
    """ header_row = tr[0]
    header_cols = header_row.find_all("th")
    [val.contents for val in header_cols]  ## wrong num """
    for row in tr[1:]:
        data_vals = row.find_all("td")
        hero, matchid, role, result, type, duration, kda, items = data_vals
        icons = role.find_all("i")
        roles = [icon["title"] for icon in icons]
        if not roles:
            continue
        hero_name = matchid.find("a").get_text()
        match_result = result.find("a").get_text()
        date_played = result.find("time").get_text()
        new_records += [
            {
                "hero": hero_name,
                "role": roles[0],
                "lane": roles[1],
                "result": match_result,
                "date_played": date_played,
            }
        ]
        print(hero_name)
    df = pd.json_normalize(new_records)
    return df


def get_filtered_df_with_rows_within_n_days(df, n) -> pd.DataFrame:
    # keep rows within n days
    current_date_time = datetime.now()
    df["date_played"] = pd.to_datetime(df["date_played"])
    df["date_difference"] = (current_date_time - df["date_played"]).dt.days
    filtered_df = df[df["date_difference"] <= n]
    return filtered_df


def agg_df_get_winrate_by_hero_role_lane(df) -> pd.DataFrame:
    # calculate aggregate stats based on hero, role, and lane
    result_mapping = {"Won Match": 1, "Lost Match": 0}
    new_records = []
    for hero_group, group_df in df.groupby(["hero", "role", "lane"]):
        num_matches = len(group_df)
        num_wins = group_df["result"].map(result_mapping).sum()
        win_rate = num_wins / num_matches
        hero, role, lane = hero_group
        new_records += [
            {
                "hero": hero,
                "role": role,
                "lane": lane,
                "num_matches": num_matches,
                "num_wins": num_wins,
                "win_rate": win_rate,
            }
        ]
    return pd.json_normalize(new_records)


def get_cached_heroes_stats_for_dotabuff_id_in_last_n_days(
    ds, player_id, n
) -> pd.DataFrame:
    partition_path = f"dotabuff/player_stats-df/ds={ds}/player_id={player_id}/days={n}"
    try:
        player_stats_df = io.read_df(partition_path)
        logger.info(f"[{io.get_io_location()}] loaded cached df from {partition_path}")
    except Exception:
        logger.info(
            f"[{io.get_io_location()}] Running making df from html for player {player_id} for last {n} days"
        )
        base_directory = f"dotabuff/ds={ds}/player_id={player_id}/days={n}"
        file_paths = io.get_file_paths(base_directory)
        all_records = []
        for file_path in file_paths:
            match_page_data = io.read_html(file_path)
            match_page_df = build_dotabuff_player_stats_from_match_page_text(
                match_page_data
            )
            all_records.append(match_page_df)
        df_output = pd.concat(all_records, ignore_index=True)
        filtered_df = get_filtered_df_with_rows_within_n_days(df_output, n)
        player_stats_df = agg_df_get_winrate_by_hero_role_lane(filtered_df)
        player_stats_df.loc[:, "dotabuff_player_id"] = player_id
        io.write_df_to_df(player_stats_df, partition_path)
    return player_stats_df


def get_dotabuff_slug_for_dota_hero(hero_name) -> str:
    slug = hero_name.lower().replace(" ", "-").replace("'", "")
    return slug


def get_counters_page_for_hero_name(hero_name) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    logger.warning(f"downloading dotabuff {hero_name} counters page")
    dotabuff_hero_slug = get_dotabuff_slug_for_dota_hero(hero_name)
    response = requests.get(
        f"https://dotabuff.com/heroes/{dotabuff_hero_slug}/counters",
        headers=headers,
    )
    return response.text


def get_cached_counters_page(ds, hero_name) -> str:
    dotabuff_hero_slug = get_dotabuff_slug_for_dota_hero(hero_name)
    partition_path = f"dotabuff/matchups/ds={ds}/hero={dotabuff_hero_slug}"
    try:
        match_page_data = io.read_html(partition_path)
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
        match_page_data = get_counters_page_for_hero_name(hero_name)
        io.write_html_to_html(match_page_data, partition_path)
    return match_page_data


def test():
    os.environ["IO_LOCATION"] = "local"

    get_cached_counters_page(
        ds=util.get_current_ds(),
        hero_name="Arc Warden",
    )

    os.environ["IO_LOCATION"] = "s3"

    get_cached_counters_page(
        ds=util.get_current_ds(),
        hero_name="Arc Warden",
    )

    return


def test2():
    os.environ["IO_LOCATION"] = "local"
    get_cached_heroes_stats_for_dotabuff_id_in_last_n_days(
        ds=util.get_current_ds(), player_id="181567803", n=30
    )

    os.environ["IO_LOCATION"] = "s3"
    get_cached_heroes_stats_for_dotabuff_id_in_last_n_days(
        ds=util.get_current_ds(), player_id="181567803", n=30
    )

    return


def test3():
    os.environ["IO_LOCATION"] = "local"
    get_cached_dotabuff_match_pages_for_past_n_days(
        ds=util.get_current_ds(), dotabuff_player_id="181567803", n=30
    )

    return


def test4():
    os.environ["IO_LOCATION"] = "local"
    get_cached_heroes_stats_for_dotabuff_id_in_last_n_days(
        ds=util.get_current_ds(), player_id="181567803", n=30
    )
    return


def build_dotabuff_counter_stats_from_hero_page_text(html_text) -> pd.DataFrame:
    new_records = []
    soup = BeautifulSoup(html_text, "html.parser")

    # Find sortable table in HTML
    tables = soup.find_all("table", class_="sortable")
    assert len(tables) == 1
    table = tables[0]
    # Find all rows
    tr = table.find_all("tr")
    for i, row in enumerate(tr[1:]):
        data_vals = row.find_all("td")
        (
            counter_hero_image_name,
            counter_hero,
            disadvantage,
            target_winrate,
            counter_target_matches_played,
        ) = data_vals
        counter_hero_name = counter_hero.find("a").get_text()
        target_disadvantage = float(disadvantage["data-value"])
        target_winrate_vs_counter = float(target_winrate["data-value"])
        counter_vs_target_matches_played = int(
            counter_target_matches_played["data-value"]
        )
        new_records += [
            {
                "counter_hero": counter_hero_name,
                "target_disadvantage": target_disadvantage,
                "target_winrate_vs_counter": target_winrate_vs_counter,
                "counter_vs_target_matches_played": counter_vs_target_matches_played,
                "source": "dotabuff",
            }
        ]
    df_output = pd.json_normalize(new_records)
    return df_output


def get_cached_hero_counters_for_hero(ds, hero_name) -> pd.DataFrame:
    dotabuff_hero_slug = get_dotabuff_slug_for_dota_hero(hero_name)
    partition_path = f"dotabuff/matchups-df/ds={ds}/hero={dotabuff_hero_slug}"
    try:
        match_page_df = io.read_df(partition_path)
        logger.info(f"[{io.get_io_location()}] loaded cached df from {partition_path}")
    except Exception:
        logger.info(
            f"[{io.get_io_location()}] Running making df from html for {hero_name}"
        )
        match_page_data = io.read_html(
            f"dotabuff/matchups/ds={ds}/hero={dotabuff_hero_slug}"
        )
        match_page_df = build_dotabuff_counter_stats_from_hero_page_text(
            match_page_data
        )
        io.write_df_to_df(match_page_df, partition_path)
    return match_page_df


def main():
    # build connections to external services (e.g. supabase, chrome browser, login w/ secrets)
    ds = util.get_current_ds()
    player_id_list = ["181567803"]
    n = 30
    hero_list = list(constants.hero_id_dict.keys())
    for hero_name in tqdm(hero_list):
        get_cached_counters_page(ds=ds, hero_name=hero_name)
        get_cached_hero_counters_for_hero(ds=ds, hero_name=hero_name)
    for id in tqdm(player_id_list):
        get_cached_dotabuff_match_pages_for_past_n_days(
            ds=ds, dotabuff_player_id=id, n=n
        )
        get_cached_heroes_stats_for_dotabuff_id_in_last_n_days(ds=ds, player_id=id, n=n)


if __name__ == "__main__":
    main()
