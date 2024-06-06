import json
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tqdm import tqdm


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


def get_cached_dotabuff_match_page(dotabuff_player_id, match_page_num) -> str:
    try:
        with open(
            f"./data/dotabuff-{dotabuff_player_id}-{match_page_num}.html", "rb"
        ) as rf:
            logger.info(f"using local data for player {dotabuff_player_id}")
            html_text = rf.read().decode("utf-8")
            return html_text
    except FileNotFoundError:
        with open(
            f"./data/dotabuff-{dotabuff_player_id}-{match_page_num}.html", "wb"
        ) as wf:
            match_page_text = get_dotabuff_match_page_text_for_player_id(
                dotabuff_player_id, match_page_num
            )
            wf.write(match_page_text.encode())
            return match_page_text


def build_dotabuff_player_stats_from_match_page_text(html_text) -> list:
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
    return new_records


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


def get_heroes_stats_for_dotabuff_id_in_last_n_days(player_id, n) -> pd.DataFrame:
    logger.info("get_dotabuff_match_page_text_for_player_id")
    matches_page_num = 0
    new_records = []
    current_date_time = datetime.now()
    earliest_difference_in_days = 0

    while earliest_difference_in_days <= n:
        # Keep downloading new pages and append to new_records
        matches_page_num += 1
        html_text = get_dotabuff_match_page_text_for_player_id(
            player_id, matches_page_num
        )
        new_records += build_dotabuff_player_stats_from_match_page_text(html_text)
        last_date_on_page = datetime.strptime(
            new_records[-1]["date_played"], "%Y-%m-%d"
        )
        earliest_date_difference = current_date_time - last_date_on_page
        earliest_difference_in_days = earliest_date_difference.days
    logger.info("finished downloading match pages")

    df_output = pd.json_normalize(new_records)

    filtered_df = get_filtered_df_with_rows_within_n_days(df_output, n)
    df_calculated_stats = agg_df_get_winrate_by_hero_role_lane(filtered_df)
    df_calculated_stats.loc[:, "dotabuff_player_id"] = player_id
    return df_calculated_stats


def get_dotabuff_slug_for_dota_hero(hero_name) -> str:
    slug = hero_name.lower().replace(" ", "-").replace("'", "")
    return slug


def get_dotabuff_counters_page_for_hero_name(hero_name) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    logger.warning(f"downloading dotabuff {hero_name} counters page")
    dotabuff_hero_slug = get_dotabuff_slug_for_dota_hero(hero_name)
    response = requests.get(
        f"https://dotabuff.com/heroes/{dotabuff_hero_slug}/counters",
        headers=headers,
    )
    return response.text


def get_cached_dotabuff_counters_page(hero_name) -> str:
    dotabuff_hero_slug = get_dotabuff_slug_for_dota_hero(hero_name)
    try:
        with open(f"./data/dotabuff-{dotabuff_hero_slug}-counters.html", "rb") as rf:
            logger.info(f"using local data for {dotabuff_hero_slug}")
            html_text = rf.read().decode("utf-8")
            return html_text
    except FileNotFoundError:
        with open(f"./data/dotabuff-{dotabuff_hero_slug}-counters.html", "wb") as wf:
            match_page_text = get_dotabuff_counters_page_for_hero_name(hero_name)
            wf.write(match_page_text.encode())
            return match_page_text


def build_dotabuff_counter_stats_from_hero_page_text(html_text) -> list:
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
    return new_records


def get_dotabuff_hero_counters_for_hero(hero_name) -> pd.DataFrame:
    html_text = get_dotabuff_counters_page_for_hero_name(hero_name)
    new_records = build_dotabuff_counter_stats_from_hero_page_text(html_text)
    df_output = pd.json_normalize(new_records)
    return df_output


def main():
    # build connections to external services (e.g. supabase, chrome browser, login w/ secrets)
    get_dotabuff_hero_counters_for_hero("Arc Warden")


if __name__ == "__main__":
    main()
