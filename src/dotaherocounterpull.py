import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tqdm import tqdm


def download_dotabuff_match_page(id, matches_page_num):
    # Avoid rate limiting
    headers = {"User-Agent": "Mozilla/5.0"}
    """ try:
        with open(f"./data/dotabuff-{id}-{matches_page_num}.html", "rb") as rf:
            logger.info(f"using local data for player {id}")
            html_text = rf.read().decode("utf-8")
    except FileNotFoundError: """
    # Download first page of matches
    logger.info(f"downloading dotabuff page {matches_page_num} data for player {id}")
    response = requests.get(
        f"https://dotabuff.com/players/{id}/matches?enhance=overview&page={matches_page_num}",
        headers=headers,
    )
    with open(f"./data/dotabuff-{id}-{matches_page_num}.html", "wb") as wf:
        wf.write(response.text.encode())
    return response.text


def process_dotabuff_player_matches_html_text(html_text):
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
        hero_name = hero.find("img")["title"]
        icons = role.find_all("i")
        roles = [icon["title"] for icon in icons]
        if not roles:
            continue
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


def filter_dataframe_by_n_days_from_now(df, n):
    # remove rows that are beyond n days
    current_date_time = datetime.now()
    df["date_played"] = pd.to_datetime(df["date_played"])
    df["date_difference"] = (current_date_time - df["date_played"]).dt.days
    filtered_df = df[df["date_difference"] <= n]
    return filtered_df


def calculate_hero_stats_in_df(df):
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
                "win_rate": win_rate,
            }
        ]
    return pd.json_normalize(new_records)


def get_heroes_stats_for_dotabuff_id_in_last_n_days(id, n):
    print("working")
    matches_page_num = 1

    html_text = download_dotabuff_match_page(id, matches_page_num)
    new_records = process_dotabuff_player_matches_html_text(html_text)
    print("done")

    current_date_time = datetime.now()
    last_date_on_page = datetime.strptime(new_records[-1]["date_played"], "%Y-%m-%d")
    earliest_date_difference = current_date_time - last_date_on_page
    earliest_difference_in_days = earliest_date_difference.days

    while earliest_difference_in_days <= n:
        # Keep downloading new pages and append to new_records
        matches_page_num += 1
        html_text = download_dotabuff_match_page(id, matches_page_num)
        new_records += process_dotabuff_player_matches_html_text(html_text)
        print("done")
        last_date_on_page = datetime.strptime(
            new_records[-1]["date_played"], "%Y-%m-%d"
        )
        earliest_date_difference = current_date_time - last_date_on_page
        earliest_difference_in_days = earliest_date_difference.days
    print("finished downloading match pages")

    df_output = pd.json_normalize(new_records)

    filtered_df = filter_dataframe_by_n_days_from_now(df_output, n)
    df_calculated_stats = calculate_hero_stats_in_df(filtered_df)
    filtered_df["dotabuff_id"] = id
    df_calculated_stats.to_csv(f"dotabuff_hero_stats_{id}.csv")
    print("done")


get_heroes_stats_for_dotabuff_id_in_last_n_days("181567803", 30)
