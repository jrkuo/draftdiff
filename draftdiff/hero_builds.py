import argparse
import asyncio
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, TypedDict

import diskcache as dc
import pandas as pd
import requests
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from tqdm import tqdm

from draftdiff.models.opendota import MatchResponse

cache_dir = Path.home() / '.cache' / 'draftdiff'
cache_dir.mkdir(parents=True, exist_ok=True)
cache = dc.Cache(str(cache_dir / 'draftdiff_cache'))


async def _fetch_tournament_match_ids(url: str) -> list[int]:
    match_ids: list[int] = []

    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=url)  # type: ignore
        if hasattr(result, 'html') and result.html:  # type: ignore
            soup = BeautifulSoup(result.html, 'html.parser')  # type: ignore
            table = soup.find('table', class_='wikitable')
            table_anchor_tags = table.find_all('a')  # type: ignore
            hrefs = [str(a['href']) if 'href' in a.attrs else '' for a in table_anchor_tags]  # type: ignore
            dotabuff_hrefs = [h for h in hrefs if h.startswith('https://www.dotabuff.com/matches/')]
            match_ids = [int(h.split('/')[-1]) for h in dotabuff_hrefs]
        time.sleep(1)

    return match_ids


async def _fetch_opendota_match_id(match_id: int) -> MatchResponse:
    time.sleep(1.05)
    response = requests.get(f'https://api.opendota.com/api/matches/{match_id}')
    response.raise_for_status()
    response_dict: dict = response.json()  # type: ignore
    match_response: MatchResponse = MatchResponse(**response_dict)  # type: ignore
    return match_response


async def tournament_match_ids(url: str) -> list[int]:
    url_hash = hashlib.md5(url.encode()).hexdigest()
    cache_key = f'tournament_matches_{url_hash}'

    cached_result = cache.get(cache_key)  # type: ignore
    if cached_result is not None:
        return cached_result  # type: ignore

    match_ids = await _fetch_tournament_match_ids(url)
    # Cache for 30 days (1 month)
    cache.set(cache_key, match_ids, expire=2592000)  # type: ignore
    return match_ids


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
    skills: list[str]
    items: list[list[str]]
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

    if not match.players:
        raise Exception(f'No players found for match {match.match_id}')

    match_heroes: list[MatchHero] = []
    for player in match.players:
        player_won: bool = (match.radiant_win and player.isRadiant) or (not match.radiant_win and not player.isRadiant)
        match_heroes += [
            MatchHero(
                {
                    'match_id': match.match_id,
                    'hero_name': hero_id_to_hero[str(player.hero_id)]['localized_name'],
                    'items': [[str(i.time), str(i.key)] for i in player.purchase_log],  # type: ignore
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
                'skills': '\n'.join(hero_build['skills']),
                'lane': hero_build['lane'],
                'won': hero_build['won'],
                'pregame_items': to_item_ssv(pregame_items),
                'laning_items': to_item_ssv(laning_items),
                'early_game_items': to_item_ssv(early_game_items),
                'mid_game_items': to_item_ssv(mid_game_items),
                'late_game_items': to_item_ssv(late_game_items),
                'super_late_game_items': to_item_ssv(super_late_game_items),
            }
        ]

    return pd.DataFrame(rows)


async def main():
    parser = argparse.ArgumentParser(description='Analyze hero builds from tournament matches')
    parser.add_argument('--hero_name', type=str, required=True, help='Name of the hero to analyze')
    parser.add_argument(
        '--liquipedia-query', type=str, required=True, help='Liquipedia query URL for tournament matches'
    )

    args = parser.parse_args()

    hero_name = args.hero_name
    liquipedia_url = args.liquipedia_query

    print(f'Analyzing builds for hero: {hero_name}')
    print(f'Using Liquipedia query: {liquipedia_url}')

    # Call the function with parsed arguments
    match_ids = await tournament_match_ids(liquipedia_url)
    print(f'Found {len(match_ids)} match URLs')

    match_responses: list[MatchResponse] = []
    for match_id in tqdm(match_ids):
        match_resp = await opendota_match(match_id)
        match_responses += [match_resp]

    hero_builds: list[MatchHero] = []
    for match_response in match_responses:
        match_hero_builds: list[MatchHero] = await parse_match_heroes(match_response)
        hero_builds += match_hero_builds

    target_hero_builds: list[MatchHero] = [b for b in hero_builds if b['hero_name'] == hero_name]
    df_hero_builds: pd.DataFrame = do_hero_build_df(target_hero_builds)
    url_hash = hashlib.md5(liquipedia_url.encode()).hexdigest()
    df_hero_builds.to_csv(f'{hero_name}_{url_hash}.csv', index=False)

    return


if __name__ == '__main__':
    asyncio.run(main())
