import argparse
import asyncio
import hashlib
import time
from pathlib import Path

import diskcache as dc
import pandas as pd
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from tqdm import tqdm

from draftdiff.models.opendota import MatchResponse
from draftdiff.opendota import MatchHero, do_hero_build_df, opendota_match, parse_match_heroes
from draftdiff.stratz import get_league_match_ids

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


async def tournament_match_ids(url: str) -> list[int]:
    url_hash = hashlib.md5(url.encode()).hexdigest()
    cache_key = f'tournament_matches_{url_hash}'

    cached_result = cache.get(cache_key)  # type: ignore
    if cached_result is not None:
        return cached_result  # type: ignore

    match_ids = await _fetch_tournament_match_ids(url)
    cache.set(cache_key, match_ids, expire=2592000)  # type: ignore
    return match_ids


async def league_match_ids(league_id: int) -> list[int]:
    cache_key = f'league_matches_{league_id}'

    cached_result = cache.get(cache_key)  # type: ignore
    if cached_result is not None:
        return cached_result  # type: ignore

    match_ids = get_league_match_ids(league_id)
    cache.set(cache_key, match_ids, expire=604800)  # type: ignore
    return match_ids


async def main():
    parser = argparse.ArgumentParser(description='Analyze hero builds from tournament matches')
    parser.add_argument(
        '--liquipedia-query', type=str, required=False, help='Liquipedia query URL for tournament matches'
    )
    parser.add_argument('--stratz-league-id', type=int, required=False, help='Dotabuff league URL')

    args = parser.parse_args()

    liquipedia_url = args.liquipedia_query
    league_id = args.stratz_league_id

    if liquipedia_url and league_id:
        raise ValueError('Cannot specify both --liquipedia-query and ---stratz-league-id')

    if not liquipedia_url and not league_id:
        raise ValueError('Must specify either --liquipedia-query or ---stratz-league-id')

    if liquipedia_url:
        print(f'Using Liquipedia query: {liquipedia_url}')
        match_ids = await tournament_match_ids(liquipedia_url)
        source = liquipedia_url
        source_type = 'tournament'
    else:
        print(f'Using League ID: {league_id}')
        match_ids = await league_match_ids(league_id)
        source = str(league_id)
        source_type = 'stratz_league'

    print(f'Found {len(match_ids)} match IDs')

    match_responses: list[MatchResponse] = []
    for match_id in tqdm(match_ids):
        match_resp = await opendota_match(match_id)
        match_responses += [match_resp]

    hero_builds: list[MatchHero] = []
    for match_response in match_responses:
        match_hero_builds: list[MatchHero] = await parse_match_heroes(match_response)
        hero_builds += match_hero_builds

    df_hero_builds: pd.DataFrame = do_hero_build_df(hero_builds)
    url_hash = hashlib.md5(source.encode()).hexdigest()

    output_dir = Path.cwd() / 'data' / 'hero_builds'
    output_dir.mkdir(parents=True, exist_ok=True)

    df_hero_builds.to_csv(output_dir / f'{source_type}_{url_hash}.csv', index=False)

    return


if __name__ == '__main__':
    asyncio.run(main())
