import os

import pandas as pd
from draftdiff import constants, dotabuff, io, stratz, util
from loguru import logger
from tqdm import tqdm


def build_counter_heroes_df(ds) -> pd.DataFrame:
    hero_list = list(constants.HERO_ID_DICT.keys())
    partition_path = f"output/counter-df/ds={ds}"
    try:
        counters_df = io.read_df(partition_path)
        logger.info(f"[{io.get_io_location()}] loaded cached df from {partition_path}")
    except Exception:
        logger.info(f"[{io.get_io_location()}] Running making counter df from dfs")
        dotabuff_counter_dfs = {}
        stratz_counter_dfs = {}
        for hero in tqdm(hero_list):
            dotabuff_slug = dotabuff.get_dotabuff_slug_for_dota_hero(hero)
            dotabuff_counter_dfs[hero] = io.read_df(
                f"dotabuff/matchups-df/ds={ds}/hero={dotabuff_slug}"
            )
            stratz_slug = stratz.get_stratz_slug_for_dota_hero(hero)
            stratz_counter_dfs[hero] = io.read_df(
                f"stratz/matchups-df/ds={ds}/hero={stratz_slug}"
            )
        counters_df = build_counter_df(dotabuff_counter_dfs, stratz_counter_dfs)
        io.write_df_to_df(counters_df, partition_path)
    return counters_df


def build_counter_df(dotabuff_counter_dfs, stratz_counter_dfs):
    hero_list = list(constants.HERO_ID_DICT.keys())
    combined_dfs = {}
    for hero in tqdm(hero_list):
        dotabuff_counter_df = dotabuff_counter_dfs[hero]
        stratz_counter_df = stratz_counter_dfs[hero]
        db_stratz_df = pd.concat(
            [dotabuff_counter_df, stratz_counter_df], axis=0, ignore_index=True
        )

        output_rows = []
        grouped_df = db_stratz_df.groupby("counter_hero")

        for counter_hero, df_group in grouped_df:
            num_sources = df_group["source"].nunique()
            weighted_disadvantage = (
                df_group["target_disadvantage"]
                * df_group["counter_vs_target_matches_played"]
            ).sum() / df_group["counter_vs_target_matches_played"].sum()
            weighted_win_percent = (
                df_group["target_winrate_vs_counter"]
                * df_group["counter_vs_target_matches_played"]
            ).sum() / df_group["counter_vs_target_matches_played"].sum()
            total_head_to_head_matches = df_group[
                "counter_vs_target_matches_played"
            ].sum()

            output_rows += [
                {
                    "hero": hero,
                    "counter_hero": counter_hero,
                    "weighted_disadvantage": weighted_disadvantage,
                    "weighted_win_percent": weighted_win_percent,
                    "total_head_to_head_matches": total_head_to_head_matches,
                    "num_sources": num_sources,
                }
            ]
        output_df = pd.json_normalize(output_rows)
        combined_dfs[hero] = output_df
    result_df = pd.concat(combined_dfs.values(), ignore_index=True)
    return result_df


def main():
    ds = util.get_current_ds()
    hero_list = list(constants.HERO_ID_DICT.keys())
    for hero_name in tqdm(hero_list):
        stratz.get_cached_matchup_stats(
            ds=ds, token=os.environ["STRATZ_API_TOKEN"], hero_name=hero_name
        )
        stratz.get_cached_hero_counters_for_hero_name(ds=ds, hero_name=hero_name)
        dotabuff.get_cached_counters_page(ds=ds, hero_name=hero_name)
        dotabuff.get_cached_hero_counters_for_hero(ds=ds, hero_name=hero_name)
    build_counter_heroes_df(ds=ds)


if __name__ == "__main__":
    main()
