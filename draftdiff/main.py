import argparse
import os

from draftdiff.aggregation import (
    build_target_heroes_counter_heroes_df_for_dotabuff_id_in_last_n_days2,
    get_cached_weighted_avg_metrics_for_player_last_n_days,
)
from draftdiff.constants import HERO_ID_DICT
from draftdiff.dotabuff import (
    get_cached_counters_page,
    get_cached_dotabuff_match_pages_for_past_n_days,
    get_cached_hero_counters_for_hero,
    get_cached_heroes_stats_for_dotabuff_id_in_last_n_days,
)
from draftdiff.io import IOLocation
from draftdiff.stratz import (
    get_cached_hero_counters_for_hero_name,
    get_cached_matchup_stats,
)
from draftdiff.util import get_current_ds
from tqdm import tqdm


def main():
    # Create the parser
    parser = argparse.ArgumentParser(
        description="Process some player IDs and an IO location."
    )

    # Add the arguments
    parser.add_argument(
        "-p", "--player_ids", nargs="+", required=True, help="List of player IDs"
    )
    parser.add_argument(
        "-i",
        "--io_location",
        choices=["s3", "local"],
        required=True,
        help='Input/Output location (either "s3" or "local")',
    )

    # Parse the arguments
    args = parser.parse_args()

    # Assign arguments to variables
    player_ids = args.player_ids
    io_location = args.io_location

    # Print the values (for demonstration purposes)
    print(f"Player IDs: {player_ids}")
    print(f"IO Location: {io_location}")

    run_pipeline(io_location=io_location, player_ids=player_ids)


def run_pipeline(io_location, player_ids):
    ds = get_current_ds()
    hero_list = list(HERO_ID_DICT.keys())
    os.environ["IO_LOCATION"] = io_location
    n = 30
    player_dfs = {}
    web_dfs = {}
    for hero_name in tqdm(hero_list):
        get_cached_matchup_stats(
            ds=ds, token=os.environ["STRATZ_API_TOKEN"], hero_name=hero_name
        )
        wdf1 = get_cached_hero_counters_for_hero_name(ds=ds, hero_name=hero_name)
        get_cached_counters_page(ds=ds, hero_name=hero_name)
        wdf2 = get_cached_hero_counters_for_hero(ds=ds, hero_name=hero_name)
        web_dfs[hero_name] = {
            "stratz_counters": wdf1,
            "dotabuff_counters": wdf2,
        }
    for player_id in tqdm(player_ids):
        player_id = int(player_id)
        get_cached_dotabuff_match_pages_for_past_n_days(
            ds=ds, dotabuff_player_id=player_id, n=n
        )
        df1 = get_cached_heroes_stats_for_dotabuff_id_in_last_n_days(
            ds=ds, player_id=player_id, n=n
        )
        df2 = build_target_heroes_counter_heroes_df_for_dotabuff_id_in_last_n_days2(
            ds=ds, player_id=player_id, n=n
        )
        df3 = get_cached_weighted_avg_metrics_for_player_last_n_days(
            ds=ds, player_id=player_id, n=n
        )
        player_dfs[player_id] = {
            "dotabuff_stats": df1,
            "played_heroes_counters": df2,
            "played_heroes_weighted_avg_counters": df3,
        }
    return player_dfs, web_dfs


if __name__ == "__main__":
    main()
