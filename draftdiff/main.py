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
from draftdiff.io import IOLocation, get_io_location, read_df, write_df_to_df
from draftdiff.stratz import (
    get_cached_hero_counters_for_hero_name,
    get_cached_matchup_stats,
)
from draftdiff.util import get_current_ds
from draftdiff.writetosheets import create_new_sheet, create_pivot_table
from loguru import logger
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


def run_pipeline(io_location, player_ids, n=30):
    ds = get_current_ds()
    hero_list = list(HERO_ID_DICT.keys())
    os.environ["IO_LOCATION"] = io_location
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
        os.environ["IO_LOCATION"] = "sheets"
        create_new_sheet(
            spreadsheet_id="19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc",
            sheet_name=f"{player_id}-{ds}-{n}days-data",
            key_file_path="credentials.json",
        )
        write_df_to_df(
            df3,
            spreadsheet_id="19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc",
            sheet_name=f"{player_id}-{ds}-{n}days-data",
            start_cell="A1",
            key_file_path="credentials.json",
        )
        create_new_sheet(
            spreadsheet_id="19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc",
            sheet_name=f"{player_id}-{ds}-{n}days-pivot",
            key_file_path="credentials.json",
        )
        create_pivot_table(
            key_file_path="credentials.json",
            spreadsheet_id="19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc",
            destinationSheetName=f"{player_id}-{ds}-{n}days-pivot",
            sourceSheetName=f"{player_id}-{ds}-{n}days-data",
            rowSourceColumn=1,
            columnSourceColumn=0,
            valueSourceColumn=4,
        )
    return player_dfs, web_dfs


# not using
def write_output_to_sheets(ds, player_id, days):
    partition_path = (
        f"output/player_counters_weighted-df/ds={ds}/player_id={player_id}/days={days}"
    )
    df3 = read_df(partition_path)
    os.environ["IO_LOCATION"] = "sheets"
    logger.info(
        f"[{get_io_location()}] Writing df to sheets for player {player_id} for last {days} days"
    )
    write_df_to_df(
        df3,
        f"output/player_counters_weighted-sheets/ds={ds}/player_id={player_id}/days={days}",
    )
    return


if __name__ == "__main__":
    main()
    # ds = get_current_ds()
    # player_id = "69576061"
    # days = 30
    # write_output_to_sheets(ds=ds, player_id=player_id, days=days)
