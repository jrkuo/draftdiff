import argparse
import os

from draftdiff.aggregation2 import build_counter_heroes_df
from draftdiff.constants import HERO_ID_DICT
from draftdiff.dotabuff import (
    get_cached_counters_page,
    get_cached_hero_counters_for_hero,
)
from draftdiff.io import write_df_to_df
from draftdiff.stratz import (
    get_cached_hero_counters_for_hero_name,
    get_cached_matchup_stats,
)
from draftdiff.util import get_current_ds
from draftdiff.writetosheets import create_new_sheet, create_pivot_table
from tqdm import tqdm


def main():
    # Create the parser
    parser = argparse.ArgumentParser(
        description="Process some player IDs and an IO location."
    )

    # Add the arguments
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
    io_location = args.io_location

    # Print the values (for demonstration purposes)
    print(f"IO Location: {io_location}")

    run_pipeline(io_location=io_location)


def run_pipeline(io_location):
    ds = get_current_ds()
    hero_list = list(HERO_ID_DICT.keys())
    os.environ["IO_LOCATION"] = io_location

    for hero_name in tqdm(hero_list):
        get_cached_matchup_stats(
            ds=ds, token=os.environ["STRATZ_API_TOKEN"], hero_name=hero_name
        )
        get_cached_hero_counters_for_hero_name(ds=ds, hero_name=hero_name)
        get_cached_counters_page(ds=ds, hero_name=hero_name)
        get_cached_hero_counters_for_hero(ds=ds, hero_name=hero_name)
    df1 = build_counter_heroes_df(ds=ds)
    os.environ["IO_LOCATION"] = "sheets"
    create_new_sheet(
        spreadsheet_id="19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc",
        sheet_name=f"{ds}-data",
        key_file_path="credentials.json",
    )
    write_df_to_df(
        df1,
        spreadsheet_id="19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc",
        sheet_name=f"{ds}-data",
        start_cell="A1",
        key_file_path="credentials.json",
    )
    create_new_sheet(
        spreadsheet_id="19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc",
        sheet_name=f"{ds}-pivot",
        key_file_path="credentials.json",
    )
    create_pivot_table(
        key_file_path="credentials.json",
        spreadsheet_id="19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc",
        destinationSheetName=f"{ds}-pivot",
        sourceSheetName=f"{ds}-data",
        rowSourceColumn=1,
        columnSourceColumn=0,
        valueSourceColumn=2,
    )


if __name__ == "__main__":
    main()
