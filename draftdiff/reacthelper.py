import os

import pandas as pd
from draftdiff import io, util
from loguru import logger


def convert_df_to_javascript_formatted_data_text(df) -> str:
    hero_dict = {}

    for _, row in df.iterrows():
        hero = row["hero"]
        counter_hero = row["counter_hero"]
        weighted_disadvantage = round(row["weighted_disadvantage"], 3)

        if hero not in hero_dict:
            hero_dict[hero] = []

        hero_dict[hero].append({"name": counter_hero, hero: weighted_disadvantage})

    output_lines = []
    for hero, counters in hero_dict.items():
        output_lines.append(f'"{hero}": [')
        for counter in counters:
            # Convert the dictionary to a string and adjust the format
            counter_str = str(counter)
            # Remove single quotes around dictionary keys
            counter_str = counter_str.replace("'name':", "name:")
            # Add a space after the opening bracket and before the closing bracket
            counter_str = counter_str.replace("{", "{ ")
            counter_str = counter_str.replace("}", " }")
            output_lines.append(f"    {counter_str},")
        output_lines.append("],")

    output_text = "\n".join(output_lines)
    return output_text


def get_javascript_formatted_counters_data(ds):
    partition_path = f"output/counters-text/ds={ds}"
    try:
        counters_text = io.read_text(partition_path)
        logger.info(
            f"[{io.get_io_location()}] loaded cached text from {partition_path}"
        )
    except Exception:
        logger.info(
            f"[{io.get_io_location()}] Running making text file from df for {ds}"
        )
        counter_df = io.read_df(f"output/counter-df/ds={ds}")
        counters_text = convert_df_to_javascript_formatted_data_text(counter_df)
        io.write_text_to_text(counters_text, partition_path)
    return counters_text


def test():
    ds = util.get_current_ds()
    os.environ["IO_LOCATION"] = "s3"
    get_javascript_formatted_counters_data(ds)


if __name__ == "__main__":
    test()
