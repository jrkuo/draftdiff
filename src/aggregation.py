import os

import pandas as pd

import dotabuff
import stratz


def build_target_heroes_counter_heroes_df_for_dotabuff_id_in_last_n_days(
    token, player_id, n
) -> pd.DataFrame:
    df_hero_stats_for_player_id = (
        dotabuff.get_heroes_stats_for_dotabuff_id_in_last_n_days(player_id, n)
    )
    combined_dfs = []
    for index, row in df_hero_stats_for_player_id.iterrows():
        df_dotabuff_counter = dotabuff.get_dotabuff_hero_counters_for_hero(row["hero"])
        df_stratz_counter = stratz.get_hero_counters_for_hero_name(token, row["hero"])
        df_dotabuff_counter_num_rows = df_dotabuff_counter.shape[0]
        df_stratz_counter_num_rows = df_stratz_counter.shape[0]
        dotabuff_repeated_rows = pd.DataFrame(
            [row] * df_dotabuff_counter_num_rows, columns=row.index
        )
        dotabuff_combined_df = pd.concat(
            [dotabuff_repeated_rows.reset_index(drop=True), df_dotabuff_counter], axis=1
        )
        stratz_repeated_rows = pd.DataFrame(
            [row] * df_stratz_counter_num_rows, columns=row.index
        )
        stratz_combined_df = pd.concat(
            [stratz_repeated_rows.reset_index(drop=True), df_stratz_counter], axis=1
        )
        combined_dfs.append(dotabuff_combined_df)
        combined_dfs.append(stratz_combined_df)
        result_df = pd.concat(combined_dfs, ignore_index=True)
    return result_df


def calculate_weighted_avg_metrics_in_counter_heroes_df(
    counter_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    >>> import pandas as pd
    >>> hero = ["bloodseeker"]*4
    >>> num_matches = [1]*4
    >>> num_wins = [1]*4
    >>> win_rate = [1]*4
    >>> counter_hero = ["medusa"]*2 + ["abaddon"]*2
    >>> target_disadvantage = [5.0105, 5.964, 1.1627, 1.658]
    >>> target_winrate_vs_counter = [44.6005, 44.4417651314203, 45.4581, 45.3179867342957]
    >>> counter_vs_target_matches_played = [22771, 4147, 27434, 5126]
    >>> source = ["dotabuff", "stratz"]*2
    >>> rows = zip(hero, num_matches, num_wins, win_rate, counter_hero, target_disadvantage, target_winrate_vs_counter, counter_vs_target_matches_played, source)
    >>> records = [{"hero": x[0], "num_matches": x[1], "num_wins": x[2], "win_rate": x[3], "counter_hero": x[4], "target_disadvantage": x[5], "target_winrate_vs_counter": x[6], "counter_vs_target_matches_played": x[7], "source": x[8]} for x in rows]
    >>> df = pd.json_normalize(records)
    >>> calculate_weighted_avg_metrics_in_counter_heroes_df(df)
              hero counter_hero  player_num_matches  player_win_rate  weighted_disadvantage  weighted_win_percent  num_sources
    0  bloodseeker      abaddon                   1              1.0               1.240676             45.436042            2
    1  bloodseeker       medusa                   1              1.0               5.157397             44.576045            2
    """
    output_rows = []
    grouped_df = counter_df.groupby(["hero", "counter_hero"])

    for (hero, counter_hero), df_group in grouped_df:
        num_matches = int(df_group["num_matches"].sum() / 2)
        num_wins = df_group["num_wins"].sum() / 2
        win_rate = num_wins / num_matches
        num_sources = df_group["source"].nunique()
        weighted_disadvantage = (
            df_group["target_disadvantage"]
            * df_group["counter_vs_target_matches_played"]
        ).sum() / df_group["counter_vs_target_matches_played"].sum()
        weighted_win_percent = (
            df_group["target_winrate_vs_counter"]
            * df_group["counter_vs_target_matches_played"]
        ).sum() / df_group["counter_vs_target_matches_played"].sum()
        output_rows += [
            {
                "hero": hero,
                "counter_hero": counter_hero,
                "player_num_matches": num_matches,
                "player_win_rate": win_rate,
                "weighted_disadvantage": weighted_disadvantage,
                "weighted_win_percent": weighted_win_percent,
                "num_sources": num_sources,
            }
        ]
    output_df = pd.json_normalize(output_rows)
    return output_df


def build_counter_df():
    token = os.environ["STRATZ_API_TOKEN"]

    build_target_heroes_counter_heroes_df_for_dotabuff_id_in_last_n_days(
        token, "181567803", 30
    )


def main():
    import doctest

    doctest.testmod()


if __name__ == "__main__":
    main()
