import json
import os

import pandas as pd
import requests
from loguru import logger

# def get_stratz_slug_for_dota_hero(hero_name) -> str:
#     slug = f"{hero_id_dict[hero_name]}-{hero_name.lower().replace(" ", "-")}"
#     return slug

# def get_stratz_counters_page_for_hero_name(hero_name) -> str:
#     headers = {"User-Agent": "Mozilla/5.0"}
#     logger.warning(f"downloading stratz {hero_name} counters page")
#     stratz_hero_slug = get_stratz_slug_for_dota_hero(hero_name)
#     response = requests.get(
#         f"https://stratz.com/heroes/{stratz_hero_slug}/matchups",
#         headers=headers,
#     )
#     return response.text

# def get_cached_stratz_counters_page(hero_name) -> str:
#     stratz_hero_slug = get_stratz_slug_for_dota_hero(hero_name)
#     try:
#         with open(f"./data/stratz-{stratz_hero_slug}-counters.html", "rb") as rf:
#             logger.info(f"using local data for {stratz_hero_slug}")
#             html_text = rf.read().decode("utf-8")
#             return html_text
#     except FileNotFoundError:
#         with open(f"./data/stratz-{stratz_hero_slug}-counters.html", "wb") as wf:
#             match_page_text = get_stratz_counters_page_for_hero_name(hero_name)
#             wf.write(match_page_text.encode())
#             return match_page_text


def create_stratz_hero_name_id_dict() -> dict:
    with open(f"./data/stratzheroes.html", "r") as rf:
        html = rf.read()
        hero_id_dict = {}
        hero_id_name = html.split("/heroes/")[15:263]
        for i in range(0, len(hero_id_name), 2):
            hero_id_name[i] = hero_id_name[i].split('"')[0]
        for i in range(1, len(hero_id_name), 2):
            hero_id_name[i] = hero_id_name[i].split('"')[2]
        for i in range(0, len(hero_id_name), 2):
            hero_id_dict[hero_id_name[i + 1]] = hero_id_name[i]

    return hero_id_dict


hero_id_dict = create_stratz_hero_name_id_dict()
id_hero_dict = {v: k for k, v in hero_id_dict.items()}


def get_matchup_stats_for_hero_name(token, hero_name) -> dict:
    heroid = hero_id_dict[hero_name]
    url = "https://api.stratz.com/graphql"

    # Define headers if needed
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    # Define your GraphQL query
    graphql_request = {
        "operationName": "GetHeroMatchUps",
        "query": """query GetHeroMatchUps($heroId: Short!, $matchLimit: Int!, $bracketBasicIds: [RankBracketBasicEnum]) {
  heroStats {
    heroVsHeroMatchup(
      heroId: $heroId
      matchLimit: $matchLimit
      bracketBasicIds: $bracketBasicIds
    ) {
      advantage {
        heroId
        matchCountWith
        matchCountVs
        with {
          heroId2
          matchCount
          winCount
          synergy
          __typename
        }
        vs {
          heroId2
          matchCount
          winCount
          synergy
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
}""",
        "variables": {
            "heroId": int(heroid),
            "matchLimit": 0,
        },
    }
    # Send the POST request with the GraphQL query
    logger.info("Sending POST request with GraphQL query to stratz API")
    response = requests.post(url, json=graphql_request, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the response JSON
        print(response.status_code)
    else:
        print("Error:", response.status_code)

    data = response.json()
    return data


def get_hero_counters_for_hero_name(token, hero_name) -> pd.DataFrame:
    data = get_matchup_stats_for_hero_name(token, hero_name)
    # nested dictionaries, list of length 1, element is a dictionary
    # type(data['data']['heroStats']['heroVsHeroMatchup']['advantage']) == list
    # len(data['data']['heroStats']['heroVsHeroMatchup']['advantage']) == 1
    # type(data["data"]["heroStats"]["heroVsHeroMatchup"]["advantage"][0]["vs"]) == list
    # first row in table for counters = data["data"]["heroStats"]["heroVsHeroMatchup"]["advantage"][0]["vs"][0]
    new_records = []
    for row in data["data"]["heroStats"]["heroVsHeroMatchup"]["advantage"][0]["vs"]:
        new_records += [
            {
                "counter_hero": id_hero_dict[str(row["heroId2"])],
                "target_disadvantage": float(-row["synergy"]),
                "target_winrate_vs_counter": float(
                    (row["winCount"] / row["matchCount"]) * 100
                ),
                "counter_vs_target_matches_played": int(row["matchCount"]),
                "source": "stratz",
            }
        ]
    df_output = pd.json_normalize(new_records)
    return df_output


def main():
    # learn about curried functions later
    token = os.environ["STRATZ_API_TOKEN"]
    get_hero_counters_for_hero_name(token, "Arc Warden")


if __name__ == "__main__":
    main()
