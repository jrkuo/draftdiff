import argparse
import asyncio
from collections import Counter
from pathlib import Path

import diskcache as dc
import pandas as pd
from tqdm import tqdm

from draftdiff.models.opendota import MatchResponse
from draftdiff.opendota import opendota_match
from draftdiff.stratz import get_league_match_ids

cache_dir = Path.home() / '.cache' / 'draftdiff'
cache_dir.mkdir(parents=True, exist_ok=True)
cache = dc.Cache(str(cache_dir / 'draftdiff_cache'))


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
    parser.add_argument('--stratz-league-id', type=int, required=True, help='Stratz league ID')

    args = parser.parse_args()

    print(f'Using League ID: {args.stratz_league_id}')
    match_ids = await league_match_ids(args.stratz_league_id)

    print(f'Found {len(match_ids)} match IDs')

    match_responses: list[MatchResponse] = []
    for match_id in tqdm(match_ids):
        match_resp = await opendota_match(match_id)
        match_responses += [match_resp]

    pairwise_wins: list[tuple[str, str]] = []
    pairwise_games: list[tuple[str, str]] = []

    for match in match_responses:
        try:
            radiant_player_names = ','.join([p.name for p in match.players if p.isRadiant])  # type: ignore
            dire_player_names = ','.join([p.name for p in match.players if not p.isRadiant])  # type: ignore
        except Exception:
            radiant_player_names = ','.join([p.personaname for p in match.players if p.isRadiant])  # type: ignore
            dire_player_names = ','.join([p.personaname for p in match.players if not p.isRadiant])  # type: ignore

        for player in match.players:
            player_name: str = player.name if player.name else player.personaname
            player_won: bool = (match.radiant_win and player.isRadiant) or (
                not match.radiant_win and not player.isRadiant
            )
            teammate_names: str = radiant_player_names if player.isRadiant else dire_player_names
            teammate_name_list: list[str] = teammate_names.split(',')
            for teammate_name in teammate_name_list:
                if teammate_name == player_name:
                    continue
                pairwise_games += [(player_name, teammate_name)]
                if player_won:
                    pairwise_wins += [(player_name, teammate_name)]

    pairwise_wins_ctr: Counter[tuple[str, str]] = Counter(pairwise_wins)
    pairwise_games_ctr: Counter[tuple[str, str]] = Counter(pairwise_games)
    players: list[str] = sorted(list(set([x[0] for x in pairwise_games_ctr] + [x[1] for x in pairwise_games_ctr])))

    win_rate_matrix = pd.DataFrame(index=players, columns=players, dtype=float)
    wins_matrix = pd.DataFrame(index=players, columns=players, dtype=int)
    games_matrix = pd.DataFrame(index=players, columns=players, dtype=int)

    for player1 in players:
        for player2 in players:
            if player1 == player2:
                win_rate_matrix.loc[player1, player2] = None
                wins_matrix.loc[player1, player2] = None
                games_matrix.loc[player1, player2] = None
                continue

            wins = pairwise_wins_ctr.get((player1, player2), 0)
            games = pairwise_games_ctr.get((player1, player2), 0)

            wins_matrix.loc[player1, player2] = wins
            games_matrix.loc[player1, player2] = games

            if games > 0:
                win_rate_matrix.loc[player1, player2] = wins / games
            else:
                win_rate_matrix.loc[player1, player2] = None

    combined_columns = []
    for player in players:
        combined_columns.extend([f'{player}_games', f'{player}_winrate'])
    combined_columns.append('total_games')

    combined_matrix = pd.DataFrame(index=players, columns=combined_columns, dtype=float)

    for player1 in players:
        total_games_for_player = 0
        for player2 in players:
            if player1 == player2:
                combined_matrix.loc[player1, f'{player2}_games'] = None
                combined_matrix.loc[player1, f'{player2}_winrate'] = None
                continue

            wins = pairwise_wins_ctr.get((player1, player2), 0)
            games = pairwise_games_ctr.get((player1, player2), 0)
            total_games_for_player += games

            combined_matrix.loc[player1, f'{player2}_games'] = games
            if games > 0:
                combined_matrix.loc[player1, f'{player2}_winrate'] = wins / games
            else:
                combined_matrix.loc[player1, f'{player2}_winrate'] = None

        combined_matrix.loc[player1, 'total_games'] = total_games_for_player

    print('\nWin Rate Matrix:')
    print(win_rate_matrix.round(3))

    print('\nWins Matrix:')
    print(wins_matrix)

    print('\nGames Matrix:')
    print(games_matrix)

    print('\nCombined Matrix (Games and Win Rates):')
    print(combined_matrix)

    output_dir = Path.cwd() / 'data' / 'pairwise_win_rates'
    output_dir.mkdir(parents=True, exist_ok=True)

    win_rate_matrix.to_csv(output_dir / 'win_rate_matrix.csv')
    wins_matrix.to_csv(output_dir / 'wins_matrix.csv')
    games_matrix.to_csv(output_dir / 'games_matrix.csv')
    combined_matrix.to_csv(output_dir / 'combined_matrix.csv')

    combined_df = pd.DataFrame()
    for player1 in players:
        for player2 in players:
            if player1 != player2:
                combined_df = pd.concat(
                    [
                        combined_df,
                        pd.DataFrame(
                            {
                                'Player1': [player1],
                                'Player2': [player2],
                                'Wins': [wins_matrix.loc[player1, player2]],
                                'Games': [games_matrix.loc[player1, player2]],
                                'WinRate': [win_rate_matrix.loc[player1, player2]],
                            }
                        ),
                    ],
                    ignore_index=True,
                )

    combined_df.to_csv(output_dir / 'pairwise_win_rates_combined.csv', index=False)

    print(f'\nMatrices saved to {output_dir}/')
    print('Files created:')
    print('- win_rate_matrix.csv')
    print('- wins_matrix.csv')
    print('- games_matrix.csv')
    print('- combined_matrix.csv')
    print('- pairwise_win_rates_combined.csv')

    return


if __name__ == '__main__':
    asyncio.run(main())
