"""Need to save the HTML from pages like this - https://dota2protracker.com/meta/trends?position=pos+1"""

import asyncio
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup, Tag


def extract_hero_data(soup: BeautifulSoup) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []

    table = soup.find('table', class_='trend-table')
    if not table or not isinstance(table, Tag):
        return results

    rows = table.find_all('tr')

    for row in rows:
        if not isinstance(row, Tag):
            continue

        hero_cell = row.find('td', class_='hero-cell')
        if not hero_cell or not isinstance(hero_cell, Tag):
            continue

        hero_name_elem = hero_cell.find('span', class_='hero-name')
        if not hero_name_elem or not isinstance(hero_name_elem, Tag):
            continue

        hero_name = hero_name_elem.get_text(strip=True)

        facet_div = hero_cell.find('div', class_='text-xs')
        facet_name = ''
        if facet_div and isinstance(facet_div, Tag):
            facet_text_div = facet_div.find('div', class_='px-1 pr-2 max-w-[100px] truncate')
            if facet_text_div and isinstance(facet_text_div, Tag):
                facet_name = facet_text_div.get_text(strip=True)

        sparkline_cells = row.find_all('td', class_='sparkline-cell')

        pick_rate = ''
        win_rate = ''

        for cell in sparkline_cells:
            if not isinstance(cell, Tag):
                continue

            cell_classes = cell.get('class', [])
            if isinstance(cell_classes, list) and 'pick-rate-group' in cell_classes:
                svg_texts = cell.find_all('text', {'font-weight': '600'})
                if len(svg_texts) >= 2 and isinstance(svg_texts[-1], Tag):
                    pick_rate = svg_texts[-1].get_text(strip=True)

            elif isinstance(cell_classes, list) and 'win-rate-group' in cell_classes:
                svg_texts = cell.find_all('text', {'font-weight': '600'})
                if len(svg_texts) >= 2 and isinstance(svg_texts[-1], Tag):
                    win_rate = svg_texts[-1].get_text(strip=True)

        if hero_name and pick_rate and win_rate:
            results.append({'hero': hero_name, 'facet': facet_name, 'pick_rate': pick_rate, 'win_rate': win_rate})

    return results


async def main() -> list[dict[str, str]]:
    d2pt_dir = Path('data/d2pt/2025-08-19')
    html_files = sorted(list(d2pt_dir.glob('*.html')))

    all_results: list[dict[str, str]] = []

    for html_file in html_files:
        print(f'Processing {html_file.name}...')
        with open(html_file) as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
            results = extract_hero_data(soup)
            for r in results:
                r['position'] = str(html_file)[-6]
                r['date'] = '2025-08-19'
            all_results.extend(results)
            print(f'Found {len(results)} hero entries')

    output_dir = Path.cwd() / 'data' / 'hero_win_rates'
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(all_results)
    df.to_csv(output_dir / '2025-08-19.csv', index=False)

    print(f'\nTotal entries found: {len(all_results)}')

    for result in all_results[:5]:
        print(f'Hero: {result["hero"]}')
        print(f'Facet: {result["facet"]}')
        print(f'Pick Rate: {result["pick_rate"]}')
        print(f'Win Rate: {result["win_rate"]}')
        print('-' * 40)

    return all_results


if __name__ == '__main__':
    asyncio.run(main())
