# DraftDiff - Dota 2 Counterpicker

## Project Overview

DraftDiff is a Dota 2 counterpicker tool that analyzes hero matchups and provides counter-pick suggestions to help players make better draft decisions. The project aggregates data from multiple sources (OpenDota, Stratz, Dotabuff) to provide both player-specific and general hero counter recommendations.

## Project Structure

```
draftdiff/
├── draftdiff/              # Main package directory
│   ├── aggregation.py      # Player-specific counter data aggregation
│   ├── aggregation2.py     # General hero counter data aggregation
│   ├── opendota.py         # OpenDota API client for match data
│   ├── stratz.py           # Stratz GraphQL API client for matchup stats
│   ├── dotabuff.py         # Dotabuff web scraper for player stats
│   ├── io.py               # Storage abstraction (local/S3/Google Sheets)
│   ├── reacthelper.py      # Frontend data formatting (JSON/JavaScript)
│   ├── writetosheets.py    # Google Sheets integration
│   ├── s3.py               # AWS S3 utilities
│   ├── constants.py        # Hero IDs, rank brackets, mappings
│   ├── util.py             # Utility functions
│   └── models/
│       └── opendota.py     # OpenDota API data models
├── main.py                 # Player-specific analysis pipeline
├── main2.py                # General hero counter pipeline
├── hero_builds.py          # Tournament hero builds analysis
├── pairwise_win_rates.py   # Team synergy calculations
├── hero_win_rates.py       # Hero win rate extraction
└── pyproject.toml          # Poetry dependencies and config
```

## Key Workflows

### 1. Player-Specific Counter Analysis
**Entry Point**: `main.py`

Analyzes a specific player's hero pool and identifies their strongest counters:

```bash
python main.py -p 69576061 181567803 -i s3
```

**Pipeline**:
1. Scrapes player's recent matches from Dotabuff
2. Calculates player's win rates by hero/role/lane
3. Fetches counter data for each hero the player uses (from both Stratz and Dotabuff)
4. Aggregates using weighted averages based on match counts
5. Outputs to Google Sheets for visualization

**Output DataFrames**:
- `dotabuff_stats` - Player's hero performance stats
- `played_heroes_counters` - Raw counter data for player's heroes
- `played_heroes_weighted_avg_counters` - Aggregated recommendations

### 2. General Hero Counters
**Entry Point**: `main2.py`

Builds comprehensive counter database for all ~150 heroes:

```bash
python main2.py -i local
```

**Pipeline**:
1. Fetches counter data from Stratz (GraphQL API)
2. Scrapes counter data from Dotabuff (web scraping)
3. Aggregates both sources with weighted metrics
4. Generates JavaScript/JSON output for web UI

### 3. Tournament Analysis
**Entry Points**: `hero_builds.py`, `pairwise_win_rates.py`

Analyzes professional tournament data:

```bash
# Get hero builds from specific tournament
python hero_builds.py --liquipedia-query URL
python hero_builds.py --stratz-league-id 123

# Calculate team synergies
python pairwise_win_rates.py --stratz-league-id 123
```

### 4. Rosh Analysis Data Generation
**Entry Point**: `generate_rosh_analysis_data.py`

Generates comprehensive hero statistics mimicking https://stratz.com/rosh/analysis:

```bash
# Generate locally (default)
python -m draftdiff.generate_rosh_analysis_data

# Generate with custom output directory
python -m draftdiff.generate_rosh_analysis_data --output-dir ./output

# Generate and upload to S3
python -m draftdiff.generate_rosh_analysis_data --io-location s3
```

**Pipeline**:
1. Fetches win rates for all heroes across all 8 brackets and 5 positions (last 7 days)
2. Fetches synergies and counter metrics for all hero pairs across 4 grouped brackets (last 7 days)
3. Combines data into single structure
4. Outputs compressed JSON file: `rosh_analysis_data_{YYYY-MM-DD}.json.gz`

**Output Data Structure**:
```json
{
  "metadata": {
    "generated_at": "2025-11-17T...",
    "time_range_days": 7,
    "brackets": ["Herald", "Guardian", ...],
    "bracket_groups": ["Herald_Guardian", ...]
  },
  "win_rates": {
    "Herald": {
      "pos1": [{"name": "Anti-Mage", "winrate": "0.52", "matchCount": 1500}, ...],
      "pos2": [...],
      ...
    },
    ...
  },
  "synergies_counters": {
    "Herald_Guardian": {
      "Anti-Mage": {
        "Medusa": {"synergy": 2.5, "counter": -3.2, "matchCountWith": 150, "matchCountVs": 200},
        ...
      },
      ...
    },
    ...
  }
}
```

## Data Sources

### OpenDota API
- **Base URL**: https://api.opendota.com
- **Purpose**: Detailed match data, hero builds, item progressions
- **Caching**: 30-day disk cache
- **Key Functions**: `opendota_match()`, `parse_match_heroes()`, `do_hero_build_df()`

### Stratz GraphQL API
- **Base URL**: https://api.stratz.com/graphql
- **Purpose**: Hero matchup statistics, win rates by rank bracket, synergies
- **Authentication**: Requires `STRATZ_API_TOKEN` environment variable
- **Key Functions**: `get_matchup_stats_for_hero_name()`, `get_all_positions_heros_winrate_for_bracket()`
- **Rank Brackets**: Herald, Guardian, Crusader, Archon, Legend, Ancient, Divine, Immortal

### Dotabuff Web Scraping
- **Base URL**: https://dotabuff.com
- **Purpose**: Player-specific stats, counter matchup tables
- **Key Functions**: `get_cached_dotabuff_match_pages_for_past_n_days()`, `get_cached_counters_page()`
- **Configurable**: Time windows (typically 30 days)

## Aggregation Logic

The core aggregation strategy combines multiple data sources using weighted averages:

```python
# Example: Two sources for same hero matchup
# Dotabuff: 5% disadvantage, 44.6% winrate, 22,771 matches
# Stratz: 5.96% disadvantage, 44.4% winrate, 4,147 matches
# Result: Weighted average considering match volume
```

**Key Metrics**:
- `weighted_disadvantage` - Average disadvantage percentage vs counter
- `weighted_win_percent` - Average win rate vs counter
- `num_sources` - Number of data sources that agree (1-2)
- `total_head_to_head_matches` - Combined match count across sources

## Storage Backends

The `io.py` module supports three storage backends (configured via `IO_LOCATION` env var):

### 1. Local Filesystem
- **Path**: `./data/`
- **Format**: CSV for DataFrames, JSON for raw data, HTML for cached pages

### 2. AWS S3
- **Bucket**: `draftdiff`
- **Format**: Parquet for DataFrames (gzip compressed), JSON/HTML for raw data
- **Requires**: AWS credentials configured

### 3. Google Sheets
- **Direct read/write** of DataFrames to sheets
- **Requires**: `credentials.json` service account file

**Data Partitioning Scheme**:
```
dotabuff/ds={date}/player_id={id}/days={n}/page-{num}
stratz/matchups/ds={date}/hero={hero_slug}
output/player_counters_weighted-df/ds={date}/player_id={id}/days={n}
```

## Configuration

### Environment Variables
- `STRATZ_API_TOKEN` - API key for Stratz GraphQL access
- `IO_LOCATION` - Storage backend: `local`, `s3`, or `sheets`

### Google Cloud Setup
- Place service account credentials at `credentials.json`
- Enable Google Sheets API
- Share target spreadsheet with service account email

### AWS Setup
- Configure AWS credentials (boto3)
- Ensure access to `draftdiff` S3 bucket

## Development Setup

### Prerequisites
- Python 3.11.9
- Poetry for dependency management

### Installation
```bash
# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

### Code Quality
```bash
# Run linter
ruff check draftdiff/

# Run formatter
ruff format draftdiff/

# Run type checker
pyright draftdiff/

# Run tests
pytest
```

### Linting & Formatting Rules
- Line length: 120 characters
- Quote style: Single quotes
- Import sorting: Enabled (isort)
- Docstring convention: Google style
- Max complexity: 15 (McCabe)

## Data Model Examples

### Player Counter DataFrame
```
hero      | counter_hero | player_num_matches | player_win_rate | weighted_disadvantage | weighted_win_percent | num_sources
----------|--------------|-------------------|-----------------|----------------------|----------------------|------------
Anti-Mage | Medusa       | 5                 | 0.6             | 5.16                 | 44.58                | 2
Anti-Mage | Lion         | 3                 | 0.33            | 8.2                  | 41.5                 | 2
```

### General Counter DataFrame
```
hero      | counter_hero | weighted_disadvantage | weighted_win_percent | total_head_to_head_matches | num_sources
----------|--------------|----------------------|----------------------|----------------------------|------------
Anti-Mage | Medusa       | 5.16                 | 44.58                | 26918                      | 2
Anti-Mage | Invoker      | 4.2                  | 45.1                 | 18500                      | 2
```

## Caching Strategy

### Disk Cache (diskcache)
- **Location**: `~/.cache/draftdiff/`
- **OpenDota match data**: 30-day expiry
- **Stratz league match IDs**: 7-day expiry
- **Liquipedia tournament URLs**: 30-day expiry

### Storage Backend Caching
- IO abstraction checks for existing files before making expensive API/web calls

## Common Tasks

### Analyze a New Player
```bash
# Get player's Dotabuff ID from their profile URL
# Example: dotabuff.com/players/69576061
python main.py -p 69576061 -i s3
```

### Update General Counter Database
```bash
python main2.py -i s3
```

### Generate Rosh Analysis Data
```bash
# Generate comprehensive hero statistics for all brackets and positions
# Mimics data from https://stratz.com/rosh/analysis

# Local output (creates data/rosh_analysis_data_YYYY-MM-DD.json.gz)
python -m draftdiff.generate_rosh_analysis_data

# Upload to S3
python -m draftdiff.generate_rosh_analysis_data --io-location s3

# Custom output directory
python -m draftdiff.generate_rosh_analysis_data --output-dir ./my_output
```

**What it generates**:
- Win rates for all ~150 heroes across 8 brackets (Herald to Immortal) and 5 positions
- Synergy and counter metrics for all hero pairs across 4 grouped brackets
- Single compressed JSON file with all data
- Based on last 7 days of matches

### Analyze Tournament Meta
```bash
# Find tournament on Liquipedia, get the URL
python hero_builds.py --liquipedia-query "https://liquipedia.net/dota2/..."

# Or use Stratz league ID
python hero_builds.py --stratz-league-id 15728
```

### Generate Frontend Data
```bash
# Outputs JavaScript-formatted JSON for web UI
python main2.py -i local
# Check output in reacthelper.py functions
```

## Architecture Notes

### Weighted Aggregation
Multiple data sources often disagree on exact statistics. The weighted aggregation approach:
1. Groups data by (hero, counter_hero) pairs
2. Uses match counts as weights
3. Calculates weighted averages for disadvantage and win rate
4. Tracks number of sources for confidence scoring

### Position-Specific Analysis
Stratz data includes role/position breakdowns:
- Position 1 (Safe Lane Carry)
- Position 2 (Mid)
- Position 3 (Offlane)
- Position 4 (Soft Support)
- Position 5 (Hard Support)

Counter recommendations can be filtered by position.

### Match Time Windows
Player statistics use configurable time windows (default 30 days) to capture recent performance while maintaining statistical significance.

## Dependencies

**Core Libraries**:
- `pandas` - Data manipulation and aggregation
- `requests` - HTTP API calls
- `beautifulsoup4` - Web scraping
- `boto3` - AWS S3 integration
- `gspread`, `google-auth`, `oauth2client` - Google Sheets integration
- `diskcache` - Local caching
- `loguru` - Structured logging
- `crawl4ai` - Advanced web crawling
- `pyarrow` - Parquet format support
- `tqdm` - Progress bars

**Development Tools**:
- `ruff` - Fast Python linter and formatter
- `pytest` - Testing framework
- `pyright` - Static type checker
- `datamodel-code-generator` - Generate Pydantic models from OpenAPI specs

## Future Improvements

- [ ] Add support for more rank-specific analysis
- [ ] Implement real-time draft recommendation API
- [ ] Add hero synergy (teammate) recommendations
- [ ] Create web dashboard for visualization
- [ ] Add confidence intervals for statistical metrics
- [ ] Implement incremental updates instead of full refreshes
- [ ] Add support for patch-specific analysis (meta changes)
- [ ] Create CLI tool for quick counter lookups

## Resources

- [OpenDota API Documentation](https://docs.opendota.com/)
- [Stratz GraphQL Explorer](https://stratz.com/graphql/explorer)
- [Dotabuff](https://www.dotabuff.com/)
- [Liquipedia Dota 2](https://liquipedia.net/dota2/)
