"""Generate comprehensive Rosh Analysis data mimicking https://stratz.com/rosh/analysis.

This script fetches:
- Win rates for all heroes across all 8 brackets and 5 positions (last 7 days)
- Synergies and counter metrics for all hero pairs across 4 grouped brackets (last 7 days)

Output is a single compressed JSON file with all the data.
"""

import argparse
import gzip
import json
import os
from datetime import datetime, timezone

from loguru import logger

from draftdiff import io, stratz, util


def generate_rosh_analysis_data(
    output_dir: str = 'data',
    io_location: str = 'local',
    filename: str | None = None,
) -> str:
    """Generate Rosh Analysis data and write to compressed JSON file.

    Args:
        output_dir: Directory to write output file (for local) or prefix (for S3)
        io_location: Storage backend ('local' or 's3')
        filename: Custom filename (default: rosh_analysis_data_{date}.json.gz)

    Returns:
        Path to the generated file
    """
    # Set IO location
    os.environ['IO_LOCATION'] = io_location

    # Get Stratz API token
    token = os.environ.get('STRATZ_API_TOKEN')
    if not token:
        raise ValueError('STRATZ_API_TOKEN environment variable is required')

    logger.info('Starting Rosh Analysis data generation')
    logger.info(f'Output location: {io_location}')
    logger.info(f'Output directory: {output_dir}')

    # Fetch win rates for all brackets
    logger.info('Fetching win rates for all 8 brackets...')
    winrates = stratz.format_hero_winrates_all_brackets(token=token)
    logger.info(f'Fetched win rates for {len(winrates)} brackets')

    # Fetch synergies/counters for all brackets
    logger.info('Fetching synergies/counters for all 4 grouped brackets...')
    synergies_counters = stratz.format_synergies_counters_all_brackets(token=token)
    logger.info(f'Fetched synergies/counters for {len(synergies_counters)} bracket groups')

    # Build final data structure
    current_time = datetime.now(timezone.utc).isoformat()
    data = {
        'metadata': {
            'generated_at': current_time,
            'time_range_days': 7,
            'brackets': list(winrates.keys()),
            'bracket_groups': list(synergies_counters.keys()),
        },
        'win_rates': winrates,
        'synergies_counters': synergies_counters,
    }

    # Calculate statistics
    total_heroes = len(set(
        hero['name']
        for bracket_data in winrates.values()
        for position_data in bracket_data.values()
        for hero in position_data
    ))

    total_pairs = sum(
        len(hero_matchups)
        for bracket_data in synergies_counters.values()
        for hero_matchups in bracket_data.values()
    )

    logger.info(f'Total unique heroes: {total_heroes}')
    logger.info(f'Total hero pairs: {total_pairs}')

    # Generate filename (use custom or default with current date)
    if filename is None:
        current_date = util.get_current_ds()
        filename = f'rosh_analysis_data_{current_date}.json.gz'

    # Write to file based on io_location
    if io_location == 'local':
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)

        logger.info(f'Writing compressed JSON to {filepath}...')
        with gzip.open(filepath, 'wt', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

        # Get file size
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        logger.info(f'File written successfully: {filepath}')
        logger.info(f'File size: {file_size_mb:.2f} MB')

    elif io_location == 's3':
        # Upload gzipped file to S3
        partition_path = f'{output_dir}/{filename}'
        logger.info(f'Writing to S3: {partition_path}...')

        # Create gzipped content in memory
        from io import BytesIO
        import boto3

        buffer = BytesIO()
        with gzip.open(buffer, 'wt', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

        # Get the compressed bytes
        buffer.seek(0)
        compressed_data = buffer.read()

        # Upload to S3
        boto3.client('s3').put_object(
            Bucket='draftdiff',
            Key=partition_path,
            Body=compressed_data,
            ContentType='application/json',
            ContentEncoding='gzip'
        )

        # Get file size
        file_size_mb = len(compressed_data) / (1024 * 1024)
        filepath = f's3://draftdiff/{partition_path}'
        logger.info(f'File uploaded to S3: {filepath}')
        logger.info(f'File size: {file_size_mb:.2f} MB')

    else:
        raise ValueError(f'Unsupported io_location: {io_location}')

    # Print summary
    logger.info('=' * 60)
    logger.info('Generation complete!')
    logger.info(f'Output file: {filepath}')
    logger.info(f'Brackets: {len(winrates)} (win rates), {len(synergies_counters)} (synergies/counters)')
    logger.info(f'Heroes: {total_heroes}')
    logger.info(f'Hero pairs: {total_pairs}')
    logger.info('=' * 60)

    return filepath


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description='Generate Rosh Analysis data from Stratz API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate locally
  python -m draftdiff.generate_rosh_analysis_data

  # Generate locally with custom output directory
  python -m draftdiff.generate_rosh_analysis_data --output-dir ./output

  # Generate and upload to S3
  python -m draftdiff.generate_rosh_analysis_data --io-location s3

  # Generate to S3 with custom prefix
  python -m draftdiff.generate_rosh_analysis_data --io-location s3 --output-dir rosh_data
        """,
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='data',
        help='Output directory for local storage or S3 prefix (default: data)',
    )

    parser.add_argument(
        '--io-location',
        '-i',
        type=str,
        choices=['local', 's3'],
        default='local',
        help='Storage backend: local or s3 (default: local)',
    )

    parser.add_argument(
        '--filename',
        '-f',
        type=str,
        default=None,
        help='Custom filename (default: rosh_analysis_data_{date}.json.gz)',
    )

    args = parser.parse_args()

    try:
        generate_rosh_analysis_data(
            output_dir=args.output_dir,
            io_location=args.io_location,
            filename=args.filename,
        )
    except Exception as e:
        logger.error(f'Error generating data: {e}')
        raise


if __name__ == '__main__':
    main()
