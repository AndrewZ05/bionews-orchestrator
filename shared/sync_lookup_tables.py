#!/usr/bin/env python3
"""
Sync Lookup Tables

Synchronizes standardized values from lime_surveys_columnar_completed
to the lookup tables (standardized_questions_lookup, standardized_answers_lookup).

This ensures the lookup tables remain the single source of truth while capturing
any values that were added directly to columnar_completed via SQL or legacy processes.

Usage:
    python shared/sync_lookup_tables.py --all           # Sync everything
    python shared/sync_lookup_tables.py --questions     # Sync questions only
    python shared/sync_lookup_tables.py --answers       # Sync answers only
    python shared/sync_lookup_tables.py --categories    # Sync categories to columnar
    python shared/sync_lookup_tables.py --all --dry-run # Preview changes
"""

import argparse
import logging
import pandas as pd
from pathlib import Path
from google.cloud import bigquery

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(env_path)

# Configuration
PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'

# Table names
QUESTIONS_LOOKUP = f'{PROJECT_ID}.{DATASET}.standardized_questions_lookup'
ANSWERS_LOOKUP = f'{PROJECT_ID}.{DATASET}.standardized_answers_lookup'
COLUMNAR_COMPLETED = f'{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_bq_client():
    """Create BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)


def preview_questions_to_sync(client) -> list:
    """
    Find standardized_question values in columnar_completed
    that don't exist in standardized_questions_lookup.

    Returns list of dicts with question info.
    """
    # Step 1: Get existing questions from lookup table
    existing_query = f"""
    SELECT DISTINCT standardized_question AS sq
    FROM `{QUESTIONS_LOOKUP}`
    WHERE standardized_question IS NOT NULL
    """
    existing_df = client.query(existing_query).to_dataframe()
    existing_questions = set(existing_df['sq'].tolist()) if not existing_df.empty else set()

    # Step 2: Get questions from columnar that might be missing
    columnar_query = f"""
    SELECT
        standardized_question AS sq,
        category AS cat,
        COUNT(*) as row_count
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_question IS NOT NULL
      AND standardized_question NOT IN ('__SKIP__', '__REVIEW__')
    GROUP BY standardized_question, category
    ORDER BY row_count DESC
    """
    columnar_df = client.query(columnar_query).to_dataframe()

    if columnar_df.empty:
        return []

    # Step 3: Find missing questions and pick best category
    missing = {}
    for _, row in columnar_df.iterrows():
        sq = row['sq']
        if sq in existing_questions:
            continue
        if sq not in missing:
            missing[sq] = {
                'standardized_question': sq,
                'display_name': sq.replace('_', ' ').title(),
                'category': row['cat'] if pd.notna(row['cat']) else 'other',
                'row_count': row['row_count']
            }
        else:
            missing[sq]['row_count'] += row['row_count']

    return sorted(missing.values(), key=lambda x: x['row_count'], reverse=True)


def sync_questions_to_lookup(client, dry_run: bool = False) -> int:
    """
    Sync standardized_question values from columnar_completed to lookup table.

    Args:
        client: BigQuery client
        dry_run: If True, only preview changes without executing

    Returns:
        Number of questions synced
    """
    # Preview what will be synced (this also gives us the data to insert)
    to_sync = preview_questions_to_sync(client)

    if not to_sync:
        logger.info("No new questions to sync")
        return 0

    logger.info(f"Found {len(to_sync)} questions to sync:")
    for q in to_sync[:20]:  # Show first 20
        logger.info(f"  - {q['standardized_question']} ({q['row_count']:,} rows, category: {q['category']})")

    if len(to_sync) > 20:
        logger.info(f"  ... and {len(to_sync) - 20} more")

    if dry_run:
        logger.info("DRY RUN - no changes made")
        return len(to_sync)

    # Insert each question individually to avoid complex query issues
    rows_inserted = 0
    for q in to_sync:
        insert_query = f"""
        INSERT INTO `{QUESTIONS_LOOKUP}` (standardized_question, display_name, category, created_at, updated_at)
        VALUES (@std_question, @display_name, @category, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('std_question', 'STRING', q['standardized_question']),
                bigquery.ScalarQueryParameter('display_name', 'STRING', q['display_name']),
                bigquery.ScalarQueryParameter('category', 'STRING', q['category'])
            ]
        )
        try:
            client.query(insert_query, job_config=job_config).result()
            rows_inserted += 1
        except Exception as e:
            logger.warning(f"Failed to insert {q['standardized_question']}: {e}")

    logger.info(f"Synced {rows_inserted} questions to lookup table")
    return rows_inserted


def preview_answers_to_sync(client) -> list:
    """
    Find standardized_answer values in columnar_completed
    that don't exist in standardized_answers_lookup.

    Returns list of dicts with answer info.
    """
    # Step 1: Get existing answers from lookup table
    existing_query = f"""
    SELECT standardized_question AS sq, standardized_answer AS sa
    FROM `{ANSWERS_LOOKUP}`
    """
    existing_df = client.query(existing_query).to_dataframe()
    existing_answers = set()
    if not existing_df.empty:
        for _, row in existing_df.iterrows():
            existing_answers.add((row['sq'], row['sa']))

    # Step 2: Get answers from columnar
    columnar_query = f"""
    SELECT
        standardized_question AS sq,
        standardized_answer AS sa,
        COUNT(*) as row_count
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_answer IS NOT NULL
      AND standardized_question IS NOT NULL
      AND standardized_answer NOT IN ('__FREE_TEXT__', '__REVIEW__', '__SKIP__')
    GROUP BY standardized_question, standardized_answer
    ORDER BY row_count DESC
    """
    columnar_df = client.query(columnar_query).to_dataframe()

    if columnar_df.empty:
        return []

    # Step 3: Find missing answers
    missing = []
    for _, row in columnar_df.iterrows():
        key = (row['sq'], row['sa'])
        if key not in existing_answers:
            missing.append({
                'standardized_question': row['sq'],
                'standardized_answer': row['sa'],
                'display_name': row['sa'].replace('_', ' ').title(),
                'row_count': row['row_count']
            })

    return missing


def sync_answers_to_lookup(client, dry_run: bool = False) -> int:
    """
    Sync standardized_answer values from columnar_completed to lookup table.

    Args:
        client: BigQuery client
        dry_run: If True, only preview changes without executing

    Returns:
        Number of answers synced
    """
    # Preview what will be synced (this also gives us the data to insert)
    to_sync = preview_answers_to_sync(client)

    if not to_sync:
        logger.info("No new answers to sync")
        return 0

    logger.info(f"Found {len(to_sync)} answers to sync:")
    for a in to_sync[:20]:  # Show first 20
        logger.info(f"  - {a['standardized_question']}: {a['standardized_answer']} ({a['row_count']:,} rows)")

    if len(to_sync) > 20:
        logger.info(f"  ... and {len(to_sync) - 20} more")

    if dry_run:
        logger.info("DRY RUN - no changes made")
        return len(to_sync)

    # Insert each answer individually to avoid complex query issues
    rows_inserted = 0
    for a in to_sync:
        insert_query = f"""
        INSERT INTO `{ANSWERS_LOOKUP}` (standardized_question, standardized_answer, display_name, sort_order, created_at, updated_at)
        VALUES (@std_question, @std_answer, @display_name, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('std_question', 'STRING', a['standardized_question']),
                bigquery.ScalarQueryParameter('std_answer', 'STRING', a['standardized_answer']),
                bigquery.ScalarQueryParameter('display_name', 'STRING', a['display_name'])
            ]
        )
        try:
            client.query(insert_query, job_config=job_config).result()
            rows_inserted += 1
        except Exception as e:
            logger.warning(f"Failed to insert {a['standardized_question']}:{a['standardized_answer']}: {e}")

    logger.info(f"Synced {rows_inserted} answers to lookup table")
    return rows_inserted


def preview_categories_to_sync(client) -> dict:
    """
    Find rows in columnar_completed where category doesn't match
    the category in standardized_questions_lookup.

    Returns dict with counts.
    """
    query = f"""
    SELECT
        COUNT(*) as rows_to_update,
        COUNT(DISTINCT c.standardized_question) as questions_affected
    FROM `{COLUMNAR_COMPLETED}` c
    JOIN `{QUESTIONS_LOOKUP}` l
        ON c.standardized_question = l.standardized_question
    WHERE c.category IS DISTINCT FROM l.category
    """

    result = list(client.query(query))
    return {
        'rows_to_update': result[0].rows_to_update,
        'questions_affected': result[0].questions_affected
    }


def sync_categories_from_lookup(client, dry_run: bool = False) -> int:
    """
    Update category in columnar_completed from standardized_questions_lookup.

    This ensures the lookup table is the source of truth for categories.

    Args:
        client: BigQuery client
        dry_run: If True, only preview changes without executing

    Returns:
        Number of rows updated
    """
    # Preview what will be synced
    preview = preview_categories_to_sync(client)

    if preview['rows_to_update'] == 0:
        logger.info("All categories are in sync")
        return 0

    logger.info(f"Found {preview['rows_to_update']:,} rows to update across {preview['questions_affected']} questions")

    if dry_run:
        logger.info("DRY RUN - no changes made")
        return preview['rows_to_update']

    # Execute sync
    query = f"""
    UPDATE `{COLUMNAR_COMPLETED}` c
    SET category = l.category,
        updated_at = CURRENT_TIMESTAMP()
    FROM `{QUESTIONS_LOOKUP}` l
    WHERE c.standardized_question = l.standardized_question
      AND c.category IS DISTINCT FROM l.category
    """

    job = client.query(query)
    job.result()

    rows_updated = job.num_dml_affected_rows or 0
    logger.info(f"Updated category for {rows_updated:,} rows")

    return rows_updated


def get_sync_stats(client) -> dict:
    """Get current sync statistics."""
    query = f"""
    WITH lookup_stats AS (
        SELECT
            (SELECT COUNT(DISTINCT standardized_question) FROM `{QUESTIONS_LOOKUP}`) as questions_in_lookup,
            (SELECT COUNT(*) FROM `{ANSWERS_LOOKUP}`) as answers_in_lookup
    ),
    columnar_stats AS (
        SELECT
            COUNT(DISTINCT standardized_question) as questions_in_columnar,
            COUNT(DISTINCT CONCAT(standardized_question, ':', standardized_answer)) as answers_in_columnar
        FROM `{COLUMNAR_COMPLETED}`
        WHERE standardized_question IS NOT NULL
          AND standardized_question NOT IN ('__SKIP__', '__REVIEW__')
    )
    SELECT * FROM lookup_stats, columnar_stats
    """

    result = list(client.query(query))
    row = result[0]

    return {
        'questions_in_lookup': row.questions_in_lookup,
        'questions_in_columnar': row.questions_in_columnar,
        'answers_in_lookup': row.answers_in_lookup,
        'answers_in_columnar': row.answers_in_columnar
    }


def main():
    parser = argparse.ArgumentParser(description='Sync lookup tables from columnar_completed')
    parser.add_argument('--questions', action='store_true', help='Sync questions to lookup')
    parser.add_argument('--answers', action='store_true', help='Sync answers to lookup')
    parser.add_argument('--categories', action='store_true', help='Sync categories from lookup to columnar')
    parser.add_argument('--all', action='store_true', help='Sync everything')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without executing')
    parser.add_argument('--stats', action='store_true', help='Show sync statistics')

    args = parser.parse_args()

    # Default to --all if no specific option provided
    if not any([args.questions, args.answers, args.categories, args.all, args.stats]):
        args.all = True

    client = get_bq_client()

    if args.stats:
        stats = get_sync_stats(client)
        logger.info("Sync Statistics:")
        logger.info(f"  Questions in lookup: {stats['questions_in_lookup']}")
        logger.info(f"  Questions in columnar: {stats['questions_in_columnar']}")
        logger.info(f"  Answers in lookup: {stats['answers_in_lookup']}")
        logger.info(f"  Answer combos in columnar: {stats['answers_in_columnar']}")
        return

    total_synced = 0

    if args.questions or args.all:
        logger.info("=" * 60)
        logger.info("Syncing questions to lookup table...")
        total_synced += sync_questions_to_lookup(client, dry_run=args.dry_run)

    if args.answers or args.all:
        logger.info("=" * 60)
        logger.info("Syncing answers to lookup table...")
        total_synced += sync_answers_to_lookup(client, dry_run=args.dry_run)

    if args.categories or args.all:
        logger.info("=" * 60)
        logger.info("Syncing categories from lookup to columnar...")
        total_synced += sync_categories_from_lookup(client, dry_run=args.dry_run)

    logger.info("=" * 60)
    if args.dry_run:
        logger.info(f"DRY RUN COMPLETE - {total_synced} items would be synced")
    else:
        logger.info(f"SYNC COMPLETE - {total_synced} items synced")


if __name__ == '__main__':
    main()
