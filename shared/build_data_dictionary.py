#!/usr/bin/env python3
"""
Build data dictionary table for lime_surveys_wide.

Creates a reference table with:
- column_name: The standardized question name
- question_type: simple, matrix, or free_text
- wide_column_count: Number of columns this creates in the wide table
- subquestion_count: Number of distinct subquestions (for matrix questions)
- description: Auto-generated description from raw questions
- sample_values: Top 5 most common answers
- fill_rate: Percentage of responses with this column filled
- unique_values: Count of distinct values
"""

import os
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GLOG_minloglevel'] = '3'

from dotenv import load_dotenv
from google.cloud import bigquery

env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

client = bigquery.Client(project='bi-data-391216')

PROJECT = 'bi-data-391216'
DATASET = 'limesurvey_data'
SOURCE_TABLE = 'lime_surveys_columnar_completed'
DICT_TABLE = 'lime_surveys_data_dictionary'


def build_data_dictionary():
    """Build the data dictionary from columnar data."""
    print("=" * 80)
    print("BUILDING DATA DICTIONARY")
    print("=" * 80)

    # Get total response count for fill rate calculation
    total_query = f"""
    SELECT COUNT(DISTINCT response_id) as total_responses
    FROM `{PROJECT}.{DATASET}.{SOURCE_TABLE}`
    """
    total_responses = list(client.query(total_query))[0].total_responses
    print(f"\nTotal unique responses: {total_responses:,}")

    # Build dictionary data
    print("\nBuilding dictionary entries...")

    dict_query = f"""
    WITH question_stats AS (
        SELECT
            standardized_question,
            COALESCE(question_type, 'simple') as question_type,
            COUNT(DISTINCT response_id) as response_count,
            COUNT(DISTINCT standardized_answer) as unique_values,
            ARRAY_AGG(DISTINCT raw_question_en LIMIT 3) as raw_questions,
            ARRAY_AGG(standardized_answer ORDER BY cnt DESC LIMIT 5) as top_answers
        FROM (
            SELECT
                standardized_question,
                question_type,
                response_id,
                standardized_answer,
                raw_question_en,
                COUNT(*) OVER (PARTITION BY standardized_question, standardized_answer) as cnt
            FROM `{PROJECT}.{DATASET}.{SOURCE_TABLE}`
            WHERE standardized_question IS NOT NULL
        )
        GROUP BY 1, 2
    )
    SELECT
        standardized_question as column_name,
        question_type,
        -- Create description from raw questions
        ARRAY_TO_STRING(raw_questions, ' | ') as description,
        -- Top 5 sample values
        ARRAY_TO_STRING(top_answers, ' | ') as sample_values,
        -- Fill rate as percentage
        ROUND(100.0 * response_count / {total_responses}, 2) as fill_rate_pct,
        unique_values,
        response_count
    FROM question_stats
    ORDER BY response_count DESC
    """

    results = list(client.query(dict_query))
    print(f"Found {len(results)} unique columns")

    # Create/replace the dictionary table
    print(f"\nCreating table {DICT_TABLE}...")

    create_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.{DICT_TABLE}` AS
    WITH question_stats AS (
        SELECT
            standardized_question,
            COALESCE(question_type, 'simple') as question_type,
            COUNT(DISTINCT response_id) as response_count,
            COUNT(DISTINCT standardized_answer) as unique_values,
            COUNT(DISTINCT subquestion_id) as subquestion_count,
            ARRAY_AGG(DISTINCT raw_question_en IGNORE NULLS LIMIT 3) as raw_questions,
            ARRAY_AGG(standardized_answer IGNORE NULLS ORDER BY cnt DESC LIMIT 5) as top_answers
        FROM (
            SELECT
                standardized_question,
                question_type,
                response_id,
                standardized_answer,
                raw_question_en,
                subquestion_id,
                COUNT(*) OVER (PARTITION BY standardized_question, standardized_answer) as cnt
            FROM `{PROJECT}.{DATASET}.{SOURCE_TABLE}`
            WHERE standardized_question IS NOT NULL
        )
        GROUP BY 1, 2
    )
    SELECT
        standardized_question as column_name,
        question_type,
        -- Single column per question (pipe-delimited format)
        1 as wide_column_count,
        subquestion_count,
        ARRAY_TO_STRING(raw_questions, ' | ') as description,
        ARRAY_TO_STRING(top_answers, ' | ') as sample_values,
        ROUND(100.0 * response_count / {total_responses}, 2) as fill_rate_pct,
        unique_values,
        response_count,
        CURRENT_TIMESTAMP() as created_at
    FROM question_stats
    ORDER BY response_count DESC
    """

    job = client.query(create_query)
    job.result()

    print(f"Table created: {PROJECT}.{DATASET}.{DICT_TABLE}")

    # Show summary with wide column counts
    verify_query = f"""
    SELECT
        question_type,
        COUNT(*) as unique_questions,
        SUM(wide_column_count) as total_wide_columns,
        ROUND(AVG(fill_rate_pct), 1) as avg_fill_rate,
        SUM(response_count) as total_rows
    FROM `{PROJECT}.{DATASET}.{DICT_TABLE}`
    GROUP BY 1
    ORDER BY total_wide_columns DESC
    """

    print("\n" + "=" * 80)
    print("DATA DICTIONARY SUMMARY")
    print("=" * 80)
    print(f"\n{'Question Type':<12} {'Questions':>10} {'Wide Cols':>12} {'Avg Fill %':>12} {'Total Rows':>12}")
    print("-" * 60)
    total_q = 0
    total_cols = 0
    for row in client.query(verify_query):
        qt = row.question_type or 'NULL'
        print(f"{qt:<12} {row.unique_questions:>10} {row.total_wide_columns:>12} {row.avg_fill_rate:>11.1f}% {row.total_rows:>12,}")
        total_q += row.unique_questions
        total_cols += row.total_wide_columns
    print("-" * 60)
    print(f"{'TOTAL':<12} {total_q:>10} {total_cols:>12}")
    print(f"\n+ ~15 metadata columns = ~{total_cols + 15} total wide table columns")

    # Show top 10 columns
    top_query = f"""
    SELECT column_name, question_type, wide_column_count, fill_rate_pct, unique_values
    FROM `{PROJECT}.{DATASET}.{DICT_TABLE}`
    ORDER BY response_count DESC
    LIMIT 10
    """

    print("\n" + "-" * 80)
    print("TOP 10 QUESTIONS BY RESPONSE COUNT")
    print("-" * 80)
    print(f"{'Column Name':<40} {'Type':<10} {'Wide#':>6} {'Fill %':>8} {'Unique':>8}")
    print("-" * 80)
    for row in client.query(top_query):
        name = row.column_name[:37] + '...' if len(row.column_name) > 40 else row.column_name
        print(f"{name:<40} {row.question_type:<10} {row.wide_column_count:>6} {row.fill_rate_pct:>7.1f}% {row.unique_values:>8}")

    print("\n" + "=" * 80)
    print("DATA DICTIONARY COMPLETE")
    print("=" * 80)


def main():
    build_data_dictionary()


if __name__ == '__main__':
    main()
