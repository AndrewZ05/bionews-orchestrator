#!/usr/bin/env python3
"""
Build per-standardized_question answer fingerprints.

For each standardized_question in lime_surveys_columnar_completed, collect its top N
distinct raw answers and (optionally) embed them as a single profile vector. Used by the
Streamlit mapping UI to:
  1. Suggest better standardized_questions for a new raw_question based on answer overlap
  2. Flag suspicious mappings where the new raw_question's answers don't resemble the
     existing answer pool for the target standardized_question

Runs idempotently: WRITE_TRUNCATE into `standardized_question_answer_fingerprints`.
Intended to be called nightly by shared/limesurvey_run_daily_pipeline.py.

Usage:
    python shared/limesurvey_build_answer_fingerprints.py
    python shared/limesurvey_build_answer_fingerprints.py --top-n 50
    python shared/limesurvey_build_answer_fingerprints.py --no-embeddings
"""

import argparse
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery

env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'
TABLE = f'{PROJECT_ID}.{DATASET}.standardized_question_answer_fingerprints'
SOURCE = f'{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed'


def build_top_answers(client: bigquery.Client, top_n: int = 30) -> int:
    """Per standardized_question, collect top_n distinct raw answers by response count."""
    query = f"""
    CREATE OR REPLACE TABLE `{TABLE}` AS
    WITH ranked AS (
        SELECT
            standardized_question,
            raw_answer_en,
            COUNT(*) AS response_count,
            ROW_NUMBER() OVER (
                PARTITION BY standardized_question
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM `{SOURCE}`
        WHERE standardized_question IS NOT NULL
          AND raw_answer_en IS NOT NULL
          AND TRIM(raw_answer_en) != ''
          AND NOT REGEXP_CONTAINS(raw_answer_en, r'^[0-9.]+$')
        GROUP BY standardized_question, raw_answer_en
    ),
    agg AS (
        SELECT
            standardized_question,
            ARRAY_AGG(raw_answer_en ORDER BY response_count DESC) AS top_answers,
            ARRAY_AGG(response_count ORDER BY response_count DESC) AS top_answer_counts,
            SUM(response_count) AS total_responses,
            COUNT(*) AS distinct_answers
        FROM ranked
        WHERE rn <= {top_n}
        GROUP BY standardized_question
    )
    SELECT
        standardized_question,
        top_answers,
        top_answer_counts,
        total_responses,
        distinct_answers,
        CURRENT_TIMESTAMP() AS fingerprint_built_at
    FROM agg
    """
    job = client.query(query)
    job.result()
    return job.num_dml_affected_rows or 0


def main():
    parser = argparse.ArgumentParser(description='Build answer fingerprints for standardized_questions')
    parser.add_argument('--top-n', type=int, default=30, help='Keep top N distinct answers per std_q')
    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT_ID)

    print(f'Building answer fingerprints (top {args.top_n} answers per standardized_question)...')
    build_top_answers(client, top_n=args.top_n)

    # Report
    stats = list(client.query(f"""
        SELECT COUNT(*) AS n_std_questions,
               AVG(distinct_answers) AS avg_distinct,
               SUM(total_responses) AS total_responses
        FROM `{TABLE}`
    """))[0]
    print(f'[OK] Built fingerprints for {stats.n_std_questions:,} standardized_questions')
    print(f'  Avg distinct answers per q: {stats.avg_distinct:.1f}')
    print(f'  Total responses covered: {stats.total_responses:,}')


if __name__ == '__main__':
    main()
