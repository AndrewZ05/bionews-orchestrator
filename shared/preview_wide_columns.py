#!/usr/bin/env python3
"""
Preview what columns would change in lime_surveys_wide after the subquestion_id fix.
This is READ-ONLY - no data is modified.
"""

import os
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GLOG_minloglevel'] = '3'

from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'

def get_bq_client():
    """Get BigQuery client with service account credentials"""
    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_path and os.path.exists(creds_path):
        credentials = service_account.Credentials.from_service_account_file(creds_path)
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    else:
        return bigquery.Client(project=PROJECT_ID)

def main():
    client = get_bq_client()

    print("="*80)
    print(" PREVIEW: Wide Table Column Impact ".center(80))
    print("="*80)

    # Query 1: Current vs New subquestion_id parsing comparison
    print("\n[1] Comparing CURRENT vs NEW subquestion_id parsing...")
    print("-"*80)

    query1 = f"""
    WITH parsed AS (
        SELECT
            survey_id,
            question_id,
            raw_column_name,
            subquestion_id as current_subq,
            -- New parsing logic (same as the fix)
            CASE
                WHEN LOWER(raw_column_name) LIKE '%other' THEN 'other'
                WHEN LOWER(raw_column_name) LIKE '%comment' THEN 'comment'
                WHEN REGEXP_CONTAINS(LOWER(raw_column_name), r'sq\\d+$') THEN REGEXP_EXTRACT(LOWER(raw_column_name), r'sq(\\d+)$')
                WHEN REGEXP_CONTAINS(raw_column_name, r'^\\d+x\\d+x\\d+$') THEN NULL
                ELSE REGEXP_EXTRACT(raw_column_name, r'^\\d+x\\d+x\\d+(.+)$')
            END as new_subq
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
    )
    SELECT
        'Rows where subquestion_id would CHANGE' as metric,
        COUNT(*) as row_count,
        COUNT(DISTINCT CONCAT(CAST(question_id AS STRING), '_', COALESCE(new_subq, 'NULL'))) as new_unique_combos
    FROM parsed
    WHERE COALESCE(current_subq, 'NULL') != COALESCE(new_subq, 'NULL')
    """

    df1 = client.query(query1).to_dataframe()
    print(df1.to_string(index=False))

    # Query 2: Sample of what would change
    print("\n[2] Sample of subquestion_id changes (showing what's currently NULL)...")
    print("-"*80)

    query2 = f"""
    WITH parsed AS (
        SELECT
            survey_id,
            question_id,
            raw_column_name,
            subquestion_id as current_subq,
            CASE
                WHEN LOWER(raw_column_name) LIKE '%other' THEN 'other'
                WHEN LOWER(raw_column_name) LIKE '%comment' THEN 'comment'
                WHEN REGEXP_CONTAINS(LOWER(raw_column_name), r'sq\\d+$') THEN REGEXP_EXTRACT(LOWER(raw_column_name), r'sq(\\d+)$')
                WHEN REGEXP_CONTAINS(raw_column_name, r'^\\d+x\\d+x\\d+$') THEN NULL
                ELSE REGEXP_EXTRACT(raw_column_name, r'^\\d+x\\d+x\\d+(.+)$')
            END as new_subq
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
    )
    SELECT
        raw_column_name,
        COALESCE(current_subq, 'NULL') as current_subquestion_id,
        COALESCE(new_subq, 'NULL') as new_subquestion_id
    FROM parsed
    WHERE COALESCE(current_subq, 'NULL') != COALESCE(new_subq, 'NULL')
    LIMIT 30
    """

    df2 = client.query(query2).to_dataframe()
    print(df2.to_string(index=False))

    # Query 3: Count of NEW column suffixes that would be created
    print("\n[3] NEW subquestion suffixes that would be created (grouped by pattern)...")
    print("-"*80)

    query3 = f"""
    WITH parsed AS (
        SELECT
            survey_id,
            question_id,
            raw_column_name,
            subquestion_id as current_subq,
            CASE
                WHEN LOWER(raw_column_name) LIKE '%other' THEN 'other'
                WHEN LOWER(raw_column_name) LIKE '%comment' THEN 'comment'
                WHEN REGEXP_CONTAINS(LOWER(raw_column_name), r'sq\\d+$') THEN REGEXP_EXTRACT(LOWER(raw_column_name), r'sq(\\d+)$')
                WHEN REGEXP_CONTAINS(raw_column_name, r'^\\d+x\\d+x\\d+$') THEN NULL
                ELSE REGEXP_EXTRACT(raw_column_name, r'^\\d+x\\d+x\\d+(.+)$')
            END as new_subq
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
        WHERE subquestion_id IS NULL
    )
    SELECT
        new_subq as new_suffix,
        COUNT(*) as row_count,
        COUNT(DISTINCT question_id) as questions_affected
    FROM parsed
    WHERE new_subq IS NOT NULL
    GROUP BY new_subq
    ORDER BY row_count DESC
    LIMIT 50
    """

    df3 = client.query(query3).to_dataframe()
    print(df3.to_string(index=False))

    # Query 4: Summary - how many NEW columns in wide table
    print("\n[4] SUMMARY: Wide table column impact...")
    print("-"*80)

    query4 = f"""
    WITH current_columns AS (
        SELECT DISTINCT
            question_id,
            COALESCE(subquestion_id, '') as subq
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
    ),
    new_columns AS (
        SELECT DISTINCT
            question_id,
            COALESCE(
                CASE
                    WHEN LOWER(raw_column_name) LIKE '%other' THEN 'other'
                    WHEN LOWER(raw_column_name) LIKE '%comment' THEN 'comment'
                    WHEN REGEXP_CONTAINS(LOWER(raw_column_name), r'sq\\d+$') THEN REGEXP_EXTRACT(LOWER(raw_column_name), r'sq(\\d+)$')
                    WHEN REGEXP_CONTAINS(raw_column_name, r'^\\d+x\\d+x\\d+$') THEN NULL
                    ELSE REGEXP_EXTRACT(raw_column_name, r'^\\d+x\\d+x\\d+(.+)$')
                END,
                ''
            ) as subq
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
    )
    SELECT
        (SELECT COUNT(*) FROM current_columns) as current_unique_columns,
        (SELECT COUNT(*) FROM new_columns) as new_unique_columns,
        (SELECT COUNT(*) FROM new_columns) - (SELECT COUNT(*) FROM current_columns) as net_change
    """

    df4 = client.query(query4).to_dataframe()
    print(df4.to_string(index=False))

    # Query 5: Breakdown by question - which questions get more columns
    print("\n[5] Questions with MOST new columns (top 20)...")
    print("-"*80)

    query5 = f"""
    WITH current_cols AS (
        SELECT
            question_id,
            COUNT(DISTINCT COALESCE(subquestion_id, '')) as current_col_count
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
        GROUP BY question_id
    ),
    new_cols AS (
        SELECT
            question_id,
            COUNT(DISTINCT COALESCE(
                CASE
                    WHEN LOWER(raw_column_name) LIKE '%other' THEN 'other'
                    WHEN LOWER(raw_column_name) LIKE '%comment' THEN 'comment'
                    WHEN REGEXP_CONTAINS(LOWER(raw_column_name), r'sq\\d+$') THEN REGEXP_EXTRACT(LOWER(raw_column_name), r'sq(\\d+)$')
                    WHEN REGEXP_CONTAINS(raw_column_name, r'^\\d+x\\d+x\\d+$') THEN NULL
                    ELSE REGEXP_EXTRACT(raw_column_name, r'^\\d+x\\d+x\\d+(.+)$')
                END,
                ''
            )) as new_col_count
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
        GROUP BY question_id
    )
    SELECT
        c.question_id,
        c.current_col_count,
        n.new_col_count,
        n.new_col_count - c.current_col_count as additional_columns
    FROM current_cols c
    JOIN new_cols n ON c.question_id = n.question_id
    WHERE n.new_col_count > c.current_col_count
    ORDER BY additional_columns DESC
    LIMIT 20
    """

    df5 = client.query(query5).to_dataframe()
    print(df5.to_string(index=False))

    print("\n" + "="*80)
    print(" ANALYSIS COMPLETE ".center(80))
    print("="*80)
    print("\nNOTE: This is a READ-ONLY preview. No data was modified.")

if __name__ == "__main__":
    main()
