#!/usr/bin/env python3
"""
Diagnose duplicates in lime_surveys_columnar tables
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
        # Fall back to default credentials
        return bigquery.Client(project=PROJECT_ID)

def run_query(client, name, query):
    """Run a query and print results"""
    print(f"\n{'='*70}")
    print(f" {name} ")
    print('='*70)
    try:
        df = client.query(query).to_dataframe()
        if df.empty:
            print("No results (empty)")
        else:
            print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"ERROR: {e}")
        return None

def main():
    client = get_bq_client()

    # Query 1: Count duplicate (survey_id, response_id) pairs
    run_query(client, "Query 1: Duplicate (survey_id, response_id) Count", f"""
        SELECT
            'Duplicate Response Count' as metric,
            COUNT(*) as duplicate_pairs,
            SUM(row_count) as total_rows_in_duplicates
        FROM (
            SELECT survey_id, response_id, COUNT(*) as row_count
            FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
            GROUP BY survey_id, response_id
            HAVING COUNT(*) > 1
        )
    """)

    # Query 2: Sample duplicates
    run_query(client, "Query 2: Sample Duplicates (top 10)", f"""
        SELECT
            survey_id,
            response_id,
            COUNT(*) as row_count,
            COUNT(DISTINCT question_id) as unique_questions,
            COUNT(DISTINCT subquestion_id) as unique_subquestions,
            COUNT(DISTINCT raw_column_name) as unique_columns
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
        GROUP BY survey_id, response_id
        HAVING COUNT(*) > 1
        ORDER BY row_count DESC
        LIMIT 10
    """)

    # Query 3: Check NULL subquestion_id count
    run_query(client, "Query 3: NULL subquestion_id Stats", f"""
        SELECT
            'NULL subquestion_id' as issue_type,
            COUNT(*) as affected_rows,
            COUNT(DISTINCT CONCAT(CAST(survey_id AS STRING), '_', response_id)) as affected_responses
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
        WHERE subquestion_id IS NULL
    """)

    # Query 4: Check composite key duplicates
    run_query(client, "Query 4: Composite Key Duplicates (survey_id, response_id, question_id, subquestion_id)", f"""
        SELECT
            survey_id,
            response_id,
            question_id,
            COALESCE(subquestion_id, 'NULL') as subquestion_id,
            COUNT(*) as duplicate_count
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
        GROUP BY survey_id, response_id, question_id, subquestion_id
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC
        LIMIT 10
    """)

    # Query 5: Analyze raw_column_name patterns with NULL subquestion_id
    run_query(client, "Query 5: Column Name Patterns with NULL subquestion_id", f"""
        SELECT
            CASE
                WHEN LOWER(raw_column_name) LIKE '%other' THEN 'other_suffix'
                WHEN REGEXP_CONTAINS(raw_column_name, r'SQ\\d+$') THEN 'sq_suffix'
                WHEN REGEXP_CONTAINS(raw_column_name, r'[A-Z]\\d*$') THEN 'letter_suffix'
                WHEN REGEXP_CONTAINS(raw_column_name, r'_\\d+$') THEN 'underscore_num'
                ELSE 'no_suffix'
            END as suffix_type,
            COUNT(*) as row_count,
            COUNT(DISTINCT CONCAT(CAST(survey_id AS STRING), '_', response_id)) as unique_responses
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar`
        WHERE subquestion_id IS NULL
        GROUP BY 1
        ORDER BY row_count DESC
    """)

    # Query 6: Check lime_surveys_columnar_completed
    run_query(client, "Query 6: Completed Table - Composite Key Duplicates", f"""
        SELECT
            'Completed Table Duplicates' as metric,
            COUNT(*) as duplicate_composite_keys
        FROM (
            SELECT survey_id, response_id, question_id, subquestion_id
            FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed`
            GROUP BY survey_id, response_id, question_id, subquestion_id
            HAVING COUNT(*) > 1
        )
    """)

    # Query 7: Check answer table for duplicate codes
    run_query(client, "Query 7: Answer Code Duplicates", f"""
        SELECT
            qid,
            code,
            COUNT(*) as answer_count
        FROM `{PROJECT_ID}.{DATASET}.lime_answers`
        GROUP BY qid, code
        HAVING COUNT(*) > 1
        ORDER BY answer_count DESC
        LIMIT 10
    """)

    print(f"\n{'='*70}")
    print(" DIAGNOSIS COMPLETE ")
    print('='*70)

if __name__ == "__main__":
    main()
