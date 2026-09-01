#!/usr/bin/env python3
"""
Show the standardized_question values that become wide table column names.
READ-ONLY query.
"""

import os
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GLOG_minloglevel'] = '3'

from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'

def get_bq_client():
    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_path and os.path.exists(creds_path):
        credentials = service_account.Credentials.from_service_account_file(creds_path)
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    return bigquery.Client(project=PROJECT_ID)

def main():
    client = get_bq_client()

    print("="*80)
    print(" WIDE TABLE COLUMN NAMES (from standardized_question) ".center(80))
    print("="*80)

    # Query: All distinct standardized_question values used for wide columns
    query = f"""
    SELECT
        standardized_question,
        COUNT(*) as row_count,
        COUNT(DISTINCT survey_id) as surveys_using
    FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed`
    WHERE standardized_question IS NOT NULL
    GROUP BY standardized_question
    ORDER BY row_count DESC
    """

    print("\n[1] Standardized questions (wide table column names)...")
    print("-"*80)

    df = client.query(query).to_dataframe()
    print(df.to_string(index=False))

    print(f"\n\nTOTAL UNIQUE COLUMNS: {len(df)}")

    # Also show NULL count
    query2 = f"""
    SELECT
        COUNT(*) as rows_without_standardized_question
    FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed`
    WHERE standardized_question IS NULL
    """

    print("\n[2] Rows without standardized_question (won't appear in wide)...")
    print("-"*80)
    df2 = client.query(query2).to_dataframe()
    print(df2.to_string(index=False))

if __name__ == "__main__":
    main()
