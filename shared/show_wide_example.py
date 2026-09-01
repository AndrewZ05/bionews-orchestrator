#!/usr/bin/env python3
"""
Show example of how columnar data becomes wide table columns.
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

    print("="*100)
    print(" EXAMPLE: Columnar to Wide Table Transformation ".center(100))
    print("="*100)

    # Get one response with multiple questions
    print("\n[1] COLUMNAR FORMAT (one row per question-answer)...")
    print("-"*100)

    query1 = f"""
    SELECT
        survey_id,
        response_id,
        raw_column_name,
        question_id,
        subquestion_id,
        standardized_question,
        raw_answer_en
    FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed`
    WHERE response_id = (
        SELECT response_id
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed`
        WHERE standardized_question IS NOT NULL
        GROUP BY response_id
        HAVING COUNT(DISTINCT standardized_question) >= 5
        LIMIT 1
    )
    ORDER BY standardized_question
    LIMIT 15
    """

    df1 = client.query(query1).to_dataframe()
    print(df1.to_string(index=False))

    if len(df1) > 0:
        response_id = df1['response_id'].iloc[0]
        survey_id = df1['survey_id'].iloc[0]

        print(f"\n\n[2] WIDE FORMAT (one row per response, questions become columns)...")
        print("-"*100)
        print(f"Response: {response_id} | Survey: {survey_id}")
        print()

        # Show as pivoted
        print("Column Name                      | Value")
        print("-"*70)
        for _, row in df1.iterrows():
            col_name = str(row['standardized_question'])[:30]
            value = str(row['raw_answer_en'])[:35] if row['raw_answer_en'] else 'NULL'
            print(f"{col_name:<32} | {value}")

    # Also show actual wide table sample if it exists
    print("\n\n[3] ACTUAL WIDE TABLE (sample columns)...")
    print("-"*100)

    query3 = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'lime_surveys_wide'
    ORDER BY ordinal_position
    LIMIT 30
    """

    try:
        df3 = client.query(query3).to_dataframe()
        if len(df3) > 0:
            print(df3.to_string(index=False))
        else:
            print("Wide table not found or empty")
    except Exception as e:
        print(f"Wide table query error: {e}")

if __name__ == "__main__":
    main()
