#!/usr/bin/env python3
"""Investigate matrix question structure for life_milestones_impact."""

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

PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'

def main():
    client = bigquery.Client(project=PROJECT_ID)

    question = sys.argv[1] if len(sys.argv) > 1 else 'life_milestones_impact'

    print(f"Investigating: {question}")
    print("=" * 100)

    # Check subquestion_id values
    print("\n1. SUBQUESTION IDs for this standardized question:")
    print("-" * 80)
    query1 = f"""
    SELECT
        subquestion_id,
        raw_column_name,
        COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed`
    WHERE standardized_question = '{question}'
    GROUP BY 1, 2
    ORDER BY cnt DESC
    """
    for row in client.query(query1):
        print(f"  Subquestion ID: {row.subquestion_id or 'NULL':<20} Column: {row.raw_column_name:<30} Count: {row.cnt}")

    # Check if there are subquestion texts
    print("\n\n2. RAW QUESTION TEXT by subquestion:")
    print("-" * 80)
    query2 = f"""
    SELECT DISTINCT
        subquestion_id,
        raw_question_en
    FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed`
    WHERE standardized_question = '{question}'
    ORDER BY subquestion_id
    """
    for row in client.query(query2):
        print(f"  [{row.subquestion_id or 'NULL'}]: {row.raw_question_en[:80]}...")

    # Check sample responses to understand the multi-select pattern
    print("\n\n3. SAMPLE RESPONSES (showing subquestions per response):")
    print("-" * 80)
    query3 = f"""
    SELECT
        response_id,
        subquestion_id,
        standardized_answer,
        raw_column_name
    FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed`
    WHERE standardized_question = '{question}'
    ORDER BY response_id, subquestion_id
    LIMIT 30
    """
    current_resp = None
    for row in client.query(query3):
        if row.response_id != current_resp:
            current_resp = row.response_id
            print(f"\n  Response {row.response_id}:")
        print(f"    [{row.subquestion_id or 'base'}] {row.raw_column_name}: {row.standardized_answer}")

    # Look at the lime_questions table to find subquestion structure
    print("\n\n4. SUBQUESTION STRUCTURE from lime_questions:")
    print("-" * 80)
    query4 = f"""
    SELECT DISTINCT
        c.question_id,
        sq.title as subq_title,
        sq.question_order,
        sql.question as subq_text
    FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed` c
    JOIN `{PROJECT_ID}.{DATASET}.lime_questions` sq
        ON c.question_id = sq.parent_qid AND sq.parent_qid != 0
    LEFT JOIN `{PROJECT_ID}.{DATASET}.lime_question_l10ns` sql
        ON sq.qid = sql.qid AND sql.language = 'en'
    WHERE c.standardized_question = '{question}'
    ORDER BY sq.question_order
    """
    results = list(client.query(query4))
    if results:
        for row in results:
            print(f"  [{row.subq_title}] {row.subq_text or 'No text'}")
    else:
        print("  No subquestions found in lime_questions")

        # Try alternate approach - look at raw column names
        print("\n  Trying alternate: checking raw column names pattern...")
        query5 = f"""
        SELECT DISTINCT raw_column_name
        FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed`
        WHERE standardized_question = '{question}'
        ORDER BY raw_column_name
        """
        for row in client.query(query5):
            print(f"    {row.raw_column_name}")


if __name__ == '__main__':
    main()
