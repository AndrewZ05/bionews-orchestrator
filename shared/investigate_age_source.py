#!/usr/bin/env python3
"""Find the source of bad age answers."""

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
TABLE = 'lime_surveys_columnar_completed'

def main():
    client = bigquery.Client(project=PROJECT_ID)

    print("=" * 100)
    print("INVESTIGATING SOURCE OF BAD AGE VALUES")
    print("=" * 100)

    # 1. Get full row details for the bad age values
    print("\n1. FULL ROW DETAILS FOR BAD AGE VALUES")
    print("-" * 80)
    query1 = f"""
    SELECT
        survey_id,
        response_id,
        raw_column_name,
        answer_value,
        raw_answer_en,
        standardized_answer,
        raw_question_en,
        standardized_question,
        site
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = 'age'
      AND standardized_answer LIKE '%Acute Physiology%'
    """
    for row in client.query(query1):
        print(f"Survey ID: {row.survey_id}")
        print(f"Response ID: {row.response_id}")
        print(f"Site: {row.site}")
        print(f"Raw Column: {row.raw_column_name}")
        print(f"Raw Question: {row.raw_question_en}")
        print(f"Answer Value (code): {row.answer_value}")
        print(f"Raw Answer EN: {row.raw_answer_en}")
        print(f"Standardized Answer: {row.standardized_answer}")
        print()

    # 2. Check what answer codes exist for the age question in lime_answers
    print("\n2. ANSWER CODES FOR AGE QUESTIONS FROM lime_answers")
    print("-" * 80)
    query2 = f"""
    SELECT DISTINCT
        a.qid,
        a.code,
        al.answer,
        ql.question
    FROM `{PROJECT_ID}.{DATASET}.lime_answers` a
    JOIN `{PROJECT_ID}.{DATASET}.lime_answer_l10ns` al ON a.aid = al.aid AND al.language = 'en'
    JOIN `{PROJECT_ID}.{DATASET}.lime_question_l10ns` ql ON a.qid = ql.qid AND ql.language = 'en'
    WHERE LOWER(ql.question) LIKE '%age%range%'
       OR LOWER(ql.question) LIKE '%how old%'
    ORDER BY a.qid, a.code
    LIMIT 50
    """
    current_qid = None
    for row in client.query(query2):
        if row.qid != current_qid:
            current_qid = row.qid
            print(f"\nQuestion {row.qid}: {row.question[:60]}...")
        print(f"  Code '{row.code}' -> '{row.answer[:60]}'")

    # 3. Check what answer codes have "Acute Physiology" text
    print("\n\n3. ANSWER CODES WITH 'APCH' OR 'Acute Physiology' TEXT")
    print("-" * 80)
    query3 = f"""
    SELECT
        a.qid,
        a.code,
        al.answer,
        ql.question
    FROM `{PROJECT_ID}.{DATASET}.lime_answers` a
    JOIN `{PROJECT_ID}.{DATASET}.lime_answer_l10ns` al ON a.aid = al.aid AND al.language = 'en'
    JOIN `{PROJECT_ID}.{DATASET}.lime_question_l10ns` ql ON a.qid = ql.qid AND ql.language = 'en'
    WHERE al.answer LIKE '%APCH%'
       OR al.answer LIKE '%Acute Physiology%'
    """
    results = list(client.query(query3))
    if results:
        for row in results:
            print(f"Question {row.qid}: {row.question[:60]}...")
            print(f"  Code '{row.code}' -> '{row.answer}'")
    else:
        print("No answer codes found with APCH/Acute Physiology text")

    # 4. Check the raw answer_value for bad rows
    print("\n\n4. RAW ANSWER CODES FOR BAD AGE VALUES")
    print("-" * 80)
    query4 = f"""
    SELECT DISTINCT answer_value
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = 'age'
      AND standardized_answer LIKE '%Acute Physiology%'
    """
    for row in client.query(query4):
        print(f"Answer code: '{row.answer_value}'")

    # 5. See if there's a mismatch in answer_id lookup
    print("\n\n5. CHECK ANSWER_ID LOOKUP FOR THESE BAD VALUES")
    print("-" * 80)
    query5 = f"""
    SELECT
        c.survey_id,
        c.question_id,
        c.answer_value,
        c.answer_id,
        c.raw_answer_en,
        al.answer as lookup_answer
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}` c
    LEFT JOIN `{PROJECT_ID}.{DATASET}.lime_answer_l10ns` al
        ON c.answer_id = al.aid AND al.language = 'en'
    WHERE c.standardized_question = 'age'
      AND c.standardized_answer LIKE '%Acute Physiology%'
    """
    for row in client.query(query5):
        print(f"Survey: {row.survey_id}, Question: {row.question_id}")
        print(f"  Answer Value: {row.answer_value}")
        print(f"  Answer ID: {row.answer_id}")
        print(f"  Raw Answer EN: {row.raw_answer_en[:60]}...")
        print(f"  Lookup Answer: {row.lookup_answer[:60] if row.lookup_answer else 'NULL'}...")


if __name__ == '__main__':
    main()
