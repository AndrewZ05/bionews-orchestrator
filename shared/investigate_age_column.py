#!/usr/bin/env python3
"""Investigate misclassified age column values."""

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
    print("INVESTIGATING AGE COLUMN MISCLASSIFICATIONS")
    print("=" * 100)

    # 1. Find the raw question text for these misclassified age values
    print("\n1. MISCLASSIFIED 'age' VALUES (non-age-range answers)")
    print("-" * 80)
    query1 = f"""
    SELECT
        raw_question_en,
        standardized_question,
        standardized_answer,
        question_match_method,
        question_match_confidence,
        COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = 'age'
      AND standardized_answer NOT IN (
          'Under 5', '5 to 12', '13 to 17', 'Under 18', '18 Plus', '18 to 24',
          '25 to 34', '35 to 44', '45 to 54', '55 to 64', '55 Plus', '65 Plus',
          'Prefer Not To Say'
      )
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY cnt DESC
    """
    for row in client.query(query1):
        print(f"\nRaw Question: {row.raw_question_en[:100] if row.raw_question_en else 'NULL'}")
        print(f"  Std Question: {row.standardized_question}")
        print(f"  Answer: {row.standardized_answer}")
        print(f"  Match Method: {row.question_match_method}")
        print(f"  Confidence: {row.question_match_confidence}")
        print(f"  Count: {row.cnt:,}")

    # 2. What other standardized_questions have APCH114 answers?
    print("\n\n2. ALL QUESTIONS WITH 'APCH' OR 'Acute Physiology' ANSWERS")
    print("-" * 80)
    query2 = f"""
    SELECT
        standardized_question,
        raw_question_en,
        standardized_answer,
        COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_answer LIKE '%APCH%'
       OR standardized_answer LIKE '%Acute Physiology%'
    GROUP BY 1, 2, 3
    ORDER BY cnt DESC
    """
    for row in client.query(query2):
        print(f"\nStd Q: {row.standardized_question}")
        print(f"  Raw Q: {row.raw_question_en[:80] if row.raw_question_en else 'NULL'}...")
        print(f"  Answer: {row.standardized_answer[:80]}...")
        print(f"  Count: {row.cnt:,}")

    # 3. What should the correct standardized_question be?
    print("\n\n3. RAW QUESTIONS THAT MENTION 'APCH' or 'Acute Physiology'")
    print("-" * 80)
    query3 = f"""
    SELECT DISTINCT
        raw_question_en,
        standardized_question,
        question_match_method
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE raw_question_en LIKE '%APCH%'
       OR raw_question_en LIKE '%Acute Physiology%'
    """
    for row in client.query(query3):
        print(f"Raw Q: {row.raw_question_en}")
        print(f"  -> Mapped to: {row.standardized_question} (via {row.question_match_method})")
        print()

    # 4. Check all age values currently in the table
    print("\n4. ALL CURRENT AGE VALUES")
    print("-" * 80)
    query4 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = 'age'
    GROUP BY 1
    ORDER BY cnt DESC
    """
    for row in client.query(query4):
        is_valid = row.standardized_answer in (
            'Under 5', '5 to 12', '13 to 17', 'Under 18', '18 Plus', '18 to 24',
            '25 to 34', '35 to 44', '45 to 54', '55 to 64', '55 Plus', '65 Plus',
            'Prefer Not To Say'
        )
        status = "OK" if is_valid else "INVALID"
        print(f"  [{status}] '{row.standardized_answer[:70]}': {row.cnt:,} rows")

    # 5. Check what raw_question_en values map to 'age'
    print("\n\n5. RAW QUESTIONS MAPPING TO 'age'")
    print("-" * 80)
    query5 = f"""
    SELECT
        raw_question_en,
        question_match_method,
        question_match_confidence,
        COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = 'age'
    GROUP BY 1, 2, 3
    ORDER BY cnt DESC
    LIMIT 20
    """
    for row in client.query(query5):
        print(f"Raw Q: {row.raw_question_en[:80] if row.raw_question_en else 'NULL'}...")
        print(f"  Method: {row.question_match_method}, Confidence: {row.question_match_confidence}")
        print(f"  Count: {row.cnt:,}")
        print()


if __name__ == '__main__':
    main()
