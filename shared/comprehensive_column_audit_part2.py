#!/usr/bin/env python3
"""
Part 2: Additional targeted audits for specific issue categories.
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

PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'
TABLE = 'lime_surveys_columnar_completed'

def main():
    client = bigquery.Client(project=PROJECT_ID)

    print("=" * 100)
    print("ADDITIONAL TARGETED AUDITS")
    print("=" * 100)

    # 1. Check for remaining Yes/No inconsistencies
    print("\n1. YES/NO VALUE CHECK")
    print("-" * 80)
    query1 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE LOWER(standardized_answer) IN ('yes', 'no', 'y', 'n', 'true', 'false', '1', '0')
    GROUP BY 1
    ORDER BY cnt DESC
    """
    for row in client.query(query1):
        status = "OK" if row.standardized_answer in ('Yes', 'No') else "NEEDS FIX"
        print(f"  [{status}] '{row.standardized_answer}': {row.cnt:,} rows")

    # 2. Check for remaining Gender inconsistencies
    print("\n2. GENDER VALUE CHECK")
    print("-" * 80)
    query2 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question IN ('gender', 'sex', 'biological_sex')
    GROUP BY 1
    ORDER BY cnt DESC
    """
    for row in client.query(query2):
        status = "OK" if row.standardized_answer in ('Male', 'Female', 'Non-Binary', 'Prefer Not to Answer', 'Other') else "REVIEW"
        print(f"  [{status}] '{row.standardized_answer}': {row.cnt:,} rows")

    # 3. Check for remaining age-related inconsistencies
    print("\n3. AGE-RELATED VALUE CHECK")
    print("-" * 80)
    query3 = f"""
    SELECT standardized_question, standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question LIKE '%age%'
    GROUP BY 1, 2
    HAVING COUNT(*) > 10
    ORDER BY 1, cnt DESC
    """
    current_q = None
    for row in client.query(query3):
        if row.standardized_question != current_q:
            current_q = row.standardized_question
            print(f"\n  {current_q}:")
        print(f"    '{row.standardized_answer}': {row.cnt:,}")

    # 4. Check condition_relationship values
    print("\n4. CONDITION_RELATIONSHIP CHECK")
    print("-" * 80)
    query4 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = 'condition_relationship'
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT 30
    """
    for row in client.query(query4):
        has_placeholder = '{' in row.standardized_answer
        status = "NEEDS FIX" if has_placeholder else "OK"
        print(f"  [{status}] '{row.standardized_answer[:60]}': {row.cnt:,} rows")

    # 5. Check respondent_type values
    print("\n5. RESPONDENT_TYPE CHECK")
    print("-" * 80)
    query5 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = 'respondent_type'
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT 30
    """
    for row in client.query(query5):
        has_placeholder = '{' in row.standardized_answer
        status = "NEEDS FIX" if has_placeholder else "OK"
        print(f"  [{status}] '{row.standardized_answer[:60]}': {row.cnt:,} rows")

    # 6. Check for numeric values that need formatting
    print("\n6. NUMERIC VALUE FORMATTING CHECK")
    print("-" * 80)
    query6 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE REGEXP_CONTAINS(standardized_answer, r'^-?[0-9]+\\.0+$')
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT 20
    """
    results = list(client.query(query6))
    if results:
        for row in results:
            print(f"  [NEEDS FIX] '{row.standardized_answer}' -> '{int(float(row.standardized_answer))}': {row.cnt:,} rows")
    else:
        print("  [OK] No numeric formatting issues found")

    # 7. Check for UTF-8 encoding issues (mojibake)
    print("\n7. UTF-8 ENCODING ISSUES (MOJIBAKE)")
    print("-" * 80)
    query7 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_answer LIKE '%Ã%'
       OR standardized_answer LIKE '%â€%'
       OR standardized_answer LIKE '%Ã¶%'
       OR standardized_answer LIKE '%Ã©%'
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT 30
    """
    for row in client.query(query7):
        print(f"  [NEEDS FIX] '{row.standardized_answer[:60]}': {row.cnt:,} rows")

    # 8. Check for N/A variations
    print("\n8. N/A VARIATION CHECK")
    print("-" * 80)
    query8 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE LOWER(TRIM(standardized_answer)) IN ('na', 'n/a', 'n\\a', 'none', 'null', '', ' ')
       OR standardized_answer IS NULL
    GROUP BY 1
    ORDER BY cnt DESC
    """
    for row in client.query(query8):
        val = repr(row.standardized_answer) if row.standardized_answer else 'NULL'
        print(f"  '{val}': {row.cnt:,} rows")

    # 9. Check source field values
    print("\n9. SOURCE FIELD VALUES")
    print("-" * 80)
    query9 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = 'source'
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT 20
    """
    for row in client.query(query9):
        print(f"  '{row.standardized_answer}': {row.cnt:,} rows")

    # 10. Check Likert scale values remaining
    print("\n10. REMAINING LIKERT SCALE ISSUES")
    print("-" * 80)
    query10 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE LOWER(standardized_answer) IN (
        'agree', 'disagree', 'strongly agree', 'strongly disagree',
        'satisfied', 'dissatisfied', 'very satisfied', 'very dissatisfied',
        'likely', 'unlikely', 'very likely', 'very unlikely', 'not at all likely',
        'excellent', 'good', 'fair', 'poor', 'very poor',
        'interested', 'very interested', 'not interested', 'somewhat interested',
        'knowledgeable', 'very knowledgeable', 'somewhat knowledgeable', 'not knowledgeable',
        'always', 'often', 'sometimes', 'rarely', 'never',
        'daily', 'weekly', 'monthly', 'yearly', 'annually',
        'mild', 'moderate', 'severe', 'extreme'
    )
      AND standardized_answer != INITCAP(standardized_answer)
      AND standardized_answer = LOWER(standardized_answer)
    GROUP BY 1
    ORDER BY cnt DESC
    """
    results = list(client.query(query10))
    if results:
        for row in results:
            print(f"  [NEEDS FIX] '{row.standardized_answer}' -> '{row.standardized_answer.title()}': {row.cnt:,} rows")
    else:
        print("  [OK] No remaining lowercase Likert scale issues")

    # 11. Check treatment values
    print("\n11. TREATMENT VALUE CHECK")
    print("-" * 80)
    query11 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question IN ('current_treatment', 'past_treatment', 'treatment_type')
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT 25
    """
    for row in client.query(query11):
        print(f"  '{row.standardized_answer[:60]}': {row.cnt:,} rows")

    # 12. Check for HTML entities
    print("\n12. HTML ENTITIES CHECK")
    print("-" * 80)
    query12 = f"""
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_answer LIKE '%&nbsp;%'
       OR standardized_answer LIKE '%&amp;%'
       OR standardized_answer LIKE '%&lt;%'
       OR standardized_answer LIKE '%&gt;%'
       OR standardized_answer LIKE '%&quot;%'
       OR standardized_answer LIKE '%&#%'
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT 20
    """
    results = list(client.query(query12))
    if results:
        for row in results:
            print(f"  [NEEDS FIX] '{row.standardized_answer[:60]}': {row.cnt:,} rows")
    else:
        print("  [OK] No HTML entities found")

    # 13. Summary of distinct answer counts by question type
    print("\n13. QUESTION CARDINALITY SUMMARY (Free-text detection)")
    print("-" * 80)
    query13 = f"""
    SELECT
        standardized_question,
        COUNT(*) as total_rows,
        COUNT(DISTINCT standardized_answer) as unique_answers,
        ROUND(COUNT(DISTINCT standardized_answer) / COUNT(*) * 100, 1) as cardinality_pct
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question IS NOT NULL
    GROUP BY 1
    HAVING COUNT(*) > 100
    ORDER BY cardinality_pct DESC
    LIMIT 25
    """
    print(f"  {'Question':<50} {'Total':<10} {'Unique':<10} {'Cardinality':<10}")
    print("  " + "-" * 78)
    for row in client.query(query13):
        q = row.standardized_question[:48] if len(row.standardized_question) > 48 else row.standardized_question
        is_freetext = " [FREE-TEXT]" if row.cardinality_pct > 80 else ""
        print(f"  {q:<50} {row.total_rows:<10,} {row.unique_answers:<10,} {row.cardinality_pct:>6.1f}%{is_freetext}")

    print("\n" + "=" * 100)
    print("ADDITIONAL AUDITS COMPLETE")
    print("=" * 100)


if __name__ == '__main__':
    main()
