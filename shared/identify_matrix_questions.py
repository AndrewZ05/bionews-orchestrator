#!/usr/bin/env python3
"""
Identify all matrix questions that have subquestions.
These need to be split into separate columns in the wide table.
"""

import os
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GLOG_minloglevel'] = '3'

from dotenv import load_dotenv
from google.cloud import bigquery

env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

client = bigquery.Client(project='bi-data-391216')

print("=" * 100)
print("MATRIX QUESTIONS WITH SUBQUESTIONS")
print("These need separate columns in wide table, not pipe-delimited values")
print("=" * 100)

# Find all standardized questions that have multiple subquestion_ids
query = """
WITH subq_counts AS (
    SELECT
        standardized_question,
        COUNT(DISTINCT subquestion_id) as num_subquestions,
        COUNT(DISTINCT response_id) as num_responses,
        COUNT(*) as total_rows
    FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
    WHERE standardized_question IS NOT NULL
      AND subquestion_id IS NOT NULL
    GROUP BY 1
    HAVING COUNT(DISTINCT subquestion_id) > 1
)
SELECT *
FROM subq_counts
ORDER BY num_subquestions DESC, total_rows DESC
"""

print(f"\n{'Standardized Question':<50} {'Subqs':>8} {'Responses':>10} {'Rows':>10}")
print("-" * 80)

matrix_questions = []
for row in client.query(query):
    print(f"{row.standardized_question:<50} {row.num_subquestions:>8} {row.num_responses:>10,} {row.total_rows:>10,}")
    matrix_questions.append(row.standardized_question)

print(f"\nTotal matrix questions: {len(matrix_questions)}")

# Get subquestion details for each matrix question
print("\n" + "=" * 100)
print("SUBQUESTION DETAILS FOR EACH MATRIX QUESTION")
print("=" * 100)

for std_q in matrix_questions[:15]:  # Show first 15
    print(f"\n{std_q}")
    print("-" * 80)

    # Get subquestion texts
    subq_query = f"""
    SELECT DISTINCT
        c.subquestion_id,
        sq.title as subq_code,
        sql.question as subq_text
    FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed` c
    LEFT JOIN `bi-data-391216.limesurvey_data.lime_questions` sq
        ON c.question_id = sq.parent_qid
        AND c.subquestion_id = sq.title
    LEFT JOIN `bi-data-391216.limesurvey_data.lime_question_l10ns` sql
        ON sq.qid = sql.qid AND sql.language = 'en'
    WHERE c.standardized_question = '{std_q}'
      AND c.subquestion_id IS NOT NULL
    ORDER BY c.subquestion_id
    """

    for row in client.query(subq_query):
        subq_text = row.subq_text[:60] if row.subq_text else '(no text found)'
        print(f"  [{row.subquestion_id}] {subq_text}")

print("\n" + "=" * 100)
print("RECOMMENDATION")
print("=" * 100)
print("""
For matrix questions, the wide table should create SEPARATE columns for each subquestion:
- symptom_onset_order_swallowing
- symptom_onset_order_breathing
- symptom_onset_order_speech
- symptom_onset_order_fatigue
- symptom_onset_order_mobility

Instead of concatenating into one useless column:
- symptom_onset_order = "1 - First|2 - Second|3 - Third|..."
""")
