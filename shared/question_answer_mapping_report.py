#!/usr/bin/env python3
"""
Generate a report showing raw questions mapped to each standardized question,
along with their answer distributions.

This helps understand which raw questions feed into each wide table column.
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

PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'
TABLE = 'lime_surveys_columnar_completed'

def main():
    client = bigquery.Client(project=PROJECT_ID)

    # Get specific columns if requested via command line
    target_columns = sys.argv[1:] if len(sys.argv) > 1 else None

    print("=" * 120)
    print("RAW QUESTION TO STANDARDIZED QUESTION MAPPING REPORT")
    print("=" * 120)

    if target_columns:
        print(f"Filtering to columns: {target_columns}")
        where_clause = f"AND standardized_question IN ({','.join([repr(c) for c in target_columns])})"
    else:
        where_clause = ""

    # Query to get all raw questions mapped to each standardized question, with answer counts
    query = f"""
    WITH question_mapping AS (
        SELECT
            standardized_question,
            raw_question_en,
            standardized_answer,
            COUNT(*) as cnt
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE standardized_question IS NOT NULL
          AND standardized_answer IS NOT NULL
          {where_clause}
        GROUP BY 1, 2, 3
    )
    SELECT
        standardized_question,
        raw_question_en,
        standardized_answer,
        cnt
    FROM question_mapping
    ORDER BY standardized_question, raw_question_en, cnt DESC
    """

    print("\nFetching data...")
    results = list(client.query(query))
    print(f"Retrieved {len(results):,} rows")

    # Organize by standardized question
    std_questions = defaultdict(lambda: defaultdict(list))
    for row in results:
        std_questions[row.standardized_question][row.raw_question_en].append({
            'answer': row.standardized_answer,
            'count': row.cnt
        })

    # Print report
    print("\n")
    for std_q in sorted(std_questions.keys()):
        raw_qs = std_questions[std_q]

        # Calculate totals
        total_rows = sum(a['count'] for rq in raw_qs.values() for a in rq)
        unique_answers = len(set(a['answer'] for rq in raw_qs.values() for a in rq))

        print("=" * 120)
        print(f"STANDARDIZED QUESTION: {std_q}")
        print(f"Total Rows: {total_rows:,} | Unique Answers: {unique_answers} | Raw Questions: {len(raw_qs)}")
        print("=" * 120)

        for raw_q in sorted(raw_qs.keys(), key=lambda x: -sum(a['count'] for a in raw_qs[x])):
            answers = raw_qs[raw_q]
            raw_total = sum(a['count'] for a in answers)

            # Truncate raw question if too long
            display_q = raw_q[:100] + "..." if len(raw_q) > 100 else raw_q

            print(f"\n  RAW QUESTION ({raw_total:,} rows): {display_q}")
            print(f"  " + "-" * 100)

            # Show top 15 answers for this raw question
            sorted_answers = sorted(answers, key=lambda x: -x['count'])
            for ans in sorted_answers[:15]:
                ans_display = ans['answer'][:70] + "..." if len(ans['answer']) > 70 else ans['answer']
                print(f"    {ans['count']:>8,} | {ans_display}")

            if len(sorted_answers) > 15:
                remaining = sum(a['count'] for a in sorted_answers[15:])
                print(f"    {remaining:>8,} | ... and {len(sorted_answers) - 15} more answer values")

        print("\n")


if __name__ == '__main__':
    main()
