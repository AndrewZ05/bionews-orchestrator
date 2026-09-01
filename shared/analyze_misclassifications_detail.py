#!/usr/bin/env python3
"""
Detailed analysis of misclassified questions with sample answers.
"""

import os
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
    for _p in (r'c:\gcp\service-account-bionews-pipeline.json', '/home/orchestrator/service-account-bionews-pipeline.json'):
        if os.path.exists(_p):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _p
            break

from google.cloud import bigquery


def analyze_with_answers(client: bigquery.Client, standardized_question: str):
    """Analyze with sample answers to understand the semantic difference."""

    query = f"""
    SELECT
        raw_question_en,
        standardized_answer,
        COUNT(*) as cnt
    FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
    WHERE standardized_question = '{standardized_question}'
    GROUP BY 1, 2
    ORDER BY raw_question_en, cnt DESC
    """

    print(f"\n{'='*100}")
    print(f"{standardized_question.upper()}")
    print("=" * 100)

    current_question = None
    answers_shown = 0
    for row in client.query(query):
        q = (row.raw_question_en or '(null)')[:70]
        a = (row.standardized_answer or '(null)')[:40]

        if q != current_question:
            current_question = q
            answers_shown = 0
            print(f"\n  RAW Q: {q}")
            print(f"  {'Answer':<45} {'Count':>10}")
            print(f"  {'-'*60}")

        if answers_shown < 5:
            print(f"    {a:<45} {row.cnt:>10,}")
            answers_shown += 1


def main():
    client = bigquery.Client(project='bi-data-391216')

    # Misclassified questions to analyze in detail
    questions = [
        'disease_stage',
        'gene_therapy_conversation_initiator',
        'clinical_trial_participation',
        'misdiagnosis_history',
        'respondent_type',
    ]

    print("DETAILED MISCLASSIFICATION ANALYSIS")
    print("Showing raw questions with their answer distributions")

    for q in questions:
        analyze_with_answers(client, q)


if __name__ == '__main__':
    main()
