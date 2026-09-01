#!/usr/bin/env python3
"""
Analyze all potentially misclassified questions in lime_surveys_columnar_completed.

Identifies questions where multiple answers per response don't make logical sense,
indicating different raw questions were incorrectly mapped to the same standardized_question.
"""

import os
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
    for _p in (r'c:\gcp\service-account-bionews-pipeline.json', '/home/orchestrator/service-account-bionews-pipeline.json'):
        if os.path.exists(_p):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _p
            break

from google.cloud import bigquery


def analyze_question(client: bigquery.Client, standardized_question: str) -> dict:
    """Analyze a single standardized_question for misclassification."""

    # Get raw question breakdown
    query = f"""
    SELECT
        raw_question_en,
        COUNT(*) as cnt
    FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
    WHERE standardized_question = '{standardized_question}'
    GROUP BY 1
    ORDER BY cnt DESC
    """

    results = []
    for row in client.query(query):
        results.append({
            'raw_question': row.raw_question_en,
            'count': row.cnt
        })

    return {
        'standardized_question': standardized_question,
        'raw_questions': results,
        'unique_raw_questions': len(results)
    }


def main():
    client = bigquery.Client(project='bi-data-391216')

    # Questions to analyze - ones where multiple answers seem illogical
    suspicious = [
        'time_since_diagnosis',
        'condition',
        'respondent_type',
        'disease_stage',
        'living_situation',
        'condition_relationship',
        'gene_therapy_conversation_initiator',
        'treatment_funding_source',
        'medication_intake_preference',
        'treatment_administration_route',
        'clinical_trial_participation',
        'genetic_testing_history',
        'misdiagnosis_history',
    ]

    print("MISCLASSIFICATION ANALYSIS")
    print("=" * 100)
    print("Looking for questions where different raw questions map to the same standardized_question\n")

    misclassified = []

    for sq in suspicious:
        result = analyze_question(client, sq)

        print(f"\n{'='*80}")
        print(f"{sq.upper()} ({result['unique_raw_questions']} distinct raw questions)")
        print("=" * 80)

        for rq in result['raw_questions'][:10]:
            q = (rq['raw_question'] or '(null)')[:75]
            print(f"{rq['count']:>6,} | {q}")

        # Flag as misclassified if multiple meaningfully different raw questions
        if result['unique_raw_questions'] > 1:
            # Check if they're actually different questions vs same question with slight variations
            raw_qs = [r['raw_question'] or '' for r in result['raw_questions']]

            # Simple heuristic: if the first 20 chars differ significantly, likely misclassified
            prefixes = set()
            for rq in raw_qs:
                prefix = rq[:30].lower().strip()
                # Normalize common variations
                prefix = prefix.replace('how long', 'howlong')
                prefix = prefix.replace('what is', 'whatis')
                prefixes.add(prefix)

            if len(prefixes) > 1:
                misclassified.append(result)
                print(f"  [!] LIKELY MISCLASSIFIED - {len(prefixes)} distinct question patterns")

    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print(f"Analyzed: {len(suspicious)} questions")
    print(f"Likely misclassified: {len(misclassified)}")

    if misclassified:
        print("\nMisclassified questions to fix:")
        for m in misclassified:
            print(f"  - {m['standardized_question']} ({m['unique_raw_questions']} raw questions)")


if __name__ == '__main__':
    main()
