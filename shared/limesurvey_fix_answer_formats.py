#!/usr/bin/env python3
"""
Fix answer format inconsistencies in lime_surveys_columnar_completed.

1. Yes/No normalization - Standardize Yes., Y, No., N variants
2. Likert scale normalization - Add labels to numeric 1-5 values

This fixes existing data. The ETL pipeline has been updated to prevent
future inconsistencies.
"""

import os
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
    for _p in (r'c:\gcp\service-account-bionews-pipeline.json', '/home/orchestrator/service-account-bionews-pipeline.json'):
        if os.path.exists(_p):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _p
            break

from google.cloud import bigquery


# Yes/No normalization fixes
YES_NO_FIXES = [
    ("Yes.", "Yes"),
    ("Y", "Yes"),
    ("No.", "No"),
    ("N", "No"),
]

# Likert scale fixes by question type
LIKERT_FIXES = [
    # fatigue_level
    {
        'question': 'fatigue_level',
        'fixes': [
            ('1', '1 - Not fatiguing at all'),
            ('2', '2 - Slightly fatiguing'),
            ('3', '3 - Moderately fatiguing'),
            ('4', '4 - Very fatiguing'),
            ('5', '5 - Extremely fatiguing'),
            ('1 not fatiguing at all', '1 - Not fatiguing at all'),
            ('5 fatigues very quickly', '5 - Extremely fatiguing'),
        ]
    },
    # symptom_qol_impact
    {
        'question': 'symptom_qol_impact',
        'fixes': [
            ('1', '1 - No impact'),
            ('2', '2 - Minor impact'),
            ('3', '3 - Moderate impact'),
            ('4', '4 - Significant impact'),
            ('5', '5 - Major impact'),
            ('1 least impactful', '1 - No impact'),
            ('5 most impactful', '5 - Major impact'),
        ]
    },
    # life_milestones_impact
    {
        'question': 'life_milestones_impact',
        'fixes': [
            ('1', '1 - No impact'),
            ('2', '2 - Minor impact'),
            ('3', '3 - Moderate impact'),
            ('4', '4 - Significant impact'),
            ('5', '5 - Major impact'),
            ('1 no impact at all', '1 - No impact'),
            ('5 significant impact', '5 - Major impact'),
        ]
    },
    # symptom_onset_order
    {
        'question': 'symptom_onset_order',
        'fixes': [
            ('1', '1 - First'),
            ('2', '2 - Second'),
            ('3', '3 - Third'),
            ('4', '4 - Fourth'),
            ('5', '5 - Fifth'),
        ]
    },
    # webinar_feedback_overall
    {
        'question': 'webinar_feedback_overall',
        'fixes': [
            ('1', '1 - Not helpful at all'),
            ('2', '2 - Slightly helpful'),
            ('3', '3 - Moderately helpful'),
            ('4', '4 - Very helpful'),
            ('5', '5 - Extremely helpful'),
            ('1 (Not helpful at all)', '1 - Not helpful at all'),
            ('5 (Extremely helpful)', '5 - Extremely helpful'),
        ]
    },
    # community_support_level_rating
    {
        'question': 'community_support_level_rating',
        'fixes': [
            ('1', '1 - No support'),
            ('2', '2 - Minimal support'),
            ('3', '3 - Some support'),
            ('4', '4 - Good support'),
            ('5', '5 - Excellent support'),
        ]
    },
]


def preview_fixes(client: bigquery.Client) -> dict:
    """Preview all fixes."""

    print("=" * 80)
    print("ANSWER FORMAT FIXES PREVIEW")
    print("=" * 80)

    total = 0

    # Yes/No fixes
    print("\n1. YES/NO NORMALIZATION")
    print("-" * 40)
    for old_val, new_val in YES_NO_FIXES:
        query = f"""
        SELECT COUNT(*) as cnt
        FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
        WHERE standardized_answer = '{old_val}'
        """
        count = list(client.query(query))[0].cnt
        if count > 0:
            print(f"  '{old_val}' -> '{new_val}': {count:,} rows")
            total += count

    # Likert fixes
    print("\n2. LIKERT SCALE NORMALIZATION")
    print("-" * 40)
    for likert in LIKERT_FIXES:
        q = likert['question']
        print(f"\n  {q}:")
        for old_val, new_val in likert['fixes']:
            query = f"""
            SELECT COUNT(*) as cnt
            FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
            WHERE standardized_question = '{q}'
              AND standardized_answer = '{old_val}'
            """
            count = list(client.query(query))[0].cnt
            if count > 0:
                print(f"    '{old_val}' -> '{new_val}': {count:,} rows")
                total += count

    print("\n" + "=" * 80)
    print(f"TOTAL ROWS TO UPDATE: {total:,}")
    print("=" * 80)

    return {'total': total}


def apply_fixes(client: bigquery.Client) -> dict:
    """Apply all fixes."""

    print("\n" + "=" * 80)
    print("APPLYING FIXES")
    print("=" * 80)

    results = {'yes_no': 0, 'likert': 0}

    # Yes/No fixes
    print("\n1. Applying Yes/No normalization...")
    for old_val, new_val in YES_NO_FIXES:
        query = f"""
        UPDATE `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
        SET standardized_answer = '{new_val}'
        WHERE standardized_answer = '{old_val}'
        """
        job = client.query(query)
        job.result()
        if job.num_dml_affected_rows > 0:
            print(f"  '{old_val}' -> '{new_val}': {job.num_dml_affected_rows:,} rows")
            results['yes_no'] += job.num_dml_affected_rows

    # Likert fixes
    print("\n2. Applying Likert scale normalization...")
    for likert in LIKERT_FIXES:
        q = likert['question']
        for old_val, new_val in likert['fixes']:
            # Escape single quotes
            old_escaped = old_val.replace("'", "\\'")
            new_escaped = new_val.replace("'", "\\'")

            query = f"""
            UPDATE `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
            SET standardized_answer = '{new_escaped}'
            WHERE standardized_question = '{q}'
              AND standardized_answer = '{old_escaped}'
            """
            job = client.query(query)
            job.result()
            if job.num_dml_affected_rows > 0:
                print(f"  {q}: '{old_val}' -> '{new_val}': {job.num_dml_affected_rows:,} rows")
                results['likert'] += job.num_dml_affected_rows

    return results


def verify_fixes(client: bigquery.Client) -> None:
    """Verify fixes were applied."""

    print("\n" + "=" * 80)
    print("VERIFICATION")
    print("=" * 80)

    # Check Yes/No
    query = """
    SELECT standardized_answer, COUNT(*) as cnt
    FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
    WHERE standardized_answer IN ('Yes', 'No', 'Yes.', 'No.', 'Y', 'N')
    GROUP BY 1
    ORDER BY cnt DESC
    """
    print("\nYes/No values after fix:")
    for row in client.query(query):
        print(f"  '{row.standardized_answer}': {row.cnt:,}")

    # Check Likert questions
    for likert in LIKERT_FIXES[:3]:  # Just check first 3
        q = likert['question']
        query = f"""
        SELECT standardized_answer, COUNT(*) as cnt
        FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
        WHERE standardized_question = '{q}'
        GROUP BY 1
        ORDER BY cnt DESC
        LIMIT 10
        """
        print(f"\n{q} values after fix:")
        for row in client.query(query):
            ans = row.standardized_answer[:50] if row.standardized_answer else '(null)'
            print(f"  {row.cnt:>5} | {ans}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Fix answer format inconsistencies')
    parser.add_argument('--apply', action='store_true', help='Apply fixes (default is preview)')
    parser.add_argument('--verify', action='store_true', help='Only verify current state')
    args = parser.parse_args()

    client = bigquery.Client(project='bi-data-391216')

    if args.verify:
        verify_fixes(client)
        return

    # Preview
    preview_fixes(client)

    if args.apply:
        results = apply_fixes(client)
        verify_fixes(client)

        print("\n" + "=" * 80)
        print("FIXES APPLIED SUCCESSFULLY!")
        print("=" * 80)
        print(f"  Yes/No rows: {results['yes_no']:,}")
        print(f"  Likert rows: {results['likert']:,}")
        print("\nNext steps:")
        print("  Rebuild wide table: python shared/limesurvey_build_wide_table.py")
    else:
        print("\n" + "=" * 80)
        print("DRY RUN COMPLETE - No changes made")
        print("Run with --apply to execute the fixes")
        print("=" * 80)


if __name__ == '__main__':
    main()
