#!/usr/bin/env python3
"""
Comprehensive fix for all misclassified questions in lime_surveys_columnar_completed.

This script fixes questions where different semantic meanings were incorrectly
mapped to the same standardized_question.

Fixes Applied:
1. time_since_diagnosis:
   - "onset of symptoms until" → time_from_symptom_to_diagnosis
   - "symptoms begin" → time_since_symptom_onset

2. disease_stage:
   - "familiar with the stages" → disease_stage_familiarity

3. gene_therapy_conversation_initiator:
   - "Have you discussed gene therapy" → gene_therapy_discussion_status

4. clinical_trial_participation:
   - "dates or time period" → clinical_trial_dates
   - "information about the trial" → clinical_trial_details
"""

import os
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
    for _p in (r'c:\gcp\service-account-bionews-pipeline.json', '/home/orchestrator/service-account-bionews-pipeline.json'):
        if os.path.exists(_p):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _p
            break

from google.cloud import bigquery


# All fixes to apply
FIXES = [
    # time_since_diagnosis fixes
    {
        'current_question': 'time_since_diagnosis',
        'new_question': 'time_from_symptom_to_diagnosis',
        'pattern': '%onset of symptoms until%',
        'description': 'Time from symptom onset to diagnosis'
    },
    {
        'current_question': 'time_since_diagnosis',
        'new_question': 'time_since_symptom_onset',
        'pattern': '%symptoms begin%',
        'description': 'Time since symptoms began'
    },

    # disease_stage fixes
    {
        'current_question': 'disease_stage',
        'new_question': 'disease_stage_familiarity',
        'pattern': '%familiar with the stages%',
        'description': 'Familiarity with disease stages (yes/no)'
    },

    # gene_therapy_conversation_initiator fixes
    {
        'current_question': 'gene_therapy_conversation_initiator',
        'new_question': 'gene_therapy_discussion_status',
        'pattern': 'Have you discussed gene therapy%',
        'description': 'Whether gene therapy was discussed with physician'
    },

    # clinical_trial_participation fixes
    {
        'current_question': 'clinical_trial_participation',
        'new_question': 'clinical_trial_dates',
        'pattern': '%dates or time period%',
        'description': 'Clinical trial dates/time period'
    },
    {
        'current_question': 'clinical_trial_participation',
        'new_question': 'clinical_trial_details',
        'pattern': '%information about the trial%',
        'description': 'Clinical trial details (name, location, etc.)'
    },
]


def preview_fixes(client: bigquery.Client) -> dict:
    """Preview all fixes without applying them."""

    print("=" * 80)
    print("MISCLASSIFICATION FIXES PREVIEW")
    print("=" * 80)

    total_rows = 0
    fix_counts = {}

    for fix in FIXES:
        query = f"""
        SELECT COUNT(*) as cnt
        FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
        WHERE standardized_question = '{fix['current_question']}'
          AND raw_question_en LIKE '{fix['pattern']}'
        """

        count = list(client.query(query))[0].cnt
        fix_counts[fix['new_question']] = count
        total_rows += count

        print(f"\n{fix['description']}:")
        print(f"  Current:  {fix['current_question']}")
        print(f"  New:      {fix['new_question']}")
        print(f"  Pattern:  {fix['pattern']}")
        print(f"  Rows:     {count:,}")

    print("\n" + "=" * 80)
    print(f"TOTAL ROWS TO UPDATE: {total_rows:,}")
    print("=" * 80)

    return fix_counts


def apply_fixes(client: bigquery.Client) -> dict:
    """Apply all fixes to the database."""

    print("\n" + "=" * 80)
    print("APPLYING FIXES")
    print("=" * 80)

    results = {}

    for fix in FIXES:
        query = f"""
        UPDATE `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
        SET standardized_question = '{fix['new_question']}'
        WHERE standardized_question = '{fix['current_question']}'
          AND raw_question_en LIKE '{fix['pattern']}'
        """

        print(f"\nApplying: {fix['current_question']} -> {fix['new_question']}")

        job = client.query(query)
        job.result()

        rows_affected = job.num_dml_affected_rows
        results[fix['new_question']] = rows_affected
        print(f"  [OK] Updated {rows_affected:,} rows")

    return results


def verify_fixes(client: bigquery.Client) -> None:
    """Verify the current state after fixes."""

    print("\n" + "=" * 80)
    print("VERIFICATION - New question classifications")
    print("=" * 80)

    # Check new questions exist
    new_questions = [fix['new_question'] for fix in FIXES]
    unique_new = list(set(new_questions))

    for q in unique_new:
        query = f"""
        SELECT COUNT(*) as cnt
        FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
        WHERE standardized_question = '{q}'
        """
        count = list(client.query(query))[0].cnt
        print(f"  {q}: {count:,} rows")

    # Check original questions no longer have the misclassified data
    print("\n" + "-" * 80)
    print("Original questions (should no longer match patterns):")
    print("-" * 80)

    original_questions = list(set([fix['current_question'] for fix in FIXES]))
    for q in original_questions:
        query = f"""
        SELECT COUNT(*) as cnt
        FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
        WHERE standardized_question = '{q}'
        """
        count = list(client.query(query))[0].cnt
        print(f"  {q}: {count:,} rows")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='Fix all misclassified questions in lime_surveys_columnar_completed',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview fixes (dry run)
  python shared/limesurvey_fix_all_misclassifications.py

  # Apply fixes
  python shared/limesurvey_fix_all_misclassifications.py --apply

  # Verify current state
  python shared/limesurvey_fix_all_misclassifications.py --verify
        """
    )
    parser.add_argument('--apply', action='store_true', help='Apply fixes (default is preview)')
    parser.add_argument('--verify', action='store_true', help='Only verify current state')
    args = parser.parse_args()

    client = bigquery.Client(project='bi-data-391216')

    if args.verify:
        verify_fixes(client)
        return

    # Preview fixes
    fix_counts = preview_fixes(client)

    if args.apply:
        # Apply fixes
        results = apply_fixes(client)

        # Verify
        verify_fixes(client)

        print("\n" + "=" * 80)
        print("FIXES APPLIED SUCCESSFULLY!")
        print("=" * 80)
        print("\nNext steps:")
        print("  1. Retrain ML model:")
        print("     python shared/limesurvey_ml_mismatch_detector.py --retrain")
        print("  2. Rebuild wide table:")
        print("     python shared/limesurvey_build_wide_table.py")
    else:
        print("\n" + "=" * 80)
        print("DRY RUN COMPLETE - No changes made")
        print("Run with --apply to execute the fixes")
        print("=" * 80)


if __name__ == '__main__':
    main()
