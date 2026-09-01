#!/usr/bin/env python3
"""
LimeSurvey Fix: age_at_diagnosis Data Quality Issues

This script fixes two types of issues in the age_at_diagnosis column:
1. QUESTION MISCLASSIFICATION - Questions mapped to wrong standardized_question
2. ANSWER STANDARDIZATION - Inconsistent answer formats

Run with --dry-run to preview changes without applying them.

Usage:
    python shared/limesurvey_fix_age_at_diagnosis.py --dry-run
    python shared/limesurvey_fix_age_at_diagnosis.py --apply
"""

import os
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
    for _p in (r'c:\gcp\service-account-bionews-pipeline.json', '/home/orchestrator/service-account-bionews-pipeline.json'):
        if os.path.exists(_p):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _p
            break

import argparse
from google.cloud import bigquery

PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'
TABLE = 'lime_surveys_columnar_completed'

# =============================================================================
# FIX 1: QUESTION MISCLASSIFICATION
# These questions are incorrectly mapped to age_at_diagnosis
# =============================================================================

QUESTION_FIXES = [
    {
        'raw_question': 'Are you a caregiver of someone that has been diagnosed with Rett syndrome?',
        'current_std_question': 'age_at_diagnosis',
        'correct_std_question': 'is_caregiver',
        'reason': 'This is a yes/no caregiver question, not an age question'
    },
    {
        'raw_question': 'How old were you, or the person in your care, when you first noticed symptoms of FA?',
        'current_std_question': 'age_at_diagnosis',
        'correct_std_question': 'age_at_symptom_onset',
        'reason': 'This asks about symptom onset, not diagnosis'
    },
]

# =============================================================================
# FIX 2: ANSWER STANDARDIZATION
# Normalize all age ranges to consistent snake_case format
# =============================================================================

ANSWER_STANDARDIZATION = {
    # Format: current_value -> standardized_value

    # Already good format (keep as-is)
    'under_18': 'under_18',
    'age_18_24': 'age_18_24',
    'age_35_44': 'age_35_44',
    'age_45_54': 'age_45_54',
    'age_65_plus': 'age_65_plus',

    # Fix inconsistent naming
    '25_34 years': 'age_25_34',
    '55_64 years': 'age_55_64',

    # Convert human-readable to snake_case
    '0-7 years': 'age_0_7',
    '8-14 years': 'age_8_14',
    '15-24 years': 'age_15_24',
    '25-39 years': 'age_25_39',
    'Over 40 years': 'age_40_plus',

    # Special cases - not real ages
    'Yes': None,  # Will be fixed by question reclassification
    'No, I am not a caregiver of someone who has been diagnosed with condition': None,
    'I have not been diagnosed with FA': 'not_diagnosed',
}


def get_current_state(client):
    """Get current state of age_at_diagnosis data"""
    query = f"""
    SELECT
        standardized_question,
        standardized_answer,
        raw_question_en,
        COUNT(*) as row_count
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = 'age_at_diagnosis'
       OR (standardized_question IN ('is_caregiver', 'age_at_symptom_onset')
           AND raw_question_en IN (
               'Are you a caregiver of someone that has been diagnosed with Rett syndrome?',
               'How old were you, or the person in your care, when you first noticed symptoms of FA?'
           ))
    GROUP BY 1, 2, 3
    ORDER BY standardized_question, row_count DESC
    """
    return client.query(query).to_dataframe()


def preview_question_fixes(client):
    """Preview which rows would be affected by question reclassification"""
    print("\n" + "=" * 70)
    print("FIX 1: QUESTION MISCLASSIFICATION")
    print("=" * 70)

    total_affected = 0

    for fix in QUESTION_FIXES:
        query = f"""
        SELECT COUNT(*) as cnt
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE raw_question_en = @raw_question
          AND standardized_question = @current_std_question
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("raw_question", "STRING", fix['raw_question']),
                bigquery.ScalarQueryParameter("current_std_question", "STRING", fix['current_std_question']),
            ]
        )
        result = list(client.query(query, job_config=job_config))[0]
        count = result.cnt
        total_affected += count

        print(f"\nQuestion: {fix['raw_question'][:70]}...")
        print(f"  Current:  standardized_question = '{fix['current_std_question']}'")
        print(f"  Change to: standardized_question = '{fix['correct_std_question']}'")
        print(f"  Reason:   {fix['reason']}")
        print(f"  Rows affected: {count}")

    print(f"\nTotal rows to reclassify: {total_affected}")
    return total_affected


def preview_answer_standardization(client):
    """Preview which rows would be affected by answer standardization"""
    print("\n" + "=" * 70)
    print("FIX 2: ANSWER STANDARDIZATION")
    print("=" * 70)

    # Only apply to rows that will remain as age_at_diagnosis
    # (i.e., exclude the questions that will be reclassified)
    excluded_questions = [fix['raw_question'] for fix in QUESTION_FIXES]

    query = f"""
    SELECT
        standardized_answer,
        COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = 'age_at_diagnosis'
      AND raw_question_en NOT IN UNNEST(@excluded_questions)
    GROUP BY 1
    ORDER BY cnt DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("excluded_questions", "STRING", excluded_questions),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()

    print("\nCurrent Value             -> New Value                | Rows")
    print("-" * 70)

    total_to_change = 0
    for _, row in df.iterrows():
        current = row['standardized_answer']
        count = row['cnt']
        new_value = ANSWER_STANDARDIZATION.get(current, current)

        if new_value is None:
            status = "(will be reclassified)"
        elif new_value == current:
            status = "(no change)"
        else:
            status = f"-> {new_value}"
            total_to_change += count

        print(f"{current:25} {status:25} | {count:>5}")

    print(f"\nTotal rows to standardize: {total_to_change}")
    return total_to_change


def apply_question_fixes(client):
    """Apply question reclassification fixes"""
    print("\n" + "=" * 70)
    print("APPLYING QUESTION FIXES...")
    print("=" * 70)

    total_updated = 0

    for fix in QUESTION_FIXES:
        update_query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.{TABLE}`
        SET standardized_question = @correct_std_question
        WHERE raw_question_en = @raw_question
          AND standardized_question = @current_std_question
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("raw_question", "STRING", fix['raw_question']),
                bigquery.ScalarQueryParameter("current_std_question", "STRING", fix['current_std_question']),
                bigquery.ScalarQueryParameter("correct_std_question", "STRING", fix['correct_std_question']),
            ]
        )

        result = client.query(update_query, job_config=job_config).result()
        updated = result.num_dml_affected_rows
        total_updated += updated

        print(f"  Updated {updated} rows: {fix['current_std_question']} -> {fix['correct_std_question']}")

    print(f"\nTotal question fixes applied: {total_updated}")
    return total_updated


def apply_answer_standardization(client):
    """Apply answer standardization fixes"""
    print("\n" + "=" * 70)
    print("APPLYING ANSWER STANDARDIZATION...")
    print("=" * 70)

    total_updated = 0

    for current_value, new_value in ANSWER_STANDARDIZATION.items():
        # Skip if no change needed or if it's a value that will be reclassified
        if new_value is None or new_value == current_value:
            continue

        update_query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.{TABLE}`
        SET standardized_answer = @new_value
        WHERE standardized_question = 'age_at_diagnosis'
          AND standardized_answer = @current_value
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("current_value", "STRING", current_value),
                bigquery.ScalarQueryParameter("new_value", "STRING", new_value),
            ]
        )

        result = client.query(update_query, job_config=job_config).result()
        updated = result.num_dml_affected_rows

        if updated > 0:
            total_updated += updated
            print(f"  Updated {updated} rows: '{current_value}' -> '{new_value}'")

    print(f"\nTotal answer standardizations applied: {total_updated}")
    return total_updated


def show_final_state(client):
    """Show the final state after fixes"""
    print("\n" + "=" * 70)
    print("FINAL STATE: age_at_diagnosis")
    print("=" * 70)

    query = f"""
    SELECT
        standardized_answer,
        COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = 'age_at_diagnosis'
    GROUP BY 1
    ORDER BY cnt DESC
    """

    df = client.query(query).to_dataframe()

    print("\nValue                     | Count")
    print("-" * 40)
    for _, row in df.iterrows():
        print(f"{row['standardized_answer']:25} | {row['cnt']:>5}")

    print(f"\nTotal rows: {df['cnt'].sum()}")


def main():
    parser = argparse.ArgumentParser(description='Fix age_at_diagnosis data quality issues')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without applying')
    parser.add_argument('--apply', action='store_true', help='Apply the fixes')
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("Please specify --dry-run or --apply")
        print("  --dry-run: Preview changes without applying")
        print("  --apply:   Apply the fixes to BigQuery")
        return

    client = bigquery.Client(project=PROJECT_ID)

    print("=" * 70)
    print("LIMESURVEY: FIX age_at_diagnosis DATA QUALITY")
    print("=" * 70)
    print(f"Mode: {'DRY RUN (preview only)' if args.dry_run else 'APPLY CHANGES'}")

    # Preview fixes
    q_affected = preview_question_fixes(client)
    a_affected = preview_answer_standardization(client)

    if args.apply:
        print("\n" + "=" * 70)
        print("APPLYING CHANGES...")
        print("=" * 70)

        # Apply question fixes first (before answer standardization)
        apply_question_fixes(client)

        # Then apply answer standardization
        apply_answer_standardization(client)

        # Show final state
        show_final_state(client)

        print("\n" + "=" * 70)
        print("DONE! Remember to retrain the ML model:")
        print("  python shared/limesurvey_ml_mismatch_detector.py --retrain")
        print("=" * 70)
    else:
        print("\n" + "=" * 70)
        print("DRY RUN COMPLETE - No changes made")
        print("Run with --apply to apply these changes")
        print("=" * 70)


if __name__ == '__main__':
    main()
