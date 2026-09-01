#!/usr/bin/env python3
"""
LimeSurvey Fix: Data Quality Issues Across Multiple Categories

This script fixes issues found during data quality analysis:

1. QUESTION MISCLASSIFICATIONS:
   - treatment_satisfaction → treatment_dissatisfaction_reasons (4,397 rows)
   - hcp_physician_visits → hcp_visit_frequency (268 rows)

2. ANSWER STANDARDIZATION:
   - age: Normalize to consistent snake_case format
   - time_since_diagnosis: Normalize to consistent snake_case format

Run with --dry-run to preview changes without applying them.

Usage:
    python shared/limesurvey_fix_data_quality.py --dry-run
    python shared/limesurvey_fix_data_quality.py --apply
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
# FIX 1: QUESTION MISCLASSIFICATIONS
# =============================================================================

QUESTION_FIXES = [
    {
        'raw_question_pattern': '%why%not%satisfied%',
        'current_std_question': 'treatment_satisfaction',
        'correct_std_question': 'treatment_dissatisfaction_reasons',
        'reason': 'Questions asking WHY users are not satisfied should be separate from satisfaction ratings'
    },
    {
        'raw_question_pattern': '%how often%',
        'current_std_question': 'hcp_physician_visits',
        'correct_std_question': 'hcp_visit_frequency',
        'reason': 'Frequency questions should be separate from count questions'
    },
    {
        'raw_question_pattern': '%how frequently%',
        'current_std_question': 'hcp_physician_visits',
        'correct_std_question': 'hcp_visit_frequency',
        'reason': 'Frequency questions should be separate from count questions'
    },
]

# =============================================================================
# FIX 2: ANSWER STANDARDIZATION FOR 'age'
# =============================================================================

AGE_STANDARDIZATION = {
    # Format: current_value -> standardized_value (snake_case)

    # Standard ranges - Title Case with "to"
    '65 Plus': 'age_65_plus',
    '55 to 64': 'age_55_64',
    '45 to 54': 'age_45_54',
    '35 to 44': 'age_35_44',
    '25 to 34': 'age_25_34',
    '18 to 24': 'age_18_24',

    # Plus formats
    '55 Plus': 'age_55_plus',
    '18 Plus': 'age_18_plus',
    '65+': 'age_65_plus',

    # Under formats
    'Under 18': 'under_18',
    'Under 5': 'under_5',

    # Child age ranges
    '5 to 12': 'age_5_12',
    '13 to 17': 'age_13_17',

    # Dash formats (already found in data)
    '55-64': 'age_55_64',
    '45-54': 'age_45_54',
    '35-44': 'age_35_44',
    '25-34': 'age_25_34',
    '18-24': 'age_18_24',

    # Other
    'Prefer Not To Say': 'prefer_not_to_say',
}

# =============================================================================
# FIX 3: ANSWER STANDARDIZATION FOR 'time_since_diagnosis'
# =============================================================================

TIME_SINCE_DIAGNOSIS_STANDARDIZATION = {
    # Format: current_value -> standardized_value

    # "to" format
    '3 to 5 Years': 'years_3_5',
    '1 to 2 Years': 'years_1_2',
    '6 to 10 Years': 'years_6_10',

    # "Less/More than" format
    'Less Than 1 Year': 'less_than_1_year',
    'More Than 10 Years': 'more_than_10_years',
    'More than 20 years': 'more_than_20_years',

    # Dash format
    '11-15 years': 'years_11_15',
    '16-20 years': 'years_16_20',
    '6-9 years': 'years_6_9',
    '2-5 years': 'years_2_5',
    '5-10 years': 'years_5_10',
    '16-19 years': 'years_16_19',
    '3-5 years': 'years_3_5',

    # Plus formats
    '20+ years': 'more_than_20_years',

    # Special cases
    'I am still seeking a diagnosis': 'seeking_diagnosis',
}


def get_current_state(client):
    """Get current state of affected categories"""
    query = f"""
    SELECT
        standardized_question,
        COUNT(*) as row_count,
        COUNT(DISTINCT standardized_answer) as unique_answers
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question IN (
        'treatment_satisfaction', 'treatment_dissatisfaction_reasons',
        'hcp_physician_visits', 'hcp_visit_frequency',
        'age', 'time_since_diagnosis'
    )
    GROUP BY 1
    ORDER BY 1
    """
    return client.query(query).to_dataframe()


def preview_question_fixes(client):
    """Preview which rows would be affected by question reclassification"""
    print("\n" + "=" * 70)
    print("FIX 1: QUESTION MISCLASSIFICATIONS")
    print("=" * 70)

    total_affected = 0

    for fix in QUESTION_FIXES:
        query = f"""
        SELECT COUNT(*) as cnt
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE LOWER(raw_question_en) LIKE @pattern
          AND standardized_question = @current_std_question
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("pattern", "STRING", fix['raw_question_pattern']),
                bigquery.ScalarQueryParameter("current_std_question", "STRING", fix['current_std_question']),
            ]
        )
        result = list(client.query(query, job_config=job_config))[0]
        count = result.cnt
        total_affected += count

        print(f"\nPattern: {fix['raw_question_pattern']}")
        print(f"  Current:  standardized_question = '{fix['current_std_question']}'")
        print(f"  Change to: standardized_question = '{fix['correct_std_question']}'")
        print(f"  Reason:   {fix['reason']}")
        print(f"  Rows affected: {count}")

    print(f"\nTotal rows to reclassify: {total_affected}")
    return total_affected


def preview_answer_standardization(client, category, mapping, category_name):
    """Preview which rows would be affected by answer standardization"""
    print(f"\n" + "=" * 70)
    print(f"FIX: {category_name.upper()} ANSWER STANDARDIZATION")
    print("=" * 70)

    query = f"""
    SELECT
        standardized_answer,
        COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = @category
    GROUP BY 1
    ORDER BY cnt DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("category", "STRING", category),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()

    print(f"\nCurrent Value                    -> New Value                | Rows")
    print("-" * 75)

    total_to_change = 0
    for _, row in df.iterrows():
        current = row['standardized_answer']
        count = row['cnt']

        if current is None:
            continue

        new_value = mapping.get(current)

        if new_value is None:
            status = "(no mapping defined)"
        elif new_value == current:
            status = "(no change)"
        else:
            status = f"-> {new_value}"
            total_to_change += count

        print(f"{str(current)[:32]:32} {status[:25]:25} | {count:>5}")

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
        WHERE LOWER(raw_question_en) LIKE @pattern
          AND standardized_question = @current_std_question
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("pattern", "STRING", fix['raw_question_pattern']),
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


def apply_answer_standardization(client, category, mapping, category_name):
    """Apply answer standardization fixes"""
    print(f"\n" + "=" * 70)
    print(f"APPLYING {category_name.upper()} ANSWER STANDARDIZATION...")
    print("=" * 70)

    total_updated = 0

    for current_value, new_value in mapping.items():
        # Skip if no change needed
        if new_value is None or new_value == current_value:
            continue

        update_query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.{TABLE}`
        SET standardized_answer = @new_value
        WHERE standardized_question = @category
          AND standardized_answer = @current_value
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("category", "STRING", category),
                bigquery.ScalarQueryParameter("current_value", "STRING", current_value),
                bigquery.ScalarQueryParameter("new_value", "STRING", new_value),
            ]
        )

        result = client.query(update_query, job_config=job_config).result()
        updated = result.num_dml_affected_rows

        if updated > 0:
            total_updated += updated
            print(f"  Updated {updated} rows: '{current_value}' -> '{new_value}'")

    print(f"\nTotal {category_name} standardizations applied: {total_updated}")
    return total_updated


def show_final_state(client, category):
    """Show the final state after fixes"""
    print(f"\n--- Final state: {category} ---")

    query = f"""
    SELECT
        standardized_answer,
        COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question = @category
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT 20
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("category", "STRING", category),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()

    print(f"\nValue                             | Count")
    print("-" * 45)
    for _, row in df.iterrows():
        ans = str(row['standardized_answer'])[:33]
        print(f"{ans:33} | {row['cnt']:>5}")


def main():
    parser = argparse.ArgumentParser(description='Fix data quality issues across multiple categories')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without applying')
    parser.add_argument('--apply', action='store_true', help='Apply the fixes')
    parser.add_argument('--questions-only', action='store_true', help='Only fix question misclassifications')
    parser.add_argument('--answers-only', action='store_true', help='Only fix answer standardization')
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("Please specify --dry-run or --apply")
        print("  --dry-run: Preview changes without applying")
        print("  --apply:   Apply the fixes to BigQuery")
        return

    client = bigquery.Client(project=PROJECT_ID)

    print("=" * 70)
    print("LIMESURVEY: FIX DATA QUALITY ISSUES")
    print("=" * 70)
    print(f"Mode: {'DRY RUN (preview only)' if args.dry_run else 'APPLY CHANGES'}")

    # Show current state
    print("\n--- Current State ---")
    df = get_current_state(client)
    for _, row in df.iterrows():
        print(f"  {row['standardized_question']}: {row['row_count']:,} rows, {row['unique_answers']} unique answers")

    # Preview all fixes
    if not args.answers_only:
        q_affected = preview_question_fixes(client)
    else:
        q_affected = 0

    if not args.questions_only:
        age_affected = preview_answer_standardization(client, 'age', AGE_STANDARDIZATION, 'age')
        time_affected = preview_answer_standardization(client, 'time_since_diagnosis',
                                                       TIME_SINCE_DIAGNOSIS_STANDARDIZATION, 'time_since_diagnosis')
    else:
        age_affected = 0
        time_affected = 0

    if args.apply:
        print("\n" + "=" * 70)
        print("APPLYING CHANGES...")
        print("=" * 70)

        if not args.answers_only:
            apply_question_fixes(client)

        if not args.questions_only:
            apply_answer_standardization(client, 'age', AGE_STANDARDIZATION, 'age')
            apply_answer_standardization(client, 'time_since_diagnosis',
                                        TIME_SINCE_DIAGNOSIS_STANDARDIZATION, 'time_since_diagnosis')

        # Show final state
        print("\n" + "=" * 70)
        print("FINAL STATE")
        print("=" * 70)

        df = get_current_state(client)
        for _, row in df.iterrows():
            print(f"  {row['standardized_question']}: {row['row_count']:,} rows, {row['unique_answers']} unique answers")

        show_final_state(client, 'age')
        show_final_state(client, 'time_since_diagnosis')

        print("\n" + "=" * 70)
        print("DONE! Remember to retrain the ML model:")
        print("  python shared/limesurvey_ml_mismatch_detector.py --retrain")
        print("=" * 70)
    else:
        print("\n" + "=" * 70)
        print("DRY RUN COMPLETE - No changes made")
        print(f"Summary: {q_affected} question fixes, {age_affected} age fixes, {time_affected} time_since_diagnosis fixes")
        print("Run with --apply to apply these changes")
        print("=" * 70)


if __name__ == '__main__':
    main()
