#!/usr/bin/env python3
"""
LimeSurvey Fix: Multi-Select 'Yes' Answers

For multi-select checkbox questions, LimeSurvey stores:
- raw_answer_en = 'Y' (meaning "selected")
- subquestion_id = '001', '002', etc. (the checkbox option ID)

The actual checkbox option text is in lime_questions (with parent_qid linking subquestions).

This script replaces 'Yes' standardized_answers with the actual subquestion text,
which is much more meaningful for analysis.

BEFORE: standardized_answer = 'Yes' (for checkbox selected)
AFTER:  standardized_answer = 'Reducing inflammation' (the actual option text)

Note: Answers stay human-readable (not snake_case). Only questions use snake_case.

Run with --dry-run to preview changes without applying them.

Usage:
    python shared/limesurvey_fix_multiselect_answers.py --dry-run
    python shared/limesurvey_fix_multiselect_answers.py --apply
"""

import os
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
    for _p in (r'c:\gcp\service-account-bionews-pipeline.json', '/home/orchestrator/service-account-bionews-pipeline.json'):
        if os.path.exists(_p):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _p
            break

import re
import argparse
from google.cloud import bigquery

PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'
TABLE = 'lime_surveys_columnar_completed'


def clean_answer_text(s):
    """Clean up answer text (keep human-readable, don't convert to snake_case)"""
    if not s:
        return s

    # Remove LimeSurvey conditional expressions {if(site == "...", "...", ...)}
    # Extract just the main text before the conditional
    if '{if(' in s or '{if (' in s:
        # Find the start of the conditional expression
        match = re.search(r'\{if\s*\(', s)
        if match:
            # Keep everything before the {if(
            main_text = s[:match.start()].strip()
            # Clean up trailing punctuation like "(e.g., " or ", "
            main_text = re.sub(r'\s*\(e\.g\.,?\s*$', '', main_text)
            main_text = re.sub(r',\s*$', '', main_text)
            main_text = re.sub(r'\(\s*$', '', main_text)
            if main_text:
                s = main_text.strip()

    # Also handle {site} placeholders - replace with "condition"
    s = re.sub(r'\{site\}', 'condition', s, flags=re.IGNORECASE)

    # Fix common encoding issues
    s = s.replace('Â', '').replace('â€™', "'").replace('â€"', '-').replace('â€œ', '"').replace('â€', '"')

    # Normalize whitespace
    s = re.sub(r'\s+', ' ', s.strip())

    return s


def analyze_yes_answers(client):
    """Analyze how many 'Yes' answers exist and their distribution"""
    query = f"""
    SELECT
        standardized_question,
        COUNT(*) as yes_count,
        COUNT(DISTINCT subquestion_id) as unique_subqs
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_answer = 'Yes'
      AND subquestion_id IS NOT NULL
      AND subquestion_id != ''
    GROUP BY 1
    ORDER BY yes_count DESC
    """

    print("=" * 70)
    print("ANALYSIS: 'Yes' answers with subquestion_id")
    print("=" * 70)

    df = client.query(query).to_dataframe()
    total = df['yes_count'].sum()

    print(f"\nCategories with 'Yes' answers (total: {total:,} rows):\n")
    print(f"{'Category':<45} | {'Yes Count':>10} | SubQs")
    print("-" * 70)

    for _, row in df.iterrows():
        print(f"{row['standardized_question']:<45} | {row['yes_count']:>10,} | {row['unique_subqs']}")

    return total


def preview_changes(client, limit=20):
    """Preview what the changes would look like"""

    # Get subquestion text mapping
    query = f"""
    WITH subquestion_mapping AS (
        SELECT DISTINCT
            c.survey_id,
            c.question_id,
            c.subquestion_id,
            -- Join to get subquestion text
            COALESCE(l10n.question, q.question) as subquestion_text
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}` c
        INNER JOIN `{PROJECT_ID}.{DATASET}.lime_questions` q
            ON c.survey_id = q.sid
            AND q.parent_qid = c.question_id
            AND q.title = CONCAT('SQ', c.subquestion_id)
        LEFT JOIN `{PROJECT_ID}.{DATASET}.lime_question_l10ns` l10n
            ON q.qid = l10n.qid AND l10n.language = 'en'
        WHERE c.standardized_answer = 'Yes'
          AND c.subquestion_id IS NOT NULL
          AND c.subquestion_id != ''
    )
    SELECT
        c.standardized_question,
        c.raw_question_en,
        c.standardized_answer as current_answer,
        m.subquestion_text as new_answer,
        COUNT(*) as affected_rows
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}` c
    INNER JOIN subquestion_mapping m
        ON c.survey_id = m.survey_id
        AND c.question_id = m.question_id
        AND c.subquestion_id = m.subquestion_id
    WHERE c.standardized_answer = 'Yes'
    GROUP BY 1, 2, 3, 4
    ORDER BY affected_rows DESC
    LIMIT {limit}
    """

    print("\n" + "=" * 70)
    print("PREVIEW: Sample transformations (Yes -> actual subquestion text)")
    print("=" * 70)

    df = client.query(query).to_dataframe()

    for _, row in df.iterrows():
        cleaned = clean_answer_text(row['new_answer'])
        q = row['raw_question_en'][:50] + '...' if len(row['raw_question_en']) > 50 else row['raw_question_en']
        print(f"\n[{row['standardized_question']}] {q}")
        print(f"  BEFORE: {row['current_answer']}")
        print(f"  AFTER:  {cleaned} ({row['affected_rows']:,} rows)")

    return len(df)


def apply_fixes(client, batch_size=1000):
    """Apply the fixes using UPDATE with CTE"""

    print("\n" + "=" * 70)
    print("APPLYING FIXES...")
    print("=" * 70)

    # First, count the mappings we'll use
    count_query = f"""
    SELECT COUNT(DISTINCT CONCAT(c.survey_id, ':', c.question_id, ':', c.subquestion_id)) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}` c
    INNER JOIN `{PROJECT_ID}.{DATASET}.lime_questions` q
        ON c.survey_id = q.sid
        AND q.parent_qid = c.question_id
        AND q.title = CONCAT('SQ', c.subquestion_id)
    WHERE c.standardized_answer = 'Yes'
      AND c.subquestion_id IS NOT NULL
      AND c.subquestion_id != ''
    """

    print("Counting subquestion mappings...")
    count_result = list(client.query(count_query).result())[0]
    print(f"Found {count_result.cnt:,} unique subquestion mappings")

    # Update using MERGE statement (BigQuery's way to do UPDATE with JOIN)
    update_query = f"""
    MERGE `{PROJECT_ID}.{DATASET}.{TABLE}` t
    USING (
        SELECT DISTINCT
            c.survey_id,
            c.question_id,
            c.subquestion_id,
            -- Clean up text: remove conditionals, fix encoding, normalize whitespace
            REGEXP_REPLACE(
                TRIM(
                    REPLACE(
                        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                            CASE
                                WHEN REGEXP_CONTAINS(COALESCE(l10n.question, q.question), r'\\{{if\\s*\\(')
                                THEN REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_EXTRACT(COALESCE(l10n.question, q.question), r'^(.*?)\\{{if\\s*\\('),
                                            r'\\s*\\(e\\.g\\.,?\\s*$', ''),
                                        r',\\s*$', ''),
                                    r'\\(\\s*$', '')
                                ELSE COALESCE(l10n.question, q.question)
                            END,
                            'Â', ''),
                            'â€™', "'"),
                            'â€"', '-'),
                            'â€œ', '"'),
                            'â€', '"'),
                        '{{site}}', 'condition')
                ),
                r'\\s+', ' ') as cleaned_text
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}` c
        INNER JOIN `{PROJECT_ID}.{DATASET}.lime_questions` q
            ON c.survey_id = q.sid
            AND q.parent_qid = c.question_id
            AND q.title = CONCAT('SQ', c.subquestion_id)
        LEFT JOIN `{PROJECT_ID}.{DATASET}.lime_question_l10ns` l10n
            ON q.qid = l10n.qid AND l10n.language = 'en'
        WHERE c.standardized_answer = 'Yes'
          AND c.subquestion_id IS NOT NULL
          AND c.subquestion_id != ''
    ) m
    ON t.survey_id = m.survey_id
       AND t.question_id = m.question_id
       AND t.subquestion_id = m.subquestion_id
       AND t.standardized_answer = 'Yes'
    WHEN MATCHED THEN
        UPDATE SET standardized_answer = m.cleaned_text
    """

    print("Applying updates...")
    result = client.query(update_query).result()
    updated = result.num_dml_affected_rows

    print(f"\n[OK] Updated {updated:,} rows")
    return updated


def show_final_state(client):
    """Show the state after fixes"""

    print("\n" + "=" * 70)
    print("VERIFICATION: Remaining 'Yes' answers")
    print("=" * 70)

    query = f"""
    SELECT
        standardized_question,
        COUNT(*) as yes_count
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_answer = 'Yes'
    GROUP BY 1
    ORDER BY yes_count DESC
    LIMIT 20
    """

    df = client.query(query).to_dataframe()
    total = df['yes_count'].sum()

    if total > 0:
        print(f"\nRemaining 'Yes' answers ({total:,} total):")
        for _, row in df.iterrows():
            print(f"  {row['standardized_question']:<45} | {row['yes_count']:>10,}")
    else:
        print("\nNo 'Yes' answers remaining!")


def main():
    parser = argparse.ArgumentParser(description='Fix multi-select Yes answers')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without applying')
    parser.add_argument('--apply', action='store_true', help='Apply the fixes')
    parser.add_argument('--preview-limit', type=int, default=20, help='Number of samples in preview')
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("Please specify --dry-run or --apply")
        print("  --dry-run: Preview changes without applying")
        print("  --apply:   Apply the fixes to BigQuery")
        return

    client = bigquery.Client(project=PROJECT_ID)

    print("=" * 70)
    print("LIMESURVEY: FIX MULTI-SELECT 'Yes' ANSWERS")
    print("=" * 70)
    print(f"Mode: {'DRY RUN (preview only)' if args.dry_run else 'APPLY CHANGES'}")

    # Analysis
    total = analyze_yes_answers(client)

    # Preview
    preview_changes(client, args.preview_limit)

    if args.apply:
        updated = apply_fixes(client)
        show_final_state(client)

        print("\n" + "=" * 70)
        print("DONE! Remember to retrain the ML model:")
        print("  python shared/limesurvey_ml_mismatch_detector.py --retrain")
        print("=" * 70)
    else:
        print("\n" + "=" * 70)
        print("DRY RUN COMPLETE - No changes made")
        print(f"Would fix approximately {total:,} 'Yes' values")
        print("Run with --apply to apply these changes")
        print("=" * 70)


if __name__ == '__main__':
    main()
