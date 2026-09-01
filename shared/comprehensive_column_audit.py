#!/usr/bin/env python3
"""
Comprehensive audit of ALL columns in lime_surveys_wide table.
Identifies every data quality issue across all 151+ standardized questions.

Output: A complete list of all required fixes (not piecemeal).
"""

import os
import sys
from pathlib import Path
from collections import defaultdict

# Fix encoding for Windows
sys.stdout.reconfigure(encoding='utf-8')

# Suppress gRPC warnings
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GLOG_minloglevel'] = '3'

from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'
TABLE = 'lime_surveys_columnar_completed'


def main():
    client = bigquery.Client(project=PROJECT_ID)

    print("=" * 100)
    print("COMPREHENSIVE WIDE TABLE COLUMN AUDIT")
    print("=" * 100)
    print()

    # Get all standardized questions and their answer distributions
    query = f"""
    WITH answer_stats AS (
        SELECT
            standardized_question,
            standardized_answer,
            COUNT(*) as cnt
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE standardized_question IS NOT NULL
          AND standardized_answer IS NOT NULL
        GROUP BY 1, 2
    )
    SELECT
        standardized_question,
        standardized_answer,
        cnt
    FROM answer_stats
    ORDER BY standardized_question, cnt DESC
    """

    print("Fetching all answer distributions...")
    results = list(client.query(query))
    print(f"Retrieved {len(results):,} distinct (question, answer) pairs")
    print()

    # Organize by question
    questions = defaultdict(list)
    for row in results:
        questions[row.standardized_question].append({
            'answer': row.standardized_answer,
            'count': row.cnt
        })

    print(f"Total standardized questions: {len(questions)}")
    print()

    # Issue tracking
    issues = {
        'placeholder_values': [],      # {site}, {condition}, {if(...)}
        'oth_codes': [],                # -oth-
        'lowercase_scales': [],         # agree, satisfied, likely, etc.
        'inconsistent_casing': [],      # Same value with different casing
        'bad_concatenation': [],        # Values with > or >> in them
        'undecoded_ao_codes': [],       # AO01, AO02, etc.
        'numeric_formatting': [],       # 8.0000000000 should be 8
        'html_remnants': [],            # &nbsp;, <br>, etc.
        'leading_trailing_spaces': [],  # " Yes" vs "Yes"
        'empty_or_null': [],            # Empty strings, literal "null"
        'missing_title_case': [],       # lowercase single words
        'special_chars': [],            # Unusual characters
    }

    # Scale words that should be title case
    scale_words = {
        # Agreement
        'agree', 'disagree', 'strongly agree', 'strongly disagree', 'neutral',
        # Satisfaction
        'satisfied', 'dissatisfied', 'very satisfied', 'very dissatisfied',
        # Likelihood
        'likely', 'unlikely', 'very likely', 'very unlikely', 'not at all likely',
        # Quality
        'excellent', 'good', 'fair', 'poor', 'very poor',
        # Interest
        'interested', 'very interested', 'not interested', 'somewhat interested',
        # Knowledge
        'knowledgeable', 'very knowledgeable', 'somewhat knowledgeable', 'not knowledgeable',
        # Frequency
        'always', 'often', 'sometimes', 'rarely', 'never',
        'daily', 'weekly', 'monthly', 'yearly', 'annually',
        # Severity
        'none', 'mild', 'moderate', 'severe', 'extreme',
        # General
        'yes', 'no', 'other', 'unknown', 'prefer not to answer',
        'male', 'female', 'patient', 'caregiver',
    }

    # Analyze each question
    for q_name, answers in sorted(questions.items()):
        answer_values = {a['answer'] for a in answers}
        answer_lower_map = defaultdict(list)  # lowercase -> [original values]

        for a in answers:
            val = a['answer']
            cnt = a['count']
            val_lower = val.lower().strip()
            answer_lower_map[val_lower].append({'value': val, 'count': cnt})

            # Check for placeholder values
            if '{' in val and '}' in val:
                issues['placeholder_values'].append({
                    'question': q_name,
                    'answer': val,
                    'count': cnt
                })

            # Check for -oth- codes
            if val == '-oth-':
                issues['oth_codes'].append({
                    'question': q_name,
                    'answer': val,
                    'count': cnt
                })

            # Check for lowercase scale words
            if val_lower in scale_words and val != val.title() and val.lower() == val:
                issues['lowercase_scales'].append({
                    'question': q_name,
                    'answer': val,
                    'expected': val.title(),
                    'count': cnt
                })

            # Check for bad concatenation (> or >>)
            if '>' in val and q_name not in ('email', 'url', 'refurl'):
                issues['bad_concatenation'].append({
                    'question': q_name,
                    'answer': val,
                    'count': cnt
                })

            # Check for undecoded AO codes (AO01, AO02, etc.)
            if val.startswith('AO') and len(val) >= 4 and val[2:4].isdigit():
                issues['undecoded_ao_codes'].append({
                    'question': q_name,
                    'answer': val,
                    'count': cnt
                })

            # Check for numeric formatting issues
            if '.' in val and val.replace('.', '').replace('-', '').isdigit():
                try:
                    num = float(val)
                    if num == int(num) and '.0' in val:
                        issues['numeric_formatting'].append({
                            'question': q_name,
                            'answer': val,
                            'expected': str(int(num)),
                            'count': cnt
                        })
                except ValueError:
                    pass

            # Check for HTML remnants
            if '&nbsp;' in val or '<br' in val.lower() or '&amp;' in val:
                issues['html_remnants'].append({
                    'question': q_name,
                    'answer': val,
                    'count': cnt
                })

            # Check for leading/trailing spaces
            if val != val.strip():
                issues['leading_trailing_spaces'].append({
                    'question': q_name,
                    'answer': repr(val),
                    'count': cnt
                })

            # Check for empty or null-like values
            if val.strip() == '' or val.lower() in ('null', 'none', 'n/a', 'na'):
                if val.lower() not in ('none',):  # 'None' is valid for severity scale
                    issues['empty_or_null'].append({
                        'question': q_name,
                        'answer': repr(val),
                        'count': cnt
                    })

            # Check for special characters (excluding normal punctuation)
            special_chars = set(val) - set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,;:!?\'"-_/()&+%@#$')
            if special_chars and q_name not in ('email', 'url', 'refurl', 'pvid'):
                issues['special_chars'].append({
                    'question': q_name,
                    'answer': val,
                    'chars': str(special_chars),
                    'count': cnt
                })

        # Check for inconsistent casing (same answer with different case)
        for lower_val, variants in answer_lower_map.items():
            if len(variants) > 1:
                issues['inconsistent_casing'].append({
                    'question': q_name,
                    'variants': [(v['value'], v['count']) for v in variants]
                })

    # Print comprehensive report
    print("=" * 100)
    print("ISSUES FOUND")
    print("=" * 100)

    total_issues = 0

    for issue_type, issue_list in issues.items():
        if issue_list:
            print()
            print("-" * 80)
            print(f"{issue_type.upper().replace('_', ' ')}: {len(issue_list)} issues")
            print("-" * 80)

            if issue_type == 'inconsistent_casing':
                for issue in issue_list[:30]:  # Show first 30
                    print(f"  Q: {issue['question']}")
                    for val, cnt in issue['variants']:
                        print(f"     '{val}' ({cnt:,} rows)")
                if len(issue_list) > 30:
                    print(f"  ... and {len(issue_list) - 30} more")
            else:
                for issue in issue_list[:50]:  # Show first 50
                    if 'expected' in issue:
                        print(f"  Q: {issue['question']}: '{issue['answer']}' -> '{issue['expected']}' ({issue['count']:,} rows)")
                    elif 'chars' in issue:
                        print(f"  Q: {issue['question']}: '{issue['answer'][:50]}' [chars: {issue['chars']}] ({issue['count']:,} rows)")
                    else:
                        print(f"  Q: {issue['question']}: '{issue['answer'][:80]}' ({issue['count']:,} rows)")
                if len(issue_list) > 50:
                    print(f"  ... and {len(issue_list) - 50} more")

            total_issues += len(issue_list)

    # Summary
    print()
    print("=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print()

    for issue_type, issue_list in issues.items():
        if issue_list:
            total_rows = sum(i.get('count', 0) for i in issue_list if 'count' in i)
            print(f"  {issue_type.replace('_', ' ').title()}: {len(issue_list)} distinct values, ~{total_rows:,} rows affected")

    print()
    print(f"TOTAL DISTINCT ISSUES: {total_issues}")
    print()

    # Generate fix recommendations
    print("=" * 100)
    print("FIX RECOMMENDATIONS (COMPLETE LIST)")
    print("=" * 100)

    fixes_needed = []

    # 1. Placeholder fixes
    if issues['placeholder_values']:
        fixes_needed.append({
            'category': 'PLACEHOLDER VALUES',
            'description': 'Replace {site}, {condition}, {if(...)} template placeholders',
            'examples': [f"'{i['answer']}' in {i['question']}" for i in issues['placeholder_values'][:5]],
            'fix_type': 'SQL UPDATE with REGEXP_REPLACE',
            'rows': sum(i['count'] for i in issues['placeholder_values'])
        })

    # 2. -oth- codes
    if issues['oth_codes']:
        fixes_needed.append({
            'category': '-OTH- CODES',
            'description': "Replace -oth- codes with 'Other'",
            'examples': [f"{i['question']}" for i in issues['oth_codes'][:5]],
            'fix_type': "SQL UPDATE SET standardized_answer = 'Other'",
            'rows': sum(i['count'] for i in issues['oth_codes'])
        })

    # 3. Lowercase scales
    if issues['lowercase_scales']:
        fixes_needed.append({
            'category': 'LOWERCASE SCALE VALUES',
            'description': 'Convert lowercase scale words to Title Case',
            'examples': [f"'{i['answer']}' -> '{i['expected']}'" for i in issues['lowercase_scales'][:10]],
            'fix_type': 'SQL UPDATE with CASE statements',
            'rows': sum(i['count'] for i in issues['lowercase_scales'])
        })

    # 4. Inconsistent casing
    if issues['inconsistent_casing']:
        fixes_needed.append({
            'category': 'INCONSISTENT CASING',
            'description': 'Standardize casing for same answer values',
            'examples': [f"{i['question']}: {[v[0] for v in i['variants']]}" for i in issues['inconsistent_casing'][:5]],
            'fix_type': 'SQL UPDATE to normalize to consistent case',
            'rows': 'varies'
        })

    # 5. Bad concatenation
    if issues['bad_concatenation']:
        fixes_needed.append({
            'category': 'BAD CONCATENATION',
            'description': "Fix values with improper > concatenation",
            'examples': [f"'{i['answer'][:50]}...'" for i in issues['bad_concatenation'][:5]],
            'fix_type': 'SQL UPDATE with string parsing',
            'rows': sum(i['count'] for i in issues['bad_concatenation'])
        })

    # 6. AO codes
    if issues['undecoded_ao_codes']:
        fixes_needed.append({
            'category': 'UNDECODED AO CODES',
            'description': 'Decode AO01, AO02 codes to actual answer text',
            'examples': [f"'{i['answer']}' in {i['question']}" for i in issues['undecoded_ao_codes'][:5]],
            'fix_type': 'SQL UPDATE with lookup join',
            'rows': sum(i['count'] for i in issues['undecoded_ao_codes'])
        })

    # 7. Numeric formatting
    if issues['numeric_formatting']:
        fixes_needed.append({
            'category': 'NUMERIC FORMATTING',
            'description': 'Clean up float formatting (8.0 -> 8)',
            'examples': [f"'{i['answer']}' -> '{i['expected']}'" for i in issues['numeric_formatting'][:5]],
            'fix_type': 'SQL UPDATE with CAST',
            'rows': sum(i['count'] for i in issues['numeric_formatting'])
        })

    # 8. HTML remnants
    if issues['html_remnants']:
        fixes_needed.append({
            'category': 'HTML REMNANTS',
            'description': 'Remove &nbsp;, <br>, etc.',
            'examples': [f"'{i['answer'][:50]}'" for i in issues['html_remnants'][:5]],
            'fix_type': 'SQL UPDATE with REGEXP_REPLACE',
            'rows': sum(i['count'] for i in issues['html_remnants'])
        })

    # 9. Leading/trailing spaces
    if issues['leading_trailing_spaces']:
        fixes_needed.append({
            'category': 'LEADING/TRAILING SPACES',
            'description': 'Trim whitespace',
            'examples': [f"{i['answer']}" for i in issues['leading_trailing_spaces'][:5]],
            'fix_type': 'SQL UPDATE with TRIM',
            'rows': sum(i['count'] for i in issues['leading_trailing_spaces'])
        })

    # 10. Special chars
    if issues['special_chars']:
        fixes_needed.append({
            'category': 'SPECIAL CHARACTERS',
            'description': 'Review/clean special characters',
            'examples': [f"'{i['answer'][:30]}' has {i['chars']}" for i in issues['special_chars'][:5]],
            'fix_type': 'Manual review + SQL UPDATE',
            'rows': sum(i['count'] for i in issues['special_chars'])
        })

    print()
    for idx, fix in enumerate(fixes_needed, 1):
        print(f"{idx}. {fix['category']}")
        print(f"   Description: {fix['description']}")
        print(f"   Examples: {fix['examples']}")
        print(f"   Fix Type: {fix['fix_type']}")
        print(f"   Rows Affected: {fix['rows']:,}" if isinstance(fix['rows'], int) else f"   Rows Affected: {fix['rows']}")
        print()

    print("=" * 100)
    print("AUDIT COMPLETE")
    print("=" * 100)

    return issues, fixes_needed


if __name__ == '__main__':
    issues, fixes = main()
