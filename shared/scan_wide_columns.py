#!/usr/bin/env python3
"""Comprehensive scan of all columns in lime_surveys_wide for data inconsistencies."""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
load_dotenv(Path(__file__).parent.parent / '.env')
from google.cloud import bigquery

client = bigquery.Client(project='bi-data-391216')

print('='*100)
print('COMPREHENSIVE WIDE TABLE COLUMN SCAN')
print('='*100)

# First, get all column names from the wide table
schema_query = """
SELECT column_name
FROM `bi-data-391216.limesurvey_data.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'lime_surveys_wide'
ORDER BY ordinal_position
"""
columns = [row.column_name for row in client.query(schema_query)]
print(f"\nTotal columns in wide table: {len(columns)}")

# Skip metadata columns
skip_columns = ['response_id', 'survey_id', 'submitdate', 'startdate', 'datestamp',
                'startlanguage', 'seed', 'lastpage', 'token', 'ipaddr', 'refurl']

data_columns = [c for c in columns if c not in skip_columns]
print(f"Data columns to scan: {len(data_columns)}")

issues_found = []

for col in data_columns:
    # Get distinct values and counts for each column
    query = f"""
    SELECT `{col}` as val, COUNT(*) as cnt
    FROM `bi-data-391216.limesurvey_data.lime_surveys_wide`
    WHERE `{col}` IS NOT NULL AND `{col}` != ''
    GROUP BY 1
    ORDER BY cnt DESC
    """
    try:
        results = list(client.query(query))
        if not results:
            continue

        # Check for various issues
        col_issues = []
        values = [(r.val, r.cnt) for r in results]

        for val, cnt in values:
            if val is None:
                continue
            val_str = str(val)

            # Check for underscore patterns (word_word)
            if '_' in val_str and not val_str.startswith('http') and '@' not in val_str:
                import re
                if re.search(r'[a-z]_[a-z]', val_str.lower()):
                    col_issues.append(('underscore', val_str, cnt))

            # Check for HTML entities
            if '&gt;' in val_str or '&lt;' in val_str or '&amp;' in val_str or '&nbsp;' in val_str:
                col_issues.append(('html_entity', val_str, cnt))

            # Check for encoding issues
            if 'â€' in val_str or 'Ã' in val_str or 'Â' in val_str:
                col_issues.append(('encoding', val_str, cnt))

            # Check for all lowercase (should be title case) for specific column types
            if col in ['site_name', 'condition', 'gender', 'ethnicity']:
                if val_str.islower() and len(val_str) > 3:
                    col_issues.append(('lowercase', val_str, cnt))

        # Check for similar values (potential duplicates)
        val_lower_map = {}
        for val, cnt in values:
            if val is None:
                continue
            val_lower = str(val).lower().strip()
            if val_lower in val_lower_map:
                val_lower_map[val_lower].append((str(val), cnt))
            else:
                val_lower_map[val_lower] = [(str(val), cnt)]

        for key, val_list in val_lower_map.items():
            if len(val_list) > 1:
                # Multiple values that differ only by case
                col_issues.append(('case_duplicate', val_list, sum(c for v, c in val_list)))

        if col_issues:
            issues_found.append((col, col_issues))

    except Exception as e:
        print(f"  Error scanning {col}: {e}")

# Print results
print('\n' + '='*100)
print('ISSUES FOUND BY COLUMN')
print('='*100)

if not issues_found:
    print("\nNo issues found!")
else:
    for col, issues in issues_found:
        print(f"\n[{col}]")
        for issue_type, val, cnt in issues:
            if issue_type == 'case_duplicate':
                # val is a list of (value, count) tuples
                vals_str = ', '.join([f"'{v}' ({c})" for v, c in val])
                print(f"  CASE DUPLICATE: {vals_str}")
            else:
                safe_val = str(val)[:60].encode('ascii', 'replace').decode()
                print(f"  {issue_type.upper()}: '{safe_val}' ({cnt} rows)")

print('\n' + '='*100)
print('SCAN COMPLETE')
print('='*100)
