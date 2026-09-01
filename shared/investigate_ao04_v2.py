#!/usr/bin/env python3
"""Investigation of AO04 issue - v2."""

import sys
from pathlib import Path
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
load_dotenv(Path(__file__).parent.parent / '.env')
from google.cloud import bigquery

client = bigquery.Client(project='bi-data-391216')

print('='*80)
print('INVESTIGATION OF AO04')
print('='*80)

# 1. Check staging table schema
print('\n1. STAGING TABLE SCHEMA (lime_surveys_columnar):')
query = '''
SELECT column_name, data_type
FROM `bi-data-391216.limesurvey_data.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'lime_surveys_columnar'
ORDER BY ordinal_position
'''
for row in client.query(query):
    print(f'  {row.column_name}: {row.data_type}')

# 2. Check all current_medication_count values in completed table
print('\n2. ALL CURRENT_MEDICATION_COUNT VALUES (completed):')
query = '''
SELECT standardized_answer, COUNT(*) as cnt
FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
WHERE standardized_question = 'current_medication_count'
GROUP BY 1
ORDER BY cnt DESC
'''
for row in client.query(query):
    print(f'  {row.standardized_answer}: {row.cnt}')

# 3. Check what's in the lime_answers for the specific survey/question
print('\n3. LIME_ANSWERS FOR SURVEY 598765:')
query = '''
SELECT q.qid, q.title, q.question, a.code, a.answer, a.sortorder
FROM `bi-data-391216.limesurvey_data.lime_questions` q
LEFT JOIN `bi-data-391216.limesurvey_data.lime_answers` a ON q.qid = a.qid
WHERE q.sid = 598765
ORDER BY q.qid, a.sortorder
LIMIT 50
'''
current_qid = None
for row in client.query(query):
    if row.qid != current_qid:
        print(f'\n  QID {row.qid}: {row.title}')
        print(f'    Q: {row.question[:70] if row.question else "N/A"}...')
        current_qid = row.qid
    if row.code:
        print(f'    {row.code}: {row.answer}')

# 4. Check raw answer column in completed table
print('\n\n4. RAW ANSWER FOR AO04 ROWS:')
query = '''
SELECT raw_answer_en, standardized_answer, COUNT(*) as cnt
FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
WHERE standardized_question = 'current_medication_count'
GROUP BY 1, 2
ORDER BY cnt DESC
'''
for row in client.query(query):
    print(f'  raw_answer_en={row.raw_answer_en}, standardized={row.standardized_answer}, cnt={row.cnt}')

print('\n' + '='*80)
