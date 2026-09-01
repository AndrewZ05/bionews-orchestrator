#!/usr/bin/env python3
"""Deeper investigation of AO04 issue."""

import sys
from pathlib import Path
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
load_dotenv(Path(__file__).parent.parent / '.env')
from google.cloud import bigquery

client = bigquery.Client(project='bi-data-391216')

print('='*80)
print('DEEPER INVESTIGATION OF AO04')
print('='*80)

# 1. Check the staging table (lime_surveys_columnar) - before NLP processing
print('\n1. CHECK STAGING TABLE (lime_surveys_columnar):')
query = '''
SELECT
    survey_id,
    raw_question_en,
    raw_answer_en,
    standardized_question,
    standardized_answer,
    COUNT(*) as cnt
FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar`
WHERE survey_id = 598765
  AND (standardized_answer = 'AO04' OR raw_answer_en = '6-9')
GROUP BY 1, 2, 3, 4, 5
ORDER BY cnt DESC
LIMIT 10
'''
for row in client.query(query):
    print(f'  raw_question: {row.raw_question_en[:60] if row.raw_question_en else "N/A"}...')
    print(f'  raw_answer_en: {row.raw_answer_en}')
    print(f'  standardized_question: {row.standardized_question}')
    print(f'  standardized_answer: {row.standardized_answer}')
    print(f'  count: {row.cnt}')
    print()

# 2. Check lime_answers for the specific question
print('\n2. LIME_ANSWERS FOR SURVEY 598765 MEDICATION COUNT:')
query = '''
SELECT q.qid, q.title, q.question, a.code, a.answer, a.sortorder
FROM `bi-data-391216.limesurvey_data.lime_questions` q
LEFT JOIN `bi-data-391216.limesurvey_data.lime_answers` a ON q.qid = a.qid
WHERE q.sid = 598765
  AND (LOWER(q.question) LIKE '%medication%' OR LOWER(q.title) LIKE '%medication%')
ORDER BY a.sortorder
'''
for row in client.query(query):
    print(f'  qid={row.qid}, title={row.title}')
    print(f'    question: {row.question[:60] if row.question else "N/A"}...')
    print(f'    code={row.code}, answer={row.answer}')

# 3. Check raw survey data for the question code
print('\n3. CHECK WHAT COLUMN HOLDS THE MEDICATION COUNT DATA:')
query = '''
SELECT column_name
FROM `bi-data-391216.limesurvey_data.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'lime_survey_598765'
  AND LOWER(column_name) LIKE '%medication%'
'''
for row in client.query(query):
    print(f'  {row.column_name}')

# 4. Sample raw data from the survey
print('\n4. SAMPLE RAW DATA FROM SURVEY 598765:')
query = '''
SELECT *
FROM `bi-data-391216.limesurvey_data.lime_survey_598765`
LIMIT 1
'''
results = list(client.query(query))
if results:
    row = results[0]
    for key in row.keys():
        if 'medication' in key.lower() or 'ao0' in str(row[key]).lower() or '6-9' in str(row[key]):
            print(f'  {key}: {row[key]}')

# 5. Check all current_medication_count values
print('\n5. ALL CURRENT_MEDICATION_COUNT VALUES:')
query = '''
SELECT standardized_answer, COUNT(*) as cnt
FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
WHERE standardized_question = 'current_medication_count'
GROUP BY 1
ORDER BY cnt DESC
'''
for row in client.query(query):
    print(f'  {row.standardized_answer}: {row.cnt}')

print('\n' + '='*80)
