#!/usr/bin/env python3
"""Investigate AO04 value in current_medication_count."""

import sys
from pathlib import Path
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
load_dotenv(Path(__file__).parent.parent / '.env')
from google.cloud import bigquery

client = bigquery.Client(project='bi-data-391216')

print('='*80)
print('INVESTIGATING AO04 IN CURRENT_MEDICATION_COUNT')
print('='*80)

# 1. Check the columnar table for raw_answer when standardized is AO04
print('\n1. RAW vs STANDARDIZED for AO04:')
query = '''
SELECT
    survey_id,
    raw_question_en,
    raw_answer_en,
    standardized_question,
    standardized_answer,
    COUNT(*) as cnt
FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
WHERE standardized_question = 'current_medication_count'
  AND standardized_answer = 'AO04'
GROUP BY 1, 2, 3, 4, 5
ORDER BY cnt DESC
LIMIT 10
'''
for row in client.query(query):
    print(f'  Survey {row.survey_id}:')
    print(f'    raw_question: {row.raw_question_en[:80] if row.raw_question_en else "N/A"}...')
    print(f'    raw_answer: {row.raw_answer_en}')
    print(f'    standardized_answer: {row.standardized_answer}')
    print(f'    count: {row.cnt}')

# 2. Check lime_answers for A004 or AO04 codes
print('\n2. CHECKING LIME_ANSWERS FOR A004/AO04:')
query = '''
SELECT qid, code, answer, sortorder
FROM `bi-data-391216.limesurvey_data.lime_answers`
WHERE code IN ('A004', 'AO04', 'A04', 'AO4')
   OR answer LIKE '%AO04%'
   OR answer LIKE '%A004%'
LIMIT 20
'''
results = list(client.query(query))
if results:
    for row in results:
        print(f'  qid={row.qid}, code={row.code}, answer={row.answer}, sort={row.sortorder}')
else:
    print('  No matches found for A004/AO04 codes')

# 3. Check what survey has current_medication_count with AO04
print('\n3. WHICH SURVEYS HAVE THIS VALUE:')
query = '''
SELECT
    survey_id,
    COUNT(*) as cnt
FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
WHERE standardized_question = 'current_medication_count'
  AND standardized_answer = 'AO04'
GROUP BY 1
ORDER BY cnt DESC
'''
for row in client.query(query):
    print(f'  Survey {row.survey_id}: {row.cnt} responses')

# 4. Check lime_questions for the question
print('\n4. CHECKING LIME_QUESTIONS FOR MEDICATION COUNT:')
query = '''
SELECT sid, qid, title, question
FROM `bi-data-391216.limesurvey_data.lime_questions`
WHERE LOWER(question) LIKE '%medication%'
  AND LOWER(question) LIKE '%count%'
   OR LOWER(title) LIKE '%medication%count%'
LIMIT 10
'''
for row in client.query(query):
    q_text = row.question[:80] if row.question else "N/A"
    print(f'  sid={row.sid}, qid={row.qid}, title={row.title}, q={q_text}...')

# 5. Check all answers for the relevant qid
print('\n5. ALL ANSWER OPTIONS FOR MEDICATION COUNT QUESTIONS:')
query = '''
SELECT a.qid, a.code, a.answer, a.sortorder, q.sid, q.title
FROM `bi-data-391216.limesurvey_data.lime_answers` a
JOIN `bi-data-391216.limesurvey_data.lime_questions` q ON a.qid = q.qid
WHERE LOWER(q.question) LIKE '%medication%'
  AND (LOWER(q.question) LIKE '%how many%' OR LOWER(q.question) LIKE '%count%' OR LOWER(q.question) LIKE '%number%')
ORDER BY a.qid, a.sortorder
'''
current_qid = None
for row in client.query(query):
    if row.qid != current_qid:
        print(f'\n  QID {row.qid} (survey {row.sid}, title={row.title}):')
        current_qid = row.qid
    print(f'    {row.code}: {row.answer}')

print('\n' + '='*80)
