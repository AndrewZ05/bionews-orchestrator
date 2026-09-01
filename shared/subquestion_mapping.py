#!/usr/bin/env python3
"""Generate subquestion ID to text mapping for audit."""

import sys
from pathlib import Path
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
load_dotenv(Path(__file__).parent.parent / '.env')

from google.cloud import bigquery
client = bigquery.Client(project='bi-data-391216')

# Get all subquestion mappings
query = '''
WITH subq_mapping AS (
    SELECT
        parent.qid as parent_qid,
        parent_l10n.question as parent_question,
        sub.title as subquestion_code,
        REGEXP_EXTRACT(sub.title, r"SQ(\\d+)") as subq_num,
        sub_l10n.question as subquestion_text
    FROM `bi-data-391216.limesurvey_data.lime_questions` parent
    JOIN `bi-data-391216.limesurvey_data.lime_question_l10ns` parent_l10n
        ON parent.qid = parent_l10n.qid AND parent_l10n.language = "en"
    JOIN `bi-data-391216.limesurvey_data.lime_questions` sub
        ON parent.qid = sub.parent_qid
    JOIN `bi-data-391216.limesurvey_data.lime_question_l10ns` sub_l10n
        ON sub.qid = sub_l10n.qid AND sub_l10n.language = "en"
    WHERE parent.parent_qid = 0
)
SELECT
    c.standardized_question,
    c.subquestion_id,
    sq.subquestion_text,
    COUNT(*) as row_count
FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed` c
LEFT JOIN subq_mapping sq
    ON c.question_id = sq.parent_qid
    AND c.subquestion_id = sq.subq_num
WHERE c.subquestion_id IS NOT NULL
  AND TRIM(c.subquestion_id) != ''
  AND c.subquestion_id NOT IN ('other')
GROUP BY 1, 2, 3
ORDER BY 1, 2
'''

print('Subquestion ID to Text Mapping:')
print('='*130)
print(f'{"standardized_question":40} | {"subq_id":7} | {"subquestion_text":55} | {"rows":>8}')
print('-'*130)

current_q = None
for row in client.query(query):
    text = (row.subquestion_text or '** NOT MAPPED **')[:55]

    if row.standardized_question != current_q:
        if current_q:
            print()
        current_q = row.standardized_question

    print(f'{row.standardized_question:40} | {row.subquestion_id:7} | {text:55} | {row.row_count:>8}')
