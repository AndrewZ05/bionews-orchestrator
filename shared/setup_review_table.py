#!/usr/bin/env python3
"""Add revised_std_question column to the review table for manual editing in BigQuery"""

import os
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
    for _p in (r'c:\gcp\service-account-bionews-pipeline.json', '/home/orchestrator/service-account-bionews-pipeline.json'):
        if os.path.exists(_p):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _p
            break

from google.cloud import bigquery

client = bigquery.Client()
table_id = 'bi-data-391216.limesurvey_data.lime_surveys_question_mismatch_review'

# Check current schema
table = client.get_table(table_id)
existing_columns = [f.name for f in table.schema]
print(f"Existing columns: {existing_columns}")

# Add revised_std_question column if needed
if 'revised_std_question' not in existing_columns:
    new_schema = list(table.schema)
    new_schema.append(bigquery.SchemaField('revised_std_question', 'STRING'))
    table.schema = new_schema
    client.update_table(table, ['schema'])
    print("Added revised_std_question column")
else:
    print("revised_std_question column already exists")

# Show current contents
query = """
SELECT
    id,
    current_std_question,
    proposed_std_question,
    revised_std_question,
    raw_question_en,
    ROUND(avg_similarity_in_group * 100, 1) as group_sim_pct,
    affected_row_count,
    status
FROM `bi-data-391216.limesurvey_data.lime_surveys_question_mismatch_review`
ORDER BY current_std_question, raw_question_en
"""

print()
print("=" * 100)
print("REVIEW TABLE: bi-data-391216.limesurvey_data.lime_surveys_question_mismatch_review")
print("=" * 100)
print()

for row in client.query(query).result():
    q = row.raw_question_en[:60] + '...' if len(row.raw_question_en) > 60 else row.raw_question_en
    print(f"FROM: {row.current_std_question} -> PROPOSED: {row.proposed_std_question}")
    print(f"  Question: \"{q}\"")
    print(f"  Similarity: {row.group_sim_pct}% | Rows: {row.affected_row_count} | Status: {row.status}")
    print(f"  revised_std_question: {row.revised_std_question or '(empty - fill this in to correct)'}")
    print()

print("=" * 100)
print("TO EDIT: Open BigQuery console and run:")
print()
print("UPDATE `bi-data-391216.limesurvey_data.lime_surveys_question_mismatch_review`")
print("SET revised_std_question = 'correct_value'")
print("WHERE id = 'row_id'")
print()
print("Then run: python shared/limesurvey_fix_mismatches.py --apply-from-review")
print("=" * 100)
