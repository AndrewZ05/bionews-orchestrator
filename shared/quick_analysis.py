#!/usr/bin/env python3
"""Quick analysis of identity hub production rebuild results."""
import os, sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
    for _p in (r'C:\gcp\service-account-bionews-pipeline.json', '/home/orchestrator/service-account-bionews-pipeline.json'):
        if os.path.exists(_p):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _p
            break
from google.cloud import bigquery
c = bigquery.Client(project='bi-data-391216')

def q(label, sql):
    print(f'\n=== {label} ===')
    try:
        for row in c.query(sql).result():
            d = dict(row)
            for k, v in d.items():
                if isinstance(v, (int, float)):
                    print(f'  {k:45} {v:>15,.0f}')
                else:
                    print(f'  {k:45} {v}')
    except Exception as e:
        print(f'  ERROR: {str(e)[:200]}')

q('Table sizes', """
    SELECT table_id, row_count
    FROM identity_hub_data.__TABLES__
    ORDER BY row_count DESC
""")

q('Tier breakdown', """
    SELECT cluster_tier, COUNT(DISTINCT bn_id) AS bn_ids
    FROM identity_hub_data.bn_id_xref
    GROUP BY cluster_tier
""")

q('Identifier types', """
    SELECT identifier_type, COUNT(DISTINCT bn_id) AS bn_ids, COUNT(*) AS total_rows
    FROM identity_hub_data.bn_id_xref
    GROUP BY identifier_type ORDER BY total_rows DESC
""")

q('Union-Find metrics', """
    SELECT metric_name, metric_value
    FROM identity_hub_data.bn_id_metrics
    WHERE run_date = (SELECT MAX(run_date) FROM identity_hub_data.bn_id_metrics)
      AND metric_name LIKE 'union_find.%'
    ORDER BY metric_name
""")

q('Connector edge counts', """
    SELECT metric_name, metric_value
    FROM identity_hub_data.bn_id_metrics
    WHERE run_date = (SELECT MAX(run_date) FROM identity_hub_data.bn_id_metrics)
      AND metric_name LIKE '%.edges'
    ORDER BY metric_value DESC
""")

q('Xref/Hub/Neighbors metrics', """
    SELECT metric_name, metric_value
    FROM identity_hub_data.bn_id_metrics
    WHERE run_date = (SELECT MAX(run_date) FROM identity_hub_data.bn_id_metrics)
      AND (metric_name LIKE 'xref_table.%' OR metric_name LIKE 'hub_table.%'
           OR metric_name LIKE 'neighbors_table.%' OR metric_name LIKE 'persistence.%'
           OR metric_name LIKE 'confidence_aggregation.%')
    ORDER BY metric_name
""")

print('\n=== Coverage Report (production) ===')
os.system(f'{sys.executable} shared/identity_hub_test_helpers.py --report --dataset identity_hub_data --no-compare')
