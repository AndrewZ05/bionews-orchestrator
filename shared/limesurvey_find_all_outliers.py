#!/usr/bin/env python3
"""
Find ALL outlier questions across all standardized_question groups.
Outputs to BigQuery table for manual review and correction.

This is a FAST batch process - it doesn't try to reclassify, just flags outliers.
"""

import os
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
    for _p in (r'c:\gcp\service-account-bionews-pipeline.json', '/home/orchestrator/service-account-bionews-pipeline.json'):
        if os.path.exists(_p):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _p
            break
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import warnings
warnings.filterwarnings('ignore')

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from google.cloud import bigquery
from sentence_transformers import SentenceTransformer, util
from collections import defaultdict
import numpy as np
from datetime import datetime
import hashlib

# Constants
PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'
SOURCE_TABLE = 'lime_surveys_columnar_completed'
OUTLIER_TABLE = 'lime_surveys_question_outliers_review'
SIMILARITY_THRESHOLD = 0.60  # Questions below this similarity are flagged

def main():
    logger.info("=" * 70)
    logger.info("LIMESURVEY QUESTION OUTLIER DETECTOR - BATCH MODE")
    logger.info("=" * 70)
    logger.info(f"Similarity threshold: {SIMILARITY_THRESHOLD:.0%}")
    logger.info("")

    bq_client = bigquery.Client(project=PROJECT_ID)

    # Step 1: Load all unique questions grouped by standardized_question
    logger.info("Step 1: Loading all question/answer pairs from BigQuery...")
    query = f"""
    SELECT
        standardized_question,
        raw_question_en,
        COUNT(*) as row_count
    FROM `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
    WHERE standardized_question IS NOT NULL
      AND raw_question_en IS NOT NULL
      AND TRIM(raw_question_en) != ''
    GROUP BY standardized_question, raw_question_en
    ORDER BY standardized_question, row_count DESC
    """

    df = bq_client.query(query).to_dataframe()
    logger.info(f"  Loaded {len(df)} unique question/standardized_question pairs")

    # Group by standardized_question
    groups = defaultdict(list)
    row_counts = {}
    for _, row in df.iterrows():
        std_q = row['standardized_question']
        raw_q = row['raw_question_en']
        groups[std_q].append(raw_q)
        row_counts[(std_q, raw_q)] = row['row_count']

    logger.info(f"  Found {len(groups)} standardized question groups")

    # Step 2: Load sentence transformer model ONCE
    logger.info("\nStep 2: Loading sentence transformer model...")
    model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    logger.info("  Model loaded")

    # Step 3: Process each group and find outliers
    logger.info("\nStep 3: Computing embeddings and finding outliers...")

    all_outliers = []
    total_questions = 0

    for std_q in sorted(groups.keys()):
        questions = groups[std_q]
        total_questions += len(questions)

        if len(questions) < 2:
            continue  # Need at least 2 questions to compute similarity

        # Compute embeddings for this group
        embeddings = model.encode(questions, convert_to_tensor=True, show_progress_bar=False)

        # Compute pairwise similarities
        sim_matrix = util.cos_sim(embeddings, embeddings).cpu().numpy()

        # For each question, compute average similarity to others in group
        for i, raw_q in enumerate(questions):
            # Exclude self-similarity
            other_sims = [sim_matrix[i][j] for j in range(len(questions)) if i != j]
            avg_sim = np.mean(other_sims) if other_sims else 1.0

            if avg_sim < SIMILARITY_THRESHOLD:
                affected_rows = row_counts.get((std_q, raw_q), 0)
                outlier_id = hashlib.md5(f"{std_q}:{raw_q}".encode()).hexdigest()[:16]

                all_outliers.append({
                    'id': outlier_id,
                    'standardized_question': std_q,
                    'raw_question_en': raw_q,
                    'avg_similarity_in_group': float(avg_sim),
                    'group_size': len(questions),
                    'affected_row_count': affected_rows,
                    'revised_std_question': None,  # User fills this in
                    'status': 'pending_review',
                    'detected_at': datetime.utcnow().isoformat()
                })

    logger.info(f"  Processed {total_questions} questions across {len(groups)} groups")
    logger.info(f"  Found {len(all_outliers)} outliers (< {SIMILARITY_THRESHOLD:.0%} similarity)")

    if not all_outliers:
        logger.info("\nNo outliers found!")
        return

    # Step 4: Write to BigQuery table
    logger.info(f"\nStep 4: Writing {len(all_outliers)} outliers to BigQuery...")

    full_table = f"{PROJECT_ID}.{DATASET}.{OUTLIER_TABLE}"

    # Create table schema
    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("standardized_question", "STRING"),
        bigquery.SchemaField("raw_question_en", "STRING"),
        bigquery.SchemaField("avg_similarity_in_group", "FLOAT64"),
        bigquery.SchemaField("group_size", "INT64"),
        bigquery.SchemaField("affected_row_count", "INT64"),
        bigquery.SchemaField("revised_std_question", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("detected_at", "TIMESTAMP"),
    ]

    # Create or replace table
    table = bigquery.Table(full_table, schema=schema)
    table = bq_client.create_table(table, exists_ok=True)

    # Clear existing data and insert new
    bq_client.query(f"TRUNCATE TABLE `{full_table}`").result()

    # Insert rows
    errors = bq_client.insert_rows_json(full_table, all_outliers)
    if errors:
        logger.error(f"Errors inserting rows: {errors[:5]}")
    else:
        logger.info(f"  Successfully wrote {len(all_outliers)} rows")

    # Step 5: Print summary
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)

    # Group outliers by standardized_question
    by_std_q = defaultdict(list)
    for o in all_outliers:
        by_std_q[o['standardized_question']].append(o)

    logger.info(f"\nOutliers by standardized_question:")
    for std_q in sorted(by_std_q.keys(), key=lambda x: -len(by_std_q[x])):
        outliers = by_std_q[std_q]
        total_rows = sum(o['affected_row_count'] for o in outliers)
        logger.info(f"  {std_q}: {len(outliers)} outliers ({total_rows:,} rows)")

    logger.info(f"\n" + "=" * 70)
    logger.info(f"REVIEW TABLE: {full_table}")
    logger.info("=" * 70)
    logger.info("\nTo review and correct:")
    logger.info("1. Open BigQuery console")
    logger.info("2. Query: SELECT * FROM `bi-data-391216.limesurvey_data.lime_surveys_question_outliers_review`")
    logger.info("         ORDER BY standardized_question, avg_similarity_in_group")
    logger.info("")
    logger.info("3. For each row, fill in 'revised_std_question' with the correct value")
    logger.info("   (leave NULL to keep current classification)")
    logger.info("")
    logger.info("4. Run: python shared/limesurvey_fix_mismatches.py --apply-from-outliers")
    logger.info("=" * 70)

if __name__ == '__main__':
    main()
