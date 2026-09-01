#!/usr/bin/env python3
"""
LimeSurvey ML-Based Mismatch Detector

Uses the trained ML model to predict what each question's standardized_question
should be, then compares to what's currently in the database.

Only flags cases where the ML model disagrees with the current classification.

Model Storage: Model files stored locally, metadata tracked in BigQuery.
"""

import os

if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or not os.path.exists(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
):
    for _p in (
        r"c:\gcp\service-account-bionews-pipeline.json",
        "/home/orchestrator/service-account-bionews-pipeline.json",
    ):
        if os.path.exists(_p):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _p
            break
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

import warnings

warnings.filterwarnings("ignore")

import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

import pickle
import hashlib
import argparse
from datetime import datetime
from pathlib import Path
from google.cloud import bigquery
import numpy as np

# Constants
PROJECT_ID = "bi-data-391216"
DATASET = "limesurvey_data"
SOURCE_TABLE = "lime_surveys_columnar_completed"
REVIEW_TABLE = "lime_surveys_ml_mismatch_review"
ML_MODEL_TABLE = "ml_question_model"  # Metadata only
MIN_CONFIDENCE = 0.70  # Only flag if ML is at least 70% confident

# Local model storage. Portable: LIMESURVEY_MODEL_DIR env var overrides; else
# <repo_root>/models. This file is in shared/, so repo_root is two parents up.
MODEL_DIR = Path(
    os.environ.get("LIMESURVEY_MODEL_DIR")
    or (Path(__file__).resolve().parent.parent / "models")
)
MODEL_FILE = MODEL_DIR / "question_classifier.pkl"
ENCODER_FILE = MODEL_DIR / "label_encoder.pkl"


def ensure_model_table_exists(bq_client):
    """Create the model metadata table if it doesn't exist"""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{ML_MODEL_TABLE}` (
        model_id STRING,
        model_path STRING,
        accuracy FLOAT64,
        trained_at TIMESTAMP,
        training_rows INT64
    )
    """
    bq_client.query(ddl).result()


def train_ml_model(bq_client):
    """Train ML model on existing data and save locally"""
    from sentence_transformers import SentenceTransformer
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder

    logger.info("=" * 60)
    logger.info("TRAINING ML MODEL")
    logger.info("=" * 60)

    ensure_model_table_exists(bq_client)

    # Get training data
    query = f"""
    SELECT DISTINCT
        raw_question_en,
        standardized_question
    FROM `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
    WHERE standardized_question IS NOT NULL
      AND raw_question_en IS NOT NULL
      AND TRIM(raw_question_en) != ''
    """

    df = bq_client.query(query).to_dataframe()
    logger.info(f"Loaded {len(df)} training examples")

    if len(df) < 10:
        logger.error("Not enough training data (need at least 10 examples)")
        return None, None, None

    questions = df["raw_question_en"].tolist()
    labels = df["standardized_question"].tolist()

    # Generate embeddings
    logger.info("Loading sentence transformer model...")
    transformer = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    logger.info("Generating embeddings...")
    embeddings = transformer.encode(questions, show_progress_bar=True)

    # Encode labels
    label_encoder = LabelEncoder()
    encoded_labels = label_encoder.fit_transform(labels)

    logger.info(f"Unique classes: {len(label_encoder.classes_)}")

    # Train model
    logger.info("Training RandomForestClassifier...")
    model = RandomForestClassifier(
        n_estimators=100, max_depth=10, n_jobs=-1, random_state=42
    )
    model.fit(embeddings, encoded_labels)

    # Calculate accuracy
    predictions = model.predict(embeddings)
    accuracy = (predictions == encoded_labels).mean()
    logger.info(f"Training accuracy: {accuracy:.2%}")

    # Save locally
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    with open(MODEL_FILE, "wb") as f:
        pickle.dump(model, f)
    logger.info(f"[OK] Model saved: {MODEL_FILE}")

    with open(ENCODER_FILE, "wb") as f:
        pickle.dump(label_encoder, f)
    logger.info(f"[OK] Encoder saved: {ENCODER_FILE}")

    # Track metadata in BigQuery
    model_id = f"model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    bq_client.query(
        f"DELETE FROM `{PROJECT_ID}.{DATASET}.{ML_MODEL_TABLE}` WHERE TRUE"
    ).result()

    insert_query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET}.{ML_MODEL_TABLE}`
    (model_id, model_path, accuracy, trained_at, training_rows)
    VALUES (@model_id, @model_path, @accuracy, @trained_at, @training_rows)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("model_id", "STRING", model_id),
            bigquery.ScalarQueryParameter("model_path", "STRING", str(MODEL_DIR)),
            bigquery.ScalarQueryParameter("accuracy", "FLOAT64", float(accuracy)),
            bigquery.ScalarQueryParameter("trained_at", "TIMESTAMP", datetime.utcnow()),
            bigquery.ScalarQueryParameter("training_rows", "INT64", len(df)),
        ]
    )

    bq_client.query(insert_query, job_config=job_config).result()
    logger.info(f"[OK] Metadata saved to BigQuery: {model_id}")

    return model, label_encoder, transformer


def load_ml_model(bq_client, auto_train=True):
    """Load the trained ML model from local files"""
    from sentence_transformers import SentenceTransformer

    try:
        # Check if local model files exist
        if not MODEL_FILE.exists() or not ENCODER_FILE.exists():
            if auto_train:
                logger.info("No model found locally - training new model...")
                return train_ml_model(bq_client)
            else:
                logger.error(f"Model files not found: {MODEL_FILE}")
                return None, None, None

        # Get metadata from BigQuery (optional, for logging)
        ensure_model_table_exists(bq_client)
        query = f"""
        SELECT model_id, accuracy, trained_at
        FROM `{PROJECT_ID}.{DATASET}.{ML_MODEL_TABLE}`
        ORDER BY trained_at DESC
        LIMIT 1
        """
        results = list(bq_client.query(query).result())
        if results:
            row = results[0]
            logger.info(
                f"Found model: {row.model_id} (accuracy: {row.accuracy:.2%}, trained: {row.trained_at})"
            )

        # Load classifier and encoder from local files
        with open(MODEL_FILE, "rb") as f:
            model = pickle.load(f)
        logger.info(f"[OK] Loaded model from: {MODEL_FILE}")

        with open(ENCODER_FILE, "rb") as f:
            encoder = pickle.load(f)
        logger.info(f"[OK] Loaded encoder from: {ENCODER_FILE}")

        # Load transformer (from HuggingFace, cached locally)
        logger.info("Loading sentence transformer model...")
        transformer = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

        logger.info(f"Loaded ML model with {len(encoder.classes_)} classes")
        return model, encoder, transformer
    except Exception as e:
        logger.error(f"Failed to load ML model: {e}")
        if auto_train:
            logger.info("Attempting to train new model...")
            return train_ml_model(bq_client)
        return None, None, None


def main():
    parser = argparse.ArgumentParser(description="ML-based mismatch detector")
    parser.add_argument(
        "--train-only",
        action="store_true",
        help="Only train the model, do not detect mismatches",
    )
    parser.add_argument(
        "--retrain",
        action="store_true",
        help="Force retrain the model before detection",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=MIN_CONFIDENCE,
        help="Minimum confidence threshold",
    )
    parser.add_argument(
        "--show-uncertain",
        type=float,
        default=None,
        help="Show all predictions below this confidence (e.g., 0.9)",
    )
    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("LIMESURVEY ML-BASED MISMATCH DETECTOR")
    logger.info("=" * 70)
    logger.info(f"Minimum ML confidence: {args.min_confidence:.0%}")
    logger.info("")

    bq_client = bigquery.Client(project=PROJECT_ID)

    # Force retrain if requested
    if args.retrain:
        logger.info("Step 1: Retraining ML model (--retrain flag)...")
        model, encoder, transformer = train_ml_model(bq_client)
    else:
        # Load ML model (will auto-train if not found)
        logger.info("Step 1: Loading ML model from BigQuery...")
        model, encoder, transformer = load_ml_model(bq_client)

    if model is None:
        logger.error("Cannot proceed without ML model")
        return

    if args.train_only:
        logger.info("Training complete. Exiting (--train-only mode).")
        return

    # Step 2: Load all unique questions
    logger.info("\nStep 2: Loading all unique questions from BigQuery...")
    query = f"""
    SELECT
        standardized_question as current_std_question,
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

    # Step 3: Run ML model on each question
    logger.info("\nStep 3: Running ML model predictions...")

    # Generate embeddings for all questions at once (faster)
    questions = df["raw_question_en"].tolist()
    logger.info(f"  Generating embeddings for {len(questions)} questions...")
    embeddings = transformer.encode(questions, show_progress_bar=True)

    mismatches = []
    uncertain = []  # Cases where model is uncertain (low confidence)

    for idx, (_, row) in enumerate(df.iterrows()):
        raw_q = row["raw_question_en"]
        current = row["current_std_question"]
        row_count = row["row_count"]

        # Get ML prediction using pre-computed embedding
        try:
            proba = model.predict_proba([embeddings[idx]])[0]
            predicted_idx = proba.argmax()
            confidence = proba[predicted_idx]
            predicted = encoder.inverse_transform([predicted_idx])[0]

            entry = {
                "id": hashlib.md5(f"{current}:{raw_q}".encode()).hexdigest()[:16],
                "current_std_question": current,
                "ml_predicted_std_question": predicted,
                "ml_confidence": float(confidence),
                "raw_question_en": raw_q,
                "affected_row_count": row_count,
                "revised_std_question": None,
                "status": "pending_review",
                "detected_at": datetime.utcnow().isoformat(),
            }

            # Check if ML disagrees with current classification
            if predicted != current and confidence >= args.min_confidence:
                mismatches.append(entry)

            # Track uncertain predictions (for --show-uncertain)
            if args.show_uncertain and confidence < args.show_uncertain:
                uncertain.append(entry)

        except Exception as e:
            logger.warning(f"  Error predicting for: {raw_q[:50]}... - {e}")

    logger.info(f"  Found {len(mismatches)} mismatches where ML disagrees")

    # Show uncertain predictions if requested
    if args.show_uncertain and uncertain:
        logger.info(f"\n" + "=" * 70)
        logger.info(f"UNCERTAIN PREDICTIONS (confidence < {args.show_uncertain:.0%})")
        logger.info("=" * 70)
        logger.info(f"Found {len(uncertain)} uncertain predictions to review:\n")

        for u in sorted(uncertain, key=lambda x: x["ml_confidence"])[:20]:
            q = (
                u["raw_question_en"][:60] + "..."
                if len(u["raw_question_en"]) > 60
                else u["raw_question_en"]
            )
            match_status = (
                "✓ MATCH"
                if u["current_std_question"] == u["ml_predicted_std_question"]
                else "✗ MISMATCH"
            )
            logger.info(
                f"  {match_status} ({u['ml_confidence']:.1%}) Current: {u['current_std_question']}"
            )
            logger.info(f"    ML predicts: {u['ml_predicted_std_question']}")
            logger.info(f'    Question: "{q}"')
            logger.info(f"    Rows: {u['affected_row_count']}")
            logger.info("")

    if not mismatches:
        logger.info(
            "\nNo high-confidence mismatches found - ML agrees with all current classifications!"
        )
        if not args.show_uncertain:
            logger.info(
                "Tip: Run with --show-uncertain 0.9 to see low-confidence predictions for manual review"
            )
        return

    # Step 4: Write to BigQuery
    logger.info(f"\nStep 4: Writing {len(mismatches)} mismatches to BigQuery...")

    full_table = f"{PROJECT_ID}.{DATASET}.{REVIEW_TABLE}"

    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("current_std_question", "STRING"),
        bigquery.SchemaField("ml_predicted_std_question", "STRING"),
        bigquery.SchemaField("ml_confidence", "FLOAT64"),
        bigquery.SchemaField("raw_question_en", "STRING"),
        bigquery.SchemaField("affected_row_count", "INT64"),
        bigquery.SchemaField("revised_std_question", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("detected_at", "TIMESTAMP"),
    ]

    table = bigquery.Table(full_table, schema=schema)
    table = bq_client.create_table(table, exists_ok=True)
    bq_client.query(f"TRUNCATE TABLE `{full_table}`").result()

    errors = bq_client.insert_rows_json(full_table, mismatches)
    if errors:
        logger.error(f"Errors inserting rows: {errors[:5]}")
    else:
        logger.info(f"  Successfully wrote {len(mismatches)} rows")

    # Step 5: Summary
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)

    # Group by current_std_question
    from collections import defaultdict

    by_current = defaultdict(list)
    for m in mismatches:
        by_current[m["current_std_question"]].append(m)

    logger.info(f"\nMismatches by current standardized_question:")
    for current in sorted(by_current.keys(), key=lambda x: -len(by_current[x])):
        items = by_current[current]
        total_rows = sum(m["affected_row_count"] for m in items)
        logger.info(f"  {current}: {len(items)} mismatches ({total_rows:,} rows)")

    # Show sample mismatches
    logger.info(f"\nSample mismatches (top 10):")
    for m in sorted(mismatches, key=lambda x: -x["ml_confidence"])[:10]:
        q = (
            m["raw_question_en"][:50] + "..."
            if len(m["raw_question_en"]) > 50
            else m["raw_question_en"]
        )
        logger.info(
            f"  Current: {m['current_std_question']} -> ML predicts: {m['ml_predicted_std_question']} ({m['ml_confidence']:.1%})"
        )
        logger.info(f'    "{q}"')
        logger.info(f"    Affected rows: {m['affected_row_count']}")

    logger.info(f"\n" + "=" * 70)
    logger.info(f"REVIEW TABLE: {full_table}")
    logger.info("=" * 70)
    logger.info("\nTo review:")
    logger.info(f"  SELECT * FROM `{full_table}` ORDER BY ml_confidence DESC")
    logger.info("\nTo correct, set revised_std_question to the correct value")
    logger.info(
        "Then run: python shared/limesurvey_fix_mismatches.py --apply-from-ml-review"
    )


if __name__ == "__main__":
    main()
