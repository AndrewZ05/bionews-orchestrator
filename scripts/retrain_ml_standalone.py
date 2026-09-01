"""Standalone retrain of the LimeSurvey question classifier.

The normal `python shared/limesurvey_process_columnar.py --retrain-model`
path OOMs when LimeSurveyProcessor.__init__ tries to eagerly load
en_ner_bc5cdr_md (spaCy biomedical NER), which the trainer doesn't actually
need. This script bypasses the constructor and replicates the
train_ml_model() body using only sentence-transformers + scikit-learn.

Outputs (matches the layout train_ml_model writes):
  c:/orchestrator/models/question_classifier.pkl
  c:/orchestrator/models/label_encoder.pkl
  + INSERT row in bi-data-391216.limesurvey_data.ml_question_model

Removable; not part of the project. Delete after we have a proper fix for
the eager-load OOM in LimeSurveyProcessor.__init__.
"""

import os
import pickle
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from sentence_transformers import SentenceTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

PROJECT_ID = "bi-data-391216"
DATASET = "limesurvey_data"
TABLE = "lime_surveys_columnar_completed"
ML_MODEL_TABLE = "ml_question_model"
# Portable: LIMESURVEY_MODEL_DIR env var overrides; else <repo_root>/models.
MODEL_DIR = Path(
    os.environ.get("LIMESURVEY_MODEL_DIR")
    or (Path(__file__).resolve().parent / "models")
)
MODEL_FILE = MODEL_DIR / "question_classifier.pkl"
ENCODER_FILE = MODEL_DIR / "label_encoder.pkl"
MODEL_DIR.mkdir(parents=True, exist_ok=True)


def cleanup_text(text: str) -> str:
    """Mirrors LimeSurveyProcessor._cleanup_text without the OOM-y constructor."""
    if not text or not text.strip():
        return ""
    cleaned = re.sub(r"<[^>]+>", "", text)
    for entity, char in (
        ("&gt;", ">"),
        ("&lt;", "<"),
        ("&amp;", "&"),
        ("&quot;", '"'),
        ("&apos;", "'"),
        ("&nbsp;", " "),
        ("&#39;", "'"),
        ("&#34;", '"'),
    ):
        cleaned = cleaned.replace(entity, char)
    for pattern, replacement in (
        (r"â€™|â€˜", "'"),
        (r"â€|â€", '"'),
    ):
        cleaned = re.sub(pattern, replacement, cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def main():
    t0 = time.time()

    print(f"[{time.strftime('%H:%M:%S')}] loading sentence-transformer...")
    tx = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    print(f"[{time.strftime('%H:%M:%S')}] querying training pool from BigQuery...")
    client = bigquery.Client(project=PROJECT_ID)
    df = client.query(
        f"""
        SELECT DISTINCT
            raw_question_en,
            standardized_question
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE standardized_question IS NOT NULL
          AND raw_question_en IS NOT NULL
          AND TRIM(raw_question_en) != ''
        """
    ).to_dataframe()
    print(f"  loaded {len(df)} distinct training rows")

    cleaned = [cleanup_text(q) for q in df["raw_question_en"]]
    labels = df["standardized_question"].tolist()
    pairs = [(q, l) for q, l in zip(cleaned, labels) if q]
    questions = [q for q, _ in pairs]
    labels = [l for _, l in pairs]
    print(f"  after cleanup: {len(questions)} training examples")

    print(f"[{time.strftime('%H:%M:%S')}] computing embeddings...")
    embeddings = tx.encode(questions, show_progress_bar=False)
    print(f"  embedding shape: {embeddings.shape}")

    print(f"[{time.strftime('%H:%M:%S')}] fitting label encoder...")
    le = LabelEncoder()
    y = le.fit_transform(labels)
    n_classes = len(le.classes_)
    print(f"  unique classes: {n_classes}")

    class_counts = pd.Series(y).value_counts()
    min_per_class = int(class_counts.min())

    if min_per_class >= 2 and len(embeddings) >= 20:
        print(f"[{time.strftime('%H:%M:%S')}] holdout split (stratified, 80/20)...")
        X_tr, X_te, y_tr, y_te = train_test_split(
            embeddings, y, test_size=0.20, stratify=y, random_state=42
        )
        model = RandomForestClassifier(
            n_estimators=100, max_depth=10, n_jobs=-1, random_state=42
        )
        model.fit(X_tr, y_tr)
        train_acc = (model.predict(X_tr) == y_tr).mean()
        val_acc = (model.predict(X_te) == y_te).mean()
        print(f"  train acc: {train_acc:.2%}")
        print(f"  validation acc (TRUE): {val_acc:.2%}")
        gap = train_acc - val_acc
        if gap > 0.10:
            print(f"  WARNING: overfitting gap = {gap:.1%}")

        print(f"[{time.strftime('%H:%M:%S')}] refitting on full data for production...")
        model = RandomForestClassifier(
            n_estimators=100, max_depth=10, n_jobs=-1, random_state=42
        )
        model.fit(embeddings, y)
        reported_accuracy = val_acc
    else:
        print(
            f"  WARNING: min samples per class = {min_per_class}, "
            f"cannot do stratified holdout. Training on full data only."
        )
        model = RandomForestClassifier(
            n_estimators=100, max_depth=10, n_jobs=-1, random_state=42
        )
        model.fit(embeddings, y)
        reported_accuracy = float((model.predict(embeddings) == y).mean())
        print(f"  training acc (possibly overfit): {reported_accuracy:.2%}")

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(MODEL_FILE, "wb") as f:
        pickle.dump(model, f)
    with open(ENCODER_FILE, "wb") as f:
        pickle.dump(le, f)
    print(f"[{time.strftime('%H:%M:%S')}] wrote {MODEL_FILE}")
    print(f"[{time.strftime('%H:%M:%S')}] wrote {ENCODER_FILE}")

    model_id = f"model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    client.query(
        f"DELETE FROM `{PROJECT_ID}.{DATASET}.{ML_MODEL_TABLE}` WHERE TRUE"
    ).result()
    client.query(
        f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.{ML_MODEL_TABLE}`
        (model_id, model_path, accuracy, trained_at, training_rows)
        VALUES (@model_id, @model_path, @accuracy, @trained_at, @training_rows)
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("model_id", "STRING", model_id),
                bigquery.ScalarQueryParameter("model_path", "STRING", str(MODEL_DIR)),
                bigquery.ScalarQueryParameter(
                    "accuracy", "FLOAT64", float(reported_accuracy)
                ),
                bigquery.ScalarQueryParameter(
                    "trained_at", "TIMESTAMP", datetime.utcnow()
                ),
                bigquery.ScalarQueryParameter("training_rows", "INT64", len(df)),
            ]
        ),
    ).result()
    print(f"  metadata row inserted: model_id={model_id}")

    print(
        f"[{time.strftime('%H:%M:%S')}] done in {time.time() - t0:.1f}s. "
        f"classes={n_classes}  reported_accuracy={reported_accuracy:.2%}"
    )


if __name__ == "__main__":
    main()
