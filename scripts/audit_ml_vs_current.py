#!/usr/bin/env python3
"""
ML vs. current-assignment audit.

Runs the trained question-classifier against every distinct raw_question_en in
lime_surveys_columnar_completed, records each prediction (whether it agrees
with the current standardized_question or not), and writes results to a new
BigQuery table `ml_audit_predictions`.

Purpose: provide a queryable comparison surface for deciding whether the ETL
cascade should keep the current order (pattern_rule first) or trust the model
over the rules (exact_match -> ml_model -> pattern_rule -> rest).

Read-only against lime_surveys_columnar_completed. Writes ONLY to the audit
table. Does NOT modify any production assignments.

Usage:
    python scripts/audit_ml_vs_current.py
    python scripts/audit_ml_vs_current.py --limit 1000    # smaller batch for testing

Output table schema (bi-data-391216.limesurvey_data.ml_audit_predictions):
    audit_run_id              STRING     -- UUID per run
    audit_run_at              TIMESTAMP
    raw_question_en           STRING
    current_std_q             STRING     -- may be NULL
    current_method            STRING     -- may be NULL
    rows_affected             INT64      -- count in columnar_completed
    distinct_surveys          INT64
    predicted_std_q           STRING
    predicted_confidence      FLOAT64
    agreement                 STRING     -- 'match' | 'differs' | 'current_null' | 'predicted_unknown_class'

Example follow-up queries are in the docstring at the bottom of this file.
"""

from __future__ import annotations

import argparse
import logging
import os
import pickle
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or not os.path.exists(
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or ""
):
    for _p in (
        r"c:\gcp\service-account-bionews-pipeline.json",
        "/home/orchestrator/service-account-bionews-pipeline.json",
    ):
        if os.path.exists(_p):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _p
            break

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
# Suppress transformer + tokenizer pre-init chatter BEFORE importing anything
# downstream of huggingface. These map to "errors only" so failures still surface.
os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
# Squash the "no HF_TOKEN set, rate-limited downloads" warning. The model is
# cached locally after the first run anyway; rate limits don't matter in practice.
import warnings as _warnings

_warnings.filterwarnings(
    "ignore", message=r".*unauthenticated requests to the HF Hub.*"
)
_warnings.filterwarnings("ignore", category=DeprecationWarning)


import numpy as np
from google.cloud import bigquery


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
# Quiet noisy third-party loggers that the audit doesn't need. WARNING level
# means errors and real warnings still appear; routine HTTP roundtrips don't.
for noisy in (
    "httpx",
    "httpcore",
    "urllib3",
    "huggingface_hub",
    "sentence_transformers",
    "transformers",
):
    logging.getLogger(noisy).setLevel(logging.WARNING)

logger = logging.getLogger("audit_ml_vs_current")


PROJECT_ID = "bi-data-391216"
DATASET = "limesurvey_data"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed"
AUDIT_TABLE = f"{PROJECT_ID}.{DATASET}.ml_audit_predictions"

REPO_ROOT = Path(__file__).resolve().parent.parent
# Honor LIMESURVEY_MODEL_DIR env var (set on Linux via the lime-edit systemd
# drop-in to /home/orchestrator/models), else fall back to <repo_root>/models.
MODEL_DIR = Path(os.environ.get("LIMESURVEY_MODEL_DIR") or (REPO_ROOT / "models"))
MODEL_FILE = MODEL_DIR / "question_classifier.pkl"
ENCODER_FILE = MODEL_DIR / "label_encoder.pkl"


def ensure_audit_table(bq: bigquery.Client) -> None:
    schema = [
        bigquery.SchemaField("audit_run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("audit_run_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("raw_question_en", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("subquestion_text", "STRING"),
        bigquery.SchemaField("ml_input", "STRING"),
        bigquery.SchemaField("current_std_q", "STRING"),
        bigquery.SchemaField("current_method", "STRING"),
        bigquery.SchemaField("rows_affected", "INT64"),
        bigquery.SchemaField("distinct_surveys", "INT64"),
        bigquery.SchemaField("predicted_std_q", "STRING"),
        bigquery.SchemaField("predicted_confidence", "FLOAT64"),
        bigquery.SchemaField("agreement", "STRING"),
    ]
    # If the table exists with the old (no subquestion_text / ml_input)
    # schema, add the new columns rather than fail. BigQuery supports
    # column additions but not removals; this is safe and idempotent.
    table = bigquery.Table(AUDIT_TABLE, schema=schema)
    try:
        bq.create_table(table, exists_ok=False)
    except Exception:
        existing = bq.get_table(AUDIT_TABLE)
        existing_names = {f.name for f in existing.schema}
        new_fields = [f for f in schema if f.name not in existing_names]
        if new_fields:
            existing.schema = list(existing.schema) + new_fields
            bq.update_table(existing, ["schema"])


def load_model():
    if not MODEL_FILE.exists() or not ENCODER_FILE.exists():
        raise SystemExit(
            f"Model files missing. Expected {MODEL_FILE} and {ENCODER_FILE}. "
            f"Train via the Model page in the mapper UI first."
        )
    with open(MODEL_FILE, "rb") as f:
        model = pickle.load(f)
    with open(ENCODER_FILE, "rb") as f:
        encoder = pickle.load(f)
    mtime = datetime.fromtimestamp(MODEL_FILE.stat().st_mtime)
    logger.info(
        f"Loaded model: {type(model).__name__}, "
        f"{len(encoder.classes_)} classes, trained {mtime:%Y-%m-%d %H:%M}"
    )
    return model, encoder


def load_transformer():
    from sentence_transformers import SentenceTransformer

    logger.info("Loading sentence-transformer all-MiniLM-L6-v2...")
    return SentenceTransformer("all-MiniLM-L6-v2")


def fetch_distinct_raws(bq: bigquery.Client, limit: int | None) -> list[dict]:
    """Return one row per distinct (raw_question_en, subquestion_text) pair.

    For matrix rows (subquestion_id is set), we join lime_questions and
    lime_question_l10ns to fetch the actual subquestion text. The audit's
    prediction then uses `parent :: subquestion` as the ML input, matching
    what train_ml_model writes to the model.

    Without this join, matrix-parent text like 'Please rate the following...'
    looks identical to the model across many different subquestions, and
    the model produces ~0.02 confidence on every one. See audit_run_id
    1381518... for the pre-fix baseline.

    A (raw_question_en, subquestion_text) tuple may have multiple distinct
    current_std_q values across surveys; we pick the most common via
    APPROX_TOP_COUNT, matching the original behavior."""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
    WITH base AS (
      SELECT
        cc.raw_question_en,
        cc.subquestion_id,
        cc.survey_id,
        cc.question_id,
        cc.standardized_question,
        cc.question_match_method,
        COALESCE(l10n.question, q.question) AS subquestion_text
      FROM `{SOURCE_TABLE}` cc
      LEFT JOIN `{PROJECT_ID}.{DATASET}.lime_questions` q
        ON q.sid = cc.survey_id
       AND q.parent_qid = cc.question_id
       AND q.title = CONCAT('SQ', cc.subquestion_id)
       AND cc.subquestion_id IS NOT NULL
       AND cc.subquestion_id != ''
      LEFT JOIN `{PROJECT_ID}.{DATASET}.lime_question_l10ns` l10n
        ON l10n.qid = q.qid
      WHERE cc.raw_question_en IS NOT NULL
        AND TRIM(cc.raw_question_en) != ''
    ),
    grouped AS (
      SELECT
        raw_question_en,
        subquestion_text,
        APPROX_TOP_COUNT(standardized_question, 1)[OFFSET(0)] AS top_std_q,
        APPROX_TOP_COUNT(question_match_method, 1)[OFFSET(0)] AS top_method,
        COUNT(*) AS rows_affected,
        COUNT(DISTINCT survey_id) AS distinct_surveys
      FROM base
      GROUP BY raw_question_en, subquestion_text
    )
    SELECT
      raw_question_en,
      subquestion_text,
      top_std_q.value AS current_std_q,
      top_method.value AS current_method,
      rows_affected,
      distinct_surveys
    FROM grouped
    ORDER BY rows_affected DESC
    {limit_clause}
    """
    rows = [dict(r) for r in bq.query(query).result()]
    n_with_subq = sum(1 for r in rows if r.get("subquestion_text"))
    logger.info(
        f"Fetched {len(rows):,} (raw_question_en, subquestion_text) pairs "
        f"({n_with_subq:,} carry a subquestion_text)"
    )
    return rows


def build_ml_input(parent: str | None, subq: str | None) -> str:
    """Mirror of LimeSurveyProcessor.build_ml_input. Kept inline so the audit
    script can run as a standalone without importing the processor (which
    pulls in heavy spaCy / UMLS dependencies at import time)."""
    parent = (parent or "").strip()
    subq = (subq or "").strip()
    if subq:
        return f"{parent} :: {subq}" if parent else subq
    return parent


def predict_batch(transformer, model, encoder, raws: list[str], batch_size: int = 128):
    """Yield (predicted_class, confidence) for each raw in raws."""
    n = len(raws)
    for start in range(0, n, batch_size):
        chunk = raws[start : start + batch_size]
        emb = transformer.encode(chunk, show_progress_bar=False, convert_to_numpy=True)
        probs = model.predict_proba(emb)
        argmax = probs.argmax(axis=1)
        confs = probs.max(axis=1)
        classes = encoder.inverse_transform(argmax)
        for pred, conf in zip(classes, confs):
            yield pred, float(conf)
        logger.info(f"  predicted {min(start + batch_size, n):,} / {n:,}")


def write_audit_rows(bq: bigquery.Client, rows: list[dict]) -> None:
    if not rows:
        logger.warning("No rows to write.")
        return
    errors = bq.insert_rows_json(AUDIT_TABLE, rows)
    if errors:
        raise RuntimeError(f"BQ insert errors: {errors}")
    logger.info(f"Wrote {len(rows):,} audit rows to {AUDIT_TABLE}")


def summarize(bq: bigquery.Client, audit_run_id: str) -> None:
    query = f"""
    SELECT
      current_method,
      agreement,
      COUNT(*) AS distinct_raws,
      SUM(rows_affected) AS rows_in_source
    FROM `{AUDIT_TABLE}`
    WHERE audit_run_id = @rid
    GROUP BY current_method, agreement
    ORDER BY current_method NULLS FIRST, agreement
    """
    job = bq.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("rid", "STRING", audit_run_id)
            ]
        ),
    )
    logger.info("=" * 70)
    logger.info("AUDIT SUMMARY by current_method x agreement")
    logger.info("=" * 70)
    logger.info(f"{'method':25s} {'agreement':25s} {'raws':>10s} {'rows':>15s}")
    logger.info("-" * 80)
    for r in job:
        method = str(r["current_method"] or "(none)")
        logger.info(
            f"{method:25s} {r['agreement']:25s} {r['distinct_raws']:>10,} "
            f"{r['rows_in_source']:>15,}"
        )
    logger.info("=" * 70)
    logger.info(
        f"To explore: SELECT * FROM `{AUDIT_TABLE}` "
        f"WHERE audit_run_id = '{audit_run_id}' ORDER BY rows_affected DESC"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only audit the top-N distinct raws by row_count (for testing).",
    )
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT_ID)

    ensure_audit_table(bq)
    model, encoder = load_model()
    encoder_class_set = set(encoder.classes_)
    transformer = load_transformer()

    raws = fetch_distinct_raws(bq, args.limit)
    if not raws:
        logger.error("No raw_question_en rows to audit.")
        return

    audit_run_id = uuid.uuid4().hex
    audit_run_at = datetime.now(timezone.utc).isoformat()
    logger.info(f"audit_run_id = {audit_run_id}")
    logger.info("Predicting...")

    # Build the ML input the same way the processor does: parent text on its
    # own for non-matrix rows, `parent :: subquestion` for matrix rows.
    ml_inputs = [
        build_ml_input(r["raw_question_en"], r.get("subquestion_text")) for r in raws
    ]
    audit_rows = []
    # predict_batch preserves input order, so zip raws and predictions positionally
    # instead of looking each one up by text (O(N^2) over 14k rows = pain).
    for meta, ml_input, (pred, conf) in zip(
        raws,
        ml_inputs,
        predict_batch(transformer, model, encoder, ml_inputs),
    ):
        current = meta["current_std_q"]
        if current is None:
            agreement = "current_null"
        elif current not in encoder_class_set:
            agreement = "predicted_unknown_class"
        elif pred == current:
            agreement = "match"
        else:
            agreement = "differs"
        audit_rows.append(
            {
                "audit_run_id": audit_run_id,
                "audit_run_at": audit_run_at,
                "raw_question_en": meta["raw_question_en"],
                "subquestion_text": meta.get("subquestion_text"),
                "ml_input": ml_input,
                "current_std_q": current,
                "current_method": meta["current_method"],
                "rows_affected": int(meta["rows_affected"]),
                "distinct_surveys": int(meta["distinct_surveys"]),
                "predicted_std_q": pred,
                "predicted_confidence": float(conf),
                "agreement": agreement,
            }
        )

    write_audit_rows(bq, audit_rows)
    summarize(bq, audit_run_id)


# ---------------------------------------------------------------------------
# AD-HOC QUERIES (run in BQ console after this script finishes)
# ---------------------------------------------------------------------------
"""
-- Latest audit run id
SELECT audit_run_id, MAX(audit_run_at) AS run_at
FROM `bi-data-391216.limesurvey_data.ml_audit_predictions`
GROUP BY audit_run_id
ORDER BY run_at DESC
LIMIT 5;

-- Agreement rate by current_method (the big question: do rules and ML agree?)
SELECT
  current_method,
  COUNT(*) AS distinct_raws,
  COUNTIF(agreement = 'match') AS agreed,
  COUNTIF(agreement = 'differs') AS disagreed,
  COUNTIF(agreement = 'current_null') AS current_null,
  ROUND(SAFE_DIVIDE(COUNTIF(agreement = 'match'), COUNTIF(agreement IN ('match','differs'))), 3) AS agree_rate
FROM `bi-data-391216.limesurvey_data.ml_audit_predictions`
WHERE audit_run_id = 'PASTE_LATEST_ID_HERE'
GROUP BY current_method
ORDER BY disagreed DESC;

-- Confident disagreements ordered by impact (biggest blast radius first)
SELECT raw_question_en, current_std_q, current_method, predicted_std_q,
       ROUND(predicted_confidence, 3) AS conf, rows_affected, distinct_surveys
FROM `bi-data-391216.limesurvey_data.ml_audit_predictions`
WHERE audit_run_id = 'PASTE_LATEST_ID_HERE'
  AND agreement = 'differs'
  AND predicted_confidence >= 0.40
ORDER BY rows_affected DESC
LIMIT 100;

-- Where the model has a confident pick but the current row is NULL
-- (these are rows the cascade fix should pick up next time)
SELECT raw_question_en, predicted_std_q, ROUND(predicted_confidence, 3) AS conf,
       rows_affected, distinct_surveys
FROM `bi-data-391216.limesurvey_data.ml_audit_predictions`
WHERE audit_run_id = 'PASTE_LATEST_ID_HERE'
  AND agreement = 'current_null'
  AND predicted_confidence >= 0.30
ORDER BY rows_affected DESC
LIMIT 100;

-- Pattern_rule rows: does the model agree with the hand-written rule?
SELECT raw_question_en, current_std_q, predicted_std_q,
       ROUND(predicted_confidence, 3) AS conf, rows_affected
FROM `bi-data-391216.limesurvey_data.ml_audit_predictions`
WHERE audit_run_id = 'PASTE_LATEST_ID_HERE'
  AND current_method IN ('pattern_rule','subquestion_pattern','subquestion_parent_pattern')
ORDER BY rows_affected DESC
LIMIT 100;
"""


if __name__ == "__main__":
    main()
