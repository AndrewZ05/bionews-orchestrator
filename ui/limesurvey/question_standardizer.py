#!/usr/bin/env python3
"""
!!! DEPRECATED — use ui/limesurvey/limesurvey_mapper.py !!!
This standalone app is kept only for reference. It writes through a different code
path with different schema/lock semantics than the unified mapper, which causes
drift. Do not run concurrently with the main mapper.

Question Standardizer - Row-by-Row Interface

A Streamlit app for standardizing LimeSurvey questions where standardized_question is blank.
Shows unique raw_question_en values and suggests existing standardized_question options.

Features:
1. View unique unmapped questions with row counts
2. Fuzzy matching suggestions from existing standardized_questions
3. Immediate BigQuery UPDATE when selection is made
4. Create new standardized_question values on the fly
5. Progress tracking
6. ML Review tab - train models and detect mismatches

Usage:
    streamlit run ui/limesurvey/question_standardizer.py
"""

import os
import streamlit as st
import pandas as pd
import subprocess
import sys
import numpy as np
import pickle
import hashlib
from datetime import datetime
from google.cloud import bigquery
from pathlib import Path
from rapidfuzz import fuzz, process

# Try to import sentence-transformers for ML-based matching
try:
    from sentence_transformers import SentenceTransformer

    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

# Try to import sklearn for ML model
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder

    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Load environment variables
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    from dotenv import load_dotenv

    load_dotenv(env_path)

# Configuration
PROJECT_ID = "bi-data-391216"
DATASET = "limesurvey_data"

# Table names
QUESTIONS_LOOKUP = f"{PROJECT_ID}.{DATASET}.standardized_questions_lookup"
QUESTION_MAPPING = f"{PROJECT_ID}.{DATASET}.question_mapping"
COLUMNAR_COMPLETED = f"{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed"
CATEGORIES_LOOKUP = f"{PROJECT_ID}.{DATASET}.categories_lookup"
ML_MODEL_TABLE = f"{PROJECT_ID}.{DATASET}.ml_question_model"
ML_REVIEW_TABLE = f"{PROJECT_ID}.{DATASET}.lime_surveys_ml_mismatch_review"

# Local model storage
# Portable: LIMESURVEY_MODEL_DIR env var overrides; else <repo_root>/models.
MODEL_DIR = Path(
    os.environ.get("LIMESURVEY_MODEL_DIR")
    or (Path(__file__).resolve().parent.parent.parent / "models")
)
MODEL_FILE = MODEL_DIR / "question_classifier.pkl"
ENCODER_FILE = MODEL_DIR / "label_encoder.pkl"

# ML constants
MIN_CONFIDENCE = 0.70  # Only flag if ML is at least 70% confident


@st.cache_resource
def get_bq_client():
    """Create BigQuery client (cached)."""
    return bigquery.Client(project=PROJECT_ID)


# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================


@st.cache_data(ttl=60)
def get_unmapped_questions(_client) -> pd.DataFrame:
    """
    Get unique raw_question_en values where standardized_question is NULL.
    Returns DataFrame with raw_question_en, row_count, sample_surveys.
    """
    query = f"""
    SELECT
        raw_question_en,
        COUNT(*) as row_count,
        COUNT(DISTINCT CONCAT(CAST(survey_id AS STRING), '-', response_id)) as response_count,
        ARRAY_AGG(DISTINCT survey_id ORDER BY survey_id LIMIT 5) as sample_surveys,
        MIN(submitdate) as first_seen,
        MAX(submitdate) as last_seen
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_question IS NULL
      AND raw_question_en IS NOT NULL
      AND TRIM(raw_question_en) != ''
    GROUP BY raw_question_en
    ORDER BY row_count DESC
    """
    return _client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def get_existing_standardized_questions(_client) -> list:
    """
    Get all standardized_question values from the lookup table.
    The lookup table is the single source of truth - use sync_lookup_tables.py
    to keep it updated with any values in columnar_completed.
    """
    query = f"""
    SELECT DISTINCT standardized_question
    FROM `{QUESTIONS_LOOKUP}`
    WHERE standardized_question IS NOT NULL
    ORDER BY standardized_question
    """
    result = _client.query(query).to_dataframe()
    return result["standardized_question"].tolist()


@st.cache_data(ttl=300)
def get_categories(_client) -> list:
    """Get all categories from lookup table."""
    query = f"""
    SELECT category FROM `{CATEGORIES_LOOKUP}` ORDER BY sort_order
    """
    result = _client.query(query).to_dataframe()
    return result["category"].tolist() if not result.empty else ["other"]


@st.cache_data(ttl=60)
def get_questions_with_categories(_client) -> pd.DataFrame:
    """Get all standardized questions with their categories from lookup table."""
    query = f"""
    SELECT
        q.standardized_question,
        q.display_name,
        q.category,
        COALESCE(stats.row_count, 0) as row_count
    FROM `{QUESTIONS_LOOKUP}` q
    LEFT JOIN (
        SELECT standardized_question, COUNT(*) as row_count
        FROM `{COLUMNAR_COMPLETED}`
        WHERE standardized_question IS NOT NULL
        GROUP BY standardized_question
    ) stats ON q.standardized_question = stats.standardized_question
    ORDER BY q.category, q.standardized_question
    """
    return _client.query(query).to_dataframe()


def update_question_category(
    client, standardized_question: str, new_category: str
) -> bool:
    """Update the category for a standardized question in the lookup table."""
    query = f"""
    UPDATE `{QUESTIONS_LOOKUP}`
    SET category = @new_category,
        updated_at = CURRENT_TIMESTAMP()
    WHERE standardized_question = @std_question
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "std_question", "STRING", standardized_question
            ),
            bigquery.ScalarQueryParameter("new_category", "STRING", new_category),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"Error updating category: {e}")
        return False


def add_new_category(client, category: str, display_name: str = None) -> bool:
    """Add a new category to the categories lookup table."""
    query = f"""
    INSERT INTO `{CATEGORIES_LOOKUP}` (category, display_name, sort_order)
    SELECT @category, @display_name,
           COALESCE((SELECT MAX(sort_order) + 1 FROM `{CATEGORIES_LOOKUP}`), 1)
    WHERE NOT EXISTS (
        SELECT 1 FROM `{CATEGORIES_LOOKUP}` WHERE category = @category
    )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("category", "STRING", category),
            bigquery.ScalarQueryParameter(
                "display_name",
                "STRING",
                display_name or category.replace("_", " ").title(),
            ),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"Error adding category: {e}")
        return False


def get_sample_answers(client, raw_question: str, limit: int = 10) -> pd.DataFrame:
    """Get sample answers for a given raw question to help identify what it is."""
    query = f"""
    SELECT DISTINCT
        raw_answer_en,
        answer_value,
        COUNT(*) as count
    FROM `{COLUMNAR_COMPLETED}`
    WHERE raw_question_en = @raw_question
      AND raw_answer_en IS NOT NULL
      AND TRIM(raw_answer_en) != ''
    GROUP BY raw_answer_en, answer_value
    ORDER BY count DESC
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("raw_question", "STRING", raw_question),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


# ============================================================================
# ML-BASED SEMANTIC MATCHING
# ============================================================================


def snake_case_to_readable(text: str) -> str:
    """Convert snake_case to readable text for semantic matching."""
    return text.replace("_", " ").lower()


@st.cache_resource
def get_sentence_model():
    """Load sentence transformer model (cached)."""
    if not SENTENCE_TRANSFORMERS_AVAILABLE:
        return None
    # Use a lightweight but effective model
    return SentenceTransformer("all-MiniLM-L6-v2")


@st.cache_data(ttl=3600)
def compute_question_embeddings(_model, questions: tuple) -> np.ndarray:
    """
    Compute embeddings for standardized questions.
    Converts snake_case to readable text before embedding.
    """
    if _model is None:
        return None
    # Convert snake_case to readable text for better semantic matching
    readable_questions = [snake_case_to_readable(q) for q in questions]
    return _model.encode(readable_questions, convert_to_numpy=True)


def get_semantic_suggestions(
    raw_question: str, existing_questions: list, top_n: int = 10, threshold: float = 0.3
) -> list:
    """
    Get ML-based semantic match suggestions using sentence-transformers.
    Returns list of tuples: (standardized_question, score as percentage)
    """
    if not existing_questions:
        return []

    model = get_sentence_model()
    if model is None:
        return []

    # Get cached embeddings for existing questions
    questions_tuple = tuple(existing_questions)  # Convert to tuple for caching
    question_embeddings = compute_question_embeddings(model, questions_tuple)

    if question_embeddings is None:
        return []

    # Compute embedding for the raw question
    raw_embedding = model.encode([raw_question], convert_to_numpy=True)[0]

    # Compute cosine similarities
    similarities = np.dot(question_embeddings, raw_embedding) / (
        np.linalg.norm(question_embeddings, axis=1) * np.linalg.norm(raw_embedding)
    )

    # Get top N matches above threshold
    top_indices = np.argsort(similarities)[::-1][:top_n]

    results = []
    for idx in top_indices:
        score = similarities[idx]
        if score >= threshold:
            # Convert to percentage (0-100 scale like fuzzy matching)
            results.append((existing_questions[idx], int(score * 100)))

    return results


# ============================================================================
# FUZZY MATCHING (FALLBACK)
# ============================================================================


def get_question_suggestions(
    raw_question: str, existing_questions: list, top_n: int = 5, threshold: int = 50
) -> list:
    """
    Get fuzzy match suggestions for a raw question.
    Returns list of tuples: (standardized_question, score)
    """
    if not existing_questions:
        return []

    matches = process.extract(
        raw_question, existing_questions, scorer=fuzz.token_sort_ratio, limit=top_n
    )

    return [(match, score) for match, score, _ in matches if score >= threshold]


def get_combined_suggestions(
    raw_question: str, existing_questions: list, top_n: int = 10, use_ml: bool = True
) -> list:
    """
    Get suggestions using ML semantic matching (primary) with fuzzy matching fallback.
    Returns list of tuples: (standardized_question, score, method)
    """
    results = []
    seen = set()

    # Try ML-based semantic matching first
    if use_ml and SENTENCE_TRANSFORMERS_AVAILABLE:
        semantic_results = get_semantic_suggestions(
            raw_question, existing_questions, top_n=top_n, threshold=0.25
        )
        for q, score in semantic_results:
            if q not in seen:
                results.append((q, score, "semantic"))
                seen.add(q)

    # Also get fuzzy matches (they may catch different patterns)
    # Convert snake_case to readable for better fuzzy matching too
    readable_map = {snake_case_to_readable(q): q for q in existing_questions}
    readable_questions = list(readable_map.keys())

    if readable_questions:
        fuzzy_matches = process.extract(
            raw_question.lower(),
            readable_questions,
            scorer=fuzz.token_sort_ratio,
            limit=top_n,
        )

        for readable, score, _ in fuzzy_matches:
            original = readable_map[readable]
            if original not in seen and score >= 40:
                results.append((original, score, "fuzzy"))
                seen.add(original)

    # Sort by score descending
    results.sort(key=lambda x: x[1], reverse=True)

    return results[:top_n]


# ============================================================================
# UPDATE FUNCTIONS
# ============================================================================


def apply_question_mapping(
    client,
    raw_question: str,
    standardized_question: str,
    category: str = None,
    match_type: str = "manual_ui",
) -> int:
    """
    Apply question mapping:
    1. Try to insert into question_mapping table (optional - for future reference)
    2. Update all matching rows in columnar_completed

    Returns: Number of rows updated
    """
    # 1. Try to insert into question_mapping (optional - table may not exist)
    try:
        insert_query = f"""
        INSERT INTO `{QUESTION_MAPPING}`
        (raw_question, standardized_question, match_type, created_by)
        SELECT @raw_question, @std_question, @match_type, 'question_standardizer_ui'
        WHERE NOT EXISTS (
            SELECT 1 FROM `{QUESTION_MAPPING}`
            WHERE raw_question = @raw_question
        )
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("raw_question", "STRING", raw_question),
                bigquery.ScalarQueryParameter(
                    "std_question", "STRING", standardized_question
                ),
                bigquery.ScalarQueryParameter("match_type", "STRING", match_type),
            ]
        )

        client.query(insert_query, job_config=job_config).result()
    except Exception:
        # question_mapping table doesn't exist - that's fine, continue with update
        pass

    # 2. Update columnar_completed
    update_query = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_question = @std_question,
        category = COALESCE(@category, category),
        question_match_method = @match_type,
        updated_at = CURRENT_TIMESTAMP()
    WHERE raw_question_en = @raw_question
      AND standardized_question IS NULL
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("raw_question", "STRING", raw_question),
            bigquery.ScalarQueryParameter(
                "std_question", "STRING", standardized_question
            ),
            bigquery.ScalarQueryParameter("category", "STRING", category),
            bigquery.ScalarQueryParameter("match_type", "STRING", match_type),
        ]
    )

    job = client.query(update_query, job_config=job_config)
    job.result()

    return job.num_dml_affected_rows


def add_new_standardized_question(
    client, standardized_question: str, display_name: str, category: str
) -> bool:
    """Add a new standardized question to the lookup table."""
    query = f"""
    INSERT INTO `{QUESTIONS_LOOKUP}`
    (standardized_question, display_name, category, description)
    SELECT @std_question, @display_name, @category, ''
    WHERE NOT EXISTS (
        SELECT 1 FROM `{QUESTIONS_LOOKUP}`
        WHERE standardized_question = @std_question
    )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "std_question", "STRING", standardized_question
            ),
            bigquery.ScalarQueryParameter(
                "display_name",
                "STRING",
                display_name or standardized_question.replace("_", " ").title(),
            ),
            bigquery.ScalarQueryParameter("category", "STRING", category),
        ]
    )

    try:
        client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"Error adding question: {e}")
        return False


def skip_question(client, raw_question: str) -> int:
    """Mark question as skipped/reviewed but not mappable."""
    return apply_question_mapping(
        client, raw_question, "__SKIP__", match_type="skipped"
    )


# ============================================================================
# ML MODEL FUNCTIONS
# ============================================================================


def ensure_ml_tables_exist(client):
    """Create ML-related tables if they don't exist."""
    # Model metadata table
    ddl_model = f"""
    CREATE TABLE IF NOT EXISTS `{ML_MODEL_TABLE}` (
        model_id STRING,
        model_path STRING,
        accuracy FLOAT64,
        trained_at TIMESTAMP,
        training_rows INT64
    )
    """
    client.query(ddl_model).result()

    # Mismatch review table
    ddl_review = f"""
    CREATE TABLE IF NOT EXISTS `{ML_REVIEW_TABLE}` (
        id STRING,
        current_std_question STRING,
        ml_predicted_std_question STRING,
        ml_confidence FLOAT64,
        raw_question_en STRING,
        affected_row_count INT64,
        revised_std_question STRING,
        status STRING,
        detected_at TIMESTAMP
    )
    """
    client.query(ddl_review).result()


@st.cache_data(ttl=60)
def get_ml_model_status(_client) -> dict:
    """Get the current ML model status from BigQuery."""
    ensure_ml_tables_exist(_client)

    query = f"""
    SELECT model_id, accuracy, trained_at, training_rows
    FROM `{ML_MODEL_TABLE}`
    ORDER BY trained_at DESC
    LIMIT 1
    """

    results = list(_client.query(query).result())

    if not results:
        return {
            "exists": False,
            "model_id": None,
            "accuracy": None,
            "trained_at": None,
            "training_rows": None,
            "local_files_exist": MODEL_FILE.exists() and ENCODER_FILE.exists(),
        }

    row = results[0]
    return {
        "exists": True,
        "model_id": row.model_id,
        "accuracy": row.accuracy,
        "trained_at": row.trained_at,
        "training_rows": row.training_rows,
        "local_files_exist": MODEL_FILE.exists() and ENCODER_FILE.exists(),
    }


def train_ml_model(client, progress_callback=None) -> dict:
    """
    Train the ML model for question classification.
    Returns dict with training results.
    """
    if not SENTENCE_TRANSFORMERS_AVAILABLE:
        return {"success": False, "error": "sentence-transformers not installed"}

    if not SKLEARN_AVAILABLE:
        return {"success": False, "error": "scikit-learn not installed"}

    ensure_ml_tables_exist(client)

    if progress_callback:
        progress_callback("Loading training data from BigQuery...")

    # Get training data
    query = f"""
    SELECT DISTINCT
        raw_question_en,
        standardized_question
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_question IS NOT NULL
      AND raw_question_en IS NOT NULL
      AND TRIM(raw_question_en) != ''
      AND standardized_question NOT IN ('__SKIP__', '__REVIEW__')
    """

    df = client.query(query).to_dataframe()

    if len(df) < 10:
        return {
            "success": False,
            "error": f"Not enough training data (need at least 10 examples, got {len(df)})",
        }

    questions = df["raw_question_en"].tolist()
    labels = df["standardized_question"].tolist()

    if progress_callback:
        progress_callback(
            f"Loaded {len(df)} training examples. Loading transformer model..."
        )

    # Generate embeddings
    transformer = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    if progress_callback:
        progress_callback("Generating embeddings...")

    embeddings = transformer.encode(questions, show_progress_bar=False)

    if progress_callback:
        progress_callback("Encoding labels...")

    # Encode labels
    label_encoder = LabelEncoder()
    encoded_labels = label_encoder.fit_transform(labels)

    num_classes = len(label_encoder.classes_)

    if progress_callback:
        progress_callback(
            f"Training RandomForestClassifier on {num_classes} classes..."
        )

    # Train model
    model = RandomForestClassifier(
        n_estimators=100, max_depth=10, n_jobs=-1, random_state=42
    )
    model.fit(embeddings, encoded_labels)

    # Calculate accuracy
    predictions = model.predict(embeddings)
    accuracy = (predictions == encoded_labels).mean()

    if progress_callback:
        progress_callback(
            f"Training complete. Accuracy: {accuracy:.2%}. Saving model..."
        )

    # Save locally
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    with open(MODEL_FILE, "wb") as f:
        pickle.dump(model, f)

    with open(ENCODER_FILE, "wb") as f:
        pickle.dump(label_encoder, f)

    # Track metadata in BigQuery
    model_id = f"model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Clear old metadata
    client.query(f"DELETE FROM `{ML_MODEL_TABLE}` WHERE TRUE").result()

    # Insert new metadata
    insert_query = f"""
    INSERT INTO `{ML_MODEL_TABLE}`
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

    client.query(insert_query, job_config=job_config).result()

    return {
        "success": True,
        "model_id": model_id,
        "accuracy": accuracy,
        "training_rows": len(df),
        "num_classes": num_classes,
    }


def load_ml_model_for_prediction():
    """Load the trained ML model for prediction."""
    if not MODEL_FILE.exists() or not ENCODER_FILE.exists():
        return None, None, None

    if not SENTENCE_TRANSFORMERS_AVAILABLE:
        return None, None, None

    try:
        with open(MODEL_FILE, "rb") as f:
            model = pickle.load(f)

        with open(ENCODER_FILE, "rb") as f:
            encoder = pickle.load(f)

        transformer = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

        return model, encoder, transformer
    except Exception:
        return None, None, None


def run_mismatch_detection(
    client, min_confidence: float = MIN_CONFIDENCE, progress_callback=None
) -> dict:
    """
    Run ML-based mismatch detection.
    Compares ML predictions with current standardized_question values.
    Returns dict with results.
    """
    ensure_ml_tables_exist(client)

    model, encoder, transformer = load_ml_model_for_prediction()

    if model is None:
        return {
            "success": False,
            "error": "ML model not found. Please train the model first.",
        }

    if progress_callback:
        progress_callback("Loading questions from BigQuery...")

    # Get all unique questions with their current standardized_question
    query = f"""
    SELECT
        standardized_question as current_std_question,
        raw_question_en,
        COUNT(*) as row_count
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_question IS NOT NULL
      AND raw_question_en IS NOT NULL
      AND TRIM(raw_question_en) != ''
      AND standardized_question NOT IN ('__SKIP__', '__REVIEW__')
    GROUP BY standardized_question, raw_question_en
    ORDER BY standardized_question, row_count DESC
    """

    df = client.query(query).to_dataframe()

    if df.empty:
        return {"success": True, "mismatches": [], "total_checked": 0}

    if progress_callback:
        progress_callback(f"Generating embeddings for {len(df)} questions...")

    # Generate embeddings for all questions at once
    questions = df["raw_question_en"].tolist()
    embeddings = transformer.encode(questions, show_progress_bar=False)

    if progress_callback:
        progress_callback("Running predictions...")

    mismatches = []

    for idx, (_, row) in enumerate(df.iterrows()):
        raw_q = row["raw_question_en"]
        current = row["current_std_question"]
        row_count = row["row_count"]

        try:
            proba = model.predict_proba([embeddings[idx]])[0]
            predicted_idx = proba.argmax()
            confidence = proba[predicted_idx]
            predicted = encoder.inverse_transform([predicted_idx])[0]

            # Check if ML disagrees with current classification
            if predicted != current and confidence >= min_confidence:
                mismatches.append(
                    {
                        "id": hashlib.md5(f"{current}:{raw_q}".encode()).hexdigest()[
                            :16
                        ],
                        "current_std_question": current,
                        "ml_predicted_std_question": predicted,
                        "ml_confidence": float(confidence),
                        "raw_question_en": raw_q,
                        "affected_row_count": row_count,
                        "revised_std_question": None,
                        "status": "pending_review",
                        "detected_at": datetime.utcnow().isoformat(),
                    }
                )
        except Exception:
            continue

    if progress_callback:
        progress_callback(f"Found {len(mismatches)} mismatches. Saving to BigQuery...")

    # Save to BigQuery
    if mismatches:
        # Truncate and reload
        client.query(f"TRUNCATE TABLE `{ML_REVIEW_TABLE}`").result()

        errors = client.insert_rows_json(ML_REVIEW_TABLE, mismatches)
        if errors:
            return {"success": False, "error": f"Error inserting rows: {errors[:3]}"}
    else:
        # Clear the table if no mismatches
        client.query(f"TRUNCATE TABLE `{ML_REVIEW_TABLE}`").result()

    return {"success": True, "mismatches": mismatches, "total_checked": len(df)}


@st.cache_data(ttl=60)
def get_ml_mismatches(_client) -> pd.DataFrame:
    """Get current ML mismatches from the review table."""
    ensure_ml_tables_exist(_client)

    query = f"""
    SELECT *
    FROM `{ML_REVIEW_TABLE}`
    WHERE status = 'pending_review'
    ORDER BY ml_confidence DESC, affected_row_count DESC
    """

    return _client.query(query).to_dataframe()


def apply_ml_correction(
    client, mismatch_id: str, corrected_value: str, apply_to_data: bool = True
) -> dict:
    """
    Apply a correction from ML review.

    Args:
        client: BigQuery client
        mismatch_id: The mismatch ID
        corrected_value: The correct standardized_question value ('keep_current', 'use_ml', or custom)
        apply_to_data: Whether to also update the data in columnar_completed

    Returns:
        dict with result info
    """
    # Get the mismatch details
    query = f"""
    SELECT * FROM `{ML_REVIEW_TABLE}`
    WHERE id = @mismatch_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("mismatch_id", "STRING", mismatch_id)
        ]
    )

    results = list(client.query(query, job_config=job_config).result())

    if not results:
        return {"success": False, "error": "Mismatch not found"}

    mismatch = dict(results[0])

    # Determine the final value
    if corrected_value == "keep_current":
        final_value = mismatch["current_std_question"]
        status = "confirmed_current"
    elif corrected_value == "use_ml":
        final_value = mismatch["ml_predicted_std_question"]
        status = "accepted_ml"
    else:
        final_value = corrected_value
        status = "manual_correction"

    # Update the review table
    update_query = f"""
    UPDATE `{ML_REVIEW_TABLE}`
    SET revised_std_question = @final_value,
        status = @status
    WHERE id = @mismatch_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("final_value", "STRING", final_value),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("mismatch_id", "STRING", mismatch_id),
        ]
    )

    client.query(update_query, job_config=job_config).result()

    rows_updated = 0

    # Apply to data if requested and the value changed
    if apply_to_data and final_value != mismatch["current_std_question"]:
        data_update = f"""
        UPDATE `{COLUMNAR_COMPLETED}`
        SET standardized_question = @final_value,
            question_match_method = 'ml_correction',
            updated_at = CURRENT_TIMESTAMP()
        WHERE raw_question_en = @raw_question
          AND standardized_question = @current_value
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("final_value", "STRING", final_value),
                bigquery.ScalarQueryParameter(
                    "raw_question", "STRING", mismatch["raw_question_en"]
                ),
                bigquery.ScalarQueryParameter(
                    "current_value", "STRING", mismatch["current_std_question"]
                ),
            ]
        )

        job = client.query(data_update, job_config=job_config)
        job.result()
        rows_updated = job.num_dml_affected_rows or 0

    return {
        "success": True,
        "status": status,
        "final_value": final_value,
        "rows_updated": rows_updated,
    }


# ============================================================================
# STREAMLIT UI
# ============================================================================


def show_category_management(client):
    """Show the category management page."""
    st.header("Manage Categories")
    st.caption("View and update categories for standardized questions")

    # Get questions with categories
    questions_df = get_questions_with_categories(client)
    categories = get_categories(client)

    if questions_df.empty:
        st.warning("No standardized questions found. Run sync first.")
        return

    # Add new category section
    with st.expander("Add New Category"):
        col1, col2 = st.columns(2)
        with col1:
            new_cat = st.text_input(
                "Category (snake_case)", placeholder="e.g., lifestyle"
            )
        with col2:
            new_cat_display = st.text_input(
                "Display Name", placeholder="e.g., Lifestyle"
            )

        if st.button("Add Category", disabled=not new_cat):
            if " " in new_cat or new_cat != new_cat.lower():
                st.error("Use snake_case format")
            elif add_new_category(client, new_cat, new_cat_display):
                st.success(f"Added category '{new_cat}'")
                get_categories.clear()
                st.rerun()

    st.divider()

    # Filter by category
    col1, col2 = st.columns([2, 1])
    with col1:
        filter_cat = st.selectbox("Filter by category", ["All"] + categories)
    with col2:
        search_q = st.text_input("Search questions", "")

    # Apply filters
    filtered_df = questions_df.copy()
    if filter_cat != "All":
        filtered_df = filtered_df[filtered_df["category"] == filter_cat]
    if search_q:
        filtered_df = filtered_df[
            filtered_df["standardized_question"].str.contains(
                search_q, case=False, na=False
            )
        ]

    st.write(f"Showing {len(filtered_df)} questions")

    # Display questions grouped by category
    for idx, row in filtered_df.iterrows():
        col1, col2, col3, col4 = st.columns([3, 2, 1, 1])

        with col1:
            st.markdown(f"`{row['standardized_question']}`")

        with col2:
            current_cat = row["category"] or "other"
            new_cat = st.selectbox(
                "Category",
                categories,
                index=categories.index(current_cat) if current_cat in categories else 0,
                key=f"cat_{row['standardized_question']}",
                label_visibility="collapsed",
            )

        with col3:
            st.caption(f"{row['row_count']:,} rows")

        with col4:
            if new_cat != current_cat:
                if st.button(
                    "Save", key=f"save_{row['standardized_question']}", type="primary"
                ):
                    if update_question_category(
                        client, row["standardized_question"], new_cat
                    ):
                        st.toast(f"Updated category to '{new_cat}'", icon="✅")
                        get_questions_with_categories.clear()
                        st.rerun()

    st.divider()

    # Sync categories to columnar button
    st.subheader("Sync Categories to Data")
    st.caption("Push category changes from lookup table to columnar_completed")

    if st.button("Sync Categories", type="primary"):
        with st.spinner("Syncing categories..."):
            script_path = (
                Path(__file__).parent.parent.parent / "shared" / "sync_lookup_tables.py"
            )
            result = subprocess.run(
                [sys.executable, str(script_path), "--categories"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                st.success("Categories synced!")
                st.code(result.stdout[-1000:], language=None)
            else:
                st.error(f"Sync failed: {result.stderr[-500:]}")


def show_mapping_page(client):
    """Show the question mapping page."""
    # Get unmapped questions
    unmapped_df = get_unmapped_questions(client)

    if unmapped_df.empty:
        st.success("All questions have been standardized! Nothing left to process.")
        return

    # Initialize session state
    if "current_index" not in st.session_state:
        st.session_state.current_index = 0
    if "search_filter" not in st.session_state:
        st.session_state.search_filter = ""

    # Sidebar content
    with st.sidebar:
        st.header("Progress")
        total = len(unmapped_df)
        st.metric("Unmapped Questions", f"{total:,}")

        st.divider()

        st.header("Search/Filter")
        search = st.text_input("Search raw questions", st.session_state.search_filter)
        if search != st.session_state.search_filter:
            st.session_state.search_filter = search
            st.session_state.current_index = 0
            st.rerun()

        st.divider()

        if st.button("Refresh Screen", use_container_width=True):
            get_unmapped_questions.clear()
            get_existing_standardized_questions.clear()
            st.rerun()

        st.divider()

        st.header("Sync Lookup Tables")
        st.caption("Sync standardized values to lookup tables")

        col_sync1, col_sync2 = st.columns(2)
        with col_sync1:
            if st.button(
                "Preview", use_container_width=True, help="Preview what will be synced"
            ):
                with st.spinner("Checking..."):
                    script_path = (
                        Path(__file__).parent.parent.parent
                        / "shared"
                        / "sync_lookup_tables.py"
                    )
                    result = subprocess.run(
                        [sys.executable, str(script_path), "--all", "--dry-run"],
                        capture_output=True,
                        text=True,
                    )
                    if result.stdout:
                        st.code(result.stdout[-2000:], language=None)
                    if result.returncode != 0 and result.stderr:
                        st.error(result.stderr[-500:])

        with col_sync2:
            if st.button(
                "Sync Now",
                use_container_width=True,
                type="primary",
                help="Sync lookup tables",
            ):
                with st.spinner("Syncing..."):
                    script_path = (
                        Path(__file__).parent.parent.parent
                        / "shared"
                        / "sync_lookup_tables.py"
                    )
                    result = subprocess.run(
                        [sys.executable, str(script_path), "--all"],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode == 0:
                        st.success("Sync complete!")
                        get_existing_standardized_questions.clear()
                        st.rerun()
                    else:
                        st.error(f"Sync failed: {result.stderr[-500:]}")

    # Apply search filter
    if st.session_state.search_filter:
        unmapped_df = unmapped_df[
            unmapped_df["raw_question_en"].str.contains(
                st.session_state.search_filter, case=False, na=False
            )
        ]

    if unmapped_df.empty:
        st.warning("No questions match your search filter.")
        return

    total_filtered = len(unmapped_df)

    # Ensure index is within bounds
    if st.session_state.current_index >= total_filtered:
        st.session_state.current_index = 0

    # Navigation
    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])

    with col1:
        if st.button(
            "< Prev",
            disabled=st.session_state.current_index == 0,
            use_container_width=True,
        ):
            st.session_state.current_index -= 1
            st.rerun()

    with col2:
        if st.button(
            "Next >",
            disabled=st.session_state.current_index >= total_filtered - 1,
            use_container_width=True,
        ):
            st.session_state.current_index += 1
            st.rerun()

    with col3:
        st.markdown(
            f"**Question {st.session_state.current_index + 1} of {total_filtered:,}**"
        )

    with col4:
        if st.button("Skip", use_container_width=True):
            st.session_state.current_index = min(
                st.session_state.current_index + 1, total_filtered - 1
            )
            st.rerun()

    with col5:
        new_idx = st.number_input(
            "Go to",
            min_value=1,
            max_value=total_filtered,
            value=st.session_state.current_index + 1,
            label_visibility="collapsed",
        )
        if new_idx - 1 != st.session_state.current_index:
            st.session_state.current_index = new_idx - 1
            st.rerun()

    st.divider()

    # Current question data
    current_row = unmapped_df.iloc[st.session_state.current_index]
    raw_question = current_row["raw_question_en"]
    row_count = current_row["row_count"]

    # Main layout
    left_col, right_col = st.columns([1, 1])

    with left_col:
        st.subheader("Raw Question")

        with st.container(border=True):
            st.markdown(f"**Question Text:**")
            st.info(raw_question)

            st.markdown(f"**Affected Rows:** {row_count:,}")
            st.markdown(f"**Unique Responses:** {current_row['response_count']:,}")

            surveys = current_row.get("sample_surveys", [])
            if surveys:
                st.markdown(f"**Sample Surveys:** {', '.join(map(str, surveys[:5]))}")

        # Sample answers to help identify the question
        st.subheader("Sample Answers")
        st.caption("Answers can help identify what this question is asking")

        sample_answers = get_sample_answers(client, raw_question)
        if not sample_answers.empty:
            with st.container(border=True, height=250):
                for _, ans_row in sample_answers.iterrows():
                    ans_text = ans_row["raw_answer_en"]
                    ans_count = ans_row["count"]
                    st.markdown(f"- **{ans_text}** ({ans_count:,})")
        else:
            st.caption("No sample answers available")

    with right_col:
        # Get existing standardized questions
        existing_questions = get_existing_standardized_questions(client)
        categories = get_categories(client)

        # ML-based semantic suggestions
        st.subheader("Suggested Matches")

        # Show ML status
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            st.caption("🤖 Using ML semantic matching")
        else:
            st.caption(
                "⚠️ ML not available - install sentence-transformers for better matching"
            )

        suggestions = get_combined_suggestions(raw_question, existing_questions)

        if suggestions:
            for std_question, score, method in suggestions:
                col_score, col_q, col_method, col_btn = st.columns([1, 2.5, 0.8, 0.7])

                with col_score:
                    if score >= 70:
                        st.success(f"{score}%")
                    elif score >= 50:
                        st.warning(f"{score}%")
                    else:
                        st.caption(f"{score}%")

                with col_q:
                    # Show readable version in tooltip
                    readable = snake_case_to_readable(std_question)
                    st.markdown(f"`{std_question}`")
                    st.caption(f"_{readable}_")

                with col_method:
                    if method == "semantic":
                        st.caption("🧠")
                    else:
                        st.caption("🔤")

                with col_btn:
                    if st.button(
                        "Use", key=f"use_{std_question}", use_container_width=True
                    ):
                        match_type = (
                            "ml_semantic" if method == "semantic" else "fuzzy_match"
                        )
                        rows_updated = apply_question_mapping(
                            client, raw_question, std_question, match_type=match_type
                        )
                        st.toast(
                            f"Mapped {rows_updated:,} rows to '{std_question}'",
                            icon="✅",
                        )
                        get_unmapped_questions.clear()
                        st.rerun()
        else:
            st.caption("No matches found. Select from dropdown or create new below.")

        st.divider()

        # Select from all existing
        st.subheader("Select Existing")

        selected_question = st.selectbox(
            "Standardized question",
            [""] + existing_questions,
            key="select_existing",
            label_visibility="collapsed",
        )

        if st.button(
            "Apply Selected", disabled=not selected_question, use_container_width=True
        ):
            rows_updated = apply_question_mapping(
                client, raw_question, selected_question, match_type="manual_select"
            )
            st.toast(
                f"Mapped {rows_updated:,} rows to '{selected_question}'", icon="✅"
            )
            get_unmapped_questions.clear()
            st.rerun()

        st.divider()

        # Create new
        st.subheader("Create New Standardized Question")

        new_std_question = st.text_input(
            "Question name (snake_case)",
            placeholder="e.g., current_medications",
            help="Use lowercase with underscores",
        )

        new_display_name = st.text_input(
            "Display name", placeholder="e.g., Current Medications"
        )

        new_category = st.selectbox("Category", categories)

        if st.button(
            "Create & Apply",
            disabled=not new_std_question,
            use_container_width=True,
            type="primary",
        ):
            # Validate snake_case
            if " " in new_std_question or new_std_question != new_std_question.lower():
                st.error(
                    "Use snake_case format (lowercase with underscores, no spaces)"
                )
            else:
                # Add to lookup table first
                if add_new_standardized_question(
                    client, new_std_question, new_display_name, new_category
                ):
                    rows_updated = apply_question_mapping(
                        client,
                        raw_question,
                        new_std_question,
                        category=new_category,
                        match_type="new_manual",
                    )
                    st.toast(
                        f"Created '{new_std_question}' and mapped {rows_updated:,} rows",
                        icon="✅",
                    )
                    get_unmapped_questions.clear()
                    get_existing_standardized_questions.clear()
                    st.rerun()

        st.divider()

        # Skip option
        st.subheader("Skip Options")

        if st.button(
            "Mark as Skip (not mappable)",
            use_container_width=True,
            help="Mark this question as reviewed but not suitable for standardization",
        ):
            rows_updated = skip_question(client, raw_question)
            st.toast(f"Skipped {rows_updated:,} rows", icon="⏭️")
            get_unmapped_questions.clear()
            st.rerun()


def show_ml_review_page(client):
    """Show the ML Review page for model training and mismatch detection."""
    st.header("ML Review")

    # Check dependencies
    deps_ok = True
    if not SENTENCE_TRANSFORMERS_AVAILABLE:
        st.error(
            "sentence-transformers not installed. Run: pip install sentence-transformers"
        )
        deps_ok = False
    if not SKLEARN_AVAILABLE:
        st.error("scikit-learn not installed. Run: pip install scikit-learn")
        deps_ok = False

    if not deps_ok:
        return

    # Model Status Section
    st.subheader("Model Status")

    model_status = get_ml_model_status(client)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if model_status["exists"]:
            st.metric(
                "Model ID",
                (
                    model_status["model_id"][:20] + "..."
                    if model_status["model_id"]
                    else "None"
                ),
            )
        else:
            st.metric("Model ID", "Not trained")

    with col2:
        if model_status["accuracy"]:
            st.metric("Accuracy", f"{model_status['accuracy']:.1%}")
        else:
            st.metric("Accuracy", "N/A")

    with col3:
        if model_status["trained_at"]:
            st.metric(
                "Last Trained", model_status["trained_at"].strftime("%Y-%m-%d %H:%M")
            )
        else:
            st.metric("Last Trained", "Never")

    with col4:
        if model_status["training_rows"]:
            st.metric("Training Rows", f"{model_status['training_rows']:,}")
        else:
            st.metric("Training Rows", "0")

    # Local files status
    if model_status["local_files_exist"]:
        st.success("Local model files found")
    else:
        st.warning("No local model files - training required")

    st.divider()

    # Training Section
    st.subheader("Train Model")
    st.caption("Train a new ML model on current standardized question mappings")

    col_train1, col_train2 = st.columns([1, 3])

    with col_train1:
        if st.button("Retrain Model", type="primary", use_container_width=True):
            progress_text = st.empty()
            progress_bar = st.progress(0)

            def update_progress(msg):
                progress_text.text(msg)

            update_progress("Starting training...")
            progress_bar.progress(10)

            result = train_ml_model(client, progress_callback=update_progress)

            if result["success"]:
                progress_bar.progress(100)
                st.success(
                    f"Model trained successfully! Accuracy: {result['accuracy']:.1%} on {result['training_rows']:,} examples with {result['num_classes']} classes"
                )
                get_ml_model_status.clear()
                st.rerun()
            else:
                st.error(f"Training failed: {result['error']}")

    with col_train2:
        st.info(
            """
        **When to retrain:**
        - After adding many new question mappings
        - If mismatches seem incorrect
        - Periodically (weekly/monthly) as data grows

        The model learns from existing (raw_question, standardized_question) pairs.
        """
        )

    st.divider()

    # Mismatch Detection Section
    st.subheader("Mismatch Detection")
    st.caption("Find questions where ML disagrees with current classification")

    col_detect1, col_detect2, col_detect3 = st.columns([1, 1, 2])

    with col_detect1:
        min_conf = st.slider(
            "Min Confidence",
            min_value=0.5,
            max_value=0.95,
            value=MIN_CONFIDENCE,
            step=0.05,
            help="Only flag mismatches where ML is at least this confident",
        )

    with col_detect2:
        if st.button("Run Detection", use_container_width=True):
            if not model_status["local_files_exist"]:
                st.error("Train the model first before running detection")
            else:
                progress_text = st.empty()

                def update_progress(msg):
                    progress_text.text(msg)

                result = run_mismatch_detection(
                    client, min_confidence=min_conf, progress_callback=update_progress
                )

                if result["success"]:
                    st.success(
                        f"Checked {result['total_checked']:,} questions. Found {len(result['mismatches'])} mismatches."
                    )
                    get_ml_mismatches.clear()
                    st.rerun()
                else:
                    st.error(f"Detection failed: {result['error']}")

    st.divider()

    # Review Mismatches Section
    st.subheader("Review Mismatches")

    mismatches_df = get_ml_mismatches(client)

    if mismatches_df.empty:
        st.info("No pending mismatches to review. Run detection to find mismatches.")
        return

    st.write(f"**{len(mismatches_df)} mismatches pending review**")

    # Get existing questions for the dropdown
    existing_questions = get_existing_standardized_questions(client)

    # Display mismatches
    for idx, row in mismatches_df.iterrows():
        with st.container(border=True):
            # Header with confidence and row count
            col_conf, col_rows, col_actions = st.columns([1, 1, 2])

            with col_conf:
                confidence = row["ml_confidence"]
                if confidence >= 0.9:
                    st.success(f"ML Confidence: {confidence:.0%}")
                elif confidence >= 0.8:
                    st.warning(f"ML Confidence: {confidence:.0%}")
                else:
                    st.info(f"ML Confidence: {confidence:.0%}")

            with col_rows:
                st.metric("Affected Rows", f"{row['affected_row_count']:,}")

            # Raw question
            st.markdown("**Raw Question:**")
            raw_q = row["raw_question_en"]
            if len(raw_q) > 200:
                st.text(raw_q[:200] + "...")
            else:
                st.text(raw_q)

            # Current vs ML prediction
            col_current, col_ml = st.columns(2)

            with col_current:
                st.markdown("**Current:**")
                st.code(row["current_std_question"])

            with col_ml:
                st.markdown("**ML Predicts:**")
                st.code(row["ml_predicted_std_question"])

            # Actions
            st.markdown("**Resolution:**")
            col_keep, col_accept, col_custom = st.columns(3)

            mismatch_id = row["id"]

            with col_keep:
                if st.button(
                    "Keep Current", key=f"keep_{mismatch_id}", use_container_width=True
                ):
                    result = apply_ml_correction(
                        client, mismatch_id, "keep_current", apply_to_data=False
                    )
                    if result["success"]:
                        st.toast(
                            "Marked as confirmed - keeping current value", icon="check"
                        )
                        get_ml_mismatches.clear()
                        st.rerun()

            with col_accept:
                if st.button(
                    "Use ML Prediction",
                    key=f"accept_{mismatch_id}",
                    use_container_width=True,
                    type="primary",
                ):
                    result = apply_ml_correction(
                        client, mismatch_id, "use_ml", apply_to_data=True
                    )
                    if result["success"]:
                        st.toast(
                            f"Applied ML prediction. Updated {result['rows_updated']:,} rows.",
                            icon="check",
                        )
                        get_ml_mismatches.clear()
                        get_unmapped_questions.clear()
                        st.rerun()

            with col_custom:
                # Custom correction
                custom_value = st.selectbox(
                    "Or select different",
                    [""] + existing_questions,
                    key=f"custom_{mismatch_id}",
                    label_visibility="collapsed",
                )

                if custom_value:
                    if st.button(
                        "Apply Custom",
                        key=f"apply_custom_{mismatch_id}",
                        use_container_width=True,
                    ):
                        result = apply_ml_correction(
                            client, mismatch_id, custom_value, apply_to_data=True
                        )
                        if result["success"]:
                            st.toast(
                                f"Applied custom correction. Updated {result['rows_updated']:,} rows.",
                                icon="check",
                            )
                            get_ml_mismatches.clear()
                            get_unmapped_questions.clear()
                            st.rerun()


def main():
    st.set_page_config(
        page_title="Question Standardizer", page_icon="❓", layout="wide"
    )

    st.title("Question Standardizer")
    st.error(
        "⚠️ **This app is deprecated.** Use `streamlit run ui/limesurvey/limesurvey_mapper.py` instead. "
        "This standalone app writes to BigQuery through a different code path with different schema "
        "and lock semantics, which can corrupt the unified workflow."
    )

    client = get_bq_client()

    # Create tabs for different pages
    tab1, tab2, tab3 = st.tabs(["Map Questions", "Manage Categories", "ML Review"])

    with tab1:
        st.caption("Map raw questions to standardized question values")
        show_mapping_page(client)

    with tab2:
        show_category_management(client)

    with tab3:
        show_ml_review_page(client)


if __name__ == "__main__":
    main()
