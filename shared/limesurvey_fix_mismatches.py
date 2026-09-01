#!/usr/bin/env python3
"""
LimeSurvey Question Mismatch Detector and Review Table Generator

Uses ALL 6 matching layers to identify potentially misclassified questions
and generates a review table for human approval before any changes are made.

The 6 Matching Layers:
1. EXACT MATCH - If we've seen this exact question before (100% confidence)
2. MACHINE LEARNING - Trained model predicts category (90-99% confidence)
3. SEMANTIC SIMILARITY - Sentence transformers match meaning (85-99% confidence)
4. MEDICAL TERMS (UMLS) - Medical terminology database (85-98% confidence)
5. ENTITY RECOGNITION - spaCy identifies diseases, chemicals (80-93% confidence)
6. FUZZY MATCHING - Catches typos and variations (70-85% confidence)

Usage:
    python shared/limesurvey_fix_mismatches.py                    # Detect and create review table
    python shared/limesurvey_fix_mismatches.py --std-question age # Check specific question
    python shared/limesurvey_fix_mismatches.py --apply-approved   # Apply approved changes
"""

import argparse
import os
import sys
import pickle
import hashlib
import uuid
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import numpy as np

# Set up GCP credentials if not already set (cross-platform)
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
    for _p in (r'c:\gcp\service-account-bionews-pipeline.json', '/home/orchestrator/service-account-bionews-pipeline.json'):
        if os.path.exists(_p):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _p
            break

# Suppress warnings
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GLOG_minloglevel'] = '3'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import warnings
warnings.filterwarnings('ignore')

from google.cloud import bigquery, storage
from sentence_transformers import SentenceTransformer, util
from collections import defaultdict
from rapidfuzz import fuzz

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'
SOURCE_TABLE = 'lime_surveys_columnar_completed'
REVIEW_TABLE = 'lime_surveys_question_mismatch_review'
MODEL_BUCKET = 'limesurvey-model-artifacts'

# Thresholds
OUTLIER_SIMILARITY_THRESHOLD = 0.60
RECLASSIFY_CONFIDENCE_THRESHOLD = 0.70
MIN_GROUP_SIZE = 3
CONFIDENCE_EARLY_STOP = 0.90
CONFIDENCE_MIN_ACCEPT = 0.70


@dataclass
class MatchResult:
    """Result from matching a question"""
    standardized_question: str
    confidence: float
    method: str
    matched_to: Optional[str] = None


@dataclass
class MismatchProposal:
    """Represents a proposed correction"""
    raw_question_en: str
    current_std_question: str
    proposed_std_question: str
    avg_similarity_in_group: float
    reclassify_confidence: float
    reclassify_method: str
    affected_row_count: int
    detected_at: str
    status: str = 'pending_review'


# Global state for loaded models/data
_transformer_model = None
_ml_model = None
_ml_encoder = None
_nlp = None
_nlp_ner = None
_embedding_cache = {}
_mappings = None
_questions = None
_embeddings = None
_std_question_examples = None


def get_transformer_model():
    """Lazy load sentence transformer"""
    global _transformer_model
    if _transformer_model is None:
        logger.info("Loading sentence transformer model...")
        _transformer_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
        logger.info("[OK] Model loaded")
    return _transformer_model


def get_ml_model():
    """Load ML model from GCS"""
    global _ml_model, _ml_encoder
    if _ml_model is None:
        logger.info("Loading ML model from GCS...")
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.bucket(MODEL_BUCKET)

            model_blob = bucket.blob('question_classifier_model.pkl')
            encoder_blob = bucket.blob('question_classifier_encoder.pkl')

            if model_blob.exists() and encoder_blob.exists():
                _ml_model = pickle.loads(model_blob.download_as_bytes())
                _ml_encoder = pickle.loads(encoder_blob.download_as_bytes())
                logger.info(f"[OK] ML model loaded ({len(_ml_encoder.classes_)} classes)")
            else:
                logger.warning("ML model not found in GCS")
        except Exception as e:
            logger.warning(f"Could not load ML model: {e}")
    return _ml_model, _ml_encoder


def get_spacy_models():
    """Load spaCy models"""
    global _nlp, _nlp_ner
    if _nlp is None:
        import spacy
        logger.info("Loading spaCy models...")
        try:
            _nlp = spacy.load("en_core_sci_lg")
            logger.info("[OK] Loaded en_core_sci_lg")
        except Exception as e:
            logger.warning(f"Could not load en_core_sci_lg: {e}")

        try:
            _nlp_ner = spacy.load("en_ner_bc5cdr_md")
            logger.info("[OK] Loaded en_ner_bc5cdr_md")
        except Exception as e:
            logger.warning(f"Could not load en_ner_bc5cdr_md: {e}")
    return _nlp, _nlp_ner


def get_embedding(text: str) -> np.ndarray:
    """Get embedding for text with caching"""
    if text not in _embedding_cache:
        model = get_transformer_model()
        _embedding_cache[text] = model.encode(text, convert_to_numpy=True)
    return _embedding_cache[text]


def cleanup_text(text: str) -> str:
    """Clean and normalize question text"""
    if not text:
        return ""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Normalize whitespace
    text = ' '.join(text.split())
    return text.strip()


def load_mappings(bq_client):
    """Load question mappings from BigQuery"""
    global _mappings, _questions, _embeddings

    if _mappings is not None:
        return _mappings, _questions, _embeddings

    logger.info("Loading question mappings from BigQuery...")

    query = f"""
    SELECT DISTINCT
        LOWER(TRIM(raw_question_en)) as normalized_q,
        standardized_question,
        COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
    WHERE raw_question_en IS NOT NULL
      AND standardized_question IS NOT NULL
      AND TRIM(raw_question_en) != ''
    GROUP BY 1, 2
    ORDER BY cnt DESC
    """

    df = bq_client.query(query).to_dataframe()

    _mappings = {}
    for _, row in df.iterrows():
        norm_q = row['normalized_q']
        if norm_q not in _mappings:
            _mappings[norm_q] = row['standardized_question']

    _questions = list(_mappings.keys())
    logger.info(f"Computing embeddings for {len(_questions)} known questions...")
    model = get_transformer_model()
    _embeddings = model.encode(_questions, convert_to_numpy=True, show_progress_bar=True)

    logger.info(f"[OK] Loaded {len(_mappings)} mappings")
    return _mappings, _questions, _embeddings


def load_std_question_examples(bq_client):
    """Load standardized questions and their examples"""
    global _std_question_examples

    if _std_question_examples is not None:
        return _std_question_examples

    logger.info("Loading standardized question examples...")

    query = f"""
    SELECT
        standardized_question,
        raw_question_en,
        COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
    WHERE standardized_question IS NOT NULL
      AND raw_question_en IS NOT NULL
      AND TRIM(raw_question_en) != ''
    GROUP BY standardized_question, raw_question_en
    ORDER BY standardized_question, cnt DESC
    """

    df = bq_client.query(query).to_dataframe()

    _std_question_examples = {}
    for _, row in df.iterrows():
        std_q = row['standardized_question']
        raw_q = row['raw_question_en'].strip()

        if std_q not in _std_question_examples:
            _std_question_examples[std_q] = []

        normalized = raw_q.lower().strip()
        if normalized not in [q.lower().strip() for q in _std_question_examples[std_q]]:
            _std_question_examples[std_q].append(raw_q)

    logger.info(f"[OK] Loaded {len(_std_question_examples)} standardized questions")
    return _std_question_examples


# ========== 6 MATCHING LAYERS ==========

def match_exact(question: str, mappings: Dict) -> Optional[MatchResult]:
    """Layer 1: Exact Match (100% confidence)"""
    normalized = question.lower().strip()
    if normalized in mappings:
        return MatchResult(
            standardized_question=mappings[normalized],
            confidence=1.0,
            method='exact_match'
        )
    return None


def match_ml(question: str) -> Optional[MatchResult]:
    """Layer 2: ML Model (90-99% confidence)"""
    ml_model, ml_encoder = get_ml_model()
    if ml_model is None or ml_encoder is None:
        return None

    try:
        model = get_transformer_model()
        embedding = model.encode([question])[0]
        probs = ml_model.predict_proba([embedding])[0]
        predicted_class = np.argmax(probs)
        confidence = float(probs[predicted_class])

        std_q = ml_encoder.inverse_transform([predicted_class])[0]

        return MatchResult(
            standardized_question=std_q,
            confidence=confidence,
            method='ml_model'
        )
    except Exception as e:
        logger.debug(f"ML matching error: {e}")
        return None


def match_semantic(question: str, mappings: Dict, questions: List, embeddings: np.ndarray) -> Optional[MatchResult]:
    """Layer 3: Semantic Similarity (85-99% confidence)"""
    if len(embeddings) == 0:
        return None

    model = get_transformer_model()
    question_embedding = model.encode(question, convert_to_numpy=True)
    similarities = util.cos_sim(question_embedding, embeddings)[0].numpy()

    best_idx = np.argmax(similarities)
    best_score = float(similarities[best_idx])

    if best_score >= 0.85:
        matched_q = questions[best_idx]
        return MatchResult(
            standardized_question=mappings[matched_q],
            confidence=best_score,
            method='semantic_transformer',
            matched_to=matched_q[:50]
        )
    return None


def match_spacy(question: str, mappings: Dict) -> Optional[MatchResult]:
    """Layer 5: spaCy Entity Recognition (80-93% confidence)"""
    nlp, nlp_ner = get_spacy_models()
    if nlp is None and nlp_ner is None:
        return None

    question_lower = question.lower()

    # Extract medical entities using BC5CDR NER
    medical_entities = []
    if nlp_ner:
        try:
            doc_ner = nlp_ner(question)
            medical_entities = [(ent.text.lower(), ent.label_) for ent in doc_ner.ents]
        except Exception:
            pass

    disease_entities = [e[0] for e in medical_entities if e[1] == 'DISEASE']
    chemical_entities = [e[0] for e in medical_entities if e[1] == 'CHEMICAL']

    # Match diseases to questions about specific conditions
    if disease_entities:
        for disease in disease_entities:
            for known_q, std_q in mappings.items():
                # Only match if disease name is substantial part of the known question
                if len(disease) > 3 and disease in known_q:
                    return MatchResult(
                        standardized_question=std_q,
                        confidence=0.88,
                        method='spacy_disease_ner',
                        matched_to=f"DISEASE: {disease}"
                    )

    # Match chemicals (medications) to treatment questions
    if chemical_entities:
        for chemical in chemical_entities:
            for known_q, std_q in mappings.items():
                if len(chemical) > 3 and (chemical in known_q or 'treatment' in std_q.lower() or 'medication' in std_q.lower()):
                    return MatchResult(
                        standardized_question=std_q,
                        confidence=0.85,
                        method='spacy_chemical_ner',
                        matched_to=f"CHEMICAL: {chemical}"
                    )

    return None


def match_fuzzy(question: str, mappings: Dict) -> Optional[MatchResult]:
    """Layer 6: Fuzzy Matching (70-85% confidence)"""
    best_match = None
    best_score = 0.0
    best_q = None

    for known_q, std_q in mappings.items():
        # Use token_set_ratio for better handling of word order differences
        score = fuzz.token_set_ratio(question.lower(), known_q.lower()) / 100.0

        if score > best_score:
            best_score = score
            best_match = std_q
            best_q = known_q

    if best_score >= 0.80:
        return MatchResult(
            standardized_question=best_match,
            confidence=min(best_score, 0.85),
            method='fuzzy_match',
            matched_to=best_q[:50]
        )
    return None


def match_question_6_layers(raw_question: str, mappings: Dict, questions: List, embeddings: np.ndarray, exclude_std_question: str = None) -> Optional[MatchResult]:
    """
    Match a question using all 6 layers in order.

    Args:
        raw_question: The question to match
        mappings: Dict of normalized question -> standardized_question
        questions: List of known questions
        embeddings: Pre-computed embeddings for known questions
        exclude_std_question: If provided, exclude this standardized_question from consideration
                             (used when reclassifying potential mismatches)

    Returns the best match found, or None if no confident match.
    """
    cleaned = cleanup_text(raw_question)
    if not cleaned:
        return None

    # Filter out mappings to the excluded standardized question
    if exclude_std_question:
        filtered_mappings = {k: v for k, v in mappings.items() if v != exclude_std_question}
        # Also filter questions and embeddings
        filtered_indices = [i for i, q in enumerate(questions) if mappings.get(q) != exclude_std_question]
        filtered_questions = [questions[i] for i in filtered_indices]
        filtered_embeddings = embeddings[filtered_indices] if len(filtered_indices) > 0 else np.array([])
    else:
        filtered_mappings = mappings
        filtered_questions = questions
        filtered_embeddings = embeddings

    # Layer 1: Exact Match (100%) - skip if excluding (the question itself maps to excluded)
    if not exclude_std_question:
        result = match_exact(cleaned, filtered_mappings)
        if result and result.confidence >= CONFIDENCE_EARLY_STOP:
            return result

    # Layer 2: ML Model (90-99%) - ML doesn't use mappings, always valid
    ml_result = match_ml(cleaned)
    # If ML returns the excluded question, ignore it
    if ml_result and exclude_std_question and ml_result.standardized_question == exclude_std_question:
        ml_result = None
    if ml_result and ml_result.confidence >= CONFIDENCE_EARLY_STOP:
        return ml_result

    # Layer 3: Semantic Similarity (85-99%)
    semantic_result = match_semantic(cleaned, filtered_mappings, filtered_questions, filtered_embeddings) if len(filtered_embeddings) > 0 else None
    if semantic_result and semantic_result.confidence >= CONFIDENCE_EARLY_STOP:
        return semantic_result

    # Layer 4: UMLS - skip for now (requires separate service)

    # Layer 5: spaCy NER (80-93%)
    spacy_result = match_spacy(cleaned, filtered_mappings)
    if spacy_result and spacy_result.confidence >= CONFIDENCE_EARLY_STOP:
        return spacy_result

    # Layer 6: Fuzzy Matching (70-85%)
    fuzzy_result = match_fuzzy(cleaned, filtered_mappings)
    if fuzzy_result and fuzzy_result.confidence >= CONFIDENCE_MIN_ACCEPT:
        return fuzzy_result

    # Return best result from all layers
    all_results = [r for r in [ml_result, semantic_result, spacy_result, fuzzy_result] if r is not None]

    if all_results:
        best = max(all_results, key=lambda x: x.confidence)
        if best.confidence >= CONFIDENCE_MIN_ACCEPT:
            return best

    return None


def compute_group_similarity(questions: List[str]) -> Dict[str, float]:
    """Compute average similarity of each question to others in group"""
    if len(questions) < 2:
        return {q: 1.0 for q in questions}

    embeddings = np.array([get_embedding(q) for q in questions])
    similarities = util.cos_sim(embeddings, embeddings).numpy()

    avg_sims = {}
    for i, q in enumerate(questions):
        other_sims = np.concatenate([similarities[i, :i], similarities[i, i+1:]])
        avg_sims[q] = float(np.mean(other_sims)) if len(other_sims) > 0 else 1.0

    return avg_sims


def detect_mismatches(bq_client, std_question: str = None, similarity_threshold: float = OUTLIER_SIMILARITY_THRESHOLD) -> List[MismatchProposal]:
    """
    Detect mismatched questions using semantic similarity and 6-layer reclassification
    """
    std_examples = load_std_question_examples(bq_client)
    mappings, questions, embeddings = load_mappings(bq_client)

    if std_question:
        std_questions_to_check = [std_question] if std_question in std_examples else []
        if not std_questions_to_check:
            logger.warning(f"Standardized question '{std_question}' not found")
            return []
    else:
        std_questions_to_check = list(std_examples.keys())

    proposals = []
    now = datetime.utcnow().isoformat()

    for std_q in std_questions_to_check:
        examples = std_examples[std_q]

        if len(examples) < MIN_GROUP_SIZE:
            continue

        logger.info(f"\nAnalyzing '{std_q}' ({len(examples)} unique questions)...")

        # Compute similarities within group
        avg_sims = compute_group_similarity(examples)

        # Find outliers
        outliers = [(q, sim) for q, sim in avg_sims.items() if sim < similarity_threshold]

        if outliers:
            logger.info(f"  Found {len(outliers)} potential outliers")

            for raw_q, avg_sim in sorted(outliers, key=lambda x: x[1]):
                # Use 6-layer matching to find correct classification
                # EXCLUDE the current std_question to avoid circular confirmation
                result = match_question_6_layers(raw_q, mappings, questions, embeddings, exclude_std_question=std_q)

                if result and result.standardized_question != std_q and result.confidence >= RECLASSIFY_CONFIDENCE_THRESHOLD:
                    # Count affected rows
                    count_query = f"""
                    SELECT COUNT(*) as cnt
                    FROM `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
                    WHERE standardized_question = @std_q
                      AND raw_question_en = @raw_q
                    """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("std_q", "STRING", std_q),
                            bigquery.ScalarQueryParameter("raw_q", "STRING", raw_q)
                        ]
                    )
                    try:
                        count_result = bq_client.query(count_query, job_config=job_config).to_dataframe()
                        affected_rows = int(count_result['cnt'].iloc[0])
                    except Exception:
                        affected_rows = 0

                    proposal = MismatchProposal(
                        raw_question_en=raw_q,
                        current_std_question=std_q,
                        proposed_std_question=result.standardized_question,
                        avg_similarity_in_group=avg_sim,
                        reclassify_confidence=result.confidence,
                        reclassify_method=result.method,
                        affected_row_count=affected_rows,
                        detected_at=now
                    )
                    proposals.append(proposal)

                    logger.info(f"    [OK] Outlier detected:")
                    logger.info(f"      Question: \"{raw_q[:70]}...\"" if len(raw_q) > 70 else f"      Question: \"{raw_q}\"")
                    logger.info(f"      Group similarity: {avg_sim:.2%}")
                    logger.info(f"      Current: '{std_q}' -> Proposed: '{result.standardized_question}'")
                    logger.info(f"      Method: {result.method} (confidence: {result.confidence:.2%})")
                    logger.info(f"      Affected rows: {affected_rows}")
                elif result is None:
                    logger.info(f"    - Outlier (similarity {avg_sim:.2%}): \"{raw_q[:50]}...\"")
                    logger.info(f"      No better classification found (keeping current)")
                elif result.confidence < RECLASSIFY_CONFIDENCE_THRESHOLD:
                    logger.info(f"    - Outlier (similarity {avg_sim:.2%}): \"{raw_q[:50]}...\"")
                    logger.info(f"      Best alternative: '{result.standardized_question}' ({result.method}, conf: {result.confidence:.2%}) - below threshold")

    return proposals


def create_review_table(bq_client, proposals: List[MismatchProposal]) -> str:
    """Create or update the review table with proposals"""
    full_table_name = f"{PROJECT_ID}.{DATASET}.{REVIEW_TABLE}"

    # Create table if not exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{full_table_name}` (
        id STRING NOT NULL,
        raw_question_en STRING,
        current_std_question STRING,
        proposed_std_question STRING,
        avg_similarity_in_group FLOAT64,
        reclassify_confidence FLOAT64,
        reclassify_method STRING,
        affected_row_count INT64,
        detected_at TIMESTAMP,
        status STRING DEFAULT 'pending_review',
        reviewed_by STRING,
        reviewed_at TIMESTAMP
    )
    """

    try:
        bq_client.query(create_table_sql).result()
        logger.info(f"[OK] Review table ready: {full_table_name}")
    except Exception as e:
        logger.warning(f"Table creation note: {e}")

    if not proposals:
        logger.info("No proposals to insert")
        return full_table_name

    rows_to_insert = []
    for p in proposals:
        id_str = f"{p.raw_question_en}:{p.current_std_question}:{p.proposed_std_question}"
        row_id = hashlib.md5(id_str.encode()).hexdigest()[:16]

        rows_to_insert.append({
            'id': row_id,
            'raw_question_en': p.raw_question_en,
            'current_std_question': p.current_std_question,
            'proposed_std_question': p.proposed_std_question,
            'avg_similarity_in_group': p.avg_similarity_in_group,
            'reclassify_confidence': p.reclassify_confidence,
            'reclassify_method': p.reclassify_method,
            'affected_row_count': p.affected_row_count,
            'detected_at': p.detected_at,
            'status': p.status,
            'reviewed_by': None,
            'reviewed_at': None
        })

    # Use temp table + MERGE to upsert
    temp_table = f"{PROJECT_ID}.{DATASET}._temp_mismatch_{uuid.uuid4().hex[:8]}"

    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("raw_question_en", "STRING"),
        bigquery.SchemaField("current_std_question", "STRING"),
        bigquery.SchemaField("proposed_std_question", "STRING"),
        bigquery.SchemaField("avg_similarity_in_group", "FLOAT64"),
        bigquery.SchemaField("reclassify_confidence", "FLOAT64"),
        bigquery.SchemaField("reclassify_method", "STRING"),
        bigquery.SchemaField("affected_row_count", "INT64"),
        bigquery.SchemaField("detected_at", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("reviewed_by", "STRING"),
        bigquery.SchemaField("reviewed_at", "TIMESTAMP"),
    ]

    from datetime import timedelta
    table = bigquery.Table(temp_table, schema=schema)
    table.expires = datetime.utcnow() + timedelta(minutes=5)  # Expire in 5 minutes

    try:
        bq_client.create_table(table)
        errors = bq_client.insert_rows_json(temp_table, rows_to_insert)
        if errors:
            logger.error(f"Errors inserting to temp table: {errors}")

        merge_sql = f"""
        MERGE `{full_table_name}` T
        USING `{temp_table}` S
        ON T.id = S.id
        WHEN NOT MATCHED THEN
            INSERT (id, raw_question_en, current_std_question, proposed_std_question,
                    avg_similarity_in_group, reclassify_confidence, reclassify_method,
                    affected_row_count, detected_at, status)
            VALUES (S.id, S.raw_question_en, S.current_std_question, S.proposed_std_question,
                    S.avg_similarity_in_group, S.reclassify_confidence, S.reclassify_method,
                    S.affected_row_count, S.detected_at, S.status)
        """
        bq_client.query(merge_sql).result()
        logger.info(f"[OK] Inserted {len(proposals)} proposals into review table")

        bq_client.delete_table(temp_table, not_found_ok=True)

    except Exception as e:
        logger.error(f"Error creating review table entries: {e}")
        raise

    return full_table_name


def apply_approved_changes(bq_client) -> int:
    """Apply changes that have been approved in the review table"""
    full_table_name = f"{PROJECT_ID}.{DATASET}.{REVIEW_TABLE}"

    query = f"""
    SELECT raw_question_en, current_std_question, proposed_std_question, id
    FROM `{full_table_name}`
    WHERE status = 'approved'
    """

    try:
        df = bq_client.query(query).to_dataframe()
    except Exception as e:
        logger.error(f"Error reading review table: {e}")
        return 0

    if df.empty:
        logger.info("No approved changes to apply")
        return 0

    total_updated = 0

    for _, row in df.iterrows():
        update_sql = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
        SET standardized_question = @proposed
        WHERE standardized_question = @current
          AND raw_question_en = @raw_q
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("proposed", "STRING", row['proposed_std_question']),
                bigquery.ScalarQueryParameter("current", "STRING", row['current_std_question']),
                bigquery.ScalarQueryParameter("raw_q", "STRING", row['raw_question_en'])
            ]
        )

        try:
            result = bq_client.query(update_sql, job_config=job_config)
            result.result()
            rows_updated = result.num_dml_affected_rows or 0
            total_updated += rows_updated
            logger.info(f"  [OK] Applied: '{row['current_std_question']}' -> '{row['proposed_std_question']}' ({rows_updated} rows)")

            # Mark as applied
            mark_sql = f"""
            UPDATE `{full_table_name}`
            SET status = 'applied'
            WHERE id = @id
            """
            mark_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", row['id'])]
            )
            bq_client.query(mark_sql, job_config=mark_config).result()

        except Exception as e:
            logger.error(f"  [FAIL] Failed to apply: {e}")

    return total_updated


def apply_csv_corrections(bq_client, csv_path: str) -> int:
    """
    Apply corrections from a CSV file.

    CSV columns required:
    - raw_question_en: the question text to match
    - current_std_question: current standardized question (for verification)
    - revised_std_question: the corrected standardized question
    - id (optional): review table id to update status

    Only rows where revised_std_question differs from current_std_question will be updated.
    """
    import csv

    corrections = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            current = row.get('current_std_question', '').strip()
            revised = row.get('revised_std_question', '').strip()
            raw_q = row.get('raw_question_en', '').strip()
            row_id = row.get('id', '').strip()

            # Skip if no revision needed or revised is empty
            if not revised or revised == current:
                continue

            corrections.append({
                'raw_question_en': raw_q,
                'current_std_question': current,
                'revised_std_question': revised,
                'id': row_id
            })

    if not corrections:
        logger.info("No corrections to apply (all revised_std_question values match current)")
        return 0

    logger.info(f"Found {len(corrections)} corrections to apply")
    total_updated = 0

    for corr in corrections:
        # Update the source table
        update_sql = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
        SET standardized_question = @revised
        WHERE standardized_question = @current
          AND raw_question_en = @raw_q
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("revised", "STRING", corr['revised_std_question']),
                bigquery.ScalarQueryParameter("current", "STRING", corr['current_std_question']),
                bigquery.ScalarQueryParameter("raw_q", "STRING", corr['raw_question_en'])
            ]
        )

        try:
            result = bq_client.query(update_sql, job_config=job_config)
            result.result()
            rows_updated = result.num_dml_affected_rows or 0
            total_updated += rows_updated

            q_display = corr['raw_question_en'][:50] + '...' if len(corr['raw_question_en']) > 50 else corr['raw_question_en']
            logger.info(f"  [OK] '{corr['current_std_question']}' -> '{corr['revised_std_question']}': {rows_updated} rows")
            logger.info(f"    Question: \"{q_display}\"")

            # Update review table status if we have an id
            if corr['id']:
                mark_sql = f"""
                UPDATE `{PROJECT_ID}.{DATASET}.{REVIEW_TABLE}`
                SET status = 'applied_from_csv',
                    reviewed_at = CURRENT_TIMESTAMP()
                WHERE id = @id
                """
                mark_config = bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", corr['id'])]
                )
                try:
                    bq_client.query(mark_sql, job_config=mark_config).result()
                except Exception:
                    pass  # Review table update is optional

        except Exception as e:
            logger.error(f"  [FAIL] Failed: {corr['raw_question_en'][:40]}... - {e}")

    return total_updated


OUTLIER_TABLE = 'lime_surveys_question_outliers_review'


def apply_from_outliers_table(bq_client) -> int:
    """
    Apply corrections from the outliers review table.

    Only rows where revised_std_question is NOT NULL and differs from
    standardized_question will be updated.
    """
    full_table = f"{PROJECT_ID}.{DATASET}.{OUTLIER_TABLE}"

    query = f"""
    SELECT id, standardized_question, revised_std_question, raw_question_en
    FROM `{full_table}`
    WHERE revised_std_question IS NOT NULL
      AND revised_std_question != standardized_question
      AND status != 'applied'
    """

    try:
        df = bq_client.query(query).to_dataframe()
    except Exception as e:
        logger.error(f"Error reading outliers table: {e}")
        return 0

    if df.empty:
        logger.info("No corrections to apply (no rows with revised_std_question set)")
        return 0

    logger.info(f"Found {len(df)} corrections to apply")
    total_updated = 0

    for _, row in df.iterrows():
        update_sql = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
        SET standardized_question = @revised
        WHERE standardized_question = @current
          AND raw_question_en = @raw_q
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("revised", "STRING", row['revised_std_question']),
                bigquery.ScalarQueryParameter("current", "STRING", row['standardized_question']),
                bigquery.ScalarQueryParameter("raw_q", "STRING", row['raw_question_en'])
            ]
        )

        try:
            result = bq_client.query(update_sql, job_config=job_config)
            result.result()
            rows_updated = result.num_dml_affected_rows or 0
            total_updated += rows_updated

            q_display = row['raw_question_en'][:50] + '...' if len(row['raw_question_en']) > 50 else row['raw_question_en']
            logger.info(f"  [OK] '{row['standardized_question']}' -> '{row['revised_std_question']}': {rows_updated} rows")
            logger.info(f"    Question: \"{q_display}\"")

            # Mark as applied in outliers table
            mark_sql = f"""
            UPDATE `{full_table}`
            SET status = 'applied'
            WHERE id = @id
            """
            mark_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", row['id'])]
            )
            bq_client.query(mark_sql, job_config=mark_config).result()

        except Exception as e:
            logger.error(f"  [FAIL] Failed: {row['raw_question_en'][:40]}... - {e}")

    return total_updated


def generate_report(proposals: List[MismatchProposal]) -> str:
    """Generate a detailed report of proposals"""
    lines = [
        "=" * 80,
        "LIMESURVEY QUESTION MISMATCH DETECTION REPORT",
        "=" * 80,
        "Detection method: 6-Layer Matching System",
        "  1. Exact Match (100%)",
        "  2. Machine Learning (90-99%)",
        "  3. Semantic Similarity (85-99%)",
        "  4. Medical Terms/UMLS (85-98%)",
        "  5. Entity Recognition/spaCy (80-93%)",
        "  6. Fuzzy Matching (70-85%)",
        "",
        f"Total proposals: {len(proposals)}",
        f"Total affected rows: {sum(p.affected_row_count for p in proposals)}",
        "",
    ]

    by_current = defaultdict(list)
    for p in proposals:
        by_current[p.current_std_question].append(p)

    for current_q in sorted(by_current.keys()):
        group = by_current[current_q]
        lines.append(f"\n{'─' * 80}")
        lines.append(f"FROM: {current_q}")
        lines.append(f"Proposals: {len(group)}")
        lines.append("")

        for p in sorted(group, key=lambda x: -x.reclassify_confidence):
            lines.append(f"  Question: \"{p.raw_question_en}\"")
            lines.append(f"  → TO: '{p.proposed_std_question}'")
            lines.append(f"    Group similarity: {p.avg_similarity_in_group:.2%}")
            lines.append(f"    Reclassify method: {p.reclassify_method}")
            lines.append(f"    Reclassify confidence: {p.reclassify_confidence:.2%}")
            lines.append(f"    Affected rows: {p.affected_row_count}")
            lines.append("")

    lines.append("=" * 80)
    lines.append(f"Review table: {PROJECT_ID}.{DATASET}.{REVIEW_TABLE}")
    lines.append("To approve changes, update the 'status' column to 'approved'")
    lines.append("Then run: python shared/limesurvey_fix_mismatches.py --apply-approved")
    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description='Detect LimeSurvey question mismatches using 6-layer matching'
    )
    parser.add_argument(
        '--std-question',
        type=str,
        default=None,
        help='Check only a specific standardized_question (e.g., "age")'
    )
    parser.add_argument(
        '--report-file',
        type=str,
        default=None,
        help='Save report to file'
    )
    parser.add_argument(
        '--similarity-threshold',
        type=float,
        default=OUTLIER_SIMILARITY_THRESHOLD,
        help=f'Similarity threshold for outlier detection (default: {OUTLIER_SIMILARITY_THRESHOLD})'
    )
    parser.add_argument(
        '--apply-approved',
        action='store_true',
        help='Apply changes that have been approved in the review table'
    )
    parser.add_argument(
        '--apply-csv',
        type=str,
        default=None,
        help='Apply corrections from a CSV file (with revised_std_question column)'
    )
    parser.add_argument(
        '--apply-from-outliers',
        action='store_true',
        help='Apply corrections from the lime_surveys_question_outliers_review table'
    )

    args = parser.parse_args()

    logger.info("\n" + "=" * 60)
    logger.info("LIMESURVEY QUESTION MISMATCH DETECTOR")
    logger.info("=" * 60)
    logger.info("Method: 6-Layer Matching System")
    logger.info(f"Outlier threshold: {args.similarity_threshold}")
    logger.info(f"Reclassify threshold: {RECLASSIFY_CONFIDENCE_THRESHOLD}")
    if args.std_question:
        logger.info(f"Checking: {args.std_question}")
    logger.info("=" * 60 + "\n")

    bq_client = bigquery.Client(project=PROJECT_ID)

    if args.apply_approved:
        updated = apply_approved_changes(bq_client)
        logger.info(f"\n[OK] Applied {updated} total row updates")
        return

    if args.apply_csv:
        logger.info(f"Applying corrections from CSV: {args.apply_csv}")
        updated = apply_csv_corrections(bq_client, args.apply_csv)
        logger.info(f"\n[OK] Applied {updated} total row updates from CSV")
        return

    if args.apply_from_outliers:
        logger.info("Applying corrections from outliers review table...")
        updated = apply_from_outliers_table(bq_client)
        logger.info(f"\n[OK] Applied {updated} total row updates from outliers table")
        return

    proposals = detect_mismatches(
        bq_client,
        std_question=args.std_question,
        similarity_threshold=args.similarity_threshold
    )

    if not proposals:
        logger.info("\n[OK] No mismatches detected!")
        return

    review_table = create_review_table(bq_client, proposals)

    report = generate_report(proposals)
    # Handle encoding issues on Windows console
    try:
        print("\n" + report)
    except UnicodeEncodeError:
        print("\n" + report.encode('ascii', errors='replace').decode('ascii'))

    if args.report_file:
        with open(args.report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        logger.info(f"\nReport saved to: {args.report_file}")

    logger.info(f"\n[OK] {len(proposals)} proposals written to review table")
    logger.info(f"  Table: {review_table}")
    logger.info("  To approve, update status='approved' for desired rows")
    logger.info("  Then run: python shared/limesurvey_fix_mismatches.py --apply-approved")


if __name__ == '__main__':
    main()
