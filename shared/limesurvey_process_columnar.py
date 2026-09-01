#!/usr/bin/env python3
"""
LimeSurvey Columnar Data Processor

Processes rows in lime_surveys_columnar_completed with intelligent NLP matching.

╔════════════════════════════════════════════════════════════════════════════╗
║  CRITICAL: lime_surveys_columnar_completed is the SOURCE OF TRUTH          ║
║  • This table is APPEND-ONLY - data is NEVER deleted                       ║
║  • This script only performs UPDATE operations (never DELETE/DROP)         ║
║  • Only standardized_question and standardized_answer columns are modified ║
╚════════════════════════════════════════════════════════════════════════════╝

SOURCE OF TRUTH: lime_surveys_columnar_completed
- All mappings are derived FROM this table (no separate mapping tables needed)
- New rows are matched against existing (raw_question_en, standardized_question) pairs

QUESTION MATCHING (6 layers, early stop at 95% confidence):
1. Exact Match (100%) - Direct string comparison
2. ML Supervised Model (90-99%) - GradientBoosting on sentence embeddings
3. Sentence Transformers (85-99%) - Semantic similarity via all-MiniLM-L6-v2
4. UMLS Semantic (85-98%) - Medical concept matching
5. spaCy NER (80-93%) - Biomedical entity extraction
6. RapidFuzz (70-85%) - Token-based fuzzy matching

ANSWER STANDARDIZATION:
- UMLS medical term normalization
- HTML/encoding cleanup
- Whitespace normalization

ML MODEL STORAGE (Local + BigQuery):
- Local: Pickled model files (c:\\orchestrator\\models\\)
- BigQuery ml_question_model: Model metadata only

Usage:
    python shared/limesurvey_process_columnar.py
    python shared/limesurvey_process_columnar.py --dry-run
    python shared/limesurvey_process_columnar.py --retrain-model
"""

import argparse
import os
import re
import pickle
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
import numpy as np

# Suppress gRPC/abseil ALTS warnings (not running on GCP)
# These environment variables must be set BEFORE importing google.cloud
os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "3"  # FATAL only
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["GRPC_TRACE"] = ""  # Disable gRPC tracing
os.environ["TOKENIZERS_PARALLELISM"] = "false"  # Suppress tokenizers fork warning

import sys
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message="BigQuery Storage module not found")
warnings.filterwarnings("ignore", message="Could not determine the type of columns")

# Filter C++ ALTS warnings from stderr at the OS file descriptor level
# gRPC writes directly to fd 2, bypassing Python's sys.stderr
import threading
import io


class _StderrFdFilter:
    """
    Filters gRPC/abseil ALTS warnings by redirecting OS file descriptor 2.

    gRPC's C++ code writes directly to stderr's file descriptor, bypassing
    Python's sys.stderr. This class redirects fd 2 to a pipe, filters
    unwanted messages in a background thread, and writes allowed messages
    to the original stderr.
    """

    _installed = False
    _filter_patterns = [b"ALTS", b"absl::InitializeLog", b"alts_credentials"]
    _original_stderr_fd = None
    _read_pipe = None
    _write_pipe = None
    _filter_thread = None
    _stop_event = None

    @classmethod
    def install(cls):
        if cls._installed:
            return

        try:
            # Save original stderr file descriptor
            cls._original_stderr_fd = os.dup(2)

            # Create a pipe
            cls._read_pipe, cls._write_pipe = os.pipe()

            # Redirect stderr (fd 2) to write end of pipe
            os.dup2(cls._write_pipe, 2)
            os.close(cls._write_pipe)

            # Start background thread to filter and forward
            cls._stop_event = threading.Event()
            cls._filter_thread = threading.Thread(
                target=cls._filter_loop, daemon=True, name="stderr-filter"
            )
            cls._filter_thread.start()
            cls._installed = True
        except Exception:
            # If anything fails, restore original state
            if cls._original_stderr_fd is not None:
                os.dup2(cls._original_stderr_fd, 2)
            pass

    @classmethod
    def _filter_loop(cls):
        """Background thread that reads from pipe, filters, and writes to original stderr."""
        buffer = b""
        while not cls._stop_event.is_set():
            try:
                # Read available data (non-blocking would be better but this works)
                chunk = os.read(cls._read_pipe, 4096)
                if not chunk:
                    break
                buffer += chunk

                # Process complete lines
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    line_with_newline = line + b"\n"

                    # Check if line should be filtered
                    should_filter = any(p in line for p in cls._filter_patterns)
                    if not should_filter:
                        os.write(cls._original_stderr_fd, line_with_newline)
            except OSError:
                break

    @classmethod
    def uninstall(cls):
        """Restore original stderr (call at program end if needed)."""
        if not cls._installed:
            return
        cls._stop_event.set()
        if cls._original_stderr_fd is not None:
            os.dup2(cls._original_stderr_fd, 2)
            os.close(cls._original_stderr_fd)
        if cls._read_pipe is not None:
            os.close(cls._read_pipe)
        cls._installed = False


_StderrFdFilter.install()

# Google Cloud
from google.cloud import bigquery

# Suppress noisy NLP library output (Loading weights bars, HTTP requests, etc.)
import logging as _logging

for _noisy in (
    "sentence_transformers",
    "transformers",
    "huggingface_hub",
    "httpx",
    "httpcore",
    "safetensors",
    "torch",
):
    _logging.getLogger(_noisy).setLevel(_logging.WARNING)

# NLP libraries
from sentence_transformers import SentenceTransformer, util
from rapidfuzz import fuzz

# spaCy is optional (not supported on Python 3.13)
try:
    import spacy

    SPACY_AVAILABLE = True
except ImportError:
    spacy = None
    SPACY_AVAILABLE = False

# ML
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

# UMLS client
try:
    from umls_client import UMLSClient
except ImportError:
    from shared.umls_client import UMLSClient

# Config loader for YAML-based rules
try:
    from shared.config_loader import load_config
except ImportError:
    from config_loader import load_config

# Logging
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ID = "bi-data-391216"
DATASET = "limesurvey_data"
TABLE = "lime_surveys_columnar_completed"
ML_EMBEDDINGS_TABLE = "ml_question_embeddings"
ML_MODEL_TABLE = "ml_question_model"  # Metadata only
UMLS_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "umls",
    "umls.sqlite3",
)

# Local model storage (GCS not available)
from pathlib import Path

# Resolves portably on any OS:
#   - If LIMESURVEY_MODEL_DIR env var is set, use that (per-host override --
#     e.g. on Linux you can point at /home/orchestrator/models without code change).
#   - Else: <repo_root>/models (this file is in shared/, so repo_root is two parents up).
# A previous hard-coded `c:\orchestrator\models` was breaking Linux deployments
# because the literal backslash path was being created on disk as a directory
# named "c:\orchestrator\models", which the audit and read paths could not find.
MODEL_DIR = Path(
    os.environ.get("LIMESURVEY_MODEL_DIR")
    or (Path(__file__).resolve().parent.parent / "models")
)
MODEL_FILE = MODEL_DIR / "question_classifier.pkl"
ENCODER_FILE = MODEL_DIR / "label_encoder.pkl"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

# Confidence thresholds
CONFIDENCE_EARLY_STOP = 0.95
CONFIDENCE_MIN_ACCEPT = 0.85
# The supervised RandomForest classifier distributes probability mass across
# ~280 classes, so a CORRECT prediction rarely scores above 0.7 (per-class
# max is bounded by the fraction of decision trees voting for it). A small
# absolute number from this model is still a strong signal because the 281
# alternative classes each get <1%.
#
# Calibrated against a 200-row probe (precision/recall curve on rows where the
# trusted standardized_question is known): precision hits 100% at threshold
# 0.25 and stays there. Going below 0.25 admits two misclassifications (one
# at conf=0.06, one at conf=0.18). Going above 0.25 rejects correct
# predictions without precision improvement. So 0.25 is the precision/recall
# sweet spot. Used by the ML layer ONLY -- semantic / umls / spacy / fuzzy
# keep their cosine-style 0.85 threshold.
ML_MIN_ACCEPT = 0.25

# =============================================================================
# YAML CONFIG LOADING
# =============================================================================
# Rules are loaded from configs/limesurvey.yaml -> nlp_processing section
# This allows non-developers to update mappings without changing code

_nlp_config_cache = None


def get_nlp_config():
    """Load NLP processing config from YAML (cached)."""
    global _nlp_config_cache
    if _nlp_config_cache is None:
        try:
            config = load_config("limesurvey")
            _nlp_config_cache = config.get("nlp_processing", {})
            logger.info(
                f"Loaded NLP config: {len(_nlp_config_cache.get('parent_rules', []))} parent rules, "
                f"{len(_nlp_config_cache.get('subquestion_rules', []))} subquestion rules"
            )
        except Exception as e:
            logger.warning(f"Could not load NLP config from YAML: {e}. Using defaults.")
            _nlp_config_cache = {}
    return _nlp_config_cache


def get_parent_rules():
    """Get 3-tuple parent rules from YAML config."""
    config = get_nlp_config()
    rules = []
    for rule in config.get("parent_rules", []):
        if isinstance(rule, dict):
            rules.append(
                (
                    rule.get("parent", ""),
                    rule.get("subquestion", ""),
                    rule.get("standardized", ""),
                )
            )
    # Fall back to hardcoded rules if YAML is empty
    if not rules:
        rules = SUBQUESTION_PARENT_RULES_DEFAULT
    return rules


def get_subquestion_rules():
    """Get 2-tuple subquestion rules from YAML config."""
    config = get_nlp_config()
    rules = []
    for rule in config.get("subquestion_rules", []):
        if isinstance(rule, dict):
            rules.append((rule.get("pattern", ""), rule.get("standardized", "")))
    # Fall back to hardcoded rules if YAML is empty
    if not rules:
        rules = SUBQUESTION_PATTERN_RULES_DEFAULT
    return rules


def get_answer_clusters():
    """Get answer clusters from YAML config."""
    config = get_nlp_config()
    clusters = config.get("answer_clusters", {})
    # Fall back to hardcoded clusters if YAML is empty
    if not clusters:
        clusters = ANSWER_CLUSTERS_DEFAULT
    return clusters


# Patterns for identifying free-text questions (used in question_type classification)
FREE_TEXT_PATTERNS = [
    "comments",
    "feedback",
    "suggestions",
    "insights",
    "other_specify",
    "reason",
    "explain",
    "describe",
    "details",
    "additional",
    "specify",
    "improvement",
    "ideas",
    "thoughts",
    "notes",
    "message",
]


def determine_question_type(
    standardized_question: str, subquestion_id: str, answer_length: int = 0
) -> str:
    """
    Determine the question type based on question characteristics.

    Returns:
        'matrix' - has subquestion_id (part of a matrix/array question)
        'free_text' - matches free-text patterns or has long answers
        'simple' - everything else (single-answer questions)
    """
    # Matrix questions have subquestion_id
    if subquestion_id:
        return "matrix"

    # Free-text questions (based on question name patterns or long answers)
    if standardized_question:
        lower_q = standardized_question.lower()
        for pattern in FREE_TEXT_PATTERNS:
            if pattern in lower_q:
                return "free_text"

    # Long answers indicate free-text
    if answer_length > 200:
        return "free_text"

    # Everything else is simple
    return "simple"


# Pattern-based rules for known misclassification cases
# These take precedence over ML model to prevent systematic errors
# Format: (pattern_substring, standardized_question)
# IMPORTANT: More specific patterns MUST come before generic patterns!
PATTERN_RULES = [
    # time_since_diagnosis misclassifications
    ("onset of symptoms until", "time_from_symptom_to_diagnosis"),
    ("symptoms begin", "time_since_symptom_onset"),
    # disease_stage misclassifications
    ("familiar with the stages", "disease_stage_familiarity"),
    # gene_therapy misclassifications
    ("have you discussed gene therapy", "gene_therapy_discussion_status"),
    # clinical_trial misclassifications
    ("dates or time period", "clinical_trial_dates"),
    ("information about the trial", "clinical_trial_details"),
    # ========================================================================
    # TREATMENT SATISFACTION - specific patterns before generic
    # ========================================================================
    ("why are you not satisfied", "treatment_dissatisfaction_reason"),
    ("fda-approved treatment options", "treatment_satisfaction_fda_options"),
    ("fda approved treatment options", "treatment_satisfaction_fda_options"),
    ("efficacy of your current treatment", "treatment_satisfaction_efficacy"),
    ("comprehensive treatment plan", "treatment_satisfaction_comprehensive"),
    ("lems treatment", "treatment_satisfaction_lems"),
    # Generic treatment satisfaction - LAST (after all specific patterns)
    (
        "satisfied are you with your current treatment plan",
        "treatment_plan_satisfaction",
    ),
    ("satisfied with your current treatment", "treatment_plan_satisfaction"),
    # ========================================================================
    # WEBINAR FEEDBACK - specific patterns before generic
    # ========================================================================
    ("satisfied were you with this webinar", "webinar_satisfaction"),
    ("rate your overall experience with the webinar", "webinar_overall_rating"),
    ("rate the webinar you just experienced", "webinar_overall_rating"),
    ("rate the following segments", "webinar_topic_ratings"),
    ("most valuable", "webinar_valuable_aspects"),
    ("ease of accessing the webinar", "webinar_access_ease"),
    ("how helpful was the information", "webinar_helpfulness"),
    ("how confident do you feel", "webinar_confidence_gained"),
    ("running time", "webinar_duration_feedback"),
    ("duration of the webinar", "webinar_duration_feedback"),
    ("learn something new", "webinar_learning_outcome"),
    ("gained actionable knowledge", "webinar_learning_outcome"),
    ("could we improve", "webinar_improvement_suggestions"),
    ("topics would you like", "webinar_future_topics"),
    ("topic or question you would like us to cover", "webinar_future_topics"),
    ("additional feedback", "webinar_additional_feedback"),
    ("any other feedback", "webinar_additional_feedback"),
    # ========================================================================
    # EMAIL TRACKING (was incorrectly mapped to 'url')
    # ========================================================================
    ("captured url", "email_tracking_id"),
    # ========================================================================
    # COLUMN CONSOLIDATION - map variations to canonical names
    # ========================================================================
    # age_diagnosed → age_at_diagnosis
    ("what age were you diagnosed", "age_at_diagnosis"),
    ("age were you diagnosed", "age_at_diagnosis"),
    ("age when diagnosed", "age_at_diagnosis"),
    ("age diagnosed", "age_at_diagnosis"),
    # date_of_diagnosis → diagnosis_date
    ("date of diagnosis", "diagnosis_date"),
    ("when were you diagnosed", "diagnosis_date"),
    ("date you were diagnosed", "diagnosis_date"),
    # relationship_to_patient → caregiver_relationship
    ("relationship to the patient", "caregiver_relationship"),
    ("relationship to patient", "caregiver_relationship"),
    ("your relationship to", "caregiver_relationship"),
    # how_heard_about_disease → disease_awareness_source
    ("how did you hear about", "disease_awareness_source"),
    ("how did you first learn", "disease_awareness_source"),
    ("how did you find out about", "disease_awareness_source"),
]


# ============================================================================
# SUBQUESTION PATTERN RULES - Match on subquestion text with optional parent context
# ============================================================================
# These rules split over-grouped questions into specific, meaningful standardized questions.
#
# Two formats supported:
#   2-tuple: (subq_pattern, std_question) - matches subquestion text only
#   3-tuple: (parent_pattern, subq_pattern, std_question) - matches both parent AND subquestion
#
# IMPORTANT: 3-tuples (more specific) are checked FIRST, then 2-tuples!

# 3-TUPLE RULES: (parent_question_pattern, subquestion_pattern, standardized_question)
# These are checked FIRST to handle cases where same subquestion appears in multiple parents
# NOTE: These are DEFAULT fallbacks - actual rules loaded from configs/limesurvey.yaml
SUBQUESTION_PARENT_RULES_DEFAULT = [
    # ========================================================================
    # SYMPTOM_QOL_IMPACT - "How has X impacted your quality of life?"
    # ========================================================================
    ("impact", "difficulty swallowing", "symptom_qol_impact_swallowing"),
    ("impact", "difficulty breathing", "symptom_qol_impact_breathing"),
    ("impact", "difficulty with speech", "symptom_qol_impact_speech"),
    ("impact", "fatigue", "symptom_qol_impact_fatigue"),
    ("impact", "changes in mobility", "symptom_qol_impact_mobility"),
    # ========================================================================
    # SYMPTOM_ONSET_ORDER - "In what order did symptoms appear?"
    # ========================================================================
    ("order", "difficulty swallowing", "symptom_onset_order_swallowing"),
    ("order", "difficulty breathing", "symptom_onset_order_breathing"),
    ("order", "difficulty with speech", "symptom_onset_order_speech"),
    ("order", "fatigue", "symptom_onset_order_fatigue"),
    ("order", "changes in mobility", "symptom_onset_order_mobility"),
    # ========================================================================
    # DISEASE_PROGRESSION - disambiguate from fatigue_level
    # Parent: "Since your diagnosis... how would you describe the changes..."
    # ========================================================================
    (
        "since your diagnosis",
        "getting dressed or bathing",
        "progression_daily_activities",
    ),
    (
        "since your diagnosis",
        "perform everyday activities",
        "progression_daily_activities",
    ),
    # ========================================================================
    # COMMUNITY ENGAGEMENT SENTIMENT - "How does engaging with [site] make you feel?"
    # Each sentiment is a separate checkbox option
    # ========================================================================
    ("engaging with", "empowered", "community_sentiment_empowered"),
    ("engaging with", "supported", "community_sentiment_supported"),
    ("engaging with", "hopeful", "community_sentiment_hopeful"),
    ("engaging with", "informed", "community_sentiment_informed"),
    ("engaging with", "connected", "community_sentiment_connected"),
    ("engaging with", "heard", "community_sentiment_heard"),
    ("engaging with", "safe", "community_sentiment_safe"),
    # ========================================================================
    # FDA APPROVED TREATMENTS - different question types
    # ========================================================================
    ("is there an fda-approved treatment", "", "fda_approved_treatment_count"),
    ("are you currently taking an fda", "", "fda_approved_treatment_current"),
    (
        "ever taken a medication that is specifically approved",
        "",
        "fda_approved_treatment_ever_taken",
    ),
]

# 2-TUPLE RULES: (subquestion_pattern, standardized_question)
# These are checked SECOND, for cases where subquestion text alone is sufficient
# NOTE: These are DEFAULT fallbacks - actual rules loaded from configs/limesurvey.yaml
SUBQUESTION_PATTERN_RULES_DEFAULT = [
    # ========================================================================
    # AGREEMENT_SCALE SPLITS - based on actual subquestion content
    # ========================================================================
    # Social isolation / relationships - SPLIT by specific aspect
    ("feel isolated", "social_isolation_feeling"),
    ("struggle to connect with others", "social_isolation_connection"),
    ("limits my ability to participate", "social_isolation_participation"),
    ("accessible or inclusive social", "social_isolation_accessibility"),
    ("affects my romantic relationships", "social_isolation_romantic"),
    # Daily symptom burden - SPLIT by burden type
    ("pain or fatigue significantly affects my daily", "symptom_burden_pain_fatigue"),
    ("unpredictable symptoms make it challenging", "symptom_burden_unpredictability"),
    ("managing side effects from treatment", "symptom_burden_side_effects"),
    # Treatment routine burden - SPLIT by burden type
    ("juggling medication schedules", "treatment_burden_medication_schedule"),
    ("treatment routines are demanding", "treatment_burden_time_demands"),
    ("finding childcare while attending", "treatment_burden_childcare"),
    # Mental health / emotional impact - SPLIT by aspect
    ("anxiety or depression", "mental_health_anxiety_depression"),
    ("living with uncertainty about the future", "mental_health_uncertainty_stress"),
    ("struggle to find healthy coping mechanisms", "mental_health_coping_mechanisms"),
    ("maintain a positive outlook", "mental_health_outlook"),
    # Webinar access ease - SPLIT by aspect
    ("easy to access the webinar", "webinar_access_ease"),
    ("instructions to join the webinar", "webinar_instructions_clarity"),
    ("join the webinar without technical issues", "webinar_technical_ease"),
    ("platform used for the webinar was user-friendly", "webinar_platform_usability"),
    # Medication experience - SPLIT by medication
    ("acetazolamide", "medication_experience_acetazolamide"),
    ("diamox", "medication_experience_acetazolamide"),
    ("omaveloxolone", "medication_experience_omaveloxolone"),
    ("skyclarys", "medication_experience_omaveloxolone"),
    ("clonazepam", "medication_experience_clonazepam"),
    ("klonopin", "medication_experience_clonazepam"),
    ("pregabalin", "medication_experience_pregabalin"),
    ("lyrica", "medication_experience_pregabalin"),
    ("5-hydroxytryptophan", "medication_experience_5htp"),
    ("oxitriptan", "medication_experience_5htp"),
    # Community engagement sentiment - SPLIT by sentiment type
    # Subquestion text IS the sentiment (Empowered, Supported, etc.)
    # These match when the subquestion text equals the sentiment word
    # Gene therapy interest/attitudes
    ("would want to get it as soon as it", "gene_therapy_early_adoption"),
    ("want to see the data from others who have tried", "gene_therapy_wait_for_data"),
    ("move up our next appointment", "gene_therapy_appointment_priority"),
    (
        "previously had conversations with our healthcare provider",
        "gene_therapy_prior_discussion",
    ),
    ("may pursue it in the future", "gene_therapy_future_consideration"),
    (
        "consider pursuing it if our healthcare provider",
        "gene_therapy_provider_recommendation",
    ),
    ("holding out for other treatment options", "gene_therapy_alternative_preference"),
    ("don't believe gene therapy is the right", "gene_therapy_skepticism"),
    ("not considering invasive treatments", "gene_therapy_invasiveness_concern"),
    ("need more information before pursuing", "gene_therapy_information_need"),
    # Fatigue (for agreement scale about fatigue)
    ("fatigue has increased as my", "fatigue_progression"),
    ("fatigue has been a primary", "fatigue_as_primary_symptom"),
    ("fatigue has had a significant impact", "fatigue_qol_impact"),
    ("fatigue makes me feel isolated", "fatigue_social_isolation"),
    ("fatigue makes enjoying social", "fatigue_social_impact"),
    ("fatigue limits my physical", "fatigue_physical_limitation"),
    ("fatigue affects my mood", "fatigue_mood_impact"),
    ("fatigue makes it hard to concentrate", "fatigue_cognitive_impact"),
    ("fatigue interferes with social", "fatigue_social_interference"),
    # Mobility aid experience
    (
        "mobility aid has made me feel more independent",
        "mobility_aid_independence_positive",
    ),
    (
        "mobility aid has made me feel less independent",
        "mobility_aid_independence_negative",
    ),
    ("able to complete tasks on my own", "mobility_aid_task_capability"),
    ("mobility aids allows me to be more present", "mobility_aid_social_presence"),
    ("worry about what others will think", "mobility_aid_social_stigma"),
    ("faced discrimination as a result of my disability", "disability_discrimination"),
    ("feel more included in group social", "mobility_aid_social_inclusion"),
    ("go out of their way to make me feel", "mobility_aid_social_support"),
    # Swallowing difficulties (agreement scale)
    ("trouble swallowing", "swallowing_difficulty"),
    ("trouble chewing", "swallowing_difficulty"),
    ("adjust my diet significantly", "swallowing_diet_adjustment"),
    ("avoid foods i used to enjoy", "swallowing_food_avoidance"),
    ("swallowing difficulties have negatively", "swallowing_qol_impact"),
    # Speech difficulties (agreement scale)
    ("difficulty speaking clearly", "speech_clarity_difficulty"),
    ("trouble understanding my speech", "speech_intelligibility"),
    (
        "make assumptions about my cognition based on my speech",
        "speech_cognitive_assumptions",
    ),
    ("talk down to me", "speech_condescension"),
    ("speech difficulty", "speech_difficulty_progression"),
    ("get frustrated when others make assumptions", "speech_frustration"),
    ("speech difficulties have a significant impact", "speech_qol_impact"),
    # Breathing difficulties (agreement scale)
    ("trouble breathing", "breathing_difficulty"),
    ("cough more frequently", "cough_frequency"),
    ("intensity/frequency of my breathing", "breathing_progression"),
    # ========================================================================
    # LIFE_MILESTONES_IMPACT SPLITS - by milestone type
    # ========================================================================
    ("learning to transfer independently", "milestone_transfer_independence"),
    ("mastering use of a new mobility device", "milestone_mobility_device_mastery"),
    ("adapting your home environment", "milestone_home_adaptation"),
    ("regaining social or recreational", "milestone_social_activity_regain"),
    ("finding new ways to care for yourself", "milestone_self_care_adaptation"),
    ("confident with your walking aids", "milestone_walking_aid_confidence"),
    # ========================================================================
    # FATIGUE_LEVEL SPLITS - by activity
    # ========================================================================
    ("walking unassisted", "fatigue_level_walking"),
    ("climbing stairs", "fatigue_level_stairs"),
    ("household tasks", "fatigue_level_household"),
    ("cleaning, laundry, cooking", "fatigue_level_household"),
    ("getting dressed", "fatigue_level_dressing"),
    ("grooming", "fatigue_level_grooming"),
    ("hygiene activities", "fatigue_level_grooming"),
    ("physical therapy", "fatigue_level_physical_therapy"),
    ("exercise", "fatigue_level_exercise"),
    ("maneuvering a wheelchair", "fatigue_level_wheelchair"),
    ("walking with cane", "fatigue_level_assisted_walking"),
    ("walking with walker", "fatigue_level_assisted_walking"),
    ("running errands", "fatigue_level_errands"),
    ("grocery shopping", "fatigue_level_errands"),
    ("eating", "fatigue_level_eating"),
    ("speaking", "fatigue_level_speaking"),
    # ========================================================================
    # DISEASE_PROGRESSION_CHANGES SPLITS - by symptom area
    # ========================================================================
    ("walk or keep your balance", "progression_walking_balance"),
    ("tasks with your hands", "progression_fine_motor"),
    ("buttoning a shirt", "progression_fine_motor"),
    ("speech or ease of swallowing", "progression_speech_swallowing"),
    ("muscle strength or energy", "progression_muscle_energy"),
    ("tingling or numbness", "progression_tingling_numbness"),
    ("heart sensations", "progression_heart_sensations"),
    ("racing or unusual feelings", "progression_heart_sensations"),
    ("management of blood sugar", "progression_blood_sugar"),
    ("perform everyday activities", "progression_daily_activities"),
    ("getting dressed or bathing", "progression_daily_activities"),
    ("mood or stress", "progression_mood_stress"),
    ("memory or concentration", "progression_memory_concentration"),
    # ========================================================================
    # WEBINAR_TOPIC_RATINGS SPLITS - by webinar segment
    # ========================================================================
    ("diagnostic process", "webinar_rating_diagnostic_journey"),
    ("diagnostic journey", "webinar_rating_diagnostic_journey"),
    ("treatment and symptom management", "webinar_rating_treatment"),
    ("lifestyle and day-to-day", "webinar_rating_lifestyle"),
    ("managing finances", "webinar_rating_finances"),
    ("q&a", "webinar_rating_qa"),
]


@dataclass
class MatchResult:
    """Result of question matching"""

    standardized_question: str
    confidence: float
    method: str
    matched_to: Optional[str] = None


@dataclass
class AnswerMatchResult:
    """Result of answer fuzzy matching"""

    standardized_answer: str
    confidence: float
    method: str
    original: str


# Default answer clusters (fallback if YAML not available)
ANSWER_CLUSTERS_DEFAULT = {
    "not_on_treatment": [
        "not on treatment",
        "no treatment",
        "not taking treatment",
        "not on any treatment",
        "not being treated",
        "i am not on treatment",
        "no current treatment",
        "none",
        "n/a",
        "na",
    ],
    "not_sure": [
        "not sure",
        "unsure",
        "don't know",
        "i don't know",
        "unknown",
        "uncertain",
        "no idea",
        "not certain",
        "maybe",
        "possibly",
    ],
    "prefer_not_to_answer": [
        "prefer not to answer",
        "prefer not to say",
        "decline to answer",
        "no answer",
        "skip",
        "rather not say",
        "private",
    ],
    "other": [
        "other",
        "other (please specify)",
        "other - please specify",
        "other_specify",
        "something else",
        "different",
    ],
}


class AnswerMatcher:
    """
    Fuzzy matcher for answers with 4-layer matching:
    1. Exact Match (100%) - Direct string match
    2. Normalized Match (95%) - Case/whitespace/punctuation variations
    3. RapidFuzz Token Match (85-94%) - Handle typos
    4. Cluster Match (90%) - Known semantic clusters

    Usage:
        matcher = AnswerMatcher()
        result = matcher.match("Mal", ["Male", "Female", "Other"])
        # Returns AnswerMatchResult(standardized_answer="Male", confidence=0.90, ...)
    """

    def __init__(self, fuzzy_threshold: int = 85):
        """
        Initialize AnswerMatcher.

        Args:
            fuzzy_threshold: Minimum RapidFuzz score to accept a match (0-100)
        """
        self.fuzzy_threshold = fuzzy_threshold
        # Build reverse lookup: variation -> canonical
        self._cluster_lookup = {}
        self._answer_clusters = get_answer_clusters()
        for canonical, variations in self._answer_clusters.items():
            for var in variations:
                self._cluster_lookup[var.lower()] = canonical

    def match(
        self, raw_answer: str, known_answers: List[str]
    ) -> Optional[AnswerMatchResult]:
        """
        Match a raw answer against known answer options.

        Args:
            raw_answer: The answer to match
            known_answers: List of valid answer options to match against

        Returns:
            AnswerMatchResult if match found, None if no good match
        """
        if not raw_answer or not known_answers:
            return None

        raw_lower = raw_answer.lower().strip()
        raw_normalized = self._normalize(raw_answer)

        # Layer 1: Exact match
        for known in known_answers:
            if raw_lower == known.lower().strip():
                return AnswerMatchResult(
                    standardized_answer=known,
                    confidence=1.0,
                    method="exact_match",
                    original=raw_answer,
                )

        # Layer 2: Normalized match (handles whitespace, punctuation variations)
        for known in known_answers:
            if raw_normalized == self._normalize(known):
                return AnswerMatchResult(
                    standardized_answer=known,
                    confidence=0.95,
                    method="normalized_match",
                    original=raw_answer,
                )

        # Layer 3: Cluster match (semantic equivalence)
        if raw_lower in self._cluster_lookup:
            canonical = self._cluster_lookup[raw_lower]
            # Find the best matching known answer for this cluster
            for known in known_answers:
                known_normalized = self._normalize(known)
                if (
                    canonical in known_normalized
                    or known_normalized in self._answer_clusters.get(canonical, [])
                ):
                    return AnswerMatchResult(
                        standardized_answer=known,
                        confidence=0.90,
                        method="cluster_match",
                        original=raw_answer,
                    )

        # Layer 4: RapidFuzz token matching (handles typos)
        best_match = None
        best_score = 0
        for known in known_answers:
            # Use token_set_ratio for flexibility with word order
            score = fuzz.token_set_ratio(raw_lower, known.lower())
            if score > best_score and score >= self.fuzzy_threshold:
                best_score = score
                best_match = known

        if best_match:
            return AnswerMatchResult(
                standardized_answer=best_match,
                confidence=best_score / 100.0,
                method="fuzzy_match",
                original=raw_answer,
            )

        return None

    def _normalize(self, text: str) -> str:
        """Normalize text for comparison: lowercase, remove punctuation, collapse whitespace."""
        if not text:
            return ""
        # Lowercase
        normalized = text.lower()
        # Replace underscores and hyphens with spaces
        normalized = normalized.replace("_", " ").replace("-", " ")
        # Remove punctuation except alphanumeric and spaces
        normalized = re.sub(r"[^a-z0-9\s]", "", normalized)
        # Collapse whitespace
        normalized = " ".join(normalized.split())
        return normalized


class LimeSurveyProcessor:
    """Processes LimeSurvey data with 6-layer NLP matching"""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.bq_client = bigquery.Client(project=PROJECT_ID)

        # Cached data (loaded lazily)
        self._known_mappings: Optional[Dict[str, str]] = None
        self._known_embeddings: Optional[np.ndarray] = None
        self._known_questions: Optional[List[str]] = None
        self._ml_model = None
        self._ml_label_encoder = None
        self._subquestion_map: Optional[Dict[tuple, str]] = (
            None  # (survey_id, question_id, subq_id) -> text
        )

        logger.info("Initializing NLP models...")

        # Sentence Transformers (required) - suppress "Loading weights" tqdm bars
        os.environ["TQDM_DISABLE"] = "1"
        self.transformer_model = SentenceTransformer(
            "sentence-transformers/all-MiniLM-L6-v2"
        )
        os.environ.pop("TQDM_DISABLE", None)
        logger.info("[OK] Sentence Transformers loaded (all-MiniLM-L6-v2)")

        # UMLS for medical term normalization.
        # Fault-tolerant: any load failure (missing file, memory pressure, etc.)
        # downgrades to "no UMLS layer" rather than crashing the whole process.
        # Consumer code guards every use with `if self.umls_client` so this is safe.
        self.umls_client = None
        if os.path.exists(UMLS_DB_PATH):
            try:
                self.umls_client = UMLSClient(
                    local_db_path=UMLS_DB_PATH, prefer_local=True
                )
                logger.info(f"[OK] UMLS loaded from {UMLS_DB_PATH}")
            except Exception as e:
                logger.warning(
                    f"[WARN] UMLS not available ({type(e).__name__}: {e}); "
                    f"continuing without the UMLS layer."
                )
        else:
            logger.warning(f"[WARN] UMLS database not found at {UMLS_DB_PATH}")

        # spaCy for biomedical NER.
        # Priority: en_ner_bc5cdr_md (chemical/disease) > en_core_sci_md (general bio) > en_core_web_sm (fallback)
        # Fault-tolerant: catches ALL exceptions, not just OSError (missing model).
        # This matters on low-memory machines where spaCy's model files load OK on
        # disk but exhaust RAM mid-deserialization with a ValueError("Could not
        # reserve memory block"). When that happens we simply skip that NER model
        # -- every consumer site guards with `if self.nlp` / `if self.nlp_ner`,
        # so an unloaded model gracefully degrades the spaCy layer to a no-op.
        # The trainer (--retrain-model) doesn't use NER at all, so this lets the
        # retrain succeed on machines where NER won't fit.
        self.nlp = None
        self.nlp_ner = None  # Specialized NER model for chemical/disease
        if SPACY_AVAILABLE:
            import warnings

            for model_name in ["en_ner_bc5cdr_md", "en_core_sci_md", "en_core_sci_sm"]:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter(
                            "ignore"
                        )  # suppress version compat warnings
                        if model_name == "en_ner_bc5cdr_md":
                            self.nlp_ner = spacy.load(model_name)
                            logger.info(
                                f"[OK] spaCy chemical/disease NER loaded ({model_name})"
                            )
                        else:
                            self.nlp = spacy.load(model_name)
                            logger.info(
                                f"[OK] spaCy biomedical model loaded ({model_name})"
                            )
                            break
                except OSError:
                    # Model not installed; try the next one.
                    continue
                except Exception as e:
                    # Memory exhaustion or any other load failure: skip THIS model
                    # but keep trying alternatives + let the process keep running.
                    logger.warning(
                        f"[WARN] spaCy model {model_name} failed to load "
                        f"({type(e).__name__}: {e}); continuing."
                    )
                    continue

            # Fallback to general English model
            if not self.nlp:
                try:
                    self.nlp = spacy.load("en_core_web_sm")
                    logger.info("[OK] spaCy loaded (en_core_web_sm)")
                except OSError:
                    logger.warning("[WARN] No spaCy model found - skipping spaCy layer")
                except Exception as e:
                    logger.warning(
                        f"[WARN] Fallback spaCy model failed to load "
                        f"({type(e).__name__}: {e}); skipping spaCy layer."
                    )
        else:
            logger.info("[WARN] spaCy not available - skipping spaCy layer")

        # Load ML model from BigQuery (if exists)
        self._load_ml_model()

        # Answer fuzzy matcher for typo correction
        self.answer_matcher = AnswerMatcher(fuzzy_threshold=85)
        logger.info("[OK] AnswerMatcher initialized (fuzzy threshold: 85%)")

    def _ensure_ml_tables_exist(self):
        """Create ML metadata table if it doesn't exist (model files stored locally)"""

        # Model table (metadata only - actual model stored locally at MODEL_DIR)
        model_ddl = f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{ML_MODEL_TABLE}` (
            model_id STRING,
            model_path STRING,
            accuracy FLOAT64,
            trained_at TIMESTAMP,
            training_rows INT64
        )
        """

        try:
            self.bq_client.query(model_ddl).result()
            logger.info("[OK] ML metadata table ready")
        except Exception as e:
            logger.warning(f"Could not create ML table: {e}")

    def _load_ml_model(self):
        """Load ML model from local files (metadata in BigQuery)"""
        try:
            # Check if local model files exist
            if not MODEL_FILE.exists() or not ENCODER_FILE.exists():
                logger.info(
                    "No ML model found locally - will use other matching layers"
                )
                return

            # Load metadata from BigQuery (optional, for logging)
            try:
                query = f"""
                SELECT model_id, accuracy, trained_at
                FROM `{PROJECT_ID}.{DATASET}.{ML_MODEL_TABLE}`
                ORDER BY trained_at DESC
                LIMIT 1
                """
                results = list(self.bq_client.query(query))
                if results:
                    row = results[0]
                    logger.info(
                        f"Model metadata: {row.model_id} (accuracy: {row.accuracy:.2%}, trained: {row.trained_at})"
                    )
            except Exception:
                pass  # Metadata is optional

            # Load classifier and encoder from local files
            with open(MODEL_FILE, "rb") as f:
                self._ml_model = pickle.load(f)

            with open(ENCODER_FILE, "rb") as f:
                self._ml_label_encoder = pickle.load(f)

            logger.info(f"[OK] ML model loaded from {MODEL_DIR}")
            logger.info(f"  Classes: {len(self._ml_label_encoder.classes_)}")
        except Exception as e:
            logger.warning(f"Could not load ML model: {e}")

    def get_known_mappings(self) -> Tuple[Dict[str, str], List[str], np.ndarray]:
        """
        Get known question mappings directly from lime_surveys_columnar_completed

        Returns:
            Tuple of (mappings dict, questions list, embeddings array)
        """
        if self._known_mappings is not None:
            return self._known_mappings, self._known_questions, self._known_embeddings

        logger.info("Loading known mappings from completed table...")

        query = f"""
        SELECT DISTINCT
            raw_question_en,
            standardized_question
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE standardized_question IS NOT NULL
          AND raw_question_en IS NOT NULL
          AND TRIM(raw_question_en) != ''
        """

        df = self.bq_client.query(query).to_dataframe()

        # Build mappings dict (normalized question → standardized_question)
        self._known_mappings = {}
        self._known_questions = []

        for _, row in df.iterrows():
            raw_q = row["raw_question_en"]
            std_q = row["standardized_question"]

            # Clean and normalize for matching
            cleaned = self._cleanup_text(raw_q)
            if cleaned:
                normalized = cleaned.lower().strip()
                self._known_mappings[normalized] = std_q
                self._known_questions.append(normalized)

        # Generate embeddings for semantic matching
        if self._known_questions:
            logger.info(
                f"Generating embeddings for {len(self._known_questions)} known questions..."
            )
            self._known_embeddings = self.transformer_model.encode(
                self._known_questions, convert_to_numpy=True, show_progress_bar=False
            )
        else:
            self._known_embeddings = np.array([])

        logger.info(f"[OK] Loaded {len(self._known_mappings)} known mappings")
        return self._known_mappings, self._known_questions, self._known_embeddings

    def _cleanup_text(self, text: str) -> str:
        """Clean text: HTML removal, encoding fixes, whitespace normalization"""
        if not text or not text.strip():
            return ""

        cleaned = text

        # Remove HTML tags
        cleaned = re.sub(r"<[^>]+>", "", cleaned)

        # Decode HTML entities
        html_entities = {
            "&gt;": ">",
            "&lt;": "<",
            "&amp;": "&",
            "&quot;": '"',
            "&apos;": "'",
            "&nbsp;": " ",
            "&#39;": "'",
            "&#34;": '"',
        }
        for entity, char in html_entities.items():
            cleaned = cleaned.replace(entity, char)

        # Fix encoding issues
        encoding_fixes = {
            r"â€™|â€˜|â€™": "'",
            r"â€œ|â€�|â€|â€�": '"',
            r"Â|Â ": " ",
        }
        for pattern, replacement in encoding_fixes.items():
            cleaned = re.sub(pattern, replacement, cleaned)

        # Normalize whitespace
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        return cleaned

    @staticmethod
    def build_ml_input(
        raw_question_en: Optional[str],
        subquestion_text: Optional[str] = None,
    ) -> str:
        """Build the text input the ML model sees, for BOTH training and inference.

        For non-matrix rows (no subquestion_text), this is just the cleaned parent
        text. For matrix rows where we have a subquestion text, this concatenates
        them as `parent :: subquestion`, so the model can disambiguate between
        many subquestions sharing the same parent ("Please rate...").

        Keep this in one place so training and inference cannot drift apart --
        a mismatch was the root cause of the model producing ~0.02 confidence
        on every matrix row (audit_run_id 1381518...).
        """
        parent = (raw_question_en or "").strip()
        subq = (subquestion_text or "").strip()
        if subq:
            return f"{parent} :: {subq}" if parent else subq
        return parent

    def _load_subquestion_map(self) -> Dict[tuple, str]:
        """Load subquestion text mapping from lime_questions table.

        For multi-select checkbox questions, LimeSurvey stores:
        - raw_answer_en = 'Y' (meaning "selected")
        - subquestion_id = '001', '002', etc.

        The actual checkbox option text is in lime_questions (with parent_qid linking).
        This builds a map: (survey_id, parent_qid, subq_title) -> subquestion_text
        """
        if self._subquestion_map is not None:
            return self._subquestion_map

        logger.info("Loading subquestion text mapping...")

        query = f"""
        SELECT
            q.sid as survey_id,
            q.parent_qid as question_id,
            q.title as subq_title,
            COALESCE(l10n.question, q.question) as subquestion_text
        FROM `{PROJECT_ID}.{DATASET}.lime_questions` q
        LEFT JOIN `{PROJECT_ID}.{DATASET}.lime_question_l10ns` l10n
            ON q.qid = l10n.qid AND l10n.language = 'en'
        WHERE q.parent_qid > 0
          AND q.title LIKE 'SQ%'
        """

        results = self.bq_client.query(query).result()

        self._subquestion_map = {}
        for row in results:
            # Extract subquestion_id from title (e.g., 'SQ001' -> '001')
            subq_id = (
                row.subq_title[2:]
                if row.subq_title.startswith("SQ")
                else row.subq_title
            )
            key = (row.survey_id, row.question_id, subq_id)
            self._subquestion_map[key] = row.subquestion_text

        logger.info(f"Loaded {len(self._subquestion_map)} subquestion mappings")
        return self._subquestion_map

    def _clean_answer_text(self, text: str) -> str:
        """Clean answer text for multi-select options.

        - Removes LimeSurvey conditional expressions {if(site == ...)}
        - Replaces {site} with 'condition'
        - Fixes encoding issues
        - Normalizes whitespace
        - Keeps human-readable (not snake_case)
        """
        if not text:
            return text

        # Remove LimeSurvey conditional expressions {if(site == "...", "...", ...)}
        if "{if(" in text or "{if (" in text:
            match = re.search(r"\{if\s*\(", text)
            if match:
                # Keep everything before the {if(
                main_text = text[: match.start()].strip()
                # Clean up trailing punctuation like "(e.g., " or ", "
                main_text = re.sub(r"\s*\(e\.g\.,?\s*$", "", main_text)
                main_text = re.sub(r",\s*$", "", main_text)
                main_text = re.sub(r"\(\s*$", "", main_text)
                if main_text:
                    text = main_text.strip()

        # Replace {site} placeholder with 'condition'
        text = re.sub(r"\{site\}", "condition", text, flags=re.IGNORECASE)

        # Fix encoding issues
        text = text.replace("Â", "").replace("â€™", "'").replace('â€"', "-")
        text = text.replace("â€œ", '"').replace("â€", '"')

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text.strip())

        return text

    def _normalize_with_umls(self, text: str) -> str:
        """Apply UMLS medical term normalization"""
        if not self.umls_client or not text:
            return text

        # Skip template variables
        if "{if(" in text or "{" in text:
            return text

        try:
            result = self.umls_client.normalize_term(text)
            if (
                result
                and result.name
                and hasattr(result, "confidence")
                and result.confidence > 0.85
            ):
                return result.name
        except Exception:
            pass

        return text

    def match_question(self, raw_question: str) -> Optional[MatchResult]:
        """
        Match question using 6-layer approach with early stopping

        Args:
            raw_question: Raw question text

        Returns:
            MatchResult if match found, None otherwise
        """
        mappings, questions, embeddings = self.get_known_mappings()

        if not mappings:
            return None

        cleaned = self._cleanup_text(raw_question)
        if not cleaned:
            return None

        normalized = cleaned.lower().strip()

        # Layer 0: Pattern Rules (100% confidence)
        # Catches known misclassification patterns before ML model can make errors
        for pattern, std_question in PATTERN_RULES:
            if pattern in normalized:
                return MatchResult(
                    standardized_question=std_question,
                    confidence=1.0,
                    method="pattern_rule",
                )

        # Layer 1: Exact Match (100% confidence)
        if normalized in mappings:
            return MatchResult(
                standardized_question=mappings[normalized],
                confidence=1.0,
                method="exact_match",
            )

        # Layer 2: ML Supervised Model. RandomForest over ~280 classes; even
        # correct predictions usually score 0.3-0.8 (probability mass spread
        # thin), so this layer uses ML_MIN_ACCEPT (0.30) instead of the cosine-
        # tuned CONFIDENCE_EARLY_STOP. We do NOT early-stop the ML layer --
        # always let semantic/umls/spacy/fuzzy run so the strongest signal wins
        # in the all_results comparison below.
        ml_result = None
        if self._ml_model and self._ml_label_encoder:
            ml_result = self._match_with_ml(cleaned)

        # Layer 3: Sentence Transformers Semantic (85-99%)
        semantic_result = self._match_semantic(cleaned, mappings, questions, embeddings)
        if semantic_result and semantic_result.confidence >= CONFIDENCE_EARLY_STOP:
            return semantic_result

        # Layer 4: UMLS Semantic Matching (85-98%)
        umls_result = None
        if self.umls_client:
            umls_result = self._match_with_umls(cleaned, mappings)
            if umls_result and umls_result.confidence >= CONFIDENCE_EARLY_STOP:
                return umls_result

        # Layer 5: spaCy biomedical NER (80-93%)
        spacy_result = None
        if self.nlp or self.nlp_ner:
            spacy_result = self._match_with_spacy(cleaned, mappings)
            if spacy_result and spacy_result.confidence >= CONFIDENCE_EARLY_STOP:
                return spacy_result

        # Layer 6: RapidFuzz Token Matching (70-85%)
        fuzzy_result = self._match_fuzzy(cleaned, mappings)
        if fuzzy_result and fuzzy_result.confidence >= CONFIDENCE_MIN_ACCEPT:
            return fuzzy_result

        # Return best result from all layers (even if below early stop). Each
        # layer is gated by its own min-accept threshold BEFORE entering the
        # comparison so a 0.05 ML prediction can't outrank a 0.40 fuzzy match.
        candidates = []
        if ml_result and ml_result.confidence >= ML_MIN_ACCEPT:
            candidates.append(ml_result)
        if semantic_result and semantic_result.confidence >= CONFIDENCE_MIN_ACCEPT:
            candidates.append(semantic_result)
        if (
            self.umls_client
            and umls_result
            and umls_result.confidence >= CONFIDENCE_MIN_ACCEPT
        ):
            candidates.append(umls_result)
        if (
            (self.nlp or self.nlp_ner)
            and spacy_result
            and spacy_result.confidence >= CONFIDENCE_MIN_ACCEPT
        ):
            candidates.append(spacy_result)
        if fuzzy_result and fuzzy_result.confidence >= CONFIDENCE_MIN_ACCEPT:
            candidates.append(fuzzy_result)

        if candidates:
            return max(candidates, key=lambda x: x.confidence)

        return None

    def _match_with_ml(self, question: str) -> Optional[MatchResult]:
        """Match using ML supervised model"""
        try:
            embedding = self.transformer_model.encode(
                [question], show_progress_bar=False
            )[0]
            probs = self._ml_model.predict_proba([embedding])[0]
            predicted_class = np.argmax(probs)
            confidence = float(probs[predicted_class])

            std_q = self._ml_label_encoder.inverse_transform([predicted_class])[0]

            return MatchResult(
                standardized_question=std_q, confidence=confidence, method="ml_model"
            )
        except Exception as e:
            logger.debug(f"ML matching error: {e}")
            return None

    def _match_semantic(
        self, question: str, mappings: Dict, questions: List, embeddings: np.ndarray
    ) -> Optional[MatchResult]:
        """Match using sentence transformer embeddings"""
        if len(embeddings) == 0:
            return None

        question_embedding = self.transformer_model.encode(
            question, convert_to_numpy=True, show_progress_bar=False
        )
        similarities = util.cos_sim(question_embedding, embeddings)[0].numpy()

        best_idx = np.argmax(similarities)
        best_score = float(similarities[best_idx])

        if best_score >= 0.85:
            matched_q = questions[best_idx]
            return MatchResult(
                standardized_question=mappings[matched_q],
                confidence=best_score,
                method="semantic_transformer",
                matched_to=matched_q[:50],
            )

        return None

    def _match_with_umls(self, question: str, mappings: Dict) -> Optional[MatchResult]:
        """Match using UMLS medical concepts"""
        try:
            result = self.umls_client.normalize_term(question)
            if not result or result.confidence < 0.85:
                return None

            # Check if normalized term matches any known question
            normalized_term = result.name.lower().strip() if result.name else ""

            for known_q, std_q in mappings.items():
                if normalized_term in known_q or known_q in normalized_term:
                    return MatchResult(
                        standardized_question=std_q,
                        confidence=min(result.confidence, 0.92),
                        method="umls_semantic",
                        matched_to=f"UMLS: {result.cui}",
                    )

            return None
        except Exception:
            return None

    def _match_with_spacy(self, question: str, mappings: Dict) -> Optional[MatchResult]:
        """Match using spaCy biomedical NER entity extraction"""
        try:
            question_lower = question.lower()

            # Extract medical entities using specialized NER model (chemical/disease)
            medical_entities = []
            if self.nlp_ner:
                doc_ner = self.nlp_ner(question)
                medical_entities = [
                    (ent.text.lower(), ent.label_) for ent in doc_ner.ents
                ]

            # Extract general entities using biomedical model
            general_entities = []
            if self.nlp:
                doc = self.nlp(question)
                general_entities = [(ent.text.lower(), ent.label_) for ent in doc.ents]

            # Match based on medical entity types (DISEASE, CHEMICAL from BC5CDR)
            disease_entities = [e[0] for e in medical_entities if e[1] == "DISEASE"]
            chemical_entities = [e[0] for e in medical_entities if e[1] == "CHEMICAL"]

            # High-confidence medical entity matching
            if disease_entities:
                for disease in disease_entities:
                    for known_q, std_q in mappings.items():
                        if disease in known_q or known_q in disease:
                            return MatchResult(
                                standardized_question=std_q,
                                confidence=0.92,
                                method="spacy_medical_ner",
                                matched_to=f"DISEASE: {disease}",
                            )

            if chemical_entities:
                # Likely asking about medications/treatments
                for chemical in chemical_entities:
                    for known_q, std_q in mappings.items():
                        if (
                            chemical in known_q
                            or "treatment" in known_q
                            or "medication" in known_q
                        ):
                            return MatchResult(
                                standardized_question=std_q,
                                confidence=0.90,
                                method="spacy_medical_ner",
                                matched_to=f"CHEMICAL: {chemical}",
                            )

            # Rule-based matching for common demographic patterns with DISAMBIGUATION
            # Fix: Use scored ranking instead of returning first match

            # AGE disambiguation - score candidates based on context keywords
            if "age" in question_lower or "old" in question_lower:
                age_disambiguation = {
                    "diagnosis": [
                        "diagnosis",
                        "diagnosed",
                        "when you were diagnosed",
                        "when diagnosed",
                    ],
                    "symptom_onset": [
                        "symptom",
                        "symptoms",
                        "onset",
                        "first noticed",
                        "began",
                        "started",
                    ],
                    "loved_one": [
                        "loved one",
                        "person in your care",
                        "caregiver",
                        "family member",
                    ],
                }

                candidates = []
                for known_q, std_q in mappings.items():
                    if "age" not in known_q and "old" not in known_q:
                        continue

                    # Calculate disambiguation score
                    score = 0.0
                    matched_context = None

                    # Check context markers in BOTH question AND known mapping
                    for context_key, context_keywords in age_disambiguation.items():
                        question_has_context = any(
                            kw in question_lower for kw in context_keywords
                        )
                        known_has_context = any(
                            kw in known_q for kw in context_keywords
                        )

                        if question_has_context and known_has_context:
                            score += 0.3  # Strong match - both have same context
                            matched_context = context_key
                        elif question_has_context != known_has_context:
                            score -= 0.15  # Penalty for mismatch

                    # Add token overlap score
                    question_tokens = set(question_lower.split())
                    known_tokens = set(known_q.split())
                    overlap = len(question_tokens & known_tokens) / max(
                        len(question_tokens), 1
                    )
                    score += overlap * 0.4

                    candidates.append((std_q, score, known_q, matched_context))

                if candidates:
                    # Return best scoring candidate
                    best = max(candidates, key=lambda x: x[1])
                    return MatchResult(
                        standardized_question=best[0],
                        confidence=min(0.85 + best[1] * 0.1, 0.93),
                        method="spacy_demographic_pattern",
                        matched_to=f"age_{best[3] or 'general'}: {best[2][:40]}",
                    )

            # GENDER disambiguation - score candidates
            if any(
                term in question_lower for term in ["sex", "gender", "male", "female"]
            ):
                gender_disambiguation = {
                    "biological": ["biological", "birth", "assigned at birth"],
                    "identity": ["identity", "identify", "describes"],
                }

                candidates = []
                for known_q, std_q in mappings.items():
                    if "gender" not in known_q and "sex" not in known_q:
                        continue

                    score = 0.0
                    matched_context = None

                    for context_key, context_keywords in gender_disambiguation.items():
                        question_has_context = any(
                            kw in question_lower for kw in context_keywords
                        )
                        known_has_context = any(
                            kw in known_q for kw in context_keywords
                        )

                        if question_has_context and known_has_context:
                            score += 0.3
                            matched_context = context_key
                        elif question_has_context != known_has_context:
                            score -= 0.1

                    # Token overlap
                    question_tokens = set(question_lower.split())
                    known_tokens = set(known_q.split())
                    overlap = len(question_tokens & known_tokens) / max(
                        len(question_tokens), 1
                    )
                    score += overlap * 0.4

                    candidates.append((std_q, score, known_q, matched_context))

                if candidates:
                    best = max(candidates, key=lambda x: x[1])
                    return MatchResult(
                        standardized_question=best[0],
                        confidence=min(0.85 + best[1] * 0.1, 0.93),
                        method="spacy_demographic_pattern",
                        matched_to=f"gender_{best[3] or 'general'}: {best[2][:40]}",
                    )

            # EMAIL - less ambiguous, simpler matching is OK
            if "email" in question_lower or "@" in question:
                for known_q, std_q in mappings.items():
                    if "email" in known_q:
                        return MatchResult(
                            standardized_question=std_q,
                            confidence=0.92,
                            method="spacy_ner",
                        )

            # Match based on extracted biomedical entities
            all_entities = set([e[0] for e in general_entities + medical_entities])
            if all_entities:
                for entity in all_entities:
                    for known_q, std_q in mappings.items():
                        if (
                            entity in known_q and len(entity) > 3
                        ):  # Avoid short false positives
                            return MatchResult(
                                standardized_question=std_q,
                                confidence=0.85,
                                method="spacy_entity",
                                matched_to=entity[:50],
                            )

            return None
        except Exception:
            return None

    def _match_fuzzy(self, question: str, mappings: Dict) -> Optional[MatchResult]:
        """Match using RapidFuzz token matching"""
        best_match = None
        best_score = 0.0
        best_question = None

        for known_q, std_q in mappings.items():
            score = fuzz.token_sort_ratio(question, known_q) / 100.0
            if score > best_score:
                best_score = score
                best_match = std_q
                best_question = known_q

        if best_score >= 0.80:
            return MatchResult(
                standardized_question=best_match,
                confidence=best_score,
                method="rapidfuzz",
                matched_to=best_question[:50] if best_question else None,
            )

        return None

    # ==========================================================================
    # ANSWER NORMALIZATION FUNCTIONS
    # ==========================================================================

    def _normalize_yes_no(self, answer: str) -> str:
        """Normalize Yes/No variants to standard form"""
        yes_no_map = {
            "yes": "Yes",
            "y": "Yes",
            "no": "No",
            "n": "No",
            "none": "None",
        }
        return yes_no_map.get(answer.lower(), answer)

    def _normalize_agreement_scale(self, answer: str, question: str = "") -> str:
        """Normalize agreement/Likert scale values"""
        answer_lower = answer.lower().strip()

        # Check if this is an agreement-type question
        agreement_questions = [
            "agreement_scale",
            "webinar_feedback_overall",
            "patient_caregiver_collab_adherence_importance",
            "patient_caregiver_collab_decisions_importance",
        ]

        # Only apply numeric conversion for agreement-type questions
        is_agreement_question = any(q in question.lower() for q in agreement_questions)

        agreement_map = {
            # Numeric to text (only for agreement questions)
            "1": "Strongly Disagree" if is_agreement_question else answer,
            "2": "Disagree" if is_agreement_question else answer,
            "3": "Neutral" if is_agreement_question else answer,
            "4": "Agree" if is_agreement_question else answer,
            "5": "Strongly Agree" if is_agreement_question else answer,
            # Labeled numeric
            "1 strongly disagree": "Strongly Disagree",
            "2 disagree": "Disagree",
            "3 neutral": "Neutral",
            "4 agree": "Agree",
            "5 strongly agree": "Strongly Agree",
            # Lowercase to Title Case
            "strongly agree": "Strongly Agree",
            "strongly disagree": "Strongly Disagree",
            "agree": "Agree",
            "disagree": "Disagree",
            "neutral": "Neutral",
            "somewhat agree": "Somewhat Agree",
            "somewhat disagree": "Somewhat Disagree",
            "neither agree nor disagree": "Neither Agree Nor Disagree",
            "neither_agree_nor_disagree": "Neither Agree Nor Disagree",
            # Underscore variants
            "strongly_agree": "Strongly Agree",
            "strongly_disagree": "Strongly Disagree",
            "somewhat_agree": "Somewhat Agree",
            "somewhat_disagree": "Somewhat Disagree",
            "neutral_agreement": "Neutral",
            # Additional variants
            "completely agree": "Completely Agree",
            "completely_agree": "Completely Agree",
            "completely disagree": "Completely Disagree",
            "completely_disagree": "Completely Disagree",
            "mostly agree": "Mostly Agree",
            "mostly_agree": "Mostly Agree",
            "mostly disagree": "Mostly Disagree",
            "mostly_disagree": "Mostly Disagree",
            "slightly agree": "Slightly Agree",
            "slightly_agree": "Slightly Agree",
            "slightly disagree": "Slightly Disagree",
            "slightly_disagree": "Slightly Disagree",
        }
        return agreement_map.get(answer_lower, answer)

    def _normalize_satisfaction_scale(self, answer: str) -> str:
        """Normalize satisfaction scale values"""
        satisfaction_map = {
            "very satisfied": "Very Satisfied",
            "very_satisfied": "Very Satisfied",
            "satisfied": "Satisfied",
            "dissatisfied": "Dissatisfied",
            "very dissatisfied": "Very Dissatisfied",
            "very_dissatisfied": "Very Dissatisfied",
            "somewhat satisfied": "Somewhat Satisfied",
            "somewhat_satisfied": "Somewhat Satisfied",
            "somewhat dissatisfied": "Somewhat Dissatisfied",
            "somewhat_dissatisfied": "Somewhat Dissatisfied",
            "neither satisfied nor dissatisfied": "Neither Satisfied Nor Dissatisfied",
            "neutral_satisfaction": "Neutral",
            "neutral": "Neutral",  # Added: standalone neutral
            "no_treatment": "No Treatment",
            "not_applicable": "Not Applicable",
        }
        return satisfaction_map.get(answer.lower(), answer)

    def _normalize_likelihood_scale(self, answer: str) -> str:
        """Normalize likelihood scale values"""
        likelihood_map = {
            "very likely": "Very Likely",
            "very_likely": "Very Likely",
            "likely": "Likely",
            "unlikely": "Unlikely",
            "very unlikely": "Very Unlikely",
            "very_unlikely": "Very Unlikely",
            "somewhat likely": "Somewhat Likely",
            "somewhat_likely": "Somewhat Likely",
            "somewhat unlikely": "Somewhat Unlikely",
            "somewhat_unlikely": "Somewhat Unlikely",
            "not at all likely": "Not At All Likely",
            "not_at_all_likely": "Not At All Likely",
            "extremely likely": "Extremely Likely",
            "extremely_likely": "Extremely Likely",
            "neutral_likelihood": "Neutral",
            "neutral": "Neutral",  # Added: standalone neutral
            "actively_changing": "Actively Changing",
            "actively changing": "Actively Changing",
            "not_sure": "Not Sure",
            "not sure": "Not Sure",
            "not_considering": "Not Considering",
            "not considering": "Not Considering",
            "very_interested": "Very Interested",
            "very interested": "Very Interested",
            "slightly_interested": "Slightly Interested",
            "slightly interested": "Slightly Interested",
            "not_interested": "Not Interested",
            "not interested": "Not Interested",
            "interested": "Interested",
        }
        return likelihood_map.get(answer.lower(), answer)

    def _normalize_knowledge_scale(self, answer: str) -> str:
        """Normalize knowledge scale values"""
        knowledge_map = {
            "very knowledgeable": "Very Knowledgeable",
            "very_knowledgeable": "Very Knowledgeable",
            "knowledgeable": "Knowledgeable",
            "somewhat knowledgeable": "Somewhat Knowledgeable",
            "not knowledgeable": "Not Knowledgeable",
            "not_knowledgeable": "Not Knowledgeable",
            "not knowledgeable at all": "Not Knowledgeable At All",
            "not_knowledgeable_at_all": "Not Knowledgeable At All",
            "extremely knowledgeable": "Extremely Knowledgeable",
            "neutral_knowledge": "Neutral",
        }
        return knowledge_map.get(answer.lower(), answer)

    def _normalize_quality_scale(self, answer: str) -> str:
        """Normalize quality scale values (Excellent/Good/Fair/Poor)"""
        quality_map = {
            "excellent": "Excellent",
            "very good": "Very Good",
            "very_good": "Very Good",
            "good": "Good",
            "fair": "Fair",
            "poor": "Poor",
            "very poor": "Very Poor",
            "very_poor": "Very Poor",
            "average": "Average",
            "above average": "Above Average",
            "above_average": "Above Average",
            "below average": "Below Average",
            "below_average": "Below Average",
        }
        return quality_map.get(answer.lower(), answer)

    def _normalize_severity_scale(self, answer: str) -> str:
        """Normalize severity scale values (None/Mild/Moderate/Severe/Extreme)"""
        severity_map = {
            "none": "None",
            "no symptoms": "No Symptoms",
            "no_symptoms": "No Symptoms",
            "mild": "Mild",
            "moderate": "Moderate",
            "severe": "Severe",
            "extreme": "Extreme",
            "very severe": "Very Severe",
            "very_severe": "Very Severe",
            "minimal": "Minimal",
            "significant": "Significant",
        }
        return severity_map.get(answer.lower(), answer)

    def _normalize_interest_scale(self, answer: str) -> str:
        """Normalize interest level values"""
        interest_map = {
            "interested": "Interested",
            "very interested": "Very Interested",
            "very_interested": "Very Interested",
            "extremely interested": "Extremely Interested",
            "somewhat interested": "Somewhat Interested",
            "somewhat_interested": "Somewhat Interested",
            "not interested": "Not Interested",
            "not_interested": "Not Interested",
            "not interested at all": "Not Interested At All",
            "neutral": "Neutral",
            "undecided": "Undecided",
            "curious": "Curious",
        }
        return interest_map.get(answer.lower(), answer)

    def _normalize_consent(self, answer: str) -> str:
        """Normalize consent values"""
        consent_map = {
            "agree": "Agree",
            "i agree": "Agree",
            "i agree.": "Agree",
            "disagree": "Disagree",
            "i do not agree": "Disagree",
            "i do not agree.": "Disagree",
            "no, i don't consent": "Disagree",
        }
        return consent_map.get(answer.lower(), answer)

    def _normalize_does_not_apply(self, answer: str) -> str:
        """Normalize 'does not apply' variants"""
        dna_variants = [
            "this does not apply to me.",
            "this does not apply to me",
            "not applicable.",
            "not applicable",
            "does not apply",
            "not applicable to me",
        ]
        if answer.lower() in dna_variants:
            return "Does Not Apply"
        return answer

    def _normalize_age_at_diagnosis(self, answer: str) -> str:
        """Normalize age at diagnosis values"""
        age_map = {
            "under_18": "Under 18",
            "age_0_7": "0 to 7",
            "age_8_14": "8 to 14",
            "age_15_24": "15 to 24",
            "age_18_24": "18 to 24",
            "age_25_34": "25 to 34",
            "age_25_39": "25 to 39",
            "age_35_44": "35 to 44",
            "age_45_54": "45 to 54",
            "age_55_64": "55 to 64",
            "age_40_plus": "40 Plus",
            "age_65_plus": "65 Plus",
            "not_diagnosed": "Not Diagnosed",
        }
        return age_map.get(answer.lower(), answer)

    def _normalize_age_range(self, answer: str) -> str:
        """Normalize general age range values"""
        # Quick validation: if answer is too long or contains medical/clinical terms,
        # it's not an age value (e.g., "Acute Physiology and Chronic Health Evaluation...")
        if len(answer) > 30:
            return answer  # Age values are short

        answer_lower = answer.lower()
        invalid_terms = [
            "acute",
            "chronic",
            "physiology",
            "evaluation",
            "result",
            "original",
            "health",
            "score",
            "scale",
            "test",
            "assessment",
            "index",
            "apch",
        ]
        if any(term in answer_lower for term in invalid_terms):
            return answer  # Not an age value

        age_map = {
            # Under variants
            "under 18": "Under 18",
            "under_18": "Under 18",
            "younger than 18 years": "Under 18",
            "younger than 18": "Under 18",
            "under 5": "Under 5",
            "under_5": "Under 5",
            # Age ranges - underscore format
            "0_to_7": "0 to 7",
            "5_to_12": "5 to 12",
            "13_to_17": "13 to 17",
            "18_to_24": "18 to 24",
            "25_to_34": "25 to 34",
            "35_to_44": "35 to 44",
            "45_to_54": "45 to 54",
            "55_to_64": "55 to 64",
            # Age ranges - hyphen format (MUST catch before UMLS corrupts them)
            "0-7": "0 to 7",
            "0-7 years": "0 to 7",
            "5-12": "5 to 12",
            "13-17": "13 to 17",
            "18-24": "18 to 24",
            "25-34": "25 to 34",
            "35-44": "35 to 44",
            "45-54": "45 to 54",
            "55-64": "55 to 64",
            # Plus formats
            "65+": "65 Plus",
            "65_plus": "65 Plus",
            "55+": "55 Plus",
            "55_plus": "55 Plus",
            "18+": "18 Plus",
            "18_plus": "18 Plus",
            # Prefer not to say
            "prefer not to answer": "Prefer Not To Say",
            "prefer not to say": "Prefer Not To Say",
            "prefer_not_to_say": "Prefer Not To Say",
            "prefer_not_to_answer": "Prefer Not To Say",
        }

        # Check mapped values first
        if answer.lower() in age_map:
            return age_map[answer.lower()]

        # Handle raw numeric ages (e.g., "7", "32", "57")
        if answer.isdigit():
            age = int(answer)
            if age < 5:
                return "Under 5"
            elif age <= 12:
                return "5 to 12"
            elif age <= 17:
                return "13 to 17"
            elif age <= 24:
                return "18 to 24"
            elif age <= 34:
                return "25 to 34"
            elif age <= 44:
                return "35 to 44"
            elif age <= 54:
                return "45 to 54"
            elif age <= 64:
                return "55 to 64"
            else:
                return "65 Plus"

        return answer

    def _normalize_gender(self, answer: str) -> str:
        """Normalize gender values"""
        gender_map = {
            "male": "Male",
            "female": "Female",
            "other": "Other",
            "prefer not to say": "Prefer Not To Say",
            "prefer_not_to_say": "Prefer Not To Say",
            "prefer not to answer": "Prefer Not To Say",
            "non-binary": "Non-Binary",
            "non_binary": "Non-Binary",
        }
        return gender_map.get(answer.lower(), answer)

    def _normalize_time_since_diagnosis(self, answer: str) -> str:
        """Normalize time since diagnosis values"""
        time_map = {
            "less_than_1_year": "Less Than 1 Year",
            "1_2_years": "1 to 2 Years",
            "3_5_years": "3 to 5 Years",
            "6_10_years": "6 to 10 Years",
            "11_15_years": "11 to 15 Years",
            "16_20_years": "16 to 20 Years",
            "more_than_10_years": "More Than 10 Years",
        }
        return time_map.get(answer.lower(), answer)

    def _normalize_caregiver_support(self, answer: str, question: str = "") -> str:
        """Normalize caregiver support values"""
        if (
            "caregiver" not in question.lower()
            and "emotional_support" not in question.lower()
        ):
            return answer

        support_map = {
            "1": "Poor",
            "2": "Fair",
            "3": "Good",
            "good": "Good",
            "fair": "Fair",
            "poor": "Poor",
            "excellent": "Excellent",
        }
        return support_map.get(answer.lower(), answer)

    def _normalize_other_specify(self, answer: str) -> str:
        """Normalize 'other specify' and '-oth-' code values"""
        answer_lower = answer.lower().strip()
        if answer_lower in ["other_specify", "-oth-", "oth", "other"]:
            return "Other"
        return answer

    def _normalize_source(self, answer: str) -> str:
        """Normalize source tracking values"""
        source_map = {
            "source_eblast": "Email Blast",
            "source_sidebar": "Sidebar",
            "eblast": "Email Blast",
            "eblat": "Email Blast",  # typo
            "sidebar": "Sidebar",
            "bitly": "Bitly Link",
            "facebook": "Facebook",
            "twitter": "Twitter",
            "website": "Website",
            "email": "Email",
            "newsletter": "Newsletter",
            "social media": "Social Media",
            "social_media": "Social Media",
        }
        return source_map.get(answer.lower(), answer)

    def _normalize_ethnicity(self, answer: str) -> str:
        """Normalize ethnicity values"""
        ethnicity_map = {
            "prefer_not_to_answer": "Prefer Not To Answer",
            "prefer not to answer": "Prefer Not To Answer",
            "prefer_not_to_say": "Prefer Not To Say",
            "two_or_more_races": "Two Or More Races",
            "american_indian_or_alaska_native": "American Indian Or Alaska Native",
            "native_hawaiian_or_pacific_islander": "Native Hawaiian Or Pacific Islander",
            "pacific_islander": "Pacific Islander",
            "black_or_african_american": "Black Or African American",
            "native_american": "Native American",
            "middle_eastern": "Middle Eastern",
            "white": "White",
            "asian": "Asian",
            "hispanic": "Hispanic",
            "latino": "Latino",
            "hispanic_or_latino": "Hispanic Or Latino",
        }
        return ethnicity_map.get(answer.lower(), answer)

    def _normalize_communication_preferences(self, answer: str) -> str:
        """Normalize communication preference values"""
        comm_map = {
            "real_time_alerts": "Real Time Alerts",
            "weekly_summaries": "Weekly Summaries",
            "daily_summaries": "Daily Summaries",
            "monthly_summaries": "Monthly Summaries",
            "less_than_monthly": "Less Than Monthly",
            "multiple_times_daily": "Multiple Times Daily",
            "weekly": "Weekly",
            "daily": "Daily",
            "monthly": "Monthly",
        }
        return comm_map.get(answer.lower(), answer)

    def _normalize_frequency(self, answer: str) -> str:
        """Normalize frequency values (yearly, monthly, etc.)"""
        freq_map = {
            "yearly": "Yearly",
            "monthly": "Monthly",
            "weekly": "Weekly",
            "daily": "Daily",
            "rarely": "Rarely",
            "often": "Often",
            "sometimes": "Sometimes",
            "never": "Never",
            "twice a year": "Twice A Year",
            "every 4-6 months": "Every 4-6 Months",
            "every 2-3 months": "Every 2-3 Months",
        }
        return freq_map.get(answer.lower(), answer)

    def _normalize_count_range(self, answer: str) -> str:
        """Normalize count range answers like '1-2 misdiagnoses', '2-3 approved therapies'"""
        # Patterns with misdiagnoses
        misdiag_map = {
            "1-2 misdiagnoses": "1-2 Misdiagnoses",
            "3-4 misdiagnoses": "3-4 Misdiagnoses",
            "5+ misdiagnoses": "5+ Misdiagnoses",
        }
        if answer.lower() in misdiag_map:
            return misdiag_map[answer.lower()]

        # Patterns with approved therapies
        therapy_map = {
            "1 approved therapy": "1 Approved Therapy",
            "2-3 approved therapies": "2-3 Approved Therapies",
            "4-5 approved therapies": "4-5 Approved Therapies",
            "5 or more>5 approved therapies": "5+ Approved Therapies",  # Fix bad concat
            "5+ approved therapies": "5+ Approved Therapies",
        }
        if answer.lower() in therapy_map:
            return therapy_map[answer.lower()]

        # FEV1 patterns
        fev1_map = {
            "80% or above of predicted value": "80%+ of Predicted Value",
            "79-71% of predicted value": "71-79% of Predicted Value",
            "50-70% of predicted value": "50-70% of Predicted Value",
            "30-40% of predicted value": "30-40% of Predicted Value",
            "less than 30% of predicted value": "<30% of Predicted Value",
        }
        if answer.lower() in fev1_map:
            return fev1_map[answer.lower()]

        return answer

    def _normalize_yes_no(self, answer: str) -> str:
        """Normalize Yes/No variations to consistent format."""
        yes_no_map = {
            "yes": "Yes",
            "yes.": "Yes",
            "y": "Yes",
            "true": "Yes",
            "no": "No",
            "no.": "No",
            "n": "No",
            "false": "No",
        }
        return yes_no_map.get(answer.lower().strip(), answer)

    def _normalize_likert_scale(self, answer: str, question: str) -> str:
        """Normalize Likert scale answers (1-5) with consistent labels.

        Different questions use different endpoint labels. This normalizes
        numeric-only answers to include context from question type.
        """
        answer_stripped = answer.strip()

        # Define scale mappings for different question types
        likert_scales = {
            "fatigue": {
                "1": "1 - Not fatiguing at all",
                "2": "2 - Slightly fatiguing",
                "3": "3 - Moderately fatiguing",
                "4": "4 - Very fatiguing",
                "5": "5 - Extremely fatiguing",
                "1 not fatiguing at all": "1 - Not fatiguing at all",
                "5 fatigues very quickly": "5 - Extremely fatiguing",
            },
            "impact": {
                "1": "1 - No impact",
                "2": "2 - Minor impact",
                "3": "3 - Moderate impact",
                "4": "4 - Significant impact",
                "5": "5 - Major impact",
                "1 no impact at all": "1 - No impact",
                "1 least impactful": "1 - No impact",
                "5 significant impact": "5 - Major impact",
                "5 most impactful": "5 - Major impact",
            },
            "helpful": {
                "1": "1 - Not helpful at all",
                "2": "2 - Slightly helpful",
                "3": "3 - Moderately helpful",
                "4": "4 - Very helpful",
                "5": "5 - Extremely helpful",
                "1 (not helpful at all)": "1 - Not helpful at all",
                "5 (extremely helpful)": "5 - Extremely helpful",
            },
            "support": {
                "1": "1 - No support",
                "2": "2 - Minimal support",
                "3": "3 - Some support",
                "4": "4 - Good support",
                "5": "5 - Excellent support",
            },
            "default": {
                "1": "1 - Lowest",
                "2": "2 - Low",
                "3": "3 - Medium",
                "4": "4 - High",
                "5": "5 - Highest",
            },
        }

        # Determine which scale to use based on question
        question_lower = question.lower()
        if "fatigue" in question_lower:
            scale = likert_scales["fatigue"]
        elif (
            "impact" in question_lower
            or "milestone" in question_lower
            or "qol" in question_lower
        ):
            scale = likert_scales["impact"]
        elif (
            "helpful" in question_lower
            or "feedback" in question_lower
            or "webinar" in question_lower
        ):
            scale = likert_scales["helpful"]
        elif "support" in question_lower or "community" in question_lower:
            scale = likert_scales["support"]
        else:
            scale = likert_scales["default"]

        # Check if answer matches a scale value
        answer_lower = answer_stripped.lower()
        if answer_lower in scale:
            return scale[answer_lower]

        return answer

    def _normalize_site_name(self, answer: str) -> str:
        """Normalize site/disease name values to Title Case"""
        # First, clean up URL-encoded values and URL garbage
        cleaned = answer
        if "%20" in cleaned:
            cleaned = cleaned.replace("%20", " ")
        if "?" in cleaned:  # Remove URL parameters
            cleaned = cleaned.split("?")[0]

        site_map = {
            # Lowercase conditions
            "cystic fibrosis": "Cystic Fibrosis",
            "pulmonary hypertension": "Pulmonary Hypertension",
            "lupus": "Lupus",
            "spinal muscular atrophy": "Spinal Muscular Atrophy",
            "hemophilia": "Hemophilia",
            "angioedema": "Angioedema",
            "hypoparathyroidism": "Hypoparathyroidism",
            "porphyria": "Porphyria",
            "epidermolysis bullosa": "Epidermolysis Bullosa",
            "neuromyelitis optica spectrum disorder": "Neuromyelitis Optica Spectrum Disorder",
            "familial amyloid polyneuropathy": "Familial Amyloid Polyneuropathy",
            "myasthenia gravis": "Myasthenia Gravis",
            "amyotrophic lateral sclerosis": "Amyotrophic Lateral Sclerosis",
            "multiple sclerosis": "Multiple Sclerosis",
            "muscular dystrophy": "Muscular Dystrophy",
            "scleroderma": "Scleroderma",
            "sarcoidosis": "Sarcoidosis",
            "pulmonary fibrosis": "Pulmonary Fibrosis",
            "sickle cell disease": "Sickle Cell Disease",
            "cold agglutinin disease": "Cold Agglutinin Disease",
            "bleeding disorder": "Bleeding Disorder",
            # Partial title case fixes
            "parkinson's disease": "Parkinson's Disease",
            "ehlers-danlos syndrome": "Ehlers-Danlos Syndrome",
            "ehlers-danlos syndrom": "Ehlers-Danlos Syndrome",  # typo fix
            "anca-associated vasculitis": "ANCA-Associated Vasculitis",
            "charcot-marie-tooth disease": "Charcot-Marie-Tooth Disease",
            "alzheimer's disease": "Alzheimer's Disease",
            "huntington's disease": "Huntington's Disease",
            "pompe disease": "Pompe Disease",
            "friedreich's ataxia": "Friedreich's Ataxia",
            "angelman syndrome": "Angelman Syndrome",
            # Sjogren's variants (handle encoding issues)
            "sjogren's syndrome": "Sjogren's Syndrome",
            "sjögren's syndrome": "Sjogren's Syndrome",
            "sjogrens syndrome": "Sjogren's Syndrome",
            # Acronyms - keep proper case
            "ahus": "aHUS",
            "copd": "COPD",
        }
        # Check for Sjogren's with various encodings
        answer_lower = cleaned.lower()
        if "sj" in answer_lower and "gren" in answer_lower:
            return "Sjogren's Syndrome"
        return site_map.get(answer_lower, cleaned)

    def _normalize_condition(self, answer: str) -> str:
        """Normalize condition/diagnosis values"""
        condition_map = {
            # Special values
            "-oth-": "Other",
            "other": "Other",
            "unspecified": "Unspecified",
            # Epidermolysis Bullosa variants
            "condition simplex": "Epidermolysis Bullosa Simplex",
            "condition acquisita": "Epidermolysis Bullosa Acquisita",
            "epidermolysis bullosa simplex": "Epidermolysis Bullosa Simplex",
            "epidermolysis bullosa acquisita": "Epidermolysis Bullosa Acquisita",
            "epidermolysis bullosa": "Epidermolysis Bullosa",
            # Porphyria variants
            "condition cutanea tarda": "Porphyria Cutanea Tarda",
            "porphyria cutanea tarda": "Porphyria Cutanea Tarda",
            # Common lowercase conditions
            "vasculitis": "Vasculitis",
            "copd": "COPD",
            "emphysema": "Emphysema",
            "sjogren": "Sjogren's Syndrome",
            "sjogren's": "Sjogren's Syndrome",
            "lgmd": "LGMD",
            "egpa": "EGPA",
            "lupus": "Lupus",
            "neuropathy": "Neuropathy",
            "long covid": "Long COVID",
        }
        answer_lower = answer.lower().strip()

        # Direct lookup
        if answer_lower in condition_map:
            return condition_map[answer_lower]

        # Title case for single-word conditions that aren't acronyms
        if len(answer.split()) == 1 and answer_lower == answer and len(answer) > 3:
            return answer.title()

        return answer

    def _normalize_treatment(self, answer: str) -> str:
        """Normalize treatment/medication values to proper casing."""
        treatment_map = {
            # Common lowercase medications
            "metoprolol": "Metoprolol",
            "ketoconazole": "Ketoconazole",
            "rituximab": "Rituximab",
            "retuximab": "Rituximab",  # typo
            "meloxicam": "Meloxicam",
            "selegiline": "Selegiline",
            "lorazepam": "Lorazepam",
            "rasagline": "Rasagiline",  # typo
            "rasagiline": "Rasagiline",
            "depakote": "Depakote",
            "eliquis": "Eliquis",
            "antidepressants": "Antidepressants",
            "duoneb": "DuoNeb",
            # Abbreviations
            "pt": "Physical Therapy",
            "o2": "Oxygen Therapy",
            "oxygen 24/7": "Oxygen Therapy (Continuous)",
            "otc": "Over-the-Counter Medications",
            "na": "Not Applicable",
            "ok": "Not Specified",
            # Informal
            "stem cells": "Stem Cell Therapy",
            "nutrition": "Nutritional Therapy",
            "chiropractic care": "Chiropractic Care",
            "no treatment available": "No Treatment Available",
            "staying inside": "Lifestyle Modifications",
            # Typos
            "exerciise. diet": "Exercise and Diet",
        }
        answer_lower = answer.lower().strip()

        # Direct lookup
        if answer_lower in treatment_map:
            return treatment_map[answer_lower]

        # If all lowercase and not an acronym, title case it
        if answer_lower == answer and len(answer) > 3 and not answer.isupper():
            return answer.title()

        return answer

    def _normalize_info_sources(self, answer: str) -> str:
        """Normalize info_sources values (clinic names, etc.)"""
        info_map = {
            # Fix lowercase 'condition' in clinic names
            "condition clinic": "Condition Clinic",
        }
        return info_map.get(answer.lower(), answer)

    def _normalize_pipe_separated(self, answer: str, normalizer_fn) -> str:
        """Apply a normalization function to each pipe-separated value.

        Args:
            answer: Potentially pipe-separated answer string
            normalizer_fn: Normalization function to apply to each part

        Returns:
            Normalized answer with all parts processed
        """
        if "|" not in answer:
            return normalizer_fn(answer)

        parts = answer.split("|")
        normalized_parts = [normalizer_fn(part.strip()) for part in parts]
        return "|".join(normalized_parts)

    # Questions that are free-text/PII and should NOT be normalized
    FREE_TEXT_QUESTIONS = {
        "email",
        "last_name",
        "first_name",
        "full_name",
        "contact_info",
        "pvid",
        "gift_card_sweepstakes",
        "condition_improvement_suggestions",
        "diagnostic_journey_improvement_ideas",
        "treatment_journey_comments",
        "independence_redefinition",
        "mobility_aids_decision_drivers",
        "coping_misunderstood_speech",
        "progression_life_events",
        "peer_support_strategies",
        "additional_insights",
        "word_choice_reason",
        "clinical_trial_details",
        "current_medications_list",
        "info_resources_most_helpful",
        "url",
        "specific_questions",
        # Gene-therapy open-ended canonicals (split out from gene_therapy_interest
        # on 2026-06-04). Cascade rewriting was modifying respondent text on
        # 70-99% of rows; per the rule "do not change free-text answers", these
        # bypass semantic normalization going forward.
        "gene_therapy_info_needs",
        "gene_therapy_concerns_barriers",
        "gene_therapy_decision_influencers",
        "gene_therapy_expectations",
        "gene_therapy_open_text",
        "gene_therapy_other_concerns",
        "gene_therapy_pretreatment_expectations",
        "gene_therapy_experience_vs_expectations",
    }

    def _replace_placeholders(self, answer: str) -> str:
        """Replace placeholder values like {site}, {condition}, {if(...)} with generic text."""
        if not answer or "{" not in answer:
            return answer

        result = answer

        # First handle complex {if(...)} template expressions
        # Pattern: {if(site == "...", "a ", "")}{site} -> "condition"
        result = re.sub(r"\{if\([^}]+\}\{site\}", "the condition", result)
        result = re.sub(r"\{if\([^}]+\}", "", result)  # Remove any remaining {if...}

        # Replace common placeholders with appropriate generic text
        replacements = {
            "{site}": "the condition",
            "{Site}": "the condition",
            "{SITE}": "the condition",
            "{condition}": "the condition",
            "{Condition}": "the condition",
            "{disease}": "the condition",
            "{Disease}": "the condition",
        }

        for placeholder, replacement in replacements.items():
            result = result.replace(placeholder, replacement)

        # Clean up any remaining curly braces (catch-all)
        result = re.sub(r"\{[^}]*\}", "", result)

        # Clean up extra whitespace that may result
        result = re.sub(r"\s+", " ", result).strip()

        return result

    def _normalize_condition_relationship(self, answer: str) -> str:
        """Normalize condition_relationship to standardized categories."""
        answer_lower = answer.lower().strip()

        # Direct mappings
        if answer_lower == "patient":
            return "Patient"
        if answer_lower in ["other", "-oth-"]:
            return "Other"

        # Caregiver of adult patterns
        adult_caregiver_patterns = [
            "caregiver of an adult",
            "caregiver of adult",
            "parent of",
            "spouse",
            "husband",
            "wife",
            "mother of an adult",
            "father of an adult",
        ]
        if any(p in answer_lower for p in adult_caregiver_patterns):
            return "Caregiver of Adult"

        # Caregiver of child patterns
        child_caregiver_patterns = [
            "caregiver of a child",
            "caregiver of child",
            "parent of a child",
            "grandparent of",
        ]
        if any(p in answer_lower for p in child_caregiver_patterns):
            return "Caregiver of Child"

        # Patient with condition (handles "I live with {site}")
        patient_patterns = ["i live with", "i have", "person with", "diagnosed with"]
        if any(p in answer_lower for p in patient_patterns):
            return "Patient"

        # Both patient and caregiver
        if "both" in answer_lower and "caregiver" in answer_lower:
            return "Patient and Caregiver"

        # Family member patterns
        family_patterns = [
            "family member",
            "grandparent",
            "sibling",
            "brother",
            "sister",
        ]
        if any(p in answer_lower for p in family_patterns):
            return "Family Member"

        # Healthcare professional
        hcp_patterns = [
            "rn",
            "nurse",
            "doctor",
            "physician",
            "healthcare",
            "hcp",
            "clinic",
        ]
        if any(p in answer_lower for p in hcp_patterns):
            return "Healthcare Professional"

        return answer

    def _normalize_respondent_type(self, answer: str) -> str:
        """Normalize respondent_type to standardized categories."""
        answer_lower = answer.lower().strip()

        # Disease names that indicate patient type
        disease_indicators = [
            "sjogren",
            "sjögren",
            "parkinson",
            "ehlers",
            "eds",
            "pulmonary fibrosis",
            "scleroderma",
            "copd",
            "sarcoidosis",
            "charcot",
            "cmt",
            "vasculitis",
            "als",
            "amyotrophic",
            "hypertension",
            "lupus",
            "alzheimer",
            "cold agglutinin",
            "muscular dystrophy",
            "angioedema",
            "cushing",
            "myeloma",
            "ankylosing",
            "sickle cell",
            "cystic fibrosis",
            "hypoparathyroidism",
            "porphyria",
            "huntington",
            "pompe",
            "fabry",
            "dravet",
            "hemolytic uremic",
            "hemophilia",
            "spinal muscular",
            "friedreich",
            "lambert-eaton",
            "inflammatory bowel",
            "neuromyelitis",
            "amyloid",
            "epidermolysis",
            "batten",
            "rett",
            "hepatitis",
            "fatty liver",
            "liver disease",
        ]

        # Check for caregiver patterns
        caregiver_patterns = [
            "caregiver",
            "family member",
            "loved one",
            "spouse",
            "parent",
            "provide care",
            "helps with care",
            "primary care",
        ]
        is_caregiver = any(p in answer_lower for p in caregiver_patterns)

        # Check for patient patterns
        patient_patterns = [
            "i am a patient",
            "i have",
            "person with",
            "patient with",
            "i am a person with",
            "diagnosed with",
            "i was diagnosed",
        ]
        is_patient = any(p in answer_lower for p in patient_patterns)

        # Check for HCP patterns
        hcp_patterns = [
            "healthcare professional",
            "healthcare provider",
            "hcp",
            "physician",
            "nurse",
            "doctor",
            "clinician",
        ]
        is_hcp = any(p in answer_lower for p in hcp_patterns)

        # Determine standardized value
        if is_hcp:
            return "Healthcare Professional"
        elif is_caregiver and is_patient:
            return "Patient and Caregiver"
        elif is_caregiver:
            return "Caregiver/Family Member"
        elif is_patient or any(d in answer_lower for d in disease_indicators):
            return "Person with Condition"
        elif "neither" in answer_lower:
            return "Neither"
        elif "other" in answer_lower:
            return "Other"

        # For answers that are just disease names without context
        for disease in disease_indicators:
            if disease in answer_lower:
                return "Person with Condition"

        return answer  # Return original if no pattern matched

    def standardize_answer(self, raw_answer: str, question: str = "") -> str:
        """
        Standardize answer using comprehensive normalization rules.

        Args:
            raw_answer: The raw answer text
            question: Optional question name for context-aware normalization

        Returns:
            Standardized answer string
        """
        if not raw_answer:
            return raw_answer

        # Ensure question is not None for downstream .lower() calls
        question = question or ""

        # Basic cleanup
        cleaned = self._cleanup_text(raw_answer)
        original = cleaned  # Track if any normalization changed the value
        question_lower = question.lower()

        # EARLY EXIT: Skip normalization for free-text/PII questions
        # These have high cardinality and shouldn't be standardized
        if question_lower in self.FREE_TEXT_QUESTIONS:
            # Only do basic cleanup, no semantic normalization
            return cleaned

        # EARLY FIX: Replace placeholder values like {site}, {condition}
        cleaned = self._replace_placeholders(cleaned)
        if cleaned != original:
            original = cleaned  # Update original for subsequent comparisons

        # SPECIAL CASE: Respondent type normalization (consolidate disease variants)
        if "respondent_type" in question_lower:
            result = self._normalize_respondent_type(cleaned)
            if result != cleaned:
                return result

        # SPECIAL CASE: Condition relationship normalization
        if "condition_relationship" in question_lower:
            result = self._normalize_condition_relationship(cleaned)
            if result != cleaned:
                return result

        # EARLY HANDLING: Pipe-separated values for specific question types
        # These columns often have multi-select values like "Very_Satisfied|Neutral"
        if "|" in cleaned:
            if (
                "treatment_satisfaction" in question_lower
                or "satisfaction" in question_lower
            ):
                return self._normalize_pipe_separated(
                    cleaned, self._normalize_satisfaction_scale
                )
            if (
                "treatment_change_intent" in question_lower
                or "change_intent" in question_lower
            ):
                return self._normalize_pipe_separated(
                    cleaned, self._normalize_likelihood_scale
                )
            if "communication" in question_lower and "preference" in question_lower:
                return self._normalize_pipe_separated(
                    cleaned, self._normalize_communication_preferences
                )

        # Apply normalization chain (order matters - specific before general)

        # 1. Does Not Apply variants (check early, very specific)
        cleaned = self._normalize_does_not_apply(cleaned)
        if cleaned != original:
            return cleaned

        # 2. Yes/No normalization (catch all variants)
        if cleaned.lower().strip() in [
            "yes",
            "yes.",
            "y",
            "true",
            "no",
            "no.",
            "n",
            "false",
        ]:
            return self._normalize_yes_no(cleaned)

        # 2b. Likert scale numeric normalization (1-5 values)
        if cleaned.strip() in ["1", "2", "3", "4", "5"]:
            likert_questions = [
                "fatigue",
                "impact",
                "milestone",
                "qol",
                "helpful",
                "feedback",
                "webinar",
                "support",
                "community",
                "rating",
                "level",
                "onset_order",
            ]
            if any(kw in question_lower for kw in likert_questions):
                result = self._normalize_likert_scale(cleaned, question)
                if result != cleaned:
                    return result

        # 3. Consent normalization
        if "consent" in question_lower or cleaned.lower() in [
            "agree",
            "disagree",
            "i agree",
            "i agree.",
            "i do not agree",
            "i do not agree.",
        ]:
            result = self._normalize_consent(cleaned)
            if result != cleaned:
                return result

        # 4. Agreement/Likert scale
        result = self._normalize_agreement_scale(cleaned, question)
        if result != cleaned:
            return result

        # 5. Satisfaction scale
        result = self._normalize_satisfaction_scale(cleaned)
        if result != cleaned:
            return result

        # 6. Likelihood scale
        result = self._normalize_likelihood_scale(cleaned)
        if result != cleaned:
            return result

        # 7. Knowledge scale
        result = self._normalize_knowledge_scale(cleaned)
        if result != cleaned:
            return result

        # 7b. Quality scale (Excellent/Good/Fair/Poor)
        result = self._normalize_quality_scale(cleaned)
        if result != cleaned:
            return result

        # 7c. Severity scale (None/Mild/Moderate/Severe/Extreme)
        result = self._normalize_severity_scale(cleaned)
        if result != cleaned:
            return result

        # 7d. Interest scale (Interested/Very Interested/Not Interested)
        result = self._normalize_interest_scale(cleaned)
        if result != cleaned:
            return result

        # 8. Age at diagnosis
        if "age_at_diagnosis" in question.lower() or cleaned.lower().startswith("age_"):
            result = self._normalize_age_at_diagnosis(cleaned)
            if result != cleaned:
                return result

        # 9. General age range - check question name OR age-like answer patterns
        # This catches hyphen formats like "55-64" before UMLS can corrupt them
        # Use word boundary to avoid matching "age" inside words like "evaluation", "percentage"
        is_age_question = bool(re.search(r"\bage\b", question.lower()))
        is_age_pattern = bool(
            re.match(r"^\d{1,2}-\d{2}$", cleaned)
        ) or cleaned.lower() in ["65+", "55+", "18+", "under 18", "under 5"]
        if is_age_question or is_age_pattern:
            result = self._normalize_age_range(cleaned)
            if result != cleaned:
                return result

        # 10. Gender
        if "gender" in question.lower():
            result = self._normalize_gender(cleaned)
            if result != cleaned:
                return result

        # 11. Time since diagnosis
        if "time_since" in question.lower() or "diagnosis" in question.lower():
            result = self._normalize_time_since_diagnosis(cleaned)
            if result != cleaned:
                return result

        # 12. Caregiver support
        result = self._normalize_caregiver_support(cleaned, question)
        if result != cleaned:
            return result

        # 13. Other specify
        result = self._normalize_other_specify(cleaned)
        if result != cleaned:
            return result

        # 14. Source tracking
        if "source" in question.lower():
            result = self._normalize_source(cleaned)
            if result != cleaned:
                return result

        # 15. Ethnicity
        if "ethnicity" in question.lower() or "race" in question.lower():
            result = self._normalize_ethnicity(cleaned)
            if result != cleaned:
                return result

        # 16. Communication preferences
        if "communication" in question.lower() or "preference" in question.lower():
            result = self._normalize_communication_preferences(cleaned)
            if result != cleaned:
                return result

        # 17. Site name (disease names to Title Case)
        if "site_name" in question.lower():
            result = self._normalize_site_name(cleaned)
            if result != cleaned:
                return result

        # 18. Condition values
        if "condition" in question.lower():
            result = self._normalize_condition(cleaned)
            if result != cleaned:
                return result
            # Also try site_name normalization for condition
            result = self._normalize_site_name(cleaned)
            if result != cleaned:
                return result

        # 18b. Treatment/medication normalization
        if "treatment" in question.lower() or "medication" in question.lower():
            result = self._normalize_treatment(cleaned)
            if result != cleaned:
                return result

        # 19. Info sources (clinic names, etc.)
        if "info_sources" in question.lower() or "source" in question.lower():
            result = self._normalize_info_sources(cleaned)
            if result != cleaned:
                return result

        # 19b. Frequency normalization (visit frequency, etc.)
        if "visit" in question.lower() or "frequency" in question.lower():
            result = self._normalize_frequency(cleaned)
            if result != cleaned:
                return result

        # 19c. Count range normalization (misdiagnoses, approved therapies, FEV1)
        if any(
            kw in question.lower()
            for kw in ["misdiagnosis", "approved", "fev1", "therapy"]
        ):
            result = self._normalize_count_range(cleaned)
            if result != cleaned:
                return result

        # 20. PROTECT SHORT VALUES FROM UMLS CORRUPTION
        # Short numeric values (1-2 digits) and very short strings should NOT go to UMLS
        # UMLS incorrectly matches "1" to "1,2-dipalmitoylphosphatidylcholine" etc.
        if len(cleaned) <= 3 or re.match(r"^\d{1,3}$", cleaned):
            return cleaned

        # 20b. PROTECT PIPE-SEPARATED RANKING/ORDERING VALUES FROM UMLS CORRUPTION
        # Values like "1|2|3" or "1 least impactful|2|3|5 most impactful" are multi-select rankings
        # UMLS incorrectly matches individual numbers to chemical compounds
        # Protect any value containing pipe-separated segments where most are simple numbers
        if "|" in cleaned:
            parts = cleaned.split("|")
            numeric_count = sum(1 for p in parts if re.match(r"^\d+(\s|$)", p.strip()))
            # If more than half the parts start with numbers, it's likely a ranking - don't UMLS normalize
            if numeric_count >= len(parts) // 2:
                return cleaned

        # 20c. PROTECT ANSWER CODES (AO01, AO02, etc.) FROM UMLS CORRUPTION
        # These are LimeSurvey answer codes that should have been decoded upstream
        # If they appear here, preserve them as-is (indicates a decoding issue to fix)
        if re.match(r"^AO\d{2}$", cleaned):
            return cleaned

        # 20d. PROTECT AGE RANGES FROM UMLS CORRUPTION
        # Patterns like "55-64", "25-34", "18-24" are age ranges that UMLS incorrectly
        # matches to medical scoring systems (APCH, etc.)
        if re.match(r"^\d{1,2}-\d{2}$", cleaned):
            # Convert to "X to Y" format before returning
            parts = cleaned.split("-")
            if len(parts) == 2:
                return f"{parts[0]} to {parts[1]}"
            return cleaned

        # 21. UMLS normalization for medical terms (ONLY if no other normalization matched)
        # This is the fallback for actual medical terms
        cleaned = self._normalize_with_umls(cleaned)

        # 22. Final fallback: Fuzzy matching against known standard answers
        # This catches typos like "Mal" → "Male", "Agre" → "Agree"
        fuzzy_result = self._fuzzy_match_answer(cleaned)
        if fuzzy_result and fuzzy_result != cleaned:
            return fuzzy_result

        return cleaned

    def _fuzzy_match_answer(self, answer: str) -> Optional[str]:
        """
        Attempt fuzzy matching against known standard answer values.
        Returns matched answer if confidence is high enough, else None.
        """
        if not answer or len(answer) < 3:
            return None

        # Build list of all known standardized answers from our maps
        known_answers = self._get_all_known_answers()

        # Use answer matcher for fuzzy matching
        result = self.answer_matcher.match(answer, known_answers)

        if result and result.confidence >= 0.88:  # Higher threshold for safety
            if result.method != "exact_match":  # Only log non-exact matches
                logger.debug(
                    f"Fuzzy answer match: '{answer}' -> '{result.standardized_answer}' "
                    f"({result.confidence:.0%}, {result.method})"
                )
            return result.standardized_answer

        return None

    def _get_all_known_answers(self) -> List[str]:
        """Collect all known standardized answer values from normalization maps."""
        known = set()

        # Agreement scale
        known.update(
            [
                "Strongly Agree",
                "Agree",
                "Neutral",
                "Disagree",
                "Strongly Disagree",
                "Somewhat Agree",
                "Somewhat Disagree",
                "Neither Agree Nor Disagree",
                "Completely Agree",
                "Completely Disagree",
                "Mostly Agree",
                "Mostly Disagree",
                "Slightly Agree",
                "Slightly Disagree",
            ]
        )

        # Satisfaction scale
        known.update(
            [
                "Very Satisfied",
                "Satisfied",
                "Dissatisfied",
                "Very Dissatisfied",
                "Somewhat Satisfied",
                "Somewhat Dissatisfied",
                "Neither Satisfied Nor Dissatisfied",
            ]
        )

        # Likelihood scale
        known.update(
            [
                "Very Likely",
                "Likely",
                "Unlikely",
                "Very Unlikely",
                "Somewhat Likely",
                "Somewhat Unlikely",
                "Not At All Likely",
                "Extremely Likely",
                "Not Sure",
                "Not Considering",
                "Actively Changing",
            ]
        )

        # Knowledge scale
        known.update(
            [
                "Very Knowledgeable",
                "Knowledgeable",
                "Somewhat Knowledgeable",
                "Not Knowledgeable",
                "Not Knowledgeable At All",
                "Extremely Knowledgeable",
            ]
        )

        # Quality scale
        known.update(
            [
                "Excellent",
                "Very Good",
                "Good",
                "Fair",
                "Poor",
                "Very Poor",
                "Average",
                "Above Average",
                "Below Average",
            ]
        )

        # Severity scale
        known.update(
            [
                "None",
                "No Symptoms",
                "Mild",
                "Moderate",
                "Severe",
                "Extreme",
                "Very Severe",
                "Minimal",
                "Significant",
            ]
        )

        # Yes/No
        known.update(["Yes", "No"])

        # Gender
        known.update(["Male", "Female", "Non-Binary", "Other", "Prefer Not To Answer"])

        # Common values
        known.update(["Does Not Apply", "Not Applicable", "Prefer Not To Say"])

        return list(known)

    def train_ml_model(self):
        """Train ML model on existing data and save to GCS.

        Training input is `build_ml_input(parent_text, subquestion_text)`. For
        non-matrix rows that's just the cleaned parent text. For matrix rows
        we join lime_questions / lime_question_l10ns to fetch the subquestion
        text and feed `parent :: subquestion`. Without that join, the model
        was being trained on identical parent text mapped to many different
        canonicals (one per subquestion) and the resulting confidence was
        effectively zero on every matrix row (audit_run_id 1381518...).
        """
        import pandas as pd

        logger.info("=" * 60)
        logger.info("TRAINING ML MODEL")
        logger.info("=" * 60)

        self._ensure_ml_tables_exist()

        # Get training data. Matrix rows get their subquestion text joined in
        # so the model has the disambiguating signal the pattern rules already
        # use. Non-matrix rows (subquestion_id IS NULL OR '') keep just the
        # parent text.
        query = f"""
        SELECT DISTINCT
            cc.raw_question_en,
            cc.standardized_question,
            COALESCE(l10n.question, q.question) AS subquestion_text
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}` cc
        LEFT JOIN `{PROJECT_ID}.{DATASET}.lime_questions` q
          ON q.sid = cc.survey_id
         AND q.parent_qid = cc.question_id
         AND q.title = CONCAT('SQ', cc.subquestion_id)
         AND cc.subquestion_id IS NOT NULL
         AND cc.subquestion_id != ''
        LEFT JOIN `{PROJECT_ID}.{DATASET}.lime_question_l10ns` l10n
          ON l10n.qid = q.qid
        WHERE cc.standardized_question IS NOT NULL
          AND cc.raw_question_en IS NOT NULL
          AND TRIM(cc.raw_question_en) != ''
        """

        df = self.bq_client.query(query).to_dataframe()
        logger.info(f"Loaded {len(df)} training examples")
        n_with_subq = int(df["subquestion_text"].notna().sum())
        logger.info(
            f"  {n_with_subq} examples carry a joined subquestion text "
            f"({n_with_subq * 100 // max(1, len(df))}%)"
        )

        if len(df) < 10:
            logger.warning("Not enough training data (need at least 10 examples)")
            return

        # Build the combined parent :: subquestion text for matrix rows and
        # just the cleaned parent text otherwise. Cleanup is applied uniformly.
        questions = [
            self._cleanup_text(self.build_ml_input(parent, subq))
            for parent, subq in zip(df["raw_question_en"], df["subquestion_text"])
        ]
        labels = df["standardized_question"].tolist()

        # Filter empty
        valid = [(q, l) for q, l in zip(questions, labels) if q]
        questions = [q for q, _ in valid]
        labels = [l for _, l in valid]

        # Generate embeddings
        logger.info("Generating embeddings...")
        embeddings = self.transformer_model.encode(questions, show_progress_bar=True)

        # Encode labels
        label_encoder = LabelEncoder()
        encoded_labels = label_encoder.fit_transform(labels)

        logger.info(f"Unique classes: {len(label_encoder.classes_)}")

        # Train model - RandomForest is much faster for multiclass
        logger.info("Training RandomForestClassifier with proper validation...")

        # PROPER VALIDATION: Use 80/20 stratified holdout split
        # Check if we have enough samples per class for stratification
        class_counts = pd.Series(encoded_labels).value_counts()
        min_samples = class_counts.min()

        if min_samples >= 2 and len(embeddings) >= 20:
            # Use stratified split to maintain class distribution
            X_train, X_test, y_train, y_test = train_test_split(
                embeddings,
                encoded_labels,
                test_size=0.20,
                stratify=encoded_labels,
                random_state=42,
            )
            logger.info(f"  Training samples: {len(X_train)}")
            logger.info(f"  Test samples: {len(X_test)}")

            # Train on training set only
            model = RandomForestClassifier(
                n_estimators=100, max_depth=10, n_jobs=-1, random_state=42
            )
            model.fit(X_train, y_train)

            # Evaluate on HELD-OUT test set (TRUE accuracy)
            test_predictions = model.predict(X_test)
            validation_accuracy = (test_predictions == y_test).mean()

            # Also compute training accuracy for reference
            train_predictions = model.predict(X_train)
            train_accuracy = (train_predictions == y_train).mean()

            logger.info(f"Training accuracy (reference): {train_accuracy:.2%}")
            logger.info(f"Validation accuracy (TRUE): {validation_accuracy:.2%}")

            # Alert if overfitting
            gap = train_accuracy - validation_accuracy
            if gap > 0.10:
                logger.warning(
                    f"OVERFITTING DETECTED: {gap:.1%} gap between train and validation"
                )

            # Retrain on FULL data for production use
            logger.info("Retraining on full dataset for production...")
            model = RandomForestClassifier(
                n_estimators=100, max_depth=10, n_jobs=-1, random_state=42
            )
            model.fit(embeddings, encoded_labels)

            accuracy = validation_accuracy  # Report validation accuracy, not training!
        else:
            # Not enough data for proper split - fall back to simple training
            logger.warning(
                f"Insufficient data for validation split (min samples per class: {min_samples})"
            )
            model = RandomForestClassifier(
                n_estimators=100, max_depth=10, n_jobs=-1, random_state=42
            )
            model.fit(embeddings, encoded_labels)

            # Fall back to training accuracy with warning
            predictions = model.predict(embeddings)
            accuracy = (predictions == encoded_labels).mean()
            logger.warning(f"Training accuracy (may be overfitted): {accuracy:.2%}")

        # Save locally + metadata to BigQuery
        if not self.dry_run:
            # Ensure model directory exists
            MODEL_DIR.mkdir(parents=True, exist_ok=True)

            # Save model and encoder to local files
            with open(MODEL_FILE, "wb") as f:
                pickle.dump(model, f)
            logger.info(f"[OK] Model saved: {MODEL_FILE}")

            with open(ENCODER_FILE, "wb") as f:
                pickle.dump(label_encoder, f)
            logger.info(f"[OK] Encoder saved: {ENCODER_FILE}")

            # Track metadata in BigQuery
            model_id = f"model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            self._ensure_ml_tables_exist()
            self.bq_client.query(
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
                    bigquery.ScalarQueryParameter(
                        "model_path", "STRING", str(MODEL_DIR)
                    ),
                    bigquery.ScalarQueryParameter(
                        "accuracy", "FLOAT64", float(accuracy)
                    ),
                    bigquery.ScalarQueryParameter(
                        "trained_at", "TIMESTAMP", datetime.utcnow()
                    ),
                    bigquery.ScalarQueryParameter("training_rows", "INT64", len(df)),
                ]
            )

            self.bq_client.query(insert_query, job_config=job_config).result()
            logger.info(f"[OK] Metadata saved to BigQuery: {model_id}")

            # Update cached model
            self._ml_model = model
            self._ml_label_encoder = label_encoder
        else:
            logger.info("[DRY RUN] Model not saved")

    def should_retrain(self, growth_threshold: float = 0.10) -> bool:
        """Check if enough new data exists to warrant retraining

        Args:
            growth_threshold: Minimum data growth ratio to trigger retraining (default 10%)

        Returns:
            True if retraining is recommended
        """
        try:
            # Get last training row count from model metadata
            meta_query = f"""
            SELECT training_rows, trained_at
            FROM `{PROJECT_ID}.{DATASET}.{ML_MODEL_TABLE}`
            ORDER BY trained_at DESC
            LIMIT 1
            """
            meta_result = self.bq_client.query(meta_query).to_dataframe()

            if meta_result.empty:
                logger.info("No existing model found - retraining recommended")
                return True

            last_trained_rows = meta_result["training_rows"].iloc[0]
            last_trained_at = meta_result["trained_at"].iloc[0]

            # Get current data count
            count_query = f"""
            SELECT COUNT(DISTINCT raw_question_en) as current_count
            FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
            WHERE standardized_question IS NOT NULL
              AND raw_question_en IS NOT NULL
            """
            count_result = self.bq_client.query(count_query).to_dataframe()
            current_count = count_result["current_count"].iloc[0]

            if last_trained_rows == 0:
                logger.info("No previous training data - retraining recommended")
                return True

            growth = (current_count - last_trained_rows) / last_trained_rows

            logger.info(
                f"Model stats: trained on {last_trained_rows} rows at {last_trained_at}"
            )
            logger.info(f"Current data: {current_count} rows ({growth:+.1%} growth)")

            if growth >= growth_threshold:
                logger.info(
                    f"Growth exceeds {growth_threshold:.0%} threshold - retraining recommended"
                )
                return True
            else:
                logger.info(
                    f"Growth below {growth_threshold:.0%} threshold - no retraining needed"
                )
                return False

        except Exception as e:
            logger.warning(f"Could not check retraining status: {e}")
            return False

    def get_rows_needing_processing(self) -> List[Dict]:
        """Get rows that need standardization.

        Selects rows missing EITHER standardized_question OR standardized_answer:
          - standardized_question IS NULL  -> needs question (and answer) derived
          - standardized_question IS NOT NULL but standardized_answer IS NULL
            -> question already mapped; only the answer needs standardizing. The
               processing loop preserves the existing standardized_question for
               these (it does not re-derive it) so a prior correct mapping is
               never disturbed.

        Excludes manual rows and free-text questions whose answers are
        intentionally left unstandardized (so we don't churn them every run).
        """
        query = f"""
        SELECT
            survey_id,
            response_id,
            question_id,
            subquestion_id,
            raw_question_en,
            raw_answer_en,
            standardized_question,
            standardized_answer
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE raw_question_en IS NOT NULL
          AND TRIM(raw_question_en) != ''
          AND COALESCE(manually_assigned, FALSE) = FALSE
          AND (
              standardized_question IS NULL
              OR (
                  standardized_answer IS NULL
                  AND raw_answer_en IS NOT NULL
                  AND TRIM(raw_answer_en) != ''
                  AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
              )
          )
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "free_text_qs", "STRING", sorted(self.FREE_TEXT_QUESTIONS)
                )
            ]
        )
        logger.info("Finding rows needing processing...")
        df = self.bq_client.query(query, job_config=job_config).to_dataframe()
        logger.info(f"Found {len(df)} rows needing processing")

        return df.to_dict("records")

    def update_rows(self, rows: List[Dict]):
        """Update rows with standardized values using batch MERGE"""
        if not rows:
            logger.info("No rows to update")
            return

        # Filter to only rows that have standardized values
        rows_to_update = [
            r
            for r in rows
            if r.get("standardized_question") or r.get("standardized_answer")
        ]

        if not rows_to_update:
            logger.info("No rows with standardized values to update")
            return

        logger.info(f"Updating {len(rows_to_update)} rows (batch MERGE)...")

        if self.dry_run:
            logger.info("[DRY RUN] Skipping updates")
            return

        import pandas as pd

        # Create a temp table with the updates
        # Note: response_id is STRING in target table, subquestion_id is STRING
        update_df = pd.DataFrame(
            [
                {
                    "survey_id": int(r["survey_id"]),
                    "response_id": str(r["response_id"]),  # STRING in target
                    "question_id": int(r["question_id"]),
                    "subquestion_id": str(
                        r.get("subquestion_id") or ""
                    ),  # STRING in target
                    "standardized_question": r.get("standardized_question"),
                    "standardized_answer": r.get("standardized_answer"),
                    "question_match_confidence": r.get("question_match_confidence"),
                    "question_match_method": r.get("question_match_method"),
                    "question_type": r.get("question_type"),
                }
                for r in rows_to_update
            ]
        )

        # Ensure correct types for nullable columns to prevent pandas INT64 inference
        # when all values are None (BigQuery target columns are STRING/FLOAT64/STRING)
        update_df["standardized_question"] = update_df["standardized_question"].astype(
            "object"
        )
        update_df["standardized_answer"] = update_df["standardized_answer"].astype(
            "object"
        )
        update_df["question_match_confidence"] = update_df[
            "question_match_confidence"
        ].astype("Float64")
        update_df["question_match_method"] = update_df["question_match_method"].astype(
            "object"
        )
        update_df["question_type"] = update_df["question_type"].astype("object")

        # Deduplicate - MERGE requires unique keys in source
        # Keep last occurrence (most recent processing result)
        key_cols = ["survey_id", "response_id", "question_id", "subquestion_id"]
        before_dedup = len(update_df)
        update_df = update_df.drop_duplicates(subset=key_cols, keep="last")
        if len(update_df) < before_dedup:
            logger.info(f"  Deduplicated: {before_dedup} -> {len(update_df)} rows")

        # Upload to temp table with explicit schema so NULL-only columns
        # are typed correctly (prevents INT64 inference for STRING columns)
        temp_table = f"{PROJECT_ID}.{DATASET}._temp_nlp_updates"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("survey_id", "INT64"),
                bigquery.SchemaField("response_id", "STRING"),
                bigquery.SchemaField("question_id", "INT64"),
                bigquery.SchemaField("subquestion_id", "STRING"),
                bigquery.SchemaField("standardized_question", "STRING"),
                bigquery.SchemaField("standardized_answer", "STRING"),
                bigquery.SchemaField("question_match_confidence", "FLOAT64"),
                bigquery.SchemaField("question_match_method", "STRING"),
                bigquery.SchemaField("question_type", "STRING"),
            ],
        )

        job = self.bq_client.load_table_from_dataframe(
            update_df, temp_table, job_config=job_config
        )
        job.result()

        # Run MERGE to update in batch
        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{TABLE}` T
        USING `{temp_table}` S
        ON T.survey_id = S.survey_id
           AND T.response_id = S.response_id
           AND T.question_id = S.question_id
           AND COALESCE(T.subquestion_id, '') = S.subquestion_id
        WHEN MATCHED AND COALESCE(T.manually_assigned, FALSE) = FALSE THEN UPDATE SET
            T.standardized_question = S.standardized_question,
            T.standardized_answer = S.standardized_answer,
            T.question_match_confidence = CAST(S.question_match_confidence AS FLOAT64),
            T.question_match_method = S.question_match_method,
            T.question_type = S.question_type
        """

        result = self.bq_client.query(merge_query).result()

        # Clean up temp table
        self.bq_client.query(f"DROP TABLE IF EXISTS `{temp_table}`").result()

        logger.info(f"[OK] Updated {len(rows_to_update)} rows via MERGE")

    def run(self):
        """Main processing workflow"""
        logger.info("\n" + "=" * 60)
        logger.info("LIMESURVEY NLP PROCESSOR")
        logger.info("=" * 60)
        logger.info("Source of truth: lime_surveys_columnar_completed")
        logger.info(
            "Matching layers: Exact -> ML -> Semantic -> UMLS -> spaCy -> Fuzzy"
        )
        logger.info("=" * 60 + "\n")

        if self.dry_run:
            logger.info("DRY RUN MODE - No changes will be written\n")

        # Get rows needing processing
        rows = self.get_rows_needing_processing()

        if not rows:
            logger.info("No rows to process - everything is up to date")
            return

        # Get known mappings once
        mappings, questions, embeddings = self.get_known_mappings()

        # Load subquestion map for subquestion-based pattern rules
        subq_map = self._load_subquestion_map()

        # Process rows with progress logging
        matched_count = 0
        unmatched_count = 0
        subq_pattern_count = 0
        total = len(rows)

        logger.info(f"Processing {total} rows...")

        for i, row in enumerate(rows):
            # Progress every 500 rows
            if (i + 1) % 500 == 0:
                logger.info(
                    f"  Progress: {i+1}/{total} ({100*(i+1)/total:.0f}%) - {matched_count} matched ({subq_pattern_count} via subq_pattern)"
                )

            # Preserve an existing standardized_question. Rows can arrive here
            # needing ONLY the answer standardized (question already mapped, e.g.
            # answer-only-NULL recovery). Re-deriving the question for those would
            # risk changing a correct prior mapping, so skip all question-matching
            # when standardized_question is already set and only standardize the
            # answer below.
            already_has_question = bool(row.get("standardized_question"))

            # Layer 0: Subquestion Pattern Rules (highest priority)
            # First check 3-tuple rules (parent + subquestion), then 2-tuple rules (subquestion only)
            subq_matched = False
            subq_id = row.get("subquestion_id", "")
            # Pull these once so the later cascade-caller can also use them
            # to look up subquestion text for ML inference on matrix rows.
            survey_id = row.get("survey_id")
            question_id = row.get("question_id")
            if not already_has_question and subq_id:
                key = (survey_id, question_id, subq_id)

                if key in subq_map:
                    subq_text = subq_map[key].lower().strip()
                    parent_text = (row.get("raw_question_en") or "").lower().strip()

                    # First: Check 3-tuple rules (parent_pattern, subq_pattern, std_question)
                    # These disambiguate cases like symptom_qol_impact vs symptom_onset_order
                    for (
                        parent_pattern,
                        subq_pattern,
                        std_question,
                    ) in get_parent_rules():
                        if parent_pattern in parent_text and subq_pattern in subq_text:
                            row["standardized_question"] = std_question
                            row["question_match_confidence"] = 1.0
                            row["question_match_method"] = "subquestion_parent_pattern"
                            matched_count += 1
                            subq_pattern_count += 1
                            subq_matched = True
                            break

                    # Second: Check 2-tuple rules (subq_pattern, std_question)
                    if not subq_matched:
                        for pattern, std_question in get_subquestion_rules():
                            if pattern in subq_text:
                                row["standardized_question"] = std_question
                                row["question_match_confidence"] = 1.0
                                row["question_match_method"] = "subquestion_pattern"
                                matched_count += 1
                                subq_pattern_count += 1
                                subq_matched = True
                                break

            # If not matched by subquestion pattern, use normal matching.
            # Skip entirely when the question is already mapped (answer-only case).
            if not already_has_question and not subq_matched:
                # Match question using exact match first (fastest).
                # For matrix rows we look up subquestion text and feed
                # `parent :: subquestion` to the ML/semantic layers -- same
                # input the model was trained on. The exact_match lookup
                # still uses parent text alone because mappings are keyed
                # on parent text.
                if row.get("raw_question_en"):
                    cleaned = self._cleanup_text(row["raw_question_en"])
                    if cleaned:
                        normalized = cleaned.lower().strip()

                        # Layer 1: Exact match (instant) -- parent text only
                        if normalized in mappings:
                            row["standardized_question"] = mappings[normalized]
                            row["question_match_confidence"] = 1.0
                            row["question_match_method"] = "exact_match"
                            matched_count += 1
                        else:
                            # Layer 2+: ML / semantic / umls / spacy / fuzzy.
                            # Build the subquestion-aware combined input so
                            # the ML model sees the same text it was trained
                            # on. Non-matrix rows fall back to parent only.
                            subq_text_for_ml = None
                            if subq_id and survey_id and question_id:
                                key = (survey_id, question_id, subq_id)
                                subq_text_for_ml = subq_map.get(key)
                            ml_input = self.build_ml_input(
                                row["raw_question_en"], subq_text_for_ml
                            )
                            result = self.match_question(ml_input)
                            if result:
                                row["standardized_question"] = (
                                    result.standardized_question
                                )
                                row["question_match_confidence"] = result.confidence
                                row["question_match_method"] = result.method
                                matched_count += 1
                            else:
                                row["standardized_question"] = None
                                row["question_match_confidence"] = None
                                row["question_match_method"] = None
                                unmatched_count += 1
                    else:
                        row["standardized_question"] = None
                        row["question_match_confidence"] = None
                        row["question_match_method"] = None
                        unmatched_count += 1
                else:
                    row["standardized_question"] = None
                    row["question_match_confidence"] = None
                    row["question_match_method"] = None

            # Standardize answer
            raw_answer = row.get("raw_answer_en", "")
            subq_id = row.get("subquestion_id", "")
            std_question = row.get("standardized_question", "")

            if raw_answer and raw_answer.upper() in ("Y", "YES") and subq_id:
                # Multi-select checkbox: look up actual subquestion text
                subq_map = self._load_subquestion_map()
                survey_id = row.get("survey_id")
                question_id = row.get("question_id")
                key = (survey_id, question_id, subq_id)

                if key in subq_map:
                    # Use subquestion text (cleaned) instead of 'Yes'
                    row["standardized_answer"] = self.standardize_answer(
                        self._clean_answer_text(subq_map[key]), std_question
                    )
                else:
                    # Fallback to standardized raw answer
                    row["standardized_answer"] = self.standardize_answer(
                        raw_answer, std_question
                    )
            elif raw_answer:
                row["standardized_answer"] = self.standardize_answer(
                    raw_answer, std_question
                )
            else:
                row["standardized_answer"] = None

            # Determine question_type based on characteristics
            answer_len = len(row.get("standardized_answer") or "")
            row["question_type"] = determine_question_type(
                std_question, subq_id, answer_len
            )

        logger.info(
            f"\nMatching complete: {matched_count} matched ({subq_pattern_count} via subquestion pattern), {unmatched_count} unmatched"
        )

        # Update database
        self.update_rows(rows)

        logger.info("\n" + "=" * 60)
        logger.info("PROCESSING COMPLETE")
        logger.info("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Process LimeSurvey data with 6-layer NLP matching"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to BigQuery",
    )
    parser.add_argument(
        "--retrain-model",
        action="store_true",
        help="Retrain ML model from current data",
    )
    parser.add_argument(
        "--retrain-if-needed",
        action="store_true",
        help="Retrain ML model only if 10%% or more new data exists, then run processing",
    )
    parser.add_argument(
        "--retrain-only",
        action="store_true",
        help="Retrain ML model only if 10%% or more new data exists and exit. "
        "No row processing. Use after NLP has already run in a pipeline so "
        "retrain does not duplicate processing or race the same tables.",
    )
    parser.add_argument(
        "--refresh-mappings-only",
        action="store_true",
        help="Reload known mappings from columnar_completed and exit. Smoke test after Apply Changes; verifies the new mappings landed. No row processing, no retrain. Next ETL run loads fresh anyway.",
    )

    args = parser.parse_args()

    processor = LimeSurveyProcessor(dry_run=args.dry_run)

    if args.refresh_mappings_only:
        # Force a fresh load of known mappings into the cache.
        # Used after the Streamlit Apply Changes step so the next ETL run sees corrected pairs immediately.
        logger.info("Refreshing known mappings cache (no row processing)...")
        processor._known_mappings = None  # invalidate cache
        processor._known_questions = None
        processor._known_embeddings = None
        mappings, questions, embeddings = processor.get_known_mappings()
        logger.info(
            f"Reloaded {len(mappings)} mappings ({len(questions)} unique questions)."
        )
        return

    if args.retrain_only:
        if processor.should_retrain():
            logger.info("Growth threshold met -- retraining ML model...")
            processor.train_ml_model()
        else:
            logger.info("Growth below threshold -- skipping retrain.")
        return

    if args.retrain_model:
        processor.train_ml_model()
    elif args.retrain_if_needed:
        if processor.should_retrain():
            processor.train_ml_model()

    processor.run()


if __name__ == "__main__":
    main()
