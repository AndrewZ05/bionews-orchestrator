#!/usr/bin/env python3
"""
Standardized Mapping Manager with Fuzzy Matching

A Streamlit app for managing LimeSurvey question and answer standardization mappings.
Uses lookup tables with proper snake_case standardized values.

Features:
1. View unmapped questions and answers from columnar_completed
2. **NEW: Fuzzy matching suggestions using RapidFuzz**
3. Assign standardized values from canonical lookup tables
4. Add new standardized values to lookup tables
5. Apply mappings to columnar_completed via UPDATE statements
6. Rename standardized values across all tables

Usage:
    streamlit run ui/limesurvey/limesurvey_mapper.py
"""

import os
import subprocess
import sys
import uuid
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from pathlib import Path
from datetime import datetime
from rapidfuzz import fuzz, process

# Load environment variables. File lives at repo root: ui/limesurvey/limesurvey_mapper.py
# → parent (limesurvey) → parent (ui) → parent (repo root).
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    from dotenv import load_dotenv

    load_dotenv(env_path)

# Configuration
PROJECT_ID = "bi-data-391216"
DATASET = "limesurvey_data"

# Table names
CATEGORIES_LOOKUP = f"{PROJECT_ID}.{DATASET}.categories_lookup"
QUESTIONS_LOOKUP = f"{PROJECT_ID}.{DATASET}.standardized_questions_lookup"
ANSWERS_LOOKUP = f"{PROJECT_ID}.{DATASET}.standardized_answers_lookup"
QUESTION_MAPPING = f"{PROJECT_ID}.{DATASET}.question_mapping"
ANSWER_MAPPING = f"{PROJECT_ID}.{DATASET}.answer_mapping"
COLUMNAR_COMPLETED = f"{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed"
MAPPING_HISTORY = f"{PROJECT_ID}.{DATASET}.mapping_history"
ANSWER_FINGERPRINTS = (
    f"{PROJECT_ID}.{DATASET}.standardized_question_answer_fingerprints"
)
USERS_TABLE = f"{PROJECT_ID}.{DATASET}.mapper_users"
USAGE_LOG_TABLE = f"{PROJECT_ID}.{DATASET}.mapper_usage_log"

# Domains for which Gmail-style dot-aliasing applies (robert.macinnis@ ==
# robertmacinnis@) and plus-tag stripping is honored (robert+foo@ ==
# robert@). bionews.com is on Google Workspace so Gmail rules apply.
EMAIL_DOT_DOMAINS = {"bionews.com", "gmail.com"}

# Seed admin for first-run bootstrap when mapper_users is empty. Normalized
# form (lowercase, no dots) so the email matches no matter which alias the
# IAP header carries.
BOOTSTRAP_ADMIN_EMAIL = "robertmacinnis@bionews.com"


def _normalize_email(email: str) -> str:
    """Canonicalize a Gmail-style email for matching the users table.

    Lowercases, strips plus-tag (foo+bar@d -> foo@d), and for Google
    Workspace domains strips dots from the local part (a.b@bionews.com ==
    ab@bionews.com). This means a user added once is reachable under
    every alias their Google account responds to.
    """
    if not email:
        return ""
    email = email.strip().lower()
    if "@" not in email:
        return email
    local, _, domain = email.partition("@")
    if "+" in local:
        local = local.split("+", 1)[0]
    if domain in EMAIL_DOT_DOMAINS:
        local = local.replace(".", "")
    return f"{local}@{domain}"


# Open-text questions whose answers are intentionally NOT standardized. Kept in
# sync with FREE_TEXT_QUESTIONS in shared/limesurvey_process_columnar.py. These
# are excluded from the Unmapped Answers queue -- their answers will always be
# NULL by design, so showing them as "needs mapping" is noise.
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
    # on 2026-06-04). Kept in sync with the same set in
    # shared/limesurvey_process_columnar.py so the mapper's "actionable
    # unmapped answer" filter and the processor's cascade agree.
    "gene_therapy_info_needs",
    "gene_therapy_concerns_barriers",
    "gene_therapy_decision_influencers",
    "gene_therapy_expectations",
    "gene_therapy_open_text",
    "gene_therapy_other_concerns",
    "gene_therapy_pretreatment_expectations",
    "gene_therapy_experience_vs_expectations",
}


def _get_caller_email() -> tuple[str, bool]:
    """Return (email, behind_iap). When the app is served behind Google IAP,
    every request carries X-Goog-Authenticated-User-Email of the form
    'accounts.google.com:user@domain'. Outside IAP (local dev), we fall back
    to a synthetic dev email derived from the OS username so the rest of the
    app can still attribute writes to a person.

    Requires Streamlit >= 1.36 for st.context.headers. On older Streamlit
    the read raises AttributeError; we fall through to the dev path. The
    deployment runbook (docs/LIMEDIT_DEPLOYMENT_RUNBOOK.md) calls this out
    so the VM is kept on a recent Streamlit -- if it isn't, every reviewer
    gets the dev fallback email and the allowlist effectively bypasses.
    """
    raw = ""
    try:
        raw = st.context.headers.get("X-Goog-Authenticated-User-Email", "")
    except Exception:
        raw = ""
    if raw:
        email = raw.split(":", 1)[1].lower() if ":" in raw else raw.lower()
        return email, True
    dev_user = os.environ.get("USER") or os.environ.get("USERNAME") or "local-dev"
    return f"{dev_user}@local-dev".lower(), False


@st.cache_data(ttl=60, show_spinner=False)
def _lookup_user(_client, normalized_email: str) -> dict | None:
    """Return the mapper_users row for this normalized email, or None if not
    present / not active. Cached 60s to avoid a BQ round-trip per rerun.
    Falls back to None on any query failure (treated as 'not authorized')."""
    if not normalized_email:
        return None
    try:
        rows = list(
            _client.query(
                f"""
                SELECT email, display_name, role, is_active
                FROM `{USERS_TABLE}`
                WHERE email = @email AND is_active = TRUE
                LIMIT 1
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "email", "STRING", normalized_email
                        ),
                    ]
                ),
            ).result()
        )
    except Exception:
        return None
    if not rows:
        return None
    r = rows[0]
    return {
        "email": r.email,
        "display_name": r.display_name,
        "role": r.role,
        "is_active": r.is_active,
    }


def _stamp_last_login(client, normalized_email: str) -> None:
    """Fire-and-forget UPDATE of last_login_at. Failures are silent; the
    login itself already succeeded."""
    try:
        client.query(
            f"""
            UPDATE `{USERS_TABLE}`
            SET last_login_at = CURRENT_TIMESTAMP()
            WHERE email = @email
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", normalized_email),
                ]
            ),
        ).result()
    except Exception:
        pass


def _log_usage_event(
    client, email: str, event_type: str, page: str | None, session_id: str
) -> None:
    """Append one row to mapper_usage_log. Fire-and-forget -- failure here
    must never block the actual page render."""
    try:
        client.query(
            f"""
            INSERT INTO `{USAGE_LOG_TABLE}`
              (event_time, email, event_type, page, session_id)
            VALUES (CURRENT_TIMESTAMP(), @email, @event_type, @page, @session_id)
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", email),
                    bigquery.ScalarQueryParameter("event_type", "STRING", event_type),
                    bigquery.ScalarQueryParameter("page", "STRING", page),
                    bigquery.ScalarQueryParameter("session_id", "STRING", session_id),
                ]
            ),
        ).result()
    except Exception:
        pass


def _log_session_start_once(client, email: str) -> None:
    """Emit a session_start row the first time this browser session sees an
    authorized user. Subsequent reruns within the same session are no-ops
    because the session_id is stashed in st.session_state."""
    if st.session_state.get("usage_session_logged"):
        return
    session_id = st.session_state.get("usage_session_id") or str(uuid.uuid4())
    st.session_state["usage_session_id"] = session_id
    _log_usage_event(client, email, "session_start", None, session_id)
    st.session_state["usage_session_logged"] = True


def _log_page_view_if_changed(client, email: str, page: str) -> None:
    """Emit a page_view row only when the current page differs from the
    last one logged in this session. Prevents flooding the log on every
    Streamlit rerun (which fires on any widget interaction)."""
    if not page:
        return
    last_page = st.session_state.get("usage_last_page")
    if last_page == page:
        return
    session_id = st.session_state.get("usage_session_id") or str(uuid.uuid4())
    st.session_state["usage_session_id"] = session_id
    _log_usage_event(client, email, "page_view", page, session_id)
    st.session_state["usage_last_page"] = page


def _require_authorized_user(client) -> dict:
    """Identify the caller via IAP, look them up in mapper_users, stash the
    user record in st.session_state, and return it. Unauthorized callers
    (no row OR is_active=FALSE) are hard-blocked with st.stop().

    Local dev (no IAP header) bypasses the BQ check and is treated as an
    admin so the app remains usable during development.
    """
    raw_email, behind_iap = _get_caller_email()
    normalized = _normalize_email(raw_email)

    if not behind_iap:
        user = {
            "email": normalized,
            "display_name": "Local Dev",
            "role": "admin",
            "is_active": True,
        }
        st.session_state["reviewer_email"] = normalized
        st.session_state["reviewer_user"] = user
        st.session_state["behind_iap"] = False
        _log_session_start_once(client, normalized)
        return user

    user = _lookup_user(client, normalized)
    if not user:
        st.error(
            f"Access denied for {raw_email} (normalized: {normalized}). "
            "You're signed in via IAP but not on the mapper users list, or "
            "your account is deactivated. Ask an admin to add you on the "
            "Users page."
        )
        st.stop()

    _stamp_last_login(client, normalized)
    st.session_state["reviewer_email"] = normalized
    st.session_state["reviewer_user"] = user
    st.session_state["behind_iap"] = True
    _log_session_start_once(client, normalized)
    return user


def _actionable_unmapped_answer_filter(table_alias: str = "") -> str:
    """SQL predicate shared by the Dashboard count and Unmapped Answers queue."""
    prefix = f"{table_alias}." if table_alias else ""
    return f"""
      {prefix}standardized_answer IS NULL
      AND {prefix}standardized_question IS NOT NULL
      AND {prefix}raw_answer_en IS NOT NULL
      AND TRIM({prefix}raw_answer_en) != ''
      AND NOT REGEXP_CONTAINS({prefix}raw_answer_en, r'^[0-9.]+$')
      AND REPLACE(REPLACE(REPLACE(LOWER(TRIM({prefix}raw_answer_en)), '/', ''), '.', ''), ' ', '') != 'na'
      AND LOWER({prefix}standardized_question) NOT IN UNNEST(@free_text_qs)
    """


@st.cache_resource
def get_bq_client():
    """Create BigQuery client (cached)."""
    client = bigquery.Client(project=PROJECT_ID)
    _ensure_mapping_tables(client)
    return client


def _ensure_mapping_tables(client):
    """Create the mapping_history audit table and the mapper_users table
    if either is missing. Idempotent. Also seeds the bootstrap admin row
    into mapper_users on first run so the first sign-in is not locked out.

    Note: question_mapping and answer_mapping are VIEWS over columnar_completed,
    not tables. They are created out-of-band (see archive_pre_view_cutover for
    the migration). Do not recreate them here.
    """
    client.query(
        f"""
    CREATE TABLE IF NOT EXISTS `{MAPPING_HISTORY}` (
        history_id STRING DEFAULT GENERATE_UUID(),
        event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        action STRING,
        raw_question STRING,
        raw_answer STRING,
        new_standardized_question STRING,
        new_standardized_answer STRING,
        affected_rows INT64,
        created_by STRING,
        undone BOOL DEFAULT FALSE,
        undone_at TIMESTAMP,
        batch_id STRING
    )
    """
    ).result()

    client.query(
        f"""
    CREATE TABLE IF NOT EXISTS `{USERS_TABLE}` (
        email STRING NOT NULL,
        display_name STRING,
        role STRING NOT NULL,
        is_active BOOL NOT NULL,
        created_at TIMESTAMP NOT NULL,
        created_by STRING,
        last_login_at TIMESTAMP,
        notes STRING
    )
    """
    ).result()

    # mapper_usage_log: lightweight per-session and per-page-view trail.
    # event_type currently emits 'session_start' (once per browser session)
    # and 'page_view' (each time the reviewer switches pages). Day-partitioned
    # so historical queries are cheap.
    client.query(
        f"""
    CREATE TABLE IF NOT EXISTS `{USAGE_LOG_TABLE}` (
        event_time TIMESTAMP NOT NULL,
        email STRING,
        event_type STRING NOT NULL,
        page STRING,
        session_id STRING
    )
    PARTITION BY DATE(event_time)
    """
    ).result()

    # Seed the bootstrap admin if the table is empty. Idempotent: subsequent
    # runs see a non-empty table and skip the insert. The MERGE form is the
    # BigQuery-correct way to express "insert this row only if no rows exist
    # yet" -- a SELECT ... WHERE NOT EXISTS without a FROM clause is rejected
    # ("Query without FROM clause cannot have a WHERE clause").
    client.query(
        f"""
        MERGE `{USERS_TABLE}` T
        USING (
          SELECT @email AS email, @display_name AS display_name
        ) S
        ON FALSE
        WHEN NOT MATCHED BY TARGET
          AND NOT EXISTS (SELECT 1 FROM `{USERS_TABLE}`)
        THEN INSERT
          (email, display_name, role, is_active, created_at, created_by, notes)
          VALUES (S.email, S.display_name, 'admin', TRUE,
                  CURRENT_TIMESTAMP(), 'bootstrap',
                  'Seeded on first run by _ensure_mapping_tables.')
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", BOOTSTRAP_ADMIN_EMAIL),
                bigquery.ScalarQueryParameter(
                    "display_name", "STRING", "Robert MacInnis"
                ),
            ]
        ),
    ).result()


# ============================================================================
# FUZZY MATCHING FUNCTIONS
# ============================================================================


def get_fuzzy_question_matches(
    raw_question: str, standardized_questions: list, top_n: int = 3, threshold: int = 60
):
    """
    Find fuzzy matches for a raw question against standardized questions.

    Args:
        raw_question: The raw question text to match
        standardized_questions: List of standardized question names
        top_n: Number of top matches to return
        threshold: Minimum score threshold (0-100)

    Returns:
        List of tuples: (standardized_question, score, index)
    """
    if not raw_question or not standardized_questions:
        return []

    # Use token_sort_ratio for better matching with word order variations
    matches = process.extract(
        raw_question, standardized_questions, scorer=fuzz.token_sort_ratio, limit=top_n
    )

    # Filter by threshold
    return [(match, score, idx) for match, score, idx in matches if score >= threshold]


def get_fuzzy_answer_matches(
    raw_answer: str, standardized_answers: list, top_n: int = 3, threshold: int = 70
):
    """
    Find fuzzy matches for a raw answer against standardized answers.

    Args:
        raw_answer: The raw answer text to match
        standardized_answers: List of standardized answer values
        top_n: Number of top matches to return
        threshold: Minimum score threshold (0-100)

    Returns:
        List of tuples: (standardized_answer, score, index)
    """
    if not raw_answer or not standardized_answers:
        return []

    # Use ratio for exact string matching
    matches = process.extract(
        raw_answer, standardized_answers, scorer=fuzz.ratio, limit=top_n
    )

    # Filter by threshold
    return [(match, score, idx) for match, score, idx in matches if score >= threshold]


# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================


@st.cache_data(ttl=300)
def get_unmapped_questions(_client) -> pd.DataFrame:
    """Get questions where the ETL couldn't classify confidently.

    Policy: ETL leaves standardized_question NULL when no matcher is confident.
    This queue is the human triage for those rows. columnar_completed is the
    single source of truth; question_mapping is just a view over it.
    """
    query = f"""
    SELECT
        raw_question_en,
        COUNT(*) as row_count,
        COUNT(DISTINCT CONCAT(CAST(survey_id AS STRING), '-', response_id)) as response_count,
        COUNT(DISTINCT survey_id) as survey_count,
        ARRAY_AGG(DISTINCT survey_id ORDER BY survey_id LIMIT 5) as sample_surveys
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_question IS NULL
      AND raw_question_en IS NOT NULL
      AND TRIM(raw_question_en) != ''
    GROUP BY raw_question_en
    ORDER BY row_count DESC
    """
    return _client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def get_unmapped_answers(_client) -> pd.DataFrame:
    """Get answers where the standardized value is missing.

    Same policy as get_unmapped_questions. Skipped from the queue (not worth a
    reviewer's time to map):
      - numeric-only raw answers
      - N/A-style non-answers: REPLACE(LOWER(raw_answer_en),'/','')='na' catches
        N/A, n/a, NA, na, N/a, etc. (periods/spaces also stripped for n.a., n a).
      - free-text questions (FREE_TEXT_QUESTIONS): their answers are never
        standardized by design, so they'd otherwise be permanent queue noise.
    """
    actionable_filter = _actionable_unmapped_answer_filter()
    query = f"""
    SELECT
        standardized_question,
        raw_answer_en,
        answer_value,
        COUNT(*) as row_count,
        COUNT(DISTINCT CONCAT(CAST(survey_id AS STRING), '-', response_id)) as response_count
    FROM `{COLUMNAR_COMPLETED}`
    WHERE {actionable_filter}
    GROUP BY standardized_question, raw_answer_en, answer_value
    ORDER BY row_count DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "free_text_qs", "STRING", sorted(FREE_TEXT_QUESTIONS)
            )
        ]
    )
    return _client.query(query, job_config=job_config).to_dataframe()


@st.cache_data(ttl=300)
def get_split_brain_candidates(_client) -> pd.DataFrame:
    """Find raw_questions that have been mapped to >=2 distinct standardized_questions.

    Each card aggregates ALL rows of a raw_question (manual + auto). Per limb we
    report: row count, manual-row count (for the bulk-overwrite warning), and which
    matchers contributed via question_match_method so reviewers see the forensics
    inline. The card-level n_std and matrix_like flag are computed over non-manual
    rows only -- they answer "is the AUTO classifier split-brain on this question?"
    """
    query = f"""
    WITH per_pair AS (
      SELECT
        raw_question_en,
        standardized_question,
        COUNT(*) AS rows_,
        COUNTIF(manually_assigned = TRUE) AS manual_rows,
        ARRAY_AGG(DISTINCT IFNULL(question_match_method, 'NULL') IGNORE NULLS) AS methods
      FROM `{COLUMNAR_COMPLETED}`
      WHERE standardized_question IS NOT NULL
        AND raw_question_en IS NOT NULL
        AND TRIM(raw_question_en) != ''
      GROUP BY raw_question_en, standardized_question
    ),
    auto_only_n_std AS (
      SELECT
        raw_question_en,
        COUNT(*) AS n_std_auto,
        COUNT(DISTINCT REGEXP_EXTRACT(standardized_question, r'^([a-z]+)_')) AS n_prefix_families_auto
      FROM per_pair
      WHERE rows_ - manual_rows > 0
      GROUP BY raw_question_en
    ),
    per_raw AS (
      SELECT
        p.raw_question_en,
        ARRAY_AGG(
          STRUCT(
            p.standardized_question AS std_q,
            p.rows_ AS rows_,
            p.manual_rows AS manual_rows,
            p.methods AS methods
          )
          ORDER BY p.rows_ DESC
        ) AS std_q_breakdown,
        COUNT(*) AS n_std,
        SUM(p.rows_) AS total_rows,
        ANY_VALUE(a.n_std_auto) AS n_std_auto,
        ANY_VALUE(a.n_prefix_families_auto) AS n_distinct_prefix_families
      FROM per_pair p
      JOIN auto_only_n_std a USING (raw_question_en)
      GROUP BY p.raw_question_en
    )
    SELECT
      raw_question_en,
      n_std,
      n_std_auto,
      total_rows,
      n_distinct_prefix_families,
      (n_distinct_prefix_families <= 1) AS single_prefix_family,
      std_q_breakdown
    FROM per_raw
    WHERE n_std_auto >= 2
    ORDER BY single_prefix_family ASC, total_rows DESC
    """
    df = _client.query(query).to_dataframe()
    # Post-process: a card is "matrix_like" (hidden by default) if EITHER
    #   (a) every std_q on the card shares a single prefix family, OR
    #   (b) the card has a clear dominant limb and every smaller limb looks like
    #       matrix-rule bleed-through (assigned only by subquestion_pattern /
    #       exact_match, and < 10% of card total).
    # Heuristic (b) is what catches the "managing {site}" pattern where one big
    # correct limb (current_treatment) is polluted by tiny subquestion_pattern
    # misfires (fatigue_level_eating, medication_experience_*).
    df["matrix_bleed"] = df.apply(_is_matrix_bleed_card, axis=1)
    df["matrix_like"] = df["single_prefix_family"] | df["matrix_bleed"]
    df = df.sort_values(
        ["matrix_like", "total_rows"], ascending=[True, False]
    ).reset_index(drop=True)
    return df


_MATRIX_BLEED_METHODS = {"subquestion_pattern", "exact_match"}


def _is_matrix_bleed_card(card_row) -> bool:
    """Return True when the card's split is dominated by subquestion-pattern style
    classifier output (i.e. matrix-rule artifacts, not a genuine split-brain).

    Two independent conditions; either one is enough:

      (A) DOMINANT-LIMB BLEED. There's a clear dominant limb (>= 70% of rows) and
          every smaller limb is matrix-rule output AND < 10% of card. Catches the
          "managing {site}" pattern: 9,830 current_treatment + tiny matrix misfires.

      (B) ALL-MATRIX MULTI-FAMILY. Every limb was assigned only by
          {subquestion_pattern, exact_match}. Catches multi-family matrix questions
          like "Please rate your level of agreement..." where each row legitimately
          routes to a different subquestion family and there's no dominant limb.
    """
    breakdown = (
        list(card_row["std_q_breakdown"])
        if card_row["std_q_breakdown"] is not None
        else []
    )
    if len(breakdown) < 2:
        return False
    total = sum(int(item["rows_"]) for item in breakdown)
    if total == 0:
        return False

    # Condition (B): every limb is matrix-rule output, regardless of dominance.
    every_limb_matrix = True
    for item in breakdown:
        methods_list = item["methods"]
        methods = set(methods_list) if methods_list is not None else set()
        if not methods or not methods.issubset(_MATRIX_BLEED_METHODS):
            every_limb_matrix = False
            break
    if every_limb_matrix:
        return True

    # Condition (A): dominant-limb bleed pattern.
    # Breakdown comes back from the ARRAY_AGG sorted by rows_ DESC, so [0] is dominant.
    dominant_rows = int(breakdown[0]["rows_"])
    if dominant_rows / total < 0.70:
        return False
    for item in breakdown[1:]:
        rows_ = int(item["rows_"])
        if rows_ / total >= 0.10:
            return False
        methods_list = item["methods"]
        methods = set(methods_list) if methods_list is not None else set()
        if not methods or not methods.issubset(_MATRIX_BLEED_METHODS):
            return False
    return True


@st.cache_data(ttl=300)
def get_categories(_client) -> pd.DataFrame:
    """Get all categories from lookup table."""
    query = f"""
    SELECT category, display_name, description, sort_order
    FROM `{CATEGORIES_LOOKUP}`
    ORDER BY sort_order
    """
    return _client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def get_standardized_questions(_client) -> pd.DataFrame:
    """Get all standardized questions from lookup table."""
    query = f"""
    SELECT standardized_question, display_name, category, description
    FROM `{QUESTIONS_LOOKUP}`
    ORDER BY category, standardized_question
    """
    return _client.query(query).to_dataframe()


@st.cache_data(ttl=600)
def _get_answer_context(
    _client, standardized_question: str, raw_answer: str, limit: int = 5
) -> pd.DataFrame:
    """Return sample respondent context for a raw_answer: survey_id, response_id,
    the raw_question_en text, and the raw_answer_en so the reviewer can see the answer
    in situ. Helps disambiguate short/ambiguous answers like 'Other', 'N/A'."""
    query = f"""
    SELECT survey_id, response_id, raw_question_en, raw_answer_en
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_question = @std_q
      AND TRIM(raw_answer_en) = TRIM(@raw_a)
    LIMIT @lim
    """
    return _client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("std_q", "STRING", standardized_question),
                bigquery.ScalarQueryParameter("raw_a", "STRING", raw_answer),
                bigquery.ScalarQueryParameter("lim", "INT64", limit),
            ]
        ),
    ).to_dataframe()


@st.cache_data(ttl=300)
def _prefetch_standardized_answers(_client, questions: tuple) -> dict[str, list[str]]:
    """Load all standardized_answers for MANY questions in a single BigQuery call.

    Returns {standardized_question: [standardized_answer, ...]}.
    """
    if not questions:
        return {}
    query = f"""
    SELECT standardized_question, standardized_answer
    FROM `{ANSWERS_LOOKUP}`
    WHERE standardized_question IN UNNEST(@questions)
    ORDER BY standardized_question, sort_order
    """
    df = _client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("questions", "STRING", list(questions))
            ]
        ),
    ).to_dataframe()
    out: dict[str, list[str]] = {q: [] for q in questions}
    for _, r in df.iterrows():
        out.setdefault(r["standardized_question"], []).append(r["standardized_answer"])
    return out


@st.cache_data(ttl=300)
def get_standardized_answers(_client, question: str = None) -> pd.DataFrame:
    """Get standardized answers, optionally filtered by question."""
    if question:
        query = f"""
        SELECT standardized_question, standardized_answer, display_name, sort_order
        FROM `{ANSWERS_LOOKUP}`
        WHERE standardized_question = @question
        ORDER BY sort_order
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("question", "STRING", question)
            ]
        )
        return _client.query(query, job_config=job_config).to_dataframe()
    else:
        query = f"""
        SELECT standardized_question, standardized_answer, display_name, sort_order
        FROM `{ANSWERS_LOOKUP}`
        ORDER BY standardized_question, sort_order
        """
        return _client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def get_coverage_stats(_client) -> dict:
    """Get current mapping coverage statistics."""
    actionable_answer_filter = _actionable_unmapped_answer_filter()
    query = f"""
    SELECT
        COUNT(*) as total_rows,
        COUNTIF(standardized_question IS NOT NULL) as has_std_question,
        COUNTIF(standardized_answer IS NOT NULL) as has_std_answer,
        COUNTIF(standardized_question IS NULL AND raw_question_en IS NOT NULL AND TRIM(raw_question_en) != '') as unmapped_questions,
        COUNTIF({actionable_answer_filter}) as unmapped_answers,
        COUNTIF(manually_assigned = TRUE) as manual_count
    FROM `{COLUMNAR_COMPLETED}`
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "free_text_qs", "STRING", sorted(FREE_TEXT_QUESTIONS)
            )
        ]
    )
    result = list(_client.query(query, job_config=job_config))
    return {
        "total_rows": result[0].total_rows,
        "has_std_question": result[0].has_std_question,
        "has_std_answer": result[0].has_std_answer,
        "unmapped_questions": result[0].unmapped_questions,
        "unmapped_answers": result[0].unmapped_answers,
        "manual_count": result[0].manual_count,
    }


@st.cache_data(ttl=300)
def get_assignment_provenance(_client) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Summarize how standardized questions and answers were assigned."""
    query = f"""
    WITH question_methods AS (
      SELECT
        'question' AS mapping_type,
        CASE
          WHEN standardized_question IS NULL THEN 'unmapped'
          WHEN question_match_method IS NOT NULL THEN question_match_method
          WHEN COALESCE(manually_assigned, FALSE) THEN 'manual_untracked_method'
          ELSE 'untracked_or_legacy'
        END AS method,
        COUNT(*) AS row_count,
        COUNT(DISTINCT raw_question_en) AS distinct_raw_values,
        AVG(question_match_confidence) AS avg_confidence
      FROM `{COLUMNAR_COMPLETED}`
      GROUP BY method
    ),
    answer_methods AS (
      SELECT
        'answer' AS mapping_type,
        CASE
          WHEN standardized_answer IS NULL THEN 'unmapped'
          WHEN answer_match_method IS NOT NULL THEN answer_match_method
          WHEN COALESCE(manually_assigned, FALSE) THEN 'manual_untracked_method'
          ELSE 'untracked_or_legacy'
        END AS method,
        COUNT(*) AS row_count,
        COUNT(DISTINCT raw_answer_en) AS distinct_raw_values,
        AVG(answer_match_confidence) AS avg_confidence
      FROM `{COLUMNAR_COMPLETED}`
      GROUP BY method
    )
    SELECT * FROM question_methods
    UNION ALL
    SELECT * FROM answer_methods
    ORDER BY mapping_type, row_count DESC
    """
    df = _client.query(query).to_dataframe()
    if df.empty:
        return df, df
    return (
        df[df["mapping_type"] == "question"].drop(columns=["mapping_type"]),
        df[df["mapping_type"] == "answer"].drop(columns=["mapping_type"]),
    )


# ============================================================================
# ANSWER PEEK + ANSWER-AWARE / SEMANTIC SUGGESTIONS
# ============================================================================


@st.cache_data(ttl=600)
def get_top_raw_answers(_client, raw_question: str, limit: int = 30) -> pd.DataFrame:
    """Top distinct answers for a raw_question_en, with response counts.

    Matrix-aware. For matrix/checkbox questions, raw_answer_en is just 'Y'/'N'
    (the tick state), which tells a reviewer nothing about WHAT the question asks.
    The meaningful content is the subquestion topic, stored in standardized_answer.
    So for matrix questions we surface the subquestion topics (with how many ticked
    each) instead of a wall of 'Y'. Non-matrix questions keep the raw_answer view.

    Returns columns: label, response_count, kind ('topic' | 'raw' | 'free_text').
    """
    # Is this a matrix question? (Cheap: limit 1.)
    type_q = f"""
    SELECT ANY_VALUE(question_type) AS qtype
    FROM `{COLUMNAR_COMPLETED}`
    WHERE raw_question_en = @raw_question
    """
    type_rows = list(
        _client.query(
            type_q,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "raw_question", "STRING", raw_question
                    )
                ]
            ),
        )
    )
    qtype = type_rows[0].qtype if type_rows else None

    if qtype == "matrix":
        # Surface the subquestion topics. standardized_answer holds the topic label;
        # if it's NULL (not yet standardized) fall back to subquestion_id so the
        # reviewer still sees the question's structure. Separate the free-text
        # 'other' answers so they're not collapsed into a topic bucket.
        query = f"""
        SELECT
          CASE
            WHEN subquestion_id = 'other'
              THEN CONCAT('[other] ', raw_answer_en)
            WHEN standardized_answer IS NOT NULL
              THEN standardized_answer
            ELSE CONCAT('[subq ', IFNULL(subquestion_id, '?'), ']')
          END AS label,
          COUNT(*) AS response_count,
          CASE
            WHEN subquestion_id = 'other' THEN 'free_text'
            ELSE 'topic'
          END AS kind
        FROM `{COLUMNAR_COMPLETED}`
        WHERE raw_question_en = @raw_question
          AND raw_answer_en IS NOT NULL
          AND TRIM(raw_answer_en) != ''
        GROUP BY label, kind
        ORDER BY response_count DESC
        LIMIT @limit
        """
    else:
        query = f"""
        SELECT raw_answer_en AS label, COUNT(*) AS response_count, 'raw' AS kind
        FROM `{COLUMNAR_COMPLETED}`
        WHERE raw_question_en = @raw_question
          AND raw_answer_en IS NOT NULL
          AND TRIM(raw_answer_en) != ''
        GROUP BY raw_answer_en
        ORDER BY response_count DESC
        LIMIT @limit
        """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("raw_question", "STRING", raw_question),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )
    df = _client.query(query, job_config=job_config).to_dataframe()
    df.attrs["question_type"] = qtype
    return df


@st.cache_data(ttl=600)
def get_cooccurrence_suggestions(
    _client, raw_question: str, top_n: int = 5
) -> pd.DataFrame:
    """B1: Suggest standardized_questions whose answer-set overlaps with this raw_question's answers.

    Logic: take the top distinct raw_answers for THIS raw_question. Find OTHER raw_questions in
    columnar_completed that are already mapped (standardized_question IS NOT NULL) and share at
    least one of those raw_answers. Rank by (number of shared answers) * (mapping frequency).
    """
    query = f"""
    WITH this_question_answers AS (
        SELECT DISTINCT TRIM(LOWER(raw_answer_en)) AS norm_answer
        FROM `{COLUMNAR_COMPLETED}`
        WHERE raw_question_en = @raw_question
          AND raw_answer_en IS NOT NULL
          AND TRIM(raw_answer_en) != ''
        LIMIT 50
    ),
    other_mapped AS (
        SELECT
            c.standardized_question,
            TRIM(LOWER(c.raw_answer_en)) AS norm_answer
        FROM `{COLUMNAR_COMPLETED}` c
        WHERE c.standardized_question IS NOT NULL
          AND c.raw_question_en != @raw_question
          AND c.raw_answer_en IS NOT NULL
          AND TRIM(c.raw_answer_en) != ''
    )
    SELECT
        o.standardized_question,
        COUNT(DISTINCT o.norm_answer) AS shared_answers,
        COUNT(*) AS total_evidence_rows
    FROM other_mapped o
    JOIN this_question_answers t USING (norm_answer)
    GROUP BY o.standardized_question
    HAVING shared_answers >= 1
    ORDER BY shared_answers DESC, total_evidence_rows DESC
    LIMIT @top_n
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("raw_question", "STRING", raw_question),
            bigquery.ScalarQueryParameter("top_n", "INT64", top_n),
        ]
    )
    return _client.query(query, job_config=job_config).to_dataframe()


@st.cache_resource
def get_semantic_index():
    """Load sentence-transformer once and cache the embeddings of every already-mapped raw_question.

    Returns dict: {'model': SentenceTransformer, 'questions': List[str],
                   'std_questions': List[str], 'embeddings': np.ndarray}
    None if sentence-transformers not available.
    """
    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except ImportError:
        return None

    client = get_bq_client()
    df = client.query(
        f"""
        SELECT DISTINCT raw_question_en, standardized_question
        FROM `{COLUMNAR_COMPLETED}`
        WHERE standardized_question IS NOT NULL
          AND raw_question_en IS NOT NULL
          AND TRIM(raw_question_en) != ''
    """
    ).to_dataframe()

    if df.empty:
        return None

    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    questions = df["raw_question_en"].tolist()
    embeddings = model.encode(questions, convert_to_numpy=True, show_progress_bar=False)
    return {
        "model": model,
        "questions": questions,
        "std_questions": df["standardized_question"].tolist(),
        "embeddings": embeddings,
    }


def get_semantic_suggestions(
    raw_question: str, top_n: int = 5
) -> list[tuple[str, float]]:
    """Embed raw_question and return the top_n standardized_questions of the most similar mapped raw_questions.

    Returns list of (standardized_question, similarity_score 0-1).
    """
    index = get_semantic_index()
    if index is None:
        return []

    import numpy as np

    query_emb = index["model"].encode(
        [raw_question], convert_to_numpy=True, show_progress_bar=False
    )[0]
    # cosine similarity
    norms = np.linalg.norm(index["embeddings"], axis=1) * np.linalg.norm(query_emb)
    norms[norms == 0] = 1
    sims = (index["embeddings"] @ query_emb) / norms

    # Aggregate by standardized_question (take max sim per std_q)
    agg: dict[str, float] = {}
    for std_q, sim in zip(index["std_questions"], sims):
        if std_q not in agg or sim > agg[std_q]:
            agg[std_q] = float(sim)

    return sorted(agg.items(), key=lambda x: x[1], reverse=True)[:top_n]


@st.cache_resource
def get_ml_model_index():
    """Load the trained RandomForest question-classifier and the
    sentence-transformer it uses, once, and return them as a dict.

    Resolves the model path identically to the ETL processor and the
    Model page:
      LIMESURVEY_MODEL_DIR env var (set on Linux via the lime-edit systemd
      drop-in to /home/orchestrator/models), else <repo_root>/models.

    Returns None silently if:
      - sentence_transformers / sklearn not installed
      - model files are missing
      - load fails (e.g. sklearn version skew)

    The 'model' is the supervised classifier; the 'transformer' produces
    the same embeddings the ETL feeds to it. The 'encoder' maps
    classifier output indices back to standardized_question strings.
    """
    try:
        import pickle

        from sentence_transformers import SentenceTransformer
    except ImportError:
        return None

    model_dir = Path(
        os.environ.get("LIMESURVEY_MODEL_DIR")
        or (Path(__file__).resolve().parent.parent.parent / "models")
    )
    model_file = model_dir / "question_classifier.pkl"
    encoder_file = model_dir / "label_encoder.pkl"
    if not model_file.exists() or not encoder_file.exists():
        return None

    try:
        with open(model_file, "rb") as f:
            classifier = pickle.load(f)
        with open(encoder_file, "rb") as f:
            encoder = pickle.load(f)
        transformer = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    except Exception:
        return None

    return {
        "classifier": classifier,
        "encoder": encoder,
        "transformer": transformer,
    }


def get_ml_model_suggestions(
    raw_question: str, top_n: int = 5
) -> list[tuple[str, float]]:
    """Run the trained classifier against the raw_question and return the
    top_n predicted standardized_questions with their probabilities.

    Mirrors the ETL processor's _match_with_ml() path exactly:
    sentence-transformer encode -> RandomForest predict_proba -> top-N.

    Same input transformation as training: the function expects the
    caller to handle subquestion_text if the row is a matrix row (the
    suggestion UI today doesn't have subquestion_id context, so this
    operates on the raw text alone, which matches the typical reviewer
    workflow on the Unmapped Questions page).

    Returns [] if the model can't be loaded.
    """
    index = get_ml_model_index()
    if index is None:
        return []

    try:
        import numpy as np

        embedding = index["transformer"].encode(
            [raw_question or ""], convert_to_numpy=True, show_progress_bar=False
        )
        probs = index["classifier"].predict_proba(embedding)[0]
        top_idx = np.argsort(probs)[::-1][:top_n]
        classes = index["encoder"].inverse_transform(top_idx)
        return [(str(c), float(probs[i])) for c, i in zip(classes, top_idx)]
    except Exception:
        return []


# Visual prefixes used to group ranked dropdown choices into sections without
# relying on disabled separator items (Streamlit selectbox doesn't support them).
# The prefix is stripped before the value is used.
RANK_PREFIX_EXISTING = "★ "
RANK_PREFIX_ML = "🤖 "
RANK_PREFIX_DIVIDER = "── all others ──"


def _rank_audit_choices(
    raw_question: str,
    breakdown: list,
    all_std_qs: list[str],
    ml_top_n: int = 5,
) -> list[str]:
    """Build a ranked option list for the Audit page dropdowns.

    Order:
      1. Existing limbs on the card, by row count desc (prefixed with ★)
      2. ML semantic top picks, by confidence desc, excluding anything already in #1
         (prefixed with 🤖, suffixed with confidence)
      3. A non-selectable divider row
      4. Every remaining std_q alphabetically

    Returns a list of display strings. Use _unwrap_audit_choice() to extract the
    underlying std_q before passing to BigQuery.
    """
    options: list[str] = [""]

    # Section 1: existing limbs by row count
    existing = []
    for item in breakdown:
        std_q = item.get("std_q") if isinstance(item, dict) else item["std_q"]
        rows_ = item.get("rows_") if isinstance(item, dict) else item["rows_"]
        existing.append((std_q, int(rows_)))
    existing_set = {std_q for std_q, _ in existing}
    for std_q, rows_ in existing:
        options.append(f"{RANK_PREFIX_EXISTING}{std_q}  ({rows_:,} rows on card)")

    # Section 2: ML top picks excluding what's already shown
    try:
        ml_picks = get_semantic_suggestions(raw_question, top_n=ml_top_n * 2)
    except Exception:
        ml_picks = []
    ml_shown = 0
    for std_q, score in ml_picks:
        if std_q in existing_set:
            continue
        options.append(f"{RANK_PREFIX_ML}{std_q}  (ML conf {score:.2f})")
        ml_shown += 1
        if ml_shown >= ml_top_n:
            break

    # Section 3 + 4: divider, then the rest
    shown_so_far = existing_set | {
        _unwrap_audit_choice(o) for o in options if o.startswith(RANK_PREFIX_ML)
    }
    rest = sorted(s for s in all_std_qs if s not in shown_so_far)
    if rest:
        options.append(RANK_PREFIX_DIVIDER)
        options.extend(rest)

    return options


def _unwrap_audit_choice(display: str) -> str:
    """Reverse the cosmetic prefixing applied by _rank_audit_choices.

    Returns the bare std_q value, or '' for the empty/divider entries.
    """
    if not display or display == RANK_PREFIX_DIVIDER:
        return ""
    s = display
    if s.startswith(RANK_PREFIX_EXISTING):
        s = s[len(RANK_PREFIX_EXISTING) :]
    elif s.startswith(RANK_PREFIX_ML):
        s = s[len(RANK_PREFIX_ML) :]
    # Strip the trailing annotation like "  (1,234 rows on card)" or "  (ML conf 0.62)"
    if "  (" in s:
        s = s.split("  (")[0]
    return s.strip()


@st.cache_data(ttl=600)
def get_fingerprint_suggestions(
    _client, raw_question: str, top_n: int = 10
) -> pd.DataFrame:
    """Rank standardized_questions by how many of their fingerprint answers overlap with
    this raw_question's top distinct answers. Uses the nightly-built fingerprint table.

    Returns empty DataFrame if the table doesn't exist yet.
    """
    query = f"""
    WITH this_q AS (
        SELECT DISTINCT TRIM(LOWER(raw_answer_en)) AS ans
        FROM `{COLUMNAR_COMPLETED}`
        WHERE raw_question_en = @raw_q
          AND raw_answer_en IS NOT NULL AND TRIM(raw_answer_en) != ''
        LIMIT 30
    ),
    fp AS (
        SELECT standardized_question, TRIM(LOWER(answer)) AS ans
        FROM `{ANSWER_FINGERPRINTS}`, UNNEST(top_answers) AS answer
    )
    SELECT
        fp.standardized_question,
        COUNT(DISTINCT fp.ans) AS fp_overlap,
        (SELECT COUNT(*) FROM this_q) AS this_q_size
    FROM fp
    JOIN this_q USING (ans)
    GROUP BY fp.standardized_question
    HAVING fp_overlap >= 1
    ORDER BY fp_overlap DESC
    LIMIT @top_n
    """
    try:
        return _client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("raw_q", "STRING", raw_question),
                    bigquery.ScalarQueryParameter("top_n", "INT64", top_n),
                ]
            ),
        ).to_dataframe()
    except Exception:
        return pd.DataFrame()


def get_blended_suggestions(
    client, raw_question: str, std_questions: list, fuzzy_threshold: int = 60
) -> pd.DataFrame:
    """Combine fuzzy + co-occurrence + semantic + answer-fingerprint + ml_model
    into a single ranked list.

    Each source contributes a 0-1 normalized score; final = sum of contributing
    source scores (so a candidate appearing in multiple sources naturally
    outranks one appearing in just one).

    The ml_model source is the trained RandomForest classifier from the ETL
    cascade. If its top pick scores >= ML_PIN_THRESHOLD it is pinned to
    position 1 of the result, regardless of blended score.
    """
    rows: dict[str, dict] = {}

    def _ensure(std_q):
        rows.setdefault(
            std_q,
            {
                "std_question": std_q,
                "fuzzy": 0.0,
                "cooccur": 0.0,
                "semantic": 0.0,
                "fingerprint": 0.0,
                "ml_model": 0.0,
                "shared_answers": 0,
                "sources": [],
            },
        )

    # 1. Fuzzy on the question text itself
    fuzzy = get_fuzzy_question_matches(
        raw_question, std_questions, top_n=10, threshold=0
    )
    for std_q, score, _ in fuzzy:
        _ensure(std_q)
        rows[std_q]["fuzzy"] = score / 100.0
        rows[std_q]["sources"].append(f"fuzzy {score}%")

    # 2. Co-occurrence on raw answers
    try:
        cooc_df = get_cooccurrence_suggestions(client, raw_question, top_n=10)
        if not cooc_df.empty:
            max_shared = cooc_df["shared_answers"].max()
            for _, r in cooc_df.iterrows():
                std_q = r["standardized_question"]
                _ensure(std_q)
                rows[std_q]["cooccur"] = (
                    float(r["shared_answers"] / max_shared) if max_shared else 0.0
                )
                rows[std_q]["shared_answers"] = int(r["shared_answers"])
                rows[std_q]["sources"].append(f"answers shared:{r['shared_answers']}")
    except Exception as e:
        st.caption(f"⚠️ Co-occurrence lookup unavailable: {e}")

    # 3. Semantic embedding match
    try:
        for std_q, sim in get_semantic_suggestions(raw_question, top_n=10):
            _ensure(std_q)
            rows[std_q]["semantic"] = float(sim)
            rows[std_q]["sources"].append(f"semantic {sim:.2f}")
    except Exception as e:
        st.caption(f"⚠️ Semantic suggestions unavailable: {e}")

    # 4. Answer fingerprint overlap (uses nightly-built table; silent no-op if missing)
    try:
        fp_df = get_fingerprint_suggestions(client, raw_question, top_n=10)
        if not fp_df.empty:
            max_fp = fp_df["fp_overlap"].max()
            for _, r in fp_df.iterrows():
                std_q = r["standardized_question"]
                _ensure(std_q)
                rows[std_q]["fingerprint"] = (
                    float(r["fp_overlap"] / max_fp) if max_fp else 0.0
                )
                rows[std_q]["sources"].append(f"fingerprint:{r['fp_overlap']}")
    except Exception:
        pass  # silent — fingerprint table is optional

    # 5. ML model (trained RandomForest classifier from the ETL cascade).
    # Probability is already 0-1; same scale as the other sources. Silent
    # no-op if the model file or sentence-transformer is unavailable.
    ml_top_pick: tuple[str, float] | None = None
    try:
        ml_picks = get_ml_model_suggestions(raw_question, top_n=10)
        for std_q, prob in ml_picks:
            _ensure(std_q)
            rows[std_q]["ml_model"] = float(prob)
            rows[std_q]["sources"].append(f"ml_model {prob:.2f}")
        if ml_picks:
            ml_top_pick = ml_picks[0]
    except Exception:
        pass  # silent — model is optional

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(list(rows.values()))

    # Filter noise: drop candidates where the three strong sources
    # (ml_model / fuzzy / semantic) all said nothing. Such rows only got
    # into the blend because cooccurrence or fingerprint happened to
    # max out -- usually a Yes/No/Not-sure answer set coinciding with
    # an unrelated canonical that uses the same scale. Without ANY
    # textual or model evidence, these are not real candidates.
    strong_present = (df["ml_model"] > 0) | (df["fuzzy"] > 0) | (df["semantic"] > 0)
    df = df[strong_present].copy()

    if df.empty:
        return pd.DataFrame()

    # Weighted score: the answer-distribution signals (cooccur + fingerprint)
    # are weak corroborators and shouldn't dominate when ml_model / fuzzy /
    # semantic are silent. ml_model gets 2x because it's the strongest
    # single signal in the cascade and the most likely to actually be right.
    df["score"] = (
        df["ml_model"] * 2.0
        + df["semantic"] * 1.0
        + df["fuzzy"] * 1.0
        + df["cooccur"] * 0.25
        + df["fingerprint"] * 0.25
    )
    df["source_count"] = df["sources"].apply(len)
    df = df.sort_values(["source_count", "score"], ascending=[False, False]).head(10)

    # Pin the model's top pick to position 1 when its confidence is high
    # enough. Reviewers are told elsewhere "ml_model uses the trained
    # classifier"; surfacing it at the top makes that signal load-bearing
    # instead of just decorative. Threshold matches the ETL's ML_MIN_ACCEPT
    # (0.30) bumped up slightly for the UI -- a UI pin should require more
    # confidence than a silent cascade write.
    ML_PIN_THRESHOLD = 0.50
    if ml_top_pick is not None and ml_top_pick[1] >= ML_PIN_THRESHOLD:
        pin_std_q = ml_top_pick[0]
        if pin_std_q in df["std_question"].values:
            mask = df["std_question"] == pin_std_q
            pinned_row = df[mask]
            other_rows = df[~mask]
            df = pd.concat([pinned_row, other_rows], ignore_index=True)

    df["sources_str"] = df["sources"].apply(lambda s: " · ".join(s))
    return df[
        [
            "std_question",
            "score",
            "fuzzy",
            "cooccur",
            "semantic",
            "fingerprint",
            "ml_model",
            "shared_answers",
            "sources_str",
        ]
    ]


@st.cache_data(ttl=300)
def get_best_suggestions_batch(
    _client, raw_questions: tuple, std_questions: tuple
) -> pd.DataFrame:
    """Fast fuzzy + semantic top-1 for MANY raw_questions at once.

    Used to populate the table view — one pass over the semantic index + local fuzzy loop
    instead of 30 separate BigQuery co-occurrence queries. Co-occurrence is intentionally
    omitted here; it kicks in on the detail panel only.

    Args: raw_questions and std_questions as tuples so they're hashable for @st.cache_data.
    Returns DataFrame with columns: raw_question, best_std, best_score.
    """
    if not raw_questions or not std_questions:
        return pd.DataFrame(columns=["raw_question", "best_std", "best_score"])

    std_list = list(std_questions)
    results = []

    index = get_semantic_index()
    sem_map: dict[str, tuple[str, float]] = {}  # raw_q -> (std_q, score)
    if index is not None:
        try:
            import numpy as np

            query_emb = index["model"].encode(
                list(raw_questions), convert_to_numpy=True, show_progress_bar=False
            )
            ref_norms = np.linalg.norm(index["embeddings"], axis=1)
            ref_norms[ref_norms == 0] = 1
            for i, rq in enumerate(raw_questions):
                q_norm = np.linalg.norm(query_emb[i]) or 1
                sims = (index["embeddings"] @ query_emb[i]) / (ref_norms * q_norm)
                # Pick best per standardized_question
                agg: dict[str, float] = {}
                for std_q, sim in zip(index["std_questions"], sims):
                    if std_q not in agg or sim > agg[std_q]:
                        agg[std_q] = float(sim)
                if agg:
                    best_std = max(agg, key=agg.get)
                    sem_map[rq] = (best_std, agg[best_std])
        except Exception:
            pass

    for rq in raw_questions:
        fuzzy_hit = next(
            iter(get_fuzzy_question_matches(rq, std_list, top_n=1, threshold=0)), None
        )
        fuzzy_score = (fuzzy_hit[1] / 100.0) if fuzzy_hit else 0.0
        fuzzy_std = fuzzy_hit[0] if fuzzy_hit else None

        sem_std, sem_score = sem_map.get(rq, (None, 0.0))

        # Blend: if both agree, sum. Else pick the higher.
        if fuzzy_std and sem_std and fuzzy_std == sem_std:
            best_std, best_score = fuzzy_std, fuzzy_score + sem_score
        elif fuzzy_score >= sem_score:
            best_std, best_score = fuzzy_std, fuzzy_score
        else:
            best_std, best_score = sem_std, sem_score

        results.append(
            {
                "raw_question": rq,
                "best_std": best_std or "",
                "best_score": round(best_score, 2),
            }
        )

    return pd.DataFrame(results)


# ============================================================================
# WRITE FUNCTIONS
# ============================================================================


def add_category(
    client, category: str, display_name: str = None, description: str = None
) -> bool:
    """Add a new category to the lookup table. Idempotent: re-inserts are no-ops."""
    query = f"""
    MERGE `{CATEGORIES_LOOKUP}` T
    USING (SELECT @category AS category) S
    ON T.category = S.category
    WHEN NOT MATCHED THEN
      INSERT (category, display_name, description, sort_order)
      VALUES (@category, @display_name, @description,
              (SELECT COALESCE(MAX(sort_order), 0) + 1 FROM `{CATEGORIES_LOOKUP}`))
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("category", "STRING", category),
            bigquery.ScalarQueryParameter(
                "display_name",
                "STRING",
                display_name or category.replace("_", " ").title(),
            ),
            bigquery.ScalarQueryParameter("description", "STRING", description or ""),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False


def update_category(
    client, category: str, display_name: str, description: str, sort_order: int
) -> bool:
    """Update an existing category's display_name, description, sort_order."""
    query = f"""
    UPDATE `{CATEGORIES_LOOKUP}`
    SET display_name = @display_name,
        description = @description,
        sort_order = @sort_order
    WHERE category = @category
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("category", "STRING", category),
            bigquery.ScalarQueryParameter("display_name", "STRING", display_name or ""),
            bigquery.ScalarQueryParameter("description", "STRING", description or ""),
            bigquery.ScalarQueryParameter("sort_order", "INT64", sort_order),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"Error updating category '{category}': {e}")
        return False


def count_questions_in_category(client, category: str) -> int:
    """How many standardized_questions are tagged with this category."""
    query = f"""
    SELECT COUNT(*) AS cnt FROM `{QUESTIONS_LOOKUP}` WHERE category = @category
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("category", "STRING", category),
        ]
    )
    try:
        return list(client.query(query, job_config=job_config))[0].cnt
    except Exception:
        return 0


def count_questions_rows_affected(client, standardized_question: str) -> int:
    """How many lime_surveys_columnar_completed rows currently carry this
    standardized_question. Used by the inline-edit confirmation panel to
    show the blast radius of a rename before applying."""
    query = f"""
    SELECT COUNT(*) AS cnt FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_question = @std_q
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("std_q", "STRING", standardized_question),
        ]
    )
    try:
        return list(client.query(query, job_config=job_config))[0].cnt
    except Exception:
        return 0


def count_answers_rows_affected(
    client, standardized_question: str, standardized_answer: str
) -> int:
    """How many lime_surveys_columnar_completed rows currently carry this
    (standardized_question, standardized_answer) pair. Used by the inline-edit
    confirmation panel to show the blast radius of an answer rename before
    applying."""
    query = f"""
    SELECT COUNT(*) AS cnt FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_question = @std_q
      AND standardized_answer = @std_a
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("std_q", "STRING", standardized_question),
            bigquery.ScalarQueryParameter("std_a", "STRING", standardized_answer),
        ]
    )
    try:
        return list(client.query(query, job_config=job_config))[0].cnt
    except Exception:
        return 0


def rename_category(client, old_name: str, new_name: str) -> bool:
    """Rename a category in categories_lookup AND cascade to standardized_questions_lookup.category."""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("old_name", "STRING", old_name),
            bigquery.ScalarQueryParameter("new_name", "STRING", new_name),
        ]
    )
    try:
        client.query(
            f"UPDATE `{CATEGORIES_LOOKUP}` SET category = @new_name WHERE category = @old_name",
            job_config=job_config,
        ).result()
        client.query(
            f"UPDATE `{QUESTIONS_LOOKUP}` SET category = @new_name WHERE category = @old_name",
            job_config=job_config,
        ).result()
        return True
    except Exception as e:
        st.error(f"Error renaming category: {e}")
        return False


def add_standardized_question(
    client,
    question: str,
    display_name: str = None,
    category: str = "other",
    description: str = None,
) -> bool:
    """Add a new standardized question to the lookup table. Idempotent: re-inserts are no-ops."""
    query = f"""
    MERGE `{QUESTIONS_LOOKUP}` T
    USING (SELECT @question AS standardized_question) S
    ON T.standardized_question = S.standardized_question
    WHEN NOT MATCHED THEN
      INSERT (standardized_question, display_name, category, description)
      VALUES (@question, @display_name, @category, @description)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("question", "STRING", question),
            bigquery.ScalarQueryParameter(
                "display_name",
                "STRING",
                display_name or question.replace("_", " ").title(),
            ),
            bigquery.ScalarQueryParameter("category", "STRING", category),
            bigquery.ScalarQueryParameter("description", "STRING", description or ""),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False


def add_standardized_answer(
    client, question: str, answer: str, display_name: str = None
) -> bool:
    """Add a new standardized answer to the lookup table. Idempotent: re-inserts are no-ops."""
    query = f"""
    MERGE `{ANSWERS_LOOKUP}` T
    USING (SELECT @question AS standardized_question, @answer AS standardized_answer) S
    ON T.standardized_question = S.standardized_question AND T.standardized_answer = S.standardized_answer
    WHEN NOT MATCHED THEN
      INSERT (standardized_question, standardized_answer, display_name, sort_order)
      VALUES (@question, @answer, @display_name,
              (SELECT COALESCE(MAX(sort_order), 0) + 1 FROM `{ANSWERS_LOOKUP}` WHERE standardized_question = @question))
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("question", "STRING", question),
            bigquery.ScalarQueryParameter("answer", "STRING", answer),
            bigquery.ScalarQueryParameter(
                "display_name",
                "STRING",
                display_name or answer.replace("_", " ").title(),
            ),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False


# ============================================================================
# LOOKUP TABLE EDIT / SPLIT / MERGE / DELETE
# ============================================================================
# Mutation primitives for the "Edit / Split / Merge" tab on the Lookup Tables
# page. Every primitive that writes to lime_surveys_columnar_completed stamps
# manually_assigned_by = 'streamlit_app' so the analyst Google Sheet (Pass A of
# sql/standardize_lime_survey_answers.sql) does not clobber the change on the
# next nightly run. Every primitive logs to mapping_history so Undo / Audit works.


def rename_standardized_question(client, old_name: str, new_name: str) -> int:
    """Rename a standardized_question key.

    Cascades to (a) standardized_questions_lookup row key,
    (b) standardized_answers_lookup.standardized_question for every answer
    under the old name, and (c) every matching row of columnar_completed.
    Returns the number of columnar_completed rows touched."""
    cc_update = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_question = @new_name,
        manually_assigned = TRUE,
        manually_assigned_at = CURRENT_TIMESTAMP(),
        manually_assigned_by = 'streamlit_app'
    WHERE standardized_question = @old_name
    """
    answers_update = f"""
    UPDATE `{ANSWERS_LOOKUP}`
    SET standardized_question = @new_name, updated_at = CURRENT_TIMESTAMP()
    WHERE standardized_question = @old_name
    """
    lookup_update = f"""
    UPDATE `{QUESTIONS_LOOKUP}`
    SET standardized_question = @new_name, updated_at = CURRENT_TIMESTAMP()
    WHERE standardized_question = @old_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("old_name", "STRING", old_name),
            bigquery.ScalarQueryParameter("new_name", "STRING", new_name),
        ]
    )
    job = client.query(cc_update, job_config=job_config)
    job.result()
    affected = job.num_dml_affected_rows or 0
    client.query(answers_update, job_config=job_config).result()
    client.query(lookup_update, job_config=job_config).result()
    _log_history(
        client,
        "lookup_rename_question",
        affected,
        new_std_question=new_name,
        raw_question=old_name,
    )
    return affected


def rename_standardized_answer(
    client, std_question: str, old_answer: str, new_answer: str
) -> int:
    """Rename a standardized_answer key within a given standardized_question."""
    cc_update = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_answer = @new_answer,
        manually_assigned = TRUE,
        manually_assigned_at = CURRENT_TIMESTAMP(),
        manually_assigned_by = 'streamlit_app'
    WHERE standardized_question = @std_question
      AND standardized_answer = @old_answer
    """
    lookup_update = f"""
    UPDATE `{ANSWERS_LOOKUP}`
    SET standardized_answer = @new_answer, updated_at = CURRENT_TIMESTAMP()
    WHERE standardized_question = @std_question
      AND standardized_answer = @old_answer
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("std_question", "STRING", std_question),
            bigquery.ScalarQueryParameter("old_answer", "STRING", old_answer),
            bigquery.ScalarQueryParameter("new_answer", "STRING", new_answer),
        ]
    )
    job = client.query(cc_update, job_config=job_config)
    job.result()
    affected = job.num_dml_affected_rows or 0
    client.query(lookup_update, job_config=job_config).result()
    _log_history(
        client,
        "lookup_rename_answer",
        affected,
        raw_question=std_question,
        raw_answer=old_answer,
        new_std_question=std_question,
        new_std_answer=new_answer,
    )
    return affected


def update_standardized_question_metadata(
    client,
    standardized_question: str,
    display_name: str | None,
    category: str | None,
    description: str | None,
) -> bool:
    """Update lookup-only fields (display_name, category, description) for a
    standardized_question. Does NOT touch the key column or the fact table --
    use rename_standardized_question for key renames."""
    try:
        client.query(
            f"""
            UPDATE `{QUESTIONS_LOOKUP}`
            SET display_name = @display_name,
                category = @category,
                description = @description,
                updated_at = CURRENT_TIMESTAMP()
            WHERE standardized_question = @key
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "key", "STRING", standardized_question
                    ),
                    bigquery.ScalarQueryParameter(
                        "display_name", "STRING", display_name
                    ),
                    bigquery.ScalarQueryParameter("category", "STRING", category),
                    bigquery.ScalarQueryParameter("description", "STRING", description),
                ]
            ),
        ).result()
        return True
    except Exception as e:
        st.error(f"Failed to update {standardized_question}: {e}")
        return False


def update_standardized_answer_metadata(
    client,
    standardized_question: str,
    standardized_answer: str,
    display_name: str | None,
    sort_order: int | None,
) -> bool:
    """Update lookup-only fields (display_name, sort_order) for a
    standardized_answer. Does NOT touch the key columns or the fact table --
    use rename_standardized_answer for key renames."""
    try:
        client.query(
            f"""
            UPDATE `{ANSWERS_LOOKUP}`
            SET display_name = @display_name,
                sort_order = @sort_order,
                updated_at = CURRENT_TIMESTAMP()
            WHERE standardized_question = @std_q
              AND standardized_answer = @std_a
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "std_q", "STRING", standardized_question
                    ),
                    bigquery.ScalarQueryParameter(
                        "std_a", "STRING", standardized_answer
                    ),
                    bigquery.ScalarQueryParameter(
                        "display_name", "STRING", display_name
                    ),
                    bigquery.ScalarQueryParameter("sort_order", "INT64", sort_order),
                ]
            ),
        ).result()
        return True
    except Exception as e:
        st.error(f"Failed to update {standardized_question}/{standardized_answer}: {e}")
        return False


def split_standardized_question(
    client, source: str, new_name: str, raw_questions_to_move: list[str]
) -> int:
    """Split a standardized_question: create new_name, and move the listed
    raw_question_en values from source to new_name in columnar_completed.
    Answers stay attached to whichever raw_question they belong to, so the
    answers_lookup needs the same answers registered under new_name as exist
    under source (we MERGE them across)."""
    add_standardized_question(client, new_name)
    cc_update = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_question = @new_name,
        manually_assigned = TRUE,
        manually_assigned_at = CURRENT_TIMESTAMP(),
        manually_assigned_by = 'streamlit_app'
    WHERE standardized_question = @source
      AND raw_question_en IN UNNEST(@raw_qs)
    """
    answers_merge = f"""
    MERGE `{ANSWERS_LOOKUP}` T
    USING (
      SELECT DISTINCT standardized_answer, display_name, sort_order
      FROM `{ANSWERS_LOOKUP}`
      WHERE standardized_question = @source
    ) S
    ON T.standardized_question = @new_name AND T.standardized_answer = S.standardized_answer
    WHEN NOT MATCHED THEN
      INSERT (standardized_question, standardized_answer, display_name, sort_order, created_at, updated_at)
      VALUES (@new_name, S.standardized_answer, S.display_name, S.sort_order, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source", "STRING", source),
            bigquery.ScalarQueryParameter("new_name", "STRING", new_name),
            bigquery.ArrayQueryParameter("raw_qs", "STRING", raw_questions_to_move),
        ]
    )
    job = client.query(cc_update, job_config=job_config)
    job.result()
    affected = job.num_dml_affected_rows or 0
    client.query(answers_merge, job_config=job_config).result()
    _log_history(
        client,
        "lookup_split_question",
        affected,
        raw_question=source,
        new_std_question=new_name,
    )
    return affected


def split_standardized_answer(
    client,
    std_question: str,
    source: str,
    new_name: str,
    raw_answers_to_move: list[str],
) -> int:
    """Split a standardized_answer: create new_name under std_question, and
    move the listed raw_answer_en values from source to new_name in
    columnar_completed."""
    add_standardized_answer(client, std_question, new_name)
    cc_update = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_answer = @new_name,
        manually_assigned = TRUE,
        manually_assigned_at = CURRENT_TIMESTAMP(),
        manually_assigned_by = 'streamlit_app'
    WHERE standardized_question = @std_question
      AND standardized_answer = @source
      AND raw_answer_en IN UNNEST(@raw_as)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("std_question", "STRING", std_question),
            bigquery.ScalarQueryParameter("source", "STRING", source),
            bigquery.ScalarQueryParameter("new_name", "STRING", new_name),
            bigquery.ArrayQueryParameter("raw_as", "STRING", raw_answers_to_move),
        ]
    )
    job = client.query(cc_update, job_config=job_config)
    job.result()
    affected = job.num_dml_affected_rows or 0
    _log_history(
        client,
        "lookup_split_answer",
        affected,
        raw_question=std_question,
        raw_answer=source,
        new_std_question=std_question,
        new_std_answer=new_name,
    )
    return affected


def merge_standardized_questions(client, target: str, sources: list[str]) -> int:
    """Merge one or more standardized_questions into target.

    For each source: update columnar_completed.standardized_question to target,
    MERGE the source's answers into target's row of answers_lookup (any
    duplicates ignored), then DELETE the source's lookup rows."""
    if target in sources:
        sources = [s for s in sources if s != target]
    if not sources:
        return 0

    cc_update = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_question = @target,
        manually_assigned = TRUE,
        manually_assigned_at = CURRENT_TIMESTAMP(),
        manually_assigned_by = 'streamlit_app'
    WHERE standardized_question IN UNNEST(@sources)
    """
    answers_merge = f"""
    MERGE `{ANSWERS_LOOKUP}` T
    USING (
      SELECT DISTINCT standardized_answer, display_name, sort_order
      FROM `{ANSWERS_LOOKUP}`
      WHERE standardized_question IN UNNEST(@sources)
    ) S
    ON T.standardized_question = @target AND T.standardized_answer = S.standardized_answer
    WHEN NOT MATCHED THEN
      INSERT (standardized_question, standardized_answer, display_name, sort_order, created_at, updated_at)
      VALUES (@target, S.standardized_answer, S.display_name, S.sort_order, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """
    answers_delete = f"""
    DELETE FROM `{ANSWERS_LOOKUP}` WHERE standardized_question IN UNNEST(@sources)
    """
    questions_delete = f"""
    DELETE FROM `{QUESTIONS_LOOKUP}` WHERE standardized_question IN UNNEST(@sources)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target", "STRING", target),
            bigquery.ArrayQueryParameter("sources", "STRING", sources),
        ]
    )
    job = client.query(cc_update, job_config=job_config)
    job.result()
    affected = job.num_dml_affected_rows or 0
    client.query(answers_merge, job_config=job_config).result()
    client.query(answers_delete, job_config=job_config).result()
    client.query(questions_delete, job_config=job_config).result()
    _log_history(
        client,
        "lookup_merge_questions",
        affected,
        raw_question=",".join(sources),
        new_std_question=target,
    )
    return affected


def merge_standardized_answers(
    client, std_question: str, target: str, sources: list[str]
) -> int:
    """Merge one or more standardized_answers within a std_question into target."""
    if target in sources:
        sources = [s for s in sources if s != target]
    if not sources:
        return 0

    cc_update = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_answer = @target,
        manually_assigned = TRUE,
        manually_assigned_at = CURRENT_TIMESTAMP(),
        manually_assigned_by = 'streamlit_app'
    WHERE standardized_question = @std_question
      AND standardized_answer IN UNNEST(@sources)
    """
    lookup_delete = f"""
    DELETE FROM `{ANSWERS_LOOKUP}`
    WHERE standardized_question = @std_question
      AND standardized_answer IN UNNEST(@sources)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("std_question", "STRING", std_question),
            bigquery.ScalarQueryParameter("target", "STRING", target),
            bigquery.ArrayQueryParameter("sources", "STRING", sources),
        ]
    )
    job = client.query(cc_update, job_config=job_config)
    job.result()
    affected = job.num_dml_affected_rows or 0
    client.query(lookup_delete, job_config=job_config).result()
    _log_history(
        client,
        "lookup_merge_answers",
        affected,
        raw_question=std_question,
        raw_answer=",".join(sources),
        new_std_question=std_question,
        new_std_answer=target,
    )
    return affected


def delete_standardized_question(
    client, std_question: str, null_fact_rows: bool = True
) -> int:
    """Remove a standardized_question from the lookup.

    If null_fact_rows is True, also NULL the columnar_completed rows that
    pointed at it so they re-enter the unmapped queue. Respects existing UI
    overrides on rows already stamped 'streamlit_app' (those keep their value
    because a reviewer explicitly chose it). Returns the count of columnar rows
    nulled (0 if null_fact_rows=False)."""
    affected = 0
    if null_fact_rows:
        cc_update = f"""
        UPDATE `{COLUMNAR_COMPLETED}`
        SET standardized_question = NULL,
            standardized_answer = NULL,
            question_match_method = NULL,
            manually_assigned = TRUE,
            manually_assigned_at = CURRENT_TIMESTAMP(),
            manually_assigned_by = 'streamlit_app'
        WHERE standardized_question = @std_question
        """
        job_config_cc = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("std_question", "STRING", std_question),
            ]
        )
        job = client.query(cc_update, job_config=job_config_cc)
        job.result()
        affected = job.num_dml_affected_rows or 0

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("std_question", "STRING", std_question),
        ]
    )
    client.query(
        f"DELETE FROM `{ANSWERS_LOOKUP}` WHERE standardized_question = @std_question",
        job_config=job_config,
    ).result()
    client.query(
        f"DELETE FROM `{QUESTIONS_LOOKUP}` WHERE standardized_question = @std_question",
        job_config=job_config,
    ).result()
    _log_history(
        client,
        "lookup_delete_question",
        affected,
        raw_question=std_question,
    )
    return affected


def delete_standardized_answer(
    client, std_question: str, std_answer: str, null_fact_rows: bool = True
) -> int:
    """Remove a (std_question, std_answer) row from the lookup. If
    null_fact_rows is True, also NULL the columnar_completed.standardized_answer
    for the matching rows so they re-enter the unmapped queue."""
    affected = 0
    if null_fact_rows:
        cc_update = f"""
        UPDATE `{COLUMNAR_COMPLETED}`
        SET standardized_answer = NULL,
            answer_match_method = NULL,
            manually_assigned = TRUE,
            manually_assigned_at = CURRENT_TIMESTAMP(),
            manually_assigned_by = 'streamlit_app'
        WHERE standardized_question = @std_question
          AND standardized_answer = @std_answer
        """
        job_config_cc = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("std_question", "STRING", std_question),
                bigquery.ScalarQueryParameter("std_answer", "STRING", std_answer),
            ]
        )
        job = client.query(cc_update, job_config=job_config_cc)
        job.result()
        affected = job.num_dml_affected_rows or 0

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("std_question", "STRING", std_question),
            bigquery.ScalarQueryParameter("std_answer", "STRING", std_answer),
        ]
    )
    client.query(
        f"""DELETE FROM `{ANSWERS_LOOKUP}`
            WHERE standardized_question = @std_question
              AND standardized_answer = @std_answer""",
        job_config=job_config,
    ).result()
    _log_history(
        client,
        "lookup_delete_answer",
        affected,
        raw_question=std_question,
        raw_answer=std_answer,
    )
    return affected


@st.cache_data(ttl=300)
def get_raw_questions_under_std_q(_client, std_question: str) -> pd.DataFrame:
    """Distinct raw_question_en values currently mapped to a standardized_question,
    with row_count and survey_count -- used by the split UI's row picker.

    Cached: the underlying query scans columnar_completed (filtered by std_q),
    so each rerun would otherwise re-hit BQ. _client prefix tells Streamlit the
    client object is unhashable; cache keys on the std_question only."""
    query = f"""
    SELECT
      raw_question_en,
      COUNT(*) AS row_count,
      COUNT(DISTINCT survey_id) AS survey_count
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_question = @std_question
      AND raw_question_en IS NOT NULL
    GROUP BY raw_question_en
    ORDER BY row_count DESC
    """
    return _client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("std_question", "STRING", std_question),
            ]
        ),
    ).to_dataframe()


@st.cache_data(ttl=300)
def get_raw_answers_under_std_a(
    _client, std_question: str, std_answer: str
) -> pd.DataFrame:
    """Distinct raw_answer_en values currently mapped to (std_q, std_a). Cached."""
    query = f"""
    SELECT
      raw_answer_en,
      COUNT(*) AS row_count,
      COUNT(DISTINCT survey_id) AS survey_count
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_question = @std_question
      AND standardized_answer = @std_answer
      AND raw_answer_en IS NOT NULL
    GROUP BY raw_answer_en
    ORDER BY row_count DESC
    """
    return _client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("std_question", "STRING", std_question),
                bigquery.ScalarQueryParameter("std_answer", "STRING", std_answer),
            ]
        ),
    ).to_dataframe()


def _regex_match_raw_values(pattern: str, raw_values: list[str]) -> list[str]:
    """Apply a RE2-style regex to a list of raw values, in-process. No BigQuery.

    Used by both Split forms for live preview of what a pattern would match.
    BigQuery RE2 is a superset of Python's re for the common cases the user
    will write (literal text, character classes, alternation, repetition,
    inline flags like (?i)). For the final apply path the regex is NOT used
    server-side -- only the matched values are passed via IN UNNEST(), so the
    SQL apply is bit-exact regardless of any RE2-vs-re differences."""
    import re as _re

    if not pattern:
        return []
    try:
        compiled = _re.compile(pattern)
    except _re.error:
        return []
    return [v for v in raw_values if v is not None and compiled.search(v)]


def _gemini_suggest_split_regex(
    source: str, sample_raw_values: list[str], description: str
) -> tuple[str, str]:
    """Ask Gemini for a RE2 regex matching the user's natural-language description
    against the source canonical's raw values.

    Returns (regex, reason). On success, regex is non-empty and reason is "".
    On failure, regex is "" and reason describes WHY so the UI can show a
    specific message instead of the generic "check GEMINI_API_KEY" warning.

    Every failure is also logged to stderr so it lands in journalctl on the
    Linux box; the UI surfaces only the short reason.

    Result is cached per (source, description) in st.session_state for the
    session, so a repeated click with the same inputs does not re-hit Gemini."""
    if not description:
        return "", "Empty description."
    cache_key = ("split_regex_cache", source, description.strip())
    cached = st.session_state.get(cache_key)
    if cached is not None:
        return cached

    def _fail(reason: str) -> tuple[str, str]:
        print(f"[gemini_split_regex] FAILED: {reason}", file=sys.stderr, flush=True)
        result = ("", reason)
        st.session_state[cache_key] = result
        return result

    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        return _fail("GEMINI_API_KEY / GOOGLE_API_KEY env var not set on the server.")

    try:
        import google.generativeai as genai
    except ImportError as e:
        return _fail(f"google-generativeai package not installed: {e}")

    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-flash-lite-latest")
        sample = "\n".join(f"  - {v!r}" for v in sample_raw_values[:30])
        prompt = (
            "You write BigQuery RE2 regular expressions.\n"
            f"Source canonical: {source}\n"
            "Existing raw values (sample, up to 30):\n"
            f"{sample}\n\n"
            "The user wants to split out the subset matching this description:\n"
            f"  {description}\n\n"
            "Return ONLY a single RE2 regex string that matches the described "
            "subset against raw values like the sample above. Make it "
            "case-insensitive via a leading (?i). No code fences, no quotes, "
            "no explanation -- just the regex on one line."
        )
        resp = model.generate_content(prompt, generation_config={"temperature": 0.2})
    except Exception as e:
        return _fail(f"Gemini API call failed: {type(e).__name__}: {e}")

    text = (resp.text or "").strip()
    if not text:
        return _fail("Gemini returned an empty response.")

    # Strip common LLM clutter: code fences, leading "regex:" label, surrounding quotes.
    for fence in ("```regex", "```re2", "```python", "```"):
        text = text.replace(fence, "")
    text = text.strip()
    if text.lower().startswith("regex:"):
        text = text.split(":", 1)[1].strip()
    if (text.startswith('"') and text.endswith('"')) or (
        text.startswith("'") and text.endswith("'")
    ):
        text = text[1:-1]
    text = text.splitlines()[0].strip() if text else ""

    if not text:
        return _fail("Gemini response was empty after stripping clutter.")

    import re as _re

    try:
        _re.compile(text)
    except _re.error as e:
        return _fail(f"Gemini returned invalid regex {text!r}: {e}")

    result = (text, "")
    st.session_state[cache_key] = result
    return result


def get_lookup_drift_counts(client) -> dict:
    """Return the four-quadrant drift counts that power the Sync from Data
    panel. Eligibility filter matches sql/sync_standardized_lookups.sql."""
    free_text = list(FREE_TEXT_QUESTIONS)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("free_text_qs", "STRING", free_text),
        ]
    )
    query = f"""
    SELECT
      (SELECT COUNT(*) FROM (
        SELECT standardized_question FROM `{QUESTIONS_LOOKUP}`
        EXCEPT DISTINCT
        SELECT DISTINCT standardized_question FROM `{COLUMNAR_COMPLETED}`
        WHERE standardized_question IS NOT NULL
          AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
      )) AS lookup_q_unused,
      (SELECT COUNT(*) FROM (
        SELECT standardized_question, standardized_answer FROM `{ANSWERS_LOOKUP}`
        EXCEPT DISTINCT
        SELECT DISTINCT standardized_question, standardized_answer FROM `{COLUMNAR_COMPLETED}`
        WHERE standardized_question IS NOT NULL
          AND standardized_answer IS NOT NULL
          AND TRIM(standardized_answer) != ''
          AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
      )) AS lookup_a_unused,
      (SELECT COUNT(*) FROM (
        SELECT DISTINCT standardized_question FROM `{COLUMNAR_COMPLETED}`
        WHERE standardized_question IS NOT NULL
          AND TRIM(standardized_question) != ''
          AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
        EXCEPT DISTINCT
        SELECT standardized_question FROM `{QUESTIONS_LOOKUP}`
      )) AS data_q_missing,
      (SELECT COUNT(*) FROM (
        SELECT DISTINCT standardized_question, standardized_answer FROM `{COLUMNAR_COMPLETED}`
        WHERE standardized_question IS NOT NULL
          AND standardized_answer IS NOT NULL
          AND TRIM(standardized_answer) != ''
          AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
        EXCEPT DISTINCT
        SELECT standardized_question, standardized_answer FROM `{ANSWERS_LOOKUP}`
      )) AS data_a_missing
    """
    for row in client.query(query, job_config=job_config):
        return {
            "lookup_q_unused": row.lookup_q_unused,
            "lookup_a_unused": row.lookup_a_unused,
            "data_q_missing": row.data_q_missing,
            "data_a_missing": row.data_a_missing,
        }
    return {
        "lookup_q_unused": 0,
        "lookup_a_unused": 0,
        "data_q_missing": 0,
        "data_a_missing": 0,
    }


def get_unused_standardized_questions(client) -> pd.DataFrame:
    """Lookup rows whose standardized_question has zero rows in columnar_completed.
    Used as the picklist for the prune panel."""
    free_text = list(FREE_TEXT_QUESTIONS)
    query = f"""
    SELECT l.standardized_question, l.display_name, l.category, l.description
    FROM `{QUESTIONS_LOOKUP}` l
    LEFT JOIN (
      SELECT DISTINCT standardized_question FROM `{COLUMNAR_COMPLETED}`
      WHERE standardized_question IS NOT NULL
        AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
    ) cc USING (standardized_question)
    WHERE cc.standardized_question IS NULL
    ORDER BY l.standardized_question
    """
    return client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("free_text_qs", "STRING", free_text),
            ]
        ),
    ).to_dataframe()


def get_unused_standardized_answers(client) -> pd.DataFrame:
    """Lookup rows whose (std_q, std_a) pair has zero rows in columnar_completed."""
    free_text = list(FREE_TEXT_QUESTIONS)
    query = f"""
    SELECT l.standardized_question, l.standardized_answer, l.display_name, l.sort_order
    FROM `{ANSWERS_LOOKUP}` l
    LEFT JOIN (
      SELECT DISTINCT standardized_question, standardized_answer
      FROM `{COLUMNAR_COMPLETED}`
      WHERE standardized_question IS NOT NULL
        AND standardized_answer IS NOT NULL
        AND TRIM(standardized_answer) != ''
        AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
    ) cc USING (standardized_question, standardized_answer)
    WHERE cc.standardized_question IS NULL
    ORDER BY l.standardized_question, l.standardized_answer
    """
    return client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("free_text_qs", "STRING", free_text),
            ]
        ),
    ).to_dataframe()


def force_sync_missing_to_lookup(client) -> tuple[int, int]:
    """Run the same MERGEs that sql/sync_standardized_lookups.sql runs nightly,
    on demand. Returns (questions_added, answers_added)."""
    before = get_lookup_drift_counts(client)
    sql_path = (
        Path(__file__).parent.parent.parent / "sql" / "sync_standardized_lookups.sql"
    )
    sql = sql_path.read_text(encoding="utf-8")
    client.query(sql).result()
    after = get_lookup_drift_counts(client)
    return (
        max(0, before["data_q_missing"] - after["data_q_missing"]),
        max(0, before["data_a_missing"] - after["data_a_missing"]),
    )


def _is_valid_snake_case(name: str) -> bool:
    """[a-z][a-z0-9_]* gate for new canonical names."""
    import re

    return bool(re.fullmatch(r"[a-z][a-z0-9_]*", name or ""))


def _near_duplicate_canonicals(
    name: str, existing: list[str], threshold: int = 85
) -> list[tuple[str, int]]:
    """Return existing canonicals that fuzz-match >= threshold against name.
    Warning-only -- the UI lets the user proceed past a near-dup hit."""
    if not name or not existing:
        return []
    matches = process.extract(name, existing, scorer=fuzz.ratio, limit=5)
    return [(m, s) for m, s, _ in matches if s >= threshold and m != name]


def _validate_canonical_name_inline(
    name: str, existing: list[str], require_snake_case: bool = True
) -> bool:
    """Shared inline validator for every place a reviewer enters a new canonical
    name (Add Question / Add Answer / Create new on detail panel / Rename / Split).

    Renders:
      - a red error if snake_case is required and the name fails the pattern
      - a yellow warning if a near-duplicate (fuzz >= 85) exists in `existing`

    Returns True if the name is safe to submit (no error -- warnings do not block).
    Pass require_snake_case=False for answer values, which legitimately include
    things like 'N/A', 'Yes', 'Prefer not to answer'."""
    if not name:
        return False
    if require_snake_case and not _is_valid_snake_case(name):
        st.error(
            "Name must match [a-z][a-z0-9_]* -- lowercase, start with a letter, "
            "no spaces or punctuation."
        )
        return False
    nd = _near_duplicate_canonicals(name, existing)
    if nd:
        st.warning(
            "Near-duplicate canonical(s) detected: "
            + ", ".join(f"`{m}` ({s})" for m, s in nd)
            + " -- consider merging into one of these instead of creating a new entry."
        )
    return True


def _log_history(
    client,
    action: str,
    affected_rows: int,
    raw_question: str = None,
    raw_answer: str = None,
    new_std_question: str = None,
    new_std_answer: str = None,
    batch_id: str = None,
) -> None:
    """Append a row to mapping_history for auditability + Undo support. Best-effort.

    created_by is the reviewer's IAP-authenticated email (stashed in
    st.session_state by _require_authorized_user). The fact-table sentinel
    'streamlit_app' on manually_assigned_by is NOT changed -- that string is
    a contract the analyst Sheet's Pass A checks to skip UI-locked rows
    (see sql/standardize_lime_survey_answers.sql). Per-reviewer attribution
    lives in mapping_history; the sentinel stays sentinel.
    """
    created_by = st.session_state.get("reviewer_email") or "streamlit_app"
    try:
        client.query(
            f"""
            INSERT INTO `{MAPPING_HISTORY}`
              (action, raw_question, raw_answer,
               new_standardized_question, new_standardized_answer,
               affected_rows, created_by, batch_id)
            VALUES (@action, @raw_question, @raw_answer,
                    @new_std_q, @new_std_a, @affected_rows, @created_by, @batch_id)
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("action", "STRING", action),
                    bigquery.ScalarQueryParameter(
                        "raw_question", "STRING", raw_question
                    ),
                    bigquery.ScalarQueryParameter("raw_answer", "STRING", raw_answer),
                    bigquery.ScalarQueryParameter(
                        "new_std_q", "STRING", new_std_question
                    ),
                    bigquery.ScalarQueryParameter(
                        "new_std_a", "STRING", new_std_answer
                    ),
                    bigquery.ScalarQueryParameter(
                        "affected_rows", "INT64", affected_rows
                    ),
                    bigquery.ScalarQueryParameter("created_by", "STRING", created_by),
                    bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id),
                ]
            ),
        ).result()
    except Exception as e:
        # History logging must never block the mapping itself, but silent failures
        # make Undo unsafe. Surface a warning so problems are visible.
        st.warning(
            f"⚠️ Mapping succeeded but history log failed (Undo may not work for this row): {e}"
        )


def add_question_mapping(
    client,
    raw_question: str,
    standardized_question: str,
    match_type: str = "manual",
    auto_assigned: bool = False,
    batch_id: str = None,
) -> bool:
    """
    Apply a (raw_question -> standardized_question) decision to columnar_completed.

    columnar_completed is the single source of truth. The question_mapping view
    derives from it (rows where manually_assigned=TRUE). The UI never writes
    elsewhere.

    Overwrite policy: applies to rows where standardized_question IS NULL OR
    manually_assigned IS NOT TRUE. Never overwrites another human's decision.
    """
    is_manual = match_type == "manual"
    apply_query = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_question = @standardized_question,
        question_match_method = @match_type,
        manually_assigned = CASE WHEN @is_manual THEN TRUE ELSE manually_assigned END,
        manually_assigned_at = CASE WHEN @is_manual THEN CURRENT_TIMESTAMP() ELSE manually_assigned_at END,
        manually_assigned_by = CASE WHEN @is_manual THEN 'streamlit_app' ELSE manually_assigned_by END
    WHERE TRIM(REGEXP_REPLACE(raw_question_en, r'\\s+', ' ')) = TRIM(REGEXP_REPLACE(@raw_question, r'\\s+', ' '))
      AND (standardized_question IS NULL OR COALESCE(manually_assigned, FALSE) = FALSE)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("raw_question", "STRING", raw_question),
            bigquery.ScalarQueryParameter(
                "standardized_question", "STRING", standardized_question
            ),
            bigquery.ScalarQueryParameter("match_type", "STRING", match_type),
            bigquery.ScalarQueryParameter("is_manual", "BOOL", is_manual),
        ]
    )
    try:
        job = client.query(apply_query, job_config=job_config)
        job.result()
        affected = job.num_dml_affected_rows or 0
        _log_history(
            client,
            "question_mapping",
            affected,
            raw_question=raw_question,
            new_std_question=standardized_question,
            batch_id=batch_id,
        )
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False


def add_answer_mapping(
    client,
    std_question: str,
    raw_answer: str,
    standardized_answer: str,
    match_type: str = "manual",
    auto_assigned: bool = False,
    batch_id: str = None,
    show_spinner: bool = True,
) -> bool:
    """
    Apply a (std_question, raw_answer -> standardized_answer) decision to columnar_completed.

    Same overwrite policy as add_question_mapping: never overwrite another manual decision.
    Shows a spinner during the BigQuery UPDATE (it scans the large columnar table).
    Callers already inside their own spinner pass show_spinner=False.
    """
    import contextlib

    is_manual = match_type == "manual"
    apply_query = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_answer = @standardized_answer,
        manually_assigned = CASE WHEN @is_manual THEN TRUE ELSE manually_assigned END,
        manually_assigned_at = CASE WHEN @is_manual THEN CURRENT_TIMESTAMP() ELSE manually_assigned_at END,
        manually_assigned_by = CASE WHEN @is_manual THEN 'streamlit_app' ELSE manually_assigned_by END
    WHERE standardized_question = @std_question
      AND (TRIM(raw_answer_en) = TRIM(@raw_answer) OR TRIM(answer_value) = TRIM(@raw_answer))
      AND (standardized_answer IS NULL OR COALESCE(manually_assigned, FALSE) = FALSE)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("std_question", "STRING", std_question),
            bigquery.ScalarQueryParameter("raw_answer", "STRING", raw_answer),
            bigquery.ScalarQueryParameter(
                "standardized_answer", "STRING", standardized_answer
            ),
            bigquery.ScalarQueryParameter("is_manual", "BOOL", is_manual),
        ]
    )
    spinner_ctx = (
        st.spinner(f"Mapping answer to '{standardized_answer}'... (updating BigQuery)")
        if show_spinner
        else contextlib.nullcontext()
    )
    try:
        with spinner_ctx:
            job = client.query(apply_query, job_config=job_config)
            job.result()
            affected = job.num_dml_affected_rows or 0
            _log_history(
                client,
                "answer_mapping",
                affected,
                raw_answer=raw_answer,
                new_std_answer=standardized_answer,
                new_std_question=std_question,
                batch_id=batch_id,
            )
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False


def undo_last_mapping(client) -> tuple[bool, str]:
    """Reverse the most recent non-undone mapping_history entry.

    Returns (ok, message). Rolls back the columnar_completed row(s) the original
    UPDATE touched (matched by raw_question/answer + standardized_* + manually_assigned_by):
    - standardized_question/answer -> NULL
    - manually_assigned -> FALSE (so ETL can retry)
    - manually_assigned_at/by -> NULL
    - question_match_method -> NULL
    - mapping_history.undone -> TRUE
    """
    last_query = f"""
    SELECT history_id, action, raw_question, raw_answer,
           new_standardized_question, new_standardized_answer, affected_rows, batch_id
    FROM `{MAPPING_HISTORY}`
    WHERE NOT undone
    ORDER BY event_time DESC
    LIMIT 1
    """
    rows = list(client.query(last_query))
    if not rows:
        return False, "Nothing to undo — mapping history is empty."

    h = rows[0]

    try:
        if h.action == "question_mapping":
            # Revert columnar_completed rows back to the "needs review" state.
            # manually_assigned=FALSE (not NULL) so the ETL will retry classification.
            client.query(
                f"""
                UPDATE `{COLUMNAR_COMPLETED}`
                SET standardized_question = NULL,
                    question_match_method = NULL,
                    manually_assigned = FALSE,
                    manually_assigned_at = NULL,
                    manually_assigned_by = NULL
                WHERE TRIM(REGEXP_REPLACE(raw_question_en, r'\\s+', ' ')) = TRIM(REGEXP_REPLACE(@raw_question, r'\\s+', ' '))
                  AND standardized_question = @std_q
                  AND manually_assigned = TRUE
                  AND manually_assigned_by = 'streamlit_app'
            """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "raw_question", "STRING", h.raw_question
                        ),
                        bigquery.ScalarQueryParameter(
                            "std_q", "STRING", h.new_standardized_question
                        ),
                    ]
                ),
            ).result()
            msg = f"Undid: '{h.raw_question[:50]}...' -> '{h.new_standardized_question}' ({h.affected_rows} rows reverted)"

        elif h.action == "answer_mapping":
            client.query(
                f"""
                UPDATE `{COLUMNAR_COMPLETED}`
                SET standardized_answer = NULL,
                    manually_assigned = FALSE,
                    manually_assigned_at = NULL,
                    manually_assigned_by = NULL
                WHERE standardized_question = @std_q
                  AND (TRIM(raw_answer_en) = TRIM(@raw_answer) OR TRIM(answer_value) = TRIM(@raw_answer))
                  AND standardized_answer = @std_a
                  AND manually_assigned = TRUE
                  AND manually_assigned_by = 'streamlit_app'
            """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "std_q", "STRING", h.new_standardized_question
                        ),
                        bigquery.ScalarQueryParameter(
                            "raw_answer", "STRING", h.raw_answer
                        ),
                        bigquery.ScalarQueryParameter(
                            "std_a", "STRING", h.new_standardized_answer
                        ),
                    ]
                ),
            ).result()
            msg = f"Undid answer: '{h.raw_answer[:40]}...' -> '{h.new_standardized_answer}' ({h.affected_rows} rows reverted)"
        else:
            return False, f"Unknown action type: {h.action}"

        # Mark history row as undone
        client.query(
            f"""
            UPDATE `{MAPPING_HISTORY}`
            SET undone = TRUE, undone_at = CURRENT_TIMESTAMP()
            WHERE history_id = @hid
        """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("hid", "STRING", h.history_id),
                ]
            ),
        ).result()

        return True, msg
    except Exception as e:
        return False, f"Error during undo: {e}"


def _undo_mapping_history_row(client, history_id: str) -> tuple[bool, str]:
    """Reverse a SPECIFIC mapping_history row (not just the latest).

    Powers the per-row undo on the recent-mappings sidebar. Reuses the same
    revert semantics as undo_last_mapping: NULL the standardized columns on
    matching columnar_completed rows, drop manually_assigned back to FALSE so
    the ETL can retry, mark the history row as undone.

    Only handles 'question_mapping' and 'answer_mapping' actions today. The
    PR 2 lookup actions (rename/split/merge/delete) are not auto-revertible
    from this path -- the helper returns a clear message so the user knows
    to use the Audit page (or to manually reverse via the Edit/Split/Merge
    tab) for those."""
    row_query = f"""
    SELECT history_id, action, raw_question, raw_answer,
           new_standardized_question, new_standardized_answer, affected_rows, undone
    FROM `{MAPPING_HISTORY}`
    WHERE history_id = @hid
    """
    rows = list(
        client.query(
            row_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("hid", "STRING", history_id),
                ]
            ),
        )
    )
    if not rows:
        return False, "History row not found."
    h = rows[0]
    if h.undone:
        return False, "Already undone."

    try:
        if h.action == "question_mapping":
            client.query(
                f"""
                UPDATE `{COLUMNAR_COMPLETED}`
                SET standardized_question = NULL,
                    question_match_method = NULL,
                    manually_assigned = FALSE,
                    manually_assigned_at = NULL,
                    manually_assigned_by = NULL
                WHERE TRIM(REGEXP_REPLACE(raw_question_en, r'\\s+', ' ')) = TRIM(REGEXP_REPLACE(@raw_question, r'\\s+', ' '))
                  AND standardized_question = @std_q
                  AND manually_assigned = TRUE
                  AND manually_assigned_by = 'streamlit_app'
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "raw_question", "STRING", h.raw_question
                        ),
                        bigquery.ScalarQueryParameter(
                            "std_q", "STRING", h.new_standardized_question
                        ),
                    ]
                ),
            ).result()
            msg = f"Undid: '{(h.raw_question or '')[:40]}...' -> '{h.new_standardized_question}'"
        elif h.action == "answer_mapping":
            client.query(
                f"""
                UPDATE `{COLUMNAR_COMPLETED}`
                SET standardized_answer = NULL,
                    manually_assigned = FALSE,
                    manually_assigned_at = NULL,
                    manually_assigned_by = NULL
                WHERE standardized_question = @std_q
                  AND (TRIM(raw_answer_en) = TRIM(@raw_answer) OR TRIM(answer_value) = TRIM(@raw_answer))
                  AND standardized_answer = @std_a
                  AND manually_assigned = TRUE
                  AND manually_assigned_by = 'streamlit_app'
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "std_q", "STRING", h.new_standardized_question
                        ),
                        bigquery.ScalarQueryParameter(
                            "raw_answer", "STRING", h.raw_answer
                        ),
                        bigquery.ScalarQueryParameter(
                            "std_a", "STRING", h.new_standardized_answer
                        ),
                    ]
                ),
            ).result()
            msg = f"Undid answer: '{(h.raw_answer or '')[:30]}...' -> '{h.new_standardized_answer}'"
        else:
            return (
                False,
                f"Cannot auto-undo '{h.action}' from sidebar. "
                f"Use the Audit page or reverse via Edit/Split/Merge.",
            )

        client.query(
            f"""
            UPDATE `{MAPPING_HISTORY}`
            SET undone = TRUE, undone_at = CURRENT_TIMESTAMP()
            WHERE history_id = @hid
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("hid", "STRING", history_id),
                ]
            ),
        ).result()
        return True, msg
    except Exception as e:
        return False, f"Error during undo: {e}"


def undo_last_batch(client) -> tuple[bool, str]:
    """Reverse the most recent bulk operation (shared batch_id)."""
    last_batch_query = f"""
    SELECT batch_id
    FROM `{MAPPING_HISTORY}`
    WHERE NOT undone AND batch_id IS NOT NULL
    ORDER BY event_time DESC
    LIMIT 1
    """
    rows = list(client.query(last_batch_query))
    if not rows:
        return False, "No bulk operations to undo."
    batch_id = rows[0].batch_id

    batch_rows_query = f"""
    SELECT history_id, action, raw_question, raw_answer,
           new_standardized_question, new_standardized_answer
    FROM `{MAPPING_HISTORY}`
    WHERE batch_id = @bid AND NOT undone
    ORDER BY event_time DESC
    """
    entries = list(
        client.query(
            batch_rows_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("bid", "STRING", batch_id)
                ]
            ),
        )
    )

    reverted = 0
    for h in entries:
        try:
            if h.action == "question_mapping":
                client.query(
                    f"""
                    UPDATE `{COLUMNAR_COMPLETED}`
                    SET standardized_question = NULL,
                        question_match_method = NULL,
                        manually_assigned = FALSE,
                        manually_assigned_at = NULL,
                        manually_assigned_by = NULL
                    WHERE TRIM(REGEXP_REPLACE(raw_question_en, r'\\s+', ' ')) = TRIM(REGEXP_REPLACE(@rq, r'\\s+', ' '))
                      AND standardized_question = @sq
                      AND manually_assigned = TRUE
                      AND manually_assigned_by = 'streamlit_app'
                """,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter(
                                "rq", "STRING", h.raw_question
                            ),
                            bigquery.ScalarQueryParameter(
                                "sq", "STRING", h.new_standardized_question
                            ),
                        ]
                    ),
                ).result()
            elif h.action == "answer_mapping":
                client.query(
                    f"""
                    UPDATE `{COLUMNAR_COMPLETED}`
                    SET standardized_answer = NULL,
                        manually_assigned = FALSE,
                        manually_assigned_at = NULL,
                        manually_assigned_by = NULL
                    WHERE standardized_question = @sq
                      AND (TRIM(raw_answer_en) = TRIM(@ra) OR TRIM(answer_value) = TRIM(@ra))
                      AND standardized_answer = @sa
                      AND manually_assigned = TRUE
                      AND manually_assigned_by = 'streamlit_app'
                """,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter(
                                "sq", "STRING", h.new_standardized_question
                            ),
                            bigquery.ScalarQueryParameter("ra", "STRING", h.raw_answer),
                            bigquery.ScalarQueryParameter(
                                "sa", "STRING", h.new_standardized_answer
                            ),
                        ]
                    ),
                ).result()
            reverted += 1
        except Exception as e:
            return False, f"Undo batch failed on row {h.history_id}: {e}"

    client.query(
        f"""
        UPDATE `{MAPPING_HISTORY}`
        SET undone = TRUE, undone_at = CURRENT_TIMESTAMP()
        WHERE batch_id = @bid AND NOT undone
    """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("bid", "STRING", batch_id)]
        ),
    ).result()

    return True, f"Undid batch {batch_id}: {reverted} mapping(s) reverted."


def update_columnar_auto_assigned(
    client,
    raw_question: str = None,
    raw_answer: str = None,
    standardized_question: str = None,
    auto_assigned: bool = True,
) -> bool:
    """
    Update auto_assigned flag in columnar_completed for rows matching the criteria.

    Args:
        client: BigQuery client
        raw_question: Filter by raw_question_en (for question mappings)
        raw_answer: Filter by raw_answer_en (for answer mappings)
        standardized_question: Filter by standardized_question (for answer mappings)
        auto_assigned: Value to set (TRUE for auto, FALSE for manual)

    Returns:
        True if successful, False otherwise
    """
    # Build WHERE clause based on provided parameters
    conditions = []
    params = []

    if raw_question:
        conditions.append("raw_question_en = @raw_question")
        params.append(
            bigquery.ScalarQueryParameter("raw_question", "STRING", raw_question)
        )

    if raw_answer and standardized_question:
        conditions.append("raw_answer_en = @raw_answer")
        conditions.append("standardized_question = @std_question")
        params.append(bigquery.ScalarQueryParameter("raw_answer", "STRING", raw_answer))
        params.append(
            bigquery.ScalarQueryParameter(
                "std_question", "STRING", standardized_question
            )
        )

    if not conditions:
        st.error("Must provide raw_question or (raw_answer + standardized_question)")
        return False

    where_clause = " AND ".join(conditions)

    query = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET auto_assigned = @auto_assigned
    WHERE {where_clause}
    """

    params.append(bigquery.ScalarQueryParameter("auto_assigned", "BOOL", auto_assigned))

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        job = client.query(query, job_config=job_config)
        job.result()
        rows_updated = job.num_dml_affected_rows or 0
        # Don't show message here, let calling function handle it
        return True
    except Exception as e:
        st.error(f"Error updating auto_assigned flag: {e}")
        return False


# ============================================================================
# MAIN APP
# ============================================================================


def _render_recent_mappings_sidebar(client) -> None:
    """Sidebar expander showing the last N mappings from THIS session.

    Reads mapping_history filtered by THIS reviewer's email and
    event_time >= session_start. Each row shows a short summary and an
    Undo button that reverses just that row (not just the latest).

    Self-contained: silently no-ops if the query fails. Does not cache --
    fast small query, and we want it fresh after every write."""
    try:
        session_start = st.session_state.get("session_start")
        reviewer_email = st.session_state.get("reviewer_email")
        if session_start is None or not reviewer_email:
            return
        # Match either the per-reviewer email (post identity rollout) OR the
        # legacy 'streamlit_app' sentinel (rows written before this change).
        query = f"""
        SELECT history_id, event_time, action,
               raw_question, raw_answer,
               new_standardized_question, new_standardized_answer,
               affected_rows, undone
        FROM `{MAPPING_HISTORY}`
        WHERE created_by IN (@reviewer_email, 'streamlit_app')
          AND event_time >= @session_start
        ORDER BY event_time DESC
        LIMIT 10
        """
        df = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "session_start", "TIMESTAMP", session_start
                    ),
                    bigquery.ScalarQueryParameter(
                        "reviewer_email", "STRING", reviewer_email
                    ),
                ]
            ),
        ).to_dataframe()
    except Exception:
        return

    with st.sidebar.expander(f"Recent mappings ({len(df)})", expanded=False):
        if df.empty:
            st.caption("No mappings this session yet.")
            return
        for _, h in df.iterrows():
            action = h.get("action") or "?"
            if action == "question_mapping":
                summary = f"Q: {(h['raw_question'] or '')[:30]} -> {h['new_standardized_question']}"
            elif action == "answer_mapping":
                summary = f"A: {(h['raw_answer'] or '')[:30]} -> {h['new_standardized_answer']}"
            else:
                summary = (
                    f"{action}: {(h['raw_question'] or h['raw_answer'] or '')[:40]}"
                )
            if h.get("undone"):
                st.caption(f"~~{summary}~~ (undone)")
                continue
            cols = st.columns([5, 2])
            cols[0].caption(summary)
            if cols[1].button(
                "Undo", key=f"recent_undo_{h['history_id']}", use_container_width=True
            ):
                ok, msg = _undo_mapping_history_row(client, h["history_id"])
                if ok:
                    st.cache_data.clear()
                    st.toast(msg)
                else:
                    st.toast(msg, icon="!")
                st.rerun()


NAV_PAGES = [
    "Dashboard",
    "Unmapped Questions",
    "Unmapped Answers",
    "Audit",
    "Lookup Tables",
    "Site Mapping",
    "Users",
    "Model",
]

# Bootstrap-icon names paired 1:1 with NAV_PAGES. streamlit-option-menu pulls
# these from the Bootstrap Icons set bundled with the component (full list:
# https://icons.getbootstrap.com/). Names use kebab-case without the 'bi-'
# prefix.
NAV_ICONS = [
    "speedometer2",  # Dashboard
    "question-circle",  # Unmapped Questions
    "search",  # Unmapped Answers
    "journal-text",  # Audit
    "table",  # Lookup Tables
    "geo-alt",  # Site Mapping
    "people",  # Users
    "cpu",  # Model
]


def _render_sidebar_nav() -> str:
    """Render the sidebar navigation using streamlit-option-menu.

    Replaces the previous button-based nav with a proper option-menu widget
    that has built-in Bootstrap icons, consistent two-column layout (icon |
    label), subtle hover/active states, and a quieter visual weight than
    Streamlit's stock buttons.

    State is held in st.session_state['page'] -- the rest of main() does
    not change shape. Component falls back to plain text buttons if the
    library is missing (e.g. a dev box where the dependency isn't installed
    yet), so the app keeps working during deployment lag."""
    if "page" not in st.session_state:
        st.session_state["page"] = NAV_PAGES[0]

    try:
        from streamlit_option_menu import option_menu
    except ImportError:
        # Fallback: plain buttons. Lets the UI keep working if the lib hasn't
        # been installed yet on a particular host.
        st.sidebar.markdown("##### NAVIGATION")
        for label in NAV_PAGES:
            active = st.session_state["page"] == label
            if st.sidebar.button(
                label,
                key=f"nav_{label}",
                use_container_width=True,
                type="primary" if active else "secondary",
            ):
                st.session_state["page"] = label
                st.rerun()
        st.sidebar.markdown("---")
        return st.session_state["page"]

    with st.sidebar:
        # Force the selected row's icon to match the active text color.
        # streamlit-option-menu's `icon` style applies to ALL rows uniformly;
        # there's no per-state config. This CSS hits the selected row's
        # <i class="bi-..."> element specifically.
        st.markdown(
            """
            <style>
            section[data-testid="stSidebar"] .nav-link.active i {
                color: #1d4ed8 !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        selected = option_menu(
            menu_title="Navigation",
            options=NAV_PAGES,
            icons=NAV_ICONS,
            menu_icon="list",
            default_index=NAV_PAGES.index(st.session_state["page"]),
            styles={
                "container": {
                    "padding": "0.5rem 0",
                    "background-color": "transparent",
                },
                "icon": {"color": "#6b7280", "font-size": "16px"},
                "nav-link": {
                    "font-size": "14px",
                    "text-align": "left",
                    "margin": "2px 0",
                    "padding": "8px 12px",
                    "color": "#1f2937",
                    "background-color": "transparent",
                    "border-radius": "6px",
                },
                "nav-link-selected": {
                    # Stronger tint and matching darker icon/text so the active
                    # row reads clearly against the gray sidebar background.
                    # Previous #eff6ff (blue-50) was too close to the sidebar
                    # color to register as "selected" at a glance.
                    "background-color": "#dbeafe",  # blue-100
                    "color": "#1d4ed8",  # blue-700
                    "font-weight": "600",
                    "border-left": "4px solid #1d4ed8",
                },
                "menu-title": {
                    "font-size": "11px",
                    "font-weight": "600",
                    "text-transform": "uppercase",
                    "letter-spacing": "0.5px",
                    "color": "#6b7280",
                    "padding-bottom": "0.5rem",
                },
            },
            key="sidebar_nav_menu",
        )
    if selected != st.session_state["page"]:
        st.session_state["page"] = selected
        st.rerun()
    st.sidebar.markdown("---")
    return st.session_state["page"]


def main():
    st.set_page_config(
        page_title="LimeSurvey Mapping Manager", page_icon="📊", layout="wide"
    )

    st.title("LimeSurvey Standardization Manager v2 🔍")

    # BQ client must come first because the auth check queries mapper_users.
    # get_bq_client also creates+seeds the users table on first run.
    client = get_bq_client()

    # Identify the caller via IAP and enforce the mapper_users allowlist
    # BEFORE any page renders. Unauthorized callers see st.error and the
    # app halts on st.stop() inside the helper.
    user = _require_authorized_user(client)

    # Signed-in badge in the sidebar so reviewers can see whose name is on
    # the writes they're about to make. Prefer display_name when set.
    behind_iap = st.session_state.get("behind_iap", False)
    label = user["display_name"] or user["email"]
    role_suffix = f" ({user['role']})" if user["role"] == "admin" else ""
    badge = f"Signed in as: **{label}**{role_suffix}  \n_{user['email']}_"
    if not behind_iap:
        badge += "  \n_(local dev -- IAP header not present)_"
    st.sidebar.info(badge)

    # Sidebar navigation. Mappings now auto-apply to columnar_completed on save,
    # so there's no separate "Apply Changes" page. Use shared/apply_mapping_changes.py for backfills.
    page = _render_sidebar_nav()

    # Usage log: emit one page_view row per actual page switch (not per
    # widget rerun). Best-effort; failures here never block render.
    _log_page_view_if_changed(client, user["email"], page)

    # Recent-mappings sidebar widget: shows last 10 mappings made in THIS
    # browser session, with one-click undo. session_start is stamped on first
    # load; undo uses _revert_mapping_history_row to undo any row (not just
    # the most recent), which the existing "Undo last" affordance can't do.
    if "session_start" not in st.session_state:
        st.session_state["session_start"] = datetime.utcnow()
    _render_recent_mappings_sidebar(client)

    if page == "Dashboard":
        show_dashboard(client)
    elif page == "Unmapped Questions":
        show_unmapped_questions(client)
    elif page == "Unmapped Answers":
        show_unmapped_answers(client)
    elif page == "Audit":
        show_audit(client)
    elif page == "Lookup Tables":
        show_lookup_tables(client)
    elif page == "Site Mapping":
        show_site_mapping(client)
    elif page == "Users":
        show_users(client)
    elif page == "Model":
        show_model(client)


def show_dashboard(client):
    """Show overview statistics."""
    st.header("Dashboard")

    stats = get_coverage_stats(client)
    total = max(1, stats["total_rows"])
    q_pct = 100 * stats["has_std_question"] / total
    a_pct = 100 * stats["has_std_answer"] / total

    # KPI strip: six equal-width bordered cards on one row. Each card combines
    # the primary number with one line of supporting context underneath.
    kpis = [
        (
            "Total Rows",
            f"{stats['total_rows']:,}",
            f"rows in columnar_completed",
            None,
        ),
        (
            "Std Question Coverage",
            f"{q_pct:.1f}%",
            f"{stats['has_std_question']:,} of {stats['total_rows']:,}",
            None,
        ),
        (
            "Std Answer Coverage",
            f"{a_pct:.1f}%",
            f"{stats['has_std_answer']:,} of {stats['total_rows']:,}",
            None,
        ),
        (
            "Unmapped Questions",
            f"{stats['unmapped_questions']:,}",
            f"rows awaiting review",
            None,
        ),
        (
            "Unmapped Answers",
            f"{stats['unmapped_answers']:,}",
            f"actionable in queue",
            None,
        ),
        (
            "Manually Locked",
            f"{stats['manual_count']:,}",
            f"protected from ETL",
            "Rows with manually_assigned=TRUE. The ETL will never overwrite these.",
        ),
    ]
    cols = st.columns(len(kpis))
    for col, (label, value, sub, help_text) in zip(cols, kpis):
        with col:
            with st.container(border=True):
                st.caption(label.upper())
                st.markdown(
                    f"<div style='font-size:2.0rem;font-weight:700;line-height:1.1'>{value}</div>",
                    unsafe_allow_html=True,
                )
                if help_text:
                    st.caption(f"{sub}", help=help_text)
                else:
                    st.caption(sub)

    st.divider()

    # Quick stats on lookup tables
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Standardized Questions by Category")
        df = get_standardized_questions(client)
        if not df.empty:
            cat_counts = df["category"].value_counts()
            st.bar_chart(cat_counts)

    with col2:
        st.subheader("Top 10 Unmapped Questions")
        unmapped = get_unmapped_questions(client)
        if not unmapped.empty:
            st.dataframe(
                unmapped.head(10)[["raw_question_en", "row_count"]],
                hide_index=True,
                width="stretch",
            )
        else:
            st.success("All questions are mapped!")


def _stable_key(raw_question: str) -> str:
    """Hash-based Streamlit widget key that's stable across reruns even when row indices shift."""
    import hashlib

    return hashlib.md5(raw_question.encode("utf-8")).hexdigest()[:12]


# Words that add no signal to a snake_case standardized-question name.
_NAME_STOPWORDS = {
    "the",
    "a",
    "an",
    "of",
    "to",
    "in",
    "on",
    "for",
    "with",
    "your",
    "you",
    "do",
    "does",
    "did",
    "is",
    "are",
    "was",
    "were",
    "have",
    "has",
    "had",
    "what",
    "which",
    "how",
    "when",
    "where",
    "who",
    "why",
    "or",
    "and",
    "if",
    "please",
    "select",
    "all",
    "that",
    "apply",
    "their",
    "they",
    "them",
    "this",
    "these",
    "those",
    "be",
    "been",
    "any",
    "from",
    "by",
    "as",
    "at",
    "person",
    "care",
    "currently",
    "most",
    "best",
    "describes",
    "following",
}


def _heuristic_snake_case_candidates(
    raw_question: str, limit: int = 3, exclude: set[str] | None = None
) -> list[str]:
    """Local fallback: derive snake_case name candidates from the question text.

    No LLM. Tokenizes, drops stopwords/punctuation, and assembles a wide set of
    word-window variants. Deterministic; used when Gemini is unavailable. The
    `exclude` set lets the caller request "different" candidates on a re-roll.
    """
    import re as _re

    exclude = exclude or set()
    words = [
        w
        for w in _re.findall(r"[a-z0-9]+", raw_question.lower())
        if w not in _NAME_STOPWORDS and len(w) > 1
    ]
    if not words:
        return []

    # Generate many variants by varying start index AND window length, so a
    # re-roll always has fresh options to draw from.
    variants: list[str] = []
    for start in range(max(1, len(words))):
        for length in (3, 4, 2, 5):
            chunk = words[start : start + length]
            if 2 <= len(chunk) <= 5:
                variants.append("_".join(chunk))

    # Dedup preserving order, then skip anything already shown.
    seen, out = set(), []
    for v in variants:
        if v and v not in seen and v not in exclude:
            seen.add(v)
            out.append(v)
            if len(out) >= limit:
                break
    return out


def _generate_snake_case_candidates(
    raw_question: str, exclude: set[str] | None = None
) -> list[str]:
    """Return up to 3 snake_case standardized-question name candidates.

    Tries Gemini (free-tier gemini-flash-lite-latest) when GEMINI_API_KEY and the
    google-generativeai SDK are both available; otherwise falls back to the local
    heuristic. Always returns something (never raises) so the UI button works
    regardless of API setup.

    Not cached -- repeated calls return FRESH suggestions. Pass the already-shown
    names in `exclude` so the caller can keep cycling for new options until the
    reviewer finds one they like. Gemini is asked at a moderate temperature so
    re-rolls produce real variety, and any name in `exclude` is filtered out of
    both the Gemini and heuristic results before returning.
    """
    import re  # used below for snake_case normalization (was undefined -- flake8 F821)

    exclude = {e.lower() for e in (exclude or set())}
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if api_key:
        try:
            import google.generativeai as genai

            genai.configure(api_key=api_key)
            # 'latest' alias auto-tracks the current lite flash model so this
            # doesn't go stale the way a pinned version (e.g. gemini-1.5-flash) did.
            model = genai.GenerativeModel("gemini-flash-lite-latest")

            # Tell the model explicitly which names to avoid; raises diversity on re-rolls.
            exclude_clause = ""
            if exclude:
                shown = ", ".join(sorted(exclude))
                exclude_clause = (
                    f"\nDo NOT repeat any of these already-shown names: {shown}. "
                    f"Propose DIFFERENT names that capture other aspects of the question."
                )
            prompt = (
                "You name database columns. Given a survey question, propose exactly 3 "
                "short snake_case identifiers (lowercase, words joined by underscores, "
                "no spaces or punctuation, 2-4 words each) that capture what the question "
                "measures. Return ONLY the 3 names, one per line, nothing else."
                f"{exclude_clause}\n\n"
                f"Question: {raw_question}"
            )
            # Moderate temperature so repeated calls produce variety. Default is
            # low/deterministic which would echo prior responses.
            resp = model.generate_content(
                prompt, generation_config={"temperature": 0.9}
            )
            text = (resp.text or "").strip()
            cands = []
            for line in text.splitlines():
                s = re.sub(r"[^a-z0-9_]", "", line.strip().lower().replace(" ", "_"))
                s = re.sub(r"_+", "_", s).strip("_")
                if s and s not in cands and s not in exclude:
                    cands.append(s)
            if cands:
                return cands[:3]
        except Exception:
            pass  # fall through to heuristic on any SDK/network/quota error
    return _heuristic_snake_case_candidates(raw_question, exclude=exclude)


def _confidence_band(score: float) -> str:
    if score >= 1.5:
        return "High"
    if score >= 0.5:
        return "Medium"
    if score > 0:
        return "Low"
    return "None"


def _render_source_chips(sug) -> str:
    """Render per-source breakdown for a suggestion as five compact chips.

    `sug` is a row from get_blended_suggestions with float columns
    ml_model / fuzzy / semantic / cooccur / fingerprint (all 0-1, 0 meaning
    that source did not contribute). Returns a markdown string suitable for
    st.markdown -- each present source shows as `name 0.82`; absent sources
    show as a dash so reviewers can see at a glance which signals agreed
    and which were silent.

    ml_model is rendered first because it is the strongest signal in the
    cascade and the most likely to drive the reviewer's accept/reject
    decision."""

    def _chip(name: str, value: float) -> str:
        if value and value > 0:
            return f"`{name} {value:.2f}`"
        return f"`{name} —`"

    return (
        _chip("ml_model", float(sug.get("ml_model", 0)))
        + " "
        + _chip("fuzzy", float(sug.get("fuzzy", 0)))
        + " "
        + _chip("semantic", float(sug.get("semantic", 0)))
        + " "
        + _chip("cooccur", float(sug.get("cooccur", 0)))
        + " "
        + _chip("fingerprint", float(sug.get("fingerprint", 0)))
    )


def _cluster_near_duplicates(
    raw_questions: list[str], threshold: int = 95
) -> dict[str, list[str]]:
    """Group raw_questions where pairwise fuzz.ratio >= threshold. Returns {leader: [members]}."""
    clusters: dict[str, list[str]] = {}
    assigned: set[str] = set()
    for q in raw_questions:
        if q in assigned:
            continue
        clusters[q] = [q]
        assigned.add(q)
        for other in raw_questions:
            if other in assigned:
                continue
            if fuzz.ratio(q, other) >= threshold:
                clusters[q].append(other)
                assigned.add(other)
    return clusters


def show_unmapped_questions(client):
    """Fast queue + on-demand detail.

    Page-load cost is one cached BigQuery query. No ML model loads, no batch
    suggestion precompute -- the detail panel does that per-card, on click.
    Bulk-on-similar lives in the detail panel's 'Near-duplicates' section.
    """
    st.header("Unmapped Questions 🔍")

    if "success_msg" in st.session_state:
        st.success(st.session_state.pop("success_msg"))

    # --- Toolbar: Undo single + Undo batch ---
    tb1, tb2, tb3 = st.columns([1, 1, 3])
    with tb1:
        if st.button("↩ Undo last", help="Reverse the most recent single mapping."):
            ok, msg = undo_last_mapping(client)
            if ok:
                st.session_state["success_msg"] = msg
                st.cache_data.clear()
                st.session_state["unmapped_q_advance"] = True
                st.session_state.pop("unmapped_q_table", None)
                st.rerun()
            else:
                st.warning(msg)
    with tb2:
        if st.button(
            "⏪ Undo last batch",
            help="Reverse every mapping from the most recent bulk operation.",
        ):
            ok, msg = undo_last_batch(client)
            if ok:
                st.session_state["success_msg"] = msg
                st.cache_data.clear()
                st.session_state["unmapped_q_advance"] = True
                st.session_state.pop("unmapped_q_table", None)
                st.rerun()
            else:
                st.warning(msg)

    unmapped = get_unmapped_questions(client)
    if unmapped.empty:
        st.success("All questions are mapped!")
        return

    # --- Fast top-line metric: how many unique unmapped questions are waiting. ---
    total_rows_affected = int(unmapped["row_count"].sum())
    m1, m2 = st.columns(2)
    with m1:
        st.metric("Unique unmapped questions", f"{len(unmapped):,}")
    with m2:
        st.metric("Total rows affected", f"{total_rows_affected:,}")

    # Lookup data needed by the detail panel (these are cheap, cached queries).
    std_questions_df = get_standardized_questions(client)
    std_questions = (
        std_questions_df["standardized_question"].tolist()
        if not std_questions_df.empty
        else []
    )
    cats_df = get_categories(client)
    categories_list = cats_df["category"].tolist() if not cats_df.empty else ["other"]

    # --- Filter presets + search ---
    # Presets narrow the queue by row_count / survey_count BEFORE the text
    # search runs. Each maps to a small lambda over the unmapped DataFrame.
    # Adjust thresholds here if reviewer needs change; the lambdas are the
    # single place this knowledge lives.
    PRESET_FILTERS = {
        "All": lambda df: df,
        "High-impact (>=100 rows)": lambda df: df[df["row_count"] >= 100],
        "Cross-survey (>=3 surveys)": lambda df: df[df["survey_count"] >= 3],
        "Single-survey noise (1 survey, >=50 rows)": lambda df: df[
            (df["survey_count"] == 1) & (df["row_count"] >= 50)
        ],
        "Quick wins (>=20 rows, <=2 surveys)": lambda df: df[
            (df["row_count"] >= 20) & (df["survey_count"] <= 2)
        ],
    }
    preset = st.segmented_control(
        "Filter",
        list(PRESET_FILTERS),
        default="All",
        key="unmapped_q_preset",
        help=(
            "Presets narrow the queue before the text search. "
            "High-impact = many rows. Cross-survey = a fix lands many "
            "surveys. Single-survey noise = often a typo cluster in one "
            "survey. Quick wins = small but visible."
        ),
    )
    # segmented_control allows deselect (returns None); fall back to All so
    # the queue never silently disappears.
    preset = preset or "All"
    filtered = PRESET_FILTERS[preset](unmapped)

    search = st.text_input("Search questions", "", key="unmapped_q_search")
    if search:
        filtered = filtered[
            filtered["raw_question_en"].str.contains(search, case=False, na=False)
        ]
    if filtered.empty:
        st.info(
            f"No unmapped questions match the '{preset}' filter"
            f"{' and your search' if search else ''}."
        )
        return

    # --- Side-by-side queue + detail ---
    table_col, detail_col = st.columns([2, 3])

    with table_col:
        st.markdown("##### Queue")
        st.caption(
            f"Showing {len(filtered):,} of {len(unmapped):,} unique questions. "
            f"Click a row to triage."
        )
        display_df = filtered[["raw_question_en", "row_count", "survey_count"]].rename(
            columns={
                "raw_question_en": "Question",
                "row_count": "Rows",
                "survey_count": "Surveys",
            }
        )
        # After a successful map we set this flag and rerun. The widget's stored
        # row-index selection is unreliable across the data shift (the mapped row
        # is gone, indices renumber), so on advance we explicitly drop the old
        # selection widget state and auto-select the new top of the queue. This
        # keeps the left queue and the right detail panel pointing at the SAME
        # next question.
        advance = st.session_state.pop("unmapped_q_advance", False)
        if advance:
            st.session_state.pop("unmapped_q_table", None)

        try:
            event = st.dataframe(
                display_df,
                hide_index=True,
                width="stretch",
                height=600,
                on_select="rerun",
                selection_mode="single-row",
                key="unmapped_q_table",
                row_height=70,
                column_config={
                    "Question": st.column_config.TextColumn(
                        "Question",
                        width="large",
                        help="Full question text (wraps).",
                    ),
                    "Rows": st.column_config.NumberColumn(
                        "Rows", width="small", format="%d"
                    ),
                    "Surveys": st.column_config.NumberColumn(
                        "Surveys",
                        width="small",
                        format="%d",
                        help="Distinct surveys this raw_question appears in. "
                        "High Surveys + medium Rows = higher leverage (one fix "
                        "lands many surveys); high Rows + Surveys=1 means a "
                        "single noisy survey.",
                    ),
                },
            )
            selected_rows = event.selection.rows if event and event.selection else []
            if selected_rows:
                selected_raw_q = filtered.iloc[selected_rows[0]]["raw_question_en"]
            elif advance and not filtered.empty:
                # Just mapped one -> jump straight to the next question so the
                # reviewer keeps moving without re-clicking.
                selected_raw_q = filtered.iloc[0]["raw_question_en"]
            else:
                selected_raw_q = None
        except Exception:
            selected_raw_q = st.selectbox(
                "Pick a question",
                options=filtered["raw_question_en"].tolist(),
                key="unmapped_q_select_fallback",
            )

    with detail_col:
        if not selected_raw_q:
            st.info("Select a row from the queue to work on it.")
            return

        row = filtered[filtered["raw_question_en"] == selected_raw_q].iloc[0]
        _render_question_detail(client, row, std_questions, categories_list)


@st.cache_data(ttl=60)
def _existing_assignment_summary(_client, raw_question: str) -> dict | None:
    """If the raw_question already has standardized_question set on some rows (from ETL),
    return a summary so the UI can warn the user before they overwrite it.
    None if no existing assignments (safe to proceed)."""
    query = f"""
    SELECT standardized_question, question_match_method,
           COUNTIF(manually_assigned = TRUE) AS manual_rows,
           COUNT(*) AS total_rows
    FROM `{COLUMNAR_COMPLETED}`
    WHERE TRIM(REGEXP_REPLACE(raw_question_en, r'\\s+', ' ')) = TRIM(REGEXP_REPLACE(@raw_q, r'\\s+', ' '))
      AND standardized_question IS NOT NULL
    GROUP BY standardized_question, question_match_method
    ORDER BY total_rows DESC
    LIMIT 1
    """
    rows = list(
        _client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("raw_q", "STRING", raw_question)
                ]
            ),
        )
    )
    if not rows:
        return None
    r = rows[0]
    return {
        "existing_std_q": r.standardized_question,
        "method": r.question_match_method or "unknown",
        "manual_rows": r.manual_rows,
        "total_rows": r.total_rows,
    }


@st.cache_data(ttl=600)
def _suspicious_mapping_warning(
    _client, raw_question: str, target_std_question: str
) -> str | None:
    """Light heuristic: compare this raw_question's top answers against the answer fingerprint
    of target_std_question (if table exists). Returns a warning string or None.

    Uses ANSWER_FINGERPRINTS if present; otherwise falls back to a direct-cooccurrence check
    in columnar_completed.
    """
    try:
        # Does fingerprints table exist?
        exists_q = f"""
        SELECT COUNT(*) AS cnt FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = 'standardized_question_answer_fingerprints'
        """
        has_fp = list(_client.query(exists_q))[0].cnt > 0

        if has_fp:
            # Compare answer overlap ratio using the fingerprint
            overlap_q = f"""
            WITH this_q AS (
                SELECT DISTINCT TRIM(LOWER(raw_answer_en)) AS ans
                FROM `{COLUMNAR_COMPLETED}`
                WHERE raw_question_en = @raw_q
                  AND raw_answer_en IS NOT NULL AND TRIM(raw_answer_en) != ''
                LIMIT 30
            ),
            fp AS (
                SELECT TRIM(LOWER(answer)) AS ans
                FROM `{ANSWER_FINGERPRINTS}`, UNNEST(top_answers) AS answer
                WHERE standardized_question = @std_q
            )
            SELECT
                (SELECT COUNT(*) FROM this_q) AS this_count,
                (SELECT COUNT(*) FROM fp) AS fp_count,
                (SELECT COUNT(*) FROM this_q t JOIN fp f USING(ans)) AS overlap
            """
        else:
            # Fallback: count overlap against all raw_questions already mapped to target std_q
            overlap_q = f"""
            WITH this_q AS (
                SELECT DISTINCT TRIM(LOWER(raw_answer_en)) AS ans
                FROM `{COLUMNAR_COMPLETED}`
                WHERE raw_question_en = @raw_q
                  AND raw_answer_en IS NOT NULL AND TRIM(raw_answer_en) != ''
                LIMIT 30
            ),
            target_q AS (
                SELECT DISTINCT TRIM(LOWER(raw_answer_en)) AS ans
                FROM `{COLUMNAR_COMPLETED}`
                WHERE standardized_question = @std_q
                  AND raw_answer_en IS NOT NULL AND TRIM(raw_answer_en) != ''
                  AND raw_question_en != @raw_q
                LIMIT 200
            )
            SELECT
                (SELECT COUNT(*) FROM this_q) AS this_count,
                (SELECT COUNT(*) FROM target_q) AS fp_count,
                (SELECT COUNT(*) FROM this_q t JOIN target_q f USING(ans)) AS overlap
            """

        result = list(
            _client.query(
                overlap_q,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("raw_q", "STRING", raw_question),
                        bigquery.ScalarQueryParameter(
                            "std_q", "STRING", target_std_question
                        ),
                    ]
                ),
            )
        )[0]

        # If we have no reference data, can't judge — don't warn
        if result.fp_count == 0 or result.this_count == 0:
            return None

        overlap_ratio = result.overlap / result.this_count
        # Suspicious if zero overlap and reference set is non-trivial
        if result.overlap == 0 and result.fp_count >= 5:
            return (
                f"This question's answers don't overlap at all with {result.fp_count} existing answers "
                f"mapped to '{target_std_question}'. This mapping might be wrong."
            )
        if overlap_ratio < 0.10 and result.fp_count >= 10:
            return (
                f"Only {result.overlap}/{result.this_count} of this question's answers match the "
                f"existing '{target_std_question}' profile ({result.fp_count} answers). Double-check."
            )
        return None
    except Exception:
        return None


def _apply_to_members(
    client,
    members: list[str],
    std_q: str,
    match_type: str = "manual",
    auto_assigned: bool = False,
    show_spinner: bool = True,
) -> int:
    """Apply the same std_q mapping to every member of a near-duplicate cluster.
    Returns count of successful saves.

    Shows a spinner for the duration -- the underlying UPDATE scans the ~935K-row
    columnar table and takes several seconds, and without immediate feedback users
    re-click the button. Callers already inside their own spinner pass
    show_spinner=False to avoid a stacked double-spinner.
    """
    import contextlib
    import uuid

    batch_id = f"cluster_{uuid.uuid4().hex[:10]}" if len(members) > 1 else None
    ok = 0
    spinner_ctx = (
        st.spinner(f"Mapping to '{std_q}'... (updating BigQuery, please wait)")
        if show_spinner
        else contextlib.nullcontext()
    )
    with spinner_ctx:
        for rq in members:
            if add_question_mapping(
                client,
                rq,
                std_q,
                match_type=match_type,
                auto_assigned=auto_assigned,
                batch_id=batch_id,
            ):
                ok += 1
    return ok


def _render_question_detail(
    client, row, std_questions, categories_list, cluster_members: list[str] = None
):
    """Render the editable detail panel for a single unmapped question."""
    raw_q = row["raw_question_en"]
    members = cluster_members or [raw_q]
    k = _stable_key(raw_q)  # stable widget-key prefix

    st.markdown(f"##### Detail")
    st.write(f"**Full question:** {raw_q}")
    if len(members) > 1:
        with st.expander(
            f"📎 This is a cluster of {len(members)} near-duplicate variants. Save will map all of them."
        ):
            for m in members:
                st.write(f"• `{m}`")
    st.write(
        f"**Responses:** {row['response_count']:,}  ·  **Rows affected:** {row['row_count']:,}"
    )

    # If this raw_question already has a standardized_question set on some rows (ETL), show it.
    # Our save UPDATE only touches NULL rows, so this is informational — but it warns the user
    # that their new mapping will not retroactively change rows the ETL already assigned.
    existing = _existing_assignment_summary(client, raw_q)
    if existing:
        st.info(
            f"ℹ️ The ETL has already assigned **`{existing['existing_std_q']}`** "
            f"(method: `{existing['method']}`) to {existing['total_rows']:,} row(s) of this raw question. "
            f"Your mapping here only applies to rows currently NULL — existing assignments stay put. "
            f"To change them, use the Rename tool or unset the ETL assignment first."
        )

    # Near-duplicates: surface text-similar unmapped questions so reviewer can map
    # a wording/typo cluster in one click.
    _render_near_duplicates_panel(client, raw_q, std_questions, key_prefix=f"nd_{k}")

    # Conditional split: assign different std_qs by raw_answer pattern (e.g. emails vs names).
    render_conditional_split_panel(
        client, raw_q, std_questions, key_prefix=f"unmapped_{k}"
    )

    # Answer peek (toggle)
    peek_key = f"peek_{k}"
    if st.button("👁 View answers", key=f"btn_{peek_key}"):
        st.session_state[peek_key] = not st.session_state.get(peek_key, False)

    if st.session_state.get(peek_key, False):
        with st.container(border=True):
            answers_df = get_top_raw_answers(client, raw_q, limit=20)
            qtype = answers_df.attrs.get("question_type")
            if qtype == "matrix":
                st.caption(
                    "Matrix question -- showing the subquestion TOPICS (not the raw "
                    "'Y'/'N' tick value). Each topic's count = how many respondents "
                    "selected it. '[other]' rows are free-text answers."
                )
            else:
                st.caption("Top distinct raw answers:")
            if answers_df.empty:
                st.write("_No answers found._")
            else:
                display = answers_df.rename(
                    columns={
                        "label": "Answer / Topic",
                        "response_count": "Count",
                        "kind": "Type",
                    }
                )
                st.dataframe(display, hide_index=True, width="stretch", height=240)

    # Blended suggestions (fuzzy + co-occurrence + semantic) — only for this one row now
    suggestions = get_blended_suggestions(
        client, raw_q, std_questions, fuzzy_threshold=60
    )

    # Very high-confidence suggestion: offer a one-click Accept instead of auto-writing.
    # (Writes on mere page render were a real hazard — now requires explicit click.)
    top_fuzzy = next(
        iter(get_fuzzy_question_matches(raw_q, std_questions, top_n=1, threshold=0)),
        None,
    )
    if top_fuzzy and top_fuzzy[1] >= 95:
        auto_match, auto_score, _ = top_fuzzy
        with st.container(border=True):
            st.markdown(f"**⚡ High-confidence match ({auto_score}%):** `{auto_match}`")
            if st.button("Accept suggestion", key=f"accept_auto_{k}", type="primary"):
                saved = _apply_to_members(
                    client,
                    members,
                    auto_match,
                    match_type="auto_approved_fuzzy",
                    auto_assigned=True,
                )
                if saved:
                    st.session_state["success_msg"] = (
                        f"Mapped ({auto_score}%): '{raw_q[:50]}...' → '{auto_match}'"
                        + (f" · {saved} variants" if saved > 1 else "")
                    )
                    st.cache_data.clear()
                    st.session_state["unmapped_q_advance"] = True
                    st.session_state.pop("unmapped_q_table", None)
                    st.rerun()

    if not suggestions.empty:
        st.markdown("**Suggested Matches** _(blended)_")
        for _, sug in suggestions.head(5).iterrows():
            cols = st.columns([1, 4, 3, 1])
            with cols[0]:
                st.metric("Score", f"{sug['score']:.2f}")
            with cols[1]:
                st.code(sug["std_question"])
            with cols[2]:
                st.markdown(_render_source_chips(sug))
            with cols[3]:
                if st.button(
                    "Use", key=f"q_use_{k}_{sug['std_question']}", type="secondary"
                ):
                    warn = _suspicious_mapping_warning(
                        client, raw_q, sug["std_question"]
                    )
                    if warn:
                        st.session_state[f"pending_save_{k}"] = {
                            "std_q": sug["std_question"],
                            "mode": "existing",
                            "warn": warn,
                        }
                        st.rerun()
                    else:
                        saved = _apply_to_members(client, members, sug["std_question"])
                        if saved:
                            st.session_state["success_msg"] = (
                                f"Mapped '{raw_q[:50]}...' → '{sug['std_question']}'"
                                + (f" · {saved} variants" if saved > 1 else "")
                            )
                            st.cache_data.clear()
                            st.session_state["unmapped_q_advance"] = True
                            st.session_state.pop("unmapped_q_table", None)
                            st.rerun()
    else:
        st.info(
            "💡 No automated suggestions. Select existing or create a new one below."
        )

    # Pending-save confirmation panel (suspicious mapping warning)
    pending = st.session_state.get(f"pending_save_{k}")
    if pending:
        with st.container(border=True):
            st.warning(f"⚠️ {pending['warn']}")
            c_yes, c_no = st.columns(2)
            with c_yes:
                if st.button("Proceed anyway", key=f"pending_yes_{k}", type="primary"):
                    saved = _apply_to_members(client, members, pending["std_q"])
                    if saved:
                        st.session_state["success_msg"] = (
                            f"Mapped (confirmed) '{raw_q[:50]}...' → '{pending['std_q']}'"
                            + (f" · {saved} variants" if saved > 1 else "")
                        )
                    st.session_state.pop(f"pending_save_{k}", None)
                    st.cache_data.clear()
                    st.session_state["unmapped_q_advance"] = True
                    st.session_state.pop("unmapped_q_table", None)
                    st.rerun()
            with c_no:
                if st.button("Cancel", key=f"pending_no_{k}"):
                    st.session_state.pop(f"pending_save_{k}", None)
                    st.rerun()

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Select Existing**")
        selected = st.selectbox(
            "Standardized question",
            [""] + std_questions,
            key=f"q_sel_{k}",
            label_visibility="collapsed",
        )
        if st.button("Map to Selected", key=f"q_map_{k}", disabled=not selected):
            warn = _suspicious_mapping_warning(client, raw_q, selected)
            if warn:
                st.session_state[f"pending_save_{k}"] = {
                    "std_q": selected,
                    "mode": "existing",
                    "warn": warn,
                }
                st.rerun()
            else:
                saved = _apply_to_members(client, members, selected)
                if saved:
                    st.session_state["success_msg"] = (
                        f"Mapped '{raw_q[:50]}...' → '{selected}'"
                        + (f" · {saved} variants" if saved > 1 else "")
                    )
                    st.cache_data.clear()
                    st.session_state["unmapped_q_advance"] = True
                    st.session_state.pop("unmapped_q_table", None)
                    st.rerun()

        # AI name suggestions: generate up to 3 snake_case candidates from the
        # question text. Clicking one fills the Create New "Question name" field.
        # The button can be pressed repeatedly to cycle through fresh suggestions
        # -- each click excludes everything previously shown for this card so the
        # reviewer keeps getting NEW options until they find one they want.
        st.markdown("---")
        cand_key = f"name_candidates_{k}"  # the 3 currently displayed
        shown_key = (
            f"name_candidates_shown_{k}"  # cumulative set of names shown for this card
        )

        already_shown = st.session_state.get(shown_key, set())
        button_label = (
            "Suggest more names (AI)" if already_shown else "Suggest names (AI)"
        )

        if st.button(
            button_label,
            key=f"suggest_names_{k}",
            help="Generate 3 snake_case name ideas. Click again for 3 fresh ones (won't repeat).",
        ):
            with st.spinner("Generating name ideas..."):
                new_cands = _generate_snake_case_candidates(
                    raw_q, exclude=already_shown
                )
            st.session_state[cand_key] = new_cands
            st.session_state[shown_key] = already_shown | set(new_cands)
            st.rerun()

        candidates = st.session_state.get(cand_key)
        if candidates:
            st.caption(
                "Click a name to use it in Create New, or press the button above for more:"
            )
            for cand in candidates:
                if st.button(
                    cand,
                    key=f"usecand_{k}_{cand}",
                    help="Fill the Create New field with this name.",
                ):
                    # Populate the Create New question-name field, then clear the
                    # candidate list AND the shown-set so the next reviewer of
                    # this card starts fresh.
                    st.session_state[f"new_std_name_{k}"] = cand
                    st.session_state.pop(cand_key, None)
                    st.session_state.pop(shown_key, None)
                    st.rerun()

    with col2:
        st.markdown("**Create New**")

        # Category picker + inline creator stays OUTSIDE the form so the "+ New category…"
        # sentinel can reveal its creation fields via live rerun. The form below submits
        # once when the user is done.
        NEW_CAT_SENTINEL = "+ New category…"
        new_std_cat = st.selectbox(
            "Category",
            categories_list + [NEW_CAT_SENTINEL],
            key=f"q_new_cat_{k}",
        )
        if new_std_cat == NEW_CAT_SENTINEL:
            c1, c2, c3 = st.columns([2, 2, 1])
            with c1:
                inline_cat = st.text_input(
                    "New category (snake_case)",
                    key=f"inline_cat_{k}",
                    placeholder="e.g. mental_health",
                )
            with c2:
                inline_cat_display = st.text_input(
                    "Display name",
                    key=f"inline_cat_disp_{k}",
                    placeholder="Human-readable",
                )
            with c3:
                st.write("")
                if st.button(
                    "Create category",
                    key=f"inline_cat_btn_{k}",
                    disabled=not inline_cat,
                ):
                    if add_category(client, inline_cat, inline_cat_display, ""):
                        st.session_state["success_msg"] = (
                            f"Created category '{inline_cat}'"
                        )
                        st.cache_data.clear()
                        st.session_state[f"q_new_cat_{k}"] = inline_cat
                        st.rerun()

        # Form batches text inputs so Streamlit doesn't rerun per keystroke.
        with st.form(key=f"create_new_form_{k}", clear_on_submit=False):
            new_std = st.text_input(
                "Question name (snake_case)",
                help="e.g., 'age_at_diagnosis'",
                placeholder="new_question_name",
                key=f"new_std_name_{k}",  # AI suggestions populate this via session_state
            )
            new_std_display = st.text_input(
                "Display name",
                placeholder="Human-readable name",
            )
            submitted = st.form_submit_button(
                "Create & Map",
                type="primary",
                disabled=(new_std_cat == NEW_CAT_SENTINEL),
            )

        # Inline validation runs OUTSIDE the form so its messages render
        # without requiring submit. Submission still goes through the form's
        # button above; this block just blocks the apply if the name is bad.
        if new_std:
            _validate_canonical_name_inline(
                new_std, std_questions, require_snake_case=True
            )

        if submitted:
            if not new_std:
                st.warning("Question name is required.")
            elif not _is_valid_snake_case(new_std):
                st.error(
                    "Question name must be snake_case "
                    "(see message above). Fix and re-submit."
                )
            elif new_std_cat == NEW_CAT_SENTINEL:
                st.warning("Create the new category first, or pick an existing one.")
            else:
                with st.spinner(f"Creating '{new_std}' and mapping rows..."):
                    created = add_standardized_question(
                        client, new_std, new_std_display, new_std_cat
                    )
                    saved = (
                        _apply_to_members(client, members, new_std, show_spinner=False)
                        if created
                        else 0
                    )
                if created and saved:
                    st.session_state["success_msg"] = (
                        f"Created '{new_std}' and mapped '{raw_q[:50]}...'"
                        + (f" · {saved} variants" if saved > 1 else "")
                    )
                    st.cache_data.clear()
                    st.session_state["unmapped_q_advance"] = True
                    st.session_state.pop("unmapped_q_table", None)
                    st.rerun()


def show_unmapped_answers(client):
    """Show and manage unmapped answers with fuzzy matching suggestions."""
    st.header("Unmapped Answers 🔍")

    # Show success message from previous mapping action (survives st.rerun)
    if "success_msg" in st.session_state:
        st.success(st.session_state.pop("success_msg"))

    unmapped = get_unmapped_answers(client)

    if unmapped.empty:
        st.success("All answers are mapped!")
        return

    st.write(f"Found {len(unmapped)} distinct unmapped answers")

    # Filter by question
    questions = unmapped["standardized_question"].unique().tolist()

    col1, col2 = st.columns([3, 1])
    with col1:
        selected_question = st.selectbox("Filter by question", ["All"] + questions)
    with col2:
        fuzzy_threshold = st.slider(
            "Match threshold",
            60,
            95,
            75,
            5,
            help="Minimum similarity score for answers",
        )

    if selected_question != "All":
        unmapped = unmapped[unmapped["standardized_question"] == selected_question]

    # Prefetch all standardized answers for the visible questions in ONE BigQuery roundtrip
    # (was: N queries in a loop, one per expander).
    visible = unmapped.head(30)
    visible_questions = tuple(visible["standardized_question"].unique().tolist())
    q_answers_map = _prefetch_standardized_answers(client, visible_questions)

    # Display each with fuzzy matching and mapping option
    for idx, row in visible.iterrows():
        with st.expander(
            f"💬 {row['standardized_question']}: **{row['raw_answer_en'][:50]}...** ({row['row_count']:,} rows)",
            expanded=idx < 3,
        ):
            st.write(f"**Question:** {row['standardized_question']}")
            st.write(f"**Raw Answer:** {row['raw_answer_en']}")
            st.write(f"**Answer Value:** {row['answer_value']}")
            st.write(f"**Occurrences:** {row['row_count']:,}")

            ctx_key = f"ctx_{idx}"
            if st.button(
                "👁 Context: see this answer in real responses",
                key=f"btn_{ctx_key}",
                help="Shows a few sample responses containing this raw answer — helps disambiguate meaning.",
            ):
                st.session_state[ctx_key] = not st.session_state.get(ctx_key, False)

            if st.session_state.get(ctx_key, False):
                with st.container(border=True):
                    ctx_df = _get_answer_context(
                        client,
                        row["standardized_question"],
                        row["raw_answer_en"],
                        limit=5,
                    )
                    if ctx_df.empty:
                        st.caption("_No context rows found._")
                    else:
                        st.caption(
                            f"Sample of up to 5 responses where this answer appeared:"
                        )
                        st.dataframe(
                            ctx_df, hide_index=True, width="stretch", height=200
                        )

            # Use prefetched answers for this question
            q_answers = q_answers_map.get(row["standardized_question"], [])

            # Get fuzzy matches
            if q_answers:
                fuzzy_matches = get_fuzzy_answer_matches(
                    row["raw_answer_en"], q_answers, top_n=5, threshold=fuzzy_threshold
                )

                # High-confidence suggestion: offer one-click Accept (no auto-write on render).
                if fuzzy_matches and fuzzy_matches[0][1] >= 95:
                    auto_match, auto_score, _ = fuzzy_matches[0]
                    ac1, ac2 = st.columns([4, 1])
                    with ac1:
                        st.markdown(
                            f"**⚡ High-confidence match ({auto_score}%):** `{auto_match}`"
                        )
                    with ac2:
                        if st.button(
                            "Accept", key=f"a_accept_auto_{idx}", type="primary"
                        ):
                            if add_answer_mapping(
                                client,
                                row["standardized_question"],
                                row["raw_answer_en"],
                                auto_match,
                                match_type="auto_approved_fuzzy",
                                auto_assigned=True,
                            ):
                                st.session_state["success_msg"] = (
                                    f"Mapped ({auto_score}%): '{row['raw_answer_en'][:30]}...' → '{auto_match}'"
                                )
                                st.cache_data.clear()
                                st.rerun()

                # Show fuzzy suggestions
                if fuzzy_matches:
                    st.markdown("**🎯 Suggested Matches:**")
                    for match, score, _ in fuzzy_matches[:3]:
                        cols = st.columns([1, 3, 1])
                        with cols[0]:
                            st.metric("Score", f"{score}%")
                        with cols[1]:
                            st.code(match)
                        with cols[2]:
                            if st.button(
                                "Use", key=f"a_use_{idx}_{match}", type="secondary"
                            ):
                                if add_answer_mapping(
                                    client,
                                    row["standardized_question"],
                                    row["raw_answer_en"],
                                    match,
                                ):
                                    st.session_state["success_msg"] = (
                                        f"Mapped '{row['raw_answer_en'][:30]}...' -> '{match}'"
                                    )
                                    st.cache_data.clear()
                                    st.rerun()
                else:
                    st.info(
                        "💡 No fuzzy matches found. Select existing or create new below."
                    )

            st.divider()

            # Manual selection or creation
            col1, col2, col3 = st.columns([2, 2, 1])

            with col1:
                st.subheader("Select Existing")
                selected = st.selectbox(
                    "Standardized answer",
                    [""] + q_answers,
                    key=f"a_sel_{idx}",
                    label_visibility="collapsed",
                )
                if st.button(
                    "Map to Selected", key=f"a_map_{idx}", disabled=not selected
                ):
                    if add_answer_mapping(
                        client,
                        row["standardized_question"],
                        row["raw_answer_en"],
                        selected,
                    ):
                        st.session_state["success_msg"] = (
                            f"Mapped '{row['raw_answer_en'][:30]}...' -> '{selected}'"
                        )
                        st.cache_data.clear()
                        st.rerun()

            with col2:
                st.subheader("Create New")
                new_ans = st.text_input(
                    "Answer value", key=f"a_new_{idx}", placeholder="new_answer_value"
                )
                new_ans_display = st.text_input(
                    "Display name",
                    key=f"a_new_display_{idx}",
                    placeholder="Human-readable name",
                )

                # Inline near-duplicate warning scoped to existing answers
                # under this question. snake_case NOT required for answers.
                name_ok = bool(new_ans)
                if new_ans:
                    name_ok = _validate_canonical_name_inline(
                        new_ans, q_answers, require_snake_case=False
                    )

                if st.button(
                    "Create & Map",
                    key=f"a_create_{idx}",
                    disabled=not new_ans or not name_ok,
                ):
                    with st.spinner(f"Creating '{new_ans}' and mapping rows..."):
                        created = add_standardized_answer(
                            client,
                            row["standardized_question"],
                            new_ans,
                            new_ans_display,
                        )
                        mapped = (
                            add_answer_mapping(
                                client,
                                row["standardized_question"],
                                row["raw_answer_en"],
                                new_ans,
                                show_spinner=False,
                            )
                            if created
                            else False
                        )
                    if created and mapped:
                        st.session_state["success_msg"] = (
                            f"Created '{new_ans}' and mapped '{row['raw_answer_en'][:30]}...'"
                        )
                        st.cache_data.clear()
                        st.rerun()

            with col3:
                st.subheader("Skip")
                if st.button(
                    "Skip (free text)",
                    key=f"a_skip_{idx}",
                    help="Mark this answer as __SKIP__ — won't appear in the queue again, and the ETL ignores it.",
                ):
                    if add_answer_mapping(
                        client,
                        row["standardized_question"],
                        row["raw_answer_en"],
                        "__SKIP__",
                        match_type="manual",
                    ):
                        st.session_state["success_msg"] = (
                            f"Skipped '{row['raw_answer_en'][:30]}...' (free-text/numeric; won't reappear)"
                        )
                        st.cache_data.clear()
                        st.rerun()


def _null_out_non_manual_assignments_for_std_q(
    client, raw_question: str, std_question: str
) -> int:
    """Null out columnar_completed rows for one (raw_q, std_q) pair, skipping any
    that are manually_assigned = TRUE. Used by the Audit page to wipe a wrong split
    so the rows re-enter the unmapped queue and the ETL/UI can reclassify them.
    """
    query = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_question = NULL,
        question_match_method = NULL,
        manually_assigned = FALSE,
        manually_assigned_at = NULL,
        manually_assigned_by = NULL
    WHERE raw_question_en = @raw_q
      AND standardized_question = @std_q
      AND COALESCE(manually_assigned, FALSE) = FALSE
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("raw_q", "STRING", raw_question),
                bigquery.ScalarQueryParameter("std_q", "STRING", std_question),
            ]
        ),
    )
    job.result()
    affected = job.num_dml_affected_rows or 0
    _log_history(
        client,
        "audit_null_out",
        affected,
        raw_question=raw_question,
        new_std_question=std_question,
    )
    return affected


def _bulk_assign_raw_question_to_std_q(
    client, raw_question: str, new_std_q: str, include_manual: bool = False
) -> int:
    """Set every row of raw_question_en to new_std_q. Treated as a manual decision
    (manually_assigned=TRUE, question_match_method='manual') so the ETL won't
    overwrite it. By default skips rows already manually_assigned=TRUE; pass
    include_manual=True after explicit user confirmation to overwrite those too.
    """
    manual_clause = (
        "" if include_manual else "AND COALESCE(manually_assigned, FALSE) = FALSE"
    )
    query = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_question = @std_q,
        question_match_method = 'manual',
        manually_assigned = TRUE,
        manually_assigned_at = CURRENT_TIMESTAMP(),
        manually_assigned_by = 'streamlit_app'
    WHERE raw_question_en = @raw_q
      AND (standardized_question IS NULL OR standardized_question != @std_q)
      {manual_clause}
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("raw_q", "STRING", raw_question),
                bigquery.ScalarQueryParameter("std_q", "STRING", new_std_q),
            ]
        ),
    )
    job.result()
    affected = job.num_dml_affected_rows or 0
    _log_history(
        client,
        "audit_bulk_assign",
        affected,
        raw_question=raw_question,
        new_std_question=new_std_q,
    )
    return affected


def _reassign_limb_to_std_q(
    client,
    raw_question: str,
    old_std_q: str,
    new_std_q: str,
    include_manual: bool = False,
) -> int:
    """Change one limb of a split: rows matching (raw_question, old_std_q) become
    (raw_question, new_std_q). Treated as a manual decision. Skips manual rows by
    default; include_manual=True after confirmation overrides.
    """
    manual_clause = (
        "" if include_manual else "AND COALESCE(manually_assigned, FALSE) = FALSE"
    )
    query = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_question = @new_std_q,
        question_match_method = 'manual',
        manually_assigned = TRUE,
        manually_assigned_at = CURRENT_TIMESTAMP(),
        manually_assigned_by = 'streamlit_app'
    WHERE raw_question_en = @raw_q
      AND standardized_question = @old_std_q
      {manual_clause}
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("raw_q", "STRING", raw_question),
                bigquery.ScalarQueryParameter("old_std_q", "STRING", old_std_q),
                bigquery.ScalarQueryParameter("new_std_q", "STRING", new_std_q),
            ]
        ),
    )
    job.result()
    affected = job.num_dml_affected_rows or 0
    _log_history(
        client,
        "audit_reassign_limb",
        affected,
        raw_question=raw_question,
        new_std_question=new_std_q,
    )
    return affected


@st.cache_data(ttl=60)
def _preview_conditional_split(_client, raw_question: str, regex_pattern: str) -> dict:
    """Read-only: count how many rows of `raw_question` match a regex applied to
    `raw_answer_en`. Used to show the reviewer 'N matched, M unmatched' before any
    write. Returns {'matched': int, 'unmatched': int, 'manual': int, 'total': int}.

    The regex follows BigQuery RE2 syntax. Skips manually-assigned rows in the
    matched/unmatched counts so the preview matches what the write will actually do.
    """
    query = f"""
    SELECT
      COUNTIF(REGEXP_CONTAINS(raw_answer_en, @pattern)
              AND COALESCE(manually_assigned, FALSE) = FALSE) AS matched,
      COUNTIF(NOT REGEXP_CONTAINS(raw_answer_en, @pattern)
              AND COALESCE(manually_assigned, FALSE) = FALSE) AS unmatched,
      COUNTIF(manually_assigned = TRUE) AS manual,
      COUNT(*) AS total
    FROM `{COLUMNAR_COMPLETED}`
    WHERE raw_question_en = @raw_q
      AND raw_answer_en IS NOT NULL
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("raw_q", "STRING", raw_question),
            bigquery.ScalarQueryParameter("pattern", "STRING", regex_pattern),
        ]
    )
    rows = list(_client.query(query, job_config=job_config))
    if not rows:
        return {"matched": 0, "unmatched": 0, "manual": 0, "total": 0}
    r = rows[0]
    return {
        "matched": int(r.matched or 0),
        "unmatched": int(r.unmatched or 0),
        "manual": int(r.manual or 0),
        "total": int(r.total or 0),
    }


def _conditional_assign_by_answer(
    client,
    raw_question: str,
    regex_pattern: str,
    match_std_q: str,
    nomatch_std_q: str | None,
) -> tuple[int, int]:
    """Assign std_q based on whether raw_answer_en matches `regex_pattern`.

    Rows matching the regex get `match_std_q`. Non-matching rows get
    `nomatch_std_q`; if nomatch_std_q is None, non-matching rows are left untouched
    (useful for "assign just the emails" without overwriting the others).

    Skips manually-assigned rows always (matches the rest of the audit page's
    policy). All written rows are flagged manually_assigned = TRUE so the ETL
    won't undo them. Returns (matched_rows_updated, unmatched_rows_updated).
    """
    # Two separate UPDATEs are clearer (and easier to audit) than one CASE-based
    # UPDATE, and BigQuery's pricing here is identical -- both scan the partition once.
    match_query = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_question = @std_q,
        question_match_method = 'manual',
        manually_assigned = TRUE,
        manually_assigned_at = CURRENT_TIMESTAMP(),
        manually_assigned_by = 'streamlit_app'
    WHERE raw_question_en = @raw_q
      AND raw_answer_en IS NOT NULL
      AND REGEXP_CONTAINS(raw_answer_en, @pattern)
      AND COALESCE(manually_assigned, FALSE) = FALSE
    """
    job1 = client.query(
        match_query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("raw_q", "STRING", raw_question),
                bigquery.ScalarQueryParameter("pattern", "STRING", regex_pattern),
                bigquery.ScalarQueryParameter("std_q", "STRING", match_std_q),
            ]
        ),
    )
    job1.result()
    matched = job1.num_dml_affected_rows or 0

    unmatched = 0
    if nomatch_std_q:
        nomatch_query = f"""
        UPDATE `{COLUMNAR_COMPLETED}`
        SET standardized_question = @std_q,
            question_match_method = 'manual',
            manually_assigned = TRUE,
            manually_assigned_at = CURRENT_TIMESTAMP(),
            manually_assigned_by = 'streamlit_app'
        WHERE raw_question_en = @raw_q
          AND raw_answer_en IS NOT NULL
          AND NOT REGEXP_CONTAINS(raw_answer_en, @pattern)
          AND COALESCE(manually_assigned, FALSE) = FALSE
        """
        job2 = client.query(
            nomatch_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("raw_q", "STRING", raw_question),
                    bigquery.ScalarQueryParameter("pattern", "STRING", regex_pattern),
                    bigquery.ScalarQueryParameter("std_q", "STRING", nomatch_std_q),
                ]
            ),
        )
        job2.result()
        unmatched = job2.num_dml_affected_rows or 0

    _log_history(
        client,
        "audit_conditional_split",
        matched + unmatched,
        raw_question=raw_question,
        new_std_question=f"{match_std_q} | {nomatch_std_q or '(unmatched untouched)'}",
    )
    return (matched, unmatched)


# Pre-built common conditional-split rules. Each entry: (label, regex, suggested_match_std_q, suggested_nomatch_std_q).
# `suggested_*` are hints for the dropdown defaults; the reviewer can override.
CONDITIONAL_SPLIT_PRESETS = [
    ("Email vs. name (contains '@')", r"@", "email", "full_name"),
    (
        "Phone number (contains digits + dashes)",
        r"^[\d\s\-\(\)\+]+$",
        "phone_number",
        None,
    ),
    ("Numeric only", r"^[0-9.]+$", "numeric_value", None),
    ("URL (http/https)", r"^https?://", "url", None),
    ("Date (YYYY-MM-DD)", r"^\d{4}-\d{2}-\d{2}", "date", None),
]


def _render_near_duplicates_panel(
    client, raw_question: str, all_std_qs: list[str], key_prefix: str
):
    """Surface text-similar unmapped questions so the reviewer can map a wording
    cluster in one click. RapidFuzz token_sort_ratio only -- no ML model load.

    Cheap to compute: just runs fuzz against the cached unmapped-questions list,
    which is already in memory.
    """
    with st.expander(
        "Find near-duplicates of this question for bulk mapping", expanded=False
    ):
        st.caption(
            "Surface other unmapped questions whose text is similar to this one "
            "(typos, wording variants). Pick a std_q once and apply to every "
            "checked match."
        )

        threshold = st.slider(
            "Similarity threshold (RapidFuzz token_sort_ratio)",
            70,
            100,
            90,
            1,
            key=f"{key_prefix}_thresh",
            help="90 catches typo variants. Lower it to surface looser matches.",
        )

        unmapped = get_unmapped_questions(client)
        candidates = unmapped["raw_question_en"].tolist()
        # Use the existing fuzzy helper; threshold=0 to retrieve all then filter.
        matches = get_fuzzy_question_matches(
            raw_question, candidates, top_n=50, threshold=threshold
        )
        # Drop self-match (always returns 100% against itself).
        matches = [(q, s, i) for (q, s, i) in matches if q != raw_question]

        if not matches:
            st.info(
                f"No near-duplicates at >= {threshold}% similarity. "
                f"Lower the threshold to widen the search."
            )
            return

        st.write(
            f"**{len(matches)}** similar unmapped question(s) found at "
            f">= {threshold}% similarity."
        )

        # Index unmapped row counts for display.
        rowcount_by_q = dict(zip(unmapped["raw_question_en"], unmapped["row_count"]))

        # Checkbox list -- each row is its own widget so state is preserved.
        selected_keys: list[str] = []
        for q, score, _ in matches:
            cb_key = f"{key_prefix}_pick_{_stable_key(q)}"
            cols = st.columns([1, 6, 1, 1])
            with cols[0]:
                checked = st.checkbox(
                    "select",
                    key=cb_key,
                    value=True,
                    label_visibility="collapsed",
                )
            with cols[1]:
                st.write(f"`{q[:90]}{'...' if len(q) > 90 else ''}`")
            with cols[2]:
                st.caption(f"{rowcount_by_q.get(q, 0):,} rows")
            with cols[3]:
                st.caption(f"{int(score)}%")
            if checked:
                selected_keys.append(q)

        if not selected_keys:
            st.caption("No matches selected.")
            return

        st.markdown("---")
        st.write(
            f"Selected: **{len(selected_keys)}** question(s) (plus the current one)."
        )

        c1, c2 = st.columns([3, 1])
        with c1:
            target_std_q = st.selectbox(
                "Apply std_q to all selected (including the current question)",
                [""] + all_std_qs,
                key=f"{key_prefix}_target",
                label_visibility="collapsed",
            )
        with c2:
            apply_clicked = st.button(
                "Apply to all",
                key=f"{key_prefix}_apply",
                type="primary",
                disabled=(not target_std_q),
            )

        if apply_clicked and target_std_q:
            import uuid

            batch_id = f"nearbulk_{uuid.uuid4().hex[:10]}"
            ok = 0
            # Include the current raw_question in the batch.
            for rq in [raw_question] + selected_keys:
                if add_question_mapping(
                    client,
                    rq,
                    target_std_q,
                    match_type="manual",
                    batch_id=batch_id,
                ):
                    ok += 1
            st.toast(
                f"Mapped {ok} similar question(s) -> '{target_std_q}'. "
                f"Undo via 'Undo last batch'.",
                icon="✅",
            )
            st.cache_data.clear()


def render_conditional_split_panel(
    client, raw_question: str, all_std_qs: list[str], key_prefix: str
):
    """Render the 'split by raw_answer pattern' UI for one raw_question.

    Reusable from both the Audit page and the Unmapped Questions detail panel.
    Picks a preset (or custom regex), shows a live row-count preview, lets the
    reviewer choose match/non-match std_qs (or leave non-match untouched), then
    applies in a single click. No confirm popup; toast feedback; no st.rerun().

    Args:
        client: BigQuery client.
        raw_question: the raw_question_en this split acts on.
        all_std_qs: list of existing standardized_questions for dropdown options.
        key_prefix: unique widget-key prefix so multiple panels can coexist on the page.
    """
    with st.expander("Split rows by raw_answer pattern (e.g. emails vs names)"):
        st.caption(
            "Assign different std_questions to rows of this raw_question based on "
            "what's in the raw_answer_en. Useful for free-text fields where the "
            "answer content determines the canonical question."
        )

        preset_labels = ["(custom regex)"] + [p[0] for p in CONDITIONAL_SPLIT_PRESETS]
        preset_key = f"{key_prefix}_split_preset"
        pattern_key = f"{key_prefix}_split_pattern"
        match_key = f"{key_prefix}_split_match"
        nomatch_key = f"{key_prefix}_split_nomatch"
        last_preset_key = f"{key_prefix}_split_last_preset"

        preset_choice = st.selectbox("Preset", preset_labels, key=preset_key)

        # When the preset selection CHANGES, write the preset's defaults into
        # session_state for the dependent widgets BEFORE they render. Streamlit
        # ignores value=/index= once a widget has stored key state, so this is
        # the correct pattern for "preset that prefills other inputs".
        last_preset = st.session_state.get(last_preset_key)
        if preset_choice != last_preset:
            st.session_state[last_preset_key] = preset_choice
            if preset_choice == "(custom regex)":
                st.session_state[pattern_key] = ""
                st.session_state[match_key] = ""
                st.session_state[nomatch_key] = "(leave untouched)"
            else:
                preset = next(
                    p for p in CONDITIONAL_SPLIT_PRESETS if p[0] == preset_choice
                )
                st.session_state[pattern_key] = preset[1]
                st.session_state[match_key] = (
                    preset[2] if preset[2] in all_std_qs else ""
                )
                st.session_state[nomatch_key] = (
                    preset[3]
                    if preset[3] and preset[3] in all_std_qs
                    else "(leave untouched)"
                )

        pattern = st.text_input(
            "Regex (BigQuery RE2 syntax)",
            key=pattern_key,
            help="Tested against raw_answer_en. Examples: '@' for emails, '^[0-9.]+$' for numerics.",
        )

        c1, c2 = st.columns(2)
        with c1:
            std_options = [""] + all_std_qs
            match_std_q = st.selectbox(
                "std_q for MATCHING rows", std_options, key=match_key
            )
        with c2:
            nomatch_options = ["(leave untouched)"] + all_std_qs
            nomatch_pick = st.selectbox(
                "std_q for NON-MATCHING rows",
                nomatch_options,
                key=nomatch_key,
            )
            nomatch_std_q = (
                None if nomatch_pick == "(leave untouched)" else nomatch_pick
            )

        # Preview row counts when there's a pattern AND a match std_q.
        if pattern and match_std_q:
            preview = _preview_conditional_split(client, raw_question, pattern)
            preview_msg = (
                f"**Preview:** {preview['matched']:,} match -> `{match_std_q}`, "
                f"{preview['unmatched']:,} no match -> "
                f"{'`' + nomatch_std_q + '`' if nomatch_std_q else '(untouched)'}, "
                f"{preview['manual']:,} manually-assigned (skipped)."
            )
            st.info(preview_msg)

            apply_disabled = preview["matched"] == 0 and (
                nomatch_std_q is None or preview["unmatched"] == 0
            )
            if st.button(
                "Apply split",
                key=f"{key_prefix}_split_apply",
                type="primary",
                disabled=apply_disabled,
                help="Runs two targeted UPDATEs and flags affected rows manually_assigned=TRUE.",
            ):
                matched, unmatched = _conditional_assign_by_answer(
                    client, raw_question, pattern, match_std_q, nomatch_std_q
                )
                if nomatch_std_q:
                    st.toast(
                        f"Split applied: {matched} -> '{match_std_q}', "
                        f"{unmatched} -> '{nomatch_std_q}'.",
                        icon="✅",
                    )
                else:
                    st.toast(
                        f"Split applied: {matched} -> '{match_std_q}', "
                        f"non-matching rows left untouched.",
                        icon="✅",
                    )
                st.cache_data.clear()
        else:
            st.caption(
                "Enter a regex AND pick a std_q for matching rows to see a preview."
            )


JUNK_DRAWER_RAW_THRESHOLD = 10
FREE_TEXT_SINGLETON_RATIO = 0.5


@st.cache_data(ttl=300, show_spinner=False)
def _get_junk_drawer_canonicals(_client) -> pd.DataFrame:
    """Standardized questions absorbing many distinct raw_question_en values.
    Possible sign of a junk-drawer canonical that should be split."""
    free_text = list(FREE_TEXT_QUESTIONS)
    query = f"""
    SELECT
      standardized_question,
      COUNT(DISTINCT raw_question_en) AS distinct_raws,
      COUNT(*) AS rows_affected,
      COUNT(DISTINCT survey_id) AS survey_count,
      ARRAY_AGG(DISTINCT raw_question_en IGNORE NULLS LIMIT 5) AS sample_raws
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_question IS NOT NULL
      AND raw_question_en IS NOT NULL
      AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
    GROUP BY standardized_question
    HAVING distinct_raws >= @threshold
    ORDER BY rows_affected DESC
    LIMIT 20
    """
    return _client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("free_text_qs", "STRING", free_text),
                bigquery.ScalarQueryParameter(
                    "threshold", "INT64", JUNK_DRAWER_RAW_THRESHOLD
                ),
            ]
        ),
    ).to_dataframe()


@st.cache_data(ttl=300, show_spinner=False)
def _get_answer_punctuation_duplicates(_client) -> pd.DataFrame:
    """Standardized answers that normalize to the same canonical string but
    exist under multiple raw spellings (e.g. 'Very Interested' vs.
    'Very interested.')."""
    free_text = list(FREE_TEXT_QUESTIONS)
    query = f"""
    WITH normalized AS (
      SELECT
        standardized_question,
        standardized_answer,
        REGEXP_REPLACE(
          REGEXP_REPLACE(LOWER(TRIM(standardized_answer)), r'[.!?,;:\\s]+$', ''),
          r'\\s+', ' '
        ) AS norm_answer,
        COUNT(*) AS row_count
      FROM `{COLUMNAR_COMPLETED}`
      WHERE standardized_question IS NOT NULL
        AND standardized_answer IS NOT NULL
        AND TRIM(standardized_answer) != ''
        AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
      GROUP BY 1, 2, 3
    )
    SELECT
      standardized_question,
      norm_answer,
      COUNT(DISTINCT standardized_answer) AS variant_count,
      SUM(row_count) AS rows_affected,
      ARRAY_AGG(
        STRUCT(standardized_answer, row_count) ORDER BY row_count DESC
      ) AS variants
    FROM normalized
    GROUP BY 1, 2
    HAVING variant_count > 1
    ORDER BY rows_affected DESC
    LIMIT 50
    """
    return _client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("free_text_qs", "STRING", free_text),
            ]
        ),
    ).to_dataframe()


@st.cache_data(ttl=300, show_spinner=False)
def _get_freetext_contamination(_client) -> pd.DataFrame:
    """Standardized questions NOT in FREE_TEXT_QUESTIONS where most of the
    answer values appear only once. High singleton ratio = answers behave
    like free text and probably should be on the free-text list."""
    free_text = list(FREE_TEXT_QUESTIONS)
    query = f"""
    WITH per_answer AS (
      SELECT
        standardized_question,
        standardized_answer,
        COUNT(*) AS row_count
      FROM `{COLUMNAR_COMPLETED}`
      WHERE standardized_question IS NOT NULL
        AND standardized_answer IS NOT NULL
        AND TRIM(standardized_answer) != ''
        AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
      GROUP BY 1, 2
    )
    SELECT
      standardized_question,
      COUNT(*) AS distinct_answers,
      SUM(row_count) AS rows_affected,
      COUNTIF(row_count = 1) AS singleton_answers,
      SAFE_DIVIDE(COUNTIF(row_count = 1), COUNT(*)) AS singleton_ratio
    FROM per_answer
    GROUP BY standardized_question
    HAVING singleton_ratio > @singleton_ratio
       AND distinct_answers >= 20
    ORDER BY rows_affected DESC
    LIMIT 20
    """
    return _client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("free_text_qs", "STRING", free_text),
                bigquery.ScalarQueryParameter(
                    "singleton_ratio", "FLOAT64", FREE_TEXT_SINGLETON_RATIO
                ),
            ]
        ),
    ).to_dataframe()


def _show_audit_structural_qa(client):
    """Four detectors that surface taxonomy-shape problems coverage % can't
    see: junk-drawer canonicals, answer punctuation duplicates, free-text
    contamination, and lookup drift. Each section starts collapsed; the
    headline number on the expander tells you whether it's worth opening.
    All queries are cached for 5 minutes."""
    st.caption(
        "Structural problems coverage % can't see. Counts cached 5 min; "
        "press R to refresh."
    )

    # ----- Detector 1: junk-drawer canonicals -----
    with st.container(border=True):
        df = _get_junk_drawer_canonicals(client)
        n_flagged = len(df)
        total_rows = int(df["rows_affected"].sum()) if not df.empty else 0
        with st.expander(f"Junk-drawer canonicals ({n_flagged})", expanded=False):
            st.caption(
                f"Standardized questions with >= {JUNK_DRAWER_RAW_THRESHOLD} "
                "distinct raw_question_en values feeding them. Often a sign "
                "the canonical absorbed too many things and should be split."
            )
            if df.empty:
                st.success("None.")
            else:
                c1, c2 = st.columns(2)
                c1.metric("Flagged canonicals", n_flagged)
                c2.metric("Rows under flagged canonicals", f"{total_rows:,}")

                # Render sample_raws (an ARRAY_AGG result -> numpy array)
                # compactly. Use a length check rather than `xs or []` because
                # `arr or default` invokes bool(arr) which raises on
                # multi-element arrays ("truth value is ambiguous").
                def _format_sample_raws(xs):
                    if xs is None or len(xs) == 0:
                        return ""
                    return "; ".join(
                        (s[:60] + "..." if len(s) > 60 else s) for s in list(xs)[:3]
                    )

                display = df.copy()
                display["sample_raws"] = display["sample_raws"].apply(
                    _format_sample_raws
                )
                st.dataframe(display, use_container_width=True, hide_index=True)
                st.caption(
                    "Action: Lookup Tables > Edit/Split/Merge > Split a " "canonical."
                )

    # ----- Detector 2: answer punctuation duplicates -----
    with st.container(border=True):
        df = _get_answer_punctuation_duplicates(client)
        n_groups = len(df)
        total_rows = int(df["rows_affected"].sum()) if not df.empty else 0
        with st.expander(f"Answer duplicates ({n_groups})", expanded=False):
            st.caption(
                "Standardized answers that normalize to the same string but "
                "exist under multiple variants (e.g. 'Very Interested' vs. "
                "'Very interested.'). Consolidate via Edit / Split / Merge > "
                "Merge."
            )
            if df.empty:
                st.success("None.")
            else:
                c1, c2 = st.columns(2)
                c1.metric("Duplicate groups", n_groups)
                c2.metric("Rows affected", f"{total_rows:,}")

                # Same numpy-array-truthiness pitfall as Detector 1: do not
                # use `xs or []`; length-check explicitly.
                def _format_variants(xs):
                    if xs is None or len(xs) == 0:
                        return ""
                    return "; ".join(
                        f"{v['standardized_answer']} ({int(v['row_count'])})"
                        for v in list(xs)
                    )

                display = df.copy()
                display["variants_pretty"] = display["variants"].apply(_format_variants)
                st.dataframe(
                    display[
                        [
                            "standardized_question",
                            "norm_answer",
                            "variant_count",
                            "rows_affected",
                            "variants_pretty",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
                st.caption(
                    "Action: Pick the canonical spelling, then "
                    "Lookup Tables > Edit/Split/Merge > Merge."
                )

    # ----- Detector 3: free-text contamination -----
    with st.container(border=True):
        df = _get_freetext_contamination(client)
        n_flagged = len(df)
        total_rows = int(df["rows_affected"].sum()) if not df.empty else 0
        with st.expander(f"Free-text contamination ({n_flagged})", expanded=False):
            st.caption(
                f"Canonicals NOT in FREE_TEXT_QUESTIONS where more than "
                f"{int(FREE_TEXT_SINGLETON_RATIO * 100)}% of answers appear "
                "only once. Behaves like free text -- probably should be on "
                "the free-text list."
            )
            if df.empty:
                st.success("None.")
            else:
                c1, c2 = st.columns(2)
                c1.metric("Suspect canonicals", n_flagged)
                c2.metric("Rows under suspect canonicals", f"{total_rows:,}")
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(
                    "Action: If genuinely open-ended, add to "
                    "FREE_TEXT_QUESTIONS in the mapper AND "
                    "shared/limesurvey_process_columnar.py "
                    "AND sql/sync_standardized_lookups.sql (3 places, kept "
                    "in sync)."
                )

    # ----- Detector 4: lookup drift -----
    with st.container(border=True):
        st.subheader("Lookup drift")
        st.caption(
            "Lookups should mirror the fact table. These counts are also "
            "shown by the 'Sync from Data' panel on the Lookup Tables page; "
            "the action lives there."
        )
        try:
            counts = get_lookup_drift_counts(client)
        except Exception as e:
            st.warning(f"Failed to load drift counts: {e}")
            counts = None

        if counts:
            c1, c2 = st.columns(2)
            c1.metric("Questions in lookup, unused in data", counts["lookup_q_unused"])
            c2.metric("Answers in lookup, unused in data", counts["lookup_a_unused"])
            c3, c4 = st.columns(2)
            c3.metric(
                "Questions in data, missing from lookup", counts["data_q_missing"]
            )
            c4.metric("Answers in data, missing from lookup", counts["data_a_missing"])
            with st.expander("Show orphan lookup rows", expanded=False):
                col_a, col_b = st.columns(2)
                col_a.markdown("**Unused questions**")
                try:
                    col_a.dataframe(get_unused_standardized_questions(client))
                except Exception as e:
                    col_a.warning(str(e))
                col_b.markdown("**Unused answers**")
                try:
                    col_b.dataframe(get_unused_standardized_answers(client))
                except Exception as e:
                    col_b.warning(str(e))
            st.caption(
                "Action: Lookup Tables > Edit/Split/Merge > "
                "Sync from Data > Add missing / Remove unused."
            )


def show_audit(client):
    """Surface suspicious classifications and structural problems for review.

    Two tabs:
    - **Split-brain candidates**: distinct raw_questions where the same text has
      been mapped to multiple standardized_questions. Excludes
      manually_assigned=TRUE rows (presumed correct). Auto-hides 'matrix-like'
      splits (all std_qs share a single prefix family) since those are
      legitimate LimeSurvey matrix subquestions.
    - **Structural QA**: four detectors that surface taxonomy-shape problems
      coverage % cannot see -- junk-drawer canonicals, answer-string
      duplicates, free-text contamination, and lookup drift.
    """
    st.header("Audit")
    st.caption(
        "Review classifications and structural issues that coverage % cannot "
        "see. Manually-assigned rows are excluded from the split-brain queue."
    )

    if "success_msg" in st.session_state:
        st.success(st.session_state.pop("success_msg"))

    tab_split, tab_qa = st.tabs(["Split-brain Candidates", "Structural QA"])

    with tab_qa:
        _show_audit_structural_qa(client)

    with tab_split:
        _show_audit_split_brain(client)


def _show_audit_split_brain(client):
    """Existing split-brain queue, factored out of show_audit when the page
    grew tabs."""
    # --- Section 1: Unmapped (link out) ---
    null_count = get_coverage_stats(client)["unmapped_questions"]
    if null_count > 0:
        st.info(
            f"There are **{null_count:,}** rows with NULL standardized_question. "
            f"Use the 'Unmapped Questions' page to triage them individually."
        )

    st.divider()

    # --- Section 2: Split-brain candidates ---
    st.subheader("Split-brain Candidates")
    st.caption(
        "Raw questions where rows have been split across multiple standardized "
        "questions. 'Matrix' badge = all targets share a single prefix family "
        "(e.g. fatigue_level_*); these are usually legitimate matrix "
        "subquestions and are hidden by default."
    )

    df = get_split_brain_candidates(client)
    if df.empty:
        st.success(
            "No split-brain candidates -- every raw_question maps to a single std_question."
        )
        return

    n_matrix = int(df["matrix_like"].sum())
    n_suspect = int((~df["matrix_like"]).sum())

    show_matrix = st.checkbox(
        f"Also show {n_matrix} matrix-like splits (likely legitimate)",
        value=False,
        key="audit_show_matrix",
        help="A split is 'matrix-like' when every standardized_question shares the same single-word prefix family (e.g. 'fatigue_*' or 'progression_*'). These are usually legitimate matrix subquestions in LimeSurvey.",
    )

    visible = df if show_matrix else df[~df["matrix_like"]].reset_index(drop=True)

    if visible.empty:
        st.success(
            "No suspicious splits. Toggle the checkbox above to view matrix-like splits."
        )
        return

    st.write(
        f"Showing **{len(visible)}** card(s) ({n_suspect} suspect, {n_matrix} matrix)."
    )

    # Load the standardized_questions dropdown once for all cards.
    std_questions_df = get_standardized_questions(client)
    all_std_qs = (
        std_questions_df["standardized_question"].tolist()
        if not std_questions_df.empty
        else []
    )
    NEW_STD_SENTINEL = "+ Create new..."

    for _, row in visible.iterrows():
        raw_q = row["raw_question_en"]
        breakdown = list(row["std_q_breakdown"])
        badge = "MATRIX (likely OK)" if row["matrix_like"] else "SUSPECT"
        color = "blue" if row["matrix_like"] else "red"
        card_total = int(row["total_rows"])
        card_manual = int(sum(int(item["manual_rows"]) for item in breakdown))

        with st.expander(
            f":{color}[{badge}]  ({row['n_std']} std_qs, {card_total:,} rows)  "
            f"\"{raw_q[:80]}{'...' if len(raw_q) > 80 else ''}\"",
            expanded=not row["matrix_like"] and card_total >= 1000,
        ):
            st.write(f"**Raw question:** {raw_q}")
            st.write(
                f"**Stats:** mapped to {row['n_std']} std_questions across "
                f"{row['n_distinct_prefix_families']} prefix families, "
                f"{card_total:,} rows total "
                f"({card_manual:,} manually-assigned)."
            )

            # ----- BULK-SET PANEL (whole card -> one std_q) -----
            k = _stable_key(raw_q)
            # Build the ranked option list once per card; reused by the per-limb pickers.
            ranked_options = _rank_audit_choices(raw_q, breakdown, all_std_qs)
            st.markdown("##### Bulk: assign ALL rows of this raw_question to one std_q")
            bulk_col1, bulk_col2 = st.columns([3, 1])
            with bulk_col1:
                # Append the create-new sentinel after the ranked list.
                bulk_options = ranked_options + [NEW_STD_SENTINEL]
                bulk_pick_display = st.selectbox(
                    "Target standardized_question",
                    bulk_options,
                    key=f"bulk_pick_{k}",
                    label_visibility="collapsed",
                    help=(
                        "★ = std_q already used on this card (most rows first). "
                        "🤖 = ML model's top suggestions for this raw_question."
                    ),
                )
                bulk_pick = _unwrap_audit_choice(bulk_pick_display)
            with bulk_col2:
                bulk_apply_clicked = st.button(
                    "Apply to all",
                    key=f"bulk_apply_{k}",
                    type="primary",
                    disabled=(
                        bulk_pick_display in ("", RANK_PREFIX_DIVIDER, NEW_STD_SENTINEL)
                    ),
                )

            if bulk_pick == NEW_STD_SENTINEL:
                with st.container(border=True):
                    st.caption(
                        "Create a new standardized_question and assign all rows to it."
                    )
                    cats_df = get_categories(client)
                    cat_list = (
                        cats_df["category"].tolist() if not cats_df.empty else ["other"]
                    )
                    nc1, nc2, nc3, nc4 = st.columns([2, 2, 2, 1])
                    with nc1:
                        new_name = st.text_input(
                            "name (snake_case)", key=f"bulk_new_name_{k}"
                        )
                    with nc2:
                        new_disp = st.text_input(
                            "display name", key=f"bulk_new_disp_{k}"
                        )
                    with nc3:
                        new_cat = st.selectbox(
                            "category", cat_list, key=f"bulk_new_cat_{k}"
                        )
                    with nc4:
                        st.write("")
                        create_and_apply = st.button(
                            "Create + apply",
                            key=f"bulk_create_apply_{k}",
                            type="primary",
                            disabled=not new_name,
                        )
                    if create_and_apply:
                        if add_standardized_question(
                            client, new_name, new_disp, new_cat
                        ):
                            affected = _bulk_assign_raw_question_to_std_q(
                                client, raw_q, new_name
                            )
                            st.toast(
                                f"Created '{new_name}' and bulk-assigned {affected} row(s).",
                                icon="✅",
                            )
                            st.cache_data.clear()

            if bulk_apply_clicked:
                affected = _bulk_assign_raw_question_to_std_q(client, raw_q, bulk_pick)
                st.toast(
                    f"Bulk-assigned {affected} row(s) -> '{bulk_pick}'.", icon="✅"
                )
                st.cache_data.clear()

            st.divider()

            # ----- CONDITIONAL SPLIT (by raw_answer pattern) -----
            render_conditional_split_panel(
                client, raw_q, all_std_qs, key_prefix=f"audit_{k}"
            )

            st.divider()

            # ----- PER-LIMB TABLE -----
            st.markdown("##### Per-limb breakdown and reassign")
            st.caption(
                "Each row is one (raw_question, std_q) pair. 'Matched by' shows which "
                "ETL layer(s) assigned it -- a forensic clue for hardening rules. "
                "Use the picker to reassign just this limb, or 'Null out' to send it "
                "back to the unmapped queue."
            )

            header = st.columns([3, 1, 1, 2, 3, 1])
            with header[0]:
                st.markdown("**std_q**")
            with header[1]:
                st.markdown("**rows**")
            with header[2]:
                st.markdown("**manual**")
            with header[3]:
                st.markdown("**matched by**")
            with header[4]:
                st.markdown("**reassign to...**")
            with header[5]:
                st.markdown("**actions**")

            for item in breakdown:
                std_q = item["std_q"]
                n_rows = int(item["rows_"])
                n_manual = int(item["manual_rows"])
                methods = list(item["methods"]) if item["methods"] is not None else []
                methods_str = ", ".join(sorted(methods)) if methods else "NULL"

                cols = st.columns([3, 1, 1, 2, 3, 1])
                with cols[0]:
                    st.code(std_q)
                with cols[1]:
                    st.write(f"{n_rows:,}")
                with cols[2]:
                    if n_manual > 0:
                        st.write(f":orange[{n_manual:,}]")
                    else:
                        st.write("0")
                with cols[3]:
                    st.caption(methods_str)
                with cols[4]:
                    limb_pick_display = st.selectbox(
                        "reassign",
                        ranked_options,
                        key=f"limb_pick_{k}_{_stable_key(std_q)}",
                        label_visibility="collapsed",
                        help=("★ = already on card. " "🤖 = ML suggestion."),
                    )
                    limb_pick = _unwrap_audit_choice(limb_pick_display)
                with cols[5]:
                    btn_key_reassign = f"limb_reassign_{k}_{_stable_key(std_q)}"
                    btn_key_null = f"limb_null_{k}_{_stable_key(std_q)}"
                    limb_apply_disabled = (
                        (not limb_pick)
                        or (limb_pick == std_q)
                        or (limb_pick_display == RANK_PREFIX_DIVIDER)
                    )
                    if st.button(
                        "Apply",
                        key=btn_key_reassign,
                        disabled=limb_apply_disabled,
                        help="Reassign this limb's rows to the picked std_q (manual rows stay).",
                    ):
                        affected = _reassign_limb_to_std_q(
                            client, raw_q, std_q, limb_pick
                        )
                        st.toast(
                            f"Reassigned {affected} row(s) '{std_q}' -> '{limb_pick}'.",
                            icon="✅",
                        )
                        st.cache_data.clear()
                    if st.button(
                        "Null",
                        key=btn_key_null,
                        help=(
                            "Revert these rows to standardized_question = NULL so they "
                            "re-enter the unmapped queue. Skips manually-assigned rows."
                        ),
                    ):
                        affected = _null_out_non_manual_assignments_for_std_q(
                            client, raw_q, std_q
                        )
                        st.toast(
                            f"Reverted {affected} row(s) of '{std_q}' to NULL.",
                            icon="✅",
                        )
                        st.cache_data.clear()


def _site_distribution(_client):
    """Live site distribution from the completed fact table."""
    q = f"""
    SELECT site, COUNT(*) AS rows_n, COUNT(DISTINCT survey_id) AS surveys
    FROM `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed`
    GROUP BY site
    ORDER BY rows_n DESC
    """
    return _client.query(q).to_dataframe()


def _load_site_overrides(_client):
    """All analyst override rows (survey_id -> site)."""
    q = f"""
    SELECT survey_id, site, survey_title, notes, updated_by, updated_at
    FROM `{PROJECT_ID}.{DATASET}.survey_site_overrides`
    ORDER BY survey_id
    """
    return _client.query(q).to_dataframe()


def _surveys_needing_review(_client):
    """Surveys whose site could not be determined from the title and have NO
    analyst override -- they fell back to 'General' (site_source =
    'fallback_general'). These are the surveys a human should confirm: a new
    survey with an unusual title lands here until someone either accepts
    'General' or adds an override. Restricted to surveys that actually have
    responses (so dormant/empty surveys don't create noise), newest first.
    """
    q = f"""
    SELECT
      m.survey_id,
      m.survey_title,
      COUNT(*)                       AS response_rows,
      MAX(c.submitdate)              AS latest_response
    FROM `{PROJECT_ID}.{DATASET}.survey_site_map` m
    JOIN `{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed` c
      ON CAST(c.survey_id AS INT64) = m.survey_id
    WHERE m.site_source = 'fallback_general'
    GROUP BY m.survey_id, m.survey_title
    ORDER BY latest_response DESC NULLS LAST, response_rows DESC
    """
    return _client.query(q).to_dataframe()


def _upsert_site_override(client, survey_id, site, survey_title, notes, email):
    """Insert-or-update a single override row by survey_id (parameterized)."""
    q = f"""
    MERGE `{PROJECT_ID}.{DATASET}.survey_site_overrides` T
    USING (SELECT @sid AS survey_id) S
    ON T.survey_id = S.survey_id
    WHEN MATCHED THEN UPDATE SET
        site = @site, survey_title = @title, notes = @notes,
        updated_by = @email, updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (survey_id, site, survey_title, notes, updated_by, updated_at)
        VALUES (@sid, @site, @title, @notes, @email, CURRENT_TIMESTAMP())
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("sid", "INT64", int(survey_id)),
            bigquery.ScalarQueryParameter("site", "STRING", site),
            bigquery.ScalarQueryParameter("title", "STRING", survey_title),
            bigquery.ScalarQueryParameter("notes", "STRING", notes),
            bigquery.ScalarQueryParameter("email", "STRING", email),
        ]
    )
    client.query(q, job_config=cfg).result()


def _delete_site_override(client, survey_id):
    q = f"DELETE FROM `{PROJECT_ID}.{DATASET}.survey_site_overrides` WHERE survey_id = @sid"
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("sid", "INT64", int(survey_id))]
    )
    client.query(q, job_config=cfg).result()


def _rebuild_site_map_and_backfill(client, backfill: bool):
    """Re-run the derive SQL (rebuild survey_site_map) and optionally backfill
    the completed fact table. Reads the same SQL files the pipeline runs so the
    UI and the nightly job stay in lockstep."""
    import os

    repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    derive_sql = open(
        os.path.join(repo_root, "sql", "limesurvey_derive_survey_site.sql"),
        encoding="utf-8",
    ).read()
    client.query(derive_sql).result()
    if backfill:
        backfill_sql = open(
            os.path.join(repo_root, "sql", "limesurvey_backfill_site.sql"),
            encoding="utf-8",
        ).read()
        client.query(backfill_sql).result()


def show_site_mapping(client):
    """Analyst editor for survey -> site mapping.

    site (brand / community) is derived from each survey's title: ~76% by
    keyword regex, the rest via the survey_site_overrides table edited here.
    After editing overrides, "Rebuild map" re-derives survey_site_map; tick
    "and backfill" to also rewrite the historical fact table immediately
    (otherwise the next pipeline run picks the change up)."""
    st.header("Site Mapping")
    st.caption(
        'The `site` column (brand / community, e.g. "Myasthenia Gravis (MG)") is '
        "derived from each survey's title. Most surveys map automatically by "
        "keyword; the overrides below handle the rest (generic surveys, "
        "multi-site gene-therapy, pharma-named, etc.). Precedence: **override → "
        "title keyword → 'General'**."
    )

    user = _require_authorized_user(client)
    email = user["email"]

    # "Needs Review" badge shows the count of unclassified surveys right on the
    # tab label, so analysts get a proactive nudge without running a query.
    review_df = _surveys_needing_review(client)
    review_n = len(review_df)
    review_label = f"Needs Review ({review_n})" if review_n else "Needs Review"

    tab_review, tab_dist, tab_overrides = st.tabs(
        [review_label, "Current Distribution", "Overrides"]
    )

    with tab_review:
        st.subheader("Surveys needing a site decision")
        st.caption(
            "These surveys have responses but their title did not match any "
            "known community, so they defaulted to **'General'** and have no "
            "override yet. Confirm each: if it really is cross-community, leave "
            "it; otherwise add an override (below) to assign the right site, then "
            "**Rebuild map**."
        )
        if review_n == 0:
            st.success(
                "Nothing to review -- every survey with responses maps to a community or a deliberate override."
            )
        else:
            st.warning(
                f"{review_n} survey(s) defaulted to 'General' and may need a site assigned."
            )
            st.dataframe(review_df, hide_index=True, width="stretch")
            st.caption(
                "To classify one: open the **Overrides** tab -> **Add a new "
                "override**, enter the Survey ID and the correct site."
            )

    with tab_dist:
        st.subheader("Live site distribution")
        st.caption("From lime_surveys_columnar_completed (the reporting fact table).")
        dist = _site_distribution(client)
        st.write(f"Distinct sites: {len(dist)}")
        st.dataframe(dist, hide_index=True, width="stretch")

    with tab_overrides:
        st.subheader("Survey → site overrides")
        st.caption(
            "Edit any cell, then **Save edits**. To add a new override use the "
            "panel below. Deleting a row lets the survey fall back to its title "
            "keyword (or 'General')."
        )
        ov = _load_site_overrides(client).reset_index(drop=True)
        st.write(f"Total overrides: {len(ov)}")

        edited = st.data_editor(
            ov,
            hide_index=True,
            width="stretch",
            num_rows="fixed",
            disabled=["updated_by", "updated_at"],
            key="site_override_editor",
        ).reset_index(drop=True)

        if st.button("Save edits", key="save_site_overrides"):
            changed = 0
            for i in range(len(ov)):
                o, n = ov.iloc[i], edited.iloc[i]
                if (
                    n["site"] != o["site"]
                    or n["survey_title"] != o["survey_title"]
                    or n["notes"] != o["notes"]
                ):
                    _upsert_site_override(
                        client,
                        n["survey_id"],
                        n["site"],
                        n["survey_title"] or "",
                        n["notes"] or "",
                        email,
                    )
                    changed += 1
            if changed:
                st.success(
                    f"Saved {changed} override edit(s). Rebuild the map to apply."
                )
                st.cache_data.clear()
            else:
                st.info("No changes detected.")

        with st.expander("➕ Add a new override"):
            new_sid = st.number_input("Survey ID", min_value=1, step=1, value=1)
            new_site = st.text_input("Site (e.g. 'Myasthenia Gravis (MG)', 'General')")
            new_title = st.text_input("Survey title (optional, for reference)")
            new_notes = st.text_input("Notes (optional)")
            if st.button("Add override", key="add_site_override"):
                if not new_site.strip():
                    st.error("Site is required.")
                else:
                    _upsert_site_override(
                        client,
                        new_sid,
                        new_site.strip(),
                        new_title.strip(),
                        new_notes.strip(),
                        email,
                    )
                    st.success(
                        f"Added override for survey {new_sid}. Rebuild the map to apply."
                    )
                    st.cache_data.clear()
                    st.rerun()

        with st.expander("🗑️ Delete an override"):
            if len(ov):
                del_sid = st.selectbox(
                    "Survey ID to delete",
                    options=list(ov["survey_id"]),
                    key="del_site_sid",
                )
                if st.button("Delete override", key="delete_site_override"):
                    _delete_site_override(client, del_sid)
                    st.success(
                        f"Deleted override for survey {del_sid}. Rebuild the map to apply."
                    )
                    st.cache_data.clear()
                    st.rerun()

    st.divider()
    st.subheader("Apply changes")
    st.caption(
        "Rebuild survey_site_map from the current overrides + title keywords. "
        "Tick **and backfill** to also rewrite the historical fact table now; "
        "otherwise the next pipeline run applies it."
    )
    also_backfill = st.checkbox("and backfill the fact table now", value=False)
    if st.button("Rebuild map", type="primary"):
        with st.spinner(
            "Rebuilding survey_site_map"
            + (" and backfilling..." if also_backfill else "...")
        ):
            _rebuild_site_map_and_backfill(client, also_backfill)
        st.success(
            "survey_site_map rebuilt."
            + (
                " Fact table backfilled."
                if also_backfill
                else " Run the pipeline (or tick backfill) to update the fact table."
            )
        )
        st.cache_data.clear()


def show_lookup_tables(client):
    """Show the lookup tables."""
    st.header("Lookup Tables")

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "Categories",
            "Standardized Questions",
            "Standardized Answers",
            "Edit / Split / Merge",
        ]
    )

    with tab1:
        st.subheader("Categories Lookup")
        st.caption(
            "Edit any cell in-place, then click 'Save edits'. Renaming a category key will cascade "
            "to `standardized_questions_lookup` — you'll see a confirmation step before it runs."
        )
        cat_df = get_categories(client).reset_index(drop=True)
        st.write(f"Total: {len(cat_df)}")

        edited_df = st.data_editor(
            cat_df,
            hide_index=True,
            width="stretch",
            num_rows="fixed",  # disallow add/delete here — use the Add panel below for inserts
            key="cat_editor",
        ).reset_index(drop=True)

        if st.button("Save edits"):
            # Compare row-by-row by position (num_rows='fixed' guarantees order stability)
            renames: list[tuple[str, str]] = []
            field_updates: list[dict] = []
            for i in range(len(cat_df)):
                orig = cat_df.iloc[i]
                new = edited_df.iloc[i]

                if new["category"] != orig["category"]:
                    renames.append((orig["category"], new["category"]))

                # Detect non-key field changes (compare against the NEW key, since rename happens first)
                if (
                    new["display_name"] != orig["display_name"]
                    or new["description"] != orig["description"]
                    or new["sort_order"] != orig["sort_order"]
                ):
                    field_updates.append(
                        {
                            "category": new["category"],
                            "display_name": new["display_name"],
                            "description": new["description"],
                            "sort_order": int(new["sort_order"]),
                        }
                    )

            if not renames and not field_updates:
                st.info("No changes detected.")
            elif renames:
                # Stage the renames in session state and show a confirmation dialog
                st.session_state["pending_cat_renames"] = renames
                st.session_state["pending_cat_field_updates"] = field_updates
                st.rerun()
            else:
                # Pure field updates — apply directly
                changed = sum(
                    1
                    for u in field_updates
                    if update_category(
                        client,
                        u["category"],
                        u["display_name"],
                        u["description"],
                        u["sort_order"],
                    )
                )
                if changed:
                    st.success(f"Saved {changed} edit{'s' if changed != 1 else ''}.")
                    st.cache_data.clear()
                    st.rerun()

        # Confirmation panel for pending category renames
        if st.session_state.get("pending_cat_renames"):
            renames = st.session_state["pending_cat_renames"]
            field_updates = st.session_state.get("pending_cat_field_updates", [])

            with st.container(border=True):
                st.warning(
                    "⚠️ Confirm category rename(s) — these cascade to standardized_questions:"
                )
                for old, new in renames:
                    affected = count_questions_in_category(client, old)
                    st.write(
                        f"• **`{old}`** → **`{new}`** (will update **{affected:,}** standardized question{'s' if affected != 1 else ''})"
                    )

                col_yes, col_no = st.columns(2)
                with col_yes:
                    if st.button("✅ Confirm and apply", type="primary"):
                        ok = True
                        for old, new in renames:
                            if not rename_category(client, old, new):
                                ok = False
                                break
                        if ok:
                            for u in field_updates:
                                update_category(
                                    client,
                                    u["category"],
                                    u["display_name"],
                                    u["description"],
                                    u["sort_order"],
                                )
                            st.session_state.pop("pending_cat_renames", None)
                            st.session_state.pop("pending_cat_field_updates", None)
                            st.success(
                                f"Applied {len(renames)} rename(s) and {len(field_updates)} field update(s)."
                            )
                            st.cache_data.clear()
                            st.rerun()
                with col_no:
                    if st.button("✖ Cancel"):
                        st.session_state.pop("pending_cat_renames", None)
                        st.session_state.pop("pending_cat_field_updates", None)
                        st.rerun()

        st.divider()
        st.subheader("Add New Category")
        col1, col2, col3 = st.columns(3)
        with col1:
            new_cat = st.text_input(
                "Category (snake_case)",
                key="new_cat",
                help="e.g., 'financial', 'mental_health'",
            )
        with col2:
            new_cat_display = st.text_input("Display Name", key="new_cat_display")
        with col3:
            new_cat_desc = st.text_input("Description", key="new_cat_desc")

        if st.button("Add Category"):
            if new_cat:
                if add_category(client, new_cat, new_cat_display, new_cat_desc):
                    st.success(f"Added category '{new_cat}'")
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.warning("Enter a category name")

    with tab2:
        st.subheader("Standardized Questions Lookup")
        st.caption(
            "Edit any cell, then click 'Save edits'. Renaming "
            "`standardized_question` cascades to every matching row in "
            "lime_surveys_columnar_completed -- you'll see a confirmation "
            "step before it runs."
        )
        df = get_standardized_questions(client)
        st.write(f"Total: {len(df)}")

        # Get categories from lookup table
        cat_df = get_categories(client)
        categories_list = cat_df["category"].tolist() if not cat_df.empty else ["other"]

        # Filter by category
        filter_categories = ["All"] + categories_list
        cat_filter = st.selectbox("Filter by category", filter_categories, key="q_cat")
        if cat_filter != "All":
            df = df[df["category"] == cat_filter]

        df = df.reset_index(drop=True)
        edited_df = st.data_editor(
            df,
            hide_index=True,
            width="stretch",
            num_rows="fixed",
            column_config={
                "category": st.column_config.SelectboxColumn(
                    "category",
                    options=categories_list,
                    required=True,
                ),
            },
            key="std_q_editor",
        ).reset_index(drop=True)

        if st.button("Save edits", key="std_q_save"):
            renames: list[tuple[str, str]] = []
            metadata_updates: list[dict] = []
            for i in range(len(df)):
                orig = df.iloc[i]
                new = edited_df.iloc[i]
                if new["standardized_question"] != orig["standardized_question"]:
                    renames.append(
                        (
                            orig["standardized_question"],
                            new["standardized_question"],
                        )
                    )
                if (
                    new["display_name"] != orig["display_name"]
                    or new["category"] != orig["category"]
                    or new["description"] != orig["description"]
                ):
                    # Compare against NEW key, since rename happens first.
                    metadata_updates.append(
                        {
                            "key": new["standardized_question"],
                            "display_name": new["display_name"],
                            "category": new["category"],
                            "description": new["description"],
                        }
                    )

            if not renames and not metadata_updates:
                st.info("No changes detected.")
            elif renames:
                st.session_state["pending_std_q_renames"] = renames
                st.session_state["pending_std_q_metadata"] = metadata_updates
                st.rerun()
            else:
                changed = sum(
                    1
                    for u in metadata_updates
                    if update_standardized_question_metadata(
                        client,
                        u["key"],
                        u["display_name"],
                        u["category"],
                        u["description"],
                    )
                )
                if changed:
                    st.success(f"Saved {changed} edit{'s' if changed != 1 else ''}.")
                    st.cache_data.clear()
                    st.rerun()

        # Confirmation panel for pending standardized_question renames.
        if st.session_state.get("pending_std_q_renames"):
            renames = st.session_state["pending_std_q_renames"]
            metadata_updates = st.session_state.get("pending_std_q_metadata", [])

            with st.container(border=True):
                st.warning(
                    "Confirm rename(s) -- these cascade to "
                    "lime_surveys_columnar_completed:"
                )
                for old, new in renames:
                    affected = count_questions_rows_affected(client, old)
                    st.write(
                        f"- **`{old}`** -> **`{new}`** (will update "
                        f"**{affected:,}** fact-table row"
                        f"{'s' if affected != 1 else ''})"
                    )

                col_yes, col_no = st.columns(2)
                with col_yes:
                    if st.button(
                        "Confirm and apply",
                        type="primary",
                        key="std_q_rename_confirm",
                    ):
                        ok = True
                        for old, new in renames:
                            try:
                                rename_standardized_question(client, old, new)
                            except Exception as e:
                                st.error(f"Rename {old} -> {new} failed: {e}")
                                ok = False
                                break
                        if ok:
                            for u in metadata_updates:
                                update_standardized_question_metadata(
                                    client,
                                    u["key"],
                                    u["display_name"],
                                    u["category"],
                                    u["description"],
                                )
                            st.session_state.pop("pending_std_q_renames", None)
                            st.session_state.pop("pending_std_q_metadata", None)
                            st.success(
                                f"Applied {len(renames)} rename(s) "
                                f"and {len(metadata_updates)} metadata update(s)."
                            )
                            st.cache_data.clear()
                            st.rerun()
                with col_no:
                    if st.button("Cancel", key="std_q_rename_cancel"):
                        st.session_state.pop("pending_std_q_renames", None)
                        st.session_state.pop("pending_std_q_metadata", None)
                        st.rerun()

        # Add new
        st.divider()
        st.subheader("Add New Standardized Question")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            new_q = st.text_input("Question (snake_case)", key="new_q")
        with col2:
            new_q_display = st.text_input("Display Name", key="new_q_display")
        with col3:
            new_q_cat = st.selectbox("Category", categories_list, key="new_q_cat")
        with col4:
            new_q_desc = st.text_input("Description", key="new_q_desc")

        # Inline validation: snake_case gate + near-duplicate warning.
        existing_qs = df["standardized_question"].tolist() if not df.empty else []
        name_ok = False
        if new_q:
            name_ok = _validate_canonical_name_inline(
                new_q, existing_qs, require_snake_case=True
            )

        if st.button("Add Question", disabled=not name_ok):
            if add_standardized_question(
                client, new_q, new_q_display, new_q_cat, new_q_desc
            ):
                st.success(f"Added '{new_q}'")
                st.cache_data.clear()
                st.rerun()

    with tab3:
        st.subheader("Standardized Answers Lookup")
        st.caption(
            "Edit any editable cell, then click 'Save edits'. Renaming "
            "`standardized_answer` cascades to every matching row in "
            "lime_surveys_columnar_completed -- you'll see a confirmation "
            "step before it runs. `standardized_question` is read-only here; "
            "to move an answer to a different question, use the Edit / "
            "Split / Merge tab."
        )
        df = get_standardized_answers(client)
        st.write(f"Total: {len(df)}")

        # Filter by question
        questions = (
            ["All"] + df["standardized_question"].unique().tolist()
            if not df.empty
            else ["All"]
        )
        q_filter = st.selectbox("Filter by question", questions, key="a_q")
        if q_filter != "All":
            df = df[df["standardized_question"] == q_filter]

        df = df.reset_index(drop=True)
        edited_df = st.data_editor(
            df,
            hide_index=True,
            width="stretch",
            num_rows="fixed",
            column_config={
                "standardized_question": st.column_config.TextColumn(
                    "standardized_question",
                    disabled=True,
                    help="Read-only here. To move an answer to a different question, use Edit / Split / Merge.",
                ),
                "sort_order": st.column_config.NumberColumn(
                    "sort_order", min_value=0, step=1
                ),
            },
            key="std_a_editor",
        ).reset_index(drop=True)

        if st.button("Save edits", key="std_a_save"):
            renames: list[tuple[str, str, str]] = []  # (std_q, old_a, new_a)
            metadata_updates: list[dict] = []
            for i in range(len(df)):
                orig = df.iloc[i]
                new = edited_df.iloc[i]
                if new["standardized_answer"] != orig["standardized_answer"]:
                    renames.append(
                        (
                            orig["standardized_question"],
                            orig["standardized_answer"],
                            new["standardized_answer"],
                        )
                    )
                # Sort_order may be NaN if user blanked it; coerce to int when set.
                try:
                    new_sort = (
                        int(new["sort_order"]) if pd.notna(new["sort_order"]) else None
                    )
                except (TypeError, ValueError):
                    new_sort = None
                try:
                    orig_sort = (
                        int(orig["sort_order"])
                        if pd.notna(orig["sort_order"])
                        else None
                    )
                except (TypeError, ValueError):
                    orig_sort = None
                if new["display_name"] != orig["display_name"] or new_sort != orig_sort:
                    metadata_updates.append(
                        {
                            "std_q": orig["standardized_question"],
                            # Use NEW std_a since rename runs first.
                            "std_a": new["standardized_answer"],
                            "display_name": new["display_name"],
                            "sort_order": new_sort,
                        }
                    )

            if not renames and not metadata_updates:
                st.info("No changes detected.")
            elif renames:
                st.session_state["pending_std_a_renames"] = renames
                st.session_state["pending_std_a_metadata"] = metadata_updates
                st.rerun()
            else:
                changed = sum(
                    1
                    for u in metadata_updates
                    if update_standardized_answer_metadata(
                        client,
                        u["std_q"],
                        u["std_a"],
                        u["display_name"],
                        u["sort_order"],
                    )
                )
                if changed:
                    st.success(f"Saved {changed} edit{'s' if changed != 1 else ''}.")
                    st.cache_data.clear()
                    st.rerun()

        # Confirmation panel for pending standardized_answer renames.
        if st.session_state.get("pending_std_a_renames"):
            renames = st.session_state["pending_std_a_renames"]
            metadata_updates = st.session_state.get("pending_std_a_metadata", [])

            with st.container(border=True):
                st.warning(
                    "Confirm answer rename(s) -- these cascade to "
                    "lime_surveys_columnar_completed:"
                )
                for std_q, old_a, new_a in renames:
                    affected = count_answers_rows_affected(client, std_q, old_a)
                    st.write(
                        f"- Under `{std_q}`: **`{old_a}`** -> **`{new_a}`** "
                        f"(will update **{affected:,}** fact-table row"
                        f"{'s' if affected != 1 else ''})"
                    )

                col_yes, col_no = st.columns(2)
                with col_yes:
                    if st.button(
                        "Confirm and apply",
                        type="primary",
                        key="std_a_rename_confirm",
                    ):
                        ok = True
                        for std_q, old_a, new_a in renames:
                            try:
                                rename_standardized_answer(client, std_q, old_a, new_a)
                            except Exception as e:
                                st.error(
                                    f"Rename {std_q}/{old_a} -> {new_a} failed: {e}"
                                )
                                ok = False
                                break
                        if ok:
                            for u in metadata_updates:
                                update_standardized_answer_metadata(
                                    client,
                                    u["std_q"],
                                    u["std_a"],
                                    u["display_name"],
                                    u["sort_order"],
                                )
                            st.session_state.pop("pending_std_a_renames", None)
                            st.session_state.pop("pending_std_a_metadata", None)
                            st.success(
                                f"Applied {len(renames)} rename(s) "
                                f"and {len(metadata_updates)} metadata update(s)."
                            )
                            st.cache_data.clear()
                            st.rerun()
                with col_no:
                    if st.button("Cancel", key="std_a_rename_cancel"):
                        st.session_state.pop("pending_std_a_renames", None)
                        st.session_state.pop("pending_std_a_metadata", None)
                        st.rerun()

        # Add new
        st.divider()
        st.subheader("Add New Standardized Answer")
        std_questions_df = get_standardized_questions(client)
        std_questions = (
            std_questions_df["standardized_question"].tolist()
            if not std_questions_df.empty
            else []
        )

        col1, col2, col3 = st.columns(3)
        with col1:
            ans_question = st.selectbox("For Question", std_questions, key="ans_q")
        with col2:
            new_ans = st.text_input("Answer Value", key="new_ans")
        with col3:
            new_ans_display = st.text_input("Display Name", key="new_ans_display")

        # Inline near-duplicate warning scoped to existing answers under the
        # selected question. snake_case is NOT required for answers (legitimate
        # values include 'N/A', 'Yes', 'Prefer not to answer').
        existing_ans = (
            df[df["standardized_question"] == ans_question][
                "standardized_answer"
            ].tolist()
            if ans_question and not df.empty
            else []
        )
        name_ok = bool(new_ans and ans_question)
        if new_ans and ans_question:
            name_ok = _validate_canonical_name_inline(
                new_ans, existing_ans, require_snake_case=False
            )

        if st.button("Add Answer", disabled=not name_ok):
            if add_standardized_answer(client, ans_question, new_ans, new_ans_display):
                st.success(f"Added '{new_ans}' for '{ans_question}'")
                st.cache_data.clear()
                st.rerun()

    with tab4:
        _render_lookup_edit_tab(client)


def _render_lookup_edit_tab(client):
    """Edit / Split / Merge / Sync from Data for the standardized lookup tables.

    All destructive operations gate behind confirmation panels that show the
    affected columnar_completed row count. Every write logs to mapping_history.
    Writes stamp manually_assigned_by='streamlit_app' so the analyst Sheet
    does not clobber the change on the next nightly run."""
    st.subheader("Edit / Split / Merge Standardized Lookups")
    st.caption(
        "Mutate the standardized_question / standardized_answer taxonomy. "
        "Every action shows the affected row count before applying. "
        "Undo is available via the Audit page."
    )

    scope = st.radio(
        "Scope", ["Questions", "Answers"], horizontal=True, key="lookup_edit_scope"
    )
    operation = st.radio(
        "Operation",
        ["Simple edit (rename)", "Split", "Merge", "Sync from Data"],
        horizontal=True,
        key="lookup_edit_op",
    )

    st.divider()

    if operation == "Simple edit (rename)":
        if scope == "Questions":
            _render_rename_question(client)
        else:
            _render_rename_answer(client)
    elif operation == "Split":
        if scope == "Questions":
            _render_split_question(client)
        else:
            _render_split_answer(client)
    elif operation == "Merge":
        if scope == "Questions":
            _render_merge_questions(client)
        else:
            _render_merge_answers(client)
    else:
        _render_sync_from_data(client)


def _question_picker(client, key: str, label: str = "Pick a standardized_question"):
    df = get_standardized_questions(client)
    choices = (
        sorted(df["standardized_question"].dropna().tolist()) if not df.empty else []
    )
    return st.selectbox(label, choices, key=key) if choices else None


def _answer_picker(
    client, std_question: str, key: str, label: str = "Pick a standardized_answer"
):
    df = get_standardized_answers(client, std_question)
    choices = (
        sorted(df["standardized_answer"].dropna().tolist()) if not df.empty else []
    )
    return st.selectbox(label, choices, key=key) if choices else None


def _show_canonical_context_question(client, std_question: str):
    """Live context for a standardized_question: row count, top raw values, surveys."""
    with st.container(border=True):
        st.caption("Current usage in completed table:")
        raws = get_raw_questions_under_std_q(client, std_question)
        if raws.empty:
            st.info("No rows in columnar_completed are mapped to this canonical.")
        else:
            total = int(raws["row_count"].sum())
            st.write(
                f"**{total:,}** rows across **{len(raws)}** distinct raw_question_en value(s)."
            )
            st.dataframe(raws.head(20), hide_index=True, width="stretch")
        return raws


def _show_canonical_context_answer(client, std_question: str, std_answer: str):
    """Live context for a (std_q, std_a) pair."""
    with st.container(border=True):
        st.caption("Current usage in completed table:")
        raws = get_raw_answers_under_std_a(client, std_question, std_answer)
        if raws.empty:
            st.info("No rows in columnar_completed are mapped to this canonical.")
        else:
            total = int(raws["row_count"].sum())
            st.write(
                f"**{total:,}** rows across **{len(raws)}** distinct raw_answer_en value(s)."
            )
            st.dataframe(raws.head(20), hide_index=True, width="stretch")
        return raws


def _render_rename_question(client):
    st.markdown("### Rename a standardized_question")
    st.caption(
        "Renames the key in standardized_questions_lookup, cascades to "
        "standardized_answers_lookup.standardized_question, and updates every "
        "matching row in lime_surveys_columnar_completed."
    )
    src = _question_picker(client, "rename_q_src")
    if not src:
        return
    _show_canonical_context_question(client, src)
    new_name = st.text_input("New name (snake_case)", key="rename_q_new")
    if not new_name:
        return
    if not _is_valid_snake_case(new_name):
        st.error(
            "Name must match [a-z][a-z0-9_]* -- lowercase, start with a letter, "
            "no spaces or punctuation."
        )
        return
    existing = get_standardized_questions(client)["standardized_question"].tolist()
    nd = _near_duplicate_canonicals(new_name, existing)
    if nd:
        st.warning(
            "Near-duplicate canonical(s) detected: "
            + ", ".join(f"`{m}` ({s})" for m, s in nd)
            + " -- did you mean to **merge** into one of these instead of rename?"
        )
    if st.button("Preview rename", type="primary", key="rename_q_btn"):
        st.session_state["pending_rename_q"] = (src, new_name)
        st.rerun()

    pending = st.session_state.get("pending_rename_q")
    if pending and pending[0] == src:
        old, new = pending
        with st.container(border=True):
            st.warning(f"Confirm rename: `{old}` -> `{new}`")
            st.caption(
                "This stamps the affected rows as manually_assigned='streamlit_app' "
                "so the analyst Sheet won't overwrite them."
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirm rename", type="primary", key="rename_q_confirm"):
                    affected = rename_standardized_question(client, old, new)
                    st.session_state.pop("pending_rename_q", None)
                    st.success(
                        f"Renamed `{old}` -> `{new}` ({affected:,} rows updated)."
                    )
                    st.cache_data.clear()
                    st.rerun()
            with c2:
                if st.button("Cancel", key="rename_q_cancel"):
                    st.session_state.pop("pending_rename_q", None)
                    st.rerun()


def _render_rename_answer(client):
    st.markdown("### Rename a standardized_answer")
    st.caption(
        "Renames the answer within a given standardized_question, in both the "
        "answers_lookup and in every matching row of columnar_completed."
    )
    std_q = _question_picker(client, "rename_a_q")
    if not std_q:
        return
    src = _answer_picker(client, std_q, "rename_a_src")
    if not src:
        return
    _show_canonical_context_answer(client, std_q, src)
    new_name = st.text_input("New answer value", key="rename_a_new")
    if not new_name:
        return
    if st.button("Preview rename", type="primary", key="rename_a_btn"):
        st.session_state["pending_rename_a"] = (std_q, src, new_name)
        st.rerun()

    pending = st.session_state.get("pending_rename_a")
    if pending and pending[0] == std_q and pending[1] == src:
        sq, old, new = pending
        with st.container(border=True):
            st.warning(f"Confirm rename within `{sq}`: `{old}` -> `{new}`")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirm rename", type="primary", key="rename_a_confirm"):
                    affected = rename_standardized_answer(client, sq, old, new)
                    st.session_state.pop("pending_rename_a", None)
                    st.success(
                        f"Renamed `{old}` -> `{new}` under `{sq}` ({affected:,} rows updated)."
                    )
                    st.cache_data.clear()
                    st.rerun()
            with c2:
                if st.button("Cancel", key="rename_a_cancel"):
                    st.session_state.pop("pending_rename_a", None)
                    st.rerun()


def _render_pattern_filter_panel(
    source_label: str,
    raws_df,
    value_col: str,
    key_prefix: str,
) -> list[str]:
    """Shared AI-first pattern panel for both split renderers.

    Flow:
      1. User describes the subset in plain English.
      2. 'Generate regex' calls Gemini -> writes RE2 regex into an editable field.
      3. Regex field is live-editable; preview table updates on every change.
      4. Preview table shows matched rows (with row_count, survey_count) so the
         user can SEE what would move before clicking Apply.
      5. 'Apply pattern to selection' sets the multi-select default for the
         caller.

    Returns the list of raw values currently selected by the pattern (empty
    until 'Apply pattern' is clicked).

    No BigQuery. Match is in-process Python (RE2 superset). Gemini calls
    cached per (source, description) in st.session_state -- identical
    re-clicks return instantly."""
    matched_state_key = f"{key_prefix}_pattern_matched"
    regex_field_key = f"{key_prefix}_regex"
    desc_field_key = f"{key_prefix}_ai_desc"

    raw_values = raws_df[value_col].tolist()

    with st.container(border=True):
        st.markdown("**Filter by pattern** (optional)")
        st.caption(
            "Describe what to split out in plain English. Gemini drafts a "
            "RE2 regex; the regex is editable and the preview updates live. "
            "Matching runs in-process; the apply step uses the explicit row "
            "list, so it's independent of the regex engine."
        )

        description = st.text_input(
            "What should move to the new canonical?",
            key=desc_field_key,
            placeholder="e.g. all questions about physical therapy",
        )

        c1, c2 = st.columns([1, 1])
        with c1:
            generate = st.button(
                "Generate regex", key=f"{key_prefix}_ai_btn", type="secondary"
            )
        with c2:
            clear = st.button("Clear regex", key=f"{key_prefix}_clear_btn")

        if generate:
            if not description.strip():
                st.warning("Type a description first.")
            else:
                suggested, reason = _gemini_suggest_split_regex(
                    source_label, raw_values, description
                )
                if suggested:
                    st.session_state[regex_field_key] = suggested
                    st.rerun()
                else:
                    st.warning(
                        f"Could not generate a regex: {reason} "
                        "Write the regex by hand below."
                    )
        if clear:
            st.session_state[regex_field_key] = ""
            st.session_state.pop(matched_state_key, None)
            st.rerun()

        regex = st.text_input(
            "Regex (editable -- refine the AI's draft or write your own)",
            key=regex_field_key,
            placeholder=r"e.g.  (?i).*physical\s+therapy.*",
        )

        matched: list[str] = []
        if regex:
            try:
                import re as _re

                _re.compile(regex)
            except _re.error as e:
                st.error(f"Invalid regex: {e}")
            else:
                matched = _regex_match_raw_values(regex, raw_values)
                st.caption(
                    f"Matches **{len(matched):,}** of {len(raw_values):,} raw values."
                )
                if matched:
                    preview_df = raws_df[raws_df[value_col].isin(matched)].head(50)
                    st.markdown("**Preview of matched rows** (top 50)")
                    st.dataframe(
                        preview_df, hide_index=True, width="stretch", height=240
                    )
                    if st.button(
                        "Apply pattern to selection",
                        key=f"{key_prefix}_apply_pattern",
                        type="primary",
                    ):
                        st.session_state[matched_state_key] = matched
                        st.success(
                            f"Preselected {len(matched):,} row(s) in the multi-select below."
                        )
                        st.rerun()
                else:
                    st.info(
                        "Regex compiles but matches nothing. Refine and the "
                        "preview will update."
                    )
        else:
            st.caption(
                "No regex yet. Generate one from a description above, or type "
                "your own to see a live preview."
            )

    return st.session_state.get(matched_state_key, [])


def _render_split_question(client):
    st.markdown("### Split a standardized_question")
    st.caption(
        "Pick the source canonical and the raw_question_en values that should "
        "move to a new canonical. Selected raw_questions get reassigned in "
        "columnar_completed; unselected ones stay where they are."
    )
    src = _question_picker(client, "split_q_src")
    if not src:
        return
    raws = get_raw_questions_under_std_q(client, src)
    if raws.empty:
        st.info("This canonical has no rows in columnar_completed. Nothing to split.")
        return
    st.dataframe(raws, hide_index=True, width="stretch")

    all_raw = raws["raw_question_en"].tolist()
    preselected = _render_pattern_filter_panel(
        source_label=src,
        raws_df=raws,
        value_col="raw_question_en",
        key_prefix="split_q",
    )
    to_move = st.multiselect(
        "raw_question_en values to move to the new canonical",
        options=all_raw,
        default=preselected,
        key="split_q_move",
    )
    new_name = st.text_input("New canonical name (snake_case)", key="split_q_new")
    if not to_move or not new_name:
        return
    if not _is_valid_snake_case(new_name):
        st.error("Name must match [a-z][a-z0-9_]*.")
        return

    moving_rows = int(raws[raws["raw_question_en"].isin(to_move)]["row_count"].sum())
    staying_rows = int(raws["row_count"].sum()) - moving_rows
    if st.button("Preview split", type="primary", key="split_q_btn"):
        st.session_state["pending_split_q"] = (
            src,
            new_name,
            to_move,
            moving_rows,
            staying_rows,
        )
        st.rerun()

    pending = st.session_state.get("pending_split_q")
    if pending and pending[0] == src:
        s, n, raws_to_move, mv, st_ = pending
        with st.container(border=True):
            st.warning(
                f"Confirm split: create `{n}`, move **{mv:,}** rows "
                f"(across {len(raws_to_move)} raw_question(s)); **{st_:,}** rows stay on `{s}`."
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirm split", type="primary", key="split_q_confirm"):
                    affected = split_standardized_question(client, s, n, raws_to_move)
                    st.session_state.pop("pending_split_q", None)
                    st.success(f"Split applied: {affected:,} rows moved to `{n}`.")
                    st.cache_data.clear()
                    st.rerun()
            with c2:
                if st.button("Cancel", key="split_q_cancel"):
                    st.session_state.pop("pending_split_q", None)
                    st.rerun()


def _render_split_answer(client):
    st.markdown("### Split a standardized_answer")
    st.caption(
        "Pick the source (std_q, std_a), then the raw_answer_en values to move "
        "to a new standardized_answer under the same standardized_question."
    )
    std_q = _question_picker(client, "split_a_q")
    if not std_q:
        return
    src = _answer_picker(client, std_q, "split_a_src")
    if not src:
        return
    raws = get_raw_answers_under_std_a(client, std_q, src)
    if raws.empty:
        st.info("No columnar rows are mapped to this answer. Nothing to split.")
        return
    st.dataframe(raws, hide_index=True, width="stretch")

    all_raw = raws["raw_answer_en"].tolist()
    preselected = _render_pattern_filter_panel(
        source_label=f"{std_q} / {src}",
        raws_df=raws,
        value_col="raw_answer_en",
        key_prefix="split_a",
    )
    to_move = st.multiselect(
        "raw_answer_en values to move",
        options=all_raw,
        default=preselected,
        key="split_a_move",
    )
    new_name = st.text_input("New standardized_answer value", key="split_a_new")
    if not to_move or not new_name:
        return

    moving_rows = int(raws[raws["raw_answer_en"].isin(to_move)]["row_count"].sum())
    staying_rows = int(raws["row_count"].sum()) - moving_rows
    if st.button("Preview split", type="primary", key="split_a_btn"):
        st.session_state["pending_split_a"] = (
            std_q,
            src,
            new_name,
            to_move,
            moving_rows,
            staying_rows,
        )
        st.rerun()

    pending = st.session_state.get("pending_split_a")
    if pending and pending[0] == std_q and pending[1] == src:
        sq, s, n, raws_to_move, mv, st_ = pending
        with st.container(border=True):
            st.warning(
                f"Confirm split under `{sq}`: create `{n}`, move **{mv:,}** rows; "
                f"**{st_:,}** rows stay on `{s}`."
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirm split", type="primary", key="split_a_confirm"):
                    affected = split_standardized_answer(client, sq, s, n, raws_to_move)
                    st.session_state.pop("pending_split_a", None)
                    st.success(f"Split applied: {affected:,} rows moved to `{n}`.")
                    st.cache_data.clear()
                    st.rerun()
            with c2:
                if st.button("Cancel", key="split_a_cancel"):
                    st.session_state.pop("pending_split_a", None)
                    st.rerun()


def _render_merge_questions(client):
    st.markdown("### Merge standardized_questions")
    st.caption(
        "Fold one or more source canonicals into a target. Source rows in "
        "columnar_completed get relabeled to the target; source rows in the "
        "lookup tables are deleted."
    )
    target = _question_picker(
        client, "merge_q_target", "Target standardized_question (keeps)"
    )
    if not target:
        return
    all_qs = get_standardized_questions(client)["standardized_question"].tolist()
    candidates = [q for q in sorted(all_qs) if q != target]
    sources = st.multiselect(
        "Source standardized_question(s) to fold INTO target (will be deleted)",
        options=candidates,
        key="merge_q_sources",
    )
    if not sources:
        return

    source_rows = {s: get_raw_questions_under_std_q(client, s) for s in sources}
    total_moving = sum(
        int(df["row_count"].sum()) if not df.empty else 0 for df in source_rows.values()
    )
    with st.container(border=True):
        st.caption("Effect of merge:")
        for s, df in source_rows.items():
            rc = int(df["row_count"].sum()) if not df.empty else 0
            st.write(f"- `{s}` -> `{target}` ({rc:,} rows)")
        st.write(f"**Total rows to relabel: {total_moving:,}**")

    if st.button("Preview merge", type="primary", key="merge_q_btn"):
        st.session_state["pending_merge_q"] = (target, sources, total_moving)
        st.rerun()

    pending = st.session_state.get("pending_merge_q")
    if pending and pending[0] == target and set(pending[1]) == set(sources):
        t, srcs, tm = pending
        with st.container(border=True):
            st.warning(
                f"Confirm merge: **{len(srcs)}** source canonical(s) folded into `{t}`, "
                f"**{tm:,}** rows relabeled, source rows deleted from lookups."
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirm merge", type="primary", key="merge_q_confirm"):
                    affected = merge_standardized_questions(client, t, srcs)
                    st.session_state.pop("pending_merge_q", None)
                    st.success(
                        f"Merged {len(srcs)} canonical(s) into `{t}` ({affected:,} rows)."
                    )
                    st.cache_data.clear()
                    st.rerun()
            with c2:
                if st.button("Cancel", key="merge_q_cancel"):
                    st.session_state.pop("pending_merge_q", None)
                    st.rerun()


def _render_merge_answers(client):
    st.markdown("### Merge standardized_answers")
    st.caption(
        "Within a single standardized_question, fold one or more source answers "
        "into a target answer."
    )
    std_q = _question_picker(client, "merge_a_q")
    if not std_q:
        return
    target = _answer_picker(
        client, std_q, "merge_a_target", "Target standardized_answer (keeps)"
    )
    if not target:
        return
    all_a = get_standardized_answers(client, std_q)["standardized_answer"].tolist()
    candidates = [a for a in sorted(all_a) if a != target]
    sources = st.multiselect(
        "Source standardized_answer(s) to fold INTO target",
        options=candidates,
        key="merge_a_sources",
    )
    if not sources:
        return

    source_rows = {s: get_raw_answers_under_std_a(client, std_q, s) for s in sources}
    total_moving = sum(
        int(df["row_count"].sum()) if not df.empty else 0 for df in source_rows.values()
    )
    with st.container(border=True):
        st.caption("Effect of merge:")
        for s, df in source_rows.items():
            rc = int(df["row_count"].sum()) if not df.empty else 0
            st.write(f"- `{s}` -> `{target}` ({rc:,} rows)")
        st.write(f"**Total rows to relabel: {total_moving:,}**")

    if st.button("Preview merge", type="primary", key="merge_a_btn"):
        st.session_state["pending_merge_a"] = (std_q, target, sources, total_moving)
        st.rerun()

    pending = st.session_state.get("pending_merge_a")
    if (
        pending
        and pending[0] == std_q
        and pending[1] == target
        and set(pending[2]) == set(sources)
    ):
        sq, t, srcs, tm = pending
        with st.container(border=True):
            st.warning(
                f"Confirm merge under `{sq}`: **{len(srcs)}** answer(s) folded into `{t}`, "
                f"**{tm:,}** rows relabeled."
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirm merge", type="primary", key="merge_a_confirm"):
                    affected = merge_standardized_answers(client, sq, t, srcs)
                    st.session_state.pop("pending_merge_a", None)
                    st.success(
                        f"Merged {len(srcs)} answer(s) into `{t}` ({affected:,} rows)."
                    )
                    st.cache_data.clear()
                    st.rerun()
            with c2:
                if st.button("Cancel", key="merge_a_cancel"):
                    st.session_state.pop("pending_merge_a", None)
                    st.rerun()


def _render_sync_from_data(client):
    st.markdown("### Sync Lookups from Data")
    st.caption(
        "The completed table is the source of truth. Additions sync "
        "automatically every night; this panel is for cleaning up canonicals "
        "that are no longer used, and for forcing a missing-add sync on demand."
    )
    if st.button("Scan for drift", key="sync_scan_btn"):
        st.session_state["lookup_drift"] = get_lookup_drift_counts(client)

    counts = st.session_state.get("lookup_drift")
    if not counts:
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Questions in lookup but unused in data", f"{counts['lookup_q_unused']:,}"
    )
    c2.metric("Answers in lookup but unused in data", f"{counts['lookup_a_unused']:,}")
    c3.metric(
        "Questions in data but missing from lookup", f"{counts['data_q_missing']:,}"
    )
    c4.metric(
        "Answers in data but missing from lookup", f"{counts['data_a_missing']:,}"
    )

    st.divider()

    st.markdown("**Add missing canonicals from data** (additive only, safe)")
    if counts["data_q_missing"] > 0 or counts["data_a_missing"] > 0:
        if st.button("Run sync now", type="primary", key="sync_run_btn"):
            q_added, a_added = force_sync_missing_to_lookup(client)
            st.success(
                f"Added {q_added:,} question(s) and {a_added:,} answer(s) to the lookup."
            )
            st.session_state["lookup_drift"] = get_lookup_drift_counts(client)
            st.cache_data.clear()
            st.rerun()
    else:
        st.info(
            "Lookups already contain every value the completed table uses. Nothing to add."
        )

    st.divider()

    st.markdown(
        "**Remove unused canonicals** (deletes from lookup; optionally NULLs fact rows)"
    )
    prune_scope = st.radio(
        "Prune which?",
        ["Questions", "Answers"],
        horizontal=True,
        key="prune_scope",
    )
    if prune_scope == "Questions":
        df = get_unused_standardized_questions(client)
        if df.empty:
            st.info("No unused questions in the lookup.")
        else:
            st.write(f"{len(df):,} unused question(s):")
            df["remove"] = False
            edited = st.data_editor(
                df,
                hide_index=True,
                width="stretch",
                key="prune_q_editor",
                column_config={"remove": st.column_config.CheckboxColumn("Remove?")},
            )
            to_remove = edited[edited["remove"]]["standardized_question"].tolist()
            if to_remove:
                st.warning(
                    f"About to remove **{len(to_remove)}** question(s) from the lookup."
                )
                if len(to_remove) > 100:
                    confirm_text = st.text_input(
                        "Type DELETE to confirm a bulk removal of more than 100 rows",
                        key="prune_q_confirm_text",
                    )
                    if confirm_text != "DELETE":
                        st.stop()
                if st.button("Remove selected", type="primary", key="prune_q_confirm"):
                    removed = 0
                    for q in to_remove:
                        delete_standardized_question(client, q, null_fact_rows=False)
                        removed += 1
                    st.success(f"Removed {removed:,} question(s) from the lookup.")
                    st.session_state["lookup_drift"] = get_lookup_drift_counts(client)
                    st.cache_data.clear()
                    st.rerun()
    else:
        df = get_unused_standardized_answers(client)
        if df.empty:
            st.info("No unused answers in the lookup.")
        else:
            st.write(f"{len(df):,} unused answer(s):")
            df["remove"] = False
            edited = st.data_editor(
                df,
                hide_index=True,
                width="stretch",
                key="prune_a_editor",
                column_config={"remove": st.column_config.CheckboxColumn("Remove?")},
            )
            to_remove_df = edited[edited["remove"]][
                ["standardized_question", "standardized_answer"]
            ]
            if not to_remove_df.empty:
                st.warning(
                    f"About to remove **{len(to_remove_df)}** answer(s) from the lookup."
                )
                if len(to_remove_df) > 100:
                    confirm_text = st.text_input(
                        "Type DELETE to confirm a bulk removal of more than 100 rows",
                        key="prune_a_confirm_text",
                    )
                    if confirm_text != "DELETE":
                        st.stop()
                if st.button("Remove selected", type="primary", key="prune_a_confirm"):
                    removed = 0
                    for _, row in to_remove_df.iterrows():
                        delete_standardized_answer(
                            client,
                            row["standardized_question"],
                            row["standardized_answer"],
                            null_fact_rows=False,
                        )
                        removed += 1
                    st.success(f"Removed {removed:,} answer(s) from the lookup.")
                    st.session_state["lookup_drift"] = get_lookup_drift_counts(client)
                    st.cache_data.clear()
                    st.rerun()


REPO_ROOT = Path(__file__).parent.parent.parent
APPLY_SCRIPT = REPO_ROOT / "shared" / "apply_mapping_changes.py"
WIDE_TABLE_SCRIPT = REPO_ROOT / "shared" / "limesurvey_build_wide_table.py"
PROCESS_COLUMNAR_SCRIPT = REPO_ROOT / "shared" / "limesurvey_process_columnar.py"


def _stream_subprocess(cmd: list[str], log_placeholder, max_lines: int = 400) -> int:
    """Run subprocess and stream stdout to a Streamlit placeholder line-by-line.
    Returns the process exit code."""
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    lines: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        lines.append(line.rstrip())
        if len(lines) > max_lines:
            lines = lines[-max_lines:]
        log_placeholder.code("\n".join(lines), language="log")
    proc.wait()
    return proc.returncode


def _run_streamed(job_name: str, cmd: list[str], max_lines: int = 400) -> int:
    """Run a subprocess with live stdout streaming into the page, and record a
    persistent 'last job' status in session_state so refreshes don't lose visibility."""
    import uuid

    run_id = uuid.uuid4().hex[:10]
    st.session_state["last_job"] = {
        "job_name": job_name,
        "run_id": run_id,
        "status": "running",
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "rc": None,
        "last_lines": "",
    }
    placeholder = st.empty()
    rc = _stream_subprocess(cmd, placeholder, max_lines=max_lines)
    st.session_state["last_job"].update(
        {
            "status": "success" if rc == 0 else "failed",
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "rc": rc,
        }
    )
    return rc


def _refresh_etl_mappings():
    """Trigger fast mapping reload in the ETL after Apply Changes lands new mappings."""
    rc = _run_streamed(
        "refresh-mappings",
        [sys.executable, str(PROCESS_COLUMNAR_SCRIPT), "--refresh-mappings-only"],
    )
    if rc == 0:
        st.info("ETL mapping cache refreshed.")
    else:
        st.warning(f"Mapping refresh exited with code {rc}.")


def _render_last_job_status():
    """Small collapsible panel showing the most recent subprocess run — survives reruns."""
    job = st.session_state.get("last_job")
    if not job:
        return
    icon = {"running": "⏳", "success": "✅", "failed": "❌"}.get(job["status"], "•")
    with st.expander(
        f"{icon} Last job: {job['job_name']} — {job['status']}",
        expanded=(job["status"] == "running"),
    ):
        st.caption(
            f"run_id={job['run_id']} · started={job['started_at']} · "
            f"finished={job.get('finished_at', '—')} · rc={job.get('rc', '—')}"
        )


def show_apply_changes(client):
    """Apply pending mapping changes to columnar_completed."""
    st.header("Apply Mapping Changes")

    st.write(
        """
    This page applies mappings from `question_mapping` / `answer_mapping` to
    `lime_surveys_columnar_completed` using UPDATE statements (no full rebuild).
    Manual mappings set `manually_assigned = TRUE` so the nightly ETL won't overwrite them.
    """
    )

    stats = get_coverage_stats(client)
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Rows missing question mapping", f"{stats['unmapped_questions']:,}")
    with col2:
        st.metric("Rows missing answer mapping", f"{stats['unmapped_answers']:,}")

    st.divider()

    _render_last_job_status()

    if st.button("Preview Changes (Dry Run)"):
        _run_streamed(
            "apply-dry-run", [sys.executable, str(APPLY_SCRIPT), "--all", "--dry-run"]
        )

    st.divider()
    st.subheader("Apply Changes")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Apply Question Mappings", type="primary"):
            rc = _run_streamed(
                "apply-questions", [sys.executable, str(APPLY_SCRIPT), "--questions"]
            )
            if rc == 0:
                st.success("Question mappings applied.")
                st.cache_data.clear()
                _refresh_etl_mappings()

    with col2:
        if st.button("Apply Answer Mappings", type="primary"):
            rc = _run_streamed(
                "apply-answers", [sys.executable, str(APPLY_SCRIPT), "--answers"]
            )
            if rc == 0:
                st.success("Answer mappings applied.")
                st.cache_data.clear()
                _refresh_etl_mappings()

    st.divider()
    st.subheader("Rebuild Wide Table")
    st.write("After applying mappings, rebuild the wide table to reflect changes.")

    if st.button("Rebuild Wide Table"):
        rc = _run_streamed(
            "rebuild-wide-table", [sys.executable, str(WIDE_TABLE_SCRIPT)]
        )
        if rc == 0:
            st.success("Wide table rebuilt!")


def _log_user_action(client, action: str, target_email: str, notes: str = "") -> None:
    """Write a row to mapping_history for user-management actions so the
    Audit page surfaces 'who added/changed/deactivated whom'. Best-effort."""
    actor = st.session_state.get("reviewer_email") or "unknown"
    try:
        client.query(
            f"""
            INSERT INTO `{MAPPING_HISTORY}`
              (action, raw_question, raw_answer,
               new_standardized_question, new_standardized_answer,
               affected_rows, created_by)
            VALUES (@action, @target, @notes, NULL, NULL, 1, @actor)
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("action", "STRING", action),
                    bigquery.ScalarQueryParameter("target", "STRING", target_email),
                    bigquery.ScalarQueryParameter("notes", "STRING", notes),
                    bigquery.ScalarQueryParameter("actor", "STRING", actor),
                ]
            ),
        ).result()
    except Exception as e:
        st.warning(f"Action succeeded but audit log failed: {e}")


def _add_user(
    client, email: str, display_name: str, role: str, notes: str
) -> tuple[bool, str]:
    """Insert a new mapper_users row. Returns (ok, message). Normalizes the
    email so dot/plus aliases collapse to one canonical row. Refuses to
    create a duplicate (active or inactive)."""
    norm = _normalize_email(email)
    if not norm or "@" not in norm:
        return False, "Invalid email."
    if role not in ("reviewer", "admin"):
        return False, "Role must be 'reviewer' or 'admin'."
    try:
        existing = list(
            client.query(
                f"SELECT email, is_active FROM `{USERS_TABLE}` WHERE email = @email",
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("email", "STRING", norm),
                    ]
                ),
            ).result()
        )
        if existing:
            row = existing[0]
            if row.is_active:
                return False, f"{norm} already exists and is active."
            return (
                False,
                f"{norm} already exists but is deactivated. Reactivate it instead of re-adding.",
            )

        actor = st.session_state.get("reviewer_email") or "unknown"
        client.query(
            f"""
            INSERT INTO `{USERS_TABLE}`
              (email, display_name, role, is_active, created_at, created_by, notes)
            VALUES (@email, @display_name, @role, TRUE,
                    CURRENT_TIMESTAMP(), @actor, @notes)
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", norm),
                    bigquery.ScalarQueryParameter(
                        "display_name", "STRING", display_name or None
                    ),
                    bigquery.ScalarQueryParameter("role", "STRING", role),
                    bigquery.ScalarQueryParameter("actor", "STRING", actor),
                    bigquery.ScalarQueryParameter("notes", "STRING", notes or None),
                ]
            ),
        ).result()
    except Exception as e:
        return False, f"Add failed: {e}"

    _log_user_action(client, "user_add", norm, notes=f"role={role}")
    _lookup_user.clear()
    return True, f"Added {norm} as {role}."


def _set_user_active(client, email: str, is_active: bool) -> tuple[bool, str]:
    """Toggle is_active on a user row."""
    try:
        client.query(
            f"""
            UPDATE `{USERS_TABLE}`
            SET is_active = @is_active
            WHERE email = @email
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", email),
                    bigquery.ScalarQueryParameter("is_active", "BOOL", is_active),
                ]
            ),
        ).result()
    except Exception as e:
        return False, f"Update failed: {e}"
    action = "user_activate" if is_active else "user_deactivate"
    _log_user_action(client, action, email)
    _lookup_user.clear()
    return True, f"{email} {'activated' if is_active else 'deactivated'}."


def _set_user_role(client, email: str, role: str) -> tuple[bool, str]:
    """Change a user's role between 'reviewer' and 'admin'."""
    if role not in ("reviewer", "admin"):
        return False, "Role must be 'reviewer' or 'admin'."
    try:
        client.query(
            f"""
            UPDATE `{USERS_TABLE}`
            SET role = @role
            WHERE email = @email
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", email),
                    bigquery.ScalarQueryParameter("role", "STRING", role),
                ]
            ),
        ).result()
    except Exception as e:
        return False, f"Update failed: {e}"
    _log_user_action(client, "user_role_change", email, notes=f"new_role={role}")
    _lookup_user.clear()
    return True, f"{email} role set to {role}."


@st.cache_data(ttl=30, show_spinner=False)
def _list_users(_client) -> pd.DataFrame:
    """Return all mapper_users rows for the Users page. Cached briefly so
    typing in forms doesn't re-query on every rerun."""
    return _client.query(
        f"""
        SELECT email, display_name, role, is_active
        FROM `{USERS_TABLE}`
        ORDER BY is_active DESC, role DESC, email
        """
    ).to_dataframe()


@st.cache_data(ttl=60, show_spinner=False)
def _get_usage_recent_sessions(_client, days: int) -> pd.DataFrame:
    """Per-(user, session_id) summary of the last N days of activity.
    One row per session. Pages-visited list comes from the page_view
    rows within that session."""
    query = f"""
    WITH events AS (
      SELECT email, session_id, event_type, page, event_time
      FROM `{USAGE_LOG_TABLE}`
      WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
    ),
    sessions AS (
      SELECT
        email,
        session_id,
        MIN(event_time) AS started_at,
        MAX(event_time) AS last_seen_at,
        COUNTIF(event_type = 'page_view') AS page_views,
        ARRAY_AGG(
          page IGNORE NULLS ORDER BY event_time LIMIT 20
        ) AS pages_visited
      FROM events
      GROUP BY email, session_id
    )
    SELECT
      email,
      started_at,
      last_seen_at,
      TIMESTAMP_DIFF(last_seen_at, started_at, MINUTE) AS minutes_active,
      page_views,
      pages_visited
    FROM sessions
    ORDER BY started_at DESC
    LIMIT 200
    """
    return _client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days", "INT64", days),
            ]
        ),
    ).to_dataframe()


@st.cache_data(ttl=60, show_spinner=False)
def _get_usage_per_user(_client, days: int) -> pd.DataFrame:
    """Per-user activity rollup for the last N days."""
    query = f"""
    SELECT
      email,
      COUNT(DISTINCT session_id) AS sessions,
      COUNTIF(event_type = 'page_view') AS page_views,
      MAX(event_time) AS last_seen_at
    FROM `{USAGE_LOG_TABLE}`
    WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
    GROUP BY email
    ORDER BY sessions DESC, page_views DESC
    """
    return _client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days", "INT64", days),
            ]
        ),
    ).to_dataframe()


@st.cache_data(ttl=60, show_spinner=False)
def _get_usage_per_page(_client, days: int) -> pd.DataFrame:
    """Page-view counts, last N days. Tells you which pages reviewers
    actually use vs. which are dead weight."""
    query = f"""
    SELECT
      page,
      COUNT(*) AS page_views,
      COUNT(DISTINCT email) AS distinct_users
    FROM `{USAGE_LOG_TABLE}`
    WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
      AND event_type = 'page_view'
      AND page IS NOT NULL
    GROUP BY page
    ORDER BY page_views DESC
    """
    return _client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days", "INT64", days),
            ]
        ),
    ).to_dataframe()


def _render_usage_tab(client):
    """Admin-only Usage tab content. Surfaces who's using the mapper, when,
    and which pages they touch. Data lives in mapper_usage_log."""
    user = st.session_state.get("reviewer_user") or {}
    if user.get("role") != "admin":
        st.info(
            "Usage analytics are admin-only. Ask an admin if you need a "
            "specific report."
        )
        return

    st.caption(
        "Activity from mapper_usage_log. One row per session_start (browser "
        "session) and per page_view (when a reviewer switches pages). Cached "
        "60s; refresh the page to repull."
    )

    days = st.selectbox(
        "Window",
        [1, 7, 14, 30, 90],
        index=1,
        format_func=lambda d: f"Last {d} day{'s' if d != 1 else ''}",
        key="usage_window_days",
    )

    # Per-user rollup
    st.subheader("By user")
    try:
        df_users = _get_usage_per_user(client, days)
    except Exception as e:
        st.warning(f"Failed to load per-user activity: {e}")
        df_users = pd.DataFrame()
    if df_users.empty:
        st.info("No activity in this window.")
    else:
        st.dataframe(df_users, use_container_width=True, hide_index=True)

    # Per-page rollup
    st.subheader("By page")
    try:
        df_pages = _get_usage_per_page(client, days)
    except Exception as e:
        st.warning(f"Failed to load per-page activity: {e}")
        df_pages = pd.DataFrame()
    if df_pages.empty:
        st.info("No page views in this window.")
    else:
        st.dataframe(df_pages, use_container_width=True, hide_index=True)

    # Recent sessions detail
    st.subheader("Recent sessions")
    try:
        df_sessions = _get_usage_recent_sessions(client, days)
    except Exception as e:
        st.warning(f"Failed to load recent sessions: {e}")
        df_sessions = pd.DataFrame()
    if df_sessions.empty:
        st.info("No sessions in this window.")
    else:
        # pages_visited is a numpy array per row (ARRAY_AGG). Flatten to a
        # short, comma-separated string for the table view.
        def _format_pages(xs):
            if xs is None or len(xs) == 0:
                return ""
            return ", ".join(list(xs))

        display = df_sessions.copy()
        display["pages_visited"] = display["pages_visited"].apply(_format_pages)
        st.dataframe(display, use_container_width=True, hide_index=True)


def _render_members_tab(client):
    """Members tab content. List visible to all signed-in users; the
    add/role/deactivate forms render only for admins."""
    user = st.session_state.get("reviewer_user") or {}
    is_admin = user.get("role") == "admin"

    st.caption(
        "These users can sign in to the mapper. Identity comes from Google "
        "IAP; dot and plus-tag aliases are normalized so each Google account "
        "matches one row. Admins can add, deactivate, or change roles."
    )

    df = _list_users(client)
    st.dataframe(df, use_container_width=True, hide_index=True)

    if not is_admin:
        st.info(
            "Read-only view. To add a user or change roles, ask an admin "
            "(rows with role = 'admin' above)."
        )
        return

    st.divider()
    st.subheader("Add a user")
    with st.form("add_user_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            new_email = st.text_input("Email", placeholder="alice@bionews.com")
            new_role = st.selectbox("Role", ["reviewer", "admin"], index=0)
        with c2:
            new_name = st.text_input(
                "Display name (optional)", placeholder="Alice Smith"
            )
            new_notes = st.text_input("Notes (optional)")
        if st.form_submit_button("Add user", type="primary"):
            ok, msg = _add_user(client, new_email, new_name, new_role, new_notes)
            if ok:
                st.success(msg)
                _list_users.clear()
                st.rerun()
            else:
                st.error(msg)

    st.divider()
    st.subheader("Change role / deactivate")
    with st.form("modify_user_form", clear_on_submit=False):
        all_emails = df["email"].tolist() if not df.empty else []
        target = st.selectbox("User", all_emails) if all_emails else ""
        c1, c2, c3 = st.columns(3)
        with c1:
            new_role_for_target = st.selectbox(
                "Set role", ["reviewer", "admin"], key="modify_role"
            )
            change_role = st.form_submit_button("Update role")
        with c2:
            deactivate = st.form_submit_button("Deactivate")
        with c3:
            reactivate = st.form_submit_button("Reactivate")

        if target and (change_role or deactivate or reactivate):
            if target == st.session_state.get("reviewer_email") and (
                deactivate or (change_role and new_role_for_target != "admin")
            ):
                st.error(
                    "You can't deactivate yourself or demote yourself out of "
                    "admin. Ask another admin to do it."
                )
            else:
                if change_role:
                    ok, msg = _set_user_role(client, target, new_role_for_target)
                elif deactivate:
                    ok, msg = _set_user_active(client, target, False)
                else:
                    ok, msg = _set_user_active(client, target, True)
                if ok:
                    st.success(msg)
                    _list_users.clear()
                    st.rerun()
                else:
                    st.error(msg)


def show_users(client):
    """Users / access management page. Two tabs:
    - Members: the user list (visible to all) + admin-only mutation forms.
    - Usage: admin-only activity analytics from mapper_usage_log."""
    st.header("Users")

    tab_members, tab_usage = st.tabs(["Members", "Usage"])
    with tab_members:
        _render_members_tab(client)
    with tab_usage:
        _render_usage_tab(client)


def show_model(client):
    """Manage the ML question-classifier: status, retrain, and live progress."""
    st.header("Model Management")

    st.write(
        """
    The ML classifier is the second matching layer (after Exact match). It learns from
    every `(raw_question_en, standardized_question)` pair already in `lime_surveys_columnar_completed`.
    Retrain after you've added a meaningful batch of manual mappings.
    """
    )

    # Model file status. Honors LIMESURVEY_MODEL_DIR env var (same as the ETL
    # processor and audit scripts) so a per-host override (e.g. on Linux:
    # /home/orchestrator/models) is reflected in the UI without code changes.
    model_dir = Path(os.environ.get("LIMESURVEY_MODEL_DIR") or (REPO_ROOT / "models"))
    model_file = model_dir / "question_classifier.pkl"
    encoder_file = model_dir / "label_encoder.pkl"

    col1, col2 = st.columns(2)
    with col1:
        if model_file.exists():
            mtime = datetime.fromtimestamp(model_file.stat().st_mtime)
            size_mb = model_file.stat().st_size / (1024 * 1024)
            st.metric(
                "Model last trained",
                mtime.strftime("%Y-%m-%d %H:%M"),
                help=f"{model_file.name} ({size_mb:.1f} MB)",
            )
        else:
            st.metric(
                "Model last trained",
                "Never",
                help="No question_classifier.pkl found. First retrain will create it.",
            )
    with col2:
        # Training pool size
        try:
            pool_query = f"""
            SELECT
                COUNT(DISTINCT CONCAT(raw_question_en, '|', standardized_question)) AS pairs
            FROM `{COLUMNAR_COMPLETED}`
            WHERE standardized_question IS NOT NULL
              AND raw_question_en IS NOT NULL
              AND TRIM(raw_question_en) != ''
            """
            pairs = list(client.query(pool_query))[0].pairs
            st.metric(
                "Training pairs available",
                f"{pairs:,}",
                help="Distinct (raw_question, standardized_question) pairs in columnar_completed.",
            )
        except Exception as e:
            st.metric("Training pairs available", "—")
            st.caption(f"Could not load: {e}")

    st.divider()

    # Retrain confirmation + execution
    st.subheader("Retrain ML Model")
    st.warning(
        f"Retraining will overwrite `{model_file}`. "
        f"The currently loaded model in any running ETL process will keep "
        f"using its in-memory copy until restarted."
    )

    confirm = st.checkbox("I understand — retrain the model now")

    # Disable button while a retrain is in progress (Streamlit reruns; use session_state)
    running = st.session_state.get("retrain_running", False)

    if st.button("Start Retrain", type="primary", disabled=not confirm or running):
        st.session_state["retrain_running"] = True
        st.rerun()

    if running:
        st.info("Retraining in progress. Do not close this tab.")
        with st.spinner("Training classifier..."):
            try:
                rc = _run_streamed(
                    "retrain-model",
                    [sys.executable, str(PROCESS_COLUMNAR_SCRIPT), "--retrain-model"],
                )
            finally:
                st.session_state["retrain_running"] = False

        if rc == 0:
            st.success("Retrain complete. New model saved.")
        else:
            st.error(f"Retrain failed with exit code {rc}. See log above for details.")

        if st.button("Close"):
            st.cache_data.clear()
            st.rerun()


if __name__ == "__main__":
    main()
