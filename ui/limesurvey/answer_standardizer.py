#!/usr/bin/env python3
"""
!!! DEPRECATED — use ui/limesurvey/limesurvey_mapper.py !!!
This standalone app is kept only for reference. It writes through a different code
path with different schema/lock semantics than the unified mapper, which causes
drift. Do not run concurrently with the main mapper.

Answer Standardizer - Row-by-Row Interface

A Streamlit app for standardizing LimeSurvey answers one row at a time,
with full response context visible for better decision making.

Features:
1. View one unmapped row at a time with all answers from same respondent
2. Fuzzy matching suggestions ranked by confidence score
3. Immediate BigQuery UPDATE when selection is made
4. Navigation (prev/next/skip) and filtering (survey/question)
5. Progress tracking (X of Y remaining)

Usage:
    streamlit run ui/limesurvey/answer_standardizer.py
"""

import os
import sys
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from pathlib import Path
from rapidfuzz import fuzz, process
from datetime import datetime

# Load environment variables
env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(env_path)

# Configuration
PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'

# Table names
ANSWERS_LOOKUP = f'{PROJECT_ID}.{DATASET}.standardized_answers_lookup'
ANSWER_MAPPING = f'{PROJECT_ID}.{DATASET}.answer_mapping'
COLUMNAR_COMPLETED = f'{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed'


@st.cache_resource
def get_bq_client():
    """Create BigQuery client (cached)."""
    return bigquery.Client(project=PROJECT_ID)


# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================

def get_unmapped_row_with_context(client, offset: int = 0,
                                   survey_filter: int = None,
                                   question_filter: str = None) -> dict:
    """
    Get a single unmapped row with full response context.

    Returns dict with:
        - row: The unmapped row data
        - response_context: All other answers from same respondent
        - total_unmapped: Total count of unmapped rows (for progress)
    """
    # Build filter conditions
    filters = """
        standardized_answer IS NULL
        AND standardized_question IS NOT NULL
        AND raw_answer_en IS NOT NULL
        AND TRIM(raw_answer_en) != ''
        AND NOT REGEXP_CONTAINS(raw_answer_en, r'^[0-9.]+$')
    """

    params = [bigquery.ScalarQueryParameter('offset', 'INT64', offset)]

    if survey_filter:
        filters += " AND survey_id = @survey_filter"
        params.append(bigquery.ScalarQueryParameter('survey_filter', 'INT64', survey_filter))

    if question_filter:
        filters += " AND standardized_question = @question_filter"
        params.append(bigquery.ScalarQueryParameter('question_filter', 'STRING', question_filter))

    query = f"""
    WITH unmapped_row AS (
        SELECT *
        FROM `{COLUMNAR_COMPLETED}`
        WHERE {filters}
        ORDER BY submitdate DESC
        LIMIT 1 OFFSET @offset
    ),
    response_context AS (
        SELECT
            c.raw_column_name,
            c.standardized_question,
            c.raw_question_en,
            c.raw_answer_en,
            c.standardized_answer
        FROM `{COLUMNAR_COMPLETED}` c
        WHERE c.survey_id = (SELECT survey_id FROM unmapped_row)
          AND c.response_id = (SELECT response_id FROM unmapped_row)
        ORDER BY c.question_id
    ),
    total_count AS (
        SELECT COUNT(*) as cnt
        FROM `{COLUMNAR_COMPLETED}`
        WHERE {filters}
    )
    SELECT
        u.*,
        (SELECT cnt FROM total_count) as total_unmapped,
        ARRAY(SELECT AS STRUCT * FROM response_context) as response_answers
    FROM unmapped_row u
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    result = client.query(query, job_config=job_config).to_dataframe()

    if result.empty:
        return None

    row = result.iloc[0].to_dict()
    return row


def get_impact_count(client, raw_answer: str, standardized_question: str) -> int:
    """Get count of rows that would be affected by this mapping."""
    query = f"""
    SELECT COUNT(*) as cnt
    FROM `{COLUMNAR_COMPLETED}`
    WHERE raw_answer_en = @raw_answer
      AND standardized_question = @std_question
      AND standardized_answer IS NULL
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('raw_answer', 'STRING', raw_answer),
            bigquery.ScalarQueryParameter('std_question', 'STRING', standardized_question)
        ]
    )

    result = client.query(query, job_config=job_config).to_dataframe()
    return int(result.iloc[0]['cnt'])


@st.cache_data(ttl=300)
def get_existing_answers_for_question(_client, standardized_question: str) -> list:
    """
    Get existing standardized answers for a given question.
    Combines answers from lookup table and already-mapped values.
    """
    query = f"""
    WITH lookup_answers AS (
        SELECT DISTINCT standardized_answer
        FROM `{ANSWERS_LOOKUP}`
        WHERE standardized_question = @std_question
    ),
    mapped_answers AS (
        SELECT DISTINCT standardized_answer
        FROM `{COLUMNAR_COMPLETED}`
        WHERE standardized_question = @std_question
          AND standardized_answer IS NOT NULL
    )
    SELECT DISTINCT standardized_answer
    FROM (
        SELECT * FROM lookup_answers
        UNION ALL
        SELECT * FROM mapped_answers
    )
    ORDER BY standardized_answer
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('std_question', 'STRING', standardized_question)
        ]
    )

    result = _client.query(query, job_config=job_config).to_dataframe()
    return result['standardized_answer'].tolist()


@st.cache_data(ttl=300)
def get_filter_options(_client):
    """Get available surveys and questions for filtering."""
    # Surveys with unmapped answers
    survey_query = f"""
    SELECT DISTINCT survey_id
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_answer IS NULL
      AND standardized_question IS NOT NULL
      AND raw_answer_en IS NOT NULL
      AND TRIM(raw_answer_en) != ''
    ORDER BY survey_id
    """

    # Questions with unmapped answers
    question_query = f"""
    SELECT DISTINCT standardized_question
    FROM `{COLUMNAR_COMPLETED}`
    WHERE standardized_answer IS NULL
      AND standardized_question IS NOT NULL
      AND raw_answer_en IS NOT NULL
      AND TRIM(raw_answer_en) != ''
    ORDER BY standardized_question
    """

    surveys = _client.query(survey_query).to_dataframe()['survey_id'].tolist()
    questions = _client.query(question_query).to_dataframe()['standardized_question'].tolist()

    return surveys, questions


# ============================================================================
# FUZZY MATCHING
# ============================================================================

def get_answer_suggestions(raw_answer: str, standardized_question: str,
                           client, top_n: int = 5, threshold: int = 50) -> list:
    """
    Get fuzzy match suggestions for an answer.

    Returns list of tuples: (standardized_answer, score)
    """
    existing_answers = get_existing_answers_for_question(client, standardized_question)

    if not existing_answers:
        return []

    # Use token_sort_ratio for better word order flexibility
    matches = process.extract(
        raw_answer,
        existing_answers,
        scorer=fuzz.token_sort_ratio,
        limit=top_n
    )

    return [(match, score) for match, score, _ in matches if score >= threshold]


# ============================================================================
# UPDATE FUNCTIONS
# ============================================================================

def apply_answer_mapping(client, raw_answer: str, standardized_question: str,
                         standardized_answer: str, match_type: str = 'manual_ui') -> int:
    """
    Apply answer mapping:
    1. Insert into answer_mapping table (for future reference)
    2. Update all matching rows in columnar_completed

    Returns: Number of rows updated
    """
    # 1. Insert into answer_mapping (ignore if exists)
    insert_query = f"""
    INSERT INTO `{ANSWER_MAPPING}`
    (standardized_question, raw_answer, standardized_answer, match_type, created_at)
    SELECT @std_question, @raw_answer, @std_answer, @match_type, CURRENT_TIMESTAMP()
    WHERE NOT EXISTS (
        SELECT 1 FROM `{ANSWER_MAPPING}`
        WHERE standardized_question = @std_question
          AND raw_answer = @raw_answer
    )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('std_question', 'STRING', standardized_question),
            bigquery.ScalarQueryParameter('raw_answer', 'STRING', raw_answer),
            bigquery.ScalarQueryParameter('std_answer', 'STRING', standardized_answer),
            bigquery.ScalarQueryParameter('match_type', 'STRING', match_type)
        ]
    )

    client.query(insert_query, job_config=job_config).result()

    # 2. Update columnar_completed
    update_query = f"""
    UPDATE `{COLUMNAR_COMPLETED}`
    SET standardized_answer = @std_answer,
        updated_at = CURRENT_TIMESTAMP()
    WHERE raw_answer_en = @raw_answer
      AND standardized_question = @std_question
      AND standardized_answer IS NULL
    """

    job = client.query(update_query, job_config=job_config)
    job.result()

    return job.num_dml_affected_rows


def mark_as_free_text(client, raw_answer: str, standardized_question: str) -> int:
    """Mark answer as free text (standardized_answer = '__FREE_TEXT__')."""
    return apply_answer_mapping(
        client, raw_answer, standardized_question,
        '__FREE_TEXT__', 'free_text'
    )


def flag_for_review(client, raw_answer: str, standardized_question: str) -> int:
    """Flag answer for later review (standardized_answer = '__REVIEW__')."""
    return apply_answer_mapping(
        client, raw_answer, standardized_question,
        '__REVIEW__', 'flagged'
    )


# ============================================================================
# STREAMLIT UI
# ============================================================================

def main():
    st.set_page_config(
        page_title="Answer Standardizer",
        page_icon="📝",
        layout="wide"
    )

    st.title("Answer Standardizer")
    st.error(
        "⚠️ **This app is deprecated.** Use `streamlit run ui/limesurvey/limesurvey_mapper.py` instead. "
        "This standalone app writes to BigQuery through a different code path with different schema "
        "and lock semantics, which can corrupt the unified workflow."
    )
    st.caption("Row-by-row answer standardization with full response context")

    # Initialize session state
    if 'current_offset' not in st.session_state:
        st.session_state.current_offset = 0
    if 'survey_filter' not in st.session_state:
        st.session_state.survey_filter = None
    if 'question_filter' not in st.session_state:
        st.session_state.question_filter = None

    client = get_bq_client()

    # Sidebar filters
    with st.sidebar:
        st.header("Filters")

        surveys, questions = get_filter_options(client)

        survey_options = [None] + surveys
        survey_filter = st.selectbox(
            "Survey ID",
            survey_options,
            format_func=lambda x: "All Surveys" if x is None else str(x),
            key='survey_select'
        )
        if survey_filter != st.session_state.survey_filter:
            st.session_state.survey_filter = survey_filter
            st.session_state.current_offset = 0

        question_options = [None] + questions
        question_filter = st.selectbox(
            "Question",
            question_options,
            format_func=lambda x: "All Questions" if x is None else x,
            key='question_select'
        )
        if question_filter != st.session_state.question_filter:
            st.session_state.question_filter = question_filter
            st.session_state.current_offset = 0

        st.divider()

        if st.button("Reset Filters", use_container_width=True):
            st.session_state.survey_filter = None
            st.session_state.question_filter = None
            st.session_state.current_offset = 0
            st.rerun()

    # Fetch current row
    row_data = get_unmapped_row_with_context(
        client,
        offset=st.session_state.current_offset,
        survey_filter=st.session_state.survey_filter,
        question_filter=st.session_state.question_filter
    )

    if not row_data:
        st.success("All answers have been standardized! Nothing left to process.")
        return

    total_unmapped = row_data.get('total_unmapped', 0)

    # Navigation bar
    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])

    with col1:
        if st.button("< Prev", disabled=st.session_state.current_offset == 0, use_container_width=True):
            st.session_state.current_offset = max(0, st.session_state.current_offset - 1)
            st.rerun()

    with col2:
        if st.button("Next >", disabled=st.session_state.current_offset >= total_unmapped - 1, use_container_width=True):
            st.session_state.current_offset += 1
            st.rerun()

    with col3:
        st.markdown(f"**Row {st.session_state.current_offset + 1} of {total_unmapped:,}**")

    with col4:
        if st.button("Skip", use_container_width=True):
            st.session_state.current_offset += 1
            st.rerun()

    with col5:
        # Jump to specific row
        new_offset = st.number_input("Go to row", min_value=1, max_value=total_unmapped,
                                      value=st.session_state.current_offset + 1, label_visibility="collapsed")
        if new_offset - 1 != st.session_state.current_offset:
            st.session_state.current_offset = new_offset - 1
            st.rerun()

    st.divider()

    # Main content - two columns
    left_col, right_col = st.columns([1, 1])

    with left_col:
        # Current Row Info
        st.subheader("Current Row")

        with st.container(border=True):
            st.markdown(f"**Survey ID:** {row_data.get('survey_id', 'N/A')}")
            st.markdown(f"**Response ID:** {row_data.get('response_id', 'N/A')}")
            st.markdown(f"**Submitted:** {row_data.get('submitdate', 'N/A')}")
            if row_data.get('site'):
                st.markdown(f"**Site:** {row_data.get('site')}")

        # Question & Answer
        st.subheader("Question & Answer")

        with st.container(border=True):
            st.markdown(f"**Standardized Question:** `{row_data.get('standardized_question', 'N/A')}`")
            st.markdown(f"**Raw Question:** {row_data.get('raw_question_en', 'N/A')}")
            st.divider()
            st.markdown(f"**Raw Answer:**")
            st.info(row_data.get('raw_answer_en', 'N/A'))
            if row_data.get('answer_value'):
                st.markdown(f"**Answer Value:** `{row_data.get('answer_value')}`")

        # Response Context
        st.subheader("Response Context")
        st.caption("Other answers from this respondent")

        response_answers = row_data.get('response_answers', [])

        if response_answers:
            with st.container(border=True, height=300):
                for ans in response_answers:
                    if ans.get('standardized_question') == row_data.get('standardized_question'):
                        continue  # Skip current question

                    q_name = ans.get('standardized_question') or ans.get('raw_question_en', 'Unknown')
                    a_value = ans.get('standardized_answer') or ans.get('raw_answer_en', 'N/A')

                    if a_value and str(a_value).strip():
                        st.markdown(f"**{q_name}:** {a_value}")

    with right_col:
        # Impact indicator
        raw_answer = row_data.get('raw_answer_en', '')
        std_question = row_data.get('standardized_question', '')

        impact_count = get_impact_count(client, raw_answer, std_question)

        if impact_count > 1:
            st.info(f"This mapping will apply to **{impact_count:,} rows** with the same raw answer.")

        # Suggested Matches
        st.subheader("Suggested Matches")

        suggestions = get_answer_suggestions(raw_answer, std_question, client)

        if suggestions:
            for std_answer, score in suggestions:
                col_score, col_answer, col_btn = st.columns([1, 3, 1])

                with col_score:
                    if score >= 90:
                        st.success(f"{score}%")
                    elif score >= 75:
                        st.warning(f"{score}%")
                    else:
                        st.caption(f"{score}%")

                with col_answer:
                    st.markdown(f"`{std_answer}`")

                with col_btn:
                    if st.button("Select", key=f"select_{std_answer}", use_container_width=True):
                        rows_updated = apply_answer_mapping(
                            client, raw_answer, std_question, std_answer
                        )
                        st.toast(f"Updated {rows_updated} rows!", icon="✅")
                        get_existing_answers_for_question.clear()
                        get_filter_options.clear()
                        st.rerun()
        else:
            st.caption("No existing matches found for this question.")

        st.divider()

        # Create New
        st.subheader("Create New Standardized Answer")

        new_answer = st.text_input(
            "New standardized answer",
            placeholder="e.g., physical_therapy_and_medication",
            help="Use snake_case format"
        )

        if st.button("Create & Apply", disabled=not new_answer, use_container_width=True, type="primary"):
            # Validate snake_case
            if ' ' in new_answer or new_answer != new_answer.lower():
                st.error("Please use snake_case format (lowercase with underscores)")
            else:
                rows_updated = apply_answer_mapping(
                    client, raw_answer, std_question, new_answer
                )
                st.toast(f"Created mapping and updated {rows_updated} rows!", icon="✅")
                get_existing_answers_for_question.clear()
                get_filter_options.clear()
                st.rerun()

        st.divider()

        # Skip Options
        st.subheader("Skip Options")

        col_skip1, col_skip2 = st.columns(2)

        with col_skip1:
            if st.button("Mark as Free Text", use_container_width=True,
                        help="This answer contains free-form text that shouldn't be standardized"):
                rows_updated = mark_as_free_text(client, raw_answer, std_question)
                st.toast(f"Marked {rows_updated} rows as free text", icon="📝")
                get_filter_options.clear()
                st.rerun()

        with col_skip2:
            if st.button("Flag for Review", use_container_width=True,
                        help="Flag this answer for later manual review"):
                rows_updated = flag_for_review(client, raw_answer, std_question)
                st.toast(f"Flagged {rows_updated} rows for review", icon="🚩")
                get_filter_options.clear()
                st.rerun()


if __name__ == "__main__":
    main()
