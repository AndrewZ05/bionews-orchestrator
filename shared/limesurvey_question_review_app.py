#!/usr/bin/env python3
"""
!!! DEPRECATED — use ui/limesurvey/limesurvey_mapper.py !!!
This standalone app is kept only for reference. The unified mapper covers its
functionality with Model tab + blended suggestions. Do not run concurrently
with the main mapper.

LimeSurvey Question Standardization Review App

Streamlit app for reviewing and correcting question standardizations.
Uses ML embeddings for smart suggestions.

Run with: streamlit run shared/limesurvey_question_review_app.py
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

import re
import pickle
import warnings

warnings.filterwarnings("ignore")

import streamlit as st
import pandas as pd
from pathlib import Path
from google.cloud import bigquery
from datetime import datetime

# Constants
PROJECT_ID = "bi-data-391216"
DATASET = "limesurvey_data"
SOURCE_TABLE = "lime_surveys_columnar_completed"
# Portable: LIMESURVEY_MODEL_DIR env var overrides; else <repo_root>/models.
MODEL_DIR = Path(
    os.environ.get("LIMESURVEY_MODEL_DIR")
    or (Path(__file__).resolve().parent.parent / "models")
)
MODEL_FILE = MODEL_DIR / "question_classifier.pkl"
ENCODER_FILE = MODEL_DIR / "label_encoder.pkl"

# Page config
st.set_page_config(
    page_title="Question Standardization Review", page_icon="📋", layout="wide"
)

# Initialize session state
if "pending_changes" not in st.session_state:
    st.session_state.pending_changes = {}  # {raw_question: new_std_question}
if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False
if "transformer" not in st.session_state:
    st.session_state.transformer = None
if "embeddings_cache" not in st.session_state:
    st.session_state.embeddings_cache = {}


def is_snake_case(s):
    """Check if string is valid snake_case"""
    if not s:
        return False
    pattern = r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$"
    return bool(re.match(pattern, s))


def to_snake_case(s):
    """Convert string to snake_case"""
    # Replace spaces and special chars with underscores
    s = re.sub(r"[^a-zA-Z0-9]", "_", s)
    # Insert underscore before uppercase letters
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    # Convert to lowercase and clean up multiple underscores
    s = re.sub(r"_+", "_", s.lower())
    # Remove leading/trailing underscores
    return s.strip("_")


@st.cache_resource
def get_bq_client():
    """Get BigQuery client"""
    return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=300)
def load_data():
    """Load question data from BigQuery"""
    client = get_bq_client()
    query = f"""
    SELECT
        standardized_question,
        raw_question_en,
        COUNT(*) as row_count
    FROM `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
    WHERE standardized_question IS NOT NULL
      AND raw_question_en IS NOT NULL
    GROUP BY standardized_question, raw_question_en
    ORDER BY standardized_question, row_count DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def get_all_standardized_questions():
    """Get all unique standardized questions with counts"""
    client = get_bq_client()
    query = f"""
    SELECT
        standardized_question,
        COUNT(*) as total_rows,
        COUNT(DISTINCT raw_question_en) as unique_questions
    FROM `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
    WHERE standardized_question IS NOT NULL
    GROUP BY standardized_question
    ORDER BY standardized_question
    """
    return client.query(query).to_dataframe()


@st.cache_resource
def load_ml_components():
    """Load ML model and transformer for suggestions"""
    try:
        from sentence_transformers import SentenceTransformer

        transformer = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

        if MODEL_FILE.exists() and ENCODER_FILE.exists():
            with open(MODEL_FILE, "rb") as f:
                model = pickle.load(f)
            with open(ENCODER_FILE, "rb") as f:
                encoder = pickle.load(f)
            return model, encoder, transformer
        else:
            return None, None, transformer
    except Exception as e:
        st.warning(f"Could not load ML components: {e}")
        return None, None, None


def get_ml_suggestions(question_text, model, encoder, transformer, top_n=5):
    """Get ML-based suggestions for a question"""
    if model is None or transformer is None:
        return []

    try:
        # Generate embedding for the question
        embedding = transformer.encode([question_text])

        # Get prediction probabilities
        proba = model.predict_proba(embedding)[0]

        # Get top N predictions
        top_indices = proba.argsort()[-top_n:][::-1]
        suggestions = []
        for idx in top_indices:
            label = encoder.inverse_transform([idx])[0]
            confidence = proba[idx]
            suggestions.append((label, confidence))

        return suggestions
    except Exception as e:
        st.warning(f"ML suggestion error: {e}")
        return []


def apply_changes_to_bigquery(changes):
    """Apply all pending changes to BigQuery"""
    client = get_bq_client()

    success_count = 0
    error_count = 0
    errors = []

    for raw_q, new_std in changes.items():
        try:
            update_query = f"""
            UPDATE `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
            SET standardized_question = @new_std
            WHERE raw_question_en = @raw_q
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("new_std", "STRING", new_std),
                    bigquery.ScalarQueryParameter("raw_q", "STRING", raw_q),
                ]
            )

            result = client.query(update_query, job_config=job_config).result()
            success_count += 1
        except Exception as e:
            error_count += 1
            errors.append(f"{raw_q[:50]}...: {e}")

    return success_count, error_count, errors


# Main app
st.title("📋 Question Standardization Review")
st.error(
    "⚠️ **This app is deprecated.** Use `streamlit run ui/limesurvey/limesurvey_mapper.py` instead. "
    "This standalone app writes to BigQuery through a different code path and can conflict with the unified workflow."
)
st.markdown(
    "Review and correct `standardized_question` values. Changes are batched and applied together."
)

# Load ML components
model, encoder, transformer = load_ml_components()

# Sidebar - Stats and pending changes
with st.sidebar:
    st.header("📊 Statistics")

    # Load data
    try:
        df = load_data()
        std_questions_df = get_all_standardized_questions()

        st.metric("Total Unique Questions", len(df))
        st.metric("Standardized Categories", len(std_questions_df))
        st.metric("ML Model Loaded", "Yes" if model else "No")

    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.stop()

    st.divider()

    # Pending changes
    st.header("📝 Pending Changes")
    if st.session_state.pending_changes:
        st.warning(f"{len(st.session_state.pending_changes)} changes pending")

        for raw_q, new_std in list(st.session_state.pending_changes.items())[:5]:
            st.text(f"→ {new_std}")
            if st.button("❌", key=f"remove_{hash(raw_q)}"):
                del st.session_state.pending_changes[raw_q]
                st.rerun()

        if len(st.session_state.pending_changes) > 5:
            st.text(f"... and {len(st.session_state.pending_changes) - 5} more")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Apply All", type="primary"):
                with st.spinner("Applying changes..."):
                    success, errors, error_msgs = apply_changes_to_bigquery(
                        st.session_state.pending_changes
                    )
                    if success > 0:
                        st.success(f"Applied {success} changes!")
                        st.session_state.pending_changes = {}
                        st.cache_data.clear()
                        st.rerun()
                    if errors > 0:
                        st.error(f"{errors} errors occurred")
                        for msg in error_msgs[:3]:
                            st.text(msg)

        with col2:
            if st.button("🗑️ Clear All"):
                st.session_state.pending_changes = {}
                st.rerun()
    else:
        st.info("No pending changes")

# Main content - Tabs
tab1, tab2, tab3 = st.tabs(
    ["📂 Browse by Category", "🔍 Search Questions", "📋 All Questions"]
)

# Tab 1: Browse by Category
with tab1:
    st.subheader("Browse by Standardized Category")

    # Category selector with row counts
    category_options = std_questions_df.apply(
        lambda r: f"{r['standardized_question']} ({r['total_rows']:,} rows, {r['unique_questions']} questions)",
        axis=1,
    ).tolist()

    selected_category_display = st.selectbox(
        "Select category:", options=category_options, key="category_select"
    )

    if selected_category_display:
        selected_category = selected_category_display.split(" (")[0]

        # Filter questions in this category
        category_df = df[df["standardized_question"] == selected_category].copy()

        st.markdown(f"**{len(category_df)} questions in this category:**")

        for idx, row in category_df.iterrows():
            raw_q = row["raw_question_en"]
            row_count = row["row_count"]

            # Check if there's a pending change
            has_pending = raw_q in st.session_state.pending_changes

            with st.expander(
                f"{'🔄 ' if has_pending else ''}{raw_q[:80]}{'...' if len(raw_q) > 80 else ''} ({row_count:,} rows)",
                expanded=has_pending,
            ):
                st.text_area(
                    "Full question:",
                    raw_q,
                    height=100,
                    disabled=True,
                    key=f"q_{hash(raw_q)}",
                )

                col1, col2 = st.columns([3, 1])

                with col1:
                    # Input for new standardized question
                    current_value = st.session_state.pending_changes.get(raw_q, "")
                    new_std = st.text_input(
                        "New standardized_question (snake_case):",
                        value=current_value,
                        key=f"new_{hash(raw_q)}",
                        placeholder="leave empty to keep current",
                    )

                    # Validate snake_case
                    if new_std and not is_snake_case(new_std):
                        st.warning(
                            f"⚠️ Not valid snake_case. Suggested: `{to_snake_case(new_std)}`"
                        )
                        if st.button("Use suggested", key=f"suggest_{hash(raw_q)}"):
                            st.session_state.pending_changes[raw_q] = to_snake_case(
                                new_std
                            )
                            st.rerun()
                    elif new_std:
                        if new_std != current_value:
                            st.session_state.pending_changes[raw_q] = new_std

                with col2:
                    # ML suggestions button
                    if st.button("🤖 Get Suggestions", key=f"ml_{hash(raw_q)}"):
                        suggestions = get_ml_suggestions(
                            raw_q, model, encoder, transformer
                        )
                        if suggestions:
                            st.markdown("**ML Suggestions:**")
                            for label, conf in suggestions:
                                if st.button(
                                    f"{label} ({conf:.1%})",
                                    key=f"pick_{hash(raw_q)}_{label}",
                                ):
                                    st.session_state.pending_changes[raw_q] = label
                                    st.rerun()

                # Quick move to existing category
                st.markdown("**Move to existing category:**")
                move_to = st.selectbox(
                    "Select category:",
                    options=[""] + std_questions_df["standardized_question"].tolist(),
                    key=f"move_{hash(raw_q)}",
                )
                if move_to and move_to != selected_category:
                    st.session_state.pending_changes[raw_q] = move_to
                    st.success(f"Will move to: {move_to}")

# Tab 2: Search Questions
with tab2:
    st.subheader("Search Questions")

    search_term = st.text_input(
        "Search in raw_question_en:", placeholder="e.g., hepatitis, age, treatment"
    )

    if search_term:
        search_df = df[
            df["raw_question_en"].str.contains(search_term, case=False, na=False)
        ]

        st.markdown(f"**Found {len(search_df)} matching questions:**")

        for idx, row in search_df.iterrows():
            raw_q = row["raw_question_en"]
            current_std = row["standardized_question"]
            row_count = row["row_count"]

            has_pending = raw_q in st.session_state.pending_changes

            with st.expander(
                f"{'🔄 ' if has_pending else ''}[{current_std}] {raw_q[:60]}... ({row_count:,} rows)"
            ):
                st.markdown(f"**Current category:** `{current_std}`")
                st.text_area(
                    "Question:",
                    raw_q,
                    height=80,
                    disabled=True,
                    key=f"sq_{hash(raw_q)}",
                )

                col1, col2 = st.columns([2, 1])

                with col1:
                    new_std = st.text_input(
                        "New standardized_question:",
                        value=st.session_state.pending_changes.get(raw_q, ""),
                        key=f"snew_{hash(raw_q)}",
                    )

                    if new_std and not is_snake_case(new_std):
                        st.warning(f"⚠️ Suggested: `{to_snake_case(new_std)}`")
                    elif new_std:
                        st.session_state.pending_changes[raw_q] = new_std

                with col2:
                    if st.button("🤖 Suggestions", key=f"sml_{hash(raw_q)}"):
                        suggestions = get_ml_suggestions(
                            raw_q, model, encoder, transformer
                        )
                        for label, conf in suggestions[:3]:
                            if st.button(
                                f"{label}", key=f"spick_{hash(raw_q)}_{label}"
                            ):
                                st.session_state.pending_changes[raw_q] = label
                                st.rerun()

                # Quick move
                move_to = st.selectbox(
                    "Move to:",
                    options=[""] + std_questions_df["standardized_question"].tolist(),
                    key=f"smove_{hash(raw_q)}",
                )
                if move_to:
                    st.session_state.pending_changes[raw_q] = move_to

# Tab 3: All Questions Table
with tab3:
    st.subheader("All Questions (Table View)")

    # Add filter
    filter_std = st.multiselect(
        "Filter by standardized_question:",
        options=std_questions_df["standardized_question"].tolist(),
    )

    display_df = df.copy()
    if filter_std:
        display_df = display_df[display_df["standardized_question"].isin(filter_std)]

    # Add pending changes column
    display_df["pending_change"] = display_df["raw_question_en"].apply(
        lambda x: st.session_state.pending_changes.get(x, "")
    )

    # Display with st.data_editor for inline editing
    st.markdown(f"**Showing {len(display_df)} questions**")

    edited_df = st.data_editor(
        display_df[
            ["standardized_question", "pending_change", "raw_question_en", "row_count"]
        ],
        column_config={
            "standardized_question": st.column_config.TextColumn(
                "Current Category", disabled=True
            ),
            "pending_change": st.column_config.TextColumn(
                "New Category (edit here)", width="medium"
            ),
            "raw_question_en": st.column_config.TextColumn(
                "Question", width="large", disabled=True
            ),
            "row_count": st.column_config.NumberColumn("Rows", disabled=True),
        },
        hide_index=True,
        use_container_width=True,
        num_rows="fixed",
    )

    # Update pending changes from edited dataframe
    for idx, row in edited_df.iterrows():
        raw_q = row["raw_question_en"]
        new_val = row["pending_change"]
        if new_val and new_val.strip():
            if not is_snake_case(new_val):
                st.warning(f"'{new_val}' is not snake_case for: {raw_q[:50]}...")
            else:
                st.session_state.pending_changes[raw_q] = new_val
        elif raw_q in st.session_state.pending_changes and not new_val:
            del st.session_state.pending_changes[raw_q]

# Footer
st.divider()
st.markdown(
    """
**Instructions:**
1. Browse categories or search for questions
2. Enter new `standardized_question` values (must be snake_case)
3. Use 🤖 to get ML suggestions based on question content
4. Review pending changes in sidebar
5. Click "Apply All" to update BigQuery
"""
)
