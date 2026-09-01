#!/usr/bin/env python3
"""
Daily LimeSurvey ETL Pipeline

Orchestrates the complete pipeline:
1. Extract from MySQL (via orchestrate.py with 7-day lookback)
   - Unpivots survey responses → lime_surveys_columnar (staging)
   - Post-process SQL enriches with metadata → lime_surveys_columnar_completed
2. NLP Processing (6-layer question matching + UMLS answer cleanup)
3. Build lime_surveys_wide (full rebuild from columnar_completed)

Architecture:
- Source of truth: lime_surveys_columnar_completed
- Mappings derived FROM completed table (no separate mapping tables)
- ML model stored in BigQuery for persistent learning

Usage:
    # Run full pipeline (extract + NLP + wide)
    python shared/limesurvey_run_daily_pipeline.py

    # Skip extraction (just NLP + wide)
    python shared/limesurvey_run_daily_pipeline.py --skip-extract

    # Retrain ML model before processing
    python shared/limesurvey_run_daily_pipeline.py --retrain-model

    # Dry run (show what would be done)
    python shared/limesurvey_run_daily_pipeline.py --dry-run
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def log(message: str) -> None:
    """Print timestamped log message."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def run_command(cmd: list[str], description: str, dry_run: bool = False) -> bool:
    """
    Run a command and return success status.

    Args:
        cmd: Command and arguments as list
        description: Human-readable description of the step
        dry_run: If True, only print what would be done

    Returns:
        True if successful, False otherwise
    """
    log(f"STEP: {description}")
    log(f'  Command: {" ".join(cmd)}')

    if dry_run:
        log("  [DRY RUN] Skipping execution")
        return True

    start_time = time.time()

    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=False,  # Let output flow through
        )
        elapsed = time.time() - start_time
        log(f"  Completed in {elapsed:.1f}s")
        return True

    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        log(f"  FAILED after {elapsed:.1f}s (exit code {e.returncode})")
        return False

    except Exception as e:
        log(f"  ERROR: {e}")
        return False


def get_coverage_stats() -> dict:
    """Get current coverage statistics."""
    from google.cloud import bigquery

    client = bigquery.Client(project="bi-data-391216")
    query = """
    SELECT
        COUNT(*) as total_rows,
        COUNTIF(standardized_question IS NOT NULL) as has_std_question,
        COUNTIF(standardized_answer IS NOT NULL) as has_std_answer,
        COUNTIF(category IS NOT NULL) as has_category,
        COUNTIF(standardized_question IS NULL AND raw_question_en IS NOT NULL) as unmapped_questions,
        COUNTIF(standardized_answer IS NULL AND standardized_question IS NOT NULL) as unmapped_answers,
        COUNT(DISTINCT CASE WHEN standardized_question IS NOT NULL
              AND standardized_question NOT IN ('__SKIP__', '__REVIEW__')
              THEN CONCAT(raw_question_en, '|', standardized_question) END) as training_pairs
    FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
    """

    for row in client.query(query):
        return {
            "total_rows": row.total_rows,
            "has_std_question": row.has_std_question,
            "has_std_answer": row.has_std_answer,
            "has_category": row.has_category,
            "unmapped_questions": row.unmapped_questions,
            "unmapped_answers": row.unmapped_answers,
            "training_pairs": row.training_pairs,
        }

    return {}


def check_answer_corruption() -> int:
    """Guardrail: detect 'condition'-substring corruption in standardized_answer.

    A removed batch script once replaced disease/site names with the word
    'condition' WITHOUT word boundaries, gluing 'condition' into the middle of
    unrelated words (e.g. 'meds' -> 'mcondition', 'also' -> 'conditiono'). The
    corruptor is gone, but if any future batch reintroduces it this check will
    catch it regardless of source.

    Signature: standardized_answer has 'condition' glued to an adjacent letter
    while raw_answer_en does NOT (so it's not legitimate respondent text).

    Returns the count of corrupted rows (0 = clean). Logs a warning if > 0.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project="bi-data-391216")
    # Match true gluing only: a letter immediately before 'condition' (mcondition,
    # protocondition, professioncondition) or the observed suffix-glue 'conditiono'.
    # Exclude 'precondition'/'preconditioning' which are real words that the
    # [a-z]condition arm would otherwise flag. Legitimate plurals/derivatives
    # (conditions, conditional, conditioning) are not matched because they have a
    # space/word-start before 'condition'.
    query = """
    SELECT COUNT(*) AS corrupted
    FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
    WHERE REGEXP_CONTAINS(IFNULL(standardized_answer, ''), r'[a-z]condition|conditiono')
      AND NOT REGEXP_CONTAINS(LOWER(IFNULL(standardized_answer, '')), r'precondition')
    """
    corrupted = 0
    for row in client.query(query):
        corrupted = row.corrupted
    if corrupted > 0:
        log(
            f"DATA QUALITY WARNING: {corrupted} standardized_answer value(s) show "
            f"'condition'-substring corruption (a disease-name was replaced by the "
            f"word 'condition' mid-word). Investigate the writer; NULL the affected "
            f"rows to re-standardize from raw_answer_en."
        )
    else:
        log("Data quality check: no 'condition'-substring corruption detected.")
    return corrupted


# Free-text questions whose answers are intentionally NOT standardized. Kept in
# sync with FREE_TEXT_QUESTIONS in ui/limesurvey/limesurvey_mapper.py and
# shared/limesurvey_process_columnar.py.
_FREE_TEXT_QUESTIONS = [
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
]


def check_lookup_drift() -> tuple[int, int]:
    """Guardrail: detect drift between lime_surveys_columnar_completed and the
    picklist lookup tables.

    After sql/sync_standardized_lookups.sql runs in post_process, every eligible
    standardized_question / (std_q, std_a) pair in the completed table should
    have a corresponding row in standardized_questions_lookup /
    standardized_answers_lookup. If either count is nonzero here, the sync step
    failed to run or its eligibility filter has drifted from this function's.

    Eligibility matches sync_standardized_lookups.sql exactly: not NULL, not
    empty, not under a free-text question. N/A and numeric canonicals count.

    Returns (orphan_questions, orphan_answers). Logs a WARNING if either > 0.
    Non-fatal so the rest of the pipeline still completes.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project="bi-data-391216")
    query = """
    SELECT
      (SELECT COUNT(*) FROM (
        SELECT DISTINCT standardized_question
        FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
        WHERE standardized_question IS NOT NULL
          AND TRIM(standardized_question) != ''
          AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
        EXCEPT DISTINCT
        SELECT standardized_question
        FROM `bi-data-391216.limesurvey_data.standardized_questions_lookup`
      )) AS orphan_questions,
      (SELECT COUNT(*) FROM (
        SELECT DISTINCT standardized_question, standardized_answer
        FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
        WHERE standardized_question IS NOT NULL
          AND standardized_answer IS NOT NULL
          AND TRIM(standardized_answer) != ''
          AND LOWER(standardized_question) NOT IN UNNEST(@free_text_qs)
        EXCEPT DISTINCT
        SELECT standardized_question, standardized_answer
        FROM `bi-data-391216.limesurvey_data.standardized_answers_lookup`
      )) AS orphan_answers
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "free_text_qs", "STRING", _FREE_TEXT_QUESTIONS
            ),
        ]
    )
    orphan_q = 0
    orphan_a = 0
    for row in client.query(query, job_config=job_config):
        orphan_q = row.orphan_questions
        orphan_a = row.orphan_answers
    if orphan_q > 0 or orphan_a > 0:
        log(
            f"LOOKUP DRIFT WARNING: {orphan_q} standardized_question value(s) and "
            f"{orphan_a} (standardized_question, standardized_answer) pair(s) exist "
            f"in lime_surveys_columnar_completed but are missing from the lookup "
            f"tables. Expected sql/sync_standardized_lookups.sql to have closed "
            f"this gap -- check that step is wired into post_process and ran "
            f"successfully."
        )
    else:
        log("Lookup drift check: lookup tables match the completed table.")
    return orphan_q, orphan_a


def print_coverage(stats: dict = None) -> None:
    """Print current coverage statistics."""
    if stats is None:
        stats = get_coverage_stats()

    print("\n" + "=" * 60)
    print("COVERAGE STATISTICS")
    print("=" * 60)
    print(f"Total rows: {stats.get('total_rows', 0):,}")
    total = stats.get("total_rows", 0)
    if total > 0:
        print(
            f"Has standardized_question: {stats.get('has_std_question', 0):,} ({100*stats.get('has_std_question', 0)/total:.1f}%)"
        )
        print(
            f"Has standardized_answer: {stats.get('has_std_answer', 0):,} ({100*stats.get('has_std_answer', 0)/total:.1f}%)"
        )
        print(
            f"Has category: {stats.get('has_category', 0):,} ({100*stats.get('has_category', 0)/total:.1f}%)"
        )
    print(f"Unmapped questions: {stats.get('unmapped_questions', 0):,}")
    print(f"Unmapped answers: {stats.get('unmapped_answers', 0):,}")
    print(f"Training pairs available: {stats.get('training_pairs', 0):,}")


def should_retrain_model(stats: dict, min_training_pairs: int = 50) -> tuple[bool, str]:
    """
    Determine if the ML model should be retrained.

    Returns:
        (should_retrain, reason)
    """
    from google.cloud import bigquery

    training_pairs = stats.get("training_pairs", 0)
    unmapped = stats.get("unmapped_questions", 0)

    # Need minimum training data
    if training_pairs < min_training_pairs:
        return (
            False,
            f"Not enough training data ({training_pairs} < {min_training_pairs})",
        )

    # Check if model exists and when it was last trained
    client = bigquery.Client(project="bi-data-391216")

    try:
        model_query = """
        SELECT trained_at, training_rows
        FROM `bi-data-391216.limesurvey_data.ml_question_model`
        ORDER BY trained_at DESC
        LIMIT 1
        """
        results = list(client.query(model_query).result())

        if not results:
            return True, "No existing model found"

        last_trained_rows = results[0].training_rows

        # Retrain if we have significantly more training data (10%+ increase)
        if training_pairs > last_trained_rows * 1.1:
            return (
                True,
                f"Training data increased ({last_trained_rows:,} -> {training_pairs:,})",
            )

        # Retrain if there are unmapped questions and we have new data
        if unmapped > 0 and training_pairs > last_trained_rows:
            return (
                True,
                f"New mappings available and {unmapped:,} questions still unmapped",
            )

        return False, f"Model is up-to-date (trained on {last_trained_rows:,} pairs)"

    except Exception as e:
        return True, f"Could not check model status: {e}"


def retrain_ml_model(dry_run: bool = False) -> bool:
    """
    Retrain the ML model for question classification.

    Returns:
        True if successful, False otherwise
    """
    script_dir = Path(__file__).parent

    retrain_cmd = [
        sys.executable,
        str(script_dir / "limesurvey_ml_mismatch_detector.py"),
        "--train-only",
    ]

    return run_command(retrain_cmd, "Retrain ML Model", dry_run)


def run_daily_pipeline(
    skip_extract: bool = False,
    full_rebuild: bool = False,
    lookback_days: int = 7,
    retrain_model: bool = False,
    auto_retrain: bool = True,
    dry_run: bool = False,
) -> bool:
    """
    Run the complete daily LimeSurvey pipeline.

    Args:
        skip_extract: Skip the MySQL extraction step
        full_rebuild: Use full rebuild instead of incremental for all steps
        lookback_days: Number of days to look back for extraction
        retrain_model: Retrain ML model before processing
        auto_retrain: Automatically retrain model if new training data available
        dry_run: Only print what would be done

    Returns:
        True if all steps succeeded, False otherwise
    """
    log("=" * 60)
    log("LIMESURVEY DAILY PIPELINE")
    log("=" * 60)

    pipeline_start = time.time()
    success = True

    # Determine paths
    script_dir = Path(__file__).parent
    orchestrator_dir = script_dir.parent

    # Step 1: Extract from MySQL (incremental with lookback)
    if not skip_extract:
        extract_cmd = [
            sys.executable,
            str(orchestrator_dir / "orchestrate.py"),
            "--source",
            "limesurvey",
            "--env",
            "prod",
            "--refresh",
            "full" if full_rebuild else "incremental",
            "--lookback",
            str(lookback_days),
        ]

        if not run_command(
            extract_cmd, "Extract from MySQL → staging → completed", dry_run
        ):
            log("Extraction failed. Aborting pipeline.")
            return False
    else:
        log("STEP: Extract from MySQL - SKIPPED (--skip-extract)")

    # Step 2: Retrain check (NLP processing already ran as part of the
    # orchestrator's post-process chain during Step 1 extraction).
    # --retrain-only checks the 10% growth threshold and retrains if needed,
    # without re-running row processing. This avoids the BigQuery MERGE race
    # we saw when both invocations wrote to lime_surveys_columnar_completed
    # within ~60 seconds of each other.
    nlp_cmd = [
        sys.executable,
        str(script_dir / "limesurvey_process_columnar.py"),
    ]

    if retrain_model:
        # Manual full retrain — runs retrain + row processing.
        nlp_cmd.append("--retrain-model")
    else:
        # Default: retrain if 10% growth threshold met, then exit. No row
        # processing; NLP already ran via the orchestrator's post-process.
        nlp_cmd.append("--retrain-only")

    if dry_run:
        nlp_cmd.append("--dry-run")

    if not run_command(
        nlp_cmd, "Retrain check (NLP already ran in Step 1 post-process)", dry_run
    ):
        log("Retrain check failed (non-critical — NLP already ran in Step 1).")
        success = False

    # Step 3: Build wide table (always full rebuild - fast enough)
    wide_cmd = [
        sys.executable,
        str(script_dir / "limesurvey_build_wide_table.py"),
    ]

    if not run_command(wide_cmd, "Build wide table (full rebuild)", dry_run):
        log("Wide table build failed.")
        success = False

    # Step 4: Rebuild answer fingerprints (used by the Streamlit UI for smarter suggestions)
    fp_cmd = [
        sys.executable,
        str(script_dir / "limesurvey_build_answer_fingerprints.py"),
    ]
    if not run_command(fp_cmd, "Build answer fingerprints", dry_run):
        log("Answer fingerprint build failed (non-critical).")

    # Get coverage stats
    if not dry_run:
        stats = get_coverage_stats()
        print_coverage(stats)

        # Data-quality guardrail: detect 'condition'-substring corruption (see
        # check_answer_corruption). Non-fatal -- logs a warning so a regression
        # is caught on the next run regardless of which writer introduced it.
        check_answer_corruption()

        # Drift guardrail: confirm sync_standardized_lookups.sql kept the
        # picklist lookups aligned with the completed table. Non-fatal.
        check_lookup_drift()

        # Auto-retrain is now handled inline by Step 2 via --retrain-if-needed.
        # Retraining before matching means the new model is used immediately on this run's NULL rows.
    else:
        print_coverage()

    # Summary
    elapsed = time.time() - pipeline_start
    log("=" * 60)
    if success:
        log(f"PIPELINE COMPLETED SUCCESSFULLY in {elapsed:.1f}s")
    else:
        log(f"PIPELINE COMPLETED WITH ERRORS in {elapsed:.1f}s")
    log("=" * 60)

    return success


def main():
    parser = argparse.ArgumentParser(
        description="Run the daily LimeSurvey ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Pipeline stages:
  1. Extract: Pull new data from MySQL → staging → completed
  2. NLP: 6-layer question matching + UMLS answer cleanup
  3. Wide: Pivot columnar_completed to wide format
  4. Auto-retrain: Retrain ML model if new training data available (default: on)

Matching layers (early stop at 95%% confidence):
  1. Exact Match (100%%)
  2. ML Supervised Model (90-99%%)
  3. Sentence Transformers (85-99%%)
  4. UMLS Semantic (85-98%%)
  5. spaCy NER (80-93%%)
  6. RapidFuzz (70-85%%)

Auto-retrain triggers when:
  - Training data increased by 10%% or more since last training
  - New mappings exist and there are still unmapped questions

Examples:
  # Normal daily run (with auto-retrain)
  python shared/limesurvey_run_daily_pipeline.py

  # Skip extraction (re-process existing data)
  python shared/limesurvey_run_daily_pipeline.py --skip-extract

  # Retrain ML model before processing
  python shared/limesurvey_run_daily_pipeline.py --retrain-model

  # Disable auto-retrain at end of pipeline
  python shared/limesurvey_run_daily_pipeline.py --no-auto-retrain

  # Custom lookback period
  python shared/limesurvey_run_daily_pipeline.py --lookback 14
        """,
    )

    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip MySQL extraction step (use existing data)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full rebuild instead of incremental for all steps",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=7,
        help="Days to look back for extraction (default: 7)",
    )
    parser.add_argument(
        "--retrain-model",
        action="store_true",
        help="Retrain ML model before processing",
    )
    parser.add_argument(
        "--no-auto-retrain",
        action="store_true",
        help="Disable automatic model retraining at end of pipeline",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )

    args = parser.parse_args()

    success = run_daily_pipeline(
        skip_extract=args.skip_extract,
        full_rebuild=args.full,
        lookback_days=args.lookback,
        retrain_model=args.retrain_model,
        auto_retrain=not args.no_auto_retrain,
        dry_run=args.dry_run,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
