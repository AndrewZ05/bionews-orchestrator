"""
Dry-run every profile_database SQL file against BigQuery, statement-by-statement,
inside a single BQ session per file so temp tables / variables persist across
statements (matching how the orchestrator runs them).

Reports column/table/type errors without billing bytes or changing data.

Usage:
    python scripts/dry_run_profile_sql.py                 # all files
    python scripts/dry_run_profile_sql.py refresh         # substring filter
    python scripts/dry_run_profile_sql.py --lookback 7    # template var
"""

import argparse
import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Authenticate the same way every other pipeline entry point does (cf.
# orchestrate.py). Without this the script inherits whatever ambient ADC the
# shell happens to carry -- typically an end-user gcloud login, which lacks
# bigquery.jobs.create on the project -- and every statement 403s. Loading .env
# here picks up GOOGLE_APPLICATION_CREDENTIALS so the validator runs as the
# pipeline service account from any shell, with no per-shell setup.
import dotenv

dotenv.load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ConnectionProperty
from shared.post_processor import _rewrite_internal_datasets, _split_sql_statements
from shared.profile_database_manifest import (
    COMPATIBILITY_VIEWS,
    DRY_RUN_SQL_FILES,
    SITE_EVENTS_LOOKBACK_DAYS,
    SITE_EVENTS_RELOAD_DAYS_FULL,
)

# The runner substitutes this from plugins/profile_database_extractor.py, which
# defines it locally rather than in the manifest. Kept in sync by
# test_dry_run_placeholder_coverage.
PROBABILISTIC_FLAG_THRESHOLD = 0.5

PROJECT_ID = "bi-data-391216"
REPO_ROOT = Path(__file__).resolve().parents[1]
DDL_SQL = REPO_ROOT / "sql" / "profile_database_ddl.sql"

SQL_FILES = list(DRY_RUN_SQL_FILES)
TABLE_TO_VIEW_MIGRATION_VIEWS = set(COMPATIBILITY_VIEWS) | {"profile_events"}
import re as _re

VIEWS_DEFINED_IN_FILE = set(
    _re.findall(
        r"CREATE OR REPLACE VIEW\s+\{view_dataset\}\.(\w+)",
        (REPO_ROOT / "sql" / "profile_database_views.sql").read_text(encoding="utf-8"),
    )
)


def _fatal_if_auth_error(exc: Exception) -> None:
    """A 403 means the run cannot validate anything -- stop instead of warning.

    Preseed failures are normally benign (a stub table that already exists, or
    real schema drift), so they print as warnings and the run continues. A
    permission error is different in kind: nothing downstream can execute, so
    every later statement fails too and the run emits hundreds of warnings that
    look like output but check nothing -- which reads as a pass. Fail loudly and
    say how to fix it.
    """
    from google.api_core.exceptions import Forbidden, Unauthorized

    if not isinstance(exc, (Forbidden, Unauthorized)):
        return
    cred = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "(unset)"
    lines = [
        "",
        f"[FATAL] BigQuery denied this identity permission to create jobs on {PROJECT_ID}.",
        f"        GOOGLE_APPLICATION_CREDENTIALS = {cred}",
        "        The validator authenticates from the repo .env. Check that it sets",
        "        GOOGLE_APPLICATION_CREDENTIALS to the pipeline service-account key,",
        "        and that a value already exported in your shell is not overriding it",
        "        with an end-user gcloud login.",
        f"        Underlying error: {str(exc)[:200]}",
    ]
    print("\n".join(lines), file=sys.stderr)
    sys.exit(2)


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def preseed(
    client: bigquery.Client,
    consumer_dataset: str,
    ops_dataset: str,
    staging_dataset: str,
):
    """Create empty stubs for tables/datasets the dry-run references but that
    don't exist in production yet (because DDL hasn't been re-applied).
    Idempotent — uses CREATE IF NOT EXISTS / CREATE SCHEMA IF NOT EXISTS.
    Runs as real queries (not dry_run) so the empty tables actually exist."""
    schema_stmts = [
        f"CREATE SCHEMA IF NOT EXISTS {consumer_dataset}",
        f"CREATE SCHEMA IF NOT EXISTS {ops_dataset}",
        f"CREATE SCHEMA IF NOT EXISTS {staging_dataset}",
    ]
    for s in schema_stmts:
        try:
            client.query(s).result()
        except Exception as e:
            _fatal_if_auth_error(e)
            print(f"[preseed WARN] {s[:70]}... -> {str(e)[:150]}")

    if DDL_SQL.exists():
        ddl_sql = _rewrite_internal_datasets(
            DDL_SQL.read_text(encoding="utf-8"),
            consumer_dataset=consumer_dataset,
            ops_dataset=ops_dataset,
            staging_dataset=staging_dataset,
        )
        for stmt in _split_sql_statements(ddl_sql):
            if not is_executable(stmt):
                continue
            try:
                client.query(stmt).result()
            except Exception as e:
                _fatal_if_auth_error(e)
                print(f"[preseed WARN] {summarize(stmt)[:70]}... -> {str(e)[:150]}")

    stmts = [
        """CREATE TABLE IF NOT EXISTS profile_staging.profile_core_app_snapshot (
            bn_id STRING,
            terms_accepted_at TIMESTAMP,
            privacy_acknowledged_at TIMESTAMP,
            user_photo_url STRING,
            nickname STRING,
            treatments_of_interest ARRAY<STRUCT<label STRING, rxnorm_id STRING>>,
            condition_subtype STRUCT<label STRING, business_code STRING, mesh_id STRING>,
            condition_subtype_source STRING,
            diagnosis_stage STRING,
            diagnosis_stage_source STRING,
            specialty STRUCT<label STRING, snomed_id STRING>,
            condition_focus ARRAY<STRUCT<label STRING, mesh_id STRING>>,
            snapshotted_at TIMESTAMP
        )""",
        # Add the two new v6.1 columns to profile_core if missing so the
        # views + enrich_v2 Part 19 dry-runs succeed.
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS preferred_condition_confidence FLOAT64",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS preferred_condition_updated_at TIMESTAMP",
        # v6.6 condition normalization columns
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS preferred_condition_normalized STRUCT<condition_key STRING, label STRING, mesh_id STRING>",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS preferred_condition_normalized_source STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS preferred_condition_normalized_confidence FLOAT64",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS preferred_condition_normalized_updated_at TIMESTAMP",
        # v6.2 MVP-gap additions — 7 new profile_core columns.
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS joined_caregiver_community BOOL",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS caregiver_focus_areas ARRAY<STRING>",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS treatments_discussed ARRAY<STRUCT<label STRING, rxnorm_id STRING>>",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS patient_volume_band STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS practice_setting STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS interest_tags ARRAY<STRING>",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS follow_conditions ARRAY<STRUCT<label STRING, mesh_id STRING>>",
        # v6.2 snapshot table additions (mirror of profile_core app-written fields).
        "ALTER TABLE profile_staging.profile_core_app_snapshot ADD COLUMN IF NOT EXISTS treatments_discussed ARRAY<STRUCT<label STRING, rxnorm_id STRING>>",
        "ALTER TABLE profile_staging.profile_core_app_snapshot ADD COLUMN IF NOT EXISTS patient_volume_band STRING",
        "ALTER TABLE profile_staging.profile_core_app_snapshot ADD COLUMN IF NOT EXISTS practice_setting STRING",
        "ALTER TABLE profile_staging.profile_core_app_snapshot ADD COLUMN IF NOT EXISTS interest_tags ARRAY<STRING>",
        "ALTER TABLE profile_staging.profile_core_app_snapshot ADD COLUMN IF NOT EXISTS follow_conditions ARRAY<STRUCT<label STRING, mesh_id STRING>>",
        # v6.3 person-attribute additions — Tier 1 demographics (sensitive-flagged).
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS age_exact INT64",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS age_band STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS ethnicity STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS veteran BOOL",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS primary_language STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS short_bio STRING",
        # v6.3 contact info.
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS phone STRING",
        # v6.3 general address (symmetric with HCP practice_*; for non-HCP home/mailing address).
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS address_city STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS address_state STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS address_postal_code STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS address_country STRING",
        # v6.3 extended name parts (from NPI, optional).
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS middle_name STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS name_suffix STRING",
        # v6.3 HCP extras from NPI.
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS provider_organization_name STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS practice_phone STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS npi_enumeration_date DATE",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS npi_deactivation_date DATE",
        # v6.3 promoted BOOL flags (were stuck in profile_segment_tags).
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS clinical_trials_interest BOOL",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS patient_community_interest BOOL",
        # v6.3 name prefix + app-written content preferences + newsletter rollup.
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS name_prefix STRING",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS content_preferences ARRAY<STRING>",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS joined_newsletter_topics ARRAY<STRING>",
        # v6.3 snapshot needs app-written columns too.
        "ALTER TABLE profile_staging.profile_core_app_snapshot ADD COLUMN IF NOT EXISTS content_preferences ARRAY<STRING>",
        # v6.4 stable-identifier columns for persistence-aware restore.
        "ALTER TABLE profile_staging.profile_core_app_snapshot ADD COLUMN IF NOT EXISTS email STRING",
        "ALTER TABLE profile_staging.profile_core_app_snapshot ADD COLUMN IF NOT EXISTS bionews_uk STRING",
        "ALTER TABLE profile_staging.profile_core_app_snapshot ADD COLUMN IF NOT EXISTS wp_user_id STRING",
        "ALTER TABLE profile_staging.profile_core_app_snapshot ADD COLUMN IF NOT EXISTS npi_number STRING",
        # v6.4 diagnostic table for snapshot rows the restore couldn't remap.
        """CREATE TABLE IF NOT EXISTS profile_ops.profile_restore_unmapped (
            bn_id_at_snapshot STRING,
            email STRING,
            bionews_uk STRING,
            wp_user_id STRING,
            npi_number STRING,
            reason STRING,
            snapshotted_at TIMESTAMP,
            attempted_at TIMESTAMP,
            build_id STRING
        )""",
        """CREATE TABLE IF NOT EXISTS profile_data.profile_preferences (
            bn_id STRING NOT NULL,
            newsletter_preferences ARRAY<STRUCT<
                site_domain STRING,
                newsletter_key STRING,
                newsletter_label STRING,
                is_subscribed BOOL,
                subscribed_at TIMESTAMP,
                unsubscribed_at TIMESTAMP,
                source STRING
            >>,
            forum_settings STRUCT<
                notify_forum_replies BOOL,
                notify_group_invites BOOL,
                notify_direct_messages BOOL,
                notify_mentions BOOL,
                subscribed_forum_ids ARRAY<STRING>,
                subscribed_group_ids ARRAY<STRING>,
                profile_visibility STRING,
                show_activity_feed BOOL,
                forum_registered_at TIMESTAMP,
                last_forum_activity TIMESTAMP
            >,
            loaded_at TIMESTAMP
        ) CLUSTER BY bn_id""",
        # v6.4 split build log — runs + steps. profile_build_log is now a VIEW
        # over these two tables (profile_database_views.sql). Drop only an old
        # BASE TABLE so `CREATE OR REPLACE VIEW profile_build_log` succeeds,
        # but do not warn if the relation is already a view.
        """EXECUTE IMMEDIATE (
            SELECT IF(
                EXISTS (
                    SELECT 1
                    FROM profile_data.INFORMATION_SCHEMA.TABLES
                    WHERE table_name = 'profile_build_log'
                      AND table_type = 'BASE TABLE'
                ),
                'DROP TABLE profile_data.profile_build_log',
                'SELECT 1'
            )
        )""",
        """CREATE TABLE IF NOT EXISTS profile_ops.profile_build_runs (
            build_id STRING NOT NULL,
            mode STRING,
            schema_version STRING,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            status STRING,
            identity_hub_manifest_id STRING,
            identity_hub_row_count INT64,
            preflight_status STRING,
            runtime_fingerprint STRING,
            total_steps INT64,
            failed_steps INT64,
            total_bytes_processed INT64,
            total_bytes_billed INT64,
            total_slot_millis INT64,
            assertion_summary STRING,
            row_delta_summary STRING,
            error_message STRING,
            metadata STRING
        )""",
        """CREATE TABLE IF NOT EXISTS profile_ops.profile_build_steps (
            build_id STRING NOT NULL,
            step_name STRING NOT NULL,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            duration_seconds FLOAT64,
            status STRING,
            rows_affected INT64,
            statements_executed INT64,
            total_bytes_processed INT64,
            total_bytes_billed INT64,
            total_slot_millis INT64,
            warnings STRING,
            error_message STRING
        ) CLUSTER BY build_id""",
        "ALTER TABLE profile_ops.profile_build_runs ADD COLUMN IF NOT EXISTS total_bytes_processed INT64",
        "ALTER TABLE profile_ops.profile_build_runs ADD COLUMN IF NOT EXISTS total_bytes_billed INT64",
        "ALTER TABLE profile_ops.profile_build_runs ADD COLUMN IF NOT EXISTS total_slot_millis INT64",
        "ALTER TABLE profile_ops.profile_build_runs ADD COLUMN IF NOT EXISTS runtime_fingerprint STRING",
        "ALTER TABLE profile_ops.profile_build_steps ADD COLUMN IF NOT EXISTS total_bytes_processed INT64",
        "ALTER TABLE profile_ops.profile_build_steps ADD COLUMN IF NOT EXISTS total_bytes_billed INT64",
        "ALTER TABLE profile_ops.profile_build_steps ADD COLUMN IF NOT EXISTS total_slot_millis INT64",
        # v6.4 point-in-time history + field lineage.
        # Schema must mirror profile_core; drop-and-recreate here so the
        # preseed stays idempotent when we change the shape during
        # development. Prod DDL uses CREATE IF NOT EXISTS; ops decides
        # when to DROP + recreate to pick up a new column set.
        "DROP TABLE IF EXISTS profile_ops.profile_core_snapshot",
        """CREATE TABLE profile_ops.profile_core_snapshot (
            snapshot_run_id STRING NOT NULL,
            snapshotted_at TIMESTAMP NOT NULL,
            bn_id STRING NOT NULL,
            bionews_uk STRING, email STRING, email_hash STRING,
            nickname STRING, user_photo_url STRING, site_domain STRING,
            consent_status STRING, terms_accepted_at TIMESTAMP, privacy_acknowledged_at TIMESTAMP,
            cookie_consent_date TIMESTAMP, tracking_consent BOOL, communication_opt_in BOOL,
            preferred_condition STRUCT<label STRING, mesh_id STRING>,
            preferred_condition_source STRING, preferred_condition_confidence FLOAT64,
            preferred_condition_updated_at TIMESTAMP,
            preferred_condition_normalized STRUCT<condition_key STRING, label STRING, mesh_id STRING>,
            preferred_condition_normalized_source STRING,
            preferred_condition_normalized_confidence FLOAT64,
            preferred_condition_normalized_updated_at TIMESTAMP,
            gender STRING, age_exact INT64, age_band STRING, ethnicity STRING,
            veteran BOOL, primary_language STRING, short_bio STRING, country STRING,
            phone STRING, address_city STRING, address_state STRING,
            address_postal_code STRING, address_country STRING,
            hcp_status BOOL, identity_confidence_score INT64, ga_user_id STRING,
            profile_stage STRING, profile_completeness INT64,
            created_at TIMESTAMP, last_active_at TIMESTAMP, profile_updated_at TIMESTAMP,
            name_prefix STRING, first_name STRING, middle_name STRING,
            last_name STRING, name_suffix STRING, display_name STRING,
            acquisition_source STRING, acquisition_medium STRING,
            acquisition_campaign STRING, signup_source STRING,
            cluster_tier STRING, cluster_size INT64,
            -- 2026-08-28: the stub had drifted from sql/profile_database_ddl.sql,
            -- which carries these three between cluster_size and source_systems.
            -- snapshot_core INSERTs them by name, so their absence failed that
            -- file with a column-not-found error that looked like a SQL defect.
            is_shared_workstation BOOL, is_suspicious BOOL, cluster_health_score INT64,
            source_systems ARRAY<STRING>,
            condition_subtype STRUCT<label STRING, business_code STRING, mesh_id STRING>,
            condition_subtype_source STRING,
            diagnosis_stage STRING, diagnosis_stage_source STRING,
            diagnosis_timing_band STRING,
            joined_patient_community BOOL,
            symptom_tags ARRAY<STRUCT<label STRING, mesh_id STRING, hpo_id STRING>>,
            treatments_current ARRAY<STRUCT<label STRING, rxnorm_id STRING>>,
            treatments_of_interest ARRAY<STRUCT<label STRING, rxnorm_id STRING>>,
            npi_number STRING, aim_dgid STRING,
            specialty STRUCT<label STRING, snomed_id STRING>,
            condition_focus ARRAY<STRUCT<label STRING, mesh_id STRING>>,
            treatments_discussed ARRAY<STRUCT<label STRING, rxnorm_id STRING>>,
            credentials STRING, primary_specialty_code STRING,
            all_specialties ARRAY<STRING>, provider_organization_name STRING,
            practice_city STRING, practice_state STRING, practice_postal_code STRING,
            practice_country STRING, practice_phone STRING, practice_setting STRING,
            years_in_practice_band STRING, patient_volume_band STRING,
            npi_enumeration_date DATE, npi_deactivation_date DATE,
            caregiver_condition STRUCT<label STRING, mesh_id STRING>,
            caregiver_relationship STRING, joined_caregiver_community BOOL,
            caregiver_focus_areas ARRAY<STRING>,
            family_condition STRUCT<label STRING, mesh_id STRING>,
            family_relationship STRING,
            interest_tags ARRAY<STRING>,
            follow_conditions ARRAY<STRUCT<label STRING, mesh_id STRING>>,
            content_preferences ARRAY<STRING>, joined_newsletter_topics ARRAY<STRING>,
            clinical_trials_interest BOOL, patient_community_interest BOOL,
            condition_subtype_confidence FLOAT64, condition_subtype_updated_at TIMESTAMP,
            diagnosis_stage_confidence FLOAT64, diagnosis_stage_updated_at TIMESTAMP,
            is_patient BOOL, is_patient_source STRING, is_patient_confidence FLOAT64, is_patient_updated_at TIMESTAMP,
            is_hcp BOOL, is_hcp_source STRING, is_hcp_confidence FLOAT64, is_hcp_updated_at TIMESTAMP,
            is_caregiver BOOL, is_caregiver_source STRING, is_caregiver_confidence FLOAT64, is_caregiver_updated_at TIMESTAMP,
            is_family_or_friend BOOL, is_family_or_friend_source STRING, is_family_or_friend_confidence FLOAT64, is_family_or_friend_updated_at TIMESTAMP,
            is_other BOOL, is_other_source STRING, is_other_confidence FLOAT64, is_other_updated_at TIMESTAMP
        ) PARTITION BY DATE(snapshotted_at) CLUSTER BY bn_id, snapshot_run_id""",
        """CREATE TABLE IF NOT EXISTS profile_ops.profile_field_changes (
            bn_id STRING NOT NULL,
            field_name STRING NOT NULL,
            old_value STRING,
            new_value STRING,
            changed_at TIMESTAMP NOT NULL,
            build_id STRING,
            source STRING,
            rule STRING
        ) PARTITION BY DATE(changed_at) CLUSTER BY bn_id, field_name""",
        # v6.5 lineage triples on profile_core (account_type triple dropped in Phase 2)
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS condition_subtype_confidence FLOAT64",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS condition_subtype_updated_at TIMESTAMP",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS diagnosis_stage_confidence FLOAT64",
        "ALTER TABLE profile_data.profile_core ADD COLUMN IF NOT EXISTS diagnosis_stage_updated_at TIMESTAMP",
        # Scope table used by the subset-enrichment predicates.
        """CREATE TABLE IF NOT EXISTS profile_staging.refresh_scope_bn_ids (
            bn_id STRING NOT NULL,
            source STRING,
            scoped_at TIMESTAMP
        ) CLUSTER BY bn_id""",
        """CREATE TABLE IF NOT EXISTS profile_staging.zero_party_staging (
            staging_id STRING NOT NULL,
            source_system STRING NOT NULL,
            interaction_type STRING NOT NULL,
            question_text STRING,
            answer_value STRING,
            answer_score FLOAT64,
            site_domain STRING,
            page_path STRING,
            source_ip STRING,
            source_user_agent STRING,
            responded_at TIMESTAMP NOT NULL,
            promoted_to_bn_id STRING,
            promoted_at TIMESTAMP,
            loaded_at TIMESTAMP
        ) CLUSTER BY source_system, site_domain""",
        """CREATE OR REPLACE VIEW profile_data.profile_signals AS
        SELECT
            pc.bn_id,
            CAST(NULL AS STRING) AS top_content_condition,
            CAST(NULL AS FLOAT64) AS top_affinity_score,
            CAST(NULL AS INT64) AS top_condition_pageviews,
            CAST(NULL AS ARRAY<STRUCT<
                content_condition STRING,
                affinity_score FLOAT64,
                pageview_count INT64,
                active_days INT64,
                first_viewed TIMESTAMP,
                last_viewed TIMESTAMP
            >>) AS content_affinity_signals,
            CAST(NULL AS ARRAY<STRUCT<
                tag_category STRING,
                tag_value STRING,
                source STRING,
                first_seen TIMESTAMP
            >>) AS segment_signals,
            CAST(NULL AS ARRAY<STRING>) AS segment_categories,
            CAST(NULL AS STRING) AS geography_tag,
            CAST(NULL AS STRING) AS hcp_verified_tag,
            CAST(NULL AS ARRAY<STRING>) AS ad_platforms,
            CAST(NULL AS ARRAY<STRING>) AS click_id_types,
            CAST(NULL AS TIMESTAMP) AS first_ad_click_at,
            CAST(NULL AS TIMESTAMP) AS last_ad_click_at,
            CAST(NULL AS INT64) AS ad_identifier_count,
            FALSE AS has_content_affinity_signal,
            FALSE AS has_segment_signal,
            FALSE AS has_paid_media_signal
        FROM profile_data.profile_core pc
        WHERE 1 = 0""",
        """CREATE OR REPLACE VIEW profile_data.profile_contactability AS
        SELECT
            pc.bn_id,
            pc.bionews_uk,
            pc.email,
            pc.consent_status,
            pc.communication_opt_in,
            pc.tracking_consent,
            pc.cookie_consent_date,
            CAST(NULL AS BOOL) AS consent_analytics,
            CAST(NULL AS BOOL) AS consent_advertising,
            CAST(NULL AS BOOL) AS consent_functional,
            pc.terms_accepted_at,
            pc.privacy_acknowledged_at,
            pc.last_active_at,
            pc.identity_confidence_score,
            pc.cluster_tier,
            pc.profile_stage,
            pc.preferred_condition,
            pc.condition_subtype,
            pc.npi_number,
            pc.hcp_status,
            FALSE AS has_email_channel,
            FALSE AS can_email_market,
            FALSE AS can_personalize,
            FALSE AS can_analytics_track,
            FALSE AS can_advertise,
            CAST(NULL AS STRING) AS marketing_status_reason,
            CAST(NULL AS STRING) AS contactability_status,
            CAST(NULL AS TIMESTAMP) AS consent_last_updated_at
        FROM profile_data.profile_core pc
        WHERE 1 = 0""",
        # symptoms_dict.aliases was added in v6.1 for dictionary-driven symptom
        # matching across conditions (was hardcoded MG-only). Until maintenance.sql
        # re-seeds the dict, the column may not exist.
        "ALTER TABLE profile_data.symptoms_dict ADD COLUMN IF NOT EXISTS aliases ARRAY<STRING>",
    ]
    for s in stmts:
        try:
            client.query(
                _rewrite_internal_datasets(
                    s,
                    consumer_dataset=consumer_dataset,
                    ops_dataset=ops_dataset,
                    staging_dataset=staging_dataset,
                )
            ).result()
        except Exception as e:
            _fatal_if_auth_error(e)
            print(f"[preseed WARN] {s[:70]}... -> {str(e)[:150]}")


def cleanup_datasets(
    client: bigquery.Client,
    consumer_dataset: str,
    ops_dataset: str,
    staging_dataset: str,
):
    """Best-effort cleanup for isolated dry-run datasets."""
    for dataset in (consumer_dataset, ops_dataset, staging_dataset):
        try:
            client.query(f"DROP SCHEMA IF EXISTS {dataset} CASCADE").result()
        except Exception as e:
            print(f"[cleanup WARN] DROP SCHEMA {dataset} -> {str(e)[:150]}")


def is_executable(stmt: str) -> bool:
    """True if stmt has any non-comment SQL content."""
    for line in stmt.split("\n"):
        s = line.strip()
        if s and not s.startswith("--"):
            return True
    return False


def _check_no_block_comments(sql_path: Path) -> list[str]:
    """Per saved memory rule (feedback_sql_comments.md): pipeline SQL must NOT
    contain `/* */` block comments — only `--` line comments are allowed.

    Why: archived `/* */` blocks have caused two diagnostic-confusion incidents.
    The statement-splitter handles them correctly, but a failed-statement
    "preview" that begins with `/*` makes a real downstream parse error look
    like a comment-handling bug, sending operators on a wrong-tree chase. By
    refusing them at lint time we eliminate that whole class of confusion.

    Returns a list of `<path>:<line>: ...` strings. Empty list = clean.
    """
    text = sql_path.read_text(encoding="utf-8")
    errors: list[str] = []
    in_string = False
    string_char = ""
    for lineno, line in enumerate(text.splitlines(), start=1):
        # Naive but adequate: skip lines whose first non-whitespace is `--`
        # (line comment) since `/*` inside a comment is not SQL.
        stripped = line.lstrip()
        if stripped.startswith("--"):
            continue
        # Find a `/*` that is not inside a string. We don't need a full
        # tokenizer here — `/*` inside a SQL string is exotic enough that we
        # just flag it and let humans decide.
        idx = line.find("/*")
        if idx >= 0:
            errors.append(
                f"{sql_path}:{lineno}: '/* */' block comment found "
                f"(use '--' line comments only — see feedback_sql_comments.md)"
            )
    return errors


def check_block_comments_across_active_sql(verbose: bool = False) -> list[str]:
    """Run `_check_no_block_comments` over every file in DRY_RUN_SQL_FILES.

    Returns a flat list of diagnostic strings; empty = clean.
    """
    all_errors: list[str] = []
    for rel in SQL_FILES:
        path = REPO_ROOT / rel
        if not path.exists():
            continue
        errs = _check_no_block_comments(path)
        if errs:
            all_errors.extend(errs)
            if verbose:
                for e in errs:
                    print(e)
    return all_errors


def run_preflight(
    client: bigquery.Client | None = None,
    lookback: int = 3,
    file_filter: str | None = None,
    verbose: bool = False,
) -> tuple[int, int, list[tuple[str, list[str]]], list[str]]:
    """Re-usable pre-flight wrapper for callers (e.g. orchestrator's run_pipeline).

    Creates an isolated stub dataset triple, preseeds it, runs comment-style
    + dry-run plan checks across every file in DRY_RUN_SQL_FILES, then cleans
    up the isolated datasets.

    Args:
        client: Optional pre-built BigQuery client. If None, builds one.
        lookback: Substituted for `{lookback_days}` in the SQL files.
        file_filter: Optional substring filter to limit the file set
            (matches the existing CLI `filter` positional arg).
        verbose: Print per-file outcome lines while running.

    Returns:
        (total_pass, total_fail, per_file_errors, comment_errors). The caller
        decides how to react. `total_fail == 0 and not comment_errors`
        is the "all green, safe to proceed" condition.
    """
    if client is None:
        client = get_bq_client()
    suffix = uuid.uuid4().hex[:8]
    consumer_dataset = f"profile_data_dryrun_{suffix}"
    ops_dataset = f"profile_ops_dryrun_{suffix}"
    staging_dataset = f"profile_staging_dryrun_{suffix}"

    # Comment-style check first — fast, runs on disk, no BQ cost.
    comment_errors = check_block_comments_across_active_sql(verbose=verbose)

    if verbose:
        print(
            f"[preflight] Creating isolated stub datasets: "
            f"{consumer_dataset}, {ops_dataset}, {staging_dataset}"
        )
    preseed(client, consumer_dataset, ops_dataset, staging_dataset)

    total_pass = 0
    total_fail = 0
    all_errors: list[tuple[str, list[str]]] = []

    try:
        for rel in SQL_FILES:
            if file_filter and file_filter not in rel:
                continue
            path = REPO_ROOT / rel
            if not path.exists():
                continue
            passed, failed, errors = dry_run_file(
                client,
                path,
                lookback,
                consumer_dataset=consumer_dataset,
                ops_dataset=ops_dataset,
                staging_dataset=staging_dataset,
            )
            total_pass += passed
            total_fail += failed
            if verbose:
                status = "OK" if failed == 0 else "FAIL"
                print(f"[{status:>4}] {rel}: {passed} passed, {failed} failed")
            if errors:
                all_errors.append((rel, errors))
    finally:
        cleanup_datasets(client, consumer_dataset, ops_dataset, staging_dataset)

    return total_pass, total_fail, all_errors, comment_errors


def summarize(stmt: str, width: int = 90) -> str:
    for line in stmt.split("\n"):
        s = line.strip()
        if s and not s.startswith("--"):
            return s[:width] + ("..." if len(s) > width else "")
    return stmt.strip()[:width]


def start_session(client: bigquery.Client) -> str:
    """Create a BQ session and return its ID."""
    cfg = QueryJobConfig(create_session=True)
    job = client.query("SELECT 1", job_config=cfg)
    job.result()
    return job.session_info.session_id


def dry_run_file(
    client: bigquery.Client,
    sql_path: Path,
    lookback: int,
    consumer_dataset: str,
    ops_dataset: str,
    staging_dataset: str,
):
    # Every template variable the runner substitutes must be substituted here
    # too. A missing one does not skip a check -- BigQuery parses the raw
    # `{name}` as a braced constructor and fails the statement, so the file
    # reports failures that say nothing about the SQL. Statements are counted as
    # failures, not skips, so an omission looks exactly like a real defect.
    # Guarded by test_dry_run_placeholder_coverage.
    sql = (
        sql_path.read_text(encoding="utf-8")
        .replace("{lookback_days}", str(lookback))
        .replace("{build_id}", "dry_run_build_id")
        .replace("{view_dataset}", consumer_dataset)
        .replace("{probabilistic_threshold}", str(PROBABILISTIC_FLAG_THRESHOLD))
        .replace("{site_events_lookback_days}", str(SITE_EVENTS_LOOKBACK_DAYS))
        .replace("{site_events_reload_days}", str(SITE_EVENTS_RELOAD_DAYS_FULL))
    )
    sql = _rewrite_internal_datasets(
        sql,
        consumer_dataset=consumer_dataset,
        ops_dataset=ops_dataset,
        staging_dataset=staging_dataset,
    )
    statements = [s for s in _split_sql_statements(sql) if is_executable(s)]

    # Start a session so temp tables and variables persist across statements.
    # NOTE: dry_run still does not create temp tables, so later statements that
    # SELECT from a temp table created earlier will error with "Table not found".
    # This is a fundamental BQ limitation — dry_run is statement-local for
    # non-scripting effects. We treat "Table not found for _tmp_*/_<leading-underscore>"
    # as a known-benign skip.
    passed = 0
    failed = 0
    errors: list[str] = []

    try:
        session_id = start_session(client)
    except Exception as e:
        return 0, len(statements), [f"  Session create failed: {e}"]

    conn_props = [ConnectionProperty(key="session_id", value=session_id)]

    for i, stmt in enumerate(statements):
        cfg = QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
            connection_properties=conn_props,
        )
        try:
            client.query(stmt, job_config=cfg)
            passed += 1
        except Exception as e:
            msg = str(e).split("\n")[0]
            # Strip POST URL prefix
            if ": " in msg:
                msg = msg.split(": ", 2)[-1]

            # Benign: temp tables created in prior statements aren't
            # visible to dry_run of subsequent statements. BigQuery reports
            # these in several forms; all are safe to skip.
            benign_temp = (
                # "Table X was not found ..."
                (
                    "not found" in msg.lower()
                    and (
                        "_tmp_" in msg
                        or 'Table "_' in msg
                        or f"{consumer_dataset}._" in msg
                    )
                )
                # "Table '_xxx' must be qualified with a dataset"
                or ('Table "_' in msg and "must be qualified" in msg)
                # "Cannot query over table X without a filter over column Y
                # that can be used for partition elimination" — only triggers
                # during dry_run because BQ can't see the filter we applied
                # via a prior CTE/temp table. At runtime the temp table has
                # already filtered the rows.
                or "partition elimination" in msg
            )
            # Benign: session variables. DECLARE in a prior statement
            # creates a variable that is NOT visible to subsequent dry_run
            # jobs because each dry_run is a standalone query planner call.
            # BQ's error text varies by context:
            #   - "Undeclared variable: X"
            #   - "Unrecognized name: X"
            #   - Just "X at [line:col]" when the parser is in an EXECUTE
            #     IMMEDIATE context.
            # We treat any of these as benign if the identifier ends in _sql
            # (our convention for EXECUTE IMMEDIATE query strings).
            benign_var = (
                "Undeclared variable" in msg
                or "Unrecognized name" in msg
                or ("_sql" in msg and " at [" in msg)
                # Column-not-found when dry-running a SELECT * FROM <view> WHERE
                # <computed_col> — BQ can't resolve computed columns from a view
                # defined earlier in the same file (view exists in prod but wasn't
                # created yet in this dry-run session). Safe: these views are tested
                # end-to-end in --build-mode views smoke tests.
                or (
                    " at [" in msg
                    and any(
                        col in msg
                        for col in (
                            "is_hcp_targetable",
                            "engagement_tier",
                            "is_marketing_eligible",
                            "is_personalization_eligible",
                            "cluster_tier",
                            # v6.5 new columns added to DDL -- not in live table yet;
                            # will resolve after next DDL run:
                            "specialty_source",
                            "specialty_confidence",
                            "specialty_updated_at",
                            "condition_focus_source",
                            "condition_focus_confidence",
                            "condition_focus_updated_at",
                        )
                    )
                )
            )
            benign_view_migration = "currently has type TABLE" in msg and any(
                f"{consumer_dataset}.{view_name}" in stmt
                for view_name in TABLE_TO_VIEW_MIGRATION_VIEWS
            )
            # A view defined EARLIER in profile_database_views.sql is not
            # materialised by a dry run, so a later view that selects from it
            # reports "not found". Derive the list from the file instead of
            # hand-maintaining it (profile_roles was missing and masked the
            # 2026-08-23 profile_metrics failure as a harness artifact).
            benign_view_dependency = "not found" in msg.lower() and any(
                f"{consumer_dataset}.{view_name}" in msg
                for view_name in VIEWS_DEFINED_IN_FILE
            )
            benign_replace_after_drop = (
                "Cannot replace a table with a different partitioning spec" in msg
                and "CREATE OR REPLACE TABLE" in stmt
            )

            if (
                benign_temp
                or benign_var
                or benign_view_migration
                or benign_view_dependency
                or benign_replace_after_drop
            ):
                passed += 1
                continue

            failed += 1
            errors.append(f"  [stmt {i}] {summarize(stmt)}\n           {msg}")

    return passed, failed, errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("filter", nargs="?", default=None)
    ap.add_argument("--lookback", type=int, default=3)
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--keep-datasets", action="store_true")
    args = ap.parse_args()

    # Step 1 (cheap, no BQ): comment-style check across all active SQL.
    # Block-comment archives have caused diagnostic confusion; refuse them at
    # lint time so a future failure preview never starts with '/*'.
    comment_errors = check_block_comments_across_active_sql()
    if comment_errors:
        print("BLOCK-COMMENT VIOLATIONS (use '--' line comments only):")
        for e in comment_errors:
            print(f"  {e}")
        print()
        # Fall through to the dry-run check too so the operator sees the full
        # picture in one report; we still exit non-zero at the end.

    client = get_bq_client()
    suffix = uuid.uuid4().hex[:8]
    consumer_dataset = f"profile_data_dryrun_{suffix}"
    ops_dataset = f"profile_ops_dryrun_{suffix}"
    staging_dataset = f"profile_staging_dryrun_{suffix}"

    print(
        f"[preseed] Creating isolated stub datasets for dry-run: "
        f"{consumer_dataset}, {ops_dataset}, {staging_dataset}"
    )
    preseed(client, consumer_dataset, ops_dataset, staging_dataset)
    total_pass = 0
    total_fail = 0
    all_errors: list[tuple[str, list[str]]] = []

    try:
        for rel in SQL_FILES:
            if args.filter and args.filter not in rel:
                continue
            path = REPO_ROOT / rel
            if not path.exists():
                print(f"[SKIP] {rel}")
                continue
            passed, failed, errors = dry_run_file(
                client,
                path,
                args.lookback,
                consumer_dataset=consumer_dataset,
                ops_dataset=ops_dataset,
                staging_dataset=staging_dataset,
            )
            total_pass += passed
            total_fail += failed
            status = "OK" if failed == 0 else f"FAIL"
            print(f"[{status:>4}] {rel}: {passed} passed, {failed} failed")
            if errors:
                all_errors.append((rel, errors))
    finally:
        if args.keep_datasets:
            print(
                f"[cleanup] Keeping isolated dry-run datasets for debugging: "
                f"{consumer_dataset}, {ops_dataset}, {staging_dataset}"
            )
        else:
            cleanup_datasets(client, consumer_dataset, ops_dataset, staging_dataset)

    print()
    print("=" * 80)
    print(
        f"TOTAL: {total_pass} passed, {total_fail} failed; "
        f"block-comment violations: {len(comment_errors)}"
    )
    print("=" * 80)

    if all_errors:
        print("\nERRORS (excluding benign temp-table / session-var artifacts):")
        for rel, errors in all_errors:
            print(f"\n{rel}:")
            for e in errors:
                print(e)

    if all_errors or comment_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
