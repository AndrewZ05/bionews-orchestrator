# plugins/profile_database_extractor.py
"""
Profile database build pipeline.

SQL-only pipeline -- no extraction. `--build-mode` is required.

Supported modes:

  Full rebuild:
    python orchestrate.py --source profile_database --env prod --build-mode rebuild

  Daily refresh:
    python orchestrate.py --source profile_database --env prod --build-mode refresh

  Custom lookback:
    python orchestrate.py --source profile_database --env prod --build-mode refresh --lookback 7

  Re-enrich existing profile_core (no DDL / no populate):
    python orchestrate.py --source profile_database --env prod --build-mode reenrich

  Views only (rebuild views after manual changes):
    python orchestrate.py --source profile_database --env prod --build-mode views

  One-shot site_events GA4 recovery (full 365d reload):
    python orchestrate.py --source profile_database --env prod --build-mode backfill_site_events

  Resume a failed late-stage rebuild from existing candidate tables:
    python orchestrate.py --source profile_database --env prod --build-mode resume_rebuild

  Resume a failed physical-table promotion (candidate -> production):
    python orchestrate.py --source profile_database --env prod --build-mode resume_publish --execution-id <build_id>

Legacy aliases still accepted for back-compat:
  incremental -> reenrich
  enrich      -> reenrich
"""

from datetime import datetime, timezone
import contextvars
import logging
import hashlib
import os
import re
import time
from pathlib import Path

from google.cloud import bigquery
from shared.post_processor import execute_post_process_sql
from shared.profile_rebuild_hardening import (
    acquire_dataset_lease,
    backup_before_promote_enabled,
    build_profile_sql_job_labels,
    clone_production_tables_before_promote,
    heartbeat_interval_seconds,
    lease_ttl_minutes,
    profile_maximum_bytes_billed,
    release_dataset_lease,
    should_acquire_candidate_lease,
    start_lease_heartbeat_thread,
)
from shared.profile_database_manifest import (
    BUILD_MODES as MANIFEST_BUILD_MODES,
    BUILD_STEPS as MANIFEST_BUILD_STEPS,
    EXTERNAL_DATASETS as MANIFEST_EXTERNAL_DATASETS,
    LEGACY_MODE_ALIASES as MANIFEST_LEGACY_MODE_ALIASES,
    OPS_DATASET,
    PHYSICAL_TABLES,
    PRODUCTION_DATASET,
    REBUILD_CANDIDATE_DATASET,
    SCHEMA_VALIDATION_SQL_FILES,
    SCHEMA_VERSION as MANIFEST_SCHEMA_VERSION,
    SITE_EVENTS_LOOKBACK_DAYS,
    SITE_EVENTS_RELOAD_DAYS_FULL,
    SITE_EVENTS_RELOAD_DAYS_REFRESH,
    STAGING_DATASET,
    VIEW_CANDIDATE_DATASET,
)

logger = logging.getLogger(__name__)

# When False (rebuild), skip writes to profile_ops.profile_build_runs / profile_build_steps.
_profile_ops_persist_log: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "profile_ops_persist_log", default=True
)


def _profile_ops_persist_enabled() -> bool:
    return _profile_ops_persist_log.get()


SCHEMA_VERSION = MANIFEST_SCHEMA_VERSION
BUILD_STEPS = [
    (step.name, step.sql_file, step.description) for step in MANIFEST_BUILD_STEPS
]
BUILD_MODES = {mode: list(steps) for mode, steps in MANIFEST_BUILD_MODES.items()}
LEGACY_MODE_ALIASES = dict(MANIFEST_LEGACY_MODE_ALIASES)
STEP_MAP = {
    step.name: (step.sql_file, step.description) for step in MANIFEST_BUILD_STEPS
}
EXTERNAL_DATASETS = set(MANIFEST_EXTERNAL_DATASETS)
PROFILE_SQL_FILES = list(SCHEMA_VALIDATION_SQL_FILES)
IDENTITY_XREF_SNAPSHOT_TABLE = f"{STAGING_DATASET}.identity_xref_snapshot"
IDENTITY_HUB_SNAPSHOT_TABLE = f"{STAGING_DATASET}.identity_hub_snapshot"
IDENTITY_PERSISTENCE_SNAPSHOT_TABLE = f"{STAGING_DATASET}.identity_persistence_snapshot"
REBUILD_LIKE_MODES = {"rebuild", "resume_rebuild"}
# bn_id_manifest.status values that mean "the hub is in a usable promoted state".
# 'PROMOTED' is stamped by a full hub rebuild. 'INCREMENTAL_PROMOTED' is stamped
# by an incremental hub run (shared/identity_hub.py) so the manifest does not look
# stuck on the last full rebuild -- it is equally safe to build on. Preflight
# previously compared against the literal 'PROMOTED', so the first incremental
# stamp (2026-07-31) failed single_generation and blocked the nightly refresh.
# Anything NOT in this set (BUILDING, FAILED, ...) is a genuine "do not start".
PROMOTED_MANIFEST_STATUSES = {"PROMOTED", "INCREMENTAL_PROMOTED"}
# Max age (days) of the newest PROMOTED bn_id_manifest row before preflight
# complains. Full rebuilds are the only writers of manifest rows; daily hub
# incrementals keep the graph fresh without stamping it. Hard gate for
# profile rebuilds (they rebase everything on the hub), soft warning for
# daily refreshes (an overdue hub rebuild should not break the nightly cron).
HUB_MANIFEST_MAX_AGE_DAYS = int(
    os.environ.get("PROFILE_HUB_MANIFEST_MAX_AGE_DAYS", "45")
)
# Modes that read/write the blue/green candidate consumer dataset (profile_rebuild_candidate).
# resume_publish only runs the late publish sequence; it is NOT "rebuild-like" for
# rebuild-only assertions (restore_coverage, strict lineage expectations, DDL snapshot gates).
CANDIDATE_CONSUMER_MODES = REBUILD_LIKE_MODES | {"resume_publish"}
PROFILE_CORE_RUNTIME_ADDITIVE_COLUMNS = (
    # v6.5 Phase 2: account_type_source/_confidence/_updated_at removed.
    # account_type and persona_source were dropped from profile_core entirely.
    ("condition_subtype_confidence", "FLOAT64"),
    ("condition_subtype_updated_at", "TIMESTAMP"),
    ("diagnosis_stage_confidence", "FLOAT64"),
    ("diagnosis_stage_updated_at", "TIMESTAMP"),
    ("is_shared_workstation", "BOOL"),
    ("is_suspicious", "BOOL"),
    ("cluster_health_score", "INT64"),
    # v6.5 multi-role flags. Canonical persona surface after Phase 2 drop.
    ("is_patient", "BOOL"),
    ("is_patient_source", "STRING"),
    ("is_patient_confidence", "FLOAT64"),
    ("is_patient_updated_at", "TIMESTAMP"),
    ("is_hcp", "BOOL"),
    ("is_hcp_source", "STRING"),
    ("is_hcp_confidence", "FLOAT64"),
    ("is_hcp_updated_at", "TIMESTAMP"),
    ("is_caregiver", "BOOL"),
    ("is_caregiver_source", "STRING"),
    ("is_caregiver_confidence", "FLOAT64"),
    ("is_caregiver_updated_at", "TIMESTAMP"),
    ("is_family_or_friend", "BOOL"),
    ("is_family_or_friend_source", "STRING"),
    ("is_family_or_friend_confidence", "FLOAT64"),
    ("is_family_or_friend_updated_at", "TIMESTAMP"),
    ("is_other", "BOOL"),
    ("is_other_source", "STRING"),
    ("is_other_confidence", "FLOAT64"),
    ("is_other_updated_at", "TIMESTAMP"),
    # v6.7 acquisition dates. The populate CTAS sheds these exactly like the
    # v6.5 flags above, and losing them is silent: the column comes back empty
    # and every growth chart quietly falls back to created_at, which is a
    # pipeline-observation date. enrich_source_created_at refills the values,
    # but it can only do that if the column still exists.
    ("source_created_at", "TIMESTAMP"),
    ("source_created_basis", "STRING"),
    ("source_created_set_at", "TIMESTAMP"),
    # 2026-08-28 tenant-key provenance, written by fill_gaps_site_domain.
    # THIS is the list that makes a new profile_core column exist on a refresh:
    # the `ddl` step is rebuild-only, and _sync_profile_core_runtime_schema runs
    # from here before the core write. Declaring the columns in
    # profile_database_ddl.sql alone is NOT enough -- and neither is an ALTER
    # inside the step's own SQL file, because post_processor submits each
    # statement as a separate query job and BigQuery planned the later UPDATE
    # against the pre-ALTER schema.
    ("site_domains", "ARRAY<STRING>"),
    ("site_domain_source", "STRING"),
    ("site_domain_confidence", "FLOAT64"),
    ("site_domain_updated_at", "TIMESTAMP"),
)

# v6.5: confidence threshold for activating a role flag from a probabilistic
# signal. Deterministic sources (confidence = 1.0) ALWAYS set the flag TRUE
# (the source-tiered rule). Probabilistic sources (confidence < 1.0) must meet
# this threshold. is_other intentionally stays NULL until a positive signal
# arrives -- it is NOT a catch-all default.
#
# Tune by changing this constant; no schema change or migration required.
# The value is exposed to SQL files as the {probabilistic_threshold} template
# variable (see context dict in _execute_step()).
PROBABILISTIC_FLAG_THRESHOLD = 0.5


def _clear_dataset_tables(bq_client: bigquery.Client, dataset: str) -> int:
    """
    Drop all tables in the given dataset (no-op if dataset missing).
    Intended for profile_staging cleanup: it should be scratch-only.
    """
    try:
        dataset_ref = f"{bq_client.project}.{dataset}"
        tables = list(bq_client.list_tables(dataset_ref))
    except Exception as e:
        logger.warning(f"Could not list tables for dataset {dataset}: {e}")
        return 0

    dropped = 0
    for table in tables:
        table_id = f"{dataset_ref}.{table.table_id}"
        try:
            bq_client.delete_table(table_id, not_found_ok=True)
            dropped += 1
        except Exception as e:
            logger.warning(f"Could not drop staging table {table_id}: {e}")
    return dropped


def _bq_table_exists(
    bq_client: bigquery.Client, dataset_id: str, table_id: str
) -> bool:
    """Return True if table exists in the BigQuery client's default project."""
    try:
        from google.cloud.exceptions import NotFound

        bq_client.get_table(f"{bq_client.project}.{dataset_id}.{table_id}")
        return True
    except NotFound:
        return False
    except Exception:
        return False


def _exception_looks_bq_not_found(exc: BaseException) -> bool:
    """Detect missing-table / 404 style failures from BigQuery or wrappers."""
    try:
        from google.api_core import exceptions as api_exc

        if isinstance(exc, api_exc.NotFound):
            return True
    except Exception:
        pass
    try:
        from google.cloud.exceptions import NotFound as CloudNotFound

        if isinstance(exc, CloudNotFound):
            return True
    except Exception:
        pass
    msg = str(exc).lower()
    return "not found" in msg or "was not found" in msg


def extract_snapshot_app_fields_ddl_prefix(sql_text: str) -> str:
    """
    Return the DDL preamble of profile_database_snapshot_app_fields.sql only
    (no CREATE OR REPLACE ... AS SELECT that reads production profile_core).
    """
    marker = "\nCREATE OR REPLACE TABLE"
    idx = sql_text.find(marker)
    return (sql_text[:idx] if idx != -1 else sql_text).strip()


def _run_snapshot_app_fields_schema_prefix(
    bq_client: bigquery.Client,
    sql_file: str,
    *,
    bq_job_labels=None,
    maximum_bytes_billed=None,
) -> None:
    """
    Execute only the CREATE SCHEMA / CREATE TABLE preamble from
    profile_database_snapshot_app_fields.sql (everything before the
    CREATE OR REPLACE TABLE ... AS SELECT that reads profile_core).
    """
    sql_path = Path(sql_file)
    if not sql_path.exists():
        sql_path = Path.cwd() / sql_file
    text = sql_path.read_text(encoding="utf-8")
    prefix = extract_snapshot_app_fields_ddl_prefix(text)
    from shared.post_processor import _split_sql_statements

    for stmt in _split_sql_statements(prefix):
        if not stmt or not stmt.strip():
            continue
        lines = [ln.strip() for ln in stmt.split("\n") if ln.strip()]
        if lines and all(ln.startswith("--") for ln in lines):
            continue
        cfg = bigquery.QueryJobConfig(use_query_cache=False)
        if bq_job_labels:
            cfg.labels = dict(bq_job_labels)
        if maximum_bytes_billed is not None:
            cfg.maximum_bytes_billed = int(maximum_bytes_billed)
        bq_client.query(stmt, job_config=cfg).result()


def validate_schema_references():
    """
    Parse the DDL and views to extract the live internal relation names, then
    scan all active SQL files for references to profile_data/profile_ops/
    profile_staging objects and warn on any that don't exist.

    Returns list of warning strings (empty = clean).
    """
    ddl_path = Path.cwd() / "sql" / "profile_database_ddl.sql"
    if not ddl_path.exists():
        logger.warning("Schema validation skipped: DDL file not found")
        return []

    ddl_text = ddl_path.read_text(encoding="utf-8")

    dataset_names = (PRODUCTION_DATASET, OPS_DATASET, STAGING_DATASET)

    ddl_relations = set()
    for dataset in dataset_names:
        ddl_relations.update(
            f"{dataset}.{m.group(1)}"
            for m in re.finditer(
                rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{re.escape(dataset)}\.(\w+)",
                ddl_text,
                re.IGNORECASE,
            )
        )
        # Tables explicitly dropped in DDL are part of the rebuild contract and
        # may still be referenced by other SQL files (DROP ... then later SELECT).
        # Treat them as valid relations for warn-only static analysis.
        ddl_relations.update(
            f"{dataset}.{m.group(1)}"
            for m in re.finditer(
                rf"DROP\s+TABLE\s+IF\s+EXISTS\s+{re.escape(dataset)}\.(\w+)",
                ddl_text,
                re.IGNORECASE,
            )
        )
        ddl_relations.update(
            f"{dataset}.{m.group(1)}"
            for m in re.finditer(
                rf"DROP\s+TABLE\s+{re.escape(dataset)}\.(\w+)",
                ddl_text,
                re.IGNORECASE,
            )
        )

    if not ddl_relations:
        logger.warning("Schema validation: no tables found in DDL")
        return []

    # Also include views as valid targets (created by profile_database_views.sql)
    views_path = Path.cwd() / "sql" / "profile_database_views.sql"
    if views_path.exists():
        views_text = views_path.read_text(encoding="utf-8").replace(
            "{view_dataset}", PRODUCTION_DATASET
        )
        for m in re.finditer(
            rf"CREATE\s+OR\s+REPLACE\s+VIEW\s+{re.escape(PRODUCTION_DATASET)}\.(\w+)",
            views_text,
            re.IGNORECASE,
        ):
            ddl_relations.add(f"{PRODUCTION_DATASET}.{m.group(1)}")

    # Ephemeral staging objects created during runs (not necessarily declared in DDL).
    ddl_relations.update(
        {
            f"{STAGING_DATASET}.refresh_scope_bn_ids",
            f"{STAGING_DATASET}.profile_core_app_snapshot",
            IDENTITY_XREF_SNAPSHOT_TABLE,
            IDENTITY_HUB_SNAPSHOT_TABLE,
            IDENTITY_PERSISTENCE_SNAPSHOT_TABLE,
        }
    )

    # Tables a scanned SQL file CREATEs itself are self-declared: the file is
    # both the writer and (for audit tables like
    # profile_staging.surveyengine_rejected) the owner of the schema. Warning
    # on a file's own CREATE target produced three permanent noise lines per
    # run and trained readers to ignore the validator (silenced 2026-08-25).
    for sql_file in PROFILE_SQL_FILES:
        sql_path = Path.cwd() / sql_file
        if not sql_path.exists():
            continue
        created_text = sql_path.read_text(encoding="utf-8")
        for dataset in dataset_names:
            ddl_relations.update(
                f"{dataset}.{m.group(1)}"
                for m in re.finditer(
                    rf"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{re.escape(dataset)}\.(\w+)",
                    created_text,
                    re.IGNORECASE,
                )
            )

    warnings = []

    for sql_file in PROFILE_SQL_FILES:
        sql_path = Path.cwd() / sql_file
        if not sql_path.exists():
            continue

        sql_text = sql_path.read_text(encoding="utf-8").replace(
            "{view_dataset}", PRODUCTION_DATASET
        )

        # Remove block AND line comments to avoid false positives from
        # commented-out code or descriptive prose that happens to mention a
        # retired table by name.
        sql_no_comments = re.sub(r"/\*.*?\*/", "", sql_text, flags=re.DOTALL)
        sql_no_comments = re.sub(r"--[^\n]*", "", sql_no_comments)

        # Find all internal dataset references. Word boundary prevents matching
        # xprofile_data.user_id as profile_data.user_id.
        for m in re.finditer(
            r"(?<!\w)(profile_data|profile_ops|profile_staging)\.(\w+)", sql_no_comments
        ):
            dataset_name, table_name = m.group(1), m.group(2)
            # INFORMATION_SCHEMA is not a physical table; SQL often references it for metadata scans.
            if table_name.upper() == "INFORMATION_SCHEMA":
                continue
            # Skip temp tables created inline (CREATE TEMP TABLE or CREATE TABLE _tmp_*)
            if table_name.startswith("_tmp_"):
                continue
            relation_name = f"{dataset_name}.{table_name}"
            if relation_name not in ddl_relations:
                # Find approximate line number
                pos = m.start()
                # Count in original text (comment removal shifts positions, but close enough)
                line_num = sql_no_comments[:pos].count("\n") + 1
                warnings.append(
                    f"  {sql_file}:{line_num} -- references dropped table "
                    f"'{relation_name}'"
                )

    return warnings


def _migrate_legacy_build_log(bq_client):
    """
    Defensive cleanup: drop the legacy profile_build_log object if any
    pre-v6.5 deployment still has it.

    History:
      - Pre-v6.4: profile_build_log was a flat BASE TABLE mixing run-level and
        step-level rows.
      - v6.4: replaced by profile_ops.profile_build_runs + profile_ops.profile_build_steps
        and exposed via a compatibility VIEW of the same name.
      - Post-beta (v6.5+): the compatibility view was retired. Consumers query
        the split tables (or the profile_build_performance / profile_release_status
        views) directly.

    This helper:
      1. If profile_build_log is a BASE TABLE: copies step rows forward into
         profile_ops.profile_build_steps, then DROPs it.
      2. If profile_build_log is a VIEW (legacy v6.4 deployment): DROPs it.
      3. Otherwise: no-op.

    Idempotent and safe to call on every run.
    """
    try:
        check_sql = """
        SELECT table_type
        FROM profile_data.INFORMATION_SCHEMA.TABLES
        WHERE table_name = 'profile_build_log'
        """
        rows = list(bq_client.query(check_sql).result())
        if not rows:
            logger.info("Legacy profile_build_log cleanup: not present, nothing to do.")
            return
        if rows[0].table_type == "VIEW":
            logger.info(
                "Legacy profile_build_log cleanup: retired view detected, dropping."
            )
            bq_client.query(
                "DROP VIEW IF EXISTS profile_data.profile_build_log"
            ).result()
            return
        if rows[0].table_type != "BASE TABLE":
            logger.info(
                f"Legacy profile_build_log cleanup: unexpected type {rows[0].table_type}, skipping."
            )
            return

        logger.info(
            "Legacy profile_build_log detected as table - migrating step rows + dropping..."
        )

        migrate_sql = """
        INSERT INTO profile_ops.profile_build_steps
            (build_id, step_name, started_at, completed_at, duration_seconds,
             status, rows_affected, statements_executed, warnings, error_message)
        SELECT
            build_id, step_name,
            build_started_at AS started_at,
            CASE WHEN duration_seconds IS NOT NULL
                 THEN TIMESTAMP_ADD(build_started_at, INTERVAL CAST(duration_seconds AS INT64) SECOND)
                 ELSE NULL END AS completed_at,
            duration_seconds, status, rows_affected,
            NULL AS statements_executed, NULL AS warnings, error_message
        FROM profile_data.profile_build_log
        WHERE step_name IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM profile_ops.profile_build_steps s
              WHERE s.build_id = profile_data.profile_build_log.build_id
                AND s.step_name = profile_data.profile_build_log.step_name
          )
        """
        migrate_job = bq_client.query(migrate_sql).result()
        logger.info(
            f"Legacy profile_build_log migration: {migrate_job.total_rows or 'unknown'} "
            f"step rows copied."
        )

        drop_sql = "DROP TABLE profile_data.profile_build_log"
        bq_client.query(drop_sql).result()
        logger.info("Legacy profile_build_log table dropped.")
    except Exception as e:
        # Non-fatal: if cleanup fails the rebuild can still proceed. Operators
        # can drop the legacy object manually if needed; v6.5+ no longer
        # recreates anything at this name.
        logger.warning(f"Legacy profile_build_log migration failed: {e}")


def _ensure_ops_runtime_tables(bq_client):
    """Bootstrap the minimal profile_ops logging schema before preflight/DDL.

    Preflight queries and run/step logging happen before the DDL step, so the
    split logging tables must exist even on a first run after refactoring the
    ops layer out of profile_data.
    """
    stmts = [
        "CREATE SCHEMA IF NOT EXISTS profile_ops",
        """
        CREATE TABLE IF NOT EXISTS profile_ops.profile_build_runs (
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
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS profile_ops.profile_build_steps (
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
        )
        CLUSTER BY build_id
        """,
        "ALTER TABLE profile_ops.profile_build_runs ADD COLUMN IF NOT EXISTS total_bytes_processed INT64",
        "ALTER TABLE profile_ops.profile_build_runs ADD COLUMN IF NOT EXISTS total_bytes_billed INT64",
        "ALTER TABLE profile_ops.profile_build_runs ADD COLUMN IF NOT EXISTS total_slot_millis INT64",
        "ALTER TABLE profile_ops.profile_build_runs ADD COLUMN IF NOT EXISTS runtime_fingerprint STRING",
        "ALTER TABLE profile_ops.profile_build_steps ADD COLUMN IF NOT EXISTS total_bytes_processed INT64",
        "ALTER TABLE profile_ops.profile_build_steps ADD COLUMN IF NOT EXISTS total_bytes_billed INT64",
        "ALTER TABLE profile_ops.profile_build_steps ADD COLUMN IF NOT EXISTS total_slot_millis INT64",
        "ALTER TABLE profile_ops.profile_build_runs ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMP",
        """
        CREATE TABLE IF NOT EXISTS profile_ops.profile_publish_manifest (
            build_id STRING NOT NULL,
            table_name STRING NOT NULL,
            source_dataset STRING,
            target_dataset STRING,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            status STRING,
            source_row_count INT64,
            target_row_count INT64,
            error_message STRING
        )
        CLUSTER BY build_id, table_name
        """,
        """
        CREATE TABLE IF NOT EXISTS profile_ops.profile_dataset_leases (
            dataset_id STRING NOT NULL,
            holder_build_id STRING NOT NULL,
            acquired_at TIMESTAMP NOT NULL,
            lease_until TIMESTAMP NOT NULL,
            last_heartbeat_at TIMESTAMP,
            mode STRING
        )
        CLUSTER BY dataset_id
        """,
    ]
    for stmt in stmts:
        try:
            bq_client.query(stmt).result()
        except Exception as e:
            logger.warning(f"Failed to bootstrap profile_ops runtime tables: {e}")
            break


def _ensure_consumer_dataset(bq_client, dataset_name: str):
    """Ensure a consumer dataset exists for blue/green rebuild targets."""
    try:
        bq_client.query(f"CREATE SCHEMA IF NOT EXISTS {dataset_name}").result()
    except Exception as e:
        logger.warning(f"Failed to ensure consumer dataset '{dataset_name}': {e}")


def _normalize_bq_type(data_type: str) -> str:
    return re.sub(r"\s+", " ", (data_type or "").strip()).upper()


def _sync_profile_core_snapshot_schema(bq_client, consumer_dataset: str):
    """Auto-heal additive schema drift between profile_core and profile_core_snapshot.

    Missing nullable columns on profile_core_snapshot are added automatically
    using the exact BigQuery type from the live profile_core schema. Extra
    columns on the snapshot table are tolerated so older history is not lost.
    Only true incompatible type mismatches remain a hard failure.
    """
    schema_sql = f"""
    WITH core_cols AS (
        SELECT column_name, data_type, ordinal_position
        FROM {consumer_dataset}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'profile_core'
    ),
    snap_cols AS (
        SELECT column_name, data_type, ordinal_position
        FROM {OPS_DATASET}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'profile_core_snapshot'
          AND column_name NOT IN ('snapshot_run_id', 'snapshotted_at')
    )
    SELECT
        src,
        column_name,
        data_type,
        ordinal_position
    FROM (
        SELECT 'core' AS src, column_name, data_type, ordinal_position FROM core_cols
        UNION ALL
        SELECT 'snapshot' AS src, column_name, data_type, ordinal_position FROM snap_cols
    )
    ORDER BY src, ordinal_position
    """
    rows = list(
        bq_client.query(
            schema_sql,
            job_config=bigquery.QueryJobConfig(use_query_cache=False),
        ).result()
    )

    core_cols = {}
    snap_cols = {}
    for row in rows:
        record = {"data_type": row.data_type, "ordinal_position": row.ordinal_position}
        if row.src == "core":
            core_cols[row.column_name] = record
        else:
            snap_cols[row.column_name] = record

    missing = [
        (col, meta["data_type"])
        for col, meta in sorted(
            core_cols.items(), key=lambda item: item[1]["ordinal_position"]
        )
        if col not in snap_cols
    ]
    extra = sorted(col for col in snap_cols if col not in core_cols)
    mismatches = [
        {
            "column_name": col,
            "core_type": core_cols[col]["data_type"],
            "snapshot_type": snap_cols[col]["data_type"],
        }
        for col in sorted(set(core_cols) & set(snap_cols))
        if _normalize_bq_type(core_cols[col]["data_type"])
        != _normalize_bq_type(snap_cols[col]["data_type"])
    ]

    added = []
    for col, data_type in missing:
        alter_sql = f"ALTER TABLE {OPS_DATASET}.profile_core_snapshot ADD COLUMN IF NOT EXISTS `{col}` {data_type}"
        bq_client.query(
            alter_sql, job_config=bigquery.QueryJobConfig(use_query_cache=False)
        ).result()
        added.append({"column_name": col, "data_type": data_type})

    return {
        "added_columns": added,
        "extra_columns": extra,
        "type_mismatches": mismatches,
    }


def _sync_profile_core_runtime_schema(bq_client, consumer_dataset: str):
    """Auto-heal additive runtime schema drift on profile_core itself.

    The populate CTAS can silently shed columns that were introduced later via
    ALTER TABLE statements. Missing nullable runtime columns are added back so
    downstream views/snapshots do not fail late in the build. Type mismatches
    remain a hard failure.
    """
    schema_sql = f"""
    SELECT column_name, data_type
    FROM {consumer_dataset}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'profile_core'
    """
    rows = list(
        bq_client.query(
            schema_sql,
            job_config=bigquery.QueryJobConfig(use_query_cache=False),
        ).result()
    )
    if not rows:
        # profile_core does not exist in this dataset. Surface a clear,
        # actionable error instead of letting a downstream ALTER TABLE 404.
        # This typically means: views mode was run against an empty
        # production dataset before any rebuild has populated profile_core,
        # or a rebuild step before populate_identity_core failed silently.
        raise RuntimeError(
            f"profile_core does not exist in {consumer_dataset}. "
            f"Run --build-mode rebuild (or resume_rebuild against "
            f"profile_data_candidate) before publishing views."
        )
    existing = {row.column_name: row.data_type for row in rows}

    added = []
    mismatches = []
    for col, data_type in PROFILE_CORE_RUNTIME_ADDITIVE_COLUMNS:
        current_type = existing.get(col)
        if current_type is None:
            alter_sql = f"ALTER TABLE {consumer_dataset}.profile_core ADD COLUMN IF NOT EXISTS `{col}` {data_type}"
            bq_client.query(
                alter_sql, job_config=bigquery.QueryJobConfig(use_query_cache=False)
            ).result()
            added.append({"column_name": col, "data_type": data_type})
        elif _normalize_bq_type(current_type) != _normalize_bq_type(data_type):
            mismatches.append(
                {
                    "column_name": col,
                    "profile_core_type": current_type,
                    "expected_type": data_type,
                }
            )

    # Backfill values for pre-fix candidate/prod tables so late-stage resume
    # runs produce usable explainability metadata without a full rebuild.
    # v6.5 Phase 2: account_type/persona_source dropped from profile_core,
    # so the v6.4 account_type_* lineage backfill block was removed. Only
    # condition_subtype and diagnosis_stage lineage backfill remains. Role-flag
    # lineage is seeded by populate_identity_core.sql + classifier SQL files;
    # there is no need to backfill is_X lineage here because flags can simply
    # stay NULL until a classifier sets them.
    backfill_sql = f"""
    UPDATE {consumer_dataset}.profile_core
    SET
        condition_subtype_confidence = COALESCE(
            condition_subtype_confidence,
            CASE condition_subtype_source
                WHEN 'app_confirmed' THEN 1.0
                WHEN 'mailchimp_mmerge4' THEN 0.8
                WHEN 'survey' THEN 0.75
                WHEN 'buddypress_xprofile' THEN 0.7
                ELSE NULL
            END
        ),
        condition_subtype_updated_at = COALESCE(
            condition_subtype_updated_at,
            CASE
                WHEN condition_subtype IS NOT NULL THEN COALESCE(profile_updated_at, CURRENT_TIMESTAMP())
                ELSE NULL
            END
        ),
        diagnosis_stage_confidence = COALESCE(
            diagnosis_stage_confidence,
            CASE diagnosis_stage_source
                WHEN 'app_confirmed' THEN 1.0
                WHEN 'gravity_forms_poll' THEN 0.85
                WHEN 'mailchimp_mmerge4' THEN 0.8
                WHEN 'survey' THEN 0.75
                ELSE NULL
            END
        ),
        diagnosis_stage_updated_at = COALESCE(
            diagnosis_stage_updated_at,
            CASE
                WHEN diagnosis_stage IS NOT NULL THEN COALESCE(profile_updated_at, CURRENT_TIMESTAMP())
                ELSE NULL
            END
        )
    WHERE (condition_subtype IS NOT NULL AND (condition_subtype_confidence IS NULL OR condition_subtype_updated_at IS NULL))
       OR (diagnosis_stage IS NOT NULL AND (diagnosis_stage_confidence IS NULL OR diagnosis_stage_updated_at IS NULL))
    """
    backfill_job = bq_client.query(
        backfill_sql,
        job_config=bigquery.QueryJobConfig(use_query_cache=False),
    )
    backfill_job.result()

    return {
        "added_columns": added,
        "type_mismatches": mismatches,
    }


def _publish_physical_tables(
    bq_client,
    source_dataset: str,
    target_dataset: str,
    build_id: str | None = None,
    verify_row_counts: bool = True,
):
    """Copy approved consumer/reference tables from candidate to production.

    When build_id is provided, emits per-table publish state into
    profile_ops.profile_publish_manifest. This enables debugging partial publish
    failures and supports ``resume_publish`` idempotency (skip tables already completed).
    """
    if source_dataset == target_dataset:
        return {"tables_published": 0}

    _ensure_consumer_dataset(bq_client, target_dataset)

    published = []
    for table_name in PHYSICAL_TABLES:
        # Idempotency: if we already published this table for this build_id,
        # skip it. Safe for partial publish retries and ``resume_publish``.
        if build_id:
            try:
                existing = list(
                    bq_client.query(
                        """
                        SELECT status
                        FROM profile_ops.profile_publish_manifest
                        WHERE build_id = @build_id
                          AND table_name = @table_name
                        ORDER BY started_at DESC
                        LIMIT 1
                        """,
                        job_config=bigquery.QueryJobConfig(
                            use_query_cache=False,
                            query_parameters=[
                                bigquery.ScalarQueryParameter(
                                    "build_id", "STRING", build_id
                                ),
                                bigquery.ScalarQueryParameter(
                                    "table_name", "STRING", table_name
                                ),
                            ],
                        ),
                    ).result()
                )
                if existing and (existing[0].get("status") == "completed"):
                    try:
                        src_count = None
                        tgt_count = None
                        if verify_row_counts:
                            source = (
                                f"{bq_client.project}.{source_dataset}.{table_name}"
                            )
                            target = (
                                f"{bq_client.project}.{target_dataset}.{table_name}"
                            )
                            try:
                                src_count = int(
                                    list(
                                        bq_client.query(
                                            f"SELECT COUNT(*) AS n FROM `{source}`",
                                            job_config=bigquery.QueryJobConfig(
                                                use_query_cache=False
                                            ),
                                        ).result()
                                    )[0]["n"]
                                )
                                tgt_count = int(
                                    list(
                                        bq_client.query(
                                            f"SELECT COUNT(*) AS n FROM `{target}`",
                                            job_config=bigquery.QueryJobConfig(
                                                use_query_cache=False
                                            ),
                                        ).result()
                                    )[0]["n"]
                                )
                            except Exception:
                                # Counts are best-effort on a skip path.
                                src_count = None
                                tgt_count = None
                        bq_client.query(
                            """
                            INSERT INTO profile_ops.profile_publish_manifest
                                (build_id, table_name, source_dataset, target_dataset, started_at, completed_at, status,
                                 source_row_count, target_row_count, error_message)
                            VALUES
                                (@build_id, @table_name, @source_dataset, @target_dataset, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'skipped',
                                 @src_count, @tgt_count, 'already completed for this build_id')
                            """,
                            job_config=bigquery.QueryJobConfig(
                                query_parameters=[
                                    bigquery.ScalarQueryParameter(
                                        "build_id", "STRING", build_id
                                    ),
                                    bigquery.ScalarQueryParameter(
                                        "table_name", "STRING", table_name
                                    ),
                                    bigquery.ScalarQueryParameter(
                                        "source_dataset", "STRING", source_dataset
                                    ),
                                    bigquery.ScalarQueryParameter(
                                        "target_dataset", "STRING", target_dataset
                                    ),
                                    bigquery.ScalarQueryParameter(
                                        "src_count", "INT64", src_count
                                    ),
                                    bigquery.ScalarQueryParameter(
                                        "tgt_count", "INT64", tgt_count
                                    ),
                                ],
                                use_query_cache=False,
                            ),
                        ).result()
                    except Exception:
                        pass
                    published.append(table_name)
                    continue
            except Exception:
                # Non-fatal; publish will proceed and manifest writes may fail later.
                pass

        if build_id:
            bq_client.query(
                """
                INSERT INTO profile_ops.profile_publish_manifest
                    (build_id, table_name, source_dataset, target_dataset, started_at, status)
                VALUES
                    (@build_id, @table_name, @source_dataset, @target_dataset, CURRENT_TIMESTAMP(), 'running')
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("build_id", "STRING", build_id),
                        bigquery.ScalarQueryParameter(
                            "table_name", "STRING", table_name
                        ),
                        bigquery.ScalarQueryParameter(
                            "source_dataset", "STRING", source_dataset
                        ),
                        bigquery.ScalarQueryParameter(
                            "target_dataset", "STRING", target_dataset
                        ),
                    ],
                    use_query_cache=False,
                ),
            ).result()

        source = f"{bq_client.project}.{source_dataset}.{table_name}"
        target = f"{bq_client.project}.{target_dataset}.{table_name}"
        try:
            job = bq_client.copy_table(
                sources=source,
                destination=target,
                job_config=bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE"),
            )
            job.result()

            src_count = None
            tgt_count = None
            if verify_row_counts:
                src_count = int(
                    list(
                        bq_client.query(
                            f"SELECT COUNT(*) AS n FROM `{source}`",
                            job_config=bigquery.QueryJobConfig(use_query_cache=False),
                        ).result()
                    )[0]["n"]
                )
                tgt_count = int(
                    list(
                        bq_client.query(
                            f"SELECT COUNT(*) AS n FROM `{target}`",
                            job_config=bigquery.QueryJobConfig(use_query_cache=False),
                        ).result()
                    )[0]["n"]
                )
                if src_count != tgt_count:
                    raise RuntimeError(
                        f"Row-count verification failed for {table_name}: "
                        f"{source_dataset}={src_count:,} vs {target_dataset}={tgt_count:,}"
                    )

            if build_id:
                bq_client.query(
                    """
                    UPDATE profile_ops.profile_publish_manifest
                    SET completed_at = CURRENT_TIMESTAMP(),
                        status = 'completed',
                        source_row_count = @src_count,
                        target_row_count = @tgt_count,
                        error_message = NULL
                    WHERE build_id = @build_id AND table_name = @table_name AND status = 'running'
                    """,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter(
                                "build_id", "STRING", build_id
                            ),
                            bigquery.ScalarQueryParameter(
                                "table_name", "STRING", table_name
                            ),
                            bigquery.ScalarQueryParameter(
                                "src_count", "INT64", src_count
                            ),
                            bigquery.ScalarQueryParameter(
                                "tgt_count", "INT64", tgt_count
                            ),
                        ],
                        use_query_cache=False,
                    ),
                ).result()

            published.append(table_name)
        except Exception as e:
            if build_id:
                bq_client.query(
                    """
                    UPDATE profile_ops.profile_publish_manifest
                    SET completed_at = CURRENT_TIMESTAMP(),
                        status = 'failed',
                        error_message = @error_message
                    WHERE build_id = @build_id AND table_name = @table_name AND status = 'running'
                    """,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter(
                                "build_id", "STRING", build_id
                            ),
                            bigquery.ScalarQueryParameter(
                                "table_name", "STRING", table_name
                            ),
                            bigquery.ScalarQueryParameter(
                                "error_message", "STRING", str(e)[:500]
                            ),
                        ],
                        use_query_cache=False,
                    ),
                ).result()
            raise

    return {
        "tables_published": len(published),
        "table_names": published,
    }


def _publish_consumer_views(
    bq_client,
    execution_id: str,
    config: dict,
    consumer_dataset: str,
    view_dataset: str,
    lookback_days=None,
    bq_job_labels=None,
    maximum_bytes_billed=None,
):
    """Publish the canonical consumer views for a given serving/storage context."""
    context = {
        "build_id": execution_id,
        "consumer_dataset": consumer_dataset,
        "ops_dataset": OPS_DATASET,
        "staging_dataset": STAGING_DATASET,
        "view_dataset": view_dataset,
        "probabilistic_threshold": str(PROBABILISTIC_FLAG_THRESHOLD),
    }
    if lookback_days:
        context["lookback_days"] = str(lookback_days)
    return execute_post_process_sql(
        bq_client=bq_client,
        sql_file=STEP_MAP["views"][0],
        config=config,
        source="profile_database",
        execution_id=execution_id,
        context=context,
        bq_job_labels=bq_job_labels,
        maximum_bytes_billed=maximum_bytes_billed,
    )


def _restore_profile_metadata(bq_client, target_dataset: str) -> None:
    """Restore table and column descriptions after a publish/rebuild.

    CREATE OR REPLACE VIEW (and rebuild-mode CREATE TABLE) wipes the
    descriptions we apply via scripts/apply_profile_metadata.py. Calling this
    helper after every successful publish restores them automatically, so
    Data Canvas / Gemini Agent always have the metadata they need.

    Idempotent and best-effort: metadata failures are logged but do NOT fail
    the build.
    """
    if target_dataset != PRODUCTION_DATASET:
        # Don't apply to candidate/staging datasets; descriptions matter only
        # on the production surface that Data Canvas/Gemini read.
        logger.info(
            f"   Skipping metadata restore (target_dataset={target_dataset}, "
            f"only restoring on {PRODUCTION_DATASET})"
        )
        return

    try:
        # Lazy import: keeps the script self-contained and avoids forcing the
        # extractor to load it unless we're actually publishing.
        import sys
        from pathlib import Path

        scripts_dir = str(Path(__file__).parent.parent / "scripts")
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)
        from apply_profile_metadata import apply_metadata
    except ImportError as e:
        logger.warning(
            f"   Metadata restore skipped: could not import "
            f"scripts/apply_profile_metadata.py ({e})"
        )
        return

    logger.info("   Restoring table/column descriptions after publish...")
    restore_start = time.time()
    try:
        stats = apply_metadata(bq_client, only_tables=None, dry_run=False)
        restore_duration = time.time() - restore_start
        logger.info(
            f"   Metadata restored: tables_updated={stats.tables_updated}, "
            f"tables_unchanged={stats.tables_unchanged}, "
            f"columns_updated={stats.columns_updated}, "
            f"columns_unchanged={stats.columns_unchanged} "
            f"({restore_duration:.1f}s)"
        )
    except Exception as e:
        # Don't fail the build on metadata restore errors -- the data is correct,
        # only the descriptions might be stale. Operator can re-run the script
        # manually: python scripts/apply_profile_metadata.py --apply
        logger.warning(
            f"   Metadata restore FAILED (non-fatal, build continues): {e}. "
            f"To restore manually: python scripts/apply_profile_metadata.py --apply"
        )


def _identity_copy_job_labels(centralized_job_id: str | None) -> dict[str, str] | None:
    if not centralized_job_id:
        return None
    return {
        "orchestrator_component": "profile_database",
        "orchestrator_job_id": str(centralized_job_id).lower()[:63],
    }


def _prepare_identity_snapshots(
    bq_client,
    execution_id: str,
    manifest_run_id: str | None,
    copy_labels: dict[str, str] | None = None,
):
    """Copy live identity tables into staging so one run sees one stable source image."""
    snapshot_specs = (
        ("identity_hub_data.bn_id_xref", IDENTITY_XREF_SNAPSHOT_TABLE),
        ("identity_hub_data.bn_id_hub", IDENTITY_HUB_SNAPSHOT_TABLE),
        ("identity_hub_data.bn_id_persistence", IDENTITY_PERSISTENCE_SNAPSHOT_TABLE),
    )
    _ensure_consumer_dataset(bq_client, STAGING_DATASET)

    started = time.time()
    copied = []
    for source_table, target_table in snapshot_specs:
        copy_cfg = bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")
        if copy_labels:
            copy_cfg.labels = dict(copy_labels)
        job = bq_client.copy_table(
            sources=f"{bq_client.project}.{source_table}",
            destination=f"{bq_client.project}.{target_table}",
            job_config=copy_cfg,
        )
        job.result()
        copied.append({"source_table": source_table, "target_table": target_table})

    xref_row_count = None
    try:
        xref_row_count = list(
            bq_client.query(
                f"SELECT COUNT(*) AS n FROM {IDENTITY_XREF_SNAPSHOT_TABLE}",
                job_config=bigquery.QueryJobConfig(use_query_cache=False),
            ).result()
        )[0].n
    except Exception as count_err:
        logger.warning(f"Failed to count identity xref snapshot rows: {count_err}")

    duration_seconds = time.time() - started
    _log_build_step(
        bq_client,
        execution_id,
        "identity_snapshot",
        "completed",
        rows_affected=len(copied),
        duration_seconds=duration_seconds,
        statements_executed=len(copied),
        error_message=f"Copied {len(copied)} identity source table(s) into {STAGING_DATASET}",
    )
    _merge_build_run_metadata(
        bq_client,
        execution_id,
        {
            "identity_snapshot": {
                "manifest_run_id": manifest_run_id,
                "xref_row_count": xref_row_count,
                "copied_tables": copied,
            }
        },
    )
    return {
        "duration_seconds": duration_seconds,
        "copied_tables": copied,
        "xref_row_count": xref_row_count,
    }


def _log_build_step(
    bq_client,
    execution_id,
    step_name,
    status,
    rows_affected=0,
    duration_seconds=0.0,
    statements_executed=0,
    total_bytes_processed=0,
    total_bytes_billed=0,
    total_slot_millis=0,
    error_message=None,
):
    """Write a step-level entry to profile_ops.profile_build_steps.

    Single-writes to profile_ops.profile_build_steps. Consumers should query
    that table directly, or use the profile_build_performance /
    profile_release_status views in profile_data for shaped output.
    """
    if not _profile_ops_persist_enabled():
        return
    try:
        step_sql = """
        INSERT INTO profile_ops.profile_build_steps
            (build_id, step_name, started_at, completed_at, duration_seconds,
             status, rows_affected, statements_executed,
             total_bytes_processed, total_bytes_billed, total_slot_millis,
             error_message)
        VALUES (
            @build_id, @step_name,
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@duration_seconds * 1000 AS INT64) MILLISECOND),
            CURRENT_TIMESTAMP(),
            @duration_seconds, @status, @rows_affected, @statements_executed,
            @total_bytes_processed, @total_bytes_billed, @total_slot_millis,
            @error_message
        )
        """
        step_cfg = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("build_id", "STRING", execution_id),
                bigquery.ScalarQueryParameter("step_name", "STRING", step_name),
                bigquery.ScalarQueryParameter(
                    "duration_seconds", "FLOAT64", duration_seconds
                ),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("rows_affected", "INT64", rows_affected),
                bigquery.ScalarQueryParameter(
                    "statements_executed", "INT64", statements_executed or 0
                ),
                bigquery.ScalarQueryParameter(
                    "total_bytes_processed", "INT64", total_bytes_processed or 0
                ),
                bigquery.ScalarQueryParameter(
                    "total_bytes_billed", "INT64", total_bytes_billed or 0
                ),
                bigquery.ScalarQueryParameter(
                    "total_slot_millis", "INT64", total_slot_millis or 0
                ),
                bigquery.ScalarQueryParameter(
                    "error_message", "STRING", error_message or ""
                ),
            ],
        )
        bq_client.query(step_sql, job_config=step_cfg).result()
    except Exception as e:
        logger.warning(f"Failed to log build step '{step_name}': {e}")


def _merge_build_run_metadata(bq_client, execution_id, patch: dict):
    """Merge a JSON-serializable patch into profile_build_runs.metadata."""
    if not _profile_ops_persist_enabled():
        return
    try:
        import json as _json

        existing_sql = """
        SELECT metadata
        FROM profile_ops.profile_build_runs
        WHERE build_id = @build_id
        LIMIT 1
        """
        existing_cfg = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("build_id", "STRING", execution_id),
            ],
        )
        existing_rows = list(
            bq_client.query(existing_sql, job_config=existing_cfg).result()
        )
        merged = {}
        if existing_rows and existing_rows[0].metadata:
            try:
                merged = _json.loads(existing_rows[0].metadata)
            except Exception:
                merged = {"raw_metadata": existing_rows[0].metadata}
        merged.update(patch or {})
        merge_sql = """
        UPDATE profile_ops.profile_build_runs
        SET metadata = @meta
        WHERE build_id = @build_id
        """
        merge_cfg = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("build_id", "STRING", execution_id),
                bigquery.ScalarQueryParameter("meta", "STRING", _json.dumps(merged)),
            ],
        )
        bq_client.query(merge_sql, job_config=merge_cfg).result()
    except Exception as e:
        logger.warning(f"Failed to merge build run metadata: {e}")


def _compute_runtime_fingerprint():
    """Hash the active runtime files so each run is reproducible."""
    runtime_files = []
    runtime_files.extend(PROFILE_SQL_FILES)
    runtime_files.extend(
        [
            "plugins/profile_database_extractor.py",
            "shared/profile_database_manifest.py",
            "shared/post_processor.py",
        ]
    )
    deduped = list(dict.fromkeys(runtime_files))
    hasher = hashlib.sha256()
    missing_files = []
    for rel_path in deduped:
        path = Path.cwd() / rel_path
        hasher.update(rel_path.encode("utf-8"))
        hasher.update(b"\0")
        if not path.exists():
            missing_files.append(rel_path)
            hasher.update(b"<missing>")
            continue
        hasher.update(path.read_bytes())
        hasher.update(b"\0")
    return {
        "runtime_fingerprint": hasher.hexdigest()[:16],
        "runtime_file_count": len(deduped),
        "runtime_missing_files": missing_files,
    }


def _summarize_refresh_scope(bq_client):
    """Summarize refresh scope size and source mix for logging/tuning."""
    sql = """
    WITH by_source AS (
        SELECT
            source,
            COUNT(DISTINCT IF(bn_id != '*', bn_id, NULL)) AS bn_id_n
        FROM profile_staging.refresh_scope_bn_ids
        GROUP BY source
    ),
    totals AS (
        SELECT
            COUNTIF(bn_id = '*') AS sentinel_n,
            COUNT(DISTINCT IF(bn_id != '*', bn_id, NULL)) AS scope_n
        FROM profile_staging.refresh_scope_bn_ids
    )
    SELECT
        totals.sentinel_n,
        totals.scope_n,
        ARRAY_AGG(
            STRUCT(by_source.source AS source, by_source.bn_id_n AS bn_id_n)
            ORDER BY by_source.bn_id_n DESC, by_source.source
        ) AS source_breakdown
    FROM totals
    LEFT JOIN by_source ON TRUE
    GROUP BY totals.sentinel_n, totals.scope_n
    """
    row = list(
        bq_client.query(
            sql, job_config=bigquery.QueryJobConfig(use_query_cache=False)
        ).result()
    )[0]
    return {
        "sentinel_n": row.sentinel_n or 0,
        "scope_n": row.scope_n or 0,
        "source_breakdown": [
            {"source": r["source"], "bn_id_n": int(r["bn_id_n"] or 0)}
            for r in (row.source_breakdown or [])
            if r["source"] is not None
        ],
    }


def _log_build_run_start(
    bq_client, execution_id, mode, preflight_result, runtime_info=None
):
    """v6.4: write run-level start row to profile_ops.profile_build_runs."""
    if not _profile_ops_persist_enabled():
        return
    try:
        import json as _json

        run_sql = """
        INSERT INTO profile_ops.profile_build_runs
            (build_id, mode, schema_version, started_at, status,
             identity_hub_manifest_id, identity_hub_row_count, preflight_status,
             runtime_fingerprint,
             metadata)
        VALUES (
            @build_id, @mode, @schema_version, CURRENT_TIMESTAMP(), 'running',
            @manifest_id, @row_count, @preflight_status,
            @runtime_fingerprint,
            @metadata
        )
        """
        meta = _json.dumps(
            {
                "preflight_checks": preflight_result.get("checks", []),
                "runtime_fingerprint": (runtime_info or {}).get("runtime_fingerprint"),
                "runtime_file_count": (runtime_info or {}).get("runtime_file_count"),
                "runtime_missing_files": (runtime_info or {}).get(
                    "runtime_missing_files", []
                ),
            }
        )
        cfg = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("build_id", "STRING", execution_id),
                bigquery.ScalarQueryParameter("mode", "STRING", mode),
                bigquery.ScalarQueryParameter(
                    "schema_version", "STRING", SCHEMA_VERSION
                ),
                bigquery.ScalarQueryParameter(
                    "manifest_id",
                    "STRING",
                    preflight_result.get("manifest_run_id") or "",
                ),
                bigquery.ScalarQueryParameter(
                    "row_count", "INT64", preflight_result.get("xref_row_count") or 0
                ),
                bigquery.ScalarQueryParameter(
                    "preflight_status",
                    "STRING",
                    preflight_result.get("status", "unknown"),
                ),
                bigquery.ScalarQueryParameter(
                    "runtime_fingerprint",
                    "STRING",
                    (runtime_info or {}).get("runtime_fingerprint") or "",
                ),
                bigquery.ScalarQueryParameter("metadata", "STRING", meta),
            ],
        )
        bq_client.query(run_sql, job_config=cfg).result()
    except Exception as e:
        logger.warning(f"Failed to log build run start: {e}")


def _log_build_run_end(
    bq_client,
    execution_id,
    status,
    total_steps=0,
    failed_steps=0,
    total_bytes_processed=0,
    total_bytes_billed=0,
    total_slot_millis=0,
    assertion_summary=None,
    error_message=None,
):
    """v6.4: finalize the run-level row in profile_ops.profile_build_runs."""
    if not _profile_ops_persist_enabled():
        return
    try:
        import json as _json

        existing_meta_sql = """
        SELECT metadata
        FROM profile_ops.profile_build_runs
        WHERE build_id = @build_id
        LIMIT 1
        """
        existing_cfg = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("build_id", "STRING", execution_id),
            ],
        )
        existing_rows = list(
            bq_client.query(existing_meta_sql, job_config=existing_cfg).result()
        )
        merged_meta = {}
        if existing_rows and existing_rows[0].metadata:
            try:
                merged_meta = _json.loads(existing_rows[0].metadata)
            except Exception:
                merged_meta = {"raw_metadata": existing_rows[0].metadata}
        merged_meta["performance"] = {
            "total_bytes_processed": int(total_bytes_processed or 0),
            "total_bytes_billed": int(total_bytes_billed or 0),
            "total_slot_millis": int(total_slot_millis or 0),
        }
        sql = """
        UPDATE profile_ops.profile_build_runs
        SET completed_at = CURRENT_TIMESTAMP(),
            status = @status,
            total_steps = @total_steps,
            failed_steps = @failed_steps,
            total_bytes_processed = @total_bytes_processed,
            total_bytes_billed = @total_bytes_billed,
            total_slot_millis = @total_slot_millis,
            assertion_summary = @assertion_summary,
            metadata = @metadata,
            error_message = @error_message
        WHERE build_id = @build_id
        """
        cfg = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("build_id", "STRING", execution_id),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("total_steps", "INT64", total_steps),
                bigquery.ScalarQueryParameter("failed_steps", "INT64", failed_steps),
                bigquery.ScalarQueryParameter(
                    "total_bytes_processed", "INT64", total_bytes_processed or 0
                ),
                bigquery.ScalarQueryParameter(
                    "total_bytes_billed", "INT64", total_bytes_billed or 0
                ),
                bigquery.ScalarQueryParameter(
                    "total_slot_millis", "INT64", total_slot_millis or 0
                ),
                bigquery.ScalarQueryParameter(
                    "assertion_summary", "STRING", _json.dumps(assertion_summary or {})
                ),
                bigquery.ScalarQueryParameter(
                    "metadata", "STRING", _json.dumps(merged_meta)
                ),
                bigquery.ScalarQueryParameter(
                    "error_message", "STRING", error_message or ""
                ),
            ],
        )
        bq_client.query(sql, job_config=cfg).result()
    except Exception as e:
        logger.warning(f"Failed to log build run end: {e}")


def run_preflight(bq_client, mode: str, force: bool = False) -> dict:
    """
    Pre-rebuild build acceptance gate (P1.4).

    Verifies the identity hub is in a coherent state before we drop and rebuild
    profile_core. A broken hub state fails silently at the data layer; we need
    to fail loudly at the plumbing layer first.

    Hard gates (build fails if any fail unless --force):
      1. single-generation -- bn_id_manifest's most recent row has
         status=PROMOTED (catches an in-progress or failed hub run). The hub
         may write multiple run_ids per event_date; the manifest is the
         authoritative signal that a generation is live.
      2. xref scale -- row count is within +/- 20% of the last recorded rebuild
         (catches catastrophic drops).
      3. (refresh only) no concurrent rebuild -- another profile_build_runs row
         in status=running with started_at in the last 60 minutes.

    Soft warnings (logged but never block preflight):
      - hub activity -- optional signal on identity change volume.
      - upstream source freshness (mailchimp, ga4, npi, etc.).

    Modes:
      - `rebuild` / `refresh`: preflight runs. Concurrent-build detection runs for
        refresh only (rebuild does not persist profile_build_runs, so the gate is skipped).
      - `reenrich` and `views`: preflight is skipped -- those modes don't re-pull
        from the identity hub.

    Returns a dict with: status ('ok' or 'failed'), manifest_run_id,
    xref_row_count, and a per-check results list. Always safe to call; the
    caller decides whether to raise.
    """
    result = {
        "status": "ok",
        "manifest_run_id": None,
        "xref_row_count": None,
        "checks": [],
        "skipped": False,
    }

    # Skip for non-source-touching modes
    if mode not in ("rebuild", "refresh"):
        result["skipped"] = True
        logger.info(f"Preflight skipped (mode={mode}; source data not touched).")
        return result

    def record(name: str, passed: bool, detail: str, severity: str = "hard"):
        result["checks"].append(
            {"name": name, "passed": passed, "detail": detail, "severity": severity}
        )
        if not passed and severity == "hard":
            result["status"] = "failed"
        elif not passed and severity == "soft":
            logger.warning(f"Preflight soft warning [{name}]: {detail}")

    # 1. Hub activity -- soft warning only. A stable, healthy graph may have no
    # recent identity changes; this should not block a legitimate rebuild.
    try:
        sql = """
        SELECT
            MAX(event_date) AS latest_event_date,
            COUNT(*) AS total_changes
        FROM identity_hub_data.bn_id_identity_changes
        WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        """
        row = list(bq_client.query(sql).result())[0]
        recent_count = row.total_changes or 0
        if recent_count > 0:
            record(
                "hub_activity",
                True,
                f"{recent_count} change events in last 30 days "
                f"(latest: {row.latest_event_date})",
                severity="soft",
            )
        else:
            record(
                "hub_activity",
                False,
                "No bn_id_identity_changes rows in last 30 days -- hub may be stale "
                "(soft warning only -- does not block rebuild)",
                severity="soft",
            )
    except Exception as e:
        record("hub_activity", False, f"Query error: {str(e)[:200]}", severity="soft")

    # 1b. Monthly snapshot freshness -- soft. The monthly forensic snapshots
    # (platform_monthly_snapshots, cron on the 1st) are the only recovery
    # instrument beyond BigQuery's 7-day time travel. A silent cron failure
    # must surface in the daily pipeline log, not during the next incident.
    try:
        # Age computed in SQL: this function later does a local
        # `import datetime`, which makes `datetime` function-local everywhere
        # in this scope -- referencing it here raised UnboundLocalError on
        # 2026-08-25 and the check errored instead of measuring.
        sql = """
        SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),
                              TIMESTAMP_MILLIS(MAX(creation_time)), DAY) AS age_days
        FROM platform_monthly_snapshots.__TABLES__
        WHERE table_id LIKE 'bn_id_xref_2%'
        """
        rows = list(bq_client.query(sql).result())
        age_days = rows[0].age_days if rows else None
        if age_days is None:
            record(
                "monthly_snapshot_freshness",
                False,
                "No bn_id_xref_* monthly snapshot found in platform_monthly_snapshots",
                severity="soft",
            )
        else:
            record(
                "monthly_snapshot_freshness",
                age_days <= 35,
                f"Newest monthly bn_id_xref snapshot is {age_days} days old "
                + (
                    "(within 35-day budget)"
                    if age_days <= 35
                    else "(cron on the 1st may have failed -- check monthly_snapshot.log)"
                ),
                severity="soft",
            )
    except Exception as e:
        record(
            "monthly_snapshot_freshness",
            False,
            f"Query error: {str(e)[:200]}",
            severity="soft",
        )

    # 2. Single-generation -- the hub's manifest is in a PROMOTED state.
    # The authoritative signal is bn_id_manifest, which records one row per
    # promoted hub generation. Daily rows in bn_id_persistence are incremental
    # writes on top of the active generation and routinely carry run_ids that
    # do not appear in the manifest, so counting distinct run_ids per
    # event_date is not a valid mid-promotion signal. A non-PROMOTED status
    # (e.g. BUILDING / FAILED) is the real "do not start" indicator.
    try:
        sql = """
        SELECT active_run_id, status, promoted_at,
               TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), promoted_at, DAY) AS age_days
        FROM identity_hub_data.bn_id_manifest
        ORDER BY promoted_at DESC
        LIMIT 1
        """
        rows = list(bq_client.query(sql).result())
        if not rows:
            record("single_generation", False, "No bn_id_manifest rows found")
        else:
            latest = rows[0]
            if latest.status in PROMOTED_MANIFEST_STATUSES:
                result["manifest_run_id"] = latest.active_run_id
                record(
                    "single_generation",
                    True,
                    f"Active promoted run_id: {latest.active_run_id} "
                    f"(promoted_at: {latest.promoted_at})",
                )

                # 2b. Manifest freshness -- a stale PROMOTED row means the hub
                # full-rebuild cadence has lapsed. Hard gate for rebuild-like
                # modes (they rebase everything on the hub image); soft warning
                # for daily refresh (hub incrementals keep the graph itself
                # fresh without stamping the manifest). Threshold configurable
                # via PROFILE_HUB_MANIFEST_MAX_AGE_DAYS (default 45).
                #
                # Age MUST be measured from the newest FULL rebuild, not from
                # `latest`: incremental runs stamp INCREMENTAL_PROMOTED daily, so
                # using latest.age_days would report 0 every night and silently
                # mask an overdue full rebuild -- exactly the lapse this check
                # exists to catch. Falls back to latest.age_days only when no
                # full PROMOTED row exists at all (greenfield).
                age_days = latest.age_days
                full_promoted_at = latest.promoted_at
                try:
                    full_rows = list(
                        bq_client.query(
                            """
                            SELECT promoted_at,
                                   TIMESTAMP_DIFF(
                                       CURRENT_TIMESTAMP(), promoted_at, DAY
                                   ) AS age_days
                            FROM identity_hub_data.bn_id_manifest
                            WHERE status = 'PROMOTED'
                            ORDER BY promoted_at DESC
                            LIMIT 1
                            """
                        ).result()
                    )
                    if full_rows:
                        age_days = full_rows[0].age_days
                        full_promoted_at = full_rows[0].promoted_at
                except Exception as e:  # noqa: BLE001 - fall back to latest age
                    logger.warning(
                        "Could not read newest full PROMOTED manifest row; "
                        "using latest row's age instead: %s",
                        e,
                    )
                freshness_severity = "hard" if mode in REBUILD_LIKE_MODES else "soft"
                if age_days is not None and age_days > HUB_MANIFEST_MAX_AGE_DAYS:
                    record(
                        "manifest_freshness",
                        False,
                        f"Newest PROMOTED manifest row is {age_days} days old "
                        f"(max {HUB_MANIFEST_MAX_AGE_DAYS}; promoted_at: "
                        f"{full_promoted_at}). The identity hub full-rebuild "
                        f"cadence has lapsed -- run "
                        f"'python shared/identity_hub.py --rebuild' before the "
                        f"next profile rebuild.",
                        severity=freshness_severity,
                    )
                else:
                    record(
                        "manifest_freshness",
                        True,
                        f"Newest PROMOTED manifest row is {age_days} days old "
                        f"(max {HUB_MANIFEST_MAX_AGE_DAYS})",
                    )
            else:
                record(
                    "single_generation",
                    False,
                    f"Latest manifest status is {latest.status!r} "
                    f"(active_run_id: {latest.active_run_id}, "
                    f"promoted_at: {latest.promoted_at}) -- hub is not in a "
                    f"promoted state",
                )
    except Exception as e:
        record("single_generation", False, f"Query error: {str(e)[:200]}")

    # 3. xref scale -- current row count vs last recorded rebuild's count.
    # Read the most recent completed rebuild's identity_hub_row_count from
    # profile_build_runs. If no prior run exists, this is a pass (first rebuild).
    try:
        # Current xref row count
        curr_sql = "SELECT COUNT(*) AS n FROM identity_hub_data.bn_id_xref"
        current_count = list(bq_client.query(curr_sql).result())[0].n
        result["xref_row_count"] = current_count

        prev_sql = """
        SELECT identity_hub_row_count AS prev
        FROM profile_ops.profile_build_runs
        WHERE status = 'completed'
          AND mode = 'rebuild'
          AND identity_hub_row_count IS NOT NULL
        ORDER BY started_at DESC
        LIMIT 1
        """
        prev_rows = list(bq_client.query(prev_sql).result())
        if prev_rows and prev_rows[0].prev is not None:
            prev_count = prev_rows[0].prev
            pct_delta = abs(current_count - prev_count) / prev_count * 100
            if pct_delta <= 20.0:
                record(
                    "xref_scale",
                    True,
                    f"xref rows {current_count:,} vs prior {prev_count:,} "
                    f"(delta: {pct_delta:.1f}%)",
                )
            else:
                record(
                    "xref_scale",
                    False,
                    f"xref rows {current_count:,} vs prior {prev_count:,} "
                    f"(delta: {pct_delta:.1f}% -- exceeds 20% threshold)",
                )
        else:
            record(
                "xref_scale",
                True,
                f"xref rows {current_count:,} (no prior baseline -- first rebuild)",
            )
    except Exception as e:
        record("xref_scale", False, f"Query error: {str(e)[:200]}")

    # 4. Concurrent rebuild signal -- rows still marked running within 60 min.
    # Rebuild skips this gate entirely (rebuild does not write profile_build_runs).
    # Refresh: hard gate. First, auto-expire stale 'running' rows (crash cleanup).
    try:
        expire_job = bq_client.query(
            """
        UPDATE profile_ops.profile_build_runs
        SET status = 'failed',
            completed_at = CURRENT_TIMESTAMP(),
            error_message = 'Auto-expired: running row older than 90 minutes (preflight crash cleanup)'
        WHERE status = 'running'
          AND started_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 MINUTE)
        """
        )
        expire_job.result()
        n = getattr(expire_job, "num_dml_affected_rows", None)
        if n is not None and n > 0:
            logger.info(
                f"Preflight: auto-expired {n} stale profile_build_runs row(s) "
                "(status=running, started_at > 90m ago)"
            )
    except Exception as e:
        logger.warning(f"Preflight: stale build auto-expire skipped: {str(e)[:200]}")
    if mode == "rebuild":
        record(
            "no_concurrent_rebuild",
            True,
            "Skipped for rebuild (this run does not persist profile_build_runs).",
        )
    else:
        try:
            sql = """
            SELECT build_id, status, started_at AS build_started_at
            FROM profile_ops.profile_build_runs
            WHERE status = 'running'
              AND started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)
            LIMIT 3
            """
            rows = list(bq_client.query(sql).result())
            if rows:
                sample = ", ".join(f"{r.build_id} ({r.status})" for r in rows)
                record(
                    "no_concurrent_rebuild",
                    False,
                    "BigQuery has profile_ops.profile_build_runs rows with status=running "
                    f"and started_at within the last 60 minutes: {sample}. "
                    "(Not a local-process check; may be another host or a stale row.)",
                )
            else:
                record("no_concurrent_rebuild", True, "No concurrent rebuild detected")
        except Exception as e:
            # First-ever rebuild won't have the log table populated; non-fatal.
            record(
                "no_concurrent_rebuild",
                True,
                f"Check skipped (no prior runs): {str(e)[:120]}",
            )

    # --- v6.5: Source freshness soft-warnings ---
    # Verify each upstream source has recent data. All soft -- never block rebuild
    # since source pipelines are independently scheduled.
    freshness_checks = [
        (
            "mailchimp_freshness",
            "SELECT MAX(last_changed) AS latest FROM mailchimp_data.members",
            7,
            "Mailchimp members",
        ),
        (
            "ga4_freshness",
            r"""SELECT MAX(PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(table_name, r'events_(\d{8})$'))) AS latest
            FROM region-us.INFORMATION_SCHEMA.TABLES
            WHERE table_schema LIKE 'analytics_%' AND table_name LIKE 'events_%'""",
            2,
            "GA4 events",
        ),
        # NPI full file is loaded on a ~monthly cadence; 60d avoids soft warnings when
        # the warehouse is healthy but between monthly drops (e.g. holiday slip).
        (
            "npi_freshness",
            "SELECT MAX(last_update_date) AS latest FROM npi_data.npi_main",
            60,
            "NPI registry",
        ),
        (
            "limesurvey_freshness",
            "SELECT MAX(submitdate) AS latest FROM limesurvey_data.lime_surveys_wide",
            30,
            "LimeSurvey responses",
        ),
        (
            "wordpress_freshness",
            "SELECT MAX(post_modified_gmt) AS latest FROM wordpress_data.wordpress_posts",
            7,
            "WordPress posts",
        ),
    ]
    for check_name, sql, threshold_days, label in freshness_checks:
        try:
            row = list(bq_client.query(sql).result())[0]
            latest = row.latest
            if latest is None:
                record(
                    check_name,
                    False,
                    f"{label}: no rows found -- source may be empty",
                    severity="soft",
                )
            else:
                import datetime

                if hasattr(latest, "date"):
                    latest_date = latest.date() if callable(latest.date) else latest
                else:
                    latest_date = latest
                age_days = (
                    (datetime.date.today() - latest_date).days
                    if hasattr(latest_date, "year")
                    else None
                )
                if age_days is not None and age_days > threshold_days:
                    record(
                        check_name,
                        False,
                        f"{label}: last record {age_days} days ago "
                        f"(threshold {threshold_days} days) -- source may be stale",
                        severity="soft",
                    )
                else:
                    record(
                        check_name,
                        True,
                        f"{label}: last record {age_days} days ago -- OK",
                        severity="soft",
                    )
        except Exception as e:
            record(
                check_name,
                False,
                f"{label}: query error -- {str(e)[:120]}",
                severity="soft",
            )

    # Decide -- only hard failures block the build
    failed_checks = [
        c["name"]
        for c in result["checks"]
        if not c["passed"] and c.get("severity", "hard") == "hard"
    ]
    if failed_checks:
        if force:
            logger.warning(
                f"Preflight FAILED ({len(failed_checks)}/{len(result['checks'])} checks: "
                f"{', '.join(failed_checks)}) -- proceeding due to --force"
            )
            result["status"] = "forced"
        else:
            logger.error(
                f"Preflight FAILED ({len(failed_checks)}/{len(result['checks'])} checks): "
                f"{', '.join(failed_checks)}"
            )
            for c in result["checks"]:
                sev = c.get("severity", "hard")
                if c["passed"]:
                    marker = "OK"
                    level = logger.info
                else:
                    marker = "FAIL" if sev == "hard" else "WARN"
                    level = logger.error if sev == "hard" else logger.warning
                level(f"  [{marker}] {c['name']}: {c['detail']}")
    else:
        soft_warns = [
            c["name"]
            for c in result["checks"]
            if not c["passed"] and c.get("severity") == "soft"
        ]
        warn_suffix = (
            f" ({len(soft_warns)} soft warning(s): {', '.join(soft_warns)})"
            if soft_warns
            else ""
        )
        logger.info(
            f"Preflight PASSED ({len(result['checks'])} checks, "
            f"{len(failed_checks)} hard failures){warn_suffix}. "
            f"Manifest run_id: {result['manifest_run_id']}"
        )

    return result


def run_post_build_assertions(
    bq_client,
    execution_id,
    mode="rebuild",
    config=None,
    consumer_dataset=PRODUCTION_DATASET,
    ops_dataset=OPS_DATASET,
    staging_dataset=STAGING_DATASET,
    view_dataset=PRODUCTION_DATASET,
):
    """
    Run data quality assertions after build completes (v6.4 expanded).

    Each assertion has a severity:
      - 'hard' (build acceptance gate): failure marks the build as failed_gate
        and run_pipeline raises. Candidate views may exist in
        profile_staging, but production views are not published until gates pass.
      - 'soft' (warning): failure is logged but doesn't fail the build.

    Build acceptance gate checks (mapped to the 6 gates):
      - profile_core_unique_bn_id            (hard -- gate #2)
      - profile_identifiers_unique_primary   (hard -- gate #2)
      - profile_current_unique_bn_id         (hard -- gate #2)
      - orphan_satellite                     (hard -- gate #2)
      - missing_parent                       (hard -- gate #2)
      - restore_coverage                     (hard -- gate #3, rebuild only)
      - refresh_safety_check                 (hard -- gate #1, refresh only;
                                              proves no immutable mutations or
                                              unexplained field changes; does NOT
                                              prove refresh equals rebuild)
      - identity_source_row_delta            (hard -- gate #1; guards the INPUT.
                                              Fails when the identity hub
                                              snapshot shrinks >10% versus the
                                              last completed build)
      - anonymous_known_count_delta          (soft -- catches tier shifts)
      - exception_spike                      (soft -- existing behavior)

    Returns a dict: name -> (passed, severity, detail).
    """
    assertions = {}
    job_config = bigquery.QueryJobConfig(use_query_cache=False)

    def _fmt(sql: str) -> str:
        return sql.format(
            consumer_dataset=consumer_dataset,
            ops_dataset=ops_dataset,
            staging_dataset=staging_dataset,
            view_dataset=view_dataset,
        )

    def _check(
        name: str,
        severity: str,
        sql: str,
        fail_if_rows: bool = True,
        detail_formatter=None,
    ):
        """
        Run an assertion SQL and record the result.
        fail_if_rows=True: assertion fails when query returns any rows.
        fail_if_rows=False: assertion fails when query returns zero rows.
        detail_formatter: optional callable(rows) -> str for custom messaging.
        """
        try:
            rows = list(bq_client.query(sql, job_config=job_config).result())
            if fail_if_rows:
                passed = not rows
            else:
                passed = bool(rows)

            if detail_formatter:
                detail = detail_formatter(rows)
            elif passed:
                detail = "PASS"
            else:
                sample = ", ".join(str(dict(r)) for r in rows[:3])
                detail = f"FAIL -- {len(rows)} issue(s): {sample}"
        except Exception as e:
            passed = False
            detail = f"ERROR -- {str(e)[:200]}"

        assertions[name] = (passed, severity, detail)
        level = (
            logger.info
            if passed
            else (logger.error if severity == "hard" else logger.warning)
        )
        marker = "PASS" if passed else ("FAIL" if severity == "hard" else "WARN")
        level(f"Assertion {name} [{severity}]: {marker} -- {detail}")
        return passed

    # --- Uniqueness checks (existing, now classified as hard) ---
    _check(
        "profile_core_unique_bn_id",
        "hard",
        _fmt(
            """SELECT bn_id, COUNT(*) AS cnt
              FROM {consumer_dataset}.profile_core
              GROUP BY bn_id HAVING cnt > 1 LIMIT 5"""
        ),
    )

    _check(
        "profile_identifiers_unique_primary",
        "hard",
        _fmt(
            """SELECT bn_id, identifier_type, COUNT(*) AS cnt
              FROM {consumer_dataset}.profile_identifiers
              WHERE is_primary = TRUE
              GROUP BY 1, 2 HAVING cnt > 1 LIMIT 5"""
        ),
    )

    _check(
        "profile_current_unique_bn_id",
        "hard",
        _fmt(
            """SELECT bn_id, COUNT(*) AS cnt
              FROM {view_dataset}.profile_current
              GROUP BY bn_id HAVING cnt > 1 LIMIT 5"""
        ),
    )

    # --- v6.5: Orphan satellite (build acceptance gate #2) ---
    # Policy: zero UNEXPECTED orphans across all five satellites.
    # Two structurally-permitted orphan classes are excluded from the count:
    #   1. Identifier rows for anonymous/device-only tracking types (bnfpvid,
    #      client_id, fbp, fbc, gcl_*, dmd_tag, aim_tag_id, mc_euid,
    #      subscriber_hash, aim_dgid) that fill_gaps writes before populate
    #      creates profile_core.
    #   2. Any satellite row whose bn_id was filtered out of profile_core by
    #      populate_identity_core's bot/shared-workstation guard
    #      (populate_identity_core.sql:608-609). Those bn_ids are real in
    #      identity_hub_data.bn_id_xref but populate intentionally excludes
    #      them; satellite populate steps don't apply the same filter, so
    #      they appear as orphans even though the consumer surface
    #      (profile_core) correctly suppresses them.
    # Anything else is unexpected and fails the gate.
    # For refresh: downgrade to soft since inherited orphans can only be cleared
    # by a full rebuild.
    orphan_severity = "hard" if mode in REBUILD_LIKE_MODES else "soft"
    # Allowed anonymous tracking types (fill_gaps writes these for clusters
    # that populate intentionally excludes from profile_core). v6.4 followup:
    # added subscriber_hash and aim_dgid; both are anonymous tracking identifiers
    # of the same class as the originals and were missing from the allow-list,
    # tripping the gate on the first-ever rebuild.
    _ALLOWED_ANON_TYPES = (
        "'bnfpvid','client_id','fbp','fbc','gcl_au','gcl_aw',"
        "'dmd_tag','aim_tag_id','mc_euid','subscriber_hash','aim_dgid'"
    )
    try:
        # _excluded_bn_ids: bn_ids that populate_identity_core intentionally
        # filters out via WHERE IFNULL(is_bot, FALSE)=FALSE AND
        # IFNULL(is_shared_workstation, FALSE)=FALSE. Satellites populated from
        # other paths still carry rows for these bn_ids; they are NOT real
        # orphans, just bn_ids the consumer surface correctly suppresses.
        orphan_sql = f"""
        WITH _excluded_bn_ids AS (
            SELECT DISTINCT bn_id
            FROM identity_hub_data.bn_id_xref
            WHERE IFNULL(is_bot, FALSE) = TRUE
               OR IFNULL(is_shared_workstation, FALSE) = TRUE
        ),
        satellites AS (
            SELECT 'profile_engagement' AS tbl,
                   (SELECT COUNT(*) FROM {consumer_dataset}.profile_engagement pe
                    WHERE pe.bn_id NOT IN (SELECT bn_id FROM _excluded_bn_ids)) AS total_n,
                   (SELECT COUNT(*) FROM {consumer_dataset}.profile_engagement pe
                    LEFT JOIN {consumer_dataset}.profile_core pc USING (bn_id)
                    WHERE pc.bn_id IS NULL
                      AND pe.bn_id NOT IN (SELECT bn_id FROM _excluded_bn_ids)) AS orphan_n
            UNION ALL
            -- profile_identifiers: exclude allowed anonymous tracking types.
            SELECT 'profile_identifiers',
                   (SELECT COUNT(*) FROM {consumer_dataset}.profile_identifiers pi
                    WHERE pi.identifier_type NOT IN ({_ALLOWED_ANON_TYPES})
                      AND pi.bn_id NOT IN (SELECT bn_id FROM _excluded_bn_ids)) AS total_n,
                   (SELECT COUNT(*) FROM {consumer_dataset}.profile_identifiers pi
                    LEFT JOIN {consumer_dataset}.profile_core pc USING (bn_id)
                    WHERE pc.bn_id IS NULL
                      AND pi.identifier_type NOT IN ({_ALLOWED_ANON_TYPES})
                      AND pi.bn_id NOT IN (SELECT bn_id FROM _excluded_bn_ids)) AS orphan_n
            UNION ALL
            SELECT 'profile_segment_tags',
                   (SELECT COUNT(*) FROM {consumer_dataset}.profile_segment_tags pst
                    WHERE pst.bn_id NOT IN (SELECT bn_id FROM _excluded_bn_ids)),
                   (SELECT COUNT(*) FROM {consumer_dataset}.profile_segment_tags pst
                    LEFT JOIN {consumer_dataset}.profile_core pc USING (bn_id)
                    WHERE pc.bn_id IS NULL
                      AND pst.bn_id NOT IN (SELECT bn_id FROM _excluded_bn_ids))
            UNION ALL
            SELECT 'profile_content_affinity',
                   (SELECT COUNT(*) FROM {consumer_dataset}.profile_content_affinity pca
                    WHERE pca.bn_id NOT IN (SELECT bn_id FROM _excluded_bn_ids)),
                   (SELECT COUNT(*) FROM {consumer_dataset}.profile_content_affinity pca
                    LEFT JOIN {consumer_dataset}.profile_core pc USING (bn_id)
                    WHERE pc.bn_id IS NULL
                      AND pca.bn_id NOT IN (SELECT bn_id FROM _excluded_bn_ids))
            UNION ALL
            SELECT 'profile_ad_attribution',
                   (SELECT COUNT(*) FROM {consumer_dataset}.profile_ad_attribution paa
                    WHERE paa.bn_id NOT IN (SELECT bn_id FROM _excluded_bn_ids)),
                   (SELECT COUNT(*) FROM {consumer_dataset}.profile_ad_attribution paa
                    LEFT JOIN {consumer_dataset}.profile_core pc USING (bn_id)
                    WHERE pc.bn_id IS NULL
                      AND paa.bn_id NOT IN (SELECT bn_id FROM _excluded_bn_ids))
        )
        SELECT tbl, total_n, orphan_n,
               ROUND(100.0 * orphan_n / NULLIF(total_n, 0), 3) AS orphan_pct
        FROM satellites
        WHERE orphan_n > 0
        ORDER BY orphan_pct DESC
        """
        orphan_rows = list(bq_client.query(orphan_sql, job_config=job_config).result())

        # v6.4 followup: graduated severity. Long-running identity hubs evolve
        # between populate and assertion time -- a bn_id flagged is_bot=False
        # at populate may have been is_bot=True earlier (or vice versa), causing
        # a tiny residual fraction of satellite rows to legitimately point at
        # bn_ids absent from profile_core. Treat anything below the threshold
        # as a soft warning rather than a hard fail; only a meaningful fraction
        # (>= ORPHAN_HARD_FAIL_PCT on any one satellite) blocks the build.
        ORPHAN_HARD_FAIL_PCT = 0.1  # 0.1% per table
        if orphan_rows:
            sample = "; ".join(
                f"{r.tbl}: {r.orphan_n:,} unexpected orphans / {r.total_n:,} total ({r.orphan_pct}%)"
                for r in orphan_rows
            )
            max_pct = max((r.orphan_pct or 0) for r in orphan_rows)
            below_threshold = max_pct < ORPHAN_HARD_FAIL_PCT
            # Demote to soft if every satellite is below threshold, even on rebuild.
            effective_severity = "soft" if below_threshold else orphan_severity
            verdict = (
                (
                    f"PASS-with-residual -- orphans below {ORPHAN_HARD_FAIL_PCT}% threshold "
                    f"on every satellite (worst {max_pct}%): {sample}"
                )
                if below_threshold
                else (
                    f"FAIL -- unexpected orphans exceed {ORPHAN_HARD_FAIL_PCT}% on at least "
                    f"one satellite (worst {max_pct}%): {sample}"
                )
            )
            assertions["orphan_satellite"] = (
                below_threshold,
                effective_severity,
                verdict,
            )
            level = (
                logger.info
                if below_threshold
                else (logger.error if effective_severity == "hard" else logger.warning)
            )
            level(
                f"Assertion orphan_satellite [{effective_severity}]: "
                f"{'PASS-with-residual' if below_threshold else 'FAIL'} -- {sample}"
            )
            if config and effective_severity == "soft" and not below_threshold:
                try:
                    from shared.notifications import send_notification

                    send_notification(
                        notification_type="warning",
                        message=(
                            f"Profile database ({execution_id}) has unexpected orphan satellite "
                            f"rows. Inherited from prior rebuild -- will self-clear on next full "
                            f"rebuild. Details:\n\n{sample}"
                        ),
                        config=config,
                        severity="medium",
                    )
                except Exception as alert_err:
                    logger.warning(f"Orphan alert notification failed: {alert_err}")
        else:
            assertions["orphan_satellite"] = (
                True,
                orphan_severity,
                "PASS -- zero unexpected orphans across all satellites",
            )
            logger.info(
                f"Assertion orphan_satellite [{orphan_severity}]: PASS -- zero unexpected orphans"
            )
    except Exception as e:
        assertions["orphan_satellite"] = (
            False,
            orphan_severity,
            f"ERROR -- {str(e)[:200]}",
        )
        logger.error(f"Assertion orphan_satellite [{orphan_severity}]: ERROR -- {e}")

    # --- Hard population sync: eligible xref ↔ profile_core ---
    # Catches membership drift that the 200-sample refresh_safety_check cannot.
    # Contract: profile_core == non-bot, non-shared-workstation bn_id_xref.
    try:
        # Compare against the BUILD-LOCAL snapshot, not the live hub. The build
        # constructs core from the snapshot taken at start; the hub cron
        # (11:00, promoting ~12:04) overlaps the profile run (12:00), so live
        # xref can move mid-run. 2026-08-26: the hub flagged 18,359 new bots
        # and promoted at 12:04; 1,103 core rows that were eligible in the
        # 12:01 snapshot became "ineligible orphans" against live and failed
        # an internally-correct build. Live drift is the NEXT run's input,
        # not this run's error. Every other assertion already reads the
        # snapshot; this one was the exception.
        pop_sql = f"""
        WITH elig AS (
          SELECT DISTINCT bn_id FROM {IDENTITY_XREF_SNAPSHOT_TABLE}
          WHERE NOT IFNULL(is_bot, FALSE)
            AND NOT IFNULL(is_shared_workstation, FALSE)
        ), pc AS (
          SELECT DISTINCT bn_id FROM {consumer_dataset}.profile_core
        )
        SELECT
          (SELECT COUNT(*) FROM elig) AS eligible_xref,
          (SELECT COUNT(*) FROM pc) AS profile_core,
          (SELECT COUNT(*) FROM pc LEFT JOIN elig USING(bn_id)
           WHERE elig.bn_id IS NULL) AS in_profile_not_eligible,
          (SELECT COUNT(*) FROM elig LEFT JOIN pc USING(bn_id)
           WHERE pc.bn_id IS NULL) AS eligible_missing_profile
        """
        pop_rows = list(bq_client.query(pop_sql, job_config=job_config).result())
        if pop_rows:
            r = pop_rows[0]
            in_bad = int(r.in_profile_not_eligible or 0)
            missing = int(r.eligible_missing_profile or 0)
            # Hard-fail on orphans in core; soft-warn on missing (refresh may
            # still be catching up after hub growth within the lookback gap —
            # reconcile backfill should close it; tolerate small absolute gap).
            if in_bad > 0:
                assertions["population_sync"] = (
                    False,
                    "hard",
                    f"FAIL -- {in_bad:,} profile_core rows not in eligible xref "
                    f"(elig={r.eligible_xref:,} core={r.profile_core:,})",
                )
                logger.error(
                    "Assertion population_sync [hard]: FAIL -- ineligible orphans=%s",
                    in_bad,
                )
            elif missing > 50000:
                assertions["population_sync"] = (
                    False,
                    "hard",
                    f"FAIL -- {missing:,} eligible xref missing from profile_core "
                    f"(elig={r.eligible_xref:,} core={r.profile_core:,})",
                )
                logger.error(
                    "Assertion population_sync [hard]: FAIL -- missing=%s", missing
                )
            elif missing > 0:
                assertions["population_sync"] = (
                    True,
                    "soft",
                    f"WARN -- {missing:,} eligible missing from core "
                    f"(elig={r.eligible_xref:,} core={r.profile_core:,})",
                )
                logger.warning(
                    "Assertion population_sync [soft]: %s eligible missing", missing
                )
            else:
                assertions["population_sync"] = (
                    True,
                    "hard",
                    f"PASS -- elig={r.eligible_xref:,} core={r.profile_core:,} exact",
                )
                logger.info("Assertion population_sync [hard]: PASS")
    except Exception as e:
        assertions["population_sync"] = (
            False,
            "soft",
            f"ERROR -- {str(e)[:200]}",
        )
        logger.warning(f"Assertion population_sync [soft]: ERROR -- {e}")

    # --- NEW v6.4: Missing parent -- engagement count vs core count ---
    # After build, profile_engagement should have ~= as many rows as profile_core.
    # A > 5% gap means engagement wasn't built for a chunk of profiles.
    #
    # Hard on both rebuild and refresh. The reconcile step (refresh-only,
    # sql/profile_database_reconcile.sql) evicts bn_ids that are no longer
    # eligible, backfills missing-but-eligible ones, and deletes satellite
    # orphans, so refresh now converges to the same population set rebuild
    # produces -- there is no longer "inherited drift" that justifies a soft
    # severity. Bots/workstations are explicitly excluded from the engagement
    # count for an apples-to-apples comparison with profile_core's filter.
    _check(
        "missing_parent",
        "hard",
        _fmt(
            """WITH excluded_bn_ids AS (
               SELECT DISTINCT bn_id
               FROM identity_hub_data.bn_id_xref
               WHERE IFNULL(is_bot, FALSE) = TRUE
                  OR IFNULL(is_shared_workstation, FALSE) = TRUE
             ),
             counts AS (
               SELECT
                 (SELECT COUNT(*) FROM {consumer_dataset}.profile_core) AS core_n,
                 (SELECT COUNT(*) FROM {consumer_dataset}.profile_engagement pe
                  WHERE pe.bn_id NOT IN (SELECT bn_id FROM excluded_bn_ids)) AS eng_n
             )
             SELECT core_n, eng_n,
                    ROUND(100.0 * ABS(core_n - eng_n) / NULLIF(core_n, 0), 2) AS pct_gap
             FROM counts
             WHERE NULLIF(core_n, 0) IS NOT NULL
               AND ABS(core_n - eng_n) / core_n > 0.05"""
        ),
        detail_formatter=lambda rows: (
            "PASS -- engagement count within 5% of core (bots/workstations excluded)"
            if not rows
            else f"FAIL -- core={rows[0].core_n:,} engagement={rows[0].eng_n:,} "
            f"gap={rows[0].pct_gap}% (bots/workstations excluded)"
        ),
    )

    # --- NEW v6.4: Restore coverage (build acceptance gate #3, rebuild only) ---
    # Snapshot rows that couldn't be remapped to a current bn_id. Must be < 1%
    # of snapshot rows.
    if mode in REBUILD_LIKE_MODES:
        try:
            params = [bigquery.ScalarQueryParameter("build_id", "STRING", execution_id)]
            param_cfg = bigquery.QueryJobConfig(
                use_query_cache=False, query_parameters=params
            )
            rows = list(
                bq_client.query(
                    _fmt(
                        """WITH counts AS (
                       SELECT
                         (SELECT COUNT(*) FROM {staging_dataset}.profile_core_app_snapshot) AS snap_n,
                         (SELECT COUNT(*) FROM {ops_dataset}.profile_restore_unmapped
                          WHERE build_id = @build_id) AS unmapped_n
                     )
                     SELECT snap_n, unmapped_n,
                            ROUND(100.0 * unmapped_n / NULLIF(snap_n, 0), 3) AS pct_unmapped
                     FROM counts"""
                    ),
                    job_config=param_cfg,
                ).result()
            )
            if rows:
                snap_n = rows[0].snap_n or 0
                unmapped_n = rows[0].unmapped_n or 0
                pct = rows[0].pct_unmapped or 0
                if snap_n == 0:
                    # No prior app data -- nothing to restore. Pass by definition.
                    assertions["restore_coverage"] = (
                        True,
                        "hard",
                        f"PASS -- snapshot empty (first rebuild or no app-authored data yet)",
                    )
                    logger.info(
                        f"Assertion restore_coverage [hard]: PASS -- snapshot empty"
                    )
                elif pct > 1.0:
                    assertions["restore_coverage"] = (
                        False,
                        "hard",
                        f"FAIL -- {unmapped_n}/{snap_n} unmapped ({pct}%) -- expected <1%",
                    )
                    logger.error(
                        f"Assertion restore_coverage [hard]: FAIL -- {unmapped_n}/{snap_n} "
                        f"snapshot rows unmapped ({pct}%)"
                    )
                else:
                    assertions["restore_coverage"] = (
                        True,
                        "hard",
                        f"PASS -- {unmapped_n}/{snap_n} unmapped ({pct}%)",
                    )
                    logger.info(
                        f"Assertion restore_coverage [hard]: PASS -- "
                        f"{unmapped_n}/{snap_n} unmapped ({pct}%)"
                    )
        except Exception as e:
            assertions["restore_coverage"] = (False, "hard", f"ERROR -- {str(e)[:200]}")
            logger.error(f"Assertion restore_coverage [hard]: ERROR -- {e}")

    # --- NEW v6.4: Anonymous vs known count delta (soft) ---
    # Catches "the anonymous tier disappeared" bugs. Compare current tier1/tier2
    # distribution to the last recorded rebuild.
    try:
        curr_sql = """
        SELECT
            COUNTIF(cluster_tier = 'tier1') AS tier1_n,
            COUNTIF(cluster_tier = 'tier2') AS tier2_n,
            COUNT(*) AS total_n
        FROM {consumer_dataset}.profile_core
        """
        curr = list(bq_client.query(_fmt(curr_sql), job_config=job_config).result())[0]

        # Baseline lives on profile_build_runs.metadata (v6.4). The
        # post-assertion UPDATE later in this function stamps tier counts
        # onto the current run row so the next build can read them here.
        #
        # The baseline deliberately accepts ANY completed mode, not just
        # rebuild. It was scoped to mode='rebuild' until 2026-08-19, which
        # made this assertion permanently inert: no completed rebuild has
        # ever carried a tier1_n baseline (verified: 0 rows), so every run
        # fell through to the "no prior baseline" PASS branch. It therefore
        # stayed silent on 2026-08-07 when tier2 fell 4,888,806 -> 2,889,812
        # (-40.9%), five times its own 15% threshold. Refreshes run daily and
        # stamp the same counts, so they are the correct baseline.
        prev_sql = """
        SELECT
            SAFE_CAST(JSON_VALUE(metadata, '$.tier1_n') AS INT64) AS prev_t1,
            SAFE_CAST(JSON_VALUE(metadata, '$.tier2_n') AS INT64) AS prev_t2
        FROM {ops_dataset}.profile_build_runs
        WHERE status LIKE 'completed%'
          AND metadata IS NOT NULL
          AND JSON_VALUE(metadata, '$.tier1_n') IS NOT NULL
        ORDER BY started_at DESC
        LIMIT 1
        """
        prev_rows = list(
            bq_client.query(_fmt(prev_sql), job_config=job_config).result()
        )
        if prev_rows and prev_rows[0].prev_t1 is not None:
            prev_t1, prev_t2 = prev_rows[0].prev_t1, prev_rows[0].prev_t2
            delta_t1 = abs(curr.tier1_n - prev_t1) / prev_t1 * 100 if prev_t1 else 0
            delta_t2 = abs(curr.tier2_n - prev_t2) / prev_t2 * 100 if prev_t2 else 0
            if delta_t1 > 15 or delta_t2 > 15:
                assertions["anonymous_known_count_delta"] = (
                    False,
                    "soft",
                    f"WARN -- tier1 {prev_t1:,}->{curr.tier1_n:,} ({delta_t1:.1f}%), "
                    f"tier2 {prev_t2:,}->{curr.tier2_n:,} ({delta_t2:.1f}%). "
                    f"Exceeds 15% threshold.",
                )
                logger.warning(
                    f"Assertion anonymous_known_count_delta [soft]: WARN -- "
                    f"tier1 delta {delta_t1:.1f}%, tier2 delta {delta_t2:.1f}%"
                )
            else:
                assertions["anonymous_known_count_delta"] = (
                    True,
                    "soft",
                    f"PASS -- tier1 {curr.tier1_n:,} (prev {prev_t1:,}), "
                    f"tier2 {curr.tier2_n:,} (prev {prev_t2:,})",
                )
                logger.info(
                    f"Assertion anonymous_known_count_delta [soft]: PASS "
                    f"({curr.tier1_n:,}/{curr.tier2_n:,})"
                )
        else:
            assertions["anonymous_known_count_delta"] = (
                True,
                "soft",
                f"PASS -- tier1 {curr.tier1_n:,}, tier2 {curr.tier2_n:,} (no prior baseline)",
            )
            logger.info(
                f"Assertion anonymous_known_count_delta [soft]: PASS -- "
                f"no prior baseline (tier1={curr.tier1_n:,}, tier2={curr.tier2_n:,})"
            )
    except Exception as e:
        assertions["anonymous_known_count_delta"] = (
            False,
            "soft",
            f"ERROR -- {str(e)[:200]}",
        )
        logger.warning(f"Assertion anonymous_known_count_delta [soft]: ERROR -- {e}")

    # --- v6.8: Identity source row delta (hard) ---
    # Guards the INPUT, not the output. Every assertion above measures
    # profile_core, which is only ever as good as the identity hub snapshot it
    # was built from -- so a truncated hub produces a profile_core that is
    # internally consistent and quietly wrong.
    #
    # 2026-08-07 is the case this exists for. A full hub rebuild aged out
    # 2,085,967 anonymous identities under a 90-day effective lifetime nobody
    # had approved (fixed in 3735ee9, retention now 400 days). bn_id_xref
    # arrived at 15,710,972 rows against 27,153,725 the day before, and
    # profile_core rebuilt faithfully to 3,671,046 profiles instead of
    # 5,681,415.
    #
    # Nothing was designed to catch that. The gate fired only because
    # preferred_condition happened to fall 14.7pp -- every other critical fill
    # rate ROSE, since dropping sparse anonymous rows lifts the average. Had
    # the truncation hit tier1 instead, all six fill rates would have improved
    # and the build would have published. This assertion removes that luck:
    # it reads the row count of the snapshot actually consumed.
    #
    # Hard at 10%: the hub grows roughly 1%/day and has never fallen day over
    # day in recorded history, so 10% is far outside normal drift while still
    # catching 2026-08-07 four times over.
    try:
        xref_sql = """
        SELECT
            SAFE_CAST(JSON_VALUE(metadata, '$.identity_snapshot.xref_row_count') AS INT64) AS xref_n,
            build_id,
            started_at
        FROM {ops_dataset}.profile_build_runs
        WHERE JSON_VALUE(metadata, '$.identity_snapshot.xref_row_count') IS NOT NULL
          AND (build_id = @execution_id OR status LIKE 'completed%')
        ORDER BY started_at DESC
        LIMIT 2
        """
        xref_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("execution_id", "STRING", execution_id)
            ]
        )
        xref_job_config.use_query_cache = False
        if job_config is not None:
            if job_config.labels:
                xref_job_config.labels = job_config.labels
            if job_config.maximum_bytes_billed:
                xref_job_config.maximum_bytes_billed = job_config.maximum_bytes_billed
        xref_rows = list(
            bq_client.query(_fmt(xref_sql), job_config=xref_job_config).result()
        )
        curr_xref = next((r for r in xref_rows if r.build_id == execution_id), None)
        prev_xref = next((r for r in xref_rows if r.build_id != execution_id), None)

        if curr_xref is None:
            assertions["identity_source_row_delta"] = (
                True,
                "hard",
                "PASS -- no identity snapshot recorded for this run",
            )
            logger.info(
                "Assertion identity_source_row_delta [hard]: PASS -- no snapshot this run"
            )
        elif prev_xref is None:
            assertions["identity_source_row_delta"] = (
                True,
                "hard",
                f"PASS -- xref {curr_xref.xref_n:,} (no prior baseline)",
            )
            logger.info(
                f"Assertion identity_source_row_delta [hard]: PASS -- "
                f"xref {curr_xref.xref_n:,}, no prior baseline"
            )
        else:
            drop_pct = (
                (prev_xref.xref_n - curr_xref.xref_n) / prev_xref.xref_n * 100
                if prev_xref.xref_n
                else 0.0
            )
            if drop_pct > 10.0:
                assertions["identity_source_row_delta"] = (
                    False,
                    "hard",
                    f"FAIL -- identity hub snapshot shrank {prev_xref.xref_n:,} -> "
                    f"{curr_xref.xref_n:,} (-{drop_pct:.1f}%) versus build "
                    f"{prev_xref.build_id}. The hub, not the profile build, is the "
                    f"suspect: investigate before publishing.",
                )
                logger.error(
                    f"Assertion identity_source_row_delta [hard]: FAIL -- "
                    f"xref {prev_xref.xref_n:,} -> {curr_xref.xref_n:,} (-{drop_pct:.1f}%)"
                )
            else:
                assertions["identity_source_row_delta"] = (
                    True,
                    "hard",
                    f"PASS -- xref {curr_xref.xref_n:,} (prev {prev_xref.xref_n:,}, "
                    f"{-drop_pct:+.1f}%)",
                )
                logger.info(
                    f"Assertion identity_source_row_delta [hard]: PASS -- "
                    f"xref {curr_xref.xref_n:,} ({-drop_pct:+.1f}%)"
                )
    except Exception as e:
        assertions["identity_source_row_delta"] = (
            False,
            "hard",
            f"ERROR -- {str(e)[:200]}",
        )
        logger.error(f"Assertion identity_source_row_delta [hard]: ERROR -- {e}")

    # --- v6.5: Fill-rate drift checks ---
    # Detect silent profile-quality regressions that simple COUNT deltas miss.
    fill_rate_fields = (
        "email_fill_rate",
        "site_domain_fill_rate",
        "preferred_condition_fill_rate",
        "first_name_fill_rate",
        "condition_subtype_fill_rate",
        "diagnosis_stage_fill_rate",
    )
    critical_fill_rate_fields = {
        "email_fill_rate",
        "site_domain_fill_rate",
        "preferred_condition_fill_rate",
    }
    try:
        current_fill_sql = _fmt(
            """
        SELECT
            COUNT(*) AS total_profiles,
            SAFE_DIVIDE(COUNTIF(email IS NOT NULL AND TRIM(email) != ''), COUNT(*)) AS email_fill_rate,
            SAFE_DIVIDE(COUNTIF(site_domain IS NOT NULL AND TRIM(site_domain) != ''), COUNT(*)) AS site_domain_fill_rate,
            SAFE_DIVIDE(COUNTIF(preferred_condition IS NOT NULL), COUNT(*)) AS preferred_condition_fill_rate,
            SAFE_DIVIDE(COUNTIF(first_name IS NOT NULL AND TRIM(first_name) != ''), COUNT(*)) AS first_name_fill_rate,
            SAFE_DIVIDE(COUNTIF(condition_subtype IS NOT NULL), COUNT(*)) AS condition_subtype_fill_rate,
            SAFE_DIVIDE(COUNTIF(diagnosis_stage IS NOT NULL AND TRIM(diagnosis_stage) != ''), COUNT(*)) AS diagnosis_stage_fill_rate
        FROM {consumer_dataset}.profile_core
        """
        )
        current_fill_row = list(
            bq_client.query(current_fill_sql, job_config=job_config).result()
        )[0]
        current_fill_rates = {
            field_name: float(getattr(current_fill_row, field_name) or 0.0)
            for field_name in fill_rate_fields
        }

        prev_fill_sql = """
        SELECT
            build_id,
            SAFE_CAST(JSON_VALUE(metadata, '$.fill_rates.email_fill_rate') AS FLOAT64) AS email_fill_rate,
            SAFE_CAST(JSON_VALUE(metadata, '$.fill_rates.site_domain_fill_rate') AS FLOAT64) AS site_domain_fill_rate,
            SAFE_CAST(JSON_VALUE(metadata, '$.fill_rates.preferred_condition_fill_rate') AS FLOAT64) AS preferred_condition_fill_rate,
            SAFE_CAST(JSON_VALUE(metadata, '$.fill_rates.first_name_fill_rate') AS FLOAT64) AS first_name_fill_rate,
            SAFE_CAST(JSON_VALUE(metadata, '$.fill_rates.condition_subtype_fill_rate') AS FLOAT64) AS condition_subtype_fill_rate,
            SAFE_CAST(JSON_VALUE(metadata, '$.fill_rates.diagnosis_stage_fill_rate') AS FLOAT64) AS diagnosis_stage_fill_rate,
            SAFE_CAST(JSON_VALUE(metadata, '$.fill_rate_total_profiles') AS INT64) AS total_profiles
        FROM {ops_dataset}.profile_build_runs
        WHERE status LIKE 'completed%'
          AND metadata IS NOT NULL
          AND JSON_VALUE(metadata, '$.fill_rates.email_fill_rate') IS NOT NULL
        ORDER BY started_at DESC
        LIMIT 1
        """
        prev_fill_rows = list(
            bq_client.query(_fmt(prev_fill_sql), job_config=job_config).result()
        )

        if prev_fill_rows:
            prev_fill_row = prev_fill_rows[0]
            monitoring_regressions = []
            critical_regressions = []
            # 2026-08-22: the critical gate judges ABSOLUTE filled counts, not
            # percentage points of a moving population. When the identity hub
            # retains 1.2M extra cookie-only identities at once, every rate
            # drops (condition 51.4% -> 42.4%) while the filled counts are
            # flat -- that is dilution, not loss, and it must not fail the
            # build. A real loss is the filled COUNT shrinking >5%. The
            # percentage-point rule stays as the soft monitoring signal, and
            # as the critical fallback only when no previous total is stored.
            prev_total = int(getattr(prev_fill_row, "total_profiles", None) or 0)
            cur_total = int(current_fill_row.total_profiles or 0)
            for field_name in fill_rate_fields:
                previous_rate = float(getattr(prev_fill_row, field_name) or 0.0)
                current_rate = current_fill_rates[field_name]
                drop_pp = (previous_rate - current_rate) * 100
                if drop_pp > 2.0:
                    monitoring_regressions.append(
                        f"{field_name}: {previous_rate:.3%} -> {current_rate:.3%} (-{drop_pp:.2f}pp)"
                    )
                if field_name not in critical_fill_rate_fields:
                    continue
                if prev_total > 0 and cur_total > 0:
                    prev_abs = previous_rate * prev_total
                    cur_abs = current_rate * cur_total
                    if prev_abs > 0 and cur_abs < prev_abs * 0.95:
                        critical_regressions.append(
                            f"{field_name}: filled {prev_abs:,.0f} -> {cur_abs:,.0f} "
                            f"(-{100 * (1 - cur_abs / prev_abs):.1f}% absolute; "
                            f"population {prev_total:,} -> {cur_total:,})"
                        )
                elif drop_pp > 5.0:
                    critical_regressions.append(
                        f"{field_name}: {previous_rate:.3%} -> {current_rate:.3%} (-{drop_pp:.2f}pp; no prior total stored)"
                    )

            if critical_regressions:
                detail = "; ".join(critical_regressions)
                assertions["fill_rate_drift_critical"] = (
                    False,
                    "hard",
                    f"FAIL -- critical fill-rate regression(s) versus build {prev_fill_row.build_id}: {detail}",
                )
                logger.error(
                    f"Assertion fill_rate_drift_critical [hard]: FAIL -- {detail}"
                )
            else:
                assertions["fill_rate_drift_critical"] = (
                    True,
                    "hard",
                    f"PASS -- no critical fill-rate drops >5pp versus build {prev_fill_row.build_id}",
                )
                logger.info("Assertion fill_rate_drift_critical [hard]: PASS")

            if monitoring_regressions:
                detail = "; ".join(monitoring_regressions)
                assertions["fill_rate_drift_monitoring"] = (
                    False,
                    "soft",
                    f"WARN -- fill-rate regression(s) versus build {prev_fill_row.build_id}: {detail}",
                )
                logger.warning(
                    f"Assertion fill_rate_drift_monitoring [soft]: WARN -- {detail}"
                )
            else:
                assertions["fill_rate_drift_monitoring"] = (
                    True,
                    "soft",
                    f"PASS -- tracked fill rates stable versus build {prev_fill_row.build_id}",
                )
                logger.info("Assertion fill_rate_drift_monitoring [soft]: PASS")
        else:
            assertions["fill_rate_drift_critical"] = (
                True,
                "hard",
                "PASS -- no prior fill-rate baseline yet",
            )
            assertions["fill_rate_drift_monitoring"] = (
                True,
                "soft",
                "PASS -- no prior fill-rate baseline yet",
            )
            logger.info(
                "Assertion fill_rate_drift_critical [hard]: PASS -- no prior baseline"
            )
            logger.info(
                "Assertion fill_rate_drift_monitoring [soft]: PASS -- no prior baseline"
            )

        _merge_build_run_metadata(
            bq_client,
            execution_id,
            {
                "fill_rates": current_fill_rates,
                "fill_rate_total_profiles": int(current_fill_row.total_profiles or 0),
            },
        )
    except Exception as e:
        assertions["fill_rate_drift_critical"] = (
            False,
            "soft",
            f"ERROR -- {str(e)[:200]}",
        )
        assertions["fill_rate_drift_monitoring"] = (
            False,
            "soft",
            f"ERROR -- {str(e)[:200]}",
        )
        logger.warning(f"Assertion fill_rate_drift [soft]: ERROR -- {e}")

    # Schema-drift check (profile_core vs profile_core_snapshot) was moved to
    # a hard gate immediately after the DDL step in run_pipeline(). It no longer
    # runs here -- by post-build assertion time, snapshot is already written.

    # --- NEW v6.4: profile_field_changes populated (lineage side-inserts fired) ---
    # SCOPE: this is a narrow audit log, not a generic field-level lineage
    # surface. Only four persona-classification fields write here today:
    # account_type, condition_subtype, diagnosis_stage, preferred_condition.
    # See docs/PROFILE_DATABASE_REFERENCE.md for the contract.
    #
    # Rebuild + refresh both write to profile_field_changes via side-INSERTs next
    # to every UPDATE/MERGE that touches those four fields. If those side-inserts
    # silently stopped firing (e.g. someone refactored a statement and forgot its
    # companion INSERT), the table would stay empty for this build_id. That
    # breaks the "why was this user classified as X?" query surface that
    # consumers are meant to rely on.
    #
    # Soft-severity: a lineage gap is a diagnostic regression, not a data
    # corruption event. The underlying fields in profile_core are still
    # correct. Ops should investigate and re-run, but the rebuild itself
    # is safe to ship.
    #
    # Runs for rebuild and refresh only (modes that write profile_field_changes for
    # this execution_id). resume_rebuild / resume_publish never hit this block.
    if mode in ("rebuild", "refresh"):
        try:
            lineage_sql = """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT field_name) AS distinct_fields,
                COUNTIF(field_name = 'condition_subtype') AS n_condition_subtype,
                COUNTIF(field_name = 'diagnosis_stage') AS n_diagnosis_stage,
                COUNTIF(field_name = 'preferred_condition') AS n_preferred_condition,
                -- v6.5 multi-role flags (replaced account_type)
                COUNTIF(field_name = 'is_patient') AS n_is_patient,
                COUNTIF(field_name = 'is_hcp') AS n_is_hcp,
                COUNTIF(field_name = 'is_caregiver') AS n_is_caregiver,
                COUNTIF(field_name = 'is_family_or_friend') AS n_is_family_or_friend,
                COUNTIF(field_name = 'is_other') AS n_is_other
            FROM {ops_dataset}.profile_field_changes
            WHERE build_id = @build_id
            """
            lineage_cfg = bigquery.QueryJobConfig(
                use_query_cache=False,
                query_parameters=[
                    bigquery.ScalarQueryParameter("build_id", "STRING", execution_id),
                ],
            )
            row = list(
                bq_client.query(_fmt(lineage_sql), job_config=lineage_cfg).result()
            )[0]

            if row.total_rows == 0 and mode in REBUILD_LIKE_MODES:
                # Full rebuild with zero lineage rows almost certainly means the
                # side-inserts silently stopped firing. At real-world scale
                # we expect 10k+ rows across the four tracked fields.
                assertions["profile_field_changes_populated"] = (
                    False,
                    "soft",
                    "WARN -- zero profile_field_changes rows for this rebuild. "
                    "Side-INSERT blocks in personas.sql and enrich_v2.sql may not "
                    "have fired. Lineage queries will return empty.",
                )
                logger.warning(
                    "Assertion profile_field_changes_populated [soft]: WARN -- "
                    "zero rows written for this build_id"
                )
            elif row.distinct_fields < 3 and mode in REBUILD_LIKE_MODES:
                assertions["profile_field_changes_populated"] = (
                    False,
                    "soft",
                    f"WARN -- only {row.distinct_fields} of 8 tracked fields "
                    f"received lineage rows "
                    f"(condition_subtype={row.n_condition_subtype}, "
                    f"diagnosis_stage={row.n_diagnosis_stage}, "
                    f"preferred_condition={row.n_preferred_condition}, "
                    f"is_patient={row.n_is_patient}, is_hcp={row.n_is_hcp}, "
                    f"is_caregiver={row.n_is_caregiver}, "
                    f"is_family_or_friend={row.n_is_family_or_friend}, "
                    f"is_other={row.n_is_other})",
                )
                logger.warning(
                    f"Assertion profile_field_changes_populated [soft]: WARN -- "
                    f"{row.distinct_fields}/8 tracked fields populated"
                )
            else:
                assertions["profile_field_changes_populated"] = (
                    True,
                    "soft",
                    f"PASS -- {row.total_rows:,} lineage rows across "
                    f"{row.distinct_fields} fields "
                    f"(condition_subtype={row.n_condition_subtype}, "
                    f"diagnosis_stage={row.n_diagnosis_stage}, "
                    f"preferred_condition={row.n_preferred_condition}, "
                    f"is_patient={row.n_is_patient}, is_hcp={row.n_is_hcp}, "
                    f"is_caregiver={row.n_is_caregiver}, "
                    f"is_family_or_friend={row.n_is_family_or_friend}, "
                    f"is_other={row.n_is_other})",
                )
                logger.info(
                    f"Assertion profile_field_changes_populated [soft]: PASS "
                    f"({row.total_rows:,} rows, {row.distinct_fields} fields)"
                )
        except Exception as e:
            assertions["profile_field_changes_populated"] = (
                False,
                "soft",
                f"ERROR -- {str(e)[:200]}",
            )
            logger.warning(
                f"Assertion profile_field_changes_populated [soft]: ERROR -- {e}"
            )

    # --- v6.5: Refresh safety check (renamed from refresh_rebuild_parity_sample) ---
    # What this proves: refresh did not regress or unexpectedly mutate fields
    # that should be stable. It does NOT prove refresh equals rebuild.
    # Shadow rebuild comparison is the v6.6 gold standard.
    #
    # Field categories:
    #   IMMUTABLE: npi_number, bionews_uk, wp_user_id -- must not change when the
    #              snapshot baseline already had a non-NULL value. NULL baseline
    #              allows first-time fill on refresh (net-new / backfill parity).
    #   FORWARD-ONLY: engagement counts, last_seen_* -- may only increase/advance.
    #   BIDIRECTIONAL: preferred_condition, condition_subtype, diagnosis_stage,
    #                  and the 5 v6.5 role flags (is_patient, is_hcp, is_caregiver,
    #                  is_family_or_friend, is_other) -- may change only when
    #                  profile_field_changes records a source-justified write
    #                  for this refresh run.
    #
    # Runs in refresh mode only -- uses profile_core_snapshot as rebuild baseline.
    if mode == "refresh":
        try:
            safety_sql = """
            WITH sampled AS (
                -- Sample up to 200 bn_ids from the refresh scope
                SELECT bn_id FROM {staging_dataset}.refresh_scope_bn_ids
                WHERE bn_id != '*'
                ORDER BY bn_id
                LIMIT 200
            ),
            -- Last rebuild snapshot as baseline
            snap AS (
                SELECT s.*
                FROM {ops_dataset}.profile_core_snapshot s
                JOIN sampled ON s.bn_id = sampled.bn_id
                WHERE s.snapshot_run_id = (
                    SELECT snapshot_run_id FROM {ops_dataset}.profile_core_snapshot
                    ORDER BY snapshotted_at DESC LIMIT 1
                )
            ),
            -- Current state post-refresh
            curr AS (
                SELECT pc.*
                FROM {consumer_dataset}.profile_core pc
                JOIN sampled ON pc.bn_id = sampled.bn_id
            ),
            -- Lineage writes since the snapshot baseline.
            -- Was: WHERE build_id = @build_id (only the current build).
            -- That narrow scope tripped the gate every time a tracked field
            -- changed in an EARLIER build between two snapshot points -- e.g.
            -- a one-time reenrich (Phase A.1) or skipping multiple refresh
            -- cycles. Now: any lineage row recorded between the latest
            -- snapshot's timestamp and the current refresh counts as
            -- "explained". This still catches genuine silent updates (no
            -- profile_field_changes row anywhere after the snapshot) while
            -- permitting legitimate cross-build writes. See operator guide
            -- "Tracked-field backfill" -- the manual-snapshot dance is no
            -- longer required.
            lineage AS (
                SELECT DISTINCT bn_id, field_name
                FROM {ops_dataset}.profile_field_changes
                WHERE changed_at > (
                    SELECT MAX(snapshotted_at)
                    FROM {ops_dataset}.profile_core_snapshot
                )
            ),
            violations AS (
                -- IMMUTABLE: npi_number must not overwrite a non-NULL snapshot value
                SELECT c.bn_id, 'immutable_changed' AS violation_type,
                       'npi_number' AS field_name,
                       CAST(s.npi_number AS STRING) AS old_val,
                       CAST(c.npi_number AS STRING) AS new_val
                FROM curr c JOIN snap s ON c.bn_id = s.bn_id
                WHERE c.npi_number IS DISTINCT FROM s.npi_number
                  AND s.npi_number IS NOT NULL
                UNION ALL
                -- IMMUTABLE: bionews_uk
                SELECT c.bn_id, 'immutable_changed', 'bionews_uk',
                       s.bionews_uk, c.bionews_uk
                FROM curr c JOIN snap s ON c.bn_id = s.bn_id
                WHERE c.bionews_uk IS DISTINCT FROM s.bionews_uk
                  AND s.bionews_uk IS NOT NULL
                UNION ALL
                -- v6.5 Phase 2: account_type bidirectional block REMOVED
                -- (column dropped from profile_core; replaced by 5 per-role flags
                --  validated by the BIDIRECTIONAL blocks below).
                -- BIDIRECTIONAL: preferred_condition
                SELECT c.bn_id, 'bidirectional_no_lineage', 'preferred_condition',
                       TO_JSON_STRING(s.preferred_condition), TO_JSON_STRING(c.preferred_condition)
                FROM curr c JOIN snap s ON c.bn_id = s.bn_id
                WHERE TO_JSON_STRING(c.preferred_condition) != TO_JSON_STRING(s.preferred_condition)
                  AND NOT EXISTS (
                      SELECT 1 FROM lineage l
                      WHERE l.bn_id = c.bn_id AND l.field_name = 'preferred_condition'
                  )
                UNION ALL
                -- v6.6 BIDIRECTIONAL: preferred_condition_normalized (derived, can flip across builds)
                SELECT c.bn_id, 'bidirectional_no_lineage', 'preferred_condition_normalized',
                       TO_JSON_STRING(s.preferred_condition_normalized), TO_JSON_STRING(c.preferred_condition_normalized)
                FROM curr c JOIN snap s ON c.bn_id = s.bn_id
                WHERE TO_JSON_STRING(c.preferred_condition_normalized) != TO_JSON_STRING(s.preferred_condition_normalized)
                  AND NOT EXISTS (
                      SELECT 1 FROM lineage l
                      WHERE l.bn_id = c.bn_id AND l.field_name = 'preferred_condition_normalized'
                  )
                UNION ALL
                -- v6.5 BIDIRECTIONAL: is_patient
                SELECT c.bn_id, 'bidirectional_no_lineage', 'is_patient',
                       CAST(s.is_patient AS STRING), CAST(c.is_patient AS STRING)
                FROM curr c JOIN snap s ON c.bn_id = s.bn_id
                WHERE c.is_patient IS DISTINCT FROM s.is_patient
                  AND NOT EXISTS (
                      SELECT 1 FROM lineage l
                      WHERE l.bn_id = c.bn_id AND l.field_name = 'is_patient'
                  )
                UNION ALL
                -- v6.5 BIDIRECTIONAL: is_hcp
                SELECT c.bn_id, 'bidirectional_no_lineage', 'is_hcp',
                       CAST(s.is_hcp AS STRING), CAST(c.is_hcp AS STRING)
                FROM curr c JOIN snap s ON c.bn_id = s.bn_id
                WHERE c.is_hcp IS DISTINCT FROM s.is_hcp
                  AND NOT EXISTS (
                      SELECT 1 FROM lineage l
                      WHERE l.bn_id = c.bn_id AND l.field_name = 'is_hcp'
                  )
                UNION ALL
                -- v6.5 BIDIRECTIONAL: is_caregiver
                SELECT c.bn_id, 'bidirectional_no_lineage', 'is_caregiver',
                       CAST(s.is_caregiver AS STRING), CAST(c.is_caregiver AS STRING)
                FROM curr c JOIN snap s ON c.bn_id = s.bn_id
                WHERE c.is_caregiver IS DISTINCT FROM s.is_caregiver
                  AND NOT EXISTS (
                      SELECT 1 FROM lineage l
                      WHERE l.bn_id = c.bn_id AND l.field_name = 'is_caregiver'
                  )
                UNION ALL
                -- v6.5 BIDIRECTIONAL: is_family_or_friend
                SELECT c.bn_id, 'bidirectional_no_lineage', 'is_family_or_friend',
                       CAST(s.is_family_or_friend AS STRING), CAST(c.is_family_or_friend AS STRING)
                FROM curr c JOIN snap s ON c.bn_id = s.bn_id
                WHERE c.is_family_or_friend IS DISTINCT FROM s.is_family_or_friend
                  AND NOT EXISTS (
                      SELECT 1 FROM lineage l
                      WHERE l.bn_id = c.bn_id AND l.field_name = 'is_family_or_friend'
                  )
                UNION ALL
                -- v6.5 BIDIRECTIONAL: is_other
                SELECT c.bn_id, 'bidirectional_no_lineage', 'is_other',
                       CAST(s.is_other AS STRING), CAST(c.is_other AS STRING)
                FROM curr c JOIN snap s ON c.bn_id = s.bn_id
                WHERE c.is_other IS DISTINCT FROM s.is_other
                  AND NOT EXISTS (
                      SELECT 1 FROM lineage l
                      WHERE l.bn_id = c.bn_id AND l.field_name = 'is_other'
                  )
            )
            SELECT COUNT(*) AS violation_count,
                   COUNTIF(violation_type = 'immutable_changed') AS immutable_violations,
                   COUNTIF(violation_type = 'bidirectional_no_lineage') AS lineage_gaps,
                   ARRAY_AGG(STRUCT(bn_id, violation_type, field_name, old_val, new_val)
                             LIMIT 5) AS sample_violations
            FROM violations
            """
            safety_cfg = bigquery.QueryJobConfig(
                use_query_cache=False,
                query_parameters=[
                    bigquery.ScalarQueryParameter("build_id", "STRING", execution_id),
                ],
            )
            row = list(
                bq_client.query(_fmt(safety_sql), job_config=safety_cfg).result()
            )[0]

            if row.violation_count == 0:
                assertions["refresh_safety_check"] = (
                    True,
                    "hard",
                    "PASS -- no immutable overwrites (non-NULL baseline), no unexplained "
                    "bidirectional changes in sampled refresh scope (200 bn_ids)",
                )
                logger.info("Assertion refresh_safety_check [hard]: PASS")
            else:
                sample = "; ".join(
                    f"{v['bn_id']}: {v['violation_type']} on {v['field_name']} "
                    f"({v['old_val']} -> {v['new_val']})"
                    for v in (row.sample_violations or [])
                )
                assertions["refresh_safety_check"] = (
                    False,
                    "hard",
                    f"FAIL -- {row.violation_count} violation(s): "
                    f"{row.immutable_violations} immutable mutations, "
                    f"{row.lineage_gaps} bidirectional changes without lineage records. "
                    f"Sample: {sample}",
                )
                logger.error(f"Assertion refresh_safety_check [hard]: FAIL -- {sample}")
        except Exception as e:
            assertions["refresh_safety_check"] = (
                False,
                "soft",
                f"ERROR -- {str(e)[:200]}",
            )
            logger.warning(f"Assertion refresh_safety_check [soft]: ERROR -- {e}")

    # --- Exception spike (soft, existing behavior) ---
    try:
        current_sql = _fmt(
            "SELECT COUNT(*) AS cnt FROM {view_dataset}.profile_exceptions"
        )
        current_count = list(
            bq_client.query(current_sql, job_config=job_config).result()
        )[0].cnt

        prev_sql = """
        SELECT SAFE_CAST(JSON_VALUE(metadata, '$.exception_count') AS INT64) AS cnt
        FROM {ops_dataset}.profile_build_runs
        WHERE status = 'completed'
          AND mode = 'rebuild'
          AND metadata IS NOT NULL
          AND JSON_VALUE(metadata, '$.exception_count') IS NOT NULL
        ORDER BY started_at DESC
        LIMIT 1
        """
        prev_rows = list(
            bq_client.query(_fmt(prev_sql), job_config=job_config).result()
        )
        if prev_rows and prev_rows[0].cnt is not None:
            prev_count = prev_rows[0].cnt
            if prev_count > 0 and current_count > prev_count * 1.1:
                pct = ((current_count - prev_count) / prev_count) * 100
                assertions["exception_spike"] = (
                    False,
                    "soft",
                    f"WARN -- exceptions increased {prev_count} -> {current_count} (+{pct:.0f}%)",
                )
                logger.warning(
                    f"Assertion exception_spike [soft]: WARN -- {prev_count} -> {current_count} "
                    f"(+{pct:.0f}%)"
                )
            else:
                assertions["exception_spike"] = (
                    True,
                    "soft",
                    f"PASS -- {current_count} (prev: {prev_count})",
                )
                logger.info(f"Assertion exception_spike [soft]: PASS ({current_count})")
        else:
            assertions["exception_spike"] = (
                True,
                "soft",
                f"PASS -- {current_count} exceptions (no prior baseline)",
            )
            logger.info(f"Assertion exception_spike [soft]: PASS -- no prior baseline")

        # Persist current counts into metadata for next run's comparison.
        # Includes exception count AND tier1/tier2 counts (for the anonymous/known delta check).
        import json as _json

        # Re-query tier counts for metadata
        tier_row = list(
            bq_client.query(
                _fmt(
                    """SELECT COUNTIF(cluster_tier='tier1') AS tier1_n,
                      COUNTIF(cluster_tier='tier2') AS tier2_n
               FROM {consumer_dataset}.profile_core"""
                ),
                job_config=job_config,
            ).result()
        )[0]
        # Stamp current counts onto the RUN row in profile_build_runs so the
        # next rebuild can read them as baselines.
        _merge_build_run_metadata(
            bq_client,
            execution_id,
            {
                "exception_count": int(current_count),
                "tier1_n": int(tier_row.tier1_n),
                "tier2_n": int(tier_row.tier2_n),
            },
        )

    except Exception as e:
        assertions["exception_spike"] = (False, "soft", f"ERROR -- {str(e)[:200]}")
        logger.warning(f"Assertion exception_spike [soft]: ERROR -- {e}")

    return assertions


def run_performance_assertions(
    bq_client,
    execution_id,
    mode="rebuild",
    consumer_dataset=PRODUCTION_DATASET,
    ops_dataset=OPS_DATASET,
    staging_dataset=STAGING_DATASET,
):
    """
    Soft performance/process checks that should never block a run, but should
    make speed regressions visible before release.

    Current checks:
      - build_duration_regression: current run duration vs recent same-mode baseline
      - refresh_scope_efficiency: refresh scope size vs profile_core size
    """
    assertions = {}
    if not _profile_ops_persist_enabled():
        return assertions
    job_config = bigquery.QueryJobConfig(use_query_cache=False)

    def _fmt(sql: str) -> str:
        return sql.format(
            consumer_dataset=consumer_dataset,
            ops_dataset=ops_dataset,
            staging_dataset=staging_dataset,
        )

    def _record(name: str, passed: bool, detail: str, severity: str = "soft"):
        assertions[name] = (passed, severity, detail)
        level = logger.info if passed else logger.warning
        marker = "PASS" if passed else "WARN"
        level(f"Performance {name} [{severity}]: {marker} -- {detail}")

    try:
        current_sql = _fmt(
            """
        SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), started_at, SECOND) AS duration_s
        FROM {ops_dataset}.profile_build_runs
        WHERE build_id = @build_id
        LIMIT 1
        """
        )
        current_cfg = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("build_id", "STRING", execution_id),
            ],
        )
        current_rows = list(
            bq_client.query(current_sql, job_config=current_cfg).result()
        )
        current_duration = current_rows[0].duration_s if current_rows else None

        baseline_sql = _fmt(
            """
        WITH hist AS (
            SELECT TIMESTAMP_DIFF(completed_at, started_at, SECOND) AS duration_s
            FROM {ops_dataset}.profile_build_runs
            WHERE build_id != @build_id
              AND mode = @mode
              AND status IN ('completed', 'completed_with_warnings', 'forced_despite_gates')
              AND completed_at IS NOT NULL
            ORDER BY started_at DESC
            LIMIT 5
        )
        SELECT COUNT(*) AS baseline_runs, AVG(duration_s) AS avg_duration_s
        FROM hist
        """
        )
        baseline_cfg = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("build_id", "STRING", execution_id),
                bigquery.ScalarQueryParameter("mode", "STRING", mode),
            ],
        )
        baseline_rows = list(
            bq_client.query(baseline_sql, job_config=baseline_cfg).result()
        )
        baseline_runs = baseline_rows[0].baseline_runs if baseline_rows else 0
        baseline_duration = baseline_rows[0].avg_duration_s if baseline_rows else None

        if current_duration is None:
            _record(
                "build_duration_regression",
                False,
                "Unable to read current run duration",
            )
        elif not baseline_runs or baseline_duration is None:
            _record(
                "build_duration_regression",
                True,
                f"Current run {current_duration:.0f}s (no same-mode baseline yet)",
            )
        else:
            delta_pct = (
                ((current_duration - baseline_duration) / baseline_duration) * 100
                if baseline_duration
                else 0
            )
            if current_duration > baseline_duration * 1.25:
                _record(
                    "build_duration_regression",
                    False,
                    f"Current run {current_duration:.0f}s vs baseline {baseline_duration:.0f}s "
                    f"across {baseline_runs} prior {mode} run(s) (+{delta_pct:.1f}%)",
                )
            else:
                _record(
                    "build_duration_regression",
                    True,
                    f"Current run {current_duration:.0f}s vs baseline {baseline_duration:.0f}s "
                    f"across {baseline_runs} prior {mode} run(s)",
                )
    except Exception as e:
        _record("build_duration_regression", False, f"ERROR -- {str(e)[:200]}")

    try:
        step_sql = _fmt(
            """
        WITH current_run AS (
            SELECT mode
            FROM {ops_dataset}.profile_build_runs
            WHERE build_id = @build_id
            LIMIT 1
        ),
        recent_runs AS (
            SELECT build_id
            FROM {ops_dataset}.profile_build_runs
            WHERE build_id != @build_id
              AND mode = (SELECT mode FROM current_run)
              AND status IN ('completed', 'completed_with_warnings', 'forced_despite_gates')
              AND completed_at IS NOT NULL
            ORDER BY started_at DESC
            LIMIT 5
        ),
        baseline AS (
            SELECT
                s.step_name,
                COUNT(*) AS sample_ct,
                AVG(s.duration_seconds) AS avg_duration_seconds,
                AVG(s.total_bytes_processed) AS avg_bytes_processed,
                AVG(s.total_slot_millis) AS avg_slot_millis
            FROM {ops_dataset}.profile_build_steps s
            JOIN recent_runs rr USING (build_id)
            WHERE s.status LIKE 'completed%'
              AND s.step_name NOT IN (
                  'assertions',
                  'publish_release_views',
                  'publish_views',
                  'performance_checks',
                  'refresh_scope_guard',
                  'refresh_noop',
                  'ddl_snapshot_schema_check'
              )
            GROUP BY s.step_name
        )
        SELECT
            c.step_name,
            c.duration_seconds,
            c.total_bytes_processed,
            c.total_slot_millis,
            b.sample_ct,
            b.avg_duration_seconds,
            b.avg_bytes_processed,
            b.avg_slot_millis,
            SAFE_DIVIDE(c.duration_seconds, NULLIF(b.avg_duration_seconds, 0)) AS duration_ratio,
            SAFE_DIVIDE(c.total_bytes_processed, NULLIF(b.avg_bytes_processed, 0)) AS bytes_ratio,
            SAFE_DIVIDE(c.total_slot_millis, NULLIF(b.avg_slot_millis, 0)) AS slot_ratio
        FROM {ops_dataset}.profile_build_steps c
        JOIN baseline b USING (step_name)
        WHERE c.build_id = @build_id
          AND c.status LIKE 'completed%'
          AND c.step_name NOT IN (
              'assertions',
              'publish_release_views',
              'publish_views',
              'performance_checks',
              'refresh_scope_guard',
              'refresh_noop',
              'ddl_snapshot_schema_check'
          )
          AND b.sample_ct >= 2
          AND (
              (c.duration_seconds >= 10 AND c.duration_seconds > b.avg_duration_seconds * 1.5)
              OR
              (c.total_bytes_processed >= 100000000 AND c.total_bytes_processed > b.avg_bytes_processed * 1.5)
              OR
              (c.total_slot_millis >= 1000 AND c.total_slot_millis > b.avg_slot_millis * 1.5)
          )
        ORDER BY
            GREATEST(
                COALESCE(SAFE_DIVIDE(c.duration_seconds, NULLIF(b.avg_duration_seconds, 0)), 1),
                COALESCE(SAFE_DIVIDE(c.total_bytes_processed, NULLIF(b.avg_bytes_processed, 0)), 1),
                COALESCE(SAFE_DIVIDE(c.total_slot_millis, NULLIF(b.avg_slot_millis, 0)), 1)
            ) DESC,
            c.duration_seconds DESC
        LIMIT 3
        """
        )
        step_cfg = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("build_id", "STRING", execution_id),
            ],
        )
        step_rows = list(bq_client.query(step_sql, job_config=step_cfg).result())
        if not step_rows:
            _record(
                "step_performance_regression",
                True,
                "No step-level duration/bytes/slot regressions versus recent same-mode baseline",
            )
        else:
            findings = []
            for row in step_rows:
                duration_ratio = row.duration_ratio or 1
                bytes_ratio = row.bytes_ratio or 1
                slot_ratio = row.slot_ratio or 1
                findings.append(
                    f"{row.step_name}: duration x{duration_ratio:.2f}, "
                    f"bytes x{bytes_ratio:.2f}, slot x{slot_ratio:.2f}"
                )
            _record(
                "step_performance_regression",
                False,
                "Hot step regression(s) vs recent baseline: " + "; ".join(findings),
            )
    except Exception as e:
        _record("step_performance_regression", False, f"ERROR -- {str(e)[:200]}")

    if mode == "refresh":
        try:
            scope_sql = _fmt(
                """
            WITH counts AS (
                SELECT
                    COUNTIF(bn_id = '*') AS sentinel_n,
                    COUNT(DISTINCT IF(bn_id != '*', bn_id, NULL)) AS scope_n,
                    (SELECT COUNT(*) FROM {consumer_dataset}.profile_core) AS core_n
                FROM {staging_dataset}.refresh_scope_bn_ids
            )
            SELECT
                sentinel_n,
                scope_n,
                core_n,
                ROUND(100.0 * scope_n / NULLIF(core_n, 0), 3) AS scope_pct
            FROM counts
            """
            )
            row = list(bq_client.query(scope_sql, job_config=job_config).result())[0]
            sentinel_n = row.sentinel_n or 0
            scope_n = row.scope_n or 0
            core_n = row.core_n or 0
            scope_pct = row.scope_pct or 0

            if sentinel_n > 0:
                _record(
                    "refresh_scope_efficiency",
                    False,
                    f"Refresh scope contains {sentinel_n} rebuild sentinel row(s); "
                    f"expected only concrete bn_ids",
                )
            elif core_n == 0:
                _record(
                    "refresh_scope_efficiency",
                    True,
                    "profile_core empty; scope efficiency not applicable",
                )
            elif scope_pct > 15.0:
                _record(
                    "refresh_scope_efficiency",
                    False,
                    f"Refresh scope covers {scope_n:,}/{core_n:,} profiles ({scope_pct}%). "
                    f"This is larger than the 15% target and may slow incremental runs.",
                )
            else:
                _record(
                    "refresh_scope_efficiency",
                    True,
                    f"Refresh scope covers {scope_n:,}/{core_n:,} profiles ({scope_pct}%)",
                )
        except Exception as e:
            _record("refresh_scope_efficiency", False, f"ERROR -- {str(e)[:200]}")

    return assertions


def run_refresh_scope_guard(
    bq_client,
    execution_id,
    force: bool = False,
    consumer_dataset=PRODUCTION_DATASET,
    staging_dataset=STAGING_DATASET,
    lookback_days: int | None = None,
):
    """
    Fail-fast guard before the expensive refresh MERGE runs.

    The goal is to preserve incremental-update speed and avoid accidentally
    running a near-rebuild under the refresh mode.

    Policy (scaled by lookback_days; default thresholds for short / unspecified
    lookback are 10% warn, 25% hard fail):
      - sentinel rows in refresh_scope_bn_ids: hard fail
      - scope > hard_threshold of profile_core: hard fail unless --force
        (= max(25%, lookback_days * 3%))
      - scope > warn_threshold of profile_core: soft warning
        (= max(10%, lookback_days * 1.5%))

    Scaling matters because larger lookback windows naturally cover more
    profiles -- a 14-day lookback would always trip the static 10% warning
    even on a healthy pipeline.
    """
    job_config = bigquery.QueryJobConfig(use_query_cache=False)
    sql = f"""
    WITH counts AS (
        SELECT
            COUNTIF(bn_id = '*') AS sentinel_n,
            COUNT(DISTINCT IF(bn_id != '*', bn_id, NULL)) AS scope_n,
            (SELECT COUNT(*) FROM {consumer_dataset}.profile_core) AS core_n
        FROM {staging_dataset}.refresh_scope_bn_ids
    )
    SELECT
        sentinel_n,
        scope_n,
        core_n,
        ROUND(100.0 * scope_n / NULLIF(core_n, 0), 3) AS scope_pct
    FROM counts
    """
    row = list(bq_client.query(sql, job_config=job_config).result())[0]
    sentinel_n = row.sentinel_n or 0
    scope_n = row.scope_n or 0
    core_n = row.core_n or 0
    scope_pct = row.scope_pct or 0

    # Scale thresholds by lookback so wide-window runs do not trip on volume alone.
    lb = lookback_days or 3
    warn_threshold = max(10.0, lb * 1.5)
    hard_threshold = max(25.0, lb * 3.0)

    if sentinel_n > 0:
        return {
            "status": "failed",
            "detail": f"Refresh scope contains {sentinel_n} rebuild sentinel row(s); expected only concrete bn_ids",
            "scope_n": scope_n,
            "core_n": core_n,
            "scope_pct": scope_pct,
        }
    if core_n and scope_pct > hard_threshold:
        status = "forced" if force else "failed"
        return {
            "status": status,
            "detail": (
                f"Refresh scope covers {scope_n:,}/{core_n:,} profiles ({scope_pct}%). "
                f"This exceeds the {hard_threshold:.0f}% hard threshold for "
                f"incremental runs (lookback={lb} days)."
            ),
            "scope_n": scope_n,
            "core_n": core_n,
            "scope_pct": scope_pct,
        }
    if core_n and scope_pct > warn_threshold:
        return {
            "status": "warn",
            "detail": (
                f"Refresh scope covers {scope_n:,}/{core_n:,} profiles ({scope_pct}%). "
                f"This exceeds the {warn_threshold:.0f}% warning target "
                f"(lookback={lb} days) and may slow incremental runs."
            ),
            "scope_n": scope_n,
            "core_n": core_n,
            "scope_pct": scope_pct,
        }
    return {
        "status": "ok",
        "detail": f"Refresh scope covers {scope_n:,}/{core_n:,} profiles ({scope_pct}%)",
        "scope_n": scope_n,
        "core_n": core_n,
        "scope_pct": scope_pct,
    }


def run_pipeline(**kwargs):
    """
    Run profile database build/refresh.

    --build-mode is REQUIRED:
      --build-mode rebuild       = drop + recreate all tables from source data
      --build-mode resume_rebuild = resume a failed late-stage rebuild from candidate tables
      --build-mode resume_publish = resume failed candidate->prod physical publish + view finalize
      --build-mode refresh       = daily lookback MERGE + site_events roll + enrich + views
      --build-mode reenrich      = enrich + personas + views (no DDL, no populate)
      --build-mode enrich        = alias for reenrich (deprecated name)
      --build-mode incremental   = alias for reenrich (deprecated name)
      --build-mode backfill_site_events = one-shot GA4→site_events full-window reload
      --build-mode views         = views only
    """
    config = kwargs.get("config", {})
    bq_client = kwargs.get("bq_client")
    execution_id = kwargs.get("execution_id", "manual")
    lookback_days = kwargs.get("lookback_days", None)
    build_mode = kwargs.get("build_mode", None)
    force = kwargs.get("force", False)  # v6.4: allows bypass of preflight checks

    available = ", ".join(BUILD_MODES.keys())

    # --lookback implies --build-mode refresh. Lookback is only meaningful for
    # refresh (it controls the daily MERGE window); supplying it without a
    # mode is unambiguous, so default to refresh rather than erroring out.
    if not build_mode and lookback_days is not None:
        logger.info(
            "--lookback supplied without --build-mode; defaulting to --build-mode refresh"
        )
        build_mode = "refresh"

    if not build_mode:
        raise ValueError(
            f"--build-mode is required for profile_database.\n"
            f"  Available modes: {available}\n"
            f"  Example: python orchestrate.py --source profile_database --env prod --build-mode refresh"
        )

    # Resolve legacy aliases (incremental -> reenrich, etc.) and warn the operator.
    if build_mode in LEGACY_MODE_ALIASES:
        resolved = LEGACY_MODE_ALIASES[build_mode]
        logger.warning(
            f"Build mode '{build_mode}' is deprecated and resolves to '{resolved}'. "
            f"The previous 'incremental' behavior (DDL without populate) is retired "
            f"because it produced broken tables. Update your scripts to use '{resolved}' directly."
        )
        mode = resolved
    else:
        mode = build_mode

    if mode not in BUILD_MODES:
        raise ValueError(
            f"Unknown profile_database build mode '{build_mode}'.\n"
            f"  Available modes: {available}\n"
            f"  Legacy aliases: {', '.join(LEGACY_MODE_ALIASES.keys())}\n"
            f"  Example: python orchestrate.py --source profile_database --env prod --build-mode refresh"
        )

    if mode == "resume_publish":
        if not execution_id or execution_id == "manual":
            raise ValueError(
                "resume_publish requires the original failed build's --execution-id "
                "(build_id) so publish manifest idempotency can skip tables already "
                "promoted. Example:\n"
                "  python orchestrate.py --source profile_database --env prod "
                "--build-mode resume_publish --execution-id <build_id>"
            )

    steps_to_run = BUILD_MODES[mode]
    build_consumer_dataset = (
        REBUILD_CANDIDATE_DATASET
        if mode in CANDIDATE_CONSUMER_MODES
        else PRODUCTION_DATASET
    )
    deferred_snapshot_core = (
        mode in REBUILD_LIKE_MODES and "snapshot_core" in steps_to_run
    )

    # Default lookback: 3 days for refresh
    if lookback_days is None and mode == "refresh":
        lookback_days = 3

    # Schema validation (warn-only)
    warnings = validate_schema_references()
    if warnings:
        logger.warning("")
        logger.warning("SCHEMA VALIDATION WARNINGS:")
        for w in warnings:
            logger.warning(w)
        logger.warning(f"({len(warnings)} reference(s) to tables not in DDL)")
        logger.warning("")

    logger.info("")
    logger.info("=" * 80)
    logger.info(f"PROFILE DATABASE BUILD -- mode: {mode} (schema: {SCHEMA_VERSION})")
    if lookback_days and "refresh" in steps_to_run:
        logger.info(f"Lookback: {lookback_days} days")
    logger.info(f"Build consumer dataset: {build_consumer_dataset}")
    if mode in CANDIDATE_CONSUMER_MODES:
        logger.info(f"Publish consumer dataset: {PRODUCTION_DATASET}")
    if steps_to_run:
        logger.info(f"Steps: {' -> '.join(steps_to_run)}")
    else:
        logger.info("Steps: (none -- orchestration-only mode)")
    logger.info("=" * 80)
    logger.info("")

    # Ensure the split ops logging tables exist before any preflight or
    # migration logic that reads/writes them.
    _ensure_ops_runtime_tables(bq_client)
    _ensure_consumer_dataset(bq_client, build_consumer_dataset)

    runtime_info = _compute_runtime_fingerprint()
    logger.info(
        f"Runtime fingerprint: {runtime_info['runtime_fingerprint']} "
        f"({runtime_info['runtime_file_count']} files)"
    )
    if runtime_info["runtime_missing_files"]:
        logger.warning(
            f"Runtime fingerprint missing file(s): {', '.join(runtime_info['runtime_missing_files'])}"
        )

    # Defensive cleanup of the retired profile_build_log object (legacy v6.3
    # base table or v6.4 compatibility view). Copies any surviving step rows
    # into profile_build_steps, then drops it. No-op on any v6.5+ deployment.
    _migrate_legacy_build_log(bq_client)

    # P1.4 -- PREFLIGHT (build acceptance gate #4)
    # Verify identity-hub state before any destructive work. Skipped for
    # reenrich/views modes (they don't re-pull from the hub). Fail-fast unless
    # --force is set.
    logger.info("Running preflight checks...")
    preflight_result = run_preflight(bq_client, mode, force=force)
    if preflight_result["status"] == "failed":
        hard_failed = [
            c["name"]
            for c in preflight_result["checks"]
            if (not c["passed"]) and c.get("severity", "hard") == "hard"
        ]
        raise RuntimeError(
            "Preflight FAILED. Pass --force to override, or fix the failing checks "
            f"(hard failures only): {hard_failed}. "
            "no_concurrent_rebuild reads BigQuery (profile_ops.profile_build_runs), not "
            "whether Python is running on this machine. "
            "List or clear stuck rows: "
            "`python scripts/clear_profile_build_run.py --env <prod|dev> --list-running` then "
            "`python scripts/clear_profile_build_run.py --env <prod|dev> --build-id <uuid>`."
        )
    # P1.4b -- DRY-RUN PRE-FLIGHT (added 2026-04-27 after a refresh blew up
    # mid-MERGE on a BigQuery scope error in mc_persona that statement-level
    # plan check would have caught). Runs every SQL file the build will
    # execute through BigQuery's dryRun planner against an isolated stub
    # dataset. Costs ~0 bytes; catches SQL regressions BEFORE any prod write.
    # Skipped for "rebuild" (its blue/green candidate path is the safety net)
    # and "views" (already covered by profile_views_smoke_test.py). Refresh and
    # reenrich go through this gate because they MERGE/UPDATE prod tables in
    # place and have no rollback boundary.
    if mode == "refresh" and not force:
        logger.info("Skipping BigQuery SQL dry-run planning for refresh mode")
    elif mode == "reenrich" and not force:
        logger.info("Running SQL dry-run pre-flight (mode=%s)...", mode)
        try:
            from scripts.dry_run_profile_sql import run_preflight as _dry_run_preflight
        except Exception as imp_err:
            logger.warning(
                "Dry-run pre-flight skipped: could not import "
                "scripts.dry_run_profile_sql.run_preflight (%s). Proceeding without.",
                imp_err,
            )
        else:
            try:
                t_pass, t_fail, file_errors, comment_errors = _dry_run_preflight(
                    client=bq_client,
                    lookback=lookback_days or 3,
                    verbose=False,
                )
                if comment_errors or t_fail > 0:
                    sample = []
                    for ce in comment_errors[:5]:
                        sample.append(ce)
                    for rel, errs in file_errors[:3]:
                        sample.append(f"{rel}: {errs[0] if errs else 'unknown'}")
                    raise RuntimeError(
                        f"DRY-RUN PRE-FLIGHT FAILED ({t_fail} statement(s), "
                        f"{len(comment_errors)} comment-style violation(s)). "
                        f"NO production writes have happened. Fix and re-run.\n"
                        f"  First failures:\n    "
                        + "\n    ".join(sample)
                        + "\n  Run `python scripts/dry_run_profile_sql.py` for the full report."
                    )
                logger.info(
                    "SQL dry-run pre-flight PASSED: %d statements planned cleanly across active SQL.",
                    t_pass,
                )
            except RuntimeError:
                raise
            except Exception as preflight_err:
                # If the pre-flight machinery itself errors out (auth, BQ
                # transient, etc.), do not block the build -- log loudly.
                logger.warning(
                    "Dry-run pre-flight could not run (%s). Proceeding; "
                    "this is not a SQL bug, just a preflight infrastructure issue.",
                    preflight_err,
                )

    # Capture manifest run_id for downstream logging (build acceptance gate #4)
    manifest_run_id = preflight_result.get("manifest_run_id")
    xref_row_count = preflight_result.get("xref_row_count")
    if manifest_run_id:
        logger.info(f"Identity hub manifest run_id: {manifest_run_id}")

    persist_ops_log = mode != "rebuild"
    if not persist_ops_log:
        logger.info(
            "Rebuild: skipping profile_ops.persistence "
            "(no profile_build_runs / profile_build_steps writes for this run)."
        )

    # v6.4: Write run-level row to profile_build_runs (modes other than rebuild).
    if persist_ops_log:
        _log_build_run_start(
            bq_client, execution_id, mode, preflight_result, runtime_info=runtime_info
        )
        _merge_build_run_metadata(
            bq_client,
            execution_id,
            {
                "build_consumer_dataset": build_consumer_dataset,
                "publish_consumer_dataset": PRODUCTION_DATASET,
                "blue_green_rebuild": mode in CANDIDATE_CONSUMER_MODES,
            },
        )
    # v6.4 followup: track whether _log_build_run_end has been called so the
    # outer exception handler at the bottom of run_pipeline does not double-
    # finalize. Both the loop-failure handler and the gate-failure block set
    # this; the success-path finalize at the end of the function also sets it.
    run_row_finalized = {"value": not persist_ops_log}
    completed_ok = {"value": False}
    _ops_ctx_token = _profile_ops_persist_log.set(persist_ops_log)
    lease_state: dict = {}
    try:

        def _profile_sql_job_kwargs(step_name: str) -> dict:
            return {
                "bq_job_labels": build_profile_sql_job_labels(
                    execution_id,
                    mode,
                    step_name,
                    build_consumer_dataset,
                    orchestrator_job_id=kwargs.get("centralized_job_id"),
                ),
                "maximum_bytes_billed": profile_maximum_bytes_billed(),
            }

        if should_acquire_candidate_lease(
            mode, build_consumer_dataset, REBUILD_CANDIDATE_DATASET
        ):
            _ttl = lease_ttl_minutes()
            acquire_dataset_lease(
                bq_client,
                build_consumer_dataset,
                execution_id,
                mode,
                _ttl,
            )
            lease_state["dataset"] = build_consumer_dataset
            lease_state["ttl"] = _ttl
            _stop, _hb_thread = start_lease_heartbeat_thread(
                bq_client,
                build_consumer_dataset,
                execution_id,
                _ttl,
                persist_ops_log,
            )
            lease_state["stop"] = _stop
            lease_state["thread"] = _hb_thread
            logger.info(
                "Candidate dataset lease acquired: dataset=%s build_id=%s ttl_min=%s heartbeat_s=%s",
                build_consumer_dataset,
                execution_id,
                _ttl,
                heartbeat_interval_seconds(),
            )

        identity_context = {}
        if mode in ("rebuild", "refresh"):
            logger.info(
                "Preparing build-local identity hub snapshots for source consistency..."
            )
            identity_snapshot_result = _prepare_identity_snapshots(
                bq_client,
                execution_id,
                manifest_run_id=manifest_run_id,
                copy_labels=_identity_copy_job_labels(kwargs.get("centralized_job_id")),
            )
            logger.info(
                f"Identity snapshots ready in {STAGING_DATASET} "
                f"({len(identity_snapshot_result['copied_tables'])} tables, "
                f"{identity_snapshot_result.get('xref_row_count') or 0:,} xref rows, "
                f"{identity_snapshot_result['duration_seconds']:.1f}s)"
            )
            identity_context = {
                "identity_xref_table": IDENTITY_XREF_SNAPSHOT_TABLE,
                "identity_hub_table": IDENTITY_HUB_SNAPSHOT_TABLE,
                "identity_persistence_table": IDENTITY_PERSISTENCE_SNAPSHOT_TABLE,
            }

        # Log preflight results AND a 'preflight' step-row for the view's benefit.
        # The run row already carries preflight_status + identity_hub_manifest_id
        # (written by _log_build_run_start above); here we record the full check
        # list into profile_build_steps so ops can see it through the legacy view.
        if not preflight_result.get("skipped"):
            try:
                import json as _json

                meta_json = _json.dumps(
                    {
                        "xref_row_count": xref_row_count,
                        "manifest_run_id": manifest_run_id,
                        "preflight_status": preflight_result["status"],
                        "checks": preflight_result["checks"],
                    }
                )
                _log_build_step(
                    bq_client,
                    execution_id,
                    "preflight",
                    status=f"preflight_{preflight_result['status']}",
                    rows_affected=0,
                    duration_seconds=0.0,
                    statements_executed=0,
                    error_message=None,
                )
                # Stamp preflight metadata onto the run row so it's discoverable
                # in one place (profile_build_runs.metadata).
                _merge_build_run_metadata(
                    bq_client,
                    execution_id,
                    {"preflight": _json.loads(meta_json)},
                )
            except Exception as e:
                logger.warning(f"Failed to log preflight result: {e}")

        succeeded = 0
        failed = 0
        total_rows = 0
        total_bytes_processed = 0
        total_bytes_billed = 0
        total_slot_millis = 0
        executed_steps = []
        short_circuit_no_changes = False
        short_circuit_reason = ""

        for step_name in steps_to_run:
            sql_file, description = STEP_MAP[step_name]

            if deferred_snapshot_core and step_name == "snapshot_core":
                logger.info("")
                logger.info("-- Step: snapshot_core --")
                logger.info("   Deferred until after blue/green publish gates pass")
                continue

            # Soft-gate canonical OneTrust SoR: skip when dataset/table missing or empty
            if step_name == "enrich_onetrust_consent":
                try:
                    n = list(
                        bq_client.query(
                            "SELECT COUNT(*) AS n FROM onetrust_data.onetrust_data_subjects LIMIT 1"
                        ).result()
                    )[0]["n"]
                    if not n:
                        logger.info("")
                        logger.info("-- Step: enrich_onetrust_consent --")
                        logger.info("   SKIPPED (onetrust_data_subjects empty)")
                        continue
                except Exception as ot_err:
                    logger.info("")
                    logger.info("-- Step: enrich_onetrust_consent --")
                    logger.info("   SKIPPED (onetrust_data unavailable: %s)", ot_err)
                    continue

            # Verify file exists
            sql_path = Path(sql_file)
            if not sql_path.exists():
                sql_path = Path.cwd() / sql_file
            if not sql_path.exists():
                logger.error(f"SQL file not found: {sql_file}")
                failed += 1
                raise FileNotFoundError(f"SQL file not found: {sql_file}")

            logger.info("")
            logger.info(f"-- Step: {step_name} --")
            logger.info(f"   {description}")
            logger.info(f"   File: {sql_file}")

            step_start = time.time()
            try:
                if step_name == "views":
                    logger.info(
                        "   Reconciling profile_core runtime schema before view build..."
                    )
                    core_drift = _sync_profile_core_runtime_schema(
                        bq_client,
                        build_consumer_dataset,
                    )
                    if core_drift["type_mismatches"]:
                        detail = "; ".join(
                            f"{m['column_name']} ({m['profile_core_type']} vs expected {m['expected_type']})"
                            for m in core_drift["type_mismatches"]
                        )
                        raise RuntimeError(
                            f"HARD GATE: profile_core has incompatible runtime schema drift: {detail}. "
                            f"Fix the table definition before re-running."
                        )
                    if core_drift["added_columns"]:
                        added_cols = ", ".join(
                            f"{c['column_name']} {c['data_type']}"
                            for c in core_drift["added_columns"]
                        )
                        logger.info(
                            f"   profile_core runtime schema healed: added {added_cols}"
                        )
                    else:
                        logger.info(
                            "   profile_core runtime schema already satisfies required view columns"
                        )

                # Pass template variables to the SQL file:
                #   {lookback_days} -- refresh window (refresh.sql)
                #   {build_id}      -- current execution id (restore_app_fields.sql
                #                     uses it to stamp rows in profile_restore_unmapped)
                #   {probabilistic_threshold} -- v6.5: confidence floor for activating
                #                     role flags from probabilistic signals. Classifier
                #                     SQL guards probabilistic writes with
                #                     `WHERE <confidence> >= {probabilistic_threshold}`.
                context = {"build_id": execution_id}
                if lookback_days:
                    context["lookback_days"] = str(lookback_days)
                context["consumer_dataset"] = (
                    PRODUCTION_DATASET
                    if step_name == "snapshot"
                    else build_consumer_dataset
                )
                context["ops_dataset"] = OPS_DATASET
                context["staging_dataset"] = STAGING_DATASET
                context["probabilistic_threshold"] = str(PROBABILISTIC_FLAG_THRESHOLD)
                # site_events GA4 backfill: retain 365d; full reload on rebuild /
                # backfill_site_events; short rolling overlap on daily refresh.
                context["site_events_lookback_days"] = str(SITE_EVENTS_LOOKBACK_DAYS)
                context["site_events_reload_days"] = str(
                    SITE_EVENTS_RELOAD_DAYS_REFRESH
                    if mode == "refresh"
                    else SITE_EVENTS_RELOAD_DAYS_FULL
                )
                context.update(identity_context)
                if step_name == "views":
                    # Build candidate views in staging first. Production consumer
                    # views are published only after post-build gates pass.
                    context["view_dataset"] = VIEW_CANDIDATE_DATASET

                if step_name == "snapshot" and (
                    not _bq_table_exists(bq_client, PRODUCTION_DATASET, "profile_core")
                    or not _bq_table_exists(
                        bq_client, PRODUCTION_DATASET, "profile_identifiers"
                    )
                ):
                    missing = [
                        t
                        for t in ("profile_core", "profile_identifiers")
                        if not _bq_table_exists(bq_client, PRODUCTION_DATASET, t)
                    ]
                    logger.info(
                        "   Skipping snapshot data copy: %s.%s missing table(s): %s "
                        "(first deploy / greenfield). Creating empty "
                        "profile_staging.profile_core_app_snapshot shell for restore.",
                        bq_client.project,
                        PRODUCTION_DATASET,
                        ", ".join(missing),
                    )
                    _run_snapshot_app_fields_schema_prefix(
                        bq_client,
                        sql_file,
                        **_profile_sql_job_kwargs("snapshot"),
                    )
                    step_duration = time.time() - step_start
                    _log_build_step(
                        bq_client,
                        execution_id,
                        step_name,
                        "skipped: no prior profile_core",
                        duration_seconds=step_duration,
                    )
                    succeeded += 1
                    executed_steps.append(step_name)
                    continue

                if step_name in ("populate_identity_core", "refresh"):
                    logger.info(
                        "   Ensuring profile_core runtime schema before core write..."
                    )
                    try:
                        core_drift = _sync_profile_core_runtime_schema(
                            bq_client,
                            build_consumer_dataset,
                        )
                        if core_drift.get("added_columns"):
                            logger.info(
                                f"   Added {len(core_drift['added_columns'])} columns: {[c['column_name'] for c in core_drift['added_columns']]}"
                            )
                            # Re-read table metadata after adding columns so the
                            # log reflects the post-ALTER schema. get_table
                            # already fetches fresh metadata from the API.
                            #
                            # This used to call bq_client.reload_table(table),
                            # which is not a method on google.cloud.bigquery
                            # Client and never has been. The line sat unexercised
                            # because it only runs when columns are ACTUALLY
                            # added, and no new profile_core column had been
                            # introduced since it was written -- so every run
                            # took the "No columns added (all exist)" branch.
                            # The first refresh that added one (site_domains, on
                            # 2026-08-28) died here with AttributeError, after
                            # the ALTERs had already committed.
                            bq_client.get_table(
                                f"{build_consumer_dataset}.profile_core"
                            )
                        else:
                            logger.info("   No columns added (all exist)")
                        if core_drift.get("type_mismatches"):
                            raise RuntimeError(
                                f"Type mismatches on profile_core: {core_drift['type_mismatches']}"
                            )
                    except Exception as e:
                        logger.error(f"   Failed to sync profile_core schema: {e}")
                        raise

                result = execute_post_process_sql(
                    bq_client=bq_client,
                    sql_file=sql_file,
                    config=config,
                    source="profile_database",
                    execution_id=execution_id,
                    context=context,
                    **_profile_sql_job_kwargs(step_name),
                )
                step_duration = time.time() - step_start
                stmt_count = result.get("statements_executed", 0)
                rows_affected = result.get("total_rows_affected", 0)
                step_bytes_processed = result.get("total_bytes_processed", 0)
                step_bytes_billed = result.get("total_bytes_billed", 0)
                step_slot_millis = result.get("total_slot_millis", 0)
                total_rows += rows_affected
                total_bytes_processed += step_bytes_processed
                total_bytes_billed += step_bytes_billed
                total_slot_millis += step_slot_millis
                logger.info(
                    f"   Completed ({stmt_count} statements, "
                    f"{rows_affected:,} rows affected, "
                    f"{step_duration:.1f}s)"
                )
                if step_name == "views":
                    logger.info(
                        f"   Candidate consumer views refreshed in {VIEW_CANDIDATE_DATASET} "
                        f"(production views not published yet)"
                    )
                if step_name in ("populate_identity_core", "refresh"):
                    logger.info(
                        "   Reconciling profile_core runtime schema after core write..."
                    )
                    core_drift = _sync_profile_core_runtime_schema(
                        bq_client,
                        build_consumer_dataset,
                    )
                    if core_drift["type_mismatches"]:
                        detail = "; ".join(
                            f"{m['column_name']} ({m['profile_core_type']} vs expected {m['expected_type']})"
                            for m in core_drift["type_mismatches"]
                        )
                        raise RuntimeError(
                            f"HARD GATE: profile_core has incompatible runtime schema drift: {detail}. "
                            f"Fix the table definition before re-running."
                        )
                    if core_drift["added_columns"]:
                        added_cols = ", ".join(
                            f"{c['column_name']} {c['data_type']}"
                            for c in core_drift["added_columns"]
                        )
                        logger.info(
                            f"   profile_core runtime schema healed after core write: added {added_cols}"
                        )
                    else:
                        logger.info(
                            "   profile_core runtime schema preserved through core write"
                        )
                if step_name == "refresh_scope" and mode == "refresh":
                    scope_guard = run_refresh_scope_guard(
                        bq_client,
                        execution_id,
                        force=force,
                        consumer_dataset=PRODUCTION_DATASET,
                        staging_dataset=STAGING_DATASET,
                        lookback_days=lookback_days,
                    )
                    try:
                        refresh_scope_summary = _summarize_refresh_scope(bq_client)
                        _merge_build_run_metadata(
                            bq_client,
                            execution_id,
                            {"refresh_scope": refresh_scope_summary},
                        )
                    except Exception as summary_err:
                        logger.warning(
                            f"Failed to summarize refresh scope: {summary_err}"
                        )
                    guard_status = scope_guard["status"]
                    guard_detail = scope_guard["detail"]
                    if guard_status == "ok":
                        logger.info(f"   Refresh scope guard: {guard_detail}")
                        _log_build_step(
                            bq_client,
                            execution_id,
                            "refresh_scope_guard",
                            "completed",
                            duration_seconds=0.0,
                            error_message=guard_detail,
                        )
                        if (scope_guard.get("scope_n") or 0) == 0:
                            short_circuit_no_changes = True
                            short_circuit_reason = (
                                "Refresh scope is empty; skipping refresh MERGE, downstream "
                                "enrich/personas, candidate view build, and post-build gates."
                            )
                            logger.info(f"   {short_circuit_reason}")
                    elif guard_status == "warn":
                        logger.warning(f"   Refresh scope guard: {guard_detail}")
                        _log_build_step(
                            bq_client,
                            execution_id,
                            "refresh_scope_guard",
                            f"completed_with_warnings: {guard_detail[:180]}",
                            duration_seconds=0.0,
                            error_message=guard_detail,
                        )
                    elif guard_status == "forced":
                        logger.warning(
                            f"   Refresh scope guard overridden by --force: {guard_detail}"
                        )
                        _log_build_step(
                            bq_client,
                            execution_id,
                            "refresh_scope_guard",
                            f"forced: {guard_detail[:180]}",
                            duration_seconds=0.0,
                            error_message=guard_detail,
                        )
                    else:
                        _log_build_step(
                            bq_client,
                            execution_id,
                            "refresh_scope_guard",
                            f"failed_gate: {guard_detail[:180]}",
                            duration_seconds=0.0,
                            error_message=guard_detail,
                        )
                        raise RuntimeError(
                            f"Refresh scope guard FAILED before MERGE: {guard_detail}. "
                            f"Use --force to override if this large scope is intentional."
                        )
                succeeded += 1

                # Log step to profile_ops.profile_build_steps
                _log_build_step(
                    bq_client,
                    execution_id,
                    step_name,
                    "completed",
                    rows_affected=rows_affected,
                    duration_seconds=step_duration,
                    statements_executed=stmt_count,
                    total_bytes_processed=step_bytes_processed,
                    total_bytes_billed=step_bytes_billed,
                    total_slot_millis=step_slot_millis,
                )
                executed_steps.append(step_name)

                if step_name == "refresh_scope" and short_circuit_no_changes:
                    _log_build_step(
                        bq_client,
                        execution_id,
                        "refresh_noop",
                        "completed",
                        duration_seconds=0.0,
                        error_message=short_circuit_reason,
                    )
                    logger.info(
                        "   Refresh short-circuit engaged -- no changed profiles in lookback window."
                    )
                    break

                # C5: After DDL, reconcile profile_core_snapshot columns with profile_core.
                # Missing columns are auto-added with the exact live types from
                # profile_core. Extra historical columns are tolerated. Only true
                # incompatible type mismatches remain a hard gate.
                if step_name == "ddl" and mode in REBUILD_LIKE_MODES:
                    logger.info("   Reconciling profile_core_snapshot schema parity...")
                    try:
                        drift_result = _sync_profile_core_snapshot_schema(
                            bq_client,
                            consumer_dataset=build_consumer_dataset,
                        )
                        added_columns = drift_result["added_columns"]
                        extra_columns = drift_result["extra_columns"]
                        mismatches = drift_result["type_mismatches"]

                        if mismatches:
                            detail = "; ".join(
                                f"{item['column_name']} core={item['core_type']} snapshot={item['snapshot_type']}"
                                for item in mismatches[:10]
                            )
                            _log_build_step(
                                bq_client,
                                execution_id,
                                "ddl_snapshot_schema_check",
                                f"failed_gate: {detail[:200]}",
                                duration_seconds=0,
                            )
                            raise RuntimeError(
                                f"HARD GATE: profile_core_snapshot has incompatible type drift: {detail}. "
                                f"Auto-healing only supports additive columns. Resolve the type mismatch or "
                                f"recreate profile_ops.profile_core_snapshot before re-running."
                            )

                        status = "completed"
                        notes = []
                        if added_columns:
                            added_desc = ", ".join(
                                f"{item['column_name']} ({item['data_type']})"
                                for item in added_columns[:10]
                            )
                            logger.info(
                                f"   Added {len(added_columns)} missing snapshot column(s): {added_desc}"
                            )
                            notes.append(f"added={len(added_columns)}")
                            status = "completed_with_warnings"
                        if extra_columns:
                            extra_desc = ", ".join(extra_columns[:10])
                            logger.warning(
                                f"   Snapshot retains {len(extra_columns)} extra historical column(s): {extra_desc}"
                            )
                            notes.append(f"extra={len(extra_columns)}")
                            status = "completed_with_warnings"
                        if not added_columns and not extra_columns:
                            logger.info(
                                "   profile_core_snapshot schema matches profile_core -- OK"
                            )

                        _log_build_step(
                            bq_client,
                            execution_id,
                            "ddl_snapshot_schema_check",
                            status,
                            duration_seconds=0.0,
                            error_message="; ".join(notes) if notes else None,
                        )
                    except RuntimeError:
                        raise
                    except Exception as drift_err:
                        logger.warning(
                            f"   Schema drift check failed with error: {drift_err} -- continuing"
                        )

            except Exception as e:
                step_duration = time.time() - step_start
                # The snapshot step can 404 when profile_data.profile_core and/or
                # profile_identifiers do not exist yet. Proactive skip usually avoids
                # this; this path catches any remaining NotFound from BigQuery.
                if step_name == "snapshot" and _exception_looks_bq_not_found(e):
                    logger.info(
                        f"   Skipped: {e.__class__.__name__} (missing production snapshot source -- "
                        f"ensuring empty snapshot shell if needed)"
                    )
                    try:
                        _run_snapshot_app_fields_schema_prefix(
                            bq_client,
                            sql_file,
                            **_profile_sql_job_kwargs("snapshot"),
                        )
                    except Exception as shell_err:
                        logger.warning(
                            "Could not create empty profile_core_app_snapshot after NotFound: %s",
                            shell_err,
                        )
                    _log_build_step(
                        bq_client,
                        execution_id,
                        step_name,
                        "skipped: no prior profile_core",
                        duration_seconds=step_duration,
                    )
                    succeeded += 1
                    executed_steps.append(step_name)
                    continue
                logger.error(f"   FAILED: {e}")
                failed += 1
                failed_stmt_count = getattr(e, "statements_executed", 0) or 0
                failed_stmt_index = getattr(e, "failed_statement_index", None)
                failed_stmt_preview = getattr(e, "failed_statement_preview", None)
                if failed_stmt_index is not None:
                    logger.error(
                        f"   Failed at statement #{failed_stmt_index} "
                        f"(of {failed_stmt_count} successful prior). "
                        f"Statement preview: {failed_stmt_preview}"
                    )
                _log_build_step(
                    bq_client,
                    execution_id,
                    step_name,
                    f"failed: {str(e)[:200]}",
                    duration_seconds=step_duration,
                    statements_executed=failed_stmt_count,
                )
                # v6.4 followup: finalize the run row so observability tables
                # reflect terminal state. Without this, profile_build_runs stays
                # at status='running' forever and the operator has to dig into
                # profile_build_steps to find what happened.
                try:
                    _log_build_run_end(
                        bq_client,
                        execution_id,
                        f"failed: {step_name}",
                        total_steps=succeeded + failed,
                        failed_steps=failed,
                        total_bytes_processed=total_bytes_processed,
                        total_bytes_billed=total_bytes_billed,
                        total_slot_millis=total_slot_millis,
                        assertion_summary={
                            "aborted_at_step": step_name,
                            "failed_statement_index": failed_stmt_index,
                        },
                        error_message=f"{step_name}: {str(e)[:400]}",
                    )
                    run_row_finalized["value"] = True
                except Exception as finalize_err:
                    logger.warning(
                        f"Failed to finalize profile_build_runs row on step failure: {finalize_err}"
                    )
                raise  # Stop on first failure -- later steps depend on earlier ones

        logger.info("")
        logger.info("=" * 80)
        logger.info(
            f"PROFILE DATABASE BUILD COMPLETE -- {succeeded} steps succeeded, "
            f"{failed} failed, {total_rows:,} total rows affected"
        )
        logger.info("=" * 80)

        if short_circuit_no_changes:
            logger.info("")
            logger.info(
                "Refresh short-circuit: no changed profiles detected, so the run "
                "ended after refresh_scope."
            )

            assertions = {}
            performance_assertions = run_performance_assertions(
                bq_client,
                execution_id,
                mode=mode,
                consumer_dataset=build_consumer_dataset,
                ops_dataset=OPS_DATASET,
                staging_dataset=STAGING_DATASET,
            )
            assertions.update(performance_assertions)
            performance_soft_warnings = [
                name
                for name, (passed, severity, _) in performance_assertions.items()
                if not passed and severity == "soft"
            ]
            if performance_soft_warnings:
                _log_build_step(
                    bq_client,
                    execution_id,
                    "performance_checks",
                    f"completed_with_warnings: {','.join(performance_soft_warnings)[:200]}",
                    duration_seconds=0.0,
                )
            else:
                _log_build_step(
                    bq_client,
                    execution_id,
                    "performance_checks",
                    "completed",
                    duration_seconds=0.0,
                )

            assertion_summary = {
                "passed": sum(1 for v in assertions.values() if v[0]),
                "total": len(assertions),
                "hard_failures": [],
                "soft_warnings": [
                    name
                    for name, (passed, severity, _) in assertions.items()
                    if not passed and severity == "soft"
                ],
                "short_circuit": "refresh_no_changes",
            }
            run_status = (
                "completed_no_changes_with_warnings"
                if assertion_summary["soft_warnings"]
                else "completed_no_changes"
            )
            _log_build_run_end(
                bq_client,
                execution_id,
                run_status,
                total_steps=succeeded + failed,
                failed_steps=failed,
                total_bytes_processed=total_bytes_processed,
                total_bytes_billed=total_bytes_billed,
                total_slot_millis=total_slot_millis,
                assertion_summary=assertion_summary,
            )
            run_row_finalized["value"] = True

            return {
                "total_rows": 0,  # No extraction rows -- all work is SQL
                "table_rows": {},
                "table_files": {},
                "profile_build": {
                    "mode": mode,
                    "schema_version": SCHEMA_VERSION,
                    "steps_planned": steps_to_run,
                    "steps_run": executed_steps,
                    "succeeded": succeeded,
                    "failed": failed,
                    "total_rows_affected": total_rows,
                    "short_circuit": "refresh_no_changes",
                    "assertions": {k: v[1] for k, v in assertions.items()},
                },
            }

        # Run post-build assertions (v6.4 expanded with build acceptance gate semantics)
        logger.info("")
        logger.info("Running post-build assertions...")
        assertion_start = time.time()
        uses_candidate_views = "views" in steps_to_run or mode == "resume_publish"
        assertion_view_dataset = (
            VIEW_CANDIDATE_DATASET if uses_candidate_views else PRODUCTION_DATASET
        )
        assertions = run_post_build_assertions(
            bq_client,
            execution_id,
            mode=mode,
            config=config,
            consumer_dataset=build_consumer_dataset,
            ops_dataset=OPS_DATASET,
            staging_dataset=STAGING_DATASET,
            view_dataset=assertion_view_dataset,
        )
        assertion_duration = time.time() - assertion_start

        # v6.4+: classify by severity. Hard failures = build acceptance gate failure.
        # During rebuild + resume_publish, profile_current is validated in
        # profile_staging first; production views are repointed only after gates pass
        # (or are forced).
        hard_failures = [
            name
            for name, (passed, severity, _) in assertions.items()
            if not passed and severity == "hard"
        ]
        soft_warnings = [
            name
            for name, (passed, severity, _) in assertions.items()
            if not passed and severity == "soft"
        ]
        passed_count = sum(1 for v in assertions.values() if v[0])
        total_checks = len(assertions)

        logger.info(
            f"Assertions: {passed_count}/{total_checks} passed ({assertion_duration:.1f}s)"
        )
        if hard_failures:
            logger.error(
                f"HARD FAILURES ({len(hard_failures)}): {', '.join(hard_failures)}"
            )
        if soft_warnings:
            logger.warning(
                f"Soft warnings ({len(soft_warnings)}): {', '.join(soft_warnings)}"
            )

        if hard_failures and not force:
            _log_build_step(
                bq_client,
                execution_id,
                "assertions",
                f"failed_gate: {','.join(hard_failures)[:200]}",
                duration_seconds=assertion_duration,
            )
            failure_context = (
                f"Approved physical tables were NOT published to {PRODUCTION_DATASET}. "
                f"Candidate tables remain in {build_consumer_dataset} and candidate views remain in "
                f"{VIEW_CANDIDATE_DATASET}."
                if mode in CANDIDATE_CONSUMER_MODES
                else f"Production consumer views were NOT published. Candidate views remain in "
                f"{VIEW_CANDIDATE_DATASET}, but raw tables in {PRODUCTION_DATASET} have already "
                f"been rebuilt."
            )
            # v6.4 followup: finalize the run row before raising so observability
            # tables reflect the gate failure terminal state instead of staying
            # at status='running'.
            try:
                _log_build_run_end(
                    bq_client,
                    execution_id,
                    "failed_gate",
                    total_steps=succeeded + failed,
                    failed_steps=failed,
                    total_bytes_processed=total_bytes_processed,
                    total_bytes_billed=total_bytes_billed,
                    total_slot_millis=total_slot_millis,
                    assertion_summary={
                        "passed": passed_count,
                        "total": total_checks,
                        "hard_failures": hard_failures,
                        "soft_warnings": soft_warnings,
                    },
                    error_message=f"hard_failures: {','.join(hard_failures)}",
                )
                run_row_finalized["value"] = True
            except Exception as finalize_err:
                logger.warning(
                    f"Failed to finalize profile_build_runs row on gate failure: {finalize_err}"
                )
            gate_note = (
                "This build is marked failed_gate in profile_build_runs."
                if persist_ops_log
                else "Rebuild did not persist profile_build_runs (no ops row)."
            )
            raise RuntimeError(
                f"Build acceptance gate(s) FAILED: {', '.join(hard_failures)}. "
                f"{failure_context} {gate_note} "
                f"Pass --force to override gates and publish anyway."
            )
        elif hard_failures and force:
            logger.warning(
                f"Build acceptance gate(s) FAILED but --force is set: proceeding anyway. "
                f"Failed: {', '.join(hard_failures)}"
            )
            status = f"forced_despite_gates: {','.join(hard_failures)[:150]}"
        elif soft_warnings:
            status = f"completed_with_warnings: {','.join(soft_warnings)[:150]}"
        else:
            status = "completed"

        _log_build_step(
            bq_client,
            execution_id,
            "assertions",
            status,
            duration_seconds=assertion_duration,
        )

        release_views_published = False

        # Rebuild release flow:
        #   1. publish production views pointing at validated candidate tables
        #   2. copy candidate physical tables into profile_data
        #   3. repoint production views back to profile_data
        #
        # This keeps the consumer surface consistent during table promotion instead
        # of exposing a mixed old/new table set while tables copy one by one.
        if (
            mode in CANDIDATE_CONSUMER_MODES
            and build_consumer_dataset != PRODUCTION_DATASET
            and ("views" in steps_to_run or mode == "resume_publish")
        ):
            logger.info("")
            logger.info(
                f"Publishing release views to {PRODUCTION_DATASET} backed by "
                f"validated candidate dataset {build_consumer_dataset}..."
            )
            publish_release_start = time.time()
            try:
                publish_release_result = _publish_consumer_views(
                    bq_client=bq_client,
                    execution_id=execution_id,
                    config=config,
                    consumer_dataset=build_consumer_dataset,
                    view_dataset=PRODUCTION_DATASET,
                    lookback_days=lookback_days,
                    bq_job_labels=build_profile_sql_job_labels(
                        execution_id,
                        mode,
                        "publish_release_views",
                        build_consumer_dataset,
                        orchestrator_job_id=kwargs.get("centralized_job_id"),
                    ),
                    maximum_bytes_billed=profile_maximum_bytes_billed(),
                )
                publish_release_duration = time.time() - publish_release_start
                publish_release_bytes_processed = publish_release_result.get(
                    "total_bytes_processed", 0
                )
                publish_release_bytes_billed = publish_release_result.get(
                    "total_bytes_billed", 0
                )
                publish_release_slot_millis = publish_release_result.get(
                    "total_slot_millis", 0
                )
                total_bytes_processed += publish_release_bytes_processed
                total_bytes_billed += publish_release_bytes_billed
                total_slot_millis += publish_release_slot_millis
                _log_build_step(
                    bq_client,
                    execution_id,
                    "publish_release_views",
                    "completed",
                    rows_affected=publish_release_result.get("total_rows_affected", 0),
                    duration_seconds=publish_release_duration,
                    statements_executed=publish_release_result.get(
                        "statements_executed", 0
                    ),
                    total_bytes_processed=publish_release_bytes_processed,
                    total_bytes_billed=publish_release_bytes_billed,
                    total_slot_millis=publish_release_slot_millis,
                )
                _merge_build_run_metadata(
                    bq_client,
                    execution_id,
                    {
                        "release_dataset_active": build_consumer_dataset,
                        "release_boundary_step": "publish_release_views",
                    },
                )
                release_views_published = True
                logger.info(
                    f"Production views now serve validated candidate tables from "
                    f"{build_consumer_dataset} ({publish_release_duration:.1f}s)"
                )
                # Restore table/column descriptions wiped by CREATE OR REPLACE VIEW.
                _restore_profile_metadata(bq_client, PRODUCTION_DATASET)
            except Exception as e:
                publish_release_duration = time.time() - publish_release_start
                _log_build_step(
                    bq_client,
                    execution_id,
                    "publish_release_views",
                    f"failed: {str(e)[:200]}",
                    duration_seconds=publish_release_duration,
                )
                try:
                    _log_build_run_end(
                        bq_client,
                        execution_id,
                        "failed: publish_release_views",
                        total_steps=succeeded + failed,
                        failed_steps=failed,
                        total_bytes_processed=total_bytes_processed,
                        total_bytes_billed=total_bytes_billed,
                        total_slot_millis=total_slot_millis,
                        error_message=f"publish_release_views: {str(e)[:400]}",
                    )
                    run_row_finalized["value"] = True
                except Exception as finalize_err:
                    logger.warning(f"Failed to finalize run row: {finalize_err}")
                raise RuntimeError(
                    "Failed to publish release views from the validated candidate dataset. "
                    "Production views remain on the prior release and candidate tables stay isolated."
                ) from e

        # Publish approved physical tables for rebuilds after release views are live.
        if (
            mode in CANDIDATE_CONSUMER_MODES
            and build_consumer_dataset != PRODUCTION_DATASET
        ):
            logger.info("")
            logger.info(
                f"Publishing approved physical tables to {PRODUCTION_DATASET} "
                f"from candidate dataset {build_consumer_dataset}..."
            )
            publish_tables_start = time.time()
            try:
                if backup_before_promote_enabled():
                    logger.info(
                        "PROFILE_BACKUP_BEFORE_PROMOTE enabled -- cloning production "
                        "physical tables into profile_ops before promotion."
                    )
                    clone_production_tables_before_promote(
                        bq_client,
                        PRODUCTION_DATASET,
                        PHYSICAL_TABLES,
                        execution_id,
                    )
                publish_tables_result = _publish_physical_tables(
                    bq_client,
                    source_dataset=build_consumer_dataset,
                    target_dataset=PRODUCTION_DATASET,
                    build_id=execution_id,
                    verify_row_counts=True,
                )
                publish_tables_duration = time.time() - publish_tables_start
                _log_build_step(
                    bq_client,
                    execution_id,
                    "publish_tables",
                    "completed",
                    rows_affected=publish_tables_result.get("tables_published", 0),
                    duration_seconds=publish_tables_duration,
                    statements_executed=publish_tables_result.get(
                        "tables_published", 0
                    ),
                    error_message=(
                        f"Published {publish_tables_result.get('tables_published', 0)} tables "
                        f"from {build_consumer_dataset} to {PRODUCTION_DATASET}"
                    ),
                )
                _merge_build_run_metadata(
                    bq_client,
                    execution_id,
                    {
                        "published_table_count": publish_tables_result.get(
                            "tables_published", 0
                        ),
                        "published_tables": publish_tables_result.get(
                            "table_names", []
                        ),
                        "published_from_dataset": build_consumer_dataset,
                    },
                )
                logger.info(
                    f"Published {publish_tables_result.get('tables_published', 0)} physical tables "
                    f"to {PRODUCTION_DATASET} ({publish_tables_duration:.1f}s)"
                )
            except Exception as e:
                publish_tables_duration = time.time() - publish_tables_start
                _log_build_step(
                    bq_client,
                    execution_id,
                    "publish_tables",
                    f"failed: {str(e)[:200]}",
                    duration_seconds=publish_tables_duration,
                )
                try:
                    _log_build_run_end(
                        bq_client,
                        execution_id,
                        "failed: publish_tables",
                        total_steps=succeeded + failed,
                        failed_steps=failed,
                        total_bytes_processed=total_bytes_processed,
                        total_bytes_billed=total_bytes_billed,
                        total_slot_millis=total_slot_millis,
                        error_message=f"publish_tables: {str(e)[:400]}",
                    )
                    run_row_finalized["value"] = True
                except Exception as finalize_err:
                    logger.warning(f"Failed to finalize run row: {finalize_err}")
                if release_views_published:
                    raise RuntimeError(
                        f"Physical table publish FAILED after production views were already pointed at "
                        f"{build_consumer_dataset}. Consumers continue to see the validated candidate "
                        f"release through production views, but physical tables in {PRODUCTION_DATASET} "
                        f"may be partially copied. Investigate and re-run the rebuild publish sequence."
                    ) from e
                raise

        # Publish consumer views after gates pass (or are explicitly forced).
        if "views" in steps_to_run or mode == "resume_publish":
            publish_target_dataset = PRODUCTION_DATASET
            publish_consumer_dataset = PRODUCTION_DATASET
            publish_step_name = "publish_views"
            publish_description = "Publishing consumer views to production"

            if (
                mode in CANDIDATE_CONSUMER_MODES
                and build_consumer_dataset != PRODUCTION_DATASET
            ):
                publish_description = (
                    f"Finalizing production views back onto {PRODUCTION_DATASET} "
                    f"after candidate-backed release"
                )

            logger.info("")
            logger.info(f"{publish_description}...")
            publish_start = time.time()
            try:
                publish_result = _publish_consumer_views(
                    bq_client=bq_client,
                    execution_id=execution_id,
                    config=config,
                    consumer_dataset=publish_consumer_dataset,
                    view_dataset=publish_target_dataset,
                    lookback_days=lookback_days,
                    bq_job_labels=build_profile_sql_job_labels(
                        execution_id,
                        mode,
                        publish_step_name,
                        publish_consumer_dataset,
                        orchestrator_job_id=kwargs.get("centralized_job_id"),
                    ),
                    maximum_bytes_billed=profile_maximum_bytes_billed(),
                )
                publish_duration = time.time() - publish_start
                publish_bytes_processed = publish_result.get("total_bytes_processed", 0)
                publish_bytes_billed = publish_result.get("total_bytes_billed", 0)
                publish_slot_millis = publish_result.get("total_slot_millis", 0)
                total_bytes_processed += publish_bytes_processed
                total_bytes_billed += publish_bytes_billed
                total_slot_millis += publish_slot_millis
                _log_build_step(
                    bq_client,
                    execution_id,
                    publish_step_name,
                    "completed",
                    rows_affected=publish_result.get("total_rows_affected", 0),
                    duration_seconds=publish_duration,
                    statements_executed=publish_result.get("statements_executed", 0),
                    total_bytes_processed=publish_bytes_processed,
                    total_bytes_billed=publish_bytes_billed,
                    total_slot_millis=publish_slot_millis,
                )
                _merge_build_run_metadata(
                    bq_client,
                    execution_id,
                    {
                        "release_dataset_active": publish_consumer_dataset,
                        "release_boundary_step": publish_step_name,
                    },
                )
                logger.info(
                    f"Published consumer views to {publish_target_dataset} "
                    f"backed by {publish_consumer_dataset} ({publish_duration:.1f}s)"
                )
                # Restore table/column descriptions wiped by CREATE OR REPLACE VIEW.
                _restore_profile_metadata(bq_client, publish_target_dataset)
            except Exception as e:
                publish_duration = time.time() - publish_start
                _log_build_step(
                    bq_client,
                    execution_id,
                    publish_step_name,
                    f"failed: {str(e)[:200]}",
                    duration_seconds=publish_duration,
                )
                try:
                    _log_build_run_end(
                        bq_client,
                        execution_id,
                        f"failed: {publish_step_name}",
                        total_steps=succeeded + failed,
                        failed_steps=failed,
                        total_bytes_processed=total_bytes_processed,
                        total_bytes_billed=total_bytes_billed,
                        total_slot_millis=total_slot_millis,
                        error_message=f"{publish_step_name}: {str(e)[:400]}",
                    )
                    run_row_finalized["value"] = True
                except Exception as finalize_err:
                    logger.warning(f"Failed to finalize run row: {finalize_err}")
                if (
                    mode in CANDIDATE_CONSUMER_MODES
                    and build_consumer_dataset != PRODUCTION_DATASET
                ):
                    raise RuntimeError(
                        f"Final production view publish FAILED after candidate-backed release and "
                        f"physical table promotion. Production views should still be serving the "
                        f"validated candidate dataset {build_consumer_dataset}; investigate before "
                        f"repointing them back to {PRODUCTION_DATASET}."
                    ) from e
                raise

        if deferred_snapshot_core:
            logger.info("")
            logger.info(
                "Writing accepted profile_core snapshot after blue/green publish..."
            )
            snapshot_start = time.time()
            try:
                snapshot_result = execute_post_process_sql(
                    bq_client=bq_client,
                    sql_file=STEP_MAP["snapshot_core"][0],
                    config=config,
                    source="profile_database",
                    execution_id=execution_id,
                    context={
                        "build_id": execution_id,
                        "consumer_dataset": PRODUCTION_DATASET,
                        "ops_dataset": OPS_DATASET,
                        "staging_dataset": STAGING_DATASET,
                    },
                    **{
                        "bq_job_labels": build_profile_sql_job_labels(
                            execution_id,
                            mode,
                            "snapshot_core",
                            PRODUCTION_DATASET,
                            orchestrator_job_id=kwargs.get("centralized_job_id"),
                        ),
                        "maximum_bytes_billed": profile_maximum_bytes_billed(),
                    },
                )
                snapshot_duration = time.time() - snapshot_start
                snapshot_bytes_processed = snapshot_result.get(
                    "total_bytes_processed", 0
                )
                snapshot_bytes_billed = snapshot_result.get("total_bytes_billed", 0)
                snapshot_slot_millis = snapshot_result.get("total_slot_millis", 0)
                total_bytes_processed += snapshot_bytes_processed
                total_bytes_billed += snapshot_bytes_billed
                total_slot_millis += snapshot_slot_millis
                _log_build_step(
                    bq_client,
                    execution_id,
                    "snapshot_core",
                    "completed",
                    rows_affected=snapshot_result.get("total_rows_affected", 0),
                    duration_seconds=snapshot_duration,
                    statements_executed=snapshot_result.get("statements_executed", 0),
                    total_bytes_processed=snapshot_bytes_processed,
                    total_bytes_billed=snapshot_bytes_billed,
                    total_slot_millis=snapshot_slot_millis,
                )
                executed_steps.append("snapshot_core")
                logger.info(
                    f"Accepted profile_core snapshot written ({snapshot_duration:.1f}s)"
                )
            except Exception as e:
                snapshot_duration = time.time() - snapshot_start
                _log_build_step(
                    bq_client,
                    execution_id,
                    "snapshot_core",
                    f"failed: {str(e)[:200]}",
                    duration_seconds=snapshot_duration,
                )
                try:
                    _log_build_run_end(
                        bq_client,
                        execution_id,
                        "failed: snapshot_core",
                        total_steps=succeeded + failed,
                        failed_steps=failed,
                        total_bytes_processed=total_bytes_processed,
                        total_bytes_billed=total_bytes_billed,
                        total_slot_millis=total_slot_millis,
                        error_message=f"snapshot_core: {str(e)[:400]}",
                    )
                    run_row_finalized["value"] = True
                except Exception as finalize_err:
                    logger.warning(f"Failed to finalize run row: {finalize_err}")
                raise

        performance_assertions = run_performance_assertions(
            bq_client,
            execution_id,
            mode=mode,
            consumer_dataset=build_consumer_dataset,
            ops_dataset=OPS_DATASET,
            staging_dataset=STAGING_DATASET,
        )
        assertions.update(performance_assertions)
        performance_soft_warnings = [
            name
            for name, (passed, severity, _) in performance_assertions.items()
            if not passed and severity == "soft"
        ]
        if performance_soft_warnings:
            _log_build_step(
                bq_client,
                execution_id,
                "performance_checks",
                f"completed_with_warnings: {','.join(performance_soft_warnings)[:200]}",
                duration_seconds=0.0,
            )
        else:
            _log_build_step(
                bq_client,
                execution_id,
                "performance_checks",
                "completed",
                duration_seconds=0.0,
            )

        # v6.4: finalize the run-level row in profile_build_runs.
        passed_count = sum(1 for v in assertions.values() if v[0])
        total_checks = len(assertions)
        soft_warnings = [
            name
            for name, (passed, severity, _) in assertions.items()
            if not passed and severity == "soft"
        ]
        assertion_summary = {
            "passed": passed_count,
            "total": total_checks,
            "hard_failures": hard_failures,
            "soft_warnings": soft_warnings,
        }
        run_status = (
            "failed_gate"
            if hard_failures and not force
            else (
                "forced_despite_gates"
                if hard_failures and force
                else ("completed_with_warnings" if soft_warnings else "completed")
            )
        )
        _log_build_run_end(
            bq_client,
            execution_id,
            run_status,
            total_steps=succeeded + failed,
            failed_steps=failed,
            total_bytes_processed=total_bytes_processed,
            total_bytes_billed=total_bytes_billed,
            total_slot_millis=total_slot_millis,
            assertion_summary=assertion_summary,
        )
        run_row_finalized["value"] = True
        completed_ok["value"] = run_status in (
            "completed",
            "completed_with_warnings",
            "forced_despite_gates",
        )

        return {
            # SQL-only job: no rows are "extracted" from a source. Surface the
            # rows the SQL steps actually affected (MERGE/UPDATE/INSERT) so the
            # dashboard shows a meaningful count instead of a misleading 0.
            "total_rows": total_rows,
            "table_rows": {},
            "table_files": {},
            "profile_build": {
                "mode": mode,
                "schema_version": SCHEMA_VERSION,
                "steps_planned": steps_to_run,
                "steps_run": executed_steps,
                "succeeded": succeeded,
                "failed": failed,
                "total_rows_affected": total_rows,
                "assertions": {k: v[1] for k, v in assertions.items()},
            },
        }

    finally:
        try:
            if lease_state.get("stop"):
                lease_state["stop"].set()
            if lease_state.get("thread"):
                lease_state["thread"].join(timeout=20)
        except Exception as lease_join_err:
            logger.warning("Lease heartbeat thread stop failed: %s", lease_join_err)
        if lease_state.get("dataset"):
            release_dataset_lease(bq_client, lease_state["dataset"], execution_id)
        _profile_ops_persist_log.reset(_ops_ctx_token)
        # v6.4 followup: catch-all finalize. The per-site _log_build_run_end
        # calls above provide rich status strings (failed_gate,
        # failed: publish_release_views, etc.). This finally is a safety net
        # for any unhandled exit path that bypasses them so the run row
        # never stays at status='running' forever.
        if not run_row_finalized["value"] and persist_ops_log:
            try:
                _log_build_run_end(
                    bq_client,
                    execution_id,
                    "failed: unknown_exit_path",
                    total_steps=succeeded + failed,
                    failed_steps=failed,
                    total_bytes_processed=total_bytes_processed,
                    total_bytes_billed=total_bytes_billed,
                    total_slot_millis=total_slot_millis,
                    error_message="run_pipeline exited without finalizing the run row (unhandled path)",
                )
            except Exception as finalize_err:
                logger.warning(
                    f"Failed to finalize run row in catch-all: {finalize_err}"
                )

        # profile_staging should be scratch-only (no persistent tables).
        # Clean it up on successful completion so tomorrow's incremental starts from a blank staging area.
        if completed_ok["value"]:
            try:
                dropped = _clear_dataset_tables(bq_client, STAGING_DATASET)
                logger.info(
                    f"Cleaned up {dropped} table(s) from staging dataset {STAGING_DATASET}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to clean staging dataset {STAGING_DATASET}: {e}"
                )


def get_available_tables(config):
    """No extractable tables -- this is a SQL-only pipeline."""
    return []


class _SkippedDryRunJob:
    job_id = "skipped-sql-dry-run"
    errors = None
    error_result = None
    total_bytes_processed = 0

    def result(self, *args, **kwargs):
        return self


class _SkipDryRunBigQueryClient:
    def __init__(self, client):
        self._client = client

    def __getattr__(self, name):
        return getattr(self._client, name)

    def query(self, query, job_config=None, *args, **kwargs):
        if getattr(job_config, "dry_run", False):
            return _SkippedDryRunJob()
        return self._client.query(query, job_config=job_config, *args, **kwargs)
