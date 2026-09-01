"""
Staging lifecycle management: scoped cleanup, archive-before-wipe snapshots,
and the staging archive manifest.

Replaces the dataset-wide staging wipe in orchestrate.py with a clean scoped
to the tables a run will actually extract, so multiple sub-sources within one
source (e.g. facebook ads vs pages, mailchimp campaign_group vs list_group)
no longer destroy each other's staging data.

Flow per run (prepare_staging_for_run):
  1. Resolve the run's staging tables, including dependent tables extracted
     via a parent (e.g. facebook adcreatives via ads).
  2. For each existing, non-empty staging table: capture the external
     companion's GCS parquet URIs, snapshot the table into the archive
     dataset with a TTL, and write a manifest row to
     orchestrator_monitoring.staging_archive_manifest.
  3. Drop ONLY the run's staging tables (materialized + _external companion).

The manifest makes archived staging data and the underlying GCS parquet
discoverable later:

    SELECT archive_table, gcs_parquet_uris, expires_at
    FROM `orchestrator_monitoring.staging_archive_manifest`
    WHERE source = 'facebook' AND table_name = 'posts'
    ORDER BY archived_at DESC
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from shared.extractor_utils import apply_table_affix

logger = logging.getLogger(__name__)

MANIFEST_TABLE_NAME = "staging_archive_manifest"

DEFAULT_ARCHIVE_TTL_DAYS = 30


def _get_monitoring_dataset() -> str:
    """Monitoring dataset name, consistent with job_manager/job_lock_bq."""
    from shared.job_manager import MONITORING_DATASET

    return os.environ.get("MONITORING_DATASET", MONITORING_DATASET)


@dataclass
class StagingTableRef:
    """One logical table's staging footprint for a run."""

    logical_name: str
    actual_name: str  # table_name from YAML or {source}_{table}
    staging_table_id: str  # project.dataset.actual_name
    external_table_id: str  # project.dataset.actual_name_external
    via_parent: Optional[str] = (
        None  # set when included as a dependent of a parent table
    )


@dataclass
class StagingPrepResult:
    """Summary of what prepare_staging_for_run did (or would do, if plan_only)."""

    scope: str = "run"
    plan_only: bool = False
    archived: List[Dict[str, Any]] = field(default_factory=list)
    dropped: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    manifest_rows: int = 0
    errors: List[str] = field(default_factory=list)
    # True when a table that HELD DATA could not be archived (archive-before-wipe
    # guarantee violated). The caller must fail closed -- the run's staging may
    # not be safely recoverable. Non-fatal issues (drop failures, manifest write
    # failures) go to `errors` only and do not set this.
    had_unsafe_failure: bool = False


def get_archive_settings(
    config: Dict[str, Any], default_source_name: str = "data"
) -> Dict[str, Any]:
    """
    Resolve archive settings from config with sane defaults.

    pipeline.staging.archive.enabled   (default True)
    pipeline.staging.archive.ttl_days  (default 30; falls back to pipeline.ttl_days)
    Archive dataset: pipeline.archive_dataset if declared, else {source}_archive.

    Accepts either the full YAML config (source is a dict with a name) or the
    flat merge-time config used by process_hash_merge (source is a string).
    """
    pipeline = config.get("pipeline", {}) or {}
    staging_cfg = pipeline.get("staging", {}) or {}
    archive_cfg = staging_cfg.get("archive", {}) or {}

    source_field = config.get("source")
    if isinstance(source_field, dict):
        source_name = source_field.get("name") or default_source_name
    elif isinstance(source_field, str) and source_field:
        source_name = source_field
    else:
        source_name = default_source_name
    archive_dataset = pipeline.get("archive_dataset") or f"{source_name}_archive"

    ttl_days = archive_cfg.get("ttl_days")
    if ttl_days is None:
        ttl_days = pipeline.get("ttl_days", DEFAULT_ARCHIVE_TTL_DAYS)

    return {
        "enabled": archive_cfg.get("enabled", True),
        "ttl_days": int(ttl_days),
        "archive_dataset": archive_dataset,
    }


def get_staging_scope(config: Dict[str, Any], cli_scope: Optional[str] = None) -> str:
    """
    Resolve the staging wipe scope: 'run' (scoped, default) or 'dataset' (legacy full wipe).
    CLI flag wins over config (pipeline.staging.scope).
    """
    if cli_scope in ("run", "dataset"):
        return cli_scope
    cfg_scope = ((config.get("pipeline", {}) or {}).get("staging", {}) or {}).get(
        "scope", "run"
    )
    return cfg_scope if cfg_scope in ("run", "dataset") else "run"


def resolve_run_staging_tables(
    config: Dict[str, Any],
    source: str,
    tables: List[str],
    project: str,
    staging_dataset: str,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
) -> List[StagingTableRef]:
    """
    Map the run's logical table list to its staging footprint.

    Includes dependent tables whose pipeline.table_dependencies entry says
    they are extracted_via a table in this run (their staging tables are
    materialized by the parent's extraction, so they belong to this run's scope).

    schema_prefix/schema_suffix (already normalized, i.e. underscores included)
    are applied to the physical staging name so cleanup/archive target the SAME
    affixed tables the transform writes (e.g. --schema-prefix test_ runs). Without
    this, a prefixed run would archive/drop the unaffixed production-staging tables.
    """
    resources = config.get("resources", {}) or {}
    table_dependencies = (config.get("pipeline", {}) or {}).get(
        "table_dependencies", {}
    ) or {}

    scope_tables: Dict[str, Optional[str]] = {t: None for t in tables}
    for dep_table, dep_info in table_dependencies.items():
        parent = (dep_info or {}).get("extracted_via")
        if parent in scope_tables and dep_table not in scope_tables:
            scope_tables[dep_table] = parent

    refs = []
    for logical, via_parent in scope_tables.items():
        table_config = resources.get(logical, {}) or {}
        actual_name = table_config.get("table_name", f"{source}_{logical}")
        actual_name = apply_table_affix(actual_name, schema_prefix, schema_suffix)
        refs.append(
            StagingTableRef(
                logical_name=logical,
                actual_name=actual_name,
                staging_table_id=f"{project}.{staging_dataset}.{actual_name}",
                external_table_id=f"{project}.{staging_dataset}.{actual_name}_external",
                via_parent=via_parent,
            )
        )
    return refs


def _ensure_archive_dataset(
    client: bigquery.Client, archive_dataset: str, default_ttl_days: int
) -> None:
    """Create the archive dataset if missing, with a default table expiration."""
    dataset_id = f"{client.project}.{archive_dataset}"
    try:
        client.get_dataset(dataset_id)
        return
    except NotFound:
        pass
    dataset_obj = bigquery.Dataset(dataset_id)
    dataset_obj.location = "US"
    # Belt-and-suspenders: anything created here without explicit expiration
    # still expires (legacy CTAS path, manual copies).
    dataset_obj.default_table_expiration_ms = default_ttl_days * 24 * 60 * 60 * 1000
    client.create_dataset(dataset_obj)
    logger.info(
        f"Created archive dataset: {dataset_id} (default ttl {default_ttl_days}d)"
    )


def archive_staging_table_snapshot(
    client: bigquery.Client,
    staging_table_id: str,
    *,
    archive_dataset: str,
    execution_id: Optional[str] = None,
    ttl_days: int = DEFAULT_ARCHIVE_TTL_DAYS,
) -> Optional[Dict[str, Any]]:
    """
    Snapshot a staging table into the archive dataset with an expiration.

    Tries a zero-copy BigQuery table snapshot first (CREATE SNAPSHOT TABLE);
    falls back to a CTAS copy with table.expires set. Returns a dict with
    archive_table / row_count / archive_method, or None if skipped.
    """
    clean_id = staging_table_id.replace("`", "")
    try:
        src = client.get_table(clean_id)
    except NotFound:
        logger.debug(f"  Skipping archive: staging table {clean_id} does not exist")
        return None
    if (src.num_rows or 0) == 0:
        logger.debug(f"  Skipping archive: staging table {clean_id} is empty")
        return None
    if getattr(src, "table_type", "TABLE") == "EXTERNAL":
        logger.debug(f"  Skipping archive: {clean_id} is an external table")
        return None

    table = clean_id.split(".")[-1]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{execution_id[:8]}" if execution_id else ""
    archive_table = f"{client.project}.{archive_dataset}.{table}__{timestamp}{suffix}"

    _ensure_archive_dataset(client, archive_dataset, ttl_days)

    snapshot_query = f"""
    -- Zero-copy snapshot of staging before scoped wipe (expires after {ttl_days} days)
    CREATE SNAPSHOT TABLE `{archive_table}`
    CLONE `{clean_id}`
    OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {ttl_days} DAY))
    """
    method = "SNAPSHOT"
    try:
        client.query(snapshot_query).result()
    except Exception as snap_err:
        logger.debug(
            f"  Snapshot failed for {clean_id}, falling back to CTAS copy: {snap_err}"
        )
        method = "CTAS"
        try:
            copy_query = f"""
            -- CTAS archive copy of staging before scoped wipe (snapshot unavailable)
            CREATE OR REPLACE TABLE `{archive_table}` AS
            SELECT * FROM `{clean_id}`
            """
            client.query(copy_query).result()
            archived = client.get_table(archive_table)
            from datetime import timedelta, timezone

            archived.expires = datetime.now(timezone.utc) + timedelta(days=ttl_days)
            client.update_table(archived, ["expires"])
        except Exception as ctas_err:
            logger.warning(f"  Could not archive staging table {clean_id}: {ctas_err}")
            return None

    logger.info(
        f"  Archived staging: {clean_id} -> {archive_table} "
        f"({src.num_rows:,} rows, {method}, ttl {ttl_days}d)"
    )
    return {
        "archive_table": archive_table,
        "row_count": int(src.num_rows or 0),
        "archive_method": method,
    }


def _ensure_manifest_table(client: bigquery.Client) -> str:
    """Create orchestrator_monitoring.staging_archive_manifest if missing."""
    table_id = f"{client.project}.{_get_monitoring_dataset()}.{MANIFEST_TABLE_NAME}"
    try:
        client.get_table(table_id)
        return table_id
    except NotFound:
        pass

    schema = [
        bigquery.SchemaField(
            "execution_id",
            "STRING",
            mode="NULLABLE",
            description="Monitoring execution id of the run that archived this table",
        ),
        bigquery.SchemaField(
            "job_id", "STRING", mode="NULLABLE", description="Centralized job id"
        ),
        bigquery.SchemaField(
            "source", "STRING", mode="REQUIRED", description="Data source name"
        ),
        bigquery.SchemaField(
            "group_name",
            "STRING",
            mode="NULLABLE",
            description="Group(s)/sub-source of the run, comma separated; 'default' when none",
        ),
        bigquery.SchemaField(
            "table_name",
            "STRING",
            mode="REQUIRED",
            description="Logical table name from config resources",
        ),
        bigquery.SchemaField(
            "staging_table",
            "STRING",
            mode="REQUIRED",
            description="Full staging table id that was archived",
        ),
        bigquery.SchemaField(
            "archive_table",
            "STRING",
            mode="REQUIRED",
            description="Full archive table id (queryable until expires_at)",
        ),
        bigquery.SchemaField(
            "gcs_parquet_uris",
            "STRING",
            mode="REPEATED",
            description="GCS parquet URIs from the staging external table; reload via CREATE EXTERNAL TABLE OPTIONS(uris=[...])",
        ),
        bigquery.SchemaField(
            "row_count",
            "INTEGER",
            mode="NULLABLE",
            description="Rows in the staging table at archive time",
        ),
        bigquery.SchemaField(
            "archived_at",
            "TIMESTAMP",
            mode="REQUIRED",
            description="When the archive snapshot was taken",
        ),
        bigquery.SchemaField(
            "expires_at",
            "TIMESTAMP",
            mode="NULLABLE",
            description="When the BQ archive table auto-expires (GCS parquet may outlive this)",
        ),
        bigquery.SchemaField(
            "archive_method",
            "STRING",
            mode="NULLABLE",
            description="SNAPSHOT (zero-copy) or CTAS",
        ),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, field="archived_at"
    )
    table.clustering_fields = ["source", "table_name"]
    table.description = (
        "Index of archived staging tables. Each row maps a run (execution_id) to the "
        "BQ archive table (queryable until expires_at) and the raw GCS parquet URIs. "
        "To reload expired data: CREATE EXTERNAL TABLE ... OPTIONS(format='PARQUET', "
        "uris=[<gcs_parquet_uris>])."
    )
    client.create_table(table)
    logger.info(f"Created manifest table: {table_id}")

    try:
        _ensure_latest_archives_view(client)
    except Exception as e:
        logger.debug(f"Could not create latest-archives view: {e}")
    return table_id


def _ensure_latest_archives_view(client: bigquery.Client) -> None:
    """Create/refresh the convenience view of the latest archive per source/table."""
    dataset = _get_monitoring_dataset()
    view_id = f"{client.project}.{dataset}.v_latest_staging_archives"
    view_query = f"""
    -- Latest staging archive per source/table (see staging_archive_manifest)
    CREATE OR REPLACE VIEW `{view_id}` AS
    SELECT *
    FROM `{client.project}.{dataset}.{MANIFEST_TABLE_NAME}`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source, table_name ORDER BY archived_at DESC
    ) = 1
    """
    client.query(view_query).result()
    logger.info(f"Created view: {view_id}")


def list_staging_archives(
    client: bigquery.Client,
    source: Optional[str] = None,
    tables: Optional[List[str]] = None,
    days: int = 30,
) -> List[Dict[str, Any]]:
    """
    Query the staging archive manifest for recent archives.

    Returns rows (newest first) with archive table, GCS parquet URIs, row
    counts, and expiry, so older staging data can be located and reloaded.
    """
    table_id = f"{client.project}.{_get_monitoring_dataset()}.{MANIFEST_TABLE_NAME}"
    try:
        client.get_table(table_id)
    except NotFound:
        return []

    conditions = [
        "archived_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)"
    ]
    params = [bigquery.ScalarQueryParameter("days", "INT64", days)]
    if source:
        conditions.append("source = @source")
        params.append(bigquery.ScalarQueryParameter("source", "STRING", source))
    if tables:
        conditions.append("table_name IN UNNEST(@tables)")
        params.append(bigquery.ArrayQueryParameter("tables", "STRING", tables))

    query = f"""
    -- Recent staging archives from the manifest
    SELECT execution_id, job_id, source, group_name, table_name,
           archive_table, gcs_parquet_uris, row_count,
           archived_at, expires_at, archive_method
    FROM `{table_id}`
    WHERE {' AND '.join(conditions)}
    ORDER BY archived_at DESC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


def write_staging_archive_manifest(
    client: bigquery.Client,
    rows: List[Dict[str, Any]],
    ttl_days: int = DEFAULT_ARCHIVE_TTL_DAYS,
) -> int:
    """
    Insert manifest rows for archived staging tables. Returns rows written.

    Uses a single parameterized DML INSERT (not streaming) so rows are
    immediately consistent and the table can be created in the same run.
    """
    if not rows:
        return 0
    table_id = _ensure_manifest_table(client)

    values_clauses = []
    params: List[bigquery.query._AbstractQueryParameter] = [
        bigquery.ScalarQueryParameter("ttl_days", "INT64", ttl_days),
    ]
    for i, row in enumerate(rows):
        values_clauses.append(
            f"(@execution_id_{i}, @job_id_{i}, @source_{i}, @group_name_{i}, "
            f"@table_name_{i}, @staging_table_{i}, @archive_table_{i}, "
            f"@gcs_uris_{i}, @row_count_{i}, CURRENT_TIMESTAMP(), "
            f"TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl_days DAY), @method_{i})"
        )
        params.extend(
            [
                bigquery.ScalarQueryParameter(
                    f"execution_id_{i}", "STRING", row.get("execution_id")
                ),
                bigquery.ScalarQueryParameter(
                    f"job_id_{i}", "STRING", row.get("job_id")
                ),
                bigquery.ScalarQueryParameter(f"source_{i}", "STRING", row["source"]),
                bigquery.ScalarQueryParameter(
                    f"group_name_{i}", "STRING", row.get("group_name")
                ),
                bigquery.ScalarQueryParameter(
                    f"table_name_{i}", "STRING", row["table_name"]
                ),
                bigquery.ScalarQueryParameter(
                    f"staging_table_{i}", "STRING", row["staging_table"]
                ),
                bigquery.ScalarQueryParameter(
                    f"archive_table_{i}", "STRING", row["archive_table"]
                ),
                bigquery.ArrayQueryParameter(
                    f"gcs_uris_{i}", "STRING", row.get("gcs_parquet_uris") or []
                ),
                bigquery.ScalarQueryParameter(
                    f"row_count_{i}", "INT64", row.get("row_count")
                ),
                bigquery.ScalarQueryParameter(
                    f"method_{i}", "STRING", row.get("archive_method")
                ),
            ]
        )

    insert_query = f"""
    -- Staging archive manifest entries for this run
    INSERT INTO `{table_id}`
      (execution_id, job_id, source, group_name, table_name, staging_table,
       archive_table, gcs_parquet_uris, row_count, archived_at, expires_at, archive_method)
    VALUES
      {','.join(values_clauses)}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(insert_query, job_config=job_config).result()
    return len(rows)


def _get_archive_external_uris(
    client: bigquery.Client, archive_dataset: str, actual_name: str
) -> List[str]:
    """
    Fallback URI source for plugin-driven pipelines (e.g. mailchimp): their
    per-table pipeline deletes the staging external table after merge and
    re-creates it in the archive dataset as {table}_{job_id}. Return the
    source URIs of the newest such external table, [] if none.
    """
    try:
        prefix = f"{actual_name}_"
        candidates = [
            t
            for t in client.list_tables(f"{client.project}.{archive_dataset}")
            if t.table_id.startswith(prefix)
            # Exclude this module's own snapshots ({table}__{ts}_{exec})
            and not t.table_id.startswith(f"{actual_name}__")
        ]
        if not candidates:
            return []
        newest = max(candidates, key=lambda t: t.created or 0)
        ext = client.get_table(f"{client.project}.{archive_dataset}.{newest.table_id}")
        if ext.external_data_configuration:
            return list(ext.external_data_configuration.source_uris or [])
    except NotFound:
        pass
    except Exception as e:
        logger.debug(f"  Archive-external URI fallback failed for {actual_name}: {e}")
    return []


def _get_external_uris(client: bigquery.Client, external_table_id: str) -> List[str]:
    """GCS parquet URIs behind a staging external table, [] if not found."""
    try:
        ext = client.get_table(external_table_id)
        if ext.external_data_configuration:
            return list(ext.external_data_configuration.source_uris or [])
    except NotFound:
        pass
    except Exception as e:
        logger.debug(
            f"  Could not read external table config for {external_table_id}: {e}"
        )
    return []


def prepare_staging_for_run(
    client: bigquery.Client,
    config: Dict[str, Any],
    source: str,
    tables: List[str],
    *,
    staging_dataset: str,
    execution_id: Optional[str] = None,
    job_id: Optional[str] = None,
    groups: Optional[List[str]] = None,
    scope: str = "run",
    plan_only: bool = False,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
) -> StagingPrepResult:
    """
    Archive-then-clean the staging area for a run.

    scope='run'     : only this run's staging tables (resolved from `tables`
                      plus extracted_via dependents) are archived and dropped.
                      Other sub-sources' staging survives for validation.
    scope='dataset' : legacy behavior, all tables in the staging dataset
                      (still archived first when archiving is enabled).
    plan_only=True  : report what would happen, change nothing (dry run).
    """
    result = StagingPrepResult(scope=scope, plan_only=plan_only)
    archive_settings = get_archive_settings(config)
    group_name = ",".join(groups) if groups else "default"

    if scope == "dataset":
        # Legacy full wipe: every table in the dataset is in scope.
        dataset_ref = f"{client.project}.{staging_dataset}"
        try:
            existing = list(client.list_tables(dataset_ref))
        except NotFound:
            existing = []
        targets = []
        seen_actuals = set()
        for t in existing:
            name = t.table_id
            seen_actuals.add(name)
            if name.endswith("_external"):
                continue
            targets.append(
                StagingTableRef(
                    logical_name=name,
                    actual_name=name,
                    staging_table_id=f"{dataset_ref}.{name}",
                    external_table_id=f"{dataset_ref}.{name}_external",
                )
            )
        # External tables with no materialized companion still need dropping.
        orphan_externals = [
            f"{dataset_ref}.{n}"
            for n in seen_actuals
            if n.endswith("_external") and n[: -len("_external")] not in seen_actuals
        ]
    else:
        targets = resolve_run_staging_tables(
            config,
            source,
            tables,
            client.project,
            staging_dataset,
            schema_prefix=schema_prefix,
            schema_suffix=schema_suffix,
        )
        orphan_externals = []

    manifest_rows = []
    for ref in targets:
        # Capture GCS URIs from the external companion BEFORE anything is dropped;
        # fall back to the newest per-job archive external (plugin pipelines
        # delete the staging external after merge and re-home it there).
        gcs_uris = _get_external_uris(client, ref.external_table_id)
        if not gcs_uris:
            gcs_uris = _get_archive_external_uris(
                client, archive_settings["archive_dataset"], ref.actual_name
            )

        archived = None
        archive_failed = False  # archive was required for this table but errored
        had_rows = False
        if archive_settings["enabled"]:
            if plan_only:
                try:
                    src = client.get_table(ref.staging_table_id)
                    if (src.num_rows or 0) > 0 and getattr(
                        src, "table_type", "TABLE"
                    ) != "EXTERNAL":
                        archived = {
                            "archive_table": "(planned)",
                            "row_count": int(src.num_rows or 0),
                            "archive_method": "SNAPSHOT",
                        }
                except NotFound:
                    pass
            else:
                # Does the table hold data worth protecting? An empty/missing
                # table needs no archive, so a "failure" to archive it is benign.
                try:
                    src = client.get_table(ref.staging_table_id)
                    had_rows = (src.num_rows or 0) > 0 and getattr(
                        src, "table_type", "TABLE"
                    ) != "EXTERNAL"
                except NotFound:
                    had_rows = False
                except Exception:
                    had_rows = True  # assume worst case if we cannot tell
                try:
                    archived = archive_staging_table_snapshot(
                        client,
                        ref.staging_table_id,
                        archive_dataset=archive_settings["archive_dataset"],
                        execution_id=execution_id,
                        ttl_days=archive_settings["ttl_days"],
                    )
                    # archive_staging_table_snapshot returns None for an
                    # empty/missing table (nothing to archive) -- that is not a
                    # failure. It only "fails" if it had rows but produced no
                    # snapshot.
                    if archived is None and had_rows:
                        archive_failed = True
                        result.errors.append(
                            f"Archive produced no snapshot for non-empty "
                            f"{ref.staging_table_id}"
                        )
                except Exception as e:
                    archive_failed = had_rows
                    msg = f"Archive failed for {ref.staging_table_id}: {e}"
                    logger.error(f"  {msg}") if had_rows else logger.warning(f"  {msg}")
                    result.errors.append(msg)

        if archived:
            result.archived.append({"table": ref.logical_name, **archived})
            manifest_rows.append(
                {
                    "execution_id": execution_id,
                    "job_id": job_id,
                    "source": source,
                    "group_name": group_name,
                    "table_name": ref.logical_name,
                    "staging_table": ref.staging_table_id,
                    "archive_table": archived["archive_table"],
                    "gcs_parquet_uris": gcs_uris,
                    "row_count": archived["row_count"],
                    "archive_method": archived["archive_method"],
                }
            )
        else:
            result.skipped.append(ref.staging_table_id)

        # FAIL CLOSED: if archiving was required for this table (it had data)
        # but failed, do NOT drop it -- dropping un-archived data is the exact
        # data-loss the archive-before-wipe guarantee exists to prevent. Leave
        # the table in place and let the caller abort.
        if archive_failed:
            result.had_unsafe_failure = True
            logger.error(
                f"  Skipping drop of {ref.staging_table_id}: its archive failed "
                f"and it holds data. Aborting the wipe for this table to avoid "
                f"un-recoverable loss."
            )
            continue

        # Drop the run's staging table and its external companion.
        for table_id in (ref.staging_table_id, ref.external_table_id):
            if plan_only:
                result.dropped.append(table_id)
            else:
                try:
                    client.delete_table(table_id, not_found_ok=True)
                    result.dropped.append(table_id)
                except Exception as e:
                    msg = f"Could not drop {table_id}: {e}"
                    logger.warning(f"  {msg}")
                    result.errors.append(msg)

    for table_id in orphan_externals:
        if plan_only:
            result.dropped.append(table_id)
        else:
            try:
                client.delete_table(table_id, not_found_ok=True)
                result.dropped.append(table_id)
            except Exception as e:
                result.errors.append(f"Could not drop {table_id}: {e}")

    if manifest_rows and not plan_only:
        try:
            result.manifest_rows = write_staging_archive_manifest(
                client, manifest_rows, ttl_days=archive_settings["ttl_days"]
            )
        except Exception as e:
            msg = f"Could not write staging archive manifest: {e}"
            logger.warning(f"  {msg}")
            result.errors.append(msg)
    elif plan_only:
        result.manifest_rows = len(manifest_rows)

    verb = "Would archive" if plan_only else "Archived"
    logger.info(
        f"Staging prep ({scope} scope): {verb} {len(result.archived)} table(s), "
        f"dropped {len(result.dropped)} object(s), skipped {len(result.skipped)} "
        f"(empty/missing) in {staging_dataset}"
    )
    return result
