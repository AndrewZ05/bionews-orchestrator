#!/usr/bin/env python3
"""
Postgres Extractor
==================

Generic extractor for single-tenant Postgres sources. Currently drives Survey
Engine (configs/surveyengine.yaml), the Laravel form/question engine hosted on
Laravel Cloud (Neon managed Postgres).

Why this is not wordpress_extractor
-----------------------------------
wordpress_extractor is MySQL + multi-tenant + SSH-tunnel + per-site credential
discovery. None of that applies here: one database, one schema, direct TLS. The
tunnel/pool machinery would be dead weight, so this module connects straight
through psycopg2 and reuses the shared Parquet/accumulator plumbing.

Incremental strategy
--------------------
Per resource, `incremental.strategy`:
  * date  -> WHERE <field> >= start AND <field> < end+1day. Laravel maintains
             `updated_at` on every write, so this catches edits as well as
             inserts.
  * none  -> full re-read. Used by the join tables that Laravel creates without
             timestamps; they are small and have no row-level date to filter on.

Rows are MERGEd downstream on the resource's primary_key, so a full re-read of
a join table is idempotent rather than duplicating.

JSON columns (`json`/`jsonb`) are serialized to STRING so the row_hash stays
stable -- psycopg2 would otherwise hand back dict/list objects whose repr
ordering is not guaranteed.
"""

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional

from shared.account_context import set_execution_metadata
from shared.extraction_result import StandardExtractionResult
from shared.extractor_runner import initialize_pipeline_environment
from shared.gcs_pipeline import extract_to_local_parquet

logger = logging.getLogger(__name__)

# Postgres identifiers we generate are always drawn from config, but quote them
# anyway so a table named e.g. "position" can never collide with a keyword.
_IDENT_OK = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


def _quote_ident(ident: str) -> str:
    """Validate + double-quote a Postgres identifier."""
    if not ident or not set(ident) <= _IDENT_OK:
        raise ValueError(f"Unsafe Postgres identifier: {ident!r}")
    return f'"{ident}"'


def get_available_tables(
    config: Dict[str, Any], group: Optional[str] = None
) -> List[str]:
    """Resource keys for a group (or every active resource when group is None)."""
    resources = config.get("resources", {})

    if group:
        names = config.get("groups", {}).get(group)
        if names is None:
            raise ValueError(f"Unknown group '{group}' in surveyengine config")
    else:
        names = list(resources.keys())

    return [n for n in names if resources.get(n, {}).get("active", True)]


def create_connection(config: Dict[str, Any]):
    """Open a psycopg2 connection from the config's connection.database block."""
    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise ImportError(
            "psycopg2-binary is required for the postgres extractor. "
            "Install it with: pip install psycopg2-binary"
        ) from exc

    db = config["source"]["connection"]["database"]

    # config_loader has already expanded ${VAR} placeholders; fall back to the
    # environment for anything a caller left unset.
    host = db.get("host") or os.getenv("SURVEY_PG_HOST")
    port = int(db.get("port") or os.getenv("SURVEY_PG_PORT") or 5432)
    dbname = db.get("name") or os.getenv("SURVEY_PG_DATABASE") or "main"
    user = db.get("user") or os.getenv("SURVEY_PG_USERNAME")
    password = db.get("password") or os.getenv("SURVEY_PG_PASSWORD")
    sslmode = db.get("sslmode") or os.getenv("SURVEY_PG_SSLMODE") or "require"

    missing = [
        k for k, v in (("host", host), ("user", user), ("password", password)) if not v
    ]
    if missing:
        raise ValueError(
            f"Missing Postgres connection settings: {', '.join(missing)}. "
            "Check SURVEY_PG_* in .env"
        )

    logger.info(
        "Connecting to Postgres %s:%s/%s (sslmode=%s)", host, port, dbname, sslmode
    )

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
        connect_timeout=int(db.get("connect_timeout") or 30),
        application_name="orchestrator-surveyengine",
    )
    # Read-only, but NOT autocommit: the server-side (named) cursors used by
    # extract_table only exist inside a transaction. psycopg2 opens one lazily
    # on first execute and it is rolled back in run_pipeline's finally.
    conn.set_session(readonly=True, autocommit=False)

    statement_timeout = db.get("statement_timeout_ms")
    if statement_timeout:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {int(statement_timeout)}")

    return conn


def build_query(
    resource_config: Dict[str, Any],
    schema_name: str,
    start_date: Optional[str],
    end_date: Optional[str],
    refresh_mode: str,
    test_mode: bool = False,
) -> tuple:
    """
    Build the SELECT for one resource.

    Returns (sql, params). Columns come from the config schema (minus the
    pipeline metadata columns, which this extractor adds itself) so a column
    dropped upstream surfaces as a Postgres error rather than silently
    producing NULLs.
    """
    META_COLUMNS = {"execution_id", "extracted_at", "source"}

    source_table = resource_config.get("source_table")
    if not source_table:
        raise ValueError("Resource is missing 'source_table'")

    columns = [c for c in resource_config.get("schema", {}) if c not in META_COLUMNS]
    if not columns:
        raise ValueError(f"Resource '{source_table}' has no schema columns")

    col_sql = ", ".join(_quote_ident(c) for c in columns)
    table_sql = f"{_quote_ident(schema_name)}.{_quote_ident(source_table)}"

    sql = f"SELECT {col_sql} FROM {table_sql}"
    params: List[Any] = []

    incremental = resource_config.get("incremental", {}) or {}
    strategy = incremental.get("strategy", "none")
    fields = incremental.get("fields") or []

    if refresh_mode != "full" and strategy == "date" and fields and start_date:
        date_field = _quote_ident(fields[0])
        # end_date is an inclusive calendar day, so compare against the
        # following midnight rather than losing same-day rows.
        clauses = [f"{date_field} >= %s"]
        params.append(start_date)
        if end_date:
            end_exclusive = (
                datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
            clauses.append(f"{date_field} < %s")
            params.append(end_exclusive)
        # A NULL updated_at would be invisible to the range filter; Laravel can
        # leave it NULL on rows written by raw SQL/seeders, so include them.
        sql += f" WHERE (({' AND '.join(clauses)}) OR {date_field} IS NULL)"
        sql += f" ORDER BY {date_field} ASC NULLS FIRST"
    elif strategy == "date" and fields:
        sql += f" ORDER BY {_quote_ident(fields[0])} ASC NULLS FIRST"

    if test_mode:
        sql += " LIMIT 100"

    return sql, params


def extract_table(
    conn,
    resource_config: Dict[str, Any],
    schema_name: str,
    start_date: Optional[str],
    end_date: Optional[str],
    refresh_mode: str,
    batch_size: int = 5000,
    test_mode: bool = False,
) -> Iterator[Dict[str, Any]]:
    """Stream rows for one resource as dicts, JSON columns serialized."""
    import json as _json

    from psycopg2.extras import RealDictCursor

    sql, params = build_query(
        resource_config, schema_name, start_date, end_date, refresh_mode, test_mode
    )
    logger.debug("SQL: %s params=%s", sql, params)

    json_fields = set(resource_config.get("json_fields") or [])

    # Named cursor => server-side, so a large event table never has to fit in
    # memory. psycopg2 requires an open transaction for these (autocommit is
    # off on this connection), and the cursor name must be unique within it.
    cursor_name = f"se_{resource_config['source_table']}_{uuid.uuid4().hex[:8]}"
    try:
        with conn.cursor(name=cursor_name, cursor_factory=RealDictCursor) as cur:
            cur.itersize = batch_size
            cur.execute(sql, params)

            for row in cur:
                record = dict(row)
                for field_name in json_fields:
                    value = record.get(field_name)
                    if value is not None and not isinstance(value, str):
                        record[field_name] = _json.dumps(value, default=str)
                yield record
    finally:
        # Read-only transaction: end it so a failed table cannot leave the
        # connection in an aborted state that poisons every later table.
        conn.rollback()


def run_pipeline(
    config: Dict[str, Any],
    sites: List[str] = None,
    tables: List[str] = None,
    group: Optional[str] = None,
    refresh_mode: str = "incremental",
    lookback_days: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    test_mode: bool = False,
    batch_size: Optional[int] = None,
    max_retries: int = 3,
    skip_validation: bool = False,
    export_format: str = None,
    export_dir: str = None,
    skip_hash_merge: bool = False,
    archive_staging: bool = False,
    truncate_staging: bool = False,
    rebuild: bool = False,
    bq_client: Any = None,
    execution_id: str = None,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Postgres extraction pipeline.

    Single tenant: extracts each requested resource to a local Parquet file and
    returns the standard accumulator result. orchestrate.py owns GCS upload,
    staging load and the hash merge from there.
    """
    pipeline_cfg = config.get("pipeline", {})

    if lookback_days is None:
        lookback_days = pipeline_cfg.get("incremental", {}).get("lookback_days", 7)
    if batch_size is None:
        batch_size = pipeline_cfg.get("parallel", {}).get("batch_size", 5000)

    # Date range. A full refresh / rebuild deliberately leaves start_date unset
    # so build_query emits no WHERE clause.
    if refresh_mode != "full" and not rebuild:
        if not start_date:
            start_date = (
                datetime.now(timezone.utc) - timedelta(days=lookback_days)
            ).strftime("%Y-%m-%d")
            logger.info(
                "Using lookback of %s days: start_date=%s", lookback_days, start_date
            )
        if not end_date:
            end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    else:
        start_date = None
        logger.info("Full refresh: no date filter applied")

    if not tables:
        tables = get_available_tables(config, group)
        logger.info("Tables to extract (%s): %s", group or "all", len(tables))

    env = initialize_pipeline_environment(
        config,
        bq_client=bq_client,
        source_default="surveyengine",
        schema_prefix=schema_prefix,
        schema_suffix=schema_suffix,
        execution_id=execution_id,
        rebuild=rebuild,
        require_bucket=False,
    )

    source = env.source_name
    run_execution_id = env.execution_id or execution_id
    schema_name = config["source"]["connection"]["database"].get("schema", "public")
    resources = config.get("resources", {})

    acc = StandardExtractionResult(
        source=source,
        execution_id=run_execution_id,
        bq_client=bq_client,
        sites=[source],
    )

    conn = None
    try:
        conn = create_connection(config)

        for table_key in tables:
            resource_config = resources.get(table_key)
            if not resource_config:
                logger.warning("Table '%s' not found in config -- skipping", table_key)
                acc.note_error(f"Unknown table '{table_key}'", table=table_key)
                continue

            if not resource_config.get("active", True):
                logger.info("Skipping inactive table: %s", table_key)
                acc.skip_table(table_key, reason="skipped")
                continue

            strategy = (resource_config.get("incremental") or {}).get(
                "strategy", "none"
            )
            logger.info(
                "Extracting %s (strategy=%s)%s",
                table_key,
                strategy,
                f" since {start_date}" if start_date and strategy == "date" else "",
            )

            try:
                rows = list(
                    extract_table(
                        conn=conn,
                        resource_config=resource_config,
                        schema_name=schema_name,
                        start_date=start_date,
                        end_date=end_date,
                        refresh_mode=refresh_mode,
                        batch_size=batch_size,
                        test_mode=test_mode,
                    )
                )
            except Exception as exc:
                logger.error("Failed to extract %s: %s", table_key, exc)
                acc.fail_table(table_key, error=str(exc), stage="extract")
                continue

            if not rows:
                logger.info("  no rows for %s", table_key)
                acc.skip_table(table_key)
                continue

            set_execution_metadata(rows, run_execution_id, source=source)

            table_with_affix = env.format_table(
                resource_config.get("table_name", f"se_{table_key}")
            )
            main_table = f"{env.project}.{env.production_dataset}.{table_with_affix}"

            try:
                local_file_path = extract_to_local_parquet(
                    data=rows,
                    source=source,
                    table=table_key,
                    job_id=run_execution_id,
                    bq_client=bq_client,
                    production_table_id=main_table,
                    rebuild_mode=rebuild,
                )
            except Exception as exc:
                logger.error("Failed to write Parquet for %s: %s", table_key, exc)
                acc.fail_table(table_key, error=str(exc), stage="parquet")
                continue

            acc.record_prewritten(table_key, local_file_path, len(rows))
            logger.info("  %s: %s rows", table_key, f"{len(rows):,}")

        logger.info("=" * 60)
        logger.info("Survey Engine extraction complete")
        logger.info("Total rows: %s", f"{acc.total_rows:,}")
        logger.info("Successful tables: %s/%s", acc.successful_tables, len(tables))
        logger.info("=" * 60)

        final = acc.finalize()
        final["tables"] = len(tables)
        return final

    finally:
        if conn is not None:
            conn.close()
