#!/usr/bin/env python3
"""
LimeSurvey Extractor
====================

Extracts LimeSurvey data from MariaDB via SSH tunnel to BigQuery.

Architecture:
- Reuses WordPress SSH tunnel and MySQL connection patterns
- Single instance (simpler than WordPress multi-site)
- Auto-discovers lime_survey_{sid} response tables
- Supports both date-based and ID-based incremental strategies
- Uses standard GCS pipeline flow (6 steps)

Data Flow:
1. SSH tunnel to LimeSurvey server
2. Connect to MariaDB database
3. Extract tables (metadata + dynamic survey responses)
4. Write to local Parquet with schema enforcement
5. Upload to GCS → External table → Production table
6. Archive and cleanup

Author: Data Pipeline Team
Created: 2025-01-10
"""

import os
import sys
import time
import socket
import subprocess
import logging
import re
import uuid
from typing import Iterator, Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import create_engine, text
from google.cloud import bigquery
import pandas as pd
from fabric import Connection

# Import shared utilities
from shared.timestamp_utils import parse_mysql_timestamp
from shared.extractor_utils import get_available_tables
from shared.account_context import set_execution_metadata
from shared.extractor_runner import initialize_pipeline_environment
from shared.gcs_pipeline import extract_to_local_parquet
from shared.external_tables import _sanitize_field_name
from shared.incremental_id_strategy import (
    get_min_id_from_lookback,
)

logger = logging.getLogger(__name__)

METADATA_FIELD_TYPES = {
    "response_id": "VARCHAR",
    "submitdate": "TIMESTAMP",
    "startdate": "TIMESTAMP",
    "datestamp": "TIMESTAMP",
    "lastpage": "BIGINT",
    "extracted_at": "TIMESTAMP",
    "execution_id": "VARCHAR",
    "source": "VARCHAR",
}

# Survey metadata fields - added separately as literal values from lime_surveys table
# These should NOT be in METADATA_FIELD_TYPES to avoid duplication
SURVEY_METADATA_FIELDS = {
    "survey_gsid": "BIGINT",
    "survey_owner_id": "VARCHAR",
    "survey_admin": "VARCHAR",
    "survey_language": "VARCHAR",
    "site": "VARCHAR",
}

METADATA_EXCLUDE = {
    "seed",
    "startlanguage",
}  # token is now included for participant linkage


def _sanitize_records(
    records: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, str], Dict[str, str]]:
    """Sanitize record keys for Parquet output and preserve name mappings."""

    sanitized_records: List[Dict[str, Any]] = []
    original_to_sanitized: Dict[str, str] = {}
    sanitized_to_original: Dict[str, str] = {}

    for record in records:
        sanitized_record: Dict[str, Any] = {}
        for key, value in record.items():
            key_str = str(key) if key is not None else "field"
            if key_str.lower() in METADATA_EXCLUDE:
                continue
            if key_str in original_to_sanitized:
                sanitized_key = original_to_sanitized[key_str]
            else:
                base_key = _sanitize_field_name(key_str) or "field"
                sanitized_key = base_key
                suffix = 1
                while (
                    sanitized_key in sanitized_to_original
                    and sanitized_to_original[sanitized_key] != key_str
                ):
                    sanitized_key = f"{base_key}_{suffix}"
                    suffix += 1

                original_to_sanitized[key_str] = sanitized_key
                sanitized_to_original[sanitized_key] = key_str

            sanitized_record[sanitized_key] = value

        sanitized_records.append(sanitized_record)

    return sanitized_records, original_to_sanitized, sanitized_to_original


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _get_survey_metadata(
    engine: sa.engine.Engine, survey_ids: List[int]
) -> Dict[int, Dict[str, Any]]:
    """
    Fetch survey metadata (tenant/site information) for a list of survey IDs.

    Args:
        engine: SQLAlchemy engine
        survey_ids: List of survey IDs to fetch metadata for

    Returns:
        Dict mapping survey_id -> {gsid, owner_id, admin, language, site}
    """
    if not survey_ids:
        return {}

    # Query lime_surveys table for basic survey metadata
    survey_ids_str = ",".join(str(sid) for sid in survey_ids)
    query = f"""
        SELECT
            sid,
            gsid,
            owner_id,
            admin,
            language
        FROM lime_surveys
        WHERE sid IN ({survey_ids_str})
    """

    try:
        rows = execute_query(engine, query)
        metadata = {}
        for row in rows:
            # NOTE: lime_surveys.admin is the survey administrator's NAME, NOT a
            # site/brand. Mapping site=admin populated the column with values like
            # "inherit" and individual employee names (BI-### site-column bug).
            # The authoritative site (brand/community, e.g. "Myasthenia Gravis
            # (MG)") is derived downstream in SQL from the survey title via
            # survey_site_map (sql/limesurvey_derive_survey_site.sql), with an
            # analyst override table. So site is left NULL here and the enrich
            # step fills it. admin is kept only as raw context.
            admin_value = row.get("admin", "")

            metadata[int(row["sid"])] = {
                "gsid": row.get("gsid"),
                "owner_id": row.get("owner_id"),
                "admin": admin_value,
                "language": row.get("language"),
                "site": None,  # derived in SQL (survey_site_map), not from admin
            }
        logger.info(
            f"Fetched metadata for {len(metadata)} surveys "
            "(site derived downstream from survey title, not admin)"
        )
        return metadata
    except Exception as e:
        logger.warning(f"Could not fetch survey metadata: {e}")
        return {}


def _melt_single_survey(
    parquet_path: str,
    survey_id: str,
    sanitized_to_original: Dict[str, str],
    output_dir: str,
    survey_metadata: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[str], int]:
    """
    Convert a single survey Parquet file into long format using DuckDB.

    Args:
        parquet_path: Path to wide-format Parquet file
        survey_id: Survey ID
        sanitized_to_original: Column name mapping
        output_dir: Output directory for long-format Parquet
        survey_metadata: Optional metadata (gsid, owner_id, admin, language) from lime_surveys table

    Returns:
        Tuple of (output_path, row_count)
    """

    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "duckdb is required to build survey_answers_long. Install via pip install duckdb"
        ) from exc

    question_columns: List[Tuple[str, str]] = []
    metadata_columns: List[Tuple[str, str]] = []

    for sanitized, original in sanitized_to_original.items():
        lowered = original.lower()
        if re.match(r"^\d+x\d+x\d+", lowered):
            question_columns.append((sanitized, lowered))
        else:
            metadata_columns.append((sanitized, lowered))

    if not question_columns:
        logger.warning("Survey %s: no question columns detected; skipping", survey_id)
        return None, 0

    # Ensure required metadata columns have sane defaults
    metadata_aliases: List[str] = []
    present_metadata: set = set()
    response_id_column = None
    for sanitized, original in metadata_columns:
        alias = original
        if original == "id":
            alias = "response_id"
            response_id_column = sanitized
        if alias.lower() in METADATA_EXCLUDE:
            continue
        present_metadata.add(alias)

        field_type = METADATA_FIELD_TYPES.get(alias)
        if field_type == "TIMESTAMP":
            expr = f'TRY_CAST(unpivoted."{sanitized}" AS TIMESTAMP)'
        elif field_type == "BIGINT":
            expr = f'TRY_CAST(unpivoted."{sanitized}" AS BIGINT)'
        else:
            # STRING-typed metadata (response_id from integer `id`, token which is
            # often all-NULL, etc.). Passing it through with its native Parquet type
            # makes the staging schema non-deterministic: response_id lands as INT64
            # and token flips INT64<->STRING depending on whether any token is set,
            # both of which mismatch the STRING production column and fail
            # schema_validation on merge. Cast to VARCHAR so the schema is stable.
            expr = f'CAST(unpivoted."{sanitized}" AS VARCHAR)'

        metadata_aliases.append(f'{expr} AS "{alias}"')

    if response_id_column is None:
        logger.warning(
            "Survey %s: did not find 'id' column; response_id will be NULL", survey_id
        )
        metadata_aliases.append('CAST(NULL AS VARCHAR) AS "response_id"')
        present_metadata.add("response_id")

    for field_name, field_type in METADATA_FIELD_TYPES.items():
        if field_name in present_metadata:
            continue
        if field_type == "TIMESTAMP":
            expr = "CAST(NULL AS TIMESTAMP)"
        elif field_type == "BIGINT":
            expr = "CAST(NULL AS BIGINT)"
        else:
            expr = "CAST(NULL AS VARCHAR)"
        metadata_aliases.append(f'{expr} AS "{field_name}"')

    column_map_values = ",\n".join(
        f"    ('{_escape_sql_literal(sanitized)}', '{_escape_sql_literal(original)}')"
        for sanitized, original in question_columns
    )

    if not column_map_values:
        return None, 0

    question_column_list = ", ".join(f'"{col}"' for col, _ in question_columns)
    metadata_select = ",\n        ".join(metadata_aliases) if metadata_aliases else ""

    temp_output = os.path.join(
        output_dir, f"survey_{survey_id}_{uuid.uuid4().hex}_long.parquet"
    )

    # The question columns are UNPIVOTed into a single answer_value column.
    # DuckDB's UNPIVOT coerces every column in the IN-list to ONE common type,
    # so if a survey's question columns are all numeric, answer_value is inferred
    # as INT/DOUBLE -- which (a) breaks the `answer_value != ''` filter with a
    # conversion error and (b) makes the written Parquet schema non-deterministic
    # vs the STRING production column (-> schema_validation failure on merge).
    # Force every question column to VARCHAR up front so the UNPIVOT's common
    # type is always VARCHAR, everywhere answer_value is referenced. UNPIVOT
    # requires bare identifiers in IN(...), so the cast must happen here, not
    # inside the UNPIVOT clause. Metadata columns keep native types (they are
    # projected separately with explicit TRY_CAST/passthrough).
    question_cast_cols = ", ".join(
        f'CAST("{col}" AS VARCHAR) AS "{col}"' for col, _ in question_columns
    )
    metadata_passthrough_cols = ", ".join(f'"{col}"' for col, _ in metadata_columns)
    survey_data_projection = question_cast_cols
    if metadata_passthrough_cols:
        survey_data_projection = f"{metadata_passthrough_cols}, {question_cast_cols}"

    conn = duckdb.connect(database=":memory:")
    rows = 0
    try:
        conn.execute("PRAGMA memory_limit='4GB'")
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE survey_data AS
            SELECT {survey_data_projection}
            FROM read_parquet('{_escape_sql_literal(parquet_path)}')
            """
        )

        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE column_map AS
            SELECT * FROM (
                VALUES
{column_map_values}
            ) AS t(sanitized, original)
            """
        )

        select_metadata = f",\n        {metadata_select}" if metadata_select else ""

        # Add survey metadata fields (tenant/site information).
        #
        # These columns are ALWAYS emitted -- even when survey_metadata is None --
        # so the output schema does not depend on which surveys in a run happen to
        # have metadata. site is intentionally NULL here; it is derived downstream in
        # SQL from the survey title (survey_site_map), not from admin. A true NULL
        # (not '') lets the enrich step's COALESCE fill it unambiguously.
        #
        # CRITICAL: every literal-or-NULL MUST be typed. A bare `NULL` is typed by
        # DuckDB as INTEGER, so a NULL site was written as an INTEGER column and
        # mismatched the STRING production `site` column -> MODIFY_TYPE
        # STRING->INTEGER, blocked, schema_validation failure (the recurring
        # 2026-06-30..07-02 break). Cast to production types:
        #   site/owner_id/admin/language -> VARCHAR (STRING), survey_gsid -> BIGINT.
        meta = survey_metadata or {}
        gsid = meta.get("gsid")
        owner_id = meta.get("owner_id", "")
        admin = meta.get("admin", "")
        language = meta.get("language", "")
        site = meta.get("site", "")

        site_literal = (
            f"'{_escape_sql_literal(str(site))}'" if site else "CAST(NULL AS VARCHAR)"
        )
        gsid_literal = (
            f"CAST({gsid} AS BIGINT)" if gsid is not None else "CAST(NULL AS BIGINT)"
        )
        survey_meta_select = f""",
        {gsid_literal} AS survey_gsid,
        CAST('{_escape_sql_literal(str(owner_id))}' AS VARCHAR) AS survey_owner_id,
        CAST('{_escape_sql_literal(str(admin))}' AS VARCHAR) AS survey_admin,
        CAST('{_escape_sql_literal(str(language))}' AS VARCHAR) AS survey_language,
        {site_literal} AS site"""

        conn.execute(
            f"""
            COPY (
                SELECT
                    CAST('{_escape_sql_literal(survey_id)}' AS BIGINT) AS survey_id,
                    column_map.original AS raw_column_name,
                    CAST(regexp_extract(column_map.original, '^\\d+x(\\d+)', 1) AS BIGINT) AS question_group_id,
                    CAST(regexp_extract(column_map.original, '^\\d+x\\d+x(\\d+)', 1) AS BIGINT) AS question_id,
                    CASE
                        WHEN lower(column_map.original) LIKE '%other' THEN 'other'
                        WHEN regexp_matches(column_map.original, 'sq\\d+$') THEN regexp_extract(column_map.original, 'sq(\\d+)$', 1)
                        ELSE NULL
                    END AS subquestion_id,
                    CAST(unpivoted.answer_value AS VARCHAR) AS answer_value
                    {select_metadata}
                    {survey_meta_select}
                FROM (
                    SELECT * FROM survey_data
                    UNPIVOT (
                        answer_value FOR sanitized_column IN ({question_column_list})
                    )
                ) AS unpivoted
                LEFT JOIN column_map ON unpivoted.sanitized_column = column_map.sanitized
                WHERE unpivoted.answer_value IS NOT NULL
                  AND unpivoted.answer_value != ''
            ) TO '{_escape_sql_literal(temp_output)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
            """
        )

        rows = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{_escape_sql_literal(temp_output)}')"
        ).fetchone()[0]
    finally:
        conn.close()

    return temp_output, rows


def _build_survey_answers_long(
    table_files: Dict[str, str],
    column_maps: Dict[str, Dict[str, str]],
    engine: Optional[sa.engine.Engine] = None,
) -> Optional[Tuple[str, int, int]]:
    """
    Build consolidated survey_answers_long Parquet from per-survey outputs.

    Args:
        table_files: Mapping of table keys to Parquet file paths
        column_maps: Column name mappings for each survey
        engine: SQLAlchemy engine (optional, for fetching survey metadata)

    Returns:
        Tuple of (output_path, total_rows, survey_count) or None
    """

    survey_entries: List[Tuple[str, str]] = []
    for table_key, file_path in table_files.items():
        if not table_key.startswith("survey_responses_"):
            continue
        if not file_path or not os.path.exists(file_path):
            continue
        survey_id = table_key.split("_")[-1]
        survey_entries.append((survey_id, file_path))

    if not survey_entries:
        return None

    # Fetch survey metadata (tenant/site information) for all surveys
    survey_metadata_map: Dict[int, Dict[str, Any]] = {}
    if engine:
        survey_ids = [int(sid) for sid, _ in survey_entries]
        survey_metadata_map = _get_survey_metadata(engine, survey_ids)

    melted_files: List[str] = []
    total_rows = 0

    for survey_id, file_path in survey_entries:
        mapping = column_maps.get(survey_id)
        if not mapping:
            logger.warning(
                "Survey %s: missing column map; skipping long-format conversion",
                survey_id,
            )
            continue
        output_dir = os.path.dirname(file_path)

        # Get survey metadata for this survey
        survey_meta = survey_metadata_map.get(int(survey_id))

        melted_path, row_count = _melt_single_survey(
            file_path, survey_id, mapping, output_dir, survey_metadata=survey_meta
        )
        if melted_path and row_count > 0:
            melted_files.append(melted_path)
            total_rows += row_count

    if not melted_files:
        return None

    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "duckdb is required to consolidate survey answers. Install via pip install duckdb"
        ) from exc

    final_output_dir = os.path.dirname(melted_files[0])
    final_output = os.path.join(
        final_output_dir, f"survey_answers_long_{uuid.uuid4().hex}.parquet"
    )

    file_list_sql = ", ".join(f"'{_escape_sql_literal(path)}'" for path in melted_files)

    conn = duckdb.connect(database=":memory:")
    try:
        conn.execute("PRAGMA memory_limit='4GB'")
        conn.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet([{file_list_sql}], union_by_name=True)
            ) TO '{_escape_sql_literal(final_output)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
            """
        )
    finally:
        conn.close()

    # Cleanup intermediate melted files
    for path in melted_files:
        try:
            os.remove(path)
        except OSError:
            pass

    for _, file_path in survey_entries:
        try:
            os.remove(file_path)
        except OSError:
            pass

    return final_output, total_rows, len(melted_files)


# ============================================================================
# SSH Tunnel Management (from WordPress)
# ============================================================================


def find_free_port() -> Tuple[int, socket.socket]:
    """
    Find a free port and return both the port number and the socket.
    Keeps socket open to prevent port reuse race condition.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    return port, s


def start_ssh_tunnel(
    ssh_host: str,
    ssh_user: str,
    ssh_keyfile: str,
    db_host: str,
    db_port: int,
    local_port: int,
    port_socket: socket.socket = None,
    use_gcloud: bool = False,
    gcloud_vm_name: str = None,
    gcloud_project: str = None,
    gcloud_zone: str = None,
    use_iap: bool = False,
) -> subprocess.Popen:
    """Start SSH tunnel for MariaDB access."""

    # Always use direct SSH with key file - works on both Windows and Linux
    # This requires the SSH key to be available on the machine running the pipeline
    # and the public key to be authorized on the target VM
    tunnel_cmd = [
        "ssh",
        "-i",
        ssh_keyfile,
        "-L",
        f"{local_port}:{db_host}:{db_port}",
        "-N",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "LogLevel=ERROR",
        "-o",
        "ConnectTimeout=30",
        "-o",
        "TCPKeepAlive=yes",
        "-o",
        "ServerAliveInterval=60",
        "-o",
        "ServerAliveCountMax=3",
        f"{ssh_user}@{ssh_host}",
    ]
    logger.info(f"Using direct SSH tunnel: {ssh_user}@{ssh_host}")

    # Start tunnel process in background (same approach as WordPress)
    # Key: Don't create console windows, run silently in background
    if sys.platform == "win32":
        # Windows: Use CREATE_NO_WINDOW to run without console (like WordPress)
        # For gcloud with PuTTY, this still allows auth but doesn't show windows
        creation_flags = (
            subprocess.CREATE_NO_WINDOW
            if hasattr(subprocess, "CREATE_NO_WINDOW")
            else 0
        )
        tunnel_process = subprocess.Popen(
            tunnel_cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creation_flags,
        )
    else:
        # Linux/Mac: Standard background process (WordPress pattern)
        tunnel_process = subprocess.Popen(
            tunnel_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

    # Close port reservation socket
    if port_socket:
        try:
            port_socket.close()
        except Exception:
            pass

    # Give tunnel time to establish (gcloud needs more time than direct SSH)
    sleep_time = 5 if use_gcloud else 2
    time.sleep(sleep_time)

    if tunnel_process.poll() is not None:
        stdout, stderr = tunnel_process.communicate(timeout=1)
        error_msg = (
            stderr.decode("utf-8", errors="ignore") if stderr else "No error message"
        )
        raise RuntimeError(f"SSH tunnel failed to start. Error: {error_msg}")

    # Wait for port to actually start listening (especially important for gcloud)
    if use_gcloud:
        logger.info(f"Waiting for port {local_port} to become available...")
        max_wait = 15  # Wait up to 15 seconds
        waited = 0
        while waited < max_wait:
            # Check if process is still running
            if tunnel_process.poll() is not None:
                # Process exited - capture error
                stdout, stderr = tunnel_process.communicate(timeout=1)
                error_msg = (
                    stderr.decode("utf-8", errors="ignore")
                    if stderr
                    else "No error message"
                )
                stdout_msg = stdout.decode("utf-8", errors="ignore") if stdout else ""
                logger.error(
                    f"gcloud SSH tunnel process exited with code {tunnel_process.returncode}"
                )
                if error_msg:
                    logger.error(f"gcloud stderr: {error_msg}")
                if stdout_msg:
                    logger.error(f"gcloud stdout: {stdout_msg}")
                raise RuntimeError(f"gcloud SSH tunnel failed: {error_msg}")

            try:
                # Try to connect to the port
                test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test_socket.settimeout(1)
                result = test_socket.connect_ex(("127.0.0.1", local_port))
                test_socket.close()
                if result == 0:
                    logger.info(f"Port {local_port} is now listening")
                    # Give it a moment more to fully initialize
                    time.sleep(2)
                    break
            except Exception:
                pass
            time.sleep(1)
            waited += 1

        if waited >= max_wait:
            # Try to get any error output from the process
            logger.warning(f"Port {local_port} not ready after {max_wait}s")
            # Check stderr if available (non-blocking)
            if tunnel_process.stderr:
                import select

                if hasattr(select, "select"):
                    readable, _, _ = select.select([tunnel_process.stderr], [], [], 0)
                    if readable:
                        partial_err = tunnel_process.stderr.read(4096)
                        if partial_err:
                            logger.warning(
                                f"gcloud partial stderr: {partial_err.decode('utf-8', errors='ignore')}"
                            )
            logger.warning("Proceeding anyway - connection may fail...")

    return tunnel_process


def stop_ssh_tunnel(tunnel_process: subprocess.Popen) -> None:
    """Stop SSH tunnel gracefully."""
    if tunnel_process:
        try:
            tunnel_process.terminate()
            tunnel_process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            tunnel_process.kill()
            tunnel_process.wait()


# ============================================================================
# Database Connection
# ============================================================================


class OptionalTunnel:
    """Context manager that conditionally opens Fabric SSH tunnel."""

    def __init__(
        self,
        ssh_conn: Optional[Connection],
        remote_host: str,
        remote_port: int,
        local_port: int,
    ):
        self.ssh_conn = ssh_conn
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.local_port = local_port
        self.tunnel_context = None

    def __enter__(self):
        if self.ssh_conn:
            logger.info(
                f"Opening SSH tunnel: localhost:{self.local_port} -> {self.remote_host}:{self.remote_port}"
            )
            self.tunnel_context = self.ssh_conn.forward_local(
                local_port=self.local_port,
                remote_port=self.remote_port,
                remote_host=self.remote_host,
            )
            self.tunnel_context.__enter__()
            logger.info("SSH tunnel established")
            # Give tunnel a moment to stabilize
            time.sleep(2)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.tunnel_context:
            logger.info("Closing SSH tunnel")
            self.tunnel_context.__exit__(exc_type, exc_val, exc_tb)
        if self.ssh_conn:
            try:
                self.ssh_conn.close()
            except:
                pass
        return False


def create_mysql_connection(
    config: Dict[str, Any],
) -> Tuple[sa.engine.Engine, Optional[Connection]]:
    """
    Create MySQL connection via SSH tunnel using Fabric (matching DBVisualizer).

    Returns:
        Tuple of (SQLAlchemy engine, Fabric Connection or None)
    """
    tunnel_config = config["source"]["connection"]["tunnel"]
    db_config = config["source"]["connection"]["database"]

    # Check if tunnel is enabled
    tunnel_enabled = tunnel_config.get("enabled", True)

    # Get DB credentials
    db_name = os.path.expandvars(db_config["name"])
    db_user = os.path.expandvars(db_config["user"])
    db_password = os.path.expandvars(db_config["password"])
    db_host = os.path.expandvars(db_config.get("host", "127.0.0.1"))
    db_port = int(os.path.expandvars(str(db_config.get("port", 3306))))

    ssh_conn = None
    tunnel_process = None

    if not tunnel_enabled:
        # Tunnel disabled - connect directly to localhost (assumes manual tunnel)
        logger.info(f"Tunnel disabled - connecting directly to {db_host}:{db_port}")
        logger.info("(Assuming SSH tunnel is already running manually)")
        port_to_use = db_port

    else:
        # Check if we should use subprocess (native SSH) or Fabric
        use_subprocess_tunnel = tunnel_config.get("use_subprocess", False)

        # Get SSH credentials
        ssh_host = os.path.expandvars(tunnel_config["host"])
        ssh_port = tunnel_config.get("port", 22)
        ssh_user = os.path.expandvars(tunnel_config["user"])
        ssh_key_path = os.path.expanduser(os.path.expandvars(tunnel_config["key_path"]))

        # Get remote database location (on the VM)
        remote_db_host = tunnel_config.get("remote_host", "127.0.0.1")
        remote_db_port = tunnel_config.get("remote_port", 3306)

        # Get local tunnel endpoint
        local_port = tunnel_config.get("local_port", 3306)

        if use_subprocess_tunnel:
            # Use native SSH subprocess (more stable, recommended)
            logger.info(
                f"Establishing SSH tunnel to {ssh_host}:{ssh_port} (using native SSH)"
            )
            logger.info(
                f"Tunnel: localhost:{local_port} -> {remote_db_host}:{remote_db_port}"
            )

            use_gcloud = tunnel_config.get("use_gcloud", False)
            use_iap = tunnel_config.get("use_iap", False)
            tunnel_process = start_ssh_tunnel(
                ssh_host=ssh_host,
                ssh_user=ssh_user,
                ssh_keyfile=ssh_key_path,
                db_host=remote_db_host,
                db_port=remote_db_port,
                local_port=local_port,
                use_gcloud=use_gcloud,
                gcloud_vm_name=tunnel_config.get("gcloud_vm_name"),
                gcloud_project=tunnel_config.get("gcloud_project"),
                gcloud_zone=tunnel_config.get("gcloud_zone"),
                use_iap=use_iap,
            )
            logger.info(
                f"SSH tunnel established (subprocess PID: {tunnel_process.pid})"
            )
            port_to_use = local_port

        else:
            # Use Fabric SSH tunnel (legacy method)
            # NOTE: Fabric SSH tunnels have limited throughput on Windows (BlockingIOError)
            # Recommend setting use_subprocess: true in config for better stability
            logger.info(
                f"Establishing SSH tunnel to {ssh_host}:{ssh_port} (using Fabric)"
            )
            logger.info(
                f"Tunnel: localhost:{local_port} -> {remote_db_host}:{remote_db_port}"
            )
            logger.warning(
                "Fabric tunnels have limited throughput - consider using 'use_subprocess: true'"
            )

            # Create Fabric SSH connection
            ssh_conn = Connection(
                host=ssh_host,
                port=ssh_port,
                user=ssh_user,
                connect_kwargs={
                    "key_filename": ssh_key_path,
                },
            )

            # Forward local port through SSH tunnel
            # Note: The tunnel is opened in a context manager during extraction
            # We'll store the connection and use it in the main extraction loop
            port_to_use = local_port

            logger.info(f"SSH connection created (tunnel will open during extraction)")

    # Create SQLAlchemy engine
    connection_string = (
        f"mysql+pymysql://{db_user}:{db_password}"
        f"@{db_host}:{port_to_use}/{db_name}"
        f"?charset=utf8mb4&connect_timeout=60&read_timeout=600&write_timeout=600"
    )

    logger.info(f"Creating MySQL connection to {db_host}:{port_to_use}/{db_name}")

    # Get max_workers from config to size connection pool appropriately
    max_workers = config.get("pipeline", {}).get("parallel", {}).get("max_workers", 5)

    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=max_workers + 2,  # Pool size = workers + 2 spare connections
        max_overflow=5,  # Allow burst capacity
        pool_timeout=60,  # Wait up to 60s for available connection
        echo=False,
        connect_args={
            "connect_timeout": 120,  # Increased from 60s
            "read_timeout": 1800,  # Increased from 600s (30 min for large queries)
            "write_timeout": 1800,  # Increased from 600s
        },
    )

    # Note: If using Fabric tunnel, we can't test connection here because
    # the tunnel isn't opened yet (it's opened in context manager)
    # Test will happen in the main extraction logic
    #
    # If using subprocess tunnel, connection is already established and ready

    return engine, ssh_conn, tunnel_process


# ============================================================================
# Table Discovery
# ============================================================================


def discover_survey_response_tables(engine: sa.engine.Engine) -> List[Tuple[int, str]]:
    """
    Auto-discover lime_survey_{sid} tables in database.

    Returns:
        List of (survey_id, table_name) tuples
    """
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name LIKE 'lime_survey_%'
      AND table_name NOT LIKE '%_timings'
      AND table_name NOT LIKE 'lime_survey_links'
      AND table_name NOT LIKE 'lime_survey_permissions'
      AND table_name NOT LIKE 'lime_survey_url_parameters'
    ORDER BY table_name
    """

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            tables = []

            for row in result:
                table_name = row[0]
                # Extract survey ID from table name (lime_survey_12345 -> 12345)
                match = re.match(r"lime_survey_(\d+)$", table_name)
                if match:
                    survey_id = int(match.group(1))
                    tables.append((survey_id, table_name))

            logger.info(f"Discovered {len(tables)} survey response tables")
            return tables

    except Exception as e:
        logger.error(f"Failed to discover survey tables: {e}")
        return []


def discover_recent_surveys(engine: sa.engine.Engine, lookback_days: int) -> List[int]:
    """Return survey IDs created within the last N days per lime_surveys.datecreated.

    Used alongside discover_survey_response_tables() to surface recently-created
    surveys that may have zero submissions yet. The caller still verifies a
    lime_survey_{sid} response table exists before extracting.
    """
    if not lookback_days or lookback_days <= 0:
        return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT sid
                    FROM lime_surveys
                    WHERE datecreated >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                    ORDER BY datecreated DESC
                    """
                ),
                {"days": int(lookback_days)},
            ).fetchall()
        sids = [int(r[0]) for r in rows]
        logger.info(
            f"Discovered {len(sids)} survey(s) with datecreated in the last "
            f"{lookback_days} days"
        )
        return sids
    except Exception as e:
        logger.warning(f"Failed to query recent surveys by datecreated: {e}")
        return []


def discover_missing_from_datamart(
    bq_client: Any,
    project: str,
    dataset: str,
) -> List[int]:
    """Return survey IDs present in BQ lime_surveys but missing from
    lime_surveys_columnar_completed — the 'never made it to the datamart' set.

    Pure BigQuery query; does not hit MariaDB. Relies on the fact that
    lime_surveys is already extracted as a core resource every pipeline run
    (see configs/limesurvey.yaml :: resources.surveys).

    Returns [] if either table is missing (fresh environment, first run).
    """
    if bq_client is None:
        logger.warning(
            "No BigQuery client available for datamart reconciliation; skipping"
        )
        return []
    try:
        sql = f"""
            SELECT s.sid
            FROM `{project}.{dataset}.lime_surveys` s
            LEFT JOIN (
                SELECT DISTINCT survey_id
                FROM `{project}.{dataset}.lime_surveys_columnar_completed`
            ) c ON c.survey_id = s.sid
            WHERE c.survey_id IS NULL
            ORDER BY s.datecreated DESC
        """
        rows = list(bq_client.query(sql).result())
        sids = [int(r.sid) for r in rows if r.sid is not None]
        logger.info(
            f"Datamart reconciliation: {len(sids)} survey(s) in lime_surveys "
            f"but absent from lime_surveys_columnar_completed"
        )
        return sids
    except Exception as e:
        # Covers NotFound (first run, tables don't exist yet) plus any other
        # transient BQ issue. Keep pipeline running.
        logger.warning(f"Datamart reconciliation query failed: {e}")
        return []


def get_latest_response_activity(
    engine: sa.engine.Engine, tables_by_sid: Dict[int, str]
) -> Optional[datetime]:
    """MAX(submitdate) across all known lime_survey_{sid} tables, in one query.

    Cheap run-level "did anything change?" signal -- one UNION ALL scan instead
    of per-survey participation counts. Returns None if no tables or on error
    (caller treats None as "unknown", never skips on unknown).
    """
    if not tables_by_sid:
        return None
    selects = " UNION ALL ".join(
        f"SELECT MAX(submitdate) AS m FROM `{t}`" for t in tables_by_sid.values()
    )
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(f"SELECT MAX(m) AS latest FROM ({selects}) x")
            ).fetchone()
        return row[0] if row else None
    except Exception as e:
        logger.warning(f"Could not compute latest response activity: {e}")
        return None


def evaluate_response_discovery_gate(
    last_gate_watermark: Optional[str],
    latest_activity: Optional[datetime],
    force: bool = False,
) -> Dict[str, Any]:
    """
    Pure decision logic (no BigQuery/MariaDB) for skipping the expensive
    per-survey participation+extraction loop when nothing changed since the
    last successful run.

    Args:
        last_gate_watermark: ISO string of the last time this gate observed
            new activity and let the loop run (from shared.watermark), or
            None on first run / unknown state.
        latest_activity: MAX(submitdate) across known survey response tables
            right now, or None if it couldn't be determined.
        force: bypass the gate unconditionally (e.g. --force CLI flag).

    Returns {"skip": bool, "reason": str}. Never skips on unknown state
    (missing watermark or unreadable activity) -- only skips when both sides
    are known and activity is not newer than the watermark.
    """
    if force:
        return {"skip": False, "reason": "forced"}
    if last_gate_watermark is None:
        return {"skip": False, "reason": "no prior watermark (first run)"}
    if latest_activity is None:
        return {"skip": False, "reason": "could not determine latest activity"}

    try:
        wm = datetime.fromisoformat(last_gate_watermark.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return {"skip": False, "reason": "unparseable watermark"}

    activity = latest_activity
    if activity.tzinfo is not None and wm.tzinfo is None:
        activity = activity.replace(tzinfo=None)
    elif activity.tzinfo is None and wm.tzinfo is not None:
        wm = wm.replace(tzinfo=None)

    if activity > wm:
        return {"skip": False, "reason": f"new activity since {wm.isoformat()}"}
    return {"skip": True, "reason": f"no activity since {wm.isoformat()}"}


def discover_token_tables(engine: sa.engine.Engine) -> List[Tuple[int, str]]:
    """
    Auto-discover lime_tokens_{sid} tables in database.

    These contain survey-specific participant tokens (PLAINTEXT, not encrypted).

    Returns:
        List of (survey_id, table_name) tuples
    """
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name LIKE 'lime_tokens_%'
    ORDER BY table_name
    """

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            tables = []

            for row in result:
                table_name = row[0]
                # Extract survey ID from table name (lime_tokens_12345 -> 12345)
                match = re.match(r"lime_tokens_(\d+)$", table_name)
                if match:
                    survey_id = int(match.group(1))
                    tables.append((survey_id, table_name))

            logger.info(f"Discovered {len(tables)} token tables")
            return tables

    except Exception as e:
        logger.error(f"Failed to discover token tables: {e}")
        return []


def extract_token_tables(
    engine: sa.engine.Engine, bq_client, dataset: str, execution_id: str = None
) -> int:
    """Extract all lime_tokens_{sid} tables from MySQL and consolidate into
    a single BigQuery table `lime_survey_tokens` with columns:
        survey_id, token, participant_id, firstname, lastname, email, ...

    INTENTIONAL standard-path exception: this loads the consolidated DataFrame
    STRAIGHT to BigQuery via load_table_from_dataframe(WRITE_TRUNCATE) (see below),
    bypassing the table_files -> GCS -> staging -> hash-merge path. That is by
    design and correct here, because:
      - it is a full-snapshot REPLACE every run (WRITE_TRUNCATE), and
      - the table is tiny and near-empty (8 rows across ~2 of 133 surveys; tokens
        are 99.99% NULL and are NOT the identity bridge -- that is columnar email),
      - and there is no `tokens` resource in configs/limesurvey.yaml for the
        standard path to merge against.
    Do not "standardize" this onto the merge path without first adding a config
    resource + schema; it would add a resource definition for an 8-row vestigial
    table for no benefit. Returns total rows loaded.
    """
    token_tables = discover_token_tables(engine)
    if not token_tables:
        logger.info("No token tables found -- skipping token extraction.")
        return 0

    import pandas as pd

    all_rows = []
    for survey_id, table_name in token_tables:
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM `{table_name}`"))
                cols = list(result.keys())
                for row in result:
                    d = dict(zip(cols, row))
                    d["survey_id"] = survey_id
                    all_rows.append(d)
        except Exception as e:
            logger.warning(f"Failed to read {table_name}: {e}")

    if not all_rows:
        logger.info("Token tables exist but contain no rows.")
        return 0

    df = pd.DataFrame(all_rows)
    # Normalize column names: tid -> token_id, token -> token, participant_id -> participant_id
    rename_map = {"tid": "token_id"}
    df.rename(columns={c: c.lower() for c in df.columns}, inplace=True)
    df.rename(columns=rename_map, inplace=True)
    # Ensure survey_id is present and typed
    df["survey_id"] = df["survey_id"].astype("Int64")
    # Keep only useful columns (token tables vary across LimeSurvey versions)
    keep = [
        "survey_id",
        "token_id",
        "token",
        "participant_id",
        "firstname",
        "lastname",
        "email",
        "language",
        "completed",
        "usesleft",
        "validfrom",
        "validuntil",
    ]
    keep = [c for c in keep if c in df.columns]
    df = df[keep]

    # Cast all to string except survey_id (BigQuery schema enforcement)
    for c in df.columns:
        if c != "survey_id":
            df[c] = df[c].astype(str).replace("None", None).replace("nan", None)

    target = f"{dataset}.lime_survey_tokens"
    from google.cloud import bigquery as bqmod

    job_config = bqmod.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_dataframe(df, target, job_config=job_config)
    job.result()
    logger.info(
        f"Loaded {len(df):,} rows into {target} from {len(token_tables)} token tables."
    )
    return len(df)


def get_survey_participation(
    engine: sa.engine.Engine, table_name: str
) -> Dict[str, int]:
    """Count respondents for a lime_survey_{sid} table.

    Returns a dict with:
      total_rows:   every row in the response table (incl. empty shells)
      participants: rows where at least one NxNxN question column is non-null/non-empty
      submitted:    rows where submitdate IS NOT NULL

    Uses the same ^[0-9]+X[0-9]+X[0-9]+ regex as _melt_single_survey() to
    identify question columns. If the table has no question columns (shouldn't
    happen in practice), participants == total_rows.
    """
    try:
        with engine.connect() as conn:
            cols = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND table_name   = :t
                      AND column_name REGEXP '^[0-9]+X[0-9]+X[0-9]+'
                    """
                ),
                {"t": table_name},
            ).fetchall()

            if not cols:
                total = (
                    conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()
                    or 0
                )
                return {
                    "total_rows": int(total),
                    "participants": int(total),
                    "submitted": 0,
                }

            q_expr = " + ".join(
                f"(CASE WHEN `{c[0]}` IS NOT NULL AND `{c[0]}` <> '' THEN 1 ELSE 0 END)"
                for c in cols
            )
            row = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) AS total_rows,
                           SUM(CASE WHEN ({q_expr}) > 0 THEN 1 ELSE 0 END) AS participants,
                           SUM(CASE WHEN submitdate IS NOT NULL THEN 1 ELSE 0 END) AS submitted
                    FROM `{table_name}`
                    """
                )
            ).fetchone()
            return {
                "total_rows": int(row[0] or 0),
                "participants": int(row[1] or 0),
                "submitted": int(row[2] or 0),
            }
    except Exception as e:
        logger.warning(f"Could not compute participation for {table_name}: {e}")
        return {"total_rows": 0, "participants": 0, "submitted": 0}


# ============================================================================
# Data Extraction
# ============================================================================


def execute_query(
    engine: sa.engine.Engine, query: str, params: Dict[str, Any] = None
) -> List[Dict[str, Any]]:
    """Execute query with retry logic."""
    from shared.error_handler import retry_on_transient_error, TransientError

    @retry_on_transient_error(max_attempts=3, base_delay=2.0)
    def _do_query():
        with engine.connect() as conn:
            # Set session timeout
            try:
                conn.execute(text("SET SESSION max_execution_time = 60000"))
            except Exception as e:
                logger.debug(f"Could not set session timeout: {e}")

            # Execute query
            try:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                rows = result.fetchall()

                if not rows:
                    return []

                return [dict(zip(result.keys(), row)) for row in rows]
            except Exception as e:
                error_str = str(e).lower()
                # Retry on transient errors
                if any(
                    keyword in error_str
                    for keyword in [
                        "timeout",
                        "connection",
                        "lock",
                        "deadlock",
                        "lost connection",
                    ]
                ):
                    raise TransientError(f"Transient database error: {e}")
                raise

    return _do_query()


def extract_limesurvey_table(
    engine: sa.engine.Engine,
    table_name: str,
    table_config: Dict[str, Any],
    config: Dict[str, Any],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    test_mode: bool = False,
) -> Iterator[Dict[str, Any]]:
    """
    Extract data from a LimeSurvey table.

    Supports both date-based and ID-based incremental strategies.
    """
    try:
        # Get primary key for ordering
        primary_key = table_config.get("primary_key", ["id"])
        order_by_field = primary_key[0] if primary_key else "id"

        # Build base query
        query = f"SELECT * FROM {table_name}"
        query_params = {}

        # Determine incremental strategy from nested block
        inc = table_config.get("incremental", {})
        inc_fields = inc.get("fields", [])
        incremental_field = inc_fields[0] if inc_fields else None
        incremental_strategy = inc.get("strategy", "date")

        # Apply filtering based on strategy
        where_clauses = []

        # HARD FILTER: Extract records with submitdate >= 2023-01-01 OR incomplete (submitdate IS NULL)
        # This applies to ALL survey response tables regardless of incremental strategy
        if (
            incremental_field and "submitdate" in incremental_field.lower()
        ) or table_name.startswith("lime_survey_"):
            min_date = datetime(2023, 1, 1)
            where_clauses.append(
                f"(submitdate >= :min_submitdate OR submitdate IS NULL)"
            )
            query_params["min_submitdate"] = min_date
            logger.info(
                f"{table_name}: Hard filter - extracting submitdate >= 2023-01-01 or incomplete (submitdate IS NULL)"
            )

        if incremental_field and start_date and incremental_strategy == "date":
            # Date-based incremental (on top of hard filter)
            where_clauses.append(f"{incremental_field} >= :start_date")
            query_params["start_date"] = start_date
            if end_date:
                where_clauses.append(f"{incremental_field} <= :end_date")
                query_params["end_date"] = end_date
            logger.info(f"{table_name}: Date-based incremental from {start_date}")

        elif incremental_strategy == "id":
            # ID-based incremental (for tables without date fields)
            # This is handled externally via get_min_id_from_lookback
            logger.info(f"{table_name}: ID-based strategy (no date filter)")

        # Add WHERE clause if needed
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        # Add ORDER BY
        query += f" ORDER BY {order_by_field}"

        # Get total count
        count_query = f"SELECT COUNT(*) as cnt FROM {table_name}"
        if where_clauses:
            count_query += " WHERE " + " AND ".join(where_clauses)

        rows = execute_query(
            engine, count_query, query_params if query_params else None
        )
        total_rows = int(rows[0]["cnt"]) if rows else 0

        logger.info(f"{table_name}: {total_rows:,} rows")

        if total_rows == 0:
            return

        # Extract data in chunks
        chunk_size = table_config.get("chunk_size", 1000)
        if test_mode:
            total_rows = min(total_rows, 100)

        offset = 0
        while offset < total_rows:
            rows_to_fetch = min(chunk_size, total_rows - offset)
            chunk_query = f"{query} LIMIT {rows_to_fetch} OFFSET {offset}"

            rows = execute_query(
                engine, chunk_query, query_params if query_params else None
            )

            for row in rows:
                # Normalize column names to lowercase
                row = {k.lower(): v for k, v in row.items()}

                # Add system tracking fields
                row["extracted_at"] = datetime.now()
                row["source"] = "limesurvey"

                # Normalize timestamp fields
                # NOTE: In LimeSurvey, 'datestamp' and 'startdate' in lime_surveys are Y/N flags,
                # not actual timestamps. Only parse actual timestamp fields.
                timestamp_fields = [
                    "submitdate",
                    "created",
                    "modified",
                    "last_login",
                    "datecreated",
                ]
                for field in timestamp_fields:
                    if field in row and row[field]:
                        value = row[field]
                        if isinstance(value, str) and value.startswith("0000-00-00"):
                            row[field] = None
                        else:
                            row[field] = parse_mysql_timestamp(value)

                yield row

            offset += chunk_size

            if test_mode and offset >= 100:
                break

            time.sleep(0.05)  # Rate limiting

    except Exception as e:
        error_str = str(e).lower()
        is_table_missing = "doesn't exist" in error_str or "1146" in error_str

        if is_table_missing:
            logger.debug(f"{table_name}: Table not found")
        else:
            logger.error(f"Failed to extract {table_name}: {e}")

        if not table_config.get("skip_on_error", True):
            raise


# ============================================================================
# Pipeline Orchestration
# ============================================================================


def extract_survey_worker(
    survey_info: Tuple[str, str, int],
    engine: Any,
    config: Dict[str, Any],
    env: Any,
    execution_id: str,
    rebuild: bool,
    refresh_mode: str,
    start_date: Optional[str],
    end_date: Optional[str],
    test_mode: bool,
    bq_client: Any,
    source: str,
    main_dataset: str,
    full_backfill_sids: Optional[set] = None,
) -> Dict[str, Any]:
    """
    Worker function to extract a single survey table.
    Thread-safe - each survey is independent.

    Args:
        survey_info: Tuple of (survey_id, table_name, row_count)
        engine: SQLAlchemy engine (thread-safe)
        config: Full pipeline configuration
        env: Pipeline environment
        execution_id: Job execution ID
        rebuild: Whether to rebuild tables
        refresh_mode: 'full' or 'incremental'
        start_date: Start date for incremental
        end_date: End date for extraction
        test_mode: Test mode flag
        bq_client: BigQuery client
        source: Source name (limesurvey)
        main_dataset: BigQuery dataset name

    Returns:
        Dict with extraction results
    """
    import threading

    survey_id, table_name, row_count = survey_info
    table_key = f"survey_responses_{survey_id}"

    thread_name = threading.current_thread().name
    logger.info(f"[{thread_name}] Processing {table_name} ({row_count:,} rows)")

    try:
        table_config = config["resources"].get(table_key, {})

        # Determine start date for incremental
        table_start_date = start_date

        # Reconciliation/catch-up surveys (never landed in the datamart) need
        # the full history, not just the lookback window. Clearing start_date
        # leaves only the 2023-01-01 hard floor from extract_limesurvey_table.
        if full_backfill_sids and survey_id in full_backfill_sids:
            logger.info(
                f"[{thread_name}] {table_name}: full-history backfill "
                f"(reconciliation/datecreated sid); ignoring lookback window"
            )
            table_start_date = None
        elif refresh_mode != "full":
            inc = table_config.get("incremental", {})
            inc_fields = inc.get("fields", [])
            incremental_field = inc_fields[0] if inc_fields else None
            incremental_strategy = inc.get("strategy", "date")

            if incremental_field and incremental_strategy == "date":
                if not table_start_date:
                    table_start_date = inc.get("initial_value", "2020-01-01")
                logger.debug(
                    f"[{thread_name}] Date-based incremental: {table_start_date} to {end_date}"
                )
            elif incremental_strategy == "id":
                logger.debug(
                    f"[{thread_name}] ID-based incremental: extracting all rows"
                )
                table_start_date = None

        # Extract data
        all_data = []
        for record in extract_limesurvey_table(
            engine=engine,
            table_name=table_name,
            table_config=table_config,
            config=config,
            start_date=table_start_date,
            end_date=end_date,
            test_mode=test_mode,
        ):
            all_data.append(record)

        table_rows = len(all_data)

        if table_rows > 0:
            # Add execution metadata before sanitizing
            set_execution_metadata(all_data, execution_id, source=source)

            sanitized_records, _, sanitized_to_original = _sanitize_records(all_data)

            # Apply schema prefix/suffix
            table_with_affix = env.format_table(table_key)
            main_table = f"{env.project}.{main_dataset}.{table_with_affix}"

            # Write to local Parquet
            local_file_path = extract_to_local_parquet(
                data=sanitized_records,
                source=source,
                table=table_key,
                job_id=execution_id,
                bq_client=bq_client,
                production_table_id=main_table,
                rebuild_mode=rebuild,
            )

            return {
                "survey_id": survey_id,
                "table_name": table_name,
                "status": "success",
                "rows": table_rows,
                "parquet_file": local_file_path,
                "column_map": sanitized_to_original,
            }
        else:
            return {
                "survey_id": survey_id,
                "table_name": table_name,
                "status": "no_data",
                "rows": 0,
                "parquet_file": None,
                "column_map": {},
            }

    except Exception as e:
        logger.error(f"[{thread_name}] Failed {table_name}: {e}")
        return {
            "survey_id": survey_id,
            "table_name": table_name,
            "status": "error",
            "error": str(e),
            "rows": 0,
            "parquet_file": None,
            "column_map": {},
        }


def run_pipeline(
    config: Dict[str, Any],
    sites: List[str],
    tables: List[str],
    group: Optional[str],
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    end_date: Optional[str],
    test_mode: bool,
    batch_size: Optional[int],
    max_retries: int,
    skip_hash_merge: bool = False,
    archive_staging: bool = False,
    truncate_staging: bool = False,
    rebuild: bool = False,
    bq_client: Any = None,
    execution_id: str = None,
    parallel_workers: Optional[int] = None,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
    survey_limit: Optional[int] = None,
    survey_created_days: Optional[int] = None,
    no_reconcile: bool = False,
    **kwargs,
) -> Dict[str, Any]:
    """
    LimeSurvey extraction pipeline.

    Single instance (no multi-tenancy like WordPress), but supports
    auto-discovery of survey response tables.

    Discovery sources (unioned before participation filter):
      * information_schema scan for lime_survey_{sid} tables (always on)
      * lime_surveys.datecreated >= today - survey_created_days
        (defaults to configs/limesurvey.yaml :: discovery.survey_created_lookback_days)
      * Surveys in BQ lime_surveys but missing from lime_surveys_columnar_completed
        (disabled via no_reconcile=True or discovery.reconcile_with_datamart: false)
    """

    engine = None
    ssh_conn = None
    tunnel_process = None

    try:
        # Calculate date range
        if not start_date and lookback_days and refresh_mode != "full":
            start_date = (datetime.now() - timedelta(days=lookback_days)).strftime(
                "%Y-%m-%d"
            )
            logger.info(
                f"Using lookback of {lookback_days} days: start_date={start_date}"
            )

        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # Get tables to extract
        if not tables:
            if group:
                tables = get_available_tables(config, group)
                logger.info(f"Using {group} group: {len(tables)} tables")
            else:
                tables = get_available_tables(config)
                logger.info(f"Using all tables: {len(tables)}")

        # Initialize pipeline environment
        env = initialize_pipeline_environment(
            config,
            bq_client=bq_client,
            source_default="limesurvey",
            schema_prefix=schema_prefix,
            schema_suffix=schema_suffix,
            execution_id=execution_id,
            rebuild=rebuild,
            require_bucket=False,
        )

        source = env.source_name
        main_dataset = env.production_dataset
        run_execution_id = env.execution_id or execution_id

        # Connect to database
        logger.info("\nConnecting to LimeSurvey database...")
        engine, ssh_conn, tunnel_process = create_mysql_connection(config)

        # Get tunnel config for Fabric context manager
        tunnel_config = config["source"]["connection"]["tunnel"]
        tunnel_enabled = tunnel_config.get("enabled", True)
        remote_db_host = tunnel_config.get("remote_host", "127.0.0.1")
        remote_db_port = tunnel_config.get("remote_port", 3306)
        local_port = tunnel_config.get("local_port", 3306)

        # Open SSH tunnel (if enabled and using Fabric)
        # Note: If using subprocess tunnel, it's already open
        with OptionalTunnel(ssh_conn, remote_db_host, remote_db_port, local_port):
            # Test connection
            logger.info("Testing database connection...")
            with engine.connect() as conn:
                result = conn.execute(text("SELECT VERSION(), DATABASE()"))
                row = result.fetchone()
                logger.info(f"Connected to: {row[1]}")
                logger.info(f"MariaDB version: {row[0]}")

            # Auto-discover survey response tables if responses group requested
            # Handle both singular 'group' and plural 'groups' parameters
            groups = kwargs.get("groups", []) or []
            should_discover_responses = (
                group == "responses"
                or "responses" in groups
                or (
                    not group and not groups
                )  # No group specified = run all including responses
            )
            survey_info = []
            if should_discover_responses:
                logger.info("\nAuto-discovering survey response tables...")
                discovery_cfg = config.get("discovery", {}) or {}
                require_participants = discovery_cfg.get("require_participants", True)

                # Source 1: scan for lime_survey_{sid} tables (always on).
                scan_tables = discover_survey_response_tables(engine)
                scan_sids = {sid for sid, _ in scan_tables}
                tables_by_sid = dict(scan_tables)

                # Source 2: surveys created within the last N days per
                # lime_surveys.datecreated (surfaces recently-created surveys
                # the scan missed or whose row counts would otherwise be 0).
                datecreated_window_days = (
                    survey_created_days
                    if survey_created_days is not None
                    else discovery_cfg.get("survey_created_lookback_days", 30)
                )
                recent_sids = set(
                    discover_recent_surveys(engine, datecreated_window_days)
                )

                # Source 3: surveys in BQ lime_surveys that never landed in
                # lime_surveys_columnar_completed (the datamart catch-up set).
                reconcile_enabled = not no_reconcile and discovery_cfg.get(
                    "reconcile_with_datamart", True
                )
                reconcile_sids: set = set()
                if reconcile_enabled:
                    reconcile_sids = set(
                        discover_missing_from_datamart(
                            bq_client=bq_client or env.bq_client,
                            project=env.project,
                            dataset=main_dataset,
                        )
                    )

                # Union all three, resolve each sid to its response table name.
                all_sids = scan_sids | recent_sids | reconcile_sids
                added_from_recent = recent_sids - scan_sids
                added_from_reconcile = reconcile_sids - scan_sids - recent_sids
                logger.info(
                    f"Discovery union: scan={len(scan_sids)}, "
                    f"datecreated<={datecreated_window_days}d={len(recent_sids)} "
                    f"(+{len(added_from_recent)} new), "
                    f"reconcile={len(reconcile_sids)} "
                    f"(+{len(added_from_reconcile)} new); total unique={len(all_sids)}"
                )

                # Run-level "anything new?" gate: skip the expensive per-survey
                # participation+extraction loop for ALREADY-KNOWN surveys when
                # no response has landed since the last time this gate saw new
                # activity. New/reconciled surveys (recent_sids, reconcile_sids)
                # are never gated -- they wouldn't show up in an old survey's
                # MAX(submitdate) scan. Bypassed by --force or when the caller
                # asked for specific tables/group (should_discover_responses
                # already implies group in (None, 'responses')).
                from shared.watermark import get_watermark, save_watermark

                gate_enabled = discovery_cfg.get("skip_if_no_new_responses", True)
                gate_force = bool(kwargs.get("force", False))
                gate = {"skip": False, "reason": "gate disabled"}
                latest_activity = None
                if gate_enabled:
                    gate_watermark = get_watermark(
                        bq_client or env.bq_client,
                        main_dataset,
                        source,
                        "responses_gate",
                    )
                    latest_activity = get_latest_response_activity(
                        engine, tables_by_sid
                    )
                    gate = evaluate_response_discovery_gate(
                        gate_watermark, latest_activity, force=gate_force
                    )
                logger.info(
                    f"Response discovery gate: skip={gate['skip']} ({gate['reason']})"
                )

                new_or_backfill_sids = recent_sids | reconcile_sids
                if gate["skip"]:
                    all_sids = new_or_backfill_sids
                    logger.info(
                        f"Gate active: limiting to {len(all_sids)} new/reconciled "
                        f"survey(s); skipping participation+extraction for the "
                        f"{len(scan_sids - new_or_backfill_sids)} unchanged existing "
                        f"survey(s)"
                    )
                elif latest_activity is not None:
                    save_watermark(
                        bq_client or env.bq_client,
                        main_dataset,
                        source,
                        "responses_gate",
                        run_execution_id or execution_id,
                        0,
                    )

                survey_tables = []
                missing_response_tables: List[int] = []
                for sid in sorted(all_sids):
                    table_name = tables_by_sid.get(sid) or f"lime_survey_{sid}"
                    if sid not in scan_sids:
                        # Sid came from datecreated window or reconciliation;
                        # verify the lime_survey_{sid} response table actually
                        # exists in source before queuing extraction.
                        try:
                            with engine.connect() as conn:
                                exists = conn.execute(
                                    text(
                                        """
                                        SELECT 1 FROM information_schema.tables
                                        WHERE table_schema = DATABASE()
                                          AND table_name   = :t
                                        LIMIT 1
                                        """
                                    ),
                                    {"t": table_name},
                                ).fetchone()
                            if not exists:
                                missing_response_tables.append(sid)
                                continue
                        except Exception as e:
                            logger.warning(
                                f"Could not verify response table for sid={sid}: {e}"
                            )
                            continue
                    survey_tables.append((sid, table_name))

                if missing_response_tables:
                    logger.info(
                        f"Skipping {len(missing_response_tables)} survey(s) with no "
                        f"lime_survey_{{sid}} response table in source "
                        f"(sample sids: {missing_response_tables[:5]})"
                    )

                # Reconciliation sids have never landed in
                # lime_surveys_columnar_completed, so they need the full
                # history (not just the lookback window). This includes
                # reconciliation sids that the scan also found — their
                # lime_survey_{sid} table exists, but normal incremental
                # would only pull the last N days. Similarly, sids surfaced
                # only by the datecreated window (i.e. not in the scan at
                # all but the response table exists) should backfill.
                full_backfill_sids = reconcile_sids | (recent_sids - scan_sids)
                if full_backfill_sids:
                    logger.info(
                        f"Full-history backfill planned for {len(full_backfill_sids)} "
                        f"survey(s) (sample sids: {sorted(full_backfill_sids)[:5]})"
                    )

                skipped_empty = 0
                for survey_id, table_name in survey_tables:
                    part = get_survey_participation(engine, table_name)
                    total_rows = part["total_rows"]
                    participants = part["participants"]
                    submitted = part["submitted"]

                    if require_participants and participants == 0:
                        logger.debug(
                            f"Skipping {table_name}: participants=0 "
                            f"(total_rows={total_rows}, submitted={submitted})"
                        )
                        skipped_empty += 1
                        continue

                    # Sort-by-size still uses total_rows for load balancing,
                    # matching the prior behavior.
                    survey_info.append((survey_id, table_name, total_rows))

                if skipped_empty:
                    logger.info(
                        f"Skipped {skipped_empty} survey(s) with zero participants"
                    )

                # Sort by size (largest first) for load balancing
                survey_info.sort(key=lambda x: x[2], reverse=True)

                # Apply survey limit for testing
                if survey_limit and survey_limit > 0:
                    total_surveys = len(survey_info)
                    total_rows = sum(s[2] for s in survey_info)

                    logger.warning(f"\n{'=' * 60}")
                    logger.warning(f"TEST MODE: Survey limit active")
                    logger.warning(f"{'=' * 60}")
                    logger.warning(f"Total surveys discovered: {total_surveys}")
                    logger.warning(f"Total rows available: {total_rows:,}")
                    logger.warning(f"Limiting to: {survey_limit} surveys")

                    survey_info = survey_info[:survey_limit]
                    limited_rows = sum(s[2] for s in survey_info)
                    logger.warning(
                        f"Processing: {len(survey_info)} surveys, {limited_rows:,} rows"
                    )
                    logger.warning(f"{'=' * 60}\n")

                # Create config resources for each survey (log summary only)
                logger.info(f"Registering {len(survey_info)} surveys for extraction")
                for survey_id, table_name, row_count in survey_info:
                    table_key = f"survey_responses_{survey_id}"
                    if table_key not in config["resources"]:
                        # Create dynamic resource config
                        config["resources"][table_key] = {
                            "table_name": table_name,
                            "group": "responses",
                            "primary_key": ["id"],
                            "incremental": {
                                "strategy": "date",
                                "fields": ["submitdate"],
                            },
                            "skip_on_error": True,
                            "chunk_size": 1000,
                        }
                        tables.append(table_key)

            # B1: the standard OUTPUT axis (table_files/total_rows/successful_tables)
            # is recorded via the shared accumulator (shared/extraction_result).
            # The survey-specific INTERMEDIATE state (per-survey input parquet files
            # and column maps, used only to build the consolidated columnar table)
            # stays in a local dict -- it is not part of the standard return and is
            # dropped before returning. The unique wide->long melt logic is unchanged.
            from shared.extraction_result import StandardExtractionResult

            acc = StandardExtractionResult(
                source=source,
                execution_id=run_execution_id or execution_id,
                job_id=run_execution_id or execution_id,
                bq_client=bq_client,
            )
            # Survey-axis intermediates (not part of the standard return shape).
            survey_state = {
                "survey_column_maps": {},
                "survey_input_files": {},
            }
            total_survey_rows = 0

            # Separate survey response tables from other tables
            survey_response_tables = [
                t for t in tables if t.startswith("survey_responses_")
            ]
            other_tables = [t for t in tables if not t.startswith("survey_responses_")]

            # Get max_workers from config or use default
            if parallel_workers is None:
                parallel_workers = (
                    config.get("pipeline", {})
                    .get("parallel", {})
                    .get("max_workers", 10)
                )

            # Process survey responses with parallel workers (if any)
            if survey_response_tables and survey_info:
                logger.info(f"\n{'=' * 60}")
                logger.info(
                    f"Parallel Extraction: {len(survey_response_tables)} surveys"
                )
                logger.info(f"Workers: {parallel_workers}")
                logger.info(f"{'=' * 60}\n")

                # Full-history backfill set from the discovery-union step
                # (may be empty if that block didn't execute; use setdefault).
                _full_backfill = locals().get("full_backfill_sids", set()) or set()

                # Use ThreadPoolExecutor for parallel extraction
                with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                    # Submit all survey extraction tasks
                    future_to_survey = {
                        executor.submit(
                            extract_survey_worker,
                            survey,
                            engine,
                            config,
                            env,
                            run_execution_id or execution_id,
                            rebuild,
                            refresh_mode,
                            start_date,
                            end_date,
                            test_mode,
                            bq_client,
                            source,
                            main_dataset,
                            _full_backfill,
                        ): survey
                        for survey in survey_info
                    }

                    completed = 0
                    failed = 0
                    no_data = 0
                    # Process completed futures
                    for future in as_completed(future_to_survey):
                        survey = future_to_survey[future]
                        completed += 1

                        try:
                            result = future.result()

                            if result["status"] == "success":
                                total_survey_rows += result["rows"]
                                table_key = f"survey_responses_{result['survey_id']}"
                                survey_state["survey_input_files"][table_key] = result[
                                    "parquet_file"
                                ]
                                if result.get("column_map"):
                                    survey_state["survey_column_maps"][
                                        str(result["survey_id"])
                                    ] = result["column_map"]
                                # Per-survey rows are intermediate; the consolidated
                                # columnar table is what gets recorded (see below).
                                # Track the running survey-row total locally so the
                                # columnar step can swap it for the merged long-row
                                # count.
                                logger.info(
                                    f"[OK] [{completed}/{len(survey_info)}] "
                                    f"Survey {result['survey_id']}: {result['rows']:,} rows"
                                )
                            elif result["status"] == "no_data":
                                no_data += 1
                                logger.info(
                                    f"o [{completed}/{len(survey_info)}] "
                                    f"Survey {result['survey_id']}: No data"
                                )
                            else:
                                failed += 1
                                logger.error(
                                    f"[FAIL] [{completed}/{len(survey_info)}] "
                                    f"Survey {result['survey_id']}: {result.get('error', 'Unknown error')}"
                                )

                        except Exception as e:
                            failed += 1
                            logger.error(
                                f"[FAIL] [{completed}/{len(survey_info)}] "
                                f"Survey {survey[0]}: {e}"
                            )

                logger.info(f"\n{'=' * 60}")
                logger.info(f"Parallel Extraction Complete")
                logger.info(f"{'=' * 60}")
                logger.info(f"  Surveys: {completed} total")
                logger.info(f"  Succeeded: {completed - failed - no_data}")
                logger.info(f"  No data: {no_data}")
                logger.info(f"  Failed: {failed}")
                logger.info(f"  Total rows: {total_survey_rows:,}")
                logger.info(f"{'=' * 60}\n")

            if survey_state["survey_input_files"]:
                build_result = _build_survey_answers_long(
                    survey_state["survey_input_files"],
                    survey_state["survey_column_maps"],
                    engine=engine,
                )
                if build_result:
                    long_path, long_rows, merged_surveys = build_result
                    # The consolidated columnar table is the real survey output
                    # (per-survey rows were intermediate and never counted into the
                    # accumulator). Record it under the logical name
                    # 'lime_surveys_columnar' (matches the SQL post-process).
                    acc.record_prewritten("lime_surveys_columnar", long_path, long_rows)
                    logger.info(
                        "lime_surveys_columnar: consolidated %d surveys into %d rows",
                        merged_surveys,
                        long_rows,
                    )

            # Extract per-survey token tables → consolidated lime_survey_tokens.
            # This is a direct WRITE_TRUNCATE to BigQuery (out-of-band of the standard
            # transform), so it must honor extract-only / skip-transform -- otherwise
            # a "no data update" run would still rewrite the token table.
            _no_write = kwargs.get("extract_only", False) or kwargs.get(
                "skip_transform", False
            )
            if should_discover_responses and not _no_write:
                try:
                    token_rows = extract_token_tables(
                        engine,
                        bq_client or env.bq_client,
                        main_dataset,
                        execution_id=run_execution_id,
                    )
                    if token_rows:
                        logger.info(
                            f"Token extraction: {token_rows:,} rows loaded into lime_survey_tokens"
                        )
                except Exception as e:
                    logger.warning(f"Token table extraction failed (non-critical): {e}")
            elif should_discover_responses and _no_write:
                logger.info(
                    "Skipping token table direct-load (extract-only/skip-transform: "
                    "no BigQuery writes)"
                )

            # Process other tables sequentially (metadata tables)
            if other_tables:
                logger.info(
                    f"\nExtracting {len(other_tables)} metadata tables sequentially"
                )

            for table in other_tables:
                table_config = config["resources"].get(table, {})
                if not table_config.get("active", True):
                    logger.info(f"Table '{table}' is marked as active: false, skipping")
                    continue

                table_name = table_config.get("table_name", table)

                logger.info(f"\n{'=' * 60}")
                logger.info(f"Processing table: {table_name}")
                logger.info(f"{'=' * 60}")

                # Determine start date
                table_start_date = start_date

                if refresh_mode != "full":
                    inc = table_config.get("incremental", {})
                    inc_fields = inc.get("fields", [])
                    incremental_field = inc_fields[0] if inc_fields else None
                    incremental_strategy = inc.get("strategy", "date")

                    if incremental_field and incremental_strategy == "date":
                        if not table_start_date:
                            table_start_date = inc.get("initial_value", "2020-01-01")
                        logger.info(
                            f"Date-based incremental: {table_start_date} to {end_date}"
                        )

                    elif incremental_strategy == "id":
                        logger.info(
                            "ID-based incremental: extracting all rows (deduplication in BigQuery)"
                        )
                        table_start_date = None

                # Extract data
                all_data = []

                try:
                    for record in extract_limesurvey_table(
                        engine=engine,
                        table_name=table_name,
                        table_config=table_config,
                        config=config,
                        start_date=table_start_date,
                        end_date=end_date,
                        test_mode=test_mode,
                    ):
                        all_data.append(record)

                except Exception as e:
                    logger.error(f"Failed to extract {table_name}: {e}")
                    continue

                table_rows = len(all_data)
                logger.info(f"Extracted {table_rows:,} rows")

                if table_rows > 0:
                    # Add execution metadata
                    set_execution_metadata(
                        all_data, run_execution_id or execution_id, source=source
                    )

                    # Apply schema prefix/suffix
                    table_with_affix = env.format_table(table)
                    main_table = f"{env.project}.{main_dataset}.{table_with_affix}"

                    # Write to local Parquet
                    logger.info("Writing to local Parquet...")
                    try:
                        local_file_path = extract_to_local_parquet(
                            data=all_data,
                            source=source,
                            table=table,
                            job_id=run_execution_id or execution_id,
                            bq_client=bq_client,
                            production_table_id=main_table,
                            rebuild_mode=rebuild,
                        )

                        if local_file_path:
                            acc.record_prewritten(table, local_file_path, table_rows)
                            logger.info(f"Parquet file created: {local_file_path}")

                    except Exception as e:
                        logger.error(f"Failed to write Parquet: {e}")
                        raise
                else:
                    logger.info("No data extracted")

                logger.info(f"Table {table_name} complete")

            # Log summary
            logger.info(f"\n{'=' * 60}")
            logger.info("LimeSurvey Extraction Complete")
            logger.info(f"{'=' * 60}")
            logger.info(f"Total rows: {acc.total_rows:,}")
            logger.info(f"Successful tables: {acc.successful_tables}/{len(tables)}")
            logger.info(f"Parquet files: {len(acc.table_files)}")
            logger.info(f"{'=' * 60}\n")

        # Standard shape from the accumulator; `tables` preserves the historical
        # REQUESTED count (not successful). survey_state is intermediate-only and
        # is intentionally NOT returned.
        final = acc.finalize()
        final["tables"] = len(tables)
        return final

    finally:
        # Cleanup connections
        if engine:
            engine.dispose()
        # SSH connection cleanup handled by OptionalTunnel context manager
        # Subprocess tunnel cleanup
        if tunnel_process:
            logger.info("Closing subprocess SSH tunnel")
            stop_ssh_tunnel(tunnel_process)


# ============================================================================
# CLI Entry Point
# ============================================================================

if __name__ == "__main__":
    """Standalone entry point for quick testing."""

    import argparse
    import yaml
    from dotenv import load_dotenv
    from shared.log_config import configure_logging

    parser = argparse.ArgumentParser(description="Run LimeSurvey extractor")
    parser.add_argument(
        "--survey-limit",
        type=int,
        default=None,
        help="Limit number of surveys to extract (testing)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help="Tables to process (defaults to metadata smoke test)",
    )
    parser.add_argument("--group", default=None, help="Table group to process")
    parser.add_argument(
        "--refresh",
        choices=["full", "incremental"],
        default="full",
        help="Refresh mode",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=None,
        help="Lookback window in days for incremental mode",
    )
    parser.add_argument("--start-date", default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Enable extractor test mode constraints",
    )
    parser.add_argument(
        "--parallel", type=int, default=None, help="Override parallel worker count"
    )
    parser.add_argument(
        "--survey-created-days",
        type=int,
        default=None,
        help="Surface surveys where lime_surveys.datecreated >= today - N days "
        "(default: configs/limesurvey.yaml :: discovery.survey_created_lookback_days)",
    )
    parser.add_argument(
        "--no-reconcile",
        action="store_true",
        help="Disable the BQ reconciliation pass that includes surveys present in "
        "lime_surveys but missing from lime_surveys_columnar_completed",
    )

    args = parser.parse_args()

    load_dotenv()
    configure_logging(level="normal")

    config_path = Path(__file__).parent.parent / "configs" / "limesurvey.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    tables_arg = args.tables
    if not tables_arg and not args.group:
        tables_arg = ["surveys", "questions"]

    stats = run_pipeline(
        config=config,
        sites=[],
        tables=tables_arg,
        group=args.group,
        refresh_mode=args.refresh,
        lookback_days=args.lookback,
        start_date=args.start_date,
        end_date=args.end_date,
        test_mode=args.test_mode,
        batch_size=None,
        max_retries=3,
        rebuild=False,
        bq_client=None,
        execution_id=str(uuid.uuid4()),
        parallel_workers=args.parallel,
        survey_limit=args.survey_limit,
        survey_created_days=args.survey_created_days,
        no_reconcile=args.no_reconcile,
    )

    print(f"\nRun complete: {stats}")
