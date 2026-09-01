"""
Standard extraction result accumulator (backlog B1).

Every standard extractor repeats the same per-table bookkeeping: stamp system
metadata onto records, write them to a local Parquet file, record
table_files[logical] = path and table_rows[logical] = count, and bump
success/failure counters -- then assemble a return dict. The shapes drifted
between plugins (some emit None paths, some omit table_status, some inconsistent
keys), and forgetting a step caused real bugs (e.g. the Phase 2B rebuild failure
when execution_id was not stamped before the Parquet write).

StandardExtractionResult centralizes that so producing the correct shape is
structural, not per-plugin discipline. It generalizes the two proven prototypes:
plugins/mailchimp_extractor._record_for_standard_path and
plugins/instagram_extractor._process_records_to_parquet.

Contract produced (matches shared/extraction_contract.validate_table_files):
  - table_files keyed by LOGICAL resource name (the orchestrator applies affixes
    and looks up config.resources[key]); NEVER a None value (skipped tables are
    simply absent), so it is B2-clean by construction.
  - finalize() -> {total_rows, table_files, table_rows, table_status,
    successful_tables, failed_tables, tables, errors}

Source-specific record SHAPING (timestamp normalization, JSON-serialization,
string-id coercion) stays in the plugin: pass already-shaped records, or a
`prepare` callable that the accumulator applies before the Parquet write.
"""

import logging
import os
import shutil
import tempfile
from typing import Any, Callable, Dict, List, Optional

from shared.account_context import set_execution_metadata
from shared.gcs_pipeline import extract_to_local_parquet

logger = logging.getLogger(__name__)

# Per-table status values (kept in sync with the orchestrator summary).
STATUS_SUCCESS = "success"
STATUS_ZERO_ROWS = "zero_rows"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"
# A table whose extraction hit API rate limits -- distinct from failed (it is a
# transient "retry later" outcome, not a hard error) and from zero_rows (data
# may exist but could not be fetched). Facebook records this so the orchestrator
# does not mislabel rate-limited tables as failures.
STATUS_RATE_LIMITED = "rate_limited"


class StandardExtractionResult:
    """
    Accumulates per-table extraction outcomes and finalizes the standard return
    dict. One instance per run_pipeline call.

    Usage:
        result = StandardExtractionResult(source="instagram", execution_id=eid,
                                          bq_client=client, sites=sites)
        for table in tables:
            records = extract(table)
            result.record_table(table, records=records, prepare=_shape)
        return result.finalize()
    """

    def __init__(
        self,
        *,
        source: str,
        execution_id: Optional[str] = None,
        job_id: Optional[str] = None,
        bq_client: Any = None,
        sites: Optional[List[Any]] = None,
    ):
        self.source = source
        # extract_to_local_parquet uses job_id as the parquet file id; fall back
        # to execution_id so callers can pass just one.
        self.execution_id = execution_id
        self.job_id = job_id or execution_id
        self.bq_client = bq_client
        self.sites = sites or []

        self.table_files: Dict[str, str] = {}
        self.table_rows: Dict[str, int] = {}
        self.table_status: Dict[str, str] = {}
        self.errors: List[Dict[str, Any]] = []
        self.total_rows = 0
        self.successful_tables = 0
        self.failed_tables = 0

    # -- recording -----------------------------------------------------------

    def record_table(
        self,
        logical_table: str,
        *,
        records: Optional[List[Dict[str, Any]]] = None,
        parquet_path: Optional[str] = None,
        row_count: Optional[int] = None,
        prepare: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
        stamp_metadata: bool = True,
        production_table_id: Optional[str] = None,
        rebuild_mode: bool = False,
        local_temp_dir: Optional[str] = None,
        authoritative_schema: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Record one successfully-extracted table for the standard load path.

        Exactly one of `records` / `parquet_path` must be given.
          - records: in-memory rows. The accumulator stamps system metadata
            (execution_id/source/extracted_at) BEFORE writing Parquet -- the step
            whose omission caused the Phase 2B rebuild bug -- then writes a local
            Parquet file. An optional `prepare(records)` runs first for
            source-specific shaping (timestamps, json, string ids).
          - parquet_path: a pre-built Parquet file (e.g. a Batch/DuckDB-merged
            file). It is COPIED to an orchestrator-owned durable temp path so it
            survives any per-call temp-dir cleanup before the orchestrator
            uploads it. row_count should be supplied (the rows aren't in memory).

        An empty/zero-row table records as zero_rows (success, no table_files
        entry) rather than emitting a None path.
        """
        if (records is None) == (parquet_path is None):
            raise ValueError(
                "record_table requires exactly one of records / parquet_path"
            )

        if records is not None:
            if not records:
                self.skip_table(logical_table, reason=STATUS_ZERO_ROWS)
                return
            if prepare is not None:
                prepare(records)
            if stamp_metadata:
                set_execution_metadata(
                    records, self.execution_id or self.job_id, source=self.source
                )
            local_path = extract_to_local_parquet(
                data=records,
                source=self.source,
                table=logical_table,
                job_id=self.job_id,
                bq_client=self.bq_client,
                production_table_id=production_table_id,
                rebuild_mode=rebuild_mode,
                local_temp_dir=local_temp_dir,
            )
            if not local_path:
                # Writer produced nothing (treated as no data).
                self.skip_table(logical_table, reason=STATUS_ZERO_ROWS)
                return
            durable_path = local_path
            n = row_count if row_count is not None else len(records)
        else:
            # Prebuilt parquet: copy to a durable orchestrator-owned location.
            fd, durable_path = tempfile.mkstemp(
                prefix=f"{self.source}_std_{logical_table}_", suffix=".parquet"
            )
            os.close(fd)
            shutil.copyfile(parquet_path, durable_path)
            n = row_count if row_count is not None else 0

        self.table_files[logical_table] = durable_path
        self.table_rows[logical_table] = n
        self.table_status[logical_table] = STATUS_SUCCESS
        self.total_rows += n
        self.successful_tables += 1

        # Optional YAML schema auto-update (preserves the behavior the legacy
        # execute_full_pipeline did for mailchimp). Best-effort: never fatal.
        if authoritative_schema:
            self._update_yaml_schema(logical_table, authoritative_schema)

    def _update_yaml_schema(
        self, logical_table: str, authoritative_schema: Dict[str, str]
    ) -> None:
        """Write newly-discovered columns back into the source's config YAML."""
        try:
            from pathlib import Path

            from shared.gcs_pipeline import update_yaml_schema_selective

            config_path = (
                Path(__file__).resolve().parents[1] / "configs" / f"{self.source}.yaml"
            )
            if config_path.exists():
                update_yaml_schema_selective(
                    str(config_path),
                    logical_table,
                    authoritative_schema,
                    "streaming_autodiscovery",
                )
        except Exception as yaml_err:  # noqa: BLE001 - schema write is best-effort
            logger.warning(
                "Could not auto-update YAML schema for %s: %s", logical_table, yaml_err
            )

    def record_prewritten(
        self, logical_table: str, local_path: Optional[str], row_count: int
    ) -> None:
        """
        Record a table whose Parquet file the PLUGIN already wrote itself (the
        plugin owns the durable path; not copied). `local_path` falsy or
        row_count 0 -> recorded as zero_rows (no table_files entry), keeping the
        result B2-clean. For migrating extractors that keep their own
        record-shaping+write helper but want standardized stats/return.
        """
        if not local_path or row_count <= 0:
            self.skip_table(logical_table, reason=STATUS_ZERO_ROWS)
            return
        self.table_files[logical_table] = local_path
        self.table_rows[logical_table] = row_count
        self.table_status[logical_table] = STATUS_SUCCESS
        self.total_rows += row_count
        self.successful_tables += 1

    def skip_table(self, logical_table: str, *, reason: str = STATUS_ZERO_ROWS) -> None:
        """
        Record a table that produced no data. Counts as a (non-failed) outcome,
        sets table_rows=0, and records NO table_files entry (so the result stays
        B2-clean -- no None paths). reason is zero_rows (default) or skipped.
        """
        self.table_rows.setdefault(logical_table, 0)
        self.table_status[logical_table] = (
            reason if reason in (STATUS_ZERO_ROWS, STATUS_SKIPPED) else STATUS_ZERO_ROWS
        )
        # zero_rows historically counted toward successful_tables in conformers;
        # preserve that so summaries match.
        if reason == STATUS_ZERO_ROWS:
            self.successful_tables += 1

    def fail_table(
        self,
        logical_table: str,
        *,
        error: str,
        stage: str = "extract",
        error_type: Optional[str] = None,
    ) -> None:
        """Record a per-table failure (isolated; does not abort the run)."""
        self.table_status[logical_table] = STATUS_FAILED
        self.failed_tables += 1
        self.errors.append(
            {
                "table": logical_table,
                "error": (error or "")[:1000],
                "stage": stage,
                "error_type": error_type,
            }
        )

    def rate_limited_table(self, logical_table: str) -> None:
        """
        Record a table whose extraction was cut short by API rate limiting.
        Distinct terminal status; no table_files entry; counts as neither a
        success nor a failure (a transient retry-later outcome). Mirrors
        facebook's TABLE_STATUS_RATE_LIMITED handling.
        """
        self.table_rows.setdefault(logical_table, 0)
        self.table_status[logical_table] = STATUS_RATE_LIMITED

    def note_error(self, message: str, *, table: Optional[str] = None) -> None:
        """
        Record a non-fatal error in `errors` WITHOUT counting a failed table.
        For issues that should be surfaced but must not flip a run to failed
        (e.g. an unknown/unsupported table name a source chooses to tolerate).
        """
        self.errors.append(
            {
                "table": table,
                "error": (message or "")[:1000],
                "stage": "extract",
                "error_type": None,
            }
        )

    # -- finalize ------------------------------------------------------------

    def finalize(self, **extra: Any) -> Dict[str, Any]:
        """
        Build the standard return dict. `extra` merges in source-specific keys
        (e.g. sites count) without overriding the standard ones.
        """
        result = {
            "total_rows": self.total_rows,
            "table_files": dict(self.table_files),
            "table_rows": dict(self.table_rows),
            "table_status": dict(self.table_status),
            "successful_tables": self.successful_tables,
            "failed_tables": self.failed_tables,
            "tables": self.successful_tables,
            "errors": list(self.errors),
            "sites": len(self.sites),
        }
        for k, v in extra.items():
            result.setdefault(k, v)
        return result
