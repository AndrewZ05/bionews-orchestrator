"""
Base extractor class that eliminates run_pipeline() boilerplate.

Every standard extractor repeats 10 identical steps in run_pipeline().
This base class handles all of them, letting subclasses only implement
the source-specific extraction logic.

Extractors that DON'T fit the standard pattern (identity_graph,
bio_acceptor, generic) should continue using standalone run_pipeline()
functions.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from shared.extractor_utils import get_available_tables
from shared.extractor_runner import initialize_pipeline_environment
from shared.extraction_result import StandardExtractionResult
from shared.incremental_resolver import resolve_incremental_dates
from shared.watermark import (
    get_watermark,
    resolve_watermark_start,
    save_watermark,
    watermark_enabled,
    watermark_safety_days,
)

logger = logging.getLogger(__name__)


class BaseExtractor:
    """
    Base class for the SIMPLE standard extractor shape: a single account x
    table loop where each (account, table) yields an in-memory list of records.

    It removes the run_pipeline boilerplate (env init, table discovery, date
    resolution, metadata stamping, Parquet writing, and the result contract) by
    delegating per-table bookkeeping to StandardExtractionResult -- the same
    accumulator the standalone extractors use -- so the return is B2-clean
    (table_files keyed by LOGICAL name, no None paths, uniform table_status).

    SCOPE (intentionally thin): this fits sources like reference_data, geocoder,
    instagram, and threads. It does NOT fit sources whose extraction is not a
    uniform per-table loop -- facebook (many irregular per-table handlers, async
    insights, rate-limit buckets), dcm (per-profile parallel extraction +
    consolidation into one file per table), mailchimp (dual load_path + Batch
    API), or wordpress (a per-site failure axis). Those keep their own
    run_pipeline (already accumulator-backed). Do not bend this base to fit them.

    MULTI-ACCOUNT CAVEAT: record_table keys by the logical table name, so a
    multi-account source that extracts the SAME table per account would overwrite
    the per-account Parquet. The base targets single-account sources
    (discover_accounts default returns [None]); a true multi-account source that
    needs per-account files should aggregate across accounts inside
    extract_table (return one combined record list per table) or keep a bespoke
    run_pipeline.

    Subclasses must set ``source_name`` and implement ``extract_table()``.
    Optionally override ``discover_accounts()``, ``initialize_source()``,
    and ``cleanup()``.

    Example::

        class ReferenceDataExtractor(BaseExtractor):
            source_name = 'reference_data'

            def extract_table(self, account, table, table_config, config,
                              start_date, end_date, test_mode, env):
                return download_reference_table(table, config)
    """

    source_name: str = ""  # Override in subclass

    def run_pipeline(
        self,
        config: Dict[str, Any],
        sites: List[str],
        tables: List[str],
        group: Optional[str],
        refresh_mode: str,
        lookback_days: Optional[int],
        start_date: Optional[str],
        end_date: Optional[str],
        test_mode: bool,
        batch_size: Optional[int] = None,
        max_retries: int = 3,
        bq_client: Any = None,
        execution_id: Optional[str] = None,
        schema_prefix: Optional[str] = None,
        schema_suffix: Optional[str] = None,
        rebuild: bool = False,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Standard pipeline entry point.

        Handles: env init, table discovery, date resolution, stats tracking,
        parquet writing, and summary logging.  Delegates actual extraction
        to ``extract_table()``.
        """
        # --- Step 1: Initialise pipeline environment ---
        env = initialize_pipeline_environment(
            config,
            bq_client=bq_client,
            source_default=self.source_name,
            schema_prefix=schema_prefix,
            schema_suffix=schema_suffix,
            execution_id=execution_id,
            rebuild=rebuild,
        )

        source = env.source_name
        main_dataset = env.production_dataset
        exec_id = env.execution_id or execution_id

        # --- Step 2: Discover accounts / sites ---
        accounts = self.discover_accounts(config, sites)

        # --- Step 3: Resolve available tables ---
        if not tables:
            tables = get_available_tables(config, group)

        # --- Step 4: Parse date range ---
        effective_start, effective_end = self._resolve_global_dates(
            refresh_mode,
            lookback_days,
            start_date,
            end_date,
        )

        # --- Step 5: Standard accumulator (shared/extraction_result) ---
        # Centralizes per-table bookkeeping AND the return contract: table_files
        # keyed by the LOGICAL resource name (NOT the affixed physical name -- the
        # orchestrator applies affixes and looks up config.resources[key]; an
        # affixed key would miss that lookup -- this is the A4 fix), never a None
        # path, uniform table_status. record_table stamps system metadata before
        # the Parquet write.
        result = StandardExtractionResult(
            source=source,
            execution_id=exec_id,
            job_id=exec_id,
            bq_client=bq_client,
            sites=accounts,
        )

        # --- Step 6: Source-specific initialisation ---
        try:
            self.initialize_source(config)
        except Exception as e:
            logger.error(f"Failed to initialise {source}: {e}")
            result.note_error(f"initialize_source failed: {e}")
            return result.finalize(success=False)

        # --- Step 7: Main extraction loop ---
        try:
            for account in accounts:
                account_label = account if account else source
                for table in tables:
                    table_config = config.get("resources", {}).get(table, {})

                    # Check active flag
                    active = table_config.get("active", True)
                    if active is False or (
                        isinstance(active, str)
                        and active.lower() in ("false", "no", "n", "0")
                    ):
                        continue

                    # Adaptive watermark (opt-in per table via
                    # incremental.use_watermark). When enabled, size the window from
                    # the last successful extraction instead of a fixed lookback.
                    # Default (flag off / no client) -> watermark_start stays None ->
                    # behavior is unchanged.
                    watermark_start = None
                    if bq_client is not None and watermark_enabled(table_config):
                        wm = get_watermark(bq_client, main_dataset, source, table)
                        watermark_start = resolve_watermark_start(
                            wm,
                            safety_days=watermark_safety_days(table_config),
                            lookback_days=table_config.get("incremental", {}).get(
                                "lookback_days", 7
                            ),
                            initial_value=table_config.get("incremental", {}).get(
                                "initial_value"
                            ),
                        )

                    # Per-table incremental date resolution
                    table_start, table_end = resolve_incremental_dates(
                        table_config,
                        refresh_mode,
                        effective_start,
                        effective_end,
                        lookback_days,
                        watermark_start=watermark_start,
                    )

                    # The affixed physical name is needed ONLY for the
                    # production_table_id (schema enforcement / rebuild). The
                    # table_files key stays LOGICAL (see Step 5).
                    formatted_table = env.format_table(table)
                    production_table_id = (
                        f"{env.project}.{main_dataset}.{formatted_table}"
                    )

                    try:
                        records = self.extract_table(
                            account=account,
                            table=table,
                            table_config=table_config,
                            config=config,
                            start_date=table_start,
                            end_date=table_end,
                            test_mode=test_mode,
                            env=env,
                        )

                        if not records:
                            logger.info(f"  {account_label}/{table}: 0 rows")
                            result.skip_table(table, reason="zero_rows")
                            continue

                        # Accumulator stamps metadata, writes the Parquet, and
                        # records the table under its LOGICAL name.
                        result.record_table(
                            table,
                            records=records,
                            production_table_id=production_table_id,
                            rebuild_mode=rebuild,
                        )
                        logger.info(f"  {account_label}/{table}: {len(records):,} rows")

                        # Record the successful-extraction watermark (only when the
                        # table opted in; best-effort, never blocks). The NEXT run
                        # sizes its window from this mark.
                        if bq_client is not None and watermark_enabled(table_config):
                            save_watermark(
                                bq_client,
                                main_dataset,
                                source,
                                table,
                                execution_id,
                                len(records),
                            )

                    except Exception as e:
                        logger.error(f"  Failed {account_label}/{table}: {e}")
                        result.fail_table(
                            table, error=str(e), error_type=type(e).__name__
                        )

        finally:
            # --- Step 8: Cleanup ---
            try:
                self.cleanup()
            except Exception as e:
                logger.warning(f"Cleanup error: {e}")

        # --- Step 9: Summary ---
        stats = result.finalize(success=(result.failed_tables == 0))
        # Preserve the historical `tables` semantics (requested count).
        stats["tables"] = len(tables)
        stats["sites"] = len(accounts)
        logger.info(
            f"Pipeline complete: {stats['total_rows']:,} rows, "
            f"{stats['successful_tables']}/{len(tables)} tables"
        )
        return stats

    # ------------------------------------------------------------------
    # Hooks for subclasses
    # ------------------------------------------------------------------

    def discover_accounts(
        self,
        config: Dict[str, Any],
        sites: List[str],
    ) -> List[Any]:
        """
        Return list of accounts/sites to extract.

        Override for multi-tenant sources. Default returns the provided
        sites list, or ``[None]`` for single-account sources.
        """
        if sites and sites != ["all"] and sites != [None]:
            return sites
        return [None]

    def extract_table(
        self,
        account: Any,
        table: str,
        table_config: Dict[str, Any],
        config: Dict[str, Any],
        start_date: Optional[str],
        end_date: Optional[str],
        test_mode: bool,
        env: Any,
    ) -> List[Dict[str, Any]]:
        """
        Extract data for one table from one account.

        **Must be overridden by subclasses.**

        Returns:
            List of record dicts ready for Parquet writing.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement extract_table()"
        )

    def initialize_source(self, config: Dict[str, Any]) -> None:
        """
        One-time source initialisation (API auth, connection setup, etc.).

        Called once before the extraction loop.  Override as needed.
        """
        pass

    def cleanup(self) -> None:
        """
        Resource cleanup (close connections, sessions, tunnels).

        Called in a ``finally`` block after extraction completes.
        Override as needed.
        """
        pass

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_global_dates(
        self,
        refresh_mode: str,
        lookback_days: Optional[int],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Tuple[Optional[str], Optional[str]]:
        """Resolve the global date range from CLI parameters."""
        if refresh_mode == "full":
            return None, None

        if start_date and end_date:
            return start_date, end_date

        if start_date:
            return start_date, end_date

        # Calculate from lookback
        days = lookback_days or 7
        calculated_start = (datetime.utcnow() - timedelta(days=days)).strftime(
            "%Y-%m-%d"
        )
        return calculated_start, end_date


def make_run_pipeline(extractor_instance: BaseExtractor):
    """
    Create a module-level ``run_pipeline`` function from an extractor instance.

    This allows backward-compatible dynamic loading via::

        importlib.import_module('plugins.wordpress_extractor').run_pipeline
    """

    def run_pipeline(
        config,
        sites,
        tables,
        group,
        refresh_mode,
        lookback_days,
        start_date,
        end_date,
        test_mode,
        batch_size=None,
        max_retries=3,
        bq_client=None,
        execution_id=None,
        schema_prefix=None,
        schema_suffix=None,
        rebuild=False,
        **kwargs,
    ):
        return extractor_instance.run_pipeline(
            config=config,
            sites=sites,
            tables=tables,
            group=group,
            refresh_mode=refresh_mode,
            lookback_days=lookback_days,
            start_date=start_date,
            end_date=end_date,
            test_mode=test_mode,
            batch_size=batch_size,
            max_retries=max_retries,
            bq_client=bq_client,
            execution_id=execution_id,
            schema_prefix=schema_prefix,
            schema_suffix=schema_suffix,
            rebuild=rebuild,
            **kwargs,
        )

    return run_pipeline


__all__ = [
    "BaseExtractor",
    "make_run_pipeline",
]
