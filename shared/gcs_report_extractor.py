#!/usr/bin/env python3
"""Shared BaseExtractor for sources that are a GCS folder of vendor report files.

Every other ETL source in this repo talks to an API or a database. DoubleVerify
and Trade Desk are different: their "source" is a folder of emailed report files
dropped into gs://email-file-ingestion/<source>/<YYYY-MM-DD>/. This class is the
whole extraction body for that shape -- list, route, read, type, dedup -- driven
entirely by the source YAML's resources.*.file_match and resources.*.schema
blocks.

A concrete plugin is then just::

    class TradeDeskExtractor(GcsReportExtractor):
        source_name = "tradedesk"

    run_pipeline = make_run_pipeline(TradeDeskExtractor())

It lives in shared/ rather than in one of the plugins so neither plugin has to
import the other.
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from google.cloud import storage

from shared.base_extractor import BaseExtractor
from shared.report_file_reader import (
    assert_required_columns,
    build_resource_specs,
    classify_html_capture,
    coalesce_key_columns,
    dedup_latest_wins,
    frame_to_records,
    is_html,
    list_day_folder_blobs,
    normalize_frame,
    read_source_file,
    route_blob,
    stamp_lineage,
)

logger = logging.getLogger(__name__)


class GcsReportExtractor(BaseExtractor):
    """Shared body for sources that are a GCS folder of vendor report files.

    Subclasses set ``source_name`` and (optionally) ``check_html``. Everything
    else -- listing, routing, reading, typing, dedup -- is config-driven from
    the source YAML's ``resources.*.file_match`` and ``schema`` blocks.
    """

    source_name: str = ""
    check_html: bool = False

    def initialize_source(self, config: Dict[str, Any]) -> None:
        conn = config.get("source", {}).get("connection", {}) or {}
        self.bucket = conn.get("source_bucket", "email-file-ingestion")
        self.prefix = conn.get("source_prefix", f"{self.source_name}/")
        project = config.get("pipeline", {}).get("project")
        self.client = storage.Client(project=project) if project else storage.Client()

        # BaseExtractor loops tables and calls extract_table() once per table.
        # Cache the listing per window so GCS is listed once per RUN, not once
        # per table.
        self._blob_cache: Dict[tuple, List[Any]] = {}
        self._specs = build_resource_specs(config)
        self._unrouted: List[str] = []
        self._html_regressions: List[str] = []
        self._html_known = 0

    def _list_window(self, start_date, end_date, ext: str) -> List[Any]:
        key = (start_date, end_date, ext)
        if key not in self._blob_cache:
            self._blob_cache[key] = list_day_folder_blobs(
                self.client, self.bucket, self.prefix, start_date, end_date, ext
            )
            self._report_unrouted(self._blob_cache[key])
        return self._blob_cache[key]

    def _report_unrouted(self, blobs: List[Any]) -> None:
        """A file no resource claims means the vendor added a new report."""
        unrouted = [b.name for b in blobs if route_blob(b.name, self._specs) is None]
        if not unrouted:
            return
        self._unrouted.extend(unrouted)
        logger.warning(
            "%d source file(s) matched NO resource regex and were not loaded. "
            "A new report variant likely appeared -- add a resource to "
            "configs/%s.yaml. Examples: %s",
            len(unrouted),
            self.source_name,
            ", ".join(sorted({u.rsplit("/", 1)[-1] for u in unrouted})[:10]),
        )

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
        file_match = table_config.get("file_match") or {}
        schema_map = table_config.get("schema") or {}
        primary_key = table_config.get("primary_key") or []
        if not file_match or not schema_map:
            logger.warning("Resource '%s' has no file_match/schema -- skipping", table)
            return []

        spec = next((s for s in self._specs if s.name == table), None)
        if spec is None:
            logger.warning("Resource '%s' is not active/routable -- skipping", table)
            return []

        blobs = self._list_window(start_date, end_date, spec.extension)
        mine = [b for b in blobs if route_blob(b.name, self._specs) == table]
        if test_mode:
            mine = mine[:3]

        if not mine:
            logger.info("  %s: no source files in window", table)
            return []

        # Rolling dedup instead of "accumulate every frame, concat once".
        #
        # These feeds restate: a DoubleVerify file carries a full month-to-date
        # window and a Trade Desk file the whole year, so N files hold roughly N
        # copies of the same rows (measured: ~91% of rows collapse). Holding all
        # of them before a single concat is what makes a full-history run peak at
        # several GB and get OOM-killed -- yet the deduped result is small.
        #
        # So we fold every FLUSH_EVERY files down to the deduped set as we go.
        # Peak memory becomes (deduped rows + one batch) rather than (every row
        # in the archive). Blobs are processed in sorted name order, which puts
        # the <YYYY-MM-DD> folder in chronological order, so "latest file wins"
        # still resolves exactly as it would in a single pass.
        FLUSH_EVERY = 25

        accumulated: Optional[pd.DataFrame] = None
        batch: List[pd.DataFrame] = []
        read_count = 0

        def _fold(
            acc: Optional[pd.DataFrame], pending: List[pd.DataFrame]
        ) -> Optional[pd.DataFrame]:
            """Merge pending frames into the accumulator and re-dedup."""
            if not pending:
                return acc
            frames = ([acc] if acc is not None else []) + pending
            merged = pd.concat(frames, ignore_index=True)
            # Coalesce NULL key columns FIRST so dedup groups them as one key,
            # then collapse restated rows to one row per key.
            merged = coalesce_key_columns(merged, primary_key)
            return dedup_latest_wins(merged, primary_key, quiet=True)

        for blob in mine:
            payload = blob.download_as_bytes()

            if self.check_html and is_html(payload):
                severity, message = classify_html_capture(
                    blob.name, file_match.get("html_expected_before")
                )
                if severity == "known":
                    self._html_known += 1
                    logger.info("  skipping known-bad HTML capture: %s", message)
                else:
                    self._html_regressions.append(message)
                    logger.error("  %s", message)
                continue

            df = read_source_file(payload, spec.reader)
            if df is None or df.empty:
                logger.debug("  %s: empty file", blob.name)
                continue

            df = normalize_frame(df, schema_map)
            assert_required_columns(df, spec, blob.name)
            df = stamp_lineage(df, blob.name, self.bucket)
            if not df.empty:
                batch.append(df)
                read_count += 1

            if len(batch) >= FLUSH_EVERY:
                accumulated = _fold(accumulated, batch)
                batch = []
                logger.debug(
                    "  %s: folded %d files -> %s rows so far",
                    table,
                    read_count,
                    f"{len(accumulated):,}",
                )

        accumulated = _fold(accumulated, batch)

        if accumulated is None or accumulated.empty:
            return []

        logger.info(
            "  %s: %s rows from %d file(s)",
            table,
            f"{len(accumulated):,}",
            len(mine),
        )
        return frame_to_records(accumulated)

    def cleanup(self) -> None:
        """Surface feed-health problems once per run, after all tables."""
        if self._html_regressions:
            logger.error(
                "%d HTML login-page capture(s) found AT OR AFTER the known-bad "
                "window -- the email ingestion is broken again and these days "
                "have NO data: %s",
                len(self._html_regressions),
                "; ".join(self._html_regressions[:5]),
            )
        if self._html_known:
            logger.info(
                "Skipped %d HTML capture(s) inside the known-bad backlog window",
                self._html_known,
            )


__all__ = ["GcsReportExtractor"]
