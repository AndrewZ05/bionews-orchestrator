#!/usr/bin/env python3
"""
Trade Desk Extractor Plugin
===========================

Loads email-delivered Trade Desk report files from
gs://email-file-ingestion/tradedesk/<YYYY-MM-DD>/*.xlsx into one BigQuery table
per report variant (2 tables -- see configs/tradedesk.yaml).

All the work lives in shared/gcs_report_extractor.GcsReportExtractor, which is
config-driven from the YAML's resources.*.file_match and resources.*.schema
blocks. Unlike DoubleVerify, the Trade Desk feed has never emitted HTML login
pages, so check_html stays off.

Usage:
  python orchestrate.py --source tradedesk
  python orchestrate.py --source tradedesk --tables ad_group_performance
  python orchestrate.py --source tradedesk --start-date 2026-07-01 --end-date 2026-07-30
"""

from shared.base_extractor import make_run_pipeline
from shared.gcs_report_extractor import GcsReportExtractor


class TradeDeskExtractor(GcsReportExtractor):
    source_name = "tradedesk"


# Backward-compatible module-level run_pipeline for dynamic plugin loading.
run_pipeline = make_run_pipeline(TradeDeskExtractor())
