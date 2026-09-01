#!/usr/bin/env python3
"""
DoubleVerify Extractor Plugin
=============================

Loads email-delivered DoubleVerify report files from
gs://email-file-ingestion/doubleverify/<YYYY-MM-DD>/*.csv into one BigQuery
table per report FORMAT (7 tables -- see configs/doubleverify.yaml).

All the work lives in shared/gcs_report_extractor.GcsReportExtractor, which is
config-driven from the YAML's resources.*.file_match and resources.*.schema
blocks. This module only pins the source name and turns on HTML-capture
checking.

Usage:
  python orchestrate.py --source doubleverify
  python orchestrate.py --source doubleverify --tables brand_detail
  python orchestrate.py --source doubleverify --start-date 2026-07-01 --end-date 2026-07-30
"""

from shared.base_extractor import make_run_pipeline
from shared.gcs_report_extractor import GcsReportExtractor


class DoubleVerifyExtractor(GcsReportExtractor):
    source_name = "doubleverify"
    # The DV email feed captured its own login page instead of report data for
    # every day from 2025-02-01 to 2025-06-13 (~318 files). Those are skipped;
    # any HTML captured on/after file_match.html_expected_before is treated as a
    # regression in the email ingestion and logged at ERROR.
    check_html = True


# Backward-compatible module-level run_pipeline for dynamic plugin loading.
run_pipeline = make_run_pipeline(DoubleVerifyExtractor())
