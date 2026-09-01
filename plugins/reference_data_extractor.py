#!/usr/bin/env python3
"""
Reference Data Extractor Plugin
================================

Downloads and loads external reference datasets into BN_Warehouse:
  - geonames_zip: US ZIP code lat/lng from GeoNames (daily updates)
  - nucc_taxonomy: Healthcare provider taxonomy codes from NUCC

Usage:
  python orchestrate.py --source reference_data --tables geonames_zip,nucc_taxonomy
  python orchestrate.py --source reference_data --tables geonames_zip
  python orchestrate.py --source reference_data --tables nucc_taxonomy
"""

import io
import logging
import zipfile
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from shared.base_extractor import BaseExtractor, make_run_pipeline

logger = logging.getLogger(__name__)

# ── Data sources ──────────────────────────────────────────────────────────────

GEONAMES_URL = "https://download.geonames.org/export/zip/US.zip"
GEONAMES_COLUMNS = [
    "country_code",
    "postal_code",
    "place_name",
    "state_name",
    "state_code",
    "county_name",
    "county_code",
    "admin_name3",
    "admin_code3",
    "latitude",
    "longitude",
    "accuracy",
]

NUCC_URL = "https://www.nucc.org/images/stories/CSV/nucc_taxonomy_251.csv"


# ── Extractors ────────────────────────────────────────────────────────────────


def _download_geonames(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Download GeoNames US ZIP code data (ZIP → TSV)."""
    url = (
        config.get("source", {}).get("connection", {}).get("geonames_url", GEONAMES_URL)
    )
    logger.info(f"Downloading GeoNames ZIP codes from {url}")

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open("US.txt") as f:
            df = pd.read_csv(
                f,
                sep="\t",
                header=None,
                names=GEONAMES_COLUMNS,
                dtype=str,
                encoding="utf-8",
            )

    # Drop internal columns we don't need
    df = df.drop(columns=["country_code", "admin_name3", "admin_code3"])

    # Cast numeric columns
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["accuracy"] = pd.to_numeric(df["accuracy"], errors="coerce").astype("Int64")

    logger.info(f"GeoNames: {len(df)} ZIP codes loaded")
    return df.to_dict(orient="records")


def _download_nucc(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Download NUCC Healthcare Provider Taxonomy CSV."""
    url = config.get("source", {}).get("connection", {}).get("nucc_url", NUCC_URL)
    logger.info(f"Downloading NUCC taxonomy from {url}")

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    df = pd.read_csv(
        io.BytesIO(resp.content),
        dtype=str,
        encoding="utf-8",
    )

    # Normalize column names to snake_case
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    logger.info(f"NUCC: {len(df)} taxonomy codes loaded")
    return df.to_dict(orient="records")


# Table name → extractor function
EXTRACTORS = {
    "geonames_zip": _download_geonames,
    "nucc_taxonomy": _download_nucc,
}


# ── Pipeline entry point (on the shared BaseExtractor) ───────────────────────
#
# reference_data is the simplest standard shape -- a single (account=None) x
# table loop where each table downloads an in-memory record list -- so it is the
# proof-of-concept adopter for shared/base_extractor.BaseExtractor. The base
# handles env init, table discovery, date resolution, metadata stamping, the
# Parquet write, and the B2-clean return contract (table_files keyed by LOGICAL
# name) via StandardExtractionResult; this subclass only maps table -> downloader.


class ReferenceDataExtractor(BaseExtractor):
    source_name = "reference_data"

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
        extractor_fn = EXTRACTORS.get(table)
        if not extractor_fn:
            # Unknown table: download nothing -> the base records it as zero_rows
            # (skipped, non-fatal), matching the prior note_error behavior of not
            # flipping the run to failed. In practice unreachable -- the configured
            # resources are exactly the EXTRACTORS keys.
            logger.warning(f"Unknown reference table: {table} (skipping)")
            return []
        logger.info(f"Extracting {table}...")
        return extractor_fn(config)


# Backward-compatible module-level run_pipeline for dynamic plugin loading.
run_pipeline = make_run_pipeline(ReferenceDataExtractor())
