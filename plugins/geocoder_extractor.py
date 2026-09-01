#!/usr/bin/env python3
"""
Geocoder Extractor Plugin
==========================

Geocodes doctor addresses via Google Maps API and maintains a BigQuery cache.
Only calls the API for addresses not already in the cache.

Usage:
  python orchestrate.py --source geocoder --env prod              # incremental (cache misses only)
  python orchestrate.py --source geocoder --env prod --refresh full  # re-geocode all

Data flow:
  1. Query BigQuery for all distinct doctor addresses (primary + secondary)
  2. Query existing ref_geocode_cache for already-resolved addresses
  3. Call Google Maps Geocoding API for cache misses only
  4. Write new results to Parquet → GCS → BigQuery via hash merge
"""

import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from shared.account_context import set_execution_metadata
from shared.extractor_utils import get_available_tables
from shared.gcs_pipeline import extract_to_local_parquet
from shared.geocoder import geocode_batch

logger = logging.getLogger(__name__)

# SQL to get all distinct doctor addresses (primary + secondary)
ADDRESSES_QUERY = """
WITH doctor_npis AS (
  SELECT npi FROM npi_data.doctors_raw
  UNION DISTINCT
  SELECT npi FROM npi_data.doctors_raw2
),
all_addresses AS (
  -- Primary locations
  SELECT DISTINCT
    n.provider_first_line_business_practice_location_address AS street1,
    n.provider_second_line_business_practice_location_address AS street2,
    n.provider_business_practice_location_address_city_name AS city,
    n.provider_business_practice_location_address_state_name AS state,
    n.provider_business_practice_location_address_postal_code AS zip_code
  FROM npi_data.npi_main n
  INNER JOIN doctor_npis d ON d.npi = n.npi
  WHERE n.provider_first_line_business_practice_location_address IS NOT NULL

  UNION DISTINCT

  -- Secondary locations
  SELECT DISTINCT
    pl.provider_secondary_practice_location_address_line_1 AS street1,
    pl.provider_secondary_practice_location_address_line_2 AS street2,
    pl.provider_secondary_practice_location_address_city_name AS city,
    pl.provider_secondary_practice_location_address_state_name AS state,
    pl.provider_secondary_practice_location_address_postal_code AS zip_code
  FROM npi_data.npi_practice_locations pl
  INNER JOIN doctor_npis d ON d.npi = pl.npi
  WHERE pl.provider_secondary_practice_location_address_line_1 IS NOT NULL
)
SELECT * FROM all_addresses
"""

# SQL to get existing cache hashes
CACHE_QUERY = """
SELECT address_hash
FROM `{project}.BN_Warehouse.ref_geocode_cache`
"""

ZIP_JOIN_QUERY = """
WITH doctor_npis AS (
  SELECT npi FROM npi_data.doctors_raw
  UNION DISTINCT
  SELECT npi FROM npi_data.doctors_raw2
),
all_addresses AS (
  -- Primary locations
  SELECT DISTINCT
    n.provider_first_line_business_practice_location_address AS street1,
    n.provider_second_line_business_practice_location_address AS street2,
    n.provider_business_practice_location_address_city_name AS city,
    n.provider_business_practice_location_address_state_name AS state,
    n.provider_business_practice_location_address_postal_code AS zip_code
  FROM npi_data.npi_main n
  INNER JOIN doctor_npis d ON d.npi = n.npi
  WHERE n.provider_first_line_business_practice_location_address IS NOT NULL

  UNION DISTINCT

  -- Secondary locations
  SELECT DISTINCT
    pl.provider_secondary_practice_location_address_line_1 AS street1,
    pl.provider_secondary_practice_location_address_line_2 AS street2,
    pl.provider_secondary_practice_location_address_city_name AS city,
    pl.provider_secondary_practice_location_address_state_name AS state,
    pl.provider_secondary_practice_location_address_postal_code AS zip_code
  FROM npi_data.npi_practice_locations pl
  INNER JOIN doctor_npis d ON d.npi = pl.npi
  WHERE pl.provider_secondary_practice_location_address_line_1 IS NOT NULL
),
normalized AS (
  SELECT
    UPPER(CONCAT(
      COALESCE(TRIM(street1), ''), ' ', COALESCE(TRIM(street2), ''),
      ', ', COALESCE(TRIM(city), ''), ', ', COALESCE(TRIM(state), ''),
      ' ', LEFT(COALESCE(TRIM(zip_code), ''), 5)
    )) AS address_input,
    TO_HEX(SHA256(UPPER(CONCAT(
      COALESCE(TRIM(street1), ''), ' ', COALESCE(TRIM(street2), ''),
      ', ', COALESCE(TRIM(city), ''), ', ', COALESCE(TRIM(state), ''),
      ' ', LEFT(COALESCE(TRIM(zip_code), ''), 5)
    )))) AS address_hash,
    LEFT(COALESCE(TRIM(zip_code), ''), 5) AS zip5
  FROM all_addresses
)
SELECT
  n.address_hash,
  n.address_input,
  gz.latitude AS latitude,
  gz.longitude AS longitude,
  'ZIP_CENTROID' AS location_type,
  CAST(NULL AS STRING) AS formatted_address,
  'ZIP_CENTROID' AS geocode_status,
  CURRENT_TIMESTAMP() AS geocoded_at
FROM normalized n
LEFT JOIN `{project}.BN_Warehouse.ref_geonames_zip` gz
  ON gz.postal_code = n.zip5
WHERE n.zip5 IS NOT NULL
  AND n.zip5 != ''
  AND gz.latitude IS NOT NULL
  AND gz.longitude IS NOT NULL
"""


def _get_doctor_addresses(bq_client: bigquery.Client) -> List[Dict[str, Any]]:
    """Query BigQuery for all distinct doctor addresses."""
    logger.info("Querying BigQuery for doctor addresses (primary + secondary)...")
    rows = list(bq_client.query(ADDRESSES_QUERY).result())
    addresses = [dict(row) for row in rows]
    logger.info(f"Found {len(addresses)} distinct addresses")
    return addresses


def _get_cached_hashes(bq_client: bigquery.Client, project: str) -> set:
    """Get set of address hashes already in the cache."""
    try:
        query = CACHE_QUERY.format(project=project)
        rows = list(bq_client.query(query).result())
        hashes = {row["address_hash"] for row in rows}
        logger.info(f"Found {len(hashes)} addresses already in cache")
        return hashes
    except Exception as e:
        # Table may not exist yet on first run
        logger.info(f"Cache table not found (first run?): {e}")
        return set()


def _extract_zip_centroid_records(
    bq_client: bigquery.Client,
    project: str,
    existing_hashes: set,
) -> List[Dict[str, Any]]:
    """
    Fast path: generate ZIP centroid "geocodes" via a single BigQuery join to ref_geonames_zip.
    Returns only rows not already present in the cache (by address_hash).
    """
    query = ZIP_JOIN_QUERY.format(project=project)
    rows = list(bq_client.query(query).result())
    records = [dict(r) for r in rows if r.get("address_hash") not in existing_hashes]
    logger.info(
        f"ZIP-centroid join produced {len(rows)} rows; "
        f"{len(records)} are new (not already cached)"
    )
    return records


def _extract_geocode_cache(
    config: Dict[str, Any],
    bq_client: bigquery.Client,
    refresh_mode: str,
    execution_id: str,
) -> List[Dict[str, Any]]:
    """
    Main extraction logic:
    1. Get all doctor addresses from BigQuery
    2. Check cache for existing geocodes
    3. Call Google API for cache misses
    4. Return records for Parquet export
    """
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    # Default to ZIP-only (cheap). Enable Google fallback explicitly if needed.
    allow_google_fallback = os.getenv(
        "GEOCODER_ALLOW_GOOGLE_FALLBACK", "false"
    ).strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if allow_google_fallback and not api_key:
        raise ValueError(
            "GOOGLE_MAPS_API_KEY environment variable not set "
            "(set GEOCODER_ALLOW_GOOGLE_FALLBACK=false to run ZIP-only)"
        )

    project = config.get("pipeline", {}).get("project", "bi-data-391216")

    # Step 1: Check cache (skip for full refresh)
    if refresh_mode == "full":
        logger.info("Full refresh -- re-geocoding all addresses")
        existing_hashes = set()
    else:
        existing_hashes = _get_cached_hashes(bq_client, project)

    # Step 2: ZIP centroid join (cheap, fast)
    zip_records = _extract_zip_centroid_records(bq_client, project, existing_hashes)
    if zip_records and not allow_google_fallback:
        logger.info(
            f"Geocoding summary: ZIP-only mode; {len(zip_records)} new ZIP centroid records"
        )
        return zip_records

    # Step 3 (optional): Google fallback for addresses not covered by ZIP join
    addresses = _get_doctor_addresses(bq_client)
    if not addresses:
        logger.warning("No doctor addresses found")
        return zip_records

    records, stats = geocode_batch(
        addresses=addresses,
        api_key=api_key or "",
        existing_hashes=existing_hashes,
        zip_centroids={},  # we already handled ZIP centroids via SQL join
        allow_google_fallback=allow_google_fallback,
    )

    # Prefer ZIP records, then add any Google results
    records = zip_records + records

    if not records:
        logger.info("All addresses already cached -- nothing to geocode")
        return []

    logger.info(
        f"Geocoding summary: {len(zip_records)} ZIP centroid records, "
        f"{stats.get('api_call', 0)} API calls, {stats.get('cache_hit', 0)} cache hits, "
        f"{stats.get('error', 0)} errors"
    )

    return records


# Table name → extractor function mapping
EXTRACTORS = {
    "ref_geocode_cache": _extract_geocode_cache,
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
    **kwargs,
) -> Dict[str, Any]:
    """
    Geocoder extraction pipeline.

    Geocodes doctor addresses via Google Maps API, using a BigQuery cache
    to avoid redundant API calls. Only new/changed addresses are geocoded.
    """
    logger.info("=" * 70)
    logger.info("GEOCODER EXTRACTION PIPELINE")
    logger.info("=" * 70)

    # Resolve tables
    if not tables:
        if group:
            tables = config.get("groups", {}).get(group, [])
        else:
            tables = get_available_tables(config)

    logger.info(f"Tables to extract: {tables}")

    # Ensure we have a BigQuery client
    if bq_client is None:
        bq_client = bigquery.Client(
            project=config.get("pipeline", {}).get("project", "bi-data-391216")
        )

    source_name = config.get("source", {}).get("name", "geocoder")
    temp_dir = tempfile.mkdtemp(prefix="geocoder_")

    # Standardized per-table accounting + return shape (shared/extraction_result).
    # record_table does the stamp-metadata + Parquet-write the inline code used to
    # do, so the contract (logical keys, no None paths, uniform status) is structural.
    from shared.extraction_result import StandardExtractionResult

    result = StandardExtractionResult(
        source=source_name,
        execution_id=execution_id,
        job_id=execution_id,
        bq_client=bq_client,
    )

    for table in tables:
        extractor_fn = EXTRACTORS.get(table)
        if not extractor_fn:
            msg = f"Unknown geocoder table: {table}"
            logger.error(msg)
            # Non-fatal: matches old behavior (logged to errors, does NOT flip
            # the run to success=False, which would abort via orchestrate.py).
            result.note_error(msg, table=table)
            continue

        try:
            logger.info(f"Extracting {table}...")
            records = extractor_fn(config, bq_client, refresh_mode, execution_id)

            if not records:
                logger.info(f"{table}: No new addresses to geocode (all cached)")
                result.skip_table(table, reason="zero_rows")
                continue

            # Stamp metadata + write Parquet + record (logical key, row count).
            result.record_table(table, records=records, local_temp_dir=temp_dir)
            logger.info(f"  {table}: {len(records)} records recorded")

        except Exception as e:
            msg = f"{table}: {e}"
            logger.error(msg, exc_info=True)
            result.fail_table(
                table, error=str(e), stage="extract", error_type=type(e).__name__
            )

    logger.info("=" * 70)
    logger.info(
        f"Geocoder complete: {result.successful_tables} tables, "
        f"{result.total_rows} total rows"
    )
    if result.errors:
        logger.warning(f"Errors: {[e['error'] for e in result.errors]}")
    logger.info("=" * 70)

    # success flag preserved for any caller that reads it (no failed tables).
    return result.finalize(success=(result.failed_tables == 0))
