#!/usr/bin/env python3
"""
Geocoder — Address normalization, hashing, and Google Maps API client.

Provides street-level geocoding with a BigQuery cache so each unique
address is only geocoded once via the Google Maps Geocoding API.

Usage (standalone):
    python -m shared.geocoder --test "1600 Amphitheatre Pkwy, Mountain View, CA 94043"
"""

import hashlib
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# Google Maps Geocoding API
GEOCODING_URL = "https://maps.googleapis.com/maps/api/geocode/json"
MAX_QPS = 45  # stay under Google's 50 QPS limit
RETRY_STATUSES = {"OVER_QUERY_LIMIT", "UNKNOWN_ERROR"}
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# ZIP-centroid fallback is handled in the extractor (BigQuery join to ref_geonames_zip).


def normalize_address(
    street1: Optional[str],
    street2: Optional[str],
    city: Optional[str],
    state: Optional[str],
    zip_code: Optional[str],
) -> str:
    """
    Normalize an address to a canonical form for hashing.

    MUST match the BigQuery SQL exactly:
        UPPER(CONCAT(street1, ' ', street2, ', ', city, ', ', state, ' ', LEFT(postal, 5)))

    Returns:
        Normalized address string for hashing.
    """
    s1 = (street1 or "").strip()
    s2 = (street2 or "").strip()
    c = (city or "").strip()
    st = (state or "").strip()
    z = (zip_code or "").strip()[:5]

    # Match SQL: CONCAT(street1, ' ', street2, ', ', city, ', ', state, ' ', zip5)
    normalized = f"{s1} {s2}, {c}, {st} {z}".upper()
    return normalized


def compute_address_hash(normalized_address: str) -> str:
    """
    Compute SHA256 hex digest of a normalized address string.

    This MUST match the BigQuery SQL:
        TO_HEX(SHA256(normalized_address))
    """
    return hashlib.sha256(normalized_address.encode("utf-8")).hexdigest()


def geocode_address(
    address: str,
    api_key: str,
) -> Dict[str, Any]:
    """
    Geocode a single address via Google Maps Geocoding API.

    Returns dict with: latitude, longitude, location_type,
    formatted_address, geocode_status.
    """
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                GEOCODING_URL,
                params={"address": address, "key": api_key},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.warning(f"Geocode HTTP error (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return _empty_result("HTTP_ERROR")

        status = data.get("status", "UNKNOWN")

        if status == "OK":
            result = data["results"][0]
            loc = result["geometry"]["location"]
            return {
                "latitude": loc["lat"],
                "longitude": loc["lng"],
                "location_type": result["geometry"].get("location_type", ""),
                "formatted_address": result.get("formatted_address", ""),
                "geocode_status": "OK",
            }
        elif status == "ZERO_RESULTS":
            return _empty_result("ZERO_RESULTS")
        elif status in RETRY_STATUSES:
            logger.warning(
                f"Geocode {status} (attempt {attempt + 1}): {address}"
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return _empty_result(status)
        else:
            # REQUEST_DENIED, INVALID_REQUEST, etc.
            error_msg = data.get("error_message", "")
            logger.error(f"Geocode {status}: {error_msg} -- address: {address}")
            return _empty_result(status)

    return _empty_result("MAX_RETRIES_EXCEEDED")


def _empty_result(status: str) -> Dict[str, Any]:
    """Return an empty geocode result with the given status."""
    return {
        "latitude": None,
        "longitude": None,
        "location_type": None,
        "formatted_address": None,
        "geocode_status": status,
    }


def geocode_batch(
    addresses: List[Dict[str, Any]],
    api_key: str,
    existing_hashes: Optional[set] = None,
    zip_centroids: Optional[Dict[str, Tuple[float, float]]] = None,
    allow_google_fallback: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Geocode a batch of addresses, skipping those already in cache.

    Args:
        addresses: List of dicts with keys: street1, street2, city, state, zip_code
        api_key: Google Maps API key
        existing_hashes: Set of address_hash values already in the cache

    Returns:
        Tuple of (records for cache table, stats dict)
    """
    if existing_hashes is None:
        existing_hashes = set()

    records = []
    stats = {
        "total": len(addresses),
        "cache_hit": 0,
        "zip_hit": 0,
        "api_call": 0,
        "error": 0,
    }
    interval = 1.0 / MAX_QPS  # minimum seconds between API calls
    last_call_time = 0.0

    if zip_centroids is None:
        zip_centroids = {}

    for i, addr in enumerate(addresses):
        normalized = normalize_address(
            addr.get("street1"),
            addr.get("street2"),
            addr.get("city"),
            addr.get("state"),
            addr.get("zip_code"),
        )
        addr_hash = compute_address_hash(normalized)

        # Skip if already cached
        if addr_hash in existing_hashes:
            stats["cache_hit"] += 1
            continue

        zip5 = (addr.get("zip_code") or "").strip()[:5]
        if zip5 and zip5 in zip_centroids:
            lat, lng = zip_centroids[zip5]
            result = {
                "latitude": lat,
                "longitude": lng,
                "location_type": "ZIP_CENTROID",
                "formatted_address": "",
                "geocode_status": "ZIP_CENTROID",
            }
            stats["zip_hit"] += 1
        else:
            if not allow_google_fallback:
                result = _empty_result("ZIP_MISS")
                stats["error"] += 1
            else:
                # Rate limit
                now = time.time()
                elapsed = now - last_call_time
                if elapsed < interval:
                    time.sleep(interval - elapsed)

                result = geocode_address(normalized, api_key)
                last_call_time = time.time()
                stats["api_call"] += 1

                if result["geocode_status"] not in ("OK", "ZERO_RESULTS"):
                    stats["error"] += 1

        record = {
            "address_hash": addr_hash,
            "address_input": normalized,
            "latitude": result["latitude"],
            "longitude": result["longitude"],
            "location_type": result["location_type"],
            "formatted_address": result["formatted_address"],
            "geocode_status": result["geocode_status"],
            "geocoded_at": datetime.now(timezone.utc).isoformat(),
        }
        records.append(record)

        # Mark as cached for this batch (prevent re-geocoding duplicates within batch)
        existing_hashes.add(addr_hash)

        # Progress logging
        if (stats["api_call"] + stats["zip_hit"]) % 500 == 0 and (stats["api_call"] + stats["zip_hit"]) > 0:
            logger.info(
                f"  Geocoded {stats['api_call'] + stats['zip_hit']}/{stats['total'] - stats['cache_hit']} "
                f"addresses ({stats['error']} errors)"
            )

    logger.info(
        f"Geocoding complete: {stats['zip_hit']} ZIP hits, {stats['api_call']} API calls, "
        f"{stats['cache_hit']} cache hits, {stats['error']} errors"
    )
    return records, stats


# ── CLI for testing ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv

    logging.basicConfig(level=logging.INFO)
    load_dotenv()

    parser = argparse.ArgumentParser(description="Test geocoder")
    parser.add_argument("--test", type=str, help="Address to geocode")
    args = parser.parse_args()

    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_MAPS_API_KEY not set in .env")
        exit(1)

    if args.test:
        result = geocode_address(args.test, api_key)
        print(f"Status:    {result['geocode_status']}")
        print(f"Lat/Lng:   {result['latitude']}, {result['longitude']}")
        print(f"Type:      {result['location_type']}")
        print(f"Formatted: {result['formatted_address']}")
    else:
        # Quick self-test
        norm = normalize_address("123 Main St", "Suite 100", "Springfield", "IL", "62701-1234")
        print(f"Normalized: {norm}")
        print(f"Hash:       {compute_address_hash(norm)}")
