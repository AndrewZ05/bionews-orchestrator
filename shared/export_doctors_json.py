#!/usr/bin/env python3
"""
Export v_doctors_enriched to Algolia-ready JSON.

Queries the BigQuery view and writes a JSON file with:
  - objectID: {npi}_{location_sequence}
  - _geoloc: {lat, lng} (Algolia's required format)
  - All doctor/location fields

Usage:
  python shared/export_doctors_json.py                          # local file
  python shared/export_doctors_json.py --output exports/doctors.json
  python shared/export_doctors_json.py --gcs                    # upload to GCS
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)

QUERY = """
SELECT
  npi,
  segment,
  hcp_name,
  account_name,
  source_table,
  fullName,
  salutation,
  specialty,
  specialtyList,
  location_type,
  location_sequence,
  street,
  city,
  postal,
  stateAbbr,
  stateFull,
  gender,
  genderFull,
  phone,
  fax,
  credentials,
  geoloc,
  geoloc_source,
  address_hash
FROM npi_data.v_doctors_enriched
ORDER BY npi, location_sequence
"""


def export_doctors_json(
    output_path: Optional[str] = None,
    upload_gcs: bool = False,
    gcs_bucket: Optional[str] = None,
    gcs_path: str = "exports/doctors_enriched.json",
) -> dict:
    """
    Export v_doctors_enriched to Algolia-ready JSON.

    Returns:
        Dict with record_count, output_path, and gcs_uri (if uploaded).
    """
    client = bigquery.Client(project="bi-data-391216")

    logger.info("Querying v_doctors_enriched...")
    rows = list(client.query(QUERY).result())
    logger.info(f"Fetched {len(rows)} records")

    records = []
    for row in rows:
        record = {
            "objectID": f"{row.npi}_{row.location_sequence}",
            "npi": row.npi,
            "segment": row.segment,
            "hcp_name": row.hcp_name,
            "account_name": row.account_name,
            "source_table": row.source_table,
            "fullName": row.fullName,
            "salutation": row.salutation,
            "specialty": row.specialty,
            "specialtyList": list(row.specialtyList) if row.specialtyList else [],
            "location_type": row.location_type,
            "location_sequence": row.location_sequence,
            "street": row.street,
            "city": row.city,
            "postal": row.postal,
            "stateAbbr": row.stateAbbr,
            "stateFull": row.stateFull,
            "gender": row.gender,
            "genderFull": row.genderFull,
            "phone": row.phone,
            "fax": row.fax,
            "credentials": row.credentials,
            "geoloc_source": row.geoloc_source,
            "address_hash": row.address_hash,
        }

        # Algolia geo format: _geoloc with underscore
        if row.geoloc and row.geoloc.get("lat") is not None:
            record["_geoloc"] = {
                "lat": row.geoloc["lat"],
                "lng": row.geoloc["lng"],
            }
        else:
            record["_geoloc"] = None

        records.append(record)

    # Write local file
    if output_path is None:
        os.makedirs("exports", exist_ok=True)
        output_path = os.path.join(
            "exports",
            f"doctors_enriched_{datetime.now(timezone.utc).strftime('%Y%m%d')}.json",
        )

    with open(output_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.info(f"Wrote {len(records)} records to {output_path} ({file_size_mb:.1f} MB)")

    result = {
        "record_count": len(records),
        "output_path": output_path,
        "file_size_mb": round(file_size_mb, 1),
    }

    # Upload to GCS if requested
    if upload_gcs:
        bucket_name = gcs_bucket or os.getenv("GCS_BUCKET", "orchestrator-data-lake")
        try:
            from google.cloud import storage

            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(output_path)
            gcs_uri = f"gs://{bucket_name}/{gcs_path}"
            logger.info(f"Uploaded to {gcs_uri}")
            result["gcs_uri"] = gcs_uri
        except Exception as e:
            logger.error(f"GCS upload failed: {e}")
            result["gcs_error"] = str(e)

    return result


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Export doctors to Algolia-ready JSON")
    parser.add_argument("--output", type=str, help="Output file path")
    parser.add_argument("--gcs", action="store_true", help="Also upload to GCS")
    parser.add_argument("--gcs-bucket", type=str, help="GCS bucket name")
    parser.add_argument("--gcs-path", type=str, default="exports/doctors_enriched.json")
    args = parser.parse_args()

    result = export_doctors_json(
        output_path=args.output,
        upload_gcs=args.gcs,
        gcs_bucket=args.gcs_bucket,
        gcs_path=args.gcs_path,
    )

    print(f"\nExport complete:")
    print(f"  Records: {result['record_count']}")
    print(f"  File:    {result['output_path']} ({result['file_size_mb']} MB)")
    if "gcs_uri" in result:
        print(f"  GCS:     {result['gcs_uri']}")
