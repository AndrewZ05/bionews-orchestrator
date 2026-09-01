#!/usr/bin/env python3
"""
Export doctor profiles from v_doctors_enriched as JSON.

Queries the BigQuery view (primary location only) and writes a formatted
JSON array matching the target profile schema.

Usage:
  python shared/export_doctors_profile.py
  python shared/export_doctors_profile.py --output exports/doctors.json
"""

import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery
from nameparser import HumanName
from titlecase import titlecase

logger = logging.getLogger(__name__)

INSURANCE_PREFIX = (
    "Accepts most major Health Plans. "
    "Please contact our office for details."
)

QUERY = """
SELECT
  npi,
  fullName,
  salutation,
  specialty,
  specialtyList,
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
  insurance
FROM npi_data.v_doctors_enriched
WHERE location_sequence = 0
  AND fullName IS NOT NULL AND TRIM(fullName) != ''
ORDER BY npi
"""


def format_phone(raw: Optional[str]) -> str:
    """Format 10-digit NPPES phone to (xxx) xxx-xxxx."""
    if not raw:
        return ""
    digits = "".join(c for c in raw if c.isdigit())
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return raw


def proper_case_name(raw: Optional[str]) -> str:
    """Proper-case a name using nameparser: 'DR. SORIN DANCIU' -> 'Dr. Sorin Danciu'."""
    if not raw:
        return ""
    name = HumanName(raw)
    name.capitalize()
    return str(name)


_DOCTOR_CREDENTIALS = re.compile(
    r"\b(M\.?D\.?|D\.?O\.?|MBBS|MB\.?B\.?S\.?)\b", re.IGNORECASE
)


def build_full_name(full_name: Optional[str], credentials: Optional[str]) -> str:
    """Proper-case and append credentials: 'Dr. John Thompson, DO'."""
    name = proper_case_name(full_name)
    cred = (credentials or "").strip()
    if cred:
        return f"{name}, {cred}"
    return name


def build_salutation(
    salutation: Optional[str], credentials: Optional[str]
) -> str:
    """Build salutation: 'Dr. Thompson'.

    If NPPES already has a prefix (Dr., Mr., Ms., Mrs.), title-case and return.
    If bare last name + doctor credentials (MD/DO), prepend 'Dr.'.
    Otherwise return empty string (bare last name without doctor credentials).
    """
    raw = (salutation or "").strip()
    if not raw:
        return ""
    cased = titlecase(raw.lower())
    # Already has a prefix from NPPES
    if re.match(r"(?i)^(dr|mr|ms|mrs)\b", raw):
        return cased
    # Bare last name — derive prefix from credentials
    cred = (credentials or "").strip()
    if cred and _DOCTOR_CREDENTIALS.search(cred):
        return f"Dr. {cased}"
    # Non-doctor without prefix — bare last name only (view handles Mr./Ms. via gender)
    return cased


_STREET_ABBREVS = {
    "BLVD": "Blvd", "AVE": "Ave", "HWY": "Hwy", "PKWY": "Pkwy",
    "FWY": "Fwy", "BLDG": "Bldg", "APT": "Apt", "STE": "Ste",
    "RD": "Rd", "DR": "Dr", "LN": "Ln", "CT": "Ct", "PL": "Pl",
    "ST": "St", "CIR": "Cir", "WAY": "Way",
}
_ADDR_KEEP_UPPER = {"NE", "NW", "SE", "SW", "PO"}


def proper_case_address(raw: Optional[str]) -> str:
    """Title-case an address using titlecase, with street abbreviation fixes."""
    if not raw:
        return ""
    result = titlecase(raw.lower())
    words = result.split()
    fixed = []
    for w in words:
        upper = w.upper()
        if upper in _ADDR_KEEP_UPPER:
            fixed.append(upper)
        elif upper in _STREET_ABBREVS and w == w.upper():
            fixed.append(_STREET_ABBREVS[upper])
        else:
            fixed.append(w)
    return " ".join(fixed)


def dedup_specialties(specialties: list) -> list:
    """Remove base specialty when a more specific sub-specialty exists.

    E.g. ['Internal Medicine', 'Internal Medicine, Cardiovascular Disease']
    becomes ['Internal Medicine, Cardiovascular Disease'].
    """
    if not specialties:
        return []
    # A specialty is redundant if another entry starts with it + ", "
    result = []
    for s in specialties:
        if any(other != s and other.startswith(s + ", ") for other in specialties):
            continue
        result.append(s)
    return result


def export_doctors_profile(output_path: Optional[str] = None) -> dict:
    """
    Export doctor profiles as formatted JSON array.

    Returns:
        Dict with record_count and output_path.
    """
    client = bigquery.Client(project="bi-data-391216")

    logger.info("Querying v_doctors_enriched (primary locations)...")
    rows = list(client.query(QUERY).result())
    logger.info(f"Fetched {len(rows)} records")

    records = []
    for row in rows:
        # Insurance: prefix message + commercial payers
        insurance_list = [INSURANCE_PREFIX]
        if row.insurance:
            insurance_list.extend(list(row.insurance))

        # Geoloc
        geoloc = None
        if row.geoloc and row.geoloc.get("lat") is not None:
            geoloc = {"lat": row.geoloc["lat"], "lng": row.geoloc["lng"]}

        record = {
            "fullName": row.fullName or "",
            "salutation": row.salutation or "",
            "npi": row.npi,
            "gender": row.gender or "U",
            "genderFull": row.genderFull or "",
            "specialty": row.specialty or "",
            "specialtyList": dedup_specialties(
                list(row.specialtyList) if row.specialtyList else []
            ),
            "street": proper_case_address(row.street),
            "city": titlecase((row.city or "").lower()),
            "stateAbbr": row.stateAbbr or "",
            "stateFull": row.stateFull or "",
            "postal": (row.postal or "")[:5],
            "phone": format_phone(row.phone),
            "fax": format_phone(row.fax),
            "_geoloc": geoloc,
            "insurance": insurance_list,
            "languages": ["English"],
            "objectID": hashlib.md5(row.npi.encode()).hexdigest(),
        }
        records.append(record)

    # Write JSON array
    if output_path is None:
        os.makedirs("exports", exist_ok=True)
        output_path = os.path.join(
            "exports",
            f"doctors_profiles_{datetime.now(timezone.utc).strftime('%Y%m%d')}.json",
        )

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.info(f"Wrote {len(records)} records to {output_path} ({file_size_mb:.1f} MB)")

    # Write to BigQuery table for QC review
    table_id = "bi-data-391216.npi_data.doctor_profiles"
    bq_rows = []
    for r in records:
        bq_row = {
            "objectID": r["objectID"],
            "npi": r["npi"],
            "fullName": r["fullName"],
            "salutation": r["salutation"],
            "gender": r["gender"],
            "genderFull": r["genderFull"],
            "specialty": r["specialty"],
            "specialtyList": r["specialtyList"],
            "street": r["street"],
            "city": r["city"],
            "stateAbbr": r["stateAbbr"],
            "stateFull": r["stateFull"],
            "postal": r["postal"],
            "phone": r["phone"],
            "fax": r["fax"],
            "latitude": r["_geoloc"]["lat"] if r["_geoloc"] else None,
            "longitude": r["_geoloc"]["lng"] if r["_geoloc"] else None,
            "insurance": r["insurance"],
            "languages": r["languages"],
        }
        bq_rows.append(bq_row)

    schema = [
        bigquery.SchemaField("objectID", "STRING"),
        bigquery.SchemaField("npi", "STRING"),
        bigquery.SchemaField("fullName", "STRING"),
        bigquery.SchemaField("salutation", "STRING"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("genderFull", "STRING"),
        bigquery.SchemaField("specialty", "STRING"),
        bigquery.SchemaField("specialtyList", "STRING", mode="REPEATED"),
        bigquery.SchemaField("street", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("stateAbbr", "STRING"),
        bigquery.SchemaField("stateFull", "STRING"),
        bigquery.SchemaField("postal", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("fax", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("insurance", "STRING", mode="REPEATED"),
        bigquery.SchemaField("languages", "STRING", mode="REPEATED"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    # Write via JSONL temp file for reliable array handling
    import tempfile
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as tmp:
        for bq_row in bq_rows:
            tmp.write(json.dumps(bq_row, ensure_ascii=False) + "\n")
        tmp_path = tmp.name

    try:
        with open(tmp_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        logger.info(f"Loaded {len(bq_rows)} rows to {table_id}")
    finally:
        os.unlink(tmp_path)

    return {
        "record_count": len(records),
        "output_path": output_path,
        "file_size_mb": round(file_size_mb, 1),
        "bigquery_table": table_id,
    }


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Export doctor profiles as JSON")
    parser.add_argument("--output", type=str, help="Output file path")
    args = parser.parse_args()

    result = export_doctors_profile(output_path=args.output)

    print(f"\nExport complete:")
    print(f"  Records: {result['record_count']}")
    print(f"  File:    {result['output_path']} ({result['file_size_mb']} MB)")
