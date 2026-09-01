#!/usr/bin/env python3
"""
Recalculate row_hash for npi_main rows that have provider_gender_code set.

One-time recovery script: the gender backfill successfully patched
provider_gender_code but failed on the row_hash recalculation due to
an ambiguous column reference. This script fixes the hashes in-place
(no JOIN, no download needed).

Usage:
  python shared/rehash_gender_rows.py
"""

import logging
import os

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def build_hash_expression(client: bigquery.Client, table_name: str) -> str:
    """Build FARM_FINGERPRINT expression matching the pipeline's add_hash_column."""
    excluded = {"loaded_at", "extracted_at", "source", "execution_id", "updated_at", "row_hash"}

    table_ref = client.get_table(table_name)
    field_types = {f.name: f.field_type for f in table_ref.schema}
    hash_fields = sorted(f for f in field_types if f not in excluded)

    parts = []
    for field in hash_fields:
        ftype = field_types[field]
        if ftype in ("TIMESTAMP", "DATETIME"):
            parts.append(f"COALESCE(CAST(TIMESTAMP_TRUNC(`{field}`, MINUTE) AS STRING), '')")
        else:
            parts.append(f"COALESCE(CAST(`{field}` AS STRING), '')")

    return " || '|' || ".join(parts)


def main():
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    project = os.getenv("GCP_PROJECT_ID", "bi-data-391216")
    prod_table = f"{project}.npi_data.npi_main"

    client = bigquery.Client(project=project)

    logger.info("Building hash expression from production schema...")
    hash_expr = build_hash_expression(client, prod_table)

    # No JOIN — just recalculate hash for rows with gender data
    sql = f"""
    UPDATE `{prod_table}`
    SET row_hash = CAST(FARM_FINGERPRINT({hash_expr}) AS STRING)
    WHERE provider_gender_code IS NOT NULL
    """

    logger.info(f"Recalculating row_hash for rows with gender data...")
    job = client.query(sql)
    job.result()
    rows_affected = job.num_dml_affected_rows
    logger.info(f"Recalculated row_hash for {rows_affected:,} rows")

    print(f"\nRehash complete: {rows_affected:,} rows updated")


if __name__ == "__main__":
    main()
