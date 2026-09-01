#!/usr/bin/env python3
"""
Backfill provider_gender_code in npi_data.npi_main from current NPPES CSV.

Downloads the monthly NPPES ZIP, reads only the NPI + Provider Sex Code
columns (renamed from Provider Gender Code by CMS in 2025), and patches
the production table via a temp BigQuery table + UPDATE join.

This avoids a full re-extraction and keeps the changelog clean.

Usage:
  python shared/backfill_gender.py
  python shared/backfill_gender.py --year 2026 --month 2
"""

import logging
import os
import sys
import tempfile
import zipfile
from datetime import datetime

import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

CMS_BASE_URL = "https://download.cms.gov/nppes"


def find_and_download_nppes_zip(temp_dir: str, year: int = None, month: int = None) -> str:
    """Download the monthly NPPES V2 ZIP, trying current then previous month."""
    import requests

    now = datetime.now()
    if year is None:
        year = now.year
    if month is None:
        month = now.month

    from calendar import month_name

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (orchestrator/gender-backfill)"})

    # Try current month, then previous month
    attempts = [(year, month)]
    if month == 1:
        attempts.append((year - 1, 12))
    else:
        attempts.append((year, month - 1))

    for y, m in attempts:
        filename = f"NPPES_Data_Dissemination_{month_name[m]}_{y}_V2.zip"
        url = f"{CMS_BASE_URL}/{filename}"
        logger.info(f"Trying: {url}")

        head = session.head(url, timeout=30)
        if head.status_code != 200:
            logger.info(f"  Not found (HTTP {head.status_code})")
            continue

        zip_path = os.path.join(temp_dir, filename)
        total_size = int(head.headers.get("content-length", 0))
        logger.info(f"Downloading {total_size / (1024**3):.1f} GB ...")

        resp = session.get(url, stream=True, timeout=1800)
        resp.raise_for_status()

        downloaded = 0
        last_pct = 0
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        pct = int(downloaded / total_size * 100)
                        if pct >= last_pct + 10:
                            logger.info(f"  {pct}% ({downloaded:,} bytes)")
                            last_pct = pct

        logger.info(f"Downloaded: {zip_path}")
        return zip_path

    raise RuntimeError("Could not find NPPES ZIP for current or previous month")


def extract_gender_from_zip(zip_path: str, temp_dir: str) -> pd.DataFrame:
    """Extract only NPI + Provider Sex Code from the npidata CSV inside the ZIP."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Find the main npidata CSV
        csv_name = None
        for name in zf.namelist():
            lower = name.lower()
            if "npidata" in lower and lower.endswith(".csv") and "fileheader" not in lower:
                csv_name = name
                break

        if not csv_name:
            raise RuntimeError(f"No npidata CSV found in {zip_path}")

        logger.info(f"Reading gender data from: {csv_name}")

        # Extract to temp dir
        zf.extract(csv_name, temp_dir)
        csv_path = os.path.join(temp_dir, csv_name)

    # Read only the two columns we need — much faster than reading all 330+
    # First, peek at the header to find the exact column name
    header_df = pd.read_csv(csv_path, nrows=0, encoding="utf-8")
    columns = list(header_df.columns)

    # CMS renamed "Provider Gender Code" to "Provider Sex Code" in 2025
    gender_col = None
    for col in columns:
        if "sex" in col.lower() or "gender" in col.lower():
            if "provider" in col.lower():
                gender_col = col
                break

    if not gender_col:
        raise RuntimeError(
            f"Could not find gender/sex column in CSV. "
            f"Sample headers: {columns[:10]}..."
        )

    logger.info(f"Found gender column: '{gender_col}'")

    # Read only NPI + gender column in chunks for memory efficiency
    chunks = []
    for chunk in pd.read_csv(
        csv_path,
        usecols=["NPI", gender_col],
        dtype=str,
        chunksize=500_000,
        encoding="utf-8",
        on_bad_lines="warn",
    ):
        # Rename to our internal column name
        chunk = chunk.rename(columns={"NPI": "npi", gender_col: "provider_gender_code"})
        # Keep only rows with actual gender data
        chunk = chunk[chunk["provider_gender_code"].notna() & (chunk["provider_gender_code"].str.strip() != "")]
        chunk["provider_gender_code"] = chunk["provider_gender_code"].str.strip()
        chunks.append(chunk)
        logger.info(f"  Read {sum(len(c) for c in chunks):,} rows with gender data so far")

    df = pd.concat(chunks, ignore_index=True)
    logger.info(f"Total rows with gender data: {len(df):,}")
    logger.info(f"Gender distribution:\n{df['provider_gender_code'].value_counts().to_string()}")

    # Clean up the large CSV
    try:
        os.remove(csv_path)
        logger.info("Cleaned up extracted CSV")
    except Exception:
        pass

    return df


def _build_hash_expression(client: bigquery.Client, table_name: str, alias: str = "") -> str:
    """Build the same FARM_FINGERPRINT hash expression used by the pipeline.

    Matches the logic in shared/bigquery_utils.add_hash_column exactly:
    - Excludes metadata fields (loaded_at, extracted_at, source, execution_id, updated_at, row_hash)
    - Sorts remaining fields alphabetically
    - TIMESTAMP_TRUNC to MINUTE for timestamp/datetime fields
    - COALESCE(CAST(... AS STRING), '') for all fields
    - Concatenates with || '|' ||

    Args:
        alias: Table alias prefix for column references (e.g. "prod" -> "prod.`npi`")
    """
    excluded = {"loaded_at", "extracted_at", "source", "execution_id", "updated_at", "row_hash"}
    prefix = f"{alias}." if alias else ""

    table_ref = client.get_table(table_name)
    field_types = {f.name: f.field_type for f in table_ref.schema}
    hash_fields = sorted(f for f in field_types if f not in excluded)

    parts = []
    for field in hash_fields:
        ftype = field_types[field]
        if ftype in ("TIMESTAMP", "DATETIME"):
            parts.append(f"COALESCE(CAST(TIMESTAMP_TRUNC({prefix}`{field}`, MINUTE) AS STRING), '')")
        else:
            parts.append(f"COALESCE(CAST({prefix}`{field}` AS STRING), '')")

    return " || '|' || ".join(parts)


def patch_production_table(df: pd.DataFrame, project: str = "bi-data-391216") -> dict:
    """Load gender data to temp table, UPDATE production + row_hash, drop temp table."""
    client = bigquery.Client(project=project)
    prod_table = f"{project}.npi_data.npi_main"
    temp_table = f"{project}.npi_staging.npi_gender_backfill"

    # Load to temp table
    logger.info(f"Loading {len(df):,} rows to {temp_table}...")

    schema = [
        bigquery.SchemaField("npi", "STRING"),
        bigquery.SchemaField("provider_gender_code", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(df, temp_table, job_config=job_config)
    job.result()
    logger.info(f"Loaded {job.output_rows:,} rows to temp table")

    # Count how many production rows will be updated
    count_sql = f"""
    SELECT COUNT(*) AS cnt
    FROM `{prod_table}` prod
    INNER JOIN `{temp_table}` fix ON prod.npi = fix.npi
    WHERE prod.provider_gender_code IS NULL
      AND fix.provider_gender_code IS NOT NULL
    """
    count_result = list(client.query(count_sql).result())
    update_count = count_result[0].cnt
    logger.info(f"Rows to update (NULL -> populated): {update_count:,}")

    # Build hash expression matching the pipeline's FARM_FINGERPRINT logic
    logger.info("Building row_hash expression from production schema...")
    hash_expr = _build_hash_expression(client, prod_table, alias="prod")

    # UPDATE production: set gender, then recalculate row_hash.
    # Must be two statements because BigQuery's UPDATE SET evaluates all
    # expressions against the ORIGINAL row values — so a hash computed in
    # the same SET would still see the old NULL gender, not the new value.

    # Step 1: patch gender
    update_gender_sql = f"""
    UPDATE `{prod_table}` prod
    SET prod.provider_gender_code = fix.provider_gender_code
    FROM `{temp_table}` fix
    WHERE prod.npi = fix.npi
      AND fix.provider_gender_code IS NOT NULL
    """
    logger.info("Step 3a: Updating provider_gender_code...")
    gender_job = client.query(update_gender_sql)
    gender_job.result()
    rows_affected = gender_job.num_dml_affected_rows
    logger.info(f"Updated {rows_affected:,} rows with gender data")

    # Step 2: recalculate row_hash for affected rows
    rehash_sql = f"""
    UPDATE `{prod_table}` prod
    SET prod.row_hash = CAST(FARM_FINGERPRINT({hash_expr}) AS STRING)
    FROM `{temp_table}` fix
    WHERE prod.npi = fix.npi
      AND fix.provider_gender_code IS NOT NULL
    """
    logger.info("Step 3b: Recalculating row_hash for updated rows...")
    rehash_job = client.query(rehash_sql)
    rehash_job.result()
    rehash_affected = rehash_job.num_dml_affected_rows
    logger.info(f"Recalculated row_hash for {rehash_affected:,} rows")

    # Drop temp table
    client.delete_table(temp_table, not_found_ok=True)
    logger.info(f"Dropped temp table {temp_table}")

    # Verify
    verify_sql = f"""
    SELECT
      provider_gender_code,
      COUNT(*) AS cnt
    FROM `{prod_table}`
    WHERE provider_gender_code IS NOT NULL
    GROUP BY provider_gender_code
    ORDER BY cnt DESC
    """
    verify = list(client.query(verify_sql).result())
    logger.info("Verification - gender distribution in production:")
    for row in verify:
        logger.info(f"  {row.provider_gender_code}: {row.cnt:,}")

    return {
        "rows_loaded": len(df),
        "rows_updated": rows_affected,
        "null_before": update_count,
    }


def main():
    import argparse
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(
        description="Backfill provider_gender_code in npi_data.npi_main from NPPES CSV"
    )
    parser.add_argument("--year", type=int, help="NPPES file year (default: current)")
    parser.add_argument("--month", type=int, help="NPPES file month (default: current)")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="npi_gender_") as temp_dir:
        logger.info("=== Step 1: Download NPPES ZIP ===")
        zip_path = find_and_download_nppes_zip(temp_dir, args.year, args.month)

        logger.info("\n=== Step 2: Extract NPI + Gender from CSV ===")
        df = extract_gender_from_zip(zip_path, temp_dir)

        # Clean up ZIP early to free disk space
        try:
            os.remove(zip_path)
            logger.info("Cleaned up ZIP file")
        except Exception:
            pass

        logger.info("\n=== Step 3: Patch production table ===")
        result = patch_production_table(df)

    print(f"\nBackfill complete:")
    print(f"  Rows with gender data: {result['rows_loaded']:,}")
    print(f"  Production rows updated: {result['rows_updated']:,}")


if __name__ == "__main__":
    main()
