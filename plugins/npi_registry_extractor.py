#!/usr/bin/env python3
"""
CMS NPI Registry Extractor Plugin

Downloads and processes the official NPI Registry from CMS NPPES.
Source: https://download.cms.gov/nppes/NPI_Files.html

Supports:
- Monthly full file download (~1GB compressed, ~8GB uncompressed)
- Reference files (other names, practice locations, endpoints)
- Chunked processing for memory efficiency
- Date-based versioning for historical snapshots
"""

import gc
import os
import sys
import time
import logging
import zipfile
import tempfile
import requests
from pathlib import Path
from typing import Dict, List, Any, Optional, Iterator
from datetime import datetime, date, timezone
from calendar import month_name
import pandas as pd
from google.cloud import bigquery

# Import shared utilities
from shared.extractor_utils import get_available_tables
from shared.account_context import set_execution_metadata
from shared.gcs_pipeline import (
    extract_to_local_parquet,
    stream_chunks_to_local_parquet,
)

# Set up logging
logger = logging.getLogger(__name__)

# CMS NPPES download configuration
CMS_BASE_URL = "https://download.cms.gov/nppes"
CMS_FILE_PATTERN = "NPPES_Data_Dissemination_{month}_{year}.zip"
CMS_FILE_PATTERN_V2 = "NPPES_Data_Dissemination_{month}_{year}_V2.zip"
# Deactivated NPI Report - separate ZIP file published monthly (~12th of month)
# Format: NPPES_Deactivated_NPI_Report_MMDDYY.zip (date is publication date, typically 12th)
# V2 has extended field lengths (same as main file)
CMS_DEACTIVATION_PATTERN = "NPPES_Deactivated_NPI_Report_{date}.zip"
CMS_DEACTIVATION_PATTERN_V2 = "NPPES_Deactivated_NPI_Report_{date}_V2.zip"


class NPIRegistryClient:
    """Client for downloading NPI Registry files from CMS NPPES"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize NPI Registry client

        Args:
            config: Source configuration from YAML
        """
        self.config = config
        connection = config.get("source", {}).get("connection", {})

        self.base_url = connection.get("base_url", CMS_BASE_URL)
        self.file_version = connection.get("file_version", 2)
        self.timeout = connection.get("download_timeout_seconds", 1800)
        self.chunk_size = connection.get("chunk_size", 8192)
        self.max_retries = connection.get("max_retries", 3)
        self.retry_delay = connection.get("retry_delay_seconds", 30)

        # Create session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (orchestrator/npi-extractor)"}
        )

    def get_download_url(self, year: int = None, month: int = None) -> str:
        """
        Build download URL for NPI file

        Args:
            year: Year (defaults to current year)
            month: Month (defaults to current month)

        Returns:
            Full download URL
        """
        if year is None:
            year = datetime.now().year
        if month is None:
            month = datetime.now().month

        month_str = month_name[month]

        if self.file_version == 2:
            filename = CMS_FILE_PATTERN_V2.format(month=month_str, year=year)
        else:
            filename = CMS_FILE_PATTERN.format(month=month_str, year=year)

        return f"{self.base_url}/{filename}"

    def download_file(self, url: str, output_path: str) -> bool:
        """
        Download NPI file with retry logic and progress tracking

        Args:
            url: Download URL
            output_path: Local path to save file

        Returns:
            True if successful, False otherwise
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    f"Downloading NPI file (attempt {attempt}/{self.max_retries})"
                )
                logger.info(f"URL: {url}")

                response = self.session.get(url, stream=True, timeout=self.timeout)
                response.raise_for_status()

                total_size = int(response.headers.get("content-length", 0))
                downloaded = 0
                last_log_pct = 0

                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)

                            if total_size > 0:
                                pct = int((downloaded / total_size) * 100)
                                # Log every 10%
                                if pct >= last_log_pct + 10:
                                    logger.info(
                                        f"Download progress: {pct}% ({downloaded:,} / {total_size:,} bytes)"
                                    )
                                    last_log_pct = pct

                logger.info(
                    f"Download complete: {downloaded:,} bytes saved to {output_path}"
                )
                return True

            except requests.exceptions.RequestException as e:
                logger.warning(f"Download attempt {attempt} failed: {e}")

                if attempt < self.max_retries:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Download failed after {self.max_retries} attempts")
                    return False

        return False

    def find_latest_file(self) -> Optional[str]:
        """
        Find the most recent NPI file available for download

        Tries current month first, then previous month

        Returns:
            Download URL if found, None otherwise
        """
        now = datetime.now()

        # Try current month
        url = self.get_download_url(now.year, now.month)
        if self._check_url_exists(url):
            return url

        # Try previous month
        if now.month == 1:
            prev_year = now.year - 1
            prev_month = 12
        else:
            prev_year = now.year
            prev_month = now.month - 1

        url = self.get_download_url(prev_year, prev_month)
        if self._check_url_exists(url):
            return url

        logger.warning("Could not find recent NPI file")
        return None

    def _check_url_exists(self, url: str) -> bool:
        """Check if URL is accessible"""
        try:
            response = self.session.head(url, timeout=30)
            return response.status_code == 200
        except Exception:
            return False

    def get_deactivation_url(self, year: int, month: int, day: int) -> str:
        """
        Build download URL for deactivated NPI report

        The deactivation report uses date format MMDDYY (6 digits).
        CMS typically publishes on the 12th of each month.

        Args:
            year: Year
            month: Month
            day: Day of month

        Returns:
            Full download URL for deactivation report (V2 version)
        """
        # Use 2-digit year format
        year_2digit = year % 100
        date_str = f"{month:02d}{day:02d}{year_2digit:02d}"

        # Prefer V2 version (extended field lengths)
        if self.file_version == 2:
            filename = CMS_DEACTIVATION_PATTERN_V2.format(date=date_str)
        else:
            filename = CMS_DEACTIVATION_PATTERN.format(date=date_str)

        return f"{self.base_url}/{filename}"

    def find_latest_deactivation_file(self) -> Optional[str]:
        """
        Find the most recent deactivated NPI report available

        CMS publishes deactivation reports around the 12th of each month.
        Tries multiple dates around that time.

        Returns:
            Download URL if found, None otherwise
        """
        now = datetime.now()

        # Try last few months, checking days around the 12th
        publication_days = [12, 11, 13, 10, 14, 15, 9]  # Most likely days

        for months_back in range(4):
            if now.month - months_back < 1:
                check_year = now.year - 1
                check_month = 12 + (now.month - months_back)
            else:
                check_year = now.year
                check_month = now.month - months_back

            for day in publication_days:
                # Try V2 first, then V1
                for version in [2, 1]:
                    self.file_version = version
                    url = self.get_deactivation_url(check_year, check_month, day)
                    logger.debug(f"Checking deactivation URL: {url}")

                    if self._check_url_exists(url):
                        logger.info(f"Found deactivation report: {url}")
                        return url

        logger.warning("Could not find recent deactivation report")
        return None


def extract_zip_contents(zip_path: str, extract_dir: str) -> Dict[str, str]:
    """
    Extract ZIP file and return paths to extracted files

    Args:
        zip_path: Path to ZIP file
        extract_dir: Directory to extract to

    Returns:
        Dictionary mapping file type to extracted path
    """
    logger.info(f"Extracting ZIP: {zip_path}")

    extracted_files = {}

    with zipfile.ZipFile(zip_path, "r") as zf:
        file_list = zf.namelist()
        logger.info(f"ZIP contains {len(file_list)} files")

        for filename in file_list:
            # Skip directories, documentation, and header files
            if filename.endswith("/") or filename.lower().endswith(".pdf"):
                continue

            # Skip header/metadata files - we want the actual data files
            if "_fileheader" in filename.lower() or "readme" in filename.lower():
                logger.debug(f"Skipping header/readme file: {filename}")
                continue

            # Identify file type
            lower_name = filename.lower()
            if "npidata" in lower_name and lower_name.endswith(".csv"):
                file_type = "npi_main"
            elif "othername" in lower_name and lower_name.endswith(".csv"):
                file_type = "other_names"
            elif "pl_pfile" in lower_name and lower_name.endswith(".csv"):
                file_type = "practice_locations"
            elif "endpoint" in lower_name and lower_name.endswith(".csv"):
                file_type = "endpoints"
            else:
                logger.debug(f"Skipping file: {filename}")
                continue

            # Extract file
            extract_path = os.path.join(extract_dir, filename)
            zf.extract(filename, extract_dir)
            extracted_files[file_type] = extract_path
            logger.info(f"Extracted {file_type}: {filename}")

    return extracted_files


def process_deactivations_zip(
    zip_path: str,
    table_config: Dict[str, Any],
    config: Dict[str, Any],
    execution_id: str,
    temp_dir: str,
    bq_client: Optional[bigquery.Client] = None,
) -> tuple[Optional[str], int]:
    """
    Process the deactivated NPI report ZIP file

    The ZIP contains a CSV file with deactivated NPI records.

    Args:
        zip_path: Path to ZIP file
        table_config: Table configuration from YAML
        config: Full config
        execution_id: Unique execution ID
        temp_dir: Temp directory for extraction
        bq_client: BigQuery client for schema enforcement

    Returns:
        Tuple of (path to parquet file or None, row count)
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing deactivations ZIP: {zip_path}")
    logger.info(f"{'='*60}")

    try:
        # Extract the ZIP file to find the data file inside
        data_path = None
        with zipfile.ZipFile(zip_path, "r") as zf:
            file_list = zf.namelist()
            logger.info(f"Deactivations ZIP contains: {file_list}")

            # Look for the data file - could be CSV or XLSX format
            file_format = None
            for filename in file_list:
                lower_name = filename.lower()
                # Skip header files and directories
                if "header" in lower_name or "readme" in lower_name:
                    continue
                if filename.endswith("/"):
                    continue
                # Accept CSV or XLSX files
                if lower_name.endswith(".csv"):
                    extract_path = os.path.join(temp_dir, filename)
                    zf.extract(filename, temp_dir)
                    data_path = extract_path
                    file_format = "csv"
                    logger.info(f"Extracted deactivation CSV: {filename}")
                    break
                elif lower_name.endswith(".xlsx") or lower_name.endswith(".xls"):
                    extract_path = os.path.join(temp_dir, filename)
                    zf.extract(filename, temp_dir)
                    data_path = extract_path
                    file_format = "xlsx"
                    logger.info(f"Extracted deactivation Excel: {filename}")
                    break

        if not data_path:
            logger.error("No data file found in deactivations ZIP")
            return None, 0

        # Read data file based on format
        # CMS Excel files have a title row before the actual headers
        # Row 0: Title (e.g., "NPPES Deactivated Records as of Jan 12 2026")
        # Row 1: Actual column headers (NPI, NPPES Deactivation Date, etc.)
        if file_format == "xlsx":
            df = pd.read_excel(data_path, dtype=str, header=1)
        else:
            df = pd.read_csv(data_path, dtype=str, encoding="utf-8")
        logger.info(f"Read {len(df):,} rows from deactivation file")
        logger.info(f"Columns: {list(df.columns)}")

        # Normalize column names
        df.columns = [normalize_column_name(col) for col in df.columns]
        logger.info(f"Normalized columns: {list(df.columns)}")

        # Map to expected schema
        # CMS deactivation report typically has: NPI, NPPES Deactivation Date, NPPES Reactivation Date
        column_map = {
            "npi": "npi",
            "nppes_deactivation_date": "deactivation_date",
            "nppes_reactivation_date": "reactivation_date",
            "deactivation_date": "deactivation_date",
            "reactivation_date": "reactivation_date",
        }

        # Apply column mapping
        rename_map = {
            col: column_map.get(col, col) for col in df.columns if col in column_map
        }
        df = df.rename(columns=rename_map)

        # Convert NPI to string
        if "npi" in df.columns:
            df["npi"] = df["npi"].astype(str).str.zfill(10)

        # Parse date columns
        for date_col in ["deactivation_date", "reactivation_date"]:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        # Convert to records
        records = df.to_dict("records")

        # Add execution metadata
        set_execution_metadata(records, execution_id, source="npi_registry")

        logger.info(f"Total deactivation records: {len(records):,}")

        # Get production table ID for schema enforcement
        pipeline_config = config.get("pipeline", {})
        project = pipeline_config.get(
            "project", os.getenv("GCP_PROJECT_ID", "bi-data-391216")
        )
        production_dataset = pipeline_config.get("production_dataset", "npi_data")
        bq_table_name = table_config.get("table_name", "npi_deactivations")
        production_table_id = f"{project}.{production_dataset}.{bq_table_name}"

        # Extract to parquet
        parquet_path = extract_to_local_parquet(
            data=records,
            source="npi_registry",
            table=bq_table_name,
            job_id=execution_id,
            bq_client=bq_client,
            production_table_id=production_table_id,
        )

        if parquet_path:
            logger.info(f"Created deactivations parquet: {parquet_path}")

        return parquet_path, len(records)

    except Exception as e:
        logger.error(f"Error processing deactivations XLSX: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return None, 0


def normalize_column_name(name: str) -> str:
    """
    Normalize column name to lowercase_underscore format

    Examples:
        "NPI" -> "npi"
        "Provider First Name" -> "provider_first_name"
        "Healthcare Provider Taxonomy Code_1" -> "healthcare_provider_taxonomy_code_1"
    """
    import re

    # Replace spaces, hyphens, parentheses with underscores
    name = re.sub(r"[\s\-\(\)]+", "_", name)
    # Remove special characters except underscores
    name = re.sub(r"[^\w_]", "", name)
    # Convert to lowercase
    name = name.lower()
    # Remove duplicate underscores
    name = re.sub(r"_+", "_", name)
    # Remove leading/trailing underscores
    name = name.strip("_")
    return name


def parse_csv_chunked(
    csv_path: str,
    table_config: Dict[str, Any],
    column_mapping: Dict[str, str],
    chunk_size: int = 100000,
) -> Iterator[pd.DataFrame]:
    """
    Parse large CSV file in chunks

    Args:
        csv_path: Path to CSV file
        table_config: Table configuration from YAML
        column_mapping: Column name mapping (CMS -> schema)
        chunk_size: Number of rows per chunk

    Yields:
        DataFrame chunks with standardized columns
    """
    schema = table_config.get("schema", {})
    date_columns = [col for col, dtype in schema.items() if dtype == "DATE"]

    logger.info(f"Parsing CSV in chunks of {chunk_size:,} rows: {csv_path}")

    # Read CSV in chunks. low_memory=False used to be set here to suppress the
    # mixed-dtype DtypeWarning, but with dtype=str declared above pandas knows
    # the column types up front so low_memory has no effect on correctness --
    # only on the tokenizer's allocation strategy. Removing it lets the C parser
    # release memory between rows within a chunk instead of pre-allocating one
    # large per-chunk buffer, which on the npi_main 9.4M-row / 330-column CSV
    # is a meaningful difference on memory-constrained worker machines.
    chunk_iter = pd.read_csv(
        csv_path,
        chunksize=chunk_size,
        dtype=str,  # Read all as string initially
        encoding="utf-8",
        on_bad_lines="warn",
    )

    chunk_num = 0
    for chunk in chunk_iter:
        chunk_num += 1
        logger.info(f"Processing chunk {chunk_num} ({len(chunk):,} rows)")

        # Log original columns on first chunk
        if chunk_num == 1:
            logger.info(
                f"Original CSV columns: {list(chunk.columns)[:10]}..."
            )  # First 10

        # First apply explicit column mapping if provided
        if column_mapping:
            chunk = chunk.rename(columns=column_mapping)

        # Then auto-normalize any remaining columns to lowercase_underscore
        auto_mapping = {col: normalize_column_name(col) for col in chunk.columns}
        chunk = chunk.rename(columns=auto_mapping)

        # Log normalized columns on first chunk
        if chunk_num == 1:
            logger.info(f"Normalized columns: {list(chunk.columns)[:10]}...")

        # Keep only columns in schema (if schema is defined)
        if schema:
            schema_columns = list(schema.keys())
            # Remove system metadata columns that aren't in source
            source_columns = [
                c
                for c in schema_columns
                if c not in ["extracted_at", "execution_id", "source"]
            ]
            available_columns = [c for c in source_columns if c in chunk.columns]

            if available_columns:
                chunk = chunk[available_columns]
            else:
                # No schema columns found - keep all columns
                logger.warning(
                    f"No schema columns matched in chunk, keeping all {len(chunk.columns)} columns"
                )

        # Parse date columns
        for col in date_columns:
            if col in chunk.columns:
                chunk[col] = pd.to_datetime(
                    chunk[col], format="%m/%d/%Y", errors="coerce"
                )

        yield chunk


def process_npi_table(
    csv_path: str,
    table_name: str,
    table_config: Dict[str, Any],
    column_mapping: Dict[str, str],
    config: Dict[str, Any],
    execution_id: str,
    test_mode: bool = False,
    bq_client: Optional[bigquery.Client] = None,
) -> tuple[Optional[str], int]:
    """
    Process a single NPI table and save to parquet

    Args:
        csv_path: Path to extracted CSV
        table_name: Table name (e.g., 'npi_main')
        table_config: Table configuration from YAML
        column_mapping: Column name mapping
        config: Full config
        execution_id: Unique execution ID
        test_mode: If True, limit to 10000 rows
        bq_client: BigQuery client for schema enforcement

    Returns:
        Tuple of (path to parquet file or None, row count)
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing table: {table_name}")
    logger.info(f"{'='*60}")

    try:
        processing_config = config.get("source", {}).get("processing", {})
        chunk_size = processing_config.get("chunk_size", 100000)

        # Get production table ID for schema enforcement (resolved up-front so
        # the streaming helper has it from the first chunk).
        pipeline_config = config.get("pipeline", {})
        project = pipeline_config.get(
            "project", os.getenv("GCP_PROJECT_ID", "bi-data-391216")
        )
        production_dataset = pipeline_config.get("production_dataset", "npi_data")
        bq_table_name = table_config.get("table_name", table_name)
        production_table_id = f"{project}.{production_dataset}.{bq_table_name}"

        # Stream chunks straight to Parquet -- no accumulation. Previously this
        # function built an all_records list (every parsed chunk extended into a
        # Python list of dicts) and handed it to extract_to_local_parquet. On
        # npi_main (9.4M rows, ~330 columns) that materialized ~15-20 GB of
        # Python dicts in RAM, which the Linux OOM-killer terminated at ~7.4M
        # rows on production worker VMs (silent "Killed" with no traceback).
        # The generator below yields one chunk_df at a time; the streaming
        # helper writes each to Parquet via pyarrow and lets each chunk be
        # garbage-collected before the next is parsed. Peak memory stays at
        # one chunk's worth (~50-100 MB) regardless of total row count.
        row_counter = {"total": 0}

        def _chunks_for_streaming():
            nonlocal_total = 0
            for chunk_df in parse_csv_chunked(
                csv_path, table_config, column_mapping, chunk_size
            ):
                # Stamp execution metadata directly on the DataFrame so we
                # avoid the chunk_df -> list-of-dicts -> back-to-DataFrame
                # round trip the non-streaming path did. extracted_at is a
                # timezone-aware UTC timestamp the schema declares as TIMESTAMP.
                chunk_df = chunk_df.copy()
                chunk_df["extracted_at"] = datetime.now(timezone.utc)
                chunk_df["execution_id"] = execution_id
                chunk_df["source"] = "npi_registry"

                nonlocal_total += len(chunk_df)
                row_counter["total"] = nonlocal_total
                logger.info(f"Total rows processed: {nonlocal_total:,}")

                yield chunk_df

                # Test mode limit: stop streaming once we've yielded enough.
                if test_mode and nonlocal_total >= 10000:
                    logger.info(f"Test mode: limiting to {nonlocal_total:,} rows")
                    return

        parquet_path = stream_chunks_to_local_parquet(
            chunk_iter=_chunks_for_streaming(),
            source="npi_registry",
            table=bq_table_name,
            job_id=execution_id,
            bq_client=bq_client,
            production_table_id=production_table_id,
        )

        total_rows = row_counter["total"]
        if parquet_path is None:
            logger.warning(f"No data extracted for {table_name}")
            return None, 0

        logger.info(
            f"Total records for {table_name}: {total_rows:,}; "
            f"parquet: {parquet_path}"
        )
        return parquet_path, total_rows

    except Exception as e:
        logger.error(f"Error processing {table_name}: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return None, 0


# Type code descriptions for Other Provider Identifiers
OTHER_ID_TYPE_DESCRIPTIONS = {
    "01": "Other",
    "02": "Medicare UPIN",
    "03": "State License Number",
    "04": "Medicare ID (Obsolete)",
    "05": "Medicaid",
    "06": "Medicare OSCAR/Certification",
    "07": "Medicare NSC",
    "08": "Medicare PIN",
}


def process_other_identifiers(
    csv_path: str,
    table_config: Dict[str, Any],
    config: Dict[str, Any],
    execution_id: str,
    test_mode: bool = False,
    bq_client: Optional[bigquery.Client] = None,
) -> tuple[Optional[str], int]:
    """
    Unpivot Other Provider Identifier columns (slots 1-50) from the wide
    npi_main CSV into a normalized table with one row per identifier.

    The NPPES CSV has 50 repeating groups of 4 columns each:
        Other Provider Identifier_{n}
        Other Provider Identifier Type Code_{n}
        Other Provider Identifier State_{n}
        Other Provider Identifier Issuer_{n}

    This function reads the CSV, extracts only NPI + these 200 columns,
    and unpivots them into rows.
    """
    logger.info(f"\n{'='*60}")
    logger.info("Processing table: other_identifiers (unpivot from npi_main CSV)")
    logger.info(f"{'='*60}")

    try:
        processing_config = config.get("source", {}).get("processing", {})
        chunk_size = processing_config.get("chunk_size", 100000)

        # Build the list of columns we need from the CSV
        # We need NPI + all 50 slots of other_provider_identifier
        cols_needed = ["NPI"]
        for i in range(1, 51):
            cols_needed.append(f"Other Provider Identifier_{i}")
            cols_needed.append(f"Other Provider Identifier Type Code_{i}")
            cols_needed.append(f"Other Provider Identifier State_{i}")
            cols_needed.append(f"Other Provider Identifier Issuer_{i}")

        logger.info(f"Reading {len(cols_needed)} columns from CSV for unpivot")

        # Get production table ID for schema enforcement (resolved up front so
        # the streaming helper has it from the first chunk).
        pipeline_config = config.get("pipeline", {})
        project = pipeline_config.get(
            "project", os.getenv("GCP_PROJECT_ID", "bi-data-391216")
        )
        production_dataset = pipeline_config.get("production_dataset", "npi_data")
        bq_table_name = table_config.get("table_name", "npi_other_identifiers")
        production_table_id = f"{project}.{production_dataset}.{bq_table_name}"

        # Stream chunks straight to Parquet. The non-streaming path appended
        # every chunk's unpivoted DataFrame into all_frames, then ran
        # pd.concat(all_frames) + .to_dict("records") at the end -- that's
        # roughly 2-3x the npi_main RAM cost because unpivot expands each row
        # by the number of populated identifier slots (typical 1-5; up to 50).
        # The generator below yields one unpivoted, metadata-stamped chunk_df
        # at a time and the helper writes each to Parquet immediately.
        row_counter = {"total": 0, "source_rows": 0}

        # Halve the chunksize for this read specifically. The npi_main
        # extractor's chunk_size (typically 100k) is sized around the full
        # 330-column schema; this read only needs 201 columns but those columns
        # are wider strings on average (provider identifiers, full state names),
        # and the tokenizer buffer is chunksize-proportional regardless. The
        # second NPI pass also kicks off RIGHT AFTER npi_main finishes, so it
        # inherits whatever string-allocator state pandas held over from that
        # read. Smaller chunks here keep the tokenizer comfortable.
        unpivot_chunk_size = min(chunk_size, 50_000)

        def _unpivoted_chunks():
            nonlocal_unpivoted = 0
            nonlocal_source = 0
            chunk_num = 0
            # usecols=cols_needed (list) instead of a lambda: pandas can prune
            # unused columns AT TOKENIZATION TIME with a list, but a lambda is
            # applied AFTER each row is fully tokenized -- meaning all ~330
            # columns are still read into temporary string buffers just to
            # throw 129 of them away. That waste was the dominant memory cost
            # on this read; switching to a list cuts the tokenizer pressure
            # roughly in proportion to the unused-column fraction.
            for chunk in pd.read_csv(
                csv_path,
                chunksize=unpivot_chunk_size,
                dtype=str,
                encoding="utf-8",
                on_bad_lines="warn",
                usecols=cols_needed,
            ):
                chunk_num += 1
                logger.info(f"Unpivot chunk {chunk_num} ({len(chunk):,} rows)")
                nonlocal_source += len(chunk)

                # Vectorized unpivot: melt each slot and concatenate within
                # this chunk. The previous pattern accumulated across chunks;
                # we keep the per-slot concat only INSIDE this chunk (always
                # small) so peak memory stays bounded.
                slot_frames = []
                for i in range(1, 51):
                    id_col = f"Other Provider Identifier_{i}"
                    type_col = f"Other Provider Identifier Type Code_{i}"
                    state_col = f"Other Provider Identifier State_{i}"
                    issuer_col = f"Other Provider Identifier Issuer_{i}"

                    if id_col not in chunk.columns:
                        continue
                    mask = chunk[id_col].notna() & (chunk[id_col].str.strip() != "")
                    if not mask.any():
                        continue

                    subset = pd.DataFrame(
                        {
                            "npi": chunk.loc[mask, "NPI"],
                            "identifier": chunk.loc[mask, id_col],
                            "type_code": (
                                chunk.loc[mask, type_col]
                                if type_col in chunk.columns
                                else None
                            ),
                            "state": (
                                chunk.loc[mask, state_col]
                                if state_col in chunk.columns
                                else None
                            ),
                            "issuer": (
                                chunk.loc[mask, issuer_col]
                                if issuer_col in chunk.columns
                                else None
                            ),
                            "identifier_sequence": i,
                        }
                    )
                    slot_frames.append(subset)

                if not slot_frames:
                    # No identifiers in this chunk; don't yield an empty
                    # frame -- the helper would skip it anyway but we save
                    # the round trip.
                    if test_mode and nonlocal_source >= 10000:
                        logger.info(
                            f"Test mode: processed {nonlocal_source:,} source rows"
                        )
                        return
                    continue

                chunk_unpivoted = pd.concat(slot_frames, ignore_index=True)
                # Clean up values.
                chunk_unpivoted["npi"] = chunk_unpivoted["npi"].str.strip()
                chunk_unpivoted["identifier"] = chunk_unpivoted[
                    "identifier"
                ].str.strip()
                chunk_unpivoted["type_code"] = (
                    chunk_unpivoted["type_code"].fillna("").str.strip()
                )
                chunk_unpivoted["type_description"] = (
                    chunk_unpivoted["type_code"]
                    .map(OTHER_ID_TYPE_DESCRIPTIONS)
                    .fillna("Unknown")
                )
                chunk_unpivoted["state"] = (
                    chunk_unpivoted["state"].fillna("").str.strip()
                )
                chunk_unpivoted["state"] = chunk_unpivoted["state"].replace("", None)
                chunk_unpivoted["issuer"] = (
                    chunk_unpivoted["issuer"].fillna("").str.strip()
                )
                chunk_unpivoted["issuer"] = chunk_unpivoted["issuer"].replace("", None)

                # Stamp execution metadata on the DataFrame directly.
                chunk_unpivoted["extracted_at"] = datetime.now(timezone.utc)
                chunk_unpivoted["execution_id"] = execution_id
                chunk_unpivoted["source"] = "npi_registry"

                nonlocal_unpivoted += len(chunk_unpivoted)
                row_counter["total"] = nonlocal_unpivoted
                row_counter["source_rows"] = nonlocal_source

                yield chunk_unpivoted

                if test_mode and nonlocal_source >= 10000:
                    logger.info(f"Test mode: processed {nonlocal_source:,} source rows")
                    return

        parquet_path = stream_chunks_to_local_parquet(
            chunk_iter=_unpivoted_chunks(),
            source="npi_registry",
            table=bq_table_name,
            job_id=execution_id,
            bq_client=bq_client,
            production_table_id=production_table_id,
        )

        total_unpivoted = row_counter["total"]
        total_source = row_counter["source_rows"]
        if parquet_path is None:
            logger.warning("No other identifiers found in data")
            return None, 0

        logger.info(
            f"Unpivoted {total_unpivoted:,} identifiers from {total_source:,} "
            f"source rows; parquet: {parquet_path}"
        )
        return parquet_path, total_unpivoted

    except Exception as e:
        logger.error(f"Error processing other_identifiers: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return None, 0


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
    NPI Registry extraction pipeline.

    Downloads monthly NPI file from CMS NPPES and processes into BigQuery.

    Args:
        config: Full orchestrator configuration
        sites: Not used (NPI is single source)
        tables: List of table names to extract (npi_main, other_names, etc.)
        group: Table group name
        refresh_mode: "full" or "incremental" (NPI is always full refresh)
        lookback_days: Not used (NPI is monthly snapshot)
        start_date: Optional specific month (YYYY-MM format)
        end_date: Not used
        test_mode: Whether running in test mode (limits rows)
        batch_size: Processing batch size
        max_retries: Number of download retries
        skip_hash_merge: Skip hash merge step
        archive_staging: Archive staging tables
        truncate_staging: Truncate staging after merge
        rebuild: Force rebuild tables
        bq_client: BigQuery client
        execution_id: Unique execution identifier
        **kwargs: Additional arguments

    Returns:
        Dictionary with extraction results and parquet file locations
    """
    logger.info("=" * 80)
    logger.info("NPI REGISTRY EXTRACTION PIPELINE")
    logger.info("=" * 80)

    # Standardized per-table accounting + return shape (shared/extraction_result).
    # NPI streams each table to its own Parquet file itself, so the consolidated
    # path is recorded via record_prewritten. A per-table processing failure is
    # NON-fatal here (matches the prior code, which left success=True and only set
    # it False on an outer exception) -- so use note_error, not fail_table.
    from shared.extraction_result import StandardExtractionResult

    source_name = config.get("source", {}).get("name", "npi_registry")
    result = StandardExtractionResult(
        source=source_name,
        execution_id=execution_id,
        job_id=execution_id,
        bq_client=bq_client,
    )

    try:
        # Determine which tables to extract
        if not tables:
            if group:
                groups = config.get("groups", {})
                tables = groups.get(group, [])
            else:
                tables = get_available_tables(config)

        logger.info(f"Tables to extract: {tables}")

        # Create temp directory for downloads
        temp_dir = tempfile.mkdtemp(prefix="npi_registry_")
        logger.info(f"Temp directory: {temp_dir}")

        # Initialize client
        client = NPIRegistryClient(config)

        # Check if we need the main ZIP file (any table other than deactivations)
        tables_from_main_zip = [t for t in tables if t != "deactivations"]
        need_main_zip = len(tables_from_main_zip) > 0

        extracted_files = {}
        zip_path = None

        if need_main_zip:
            # Determine which file to download
            if start_date:
                # Parse YYYY-MM format
                dt = datetime.strptime(start_date, "%Y-%m")
                download_url = client.get_download_url(dt.year, dt.month)
            else:
                # Find latest available file
                download_url = client.find_latest_file()

            if not download_url:
                error_msg = "Could not determine NPI file to download"
                logger.error(error_msg)
                result.note_error(error_msg)
                return result.finalize(success=False)

            logger.info(f"Download URL: {download_url}")

            # Download ZIP file
            zip_path = os.path.join(temp_dir, "npi_data.zip")

            if not client.download_file(download_url, zip_path):
                error_msg = f"Failed to download NPI file from {download_url}"
                logger.error(error_msg)
                result.note_error(error_msg)
                return result.finalize(success=False)

            # Extract ZIP contents
            extracted_files = extract_zip_contents(zip_path, temp_dir)
            logger.info(f"Extracted files: {list(extracted_files.keys())}")
        else:
            logger.info("Skipping main ZIP download - only deactivations requested")

        # Get column mappings from config
        column_mappings = config.get("column_mapping", {})

        # Process each requested table
        resources = config.get("resources", {})

        for table_name in tables:
            table_config = resources.get(table_name)

            if not table_config:
                logger.warning(
                    f"Table '{table_name}' not found in configuration, skipping"
                )
                continue

            if not table_config.get("active", True):
                logger.info(f"Table '{table_name}' is inactive, skipping")
                continue

            # Force a GC pass before each table processes its CSV. Python does
            # not return memory to the OS aggressively after a large pandas
            # read; running a collect cycle between tables reclaims the prior
            # table's tokenizer buffers and string objects before the next
            # read_csv pre-allocates its own. Without this, the second NPI
            # read (other_identifiers) inherited whatever allocator state
            # npi_main held over and OOMed in the C tokenizer on chunk 23.
            gc.collect()

            # other_identifiers uses the npi_main CSV with a custom unpivot processor
            if table_name == "other_identifiers":
                if "npi_main" not in extracted_files:
                    logger.warning("other_identifiers requires npi_main CSV, skipping")
                    continue
                parquet_path, row_count = process_other_identifiers(
                    csv_path=extracted_files["npi_main"],
                    table_config=table_config,
                    config=config,
                    execution_id=execution_id,
                    test_mode=test_mode,
                    bq_client=bq_client,
                )
            else:
                # Check if we have the source file
                if table_name not in extracted_files:
                    logger.warning(
                        f"Source file for '{table_name}' not found in ZIP, skipping"
                    )
                    continue

                csv_path = extracted_files[table_name]
                column_mapping = column_mappings.get(table_name, {})

                # Process table
                parquet_path, row_count = process_npi_table(
                    csv_path=csv_path,
                    table_name=table_name,
                    table_config=table_config,
                    column_mapping=column_mapping,
                    config=config,
                    execution_id=execution_id,
                    test_mode=test_mode,
                    bq_client=bq_client,
                )

            if parquet_path:
                bq_table_name = table_config.get("table_name", table_name)
                # Record under the LOGICAL resource key (table_name) so the
                # orchestrator can look up config.resources[key].
                result.record_prewritten(table_name, parquet_path, row_count)
                logger.info(f"Table {bq_table_name}: {row_count:,} rows")
            else:
                result.note_error(f"Failed to process {table_name}", table=table_name)

        # Process deactivations if requested (separate XLSX download)
        if "deactivations" in tables:
            deact_config = resources.get("deactivations", {})
            if deact_config.get("active", False):
                logger.info("\n" + "=" * 60)
                logger.info("DOWNLOADING DEACTIVATED NPI REPORT")
                logger.info("=" * 60)

                # Find and download deactivations ZIP
                deact_url = client.find_latest_deactivation_file()
                if deact_url:
                    deact_zip_path = os.path.join(temp_dir, "npi_deactivations.zip")
                    if client.download_file(deact_url, deact_zip_path):
                        parquet_path, row_count = process_deactivations_zip(
                            zip_path=deact_zip_path,
                            table_config=deact_config,
                            config=config,
                            execution_id=execution_id,
                            temp_dir=temp_dir,
                            bq_client=bq_client,
                        )

                        if parquet_path:
                            bq_table_name = deact_config.get(
                                "table_name", "npi_deactivations"
                            )
                            # Record under the LOGICAL resource key 'deactivations'.
                            result.record_prewritten(
                                "deactivations", parquet_path, row_count
                            )
                            logger.info(f"Table {bq_table_name}: {row_count:,} rows")
                        else:
                            result.note_error(
                                "Failed to process deactivations ZIP",
                                table="deactivations",
                            )

                        # Cleanup deactivations ZIP
                        try:
                            os.remove(deact_zip_path)
                        except Exception:
                            pass
                    else:
                        result.note_error(
                            f"Failed to download deactivations from {deact_url}",
                            table="deactivations",
                        )
                else:
                    result.note_error(
                        "Could not find deactivated NPI report URL",
                        table="deactivations",
                    )

        # Cleanup: remove main ZIP file if it was downloaded
        if zip_path and os.path.exists(zip_path):
            try:
                os.remove(zip_path)
                logger.info("Cleaned up main ZIP file")
            except Exception as e:
                logger.warning(f"Could not remove ZIP file: {e}")

        logger.info("\n" + "=" * 80)
        logger.info(f"NPI EXTRACTION COMPLETE")
        logger.info(f"Tables extracted: {result.successful_tables}")
        logger.info(f"Total rows: {result.total_rows:,}")
        logger.info(f"Parquet files: {list(result.table_files.keys())}")
        logger.info("=" * 80)

        # success stays True even if individual tables failed (note_error keeps
        # failed_tables at 0) -- matches the prior behavior, which only set
        # success=False on an outer exception.
        return result.finalize(
            success=(result.failed_tables == 0),
            stats={
                "tables_extracted": result.successful_tables,
                "total_rows": result.total_rows,
            },
        )

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback

        logger.error(traceback.format_exc())

        result.note_error(str(e))
        return result.finalize(
            success=False,
            stats={
                "tables_extracted": result.successful_tables,
                "total_rows": result.total_rows,
            },
        )


if __name__ == "__main__":
    """
    Test mode: Run extraction with sample data

    Usage:
        python -m plugins.npi_registry_extractor
    """
    import yaml

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Load configuration
    config_path = "configs/npi_registry.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Run in test mode
    results = run_pipeline(
        config=config,
        sites=[],
        tables=["npi_main"],
        group=None,
        refresh_mode="full",
        lookback_days=None,
        start_date=None,
        end_date=None,
        test_mode=True,
        batch_size=10000,
        max_retries=3,
        execution_id=f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    )

    print(f"\nResults: {results}")
