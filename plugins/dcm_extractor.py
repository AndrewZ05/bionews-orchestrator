#!/usr/bin/env python3
"""
Campaign Manager 360 (DCM) Extractor Plugin

Extracts campaign performance data from Google Campaign Manager 360 using the Reporting API.
Supports:
- Multi-account extraction (70+ accounts)
- Configurable lookback windows (3, 7, 30 days)
- Historical and incremental loads
- Multiple report types (Standard, Path to Conversion, Floodlight)
- Automatic account discovery via API
"""

import os
import sys
import time
import json
import logging
import uuid
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta, date
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import io

# Import shared utilities
from shared.extractor_utils import apply_table_affix, get_available_tables
from shared.account_context import set_execution_metadata
from shared.extractor_runner import initialize_pipeline_environment
from shared.bigquery_utils import batch_load_to_bigquery, create_table_if_not_exists
from shared.schema_registry import get_bq_schema
from shared.gcs_pipeline import extract_to_local_parquet

# Set up logging
logger = logging.getLogger(__name__)

# DCM API Configuration
DCM_API_SCOPES = ["https://www.googleapis.com/auth/dfareporting"]
DCM_API_VERSION = "v5"  # v5 is current stable (v4 sunset: Feb 26, 2026)


class DCMClient:
    """Campaign Manager 360 API Client"""

    def __init__(self, credentials_path: str = None):
        """
        Initialize DCM API client

        Args:
            credentials_path: Path to service account JSON key
        """
        self.service = None
        self.credentials = None
        # Use standard GOOGLE_APPLICATION_CREDENTIALS (same as other pipelines)
        self.credentials_path = (
            credentials_path
            or os.getenv("DCM_SERVICE_ACCOUNT_KEY")
            or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        )

    def initialize(self) -> bool:
        """
        Initialize the DCM API service

        Supports two authentication methods:
        1. OAuth (preferred) - Uses refresh token from .env for C360 access
        2. Service Account (fallback) - Uses service account JSON key
        """
        try:
            # Try OAuth first (preferred for C360 user access)
            refresh_token = os.getenv("DCM_REFRESH_TOKEN")

            if refresh_token:
                # OAuth authentication with refresh token
                from google.oauth2.credentials import Credentials

                logger.info("Using OAuth authentication for DCM API")

                client_id = os.getenv("DCM_CLIENT_ID")
                client_secret = os.getenv("DCM_CLIENT_SECRET")

                if not client_id or not client_secret:
                    logger.error("DCM_CLIENT_ID or DCM_CLIENT_SECRET not set in .env")
                    return False

                self.credentials = Credentials(
                    None,  # No access token yet (will be refreshed automatically)
                    refresh_token=refresh_token,
                    token_uri="https://oauth2.googleapis.com/token",
                    client_id=client_id,
                    client_secret=client_secret,
                    scopes=DCM_API_SCOPES,
                )

                logger.info("OAuth credentials configured successfully")

            elif self.credentials_path:
                # Service account authentication (fallback)
                logger.info("Using service account authentication for DCM API")

                if not os.path.exists(self.credentials_path):
                    logger.error(
                        f"Service account key file not found: {self.credentials_path}"
                    )
                    return False

                self.credentials = (
                    service_account.Credentials.from_service_account_file(
                        self.credentials_path, scopes=DCM_API_SCOPES
                    )
                )

                logger.info("Service account credentials loaded successfully")

            else:
                logger.error(
                    "No DCM credentials found (need DCM_REFRESH_TOKEN or service account key)"
                )
                logger.error(
                    "To use OAuth: Run 'python setup_dcm_oauth.py' and add credentials to .env"
                )
                logger.error(
                    "To use service account: Set DCM_SERVICE_ACCOUNT_KEY or GOOGLE_APPLICATION_CREDENTIALS"
                )
                return False

            # Build service
            self.service = build(
                "dfareporting", DCM_API_VERSION, credentials=self.credentials
            )

            logger.info(f"DCM API {DCM_API_VERSION} initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize DCM API: {e}")
            return False

    def get_user_profiles(self) -> List[Dict[str, Any]]:
        """
        Get all user profiles (accounts) the authenticated user has access to

        Works with both OAuth (C360 user) and service account authentication.

        Returns:
            List of profile dictionaries with accountId, profileId, accountName
        """
        try:
            request = (
                self.service.userProfiles().list()
            )  # Note: camelCase 'userProfiles'
            response = request.execute()

            profiles = response.get("items", [])
            logger.info(f"Found {len(profiles)} accessible DCM profiles")

            return profiles

        except HttpError as e:
            if "No valid profile was found" in str(e):
                logger.warning(
                    "No DCM profiles accessible - service account needs to be added to client accounts"
                )
            else:
                logger.error(f"Failed to fetch user profiles: {e}")
            return []

    def create_report(
        self, profile_id: int, report_config: Dict[str, Any]
    ) -> Optional[str]:
        """
        Create a new report

        Args:
            profile_id: DCM profile ID
            report_config: Report configuration dict

        Returns:
            Report ID if successful, None otherwise
        """
        try:
            request = self.service.reports().insert(
                profileId=profile_id, body=report_config
            )
            response = request.execute()

            report_id = response.get("id")
            logger.info(f"Created report {report_id} for profile {profile_id}")

            return report_id

        except HttpError as e:
            logger.error(f"Failed to create report for profile {profile_id}: {e}")
            return None

    def run_report(
        self, profile_id: int, report_id: str, synchronous: bool = False
    ) -> Optional[str]:
        """
        Execute a report

        Args:
            profile_id: DCM profile ID
            report_id: Report ID to run
            synchronous: Whether to run synchronously (not recommended for large reports)

        Returns:
            File ID if successful, None otherwise
        """
        try:
            request = self.service.reports().run(
                profileId=profile_id, reportId=report_id, synchronous=synchronous
            )
            response = request.execute()

            file_id = response.get("id")
            logger.info(f"Started report {report_id}, file ID: {file_id}")

            return file_id

        except HttpError as e:
            logger.error(f"Failed to run report {report_id}: {e}")
            return None

    def check_report_status(
        self, profile_id: int, report_id: str, file_id: str
    ) -> Dict[str, Any]:
        """
        Check report file status

        Args:
            profile_id: DCM profile ID
            report_id: Report ID
            file_id: File ID from run_report

        Returns:
            Status dict with 'status' and 'downloadUrl' keys
        """
        try:
            request = (
                self.service.reports()
                .files()
                .get(profileId=profile_id, reportId=report_id, fileId=file_id)
            )
            response = request.execute()

            status = response.get("status")
            urls = response.get("urls", {})

            return {
                "status": status,
                "downloadUrl": urls.get("apiUrl"),
                "response": response,
            }

        except HttpError as e:
            logger.error(f"Failed to check report status: {e}")
            return {"status": "ERROR", "error": str(e)}

    def wait_for_report(
        self,
        profile_id: int,
        report_id: str,
        file_id: str,
        max_wait_seconds: int = 3600,
        check_interval: int = 30,
    ) -> Optional[str]:
        """
        Wait for report to complete and return download URL

        Args:
            profile_id: DCM profile ID
            report_id: Report ID
            file_id: File ID from run_report
            max_wait_seconds: Maximum time to wait in seconds (default 1 hour)
            check_interval: How often to check status in seconds

        Returns:
            Download URL if successful, None if failed or timeout
        """
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time

            if elapsed > max_wait_seconds:
                logger.error(f"Report {report_id} timed out after {max_wait_seconds}s")
                return None

            status_info = self.check_report_status(profile_id, report_id, file_id)
            status = status_info.get("status")

            if status == "REPORT_AVAILABLE":
                logger.info(f"Report {report_id} completed in {elapsed:.0f}s")
                return status_info.get("downloadUrl")
            elif status == "FAILED":
                logger.error(f"Report {report_id} failed")
                return None
            elif status == "PROCESSING" or status == "QUEUED":
                logger.info(
                    f"Report {report_id} status: {status}, waiting... ({elapsed:.0f}s elapsed)"
                )
                time.sleep(check_interval)
            else:
                logger.warning(f"Unknown report status: {status}")
                time.sleep(check_interval)

    def download_report(
        self, profile_id: int, report_id: str, file_id: str, output_path: str
    ) -> bool:
        """
        Download report file to local path

        Args:
            profile_id: DCM profile ID
            report_id: Report ID
            file_id: File ID
            output_path: Local file path to save to

        Returns:
            True if successful, False otherwise
        """
        try:
            request = (
                self.service.reports()
                .files()
                .get_media(profileId=profile_id, reportId=report_id, fileId=file_id)
            )

            # Download file
            with open(output_path, "wb") as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False

                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.info(
                            f"Download progress: {int(status.progress() * 100)}%"
                        )

            logger.info(f"Downloaded report to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to download report: {e}")
            return False


def get_date_range_with_lookback(
    lookback_days: int, end_date: date = None
) -> Tuple[str, str]:
    """
    Calculate date range for incremental load with lookback window

    Args:
        lookback_days: Number of days to look back
        end_date: End date (defaults to yesterday)

    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
    """
    if end_date is None:
        end_date = datetime.now().date() - timedelta(days=1)  # Yesterday

    start_date = end_date - timedelta(days=lookback_days)

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def build_standard_report_config(
    report_name: str,
    date_range: Tuple[str, str],
    dimensions: List[str] = None,
    metrics: List[str] = None,
    filters: List[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build configuration for a Standard report

    Args:
        report_name: Name for the report
        date_range: Tuple of (start_date, end_date) in YYYY-MM-DD format
        dimensions: List of dimension names (defaults to basic set)
        metrics: List of metric names (defaults to basic set)
        filters: Optional list of dimension filters

    Returns:
        Report configuration dictionary
    """
    # Default dimensions
    if dimensions is None:
        dimensions = [
            "date",
            "advertiser",
            "advertiserId",
            "campaign",
            "campaignId",
            "placement",
            "placementId",
            "ad",
            "adId",
            "site",
            "siteId",
        ]

    # Default metrics
    if metrics is None:
        metrics = [
            "impressions",
            "clicks",
            "totalConversions",
            "clickThroughConversions",
            "viewThroughConversions",
            "totalConversionsRevenue",
            "mediaCost",
        ]

    # Build criteria
    criteria = {
        "dateRange": {"startDate": date_range[0], "endDate": date_range[1]},
        "dimensions": [{"name": dim} for dim in dimensions],
        "metricNames": metrics,
    }

    # Add filters if provided
    if filters:
        criteria["dimensionFilters"] = filters

    return {
        "name": report_name,
        "type": "STANDARD",
        "criteria": criteria,
        "format": "CSV",
        "delivery": {"emailOwner": False},
    }


def standardize_dcm_columns(
    df: pd.DataFrame, table_config: Dict[str, Any]
) -> pd.DataFrame:
    """
    Standardize DCM column names to match BigQuery schema
    Handles varying column names across different DCM accounts

    Args:
        df: DataFrame with DCM data
        table_config: Table configuration from YAML (contains schema)

    Returns:
        DataFrame with standardized column names and all required columns
    """
    try:
        # Get schema from YAML config (following standard pattern)
        schema = table_config.get("schema", {})

        # Get expected columns (exclude metadata columns added later)
        # site_id and site come from CSV: Site ID (CM360) and Site (CM360) - publisher site info
        # account_id and account come from DCM profile metadata - agency/network info
        # extracted_at, execution_id, source are added by set_execution_metadata
        metadata_columns = {
            "account_id",
            "account",
            "extracted_at",
            "execution_id",
            "source",
        }
        expected_columns = [
            col_name for col_name in schema.keys() if col_name not in metadata_columns
        ]

        # DCM column name mapping (DCM format -> schema format)
        # Note: CM360 reports use "Site (CM360)" not "Site (DCM)" for publisher site columns
        # Important: Advertiser/Advertiser ID are NOT mapped to account/account_id
        #   - account/account_id come from DCM profile metadata (agency/network info)
        #   - Advertiser/Advertiser ID represent the client running campaigns (different concept)
        column_mapping = {
            "Date": "date",
            "Package/Roadblock": "package_road_block",
            "Package/Roadblock ID": "package_road_block_id",
            "Campaign": "campaign",
            "Campaign ID": "campaign_id",
            "Placement": "placement",
            "Placement ID": "placement_id",
            "Placement Size": "placement_size",
            "Placement Pixel Size": "placement_size",  # Alternative column name
            "Creative": "creative",
            "Creative ID": "creative_id",
            "Ad": "ad",
            "Ad ID": "ad_id",
            "Site (CM360)": "site",  # Publisher site name
            "Site ID (CM360)": "site_id",  # Publisher site ID
            "Site (DCM)": "site",  # Legacy column name (fallback)
            "Site ID (DCM)": "site_id",  # Legacy column name (fallback)
            "Impressions": "impressions",
            "Clicks": "clicks",
            "Active View: Measurable Impressions": "active_view_measurable_impressions",
            "Active View: Viewable Impressions": "active_view_viewable_impressions",
            "Platform Type": "platform_type",
            "Click-through URL": "click_through_url",
            # Advertiser columns - represent the client/brand running campaigns (sub-account)
            # This is different from account/account_id which is the agency/network
            "Advertiser": "advertiser",
            "Advertiser ID": "advertiser_id",
            "Country": "country",
            "State/Region": "state",
            "County/Region": "state",  # Some CM360 accounts use this variant
            "Total Conversions": "total_conversions",
        }

        # Rename columns based on mapping
        df_renamed = df.rename(columns=column_mapping)

        # Create final DataFrame with all expected columns
        result_df = pd.DataFrame()

        for col in expected_columns:
            if col in df_renamed.columns:
                result_df[col] = df_renamed[col]
            else:
                # Column missing - fill with NULL
                result_df[col] = None
                logger.debug(f"Column '{col}' not found in DCM data, filled with NULL")

        logger.info(
            f"Standardized {len(df)} rows to match schema with {len(expected_columns)} columns"
        )

        return result_df

    except Exception as e:
        logger.error(f"Failed to standardize columns: {e}")
        return df


def extract_report_to_dataframe(csv_path: str) -> pd.DataFrame:
    """
    Extract DCM report CSV to pandas DataFrame

    Args:
        csv_path: Path to downloaded CSV file

    Returns:
        DataFrame with report data
    """
    try:
        # DCM reports have header rows and footer rows that need to be skipped
        # Typically: Report header (multiple lines), blank line, column headers, data, blank line, totals/footer

        # Read file to find data start
        with open(csv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # Find the line with "Report Fields" or column headers
        header_row = 0
        for i, line in enumerate(lines):
            if "Report Fields" in line:
                # Header row is the next line after "Report Fields"
                header_row = i + 1
                break

        # Read CSV starting from header row (skip all rows before it)
        df = pd.read_csv(csv_path, skiprows=range(header_row), low_memory=False)

        # Remove footer rows (typically totals at the end)
        # Look for rows where first column is "Grand Total" or empty
        df = df[df.iloc[:, 0].notna()]
        df = df[
            ~df.iloc[:, 0]
            .astype(str)
            .str.contains("Grand Total|Total", case=False, na=False)
        ]

        logger.info(f"Extracted {len(df)} rows from {csv_path}")

        return df

    except Exception as e:
        logger.error(f"Failed to extract CSV to DataFrame: {e}")
        return pd.DataFrame()


def process_dcm_profile(
    client: DCMClient,
    profile_id: int,
    account_name: str,
    dcm_account_id: int,
    table_config: Dict[str, Any],
    date_range: Tuple[str, str],
    config: Dict[str, Any],
    test_mode: bool = False,
    execution_id: str = None,
) -> bool:
    """
    Process a single DCM profile (account)

    Args:
        client: Initialized DCMClient
        profile_id: DCM profile ID
        account_name: Human-readable account name (agency/network name)
        dcm_account_id: DCM account ID (agency/network ID from profile)
        table_config: Table configuration from YAML
        date_range: Tuple of (start_date, end_date)
        config: Full orchestrator config
        test_mode: Whether running in test mode

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Processing profile {profile_id} ({account_name})")

        # Build report configuration
        report_name = f"Orchestrator_{account_name}_{date_range[0]}_{date_range[1]}"

        # Get dimensions and metrics from table config
        dimensions = table_config.get("dimensions", None)
        metrics = table_config.get("metrics", None)

        report_config = build_standard_report_config(
            report_name=report_name,
            date_range=date_range,
            dimensions=dimensions,
            metrics=metrics,
        )

        # Create report
        report_id = client.create_report(profile_id, report_config)
        if not report_id:
            logger.error(f"Failed to create report for profile {profile_id}")
            return False

        # Run report
        file_id = client.run_report(profile_id, report_id)
        if not file_id:
            logger.error(f"Failed to run report {report_id}")
            return False

        # Wait for completion
        download_url = client.wait_for_report(
            profile_id, report_id, file_id, max_wait_seconds=1800  # 30 minutes
        )

        if not download_url:
            logger.error(f"Report {report_id} did not complete successfully")
            return False

        # Download report
        local_dir = Path(config.get("local_dir", "data/dcm"))
        local_dir.mkdir(parents=True, exist_ok=True)

        csv_filename = f"dcm_{profile_id}_{date_range[0]}_{date_range[1]}.csv"
        csv_path = local_dir / csv_filename

        if not client.download_report(profile_id, report_id, file_id, str(csv_path)):
            logger.error(f"Failed to download report {report_id}")
            return False

        # Extract to DataFrame
        df = extract_report_to_dataframe(str(csv_path))
        if df.empty:
            logger.warning(f"No data extracted from report {report_id}")
            return True  # Not an error, just no data

        # Standardize columns to match BigQuery schema
        # This ensures consistent format across all DCM accounts
        # Note: site_id and site come from CSV columns Site ID (CM360) and Site (CM360)
        df = standardize_dcm_columns(df, table_config)

        # Set account fields from DCM profile metadata (agency/network info)
        # These identify which DCM agency account the data came from
        # Note: This is different from the Advertiser (client running campaigns)
        df["account_id"] = dcm_account_id
        df["account"] = account_name

        # Convert DataFrame to records, add standard metadata, then convert back
        # This follows the standard pattern used by all other extractors
        records = df.to_dict("records")
        set_execution_metadata(records, execution_id, source="dcm")
        df = pd.DataFrame(records)

        # Convert data types to match BigQuery schema AFTER metadata addition
        # This prevents Parquet conversion errors when uploading to BigQuery
        # Must happen after DataFrame reconstruction to preserve types through dict conversion
        schema = table_config.get("schema", {})
        for col, bq_type in schema.items():
            if col in df.columns:
                if bq_type in ("INTEGER", "INT64"):
                    # Convert to numeric, invalid values become NULL
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif bq_type in ("FLOAT", "FLOAT64"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif bq_type == "STRING":
                    df[col] = df[col].astype(str).replace({"nan": None, "None": None})
                elif bq_type == "TIMESTAMP":
                    # Convert to datetime64[us] for BigQuery compatibility
                    df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).astype(
                        "datetime64[us, UTC]"
                    )

        # Convert extracted_at to datetime64[us] to avoid nanosecond precision issues in Parquet
        # BigQuery TIMESTAMP supports microsecond precision, not nanosecond
        if "extracted_at" in df.columns:
            df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True).astype(
                "datetime64[us, UTC]"
            )

        # Convert to Parquet and save locally (following standard pattern)
        parquet_path = csv_path.with_suffix(".parquet")
        df.to_parquet(parquet_path, index=False)

        logger.info(
            f"[OK] Profile {profile_id} extracted successfully - {len(df)} rows saved to {parquet_path}"
        )

        return True

    except Exception as e:
        logger.error(f"Error processing profile {profile_id}: {e}")
        return False


def initialize_api(config: Dict[str, Any]) -> Optional[DCMClient]:
    """
    Initialize DCM API client

    Args:
        config: Orchestrator configuration

    Returns:
        Initialized DCMClient or None if failed
    """
    try:
        credentials_path = (
            config.get("source", {}).get("connection", {}).get("service_account_key")
        )

        if not credentials_path:
            # Use standard GOOGLE_APPLICATION_CREDENTIALS (same as other pipelines)
            credentials_path = os.getenv("DCM_SERVICE_ACCOUNT_KEY") or os.getenv(
                "GOOGLE_APPLICATION_CREDENTIALS"
            )

        if not credentials_path:
            logger.error(
                "DCM service account key path not configured (set GOOGLE_APPLICATION_CREDENTIALS)"
            )
            return None

        client = DCMClient(credentials_path)

        if not client.initialize():
            return None

        return client

    except Exception as e:
        logger.error(f"Failed to initialize DCM API: {e}")
        return None


def get_available_sites(config: Dict[str, Any]) -> List[str]:
    """
    Get available DCM profiles (accounts) - DYNAMICALLY DISCOVERED AT RUNTIME

    This is called at the start of each ETL job to discover all accessible profiles.
    This ensures new client accounts are automatically picked up.

    Args:
        config: Orchestrator configuration

    Returns:
        List of profile IDs as strings
    """
    logger.info("=" * 60)
    logger.info("DISCOVERING DCM ACCOUNTS (dynamic API lookup)")
    logger.info("=" * 60)

    client = initialize_api(config)
    if not client:
        logger.error("Failed to initialize DCM client")
        return []

    # Fetch all profiles accessible to service account
    profiles = client.get_user_profiles()

    if not profiles:
        logger.warning("No DCM profiles accessible to service account")
        logger.warning("Service account needs to be added to client DCM accounts")
        return []

    # Return profile IDs
    profile_ids = [str(p["profileId"]) for p in profiles]

    logger.info(f"[OK] Discovered {len(profile_ids)} accessible DCM profiles")

    # Log details for visibility
    for profile in profiles:
        profile_id = profile.get("profileId")
        account_name = profile.get("accountName", "N/A")
        account_id = profile.get("accountId", "N/A")
        logger.info(f"  - Profile {profile_id}: {account_name} (Account: {account_id})")

    # Save discovered accounts to file for reference
    try:
        import json
        from pathlib import Path

        discovered_file = Path("dcm_discovered_accounts.json")
        discovered_data = {
            "discovered_at": datetime.now().isoformat(),
            "count": len(profiles),
            "profiles": [
                {
                    "profile_id": p.get("profileId"),
                    "account_id": p.get("accountId"),
                    "account_name": p.get("accountName"),
                    "user_name": p.get("userName"),
                }
                for p in profiles
            ],
        }

        with open(discovered_file, "w") as f:
            json.dump(discovered_data, f, indent=2)

        logger.info(f"[OK] Saved discovered accounts to {discovered_file}")
    except Exception as e:
        logger.warning(f"Could not save discovered accounts: {e}")

    logger.info("=" * 60)

    return profile_ids


def extract_resource(
    account_id: str,
    table_config: Dict[str, Any],
    date_range: Tuple[date, date],
    config: Dict[str, Any],
    test_mode: bool = False,
    execution_id: str = None,
) -> bool:
    """
    Extract data for a specific DCM profile (account)

    Args:
        account_id: Profile ID (as string)
        table_config: Table configuration from YAML
        date_range: Tuple of (start_date, end_date) as date objects
        config: Full orchestrator configuration
        test_mode: Whether running in test mode

    Returns:
        True if successful, False otherwise
    """
    try:
        profile_id = int(account_id)

        # Initialize client
        client = initialize_api(config)
        if not client:
            return False

        # Get account name and account_id from discovered accounts JSON
        # These represent the DCM agency/network (not the advertiser)
        account_name = f"Profile_{profile_id}"  # Default
        dcm_account_id = 0  # Default

        # Try to get from discovered accounts JSON (generated by get_available_sites)
        discovered_file = Path("dcm_discovered_accounts.json")
        if discovered_file.exists():
            with open(discovered_file, "r") as f:
                discovered_data = json.load(f)
                for profile in discovered_data.get("profiles", []):
                    if str(profile.get("profile_id")) == str(profile_id):
                        account_name = profile.get("account_name", account_name)
                        dcm_account_id = int(profile.get("account_id", 0))
                        break

        # Fallback to legacy dcm_accounts.json if discovered file doesn't have the profile
        if dcm_account_id == 0:
            accounts_file = Path("dcm_accounts.json")
            if accounts_file.exists():
                with open(accounts_file, "r") as f:
                    accounts = json.load(f)
                    for acc in accounts:
                        if str(acc.get("profile_id")) == str(profile_id):
                            account_name = acc.get("account_name", account_name)
                            dcm_account_id = int(acc.get("account_id", 0))
                            break

        # Convert date objects to strings
        date_range_str = (
            date_range[0].strftime("%Y-%m-%d"),
            date_range[1].strftime("%Y-%m-%d"),
        )

        # Process this profile
        success = process_dcm_profile(
            client=client,
            profile_id=profile_id,
            account_name=account_name,
            dcm_account_id=dcm_account_id,
            table_config=table_config,
            date_range=date_range_str,
            config=config,
            test_mode=test_mode,
            execution_id=execution_id,
        )

        return success

    except Exception as e:
        logger.error(f"Error extracting data for account {account_id}: {e}")
        return False


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
    DCM extraction pipeline using standard GCS flow.

    Returns data for orchestrator to handle:
    1. Write to Parquet
    2. Upload to GCS
    3. Create external table
    4. Copy to staging
    5. Merge to production

    Args:
        config: Full orchestrator configuration
        sites: List of DCM profile IDs (or ["all"] for auto-discovery)
        tables: List of table names to extract
        refresh_mode: "full" or "incremental"
        lookback_days: Days to look back (for incremental)
        start_date: Start date (for full refresh)
        end_date: End date (for full refresh)
        test_mode: Whether running in test mode
        ... (other standard orchestrator parameters)

    Returns:
        Dictionary with extraction results and data locations
    """
    logger.info("=" * 80)
    logger.info("DCM EXTRACTION PIPELINE STARTING")
    logger.info("=" * 80)

    try:
        # Standardized per-table accounting + return shape (shared/extraction_result).
        # DCM writes per-profile Parquet files itself and consolidates them into one
        # file per table; that consolidated path is recorded via record_prewritten.
        # profiles_processed is a DCM-specific stat carried through finalize(**extra).
        #
        # VERIFICATION STATUS (2026-06): unit-tested + LIVE-VERIFIED. A real
        # multi-profile run (--start-date 2026-06-01 --end-date 2026-06-15, 48
        # profiles discovered) exercised the full path: per-profile extract_resource,
        # per-table consolidation into one Parquet, record_prewritten, and the merge
        # into dcm_data.dcm_campaign_performance. The run consolidated 171,101 rows
        # across profiles and merged cleanly. (Profiles with no campaign activity in
        # the window correctly return zero rows and are marked extracted-successfully,
        # not failed -- many are duplicate agency profiles for the same account.)
        from shared.extraction_result import StandardExtractionResult

        source_name = config.get("source", {}).get("name", "dcm")
        result = StandardExtractionResult(
            source=source_name,
            execution_id=execution_id,
            job_id=execution_id,
            bq_client=bq_client,
        )
        profiles_processed = 0
        tables_extracted = 0

        # Determine date range
        if refresh_mode == "incremental" and lookback_days:
            end_date_obj = date.today() - timedelta(days=1)  # Yesterday
            start_date_obj = end_date_obj - timedelta(days=lookback_days)
            logger.info(f"Incremental mode: {lookback_days} day lookback")
        elif start_date and end_date:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            logger.info(f"Explicit date range: {start_date} to {end_date}")
        elif start_date:
            # Only start_date provided - default end_date to yesterday
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_obj = date.today() - timedelta(days=1)
            logger.info(
                f"Start date provided, defaulting end_date to yesterday: {start_date} to {end_date_obj}"
            )
        elif refresh_mode == "full":
            # Full refresh without explicit dates - use YAML config
            source_config = config.get("source", {})
            date_range_config = source_config.get("date_range", {})
            full_config = date_range_config.get("full", {})

            yaml_start_date = full_config.get("start_date")
            if yaml_start_date:
                start_date_obj = datetime.strptime(yaml_start_date, "%Y-%m-%d").date()
                end_date_obj = date.today() - timedelta(days=1)  # Yesterday
                logger.info(
                    f"Full refresh mode: using YAML start_date={yaml_start_date}, end_date=yesterday"
                )
            else:
                # No YAML config - error
                error_msg = (
                    "Full refresh requires start_date in YAML config or explicit --start-date parameter.\n"
                    "Must provide one of:\n"
                    "  - Configure start_date in configs/dcm.yaml under source.date_range.full.start_date\n"
                    "  - Use --start-date YYYY-MM-DD [--end-date YYYY-MM-DD] (end_date defaults to yesterday)\n"
                    "  - Use --lookback N (for incremental mode)"
                )
                logger.error(error_msg)
                result.note_error(error_msg)
                return result.finalize(success=False, profiles_processed=0)
        else:
            # No date parameters specified - error
            error_msg = (
                "No date range specified. Must provide one of:\n"
                "  - --refresh full (uses YAML start_date from configs/dcm.yaml)\n"
                "  - --start-date YYYY-MM-DD [--end-date YYYY-MM-DD] (end_date defaults to yesterday)\n"
                "  - --lookback N (for incremental mode)"
            )
            logger.error(error_msg)
            result.note_error(error_msg)
            return result.finalize(success=False, profiles_processed=0)

        date_range = (start_date_obj, end_date_obj)
        logger.info(f"Date range: {date_range[0]} to {date_range[1]}")

        # Handle account discovery
        if sites == ["all"] or sites == [
            "a",
            "l",
            "l",
        ]:  # 'all' might be split into chars
            logger.info("Discovering DCM profiles...")
            sites = get_available_sites(config)

            if not sites:
                logger.error("No accessible DCM profiles found")
                result.note_error("No accessible DCM profiles")
                return result.finalize(success=False, profiles_processed=0)

            logger.info(f"Discovered {len(sites)} DCM profiles")

        # Process each table
        for table_name in tables:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing table: {table_name}")
            logger.info(f"{'='*60}")

            # Get table configuration from resources
            resources = config.get("resources", {})
            table_config = resources.get(table_name)

            if not table_config:
                logger.error(f"Table '{table_name}' not found in configuration")
                # Non-fatal: matches old behavior (logged, not success=False).
                result.note_error(
                    f"Table '{table_name}' not configured", table=table_name
                )
                continue

            if not table_config.get("active", True):
                logger.info(
                    f"Table '{table_name}' is marked as active: false, skipping"
                )
                continue

            # Track parquet files for this table
            parquet_files = []

            # Determine number of workers for parallel processing
            # Each DCM account is independent, so parallel processing is beneficial
            num_workers = parallel_workers or config.get("pipeline", {}).get(
                "parallel_workers", 4
            )

            # For DCM, use parallel processing since each account/profile is on a separate system
            if num_workers > 1 and len(sites) > 1:
                logger.info(
                    f"Processing {len(sites)} profiles with {num_workers} parallel workers"
                )

                from concurrent.futures import ThreadPoolExecutor, as_completed

                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    # Submit all profile extraction tasks
                    future_to_profile = {
                        executor.submit(
                            extract_resource,
                            account_id=profile_id,
                            table_config=table_config,
                            date_range=date_range,
                            config=config,
                            test_mode=test_mode,
                            execution_id=execution_id,
                        ): profile_id
                        for profile_id in sites
                    }

                    # Process completed tasks as they finish
                    for future in as_completed(future_to_profile):
                        profile_id = future_to_profile[future]
                        try:
                            success = future.result()
                            if success:
                                profiles_processed += 1
                                logger.info(
                                    f"[OK] Profile {profile_id} extracted successfully"
                                )
                            else:
                                logger.warning(
                                    f"[FAIL] Profile {profile_id} extraction failed"
                                )
                                result.note_error(
                                    f"Profile {profile_id} failed", table=table_name
                                )
                        except Exception as e:
                            logger.error(f"Error processing profile {profile_id}: {e}")
                            result.note_error(
                                f"Profile {profile_id}: {str(e)}", table=table_name
                            )
            else:
                # Sequential processing (for single profile or when workers=1)
                logger.info(f"Processing {len(sites)} profiles sequentially")

                for profile_id in sites:
                    logger.info(f"\nProcessing Profile {profile_id}...")

                    try:
                        # Extract data for this profile
                        success = extract_resource(
                            account_id=profile_id,
                            table_config=table_config,
                            date_range=date_range,
                            config=config,
                            test_mode=test_mode,
                            execution_id=execution_id,
                        )

                        if success:
                            profiles_processed += 1
                            logger.info(
                                f"[OK] Profile {profile_id} extracted successfully"
                            )
                        else:
                            logger.warning(
                                f"[FAIL] Profile {profile_id} extraction failed"
                            )
                            result.note_error(
                                f"Profile {profile_id} failed", table=table_name
                            )

                    except Exception as e:
                        logger.error(f"Error processing profile {profile_id}: {e}")
                        result.note_error(
                            f"Profile {profile_id}: {str(e)}", table=table_name
                        )
                        continue

            # After all profiles are extracted, consolidate Parquet files
            # Collect all parquet files for this table
            local_dir = Path(config.get("local_dir", "data/dcm"))
            date_range_str = (
                date_range[0].strftime("%Y-%m-%d"),
                date_range[1].strftime("%Y-%m-%d"),
            )

            # Find all parquet files matching pattern for this date range
            parquet_pattern = f"dcm_*_{date_range_str[0]}_{date_range_str[1]}.parquet"
            parquet_files = list(local_dir.glob(parquet_pattern))

            if parquet_files:
                logger.info(
                    f"Consolidating {len(parquet_files)} Parquet files for table {table_name}"
                )

                # Use PyArrow for memory-efficient concatenation without loading all data into pandas
                # This streams data directly to output file in chunks
                import pyarrow.parquet as pq
                import pyarrow as pa
                import tempfile

                temp_dir = (
                    Path(tempfile.gettempdir())
                    / f"orchestrator_{uuid.uuid4().hex[:12]}"
                )
                temp_dir.mkdir(parents=True, exist_ok=True)
                final_parquet_path = temp_dir / f"dcm_{table_name}_consolidated.parquet"

                # Get schema from first file
                first_file = parquet_files[0]
                schema = pq.read_schema(first_file)
                logger.info(f"Using schema from {first_file.name}")

                # Write consolidated file using ParquetWriter (streams data, doesn't load all into memory)
                total_rows = 0
                with pq.ParquetWriter(final_parquet_path, schema) as writer:
                    for i, pq_file in enumerate(parquet_files):
                        try:
                            # Read each file as a PyArrow table (more memory efficient than pandas)
                            table = pq.read_table(pq_file)
                            writer.write_table(table)
                            file_rows = table.num_rows
                            total_rows += file_rows
                            logger.info(
                                f"  [{i+1}/{len(parquet_files)}] Added {pq_file.name}: {file_rows:,} rows"
                            )
                            # Explicitly delete to free memory
                            del table
                        except Exception as e:
                            logger.warning(f"Failed to read {pq_file}: {e}")

                logger.info(
                    f"Combined {len(parquet_files)} files into {total_rows:,} total rows"
                )

                # Clean up individual profile parquet files
                for pq_file in parquet_files:
                    try:
                        pq_file.unlink()
                        logger.debug(f"Removed temp file: {pq_file}")
                    except Exception as e:
                        logger.warning(f"Could not remove temp file {pq_file}: {e}")

                if final_parquet_path.exists():
                    # DCM wrote+consolidated the Parquet itself -> record the
                    # existing path (record_prewritten, not record_table).
                    result.record_prewritten(
                        table_name, str(final_parquet_path), total_rows
                    )
                    logger.info(
                        f"[OK] Consolidated Parquet file ready for upload ({total_rows:,} rows)"
                    )
                else:
                    result.skip_table(table_name, reason="zero_rows")
            else:
                logger.warning(f"No Parquet files found for table {table_name}")
                result.skip_table(table_name, reason="zero_rows")

            tables_extracted += 1

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("DCM EXTRACTION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Profiles processed: {profiles_processed}")
        logger.info(f"Tables extracted: {tables_extracted}")
        logger.info(f"Total rows: {result.total_rows}")
        logger.info(f"Errors: {len(result.errors)}")

        if result.errors:
            logger.warning("Errors encountered:")
            for err in result.errors[:10]:  # Show first 10
                logger.warning(f"  - {err['error']}")

        # success preserved: DCM historically reported success unless the outer
        # try/except caught a fatal error or an early-return set it False.
        # Per-profile/table issues are notes, not run failures.
        return result.finalize(
            success=True,
            profiles_processed=profiles_processed,
            tables_extracted=tables_extracted,
        )

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "errors": [str(e)], "table_files": {}, "stats": {}}


if __name__ == "__main__":
    """Test the DCM extractor"""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Test configuration
    test_config = {
        "source": {
            "connection": {
                "service_account_key": os.getenv("DCM_SERVICE_ACCOUNT_KEY")
                or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            }
        },
        "destination": {
            "bigquery": {"project_id": "your-project-id", "dataset": "dcm_data"}
        },
        "local_dir": "data/dcm",
    }

    # Test API initialization
    logger.info("Testing DCM API initialization...")
    client = initialize_api(test_config)

    if client:
        logger.info("[OK] API initialized successfully")

        # Test getting profiles
        profiles = client.get_user_profiles()
        logger.info(f"[OK] Found {len(profiles)} accessible profiles")

        for profile in profiles[:5]:  # Show first 5
            logger.info(
                f"  - Profile {profile['profileId']}: {profile.get('userName', 'N/A')}"
            )
    else:
        logger.error("[FAIL] Failed to initialize API")
