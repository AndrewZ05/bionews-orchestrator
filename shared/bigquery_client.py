#!/usr/bin/env python3
# BigQuery Client Singleton
# Provides centralized BigQuery client management with proper resource cleanup

import os
import logging
import threading
from typing import Optional
from google.cloud import bigquery
from google.cloud import storage as gcs
from google.oauth2 import service_account

# Configure logging
logger = logging.getLogger(__name__)

# Thread-safe client cache (singleton pattern)
_client_lock = threading.Lock()
_bigquery_client: Optional[bigquery.Client] = None
_gcs_client: Optional[gcs.Client] = None

# OAuth scopes for BigQuery with Google Sheets external table support
# drive.readonly is required for accessing Google Sheets external tables
BIGQUERY_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Cross-platform credential file fallback paths
# Checked in order after GOOGLE_APPLICATION_CREDENTIALS env var
CREDENTIAL_FALLBACK_PATHS = [
    "C:/gcp/service-account-bionews-pipeline.json",  # Windows
    "/home/orchestrator/service-account-bionews-pipeline.json",  # Linux
]


def setup_gcp_credentials() -> Optional[str]:
    """Ensure GOOGLE_APPLICATION_CREDENTIALS points at an existing file.

    Call from scripts BEFORE `from google.cloud import ...`. If the env var is
    unset or points at a missing file (e.g. a stale Windows path on Linux),
    rewrites it to the first existing fallback path. Returns the resolved path
    or None if no credentials were found.
    """
    current = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if current and os.path.exists(current):
        return current
    for candidate in CREDENTIAL_FALLBACK_PATHS:
        if os.path.exists(candidate):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = candidate
            return candidate
    return None


def _get_credentials():
    """
    Get GCP credentials from environment with appropriate scopes.

    Includes drive.readonly scope to enable access to Google Sheets external tables.
    Checks GOOGLE_APPLICATION_CREDENTIALS first, then falls back to known paths.

    Returns:
        Credentials object or None if using application default credentials
    """
    # Build list of paths to try: env var first, then fallbacks
    paths_to_try = []

    gcp_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gcp_path:
        paths_to_try.append(gcp_path)

    paths_to_try.extend(CREDENTIAL_FALLBACK_PATHS)

    # Try each path until one works
    for path in paths_to_try:
        if os.path.exists(path):
            try:
                creds = service_account.Credentials.from_service_account_file(
                    path, scopes=BIGQUERY_SCOPES
                )
                logger.info(f"Loaded credentials from: {path}")
                return creds
            except Exception as e:
                logger.warning(f"Failed to load credentials from {path}: {e}")
                continue

    logger.warning("No valid credential file found, falling back to ADC")
    return None


def get_bigquery_client(project: str = None, credentials=None) -> bigquery.Client:
    """
    Get or create BigQuery client (thread-safe singleton pattern).

    Creates a single shared BigQuery client instance for the entire application.
    Subsequent calls return the cached client, avoiding unnecessary connections.
    Uses double-check locking pattern for thread safety.

    Args:
        project: GCP project ID (defaults to GCP_PROJECT_ID env var)
        credentials: Optional credentials object

    Returns:
        Configured BigQuery client instance

    Example:
        >>> client = get_bigquery_client()
        >>> client.project
        'bi-data-391216'

        >>> # Later in the code...
        >>> same_client = get_bigquery_client()  # Returns cached instance
    """
    global _bigquery_client

    # Double-check locking pattern for thread safety
    if _bigquery_client is not None:
        return _bigquery_client

    with _client_lock:
        # Check again inside lock to prevent race condition
        if _bigquery_client is not None:
            return _bigquery_client

        # Determine project ID
        if project is None:
            project = os.getenv("GCP_PROJECT_ID", "bi-data-391216")

        # Auto-detect credentials if not provided
        if credentials is None:
            credentials = _get_credentials()

        # Create client with or without credentials
        if credentials:
            _bigquery_client = bigquery.Client(credentials=credentials, project=project)
            logger.info(
                f"Initialized BigQuery client with credentials for project: {project}"
            )
        else:
            _bigquery_client = bigquery.Client(project=project)
            logger.info(f"Initialized BigQuery client for project: {project}")

        return _bigquery_client


def set_default_job_labels(labels: dict) -> None:
    """
    Attach labels to the shared client's default_query_job_config so EVERY subsequent
    client.query() carries them automatically -- without each plugin having to set
    labels itself. Used to stamp orchestrator_job_id on all child jobs for cost
    attribution (INFORMATION_SCHEMA rollup). Best-effort; a bad label is skipped.

    BigQuery label rules: key/value must be lowercase letters, digits, '-' or '_',
    <=63 chars; value may be empty. We sanitize to fit.
    """
    if not labels:
        return
    client = get_bigquery_client()
    try:
        from google.cloud import bigquery as _bq

        def _san(s):
            s = str(s).lower()[:63]
            return "".join(ch if (ch.isalnum() or ch in "-_") else "_" for ch in s)

        clean = {_san(k): _san(v) for k, v in labels.items() if k}
        existing = getattr(client, "_default_query_job_config", None)
        cfg = existing or _bq.QueryJobConfig()
        merged = dict(cfg.labels or {})
        merged.update(clean)
        cfg.labels = merged
        client.default_query_job_config = cfg
    except Exception as e:  # noqa: BLE001
        logger.debug("Could not set default job labels: %s", e)


def get_gcs_client(project: str = None, credentials=None) -> gcs.Client:
    """
    Get or create GCS client (thread-safe singleton pattern).

    Creates a single shared GCS client instance for the entire application.
    Subsequent calls return the cached client, avoiding unnecessary connections.
    Uses double-check locking pattern for thread safety.

    Args:
        project: GCP project ID (defaults to GCP_PROJECT_ID env var)
        credentials: Optional credentials object

    Returns:
        Configured GCS client instance

    Example:
        >>> client = get_gcs_client()
        >>> client.project
        'bi-data-391216'
    """
    global _gcs_client

    # Double-check locking pattern for thread safety
    if _gcs_client is not None:
        return _gcs_client

    with _client_lock:
        # Check again inside lock to prevent race condition
        if _gcs_client is not None:
            return _gcs_client

        # Determine project ID
        if project is None:
            project = os.getenv("GCP_PROJECT_ID", "bi-data-391216")

        # Auto-detect credentials if not provided
        if credentials is None:
            credentials = _get_credentials()

        # Create client with or without credentials
        if credentials:
            _gcs_client = gcs.Client(credentials=credentials, project=project)
            logger.info(
                f"Initialized GCS client with credentials for project: {project}"
            )
        else:
            _gcs_client = gcs.Client(project=project)
            logger.info(f"Initialized GCS client for project: {project}")

        return _gcs_client


def close_bigquery_client() -> None:
    """
    Close and clear cached BigQuery client.

    Should be called in finally blocks to ensure proper resource cleanup.
    After calling this, the next call to get_bigquery_client() will create
    a new client instance.

    Example:
        >>> client = get_bigquery_client()
        >>> try:
        ...     # Do work with client
        ...     pass
        ... finally:
        ...     close_bigquery_client()
    """
    global _bigquery_client
    with _client_lock:
        if _bigquery_client:
            _bigquery_client.close()
            _bigquery_client = None
            logger.info("Closed BigQuery client")


def close_gcs_client() -> None:
    """
    Close and clear cached GCS client.

    Should be called in finally blocks to ensure proper resource cleanup.
    """
    global _gcs_client
    with _client_lock:
        if _gcs_client:
            _gcs_client.close()
            _gcs_client = None
            logger.info("Closed GCS client")


def reset_bigquery_client() -> None:
    """
    Reset BigQuery client (alias for close_bigquery_client).

    Useful for testing or when you need to force creation of a new client
    with different credentials.
    """
    close_bigquery_client()


def reset_gcs_client() -> None:
    """
    Reset GCS client (alias for close_gcs_client).

    Useful for testing or when you need to force creation of a new client
    with different credentials.
    """
    close_gcs_client()
