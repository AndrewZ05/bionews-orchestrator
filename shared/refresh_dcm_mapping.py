#!/usr/bin/env python3
"""
Refresh DCM mapping native table from Google Sheet.

This script uses ADC credentials (personal account) to access Google Sheets,
then copies the data to a native BigQuery table that the service account can access.

Called automatically as pre-step in DCM pipeline post-processing.
"""

import os
import sys
import logging

logger = logging.getLogger(__name__)

def main():
    # Force use of ADC by removing service account credential
    # This allows access to Google Sheets via personal Drive permissions
    sa_creds = os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)

    if sa_creds:
        logger.info(f"Temporarily using ADC instead of service account for Google Sheets access")

    try:
        from google.cloud import bigquery

        # Create client without service account - uses ADC
        client = bigquery.Client(project='bi-data-391216')

        logger.info("Refreshing DCM mapping native table from Google Sheet...")
        print("Refreshing DCM mapping native table from Google Sheet...")

        query = """
        CREATE OR REPLACE TABLE `bi-data-391216.google_sheets.pacing_dcm_mapping_native` AS
        SELECT * FROM `bi-data-391216.google_sheets.pacing_dcm_mapping_2026`
        """

        job = client.query(query)
        job.result()

        # Verify
        count_query = "SELECT COUNT(*) as cnt FROM `bi-data-391216.google_sheets.pacing_dcm_mapping_native`"
        result = list(client.query(count_query).result())
        row_count = result[0].cnt

        msg = f"SUCCESS: Refreshed pacing_dcm_mapping_native with {row_count} rows"
        logger.info(msg)
        print(msg)

        return 0

    finally:
        # Restore service account credential for subsequent steps
        if sa_creds:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = sa_creds
            logger.info("Restored service account credentials")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
