#!/usr/bin/env python3
"""
Facebook Pages/Posts/Insights Backfill - 3 Month Increments
Runs sequential extraction batches to restore historical data from Jan 2025 to May 2026
"""

import subprocess
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

# Setup logging
LOG_DIR = Path("/c/orchestrator/logs/backfill")
LOG_DIR.mkdir(parents=True, exist_ok=True)

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
MASTER_LOG = LOG_DIR / f"backfill_pages_posts_{TIMESTAMP}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(MASTER_LOG),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Backfill batches: 3 months each
BATCHES: List[Tuple[str, str]] = [
    ("2025-01-01", "2025-03-31"),
    ("2025-04-01", "2025-06-29"),
    ("2025-06-30", "2025-09-27"),
    ("2025-09-28", "2025-12-26"),
    ("2025-12-27", "2026-03-26"),
    ("2026-03-27", "2026-05-09"),
]

def run_backfill_batch(batch_num: int, start_date: str, end_date: str) -> bool:
    """
    Run a single backfill batch for a date range.

    Args:
        batch_num: Batch number (1-indexed)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        True if successful, False otherwise
    """
    batch_log = LOG_DIR / f"batch_{batch_num}_{start_date}_{end_date}.log"

    logger.info(f"{'='*80}")
    logger.info(f"Batch {batch_num}/{len(BATCHES)}: {start_date} to {end_date}")
    logger.info(f"{'='*80}")
    logger.info(f"Start time: {datetime.now()}")
    logger.info(f"Log file: {batch_log}")

    cmd = [
        "python", "orchestrate.py",
        "--source", "facebook",
        "--env", "prod",
        "--tables", "pages", "posts", "page_insights", "post_insights",
        "--start-date", start_date,
        "--end-date", end_date
    ]

    try:
        with open(batch_log, "w") as log_file:
            result = subprocess.run(
                cmd,
                cwd="/c/orchestrator",
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=3600  # 1 hour timeout per batch
            )

        if result.returncode == 0:
            logger.info(f"✓ Batch {batch_num} completed successfully")
            return True
        else:
            logger.error(f"✗ Batch {batch_num} failed with return code {result.returncode}")
            logger.error(f"  Check log: {batch_log}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"✗ Batch {batch_num} timed out (exceeded 1 hour)")
        return False
    except Exception as e:
        logger.error(f"✗ Batch {batch_num} error: {e}")
        return False

def main():
    """Run all backfill batches."""
    logger.info(f"{'='*80}")
    logger.info(f"Facebook Backfill: Pages, Posts, Page Insights, Post Insights")
    logger.info(f"3-Month Increments: Jan 2025 - May 2026")
    logger.info(f"Started: {datetime.now()}")
    logger.info(f"{'='*80}")

    logger.info(f"\nBackfill Schedule ({len(BATCHES)} batches):")
    for i, (start, end) in enumerate(BATCHES, 1):
        logger.info(f"  Batch {i}: {start} to {end}")
    logger.info("")

    successful = []
    failed = []

    # Run each batch
    for i, (start_date, end_date) in enumerate(BATCHES, 1):
        if run_backfill_batch(i, start_date, end_date):
            successful.append((start_date, end_date))
        else:
            failed.append((start_date, end_date))

        # Add delay between batches (except after last batch)
        if i < len(BATCHES):
            logger.info(f"\nWaiting 60 seconds before next batch...")
            import time
            time.sleep(60)

    # Summary
    logger.info(f"\n{'='*80}")
    logger.info(f"Backfill Complete: {datetime.now()}")
    logger.info(f"{'='*80}")
    logger.info(f"\nResults Summary:")
    logger.info(f"  Successful: {len(successful)}/{len(BATCHES)} batches")
    logger.info(f"  Failed: {len(failed)}/{len(BATCHES)} batches")

    if successful:
        logger.info(f"\n✓ Successful batches:")
        for start, end in successful:
            logger.info(f"    {start} to {end}")

    if failed:
        logger.info(f"\n✗ Failed batches:")
        for start, end in failed:
            logger.info(f"    {start} to {end}")
        logger.info(f"\nCheck logs in {LOG_DIR} for error details")

    logger.info(f"\n{'='*80}")
    logger.info(f"Master log: {MASTER_LOG}")
    logger.info(f"{'='*80}")

    return 0 if not failed else 1

if __name__ == "__main__":
    sys.exit(main())
