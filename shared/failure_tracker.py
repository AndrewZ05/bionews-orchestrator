"""
Centralized failure tracking for pipeline extractions.

Tracks failures across all tables in a pipeline run, generates failure manifests,
and provides retry capabilities.
"""

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import logging

logger = logging.getLogger(__name__)


class FailureTracker:
    """Thread-safe failure tracker for pipeline extractions."""

    def __init__(self, execution_id: str, source: str, group: Optional[str] = None):
        """
        Initialize failure tracker.

        Args:
            execution_id: Unique ID for this pipeline execution
            source: Data source (e.g., 'mailchimp', 'wordpress')
            group: Optional table group being extracted
        """
        self.execution_id = execution_id
        self.source = source
        self.group = group
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.date_range: Dict[str, Optional[str]] = {
            'start_date': None,
            'end_date': None
        }

        # failures_by_table[table_name] = list of failure dicts
        self.failures_by_table: Dict[str, List[Dict[str, Any]]] = {}
        self.lock = threading.Lock()

    def set_date_range(self, start_date: Optional[str], end_date: Optional[str]) -> None:
        """Set the date range for this extraction."""
        with self.lock:
            self.date_range['start_date'] = start_date
            self.date_range['end_date'] = end_date

    def register_failure(
        self,
        table: str,
        item_id: str,
        error: str,
        attempts: int = 1,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Register a failed item extraction.

        Args:
            table: Table name where failure occurred
            item_id: Unique identifier for the failed item (e.g., campaign_id)
            error: Error message or exception string
            attempts: Number of retry attempts made
            metadata: Optional additional context
        """
        failure_record = {
            'item_id': item_id,
            'error': str(error),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'attempts': attempts
        }

        if metadata:
            failure_record['metadata'] = metadata

        with self.lock:
            if table not in self.failures_by_table:
                self.failures_by_table[table] = []
            self.failures_by_table[table].append(failure_record)

    def get_failed_items(self, table: str) -> List[str]:
        """Get list of failed item IDs for a specific table."""
        with self.lock:
            if table not in self.failures_by_table:
                return []
            return [f['item_id'] for f in self.failures_by_table[table]]

    def has_failures(self) -> bool:
        """Check if any failures were recorded."""
        with self.lock:
            return len(self.failures_by_table) > 0

    def get_failure_count(self, table: Optional[str] = None) -> int:
        """Get total failure count for a table or all tables."""
        with self.lock:
            if table:
                return len(self.failures_by_table.get(table, []))
            return sum(len(failures) for failures in self.failures_by_table.values())

    def get_summary(self) -> Dict[str, Any]:
        """Get failure summary statistics."""
        with self.lock:
            total_failures = sum(len(failures) for failures in self.failures_by_table.values())
            affected_tables = len(self.failures_by_table)

            return {
                'total_failures': total_failures,
                'affected_tables': affected_tables,
                'tables': list(self.failures_by_table.keys())
            }

    def to_manifest(self) -> Dict[str, Any]:
        """Convert failures to manifest dictionary."""
        with self.lock:
            summary = self.get_summary()

            # Build per-table details
            failures_by_table = {}
            for table, failures in self.failures_by_table.items():
                failures_by_table[table] = {
                    'failed_count': len(failures),
                    'failed_items': failures
                }

            return {
                'execution_id': self.execution_id,
                'source': self.source,
                'group': self.group,
                'timestamp': self.timestamp,
                'date_range': self.date_range,
                'failures_by_table': failures_by_table,
                'summary': summary
            }

    def save_manifest(self, output_dir: str = 'failures') -> Optional[str]:
        """
        Save failure manifest to JSON file.

        Args:
            output_dir: Directory to save manifests (default: 'failures/')

        Returns:
            Path to saved manifest, or None if no failures
        """
        if not self.has_failures():
            return None

        # Create output directory
        Path(output_dir).mkdir(exist_ok=True)

        # Generate filename: {source}_{timestamp}_{execution_id}.json
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.source}_{timestamp_str}_{self.execution_id[:8]}.json"
        filepath = os.path.join(output_dir, filename)

        # Write manifest
        manifest = self.to_manifest()
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        logger.info(f"Failure manifest saved to: {filepath}")
        return filepath

    def log_summary(self) -> None:
        """Log failure summary to console."""
        if not self.has_failures():
            return

        summary = self.get_summary()
        total = summary['total_failures']

        logger.warning("=" * 80)
        logger.warning("EXTRACTION COMPLETED WITH PARTIAL FAILURES")
        logger.warning("=" * 80)
        logger.warning("")
        logger.warning(f"Total failures: {total}")
        logger.warning(f"Affected tables: {summary['affected_tables']}")
        logger.warning("")

        for table in summary['tables']:
            count = self.get_failure_count(table)
            failed_ids = self.get_failed_items(table)
            logger.warning(f"  {table}: {count} items failed")
            if count <= 10:
                for item_id in failed_ids:
                    logger.warning(f"    - {item_id}")
            else:
                for item_id in failed_ids[:5]:
                    logger.warning(f"    - {item_id}")
                logger.warning(f"    ... and {count - 5} more")

        logger.warning("")
        logger.warning("=" * 80)


def load_failure_manifest(filepath: str) -> Dict[str, Any]:
    """
    Load failure manifest from JSON file.

    Args:
        filepath: Path to manifest file

    Returns:
        Manifest dictionary

    Raises:
        FileNotFoundError: If manifest doesn't exist
        json.JSONDecodeError: If manifest is invalid
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_retry_filters(manifest: Dict[str, Any]) -> Dict[str, Set[str]]:
    """
    Extract retry filters from failure manifest.

    Args:
        manifest: Failure manifest dictionary

    Returns:
        Dict mapping table names to sets of failed item IDs
    """
    retry_filters = {}

    failures_by_table = manifest.get('failures_by_table', {})
    for table, table_data in failures_by_table.items():
        failed_items = table_data.get('failed_items', [])
        retry_filters[table] = {item['item_id'] for item in failed_items}

    return retry_filters


class FailureManifestManager:
    """Manages discovery and processing of failure manifests."""

    def __init__(self, manifest_dir: str = 'failures'):
        """
        Initialize manifest manager.

        Args:
            manifest_dir: Directory containing failure manifests
        """
        self.manifest_dir = Path(manifest_dir)
        self.archive_dir = self.manifest_dir / 'archive'

    def find_manifests(
        self,
        source: Optional[str] = None,
        group: Optional[str] = None,
        table: Optional[str] = None
    ) -> List[Path]:
        """
        Find failure manifests matching criteria.

        Args:
            source: Filter by source (e.g., 'mailchimp')
            group: Filter by group (e.g., 'campaign_group')
            table: Filter by table (e.g., 'campaign_locations')

        Returns:
            List of matching manifest file paths, sorted by timestamp (newest first)
        """
        if not self.manifest_dir.exists():
            return []

        # Get all JSON files in manifest directory (not archive)
        all_manifests = list(self.manifest_dir.glob('*.json'))

        # Filter by criteria
        matching = []
        for filepath in all_manifests:
            try:
                manifest = load_failure_manifest(str(filepath))

                # Filter by source
                if source and manifest.get('source') != source:
                    continue

                # Filter by group
                if group and manifest.get('group') != group:
                    continue

                # Filter by table
                if table:
                    failures_by_table = manifest.get('failures_by_table', {})
                    if table not in failures_by_table:
                        continue

                matching.append(filepath)

            except (json.JSONDecodeError, KeyError, FileNotFoundError):
                logger.warning(f"Invalid manifest file: {filepath}")
                continue

        # Sort by modification time (newest first)
        matching.sort(key=lambda p: p.stat().st_mtime, reverse=True)

        return matching

    def get_combined_failures(
        self,
        manifests: List[Path],
        table_filter: Optional[str] = None
    ) -> Dict[str, Set[str]]:
        """
        Combine failures from multiple manifests.

        Args:
            manifests: List of manifest file paths
            table_filter: Optional table name to filter

        Returns:
            Dict mapping table names to sets of failed item IDs
        """
        combined_failures: Dict[str, Set[str]] = {}

        for filepath in manifests:
            try:
                manifest = load_failure_manifest(str(filepath))
                failures_by_table = manifest.get('failures_by_table', {})

                for table, table_data in failures_by_table.items():
                    # Apply table filter if specified
                    if table_filter and table != table_filter:
                        continue

                    # Add failed items to combined set
                    if table not in combined_failures:
                        combined_failures[table] = set()

                    failed_items = table_data.get('failed_items', [])
                    for item in failed_items:
                        combined_failures[table].add(item['item_id'])

            except (json.JSONDecodeError, KeyError, FileNotFoundError) as e:
                logger.warning(f"Error reading manifest {filepath}: {e}")
                continue

        return combined_failures

    def archive_manifest(self, filepath: Path, reason: str = 'completed') -> Path:
        """
        Move manifest to archive directory.

        Args:
            filepath: Path to manifest file
            reason: Reason for archiving (e.g., 'completed', 'partial')

        Returns:
            Path to archived file
        """
        # Create archive directory
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        # Generate archive filename with reason
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_name = f"{filepath.stem}_{reason}_{timestamp}.json"
        archive_path = self.archive_dir / archive_name

        # Move file
        filepath.rename(archive_path)
        logger.info(f"Archived manifest: {filepath.name} -> {archive_path.name}")

        return archive_path

    def remove_manifest(self, filepath: Path) -> None:
        """
        Delete manifest file (after 100% success).

        Args:
            filepath: Path to manifest file
        """
        filepath.unlink()
        logger.info(f"Deleted manifest (100% success): {filepath.name}")

    def update_manifest(
        self,
        original_path: Path,
        new_failures: Dict[str, Set[str]]
    ) -> Optional[Path]:
        """
        Update manifest with new failure set (after partial retry success).

        Args:
            original_path: Path to original manifest
            new_failures: New set of failures by table

        Returns:
            Path to new manifest, or None if no failures remain
        """
        if not new_failures or all(len(ids) == 0 for ids in new_failures.values()):
            # 100% success - remove manifest
            self.remove_manifest(original_path)
            return None

        # Load original manifest
        original_manifest = load_failure_manifest(str(original_path))

        # Build new manifest with updated failures
        new_manifest = {
            'execution_id': original_manifest.get('execution_id'),
            'source': original_manifest.get('source'),
            'group': original_manifest.get('group'),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'date_range': original_manifest.get('date_range'),
            'original_manifest': str(original_path.name),
            'retry_iteration': original_manifest.get('retry_iteration', 0) + 1,
            'failures_by_table': {},
            'summary': {}
        }

        # Add remaining failures
        total_failures = 0
        for table, failed_ids in new_failures.items():
            if len(failed_ids) == 0:
                continue

            # Find original failure details for these items
            original_table_data = original_manifest.get('failures_by_table', {}).get(table, {})
            original_items = {item['item_id']: item for item in original_table_data.get('failed_items', [])}

            # Build new failed_items list
            new_failed_items = []
            for item_id in failed_ids:
                if item_id in original_items:
                    # Preserve original error info
                    new_failed_items.append(original_items[item_id])
                else:
                    # New failure (shouldn't happen, but handle it)
                    new_failed_items.append({
                        'item_id': item_id,
                        'error': 'Failed during retry',
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'attempts': 1
                    })

            new_manifest['failures_by_table'][table] = {
                'failed_count': len(new_failed_items),
                'failed_items': new_failed_items
            }
            total_failures += len(new_failed_items)

        # Update summary
        new_manifest['summary'] = {
            'total_failures': total_failures,
            'affected_tables': len(new_manifest['failures_by_table']),
            'tables': list(new_manifest['failures_by_table'].keys())
        }

        # Generate new filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        retry_num = new_manifest['retry_iteration']
        new_filename = f"{original_manifest['source']}_{timestamp}_retry{retry_num}_{original_manifest['execution_id'][:8]}.json"
        new_path = self.manifest_dir / new_filename

        # Save new manifest
        with open(new_path, 'w', encoding='utf-8') as f:
            json.dump(new_manifest, f, indent=2, ensure_ascii=False)

        # Archive original
        self.archive_manifest(original_path, reason=f'retry{retry_num}_partial')

        logger.info(f"Updated manifest: {original_path.name} -> {new_path.name}")
        logger.info(f"  Remaining failures: {total_failures} across {len(new_manifest['failures_by_table'])} tables")

        return new_path
