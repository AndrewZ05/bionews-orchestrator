#!/usr/bin/env python3
"""
Automatic Recovery File Cleanup
================================

Integrated cleanup of recovery checkpoint files.

This module provides automatic cleanup that can be triggered:
1. After successful pipeline runs
2. On orchestrator startup (clean old files)
3. Via scheduled task

Author: Data Pipeline Team
Created: 2025-10-07
"""

import os
import time
import logging
import tempfile
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def cleanup_recovery_files(
    max_age_days: float = 7.0,
    min_age_hours: float = 24.0
) -> Dict[str, Any]:
    """
    Clean up recovery files older than max_age_days but newer than min_age_hours.

    This ensures:
    - Very recent files (< min_age_hours) are kept (might be in-progress)
    - Old files (> max_age_days) are deleted
    - Files in between are evaluated based on age

    Args:
        max_age_days: Maximum age before deletion (default: 7 days)
        min_age_hours: Minimum age before deletion (default: 24 hours)

    Returns:
        Dict with cleanup statistics
    """
    stats = {
        'scanned': 0,
        'deleted': 0,
        'kept_recent': 0,
        'kept_active': 0,
        'errors': 0,
        'space_freed_mb': 0.0
    }

    temp_dir = tempfile.gettempdir()
    min_age_seconds = min_age_hours * 3600
    max_age_seconds = max_age_days * 86400

    try:
        for filename in os.listdir(temp_dir):
            if not (filename.startswith('recovery_') and filename.endswith('.pkl')):
                continue

            stats['scanned'] += 1
            file_path = os.path.join(temp_dir, filename)

            try:
                # Get file age
                mtime = os.path.getmtime(file_path)
                age_seconds = time.time() - mtime
                file_size = os.path.getsize(file_path)

                # Decision logic:
                # 1. If file is very recent (< min_age_hours), keep it (might be in-progress)
                # 2. If file is old (> max_age_days), delete it
                # 3. Files in between: keep for now (potential active recovery)

                if age_seconds < min_age_seconds:
                    # Too recent - might be active
                    stats['kept_recent'] += 1
                    logger.debug(f"Keeping recent file: {filename} (age: {age_seconds/3600:.1f}h)")

                elif age_seconds > max_age_seconds:
                    # Too old - delete
                    try:
                        os.remove(file_path)
                        stats['deleted'] += 1
                        stats['space_freed_mb'] += file_size / (1024 * 1024)
                        logger.info(f"Deleted old recovery file: {filename} (age: {age_seconds/86400:.1f} days, size: {file_size/(1024*1024):.2f} MB)")
                    except OSError as e:
                        logger.error(f"Failed to delete {filename}: {e}")
                        stats['errors'] += 1

                else:
                    # In between - keep as potentially active
                    stats['kept_active'] += 1
                    logger.debug(f"Keeping active-range file: {filename} (age: {age_seconds/3600:.1f}h)")

            except OSError as e:
                logger.warning(f"Error processing {filename}: {e}")
                stats['errors'] += 1

    except OSError as e:
        logger.error(f"Error listing temp directory: {e}")
        stats['errors'] += 1

    # Log summary if anything was cleaned
    if stats['deleted'] > 0 or stats['scanned'] > 5:
        logger.info(f"Recovery file cleanup: scanned={stats['scanned']}, deleted={stats['deleted']}, freed={stats['space_freed_mb']:.2f}MB")

    return stats


def cleanup_specific_file(recovery_file: str) -> bool:
    """
    Delete a specific recovery file.

    Args:
        recovery_file: Full path to recovery file

    Returns:
        True if deleted successfully, False otherwise
    """
    try:
        if os.path.exists(recovery_file):
            file_size = os.path.getsize(recovery_file)
            os.remove(recovery_file)
            logger.debug(f"Deleted recovery file: {recovery_file} ({file_size/(1024*1024):.2f} MB)")
            return True
        else:
            logger.debug(f"Recovery file already deleted: {recovery_file}")
            return False
    except OSError as e:
        logger.warning(f"Failed to delete recovery file {recovery_file}: {e}")
        return False


def list_recovery_files(max_results: int = None) -> List[Dict[str, Any]]:
    """
    List all recovery files with metadata.

    Args:
        max_results: Maximum number of results to return

    Returns:
        List of file info dicts
    """
    files = []
    temp_dir = tempfile.gettempdir()

    try:
        for filename in os.listdir(temp_dir):
            if not (filename.startswith('recovery_') and filename.endswith('.pkl')):
                continue

            file_path = os.path.join(temp_dir, filename)

            try:
                stat = os.stat(file_path)
                age_seconds = time.time() - stat.st_mtime

                files.append({
                    'path': file_path,
                    'filename': filename,
                    'size_bytes': stat.st_size,
                    'size_mb': stat.st_size / (1024 * 1024),
                    'modified': datetime.fromtimestamp(stat.st_mtime),
                    'age_hours': age_seconds / 3600,
                    'age_days': age_seconds / 86400
                })

                if max_results and len(files) >= max_results:
                    break

            except OSError:
                continue

    except OSError as e:
        logger.error(f"Error listing recovery files: {e}")

    # Sort by age (oldest first)
    files.sort(key=lambda x: x['age_days'], reverse=True)

    return files


def get_recovery_stats() -> Dict[str, Any]:
    """
    Get statistics about recovery files.

    Returns:
        Dict with stats: count, total_size_mb, oldest_age_days
    """
    files = list_recovery_files()

    stats = {
        'count': len(files),
        'total_size_mb': sum(f['size_mb'] for f in files),
        'oldest_age_days': max((f['age_days'] for f in files), default=0),
        'newest_age_hours': min((f['age_hours'] for f in files), default=0),
        'files': files
    }

    return stats
