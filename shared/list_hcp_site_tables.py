#!/usr/bin/env python3
"""
List all tables for WordPress HCP sites only.
Filters to sites containing 'hcp' in the name.
"""

import os
import sys
import logging
import requests
import dotenv
from collections import Counter

# Load environment variables from .env file
dotenv.load_dotenv()

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.config_loader import load_config
from plugins.wordpress_extractor import (
    get_pooled_connection,
    execute_query,
    cleanup_all_connections
)

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_hcp_sites():
    """Get only HCP sites from discovery API."""
    api_url = "https://pipeline.bionews.com/query/sites"

    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Filter to sites containing 'hcp'
        sites = []
        for topic in data.get('topics', []):
            name = topic.get('name', '')
            if name and 'hcp' in name.lower():
                sites.append(name)

        return sorted(sites)
    except Exception as e:
        print(f"Error fetching sites: {e}")
        return []


def list_tables_for_hcp_sites():
    """List all tables for each HCP site."""

    # Load config using config_loader (resolves ${ENV_VAR} patterns)
    config = load_config('wordpress')

    # Get HCP sites only
    sites = get_hcp_sites()
    print(f"Found {len(sites)} HCP sites from API")
    print(f"Sites: {', '.join(sites)}")
    print()

    # Track results
    all_site_tables = {}
    failed_sites = []

    for i, site in enumerate(sites, 1):
        print(f"[{i}/{len(sites)}] {site}...", end=" ", flush=True)
        try:
            connection_info = get_pooled_connection(site, config)
            prefix = connection_info.get('table_prefix', '')

            # Get all tables
            rows = execute_query(connection_info, "SHOW TABLES")

            # Extract base table names (strip prefix)
            tables = []
            for row in rows:
                full_name = str(list(row.values())[0])
                if full_name.startswith(prefix):
                    base_name = full_name[len(prefix):]
                else:
                    base_name = full_name
                if base_name:
                    tables.append(base_name)

            all_site_tables[site] = {
                'prefix': prefix,
                'tables': sorted(tables)
            }
            print(f"OK - {len(tables)} tables (prefix: {prefix})")

        except Exception as e:
            error_msg = str(e)[:60]
            print(f"FAILED: {error_msg}")
            failed_sites.append({'site': site, 'error': str(e)})

    # Print detailed results
    print()
    print("=" * 80)
    print("TABLES BY HCP SITE")
    print("=" * 80)

    for site, info in sorted(all_site_tables.items()):
        print(f"\n{site} (prefix: {info['prefix']}, {len(info['tables'])} tables):")
        for table in info['tables']:
            print(f"  - {table}")

    # Count table occurrences across all sites
    table_counts = Counter()
    for info in all_site_tables.values():
        table_counts.update(info['tables'])

    # Print tables with counts (sorted by count descending, then alphabetically)
    print()
    print("=" * 80)
    print("TABLE COUNTS ACROSS HCP SITES (sorted by frequency)")
    print("=" * 80)

    # Sort by count (descending), then by name (ascending)
    sorted_tables = sorted(table_counts.items(), key=lambda x: (-x[1], x[0]))

    for table, count in sorted_tables:
        print(f"{count:4d}  {table}")

    all_unique_tables = set(table_counts.keys())

    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total HCP sites: {len(sites)}")
    print(f"Successful connections: {len(all_site_tables)}")
    print(f"Failed connections: {len(failed_sites)}")
    print(f"Unique tables found: {len(all_unique_tables)}")

    if failed_sites:
        print()
        print("Failed sites:")
        for f in failed_sites:
            print(f"  {f['site']}: {f['error'][:50]}")

    cleanup_all_connections()
    return all_site_tables, all_unique_tables


if __name__ == '__main__':
    list_tables_for_hcp_sites()
