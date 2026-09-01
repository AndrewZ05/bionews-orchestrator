#!/usr/bin/env python3
"""
Find PMS tables across ALL WordPress sites using multithreading.
Reports which sites have pms_* tables.
"""

import os
import sys
import logging
import requests
import dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

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

# Suppress most logging for cleaner output
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# PMS tables we're looking for
PMS_TABLES = [
    'pms_member_subscriptions',
    'pms_member_subscriptionmeta',
    'pms_payments',
    'pms_paymentmeta'
]


def get_all_sites():
    """Get ALL sites from discovery API."""
    api_url = "https://pipeline.bionews.com/query/sites"

    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()

        sites = []
        for topic in data.get('topics', []):
            name = topic.get('name', '')
            if name:
                sites.append(name)

        return sorted(sites)
    except Exception as e:
        print(f"Error fetching sites: {e}")
        return []


def check_site_for_pms(site, config):
    """Check a single site for PMS tables. Returns dict with results."""
    result = {
        'site': site,
        'success': False,
        'has_pms': False,
        'pms_tables': [],
        'table_counts': {},
        'error': None
    }

    try:
        connection_info = get_pooled_connection(site, config)
        prefix = connection_info.get('table_prefix', '')

        # Get all tables
        rows = execute_query(connection_info, "SHOW TABLES")

        # Check for PMS tables
        for row in rows:
            full_name = str(list(row.values())[0])
            # Strip prefix to get base name
            if full_name.startswith(prefix):
                base_name = full_name[len(prefix):]
            else:
                base_name = full_name

            # Check if it's a PMS table
            if base_name.startswith('pms_'):
                result['pms_tables'].append(base_name)
                # Get row count for this table
                try:
                    count_result = execute_query(connection_info, f"SELECT COUNT(*) as cnt FROM {full_name}")
                    result['table_counts'][base_name] = count_result[0]['cnt']
                except:
                    result['table_counts'][base_name] = '?'

        result['success'] = True
        result['has_pms'] = len(result['pms_tables']) > 0

    except Exception as e:
        result['error'] = str(e)[:60]

    return result


def find_pms_tables_parallel(max_workers=5):
    """Find PMS tables across all sites using parallel threads."""

    # Load config
    config = load_config('wordpress')

    # Get all sites
    sites = get_all_sites()
    print(f"Checking {len(sites)} sites for PMS tables...")
    print(f"Using {max_workers} parallel workers")
    print()

    results = []
    sites_with_pms = []
    failed_sites = []

    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_site = {
            executor.submit(check_site_for_pms, site, config): site
            for site in sites
        }

        for future in as_completed(future_to_site):
            site = future_to_site[future]
            completed += 1

            try:
                result = future.result()
                results.append(result)

                if result['success']:
                    if result['has_pms']:
                        sites_with_pms.append(result)
                        pms_info = ', '.join([f"{t}({result['table_counts'].get(t, '?')})" for t in result['pms_tables']])
                        print(f"[{completed}/{len(sites)}] {site}: PMS FOUND - {pms_info}")
                    else:
                        print(f"[{completed}/{len(sites)}] {site}: no PMS tables", end='\r')
                else:
                    failed_sites.append(result)
                    print(f"[{completed}/{len(sites)}] {site}: FAILED - {result['error']}", end='\r')

            except Exception as e:
                print(f"[{completed}/{len(sites)}] {site}: ERROR - {e}")

    # Clear the line
    print(" " * 100)

    # Summary
    print()
    print("=" * 80)
    print("SITES WITH PMS TABLES")
    print("=" * 80)

    if sites_with_pms:
        # Group by whether site contains 'hcp'
        hcp_sites = [r for r in sites_with_pms if 'hcp' in r['site'].lower()]
        non_hcp_sites = [r for r in sites_with_pms if 'hcp' not in r['site'].lower()]

        if hcp_sites:
            print("\nHCP Sites:")
            for r in sorted(hcp_sites, key=lambda x: x['site']):
                tables_info = ', '.join([f"{t}({r['table_counts'].get(t, '?')} rows)" for t in sorted(r['pms_tables'])])
                print(f"  {r['site']}: {tables_info}")

        if non_hcp_sites:
            print("\nNon-HCP Sites:")
            for r in sorted(non_hcp_sites, key=lambda x: x['site']):
                tables_info = ', '.join([f"{t}({r['table_counts'].get(t, '?')} rows)" for t in sorted(r['pms_tables'])])
                print(f"  {r['site']}: {tables_info}")
    else:
        print("No sites found with PMS tables.")

    # Table frequency
    print()
    print("=" * 80)
    print("PMS TABLE FREQUENCY")
    print("=" * 80)

    table_freq = defaultdict(int)
    for r in sites_with_pms:
        for t in r['pms_tables']:
            table_freq[t] += 1

    for table, count in sorted(table_freq.items(), key=lambda x: -x[1]):
        print(f"  {count:3d} sites have: {table}")

    # Summary stats
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total sites checked: {len(sites)}")
    print(f"Successful connections: {len(sites) - len(failed_sites)}")
    print(f"Failed connections: {len(failed_sites)}")
    print(f"Sites with PMS tables: {len(sites_with_pms)}")
    print(f"  - HCP sites: {len([r for r in sites_with_pms if 'hcp' in r['site'].lower()])}")
    print(f"  - Non-HCP sites: {len([r for r in sites_with_pms if 'hcp' not in r['site'].lower()])}")

    cleanup_all_connections()
    return sites_with_pms


if __name__ == '__main__':
    find_pms_tables_parallel()
