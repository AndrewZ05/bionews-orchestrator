#!/usr/bin/env python3
"""
Identity Hub Test Helpers — Baseline measurement and coverage reporting.

Use this to measure identity linkage coverage against a test dataset,
save baselines, and diff new runs against them.

Usage:
    # Save baseline (run after a test build you trust)
    python shared/identity_hub_test_helpers.py --save-baseline --dataset identity_hub_data_test

    # Report coverage and compare against baseline
    python shared/identity_hub_test_helpers.py --report --dataset identity_hub_data_test

    # Report without comparison
    python shared/identity_hub_test_helpers.py --report --dataset identity_hub_data_test --no-compare
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

# Setup credentials before google.cloud import (cross-platform)
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
    for _p in (r'c:\gcp\service-account-bionews-pipeline.json', '/home/orchestrator/service-account-bionews-pipeline.json'):
        if os.path.exists(_p):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _p
            break

from google.cloud import bigquery


BASELINE_PATH = Path(__file__).parent.parent / 'docs' / 'identity_hub_baseline.json'

# Personal/anchor identifier types — a bn_id is "known" if it has any of these
PERSONAL_TYPES = (
    'email', 'mc_euid', 'bionews_uk', 'npi_number',
    'wp_user_id', 'phone', 'aim_dgid', 'agile_crm_guid', 'subscriber_hash'
)


def build_queries(project: str, dataset: str) -> Dict[str, str]:
    """Build all coverage measurement queries for a given dataset."""
    xref = f"`{project}.{dataset}.bn_id_xref`"
    hub = f"`{project}.{dataset}.bn_id_hub`"
    personal_list = ",".join(f"'{t}'" for t in PERSONAL_TYPES)

    return {
        'total_bn_ids': f"SELECT COUNT(DISTINCT bn_id) AS v FROM {xref}",

        'known_bn_ids': f"""
            SELECT COUNT(DISTINCT bn_id) AS v FROM {xref}
            WHERE bn_id IN (
                SELECT DISTINCT bn_id FROM {xref}
                WHERE identifier_type IN ({personal_list})
            )
        """,

        'anonymous_bn_ids': f"""
            SELECT COUNT(DISTINCT bn_id) AS v FROM {xref}
            WHERE bn_id NOT IN (
                SELECT DISTINCT bn_id FROM {xref}
                WHERE identifier_type IN ({personal_list})
            )
        """,

        'emails_in_graph': f"""
            SELECT COUNT(DISTINCT bn_id) AS v FROM {xref}
            WHERE identifier_type = 'email'
        """,

        'emails_linked_to_bnfpvid': f"""
            SELECT COUNT(DISTINCT bn_id) AS v FROM {xref}
            WHERE bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'email')
              AND bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'bnfpvid')
        """,

        'emails_linked_to_client_id': f"""
            SELECT COUNT(DISTINCT bn_id) AS v FROM {xref}
            WHERE bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'email')
              AND bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'client_id')
        """,

        'emails_linked_to_mc_euid': f"""
            SELECT COUNT(DISTINCT bn_id) AS v FROM {xref}
            WHERE bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'email')
              AND bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'mc_euid')
        """,

        'emails_stranded_no_web_id': f"""
            SELECT COUNT(DISTINCT bn_id) AS v FROM {xref}
            WHERE bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'email')
              AND bn_id NOT IN (
                  SELECT bn_id FROM {xref}
                  WHERE identifier_type IN ('bnfpvid','client_id','dmd_tag','fbc','gcl_au','aim_tag_id')
              )
        """,

        'full_chain_email_mceuid_clientid_bnfpvid': f"""
            SELECT COUNT(DISTINCT bn_id) AS v FROM {xref}
            WHERE bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'email')
              AND bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'mc_euid')
              AND bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'client_id')
              AND bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'bnfpvid')
        """,

        'npis_in_graph': f"""
            SELECT COUNT(DISTINCT bn_id) AS v FROM {xref}
            WHERE identifier_type = 'npi_number'
        """,

        'npis_linked_to_bnfpvid': f"""
            SELECT COUNT(DISTINCT bn_id) AS v FROM {xref}
            WHERE bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'npi_number')
              AND bn_id IN (SELECT bn_id FROM {xref} WHERE identifier_type = 'bnfpvid')
        """,

        'xref_total_rows': f"SELECT COUNT(*) AS v FROM {xref}",

        'hub_total_rows': f"SELECT COUNT(*) AS v FROM {hub}",
    }


def measure_coverage(client: bigquery.Client, project: str, dataset: str) -> Dict[str, int]:
    """Run all coverage queries and return a dict of metric → count."""
    queries = build_queries(project, dataset)
    results = {}
    for name, sql in queries.items():
        try:
            row = next(iter(client.query(sql).result()))
            results[name] = int(row['v'] or 0)
        except Exception as e:
            results[name] = -1  # Sentinel for "query failed"
            print(f"  [warn] {name}: {str(e)[:100]}", file=sys.stderr)
    return results


def save_baseline(metrics: Dict[str, int], dataset: str, sample_days: int = None) -> None:
    """Save metrics as the reference baseline."""
    BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'saved_at': datetime.now(timezone.utc).isoformat(),
        'dataset': dataset,
        'sample_days': sample_days,
        'metrics': metrics,
    }
    BASELINE_PATH.write_text(json.dumps(payload, indent=2))
    print(f"\n[saved] Baseline written to {BASELINE_PATH}")


def load_baseline() -> Dict[str, Any]:
    """Load the previously saved baseline, or None."""
    if not BASELINE_PATH.exists():
        return None
    return json.loads(BASELINE_PATH.read_text())


def format_delta(current: int, baseline: int) -> str:
    """Format delta as '+N (+X%)' or '-N (-X%)'."""
    if baseline is None or baseline == 0:
        return '(no baseline)'
    delta = current - baseline
    pct = (delta / baseline) * 100
    sign = '+' if delta >= 0 else ''
    return f"{sign}{delta:,} ({sign}{pct:.1f}%)"


def print_report(metrics: Dict[str, int], baseline: Dict[str, Any] = None) -> None:
    """Print a human-readable coverage report."""
    print()
    print('=' * 80)
    print('IDENTITY HUB COVERAGE REPORT')
    print('=' * 80)
    if baseline:
        print(f"Baseline saved: {baseline['saved_at']} (dataset={baseline['dataset']}, sample_days={baseline.get('sample_days')})")
    print()

    baseline_metrics = baseline['metrics'] if baseline else {}

    print(f"  {'METRIC':<45} {'CURRENT':>15} {'vs BASELINE':>18}")
    print(f"  {'-' * 45:<45} {'-' * 15:>15} {'-' * 18:>18}")

    groups = [
        ('Graph scale', [
            'total_bn_ids', 'known_bn_ids', 'anonymous_bn_ids',
            'xref_total_rows', 'hub_total_rows',
        ]),
        ('Email coverage', [
            'emails_in_graph', 'emails_linked_to_mc_euid',
            'emails_linked_to_client_id', 'emails_linked_to_bnfpvid',
            'emails_stranded_no_web_id', 'full_chain_email_mceuid_clientid_bnfpvid',
        ]),
        ('NPI coverage', [
            'npis_in_graph', 'npis_linked_to_bnfpvid',
        ]),
    ]

    for group_name, keys in groups:
        print(f"\n  [{group_name}]")
        for k in keys:
            current = metrics.get(k, -1)
            baseline_val = baseline_metrics.get(k)
            delta = format_delta(current, baseline_val) if baseline_val is not None else '(no baseline)'
            current_str = f"{current:,}" if current >= 0 else "ERROR"
            print(f"  {k:<45} {current_str:>15} {delta:>18}")

    print()

    # Key ratio: email coverage
    emails = metrics.get('emails_in_graph', 0)
    linked = metrics.get('emails_linked_to_bnfpvid', 0)
    stranded = metrics.get('emails_stranded_no_web_id', 0)
    if emails > 0:
        print(f"  Email -> bnfpvid coverage: {linked:,} / {emails:,} ({100.0 * linked / emails:.1f}%)")
        print(f"  Emails stranded (no web id): {stranded:,} / {emails:,} ({100.0 * stranded / emails:.1f}%)")

    # NPI coverage
    npis = metrics.get('npis_in_graph', 0)
    npi_linked = metrics.get('npis_linked_to_bnfpvid', 0)
    if npis > 0:
        print(f"  NPI -> bnfpvid coverage:   {npi_linked:,} / {npis:,} ({100.0 * npi_linked / npis:.1f}%)")
    print('=' * 80)


def main():
    parser = argparse.ArgumentParser(description='Identity Hub test helpers')
    parser.add_argument('--dataset', default='identity_hub_data_test',
                        help='Dataset to measure (default: identity_hub_data_test)')
    parser.add_argument('--project', default='bi-data-391216',
                        help='GCP project ID')
    parser.add_argument('--save-baseline', action='store_true',
                        help='Save the current metrics as the reference baseline')
    parser.add_argument('--report', action='store_true',
                        help='Print a coverage report')
    parser.add_argument('--sample-days', type=int, default=None,
                        help='Metadata: record which sample window was used')
    parser.add_argument('--no-compare', action='store_true',
                        help='Report without comparing against baseline')

    args = parser.parse_args()

    if not args.save_baseline and not args.report:
        parser.print_help()
        sys.exit(1)

    client = bigquery.Client(project=args.project)
    print(f"Measuring coverage: {args.project}.{args.dataset}")
    metrics = measure_coverage(client, args.project, args.dataset)

    if args.save_baseline:
        save_baseline(metrics, args.dataset, args.sample_days)

    if args.report:
        baseline = None if args.no_compare else load_baseline()
        print_report(metrics, baseline)


if __name__ == '__main__':
    main()
