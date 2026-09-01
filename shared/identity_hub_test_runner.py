#!/usr/bin/env python3
"""
Identity Hub Test Runner — One-command test build + measurement.

Runs a test-mode identity hub build on a sampled window, then reports
coverage metrics compared against the saved baseline.

Usage:
    # Quick test: 3-day sample, report against baseline
    python shared/identity_hub_test_runner.py

    # Custom sample window
    python shared/identity_hub_test_runner.py --sample-days 7

    # Run only specific connectors (fast iteration on a single change)
    python shared/identity_hub_test_runner.py --connectors localstorage

    # Test a specific dataset name
    python shared/identity_hub_test_runner.py --dataset identity_hub_data_dev

    # Save the results as the new baseline (use after validating a change)
    python shared/identity_hub_test_runner.py --save-baseline
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

# Script dir -> project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
IDENTITY_HUB_SCRIPT = PROJECT_ROOT / 'shared' / 'identity_hub.py'
TEST_HELPERS_SCRIPT = PROJECT_ROOT / 'shared' / 'identity_hub_test_helpers.py'


def run_cmd(cmd: list, label: str) -> int:
    """Run a subprocess command with a label, return exit code."""
    print()
    print('#' * 80)
    print(f'# {label}')
    print(f'# $ {" ".join(cmd)}')
    print('#' * 80)
    sys.stdout.flush()
    return subprocess.call(cmd)


def main():
    parser = argparse.ArgumentParser(description='Identity Hub test runner')
    parser.add_argument('--sample-days', type=int, default=3,
                        help='Days of data to process (default: 3)')
    parser.add_argument('--dataset', default='identity_hub_data_test',
                        help='Test output dataset name')
    parser.add_argument('--connectors', nargs='+', default=None,
                        help='Run only specific connectors (for fast iteration)')
    parser.add_argument('--save-baseline', action='store_true',
                        help='Save the results as the new reference baseline')
    parser.add_argument('--skip-build', action='store_true',
                        help='Only run the report, skip the test build')
    parser.add_argument('--project', default='bi-data-391216',
                        help='GCP project')

    args = parser.parse_args()

    python_exe = sys.executable

    overall_start = time.time()

    if not args.skip_build:
        # Build command
        build_cmd = [
            python_exe, str(IDENTITY_HUB_SCRIPT),
            '--test-dataset', args.dataset,
            '--sample-days', str(args.sample_days),
        ]
        if args.connectors:
            build_cmd.extend(['--connectors'] + args.connectors)

        build_rc = run_cmd(build_cmd, f'BUILD: test identity hub ({args.sample_days}-day window)')
        if build_rc != 0:
            print(f"\n[error] Build failed with exit code {build_rc}")
            sys.exit(build_rc)

    # Report command
    report_cmd = [
        python_exe, str(TEST_HELPERS_SCRIPT),
        '--report',
        '--dataset', args.dataset,
        '--project', args.project,
    ]
    if args.save_baseline:
        report_cmd.append('--save-baseline')
        report_cmd.extend(['--sample-days', str(args.sample_days)])

    report_rc = run_cmd(report_cmd, 'REPORT: coverage metrics')

    elapsed = time.time() - overall_start
    print(f'\n[done] Total elapsed: {elapsed:.1f}s')

    sys.exit(report_rc)


if __name__ == '__main__':
    main()
