#!/usr/bin/env python3
"""
Run the local release-readiness checks for the profile database project.

This is the one-command process audit for pre-release work on the profile
database. It intentionally bundles the checks that matter most before a real
rebuild:

  - full SQL dry-run
  - refresh-scope audit
  - operator guide regeneration + drift check
  - Python syntax compilation for the key runtime/tooling files

Usage:
    python scripts/profile_release_check.py
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent

BIGQUERY_CHECKS = (
    ("SQL dry-run", [sys.executable, "scripts/dry_run_profile_sql.py"]),
    ("Views smoke test", [sys.executable, "scripts/profile_views_smoke_test.py"]),
    (
        "View column coverage (candidate)",
        [
            sys.executable,
            "scripts/profile_core_view_coverage_check.py",
            "--dataset",
            "profile_data_candidate",
        ],
    ),
    ("SQL module tests", [sys.executable, "scripts/profile_sql_module_tests.py"]),
)

LOCAL_CHECKS = (
    ("Scope audit", [sys.executable, "scripts/scope_predicate_audit.py"]),
    ("Build modes contract check", [sys.executable, "scripts/build_modes_contract_check.py"]),
    ("Refresh-scope contract check", [sys.executable, "scripts/refresh_scope_contract_check.py"]),
    ("Publish manifest contract check", [sys.executable, "scripts/publish_manifest_contract_check.py"]),
    ("Build report contract check", [sys.executable, "scripts/build_report_contract_check.py"]),
    ("Schema contract generate", [sys.executable, "scripts/generate_schema_contract.py"]),
    ("Schema contract check", [sys.executable, "scripts/generate_schema_contract.py", "--check"]),
    ("Operator guide generate", [sys.executable, "scripts/generate_operator_guide.py"]),
    ("Operator guide check", [sys.executable, "scripts/generate_operator_guide.py", "--check"]),
    ("Lineage coverage contract check", [sys.executable, "scripts/lineage_coverage_contract_check.py"]),
    ("Operator playbook contract check", [sys.executable, "scripts/operator_playbook_contract_check.py"]),
)

PY_COMPILE_TARGETS = (
    "plugins/profile_database_extractor.py",
    "shared/profile_database_manifest.py",
    "shared/post_processor.py",
    "scripts/dry_run_profile_sql.py",
    "scripts/profile_views_smoke_test.py",
    "scripts/profile_core_view_coverage_check.py",
    "scripts/generate_schema_contract.py",
    "scripts/generate_operator_guide.py",
    "scripts/scope_predicate_audit.py",
    "scripts/build_modes_contract_check.py",
    "scripts/refresh_scope_contract_check.py",
    "scripts/publish_manifest_contract_check.py",
    "scripts/build_report_contract_check.py",
    "scripts/lineage_coverage_contract_check.py",
    "scripts/operator_playbook_contract_check.py",
    "scripts/profile_release_check.py",
    "scripts/profile_sql_module_tests.py",
    "shared/profile_db_check.py",
    "shared/profile_facts.py",
)


def run_command(label: str, argv: list[str]) -> None:
    print(f"[RUN] {label}: {' '.join(argv)}")
    completed = subprocess.run(argv, cwd=REPO_ROOT)
    if completed.returncode != 0:
        raise RuntimeError(f"{label} failed with exit code {completed.returncode}")
    print(f"[OK ] {label}")


def run_syntax_check() -> None:
    print("[RUN] Python syntax compilation")
    for rel_path in PY_COMPILE_TARGETS:
        path = REPO_ROOT / rel_path
        source = path.read_text(encoding="utf-8")
        compile(source, str(path), "exec")
        print(f"[OK ] compile {rel_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run profile database release-readiness checks."
    )
    parser.add_argument(
        "--skip-bq",
        action="store_true",
        help="Skip BigQuery-dependent checks (useful for CI without credentials).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        for label, argv in LOCAL_CHECKS:
            run_command(label, list(argv))
        if args.skip_bq:
            print("[SKIP] BigQuery-dependent checks (--skip-bq)")
        else:
            for label, argv in BIGQUERY_CHECKS:
                run_command(label, list(argv))
        run_syntax_check()
    except Exception as exc:
        print(f"[FAIL] {exc}")
        return 1

    print("[OK ] Profile release check completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
