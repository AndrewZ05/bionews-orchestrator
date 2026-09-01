#!/usr/bin/env python3
"""
Execute profile_database_views.sql against isolated BigQuery datasets.

Unlike the dry-run harness, this catches real dependency-order problems between
views inside the file (for example, creating an audience view before
profile_current_safe exists in the target view dataset).

Usage:
    python scripts/profile_views_smoke_test.py

Cleanup hardening:
    The test creates three temporary datasets (profile_data_viewsmoke_<uuid>,
    profile_ops_viewsmoke_<uuid>, profile_staging_viewsmoke_<uuid>) and removes
    them at exit. Cleanup runs from:
      1. try/finally on the happy path
      2. atexit handler for unexpected returns
      3. SIGINT / SIGTERM handlers for Ctrl-C and terminal close

    If a previous run was killed before cleanup completed (e.g., closed terminal
    on Windows where SIGTERM cannot be intercepted), use:
        python scripts/sweep_smoke_test_datasets.py
"""

from __future__ import annotations

import atexit
import signal
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.cloud import bigquery

from shared.post_processor import execute_post_process_sql
from scripts.dry_run_profile_sql import cleanup_datasets, get_bq_client, preseed


REPO_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_VIEWS = (
    "profile_signals",
    "profile_explain",
    "profile_current",
    "profile_current_safe",
    "profile_contactability",
    "profile_marketing_audience",
    "profile_analytics_audience",
    "profile_ops_audience",
    "profile_audience_hcp",
    "profile_audience_patients_confirmed",
    "profile_audience_caregivers",
    "profile_audience_high_engagement",
)


def assert_views_exist(client: bigquery.Client, dataset_name: str) -> None:
    sql = f"""
    SELECT table_name
    FROM {dataset_name}.INFORMATION_SCHEMA.VIEWS
    """
    existing = {row.table_name for row in client.query(sql).result()}
    missing = [view_name for view_name in REQUIRED_VIEWS if view_name not in existing]
    if missing:
        raise RuntimeError(
            f"Missing expected views in {dataset_name}: {', '.join(missing)}"
        )


def run_phase(
    client: bigquery.Client,
    consumer_dataset: str,
    ops_dataset: str,
    staging_dataset: str,
    view_dataset: str,
    label: str,
) -> None:
    result = execute_post_process_sql(
        bq_client=client,
        sql_file="sql/profile_database_views.sql",
        config={},
        source="profile_database",
        execution_id=f"views_smoke_{label}",
        context={
            "build_id": f"views_smoke_{label}",
            "consumer_dataset": consumer_dataset,
            "ops_dataset": ops_dataset,
            "staging_dataset": staging_dataset,
            "view_dataset": view_dataset,
        },
    )
    statements = result.get("statements_executed", 0)
    print(f"[OK ] {label}: executed {statements} statements into {view_dataset}")
    assert_views_exist(client, view_dataset)
    print(f"[OK ] {label}: required views present in {view_dataset}")


# Module-level cleanup state so signal handlers + atexit can reach it without
# a closure. Set by main(); read by _emergency_cleanup().
_CLEANUP_STATE: dict = {
    "client": None,
    "datasets": (),
    "done": False,
}


def _emergency_cleanup() -> None:
    """Idempotent cleanup callable from atexit + signal handlers.

    Safe to call multiple times; once `done` is set, subsequent calls no-op.
    Uses cleanup_datasets() with not_found_ok semantics so a partially-cleaned
    state is fine.
    """
    if _CLEANUP_STATE["done"]:
        return
    client = _CLEANUP_STATE["client"]
    datasets = _CLEANUP_STATE["datasets"]
    if client is None or not datasets:
        return
    try:
        print(f"[cleanup] Removing smoke-test datasets: {', '.join(datasets)}")
        cleanup_datasets(client, *datasets)
        _CLEANUP_STATE["done"] = True
        print("[cleanup] Done.")
    except Exception as exc:
        print(f"[cleanup WARN] {exc}", file=sys.stderr)


def _signal_cleanup(signum, _frame):
    """SIGINT / SIGTERM handler. Cleans up then re-raises with the right exit
    code so callers see the signal-based exit."""
    sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
    print(f"\n[signal] received {sig_name}; running cleanup before exit", file=sys.stderr)
    _emergency_cleanup()
    sys.exit(128 + signum)


def main() -> int:
    client = get_bq_client()
    suffix = uuid.uuid4().hex[:8]
    consumer_dataset = f"profile_data_viewsmoke_{suffix}"
    ops_dataset = f"profile_ops_viewsmoke_{suffix}"
    staging_dataset = f"profile_staging_viewsmoke_{suffix}"

    # Wire all three cleanup paths BEFORE creating any datasets, so any exit
    # path beyond this point still sweeps the temporary datasets.
    _CLEANUP_STATE["client"] = client
    _CLEANUP_STATE["datasets"] = (consumer_dataset, ops_dataset, staging_dataset)
    atexit.register(_emergency_cleanup)
    # SIGTERM is reliably deliverable on POSIX; on Windows Python only handles
    # SIGINT (Ctrl-C) and SIGBREAK. Register all three; missing ones are no-ops.
    for sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                signal.signal(sig, _signal_cleanup)
            except (ValueError, OSError):
                # Some signals can only be set on the main thread; fine.
                pass

    print(
        "[preseed] Creating isolated smoke-test datasets: "
        f"{consumer_dataset}, {ops_dataset}, {staging_dataset}"
    )
    try:
        preseed(client, consumer_dataset, ops_dataset, staging_dataset)
        run_phase(
            client,
            consumer_dataset=consumer_dataset,
            ops_dataset=ops_dataset,
            staging_dataset=staging_dataset,
            view_dataset=staging_dataset,
            label="candidate_views",
        )
        run_phase(
            client,
            consumer_dataset=consumer_dataset,
            ops_dataset=ops_dataset,
            staging_dataset=staging_dataset,
            view_dataset=consumer_dataset,
            label="published_views",
        )
    finally:
        _emergency_cleanup()

    print("[OK ] profile_database_views smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
