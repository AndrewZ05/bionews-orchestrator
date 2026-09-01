#!/usr/bin/env python3
"""
Mark a stuck profile_ops.profile_build_runs row as failed, or list in-flight runs.

Use when preflight fails with no_concurrent_rebuild after a crashed or killed job
that never finalized its build row.

Examples:
  python scripts/clear_profile_build_run.py --env prod --list-running
  python scripts/clear_profile_build_run.py --env prod --build-id 4f38e2da-b5eb-488c-b9b6-d3d9c7d568c0
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import dotenv
from google.cloud import bigquery

REPO_ROOT = Path(__file__).resolve().parent.parent

ENV_TO_PROJECT = {
    "prod": "bi-data-391216",
    "dev": "bi-dev-391216",
    "test": "bi-dev-391216",
}


def _configure_project(env: str) -> str:
    project_id = ENV_TO_PROJECT[env]
    os.environ["GCP_PROJECT_ID"] = project_id
    return project_id


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env",
        required=True,
        choices=tuple(ENV_TO_PROJECT),
        help="Target environment (maps to GCP project)",
    )
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument(
        "--list-running",
        action="store_true",
        help="Print running builds (no updates)",
    )
    g.add_argument(
        "--build-id",
        type=str,
        metavar="UUID",
        help="Mark this build_id failed if it is still status=running",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="With --build-id, show the row but do not update",
    )
    args = parser.parse_args()

    sys.path.insert(0, str(REPO_ROOT))
    dotenv.load_dotenv(REPO_ROOT / ".env")

    project_id = _configure_project(args.env)

    from shared.cli_utils import get_bigquery_client

    client = get_bigquery_client()

    if args.list_running:
        sql = """
        SELECT
          build_id,
          mode,
          status,
          started_at,
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), started_at, MINUTE) AS age_minutes
        FROM profile_ops.profile_build_runs
        WHERE status = 'running'
        ORDER BY started_at DESC
        LIMIT 50
        """
        rows = list(client.query(sql).result())
        print(f"project={project_id} running rows: {len(rows)}")
        for r in rows:
            print(
                f"  {r.build_id}  mode={r.mode}  age_minutes={r.age_minutes}  "
                f"started_at={r.started_at}"
            )
        return 0

    build_id = args.build_id.strip()
    sel = """
    SELECT build_id, mode, status, started_at, error_message
    FROM profile_ops.profile_build_runs
    WHERE build_id = @build_id
    LIMIT 1
    """
    cfg = bigquery.QueryJobConfig(
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("build_id", "STRING", build_id),
        ],
    )
    found = list(client.query(sel, job_config=cfg).result())
    if not found:
        print(f"No row for build_id={build_id}", file=sys.stderr)
        return 1
    row = found[0]
    print(
        f"project={project_id} build_id={row.build_id} status={row.status} "
        f"mode={row.mode} started_at={row.started_at}"
    )
    if row.status != "running":
        print("Row is not running; nothing to clear.", file=sys.stderr)
        return 1
    if args.dry_run:
        print("dry-run: no UPDATE executed")
        return 0

    msg = "Operator cleared stuck running row via scripts/clear_profile_build_run.py"
    upd = """
    UPDATE profile_ops.profile_build_runs
    SET status = 'failed',
        completed_at = CURRENT_TIMESTAMP(),
        error_message = @msg
    WHERE build_id = @build_id
      AND status = 'running'
    """
    cfg2 = bigquery.QueryJobConfig(
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("build_id", "STRING", build_id),
            bigquery.ScalarQueryParameter("msg", "STRING", msg),
        ],
    )
    job = client.query(upd, job_config=cfg2)
    job.result()
    n = getattr(job, "num_dml_affected_rows", None)
    print(f"UPDATE complete (rows affected: {n})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
