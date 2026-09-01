#!/usr/bin/env python3
"""
Static check: ensure publish-manifest telemetry is part of the runtime contract.

The rebuild publish flow now emits per-table telemetry into
profile_ops.profile_publish_manifest. This script enforces that the table is:
  1) declared in the shared manifest (so docs/tooling know it exists)
  2) created in the DDL (so first builds can bootstrap it)

Usage:
    python scripts/publish_manifest_contract_check.py
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DDL = REPO_ROOT / "sql" / "profile_database_ddl.sql"

sys.path.insert(0, str(REPO_ROOT))
from shared.profile_database_manifest import OPS_TABLE_GROUPS  # noqa: E402


def main() -> int:
    ops_tables = [t for _, tables in OPS_TABLE_GROUPS for t in tables]
    if "profile_publish_manifest" not in ops_tables:
        print("[FAIL] shared/profile_database_manifest.py is missing profile_publish_manifest in OPS_TABLE_GROUPS.")
        return 1

    ddl_text = DDL.read_text(encoding="utf-8")
    if "CREATE TABLE IF NOT EXISTS profile_ops.profile_publish_manifest" not in ddl_text:
        print("[FAIL] sql/profile_database_ddl.sql is missing CREATE TABLE for profile_ops.profile_publish_manifest.")
        return 1

    print("[OK ] publish manifest table is present in manifest + DDL.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
