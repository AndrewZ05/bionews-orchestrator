#!/usr/bin/env python3
"""
Static check: ensure lineage coverage guardrails are present in generated docs.

This prevents subtle drift where profile_field_changes is still narrow, but the
operator docs stop making that explicit.

Usage:
    python scripts/lineage_coverage_contract_check.py
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
OP_GUIDE = REPO_ROOT / "docs" / "PROFILE_DATABASE_OPERATOR_GUIDE.md"

sys.path.insert(0, str(REPO_ROOT))
from shared.profile_database_manifest import FIELD_CHANGES_TRACKED_FIELDS  # noqa: E402


def main() -> int:
    text = OP_GUIDE.read_text(encoding="utf-8")

    required_header = "#### Lineage coverage (`profile_field_changes`)"
    if required_header not in text:
        print(f"[FAIL] Missing required operator-guide section: {required_header}")
        return 1

    missing = [f for f in FIELD_CHANGES_TRACKED_FIELDS if f"`{f}`" not in text]
    if missing:
        print(f"[FAIL] Operator guide lineage section missing fields: {missing}")
        return 1

    print(f"[OK ] Operator guide lineage coverage section present for: {list(FIELD_CHANGES_TRACKED_FIELDS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
