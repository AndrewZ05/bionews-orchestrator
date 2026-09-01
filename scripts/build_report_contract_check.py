#!/usr/bin/env python3
"""
Static check: ensure docs/PROFILE_DATABASE_BUILD_REPORT.md runtime-shape counts
match the live runtime contract (shared/profile_database_manifest.py).

Usage:
    python scripts/build_report_contract_check.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD_REPORT = REPO_ROOT / "docs" / "PROFILE_DATABASE_BUILD_REPORT.md"

sys.path.insert(0, str(REPO_ROOT))
from shared.profile_database_manifest import (  # noqa: E402
    OPS_TABLE_GROUPS,
    PHYSICAL_TABLE_GROUPS,
    STAGING_TABLE_GROUPS,
    VIEW_GROUPS,
)


def count_grouped(groups: tuple[tuple[str, tuple[str, ...]], ...]) -> int:
    return len({item for _, items in groups for item in items})


def parse_metric(text: str, label: str) -> int:
    m = re.search(rf"\|\s*{re.escape(label)}\s*\|\s*(\d+)\s*\|", text)
    if not m:
        raise ValueError(f"Could not find metric row for: {label}")
    return int(m.group(1))


def main() -> int:
    text = BUILD_REPORT.read_text(encoding="utf-8")

    expected_consumer = count_grouped(PHYSICAL_TABLE_GROUPS)
    expected_ops = count_grouped(OPS_TABLE_GROUPS)
    expected_staging = count_grouped(STAGING_TABLE_GROUPS)
    expected_views = count_grouped(VIEW_GROUPS)

    got_consumer = parse_metric(text, "Consumer/reference tables (`profile_data`)")
    got_ops = parse_metric(text, "Ops/history tables (`profile_ops`)")
    got_staging = parse_metric(text, "Staging helpers (`profile_staging`)")
    got_views = parse_metric(text, "Views")

    mismatches = []
    if got_consumer != expected_consumer:
        mismatches.append(("consumer", got_consumer, expected_consumer))
    if got_ops != expected_ops:
        mismatches.append(("ops", got_ops, expected_ops))
    if got_staging != expected_staging:
        mismatches.append(("staging", got_staging, expected_staging))
    if got_views != expected_views:
        mismatches.append(("views", got_views, expected_views))

    if mismatches:
        print("[FAIL] Build report runtime-shape counts are stale:")
        for name, got, exp in mismatches:
            print(f"  {name}: report={got} expected={exp}")
        return 1

    print(
        f"[OK ] Build report counts match contract: "
        f"consumer={expected_consumer}, ops={expected_ops}, staging={expected_staging}, views={expected_views}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
