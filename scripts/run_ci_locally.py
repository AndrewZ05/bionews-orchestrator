#!/usr/bin/env python3
"""Run every step of .github/workflows/ci.yml locally, in order.

WHY THIS EXISTS
---------------
CI was red for 40+ consecutive runs while `pytest tests/unit` passed locally.
The failing step was `profile_release_check.py --skip-bq`, which nobody was
running outside CI. Passing tests is not the same as passing CI.

Run this before pushing. It stops at the first failure, exactly as CI does
(the workflow uses --maxfail=1 on the unit tests).

Usage:
    python scripts/run_ci_locally.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Mirrors the `test` job in .github/workflows/ci.yml. Keep in sync: if a step is
# added there and not here, this script goes back to giving false confidence.
STEPS: list[tuple[str, list[str]]] = [
    ("syntax tests", [sys.executable, "run_tests.py", "--category", "syntax"]),
    (
        "unit tests",
        [sys.executable, "-m", "pytest", "tests/unit", "--maxfail=1", "--disable-warnings", "-q"],
    ),
    (
        "profile DB release checks (no BigQuery)",
        [sys.executable, "scripts/profile_release_check.py", "--skip-bq"],
    ),
]


def main() -> int:
    failures = []
    for name, cmd in STEPS:
        print(f"\n=== {name} ===", flush=True)
        completed = subprocess.run(cmd, cwd=REPO_ROOT)
        if completed.returncode == 0:
            print(f"[OK  ] {name}")
        else:
            print(f"[FAIL] {name} (exit {completed.returncode})")
            failures.append(name)
            break  # CI stops here too

    print()
    if failures:
        print(f"CI WOULD FAIL: {failures[0]}")
        return 1
    print(f"All {len(STEPS)} CI steps passed. Safe to push.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
