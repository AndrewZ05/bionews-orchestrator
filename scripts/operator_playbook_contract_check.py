#!/usr/bin/env python3
"""
Static check: ensure the operator guide contains the failure playbook section.

Usage:
    python scripts/operator_playbook_contract_check.py
"""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
OP_GUIDE = REPO_ROOT / "docs" / "PROFILE_DATABASE_OPERATOR_GUIDE.md"


REQUIRED_STRINGS = (
    "## Failure playbook",
    "### Release check fails (local)",
    "### Preflight fails (identity hub not in expected state)",
    "### Refresh scope is unexpectedly large (scope guard trips)",
    "### Refresh produces an empty scope (no-op)",
    "### Schema drift gate fails (profile_core type mismatch)",
    "### Build fails mid-run (SQL step error)",
    "### Publish/promotion issues (rebuild)",
)


def main() -> int:
    text = OP_GUIDE.read_text(encoding="utf-8")
    missing = [s for s in REQUIRED_STRINGS if s not in text]
    if missing:
        print(f"[FAIL] Operator guide missing required playbook sections: {missing}")
        return 1
    print("[OK ] Operator failure playbook present.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
