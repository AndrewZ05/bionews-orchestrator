#!/usr/bin/env python3
"""
Static check: BUILD_MODES step names resolve to BUILD_STEPS, and orchestration-only
modes (e.g. resume_publish) stay explicitly empty in the manifest.

Usage:
    python scripts/build_modes_contract_check.py
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from shared.profile_database_manifest import (  # noqa: E402
    BUILD_MODES,
    BUILD_STEP_MAP,
    LEGACY_MODE_ALIASES,
)


def main() -> int:
    if "resume_publish" not in BUILD_MODES:
        print("[FAIL] BUILD_MODES is missing resume_publish.")
        return 1
    if BUILD_MODES["resume_publish"] != ():
        print("[FAIL] resume_publish must have an empty SQL step tuple (orchestration-only).")
        return 1

    for mode, steps in BUILD_MODES.items():
        unknown = [s for s in steps if s not in BUILD_STEP_MAP]
        if unknown:
            print(f"[FAIL] Mode {mode!r} references unknown step(s): {unknown}")
            return 1

    for alias, resolved in LEGACY_MODE_ALIASES.items():
        if resolved not in BUILD_MODES:
            print(
                f"[FAIL] LEGACY_MODE_ALIASES[{alias!r}] -> {resolved!r} but {resolved!r} "
                f"not in BUILD_MODES."
            )
            return 1

    print("[OK ] BUILD_MODES / BUILD_STEPS / legacy alias contract is consistent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
