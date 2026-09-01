"""
Static audit: find every UPDATE/MERGE targeting profile_core or
profile_engagement in the active split enrich/personas modules, and report
whether refresh scope is applied directly on the write or indirectly via a
refresh-scoped TEMP source.

A write is considered refresh-scoped when either:
  - the statement itself contains the canonical predicate:
      EXISTS (SELECT 1 FROM profile_staging.refresh_scope_bn_ids WHERE bn_id = '*')
      OR <alias>.bn_id IN (SELECT bn_id FROM profile_staging.refresh_scope_bn_ids)
  - the statement reads from a TEMP table whose CREATE AS SELECT already
    contains the canonical predicate.

Usage:
    python scripts/scope_predicate_audit.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.profile_database_manifest import BUILD_STEPS

TARGET_SQL_FILES = tuple(
    step.sql_file
    for step in BUILD_STEPS
    if step.name.startswith("enrich_") or step.name.startswith("personas_")
)

WRITE_RE = re.compile(
    r"^(UPDATE|MERGE INTO)\s+profile_data\.(profile_core|profile_engagement)",
    re.MULTILINE,
)
TEMP_TABLE_RE = re.compile(
    r"^(CREATE(?: OR REPLACE)? TEMP TABLE)\s+(_[A-Za-z0-9_]+)",
    re.MULTILINE,
)

SCOPE_SENTINEL_RE = re.compile(
    r"refresh_scope_bn_ids\s+WHERE\s+bn_id\s*=\s*'\*'",
    re.IGNORECASE,
)
SCOPE_IN_RE = re.compile(
    r"bn_id\s+IN\s*\(\s*SELECT\s+bn_id\s+FROM\s+profile_staging\.refresh_scope_bn_ids",
    re.IGNORECASE,
)
GLOBAL_MARKER = "SCOPE: global_reconcile"


def extract_statement(text: str, start_pos: int, search_from: int) -> str:
    tail = text[start_pos:]
    lines = tail.splitlines(keepends=True)
    collected: list[str] = []

    for line in lines:
        collected.append(line)
        code_only = re.sub(r"--.*$", "", line)
        if ";" in code_only:
            break

    return "".join(collected)


def classify_scope(stmt: str) -> tuple[bool, bool]:
    has_sentinel = bool(SCOPE_SENTINEL_RE.search(stmt))
    has_in_filter = bool(SCOPE_IN_RE.search(stmt))
    return has_sentinel, has_in_filter


def get_statement_prefix(text: str, start_pos: int, line_window: int = 3) -> str:
    prefix_lines = text[:start_pos].split("\n")
    return "\n".join(prefix_lines[-line_window:])


def find_scoped_temp_tables(text: str) -> set[str]:
    scoped_temps: set[str] = set()
    changed = True

    # Allow a scoped TEMP to depend on an earlier scoped TEMP in the same file.
    while changed:
        changed = False
        for match in TEMP_TABLE_RE.finditer(text):
            temp_name = match.group(2)
            if temp_name in scoped_temps:
                continue

            stmt = extract_statement(text, match.start(), match.end())
            has_sentinel, has_in_filter = classify_scope(stmt)
            references_scoped_temp = any(
                other != temp_name and re.search(rf"\b{re.escape(other)}\b", stmt)
                for other in scoped_temps
            )

            if (has_sentinel and has_in_filter) or references_scoped_temp:
                scoped_temps.add(temp_name)
                changed = True

    return scoped_temps


def audit_file(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    scoped_temps = find_scoped_temp_tables(text)
    results = []

    for match in WRITE_RE.finditer(text):
        start_pos = match.start()
        stmt = extract_statement(text, start_pos, match.end())
        line_num = text[:start_pos].count("\n") + 1

        has_sentinel, has_in_filter = classify_scope(stmt)
        stmt_temp_refs = sorted(
            temp_name for temp_name in scoped_temps
            if re.search(rf"\b{re.escape(temp_name)}\b", stmt)
        )
        prefix = get_statement_prefix(text, start_pos)

        direct_scoped = has_sentinel and has_in_filter
        temp_scoped = bool(stmt_temp_refs) and not direct_scoped
        global_reconcile = GLOBAL_MARKER in prefix

        summary = next(
            (
                line.strip()
                for line in stmt.split("\n")
                if line.strip() and not line.strip().startswith("--")
            ),
            stmt.strip().split("\n")[0],
        )

        results.append(
            {
                "file": str(path),
                "line": line_num,
                "summary": summary[:120],
                "has_sentinel": has_sentinel,
                "has_in_filter": has_in_filter,
                "direct_scoped": direct_scoped,
                "temp_scoped": temp_scoped,
                "global_reconcile": global_reconcile,
                "scoped_temp_refs": stmt_temp_refs,
            }
        )

    return results


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    target_files = [
        root / sql_file
        for sql_file in TARGET_SQL_FILES
        if (root / sql_file).exists()
    ]

    if not target_files:
        print("[SKIP] No active enrich/personas modules found", file=sys.stderr)
        sys.exit(0)

    all_results: list[dict] = []
    for path in target_files:
        all_results.extend(audit_file(path))

    direct_scoped = [r for r in all_results if r["direct_scoped"]]
    temp_scoped = [r for r in all_results if r["temp_scoped"]]
    global_reconcile = [r for r in all_results if r["global_reconcile"]]
    partial = [
        r
        for r in all_results
        if (r["has_sentinel"] or r["has_in_filter"])
        and not r["direct_scoped"]
        and not r["temp_scoped"]
        and not r["global_reconcile"]
    ]
    unscoped = [
        r
        for r in all_results
        if not r["has_sentinel"]
        and not r["has_in_filter"]
        and not r["temp_scoped"]
        and not r["global_reconcile"]
    ]

    print(f"Total writes to profile_core/profile_engagement: {len(all_results)}")
    print(f"  Directly scoped (statement-local predicate): {len(direct_scoped)}")
    print(f"  Scoped via refresh-scoped TEMP source:       {len(temp_scoped)}")
    print(f"  Explicit global reconciliations:             {len(global_reconcile)}")
    print(f"  Partial (has one but not both):              {len(partial)}")
    print(f"  Unscoped (no scope predicate at all):        {len(unscoped)}")
    print()

    if temp_scoped:
        print("=" * 70)
        print("SCOPED VIA REFRESH-SCOPED TEMP SOURCE")
        print("=" * 70)
        for result in temp_scoped:
            refs = ", ".join(result["scoped_temp_refs"])
            print(f"  {result['file']}:{result['line']}")
            print(f"    {result['summary']}")
            print(f"    scoped temp(s): {refs}")
            print()

    if global_reconcile:
        print("=" * 70)
        print("EXPLICIT GLOBAL RECONCILIATIONS")
        print("=" * 70)
        for result in global_reconcile:
            print(f"  {result['file']}:{result['line']}")
            print(f"    {result['summary']}")
            print()

    if partial:
        print("=" * 70)
        print("PARTIAL SCOPING (needs fixing - has one half of the predicate)")
        print("=" * 70)
        for result in partial:
            missing = []
            if not result["has_sentinel"]:
                missing.append("sentinel EXISTS(*)")
            if not result["has_in_filter"]:
                missing.append("bn_id IN (scope)")
            print(f"  {result['file']}:{result['line']}")
            print(f"    {result['summary']}")
            print(f"    missing: {', '.join(missing)}")
            print()

    if unscoped:
        print("=" * 70)
        print("UNSCOPED (refresh mode will rewrite every row)")
        print("=" * 70)
        for result in unscoped:
            print(f"  {result['file']}:{result['line']}  - {result['summary']}")
        print()
        print(f"{len(unscoped)} unscoped writes found. Each either needs a scope")
        print("predicate OR must be explicitly documented as a global reconciliation step.")
        sys.exit(1)

    if partial:
        print()
        print(f"{len(partial)} partially scoped writes found. Fix the predicate to include")
        print("both the sentinel EXISTS(*) and the bn_id IN (scope) filter (or scope via TEMP).")
        sys.exit(1)


if __name__ == "__main__":
    main()
