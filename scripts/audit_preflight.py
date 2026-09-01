#!/usr/bin/env python3
"""Audit preflight lint: catch config problems at CHANGE time, not run time.

Validates the audit configuration against live BigQuery WITHOUT running an
audit, so a bad YAML edit surfaces the day it is made:

  1. every source loads and its audit: blocks parse (a malformed override --
     e.g. cadence without reason -- fails the source load);
  2. every active audited resource's TABLE exists in its production dataset;
  3. every resolved DATE COLUMN exists on its table;
  4. every declared PRIMARY KEY column exists on its table;
  5. YAML schema vs live columns drift summary (informational).

One INFORMATION_SCHEMA query per dataset (reuses the anomaly audit's
fetch_live_columns). Exit code 0 = clean, 1 = problems found -- suitable for CI
or a pre-commit habit:

    python scripts/audit_preflight.py
    python scripts/audit_preflight.py --sources facebook mailchimp

ASCII-only output.
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from shared.anomaly_audit.schema_detector import detect_schema_drift, fetch_live_columns
from shared.bigquery_client import get_bigquery_client
from shared.config_loader import load_config
from shared.freshness_audit.yaml_target_loader import build_targets_for_source

logger = logging.getLogger(__name__)

DEFAULT_SOURCES = [
    "facebook",
    "wordpress",
    "mailchimp",
    "instagram",
    "limesurvey",
    "identity_hub",
]


def main(argv=None) -> int:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Validate audit configuration against live BigQuery."
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        default=None,
        help="Sources to lint (default: the anomaly_audit config's sources list).",
    )
    args = parser.parse_args(argv)

    sources = args.sources
    if not sources:
        try:
            cfg = load_config("anomaly_audit")
            sources = (cfg.get("audit") or {}).get("sources") or DEFAULT_SOURCES
        except Exception:
            sources = DEFAULT_SOURCES

    client = get_bigquery_client()
    problems = []
    infos = []

    # 1. Build targets per source (a malformed audit: block raises here).
    targets = []
    for source in sources:
        try:
            targets.extend(build_targets_for_source(source, client.project))
        except Exception as exc:  # noqa: BLE001 - reported, not fatal to the lint
            problems.append("[config] source {0}: {1}".format(source, exc))

    # 2. One live-columns fetch per dataset.
    live_by_dataset = {}
    for ds in sorted({t["dataset_name"] for t in targets}):
        try:
            live_by_dataset[ds] = fetch_live_columns(client, client.project, ds)
        except Exception as exc:  # noqa: BLE001
            problems.append("[dataset] {0}: cannot list columns ({1})".format(ds, exc))
            live_by_dataset[ds] = None

    # 3-5. Per-target checks.
    for t in targets:
        label = "{0}.{1}".format(t["dataset_name"], t["table_name"])
        live = live_by_dataset.get(t["dataset_name"])
        if live is None:
            continue
        columns = live.get(t["table_name"])
        if columns is None:
            problems.append(
                "[missing-table] {0} is audited (active) but absent in BigQuery "
                "-- mark audit: false or fix the resource".format(label)
            )
            continue
        if t["date_column"] not in columns:
            problems.append(
                "[date-column] {0}: resolved column '{1}' does not exist".format(
                    label, t["date_column"]
                )
            )
        for pk in t.get("primary_key") or []:
            if pk not in columns:
                problems.append(
                    "[primary-key] {0}: pk column '{1}' does not exist".format(
                        label, pk
                    )
                )
        for finding in detect_schema_drift(t, columns, today=None):
            infos.append(
                "[schema-drift] {0}: {1}".format(label, finding["error_message"])
            )

    print(
        "Audit preflight: {0} target(s) across {1} source(s)".format(
            len(targets), len(sources)
        )
    )
    for line in problems:
        print("PROBLEM  " + line)
    for line in infos:
        print("info     " + line)
    print(
        "RESULT: {0} problem(s), {1} schema-drift note(s)".format(
            len(problems), len(infos)
        )
    )
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
