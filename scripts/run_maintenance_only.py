"""Run ONLY the profile_database `maintenance` step.

WHY THIS EXISTS
orchestrate.py has no --build-mode that runs maintenance on its own: it appears
in `rebuild` and `refresh` only (not even `reenrich`). A reference-data change --
a new site, a corrected condition mapping, a site rollup -- therefore costs a
full ~25-minute refresh even though the actual work is seeding a handful of
lookup tables.

WHAT IT TOUCHES
  profile_data.profile_lookup     deleted and reseeded (all lookup types)
  profile_data.conditions_dict    CREATE OR REPLACE
  profile_data.symptoms_dict      CREATE OR REPLACE
  profile_data.treatments_dict    CREATE OR REPLACE
  profile_data.subtypes_dict      CREATE OR REPLACE
  profile_data.dictionary_meta    CREATE OR REPLACE
  profile_data.profile_engagement two forward-only MERGEs (Mailchimp status)
  profile_data.profile_core       two forward-only MERGEs (last_active_at, NPI)

WHY IT IS SAFE STANDALONE
The step reads no refresh-scope staging table -- verified: the only mention of
refresh_scope_bn_ids in the SQL is a comment stating a merge is DELIBERATELY
global. Its two profile_core merges read external sources (Mailchimp member
status, the NPI registry), not anything the refresh pipeline produces earlier in
the run. So it does not depend on refresh_scope / refresh / reconcile having run
first.

WHAT IT DOES NOT DO
Nothing downstream of maintenance. If a reference change should propagate into
profiles -- a new site_condition_mapping row filling somebody's condition, or a
new site_registry row letting fill_gaps_site_domain resolve traffic -- those are
separate steps and still need a refresh. This script updates the vocabulary, not
the profiles that consume it.

USAGE
    python scripts/run_maintenance_only.py            # execute
    python scripts/run_maintenance_only.py --dry-run  # validate only, no writes
"""

import argparse
import os
import sys
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# Authenticate the way every other pipeline entry point does (cf.
# orchestrate.py). Without this the script inherits whatever ambient ADC the
# shell carries -- typically an end-user gcloud login with no
# bigquery.jobs.create on the project.
import dotenv  # noqa: E402

dotenv.load_dotenv(REPO_ROOT / ".env")

from google.cloud import bigquery  # noqa: E402

from shared.post_processor import _split_sql_statements  # noqa: E402

SQL_PATH = REPO_ROOT / "sql" / "profile_database_maintenance.sql"
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "bi-data-391216")


def _statements() -> list[str]:
    """Executable statements, with {build_id} substituted."""
    build_id = f"maintenance_only_{uuid.uuid4().hex[:12]}"
    sql = SQL_PATH.read_text(encoding="utf-8").replace("{build_id}", build_id)
    return [
        s
        for s in _split_sql_statements(sql)
        if not all(
            line.strip().startswith("--") or not line.strip() for line in s.split("\n")
        )
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Validate statements against BigQuery without writing. NOTE: a "
            "dry run does not APPLY the ALTER statements, so it stops at the "
            "first INSERT naming a column this file is about to add. Use "
            "scripts/dry_run_profile_sql.py for whole-file validation -- it "
            "builds stub tables from the new DDL first."
        ),
    )
    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT_ID)
    statements = _statements()

    mode = "DRY RUN" if args.dry_run else "EXECUTING"
    print(f"{mode}: {SQL_PATH.relative_to(REPO_ROOT)} ({len(statements)} statements)")
    print(f"  project: {PROJECT_ID}")
    if not args.dry_run:
        print("  NOTE: profile_lookup is deleted and reseeded; the dictionaries are")
        print("        CREATE OR REPLACE'd. Reference data only -- profiles are not")
        print("        rebuilt. Run a refresh to propagate changes into profiles.")
    print()

    # The file uses CREATE OR REPLACE TEMP TABLE (_site_group). A temp table
    # lives in a SESSION, so every statement has to run inside the same one --
    # submitted as independent jobs, BigQuery rejects it with "Use of CREATE
    # TEMPORARY TABLE requires a script or session". shared/post_processor.py
    # solves this by creating the session on the first statement and threading
    # its id through the rest; mirrored here.
    uses_temp_tables = any("TEMP TABLE" in s.upper() for s in statements)
    session_id: str | None = None

    failures = 0
    for i, sql in enumerate(statements):
        preview = next(
            (
                line.strip()
                for line in sql.split("\n")
                if line.strip() and not line.strip().startswith("--")
            ),
            "",
        )[:88]
        try:
            job_config = bigquery.QueryJobConfig(dry_run=args.dry_run)
            if uses_temp_tables and not args.dry_run:
                if session_id is None:
                    job_config.create_session = True
                else:
                    job_config.connection_properties = [
                        bigquery.query.ConnectionProperty(
                            key="session_id", value=session_id
                        )
                    ]

            job = client.query(sql, job_config=job_config)
            if not args.dry_run:
                job.result()
                if uses_temp_tables and session_id is None:
                    session_id = job.session_info.session_id
                affected = job.num_dml_affected_rows
                suffix = f"  ({affected:,} rows)" if affected is not None else ""
            else:
                suffix = ""
            print(f"  [{i:>2}] OK   {preview}{suffix}")
        except Exception as exc:  # noqa: BLE001 - report and stop
            message = str(exc)
            # A dry run validates every statement against the CURRENT schema and
            # never applies the ALTERs above, so an INSERT naming a
            # not-yet-added column is expected here and says nothing about the
            # SQL. Real execution applies them in order and is unaffected.
            if args.dry_run and "is not present in table" in message:
                print(f"  [{i:>2}] SKIP {preview}")
                print(
                    "       references a column the ALTERs above add; a dry run "
                    "cannot see it."
                )
                print()
                print(
                    "Dry run stops here by design. Statements 0-"
                    f"{i - 1} validated. For whole-file validation run:"
                )
                print(
                    "  python scripts/dry_run_profile_sql.py profile_database_maintenance"
                )
                return 0
            failures += 1
            print(f"  [{i:>2}] FAIL {preview}")
            print(f"       {message[:220]}")
            # A failed statement usually invalidates what follows (a dropped
            # column, an unseeded table), so stop rather than cascade.
            break

    print()
    if failures:
        print(f"FAILED after {failures} error(s). Nothing further was run.")
        return 1
    print(
        f"{'Validated' if args.dry_run else 'Completed'}: {len(statements)} statements."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
