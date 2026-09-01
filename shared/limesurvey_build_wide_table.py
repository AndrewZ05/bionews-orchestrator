#!/usr/bin/env python3
"""
Build lime_surveys_wide table from lime_surveys_columnar_completed.

Creates one column per standardized question. Multiple answers (from multi-select
or matrix subquestions) are combined with pipe delimiter:
    symptom_qol_impact = "Difficulty swallowing | Fatigue | Changes in mobility"
"""

import os
import re
import sys
import threading
from pathlib import Path

# Suppress gRPC/abseil ALTS warnings
os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "3"

sys.stdout.reconfigure(encoding="utf-8")


class _StderrFdFilter:
    """Filter gRPC ALTS warnings from stderr at OS file descriptor level."""

    _installed = False
    _filter_patterns = [b"ALTS", b"absl::InitializeLog", b"alts_credentials"]
    _original_stderr_fd = None
    _read_pipe = None

    @classmethod
    def install(cls):
        if cls._installed:
            return
        try:
            cls._original_stderr_fd = os.dup(2)
            cls._read_pipe, write_pipe = os.pipe()
            os.dup2(write_pipe, 2)
            os.close(write_pipe)
            threading.Thread(target=cls._filter_loop, daemon=True).start()
            cls._installed = True
        except Exception:
            if cls._original_stderr_fd:
                os.dup2(cls._original_stderr_fd, 2)

    @classmethod
    def _filter_loop(cls):
        buffer = b""
        while True:
            try:
                chunk = os.read(cls._read_pipe, 4096)
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if not any(p in line for p in cls._filter_patterns):
                        os.write(cls._original_stderr_fd, line + b"\n")
            except OSError:
                break


_StderrFdFilter.install()

from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)


def sanitize_column_name(name: str) -> str:
    """Sanitize a name for use as a BigQuery column name."""
    if not name:
        return "unknown_column"

    # Replace spaces and special chars with underscores
    col = re.sub(r"[^a-zA-Z0-9_]", "_", name)

    # Collapse multiple underscores
    col = re.sub(r"_+", "_", col)

    # Remove leading/trailing underscores
    col = col.strip("_")

    # Ensure starts with letter or underscore
    if col and col[0].isdigit():
        col = "_" + col

    # Truncate to 128 chars
    col = col[:128]

    return col.lower() if col else "unknown_column"


def get_all_questions(client: bigquery.Client) -> list[str]:
    """Get all distinct standardized questions."""
    query = """
    SELECT DISTINCT standardized_question
    FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
    WHERE standardized_question IS NOT NULL
    ORDER BY standardized_question
    """

    questions = [row.standardized_question for row in client.query(query)]
    print(f"Found {len(questions)} unique standardized questions")
    return questions


def build_wide_table_sql(questions: list[str]) -> str:
    """
    Generate the SQL to create the wide table.

    Each question becomes one column. Multiple answers are pipe-delimited.
    """
    # Metadata columns that form the natural key (GROUP BY these)
    group_cols = [
        "survey_id",
        "response_id",
        "submitdate",
        "lastpage",
        "startdate",
        "datestamp",
        "ipaddr",
        "refurl",
        "source",
        "survey_gsid",
        "survey_owner_id",
        "survey_admin",
        "survey_language",
        "survey_title",
        "site",
    ]
    # Columns that vary across executions — aggregate with MAX() to deduplicate
    agg_cols = ["execution_id", "extracted_at"]
    metadata_cols = group_cols + agg_cols

    reserved_names = set(c.lower() for c in metadata_cols)
    used_col_names = set()

    def get_unique_col_name(base_name: str) -> str:
        """Ensure column name is unique and not reserved."""
        col_name = base_name
        if col_name in reserved_names:
            col_name = f"q_{col_name}"

        original = col_name
        suffix = 1
        while col_name in used_col_names:
            col_name = f"{original}_{suffix}"
            suffix += 1
        used_col_names.add(col_name)
        return col_name

    pivot_cols = []

    # Each question becomes one column with pipe-delimited answers
    for q in questions:
        col_name = get_unique_col_name(sanitize_column_name(q))
        q_escaped = q.replace("'", "\\'")
        pivot_cols.append(
            f"    MAX(IF(standardized_question = '{q_escaped}', answer, NULL)) as {col_name}"
        )

    pivot_sql = ",\n".join(pivot_cols)
    group_list = ", ".join([f"m.{c}" for c in group_cols])
    group_select = ",\n    ".join([f"m.{c}" for c in group_cols])
    agg_select = ",\n    ".join([f"MAX(m.{c}) AS {c}" for c in agg_cols])
    metadata_select = f"{group_select},\n    {agg_select}"

    sql = f"""CREATE OR REPLACE TABLE `bi-data-391216.limesurvey_data.lime_surveys_wide`
PARTITION BY DATE(submitdate)
CLUSTER BY survey_id
AS
WITH aggregated AS (
    -- Aggregate all answers per question with pipe delimiter
    -- This combines multiple selections AND matrix subquestion answers
    SELECT
        survey_id,
        response_id,
        standardized_question,
        STRING_AGG(DISTINCT standardized_answer, ' | ' ORDER BY standardized_answer) as answer
    FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
    WHERE standardized_question IS NOT NULL
      AND standardized_answer IS NOT NULL
    GROUP BY survey_id, response_id, standardized_question
),
metadata AS (
    SELECT
        c.survey_id, c.response_id, c.submitdate, c.lastpage, c.startdate,
        c.datestamp, c.ipaddr, c.refurl, c.source,
        c.survey_gsid, c.survey_owner_id, c.survey_admin, c.survey_language,
        ls.surveyls_title AS survey_title,
        c.site,
        MAX(c.execution_id) AS execution_id,
        MAX(c.extracted_at) AS extracted_at
    FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed` c
    LEFT JOIN `bi-data-391216.limesurvey_data.lime_surveys_languagesettings` ls
        ON c.survey_id = ls.surveyls_survey_id
        AND ls.surveyls_language = 'en'
    GROUP BY c.survey_id, c.response_id, c.submitdate, c.lastpage, c.startdate,
        c.datestamp, c.ipaddr, c.refurl, c.source,
        c.survey_gsid, c.survey_owner_id, c.survey_admin, c.survey_language,
        ls.surveyls_title, c.site
)
SELECT
    {metadata_select},
{pivot_sql}
FROM metadata m
LEFT JOIN aggregated a ON m.survey_id = a.survey_id AND m.response_id = a.response_id
GROUP BY {group_list}"""

    return sql


def verify_table(client: bigquery.Client) -> None:
    """Run verification queries after table creation."""
    print("\n" + "=" * 80)
    print("VERIFICATION")
    print("=" * 80)

    # Row count
    wide_count = list(
        client.query(
            "SELECT COUNT(*) as cnt FROM `bi-data-391216.limesurvey_data.lime_surveys_wide`"
        )
    )[0].cnt

    columnar_count = list(
        client.query(
            """
        SELECT COUNT(DISTINCT CONCAT(CAST(survey_id AS STRING), '-', response_id)) as cnt
        FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
    """
        )
    )[0].cnt

    print(f"\nRow counts:")
    print(f"  lime_surveys_wide: {wide_count:,}")
    print(f"  Unique responses in source: {columnar_count:,}")
    print(f'  Match: {"YES" if wide_count == columnar_count else "NO"}')

    if wide_count != columnar_count:
        # Diagnose: check if duplicates come from metadata variance (execution_id, extracted_at)
        dup_query = """
            SELECT survey_id, response_id, COUNT(*) as dupes
            FROM `bi-data-391216.limesurvey_data.lime_surveys_wide`
            GROUP BY survey_id, response_id
            HAVING COUNT(*) > 1
            ORDER BY dupes DESC
            LIMIT 10
        """
        dup_rows = list(client.query(dup_query))
        if dup_rows:
            total_dups = list(
                client.query(
                    """
                SELECT COUNT(*) as cnt FROM (
                    SELECT survey_id, response_id
                    FROM `bi-data-391216.limesurvey_data.lime_surveys_wide`
                    GROUP BY survey_id, response_id
                    HAVING COUNT(*) > 1
                )
            """
                )
            )[0].cnt
            print(
                f"\n  MISMATCH DIAGNOSIS: {total_dups} (survey_id, response_id) pairs have duplicate rows"
            )
            print(f"  Top duplicates:")
            for r in dup_rows[:5]:
                print(
                    f"    survey_id={r.survey_id}, response_id={r.response_id}: {r.dupes} rows"
                )
            # Check if it's from execution_id variance
            sample = dup_rows[0]
            exec_query = f"""
                SELECT DISTINCT execution_id, extracted_at
                FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
                WHERE survey_id = {sample.survey_id} AND response_id = '{sample.response_id}'
            """
            exec_rows = list(client.query(exec_query))
            if len(exec_rows) > 1:
                print(
                    f"  Cause: multiple execution_ids per response (pre-existing, not from survey_title JOIN)"
                )
                for er in exec_rows:
                    print(
                        f"    execution_id={er.execution_id}, extracted_at={er.extracted_at}"
                    )
            else:
                print(f"  Cause: not from execution_id variance - investigate further")
        else:
            print(
                f"  No duplicate (survey_id, response_id) pairs found - mismatch may be from NULL response_ids"
            )

    # Column count
    col_count = list(
        client.query(
            """
        SELECT COUNT(*) as col_count
        FROM `bi-data-391216.limesurvey_data.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'lime_surveys_wide'
    """
        )
    )[0].col_count

    print(f"\nColumn count: {col_count}")
    print(f"  (17 metadata + {col_count - 17} question columns)")

    # Sample data for a question column showing pipe-delimited format
    print("\nSample pipe-delimited answers:")
    try:
        # Dynamically find a question column that has pipe-delimited values
        sample_col_query = """
            SELECT column_name
            FROM `bi-data-391216.limesurvey_data.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'lime_surveys_wide'
              AND column_name NOT IN (
                'survey_id', 'response_id', 'submitdate', 'lastpage', 'startdate',
                'datestamp', 'ipaddr', 'refurl', 'extracted_at', 'source',
                'execution_id', 'survey_gsid', 'survey_owner_id', 'survey_admin',
                'survey_language', 'survey_title'
              )
            LIMIT 1
        """
        col_row = list(client.query(sample_col_query))
        if col_row:
            col_name = col_row[0].column_name
            sample_query = f"""
                SELECT response_id, `{col_name}` AS answer
                FROM `bi-data-391216.limesurvey_data.lime_surveys_wide`
                WHERE `{col_name}` IS NOT NULL
                LIMIT 5
            """
            print(f"  Column: {col_name}")
            for row in client.query(sample_query):
                print(f"    Response {row.response_id}: {row.answer}")
    except Exception as e:
        print(f"  Error sampling: {e}")

    # Show question type distribution
    print("\nQuestion type distribution:")
    type_query = """
        SELECT
            COALESCE(question_type, 'unknown') as qtype,
            COUNT(DISTINCT standardized_question) as cnt
        FROM `bi-data-391216.limesurvey_data.lime_surveys_columnar_completed`
        WHERE standardized_question IS NOT NULL
        GROUP BY 1
        ORDER BY cnt DESC
    """
    for row in client.query(type_query):
        print(f"  {row.qtype}: {row.cnt} questions")


def main():
    client = bigquery.Client(project="bi-data-391216")

    print("=" * 80)
    print("WIDE TABLE BUILDER - SINGLE COLUMN FORMAT")
    print("=" * 80)
    print("Each question = one column. Multiple answers are pipe-delimited.")

    # Step 1: Get all questions
    questions = get_all_questions(client)

    # Step 2: Generate and execute SQL
    print("\n" + "=" * 80)
    print("CREATING WIDE TABLE")
    print("=" * 80)

    sql = build_wide_table_sql(questions)

    print(f"\nColumns to create:")
    print(f"  17 metadata columns")
    print(f"  {len(questions)} question columns")
    print(f"  Total: {17 + len(questions)} columns")

    # Save SQL for reference
    sql_path = "pipelines/cde/generated_wide_table.sql"
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write(sql)
    print(f"\nGenerated SQL saved to: {sql_path}")
    print(f"SQL length: {len(sql):,} characters")

    print("\nExecuting SQL (this may take a minute)...")
    job = client.query(sql)
    job.result()
    print("Table created successfully!")

    # Step 3: Verify
    verify_table(client)

    print("\n" + "=" * 80)
    print("DONE")
    print("=" * 80)
    print("\nTable: bi-data-391216.limesurvey_data.lime_surveys_wide")
    print(
        'Matrix questions show pipe-delimited answers (e.g., "Fatigue | Changes in mobility")'
    )


if __name__ == "__main__":
    main()
