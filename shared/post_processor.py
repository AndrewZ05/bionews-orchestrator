# shared/post_processor.py
"""
Post-processing execution for orchestrator pipelines.
Runs SQL files or Python scripts after table/group/pipeline completion.
"""

import logging
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def _rewrite_internal_datasets(
    sql: str,
    consumer_dataset: str | None = None,
    ops_dataset: str | None = None,
    staging_dataset: str | None = None,
    identity_xref_table: str | None = None,
    identity_hub_table: str | None = None,
    identity_persistence_table: str | None = None,
) -> str:
    """Rewrite internal dataset references for blue/green execution contexts."""
    table_rewrites = (
        ("identity_hub_data.bn_id_xref", identity_xref_table),
        ("identity_hub_data.bn_id_hub", identity_hub_table),
        ("identity_hub_data.bn_id_persistence", identity_persistence_table),
    )
    for original, replacement in table_rewrites:
        if replacement and replacement != original:
            sql = re.sub(
                rf"(?<![\w`]){re.escape(original)}(?![\w`])",
                replacement,
                sql,
            )

    rewrites = (
        ("profile_data", consumer_dataset),
        ("profile_ops", ops_dataset),
        ("profile_staging", staging_dataset),
    )
    for original, replacement in rewrites:
        if replacement and replacement != original:
            sql = re.sub(
                rf"(?<![\w`]){re.escape(original)}\.",
                f"{replacement}.",
                sql,
            )
    return sql


def _split_sql_statements(sql: str) -> List[str]:
    """
    Split SQL into statements by semicolons, but ignore semicolons inside
    string literals, comments, and BEGIN...END scripting blocks (BigQuery
    scripting).

    This supports two patterns in post-process SQL files:

    1. Multiple independent statements bundled in one file:
           CREATE OR REPLACE TABLE foo AS SELECT ...;
           CREATE OR REPLACE TABLE bar AS SELECT ...;
       Each is executed sequentially as its own query job.

    2. BigQuery scripting blocks:
           BEGIN
             DECLARE x INT64 DEFAULT 0;
             SET x = x + 1;
             EXECUTE IMMEDIATE ...;
           END;
       The whole block is kept together and submitted as one script job;
       inner semicolons do not split the block.
    """
    statements = []
    current = []
    i = 0
    n = len(sql)
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    begin_depth = 0  # tracks nesting of BEGIN...END scripting blocks

    # Helper: peek at an uppercase keyword starting at position j
    def peek_keyword(j):
        if j >= n or not (sql[j].isalpha() or sql[j] == "_"):
            return ""
        end = j
        while end < n and (sql[end].isalpha() or sql[end] == "_"):
            end += 1
        # only a keyword if followed by whitespace, semicolon, or end-of-string
        if end < n and sql[end] not in (" ", "\t", "\n", "\r", ";", "-", "/"):
            return ""
        return sql[j:end].upper()

    while i < n:
        c = sql[i]

        if in_line_comment:
            current.append(c)
            if c == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            current.append(c)
            if c == "*" and i + 1 < n and sql[i + 1] == "/":
                current.append("/")
                i += 2
                in_block_comment = False
            else:
                i += 1
            continue

        if in_single:
            current.append(c)
            if c == "'" and (i + 1 >= n or sql[i + 1] != "'"):
                in_single = False
            elif c == "'" and i + 1 < n and sql[i + 1] == "'":
                current.append("'")
                i += 2
                continue
            i += 1
            continue

        if in_double:
            current.append(c)
            if c == '"' and (i + 1 >= n or sql[i + 1] != '"'):
                in_double = False
            elif c == '"' and i + 1 < n and sql[i + 1] == '"':
                current.append('"')
                i += 2
                continue
            i += 1
            continue

        # Not in string or comment — check for keywords and punctuation
        if c == "-" and i + 1 < n and sql[i + 1] == "-":
            current.append(c)
            current.append(sql[i + 1])
            i += 2
            in_line_comment = True
            continue

        if c == "/" and i + 1 < n and sql[i + 1] == "*":
            current.append(c)
            current.append(sql[i + 1])
            i += 2
            in_block_comment = True
            continue

        if c == "'":
            current.append(c)
            in_single = True
            i += 1
            continue

        if c == '"':
            current.append(c)
            in_double = True
            i += 1
            continue

        # Track BEGIN...END depth so inner semicolons are not split points.
        # Only treat BEGIN/END as keywords when at word boundary (preceded by
        # whitespace/newline or start of current statement).
        if c in ("B", "b", "E", "e") and (
            i == 0 or sql[i - 1] in (" ", "\t", "\n", "\r", ";")
        ):
            kw = peek_keyword(i)
            if kw == "BEGIN":
                begin_depth += 1
                kw_len = len("BEGIN")
                current.append(sql[i : i + kw_len])
                i += kw_len
                continue
            elif kw == "END" and begin_depth > 0:
                begin_depth -= 1
                kw_len = len("END")
                current.append(sql[i : i + kw_len])
                i += kw_len
                continue

        if c == ";":
            if begin_depth > 0:
                # Inside a scripting block — keep semicolon as part of the block
                current.append(c)
                i += 1
                continue
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue

        current.append(c)
        i += 1

    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


def execute_post_process_python(
    script_path: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    source: str = "",
    execution_id: str = "",
    context: Optional[Dict[str, str]] = None,
    command: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute a Python post-processing script or full command.

    Two call modes:
      1. script_path="shared/foo.py" — runs `python shared/foo.py`, no arguments.
      2. command="python shared/foo.py --arg value" — parses the full command
         with shlex. The first token may be "python", "python3", or an
         absolute path to a python interpreter; it is replaced with
         sys.executable so the subprocess uses the orchestrator's venv.

    stdout and stderr are merged and streamed line-by-line into the
    orchestrator logger at INFO level, prefixed with the script filename,
    so output appears inline in the job log file in real time.

    Args:
        script_path: Path to Python script (mutually exclusive with command).
        config: Pipeline configuration (unused, kept for signature symmetry).
        source: Data source name (populates SOURCE env var).
        execution_id: Current job execution ID (populates EXECUTION_ID env var).
        context: Additional environment variables (keys uppercased).
        command: Full command string, parsed via shlex.

    Returns:
        Dict with execution results.
    """
    if bool(script_path) == bool(command):
        raise ValueError(
            "execute_post_process_python: pass exactly one of script_path or command"
        )

    # Resolve the .py file (to verify it exists) and build argv.
    if command:
        tokens = shlex.split(command)
        if not tokens:
            raise ValueError(f"Post-process command is empty: {command!r}")
        # Find the .py token so we can resolve/verify it
        py_tokens = [t for t in tokens if t.endswith(".py")]
        if not py_tokens:
            raise ValueError(
                f"Post-process command must reference a .py file: {command!r}"
            )
        script_rel = py_tokens[0]
        # If the first token looks like a python interpreter, replace it with sys.executable.
        first = tokens[0]
        if (
            first == "python"
            or first == "python3"
            or first.endswith("/python")
            or first.endswith("\\python")
            or first.endswith("/python3")
            or first.endswith("\\python3")
        ):
            argv = [sys.executable] + tokens[1:]
        else:
            # Assume command is ["script.py", "--arg", ...] — prepend interpreter.
            argv = [sys.executable] + tokens
        display = command
    else:
        script_rel = script_path
        argv = [sys.executable, script_rel]
        display = script_rel

    # Resolve script path for existence check
    py_path = Path(script_rel)
    if not py_path.is_absolute():
        py_path = Path.cwd() / script_rel

    if not py_path.exists():
        raise FileNotFoundError(f"Post-process Python script not found: {script_rel}")

    # Replace the bare script_rel in argv with the resolved absolute path
    # so cwd differences don't break things. Only substitute the first match.
    for idx, tok in enumerate(argv):
        if tok == script_rel:
            argv[idx] = str(py_path)
            break

    logger.info(f"Executing post-process Python: {display}")

    # Build environment variables
    import os

    env = os.environ.copy()
    env["EXECUTION_ID"] = execution_id
    env["SOURCE"] = source
    # Ensure child Python flushes line-by-line so streaming works
    env["PYTHONUNBUFFERED"] = "1"
    # Silence progress bars in child scripts. tqdm / HuggingFace emit a fresh
    # progress line for every update (e.g. ML model weight-loading prints ~100+
    # "Loading weights: NN%|..." lines), which floods the job log with no
    # post-run value. Disable at the source; the streamer below also drops any
    # that slip through. A child can override by unsetting these if it must show
    # progress. (Set only if not already present, so an explicit env wins.)
    env.setdefault("TQDM_DISABLE", "1")
    env.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    env.setdefault("TRANSFORMERS_NO_ADVISORY_WARNINGS", "1")

    # Add context variables if provided
    if context:
        for key, value in context.items():
            env[key.upper()] = str(value)

    script_label = py_path.name

    # Stream output line-by-line into the logger so it appears inline in the
    # job log file in real time. stderr is merged into stdout to preserve
    # interleaving with what the script would print to a terminal.
    proc = subprocess.Popen(
        argv,
        cwd=Path.cwd(),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # line-buffered
    )

    # Drop tqdm/HuggingFace-style progress-bar updates from the log. These are
    # intermediate redraws (e.g. "Loading weights:  42%|####  | 43/103 [..it/s]")
    # that add hundreds of near-identical lines with no post-run value. The env
    # vars above disable most at the source; this catches any that remain. A
    # 100% / completed line is also a progress redraw, so it is dropped too --
    # the script's own "[OK] ... loaded" message is the real completion signal.
    _progress_re = re.compile(r"\d+%\|.*\|.*\d+/\d+")

    def _is_progress_line(text: str) -> bool:
        return bool(_progress_re.search(text)) or "it/s]" in text or "B/s]" in text

    captured_lines = []
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            stripped = line.rstrip()
            if not stripped or _is_progress_line(stripped):
                continue
            captured_lines.append(stripped)
            logger.info(f"[{script_label}] {stripped}")
    finally:
        rc = proc.wait()

    if rc != 0:
        raise RuntimeError(f"Python post-process failed with exit code {rc}: {display}")

    logger.info(f"[OK] Post-process Python completed: {display}")

    return {
        "status": "success",
        "script_path": script_rel,
        "command": display,
        "exit_code": rc,
        "output": "\n".join(captured_lines),
    }


def execute_post_process_sql(
    bq_client: bigquery.Client,
    sql_file: str,
    config: Dict[str, Any],
    source: str,
    execution_id: str,
    context: Optional[Dict[str, str]] = None,
    bq_job_labels: Optional[Dict[str, str]] = None,
    maximum_bytes_billed: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Execute a post-processing SQL file with template variable substitution.

    Args:
        bq_client: BigQuery client
        sql_file: Path to SQL file (relative to project root)
        config: Pipeline configuration
        source: Data source name (e.g., 'mailchimp')
        execution_id: Current job execution ID
        context: Additional template variables
        bq_job_labels: Optional labels applied to each BigQuery query job
        maximum_bytes_billed: Optional per-query bytes cap (fails job if exceeded)

    Returns:
        Dict with execution results
    """
    # Resolve SQL file path
    sql_path = Path(sql_file)
    if not sql_path.is_absolute():
        # Assume relative to project root
        sql_path = Path.cwd() / sql_file

    if not sql_path.exists():
        raise FileNotFoundError(f"Post-process SQL file not found: {sql_file}")

    # Read SQL file
    with open(sql_path, "r", encoding="utf-8") as f:
        sql_template = f.read()

    # Build template variables
    pipeline_config = config.get("pipeline", {})
    project_id = bq_client.project
    production_dataset = pipeline_config.get(
        "production_dataset", f"{source}_production"
    )
    staging_dataset = pipeline_config.get("staging_dataset", f"{source}_staging")
    bi_dataset = pipeline_config.get("bi_dataset", f"{source}_bi")

    template_vars = {
        "project_id": project_id,
        "production_dataset": production_dataset,
        "staging_dataset": staging_dataset,
        "bi_dataset": bi_dataset,
        "execution_id": execution_id,
        "source": source,
    }

    # Add context variables if provided
    if context:
        template_vars.update(context)

    # Substitute variables in SQL (use manual replacement to avoid breaking
    # on literal braces in regex patterns like {2,} in BigQuery SQL)
    sql = sql_template
    for key, value in template_vars.items():
        sql = sql.replace("{" + key + "}", str(value))

    sql = _rewrite_internal_datasets(
        sql,
        consumer_dataset=template_vars.get("consumer_dataset"),
        ops_dataset=template_vars.get("ops_dataset"),
        staging_dataset=template_vars.get("staging_dataset"),
        identity_xref_table=template_vars.get("identity_xref_table"),
        identity_hub_table=template_vars.get("identity_hub_table"),
        identity_persistence_table=template_vars.get("identity_persistence_table"),
    )

    # Log execution
    logger.info(f"Executing post-process SQL: {sql_file}")
    logger.debug(f"Template variables: {template_vars}")

    # Execute SQL — split on semicolons and execute each statement individually.
    # If the SQL uses TEMP tables, run all statements within a BigQuery session
    # so temp tables persist across statements (otherwise each statement is
    # isolated and CREATE TEMP TABLE fails).
    uses_temp_tables = bool(
        re.search(
            r"CREATE\s+(OR\s+REPLACE\s+)?TEMP(ORARY)?\s+TABLE", sql, re.IGNORECASE
        )
    )

    statements = _split_sql_statements(sql)

    results = []
    total_rows_affected = 0
    total_bytes_processed = 0
    total_bytes_billed = 0
    total_slot_millis = 0
    stmt_count = 0
    session_id = None

    for i, statement in enumerate(statements):
        try:
            # Skip empty statements
            if not statement:
                continue

            # Skip comments-only statements (all lines start with --)
            statement_lines = [
                line.strip() for line in statement.split("\n") if line.strip()
            ]
            if all(line.startswith("--") for line in statement_lines):
                logger.debug(f"Skipping comment-only statement {i+1}")
                continue

            logger.debug(f"Executing statement {i+1}/{len(statements)}")

            # Build job config — create or reuse session when temp tables present
            job_config = bigquery.QueryJobConfig(use_query_cache=False)
            merged_labels = dict(bq_job_labels) if bq_job_labels else {}
            orch_id = (config or {}).get("_orchestrator_job_id")
            if orch_id:
                # Must match orchestrator_monitoring.jobs.job_id for INFORMATION_SCHEMA rollup.
                merged_labels["orchestrator_job_id"] = str(orch_id).lower()[:63]
            if merged_labels:
                job_config.labels = merged_labels
            if maximum_bytes_billed is not None:
                job_config.maximum_bytes_billed = int(maximum_bytes_billed)
            if uses_temp_tables:
                if session_id is None:
                    # First statement: create a new session
                    job_config.create_session = True
                else:
                    # Subsequent statements: reuse the session
                    job_config.connection_properties = [
                        bigquery.query.ConnectionProperty(
                            key="session_id", value=session_id
                        )
                    ]

            query_job = bq_client.query(statement, job_config=job_config)
            result = query_job.result()
            stmt_count += 1

            # Capture session ID from the first statement
            if uses_temp_tables and session_id is None:
                session_id = query_job.session_info.session_id
                logger.debug(f"Created BigQuery session: {session_id}")

            # Capture DML rows affected (MERGE, INSERT, UPDATE, DELETE)
            if query_job.num_dml_affected_rows is not None:
                total_rows_affected += query_job.num_dml_affected_rows
            if getattr(query_job, "total_bytes_processed", None) is not None:
                total_bytes_processed += query_job.total_bytes_processed
            if getattr(query_job, "total_bytes_billed", None) is not None:
                total_bytes_billed += query_job.total_bytes_billed
            if getattr(query_job, "slot_millis", None) is not None:
                total_slot_millis += query_job.slot_millis

            statement_upper = statement.strip().upper()

            # Capture results if it's a SELECT
            if statement_upper.startswith("SELECT"):
                rows = list(result)
                if rows:
                    results.append(
                        {"statement_num": i + 1, "rows": [dict(row) for row in rows]}
                    )

        except Exception as e:
            logger.error(f"Failed to execute statement {i+1}: {e}")
            logger.error(f"Statement: {statement[:200]}...")
            try:
                e.statements_executed = stmt_count
                e.failed_statement_index = i + 1
                e.failed_statement_preview = statement.strip()[:400]
            except Exception:
                pass
            raise

    logger.info(f"[OK] Post-process SQL completed: {sql_file}")

    return {
        "status": "success",
        "sql_file": sql_file,
        "statements_executed": stmt_count,
        "total_rows_affected": total_rows_affected,
        "total_bytes_processed": total_bytes_processed,
        "total_bytes_billed": total_bytes_billed,
        "total_slot_millis": total_slot_millis,
        "results": results,
    }


def evaluate_post_process_outcome(post_results: Dict[str, Any]) -> Dict[str, Any]:
    """Decide whether a post-processing result should escalate the job to WARNING.

    Post-processing builds the derived/rollup tables (e.g. *_daily, the
    BN_Warehouse.* enriched outputs) AFTER the main extract/merge succeeds. If a
    step fails, the job's OUTPUT is broken even though the load worked -- so the
    job must report WARNING (+ alert), not a misleading COMPLETED. Same principle
    as BI-486: never report success when a derived table failed.

    Pure function (no IO) so the escalation rule is unit-testable apart from the
    2,600-line main(). Accepts the dict returned by ``run_post_processing``
    (keys: status, total, success, failed, results:[{file_path,status,error}]) and
    is also safe to call after a post-process EXCEPTION by passing
    ``{"status": "exception", "error": "..."}``.

    Returns:
        {
          "failed": bool,                 # True -> escalate to WARNING
          "failed_details": [             # one entry per failed step (for the
            {"table","stage","error"}     # email's failed-tables detail list)
          ],
          "summary": str,                 # one-line human summary (empty if ok)
        }
    """
    status = (post_results or {}).get("status")

    # A post-process EXCEPTION (whole phase blew up) is a failure with no per-step
    # breakdown.
    if status == "exception":
        err = str((post_results or {}).get("error", "post-process phase failed"))[:500]
        return {
            "failed": True,
            "failed_details": [
                {"table": "(post-process)", "stage": "post_process", "error": err}
            ],
            "summary": "Post-processing raised an exception: {0}".format(err),
        }

    failed_count = (post_results or {}).get("failed", 0) or 0
    if status != "completed" or failed_count <= 0:
        # skipped / no-config / all-succeeded -> no escalation.
        return {"failed": False, "failed_details": [], "summary": ""}

    # Per-step failures: one detail row each (named by SQL file), so the alert
    # email pinpoints which derived tables broke.
    failed_steps = [
        r for r in (post_results.get("results") or []) if r.get("status") == "failed"
    ]
    details = []
    for step in failed_steps:
        fname = Path(str(step.get("file_path", "?"))).name
        details.append(
            {
                "table": "(post-process) {0}".format(fname),
                "stage": "post_process",
                "error": str(step.get("error", "post-process SQL failed"))[:500],
            }
        )
    if not details:
        # failed_count > 0 but no per-step records -- still escalate with a count.
        details.append(
            {
                "table": "(post-process)",
                "stage": "post_process",
                "error": "{0} post-process SQL step(s) failed".format(failed_count),
            }
        )

    named = ", ".join(d["table"] for d in details)
    return {
        "failed": True,
        "failed_details": details,
        "summary": "Post-processing failed after a successful load: {0}".format(named),
    }


def run_post_processing(
    bq_client: bigquery.Client,
    config: Dict[str, Any],
    source: str,
    execution_id: str,
    level: str = "pipeline",
    table_name: Optional[str] = None,
    group_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run post-processing SQL based on configuration level.

    Args:
        bq_client: BigQuery client
        config: Pipeline configuration
        source: Data source name
        execution_id: Current job execution ID
        level: 'pipeline', 'table', or 'group'
        table_name: Table name (for table-level post-processing)
        group_name: Group name (for group-level post-processing)

    Returns:
        Dict with post-processing results
    """
    post_process_configs = []

    # Find post-process configurations
    if level == "pipeline":
        pipeline_config = config.get("pipeline", {})
        post_process = pipeline_config.get("post_process", [])

        # Support single dict or list of dicts
        if isinstance(post_process, dict):
            post_process = [post_process]

        post_process_configs = post_process

    elif level == "table" and table_name:
        resources = config.get("resources", {})
        table_config = resources.get(table_name, {})
        post_process = table_config.get("post_process")

        if post_process:
            if isinstance(post_process, dict):
                post_process_configs = [post_process]
            else:
                post_process_configs = post_process

    elif level == "group" and group_name:
        groups = config.get("groups", {})
        group_config = groups.get(group_name, {})
        post_process = group_config.get("post_process")

        if post_process:
            if isinstance(post_process, dict):
                post_process_configs = [post_process]
            else:
                post_process_configs = post_process

    if not post_process_configs:
        logger.debug(f"No post-processing configured for {level}")
        return {"status": "skipped", "reason": "no_config"}

    # Execute each post-process SQL
    results = []
    context = {}
    if table_name:
        context["table_name"] = table_name
    if group_name:
        context["group_name"] = group_name

    for post_config in post_process_configs:
        # Handle both string and dict formats.
        # A string entry may be:
        #   - a path to a .sql file
        #   - a path to a .py file
        #   - a full command like "python path/to/foo.py --arg value"
        command = None
        if isinstance(post_config, str):
            file_path = post_config
            description = file_path
            enabled = True
            # Detect command form: contains whitespace AND references a .py file.
            stripped = post_config.strip()
            if " " in stripped and ".py" in stripped:
                command = stripped
                # Pull the .py token out for logging/detection
                tokens = shlex.split(command)
                py_tokens = [t for t in tokens if t.endswith(".py")]
                file_path = py_tokens[0] if py_tokens else stripped
        else:
            # Detailed format: dict with options
            file_path = (
                post_config.get("sql_file")
                or post_config.get("script_path")
                or post_config.get("command")
            )
            command = post_config.get("command")
            if not file_path:
                logger.warning(
                    f"Post-process config missing sql_file/script_path/command: {post_config}"
                )
                continue

            description = post_config.get("description", file_path)
            enabled = post_config.get("enabled", True)

        # Check if enabled
        if not enabled:
            logger.info(f"Skipping disabled post-process: {description}")
            continue

        logger.info(f"Running post-process: {description}")

        # Detect if it's a Python script/command or SQL file
        is_python = command is not None or file_path.endswith(".py")

        try:
            if is_python:
                # Execute Python script or full command
                if command:
                    result = execute_post_process_python(
                        command=command,
                        config=config,
                        source=source,
                        execution_id=execution_id,
                        context=context,
                    )
                else:
                    result = execute_post_process_python(
                        script_path=file_path,
                        config=config,
                        source=source,
                        execution_id=execution_id,
                        context=context,
                    )
            else:
                # Execute SQL file
                result = execute_post_process_sql(
                    bq_client=bq_client,
                    sql_file=file_path,
                    config=config,
                    source=source,
                    execution_id=execution_id,
                    context=context,
                )

            results.append(
                {
                    "file_path": file_path,
                    "description": description,
                    "status": "success",
                    "result": result,
                }
            )
        except Exception as e:
            logger.error(f"Post-process failed: {description} - {e}")
            results.append(
                {
                    "file_path": file_path,
                    "description": description,
                    "status": "failed",
                    "error": str(e),
                }
            )
            # Don't raise - allow other post-processes to run

    success_count = len([r for r in results if r["status"] == "success"])
    failed_count = len([r for r in results if r["status"] == "failed"])

    return {
        "status": "completed",
        "total": len(results),
        "success": success_count,
        "failed": failed_count,
        "results": results,
    }
