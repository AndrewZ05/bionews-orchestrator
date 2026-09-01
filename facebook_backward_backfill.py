#!/usr/bin/env python3
"""
Generic Backfill Runner with Per-Page Cleanup

Runs a sequence of shell commands from a text file, with:
- Clear delineation between jobs (banners + blank lines)
- Automatic rate-limit detection and exponential backoff
- Per-page failure detection (parses extractor logs for failed pages)
- Automatic cleanup re-runs for failed pages only (--sites <page_ids>)
- Stop-on-failure for non-rate-limit errors and unrecoverable pages
- Summary report at the end

Usage:
    python facebook_backward_backfill.py [commands_file]

    Default commands file: backfill_commands.txt

Command file format:
    - One command per line
    - Lines starting with # are comments (ignored)
    - Blank lines are ignored
"""

import json
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


DEFAULT_COMMANDS_FILE = "backfill_commands.txt"
LOG_DIR = Path("logs/backfill")

RATE_LIMIT_MARKERS = [
    "rate limit",
    "too many requests",
    "user request limit",
    "too many calls",
    "(#17)",
    "(#4)",
    "(#32)",
    "(#613)",
    "throttled",
    "api call limit",
]

INITIAL_BACKOFF_SECONDS = 60
MAX_BACKOFF_SECONDS = 3600
MAX_RETRIES = 5
MAX_CLEANUP_ATTEMPTS = 3

FAILED_PAGE_PATTERN = re.compile(r"Page:\s+(.+?)\s+\(ID:\s+(\d+)\)")
EXTRACTION_SUMMARY_PATTERN = re.compile(
    r"([A-Z_]+)\s+EXTRACTION\s+SUMMARY:\s+(\d+)\s+page\(s\)\s+failed",
    re.IGNORECASE,
)

# Maps extractor-reported names (uppercase) to orchestrate.py table names.
TABLE_NAME_MAP = {
    "POSTS": "posts",
    "PAGES": "pages",
    "PAGE_INSIGHTS": "page_insights",
    "POST_INSIGHTS": "post_insights",
}


class Tee:
    """File-like object that writes to multiple underlying streams.

    Used to mirror stdout to both the terminal and a log file so the user
    can tail progress in real time AND keep a persistent record across
    reboots / closed terminals.

    Lines written to the file stream are prefixed with a timestamp so
    each entry in the log file is self-dating. The console stream is
    NOT prefixed (it stays clean for interactive viewing).
    """

    def __init__(self, console_stream, file_stream):
        self.console_stream = console_stream
        self.file_stream = file_stream
        self._at_line_start = True

    def write(self, data):
        # Console: write as-is.
        self.console_stream.write(data)
        self.console_stream.flush()

        # File: prefix each new line with a timestamp.
        if not data:
            return
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        out = []
        for ch in data:
            if self._at_line_start:
                out.append(f"[{ts}] ")
                self._at_line_start = False
            out.append(ch)
            if ch == "\n":
                self._at_line_start = True
        self.file_stream.write("".join(out))
        self.file_stream.flush()

    def flush(self):
        self.console_stream.flush()
        self.file_stream.flush()


# Globals populated by setup_logging().
_RUN_LOG_FILE = None
_EVENTS_FILE = None
_RUN_ID = None


def setup_logging():
    """Create the log directory, open log files, and tee stdout/stderr.

    Returns (run_log_path, events_path, run_id).
    """
    global _RUN_LOG_FILE, _EVENTS_FILE, _RUN_ID

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    _RUN_ID = run_id

    run_log_path = LOG_DIR / f"run_{run_id}.log"
    events_path = LOG_DIR / f"run_{run_id}_events.jsonl"

    _RUN_LOG_FILE = open(run_log_path, "a", encoding="utf-8", buffering=1)
    _EVENTS_FILE = open(events_path, "a", encoding="utf-8", buffering=1)

    # Tee stdout and stderr to file as well as console.
    sys.stdout = Tee(sys.__stdout__, _RUN_LOG_FILE)
    sys.stderr = Tee(sys.__stderr__, _RUN_LOG_FILE)

    return run_log_path, events_path, run_id


def log_event(event_type, **fields):
    """Append a structured event to the JSONL events file.

    Each line is a self-contained JSON object with a timestamp, event
    type, and arbitrary fields. Use this file for machine parsing or
    resume-from-last-success logic.
    """
    if _EVENTS_FILE is None:
        return
    record = {
        "timestamp": datetime.now().isoformat(),
        "run_id": _RUN_ID,
        "event": event_type,
        **fields,
    }
    _EVENTS_FILE.write(json.dumps(record) + "\n")


def print_banner(text, char="="):
    bar = char * 78
    print()
    print()
    print()
    print(bar)
    print(f"  {text}")
    print(bar)
    print()


def print_separator():
    print()
    print()
    print()


def is_rate_limit_error(output):
    lowered = output.lower()
    return any(marker in lowered for marker in RATE_LIMIT_MARKERS)


def parse_failed_pages_by_table(output):
    """Extract failed page entries grouped by which table failed.

    The extractor emits a section like:
        POSTS EXTRACTION SUMMARY: 2 page(s) failed
        ...
        - Page: Myasthenia Gravis News (ID: 656694914539665)
        - Page: Pulmonary Fibrosis News (ID: 938839346130112)
        ...
        TO REPROCESS FAILED PAGES:

    This function walks the output line by line, tracking the active
    table whenever an EXTRACTION SUMMARY header is seen, and assigns
    each "Page: <name> (ID: <id>)" entry to that table.

    Returns: dict mapping table name (e.g. "posts") -> list of unique
    {'name': ..., 'id': ...} dicts.
    """
    by_table = {}
    current_table = None
    seen_for_table = {}

    for line in output.splitlines():
        summary_match = EXTRACTION_SUMMARY_PATTERN.search(line)
        if summary_match:
            raw_name = summary_match.group(1).upper()
            current_table = TABLE_NAME_MAP.get(raw_name)
            if current_table and current_table not in by_table:
                by_table[current_table] = []
                seen_for_table[current_table] = set()
            continue

        # Stop assigning pages to this table once the reprocess hint starts,
        # so we don't double-count anything echoed in the hint footer.
        if "TO REPROCESS FAILED PAGES" in line.upper():
            current_table = None
            continue

        if current_table is None:
            continue

        page_match = FAILED_PAGE_PATTERN.search(line)
        if not page_match:
            continue
        page_name = page_match.group(1).strip()
        page_id = page_match.group(2).strip()
        if page_id in seen_for_table[current_table]:
            continue
        seen_for_table[current_table].add(page_id)
        by_table[current_table].append({"name": page_name, "id": page_id})

    return by_table


def load_commands(commands_file):
    path = Path(commands_file)
    if not path.exists():
        print(f"ERROR: Commands file not found: {commands_file}")
        sys.exit(1)

    commands = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            commands.append(stripped)

    if not commands:
        print(f"ERROR: No commands found in {commands_file}")
        sys.exit(1)

    return commands


def run_subprocess_with_backoff(command_str, label):
    """Run a single subprocess command with rate-limit backoff.

    Streams subprocess output to the console in real-time while also
    capturing it for rate-limit detection and failed-page parsing.

    Returns a tuple: (success: bool, combined_output: str, returncode: int).
    """
    args = shlex.split(command_str)

    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Command: {command_str}")
    print()

    backoff = INITIAL_BACKOFF_SECONDS
    attempt = 0

    while attempt <= MAX_RETRIES:
        attempt += 1
        if attempt > 1:
            print(f"Retry attempt {attempt}/{MAX_RETRIES + 1}")

        log_event(
            "subprocess_start",
            label=label,
            command=command_str,
            attempt=attempt,
        )

        start_time = time.time()
        captured_lines = []

        # Stream output line by line so the user sees progress in real time.
        # stderr is merged into stdout so we get a single stream in order.
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # line-buffered
        )

        try:
            for line in process.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                captured_lines.append(line)
        finally:
            process.stdout.close()
            returncode = process.wait()

        duration = time.time() - start_time
        combined_output = "".join(captured_lines)

        if returncode == 0:
            print()
            print(f"{label} SUCCESS in {duration / 60:.1f} minutes")
            print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            log_event(
                "subprocess_success",
                label=label,
                command=command_str,
                attempt=attempt,
                duration_seconds=duration,
            )
            return True, combined_output, 0

        if is_rate_limit_error(combined_output):
            print()
            print(f"RATE LIMIT detected after {duration / 60:.1f} minutes")
            log_event(
                "rate_limit",
                label=label,
                command=command_str,
                attempt=attempt,
                duration_seconds=duration,
            )
            if attempt > MAX_RETRIES:
                print(f"Exceeded max retries ({MAX_RETRIES}). Stopping.")
                log_event(
                    "rate_limit_exhausted",
                    label=label,
                    command=command_str,
                    attempts=attempt,
                )
                return False, combined_output, returncode

            wait = min(backoff, MAX_BACKOFF_SECONDS)
            print(f"Backing off for {wait} seconds before retry...")
            log_event("rate_limit_backoff", wait_seconds=wait)
            time.sleep(wait)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            continue

        # Hard failure - not a rate limit
        print()
        print("!" * 78)
        print(
            f"  HARD FAILURE in {duration / 60:.1f} minutes "
            f"(exit code {returncode})"
        )
        print("  Not a rate-limit error - stopping immediately.")
        print("!" * 78)
        log_event(
            "subprocess_hard_failure",
            label=label,
            command=command_str,
            attempt=attempt,
            duration_seconds=duration,
            returncode=returncode,
            stderr_tail=combined_output[-2000:],
        )
        return False, combined_output, returncode

    return False, "", -1


def build_cleanup_command(base_command_str, table, failed_pages):
    """Build a cleanup command that re-runs only the given table for the given pages.

    Strips out any existing --tables and --sites arguments from base_command_str
    and replaces them with the failed table + failed page ids, preserving the
    rest of the command (especially --start-date / --end-date).
    """
    tokens = shlex.split(base_command_str)
    cleaned = []
    skip_next = False
    skip_multi = False
    for token in tokens:
        if skip_multi:
            if token.startswith("--"):
                skip_multi = False
                # fall through and decide whether to keep this token
            else:
                continue
        if skip_next:
            skip_next = False
            continue
        if token in ("--tables", "--sites"):
            skip_multi = True
            continue
        cleaned.append(token)

    page_ids = [p["id"] for p in failed_pages]
    cleaned.extend(["--tables", table, "--sites", *page_ids])
    return " ".join(shlex.quote(t) for t in cleaned)


def run_cleanup_for_table(base_command_str, table, failed_pages):
    """Run cleanup attempts for a single table + failed pages.

    Iterates up to MAX_CLEANUP_ATTEMPTS, narrowing the page list each time
    to whatever is still failing in subsequent extractor output.

    Returns (success: bool, unrecovered: list of failed page dicts).
    """
    current_failed = failed_pages

    for attempt in range(1, MAX_CLEANUP_ATTEMPTS + 1):
        print_banner(
            f"CLEANUP ATTEMPT {attempt}/{MAX_CLEANUP_ATTEMPTS} "
            f"for table '{table}' - {len(current_failed)} failed page(s)",
            char="-",
        )
        for page in current_failed:
            print(f"  - {page['name']} (ID: {page['id']})")
        print()

        cleanup_cmd = build_cleanup_command(base_command_str, table, current_failed)
        ok, output, _ = run_subprocess_with_backoff(cleanup_cmd, label="CLEANUP")
        if not ok:
            return False, current_failed

        new_failed_by_table = parse_failed_pages_by_table(output)
        new_failed = new_failed_by_table.get(table, [])
        if not new_failed:
            print(f"All {len(current_failed)} page(s) for table '{table}' recovered.")
            return True, []

        prior_ids = {p["id"] for p in current_failed}
        new_ids = {p["id"] for p in new_failed}
        if new_ids == prior_ids:
            print(
                f"WARNING: Same {len(new_failed)} page(s) failing repeatedly for "
                f"table '{table}' - will keep retrying until max attempts."
            )

        current_failed = new_failed

    return False, current_failed


def run_job(job_num, total_jobs, command_str):
    """Run one job (main chunk + any per-table cleanup runs needed).

    Returns (success: bool, unrecovered: dict of {table: [pages]}).
    """
    print_banner(f"JOB {job_num}/{total_jobs}")
    log_event(
        "job_start", job_num=job_num, total_jobs=total_jobs, command=command_str
    )

    ok, output, _ = run_subprocess_with_backoff(command_str, label="JOB")
    if not ok:
        log_event(
            "job_failed_hard", job_num=job_num, command=command_str
        )
        return False, {}

    failed_by_table = parse_failed_pages_by_table(output)
    if not failed_by_table:
        log_event(
            "job_success_clean", job_num=job_num, command=command_str
        )
        return True, {}

    total_failed = sum(len(pages) for pages in failed_by_table.values())
    print()
    print(
        f"WARNING: {total_failed} page(s) failed within this job, "
        f"across {len(failed_by_table)} table(s):"
    )
    for table, pages in failed_by_table.items():
        print(f"  Table '{table}': {len(pages)} page(s)")
        for page in pages:
            print(f"    - {page['name']} (ID: {page['id']})")
    print()

    log_event(
        "job_pages_failed",
        job_num=job_num,
        command=command_str,
        failed_by_table={
            t: [p["id"] for p in pages] for t, pages in failed_by_table.items()
        },
    )

    unrecovered = {}
    for table, pages in failed_by_table.items():
        log_event(
            "cleanup_start",
            job_num=job_num,
            table=table,
            page_ids=[p["id"] for p in pages],
        )
        cleanup_ok, still_failing = run_cleanup_for_table(command_str, table, pages)
        if not cleanup_ok:
            unrecovered[table] = still_failing
            log_event(
                "cleanup_failed",
                job_num=job_num,
                table=table,
                unrecovered_page_ids=[p["id"] for p in still_failing],
            )
        else:
            log_event("cleanup_success", job_num=job_num, table=table)

    if unrecovered:
        return False, unrecovered
    log_event("job_success_after_cleanup", job_num=job_num, command=command_str)
    return True, {}


def main():
    commands_file = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_COMMANDS_FILE
    commands = load_commands(commands_file)

    run_log_path, events_path, run_id = setup_logging()

    overall_start = datetime.now()
    total = len(commands)
    succeeded = 0
    failed_jobs = []

    print_banner(f"BACKFILL RUNNER - {total} JOBS")
    print(f"Run ID:         {run_id}")
    print(f"Run started at: {overall_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Commands file:  {commands_file}")
    print(f"Run log:        {run_log_path}")
    print(f"Events log:     {events_path}")
    print(f"Max retries per job:    {MAX_RETRIES}")
    print(f"Max cleanup attempts:   {MAX_CLEANUP_ATTEMPTS}")

    log_event(
        "run_start",
        commands_file=commands_file,
        total_jobs=total,
        commands=commands,
        max_retries=MAX_RETRIES,
        max_cleanup_attempts=MAX_CLEANUP_ATTEMPTS,
    )

    for idx, command_str in enumerate(commands, start=1):
        ok, unrecovered = run_job(idx, total, command_str)
        if ok:
            succeeded += 1
        else:
            failed_jobs.append(
                {
                    "job_num": idx,
                    "command": command_str,
                    "unrecovered_by_table": unrecovered,
                }
            )
            print()
            print(f"Stopping after failure on job {idx}.")
            break
        print_separator()

    overall_end = datetime.now()
    elapsed = (overall_end - overall_start).total_seconds() / 60

    print_banner("BACKFILL SUMMARY")
    print(f"Started:   {overall_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Finished:  {overall_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Elapsed:   {elapsed:.1f} minutes")
    print(f"Succeeded: {succeeded}/{total} jobs")
    if failed_jobs:
        print(f"Failed:    {len(failed_jobs)} job(s)")
        for failure in failed_jobs:
            print(f"  - Job {failure['job_num']}: {failure['command']}")
            if failure["unrecovered_by_table"]:
                print(
                    f"    Unrecovered pages "
                    f"(after {MAX_CLEANUP_ATTEMPTS} cleanup attempts):"
                )
                for table, pages in failure["unrecovered_by_table"].items():
                    print(f"      Table '{table}':")
                    for page in pages:
                        print(f"        - {page['name']} (ID: {page['id']})")
        log_event(
            "run_end",
            status="failed",
            succeeded=succeeded,
            total=total,
            elapsed_minutes=elapsed,
            failed_jobs=[
                {
                    "job_num": f["job_num"],
                    "command": f["command"],
                    "unrecovered_by_table": {
                        t: [p["id"] for p in pages]
                        for t, pages in f["unrecovered_by_table"].items()
                    },
                }
                for f in failed_jobs
            ],
        )
        sys.exit(1)

    print("All jobs completed successfully.")
    log_event(
        "run_end",
        status="success",
        succeeded=succeeded,
        total=total,
        elapsed_minutes=elapsed,
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
