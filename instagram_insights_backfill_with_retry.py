#!/usr/bin/env python3
"""
Instagram Insights Backfill with Automatic Retry.

Runs month-sized Instagram media + insight backfills sequentially, with durable
state, per-batch child logs, and retry of username-specific transient failures.

Modeled on facebook_insights_backfill_with_retry.py. Key differences:
  - --sites carries IG usernames (not Page IDs) — see plugins/instagram_extractor.py
    discover_ig_accounts() filter.
  - Tables: ig_accounts, media, media_insights, account_insights. Stories are
    24hr-ephemeral and are intentionally skipped — there is no historical chunk
    to backfill for them.
  - Instagram Graph API quota is 200 calls/user/hour (shared across all 8 sites
    in this run), so a long backfill is dominated by quota-wait rather than CPU.
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from collections import deque
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parent
LOG_DIR = REPO_ROOT / "logs"
CHILD_LOG_DIR = LOG_DIR / "instagram_backfill_batches"
WRAPPER_LOG_FILE = LOG_DIR / "instagram_backfill_wrapper.log"
DEFAULT_STATE_FILE = LOG_DIR / "instagram_insights_backfill_state.json"

# Instagram API only retains account_insights for ~2 years.
DEFAULT_START_DATE = (date.today() - timedelta(days=730)).isoformat()
DEFAULT_TIMEOUT_MINUTES = 360
DEFAULT_MAX_RETRIES = 3
TAIL_LINE_COUNT = 80

TABLES = ("ig_accounts", "media", "media_insights", "account_insights")

# When --sites is omitted, the wrapper runs against every IG account the
# service account's token can see (the extractor's discover_ig_accounts()
# without a username filter). Pass --sites to restrict the run.

# Match "@username" mentions in Instagram error log lines, e.g.:
#   "Media extraction failed for @alsnewstoday: ..."
#   "Gave up on @cushingsdiseasenews after 3 rate-limit retries"
USERNAME_RE = re.compile(r"@([A-Za-z0-9._]+)")
FAILURE_MARKERS = (
    "Media extraction failed for @",
    "Gave up on @",
    "Unexpected error extracting media for @",
)
LOCK_CONTENTION_RE = re.compile(
    r"Cannot acquire (?:ETL )?lock|"
    r"Could not acquire (?:distributed |ETL )?lock|"
    r"Another (?:ETL )?job is already running|"
    r"Another job for this source is already running",
    re.IGNORECASE,
)


def configure_logging() -> logging.Logger:
    """Configure UTF-8 file logging and console logging that survives cp1252."""
    LOG_DIR.mkdir(exist_ok=True)
    CHILD_LOG_DIR.mkdir(exist_ok=True)

    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass

    logger = logging.getLogger("instagram_backfill")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler(WRAPPER_LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


logger = configure_logging()


class FailedUsernameParser:
    """Incrementally parse IG usernames from per-account failure lines."""

    def __init__(self) -> None:
        self.failed_usernames = set()

    def feed(self, line: str) -> None:
        if any(marker in line for marker in FAILURE_MARKERS):
            match = USERNAME_RE.search(line)
            if match:
                self.failed_usernames.add(match.group(1).lower())

    def result(self) -> List[str]:
        return sorted(self.failed_usernames)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def month_end(value: date) -> date:
    if value.month == 12:
        next_month = date(value.year + 1, 1, 1)
    else:
        next_month = date(value.year, value.month + 1, 1)
    return next_month - timedelta(days=1)


def generate_month_batches(start_date: str, end_date: str) -> List[Tuple[str, str]]:
    start = parse_date(start_date)
    end = parse_date(end_date)
    if end < start:
        raise ValueError(f"end-date {end_date} is before start-date {start_date}")

    batches: List[Tuple[str, str]] = []
    current = start
    while current <= end:
        batch_end = min(month_end(current), end)
        batches.append((current.isoformat(), batch_end.isoformat()))
        current = batch_end + timedelta(days=1)
    # Newest months first so recent data lands in production early.
    batches.reverse()
    return batches


def batch_key(start_date: str, end_date: str) -> str:
    return f"{start_date}:{end_date}"


def load_state(path: Path) -> Dict[str, Dict[str, object]]:
    if not path.exists():
        return {"completed": {}, "failed": {}}
    try:
        with path.open("r", encoding="utf-8") as fh:
            state = json.load(fh)
    except Exception as exc:
        logger.warning("Could not read state file %s: %s", path, exc)
        return {"completed": {}, "failed": {}}

    state.setdefault("completed", {})
    state.setdefault("failed", {})
    return state


def save_state(path: Path, state: Dict[str, Dict[str, object]]) -> None:
    path.parent.mkdir(exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
        fh.write("\n")
    os.replace(tmp_path, path)


def should_echo_child_line(line: str) -> bool:
    important_fragments = (
        " - ERROR - ",
        " - WARNING - ",
        "Pipeline Validation Report",
        "[VALID]",
        "Processing table:",
        "Progress:",
        "PIPELINE COMPLETE",
        "Table ",
        "Created job ",
        "Found ",
        "Rate limit",
        "Discovered IG account",
        "Filtered IG accounts",
        "Extracted ",
        "No data",
    )
    return any(fragment in line for fragment in important_fragments)


def child_log_path(start_date: str, end_date: str, retry_attempt: int) -> Path:
    suffix = f"{start_date}_{end_date}_attempt{retry_attempt}.log"
    return CHILD_LOG_DIR / f"instagram_insights_{suffix}"


def build_command(
    start_date: str,
    end_date: str,
    sites: Iterable[str],
    extra_args: Iterable[str],
    env_flag: str,
) -> List[str]:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "orchestrate.py"),
        "--source",
        "instagram",
        "--env",
        env_flag,
        "--tables",
        *TABLES,
        "--start-date",
        start_date,
        "--end-date",
        end_date,
    ]
    sites_list = list(sites)
    if sites_list:
        cmd.extend(["--sites", *sites_list])
    else:
        # Empty --sites means "all discovered accounts" in the extractor;
        # use the explicit sentinel so orchestrate.py validation doesn't
        # require a site selection method.
        cmd.extend(["--all-sites"])
    cmd.extend(extra_args)
    return cmd


def run_batch(
    start_date: str,
    end_date: str,
    *,
    sites: Iterable[str],
    retry_attempt: int = 1,
    timeout_seconds: int,
    extra_args: Iterable[str],
    env_flag: str,
) -> Dict[str, object]:
    """Run one batch and stream child output to a bounded tail plus log file."""
    batch_label = f"{start_date} to {end_date}"
    if retry_attempt > 1:
        batch_label += f" (retry {retry_attempt})"

    sites_list = list(sites)
    cmd = build_command(start_date, end_date, sites_list, extra_args, env_flag)
    cmd_display = subprocess.list2cmdline(cmd)
    log_path = child_log_path(start_date, end_date, retry_attempt)
    parser = FailedUsernameParser()
    output_tail = deque(maxlen=TAIL_LINE_COUNT)
    lock_contention = False

    logger.info("=" * 80)
    logger.info("Running: %s", batch_label)
    logger.info("Sites: %s", ", ".join(sites_list) if sites_list else "ALL (--all-sites)")
    logger.info("Command: %s", cmd_display)
    logger.info("Child log: %s", log_path)
    logger.info("=" * 80)

    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")

    started = time.monotonic()
    timed_out = False
    returncode: Optional[int] = None

    with log_path.open("w", encoding="utf-8", buffering=1) as child_log:
        child_log.write(f"Started: {datetime.now().isoformat()}\n")
        child_log.write(f"Command: {cmd_display}\n")
        child_log.write("=" * 80 + "\n")

        try:
            process = subprocess.Popen(
                cmd,
                cwd=str(REPO_ROOT),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except Exception as exc:
            logger.error("[ERROR] Could not start batch %s: %s", batch_label, exc)
            return {
                "success": False,
                "failed_usernames": [],
                "output_tail": str(exc),
                "log_path": str(log_path),
                "returncode": None,
                "timed_out": False,
                "lock_contention": False,
                "elapsed_seconds": 0,
            }

        assert process.stdout is not None
        for raw_line in process.stdout:
            line = raw_line.rstrip("\r\n")
            child_log.write(raw_line)
            output_tail.append(line)
            parser.feed(line)
            if LOCK_CONTENTION_RE.search(line):
                lock_contention = True

            if should_echo_child_line(line):
                logger.info("[child] %s", line)

            if time.monotonic() - started > timeout_seconds:
                timed_out = True
                logger.error("[TIMEOUT] Killing batch after %s seconds: %s", timeout_seconds, batch_label)
                process.kill()
                break

        returncode = process.wait()

    elapsed_seconds = round(time.monotonic() - started, 1)
    success = returncode == 0 and not timed_out
    failed_usernames = parser.result()
    tail_text = "\n".join(line for line in output_tail if line.strip())

    if success:
        logger.info("[OK] Batch completed: %s in %.1fs", batch_label, elapsed_seconds)
        if failed_usernames:
            logger.warning("  %d username(s) were reported for retry", len(failed_usernames))
    else:
        logger.error("[FAILED] Batch failed: %s (exit code: %s, elapsed: %.1fs)", batch_label, returncode, elapsed_seconds)
        if tail_text:
            logger.error("Last command output:\n%s", tail_text)

    return {
        "success": success,
        "failed_usernames": failed_usernames,
        "output_tail": tail_text,
        "log_path": str(log_path),
        "returncode": returncode,
        "timed_out": timed_out,
        "lock_contention": lock_contention,
        "elapsed_seconds": elapsed_seconds,
    }


def parse_sites_arg(raw: Optional[str]) -> List[str]:
    """Accept space- or comma-separated usernames. Empty/None means 'all accounts'."""
    if not raw:
        return []
    tokens = [t.strip().lstrip("@") for t in re.split(r"[,\s]+", raw) if t.strip()]
    return tokens


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Instagram insights monthly backfill with retry and resume state.",
    )
    parser.add_argument(
        "--sites",
        default=None,
        help=(
            "Comma- or space-separated IG usernames to backfill. "
            "Omit to backfill ALL discovered IG accounts."
        ),
    )
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Backfill start date, YYYY-MM-DD (default: 2 years ago).")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="Backfill end date, YYYY-MM-DD.")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE), help="JSON state file for completed/failed batches.")
    parser.add_argument("--timeout-minutes", type=int, default=DEFAULT_TIMEOUT_MINUTES, help="Wall-clock timeout per child batch.")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Maximum attempts per failed username retry pass.")
    parser.add_argument("--rerun-completed", action="store_true", help="Do not skip batches already marked complete in state.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned batches and exit without running orchestrate.py.")
    parser.add_argument("--env", default="prod", help="--env flag passed to orchestrate.py (default: prod).")
    parser.add_argument(
        "--orchestrate-arg",
        action="append",
        default=[],
        help="Extra argument to pass through to orchestrate.py. Repeat for multiple args.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sites = parse_sites_arg(args.sites)
    batches = generate_month_batches(args.start_date, args.end_date)
    state_path = Path(args.state_file)
    state = load_state(state_path)
    timeout_seconds = args.timeout_minutes * 60

    logger.info("=" * 80)
    logger.info("Instagram Insights Backfill with Automatic Retry")
    logger.info("Started: %s", datetime.now())
    logger.info("Date range: %s to %s", args.start_date, args.end_date)
    if sites:
        logger.info("Sites (%d): %s", len(sites), ", ".join(sites))
    else:
        logger.info("Sites: ALL discovered IG accounts (no --sites filter)")
    logger.info("Tables: %s", ", ".join(TABLES))
    logger.info("Total batches: %d", len(batches))
    logger.info("State file: %s", state_path)
    logger.info("=" * 80)

    if args.dry_run:
        for idx, (start_date, end_date) in enumerate(batches, 1):
            logger.info("[Batch %d/%d] %s to %s", idx, len(batches), start_date, end_date)
        return 0

    completed_batches = 0
    skipped_batches = 0
    failed_batches = 0
    total_retries = 0
    lock_blocked = False

    for batch_num, (start_date, end_date) in enumerate(batches, 1):
        key = batch_key(start_date, end_date)
        if key in state["completed"] and not args.rerun_completed:
            skipped_batches += 1
            logger.info("[Batch %d/%d] Skipping completed batch %s", batch_num, len(batches), key)
            continue

        logger.info("")
        logger.info("[Batch %d/%d] Processing %s to %s", batch_num, len(batches), start_date, end_date)
        logger.info("")
        result = run_batch(
            start_date,
            end_date,
            sites=sites,
            retry_attempt=1,
            timeout_seconds=timeout_seconds,
            extra_args=args.orchestrate_arg,
            env_flag=args.env,
        )

        if result.get("lock_contention"):
            lock_blocked = True
            logger.error("[ABORTED] Active ETL job lock detected; stopping before marking this batch failed")
            logger.error(
                "  View active locks with: %s",
                subprocess.list2cmdline([sys.executable, str(REPO_ROOT / "orchestrate.py"), "--env", args.env, "--job-locks"]),
            )
            logger.error("  Re-run this wrapper after the active ETL job completes or the stale lock is released")
            break

        failed_usernames = list(result["failed_usernames"])
        process_failed_without_username_list = not result["success"] and not failed_usernames

        retry_attempt = 2
        while failed_usernames and retry_attempt <= args.max_retries:
            logger.warning(
                "\n[Batch %d retry %d] %d username(s) failed, retrying...",
                batch_num,
                retry_attempt - 1,
                len(failed_usernames),
            )

            backoff_seconds = min(60 * (2 ** (retry_attempt - 2)), 300)
            logger.info("Waiting %d seconds before retry...", backoff_seconds)
            time.sleep(backoff_seconds)

            retry_result = run_batch(
                start_date,
                end_date,
                sites=failed_usernames,
                retry_attempt=retry_attempt,
                timeout_seconds=timeout_seconds,
                extra_args=args.orchestrate_arg,
                env_flag=args.env,
            )

            if retry_result.get("lock_contention"):
                lock_blocked = True
                result = retry_result
                logger.error("[ABORTED] Active ETL job lock detected during retry; stopping before marking this batch failed")
                logger.error(
                    "  View active locks with: %s",
                    subprocess.list2cmdline([sys.executable, str(REPO_ROOT / "orchestrate.py"), "--env", args.env, "--job-locks"]),
                )
                logger.error("  Re-run this wrapper after the active ETL job completes or the stale lock is released")
                break

            new_failed_usernames = list(retry_result["failed_usernames"])
            recovered = len(failed_usernames) - len(new_failed_usernames)
            if recovered > 0:
                logger.info("[OK] Recovered %d username(s) on retry %d", recovered, retry_attempt - 1)
                total_retries += recovered

            if not retry_result["success"] and not new_failed_usernames:
                process_failed_without_username_list = True
                result = retry_result
                break

            failed_usernames = new_failed_usernames
            result = retry_result
            retry_attempt += 1

        if lock_blocked:
            break

        now = datetime.now().isoformat()
        if process_failed_without_username_list:
            logger.error("[FAILED] Batch %d failed before username-specific retry information could be parsed", batch_num)
            logger.error("  Manual reprocessing command:")
            logger.error(
                "  %s",
                subprocess.list2cmdline(build_command(start_date, end_date, sites, args.orchestrate_arg, args.env)),
            )
            failed_batches += 1
            state["failed"][key] = {
                "completed_at": now,
                "failed_usernames": [],
                "last_log": result.get("log_path"),
                "returncode": result.get("returncode"),
                "timed_out": result.get("timed_out"),
            }
            state["completed"].pop(key, None)
        elif failed_usernames:
            logger.error("[FAILED] Batch %d has %d permanent username failure(s) after %d retries", batch_num, len(failed_usernames), args.max_retries)
            logger.error("  Failed usernames: %s", ",".join(failed_usernames))
            logger.error("  Manual reprocessing command:")
            logger.error(
                "  %s",
                subprocess.list2cmdline(build_command(start_date, end_date, failed_usernames, args.orchestrate_arg, args.env)),
            )
            failed_batches += 1
            state["failed"][key] = {
                "completed_at": now,
                "failed_usernames": failed_usernames,
                "last_log": result.get("log_path"),
                "returncode": result.get("returncode"),
                "timed_out": result.get("timed_out"),
            }
            state["completed"].pop(key, None)
        else:
            logger.info("[OK] Batch %d completed successfully", batch_num)
            completed_batches += 1
            state["completed"][key] = {
                "completed_at": now,
                "last_log": result.get("log_path"),
                "elapsed_seconds": result.get("elapsed_seconds"),
            }
            state["failed"].pop(key, None)

        save_state(state_path, state)

    logger.info("\n%s", "=" * 80)
    logger.info("BACKFILL ABORTED: ACTIVE ETL LOCK" if lock_blocked else "BACKFILL COMPLETE")
    logger.info("%s", "=" * 80)
    logger.info("Completed this run: %d/%d batches", completed_batches, len(batches))
    logger.info("Skipped from state: %d/%d batches", skipped_batches, len(batches))
    logger.info("Failed this run: %d/%d batches", failed_batches, len(batches))
    logger.info("Total usernames recovered via wrapper retry: %d", total_retries)
    logger.info("Finished: %s", datetime.now())
    logger.info("%s", "=" * 80)

    return 1 if lock_blocked or failed_batches else 0


if __name__ == "__main__":
    sys.exit(main())
