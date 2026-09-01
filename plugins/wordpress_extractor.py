#!/usr/bin/env python3
# wordpress_extractor.py - Complete production version

import os
import sys
import io
import time
import socket
import subprocess
import random
import re
import json
import threading
import hashlib
import uuid
import logging
import collections
from fabric import Connection
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from typing import Iterator, Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta

# Import centralized timestamp utilities
from shared.timestamp_utils import parse_mysql_timestamp
from shared.extractor_utils import get_available_tables
from shared.account_context import set_execution_metadata
from shared.extractor_runner import initialize_pipeline_environment
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from google.cloud import bigquery
import pandas as pd

# Logger
logger = logging.getLogger(__name__)

# Connection pooling
CONNECTION_POOL = {}
POOL_LOCK = threading.Lock()

# LRU cap on live SSH+MySQL connections. Discovered 2026-05-21: holding more
# than ~50 simultaneous open SSH tunnels triggers a cascade where WP Engine
# rejects new connections with "Lost connection during query." Bounding the
# live count to a few times max_workers keeps us well below that ceiling
# while still amortizing handshake cost across consecutive uses of the same
# site (e.g. the same site appearing 100+ times across the table loop).
#
# WP_POOL_MAX_SIZE is overridable via YAML (pipeline.parallel.connection_pool.max_size)
# through set_connection_pool_config(). Start conservative; ratchet up after
# observing the pool_stats summary at end-of-run.
WP_POOL_MAX_SIZE = 4
# LRU ordering: dict of site_name -> last_used_ts. Python 3.7+ dicts preserve
# insertion order, but we touch entries on every use so we rely on
# move_to_end semantics from OrderedDict.
_POOL_LRU = collections.OrderedDict()
# Counters for end-of-run visibility (reset each run via reset_pool_stats()).
_POOL_STATS = {
    "opens": 0,  # how many times we opened a fresh tunnel+conn
    "hits": 0,  # how many times we returned a cached conn
    "evictions": 0,  # how many LRU evictions to make room
    "max_concurrent": 0,  # peak size of CONNECTION_POOL during the run
}

# Pool heartbeat thread state. After pre-warming connections, WP Engine's
# MySQL idle-timeout (~60-90s observed 2026-05-21) silently drops sessions
# that haven't issued a query. The heartbeat thread runs SELECT 1 against
# every pooled connection every WP_POOL_HEARTBEAT_INTERVAL_S to keep them
# alive. Started lazily by prewarm_site_connections(), stopped by
# cleanup_all_connections().
WP_POOL_HEARTBEAT_INTERVAL_S = 30  # well under the observed 60s timeout
_POOL_HEARTBEAT_THREAD = None
_POOL_HEARTBEAT_STOP = threading.Event()

# ---------------------------------------------------------------------------
# ETL retry / timeout policy
#
# WP Engine SSH endpoints have unpredictable latency - cold sessions can take
# tens of seconds to respond to the first shell command, and a `find` over
# ~/sites can briefly stall under load. The previous defaults (10-30s with
# 2-4s backoff) misclassified these as "wp-config missing" and gave up.
#
# These constants are the single source of truth for every retry layer below:
#   1) Per-shell-command timeouts inside _read_wp_config_once
#   2) SSH reconnect-and-retry around get_wp_config_data
#   3) Table-extraction retry around extract_wordpress_table
#
# Tuned against the SSH-login diagnostic sweep (May 2026): every cohort site
# resolves with these budgets, and total per-site worst-case is bounded.
# ---------------------------------------------------------------------------

# Per-shell-command timeouts (inside one Fabric session)
#
# Empirically (May 2026 diagnostic sweep against 131 sites), individual
# `test -f` and `cat` commands can take 7-15s on cold/contended WP Engine
# sessions, and the slowest end-to-end probe was ~37s. These ceilings give
# 2-3x headroom over the observed maxima.
WP_PROBE_CMD_TIMEOUT_S = 90  # `test -f`, `echo OK`, etc. (was 60)
WP_PROBE_READ_TIMEOUT_S = 120  # `cat wp-config.php` - can be slow on bursty links
WP_PROBE_FIND_TIMEOUT_S = 180  # bounded `find ~/sites` discovery fallback

# wp-config probe retry policy (inside a single Fabric session - same conn)
WP_PROBE_INNER_ATTEMPTS = 3
WP_PROBE_INNER_BACKOFF_S = (4, 8)  # 4s, 8s between in-session retries

# SSH reconnect retry policy (fresh Fabric Connection each time)
WP_SSH_CONNECT_TIMEOUT_S = (
    90  # was 60 - SSH handshake itself can take 30s+ on cold endpoints
)
WP_SSH_RECONNECT_ATTEMPTS = 3
WP_SSH_RECONNECT_BACKOFF_S = (
    15,
    30,
    60,
)  # 15s, 30s, 60s between fresh-connection retries

# Top-level extract retry policy (around a whole table extract)
WP_EXTRACT_MAX_RETRIES = 3
WP_EXTRACT_BACKOFF_S = (60, 120, 180)  # 60s, 120s, 180s

# MySQL connection and query timeouts (through the SSH tunnel)
#
# Previous defaults capped server-side execution at 60s. On large meta tables
# (usermeta, commentmeta) under worker contention, deep-OFFSET chunked reads
# routinely exceed 60s - the server killed the query, client saw "lost
# connection during query", retry hit the same wall, cascade. Bumped to 300s
# (matches read/write_timeout) so the server stops sabotaging legitimate work.
#
# pool_recycle: was 300s. SQLAlchemy recycles connections older than this on
# next use. Under load that meant fresh SSH tunnel setups every 5 minutes -
# precisely the burst pattern WP Engine's gateway rate-limits. 900s gives the
# tunnels enough lifetime to amortize the SSH connect cost.
# MySQL connect timeout: 2026-05-21 diagnostic against bnphforumprd /
# bnaadcprd / postmeta showed the MySQL TCP+handshake through the SSH tunnel
# consistently takes 10-30 seconds, occasionally more, even on healthy sites.
# (Query execution itself is ~0.00s for a COUNT - the bottleneck is purely
# the handshake.) Previous 30s ceiling was barely above the median; the tail
# beyond 30s manifested to the orchestrator as `(2013, 'Lost connection to
# MySQL server during query')` - misleading but actually a handshake timeout.
# 120s gives ~4x margin over observed maxima.
WP_MYSQL_CONNECT_TIMEOUT_S = 120
WP_MYSQL_READ_TIMEOUT_S = 300
WP_MYSQL_WRITE_TIMEOUT_S = 300
WP_MYSQL_MAX_EXEC_TIME_MS = 300000  # 5 minutes, server-side per-query cap
WP_MYSQL_POOL_RECYCLE_S = -1  # never auto-recycle; tunnels close at end of site

# Circuit breaker against WP Engine rate-limit cascades.
#
# Observed pattern (2026-05-20): WP Engine puts our IP in a burst-suppression
# window after ~30-50 sites extract successfully. Every subsequent site's
# tunnel dies mid-query with "Lost connection during query". Each retry burst
# re-arms the penalty timer, so naive fixed-interval retries keep us in the
# penalty box indefinitely.
#
# The breaker is a sliding-window failure-rate detector with exponential
# cool-down. When 3+ of the last 6 sites fail, it opens for an increasing
# pause (5/10/20/40/40 min). Once tripped, per-site max_retries also drops
# to 1 so we stop hammering the gateway while it's angry. After 5 trips, we
# give up entirely and abort the run with a clean SITES TO RETRY list.
#
# All defaults are tunable via the `pipeline.parallel.circuit_breaker` block
# in configs/wordpress.yaml.
WP_CIRCUIT_BREAKER_WINDOW_SIZE = 6  # rolling window of recent site outcomes
WP_CIRCUIT_BREAKER_MIN_SAMPLES = 4  # need at least N sites before tripping
WP_CIRCUIT_BREAKER_FAILURE_RATE = 0.5  # trip when >= 50% of window failed
WP_CIRCUIT_BREAKER_PAUSE_S = 300  # first trip pause: 5 minutes
WP_CIRCUIT_BREAKER_PAUSE_MULTIPLIER = 2.0  # each trip doubles the pause
WP_CIRCUIT_BREAKER_PAUSE_MAX_S = 2400  # 40-minute cap per trip
WP_CIRCUIT_BREAKER_GIVEUP_AFTER_TRIPS = 5  # abort run after this many trips
WP_CIRCUIT_BREAKER_BACKOFF_JITTER_S = 30  # ± random seconds on retry backoff
WP_EXTRACT_MAX_RETRIES_POST_TRIP = 1  # once tripped, retries hurt more than help

# Module-level circuit breaker state. Shared across worker threads so that
# any thread tripping the breaker pauses or aborts ALL threads.
_CIRCUIT_BREAKER_LOCK = threading.Lock()
_CIRCUIT_BREAKER_WINDOW: "collections.deque[bool]" = None  # initialized below
_CIRCUIT_BREAKER_OPEN_UNTIL = 0.0  # epoch seconds; if > time.time(), workers wait
_CIRCUIT_BREAKER_TOTAL_TRIPS = 0  # count of times the breaker has opened this run
_CIRCUIT_BREAKER_ABORTED = threading.Event()  # set when giveup threshold crossed


def _init_circuit_breaker_window() -> None:
    """Initialize/reset the breaker window. Called lazily and by tests."""
    global _CIRCUIT_BREAKER_WINDOW
    import collections as _collections

    _CIRCUIT_BREAKER_WINDOW = _collections.deque(maxlen=WP_CIRCUIT_BREAKER_WINDOW_SIZE)


_init_circuit_breaker_window()


class CircuitBreakerAbortError(RuntimeError):
    """Raised inside a worker when the breaker has hit the give-up threshold.

    The worker re-raises this as a non-transient error so the retry loop
    terminates immediately. extract_site_data wraps it as a structured
    failure with category='circuit_breaker_aborted' so the SITES TO RETRY
    block surfaces it to the operator.
    """


def set_circuit_breaker_config(**overrides) -> None:
    """Allow YAML/runtime overrides of the breaker constants.

    Any key matching a `WP_CIRCUIT_BREAKER_*` constant (case-insensitive
    short name -> full constant) replaces the module-level default. Resets
    the window deque if `window_size` changed.
    """
    # NOTE: no `global` declarations needed -- the constants are rebound via
    # globals()[const_name] = ... below, which writes module scope directly. The
    # previous `global` statements were unused (flake8 F824) because no bare
    # `WP_... =` assignment appears in this function.

    mapping = {
        "window_size": ("WP_CIRCUIT_BREAKER_WINDOW_SIZE", int),
        "min_samples": ("WP_CIRCUIT_BREAKER_MIN_SAMPLES", int),
        "failure_rate": ("WP_CIRCUIT_BREAKER_FAILURE_RATE", float),
        "pause_s": ("WP_CIRCUIT_BREAKER_PAUSE_S", int),
        "pause_multiplier": ("WP_CIRCUIT_BREAKER_PAUSE_MULTIPLIER", float),
        "pause_max_s": ("WP_CIRCUIT_BREAKER_PAUSE_MAX_S", int),
        "giveup_after_trips": ("WP_CIRCUIT_BREAKER_GIVEUP_AFTER_TRIPS", int),
        "backoff_jitter_s": ("WP_CIRCUIT_BREAKER_BACKOFF_JITTER_S", int),
        "post_trip_max_retries": ("WP_EXTRACT_MAX_RETRIES_POST_TRIP", int),
    }
    window_size_changed = False
    for key, value in overrides.items():
        if value is None:
            continue
        if key not in mapping:
            logger.warning(f"circuit-breaker: unknown override key {key!r} ignored")
            continue
        const_name, converter = mapping[key]
        new_value = converter(value)
        globals()[const_name] = new_value
        if const_name == "WP_CIRCUIT_BREAKER_WINDOW_SIZE":
            window_size_changed = True
    if window_size_changed:
        with _CIRCUIT_BREAKER_LOCK:
            _init_circuit_breaker_window()


def reset_circuit_breaker_state() -> None:
    """Reset breaker counters and abort event. Called at start of each run and
    by tests."""
    global _CIRCUIT_BREAKER_OPEN_UNTIL, _CIRCUIT_BREAKER_TOTAL_TRIPS
    with _CIRCUIT_BREAKER_LOCK:
        _init_circuit_breaker_window()
        _CIRCUIT_BREAKER_OPEN_UNTIL = 0.0
        _CIRCUIT_BREAKER_TOTAL_TRIPS = 0
    _CIRCUIT_BREAKER_ABORTED.clear()


def _circuit_breaker_has_ever_tripped() -> bool:
    """Has the breaker opened at least once during this run?"""
    with _CIRCUIT_BREAKER_LOCK:
        return _CIRCUIT_BREAKER_TOTAL_TRIPS > 0


def _circuit_breaker_record_outcome(succeeded: bool) -> None:
    """Append outcome to the rolling window. Trip the breaker if the window
    crosses the failure-rate threshold. Set the abort Event if total trips
    crosses the give-up threshold."""
    global _CIRCUIT_BREAKER_OPEN_UNTIL, _CIRCUIT_BREAKER_TOTAL_TRIPS
    with _CIRCUIT_BREAKER_LOCK:
        _CIRCUIT_BREAKER_WINDOW.append(not succeeded)  # store FAILURE booleans
        n = len(_CIRCUIT_BREAKER_WINDOW)
        failures = sum(_CIRCUIT_BREAKER_WINDOW)

        # Don't trip until we have enough samples to be confident
        if n < WP_CIRCUIT_BREAKER_MIN_SAMPLES:
            return
        # Don't re-trip while still open
        if _CIRCUIT_BREAKER_OPEN_UNTIL > time.time():
            return
        rate = failures / n
        if rate < WP_CIRCUIT_BREAKER_FAILURE_RATE:
            return

        # Trip. Exponential pause based on how many trips we've already had.
        trips_before = _CIRCUIT_BREAKER_TOTAL_TRIPS
        pause = min(
            WP_CIRCUIT_BREAKER_PAUSE_MAX_S,
            int(
                WP_CIRCUIT_BREAKER_PAUSE_S
                * (WP_CIRCUIT_BREAKER_PAUSE_MULTIPLIER**trips_before)
            ),
        )
        _CIRCUIT_BREAKER_OPEN_UNTIL = time.time() + pause
        _CIRCUIT_BREAKER_TOTAL_TRIPS += 1
        new_trips = _CIRCUIT_BREAKER_TOTAL_TRIPS

        logger.warning(
            f"circuit-breaker: TRIPPED (trip {new_trips}/{WP_CIRCUIT_BREAKER_GIVEUP_AFTER_TRIPS}) "
            f"window={failures}/{n} failures, pause={pause}s, "
            f"post-trip max_retries={WP_EXTRACT_MAX_RETRIES_POST_TRIP}, "
            f"will GIVE UP after trip {WP_CIRCUIT_BREAKER_GIVEUP_AFTER_TRIPS}"
        )

        # Hit the give-up threshold? Set the abort flag so workers short-circuit
        # and the run_pipeline loop bails between tables.
        if new_trips >= WP_CIRCUIT_BREAKER_GIVEUP_AFTER_TRIPS:
            _CIRCUIT_BREAKER_ABORTED.set()
            logger.error(
                f"circuit-breaker: ABORT - {new_trips} trips reached giveup threshold; "
                f"remaining sites will be marked 'circuit_breaker_aborted'. "
                f"Recommend waiting >= 30 minutes before re-running this command."
            )


def _circuit_breaker_wait_if_open() -> None:
    """If the breaker is open, sleep until it closes. If the giveup threshold
    has been crossed, raise CircuitBreakerAbortError so the caller terminates
    immediately. Called by every worker before opening a new tunnel."""
    if _CIRCUIT_BREAKER_ABORTED.is_set():
        raise CircuitBreakerAbortError(
            f"circuit-breaker aborted: {WP_CIRCUIT_BREAKER_GIVEUP_AFTER_TRIPS} trips exceeded"
        )
    with _CIRCUIT_BREAKER_LOCK:
        open_until = _CIRCUIT_BREAKER_OPEN_UNTIL
    now = time.time()
    if open_until > now:
        wait_s = int(open_until - now) + 1
        logger.info(f"circuit-breaker: open; sleeping {wait_s}s before next site")
        time.sleep(wait_s)
        # Re-check after sleeping - the giveup threshold may have been crossed
        # by another worker while we were waiting.
        if _CIRCUIT_BREAKER_ABORTED.is_set():
            raise CircuitBreakerAbortError(
                f"circuit-breaker aborted: {WP_CIRCUIT_BREAKER_GIVEUP_AFTER_TRIPS} trips exceeded"
            )


def find_free_port() -> Tuple[int, socket.socket]:
    """
    Find a free port and return both the port number and the socket.
    Fix #7: Keep socket open to prevent port reuse race condition.
    Caller must close the socket after using the port.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    return port, s


def start_ssh_tunnel(
    ssh_host: str,
    ssh_user: str,
    ssh_keyfile: str,
    mysql_host: str,
    mysql_port: int,
    local_port: int,
    port_socket: socket.socket = None,
) -> subprocess.Popen:
    """Open an SSH port-forwarding tunnel and wait until it's actually live.

    Previously the code did `time.sleep(1)` then `poll()`. On contended WP
    Engine endpoints the SSH handshake can take 3-10s, so a 1-second wait
    returned a 'not-yet-failed-but-not-yet-ready' tunnel. MySQL then connected
    to the local port and got Errno 111 because nothing was listening yet,
    misclassified as a transient MySQL error and retried 60s later.

    We now poll for the local port to accept a TCP connect, up to
    WP_SSH_CONNECT_TIMEOUT_S. If the ssh subprocess dies during the wait,
    we surface its stderr instead of "Errno 111".
    """
    tunnel_cmd = [
        "ssh",
        "-i",
        ssh_keyfile,
        "-L",
        f"{local_port}:{mysql_host}:{mysql_port}",
        "-N",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "LogLevel=ERROR",
        "-o",
        "ConnectTimeout=30",
        "-o",
        "TCPKeepAlive=yes",
        "-o",
        "ServerAliveInterval=60",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "ExitOnForwardFailure=yes",  # die immediately if port forward setup fails
        f"{ssh_user}@{ssh_host}",
    ]

    tunnel_process = subprocess.Popen(
        tunnel_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    # Close the port reservation socket now that tunnel is started
    # This releases the port for the SSH tunnel to bind to
    if port_socket:
        try:
            port_socket.close()
        except Exception:
            pass

    # Wait until the local port actually accepts a TCP connection (true
    # readiness signal), or the SSH subprocess dies (definite failure).
    deadline = time.time() + WP_SSH_CONNECT_TIMEOUT_S
    while time.time() < deadline:
        # Has SSH already exited?
        if tunnel_process.poll() is not None:
            try:
                _stdout, stderr = tunnel_process.communicate(timeout=2)
            except Exception:
                stderr = b""
            err_text = (stderr or b"").decode("utf-8", errors="ignore").strip()[:200]
            raise RuntimeError(
                f"SSH tunnel exited before becoming ready: {err_text or 'no error output'}"
            )

        # Try to connect to the local port. If it accepts, the tunnel is live.
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.settimeout(1.0)
        try:
            probe.connect(("127.0.0.1", local_port))
            probe.close()
            return tunnel_process
        except (ConnectionRefusedError, OSError, socket.timeout):
            probe.close()
            time.sleep(0.5)

    # Timed out waiting for the tunnel to come up. Kill it cleanly so we don't
    # leak the subprocess, and surface a useful error.
    try:
        tunnel_process.terminate()
        tunnel_process.wait(timeout=2)
    except Exception:
        try:
            tunnel_process.kill()
        except Exception:
            pass
    raise RuntimeError(
        f"SSH tunnel never became ready on 127.0.0.1:{local_port} within {WP_SSH_CONNECT_TIMEOUT_S}s"
    )


def stop_ssh_tunnel(tunnel_process: subprocess.Popen) -> None:
    if tunnel_process:
        try:
            tunnel_process.terminate()
            tunnel_process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            tunnel_process.kill()
            tunnel_process.wait()


def get_available_sites(config: Dict[str, Any]) -> List[str]:
    # Get list of available WordPress sites from discovery API with fallback
    import requests

    # Try discovery API first
    api_url = config.get("source", {}).get("discovery_api") or config.get(
        "sites", {}
    ).get("discovery_api")
    if api_url:
        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            sites = []

            # Get environment filter (default: 'prd')
            env_filter = config.get("sites", {}).get("filter_suffix", "prd")

            for topic in data.get("topics", []):
                name = topic.get("name", "")
                if name.endswith(env_filter):
                    sites.append(name)

            if sites:
                logger.info(f"Discovered {len(sites)} sites from API")
                return sites
            else:
                logger.warning(
                    f"Discovery API returned 0 sites matching filter '{env_filter}'"
                )
        except Exception as e:
            logger.warning(f"Failed to discover sites from API: {e}")

    # Fallback to configuration
    fallback_sites = config.get("sites", {}).get("fallback", [])
    if fallback_sites:
        logger.info(f"Using fallback sites from config: {fallback_sites}")
        return fallback_sites

    logger.error("No sites discovered and no fallback sites configured")
    return []


def extract_wp_constant(
    content: str, constant_name: str, default: Optional[str] = None
) -> str:
    # Fix #5: Prevent catastrophic backtracking by limiting content size and using non-greedy match
    # Also add timeout protection by limiting search space
    pattern = (
        r"define\(\s*['\"]"
        + re.escape(constant_name)
        + r"['\"]\s*,\s*(['\"])([^'\"]{0,1000})\1\s*\)\s*;"
    )
    match = re.search(pattern, content[:50000])  # Limit to first 50KB of config file

    if not match:
        if default is not None:
            return default
        raise RuntimeError(f"Could not find {constant_name}")

    return match.group(2)


def extract_wp_variable(content: str, var_name: str, default: str = "wp_") -> str:
    pattern = r"\$" + re.escape(var_name) + r"\s*=\s*['\"]([^'\"]*)['\"]"
    match = re.search(pattern, content)

    if match:
        return match.group(1)
    return default


def _sanitize_sql_ident(ident: str) -> str:
    # Strip characters that have no business appearing in a backtick-quoted
    # MySQL identifier we control. Defends the join-on-parent path from a
    # malformed YAML entry; not a substitute for parameterized queries.
    return ident.replace("`", "").replace(";", "").replace("--", "").replace(" ", "")


def apply_field_truncation(
    row: Dict[str, Any], table_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Clip over-long free-text fields per the table's ``field_truncation`` map.

    Fields declared under ``json_fields`` are NEVER truncated: a JSON payload
    clipped at an arbitrary character boundary is corrupted mid-token and
    becomes unparseable (BS-51 -- Gravity Forms display_meta cut at 10k chars).
    Guards against a whole class of "silent JSON corruption" bugs, not just the
    one field that surfaced it. Mutates and returns ``row``.
    """
    truncation = table_config.get("field_truncation", {})
    json_fields = set(table_config.get("json_fields", []))
    for field, max_length in truncation.items():
        if field in json_fields:
            continue
        value = row.get(field)
        if value and len(str(value)) > max_length:
            row[field] = str(value)[:max_length]
    return row


def get_effective_table_prefix(table_prefix: str, table_name: str) -> str:
    """
    Adjust table prefix based on table type for forum sites.

    WordPress multisite/BuddyPress forum sites use a 'com_' prefix for most tables,
    but users/usermeta tables are shared across the network and don't use the 'com_' prefix.

    Example:
        Forum site has table_prefix = 'com_ws9defr5p_' in wp-config.php
        - Posts table: com_ws9defr5p_posts (uses full prefix)
        - Users table: ws9defr5p_users (strips 'com_' prefix)

    Args:
        table_prefix: The prefix from wp-config.php (e.g., 'com_ws9defr5p_')
        table_name: The base table name (e.g., 'users', 'posts')

    Returns:
        The effective prefix to use for this table
    """
    # User-related tables are shared across forum network and don't use 'com_' prefix
    USER_SHARED_TABLES = ["users", "usermeta"]

    if table_prefix.startswith("com_") and table_name in USER_SHARED_TABLES:
        # Strip 'com_' prefix for user tables on forum sites
        return table_prefix.removeprefix("com_")

    return table_prefix


class WpConfigNotFoundError(RuntimeError):
    """Raised when wp-config.php cannot be located on a WordPress site.

    A WordPress install with no wp-config.php is unrunnable: every downstream
    step (DB credentials, table prefix, MySQL tunnel target) needs it. Failing
    loudly here is correct - if we silently skip the site, BigQuery slowly
    drains under the merge-and-delete pattern.
    """


def _read_wp_config_once(
    conn: Connection, site_name: str
) -> Tuple[Optional[str], Optional[str], list]:
    """One attempt to locate and read wp-config.php on the remote site.

    Returns (content, path_used, attempts) where attempts is a list of
    (path, error) tuples for diagnostic purposes. Returns (None, None, attempts)
    if every path failed - caller decides whether to retry or raise.

    Per-path classification:
      - "not present"   (test -f returned non-zero): file genuinely doesn't exist
      - "transient"     (CommandTimedOut, SSH-level error): worth retrying
      - "cat failed"    (read error after test succeeded): worth retrying

    If all known-good paths come back "not present", does one bounded
    `find ~/sites -maxdepth 3 -name wp-config.php` as a discovery fallback -
    same logic that the scripts/check_wp_ssh_login.py diagnostic uses. When
    the find returns exactly one match, that path is read. This handles WP
    Engine installs where the site directory has been renamed.
    """
    from invoke.exceptions import CommandTimedOut, UnexpectedExit

    candidate_paths = [
        f"sites/{site_name}/wp-config.php",
        f"~/sites/{site_name}/wp-config.php",
        "wp-config.php",
    ]
    attempts = []
    saw_transient = False

    for path in candidate_paths:
        try:
            test_result = conn.run(
                f'test -f "{path}"',
                hide=True,
                warn=True,
                timeout=WP_PROBE_CMD_TIMEOUT_S,
            )
        except CommandTimedOut:
            attempts.append((path, "transient: probe timed out"))
            saw_transient = True
            # If the probe is timing out, the underlying SSH session is likely
            # wedged. Don't waste budget on the next path - signal retry.
            return None, None, attempts
        except Exception as e:
            attempts.append((path, f"transient: {type(e).__name__}"))
            saw_transient = True
            continue

        if not test_result.ok:
            attempts.append((path, "not present"))
            continue

        try:
            cat_result = conn.run(
                f'cat "{path}"',
                hide=True,
                warn=True,
                timeout=WP_PROBE_READ_TIMEOUT_S,
            )
        except CommandTimedOut:
            attempts.append((path, "transient: cat timed out"))
            return None, None, attempts
        except Exception as e:
            attempts.append((path, f"transient: cat {type(e).__name__}"))
            continue

        if cat_result.ok:
            return cat_result.stdout, path, attempts
        attempts.append(
            (path, f"cat failed: {(cat_result.stderr or '').strip()[:100]}")
        )

    # Static paths exhausted. If we never saw a transient, none of the known
    # paths exist - try the `find` fallback to discover non-standard locations
    # before giving up. Same pattern as the SSH diagnostic.
    if not saw_transient:
        try:
            find_result = conn.run(
                "find ~/sites -maxdepth 3 -name wp-config.php -type f 2>/dev/null | head -5",
                hide=True,
                warn=True,
                timeout=WP_PROBE_FIND_TIMEOUT_S,
            )
            hits = [
                p.strip() for p in (find_result.stdout or "").splitlines() if p.strip()
            ]
        except CommandTimedOut:
            attempts.append(("find ~/sites", "transient: find timed out"))
            return None, None, attempts
        except Exception as e:
            attempts.append(("find ~/sites", f"transient: find {type(e).__name__}"))
            return None, None, attempts

        if len(hits) == 1:
            path = hits[0]
            logger.warning(f"{site_name}: wp-config at non-standard path {path}")
            try:
                cat_result = conn.run(
                    f'cat "{path}"',
                    hide=True,
                    warn=True,
                    timeout=WP_PROBE_READ_TIMEOUT_S,
                )
                if cat_result.ok:
                    return cat_result.stdout, path, attempts
                attempts.append(
                    (path, f"cat failed: {(cat_result.stderr or '').strip()[:100]}")
                )
            except Exception as e:
                attempts.append((path, f"transient: cat {type(e).__name__}"))
        elif len(hits) > 1:
            attempts.append(
                (
                    "find ~/sites",
                    f"ambiguous - {len(hits)} matches: {hits[:3]}",
                )
            )
        # len(hits) == 0: leave attempts as-is; caller will see all "not present"

    return None, None, attempts


def get_wp_config_data(conn: Connection, site_name: str) -> Dict[str, Any]:
    # In-session retry: retries reuse the SAME Fabric Connection. Use this
    # for transient errors on a connection that may still recover (the first
    # command after connect occasionally hangs and a fresh command works).
    # If the SSH session itself is wedged, the outer _probe_wp_config_via_ssh
    # layer opens a NEW Connection and tries again.
    #
    # If every in-session attempt comes back "not present" (no transients),
    # the file genuinely doesn't exist - bail immediately, don't waste retries.
    all_attempts = []
    content = None
    found_path = None
    last_had_transient = False

    for attempt in range(1, WP_PROBE_INNER_ATTEMPTS + 1):
        content, found_path, attempts = _read_wp_config_once(conn, site_name)
        all_attempts.append((attempt, attempts))
        if content is not None:
            if attempt > 1:
                logger.info(f"{site_name}: wp-config located on attempt {attempt}")
            break

        had_transient = any("transient" in why for _p, why in attempts)
        last_had_transient = had_transient
        if not had_transient:
            logger.debug(f"{site_name}: no transients, all paths absent - not retrying")
            break

        if attempt < WP_PROBE_INNER_ATTEMPTS:
            # Backoff vector is 0-indexed; clamp to the last value if attempts
            # exceeds the configured backoff length.
            idx = min(attempt - 1, len(WP_PROBE_INNER_BACKOFF_S) - 1)
            backoff = WP_PROBE_INNER_BACKOFF_S[idx]
            logger.info(
                f"{site_name}: wp-config probe transient on attempt {attempt}/{WP_PROBE_INNER_ATTEMPTS}, retrying in {backoff}s"
            )
            time.sleep(backoff)

    if content is None:
        # Build error message from the most recent attempt for clarity
        last_attempts = all_attempts[-1][1] if all_attempts else []
        detail = (
            "; ".join(f"{p} -> {why}" for p, why in last_attempts) or "no paths tried"
        )
        category = (
            "transient SSH after retries" if last_had_transient else "file not present"
        )
        raise WpConfigNotFoundError(
            f"wp-config.php not located for {site_name} ({category}): {detail}"
        )

    # Parse the config we found
    php_start = content.find("<?php")
    if php_start > 0:
        content = content[php_start:]

    db_name = extract_wp_constant(content, "DB_NAME")
    db_user = extract_wp_constant(content, "DB_USER")
    db_password = extract_wp_constant(content, "DB_PASSWORD")
    db_host_raw = extract_wp_constant(content, "DB_HOST", default="localhost")

    if ":" in db_host_raw:
        db_host, db_port = db_host_raw.split(":")
        db_port = int(db_port)
    else:
        db_host = db_host_raw
        db_port = 3306

    table_prefix = extract_wp_variable(content, "table_prefix", default="wp_")

    return {
        "name": db_name,
        "user": db_user,
        "password": db_password,
        "host": db_host,
        "port": db_port,
        "table_prefix": table_prefix,
    }


# ---------------------------------------------------------------------------
# wp-config credentials cache
#
# wp-config.php credentials (DB name/user/password/host/port, table_prefix)
# don't change between runs. Re-probing them over SSH on every table extract
# means 9-30 tables x 131 sites = thousands of SSH round-trips per run, each
# one a chance for WP Engine SSH latency to mis-classify as "wp-config not found".
#
# We cache in two layers:
#   - In-memory dict for the current process (handles the per-run multiplication)
#   - On-disk JSON at .wp_credentials.json (handles re-runs - typically a fresh
#     run doesn't need to SSH for wp-config at all)
#
# Cache entries carry a fetched_at timestamp; entries older than CACHE_TTL_DAYS
# are refreshed. SSH probes only happen for: new sites, sites whose cached
# entry has expired, or explicit cache invalidation.
# ---------------------------------------------------------------------------

WP_CREDS_CACHE_PATH = Path(__file__).parent.parent / ".wp_credentials.json"
WP_CREDS_TTL_DAYS = 30
_WP_CREDS_MEM: Dict[str, Dict[str, Any]] = {}  # site_name -> {creds, fetched_at}
_WP_CREDS_FILE_LOCK = threading.Lock()


def _load_wp_creds_cache() -> Dict[str, Dict[str, Any]]:
    """Read on-disk cache. Returns {} on missing/corrupt file."""
    try:
        if not WP_CREDS_CACHE_PATH.exists():
            return {}
        with open(WP_CREDS_CACHE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning(
            f"wp-config cache read failed ({type(e).__name__}: {e}); starting empty"
        )
        return {}


def _save_wp_creds_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    """Atomic write of on-disk cache (write to tmp then rename)."""
    try:
        WP_CREDS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = WP_CREDS_CACHE_PATH.with_suffix(".json.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
        tmp_path.replace(WP_CREDS_CACHE_PATH)
    except Exception as e:
        logger.warning(f"wp-config cache write failed: {type(e).__name__}: {e}")


def _get_cached_wp_creds(site_name: str) -> Optional[Dict[str, Any]]:
    """Check in-memory then on-disk for valid creds. Returns None if absent/expired."""
    # In-memory hit
    if site_name in _WP_CREDS_MEM:
        entry = _WP_CREDS_MEM[site_name]
        if _entry_is_fresh(entry):
            return entry["creds"]

    # Fall back to on-disk
    with _WP_CREDS_FILE_LOCK:
        on_disk = _load_wp_creds_cache()
    entry = on_disk.get(site_name)
    if entry and _entry_is_fresh(entry):
        _WP_CREDS_MEM[site_name] = entry  # promote to memory cache
        return entry["creds"]
    return None


def _entry_is_fresh(entry: Dict[str, Any]) -> bool:
    try:
        fetched = datetime.fromisoformat(entry["fetched_at"])
    except Exception:
        return False
    age_days = (datetime.now() - fetched).total_seconds() / 86400.0
    return age_days < WP_CREDS_TTL_DAYS


def _store_wp_creds(site_name: str, creds: Dict[str, Any]) -> None:
    """Persist fresh creds to in-memory and on-disk caches."""
    entry = {"creds": creds, "fetched_at": datetime.now().isoformat()}
    _WP_CREDS_MEM[site_name] = entry
    with _WP_CREDS_FILE_LOCK:
        on_disk = _load_wp_creds_cache()
        on_disk[site_name] = entry
        _save_wp_creds_cache(on_disk)


def invalidate_wp_creds(site_name: str) -> None:
    """Remove cached creds for a site (e.g. after auth failure forces re-probe)."""
    _WP_CREDS_MEM.pop(site_name, None)
    with _WP_CREDS_FILE_LOCK:
        on_disk = _load_wp_creds_cache()
        if site_name in on_disk:
            del on_disk[site_name]
            _save_wp_creds_cache(on_disk)


def _probe_wp_config_via_ssh(site_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Open a fresh SSH connection and read wp-config. Used only on cache miss.

    Retries open a NEW connection each time - retrying on the same wedged
    SSH session doesn't help, and that was the bug in earlier retry attempts.
    Total worst-case wall-clock: ~3 minutes per site if every attempt times
    out (60s probe + 8s/16s/32s backoff).
    """
    conn_patterns = config.get("connection_patterns", {}).get("wpengine", {})
    ssh_host = conn_patterns.get(
        "ssh_host_pattern", "{site_name}.ssh.wpengine.net"
    ).format(site_name=site_name)
    ssh_user = conn_patterns.get("ssh_user_pattern", "{site_name}").format(
        site_name=site_name
    )
    ssh_key_path = conn_patterns.get("ssh_key") or os.getenv("WP_SSH_KEY_PATH")
    if not ssh_key_path:
        raise ValueError("SSH key path not configured")
    ssh_key_path = os.path.expanduser(ssh_key_path)

    import logging as py_logging

    paramiko_logger = py_logging.getLogger("paramiko")
    old_paramiko_level = paramiko_logger.level
    paramiko_logger.setLevel(py_logging.ERROR)

    last_exc: Optional[Exception] = None
    try:
        for attempt in range(1, WP_SSH_RECONNECT_ATTEMPTS + 1):
            try:
                # Fabric's SSHClient defaults to AutoAddPolicy, so unknown host
                # keys are auto-accepted - no fingerprint prompt blocks first-touch
                # WP Engine sites. The ssh tunnel subprocess (start_ssh_tunnel)
                # also passes -o StrictHostKeyChecking=no for the same reason.
                with Connection(
                    host=ssh_host,
                    user=ssh_user,
                    connect_timeout=WP_SSH_CONNECT_TIMEOUT_S,
                    connect_kwargs={"key_filename": ssh_key_path},
                ) as fabric_conn:
                    creds = get_wp_config_data(fabric_conn, site_name)
                    if attempt > 1:
                        logger.info(
                            f"{site_name}: wp-config probe succeeded on attempt {attempt}"
                        )
                    return creds
            except WpConfigNotFoundError as e:
                # WpConfigNotFoundError already distinguishes file-not-present
                # from transient-after-retries. Don't outer-retry on "file not
                # present" - that won't change. Re-raise to caller.
                if "file not present" in str(e):
                    raise
                last_exc = e
            except Exception as e:
                last_exc = e

            if attempt < WP_SSH_RECONNECT_ATTEMPTS:
                idx = min(attempt - 1, len(WP_SSH_RECONNECT_BACKOFF_S) - 1)
                backoff = WP_SSH_RECONNECT_BACKOFF_S[idx]
                logger.warning(
                    f"{site_name}: wp-config probe failed (attempt {attempt}/{WP_SSH_RECONNECT_ATTEMPTS}): {str(last_exc)[:120]}; reconnecting in {backoff}s"
                )
                time.sleep(backoff)
    finally:
        paramiko_logger.setLevel(old_paramiko_level)

    # All outer attempts failed
    raise (
        last_exc
        if last_exc
        else RuntimeError(
            f"wp-config probe failed for {site_name} with no captured exception"
        )
    )


def get_wp_credentials(
    site_name: str, config: Dict[str, Any], force_refresh: bool = False
) -> Dict[str, Any]:
    """Public entrypoint: return wp-config credentials for a site, using cache.

    Falls back to SSH probe only on cache miss or force_refresh. After a
    successful SSH probe, persists to both in-memory and on-disk caches.
    """
    if not force_refresh:
        cached = _get_cached_wp_creds(site_name)
        if cached is not None:
            logger.debug(
                f"{site_name}: wp-config from cache (db={cached['name']}, prefix={cached['table_prefix']})"
            )
            return cached

    logger.info(f"{site_name}: probing wp-config via SSH (cache miss)")
    creds = _probe_wp_config_via_ssh(site_name, config)
    _store_wp_creds(site_name, creds)
    return creds


def ssh_connect(site_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    conn_patterns = config.get("connection_patterns", {}).get("wpengine", {})

    ssh_host = conn_patterns.get(
        "ssh_host_pattern", "{site_name}.ssh.wpengine.net"
    ).format(site_name=site_name)
    ssh_user = conn_patterns.get("ssh_user_pattern", "{site_name}").format(
        site_name=site_name
    )
    ssh_key_path = conn_patterns.get("ssh_key") or os.getenv("WP_SSH_KEY_PATH")

    if not ssh_key_path:
        raise ValueError("SSH key path not configured")

    ssh_key_path = os.path.expanduser(ssh_key_path)

    logger.debug(f"Connecting to {ssh_host} as {ssh_user}")

    # Fix #10: Don't suppress ALL stderr globally - only suppress Paramiko warnings
    # Use logging configuration instead of suppressing stderr
    import logging as py_logging

    paramiko_logger = py_logging.getLogger("paramiko")
    old_paramiko_level = paramiko_logger.level
    paramiko_logger.setLevel(py_logging.ERROR)

    try:
        # Use cached wp-config credentials when possible; only SSH for wp-config
        # on cache miss. The MySQL tunnel still needs an SSH connection, but
        # that opens once per site and is reused across all tables on that site
        # via get_pooled_connection.
        db_config = get_wp_credentials(site_name, config)

        # Restore Paramiko logging level
        paramiko_logger.setLevel(old_paramiko_level)

        logger.debug(
            f"Database: {db_config['name']}, Prefix: {db_config['table_prefix']}"
        )

        # Fix #7: Get port and socket together to prevent race condition
        local_port, port_socket = find_free_port()
        logger.debug(f"Starting SSH tunnel on port {local_port}...")

        tunnel_process = start_ssh_tunnel(
            ssh_host,
            ssh_user,
            ssh_key_path,
            db_config["host"],
            db_config["port"],
            local_port,
            port_socket=port_socket,
        )

        logger.debug(f"SSH tunnel established")

        connection_string = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@127.0.0.1:{local_port}/{db_config['name']}"
            f"?charset=utf8mb4"
            f"&connect_timeout={WP_MYSQL_CONNECT_TIMEOUT_S}"
            f"&read_timeout={WP_MYSQL_READ_TIMEOUT_S}"
            f"&write_timeout={WP_MYSQL_WRITE_TIMEOUT_S}"
        )

        logger.debug(f"Creating MySQL engine...")
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_recycle=WP_MYSQL_POOL_RECYCLE_S,
            echo=False,
            connect_args={
                "read_timeout": WP_MYSQL_READ_TIMEOUT_S,
                "write_timeout": WP_MYSQL_WRITE_TIMEOUT_S,
            },
        )

        logger.debug(f"Connection ready")

        return {
            "engine": engine,
            "tunnel": tunnel_process,
            "table_prefix": db_config["table_prefix"],
            "site_name": site_name,
        }

    except Exception as e:
        # Restore Paramiko logging level on error
        paramiko_logger.setLevel(old_paramiko_level)

        # If the failure looks credential-related (MySQL auth, host not found),
        # invalidate the cache so the next attempt forces a fresh wp-config
        # probe. Transient SSH/network errors do NOT invalidate.
        err_str = str(e).lower()
        cred_failure_signs = (
            "access denied",
            "1045",
            "authentication",
            "1044",
            "unknown database",
        )
        if any(s in err_str for s in cred_failure_signs):
            logger.warning(
                f"{site_name}: credential-related failure - invalidating cached wp-config and retrying on next attempt"
            )
            invalidate_wp_creds(site_name)

        raise Exception(f"Failed to connect to {site_name}: {e}")


def _evict_lru_for_room(needed: int = 1) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Caller holds POOL_LOCK. If the pool would exceed WP_POOL_MAX_SIZE after
    adding `needed` new entries, pop the oldest entries from _POOL_LRU and
    return them as (site_name, conn_info) pairs. The CALLER must close them
    OUTSIDE the lock - this function only mutates the in-memory pool state.
    """
    evicted: List[Tuple[str, Dict[str, Any]]] = []
    while len(CONNECTION_POOL) + needed > WP_POOL_MAX_SIZE and _POOL_LRU:
        oldest_site, _ = _POOL_LRU.popitem(last=False)
        if oldest_site in CONNECTION_POOL:
            conn_info = CONNECTION_POOL.pop(oldest_site)
            evicted.append((oldest_site, conn_info))
            _POOL_STATS["evictions"] += 1
    return evicted


def get_pooled_connection(site_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get a pooled connection for a site, creating it if necessary.
    Thread-safe with proper double-check locking. Holds at most
    WP_POOL_MAX_SIZE live SSH+MySQL connections; evicts least-recently-used
    entries when full.
    """
    # 1) Fast path: cache hit. Touch LRU bookkeeping and return.
    with POOL_LOCK:
        if site_name in CONNECTION_POOL:
            conn_info = CONNECTION_POOL[site_name]
            _POOL_LRU.move_to_end(site_name)  # mark MRU
            _POOL_STATS["hits"] += 1
            # Health check OUTSIDE the lock (slow)

    if site_name in CONNECTION_POOL:
        conn_info = CONNECTION_POOL[site_name]
        if not test_connection_health(conn_info, site_name):
            logger.warning(
                f"Unhealthy connection detected for {site_name}, recreating..."
            )
            with POOL_LOCK:
                if site_name in CONNECTION_POOL:
                    del CONNECTION_POOL[site_name]
                _POOL_LRU.pop(site_name, None)
            _cleanup_connection_unlocked(conn_info, site_name)
            # Fall through to create new
        else:
            return conn_info

    # 2) Cache miss: open a new connection OUTSIDE the lock (slow, 10-30s).
    logger.debug(f"pool: opening new connection for {site_name}")
    conn_info = ssh_connect(site_name, config)

    # 3) Re-acquire lock to publish + maybe evict.
    to_close_outside_lock: List[Tuple[str, Dict[str, Any]]] = []
    existing_conn = None
    with POOL_LOCK:
        if site_name in CONNECTION_POOL:
            # Race: another worker beat us to it. Drop our duplicate.
            existing_conn = CONNECTION_POOL[site_name]
            _POOL_LRU.move_to_end(site_name)
            to_close_outside_lock.append((site_name + "(dup)", conn_info))
        else:
            # Make room before publishing the new entry.
            to_close_outside_lock.extend(_evict_lru_for_room(needed=1))
            CONNECTION_POOL[site_name] = conn_info
            _POOL_LRU[site_name] = time.time()
            _POOL_STATS["opens"] += 1
            if len(CONNECTION_POOL) > _POOL_STATS["max_concurrent"]:
                _POOL_STATS["max_concurrent"] = len(CONNECTION_POOL)
            pool_size_now = len(CONNECTION_POOL)
            evict_count = len(
                [x for x in to_close_outside_lock if not x[0].endswith("(dup)")]
            )
            if evict_count:
                logger.debug(
                    f"pool: opened {site_name} (pool={pool_size_now}/{WP_POOL_MAX_SIZE}, "
                    f"evicted {evict_count} LRU: "
                    f"{[s for s, _ in to_close_outside_lock if not s.endswith('(dup)')]})"
                )
            else:
                logger.debug(
                    f"pool: opened {site_name} (pool={pool_size_now}/{WP_POOL_MAX_SIZE})"
                )

    # 4) Close evicted/duplicate connections OUTSIDE the lock (slow).
    for evicted_site, evicted_conn in to_close_outside_lock:
        try:
            _cleanup_connection_unlocked(evicted_conn, evicted_site)
        except Exception as e:
            logger.warning(f"pool: error closing {evicted_site}: {e}")

    return existing_conn if existing_conn is not None else conn_info


def _cleanup_connection_unlocked(conn_info: Dict[str, Any], site_name: str):
    """
    Internal helper to cleanup connection resources.
    Called with connection info already removed from pool.
    Does NOT acquire lock - caller must handle synchronization.
    """
    try:
        conn_info["engine"].dispose()
    except Exception as e:
        logger.warning(f"Failed to dispose engine for {site_name}: {e}")

    try:
        stop_ssh_tunnel(conn_info["tunnel"])
    except Exception as e:
        logger.warning(f"Failed to stop SSH tunnel for {site_name}: {e}")


def cleanup_pooled_connection(site_name: str):
    """
    Clean up a pooled connection.
    Thread-safe - acquires POOL_LOCK internally.
    """
    with POOL_LOCK:
        if site_name not in CONNECTION_POOL:
            _POOL_LRU.pop(site_name, None)
            return
        conn_info = CONNECTION_POOL[site_name]
        del CONNECTION_POOL[site_name]
        _POOL_LRU.pop(site_name, None)

    _cleanup_connection_unlocked(conn_info, site_name)


def cleanup_all_connections():
    """
    Clean up all pooled connections.
    Thread-safe - acquires POOL_LOCK internally.
    """
    # Stop the heartbeat thread first so it doesn't race against the cleanup
    # by trying to ping connections we're tearing down.
    stop_pool_heartbeat()

    with POOL_LOCK:
        connections_to_cleanup = list(CONNECTION_POOL.items())
        CONNECTION_POOL.clear()
        _POOL_LRU.clear()

    for site_name, conn_info in connections_to_cleanup:
        _cleanup_connection_unlocked(conn_info, site_name)


def reset_pool_stats() -> None:
    """Zero the pool-stats counters. Call at start of each run."""
    with POOL_LOCK:
        for k in _POOL_STATS:
            _POOL_STATS[k] = 0


def get_pool_stats_snapshot() -> Dict[str, Any]:
    """Return a copy of current pool stats for logging."""
    with POOL_LOCK:
        snap = dict(_POOL_STATS)
        snap["current_size"] = len(CONNECTION_POOL)
        snap["cap"] = WP_POOL_MAX_SIZE
        total = snap["opens"] + snap["hits"]
        snap["hit_rate"] = (snap["hits"] / total) if total else 0.0
        return snap


def set_connection_pool_config(max_size: Optional[int] = None) -> None:
    """Override the LRU pool cap from YAML at run startup."""
    global WP_POOL_MAX_SIZE
    if max_size is not None and max_size >= 1:
        WP_POOL_MAX_SIZE = int(max_size)
        logger.info(f"pool: WP_POOL_MAX_SIZE set to {WP_POOL_MAX_SIZE} from YAML")


def prewarm_site_connections(
    sites: List[str],
    config: Dict[str, Any],
    max_workers: int = 8,
) -> Dict[str, str]:
    """Open SSH tunnel + MySQL engine for each site IN PARALLEL, upfront.

    Once warmed, the existing per-table extraction loop reuses these
    connections via CONNECTION_POOL. The slow part of WordPress extraction
    is the MySQL handshake through the SSH tunnel (observed at 10-30s per
    site against WP Engine). Pre-warming pays that cost in parallel ONCE
    instead of letting it block table-extraction serially.

    Returns a dict mapping failed_site -> short error string. Successful
    sites land in CONNECTION_POOL ready for use.

    No retries during pre-warm - if a site can't be reached, the failure
    is recorded and the run continues without that site. The per-table
    extraction would just re-discover the same failure for that site;
    paying the cost twice is pointless.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not sites:
        return {}

    logger.info(
        f"prewarm: opening {len(sites)} site connections in parallel "
        f"(workers={max_workers})..."
    )
    t_start = time.time()

    def warm_one(site_name: str) -> Tuple[str, Optional[str]]:
        """Open the connection AND verify it with a real query.

        get_pooled_connection() creates the SSH tunnel + SQLAlchemy engine,
        but the engine connects lazily - the actual MySQL handshake doesn't
        happen until the first query. Without forcing a query here, "pre-warm
        complete" can be a lie: the engine is created but the MySQL TCP
        handshake hasn't happened yet, so cost+failure shifts back into the
        table loop. We run SELECT 1 to force the handshake and confirm the
        connection actually works end-to-end.

        Returns (site, None) on success or (site, error_msg) on failure.
        """
        try:
            conn_info = get_pooled_connection(site_name, config)
            engine = conn_info.get("engine")
            if engine is None:
                return site_name, "no engine returned"
            # Force the MySQL handshake by running a trivial query. If
            # WP Engine refuses or drops the connection, we catch it now
            # rather than 5 minutes later during a chunk read.
            with engine.connect() as c:
                c.execute(text("SELECT 1")).fetchone()
            return site_name, None
        except Exception as e:
            # Connection failed verification. Make sure we don't leave a
            # half-dead entry in CONNECTION_POOL - subsequent code paths
            # would think it's healthy. cleanup_pooled_connection is a
            # no-op if the site never made it into the pool.
            try:
                cleanup_pooled_connection(site_name)
            except Exception:
                pass
            return site_name, f"{type(e).__name__}: {str(e)[:150]}"

    failures: Dict[str, str] = {}
    n_success = 0
    n_done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(warm_one, s): s for s in sites}
        for fut in as_completed(futures):
            site_name, err = fut.result()
            n_done += 1
            if err is None:
                n_success += 1
                # Quiet success - one progress line every 20 to avoid log spam
                if n_done % 20 == 0 or n_done == len(sites):
                    logger.info(f"prewarm: {n_done}/{len(sites)} sites warmed")
            else:
                failures[site_name] = err
                logger.warning(f"prewarm: {site_name} FAILED - {err}")

    elapsed = time.time() - t_start
    logger.info(
        f"prewarm complete: {n_success}/{len(sites)} sites warmed in {elapsed:.1f}s "
        f"({len(failures)} failed)"
    )

    # Start the heartbeat thread so pre-warmed connections don't go stale
    # while waiting their turn through the table loop. Without this, sites
    # at the tail of the queue get hit by WP Engine's ~60s idle-timeout.
    if n_success > 0:
        start_pool_heartbeat()

    return failures


def start_pool_heartbeat() -> None:
    """Start the background heartbeat thread that pings every pooled
    connection periodically. Idempotent - calling twice is a no-op."""
    global _POOL_HEARTBEAT_THREAD
    if _POOL_HEARTBEAT_THREAD is not None and _POOL_HEARTBEAT_THREAD.is_alive():
        return  # already running

    _POOL_HEARTBEAT_STOP.clear()
    _POOL_HEARTBEAT_THREAD = threading.Thread(
        target=_pool_heartbeat_loop,
        name="wp-pool-heartbeat",
        daemon=True,  # don't block process exit if the run is interrupted
    )
    _POOL_HEARTBEAT_THREAD.start()
    logger.info(
        f"pool-heartbeat: thread started, pinging every {WP_POOL_HEARTBEAT_INTERVAL_S}s"
    )


def stop_pool_heartbeat() -> None:
    """Stop the heartbeat thread. Called by cleanup_all_connections."""
    global _POOL_HEARTBEAT_THREAD
    if _POOL_HEARTBEAT_THREAD is None:
        return
    _POOL_HEARTBEAT_STOP.set()
    try:
        _POOL_HEARTBEAT_THREAD.join(timeout=5)
    except Exception:
        pass
    _POOL_HEARTBEAT_THREAD = None
    logger.debug("pool-heartbeat: thread stopped")


def _pool_heartbeat_loop() -> None:
    """Background loop that runs SELECT 1 against every pooled connection
    every WP_POOL_HEARTBEAT_INTERVAL_S seconds. Stops when
    _POOL_HEARTBEAT_STOP is set.

    Bad connections (heartbeat fails) are silently removed from the pool;
    the next call to get_pooled_connection() will rebuild them. We do NOT
    log every successful ping or routine cleanup - only unexpected errors.
    """
    while not _POOL_HEARTBEAT_STOP.is_set():
        # Snapshot the pool so we don't hold POOL_LOCK while running queries
        with POOL_LOCK:
            snapshot = list(CONNECTION_POOL.items())

        n_pinged = 0
        n_dropped = 0
        for site_name, conn_info in snapshot:
            if _POOL_HEARTBEAT_STOP.is_set():
                break
            engine = conn_info.get("engine")
            if engine is None:
                continue
            try:
                with engine.connect() as c:
                    c.execute(text("SELECT 1")).fetchone()
                n_pinged += 1
            except Exception:
                # Connection died - remove from pool so the next caller
                # rebuilds it. Cleanup of the dead resources is fire-and-forget.
                n_dropped += 1
                with POOL_LOCK:
                    if (
                        site_name in CONNECTION_POOL
                        and CONNECTION_POOL[site_name] is conn_info
                    ):
                        del CONNECTION_POOL[site_name]
                try:
                    _cleanup_connection_unlocked(conn_info, site_name)
                except Exception:
                    pass

        if n_dropped > 0:
            logger.info(
                f"pool-heartbeat: pinged {n_pinged}, dropped {n_dropped} stale connection(s)"
            )

        # Sleep with interruptibility
        _POOL_HEARTBEAT_STOP.wait(timeout=WP_POOL_HEARTBEAT_INTERVAL_S)


def test_connection_health(conn_info: Dict[str, Any], site_name: str) -> bool:
    """
    Test if a pooled connection is still healthy.
    Returns True if healthy, False if should be removed from pool.
    """
    try:
        # Quick health check - try a simple query
        engine = conn_info.get("engine")
        if not engine:
            logger.debug(f"Connection for {site_name} has no engine")
            return False

        # Test with simple query (1 second timeout)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        return True
    except Exception as e:
        logger.debug(f"Connection health check failed for {site_name}: {e}")
        return False


def execute_query(
    connection_info: Dict[str, Any], query: str, params: Dict[str, Any] = None
) -> List[Dict[str, Any]]:
    from shared.error_handler import retry_on_transient_error, TransientError

    engine = connection_info["engine"]

    logger.debug(f"Executing query: {query[:100]}...")

    # MySQL-query-level retry. Exponential backoff (5s, 10s, 20s, 40s) with
    # jitter. The decorator returns on the first successful call; transient
    # MySQL errors (timeout/lost connection/deadlock) are caught inside _do_query
    # and re-raised as TransientError to trigger the retry. Non-transient errors
    # (syntax, permission) propagate immediately.
    @retry_on_transient_error(max_attempts=4, base_delay=5.0, max_delay=60.0)
    def _do_query():
        with engine.connect() as conn:
            logger.debug(f"Connected to database")

            # Set MySQL server-side per-query cap. Has to be large enough for
            # deep-OFFSET reads on large meta tables (usermeta/commentmeta) under
            # worker contention. Tuned via WP_MYSQL_MAX_EXEC_TIME_MS.
            try:
                conn.execute(
                    text(
                        f"SET SESSION max_execution_time = {WP_MYSQL_MAX_EXEC_TIME_MS}"
                    )
                )
                logger.debug(f"Set session timeout to {WP_MYSQL_MAX_EXEC_TIME_MS}ms")
            except Exception as e:
                logger.debug(f"Warning: Could not set session timeout: {e}")

            logger.debug(f"Running query...")
            # Use parameterized query if params provided
            try:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                rows = result.fetchall()
                logger.debug(f"Query returned {len(rows)} rows")

                if not rows:
                    return []

                return [dict(zip(result.keys(), row)) for row in rows]
            except Exception as e:
                error_str = str(e).lower()
                # Retry on transient database/network errors
                if any(
                    keyword in error_str
                    for keyword in [
                        "timeout",
                        "connection",
                        "lock",
                        "deadlock",
                        "lost connection",
                    ]
                ):
                    raise TransientError(f"Transient database error: {e}")
                # Don't retry on syntax or permission errors
                raise

    return _do_query()


def extract_wordpress_table(
    site_name: str,
    table_name: str,
    table_config: Dict[str, Any],
    config: Dict[str, Any],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    test_mode: bool = False,
) -> Iterator[Dict[str, Any]]:

    # Retry logic for transient errors (SSH timeout, MySQL connection issues).
    # Constants live at module top under "ETL retry / timeout policy".
    #
    # Adaptive max_retries: once the breaker has tripped at least once this
    # run, we're already in a WP Engine penalty window. Additional retries
    # re-arm the gateway's rate-limiter and waste time. Drop to 1 retry per
    # site after the first trip.
    retry_count = 0
    if _circuit_breaker_has_ever_tripped():
        max_retries = WP_EXTRACT_MAX_RETRIES_POST_TRIP
    else:
        max_retries = WP_EXTRACT_MAX_RETRIES

    while retry_count <= max_retries:
        # Circuit breaker: if a previous worker tripped it, wait until the
        # cool-down ends before opening a new tunnel. This prevents the
        # cascade where retry-bursts re-arm WP Engine's rate limiter.
        _circuit_breaker_wait_if_open()

        try:
            connection_info = get_pooled_connection(site_name, config)

            table_prefix = connection_info["table_prefix"]
            # Sanitize table name to prevent SQL injection
            safe_table_name = (
                table_name.replace("`", "").replace(";", "").replace("--", "")
            )

            # Apply forum site prefix adjustment for users/usermeta tables
            effective_prefix = get_effective_table_prefix(table_prefix, safe_table_name)
            full_table_name = f"`{effective_prefix}{safe_table_name}`"

            # Get primary key for ORDER BY (default to 'id' if not specified)
            primary_key = table_config.get("primary_key", ["site", "id"])
            # Use first key that's not 'site' for ordering, fallback to 'id'
            order_by_field = next((k for k in primary_key if k != "site"), "id")

            inc = table_config.get("incremental", {})
            inc_strategy = inc.get("strategy")
            inc_fields = inc.get("fields", [])
            incremental_field = inc_fields[0] if inc_fields else None
            parent_join = (
                inc.get("parent_join") if inc_strategy == "parent_join" else None
            )

            # parent_join lets us date-filter a child table that has no timestamp
            # of its own (e.g. usermeta, commentmeta, term_relationships) by joining
            # to a parent table whose timestamp IS reliable. When start_date is not
            # supplied this falls through to the same full-extract behavior as
            # strategy: none, so daily incrementals without --start-date are unchanged.
            query_params = {}
            if parent_join and start_date:
                parent_table_base = _sanitize_sql_ident(parent_join["parent_table"])
                child_fk = _sanitize_sql_ident(parent_join["child_fk"])
                parent_pk = _sanitize_sql_ident(parent_join["parent_pk"])
                parent_ts = _sanitize_sql_ident(parent_join["parent_ts_field"])

                # Parent table also needs the prefix adjustment (e.g. forum sites)
                parent_full = f"`{get_effective_table_prefix(table_prefix, parent_table_base)}{parent_table_base}`"

                where_parts = [f"parent.`{parent_ts}` >= :start_date"]
                query_params["start_date"] = start_date
                if end_date:
                    where_parts.append(f"parent.`{parent_ts}` <= :end_date")
                    query_params["end_date"] = end_date
                where_clause = " AND ".join(where_parts)

                query = (
                    f"SELECT child.* FROM {full_table_name} child"
                    f" JOIN {parent_full} parent ON child.`{child_fk}` = parent.`{parent_pk}`"
                    f" WHERE {where_clause}"
                    f" ORDER BY child.`{order_by_field}`"
                )
                count_query = (
                    f"SELECT COUNT(*) as cnt FROM {full_table_name} child"
                    f" JOIN {parent_full} parent ON child.`{child_fk}` = parent.`{parent_pk}`"
                    f" WHERE {where_clause}"
                )
            else:
                query = f"SELECT * FROM {full_table_name}"

                if incremental_field and start_date:
                    safe_incremental_field = _sanitize_sql_ident(incremental_field)
                    query += f" WHERE `{safe_incremental_field}` >= :start_date"
                    query_params["start_date"] = start_date
                    if end_date:
                        query += f" AND `{safe_incremental_field}` <= :end_date"
                        query_params["end_date"] = end_date

                # Add ORDER BY for deterministic OFFSET
                query += f" ORDER BY `{order_by_field}`"

                count_query = f"SELECT COUNT(*) as cnt FROM {full_table_name}"
                if "WHERE" in query:
                    where_clause = query[query.index("WHERE") : query.index("ORDER BY")]
                    count_query += " " + where_clause

            rows = execute_query(
                connection_info, count_query, query_params if query_params else None
            )
            total_rows = int(rows[0]["cnt"]) if rows else 0

            logger.debug(f"{site_name}/{table_name}: {total_rows} rows")

            if total_rows == 0 and table_config.get("skip_if_empty", False):
                return

            chunk_size = table_config.get("chunk_size", 1000)
            if test_mode:
                total_rows = min(total_rows, 100)

            offset = 0

            while offset < total_rows:
                rows_to_fetch = min(chunk_size, total_rows - offset)
                chunk_query = f"{query} LIMIT {rows_to_fetch} OFFSET {offset}"

                rows = execute_query(
                    connection_info, chunk_query, query_params if query_params else None
                )

                for row in rows:
                    # NORMALIZE COLUMN NAMES TO LOWERCASE
                    # WordPress MySQL tables use uppercase 'ID', but our schemas use lowercase
                    row = {k.lower(): v for k, v in row.items()}

                    # Add multitenancy field
                    row["site"] = site_name

                    # Add system tracking fields (matching YAML schemas and hash_merge expectations)
                    row["extracted_at"] = datetime.now()
                    row["source"] = "wordpress"

                    # Normalize timestamp fields - handle MySQL's "0000-00-00 00:00:00" and invalid dates
                    timestamp_fields = [
                        "post_date",
                        "post_date_gmt",
                        "post_modified",
                        "post_modified_gmt",
                        "comment_date",
                        "comment_date_gmt",
                        "user_registered",
                    ]
                    for field in timestamp_fields:
                        if field in row and row[field]:
                            value = row[field]
                            # Check for MySQL NULL timestamp
                            if isinstance(value, str) and value.startswith(
                                "0000-00-00"
                            ):
                                row[field] = None
                            # Parse timestamp using centralized utility
                            else:
                                row[field] = parse_mysql_timestamp(value)

                    # Apply field truncation (never clips json_fields; see BS-51)
                    apply_field_truncation(row, table_config)

                    yield row

                offset += chunk_size

                if test_mode and offset >= 100:
                    break

                time.sleep(0.1)

            # Success - exit retry loop. Tell the breaker so consecutive
            # successes after a near-trip reset the counter.
            _circuit_breaker_record_outcome(succeeded=True)
            return

        except CircuitBreakerAbortError:
            # The breaker has hit the give-up threshold. Don't retry, don't
            # count this as a per-site failure (it isn't), just propagate so
            # extract_site_data can mark this site 'circuit_breaker_aborted'.
            raise

        except Exception as e:
            error_str = str(e)

            # Check for transient errors that should be retried
            is_transient = any(
                pattern in error_str.lower()
                for pattern in [
                    "timeout",
                    "timed out",
                    "connection",
                    "lost connection",
                    "ssh",
                    "broken pipe",
                    "connection reset",
                    "connection refused",
                    "can't connect",
                    "cannot connect",
                    "max_execution_time",
                    "packet sequence",
                ]
            )

            # Check if error is "table doesn't exist" (not transient)
            is_table_missing = (
                "doesn't exist" in error_str.lower() or "1146" in error_str
            )

            if is_transient and retry_count < max_retries:
                retry_count += 1
                idx = min(retry_count - 1, len(WP_EXTRACT_BACKOFF_S) - 1)
                # Add +/- jitter so two parallel workers don't retry in lock-step
                # and re-trigger the same WP Engine burst-rate ceiling.
                jitter = random.uniform(
                    -WP_CIRCUIT_BREAKER_BACKOFF_JITTER_S,
                    +WP_CIRCUIT_BREAKER_BACKOFF_JITTER_S,
                )
                backoff = max(5, int(WP_EXTRACT_BACKOFF_S[idx] + jitter))
                logger.warning(
                    f"{site_name}/{table_name}: Transient error, retrying in {backoff}s (attempt {retry_count}/{max_retries}): {str(e)[:100]}"
                )

                # Clean up connection pool for this site before retry. The next
                # iteration goes back through get_pooled_connection -> ssh_connect,
                # which uses the cached wp-config credentials (no SSH re-probe)
                # and opens a fresh MySQL tunnel.
                cleanup_pooled_connection(site_name)

                time.sleep(backoff)
                continue  # Retry
            else:
                # Non-transient error or max retries exceeded - handle as before.
                # table_missing is a per-site config thing (plugin not installed),
                # NOT a sign of a rate-limit problem - don't trip the breaker on it.
                if is_table_missing:
                    logger.debug(
                        f"{site_name}/{table_name}: Table not found (site may not have this plugin/feature)"
                    )
                    # treat as a benign "success" for breaker purposes
                    _circuit_breaker_record_outcome(succeeded=True)
                elif retry_count >= max_retries:
                    logger.error(
                        f"{site_name}/{table_name}: Max retries ({max_retries}) exceeded: {str(e)[:100]}"
                    )
                    _circuit_breaker_record_outcome(succeeded=False)
                else:
                    logger.warning(f"Failed to extract {site_name}/{table_name}: {e}")
                    _circuit_breaker_record_outcome(succeeded=False)

                if not table_config.get("skip_on_error", True):
                    raise
                return  # Exit function


def _classify_site_error(exc: Exception) -> str:
    # WP-config missing is a configuration problem on the site, not transient.
    # Tag separately so the run summary can flag it.
    if isinstance(exc, WpConfigNotFoundError) or "wp-config" in str(exc).lower():
        return "wp_config_missing"
    msg = str(exc).lower()
    if any(p in msg for p in ("ssh", "tunnel", "refused", "host", "no route")):
        return "ssh_unreachable"
    if any(
        p in msg
        for p in ("timeout", "timed out", "lost connection", "broken pipe", "reset")
    ):
        return "transient"
    if "doesn't exist" in msg or "1146" in msg:
        return "table_missing"
    return "other"


def extract_site_data(
    site: str,
    table: str,
    table_config: Dict,
    config: Dict,
    start_date: str,
    end_date: str,
    test_mode: bool,
) -> Tuple[str, List[Dict], Optional[Dict]]:
    data = []
    error_info = None

    # Fast short-circuit: if the circuit breaker has hit the give-up threshold,
    # don't even attempt SSH/MySQL for this site. The ThreadPoolExecutor may
    # have ~120 already-queued futures; this drains them in milliseconds and
    # the SITES TO RETRY block surfaces them as 'circuit_breaker_aborted' so
    # the operator knows to retry later (after WP Engine's penalty clears).
    if _CIRCUIT_BREAKER_ABORTED.is_set():
        error_info = {
            "site": site,
            "table": table,
            "error_type": "CircuitBreakerAbortError",
            "error_category": "circuit_breaker_aborted",
            "error_message": "Run aborted after circuit-breaker giveup threshold",
            "stage": "pre_extraction",
        }
        return site, data, error_info

    try:
        for record in extract_wordpress_table(
            site_name=site,
            table_name=table,
            table_config=table_config,
            config=config,
            start_date=start_date,
            end_date=end_date,
            test_mode=test_mode,
        ):
            data.append(record)
    except CircuitBreakerAbortError as e:
        # Breaker tripped the giveup threshold mid-extract. Don't classify as
        # a normal site failure - it's a run-level decision.
        error_info = {
            "site": site,
            "table": table,
            "error_type": "CircuitBreakerAbortError",
            "error_category": "circuit_breaker_aborted",
            "error_message": str(e)[:300],
            "stage": "extraction",
        }
        logger.warning(f"{site}/{table}: aborted due to circuit-breaker giveup")
    except Exception as e:
        category = _classify_site_error(e)

        # Connection-state errors need pool cleanup so the next site doesn't
        # inherit a corrupted SSH tunnel.
        if category in ("ssh_unreachable", "transient", "wp_config_missing"):
            logger.debug(
                f"Cleaning up connection pool for {site} due to {category}: {str(e)[:100]}"
            )
            cleanup_pooled_connection(site)

        error_info = {
            "site": site,
            "table": table,
            "error_type": type(e).__name__,
            "error_category": category,
            "error_message": str(e)[:300],
            "stage": "extraction",
        }
        # wp-config-missing is a real configuration problem - it means we cannot
        # extract from this site at all. Log at ERROR so it's visible in alerts.
        if category == "wp_config_missing":
            logger.error(
                f"{site}: wp-config.php not found - site cannot be extracted. {str(e)[:200]}"
            )
        elif category == "table_missing":
            logger.debug(f"{site}/{table}: table not present (plugin not installed)")
        else:
            logger.warning(
                f"{site}/{table}: {error_info['error_type']} - {error_info['error_message']}"
            )

    return site, data, error_info


def extract_all_tables_for_site(
    site: str,
    tables: List[str],
    table_configs: Dict[str, Dict],
    table_start_dates: Dict[str, Optional[str]],
    config: Dict,
    end_date: str,
    test_mode: bool,
) -> Tuple[str, Dict[str, Tuple[List[Dict], Optional[Dict]]]]:
    """Per-site iteration: run all tables for one site on a SHARED connection.

    The pool caches the connection between consecutive `extract_site_data`
    calls for the same site, so we pay one SSH+MySQL handshake instead of N.
    This is the primary speedup: 9 tables x 131 sites = 1,179 connection
    setups in per-table mode vs ~131 in per-site mode.

    Returns (site, {table: (data, error_info)}).

    A connection error mid-iteration is handled by the existing per-table
    retry logic inside extract_wordpress_table; if a site's tunnel dies
    permanently, the remaining tables for that site will each fail their
    own retry budget independently (acceptable - same blast radius as
    today, just clustered).
    """
    results: Dict[str, Tuple[List[Dict], Optional[Dict]]] = {}
    # Fast short-circuit: breaker giveup means stop everything for this site.
    if _CIRCUIT_BREAKER_ABORTED.is_set():
        aborted = {
            "site": site,
            "table": None,
            "error_type": "CircuitBreakerAbortError",
            "error_category": "circuit_breaker_aborted",
            "error_message": "Run aborted after circuit-breaker giveup threshold",
            "stage": "pre_extraction",
        }
        for t in tables:
            results[t] = ([], dict(aborted, table=t))
        return site, results

    for table in tables:
        if _CIRCUIT_BREAKER_ABORTED.is_set():
            results[table] = (
                [],
                {
                    "site": site,
                    "table": table,
                    "error_type": "CircuitBreakerAbortError",
                    "error_category": "circuit_breaker_aborted",
                    "error_message": "Run aborted after circuit-breaker giveup threshold",
                    "stage": "pre_extraction",
                },
            )
            continue
        tc = table_configs[table]
        sd = table_start_dates[table]
        _, data, error_info = extract_site_data(
            site,
            table,
            tc,
            config,
            sd,
            end_date,
            test_mode,
        )
        results[table] = (data, error_info)
    return site, results


# Watermark read/write now live in shared.watermark (the single shared
# implementation, generalized from this extractor's original code). These thin
# wrappers preserve the exact prior WordPress behavior -- raw last_extracted_at with
# NO safety margin -- so the calling logic below is unchanged.
def get_incremental_state(bq_client: Any, dataset: str, table: str) -> Optional[str]:
    from shared.watermark import get_watermark

    return get_watermark(bq_client, dataset, "wordpress", table)


def save_incremental_state(
    bq_client: Any, dataset: str, table: str, execution_id: str, rows_loaded: int
) -> None:
    from shared.watermark import save_watermark

    save_watermark(bq_client, dataset, "wordpress", table, execution_id, rows_loaded)


def ensure_dataset_exists(client: bigquery.Client, dataset_id: str):
    # Create dataset if it doesn't exist
    try:
        client.get_dataset(dataset_id)
        logger.info(f"Dataset exists: {dataset_id}")
    except Exception as e:
        logger.info(f"Dataset doesn't exist, creating: {dataset_id}")
        try:
            dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
            dataset.location = "US"
            dataset = client.create_dataset(dataset)
            logger.info(f"Created dataset: {dataset_id}")
        except Exception as create_error:
            logger.error(f"Failed to create dataset {dataset_id}: {create_error}")
            raise


def check_site_has_plugin_tables(
    site_name: str,
    config: Dict[str, Any],
    table_pattern: str,
    max_attempts: int = 3,
) -> Optional[bool]:
    """
    Check if a site has at least one table matching the pattern.
    Used to filter sites for plugin-specific groups (Gravity Forms, BuddyPress).

    Retries on transient SSH/MySQL errors (lost connection, port collision,
    timeout) with short backoff so the filter survives the same cascade
    conditions the main extractor handles. Only returns None if all
    retry budget is exhausted.

    Returns:
        True  - site definitely has matching tables
        False - site definitely does NOT have matching tables
        None  - check could not complete after retries. Caller treats this
                as "include this site" so the per-table extractor can do
                its own retry pass; better than silently dropping sites
                because of transient connection errors.
    """
    pattern = table_pattern.replace("%", "")
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            connection_info = get_pooled_connection(site_name, config)
            table_prefix = connection_info["table_prefix"]
            query = f"""
                SELECT COUNT(*) as cnt
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                AND table_name LIKE '{table_prefix}{pattern}%'
            """
            rows = execute_query(connection_info, query)
            count = int(rows[0]["cnt"]) if rows else 0
            return count > 0
        except Exception as e:
            last_error = e
            err_str = str(e).lower()
            transient = any(
                p in err_str
                for p in (
                    "lost connection",
                    "timeout",
                    "timed out",
                    "ssh tunnel",
                    "address already in use",
                    "broken pipe",
                    "connection reset",
                    "can't connect",
                    "cannot connect",
                    "packet sequence",
                )
            )
            if not transient or attempt >= max_attempts:
                break
            # Drop the bad pooled connection so the next attempt opens fresh.
            try:
                cleanup_pooled_connection(site_name)
            except Exception:
                pass
            backoff = min(30, 5 * attempt) + random.uniform(-2, 2)
            logger.debug(
                f"filter check {site_name} attempt {attempt}/{max_attempts} failed; "
                f"retrying in {backoff:.1f}s: {type(e).__name__}: {str(e)[:80]}"
            )
            time.sleep(max(1.0, backoff))

    logger.warning(
        f"filter check failed for {site_name} after {max_attempts} attempt(s): "
        f"{type(last_error).__name__}: {str(last_error)[:120]}"
    )
    return None


def filter_sites_for_group(
    sites: List[str], group: str, config: Dict[str, Any]
) -> List[str]:
    """
    Filter sites based on group requirements.
    For groups with requires_check enabled, only include sites that have the plugin tables.

    Supports two filtering modes:
    - site_pattern: Filter sites by name (e.g., 'hcp' to match sites containing 'hcp')
    - table_pattern: Filter sites by checking if they have tables matching pattern

    Special case: 'core' group always returns all sites since core WordPress tables
    (users, posts, comments, etc.) should exist on every WordPress installation.
    """
    # Core group should always extract from ALL sites (no plugin filtering needed)
    if group == "core":
        logger.info(
            f"Using core group - extracting from all {len(sites)} sites (no plugin filtering)"
        )
        return sites

    groups = config.get("groups", {})
    group_config = groups.get(group, {})

    requires_check = group_config.get("requires_check", {})
    if not requires_check.get("enabled", False):
        return sites

    # First, filter by site name pattern if specified
    site_pattern = requires_check.get("site_pattern")
    if site_pattern:
        original_count = len(sites)
        sites = [s for s in sites if site_pattern.lower() in s.lower()]
        logger.info(
            f"Filtered sites by name pattern '{site_pattern}': {original_count} -> {len(sites)} sites"
        )
        if not sites:
            return sites

    table_pattern = requires_check.get("table_pattern")
    if not table_pattern:
        return sites

    # Extract pattern name for logging (e.g., '%gf_%' -> 'gf_', '%bp_%' -> 'bp_')
    pattern_name = table_pattern.replace("%", "")

    logger.info(
        f"Filtering sites for {group} group (checking for {pattern_name} tables)..."
    )

    # Filter must obey the same connection budget as the main extractor. The
    # main run uses max_workers + inter_site_throttle_s + LRU pool cap to
    # stay under WP Engine's per-IP rolling connect-rate ceiling. Without
    # the same throttle, the filter opens N tunnels at once at the START
    # of every run (cold pool) and trips the cascade before any real work
    # happens. Reuse the same knobs.
    parallel_cfg = config.get("pipeline", {}).get("parallel", {})
    filter_workers = int(parallel_cfg.get("max_workers", 2))
    inter_site_throttle_s = float(parallel_cfg.get("inter_site_throttle_s", 2.0))

    filtered_sites = []
    inconclusive_sites = []
    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=filter_workers) as executor:
        future_to_site = {}
        for idx, site in enumerate(sites):
            future_to_site[
                executor.submit(
                    check_site_has_plugin_tables,
                    site,
                    config,
                    table_pattern,
                )
            ] = site
            # Same throttling as main extraction submission: pace task
            # submission so SSH-connect rate stays at most ~0.5/s.
            if (
                inter_site_throttle_s > 0
                and idx + 1 >= filter_workers
                and idx + 1 < len(sites)
            ):
                time.sleep(inter_site_throttle_s)

        completed = 0
        for future in as_completed(future_to_site):
            site = future_to_site[future]
            completed += 1
            try:
                has_tables = future.result()
            except Exception as e:
                logger.warning(
                    f"[{completed}/{len(sites)}] {site}: future raised - {e}"
                )
                has_tables = None

            if has_tables is True:
                filtered_sites.append(site)
                logger.info(
                    f"[{completed}/{len(sites)}] {site}: Has {pattern_name} tables"
                )
            elif has_tables is False:
                logger.debug(
                    f"[{completed}/{len(sites)}] {site}: No {pattern_name} tables"
                )
            else:  # None - check failed even after retries
                inconclusive_sites.append(site)
                # Include it. If the table really isn't there, the per-table
                # extractor catches "table doesn't exist" cleanly. Better
                # to over-include than to silently drop sites because of
                # transient SSH/MySQL issues.
                filtered_sites.append(site)

    if inconclusive_sites:
        logger.warning(
            f"filter: {len(inconclusive_sites)} site(s) returned inconclusive checks "
            f"after retries - INCLUDING them in the run: "
            f"{inconclusive_sites[:10]}"
            + (" ..." if len(inconclusive_sites) > 10 else "")
        )

    logger.info(
        f"Filtered to {len(filtered_sites)}/{len(sites)} sites with {pattern_name} tables "
        f"(filter_workers={filter_workers}, throttle={inter_site_throttle_s}s; "
        f"{len(filtered_sites) - len(inconclusive_sites)} confirmed, "
        f"{len(inconclusive_sites)} inconclusive-included)"
    )

    return filtered_sites


def filter_sites_for_groups(
    sites: List[str], groups: List[str], config: Dict[str, Any]
) -> List[str]:
    """
    Resolve the site set for a run spanning MULTIPLE groups as the UNION of
    each group's site filter, preserving input order.

    Why union (and why this matters): when a run mixes groups, a site only
    needs to qualify for ONE of them to be in scope. Picking a single group's
    filter (the old `groups[0]` behavior) was a silent-data-loss bug:
      - groups=[core, buddypress] filtered by 'buddypress' alone dropped every
        non-BuddyPress site, so core tables (which exist everywhere) were
        extracted from only the BuddyPress subset.
      - groups=[buddypress, core] filtered by 'core' alone returned all sites,
        flooding non-BuddyPress sites with "bp_* table missing" work.

    'core' (and any group without requires_check) short-circuits to all sites,
    since its tables exist on every install. Plugin tables that don't exist on
    a given site are handled cleanly per-table at extraction time.
    """
    if not groups:
        return sites

    # Any group that maps to "all sites" makes the union all sites - skip the
    # expensive per-site plugin checks entirely.
    groups_cfg = config.get("groups", {}) or {}
    for g in groups:
        gc = groups_cfg.get(g, {}) or {}
        if g == "core" or not gc.get("requires_check", {}).get("enabled", False):
            logger.info(
                f"Group '{g}' spans all sites; multi-group union = all {len(sites)} sites "
                f"(skipping per-group plugin filtering)"
            )
            return sites

    # Otherwise union the plugin-filtered subsets, preserving order.
    selected = set()
    ordered = []
    for g in groups:
        for s in filter_sites_for_group(sites, g, config):
            if s not in selected:
                selected.add(s)
                ordered.append(s)
    logger.info(
        f"Multi-group site union over {groups}: {len(ordered)}/{len(sites)} sites"
    )
    return ordered


def run_pipeline(
    config: Dict[str, Any],
    sites: List[str],
    tables: List[str],
    group: Optional[str],
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    end_date: Optional[str],
    test_mode: bool,
    batch_size: Optional[int],
    max_retries: int,
    skip_hash_merge: bool = False,
    archive_staging: bool = False,
    truncate_staging: bool = False,
    rebuild: bool = False,  # Drop and rebuild tables completely
    bq_client: Any = None,
    execution_id: str = None,
    parallel_workers: Optional[int] = None,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    WordPress extraction pipeline using standard GCS flow.

    Returns data for orchestrator to handle:
    1. Write to Parquet
    2. Upload to GCS
    3. Create external table
    4. Copy to staging
    5. Merge to production
    """

    from concurrent.futures import ThreadPoolExecutor, as_completed
    from shared.gcs_pipeline import extract_to_local_parquet

    # Fresh breaker state every run. Per the plan: no persistence across
    # invocations - if you re-run immediately after an abort you'll get
    # penalized immediately, which is the right signal to wait.
    reset_circuit_breaker_state()

    try:
        # Calculate start_date from lookback_days if not provided
        if not start_date and lookback_days and refresh_mode != "full":
            start_date = (datetime.now() - timedelta(days=lookback_days)).strftime(
                "%Y-%m-%d"
            )
            logger.info(
                f"Using lookback of {lookback_days} days: start_date={start_date}"
            )

        # Set end_date to today if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # Safer check for 'all' sites
        if (
            isinstance(sites, list)
            and len(sites) == 1
            and str(sites[0]).lower() == "all"
        ) or sites == "all":
            logger.info("Using 'all' sites mode - discovering from API/config")
            sites = get_available_sites(config)
            if not sites:
                raise ValueError(
                    "No sites discovered and none configured. Please specify --sites explicitly."
                )

        if not sites:
            logger.info("No sites specified. Use --sites all or --sites site1,site2")
            return {"total_rows": 0, "sites": 0, "tables": 0}

        # Resolve the run's group set. The orchestrator passes the full list as
        # `groups`; `group` is the legacy single-group alias (= groups[0]).
        # Using only `group` for multi-group runs silently mis-scoped sites.
        groups = kwargs.get("groups") or ([group] if group else [])

        # Get tables based on group(s) or all
        if not tables:
            if groups:
                seen = set()
                tables = []
                for g in groups:
                    for t in get_available_tables(config, g):
                        if t not in seen:
                            seen.add(t)
                            tables.append(t)
                logger.info(f"Using groups {groups}: {len(tables)} tables")
            else:
                tables = get_available_tables(config)
                logger.info(f"Using all tables: {len(tables)}")

        parallel_config = config.get("pipeline", {}).get("parallel", {})
        # WP Engine SSH gateway uses a rolling-window rate limit on SSH connects,
        # not just a peak-concurrency limit. Live observations (May 2026): even
        # with workers=4, after ~50 successful tunnels the gateway started
        # killing subsequent ones in lock-step. Two workers stays safely under
        # any reasonable burst limit. Operator can raise this in YAML once a
        # stable budget is empirically confirmed.
        max_workers = parallel_workers or parallel_config.get("max_workers", 2)
        batch_size_setting = batch_size or parallel_config.get("batch_size", 2)
        # Pace task submissions to throttle SSH-connect rate to the gateway.
        # 2.0s between submissions caps connect rate at ~0.5/s per worker pool,
        # avoiding bursty traffic that triggers fail2ban-style auto-bans.
        inter_site_throttle_s = float(parallel_config.get("inter_site_throttle_s", 2.0))

        # Apply optional YAML overrides to the circuit-breaker constants.
        # Keys under pipeline.parallel.circuit_breaker.{window_size, min_samples,
        # failure_rate, pause_s, pause_multiplier, pause_max_s,
        # giveup_after_trips, backoff_jitter_s, post_trip_max_retries}.
        cb_overrides = parallel_config.get("circuit_breaker", {}) or {}
        if cb_overrides:
            set_circuit_breaker_config(**cb_overrides)
            logger.info(f"circuit-breaker overrides applied from YAML: {cb_overrides}")

        # LRU connection-pool cap. 2026-05-21: holding ~50+ open SSH tunnels
        # triggered the cascade we'd been chasing for a week. Bounding live
        # connections to a small multiple of max_workers (default cap=4)
        # keeps us well under that ceiling while still amortizing handshake
        # cost across consecutive uses of the same site. Ratchet up over
        # runs while watching the pool_stats summary at end of run.
        # IMPORTANT: this must run BEFORE filter_sites_for_group below, so
        # that the filter's check_site_has_plugin_tables calls hit the same
        # pool cap as the main extraction. Otherwise the filter runs with
        # the default cap=4 and 10 workers, hits pool churn, and silently
        # excludes sites (the bug seen on 2026-05-26 buddyboss run).
        pool_cfg = parallel_config.get("connection_pool", {}) or {}
        if pool_cfg.get("max_size") is not None:
            set_connection_pool_config(max_size=pool_cfg["max_size"])
        reset_pool_stats()

        # Filter sites for plugin-specific groups (Gravity Forms, BuddyPress).
        # Runs AFTER pool/circuit-breaker config so filter checks use the
        # same safety budget as extraction. For multi-group runs the site set
        # is the UNION across groups (see filter_sites_for_groups) so no group's
        # tables are dropped or over-extracted.
        if groups:
            sites = filter_sites_for_groups(sites, groups, config)
            if not sites:
                logger.info(f"No sites found for any of the groups: {groups}")
                return {"total_rows": 0, "sites": 0, "tables": 0}

        # Loop iteration mode:
        #   per_table  (default): for table: for site: extract  (one parquet
        #                          written immediately after each table)
        #   per_site            : for site: for table: extract on shared conn
        #                          Pays one SSH+MySQL handshake per site
        #                          instead of N, expected ~2-3x faster.
        iteration_mode = (parallel_config.get("iteration_mode") or "per_table").lower()
        if iteration_mode not in ("per_table", "per_site"):
            logger.warning(
                f"Unknown iteration_mode={iteration_mode!r}, falling back to per_table"
            )
            iteration_mode = "per_table"
        logger.info(f"iteration_mode={iteration_mode}")

        logger.info(f"Extracting {len(tables)} tables from {len(sites)} sites")
        logger.info(f"Workers: {max_workers}, Batch size: {batch_size_setting}")

        # Get dataset names from config
        env = initialize_pipeline_environment(
            config,
            bq_client=bq_client,
            source_default="wordpress",
            schema_prefix=schema_prefix,
            schema_suffix=schema_suffix,
            execution_id=execution_id,
            rebuild=rebuild,
            require_bucket=False,
        )

        source = env.source_name
        main_dataset = env.production_dataset
        run_execution_id = env.execution_id or execution_id

        # B1: the TABLE axis (table_files/total_rows/table_rows/table_status/
        # successful_tables) is standardized via the shared accumulator. The SITE
        # axis below (site_failures and its roll-ups) is wordpress-specific -- it
        # has no analog in the single-source extractors and is consumed by
        # orchestrate.py (~3268) to escalate the job when sites are silently
        # unreachable -- so it stays in `stats` and is carried into the return via
        # finalize(**extra). Surgical migration: standardize what is shared, keep
        # what is genuinely source-specific.
        #
        # LIVE-VERIFIED (2026-06): a bounded single-site/single-table run
        # (--site bnnmoprd --tables users) exercised the full migrated path --
        # SSH/MySQL extract -> record_prewritten -> parquet -> staging -> hash
        # merge -- with exact production parity (80,956 total / 229 for the site
        # unchanged; Inserted 0, Updated 3). The empty-data and per-site failure
        # paths were confirmed in a prior run (site unreachable -> site_failures,
        # zero rows, clean skip of downstream).
        from shared.extraction_result import StandardExtractionResult

        result = StandardExtractionResult(
            source=source,
            execution_id=run_execution_id or execution_id,
            job_id=run_execution_id or execution_id,
            bq_client=bq_client,
            sites=sites,
        )

        # Site-axis stats (wordpress-specific). Surface per-site failures so
        # orchestrate.py can mark the job FAILED when sites are silently
        # unreachable (e.g. wp-config missing). Without this, the job is marked
        # COMPLETED even when half the fleet failed to extract.
        stats = {
            "site_failures": {},  # {error_category: [site, ...]}
            "failed_site_count": 0,
        }
        # Track which sites are known-broken before we even start, so we don't
        # double-count them per table.
        sites_with_fatal_error = set()

        # Pre-warm all site connections IN PARALLEL before the table loop.
        # The WP Engine MySQL handshake takes 10-30s per site (measured
        # 2026-05-21); paying that cost serially during extraction is the
        # main reason runs are slow. Pre-warming with 8 workers turns 33
        # minutes of serial handshakes into ~4 minutes of parallel ones.
        # Verified safe by scripts/probe_wp_mysql.py --reuse: a single
        # connection survives 20 back-to-back queries and 9 queries with
        # 30s idle gaps, both with 100% success.
        prewarm_cfg = parallel_config.get("prewarm", {}) or {}
        prewarm_enabled = bool(prewarm_cfg.get("enabled", True))
        prewarm_workers = int(prewarm_cfg.get("max_workers", 8))
        if prewarm_enabled:
            prewarm_failures = prewarm_site_connections(
                sites,
                config,
                max_workers=prewarm_workers,
            )
            if prewarm_failures:
                # Mark these sites as fatally-failed before any table runs.
                # The per-table extractor will short-circuit them via the
                # sites_with_fatal_error set; SITES TO RETRY surfaces them
                # under the 'prewarm_failed' category so the operator knows
                # to investigate (likely SSH or wp-config issues - try
                # scripts/check_wp_ssh_login.py against those sites).
                sites_with_fatal_error.update(prewarm_failures.keys())
                stats["site_failures"].setdefault("prewarm_failed", set()).update(
                    prewarm_failures.keys()
                )
        else:
            logger.info(
                "prewarm disabled via YAML (pipeline.parallel.prewarm.enabled=false)"
            )

        # Pre-compute per-table state used by BOTH iteration modes. In
        # per_table mode this is duplicative with the per-table block below
        # (which computes it inline) - that's deliberate; the per-table
        # branch is the proven-good path and we don't touch it.
        def _resolve_table_start_date(table: str, table_cfg: Dict) -> Optional[str]:
            inc = table_cfg.get("incremental", {})
            strat = inc.get("strategy", "date")
            sd = start_date
            if refresh_mode == "full":
                return sd
            if strat == "id":
                return None
            if not sd:
                last_state = get_incremental_state(bq_client, main_dataset, table)
                if last_state:
                    # Opt-in adaptive watermark: apply the safety cushion + fallback
                    # (shared resolver) so a late-arriving record dated before the
                    # watermark is not silently missed. Without the flag, keep the
                    # prior behavior EXACTLY (raw last_extracted_at, no cushion).
                    from shared.watermark import (
                        watermark_enabled,
                        resolve_watermark_start,
                        watermark_safety_days,
                    )

                    if watermark_enabled(table_cfg):
                        return resolve_watermark_start(
                            last_state,
                            safety_days=watermark_safety_days(table_cfg),
                            lookback_days=inc.get("lookback_days", 7),
                            initial_value=inc.get("initial_value"),
                        )
                    return last_state
                return inc.get("initial_value", "2020-01-01")
            return sd

        sites_to_extract = [s for s in sites if s not in sites_with_fatal_error]

        if iteration_mode == "per_site":
            # ============================================================
            # PER-SITE MODE: open each site's connection ONCE, run all
            # tables on it, then release. ~9x fewer connection setups.
            # ============================================================
            active_tables = [
                t for t in tables if config["resources"].get(t, {}).get("active", True)
            ]
            inactive_tables = [t for t in tables if t not in active_tables]
            for t in inactive_tables:
                logger.info(f"Table '{t}' is marked as active: false, skipping")

            # Resolve start_date + log strategy once per table (matches
            # per_table mode logging behavior).
            table_configs: Dict[str, Dict] = {}
            table_start_dates: Dict[str, Optional[str]] = {}
            for t in active_tables:
                tc = config["resources"].get(t, {})
                table_configs[t] = tc
                tsd = _resolve_table_start_date(t, tc)
                table_start_dates[t] = tsd
                inc = tc.get("incremental", {})
                strat = inc.get("strategy", "date")
                if strat == "id":
                    logger.info(
                        f"[{t}] ID-based incremental: extracting all rows (BigQuery dedup)"
                    )
                elif strat == "parent_join":
                    pj = inc.get("parent_join") or {}
                    logger.info(
                        f"[{t}] Parent-join incremental with lookback: {tsd} to {end_date} "
                        f"(parent={pj.get('parent_table')}.{pj.get('parent_ts_field')})"
                    )
                else:
                    logger.info(
                        f"[{t}] Date-based incremental with lookback: {tsd} to {end_date}"
                    )

            if len(sites_to_extract) < len(sites):
                logger.info(
                    f"Skipping {len(sites) - len(sites_to_extract)} sites that failed pre-warm"
                )
            logger.info(
                f"per_site: extracting {len(active_tables)} table(s) from "
                f"{len(sites_to_extract)} site(s) with {max_workers} worker(s)"
            )

            # Per-table accumulators. Workers append under a single lock.
            per_table_data: Dict[str, List[Dict]] = {t: [] for t in active_tables}
            per_table_failed: Dict[str, List[str]] = {t: [] for t in active_tables}
            per_table_failed_cat: Dict[str, Dict[str, str]] = {
                t: {} for t in active_tables
            }
            buf_lock = threading.Lock()

            t_start = time.time()
            completed_sites = 0
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                for idx, site in enumerate(sites_to_extract):
                    fut = executor.submit(
                        extract_all_tables_for_site,
                        site,
                        active_tables,
                        table_configs,
                        table_start_dates,
                        config,
                        end_date,
                        test_mode,
                    )
                    futures[fut] = site
                    if (
                        inter_site_throttle_s > 0
                        and idx + 1 >= max_workers
                        and idx + 1 < len(sites_to_extract)
                    ):
                        time.sleep(inter_site_throttle_s)

                for fut in as_completed(futures):
                    site, per_table_result = fut.result()
                    with buf_lock:
                        completed_sites += 1
                        for table, (data, error_info) in per_table_result.items():
                            if error_info:
                                category = error_info.get("error_category", "other")
                                per_table_failed[table].append(site)
                                per_table_failed_cat[table][site] = category
                                if category != "table_missing":
                                    stats["site_failures"].setdefault(
                                        category, set()
                                    ).add(site)
                            else:
                                if data:
                                    per_table_data[table].extend(data)
                    if completed_sites % 20 == 0 or completed_sites == len(
                        sites_to_extract
                    ):
                        elapsed = time.time() - t_start
                        rate = completed_sites / elapsed if elapsed > 0 else 0
                        eta = (
                            (len(sites_to_extract) - completed_sites) / rate
                            if rate > 0
                            else 0
                        )
                        logger.info(
                            f"per_site: {completed_sites}/{len(sites_to_extract)} sites done "
                            f"elapsed={elapsed:.0f}s eta={eta:.0f}s"
                        )

            # Now run per-table post-processing (retry pass, parquet write,
            # summary). Same logic as per_table mode but operating on the
            # already-buffered data.
            RETRYABLE_CATEGORIES = (
                "transient",
                "ssh_unreachable",
                "circuit_breaker_aborted",
            )
            for table in active_tables:
                if _CIRCUIT_BREAKER_ABORTED.is_set():
                    logger.error(
                        f"circuit-breaker: skipping parquet write for remaining tables "
                        f"due to giveup threshold reached"
                    )
                    break
                table_config = table_configs[table]
                table_start_date = table_start_dates[table]
                table_with_affix = env.format_table(table)
                main_table = f"{env.project}.{main_dataset}.{table_with_affix}"
                all_data = per_table_data[table]
                failed_sites = per_table_failed[table]
                failed_category_for_site = per_table_failed_cat[table]

                # Retry pass: any retryable categories for this table?
                retry_candidates = [
                    s
                    for s in failed_sites
                    if failed_category_for_site.get(s) in RETRYABLE_CATEGORIES
                ]
                if retry_candidates:
                    logger.info(
                        f"retry pass for {table}: {len(retry_candidates)} site(s) "
                        f"with retryable failures: {retry_candidates[:10]}"
                        + (" ..." if len(retry_candidates) > 10 else "")
                    )
                    retry_successes = []
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        retry_futures = {
                            executor.submit(
                                extract_site_data,
                                s,
                                table,
                                table_config,
                                config,
                                table_start_date,
                                end_date,
                                test_mode,
                            ): s
                            for s in retry_candidates
                        }
                        for fut in as_completed(retry_futures):
                            s, data, error_info = fut.result()
                            if error_info:
                                logger.warning(
                                    f"retry {table}/{s}: still failing "
                                    f"[{error_info.get('error_category', 'other')}]"
                                )
                            else:
                                retry_successes.append(s)
                                if data:
                                    all_data.extend(data)
                    for s in retry_successes:
                        if s in failed_sites:
                            failed_sites.remove(s)
                        failed_category_for_site.pop(s, None)
                        for cat_sites in stats["site_failures"].values():
                            cat_sites.discard(s)
                    if retry_successes:
                        logger.info(
                            f"retry pass for {table}: recovered {len(retry_successes)}/{len(retry_candidates)} "
                            f"site(s): {retry_successes[:10]}"
                            + (" ..." if len(retry_successes) > 10 else "")
                        )

                # Write Parquet
                table_rows = len(all_data)
                logger.info(f"\n\nProcessing table: {table}")
                logger.info(f"Total extracted: {table_rows} rows")
                if table_rows > 0:
                    set_execution_metadata(
                        all_data, run_execution_id or execution_id, source=source
                    )
                    logger.info(f"Writing data to local Parquet file...")
                    try:
                        local_file_path = extract_to_local_parquet(
                            data=all_data,
                            source=source,
                            table=table,
                            job_id=run_execution_id or execution_id,
                            bq_client=bq_client,
                            production_table_id=main_table,
                            rebuild_mode=rebuild,
                        )
                        if local_file_path:
                            result.record_prewritten(table, local_file_path, table_rows)
                            logger.info(f"Parquet file created: {local_file_path}")
                        else:
                            logger.warning(f"No Parquet file created (empty data)")
                    except Exception as e:
                        logger.error(f"Failed to write Parquet file: {e}")
                        raise
                else:
                    logger.info(f"No data extracted for {table}")

                # Per-table summary
                total_sites = len(sites_to_extract)
                ok_sites = total_sites - len(failed_category_for_site)
                if failed_category_for_site:
                    cat_counter = collections.Counter(failed_category_for_site.values())
                    cat_summary = " ".join(
                        f"{c}:{n}" for c, n in cat_counter.most_common()
                    )
                    logger.info(
                        f"Table {table} complete: {table_rows} rows, "
                        f"{ok_sites}/{total_sites} sites OK, "
                        f"{len(failed_category_for_site)} failed [{cat_summary}]"
                    )
                else:
                    logger.info(
                        f"Table {table} complete: {table_rows} rows, "
                        f"{ok_sites}/{total_sites} sites OK"
                    )
                ps = get_pool_stats_snapshot()
                logger.info(
                    f"pool: after {table} - size={ps['current_size']}/{ps['cap']} "
                    f"opens={ps['opens']} hits={ps['hits']} evictions={ps['evictions']}"
                )

            # Note: stats['tables'] is set at end of function via len(tables)
            # which is correct for both modes.

        for table in tables if iteration_mode == "per_table" else []:
            # If the breaker hit the giveup threshold during a prior table,
            # don't even start the next one. The operator can re-run later.
            if _CIRCUIT_BREAKER_ABORTED.is_set():
                logger.error(
                    f"circuit-breaker: skipping remaining tables {tables[tables.index(table) :]} "
                    f"due to giveup threshold reached"
                )
                break

            table_config = config["resources"].get(table, {})
            if not table_config.get("active", True):
                logger.info(f"Table '{table}' is marked as active: false, skipping")
                continue

            logger.info(f"\n\n\nProcessing table: {table}")

            # Determine incremental strategy from nested block
            inc = table_config.get("incremental", {})
            incremental_strategy = inc.get("strategy", "date")
            inc_fields = inc.get("fields", [])
            incremental_field = inc_fields[0] if inc_fields else None

            # Calculate start_date based on strategy
            table_start_date = start_date

            if refresh_mode != "full":
                # DATE-BASED INCREMENTAL: Uses MySQL date field for filtering
                if incremental_field and incremental_strategy == "date":
                    if not table_start_date:
                        # Fall back to last extraction state if no lookback specified
                        last_state = get_incremental_state(
                            bq_client, main_dataset, table
                        )
                        if last_state:
                            table_start_date = last_state
                            logger.info(
                                f"Date-based incremental from: {table_start_date}"
                            )
                        else:
                            # Use table's initial_value if no state exists
                            table_start_date = inc.get("initial_value", "2020-01-01")
                            logger.info(
                                f"Date-based incremental from initial_value: {table_start_date}"
                            )
                    else:
                        logger.info(
                            f"Date-based incremental with lookback: {table_start_date} to {end_date}"
                        )

                # PARENT-JOIN INCREMENTAL: child table has no timestamp of its own;
                # filter by joining to a parent table's timestamp. Same state/lookback
                # semantics as date-based - lookback flows in via start_date.
                elif incremental_strategy == "parent_join":
                    pj = inc.get("parent_join") or {}
                    if not table_start_date:
                        last_state = get_incremental_state(
                            bq_client, main_dataset, table
                        )
                        if last_state:
                            table_start_date = last_state
                            logger.info(
                                f"Parent-join incremental from: {table_start_date} (parent={pj.get('parent_table')}.{pj.get('parent_ts_field')})"
                            )
                        else:
                            table_start_date = inc.get("initial_value", "2020-01-01")
                            logger.info(
                                f"Parent-join incremental from initial_value: {table_start_date} (parent={pj.get('parent_table')}.{pj.get('parent_ts_field')})"
                            )
                    else:
                        logger.info(
                            f"Parent-join incremental with lookback: {table_start_date} to {end_date} (parent={pj.get('parent_table')}.{pj.get('parent_ts_field')})"
                        )

                # ID-BASED INCREMENTAL: Extract all, dedupe in BigQuery via hash_merge
                elif incremental_strategy == "id":
                    logger.info(
                        f"ID-based incremental: extracting all rows (deduplication in BigQuery)"
                    )
                    table_start_date = None  # No MySQL filtering

            # Table references for production table (needed for schema discovery)
            # Apply schema prefix/suffix to table name
            table_with_affix = env.format_table(table)
            main_table = f"{env.project}.{main_dataset}.{table_with_affix}"

            # Accumulate all data (no direct BigQuery load)
            all_data = []
            failed_sites = []

            # Parallel extraction.
            #
            # Tasks are submitted with inter_site_throttle_s spacing between
            # them to pace SSH-tunnel creation against the WP Engine gateway's
            # rolling-window rate limit. With max_workers=2 and throttle=2.0s,
            # new SSH connects start at most ~0.5/s after the initial 2 fill
            # the worker pool. The throttle only applies to submission;
            # already-running workers continue uninterrupted.
            # Skip sites that already failed during pre-warm. The SITES TO RETRY
            # block will surface them with the 'prewarm_failed' category - no
            # need to re-attempt them per table and accumulate the same failure
            # in every group's stats.
            sites_to_extract = [s for s in sites if s not in sites_with_fatal_error]
            if len(sites_to_extract) < len(sites):
                logger.info(
                    f"Skipping {len(sites) - len(sites_to_extract)} sites that failed pre-warm; "
                    f"extracting {table} from {len(sites_to_extract)} sites"
                )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                for idx, site in enumerate(sites_to_extract):
                    fut = executor.submit(
                        extract_site_data,
                        site,
                        table,
                        table_config,
                        config,
                        table_start_date,
                        end_date,
                        test_mode,
                    )
                    futures[fut] = site
                    # Throttle starts after the first max_workers tasks - those
                    # fill the pool immediately, then we pace any further
                    # submissions so the pool never sees a burst.
                    if (
                        inter_site_throttle_s > 0
                        and idx + 1 >= max_workers
                        and idx + 1 < len(sites_to_extract)
                    ):
                        time.sleep(inter_site_throttle_s)

                completed = 0
                # Local per-table failure tracking so we can print a
                # one-line summary "OK/total sites, N failed [cat:k cat:m]"
                # without grepping the cumulative stats['site_failures'].
                failed_category_for_site: Dict[str, str] = {}
                for future in as_completed(futures):
                    site, data, error_info = future.result()
                    completed += 1

                    if error_info:
                        category = error_info.get("error_category", "other")
                        logger.info(
                            f"[{completed}/{len(sites_to_extract)}] {site}: Failed [{category}] - {error_info['error_type']}: {error_info['error_message'][:100]}"
                        )
                        failed_sites.append(site)
                        failed_category_for_site[site] = category
                        # Roll up into job-level failure tracking. Skip
                        # table_missing - that's expected (plugin not installed).
                        if category != "table_missing":
                            stats["site_failures"].setdefault(category, set()).add(site)
                    else:
                        if data:
                            logger.debug(
                                f"[{completed}/{len(sites_to_extract)}] {site}: {len(data)} records"
                            )
                            all_data.extend(data)
                        else:
                            logger.debug(
                                f"[{completed}/{len(sites_to_extract)}] {site}: No data"
                            )

            if failed_sites:
                logger.info(f"Failed sites: {', '.join(failed_sites[:10])}")

            # In-process retry pass for cascade-style failures only.
            # Categories worth retrying (transient connection issues that
            # often clear on a second attempt): transient, ssh_unreachable,
            # circuit_breaker_aborted. NOT retried: wp_config_missing,
            # table_missing, auth_failure - those are real config problems
            # that won't fix themselves.
            RETRYABLE_CATEGORIES = (
                "transient",
                "ssh_unreachable",
                "circuit_breaker_aborted",
            )
            retry_candidates = []
            for failed_site in list(failed_sites):
                cats = [
                    c for c, s in stats["site_failures"].items() if failed_site in s
                ]
                if any(c in RETRYABLE_CATEGORIES for c in cats):
                    retry_candidates.append(failed_site)

            if retry_candidates:
                logger.info(
                    f"retry pass for {table}: {len(retry_candidates)} site(s) "
                    f"with retryable failures: {retry_candidates[:10]}"
                    + (" ..." if len(retry_candidates) > 10 else "")
                )
                retry_successes = []
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    retry_futures = {
                        executor.submit(
                            extract_site_data,
                            s,
                            table,
                            table_config,
                            config,
                            table_start_date,
                            end_date,
                            test_mode,
                        ): s
                        for s in retry_candidates
                    }
                    for fut in as_completed(retry_futures):
                        s, data, error_info = fut.result()
                        if error_info:
                            logger.warning(
                                f"retry {table}/{s}: still failing "
                                f"[{error_info.get('error_category', 'other')}]"
                            )
                        else:
                            retry_successes.append(s)
                            if data:
                                all_data.extend(data)
                # On retry success: clear the site from failed_sites, the
                # local per-table category map, and stats['site_failures']
                # so SITES TO RETRY doesn't list it.
                for s in retry_successes:
                    if s in failed_sites:
                        failed_sites.remove(s)
                    failed_category_for_site.pop(s, None)
                    for cat_sites in stats["site_failures"].values():
                        cat_sites.discard(s)
                if retry_successes:
                    logger.info(
                        f"retry pass for {table}: recovered {len(retry_successes)}/{len(retry_candidates)} "
                        f"site(s): {retry_successes[:10]}"
                        + (" ..." if len(retry_successes) > 10 else "")
                    )

            # Write extracted data to local Parquet file
            table_rows = len(all_data)
            logger.info(f"Total extracted: {table_rows} rows")

            if table_rows > 0:
                # Add execution metadata (execution_id, optional source) to records
                set_execution_metadata(
                    all_data, run_execution_id or execution_id, source=source
                )

                # Write to local Parquet file (standard GCS pipeline flow)
                logger.info(f"Writing data to local Parquet file...")
                try:
                    local_file_path = extract_to_local_parquet(
                        data=all_data,
                        source=source,
                        table=table,
                        job_id=run_execution_id or execution_id,
                        bq_client=bq_client,
                        production_table_id=main_table,
                        rebuild_mode=rebuild,
                    )

                    if local_file_path:
                        # Store file path for orchestrator to upload to GCS
                        result.record_prewritten(table, local_file_path, table_rows)
                        logger.info(f"Parquet file created: {local_file_path}")
                    else:
                        logger.warning(f"No Parquet file created (empty data)")

                except Exception as e:
                    logger.error(f"Failed to write Parquet file: {e}")
                    raise
            else:
                logger.info(f"No data extracted for {table}")

            total_sites = len(sites_to_extract)
            ok_sites = total_sites - len(failed_category_for_site)
            if failed_category_for_site:
                cat_counter = collections.Counter(failed_category_for_site.values())
                cat_summary = " ".join(f"{c}:{n}" for c, n in cat_counter.most_common())
                logger.info(
                    f"Table {table} complete: {table_rows} rows, "
                    f"{ok_sites}/{total_sites} sites OK, "
                    f"{len(failed_category_for_site)} failed [{cat_summary}]"
                )
            else:
                logger.info(
                    f"Table {table} complete: {table_rows} rows, "
                    f"{ok_sites}/{total_sites} sites OK"
                )
            ps = get_pool_stats_snapshot()
            logger.info(
                f"pool: after {table} - size={ps['current_size']}/{ps['cap']} "
                f"opens={ps['opens']} hits={ps['hits']} evictions={ps['evictions']}"
            )

        # Convert site_failures sets to sorted lists (sets aren't JSON-serializable)
        # and roll up an aggregate error message for the job monitoring layer.
        site_failures_serialized = {
            cat: sorted(sites_set) for cat, sites_set in stats["site_failures"].items()
        }
        unique_failed_sites = set()
        for s_set in stats["site_failures"].values():
            unique_failed_sites.update(s_set)
        stats["site_failures"] = site_failures_serialized
        stats["failed_site_count"] = len(unique_failed_sites)

        # If wp-config or ssh failures occurred, surface them as a job-level
        # error so orchestrate.py marks the run FAILED rather than COMPLETED.
        fatal_categories = ("wp_config_missing", "ssh_unreachable")
        fatal_sites = []
        for cat in fatal_categories:
            fatal_sites.extend(site_failures_serialized.get(cat, []))
        if fatal_sites:
            stats["has_fatal_failures"] = True
            preview = sorted(set(fatal_sites))[:10]
            stats["error_message"] = (
                f"{len(set(fatal_sites))} sites failed to extract due to "
                f"wp-config/ssh issues: {preview}"
                + (" (and more)" if len(set(fatal_sites)) > len(preview) else "")
            )

        # Log summary
        logger.info(f"\n{'=' * 72}")
        logger.info(f"WordPress Extraction Complete")
        logger.info(f"{'=' * 72}")
        logger.info(f"Total rows extracted: {result.total_rows:,}")
        logger.info(f"Successful tables: {result.successful_tables}/{len(tables)}")
        logger.info(f"Parquet files created: {len(result.table_files)}")

        # Per-category retry block: full site lists + copy-paste rerun commands.
        # Categories are bucketed by retry safety:
        #   transient         - safe to rerun as-is (timeouts, lost connections)
        #   ssh_unreachable   - check WP Engine reachability before rerunning
        #   wp_config_missing - investigate (file moved / install renamed) before rerun
        #   other             - unclassified; review the per-site detail in the log above
        if site_failures_serialized:
            CATEGORY_GUIDANCE = {
                "prewarm_failed": "SSH or MySQL handshake failed at run start - check scripts/check_wp_ssh_login.py first",
                "transient": "safe to rerun as-is - most likely WP Engine SSH timeouts",
                "ssh_unreachable": "verify SSH reachability before rerunning (scripts/check_wp_ssh_login.py)",
                "wp_config_missing": "investigate first - wp-config.php may have moved or site may be deactivated",
                "circuit_breaker_aborted": "wait >= 30 min for WP Engine penalty window to clear, then rerun",
                "other": "unclassified - review the per-site error lines above",
            }
            # Reconstruct the orchestrate command we'd suggest for retry.
            # Includes the exact start_date / end_date / table list used by this run.
            tables_arg = " ".join(tables) if tables else ""
            group_arg = f" --group {group}" if group else ""
            date_args = []
            if start_date:
                date_args.append(f"--start-date {start_date}")
            if end_date:
                date_args.append(f"--end-date {end_date}")
            date_str = (" " + " ".join(date_args)) if date_args else ""
            refresh_arg = (
                f" --refresh {refresh_mode}"
                if refresh_mode and refresh_mode != "incremental"
                else ""
            )

            logger.warning(f"\n{'=' * 72}")
            logger.warning(
                f"SITES TO RETRY ({stats['failed_site_count']} unique sites failed)"
            )
            logger.warning(f"{'=' * 72}")

            # Order categories: safer-to-retry first, abort-related last so the
            # operator sees actionable items before the giveup list.
            order = [
                "prewarm_failed",
                "transient",
                "ssh_unreachable",
                "wp_config_missing",
                "circuit_breaker_aborted",
                "other",
            ]
            for cat in order:
                failed_list = site_failures_serialized.get(cat)
                if not failed_list:
                    continue
                guidance = CATEGORY_GUIDANCE.get(cat, "")
                logger.warning(f"\n[{cat}] {len(failed_list)} sites - {guidance}")
                # Print ALL failed sites, not just the first 5
                logger.warning(f"  {' '.join(failed_list)}")
                # Print a ready-to-paste rerun command
                sites_arg = " ".join(failed_list)
                if tables_arg:
                    cmd = (
                        f"python orchestrate.py --source wordpress --env prod"
                        f"{refresh_arg}{date_str} "
                        f"--tables {tables_arg} "
                        f"--sites {sites_arg}"
                    )
                else:
                    cmd = (
                        f"python orchestrate.py --source wordpress --env prod"
                        f"{refresh_arg}{group_arg}{date_str} "
                        f"--sites {sites_arg}"
                    )
                logger.warning(f"  Rerun:")
                logger.warning(f"    {cmd}")

            # Catch any leftover categories not in `order` (defensive)
            for cat, failed_list in site_failures_serialized.items():
                if cat not in order:
                    logger.warning(
                        f"\n[{cat}] {len(failed_list)} sites (uncategorized)"
                    )
                    logger.warning(f"  {' '.join(failed_list)}")

            logger.warning(f"\n{'=' * 72}")

        if stats.get("has_fatal_failures"):
            logger.error(f"Job has fatal site failures: {stats['error_message']}")
        logger.info(f"{'=' * 72}\n")

        # Standard shape (table axis) from the accumulator + wordpress-specific
        # site-axis fields carried via **extra. Preserve wordpress's historical
        # `tables` semantics (REQUESTED count, not successful) by overriding the
        # accumulator default after finalize.
        final = result.finalize(**stats)
        final["tables"] = len(tables)
        return final

    finally:
        ps = get_pool_stats_snapshot()
        logger.info(
            f"pool_stats: cap={ps['cap']} opens={ps['opens']} hits={ps['hits']} "
            f"hit_rate={ps['hit_rate']:.2%} evictions={ps['evictions']} "
            f"max_concurrent={ps['max_concurrent']} current_size={ps['current_size']}"
        )
        cleanup_all_connections()
