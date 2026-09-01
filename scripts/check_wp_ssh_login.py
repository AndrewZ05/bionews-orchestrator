#!/usr/bin/env python3
"""SSH-login reachability test for all WordPress sites.

Uses Fabric (which wraps Paramiko) to perform a real SSH login on each site:
TCP connect -> SSH handshake -> host-key accept (auto via Fabric's default
AutoAddPolicy) -> public-key auth -> `echo OK` -> disconnect.

This exercises every layer the orchestrator's extractor uses, unlike the
TCP-only check_wp_ssh_access.py.

Serial execution with pacing to avoid retripping any WP Engine rate-limit.
Aborts early after 5 consecutive auth/connection failures - if the IP is
banned, there's no point making 126 more refused connections.

Per-site, the probe distinguishes:
  ssh=OK,        wp_config=FOUND               - found at a standard path
  ssh=OK,        wp_config=FOUND_NONSTANDARD   - found via `find`; not at known paths
  ssh=OK,        wp_config=NOT_FOUND           - SSH ok, file genuinely missing
  ssh=OK,        wp_config=TIMEOUT             - SSH ok, but the wp-config probe ran over budget
  ssh=REFUSED|TIMEOUT|AUTH_FAIL|...,
                  wp_config=N/A                 - never got to wp-config probing

When you see ssh=OK + wp_config=TIMEOUT, that means SSH is healthy; the slow part
was the shell command. Re-run those sites with --find-timeout 300 (or higher).

Usage:
  python scripts/check_wp_ssh_login.py                       # all sites from discovery API
  python scripts/check_wp_ssh_login.py bnamlprd bncatprd     # specific sites
  python scripts/check_wp_ssh_login.py --pace 1.0            # 1s between sites (default 0.5s)
  python scripts/check_wp_ssh_login.py --timeout 90          # SSH connect timeout (default 90s)
  python scripts/check_wp_ssh_login.py --cmd-timeout 90      # per-command timeout (default 90s)
  python scripts/check_wp_ssh_login.py --find-timeout 180    # `find` fallback timeout (default 180s)
  python scripts/check_wp_ssh_login.py --no-abort            # run all sites no matter what

Exit codes:
  0 = all sites OK and wp-config FOUND at a known path
  1 = some sites failed or had non-standard / missing / unknown wp-config
  2 = early abort (likely IP banned or key broken)
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import socket
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import List

# Suppress paramiko/invoke chatter
logging.getLogger("paramiko").setLevel(logging.ERROR)
logging.getLogger("invoke").setLevel(logging.ERROR)
logging.getLogger("fabric").setLevel(logging.ERROR)

# Make repo root importable so we can reuse the .env loader if present
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fabric import Connection

DEFAULT_API = "https://pipeline.bionews.com/query/sites"
SSH_HOST_FMT = "{site}.ssh.wpengine.net"
SSH_USER_FMT = "{site}"


def fetch_sites_from_api(url: str) -> List[str]:
    """Pull active production site names from the discovery API."""
    import urllib.request
    import json
    with urllib.request.urlopen(url, timeout=15) as r:
        data = json.loads(r.read().decode("utf-8"))
    sites = [t.get("name", "") for t in data.get("topics", [])]
    return sorted(s for s in sites if s.endswith("prd"))


def classify_error(exc: Exception) -> str:
    """Map a Fabric/Paramiko exception to a short status code."""
    name = type(exc).__name__
    msg = str(exc).lower()
    if isinstance(exc, ConnectionRefusedError) or "refused" in msg:
        return "REFUSED"
    if isinstance(exc, socket.timeout) or "timed out" in msg or "timeout" in msg:
        return "TIMEOUT"
    if "no valid connections" in msg or "novalidconnections" in name.lower():
        return "REFUSED"  # Paramiko wraps refused conns under this name
    if "authentication" in msg or "auth" in msg.lower() or "permission denied" in msg:
        return "AUTH_FAIL"
    if "name or service" in msg or "name resolution" in msg or "gaierror" in name.lower():
        return "DNS"
    if "host key" in msg or "hostkey" in msg:
        return "HOSTKEY"
    return "OTHER"


def _clean_exc_detail(exc: Exception, limit: int = 160) -> str:
    """Render an exception as a single short line for the report.

    Invoke/Fabric CommandTimedOut exceptions embed the entire command,
    stdout, and stderr in their str() form across multiple lines. Squash
    to a single line and drop the noisy 'Command: ...'/'Stdout:'/'Stderr:'
    blocks so the report stays tidy.
    """
    raw = str(exc)
    # Drop the boilerplate that CommandTimedOut adds
    lines = []
    for ln in raw.splitlines():
        s = ln.strip()
        if not s:
            continue
        low = s.lower()
        if low.startswith(("command:", "stdout:", "stderr:")):
            continue
        lines.append(s)
    flat = " ".join(lines)
    return f"{type(exc).__name__}: {flat[:limit]}"


# Paths to check for wp-config.php in priority order. The extractor checks
# the same paths; we add a bounded `find` as a last-resort discovery probe so
# we can detect (and report) WP Engine installs where the file has been moved.
WP_CONFIG_STATIC_PATHS = [
    "sites/{site}/wp-config.php",
    "~/sites/{site}/wp-config.php",
    "wp-config.php",
]


def probe_one(
    site: str,
    ssh_key_path: str,
    connect_timeout: int,
    cmd_timeout: int,
    find_timeout: int,
) -> dict:
    """Open a fresh SSH connection, sanity-check, locate wp-config.php, disconnect.

    Each step has its own timeout and is wrapped so a slow `find` doesn't lose
    the fact that SSH itself worked. The report distinguishes:
      ssh=OK,        wp_config=FOUND               - happy path
      ssh=OK,        wp_config=FOUND_NONSTANDARD   - found via `find`; not at known paths
      ssh=OK,        wp_config=NOT_FOUND           - SSH ok, file missing
      ssh=OK,        wp_config=TIMEOUT             - SSH ok, but `find` ran past find_timeout
      ssh=OK,        wp_config=ERROR               - SSH ok, but a shell command erred
      ssh=REFUSED|TIMEOUT|AUTH_FAIL|..., wp_config=N/A - never got to wp-config probing
    """
    host = SSH_HOST_FMT.format(site=site)
    user = SSH_USER_FMT.format(site=site)
    t0 = time.time()
    result = {
        "site": site,
        "status": "?",
        "wp_config": "?",
        "wp_config_path": "",
        "elapsed_s": 0.0,
        "detail": "",
        "remote_user": "",
    }
    try:
        with Connection(
            host=host,
            user=user,
            connect_timeout=connect_timeout,
            connect_kwargs={"key_filename": ssh_key_path},
        ) as c:
            # 1) Sanity check: confirm we can run a command. If THIS times out,
            # SSH technically connected but the shell isn't usable - classify as
            # TIMEOUT at the SSH layer.
            try:
                r = c.run("echo OK && whoami", hide=True, warn=True, timeout=cmd_timeout)
            except Exception as e:
                result["status"] = classify_error(e)
                result["wp_config"] = "N/A"
                result["detail"] = _clean_exc_detail(e, limit=200) + " (during sanity check)"
                result["elapsed_s"] = round(time.time() - t0, 2)
                return result

            if not (r.ok and "OK" in r.stdout):
                result["status"] = "OTHER"
                result["wp_config"] = "N/A"
                result["detail"] = (r.stderr or r.stdout or "no output").strip()[:160] + " (sanity check)"
                result["elapsed_s"] = round(time.time() - t0, 2)
                return result

            # SSH itself is healthy from here on. Any later timeout/error gets
            # attributed to wp_config, not to ssh.
            lines = r.stdout.strip().splitlines()
            result["remote_user"] = lines[1] if len(lines) > 1 else ""
            result["status"] = "OK"

            # 2) Try the well-known static paths first
            static_timeout_seen = False
            for tmpl in WP_CONFIG_STATIC_PATHS:
                path = tmpl.format(site=site)
                try:
                    t = c.run(f'test -f "{path}" && echo HIT', hide=True, warn=True, timeout=cmd_timeout)
                except Exception as e:
                    static_timeout_seen = True
                    result["detail"] = f"as {result['remote_user']}; {_clean_exc_detail(e, 100)} on `test -f {path}`"
                    continue  # try next path
                if t.ok and "HIT" in t.stdout:
                    result["wp_config"] = "FOUND"
                    result["wp_config_path"] = path
                    result["detail"] = f"as {result['remote_user']}; wp-config at {path}"
                    break

            # 3) Static paths missed - do a bounded `find` to discover non-standard locations
            if result["wp_config"] == "?":
                try:
                    f = c.run(
                        'find ~/sites -maxdepth 3 -name wp-config.php -type f 2>/dev/null | head -5',
                        hide=True, warn=True, timeout=find_timeout,
                    )
                except Exception as e:
                    # SSH was fine; `find` just took too long. Don't lose that.
                    result["wp_config"] = "TIMEOUT"
                    result["detail"] = f"as {result['remote_user']}; {_clean_exc_detail(e, 100)} during find"
                else:
                    hits = [p.strip() for p in (f.stdout or "").splitlines() if p.strip()]
                    if hits:
                        result["wp_config"] = "FOUND_NONSTANDARD"
                        result["wp_config_path"] = hits[0]
                        extra = f" (+{len(hits)-1} more)" if len(hits) > 1 else ""
                        result["detail"] = f"as {result['remote_user']}; wp-config at {hits[0]}{extra}"
                    elif static_timeout_seen:
                        # All static checks timed out AND find returned nothing.
                        # Probably the SSH session is degraded - report TIMEOUT not NOT_FOUND.
                        result["wp_config"] = "TIMEOUT"
                        result["detail"] = f"as {result['remote_user']}; static checks timed out; find returned no hits"
                    else:
                        result["wp_config"] = "NOT_FOUND"
                        result["detail"] = f"as {result['remote_user']}; no wp-config.php under ~/sites/**"

            result["elapsed_s"] = round(time.time() - t0, 2)
    except Exception as e:
        result["elapsed_s"] = round(time.time() - t0, 2)
        result["status"] = classify_error(e)
        result["wp_config"] = "N/A"
        result["detail"] = _clean_exc_detail(e, limit=200)
    return result


def main():
    ap = argparse.ArgumentParser(description="SSH-login reachability test for WordPress sites (serial)")
    ap.add_argument("sites", nargs="*", help="Site names; omit to fetch from discovery API")
    ap.add_argument("--api", default=DEFAULT_API)
    ap.add_argument("--timeout", type=int, default=90, help="SSH connect timeout seconds (default 90)")
    ap.add_argument("--cmd-timeout", type=int, default=90, help="Per-command timeout for `echo OK` and `test -f` (default 90)")
    ap.add_argument("--find-timeout", type=int, default=180, help="Timeout for the wp-config `find` fallback (default 180)")
    ap.add_argument("--pace", type=float, default=0.5, help="Seconds between sites (default 0.5)")
    ap.add_argument("--no-abort", action="store_true", help="Don't early-abort on 5 consecutive REFUSED/AUTH_FAIL")
    ap.add_argument("--key", default=None, help="SSH private key path; default = $WP_SSH_KEY_PATH")
    ap.add_argument("--out", default=None, help="CSV output path; default = docs/wordpress_audit/ssh_login_<timestamp>.csv")
    args = ap.parse_args()

    ssh_key_path = args.key or os.environ.get("WP_SSH_KEY_PATH")
    if not ssh_key_path:
        print("ERROR: SSH key path not set. Pass --key or set WP_SSH_KEY_PATH in .env", file=sys.stderr)
        sys.exit(2)
    ssh_key_path = os.path.expanduser(ssh_key_path.replace("\\", "/"))
    if not Path(ssh_key_path).exists():
        print(f"ERROR: SSH key not found at {ssh_key_path}", file=sys.stderr)
        sys.exit(2)

    # Build site list
    if args.sites:
        sites = args.sites
        print(f"Testing {len(sites)} site(s) from command line")
    else:
        print(f"Fetching site list from {args.api}...")
        try:
            sites = fetch_sites_from_api(args.api)
        except Exception as e:
            print(f"ERROR: could not fetch sites: {type(e).__name__}: {e}", file=sys.stderr)
            sys.exit(2)
        print(f"  {len(sites)} sites discovered")
    print(f"Using SSH key: {ssh_key_path}")
    print(f"Timeouts: connect={args.timeout}s, cmd={args.cmd_timeout}s, find={args.find_timeout}s; pace={args.pace}s")
    print()

    # Output CSV
    if args.out:
        out_path = Path(args.out)
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path("docs/wordpress_audit") / f"ssh_login_{stamp}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"{'#':>4s}  {'site':<22s} {'ssh':<10s} {'wp-config':<18s} {'elapsed':>8s}  detail")
    print(f"{'-' * 4}  {'-' * 22} {'-' * 10} {'-' * 18} {'-' * 8}  {'-' * 60}")

    results = []
    counts = Counter()
    wp_counts = Counter()
    consec_fail = 0
    early_abort = False

    for i, site in enumerate(sites, 1):
        r = probe_one(
            site,
            ssh_key_path,
            connect_timeout=args.timeout,
            cmd_timeout=args.cmd_timeout,
            find_timeout=args.find_timeout,
        )
        results.append(r)
        counts[r["status"]] += 1
        wp_counts[r["wp_config"]] += 1

        elapsed_str = f"{r['elapsed_s']:.1f}s"
        print(f"{i:>4d}  {site:<22s} {r['status']:<10s} {r['wp_config']:<18s} {elapsed_str:>8s}  {r['detail'][:60]}")

        # Track consecutive hard failures (refused or auth_fail) - these
        # signal infrastructure problems, not site-specific. Timeouts are
        # transient and don't count for the early-abort trigger.
        if r["status"] in ("REFUSED", "AUTH_FAIL"):
            consec_fail += 1
        else:
            consec_fail = 0

        if not args.no_abort and consec_fail >= 5:
            print()
            print("=" * 80)
            print(f"ABORT: 5 consecutive {r['status']} results.")
            if r["status"] == "REFUSED":
                print("Our IP is likely still on WP Engine's block list. Stopping to avoid")
                print("extending the ban. Pass --no-abort if you want to test all sites anyway.")
            else:
                print("SSH key auth failing across multiple sites - check your key path or")
                print("the key's authorized status on WP Engine.")
            print("=" * 80)
            early_abort = True
            break

        if i < len(sites):
            time.sleep(args.pace)

    # Write CSV
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["site", "status", "wp_config", "wp_config_path", "elapsed_s", "remote_user", "detail"],
        )
        w.writeheader()
        for r in results:
            w.writerow(r)

    # Summary
    print()
    print("=" * 80)
    print(f"SUMMARY  ({len(results)}/{len(sites)} sites tested{'  [EARLY ABORT]' if early_abort else ''})")
    print("=" * 80)
    print("SSH layer:")
    for status in ("OK", "REFUSED", "TIMEOUT", "AUTH_FAIL", "HOSTKEY", "DNS", "OTHER"):
        n = counts.get(status, 0)
        if n:
            print(f"  {status:<10s} {n:>4d}")

    if counts.get("OK", 0):
        ok_results = [r for r in results if r["status"] == "OK"]
        avg = sum(r["elapsed_s"] for r in ok_results) / len(ok_results)
        print(f"  (avg login time for OK: {avg:.1f}s)")

    print("wp-config.php (only meaningful where SSH worked):")
    for wp in ("FOUND", "FOUND_NONSTANDARD", "NOT_FOUND", "TIMEOUT", "ERROR", "N/A"):
        n = wp_counts.get(wp, 0)
        if n:
            print(f"  {wp:<18s} {n:>4d}")

    # Failure cohorts the user cares about:
    #   1) SSH itself failed
    #   2) SSH worked but wp-config probe TIMED OUT (couldn't determine - retry with longer find timeout)
    #   3) SSH worked but wp-config was at a non-standard location (needs YAML/extractor update)
    #   4) SSH worked but no wp-config was found anywhere (site archived/deactivated)
    ssh_failed = [r for r in results if r["status"] != "OK"]
    wp_timeout = [r for r in results if r["status"] == "OK" and r["wp_config"] == "TIMEOUT"]
    wp_nonstd  = [r for r in results if r["status"] == "OK" and r["wp_config"] == "FOUND_NONSTANDARD"]
    wp_missing = [r for r in results if r["status"] == "OK" and r["wp_config"] == "NOT_FOUND"]

    if ssh_failed:
        print()
        print(f"SSH FAILURES  ({len(ssh_failed)} sites)")
        print(f"{'-' * 80}")
        print(f"{'site':<22s} {'status':<10s} detail")
        for r in sorted(ssh_failed, key=lambda x: (x["status"], x["site"])):
            print(f"{r['site']:<22s} {r['status']:<10s} {r['detail'][:80]}")

    if wp_nonstd:
        print()
        print(f"WP-CONFIG AT NON-STANDARD PATH  ({len(wp_nonstd)} sites)")
        print(f"  These SSH'd fine but wp-config.php is NOT at any of:")
        for p in WP_CONFIG_STATIC_PATHS:
            print(f"    {p}")
        print(f"  -> the extractor will NOT find it; YAML/extractor update needed")
        print(f"{'-' * 80}")
        print(f"{'site':<22s} actual path")
        for r in sorted(wp_nonstd, key=lambda x: x["site"]):
            print(f"{r['site']:<22s} {r['wp_config_path']}")

    if wp_missing:
        print()
        print(f"WP-CONFIG NOT FOUND  ({len(wp_missing)} sites)")
        print(f"  SSH'd fine but no wp-config.php anywhere under ~/sites/** (depth 3).")
        print(f"  These sites are probably deactivated, archived, or moved.")
        print(f"{'-' * 80}")
        for r in sorted(wp_missing, key=lambda x: x["site"]):
            print(f"  {r['site']}")

    if wp_timeout:
        print()
        print(f"WP-CONFIG PROBE TIMED OUT  ({len(wp_timeout)} sites)")
        print(f"  SSH worked but the `find` (or `test -f`) timed out before we could")
        print(f"  determine the wp-config location. NOT a confirmed missing file.")
        print(f"  Re-run with --find-timeout 300 (or higher) on these sites:")
        print(f"{'-' * 80}")
        for r in sorted(wp_timeout, key=lambda x: x["site"]):
            print(f"  {r['site']}    {r['detail'][:80]}")

    print()
    print(f"CSV: {out_path}")

    if early_abort:
        sys.exit(2)
    # Exit 0 only if every site has SSH=OK AND wp-config FOUND at a known path
    all_clean = all(r["status"] == "OK" and r["wp_config"] == "FOUND" for r in results)
    if all_clean and len(results) == len(sites):
        sys.exit(0)
    sys.exit(1)


if __name__ == "__main__":
    main()
