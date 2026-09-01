#!/usr/bin/env python3
"""TCP-only SSH reachability check for all WordPress sites.

Probes <site>.ssh.wpengine.net:22 with a plain TCP connect - no SSH
handshake, no auth, no key. Distinguishes:
  OK       - port accepted the TCP connection (server is up & not firewalled)
  REFUSED  - immediate RST (firewall block on our IP, or service down)
  TIMEOUT  - connect() did not return within timeout (route/network issue)
  DNS      - hostname did not resolve

Runs serially with pacing between probes so a fresh WP Engine IP-ban
can't be triggered or extended. Aborts early after 5 consecutive REFUSED
results - if we're banned, there's no point making 126 more refused
connections.

Usage:
  python scripts/check_wp_ssh_access.py                      # all sites from discovery API
  python scripts/check_wp_ssh_access.py bnamlprd bncatprd    # specific sites
  python scripts/check_wp_ssh_access.py --pace 0.2           # tune pacing (default 0.5s)
  python scripts/check_wp_ssh_access.py --timeout 5          # per-probe timeout (default 5s)
  python scripts/check_wp_ssh_access.py --no-abort           # don't abort on 5 consecutive REFUSED

Exit codes:
  0 = all sites OK
  1 = some sites failed
  2 = early abort (likely IP-banned)
"""
from __future__ import annotations

import argparse
import csv
import socket
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import List

DEFAULT_API = "https://pipeline.bionews.com/query/sites"


def fetch_sites_from_api(url: str) -> List[str]:
    import urllib.request
    import json
    with urllib.request.urlopen(url, timeout=15) as r:
        data = json.loads(r.read().decode("utf-8"))
    sites = [t.get("name", "") for t in data.get("topics", [])]
    return sorted(s for s in sites if s.endswith("prd"))


def probe(host: str, port: int, timeout: float) -> tuple[str, int, str]:
    """Return (status, latency_ms, detail). status is OK|REFUSED|TIMEOUT|DNS|OTHER."""
    t0 = time.time()
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror as e:
        return "DNS", int((time.time() - t0) * 1000), f"{type(e).__name__}: {e}"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((ip, port))
        return "OK", int((time.time() - t0) * 1000), f"connected to {ip}:{port}"
    except ConnectionRefusedError:
        return "REFUSED", int((time.time() - t0) * 1000), f"refused by {ip}:{port}"
    except socket.timeout:
        return "TIMEOUT", int((time.time() - t0) * 1000), f"no response from {ip}:{port}"
    except OSError as e:
        return "OTHER", int((time.time() - t0) * 1000), f"{type(e).__name__}: {e}"
    finally:
        try:
            sock.close()
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser(description="TCP SSH reachability check for WordPress sites")
    ap.add_argument("sites", nargs="*", help="Site names; omit to fetch from discovery API")
    ap.add_argument("--api", default=DEFAULT_API, help=f"Discovery API URL (default {DEFAULT_API})")
    ap.add_argument("--timeout", type=float, default=5.0, help="Per-probe TCP connect timeout in seconds (default 5)")
    ap.add_argument("--pace", type=float, default=0.5, help="Seconds between probes (default 0.5)")
    ap.add_argument("--port", type=int, default=22, help="Port to probe (default 22)")
    ap.add_argument("--no-abort", action="store_true", help="Don't abort early on 5 consecutive REFUSED")
    ap.add_argument("--out", default=None, help="CSV output path; default = docs/wordpress_audit/ssh_reachability_<timestamp>.csv")
    args = ap.parse_args()

    # Resolve site list
    if args.sites:
        sites = args.sites
        print(f"Probing {len(sites)} site(s) provided on the command line")
    else:
        print(f"Fetching site list from {args.api}...")
        try:
            sites = fetch_sites_from_api(args.api)
        except Exception as e:
            print(f"ERROR: failed to fetch site list: {type(e).__name__}: {e}", file=sys.stderr)
            sys.exit(2)
        print(f"  {len(sites)} sites discovered")
    print()

    # Output CSV path
    if args.out:
        out_path = Path(args.out)
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path("docs/wordpress_audit") / f"ssh_reachability_{stamp}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"{'site':<22s} {'status':<8s} {'latency':>8s}  detail")
    print(f"{'-' * 22} {'-' * 8} {'-' * 8}  {'-' * 60}")

    results = []
    counts = Counter()
    consec_refused = 0
    early_abort = False

    for i, site in enumerate(sites, 1):
        host = f"{site}.ssh.wpengine.net"
        status, latency_ms, detail = probe(host, args.port, args.timeout)
        results.append({
            "site": site,
            "status": status,
            "latency_ms": latency_ms,
            "detail": detail,
        })
        counts[status] += 1

        print(f"{site:<22s} {status:<8s} {latency_ms:>6d}ms  {detail}")

        if status == "REFUSED":
            consec_refused += 1
        else:
            consec_refused = 0

        if not args.no_abort and consec_refused >= 5:
            print()
            print("=" * 70)
            print("ABORT: 5 consecutive 'Connection refused' results.")
            print("Our IP is likely still on WP Engine's block list. Stopping here")
            print("to avoid extending the ban. Re-run later or pass --no-abort to override.")
            print("=" * 70)
            early_abort = True
            break

        if i < len(sites):
            time.sleep(args.pace)

    # Write CSV
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["site", "status", "latency_ms", "detail"])
        w.writeheader()
        w.writerows(results)

    # Summary
    print()
    print("=" * 70)
    print(f"SUMMARY  ({len(results)}/{len(sites)} probed{'  [EARLY ABORT]' if early_abort else ''})")
    print("=" * 70)
    for status in ("OK", "REFUSED", "TIMEOUT", "DNS", "OTHER"):
        n = counts.get(status, 0)
        if n:
            print(f"  {status:<8s} {n:>4d}")

    if counts.get("OK", 0):
        oks = [r for r in results if r["status"] == "OK"]
        avg_lat = sum(r["latency_ms"] for r in oks) // len(oks)
        print(f"  (avg latency for OK: {avg_lat}ms)")

    failures_by_status = {
        s: [r["site"] for r in results if r["status"] == s]
        for s in ("REFUSED", "TIMEOUT", "DNS", "OTHER")
    }
    for status, sites_list in failures_by_status.items():
        if sites_list:
            preview = ", ".join(sites_list[:8])
            more = f" + {len(sites_list)-8} more" if len(sites_list) > 8 else ""
            print(f"  {status} sites: {preview}{more}")

    print()
    print(f"CSV: {out_path}")

    if early_abort:
        sys.exit(2)
    if counts.get("OK", 0) == len(results):
        sys.exit(0)
    sys.exit(1)


if __name__ == "__main__":
    main()
