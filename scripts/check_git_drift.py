#!/usr/bin/env python3
"""Alert when this checkout lags origin/main -- a stuck `git pull` is invisible.

Background: on 2026-06-29 the prod VM checkout sat 6 commits / ~3 days behind
origin/main because a generated file dirtied the tree and silently aborted every
`git pull --ff-only`. The size-gate fix never reached the runner, every WordPress
nightly hard-blocked, and the Identity Hub upstream gate finally tripped 4 days
later. Nothing flagged the drift. This script closes that gap: run it from cron
on the VM; it fetches, measures how far HEAD is behind the upstream branch, and
emails the operator past a threshold.

Read-only on git (fetch only -- never pulls/resets). Exit 0 = ok/in-sync,
exit 2 = drift alert sent, exit 1 = could not evaluate (also alerts).
"""
import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

# Make the repo root importable when run as `python scripts/check_git_drift.py`
# (cron's invocation puts scripts/ on sys.path, not the root).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except (
    Exception
):  # noqa: BLE001 - .env / dotenv optional; cron env may already carry vars
    pass

from shared.config_loader import expand_env_vars
from shared.notifications import send_email_notification

logger = logging.getLogger("check_git_drift")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def _git(args, cwd):
    """Run a git command; return stripped stdout, or raise on failure."""
    out = subprocess.run(
        ["git", "-C", cwd, *args],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if out.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {out.stderr.strip()}")
    return out.stdout.strip()


def evaluate_drift(repo, branch="main", remote="origin"):
    """Fetch and return drift facts. Pure-ish: only git I/O, no email/exit.

    Returns dict: {behind, ahead, local, remote_sha, dirty, upstream}.
    """
    # Fetch is the only network call; it does NOT modify the working tree.
    _git(["fetch", remote, branch], cwd=repo)
    upstream = f"{remote}/{branch}"
    behind = int(_git(["rev-list", "--count", f"HEAD..{upstream}"], cwd=repo))
    ahead = int(_git(["rev-list", "--count", f"{upstream}..HEAD"], cwd=repo))
    local = _git(["log", "-1", "--format=%h %ci %s", "HEAD"], cwd=repo)
    remote_sha = _git(["log", "-1", "--format=%h %ci %s", upstream], cwd=repo)
    # Tracked-file changes only; untracked junk (logs, generated artifacts) is
    # expected on the VM and must NOT count as "dirty" here.
    dirty = bool(_git(["status", "--porcelain", "--untracked-files=no"], cwd=repo))
    return {
        "behind": behind,
        "ahead": ahead,
        "local": local,
        "remote_sha": remote_sha,
        "dirty": dirty,
        "upstream": upstream,
    }


def main():
    ap = argparse.ArgumentParser(description="Alert when checkout lags origin/main.")
    ap.add_argument(
        "--repo", default=".", help="Path to the git checkout (default: cwd)"
    )
    ap.add_argument("--branch", default="main")
    ap.add_argument("--remote", default="origin")
    ap.add_argument(
        "--max-behind",
        type=int,
        default=1,
        help="Alert when HEAD is more than this many commits behind upstream (default: 1)",
    )
    ap.add_argument("--quiet", action="store_true", help="Only log/alert on problems")
    args = ap.parse_args()

    try:
        d = evaluate_drift(args.repo, args.branch, args.remote)
    except (
        Exception
    ) as e:  # noqa: BLE001 - any git/network failure is itself alert-worthy
        msg = f"Git drift check could not evaluate {args.repo}: {e}"
        logger.error(msg)
        _alert("Git drift check FAILED to run", msg)
        return 1

    problems = []
    if d["behind"] > args.max_behind:
        problems.append(
            f"HEAD is {d['behind']} commit(s) behind {d['upstream']} "
            f"(threshold {args.max_behind})."
        )
    if d["dirty"]:
        problems.append(
            "Working tree has uncommitted TRACKED changes -- these will abort "
            "`git pull --ff-only` and freeze deploys."
        )

    if not problems:
        if not args.quiet:
            logger.info(
                "In sync: behind=%d ahead=%d local=[%s]",
                d["behind"],
                d["ahead"],
                d["local"],
            )
        return 0

    body = (
        "Deploy drift detected on a pipeline checkout. A checkout that lags the "
        "remote runs STALE CODE -- fixes pushed to "
        f"{args.branch} never reach this runner until it is pulled.\n\n"
        f"Repo:        {args.repo}\n"
        f"Local HEAD:  {d['local']}\n"
        f"{d['upstream']}: {d['remote_sha']}\n"
        f"Behind:      {d['behind']}   Ahead: {d['ahead']}   "
        f"Dirty(tracked): {d['dirty']}\n\n"
        "Problems:\n  - " + "\n  - ".join(problems) + "\n\n"
        "Fix: cd into the repo, resolve any tracked changes, then "
        "`git pull --ff-only`. If dirty on a GENERATED file, untrack it "
        "(see .gitignore notes) so it stops blocking pulls."
    )
    logger.warning("DRIFT: %s", " ".join(problems))
    _alert(f"Pipeline checkout drift: {d['behind']} commits behind {args.branch}", body)
    return 2


def _alert(title, message):
    """Best-effort email via the shared notifications config (configs/defaults.yaml).

    Reads defaults.yaml directly (not load_config) because "defaults" is not a
    real source and would fail source-schema validation. We only need the
    notifications.channels.email block with env vars expanded.
    """
    try:
        import yaml

        defaults_path = (
            Path(__file__).resolve().parent.parent / "configs" / "defaults.yaml"
        )
        with open(defaults_path, "r", encoding="utf-8") as f:
            cfg = expand_env_vars(yaml.safe_load(f) or {})
        email_cfg = cfg.get("notifications", {}).get("channels", {}).get("email", {})
        if not email_cfg.get("enabled"):
            logger.warning("Email channel disabled; drift alert not sent. %s", title)
            return
        send_email_notification(title, message, email_cfg)
    except Exception as e:  # noqa: BLE001 - never let alerting crash the check
        logger.error("Could not send drift alert (%s): %s", title, e)


if __name__ == "__main__":
    sys.exit(main())
