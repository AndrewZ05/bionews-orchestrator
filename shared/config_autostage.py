"""Auto-commit + push changed configs from the box to both repos, at run exit.

When schema auto-discovery writes a config during a run (any hour), that config
is marked dirty here. A single atexit handler then commits ALL dirty configs and
pushes them to BOTH git remotes (origin=bmacinnis, bionews) once per run. The
change then flows the normal git way: box -> GitHub -> laptop `git pull`.

Design (matches the operator's model "yaml changes are committed + pushed to the
two repos, then pulled to the laptop"). Two problems that once made
git-push-from-prod dangerous are now solved elsewhere:
  * comment stripping -- gcs_pipeline now preserves comments on write, so what
    the box commits is clean and reviewable.
  * frozen deploy pulls -- the box COMMITS the change (clean tree) instead of
    leaving an uncommitted file that would abort deploy.sh's `git pull`.

Enabled only when CONFIG_AUTO_PUSH is truthy, so local dev / tests / any box that
hasn't opted in are unaffected. Every path is wrapped so this can NEVER crash or
slow a pipeline run: it is best-effort side work after the real job is done.
Commits ONLY the specific configs/*.yaml paths (never `git add -A`), and pushes
to both remotes so they stay identical.
"""

import atexit
import logging
import os
import subprocess
import threading

logger = logging.getLogger("config_autopush")

_lock = threading.Lock()
_dirty = set()  # basenames of configs written this run, e.g. "mailchimp.yaml"
_registered = False

REMOTES = ("origin", "bionews")
BRANCH = "main"


def _enabled() -> bool:
    val = os.environ.get("CONFIG_AUTO_PUSH", "").strip().lower()
    return val in ("1", "true", "yes", "on")


def mark_config_dirty(config_path: str) -> None:
    """Record that a config file was changed this run; register the exit push.

    Called from the schema writer right after it updates a config. No-op (and no
    cost) when auto-push isn't enabled. Never raises.
    """
    global _registered
    try:
        if not _enabled():
            return
        name = os.path.basename(config_path)
        if not name.endswith(".yaml"):
            return
        with _lock:
            _dirty.add(name)
            if not _registered:
                atexit.register(_push_at_exit)
                _registered = True
                logger.debug("Registered config auto-push at exit.")
    except Exception:  # noqa: BLE001 - marking dirty must never affect the run
        pass


def _git(args, cwd, timeout=120):
    """Run a git command; return (rc, stdout, stderr). Never raises."""
    try:
        out = subprocess.run(
            ["git", "-C", cwd, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return out.returncode, out.stdout.strip(), out.stderr.strip()
    except Exception as e:  # noqa: BLE001
        return 1, "", str(e)


def _push_at_exit() -> None:
    """Commit + push all configs marked dirty this run. Best-effort; never raises."""
    try:
        with _lock:
            if not _dirty:
                return
            names = sorted(_dirty)

        rc, repo_root, _ = _git(["rev-parse", "--show-toplevel"], cwd=".")
        if rc != 0 or not repo_root:
            logger.warning("Config auto-push: not a git checkout; skipping.")
            return

        paths = [f"configs/{n}" for n in names]

        # Only proceed for files that actually differ from HEAD (a no-op write
        # shouldn't produce an empty commit).
        rc, diff, _ = _git(["diff", "--name-only", "HEAD", "--", *paths], cwd=repo_root)
        if rc != 0 or not diff.strip():
            logger.info("Config auto-push: nothing to commit for %s.", ", ".join(names))
            return

        logger.info("Config auto-push: committing %s", ", ".join(names))

        # Commit FIRST (staging only the named config paths -- never `git add -A`),
        # so the working tree is clean before we touch the remote. Committing then
        # reconciling avoids a dirty-tree fast-forward failure.
        rc, _, err = _git(["add", "--", *paths], cwd=repo_root)
        if rc != 0:
            logger.warning("Config auto-push: git add failed (%s); aborting.", err)
            return

        msg = (
            "config(auto): schema auto-discovery updated "
            + ", ".join(names)
            + "\n\nCommitted automatically by the pipeline on the prod box "
            "(shared/config_autostage.py) so the change flows to both repos."
        )
        rc, _, err = _git(["commit", "-m", msg], cwd=repo_root)
        if rc != 0:
            logger.warning("Config auto-push: git commit failed (%s); aborting.", err)
            return

        # Now reconcile with the remote: fetch + fast-forward the branch onto the
        # latest remote (rebasing our new commit on top). If it can't fast-forward
        # (genuine divergence), STOP -- never merge/reset/force on a prod box; the
        # commit stays local and the push below will be rejected, which we surface.
        _git(["fetch", REMOTES[0], BRANCH], cwd=repo_root, timeout=120)
        rc_rb, _, rb_err = _git(["rebase", f"{REMOTES[0]}/{BRANCH}"], cwd=repo_root)
        if rc_rb != 0:
            _git(["rebase", "--abort"], cwd=repo_root)  # leave a clean state
            logger.warning(
                "Config auto-push: local branch diverged from %s/%s (%s); committed "
                "locally but NOT pushing. Reconcile by hand.",
                REMOTES[0],
                BRANCH,
                rb_err,
            )
            return

        # Push to BOTH remotes; they must stay identical.
        all_ok = True
        for r in REMOTES:
            rc, _, err = _git(["push", r, f"HEAD:{BRANCH}"], cwd=repo_root, timeout=120)
            if rc == 0:
                logger.info("Config auto-push: pushed to %s.", r)
            else:
                all_ok = False
                logger.warning("Config auto-push: push to %s FAILED (%s).", r, err)

        if all_ok:
            logger.info(
                "Config auto-push: committed + pushed %s to %s. Pull on the laptop.",
                ", ".join(names),
                " + ".join(REMOTES),
            )
            # Notify so the operator knows to pull. Best-effort; never raises.
            _sha = _git(["rev-parse", "--short", "HEAD"], cwd=repo_root)[1]
            _stat = _git(
                ["show", "--stat", "--oneline", "HEAD", "--", *paths], cwd=repo_root
            )[1]
            _email_pushed(names, _sha, _stat)
        else:
            logger.warning(
                "Config auto-push: commit made but not all remotes received it; "
                "origin and bionews may differ -- reconcile by hand."
            )
    except Exception as e:  # noqa: BLE001 - exit-time push is best-effort
        logger.warning("Config auto-push at exit failed (non-fatal): %s", e)


# Operators to notify when a config is auto-pushed.
_ALERT_RECIPIENTS = [
    "bmacinnis@comcast.net",
    "bobmacinnis@gmail.com",
    "robertmacinnis@bionews.com",
]


def _email_pushed(names, sha, diffstat) -> None:
    """Email the operators that config(s) were auto-committed + pushed, so they
    know to pull. Uses the proven SMTP env vars the pipeline alerts use
    (SMTP_HOST / ALERT_SENDER_EMAIL / ALERT_SENDER_PASSWORD). Best-effort."""
    try:
        smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
        smtp_user = os.environ.get("SMTP_USER") or os.environ.get("ALERT_SENDER_EMAIL")
        smtp_password = os.environ.get("SMTP_PASSWORD") or os.environ.get(
            "ALERT_SENDER_PASSWORD"
        )
        if not smtp_user or not smtp_password:
            logger.warning(
                "Config auto-push: SMTP creds not in env; push email skipped."
            )
            return
        from shared.notifications import send_email_notification

        email_cfg = {
            "enabled": True,
            "smtp_host": smtp_host,
            "smtp_port": int(os.environ.get("SMTP_PORT", 587)),
            "smtp_user": smtp_user,
            "smtp_password": smtp_password,
            "from_email": os.environ.get("ALERT_SENDER_EMAIL", smtp_user),
            "recipients": [],
        }
        subject = f"Config auto-pushed: {', '.join(names)} ({sha})"
        body = (
            "The prod box auto-committed and pushed config change(s) to BOTH repos.\n"
            "Pull on your laptop to get them:  git pull   (or scripts\\pull_configs.bat)\n\n"
            f"Commit: {sha}\nFiles:  {', '.join(names)}\n\n"
            f"{diffstat}\n"
        )
        send_email_notification(
            subject, body, email_cfg, additional_recipients=_ALERT_RECIPIENTS
        )
        logger.info("Config auto-push: notified operators via email.")
    except Exception as e:  # noqa: BLE001 - email must never affect the run
        logger.warning("Config auto-push: could not send push email (%s).", e)
