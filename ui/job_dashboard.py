#!/usr/bin/env python3
"""
Pipeline Job Dashboard
======================

Streamlit app for viewing, filtering, and inspecting orchestrator pipeline jobs.
Queries the orchestrator_monitoring.jobs BigQuery table and displays results
with sorting, filtering, and clickable log file viewing.

Usage:
    # Dashboard listens on 8502 (limedit owns 8501). Bind 0.0.0.0 when serving
    # to users over a VM so the firewall/LB can reach it.
    streamlit run ui/job_dashboard.py --server.port 8502 --server.address 0.0.0.0
"""

from __future__ import annotations

import os

# Silence the gRPC/Abseil startup noise from the BigQuery client stack before any
# Google import pulls in the gRPC C-extension (these are read once, at load time):
#   - "ALTS creds ignored. Not running on GCP ..." (alts_credentials.cc)
#   - "All log messages before absl::InitializeLog() ... written to STDERR"
# Both are harmless "not on GCP" probes. setdefault so an operator can still
# override with a louder level from the environment when debugging.
os.environ.setdefault("GRPC_VERBOSITY", "NONE")
os.environ.setdefault("GLOG_minloglevel", "3")

import sys
from typing import Optional
from pathlib import Path

import streamlit as st
import pandas as pd

# ---------------------------------------------------------------------------
# Environment / path setup
# ---------------------------------------------------------------------------
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

env_path = _project_root / ".env"
if env_path.exists():
    try:
        from dotenv import load_dotenv

        load_dotenv(env_path)
    except ImportError:
        pass

from google.cloud import bigquery
from shared.monitoring import update_job_status
from shared.job_manager import cleanup_orphaned_jobs

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ENV_PROJECT_MAP = {
    "prod": "bi-data-391216",
    "dev": "bi-dev-391216",
}
MONITORING_DATASET = "orchestrator_monitoring"
JOBS_TABLE = "jobs"
LOCKS_TABLE = "job_locks"

# Data-quality audit surfaces (produced daily by `orchestrate.py --source anomaly_audit`
# and the freshness audit). Read-only here -- the dashboard just renders findings.
ANOMALY_DATASET = "anomaly_audit"
FRESHNESS_DATASET = "freshness_audit"
V_ANOMALY_LATEST_RUN = "v_latest_anomaly_run"
V_TABLE_HEALTH = "v_table_health"

# ---------------------------------------------------------------------------
# Access control (mirrors the limedit mapper's IAP + BigQuery-allowlist model).
# Identity comes from Google IAP; the allowlist + role live in a BigQuery table.
# The users table is pinned to PROD regardless of the env toggle -- access is
# not per-environment. `admin` unlocks destructive actions + user management;
# `reviewer` is read-only.
# ---------------------------------------------------------------------------
AUTH_PROJECT_ID = "bi-data-391216"
USERS_TABLE = f"{AUTH_PROJECT_ID}.{MONITORING_DATASET}.dashboard_users"
# Gmail-style dot/plus normalization so one Google account matches one row.
EMAIL_DOT_DOMAINS = {"bionews.com", "gmail.com"}
# Seeds the first admin when dashboard_users is empty (normalized form).
BOOTSTRAP_ADMIN_EMAIL = "robertmacinnis@bionews.com"

JOB_STATUSES = [
    "QUEUED",
    "RUNNING",
    "COMPLETED",
    "FAILED",
    "WARNING",
    "CANCELLED",
    "KILLED",
    "REVIEWED",
]

QUERY_COLUMNS = [
    "job_id",
    "source",
    "job_name",
    "status",
    "triggered_by",
    "refresh_mode",
    "hostname",
    "user",
    "queued_at",
    "started_at",
    "completed_at",
    "duration_seconds",
    "total_rows",
    "tables_processed",
    "sites_processed",
    "error_message",
    "error_type",
    "log_file_path",
    "command_line",
    "tables",
    "groups",
]

STATUS_COLORS = {
    "success": ("#0e8a16", "#dafbe1"),
    "completed": ("#0e8a16", "#dafbe1"),
    "running": ("#0366d6", "#dbedff"),
    "queued": ("#6f42c1", "#f0e6ff"),
    "failed": ("#cb2431", "#ffeef0"),
    "killed": ("#e36209", "#fff5e6"),
    "cancelled": ("#6a737d", "#f1f1f1"),
    "warning": ("#e36209", "#fff5e6"),
    "reviewed": ("#2c6fbb", "#e0ecf8"),
}


# ---------------------------------------------------------------------------
# Helpers (defined before use)
# ---------------------------------------------------------------------------
def _escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def fmt_duration(seconds) -> str:
    try:
        if pd.isna(seconds) or seconds is None:
            return "-"
    except (TypeError, ValueError):
        return "-"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"


def fmt_rows(val) -> str:
    try:
        if pd.isna(val) or val is None:
            return "-"
    except (TypeError, ValueError):
        return "-"
    return f"{int(val):,}"


def fmt_ts(val) -> str:
    try:
        if pd.isna(val) or val is None:
            return "-"
    except (TypeError, ValueError):
        return "-"
    try:
        return val.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(val)


def style_status(val):
    status_lower = val.lower() if isinstance(val, str) else ""
    fg, bg = STATUS_COLORS.get(status_lower, ("#333", "#eee"))
    return f"color: {fg}; background-color: {bg}; font-weight: 600; border-radius: 4px; padding: 2px 6px;"


def lock_badge(status: str, source: str, active_locks: dict) -> str:
    """Lock-state badge for a job row (feature #2).

    Only meaningful for RUNNING jobs: a live lock means the process is genuinely
    running; its absence means the row is orphaned and due to be KILLED on the
    next reconcile. Non-RUNNING rows show "-". If lock info is unavailable
    (empty dict), RUNNING rows show a neutral "?" rather than a false red.
    """
    if str(status).upper() != "RUNNING":
        return '<span class="muted">-</span>'
    if not active_locks:
        # unknown lock state (lock table unavailable)
        return _pill("? unknown", "#8a5a00", "#fff3d6")
    expires = active_locks.get(source)
    if expires is None or pd.isna(expires):
        return _pill("no lock", "#cb2431", "#ffeef0")  # orphaned
    try:
        now = pd.Timestamp.now(tz="UTC")
        secs = int((expires - now).total_seconds())
        if secs <= 0:
            return _pill("no lock", "#cb2431", "#ffeef0")
        mins = secs // 60
        rem = f"{mins}m" if mins else f"{secs}s"
        return _pill(rem, "#0e8a16", "#dafbe1")  # held + countdown to lease expiry
    except Exception:
        return _pill("held", "#0e8a16", "#dafbe1")


def _pill(text: str, fg: str, bg: str) -> str:
    """Render a rounded colored pill (HTML) using the global .pill class."""
    return (
        f'<span class="pill" style="color:{fg};background:{bg};">'
        f"{_escape_html(str(text))}</span>"
    )


def status_pill(status: str) -> str:
    """Colored pill for a job status, using the shared STATUS_COLORS map (#1)."""
    s = str(status or "").strip()
    fg, bg = STATUS_COLORS.get(s.lower(), ("#57606a", "#eaecef"))
    return _pill(s.upper() or "?", fg, bg)


def trigger_pill(trigger: str) -> str:
    """Pill for how a job was triggered.

    orchestrate.py stamps `triggered_by` as one of manual/scheduled/api/chained
    (default manual). We distinguish automated runs (scheduled/api/chained) from
    hand-run ones so the source of a job is obvious at a glance.
    """
    t = str(trigger or "").strip().lower()
    label, fg, bg = {
        "scheduled": ("\U0001f552 Scheduled", "#0b5cad", "#e6f0fb"),
        "manual": ("✋ Manual", "#6a4a00", "#fff3d6"),
        "api": ("\U0001f517 API", "#5a3aa8", "#efe9fb"),
        "chained": ("\U0001f517 Chained", "#57606a", "#eaecef"),
    }.get(t, (t or "-", "#57606a", "#eaecef"))
    if not t:
        return '<span class="muted">-</span>'
    return _pill(label, fg, bg)


def mono(text) -> str:
    """Monospace chip for IDs / hostnames (#6)."""
    t = "" if text is None else str(text)
    return f'<span class="mono">{_escape_html(t)}</span>' if t else "-"


def fmt_relative(val) -> str:
    """Human 'time ago' for a timestamp (#6). Empty string if not parseable."""
    try:
        if pd.isna(val) or val is None:
            return ""
    except (TypeError, ValueError):
        return ""
    try:
        now = pd.Timestamp.now(tz="UTC")
        ts = pd.Timestamp(val)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        secs = int((now - ts).total_seconds())
        if secs < 0:
            return "in the future"
        if secs < 60:
            return f"{secs}s ago"
        mins = secs // 60
        if mins < 60:
            return f"{mins}m ago"
        hrs = mins // 60
        if hrs < 24:
            return f"{hrs}h ago"
        days = hrs // 24
        return f"{days}d ago"
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Pipeline Jobs",
    page_icon="\U0001f527",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
    .block-container { padding-top: 1.2rem; }
    .log-viewer {
        background: #1e1e1e;
        color: #d4d4d4;
        font-family: 'Cascadia Code', 'Fira Code', 'Consolas', monospace;
        font-size: 0.78rem;
        line-height: 1.5;
        padding: 16px;
        border-radius: 8px;
        max-height: 600px;
        overflow: auto;
        white-space: pre-wrap;
        word-break: break-word;
    }

    /* ---- Status / lock pills (#1) ---- */
    .pill {
        display: inline-block;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.02em;
        padding: 2px 9px;
        border-radius: 999px;
        line-height: 1.5;
        white-space: nowrap;
    }
    .mono {
        font-family: 'Cascadia Code', 'Fira Code', 'Consolas', monospace;
        font-size: 0.78rem;
        background: rgba(128, 128, 128, 0.12);
        padding: 1px 6px;
        border-radius: 4px;
    }
    .muted { color: #8a929c; font-size: 0.78rem; }

    /* ---- Metric cards (#2): tint by semantic role ---- */
    div[data-testid="stMetric"] {
        background: rgba(128, 128, 128, 0.06);
        border: 1px solid rgba(128, 128, 128, 0.18);
        border-radius: 10px;
        padding: 12px 14px;
    }
    div[data-testid="stMetric"].metric-good { background: #eaf7ee; border-color: #b7e2c4; }
    div[data-testid="stMetric"].metric-bad  { background: #fdeef0; border-color: #f3c2c8; }
    div[data-testid="stMetric"].metric-run  { background: #eaf2fd; border-color: #bcd6f5; }

    /* ---- Zebra striping + hover on the hand-built row grid (#4) ---- */
    .job-rows div[data-testid="stHorizontalBlock"] {
        padding: 3px 4px;
        border-radius: 6px;
        align-items: center;
    }
    .job-rows div[data-testid="stHorizontalBlock"]:nth-of-type(even) {
        background: rgba(128, 128, 128, 0.05);
    }
    .job-rows div[data-testid="stHorizontalBlock"]:hover {
        background: rgba(3, 102, 214, 0.09);
    }

    /* ---- Section separators (#7) ---- */
    .section-head {
        font-size: 1.05rem;
        font-weight: 700;
        margin: 0.2rem 0 0.4rem 0;
    }
    .thin-rule {
        border: none;
        border-top: 1px solid rgba(128, 128, 128, 0.22);
        margin: 0.6rem 0 0.8rem 0;
    }

    /* ---- Environment badge (#8) ---- */
    .env-badge {
        display: inline-block;
        font-size: 0.72rem;
        font-weight: 800;
        letter-spacing: 0.06em;
        padding: 3px 10px;
        border-radius: 6px;
        vertical-align: middle;
        margin-left: 10px;
    }
    .env-prod { background: #fdeef0; color: #cb2431; border: 1px solid #f3c2c8; }
    .env-dev  { background: #eaf7ee; color: #0e8a16; border: 1px solid #b7e2c4; }
</style>
""",
    unsafe_allow_html=True,
)

if "visible_logs" not in st.session_state:
    st.session_state.visible_logs = {}


# ---------------------------------------------------------------------------
# Log viewer dialog
# ---------------------------------------------------------------------------
@st.dialog("Job Log", width="large")
def show_log_dialog(job_id: str, log_path: str):
    """Modal dialog that fetches and displays a job's log file."""
    st.caption(f"**Job:** {job_id}")
    st.caption(f"**Path:** {log_path}")
    content = fetch_log_content(log_path)
    if content:
        lines = content.split("\n")
        st.caption(f"{len(lines):,} lines")
        st.markdown(
            f'<div class="log-viewer">{_escape_html(content)}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.warning("Log file could not be retrieved.")


# ---------------------------------------------------------------------------
# BigQuery / GCS helpers
# ---------------------------------------------------------------------------
@st.cache_resource
def get_bq_client(project_id: str):
    """Create BigQuery client (cached) — matches pattern in other UI apps."""
    return bigquery.Client(project=project_id)


# ---------------------------------------------------------------------------
# Access control (IAP identity + BigQuery allowlist). Lifted from the limedit
# mapper so the two operator apps gate access the same way.
# ---------------------------------------------------------------------------
def _normalize_email(email: str) -> str:
    """Canonicalize a Gmail-style email for matching the users table: lowercase,
    strip plus-tag, and drop dots in the local part for Workspace/Gmail domains
    so one Google account matches one row under any alias."""
    if not email:
        return ""
    email = email.strip().lower()
    if "@" not in email:
        return email
    local, _, domain = email.partition("@")
    if "+" in local:
        local = local.split("+", 1)[0]
    if domain in EMAIL_DOT_DOMAINS:
        local = local.replace(".", "")
    return f"{local}@{domain}"


def _get_caller_email() -> "tuple[str, bool]":
    """Return (email, behind_iap). Behind Google IAP every request carries
    X-Goog-Authenticated-User-Email as 'accounts.google.com:user@domain'.
    Outside IAP (local dev) fall back to a synthetic OS-username email.

    Requires Streamlit >= 1.36 for st.context.headers -- on older Streamlit the
    read raises and every caller silently gets the dev fallback, bypassing the
    allowlist. The deploy runbook pins the VM to a recent Streamlit."""
    raw = ""
    try:
        raw = st.context.headers.get("X-Goog-Authenticated-User-Email", "")
    except Exception:
        raw = ""
    if raw:
        email = raw.split(":", 1)[1].lower() if ":" in raw else raw.lower()
        return email, True
    dev_user = os.environ.get("USER") or os.environ.get("USERNAME") or "local-dev"
    return f"{dev_user}@local-dev".lower(), False


@st.cache_resource
def _ensure_users_table() -> None:
    """Create dashboard_users (idempotent) and seed the bootstrap admin when the
    table is empty. Cached as a resource so it runs once per server process."""
    client = get_bq_client(AUTH_PROJECT_ID)
    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{USERS_TABLE}` (
            email STRING NOT NULL,
            display_name STRING,
            role STRING NOT NULL,
            is_active BOOL NOT NULL,
            created_at TIMESTAMP NOT NULL,
            created_by STRING,
            last_login_at TIMESTAMP,
            notes STRING
        )
        """
    ).result()
    # Seed bootstrap admin only if the table is empty (MERGE ON FALSE is the
    # BigQuery-correct "insert if no rows exist" idiom).
    client.query(
        f"""
        MERGE `{USERS_TABLE}` T
        USING (SELECT @email AS email, @display_name AS display_name) S
        ON FALSE
        WHEN NOT MATCHED BY TARGET
          AND NOT EXISTS (SELECT 1 FROM `{USERS_TABLE}`)
        THEN INSERT
          (email, display_name, role, is_active, created_at, created_by, notes)
          VALUES (S.email, S.display_name, 'admin', TRUE,
                  CURRENT_TIMESTAMP(), 'bootstrap',
                  'Seeded on first run by _ensure_users_table.')
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", BOOTSTRAP_ADMIN_EMAIL),
                bigquery.ScalarQueryParameter(
                    "display_name", "STRING", "Robert MacInnis"
                ),
            ]
        ),
    ).result()


@st.cache_data(ttl=60, show_spinner=False)
def _lookup_user(_client, normalized_email: str) -> "dict | None":
    """Return the dashboard_users row for this email, or None if absent/inactive.
    Cached 60s; fail-closed (None) on any query error."""
    if not normalized_email:
        return None
    try:
        rows = list(
            _client.query(
                f"""
                SELECT email, display_name, role, is_active
                FROM `{USERS_TABLE}`
                WHERE email = @email AND is_active = TRUE
                LIMIT 1
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "email", "STRING", normalized_email
                        )
                    ]
                ),
            ).result()
        )
    except Exception:
        return None
    if not rows:
        return None
    r = rows[0]
    return {
        "email": r.email,
        "display_name": r.display_name,
        "role": r.role,
        "is_active": r.is_active,
    }


def _stamp_last_login(client, normalized_email: str) -> None:
    """Fire-and-forget UPDATE of last_login_at; failures are silent."""
    try:
        client.query(
            f"UPDATE `{USERS_TABLE}` SET last_login_at = CURRENT_TIMESTAMP() WHERE email = @email",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", normalized_email)
                ]
            ),
        ).result()
    except Exception:
        pass


def _require_authorized_user() -> dict:
    """Identify the caller via IAP, look them up in dashboard_users, stash the
    record in session_state, and return it. Unauthorized callers are hard-blocked
    with st.stop(). Local dev (no IAP header) is treated as admin so the app
    stays usable while developing."""
    raw_email, behind_iap = _get_caller_email()
    normalized = _normalize_email(raw_email)

    try:
        _ensure_users_table()
        client = get_bq_client(AUTH_PROJECT_ID)
    except Exception as exc:
        # Fail CLOSED: if the access backend is unreachable we cannot verify the
        # allowlist, so block rather than let anyone through. (Local dev without
        # IAP is handled below and never reaches this path in practice.)
        if not behind_iap:
            client = None  # dev bypass below doesn't need the users table
        else:
            st.error(
                "Access control is temporarily unavailable (could not reach the "
                f"users table): {exc}. Try again shortly or contact an admin."
            )
            st.stop()

    if not behind_iap:
        user = {
            "email": normalized,
            "display_name": "Local Dev",
            "role": "admin",
            "is_active": True,
        }
        st.session_state["dashboard_user"] = user
        st.session_state["dashboard_email"] = normalized
        st.session_state["behind_iap"] = False
        return user

    user = _lookup_user(client, normalized)
    if not user:
        st.error(
            f"Access denied for {raw_email} (normalized: {normalized}). "
            "You're signed in via IAP but not on the dashboard users list, or "
            "your account is deactivated. Ask an admin to add you."
        )
        st.stop()

    _stamp_last_login(client, normalized)
    st.session_state["dashboard_user"] = user
    st.session_state["dashboard_email"] = normalized
    st.session_state["behind_iap"] = True
    return user


def _is_admin() -> bool:
    """True if the current session's user has the admin role."""
    return (st.session_state.get("dashboard_user") or {}).get("role") == "admin"


def _add_user(
    client, email: str, display_name: str, role: str, notes: str
) -> "tuple[bool, str]":
    """Insert a dashboard_users row (normalized email). Refuses duplicates."""
    norm = _normalize_email(email)
    if not norm or "@" not in norm:
        return False, "Invalid email."
    if role not in ("reviewer", "admin"):
        return False, "Role must be 'reviewer' or 'admin'."
    try:
        existing = list(
            client.query(
                f"SELECT email, is_active FROM `{USERS_TABLE}` WHERE email = @email",
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("email", "STRING", norm)
                    ]
                ),
            ).result()
        )
        if existing:
            if existing[0].is_active:
                return False, f"{norm} already exists and is active."
            return False, f"{norm} exists but is deactivated -- reactivate it instead."
        actor = st.session_state.get("dashboard_email") or "unknown"
        client.query(
            f"""
            INSERT INTO `{USERS_TABLE}`
              (email, display_name, role, is_active, created_at, created_by, notes)
            VALUES (@email, @display_name, @role, TRUE,
                    CURRENT_TIMESTAMP(), @actor, @notes)
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", norm),
                    bigquery.ScalarQueryParameter(
                        "display_name", "STRING", display_name or None
                    ),
                    bigquery.ScalarQueryParameter("role", "STRING", role),
                    bigquery.ScalarQueryParameter("actor", "STRING", actor),
                    bigquery.ScalarQueryParameter("notes", "STRING", notes or None),
                ]
            ),
        ).result()
    except Exception as e:
        return False, f"Add failed: {e}"
    _lookup_user.clear()
    return True, f"Added {norm} as {role}."


def _set_user_active(client, email: str, is_active: bool) -> "tuple[bool, str]":
    """Toggle is_active on a user row."""
    try:
        client.query(
            f"UPDATE `{USERS_TABLE}` SET is_active = @is_active WHERE email = @email",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", email),
                    bigquery.ScalarQueryParameter("is_active", "BOOL", is_active),
                ]
            ),
        ).result()
    except Exception as e:
        return False, f"Update failed: {e}"
    _lookup_user.clear()
    return True, f"{email} {'activated' if is_active else 'deactivated'}."


def _set_user_role(client, email: str, role: str) -> "tuple[bool, str]":
    """Change a user's role between reviewer and admin."""
    if role not in ("reviewer", "admin"):
        return False, "Role must be 'reviewer' or 'admin'."
    try:
        client.query(
            f"UPDATE `{USERS_TABLE}` SET role = @role WHERE email = @email",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", email),
                    bigquery.ScalarQueryParameter("role", "STRING", role),
                ]
            ),
        ).result()
    except Exception as e:
        return False, f"Update failed: {e}"
    _lookup_user.clear()
    return True, f"{email} role set to {role}."


@st.cache_data(ttl=30, show_spinner=False)
def _list_users(_client) -> pd.DataFrame:
    """All dashboard_users rows for the Users panel. Cached briefly."""
    return _client.query(
        f"""
        SELECT email, display_name, role, is_active
        FROM `{USERS_TABLE}`
        ORDER BY is_active DESC, role DESC, email
        """
    ).to_dataframe()


@st.cache_data(ttl=120, show_spinner="Querying jobs from BigQuery...")
def load_jobs(_project_id: str, days_back: int = 30, limit: int = 500) -> pd.DataFrame:
    """Fetch recent jobs from BigQuery and return as a DataFrame."""
    client = get_bq_client(_project_id)
    table_id = f"{_project_id}.{MONITORING_DATASET}.{JOBS_TABLE}"
    cols = ",\n        ".join(f"`{c}`" for c in QUERY_COLUMNS)

    query = f"""
    SELECT
        {cols}
    FROM `{table_id}`
    WHERE queued_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days_back DAY)
    ORDER BY queued_at DESC
    LIMIT @row_limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("days_back", "INT64", days_back),
            bigquery.ScalarQueryParameter("row_limit", "INT64", limit),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()

    for col in ("queued_at", "started_at", "completed_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    for col in (
        "total_rows",
        "tables_processed",
        "sites_processed",
        "duration_seconds",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.lower()

    return df


@st.cache_data(ttl=30, show_spinner=False)
def load_active_locks(_project_id: str) -> dict:
    """Return {source: earliest unexpired expires_at (UTC Timestamp)} from job_locks.

    A source present here has a live lock (the process is really running). The
    earliest expiry is used for the countdown. Returns {} if the table is missing
    or unreadable -- the dashboard treats "no lock info" as "unknown", not "dead".
    """
    client = get_bq_client(_project_id)
    table_id = f"{_project_id}.{MONITORING_DATASET}.{LOCKS_TABLE}"
    query = f"""
    SELECT source, MIN(expires_at) AS expires_at
    FROM `{table_id}`
    WHERE expires_at > CURRENT_TIMESTAMP()
      AND source IS NOT NULL
    GROUP BY source
    """
    try:
        rows = client.query(query).result()
        out = {}
        for r in rows:
            out[r.source] = pd.to_datetime(r.expires_at, utc=True, errors="coerce")
        return out
    except Exception:
        # Missing table / permissions / cold table: fall back to "unknown".
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def load_table_health(_project_id: str) -> pd.DataFrame:
    """One row per audited table: freshness status + active-anomaly rollup.

    Reads freshness_audit.v_table_health. Returns an empty DataFrame if the view
    is missing (e.g. the audit has never run in this env) -- the caller treats
    that as "no audit yet", not an error.
    """
    client = get_bq_client(_project_id)
    view_id = f"{_project_id}.{FRESHNESS_DATASET}.{V_TABLE_HEALTH}"
    query = f"SELECT * FROM `{view_id}`"
    try:
        df = client.query(query).to_dataframe()
        # available=True means the query ran (0 rows == audited & clean, not missing).
        df.attrs["available"] = True
        df.attrs["error"] = None
    except Exception as exc:
        empty = pd.DataFrame()
        empty.attrs["available"] = False
        empty.attrs["error"] = str(exc).splitlines()[0] if str(exc) else "query failed"
        return empty

    if "most_recent_date" in df.columns:
        df["most_recent_date"] = pd.to_datetime(df["most_recent_date"], errors="coerce")
    if "freshness_audited_at" in df.columns:
        df["freshness_audited_at"] = pd.to_datetime(
            df["freshness_audited_at"], utc=True, errors="coerce"
        )
    for col in ("days_behind", "active_anomalies"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_active_anomalies(_project_id: str) -> pd.DataFrame:
    """ACTIVE anomaly findings from the most recent audit run.

    Reads anomaly_audit.v_latest_anomaly_run filtered to the rows an operator
    should act on today (status=ANOMALY, scope=ACTIVE). Empty DataFrame if the
    view is missing or the latest run was clean.
    """
    client = get_bq_client(_project_id)
    view_id = f"{_project_id}.{ANOMALY_DATASET}.{V_ANOMALY_LATEST_RUN}"
    query = f"""
    SELECT
        dataset_name, table_name, detector, anomaly_date,
        observed_count, expected_low, median_count, date_lag_days,
        severity, scope, audit_status, audited_at
    FROM `{view_id}`
    WHERE audit_status = 'ANOMALY' AND scope = 'ACTIVE'
    """
    try:
        df = client.query(query).to_dataframe()
        df.attrs["available"] = True
        df.attrs["error"] = None
    except Exception as exc:
        empty = pd.DataFrame()
        empty.attrs["available"] = False
        empty.attrs["error"] = str(exc).splitlines()[0] if str(exc) else "query failed"
        return empty

    if "anomaly_date" in df.columns:
        df["anomaly_date"] = pd.to_datetime(df["anomaly_date"], errors="coerce")
    if "audited_at" in df.columns:
        df["audited_at"] = pd.to_datetime(df["audited_at"], utc=True, errors="coerce")
    for col in ("observed_count", "expected_low", "median_count", "date_lag_days"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def reconcile_running_jobs(project_id: str) -> int:
    """Kill stale RUNNING rows (no live process AND no active lock), any host/OS.

    Delegates to the shared orchestrator cleanup so the dashboard applies the
    exact same rules as the pipeline. Returns the number of rows marked KILLED.
    """
    client = get_bq_client(project_id)
    return cleanup_orphaned_jobs(client, MONITORING_DATASET)


def fetch_log_content(log_path: str) -> Optional[str]:
    """Download log content from GCS or local filesystem."""
    if not log_path:
        return None
    try:
        if log_path.startswith("gs://"):
            from google.cloud import storage

            parts = log_path[5:].split("/", 1)
            if len(parts) != 2:
                return None
            bucket_name, blob_name = parts
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            if not blob.exists():
                return f"[Log file not found in GCS: {log_path}]"
            return blob.download_as_text()
        else:
            if os.path.exists(log_path):
                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                    return f.read()
            return f"[Log file not found locally: {log_path}]"
    except Exception as exc:
        return f"[Error fetching log: {exc}]"


# ===================================================================
# ACCESS GATE -- runs before any page content. Unauthorized callers are
# stopped here; the authorized user + role are stashed in session_state.
# ===================================================================
_auth_user = _require_authorized_user()
_behind_iap = st.session_state.get("behind_iap", False)
_role_suffix = f" ({_auth_user['role']})" if _auth_user["role"] == "admin" else ""
_auth_label = _auth_user["display_name"] or _auth_user["email"]
_auth_badge = (
    f"Signed in as: **{_auth_label}**{_role_suffix}  \n_{_auth_user['email']}_"
)
if not _behind_iap:
    _auth_badge += "  \n_(local dev -- IAP header not present)_"

# ===================================================================
# SIDEBAR (#5: grouped, tightened)
# ===================================================================
st.sidebar.markdown("### \U0001f527 Pipeline Jobs")
st.sidebar.info(_auth_badge)

env = st.sidebar.radio("Environment", list(ENV_PROJECT_MAP.keys()), horizontal=True)
project_id = ENV_PROJECT_MAP[env]

# -------------------------------------------------------------------
# Reconcile stale RUNNING rows (feature #1).
# Viewing the dashboard is what flips a bogus RUNNING -> KILLED: on the
# first load for a given environment we auto-reconcile once, and a manual
# button lets the operator re-run it on demand. Both apply the shared
# orchestrator rules (dead PID OR no active lock, any host/OS).
# -------------------------------------------------------------------
if "reconciled_envs" not in st.session_state:
    st.session_state.reconciled_envs = set()
if "last_updated" not in st.session_state:
    st.session_state.last_updated = {}


def _run_reconcile(pid: str, quiet: bool = False) -> None:
    try:
        killed = reconcile_running_jobs(pid)
        # Fresh statuses/locks must be re-read after a reconcile.
        load_jobs.clear()
        load_active_locks.clear()
        if killed:
            st.sidebar.success(f"Reconciled: {killed} stale RUNNING job(s) -> KILLED")
        elif not quiet:
            st.sidebar.info("Reconciled: no stale RUNNING jobs found")
    except Exception as exc:  # noqa: BLE001 -- surface, never crash the page
        st.sidebar.warning(f"Reconcile failed: {exc}")


# Auto-reconcile on first load KILLS stale RUNNING rows, so it's admin-only.
if _is_admin() and env not in st.session_state.reconciled_envs:
    _run_reconcile(project_id, quiet=True)
    st.session_state.reconciled_envs.add(env)
    st.session_state.last_updated[env] = pd.Timestamp.now(tz="UTC")

# Refresh (read-only, everyone) + Reconcile (destructive, admin-only).
_b1, _b2 = st.sidebar.columns(2)
if _b1.button("\U0001f504 Refresh", use_container_width=True, help="Re-query jobs"):
    load_jobs.clear()
    load_active_locks.clear()
    st.session_state.visible_logs = {}
    st.session_state.last_updated[env] = pd.Timestamp.now(tz="UTC")
    st.rerun()
if _b2.button(
    "\U0001f9f9 Reconcile",
    use_container_width=True,
    help=(
        "Kill stale RUNNING jobs (no live lock/process)"
        if _is_admin()
        else "Admin only -- ask an admin to reconcile"
    ),
    disabled=not _is_admin(),
):
    _run_reconcile(project_id)
    st.session_state.last_updated[env] = pd.Timestamp.now(tz="UTC")
    st.rerun()

_lu = st.session_state.last_updated.get(env)
if _lu is not None:
    st.sidebar.caption(f"Last updated {fmt_relative(_lu)}")

# Filters grouped in a collapsible section (#5)
_filters = st.sidebar.expander("\U0001f50d Filters", expanded=True)
days_back = _filters.slider("Days back", 1, 90, 14)
row_limit = _filters.number_input("Max rows", 50, 5000, 500, step=50)


def _render_users_panel(container) -> None:
    """User/access management, mirroring the limedit Members tab. The list is
    visible to everyone; the add/role/deactivate forms render only for admins.
    Includes the self-lockout guard (can't deactivate or demote yourself)."""
    client = get_bq_client(AUTH_PROJECT_ID)
    container.caption(
        "These users can sign in. Identity comes from Google IAP; dot/plus-tag "
        "aliases are normalized so each Google account matches one row."
    )
    try:
        users_df = _list_users(client)
    except Exception as exc:
        container.warning(f"Could not load users: {exc}")
        return
    container.dataframe(users_df, use_container_width=True, hide_index=True)

    if not _is_admin():
        container.info(
            "Read-only view. Ask an admin (role = 'admin') to change access."
        )
        return

    with container.form("dash_add_user", clear_on_submit=True):
        st.markdown("**Add a user**")
        new_email = st.text_input("Email", placeholder="alice@bionews.com")
        new_role = st.selectbox("Role", ["reviewer", "admin"], index=0)
        new_name = st.text_input("Display name (optional)")
        if st.form_submit_button("Add user", type="primary"):
            ok, msg = _add_user(client, new_email, new_name, new_role, "")
            (st.success if ok else st.error)(msg)
            if ok:
                _list_users.clear()
                st.rerun()

    with container.form("dash_modify_user", clear_on_submit=False):
        st.markdown("**Change role / deactivate**")
        all_emails = users_df["email"].tolist() if not users_df.empty else []
        target = st.selectbox("User", all_emails) if all_emails else ""
        set_role = st.selectbox("Set role", ["reviewer", "admin"], key="dash_set_role")
        c1, c2, c3 = st.columns(3)
        change_role = c1.form_submit_button("Update role")
        deactivate = c2.form_submit_button("Deactivate")
        reactivate = c3.form_submit_button("Reactivate")
        if target and (change_role or deactivate or reactivate):
            me = st.session_state.get("dashboard_email")
            if target == me and (deactivate or (change_role and set_role != "admin")):
                st.error("You can't deactivate or demote yourself. Ask another admin.")
            else:
                if change_role:
                    ok, msg = _set_user_role(client, target, set_role)
                elif deactivate:
                    ok, msg = _set_user_active(client, target, False)
                else:
                    ok, msg = _set_user_active(client, target, True)
                (st.success if ok else st.error)(msg)
                if ok:
                    _list_users.clear()
                    st.rerun()


_users_exp = st.sidebar.expander("\U0001f465 Users", expanded=False)
_render_users_panel(_users_exp)

# Load active locks (feature #2) so every RUNNING row can show its lock state.
active_locks = load_active_locks(project_id)

# Load data with visible error handling
try:
    df_all = load_jobs(project_id, days_back, row_limit)
except Exception as e:
    st.error(f"Failed to load jobs from BigQuery: {e}")
    st.info(f"Project: {project_id} | Table: {MONITORING_DATASET}.{JOBS_TABLE}")
    st.stop()

if df_all.empty:
    st.markdown("## Pipeline Job Dashboard")
    st.warning(
        "No jobs found for the selected period. Try increasing 'Days back' in the sidebar."
    )
    st.stop()

# Dynamic filter options from loaded data
source_options = sorted(df_all["source"].dropna().unique().tolist())
status_options = sorted(df_all["status"].dropna().unique().tolist())
host_options = sorted(df_all["hostname"].dropna().unique().tolist())
user_options = sorted(df_all["user"].dropna().unique().tolist())
trigger_options = sorted(df_all["triggered_by"].dropna().unique().tolist())
refresh_options = sorted(df_all["refresh_mode"].dropna().unique().tolist())

sel_sources = _filters.multiselect("Source", source_options)
sel_statuses = _filters.multiselect("Status", status_options)
sel_hosts = _filters.multiselect("Hostname", host_options)
sel_users = _filters.multiselect("User", user_options)
sel_triggers = _filters.multiselect("Triggered By", trigger_options)
sel_refresh = _filters.multiselect("Refresh Mode", refresh_options)

search_text = _filters.text_input("Search (job ID, error, command...)")

# Apply filters
df = df_all.copy()
if sel_sources:
    df = df[df["source"].isin(sel_sources)]
if sel_statuses:
    df = df[df["status"].isin(sel_statuses)]
if sel_hosts:
    df = df[df["hostname"].isin(sel_hosts)]
if sel_users:
    df = df[df["user"].isin(sel_users)]
if sel_triggers:
    df = df[df["triggered_by"].isin(sel_triggers)]
if sel_refresh:
    df = df[df["refresh_mode"].isin(sel_refresh)]
if search_text:
    mask = pd.Series(False, index=df.index)
    for col in ("job_id", "error_message", "command_line", "job_name", "error_type"):
        if col in df.columns:
            mask |= df[col].astype(str).str.contains(search_text, case=False, na=False)
    df = df[mask]

st.sidebar.caption(f"Showing **{len(df)}** of {len(df_all)} jobs")


# ===================================================================
# HEADER METRICS
# ===================================================================
# Title + environment badge (#8): unmistakable which env you are mutating.
_env_cls = "env-prod" if env == "prod" else "env-dev"
st.markdown(
    f'<h2 style="margin-bottom:0.4rem;">Pipeline Job Dashboard'
    f'<span class="env-badge {_env_cls}">{env.upper()}</span></h2>',
    unsafe_allow_html=True,
)

total = len(df)
_status_u = df["status"].astype(str).str.strip().str.upper()
success_count = int(_status_u.isin(["SUCCESS", "COMPLETED"]).sum())
failed_count = int((_status_u == "FAILED").sum())
running_count = int((_status_u == "RUNNING").sum())
avg_duration = df["duration_seconds"].dropna().mean()
# Success rate over terminal (finished) jobs, shown as the "Succeeded" delta (#2).
_terminal = success_count + failed_count
success_rate = (success_count / _terminal * 100) if _terminal else None

# Tint the 2nd/3rd/4th metric cards (Succeeded/Failed/Running) by role (#2).
st.markdown(
    """
    <style>
    div[data-testid="stHorizontalBlock"]:has(> div:nth-child(5))
      > div:nth-child(2) div[data-testid="stMetric"]
      { background:#eaf7ee; border-color:#b7e2c4; }
    div[data-testid="stHorizontalBlock"]:has(> div:nth-child(5))
      > div:nth-child(3) div[data-testid="stMetric"]
      { background:#fdeef0; border-color:#f3c2c8; }
    div[data-testid="stHorizontalBlock"]:has(> div:nth-child(5))
      > div:nth-child(4) div[data-testid="stMetric"]
      { background:#eaf2fd; border-color:#bcd6f5; }
    </style>
    """,
    unsafe_allow_html=True,
)

c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.metric("Total Jobs", f"{total:,}")
with c2:
    st.metric(
        "Succeeded",
        f"{success_count:,}",
        delta=(f"{success_rate:.0f}% success" if success_rate is not None else None),
        delta_color="off",
    )
with c3:
    st.metric("Failed", f"{failed_count:,}")
with c4:
    st.metric("Running", f"{running_count:,}")
with c5:
    st.metric("Avg Duration", fmt_duration(avg_duration))

st.markdown('<hr class="thin-rule">', unsafe_allow_html=True)

# ===================================================================
# SORT CONTROLS
# ===================================================================
sort_col_left, sort_dir_right = st.columns([3, 1])

sortable_cols = {
    "Queued At": "queued_at",
    "Started At": "started_at",
    "Completed At": "completed_at",
    "Source": "source",
    "Status": "status",
    "Duration": "duration_seconds",
    "Total Rows": "total_rows",
    "Job Name": "job_name",
    "Hostname": "hostname",
    "User": "user",
}

with sort_col_left:
    sort_label = st.selectbox("Sort by", list(sortable_cols.keys()), index=0)
with sort_dir_right:
    sort_asc = st.selectbox("Order", ["Descending", "Ascending"]) == "Ascending"

sort_field = sortable_cols[sort_label]
df = df.sort_values(sort_field, ascending=sort_asc, na_position="last")

# ===================================================================
# SUMMARY TABLE  (per-row layout with status dropdown + log button)
# ===================================================================
st.markdown(
    f'<div class="section-head">\U0001f4cb Jobs ({len(df)})</div>',
    unsafe_allow_html=True,
)

# Column widths: Status | Lock | Source | Job Name | Started | Duration | Rows | Host | Trigger | Log
_col_widths = [1.3, 0.9, 1, 1.4, 1.3, 0.7, 0.7, 1, 0.8, 0.5]

# Header row
_hdr = st.columns(_col_widths)
for _c, _label in zip(
    _hdr,
    [
        "Status",
        "Lock",
        "Source",
        "Job Name",
        "Started",
        "Duration",
        "Rows",
        "Host",
        "Trigger",
        "Log",
    ],
):
    _c.markdown(f"**{_label}**")

# Open the zebra/hover wrapper (#4): the row HorizontalBlocks render inside it.
st.markdown('<div class="job-rows">', unsafe_allow_html=True)

for idx, (_, row) in enumerate(df.iterrows()):
    job_id = str(row.get("job_id") or "")
    current_status = str(row.get("status") or "").upper()
    log_path = row.get("log_file_path")

    cols = st.columns(_col_widths)

    # Status: colored pill (#1). The editable dropdown is hidden behind an "Edit"
    # popover -- and it's admin-only, since overriding a job's status is a
    # destructive action. Reviewers see the pill without the Edit control.
    with cols[0]:
        st.markdown(status_pill(current_status), unsafe_allow_html=True)
        if _is_admin():
            with st.popover("Edit", use_container_width=False):
                status_list = list(JOB_STATUSES)
                try:
                    sel_idx = status_list.index(current_status)
                except ValueError:
                    status_list.insert(0, current_status)
                    sel_idx = 0
                new_status = st.selectbox(
                    "Set status",
                    status_list,
                    index=sel_idx,
                    key=f"status_{job_id}_{idx}",
                )
                if new_status != current_status:
                    update_job_status(job_id, new_status)
                    load_jobs.clear()
                    load_active_locks.clear()
                    st.rerun()

    # Lock state (feature #2)
    _source = str(row.get("source") or "")
    _badge = lock_badge(current_status, _source, active_locks)
    _lock_help = (
        "Live lock -> process is really running (countdown to lease expiry). "
        "'no lock' -> orphaned, will be KILLED on next reconcile."
    )
    cols[1].markdown(_badge, help=_lock_help, unsafe_allow_html=True)
    cols[2].write(_source or "-")
    cols[3].write(str(row.get("job_name") or "-"))
    # Started: relative-first, absolute on hover (#6).
    _started_abs = fmt_ts(row.get("started_at"))
    _started_rel = fmt_relative(row.get("started_at"))
    if _started_rel:
        cols[4].markdown(
            f'<span title="{_started_abs}">{_started_rel}</span>',
            unsafe_allow_html=True,
        )
    else:
        cols[4].write(_started_abs)
    cols[5].write(fmt_duration(row.get("duration_seconds")))
    cols[6].write(fmt_rows(row.get("total_rows")))
    cols[7].markdown(mono(row.get("hostname")), unsafe_allow_html=True)  # (#6)
    cols[8].markdown(trigger_pill(row.get("triggered_by")), unsafe_allow_html=True)

    # Log button (#3): labeled + disabled (not "-") when no log, keeps alignment.
    with cols[9]:
        if log_path:
            if st.button(
                "\U0001f4c4 Log", key=f"log_{job_id}_{idx}", help="View execution log"
            ):
                show_log_dialog(job_id, str(log_path))
        else:
            st.button(
                "\U0001f4c4 Log",
                key=f"log_{job_id}_{idx}",
                help="No log file recorded",
                disabled=True,
            )

# Close the zebra/hover wrapper (#4).
st.markdown("</div>", unsafe_allow_html=True)

# ===================================================================
# DATA HEALTH  (anomaly-audit + freshness findings)
# ===================================================================
st.markdown('<hr class="thin-rule">', unsafe_allow_html=True)
st.markdown(
    '<div class="section-head">\U0001fa7a Data Health</div>',
    unsafe_allow_html=True,
)

health_df = load_table_health(project_id)
active_anoms = load_active_anomalies(project_id)

# Distinguish "view unavailable" (query errored) from "audited & clean" (query ran,
# 0 rows). Without this, a missing/renamed view looks identical to a healthy audit.
_health_ok = health_df.attrs.get("available", False)
_anom_ok = active_anoms.attrs.get("available", False)


def _view_status(name: str, ok: bool, err: str | None) -> str:
    if ok:
        return f'<span style="color:#0e8a16;">✓ {name}</span>'
    reason = f" ({err})" if err else ""
    return f'<span style="color:#b0870b;">✗ {name} unavailable{reason}</span>'


st.markdown(
    "<div class='muted' style='margin:-0.2rem 0 0.5rem 0;'>"
    + " &nbsp;·&nbsp; ".join(
        [
            _view_status("Freshness", _health_ok, health_df.attrs.get("error")),
            _view_status("Anomalies", _anom_ok, active_anoms.attrs.get("error")),
        ]
    )
    + "</div>",
    unsafe_allow_html=True,
)

if not _health_ok and not _anom_ok:
    st.info(
        "Data-quality audit views are not available in this environment. "
        f"Expected: `{FRESHNESS_DATASET}.{V_TABLE_HEALTH}` and "
        f"`{ANOMALY_DATASET}.{V_ANOMALY_LATEST_RUN}`. "
        "Run `orchestrate.py --source anomaly_audit` to populate them."
    )
elif health_df.empty and active_anoms.empty:
    st.success("Audit ran and found no active anomalies or stale tables. All clear.")
else:
    # ---- Header metric cards (mirror the Jobs metric row) ----
    tables_audited = int(len(health_df)) if not health_df.empty else 0
    n_active = int(len(active_anoms))
    if not active_anoms.empty and "severity" in active_anoms.columns:
        _sev_u = active_anoms["severity"].astype(str).str.strip().str.upper()
        n_high = int((_sev_u == "HIGH").sum())
    else:
        n_high = 0
    if not health_df.empty and "freshness_status" in health_df.columns:
        _fs_u = health_df["freshness_status"].astype(str).str.strip().str.upper()
        n_stale = int(_fs_u.isin(["STALE", "ERROR"]).sum())
    else:
        n_stale = 0

    h1, h2, h3, h4 = st.columns(4)
    with h1:
        st.metric("Tables Audited", f"{tables_audited:,}")
    with h2:
        st.metric("Active Anomalies", f"{n_active:,}")
    with h3:
        st.metric("High Severity", f"{n_high:,}")
    with h4:
        st.metric("Stale Tables", f"{n_stale:,}")

    # Freshness of the audit itself, so users know how current this is.
    _audit_ts = None
    if not active_anoms.empty and "audited_at" in active_anoms.columns:
        _audit_ts = active_anoms["audited_at"].dropna().max()
    if _audit_ts is None and not health_df.empty:
        if "freshness_audited_at" in health_df.columns:
            _audit_ts = health_df["freshness_audited_at"].dropna().max()
    if _audit_ts is not None and not pd.isna(_audit_ts):
        _rel = fmt_relative(_audit_ts)
        try:
            _age_h = (pd.Timestamp.now(tz="UTC") - _audit_ts).total_seconds() / 3600
        except Exception:
            _age_h = 0
        if _age_h > 24:
            st.warning(
                f"Latest audit ran {_rel} -- over 24h ago. The audit may have "
                "stopped running; these findings could be stale."
            )
        else:
            st.caption(
                f"Latest audit run: {_rel}" if _rel else "Latest audit run recorded."
            )

    # ---- Active anomalies: the actionable list ----
    st.markdown(
        f'<div class="section-head">⚠️ Active Anomalies ({n_active})</div>',
        unsafe_allow_html=True,
    )
    if active_anoms.empty:
        st.success("No active anomalies in the latest audit run.")
    else:
        _sev_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        disp = active_anoms.copy()
        disp["_rank"] = (
            disp["severity"].astype(str).str.upper().map(_sev_rank).fillna(9)
        )
        disp = disp.sort_values(["_rank", "dataset_name", "table_name"]).reset_index(
            drop=True
        )

        # Header row + hand-built rows (matches the Jobs grid idiom).
        st.markdown('<div class="job-rows">', unsafe_allow_html=True)
        hdr = st.columns([1.1, 3.2, 1.6, 1.3, 3.0])
        for col, label in zip(hdr, ["Severity", "Table", "Detector", "Date", "Detail"]):
            col.markdown(f"**{label}**")

        for _, r in disp.iterrows():
            c_sev, c_tbl, c_det, c_date, c_why = st.columns([1.1, 3.2, 1.6, 1.3, 3.0])
            sev = str(r.get("severity") or "").strip().upper() or "?"
            sev_fg, sev_bg = {
                "HIGH": ("#a40e26", "#ffdce0"),
                "MEDIUM": ("#9a6700", "#fff5cc"),
                "LOW": ("#57606a", "#eaecef"),
            }.get(sev, ("#57606a", "#eaecef"))
            c_sev.markdown(_pill(sev, sev_fg, sev_bg), unsafe_allow_html=True)

            tbl = f"{r.get('dataset_name', '?')}.{r.get('table_name', '?')}"
            c_tbl.markdown(mono(tbl), unsafe_allow_html=True)
            c_det.markdown(str(r.get("detector") or "-"))

            adate = r.get("anomaly_date")
            c_date.markdown(adate.strftime("%Y-%m-%d") if pd.notna(adate) else "-")

            # Human-readable "why" per detector shape.
            lag = r.get("date_lag_days")
            obs = r.get("observed_count")
            exp = r.get("expected_low")
            med = r.get("median_count")
            if pd.notna(lag) and lag and lag > 0:
                why = f"{int(lag)} day(s) behind"
            elif pd.notna(obs) and pd.notna(exp):
                why = f"{int(obs):,} rows (expected ≥ {int(exp):,}"
                if pd.notna(med):
                    why += f", median {int(med):,}"
                why += ")"
            elif pd.notna(obs):
                why = f"{int(obs):,} rows"
            else:
                why = "-"
            c_why.markdown(
                f'<span class="muted">{_escape_html(why)}</span>',
                unsafe_allow_html=True,
            )
        st.markdown("</div>", unsafe_allow_html=True)

    # ---- Full table-health roster (collapsed; shows healthy tables too) ----
    if not health_df.empty:
        with st.expander(
            f"Table health roster ({len(health_df)} tables)", expanded=False
        ):
            roster = health_df.copy()
            roster["table"] = (
                roster["dataset_name"].astype(str)
                + "."
                + roster["table_name"].astype(str)
            )
            show_cols = [
                c
                for c in [
                    "table",
                    "freshness_status",
                    "days_behind",
                    "most_recent_date",
                    "active_anomalies",
                    "max_anomaly_severity",
                ]
                if c in roster.columns
            ]
            roster = roster.sort_values(
                ["active_anomalies", "days_behind"],
                ascending=[False, False],
                na_position="last",
            )
            st.dataframe(
                roster[show_cols],
                use_container_width=True,
                hide_index=True,
            )

# ===================================================================
# JOB DETAIL EXPANDERS  (with clickable log viewer)
# ===================================================================
st.markdown('<hr class="thin-rule">', unsafe_allow_html=True)
st.markdown(
    '<div class="section-head">\U0001f4dc Job Details &amp; Logs</div>',
    unsafe_allow_html=True,
)
st.caption(
    "Expand any job below to see full details, error info, and the execution log."
)

STATUS_ICONS = {
    "SUCCESS": "\u2705",
    "COMPLETED": "\u2705",
    "FAILED": "\u274c",
    "RUNNING": "\u23f3",
    "KILLED": "\U0001f480",
    "CANCELLED": "\u23f9",
    "QUEUED": "\U0001f552",
    "WARNING": "\u26a0\ufe0f",
    "REVIEWED": "\u2611\ufe0f",
}

for idx, (_, row) in enumerate(df.iterrows()):
    job_id = str(row.get("job_id") or "?")
    source = str(row.get("source") or "?")
    status = str(row.get("status") or "unknown").upper()
    started = fmt_ts(row.get("started_at"))
    duration = fmt_duration(row.get("duration_seconds"))

    icon = STATUS_ICONS.get(status, "\u2022")
    label = f"{icon}  {status}  |  {source}  |  {row.get('job_name', '')}  |  {started}  |  {duration}"

    with st.expander(label, expanded=False):
        # ---- Detail grid ----
        d1, d2, d3 = st.columns(3)
        with d1:
            st.markdown("**Job ID**")
            st.code(job_id, language=None)
            st.markdown(f"**Source:** {source}")
            st.markdown(f"**Job Name:** {row.get('job_name', '-')}")
            st.markdown(f"**Trigger:** {row.get('triggered_by', '-')}")
            st.markdown(f"**Refresh Mode:** {row.get('refresh_mode', '-')}")

        with d2:
            st.markdown(f"**Status:** {status_pill(status)}", unsafe_allow_html=True)
            st.markdown(
                f"**Lock:** {lock_badge(status, source, active_locks)}",
                unsafe_allow_html=True,
            )
            _q_rel = fmt_relative(row.get("queued_at"))
            _s_rel = fmt_relative(row.get("started_at"))
            _c_rel = fmt_relative(row.get("completed_at"))
            st.markdown(
                f"**Queued:** {fmt_ts(row.get('queued_at'))}"
                + (f" <span class='muted'>({_q_rel})</span>" if _q_rel else ""),
                unsafe_allow_html=True,
            )
            st.markdown(
                f"**Started:** {started}"
                + (f" <span class='muted'>({_s_rel})</span>" if _s_rel else ""),
                unsafe_allow_html=True,
            )
            st.markdown(
                f"**Completed:** {fmt_ts(row.get('completed_at'))}"
                + (f" <span class='muted'>({_c_rel})</span>" if _c_rel else ""),
                unsafe_allow_html=True,
            )
            st.markdown(f"**Duration:** {duration}")

        with d3:
            st.markdown(f"**Rows:** {fmt_rows(row.get('total_rows'))}")
            st.markdown(f"**Tables:** {fmt_rows(row.get('tables_processed'))}")
            st.markdown(f"**Sites:** {fmt_rows(row.get('sites_processed'))}")
            st.markdown(
                f"**Host:** {mono(row.get('hostname'))}", unsafe_allow_html=True
            )
            st.markdown(f"**User:** {row.get('user', '-')}")

        # Tables / groups arrays
        tables_list = row.get("tables")
        groups_list = row.get("groups")
        if isinstance(tables_list, list) and len(tables_list) > 0:
            st.markdown("**Tables:** " + ", ".join(f"`{t}`" for t in tables_list))
        if isinstance(groups_list, list) and len(groups_list) > 0:
            st.markdown("**Groups:** " + ", ".join(f"`{g}`" for g in groups_list))

        # Command line
        cmd = row.get("command_line")
        if cmd:
            st.markdown("**Command:**")
            st.code(str(cmd), language="bash")

        # ---- Error section ----
        error_msg = row.get("error_message")
        error_type = row.get("error_type")
        if error_msg:
            st.markdown("**Error:**")
            err_header = f"**{error_type}** " if error_type else ""
            st.error(f"{err_header}{error_msg}")

        # ---- Log viewer (uses session_state to persist across reruns) ----
        log_path = row.get("log_file_path")
        if log_path:
            st.markdown(f"**Log file:** `{log_path}`")

            log_key = f"log_{idx}_{job_id}"
            is_visible = st.session_state.visible_logs.get(log_key, False)

            btn_label = "Hide log" if is_visible else "View log"
            if st.button(btn_label, key=f"btn_{log_key}"):
                st.session_state.visible_logs[log_key] = not is_visible
                st.rerun()

            if st.session_state.visible_logs.get(log_key, False):
                content = fetch_log_content(str(log_path))
                if content:
                    lines = content.split("\n")
                    st.caption(f"{len(lines):,} lines")
                    st.markdown(
                        f'<div class="log-viewer">{_escape_html(content)}</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.warning("Log file could not be retrieved.")
        else:
            st.caption("No log file recorded for this job.")
