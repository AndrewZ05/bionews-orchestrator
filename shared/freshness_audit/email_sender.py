#!/usr/bin/env python3
"""Email summary builder and sender for the Table Freshness Auditing feature.

This module turns a list of per-table audit result dicts (the schema-shaped
dicts produced by ``audit_runner``, optionally carrying the transient helper
keys ``email_recipients`` and ``send_email_notification``) into:

  * a PLAIN-text fixed-width summary (``build_plain_summary``) -- this is what
    actually gets emailed, because ``shared.notifications.send_email_notification``
    wraps its body in ``MIMEText(message, 'plain')`` and would render HTML as raw
    markup; and
  * an HTML summary (``build_html_summary``) -- returned for unit tests and any
    future HTML-capable channel.

For environments where we want a real multipart email (both plain + HTML), this
module also exposes ``send_html_email_notification`` which builds a
``MIMEMultipart('alternative')`` and sends it over smtplib using the SAME
``email_config`` keys as ``shared.notifications`` (smtp_host / smtp_port /
smtp_user / smtp_password / from_email / recipients).

ASCII-ONLY RULE: the plain-text body and every log string in this module are
pure ASCII (no em-dashes, no unicode). This is enforced by a unit test that does
``body.encode('ascii')``. Use '-' / '--' / ':' instead of unicode punctuation.
"""

import html as _html
import logging
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

from shared.notifications import send_email_notification
from shared.secret_manager import get_secret_or_env
from shared.freshness_audit.status_rules import (
    STATUS_PASS,
    STATUS_WARNING,
    STATUS_FAIL,
    STATUS_ERROR,
    STATUS_STATIC,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _count_by_status(results: List[Dict[str, Any]]) -> Dict[str, int]:
    """Return a {PASS, WARNING, FAIL, ERROR, STATIC} count map over result dicts."""
    counts = {
        STATUS_PASS: 0,
        STATUS_WARNING: 0,
        STATUS_FAIL: 0,
        STATUS_ERROR: 0,
        STATUS_STATIC: 0,
    }
    for row in results:
        status = row.get("audit_status")
        if status in counts:
            counts[status] += 1
    return counts


def _fmt(value: Any) -> str:
    """Render a cell value for the fixed-width plain-text table (ASCII-safe)."""
    if value is None:
        return "-"
    return str(value)


def _qualified_name(row: Dict[str, Any]) -> str:
    """Build the 'dataset.table' label used in summaries."""
    dataset = _fmt(row.get("dataset_name"))
    table = _fmt(row.get("table_name"))
    return f"{dataset}.{table}"


def _run_id(run_meta: Dict[str, Any]) -> str:
    """Extract the audit_run_id from run_meta (tolerant of absence)."""
    return str(run_meta.get("audit_run_id", "unknown"))


def _split_recipients(raw: Any) -> List[str]:
    """Split a per-config email_recipients string on commas/semicolons.

    Returns a list of trimmed, non-empty addresses. Accepts None or a list as
    well (defensive) -- a list is returned as-is after trimming.
    """
    if not raw:
        return []
    if isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        # Normalize semicolons to commas, then split.
        items = str(raw).replace(";", ",").split(",")
    return [item.strip() for item in items if item and str(item).strip()]


def _console_link(run_id: str, results_dataset: str, results_table: str) -> str:
    """Build a BigQuery console query link showing this run's full results.

    Ported from the anomaly audit's email sender so both audits offer the same
    one-click path to the complete result set (sortable/filterable in console).
    """
    project = "bi-data-391216"
    sql = (
        "SELECT * FROM `{0}.{1}.{2}` "
        "WHERE audit_run_id = '{3}' ORDER BY audit_status, dataset_name".format(
            project, results_dataset, results_table, run_id
        )
    )
    import urllib.parse

    return "https://console.cloud.google.com/bigquery?project={0}&ws=!1m0&q={1}".format(
        project, urllib.parse.quote(sql)
    )


def _group_by_dataset(results: List[Dict[str, Any]]):
    """Yield (dataset_name, rows, counts) groups sorted by dataset then table.

    counts is the per-group status map -- rendered as a subtotal header so a
    reader can scan source health without reading every row.
    """
    ordered = sorted(
        results,
        key=lambda r: (str(r.get("dataset_name")), str(r.get("table_name"))),
    )
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in ordered:
        groups.setdefault(str(row.get("dataset_name")), []).append(row)
    for dataset in sorted(groups):
        rows = groups[dataset]
        yield dataset, rows, _count_by_status(rows)


def _group_header(dataset: str, counts: Dict[str, int]) -> str:
    """One-line per-dataset subtotal, e.g. 'facebook_data: 12 PASS, 1 FAIL'."""
    parts = []
    for status in (
        STATUS_PASS,
        STATUS_WARNING,
        STATUS_FAIL,
        STATUS_ERROR,
        STATUS_STATIC,
    ):
        if counts.get(status):
            parts.append("{0} {1}".format(counts[status], status))
    return "{0}: {1}".format(dataset, ", ".join(parts) if parts else "0 rows")


# ---------------------------------------------------------------------------
# Body builders
# ---------------------------------------------------------------------------


def build_plain_summary(results: List[Dict[str, Any]], run_meta: Dict[str, Any]) -> str:
    """Build an ASCII-only fixed-width plain-text summary body.

    Layout:
      * a header line with the audit_run_id and a status count line
        (PASS / WARNING / FAIL / ERROR);
      * a fixed-width table of every result: status, dataset.table,
        most_recent_date, days_behind, total_row_count, recent_date_row_count;
      * an ERROR detail section listing each ERROR row's error_message.

    All content is ASCII only.
    """
    counts = _count_by_status(results)
    lines: List[str] = []

    lines.append("Table Freshness Audit Results")
    lines.append("=" * 60)
    lines.append(f"Audit run id: {_run_id(run_meta)}")
    generated_at = run_meta.get("audited_at") or datetime.now(timezone.utc)
    lines.append(f"Generated at: {generated_at} UTC")
    lines.append(
        "Summary: {p} PASS, {w} WARNING, {f} FAIL, {e} ERROR, {s} STATIC "
        "(total {t})".format(
            p=counts[STATUS_PASS],
            w=counts[STATUS_WARNING],
            f=counts[STATUS_FAIL],
            e=counts[STATUS_ERROR],
            s=counts[STATUS_STATIC],
            t=len(results),
        )
    )

    # "Changes since last run" -- the run-over-run delta computed by the runner.
    # This is the most actionable part of the email: what BROKE and what HEALED
    # since yesterday, named explicitly.
    delta = run_meta.get("delta") or {}
    newly_failed = delta.get("newly_failed") or []
    recovered = delta.get("recovered") or []
    if newly_failed or recovered:
        lines.append("")
        lines.append("Changes since last run:")
        lines.append("-" * 60)
        for label in newly_failed:
            lines.append("  NEW FAILURE: {0}".format(label))
        for label in recovered:
            lines.append("  recovered:   {0}".format(label))

    # Console link to the FULL result set for this run.
    link = _console_link(
        _run_id(run_meta),
        run_meta.get("results_dataset", "freshness_audit"),
        run_meta.get("results_table", "table_freshness_results"),
    )
    lines.append("Full results: {0}".format(link))
    lines.append("")

    # Fixed-width table GROUPED BY DATASET with a per-group subtotal header so a
    # reader can scan source health at a glance. The POLICY column shows the
    # cadence preset (e.g. 'rare') when the row is running on a non-default
    # tolerance; rows under the source-level default render an empty cell so the
    # grid is not visually noisy in the common case. Full reasons land in the
    # "Policy notes" section below.
    header = "{status:<8} {table:<40} {recent:<12} {behind:>7} {total:>12} {recent_rows:>12} {policy:<9}".format(
        status="STATUS",
        table="DATASET.TABLE",
        recent="LAST_DATE",
        behind="BEHIND",
        total="ROWS",
        recent_rows="RECENT_ROWS",
        policy="POLICY",
    )
    lines.append(header)
    lines.append("-" * len(header))

    for dataset, rows, group_counts in _group_by_dataset(results):
        lines.append("")
        lines.append("== {0}".format(_group_header(dataset, group_counts)))
        for row in rows:
            cadence = row.get("audit_cadence") or ""
            lines.append(
                "{status:<8} {table:<40} {recent:<12} {behind:>7} {total:>12} {recent_rows:>12} {policy:<9}".format(
                    status=_fmt(row.get("audit_status"))[:8],
                    table=_qualified_name(row)[:40],
                    recent=_fmt(row.get("most_recent_date"))[:12],
                    behind=_fmt(row.get("days_behind"))[:7],
                    total=_fmt(row.get("total_row_count"))[:12],
                    recent_rows=_fmt(row.get("recent_date_row_count"))[:12],
                    policy=str(cadence)[:9],
                )
            )

    # Policy notes: one line per row whose audit policy differs from the source
    # default, naming the cadence preset and the operator-supplied reason so the
    # rationale travels with the email and is not hidden in BQ.
    policy_rows = [
        r for r in results if r.get("audit_cadence") or r.get("audit_reason")
    ]
    if policy_rows:
        lines.append("")
        lines.append("Policy notes:")
        lines.append("-" * 60)
        for row in policy_rows:
            cadence = row.get("audit_cadence") or "(custom)"
            reason = row.get("audit_reason") or "(no reason supplied)"
            lines.append(
                "  {table}: {cadence} -- {reason}".format(
                    table=_qualified_name(row),
                    cadence=cadence,
                    reason=reason,
                )
            )

    # ERROR detail section: surface the error_message for any ERROR row so the
    # operator does not have to query the results table to triage.
    error_rows = [r for r in results if r.get("audit_status") == STATUS_ERROR]
    if error_rows:
        lines.append("")
        lines.append("Errors:")
        lines.append("-" * 60)
        for row in error_rows:
            lines.append(f"  {_qualified_name(row)}: {_fmt(row.get('error_message'))}")

    # Config-drift skip note: tables active in YAML but absent in BigQuery are
    # skipped (no row), surfaced here so the count never disappears silently.
    skipped = run_meta.get("skipped_count") or 0
    if skipped:
        lines.append("")
        lines.append(
            "{0} known-missing table(s) skipped (config drift) -- see "
            "docs/ANOMALY_AUDIT_MISSING_TABLES.md".format(skipped)
        )

    return "\n".join(lines)


def build_html_summary(results: List[Dict[str, Any]], run_meta: Dict[str, Any]) -> str:
    """Build an HTML summary body.

    Returned for unit tests and for any future HTML-capable channel (and used as
    the HTML alternative part by ``send_html_email_notification``). It is NOT
    passed to ``shared.notifications.send_email_notification`` (which is plain
    text only).
    """
    counts = _count_by_status(results)
    run_id = _run_id(run_meta)
    generated_at = run_meta.get("audited_at") or datetime.now(timezone.utc)

    # Per-status row background colors for quick visual triage. STATIC uses a
    # distinct teal-tinted green -- clearly "green/healthy" like PASS, but
    # different enough to read at a glance as "intentionally unchanging".
    status_colors = {
        STATUS_PASS: "#e6f4ea",
        STATUS_WARNING: "#fff4e5",
        STATUS_FAIL: "#fdecea",
        STATUS_ERROR: "#f3e5f5",
        STATUS_STATIC: "#d3ede2",
    }

    parts: List[str] = []
    parts.append('<html><body style="font-family: Arial, sans-serif;">')
    parts.append("<h2>Table Freshness Audit Results</h2>")
    parts.append(f"<p><strong>Audit run id:</strong> {run_id}</p>")
    parts.append(f"<p><strong>Generated at:</strong> {generated_at} UTC</p>")
    parts.append(
        "<p><strong>Summary:</strong> "
        f"{counts[STATUS_PASS]} PASS, "
        f"{counts[STATUS_WARNING]} WARNING, "
        f"{counts[STATUS_FAIL]} FAIL, "
        f"{counts[STATUS_ERROR]} ERROR, "
        f"{counts[STATUS_STATIC]} STATIC "
        f"(total {len(results)})</p>"
    )

    # "Changes since last run" -- named newly-failed/recovered tables, the most
    # actionable section, rendered prominently above the full status table.
    delta = run_meta.get("delta") or {}
    newly_failed = delta.get("newly_failed") or []
    recovered = delta.get("recovered") or []
    if newly_failed or recovered:
        parts.append("<h3>Changes since last run</h3><ul>")
        for label in newly_failed:
            parts.append(
                '<li style="color:#b00020;"><strong>NEW FAILURE:</strong> '
                "{0}</li>".format(label)
            )
        for label in recovered:
            parts.append('<li style="color:#1b7f37;">recovered: {0}</li>'.format(label))
        parts.append("</ul>")

    # Console link to the FULL result set for this run.
    link = _console_link(
        _run_id(run_meta),
        run_meta.get("results_dataset", "freshness_audit"),
        run_meta.get("results_table", "table_freshness_results"),
    )
    parts.append('<p><a href="{0}">Open full results in BigQuery</a></p>'.format(link))

    parts.append(
        '<table border="1" cellpadding="6" cellspacing="0" '
        'style="border-collapse: collapse;">'
    )
    parts.append(
        '<tr style="background-color:#333;color:#fff;">'
        "<th>Status</th><th>Dataset.Table</th><th>Last Date</th>"
        "<th>Days Behind</th><th>Total Rows</th><th>Recent Rows</th>"
        "<th>Policy</th><th>Error</th></tr>"
    )
    # Rows grouped by dataset with a bold subtotal header row per group.
    # The Policy column shows the cadence preset name (e.g. 'rare') when the
    # row is running on a non-default tolerance; the operator-supplied reason
    # is attached as a tooltip via the title= attribute so hovering exposes
    # the rationale without bloating the cell.
    for dataset, rows, group_counts in _group_by_dataset(results):
        parts.append(
            '<tr style="background-color:#e8eaf6;font-weight:bold;">'
            '<td colspan="8">{0}</td></tr>'.format(_group_header(dataset, group_counts))
        )
        for row in rows:
            status = _fmt(row.get("audit_status"))
            bg = status_colors.get(status, "#ffffff")
            cadence = row.get("audit_cadence")
            reason = row.get("audit_reason")
            if cadence or reason:
                policy_cell = '<td title="{reason}">{cadence}</td>'.format(
                    reason=_html.escape(str(reason or ""), quote=True),
                    cadence=_html.escape(str(cadence or "(custom)"), quote=True),
                )
            else:
                policy_cell = "<td></td>"
            parts.append(
                '<tr style="background-color:{bg};">'
                "<td>{status}</td><td>{table}</td><td>{recent}</td>"
                '<td align="right">{behind}</td><td align="right">{total}</td>'
                '<td align="right">{recent_rows}</td>{policy_cell}'
                "<td>{error}</td></tr>".format(
                    bg=bg,
                    status=status,
                    table=_qualified_name(row),
                    recent=_fmt(row.get("most_recent_date")),
                    behind=_fmt(row.get("days_behind")),
                    total=_fmt(row.get("total_row_count")),
                    recent_rows=_fmt(row.get("recent_date_row_count")),
                    policy_cell=policy_cell,
                    error=(
                        _fmt(row.get("error_message")) if status == STATUS_ERROR else ""
                    ),
                )
            )
    parts.append("</table>")
    skipped = run_meta.get("skipped_count") or 0
    if skipped:
        parts.append(
            "<p>{0} known-missing table(s) skipped (config drift) -- see "
            "docs/ANOMALY_AUDIT_MISSING_TABLES.md</p>".format(skipped)
        )

    parts.append("</body></html>")

    return "".join(parts)


# ---------------------------------------------------------------------------
# Recipient computation
# ---------------------------------------------------------------------------


def compute_extra_recipients(results: List[Dict[str, Any]]) -> List[str]:
    """Union per-row email_recipients for actionable rows.

    A row contributes its recipients ONLY when:
      * its transient helper ``send_email_notification`` is truthy, AND
      * its ``audit_status`` is not PASS (i.e. WARNING / FAIL / ERROR).

    Recipients are split on commas/semicolons, deduplicated while preserving
    first-seen order, and returned as a flat list.
    """
    seen: Dict[str, None] = {}
    for row in results:
        if not row.get("send_email_notification"):
            continue
        if row.get("audit_status") == STATUS_PASS:
            continue
        for addr in _split_recipients(row.get("email_recipients")):
            if addr not in seen:
                seen[addr] = None
    return list(seen.keys())


# ---------------------------------------------------------------------------
# Multipart sender (plain + HTML over smtplib)
# ---------------------------------------------------------------------------


def send_html_email_notification(
    title: str,
    html_body: str,
    text_body: str,
    email_config: Dict[str, Any],
    additional_recipients: Optional[List[str]] = None,
) -> bool:
    """Send a multipart (plain + HTML) email using the same email_config schema.

    Uses the SAME ``email_config`` keys as ``shared.notifications``:
    ``smtp_host``, ``smtp_port``, ``smtp_user``, ``smtp_password``,
    ``from_email``, ``recipients``. Falls back to ``get_secret_or_env`` only for
    individual values that are missing from the config.

    The HTML part is added last so that MIME-aware clients prefer it, while
    plain-text-only clients still get a readable body.

    Returns True on success, False on any failure or missing required config.
    """
    try:
        # Resolve connection settings, preferring explicit config and falling
        # back to Secret Manager / env only when a value is absent.
        smtp_host = (
            email_config.get("smtp_host")
            or get_secret_or_env("SMTP_HOST")
            or "smtp.gmail.com"
        )
        smtp_port = (
            email_config.get("smtp_port") or get_secret_or_env("SMTP_PORT") or 587
        )
        smtp_user = email_config.get("smtp_user") or get_secret_or_env("SMTP_USER")
        smtp_password = email_config.get("smtp_password") or get_secret_or_env(
            "SMTP_PASSWORD"
        )
        from_email = (
            email_config.get("from_email")
            or get_secret_or_env("ALERT_SENDER_EMAIL")
            or "pipeline@company.com"
        )

        # smtp_port may arrive as a string from env expansion; coerce to int.
        try:
            smtp_port = int(smtp_port)
        except (TypeError, ValueError):
            smtp_port = 587

        # Build the recipient list: config recipients + any additional, deduped.
        base_recipients = list(email_config.get("recipients", []) or [])
        recipients = base_recipients.copy()
        if additional_recipients:
            recipients.extend(additional_recipients)
        # Remove duplicates while preserving order.
        recipients = list(dict.fromkeys(recipients))

        if not smtp_user or not smtp_password or not recipients:
            logger.warning(
                "Missing email configuration, skipping HTML email notification"
            )
            return False

        # Assemble the multipart/alternative message: text first, HTML second.
        msg = MIMEMultipart("alternative")
        msg["From"] = from_email
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = title
        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        # Send over an explicit STARTTLS connection (mirrors shared.notifications).
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()

        logger.info(
            "Sent HTML audit email to %d recipient(s): %s",
            len(recipients),
            ", ".join(recipients),
        )
        return True
    except Exception as exc:
        logger.error("Error sending HTML audit email: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Public send entrypoint
# ---------------------------------------------------------------------------


def _build_subject(
    counts: Dict[str, int], delta: Optional[Dict[str, Any]] = None
) -> str:
    """Build the audit email subject line (ASCII only, no em-dash).

    Date-stamped, flags failures, and -- when a run-over-run delta is available --
    leads with the CHANGE counts so the subject answers "what is new today":
      'Table Freshness Audit Results - 2 NEW FAILURES, 1 recovered - YYYY-MM-DD'
      'Table Freshness Audit Results - FAILURES FOUND - YYYY-MM-DD'  (no delta)
      'Table Freshness Audit Results - YYYY-MM-DD'                   (all clear)
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    delta = delta or {}
    new_failed = len(delta.get("newly_failed") or [])
    recovered = len(delta.get("recovered") or [])
    if new_failed or recovered:
        parts = []
        if new_failed:
            parts.append(
                "{0} NEW FAILURE{1}".format(new_failed, "S" if new_failed != 1 else "")
            )
        if recovered:
            parts.append("{0} recovered".format(recovered))
        return "Table Freshness Audit Results - {0} - {1}".format(
            ", ".join(parts), today
        )
    has_failures = counts[STATUS_FAIL] > 0 or counts[STATUS_ERROR] > 0
    if has_failures:
        return f"Table Freshness Audit Results - FAILURES FOUND - {today}"
    return f"Table Freshness Audit Results - {today}"


def send_summary_email(
    results: List[Dict[str, Any]],
    run_meta: Dict[str, Any],
    email_config: Dict[str, Any],
    additional_recipients: Optional[List[str]] = None,
) -> bool:
    """Decide whether to send, build the bodies, and dispatch the audit email.

    Send decision: send if ANY result is WARNING / FAIL / ERROR, OR if
    ``run_meta['always_send']`` is True. If nothing is actionable and
    always_send is not set, no email is sent and this returns False.

    Recipients: ``additional_recipients`` (passed by the runner) are unioned
    with the per-row recipients computed by ``compute_extra_recipients`` over the
    results, so per-config ``email_recipients`` for actionable rows are honored.

    The actual send uses the multipart path (``send_html_email_notification``)
    so recipients get both a plain-text and an HTML view. Returns the send bool.
    """
    counts = _count_by_status(results)
    actionable = (
        counts[STATUS_WARNING] > 0
        or counts[STATUS_FAIL] > 0
        or counts[STATUS_ERROR] > 0
    )
    always_send = bool(run_meta.get("always_send"))

    if not actionable and not always_send:
        logger.info(
            "No actionable results and always_send is False -- skipping audit email"
        )
        return False

    # Union the runner-supplied recipients with per-row recipients for
    # actionable, notify-enabled rows. Dedup while preserving order.
    extra = list(additional_recipients or [])
    extra.extend(compute_extra_recipients(results))
    extra = list(dict.fromkeys(extra))

    subject = _build_subject(counts, run_meta.get("delta"))
    text_body = build_plain_summary(results, run_meta)
    html_body = build_html_summary(results, run_meta)

    sent = send_html_email_notification(
        subject,
        html_body,
        text_body,
        email_config,
        additional_recipients=extra,
    )

    if not sent:
        # Fall back to the plain-text-only path used elsewhere in the platform,
        # so a multipart/SMTP hiccup does not silently drop the alert.
        logger.warning(
            "Multipart send failed or skipped -- falling back to plain-text email"
        )
        sent = send_email_notification(
            subject, text_body, email_config, additional_recipients=extra
        )

    return sent
