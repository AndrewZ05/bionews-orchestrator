#!/usr/bin/env python3
"""Email summary builder and sender for the Anomaly Audit feature.

Renders ACTIVE-only findings into a plain-text and an HTML summary and dispatches
the email by REUSING the freshness audit's multipart sender directly (no SMTP is
reimplemented here):

  * ``build_plain_summary`` builds an ASCII-only fixed-width body -- one row per
    ACTIVE finding, grouped/sorted severity HIGH -> MEDIUM -> LOW;
  * ``build_html_summary`` builds the HTML alternative part for the same rows;
  * ``send_anomaly_summary`` decides whether to send, computes the per-row extra
    recipients, then calls
    ``shared.freshness_audit.email_sender.send_html_email_notification``; on
    failure it falls back to ``shared.notifications.send_email_notification`` with
    the plain body.

HISTORICAL findings NEVER appear as rows. They surface ONLY via a single one-line
note 'N new historical outlier(s) recorded -- see anomaly_results', which is
appended to both bodies (and omitted when N == 0).

ASCII-ONLY RULE: the plain-text body and every log string in this module are pure
ASCII (no em-dashes, no unicode). Enforced by a unit test that does
``body.encode('ascii')``. Use '-' / '--' / ':' instead of unicode punctuation.
"""

import html as _html
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from shared.notifications import send_email_notification
from shared.freshness_audit.email_sender import send_html_email_notification
from shared.anomaly_audit import (
    SCOPE_ACTIVE,
    SEVERITY_HIGH,
    SEVERITY_MEDIUM,
    SEVERITY_LOW,
)

logger = logging.getLogger(__name__)

# Stable ordering for grouping/sorting the ACTIVE rows: HIGH first, then MEDIUM,
# then LOW. Anything unexpected sorts last.
_SEVERITY_ORDER = {SEVERITY_HIGH: 0, SEVERITY_MEDIUM: 1, SEVERITY_LOW: 2}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


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


def _detail(row: Dict[str, Any]) -> str:
    """Build the 'expected_low/median_count or date_lag_days' detail cell.

    Recency lags carry a date_lag_days; volume/structural findings carry the
    expected_low / median_count band context. The cell summarizes whichever
    applies so a reader can triage from the email alone.
    """
    lag = row.get("date_lag_days")
    if lag is not None:
        return "lag={0}d".format(lag)
    low = row.get("expected_low")
    med = row.get("median_count")
    if low is not None or med is not None:
        return "low={0} median={1}".format(_fmt(low), _fmt(med))
    return "-"


def _active_only(findings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter to ACTIVE findings (used for the send decision + recipient union)."""
    return [f for f in findings if f.get("scope") == SCOPE_ACTIVE]


def _sorted_all(findings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort ALL findings for the full email table.

    Order: severity HIGH -> MEDIUM -> LOW, then ACTIVE before HISTORICAL within a
    severity (urgent first), then dataset.table for a stable read.
    """
    return sorted(
        findings,
        key=lambda f: (
            _SEVERITY_ORDER.get(f.get("severity"), 99),
            0 if f.get("scope") == SCOPE_ACTIVE else 1,
            _qualified_name(f),
        ),
    )


def _console_link(run_id: str, results_dataset: str, results_table: str) -> str:
    """Build a BigQuery console query link showing this run's full results.

    Opens the BigQuery console with a prefilled SELECT for the audit_run_id so the
    reader can sort/filter the complete result set (every finding + OK/ERROR rows).
    """
    project = "bi-data-391216"
    sql = (
        "SELECT * FROM `{0}.{1}.{2}` "
        "WHERE audit_run_id = '{3}' ORDER BY severity, scope, dataset_name".format(
            project, results_dataset, results_table, run_id
        )
    )
    import urllib.parse

    return "https://console.cloud.google.com/bigquery?project={0}&ws=!1m0&q={1}".format(
        project, urllib.parse.quote(sql)
    )


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
        items = str(raw).replace(";", ",").split(",")
    return [item.strip() for item in items if item and str(item).strip()]


def _historical_note(new_historical_count: int) -> Optional[str]:
    """Build the one-line new-historical-outlier note, or None when N == 0."""
    if not new_historical_count:
        return None
    return "{0} new historical outlier(s) recorded -- see anomaly_results".format(
        new_historical_count
    )


def _counts_by_severity(active: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count ACTIVE findings per severity (HIGH/MEDIUM/LOW)."""
    counts = {SEVERITY_HIGH: 0, SEVERITY_MEDIUM: 0, SEVERITY_LOW: 0}
    for f in active:
        sev = f.get("severity")
        if sev in counts:
            counts[sev] += 1
    return counts


def _counts_by_detector(active: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count ACTIVE findings per detector, for the summary line."""
    counts: Dict[str, int] = {}
    for f in active:
        det = _fmt(f.get("detector"))
        counts[det] = counts.get(det, 0) + 1
    return counts


def _counts_by_dataset(active: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count ACTIVE findings per source dataset, for the per-source rollup."""
    counts: Dict[str, int] = {}
    for f in active:
        ds = _fmt(f.get("dataset_name"))
        counts[ds] = counts.get(ds, 0) + 1
    return counts


def _summary_lines(active: List[Dict[str, Any]]) -> List[str]:
    """Build the lead summary lines: counts per severity, detector, and dataset.

    The email LEADS with these rollups so a reader can scan WHICH sources are
    affected without reading every row (the findings table itself stays sorted
    by severity -- urgency first -- rather than by dataset).
    """
    sev = _counts_by_severity(active)
    det = _counts_by_detector(active)
    det_str = ", ".join("{0}={1}".format(k, v) for k, v in sorted(det.items()))
    ds = _counts_by_dataset(active)
    ds_str = ", ".join("{0}={1}".format(k, v) for k, v in sorted(ds.items()))
    return [
        "Active findings: {0} (HIGH {1}, MEDIUM {2}, LOW {3})".format(
            len(active), sev[SEVERITY_HIGH], sev[SEVERITY_MEDIUM], sev[SEVERITY_LOW]
        ),
        "By detector: {0}".format(det_str or "-"),
        "By dataset: {0}".format(ds_str or "-"),
    ]


# ---------------------------------------------------------------------------
# Body builders
# ---------------------------------------------------------------------------


def build_plain_summary(
    active_findings: List[Dict[str, Any]],
    run_meta: Dict[str, Any],
    new_historical_count: int,
) -> str:
    """Build an ASCII-only fixed-width plain-text summary of ACTIVE findings.

    Layout:
      * a header with the audit_run_id and generation time;
      * a count line of ACTIVE findings;
      * a fixed-width table of every ACTIVE finding (severity, dataset.table,
        detector, anomaly_date, observed_count, detail, scope), sorted
        HIGH -> MEDIUM -> LOW;
      * the one-line new-historical note (omitted when N == 0).

    All content is ASCII only.
    """
    active = _active_only(active_findings)
    all_rows = _sorted_all(active_findings)
    lines: List[str] = []

    lines.append("Anomaly Audit Results")
    lines.append("=" * 60)
    lines.append(f"Audit run id: {_run_id(run_meta)}")
    generated_at = run_meta.get("audited_at") or datetime.now(timezone.utc)
    lines.append(f"Generated at: {generated_at} UTC")
    # Lead with the summary counts (severity + detector) over ACTIVE findings.
    lines.extend(_summary_lines(active))

    # Console link to the FULL result set for this run (sort/filter ad hoc).
    link = _console_link(
        _run_id(run_meta),
        run_meta.get("results_dataset", "anomaly_audit"),
        run_meta.get("results_table", "anomaly_results"),
    )
    lines.append("Full results: {0}".format(link))
    lines.append("")

    # "Changes since last run" -- new/resolved ACTIVE anomalies vs the prior run.
    delta = run_meta.get("delta") or {}
    new_active = delta.get("new_active") or []
    resolved_active = delta.get("resolved_active") or []
    if new_active or resolved_active:
        lines.append("Changes since last run:")
        lines.append("-" * 60)
        for label in new_active:
            lines.append("  NEW:      {0}".format(label))
        for label in resolved_active:
            lines.append("  resolved: {0}".format(label))
        lines.append("")

    # Mutual watchdog: the sibling freshness audit went silent.
    watchdog = run_meta.get("watchdog_note")
    if watchdog:
        lines.append(watchdog)
        lines.append("")

    # Pipeline-job context for affected sources (best-effort; absent when the
    # orchestrator monitoring tables are unavailable).
    job_context = run_meta.get("job_context") or {}
    if job_context:
        lines.append("Pipeline job context:")
        for source in sorted(job_context):
            lines.append("  {0}: {1}".format(source, job_context[source]))
        lines.append("")

    # FULL TABLE: every finding (ACTIVE + HISTORICAL), sorted HIGH->MEDIUM->LOW,
    # ACTIVE before HISTORICAL within a severity. No truncation -- the email is
    # self-contained.
    lines.append("All findings ({0}):".format(len(all_rows)))
    # POLICY column shows the cadence preset (e.g. 'rare') for findings on
    # non-default tolerance tables; rows under source defaults render empty.
    # Full reasons land in the "Policy notes" section below the grid.
    header = (
        "{sev:<8} {scope:<10} {table:<40} {detector:<16} "
        "{adate:<12} {obs:>10} {detail:<22} {policy:<9}".format(
            sev="SEVERITY",
            scope="SCOPE",
            table="DATASET.TABLE",
            detector="DETECTOR",
            adate="DATE",
            obs="OBSERVED",
            detail="DETAIL",
            policy="POLICY",
        )
    )
    lines.append(header)
    lines.append("-" * len(header))
    for row in all_rows:
        cadence = row.get("audit_cadence") or ""
        lines.append(
            "{sev:<8} {scope:<10} {table:<40} {detector:<16} "
            "{adate:<12} {obs:>10} {detail:<22} {policy:<9}".format(
                sev=_fmt(row.get("severity"))[:8],
                scope=_fmt(row.get("scope"))[:10],
                table=_qualified_name(row)[:40],
                detector=_fmt(row.get("detector"))[:16],
                adate=_fmt(row.get("anomaly_date"))[:12],
                obs=_fmt(row.get("observed_count"))[:10],
                detail=_detail(row)[:22],
                policy=str(cadence)[:9],
            )
        )

    # Policy notes: one line per row on a non-default audit policy.
    policy_rows = [
        r for r in all_rows if r.get("audit_cadence") or r.get("audit_reason")
    ]
    if policy_rows:
        lines.append("")
        lines.append("Policy notes:")
        lines.append("-" * 60)
        # Dedupe by (table, cadence, reason) -- one table may emit multiple
        # findings on the same run; one note per unique policy is enough.
        seen = set()
        for row in policy_rows:
            key = (
                _qualified_name(row),
                row.get("audit_cadence"),
                row.get("audit_reason"),
            )
            if key in seen:
                continue
            seen.add(key)
            cadence = row.get("audit_cadence") or "(custom)"
            reason = row.get("audit_reason") or "(no reason supplied)"
            lines.append(
                "  {table}: {cadence} -- {reason}".format(
                    table=key[0], cadence=cadence, reason=reason
                )
            )

    note = _historical_note(new_historical_count)
    if note:
        lines.append("")
        lines.append(note)

    return "\n".join(lines)


def build_html_summary(
    active_findings: List[Dict[str, Any]],
    run_meta: Dict[str, Any],
    new_historical_count: int,
) -> str:
    """Build an HTML summary of the ACTIVE findings (HTML alternative part).

    Renders one table row per ACTIVE finding sorted HIGH -> MEDIUM -> LOW, with a
    per-severity background color for quick triage, and appends the one-line
    new-historical note (omitted when N == 0). HISTORICAL findings never appear.
    """
    active = _active_only(active_findings)
    high = [f for f in active if f.get("severity") == SEVERITY_HIGH]
    other = len(active) - len(high)
    run_id = _run_id(run_meta)
    generated_at = run_meta.get("audited_at") or datetime.now(timezone.utc)
    sev = _counts_by_severity(active)
    det = _counts_by_detector(active)
    det_str = ", ".join("{0}={1}".format(k, v) for k, v in sorted(det.items()))
    ds = _counts_by_dataset(active)
    ds_str = ", ".join("{0}={1}".format(k, v) for k, v in sorted(ds.items()))

    parts: List[str] = []
    parts.append('<html><body style="font-family: Arial, sans-serif;">')
    parts.append("<h2>Anomaly Audit Results</h2>")
    parts.append(f"<p><strong>Audit run id:</strong> {run_id}</p>")
    parts.append(f"<p><strong>Generated at:</strong> {generated_at} UTC</p>")
    # Lead with summary counts (severity + detector + per-source rollup).
    parts.append(
        "<p><strong>Active findings:</strong> {0} "
        "(HIGH {1}, MEDIUM {2}, LOW {3})<br>"
        "<strong>By detector:</strong> {4}<br>"
        "<strong>By dataset:</strong> {5}</p>".format(
            len(active),
            sev[SEVERITY_HIGH],
            sev[SEVERITY_MEDIUM],
            sev[SEVERITY_LOW],
            det_str or "-",
            ds_str or "-",
        )
    )

    # Console link to the FULL result set for this run.
    link = _console_link(
        run_id,
        run_meta.get("results_dataset", "anomaly_audit"),
        run_meta.get("results_table", "anomaly_results"),
    )
    parts.append('<p><a href="{0}">Open full results in BigQuery</a></p>'.format(link))

    # "Changes since last run" -- new/resolved ACTIVE anomalies vs the prior run.
    delta = run_meta.get("delta") or {}
    new_active = delta.get("new_active") or []
    resolved_active = delta.get("resolved_active") or []
    if new_active or resolved_active:
        parts.append("<h3>Changes since last run</h3><ul>")
        for label in new_active:
            parts.append(
                '<li style="color:#b00020;"><strong>NEW:</strong> {0}</li>'.format(
                    label
                )
            )
        for label in resolved_active:
            parts.append('<li style="color:#1b7f37;">resolved: {0}</li>'.format(label))
        parts.append("</ul>")

    # Mutual watchdog warning (sibling freshness audit silent).
    watchdog = run_meta.get("watchdog_note")
    if watchdog:
        parts.append(
            '<p style="color:#b00020;"><strong>{0}</strong></p>'.format(watchdog)
        )

    # Pipeline-job context for affected sources (absent when the orchestrator
    # monitoring tables are unavailable in this project).
    job_context = run_meta.get("job_context") or {}
    if job_context:
        parts.append("<h3>Pipeline job context</h3><ul>")
        for source in sorted(job_context):
            parts.append("<li>{0}: {1}</li>".format(source, job_context[source]))
        parts.append("</ul>")

    # FULL TABLE: every finding (ACTIVE + HISTORICAL), sorted HIGH->MEDIUM->LOW,
    # ACTIVE before HISTORICAL. Per-severity row color for quick triage; HISTORICAL
    # rows are dimmed so urgent ACTIVE rows stand out.
    all_rows = _sorted_all(active_findings)
    severity_colors = {
        SEVERITY_HIGH: "#fdecea",
        SEVERITY_MEDIUM: "#fff4e5",
        SEVERITY_LOW: "#e6f4ea",
    }
    parts.append("<h3>All findings ({0})</h3>".format(len(all_rows)))
    parts.append(
        '<table border="1" cellpadding="6" cellspacing="0" '
        'style="border-collapse: collapse;">'
    )
    parts.append(
        '<tr style="background-color:#333;color:#fff;">'
        "<th>Severity</th><th>Scope</th><th>Dataset.Table</th><th>Detector</th>"
        "<th>Date</th><th>Observed</th><th>Detail</th><th>Policy</th></tr>"
    )
    # Policy column shows the cadence preset (e.g. 'rare') for rows on a non-
    # default audit policy; operator-supplied reason is attached as a title=
    # tooltip so the rationale is one hover away without cluttering the cell.
    for row in all_rows:
        severity = _fmt(row.get("severity"))
        bg = severity_colors.get(severity, "#ffffff")
        # Dim HISTORICAL rows (lighter text) so ACTIVE rows read first.
        style = "background-color:{0};".format(bg)
        if row.get("scope") != SCOPE_ACTIVE:
            style += "color:#888;"
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
            '<tr style="{style}">'
            "<td>{sev}</td><td>{scope}</td><td>{table}</td><td>{detector}</td>"
            "<td>{adate}</td>"
            '<td align="right">{obs}</td><td>{detail}</td>{policy_cell}</tr>'.format(
                style=style,
                sev=severity,
                scope=_fmt(row.get("scope")),
                table=_qualified_name(row),
                detector=_fmt(row.get("detector")),
                adate=_fmt(row.get("anomaly_date")),
                obs=_fmt(row.get("observed_count")),
                detail=_detail(row),
                policy_cell=policy_cell,
            )
        )
    parts.append("</table>")

    note = _historical_note(new_historical_count)
    if note:
        parts.append(f"<p>{note}</p>")

    parts.append("</body></html>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Recipient computation
# ---------------------------------------------------------------------------


def compute_extra_recipients(active_findings: List[Dict[str, Any]]) -> List[str]:
    """Union per-row email_recipients for actionable ACTIVE rows.

    A row contributes its recipients ONLY when:
      * its scope is ACTIVE, AND
      * its transient helper ``send_email_notification`` is truthy.

    Recipients are split on commas/semicolons, deduplicated while preserving
    first-seen order, and returned as a flat list.
    """
    seen: Dict[str, None] = {}
    for row in active_findings:
        if row.get("scope") != SCOPE_ACTIVE:
            continue
        if not row.get("send_email_notification"):
            continue
        for addr in _split_recipients(row.get("email_recipients")):
            if addr not in seen:
                seen[addr] = None
    return list(seen.keys())


# ---------------------------------------------------------------------------
# Public send entrypoint
# ---------------------------------------------------------------------------


def _build_subject(active: List[Dict[str, Any]]) -> str:
    """Build the audit email subject line (ASCII only, no em-dash).

    'Anomaly Audit Results - ANOMALIES FOUND - YYYY-MM-DD' when any ACTIVE finding
    exists, else 'Anomaly Audit Results - YYYY-MM-DD'.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if active:
        return f"Anomaly Audit Results - ANOMALIES FOUND - {today}"
    return f"Anomaly Audit Results - {today}"


def send_anomaly_summary(
    active_findings: List[Dict[str, Any]],
    run_meta: Dict[str, Any],
    email_config: Dict[str, Any],
    new_historical_count: int,
    additional_recipients: Optional[List[str]] = None,
) -> bool:
    """Decide whether to send, build the bodies, and dispatch the anomaly email.

    Send decision: send when there is >= 1 ACTIVE finding OR
    ``new_historical_count`` > 0 OR ``run_meta['always_send']`` is True. Otherwise
    no email is sent and this returns False.

    Recipients: ``additional_recipients`` (passed by the runner) are unioned with
    the per-row recipients computed by ``compute_extra_recipients`` over the ACTIVE
    findings, so per-config ``email_recipients`` for notify-enabled actionable rows
    are honored. The list is deduped while preserving first-seen order.

    The send uses the freshness multipart path
    (``send_html_email_notification``) so recipients get both a plain-text and an
    HTML view; on failure it falls back to the plain-text-only
    ``shared.notifications.send_email_notification``. Returns the send bool.
    """
    active = _active_only(active_findings)
    always_send = bool(run_meta.get("always_send"))

    if not active and not new_historical_count and not always_send:
        logger.info(
            "No active findings, no new historical outliers, always_send False "
            "-- skipping anomaly email"
        )
        return False

    # Union the runner-supplied recipients with per-row recipients for ACTIVE,
    # notify-enabled rows. Dedup while preserving order.
    extra = list(additional_recipients or [])
    extra.extend(compute_extra_recipients(active_findings))
    extra = list(dict.fromkeys(extra))

    subject = _build_subject(active)
    text_body = build_plain_summary(active_findings, run_meta, new_historical_count)
    html_body = build_html_summary(active_findings, run_meta, new_historical_count)

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
