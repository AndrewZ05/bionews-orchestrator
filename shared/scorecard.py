#!/usr/bin/env python3
"""
Source Reliability Scorecard
============================

Turns data the orchestrator ALREADY records -- orchestrator_monitoring.jobs
(status / duration / cost), dead_letter_queue (unresolved failures + retries),
schema_changes (drift), extraction_state (freshness) -- into one daily 0-100
reliability score per source, so "which sources are fragile vs dependable" has an
evidence-based answer instead of a gut feel.

Two layers:
  - compute_score(): a PURE function over already-aggregated component metrics.
    Transparent, tunable weights; no BigQuery. This is the unit-tested core.
  - build_scorecard_view_sql() / query_scorecard(): the BigQuery view + reader that
    feed compute_score from the live monitoring tables.

Scoring is a weighted blend of reliability signals. Cost is recorded but NOT scored
(the bq_estimated_cost_usd rollup is currently unpopulated -- $0 across sources -- so
scoring on it would be misleading; it rides along as informational only).
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

MONITORING_DATASET = "orchestrator_monitoring"
VIEW_NAME = "v_source_scorecard"

# Component weights (sum = 1.0). success_rate dominates -- a source that fails is
# unreliable regardless of how fresh or fast it is. Tunable; documented per field.
WEIGHTS = {
    "success_rate": 0.45,  # COMPLETED / total runs (WARNING counts as half-credit)
    "freshness": 0.20,  # how recently the source last extracted vs its cadence
    "failure_load": 0.15,  # penalty for unresolved DLQ failures + retries
    "schema_stability": 0.10,  # penalty for recent schema-drift events
    "duration_stability": 0.10,  # penalty for high run-duration variance
}


def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else (1.0 if x > 1 else x)


def compute_score(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pure scoring of one source's component metrics into a 0-100 reliability score.

    Expected keys in `metrics` (all best-effort; missing -> neutral/no-penalty):
      runs:               total runs in window (int)
      completed:          COMPLETED runs (int)
      warnings:           WARNING runs (int)   -- half credit
      failed:             FAILED runs (int)
      freshness_hours:    hours since last successful extraction (float|None)
      freshness_sla_hours: expected max age before "stale" (float, default 36)
      open_dlq:           unresolved dead-letter rows (int)
      max_retry:          highest retry_count among open DLQ rows (int)
      drift_events:       schema-change events in window (int)
      dur_cv:             coefficient of variation of run duration (float|None)

    Returns {score: 0-100, grade: A-F, components: {name: 0-1}, reasons: [str]}.
    """
    runs = int(metrics.get("runs") or 0)
    reasons: List[str] = []

    # --- success_rate: COMPLETED full credit, WARNING half, FAILED zero ---
    if runs > 0:
        completed = int(metrics.get("completed") or 0)
        warnings = int(metrics.get("warnings") or 0)
        success_rate = _clamp01((completed + 0.5 * warnings) / runs)
    else:
        success_rate = (
            1.0  # no runs in window -> don't penalize (freshness covers staleness)
        )
        reasons.append("no runs in window")
    if success_rate < 0.9 and runs > 0:
        reasons.append(f"success {success_rate:.0%}")

    # --- freshness: 1.0 at/under SLA, decays to 0 by 3x SLA ---
    fh = metrics.get("freshness_hours")
    sla = float(metrics.get("freshness_sla_hours") or 36.0)
    if fh is None:
        freshness = 1.0  # unknown freshness -> neutral, not a penalty
    elif fh <= sla:
        freshness = 1.0
    else:
        # linear decay from SLA (1.0) to 3*SLA (0.0)
        freshness = _clamp01(1.0 - (fh - sla) / (2.0 * sla))
        reasons.append(f"stale {fh:.0f}h (> {sla:.0f}h)")

    # --- failure_load: penalty for open DLQ rows + escalating retries ---
    open_dlq = int(metrics.get("open_dlq") or 0)
    max_retry = int(metrics.get("max_retry") or 0)
    if open_dlq == 0:
        failure_load = 1.0
    else:
        # each open table costs 0.2; an at-max-retries table is worse
        failure_load = _clamp01(1.0 - 0.2 * open_dlq - 0.1 * max_retry)
        reasons.append(f"{open_dlq} open DLQ table(s)")

    # --- schema_stability: penalty for recent drift (some drift is normal) ---
    drift = int(metrics.get("drift_events") or 0)
    schema_stability = _clamp01(1.0 - 0.15 * drift)
    if drift:
        reasons.append(f"{drift} schema change(s)")

    # --- duration_stability: penalize high variance (CV); cap penalty ---
    cv = metrics.get("dur_cv")
    if cv is None:
        duration_stability = 1.0
    else:
        duration_stability = _clamp01(1.0 - min(float(cv), 1.0))
        if cv and cv > 0.5:
            reasons.append(f"duration CV {cv:.0%}")

    components = {
        "success_rate": success_rate,
        "freshness": freshness,
        "failure_load": failure_load,
        "schema_stability": schema_stability,
        "duration_stability": duration_stability,
    }
    score = sum(components[k] * WEIGHTS[k] for k in WEIGHTS) * 100.0

    if score >= 90:
        grade = "A"
    elif score >= 80:
        grade = "B"
    elif score >= 70:
        grade = "C"
    elif score >= 60:
        grade = "D"
    else:
        grade = "F"

    return {
        "score": round(score, 1),
        "grade": grade,
        "components": {k: round(v, 3) for k, v in components.items()},
        "reasons": reasons,
    }


def build_scorecard_view_sql(project: str, window_days: int = 14) -> str:
    """
    SQL for the orchestrator_monitoring.v_source_scorecard view: one row per source
    over a rolling window with the component metrics compute_score() consumes. The
    Python layer applies the weights (keeps weight tuning in one place, code-side).
    """
    md = f"`{project}.{MONITORING_DATASET}"
    return f"""
CREATE OR REPLACE VIEW {md}.{VIEW_NAME}` AS
WITH job_stats AS (
  SELECT
    source,
    COUNT(*) AS runs,
    COUNTIF(status = 'COMPLETED') AS completed,
    COUNTIF(status = 'WARNING') AS warnings,
    COUNTIF(status = 'FAILED') AS failed,
    AVG(duration_seconds) AS avg_dur,
    STDDEV_SAMP(duration_seconds) AS sd_dur,
    SUM(bq_estimated_cost_usd) AS cost_usd
  FROM {md}.jobs`
  WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(window_days)} DAY)
    AND status IN ('COMPLETED', 'WARNING', 'FAILED')
  GROUP BY source
),
fresh AS (
  SELECT source,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(last_extracted_at), HOUR) AS freshness_hours
  FROM {md}.extraction_state`
  GROUP BY source
),
dlq AS (
  SELECT source,
    COUNT(*) AS open_dlq,
    MAX(retry_count) AS max_retry
  FROM {md}.dead_letter_queue`
  WHERE resolved = FALSE
  GROUP BY source
),
drift AS (
  SELECT source, COUNT(*) AS drift_events
  FROM {md}.schema_changes`
  WHERE applied = TRUE
    AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(window_days)} DAY)
  GROUP BY source
)
SELECT
  j.source,
  j.runs, j.completed, j.warnings, j.failed,
  f.freshness_hours,
  COALESCE(d.open_dlq, 0) AS open_dlq,
  COALESCE(d.max_retry, 0) AS max_retry,
  COALESCE(s.drift_events, 0) AS drift_events,
  j.avg_dur,
  SAFE_DIVIDE(j.sd_dur, NULLIF(j.avg_dur, 0)) AS dur_cv,
  j.cost_usd
FROM job_stats j
LEFT JOIN fresh f USING (source)
LEFT JOIN dlq d USING (source)
LEFT JOIN drift s USING (source)
ORDER BY j.source
""".strip()


def query_scorecard(
    bq_client,
    project: Optional[str] = None,
    window_days: int = 14,
    sla_hours: float = 36.0,
) -> List[Dict[str, Any]]:
    """
    Build/refresh the view, read its rows, and apply compute_score() to each source.
    Returns a list of {source, score, grade, reasons, ...components+raw} sorted worst
    (most fragile) first. Best-effort: returns [] on error.
    """
    project = project or bq_client.project
    try:
        bq_client.query(build_scorecard_view_sql(project, window_days)).result()
        rows = list(
            bq_client.query(
                f"SELECT * FROM `{project}.{MONITORING_DATASET}.{VIEW_NAME}`"
            ).result()
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("Scorecard query failed: %s", e)
        return []

    out = []
    for r in rows:
        metrics = {
            "runs": r["runs"],
            "completed": r["completed"],
            "warnings": r["warnings"],
            "failed": r["failed"],
            "freshness_hours": r["freshness_hours"],
            "freshness_sla_hours": sla_hours,
            "open_dlq": r["open_dlq"],
            "max_retry": r["max_retry"],
            "drift_events": r["drift_events"],
            "dur_cv": r["dur_cv"],
        }
        scored = compute_score(metrics)
        out.append(
            {
                "source": r["source"],
                "score": scored["score"],
                "grade": scored["grade"],
                "reasons": scored["reasons"],
                "runs": r["runs"],
                "failed": r["failed"],
                "warnings": r["warnings"],
                "open_dlq": r["open_dlq"],
                "drift_events": r["drift_events"],
                "freshness_hours": r["freshness_hours"],
                "avg_dur": r["avg_dur"],
                "cost_usd": r["cost_usd"],
            }
        )
    out.sort(key=lambda x: x["score"])  # most fragile first
    return out


def load_notification_config() -> Dict[str, Any]:
    """
    Load just the global notification/email config (defaults.yaml + alerting.yaml)
    so the scorecard alert works WITHOUT a --source. Notifications are global, not
    per-source. Best-effort: returns {} if files are absent.
    """
    import os
    import yaml

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    merged: Dict[str, Any] = {}
    for name in ("defaults.yaml", "alerting.yaml"):
        path = os.path.join(root, "configs", name)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                # shallow-merge the keys we care about
                for k in ("notifications", "email", "monitoring"):
                    if k in data:
                        merged[k] = data[k]
            except Exception as e:  # noqa: BLE001
                logger.debug("Could not load %s for notifications: %s", name, e)
    return merged


def evaluate_degradation(
    current: List[Dict[str, Any]],
    baseline_scores: Dict[str, float],
    *,
    floor: float = 70.0,
    drop_threshold: float = 15.0,
) -> List[Dict[str, Any]]:
    """
    Pure degradation check (no BigQuery). Flags a source when it is either:
      - below an absolute floor (score < floor), OR
      - sharply down vs its own baseline (baseline - current >= drop_threshold).

    `current` is query_scorecard() output; `baseline_scores` maps source -> a prior
    trailing score (e.g. the previous longer window). Returns the flagged subset with
    a `flag_reason`, worst first. A new source absent from baseline is judged on the
    floor alone (no false "dropped" alert).
    """
    flagged = []
    for row in current:
        src = row["source"]
        score = row["score"]
        base = baseline_scores.get(src)
        reasons = []
        if score < floor:
            reasons.append(f"score {score:.0f} below floor {floor:.0f}")
        if base is not None and (base - score) >= drop_threshold:
            reasons.append(f"dropped {base - score:.0f} pts vs baseline {base:.0f}")
        if reasons:
            flagged.append({**row, "flag_reason": "; ".join(reasons), "baseline": base})
    flagged.sort(key=lambda x: x["score"])
    return flagged


def check_scorecard_degradation(
    bq_client,
    project: Optional[str] = None,
    *,
    window_days: int = 7,
    baseline_days: int = 30,
    floor: float = 70.0,
    drop_threshold: float = 15.0,
    sla_hours: float = 36.0,
) -> List[Dict[str, Any]]:
    """
    Compute the recent scorecard (window_days) and a baseline scorecard
    (baseline_days), then return the degraded sources (evaluate_degradation).
    Best-effort: returns [] on error. The caller decides whether to alert.
    """
    project = project or bq_client.project
    current = query_scorecard(bq_client, project, window_days, sla_hours)
    baseline_rows = query_scorecard(bq_client, project, baseline_days, sla_hours)
    baseline_scores = {r["source"]: r["score"] for r in baseline_rows}
    return evaluate_degradation(
        current, baseline_scores, floor=floor, drop_threshold=drop_threshold
    )


def format_scorecard(rows: List[Dict[str, Any]]) -> str:
    """ASCII table, fragile -> dependable. (No non-ASCII -- prod-log safe.)"""
    if not rows:
        return "Scorecard: no source data in window."
    lines = [
        "SOURCE RELIABILITY SCORECARD (worst -> best)",
        "-" * 78,
        f"{'source':<18}{'score':>6} {'grade':>5}  {'runs':>4} {'fail':>4} {'warn':>4} {'drift':>5}  reasons",
        "-" * 78,
    ]
    for r in rows:
        reasons = "; ".join(r["reasons"])[:28]
        lines.append(
            f"{r['source']:<18}{r['score']:>6.1f} {r['grade']:>5}  "
            f"{r['runs']:>4} {r['failed']:>4} {r['warnings']:>4} {r['drift_events']:>5}  {reasons}"
        )
    return "\n".join(lines)
