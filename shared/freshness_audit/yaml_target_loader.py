#!/usr/bin/env python3
# Builds the freshness audit target list from the per-source YAML configs, so the
# audit checks exactly the production "_data" tables the pipeline declares it
# delivers -- no hand-maintained BigQuery config table.
"""YAML-driven freshness target loader.

Replaces the BigQuery ``table_freshness_config`` table as the source of truth.
For each configured source (facebook, wordpress, ...), this reads the source's
merged YAML via ``shared.config_loader.load_config`` and produces one audit
target dict per ACTIVELY-DELIVERED production table:

  * production dataset  = ``pipeline.production_dataset`` (the "_data" dataset);
  * table_name          = each resource's ``table_name``;
  * date_column         = first entry of the resource's ``incremental.fields``
                          (the cursor the pipeline itself increments on), or the
                          ETL load column ``extracted_at`` as a fallback when the
                          resource has no incremental date (strategy ``none`` or a
                          ``via_parent`` child with its own load timestamp).

Resources marked ``active: false`` (not delivered) or ``active: child`` /
``incremental.strategy: via_parent`` whose data is delivered THROUGH a parent are
treated per the rules below. The emitted dicts have the SAME shape the old
BigQuery config loader returned, so every downstream module (validator,
calculator, status_rules, result_writer, email_sender) is unchanged.

ASCII-only logging. No SQL here -- this module only reads YAML.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from shared.config_loader import load_config

logger = logging.getLogger(__name__)

# Path to the audit sources registry. Single source of truth for which sources
# the freshness AND anomaly audits scan. See configs/audit_sources.yaml for the
# schema. Override in tests by patching _AUDIT_SOURCES_PATH directly.
_AUDIT_SOURCES_PATH = (
    Path(__file__).resolve().parent.parent.parent / "configs" / "audit_sources.yaml"
)

# Default freshness policy: a delivered table is FAIL only when its most recent
# data is older than 3 days. warning == freshness so there is no early WARNING
# band -- PASS until day 3, FAIL after. (Per the operator's "older than 3 days"
# rule.) Override per-source via audit.freshness_threshold_days in the source YAML.
_DEFAULT_FRESHNESS_THRESHOLD_DAYS = 3
_DEFAULT_WARNING_THRESHOLD_DAYS = 3

# Resource ``active`` values that mean "not independently delivered" and so are
# excluded from the audit: False (disabled) and 'child' (delivered via a parent).
_INACTIVE_ACTIVE_VALUES = (False, "child")

# ETL load-time column present in every resource schema; used as the freshness
# column when a resource declares no incremental date field.
_EXTRACTED_AT_FALLBACK = "extracted_at"

# Cadence presets: one-word knobs that set sensible freshness/warning thresholds
# for tables that legitimately update on a non-daily rhythm. Operators pick a
# preset on the resource's audit: block; explicit ``freshness_threshold_days`` /
# ``warning_threshold_days`` on the same block override the preset's defaults.
# ``manual`` deliberately keeps a 365-day FAIL backstop rather than disabling, so
# "did anyone touch this in a year?" still surfaces as signal. ``warning`` may
# be None, meaning "no early warning band -- PASS straight to FAIL at threshold".
_CADENCE_PRESETS: Dict[str, Dict[str, Any]] = {
    "daily": {"freshness_threshold_days": 3, "warning_threshold_days": 3},
    "weekly": {"freshness_threshold_days": 14, "warning_threshold_days": 10},
    "biweekly": {"freshness_threshold_days": 30, "warning_threshold_days": 21},
    "monthly": {"freshness_threshold_days": 60, "warning_threshold_days": 45},
    "rare": {"freshness_threshold_days": 120, "warning_threshold_days": 90},
    "snapshot": {"freshness_threshold_days": 365, "warning_threshold_days": 180},
    "manual": {"freshness_threshold_days": 365, "warning_threshold_days": None},
    # static: intentionally-unchanging reference/config data. A within-threshold
    # result is reported as the distinct STATUS_STATIC (not PASS), with no warning
    # band. The threshold is large but finite so a static table that disappears or
    # goes absurdly stale (or errors / empties) still FAILs/ERRORs -- "static" is
    # a freshness exemption, not a check exemption.
    "static": {"freshness_threshold_days": 3650, "warning_threshold_days": None},
}

# Cadence presets that mark a target STATIC (within-threshold -> STATUS_STATIC).
_STATIC_CADENCES = frozenset({"static"})


class AuditConfigError(ValueError):
    """Raised when a resource's audit: block is malformed (e.g. override without
    ``reason``). A run-level fatal: a missing reason is operator intent we will
    not silently honor, because the whole point of the reason is to make the
    exclusion auditable later."""


def _audit_block(
    resource: Dict[str, Any], *, resource_label: str = "<unknown>"
) -> Dict[str, Any]:
    """Normalize a resource's optional ``audit:`` key into a canonical dict.

    Supported forms in the source YAML, per resource:
      * absent            -> defaults (enabled, no overrides)
      * ``audit: false``  -> exclude from BOTH audits
      * ``audit: {enabled, date_column, cadence, freshness_threshold_days,
        warning_threshold_days, reason, anomaly}`` -> explicit control. Every
        key is optional; ``enabled`` defaults True. Any of cadence/threshold
        overrides REQUIRES ``reason`` (one-line operator-supplied string).

    Returned shape (always):
        {
          "enabled": bool,
          "date_column": Optional[str],
          "cadence": Optional[str],                  # preset name if set
          "freshness_threshold_days": Optional[int], # explicit override
          "warning_threshold_days": Optional[int],   # explicit override (None ok)
          "reason": Optional[str],
          "anomaly": Dict[str, Any],                 # raw passthrough for the
                                                     # anomaly audit (step 2)
        }

    Raises:
        AuditConfigError if cadence or thresholds are set without ``reason``,
        or if ``cadence`` is not a known preset name.
    """
    raw = resource.get("audit")
    base = {
        "enabled": True,
        "date_column": None,
        "cadence": None,
        "freshness_threshold_days": None,
        "warning_threshold_days": None,
        "reason": None,
        "anomaly": {},
    }
    if raw is None:
        return base
    if isinstance(raw, bool):
        base["enabled"] = raw
        return base
    if not isinstance(raw, dict):
        # Unrecognized shape -- fail open (audit the table) so a typo never
        # silently removes a table from coverage.
        return base

    base["enabled"] = bool(raw.get("enabled", True))
    date_column = raw.get("date_column")
    if date_column:
        base["date_column"] = str(date_column)

    cadence = raw.get("cadence")
    if cadence is not None:
        if cadence not in _CADENCE_PRESETS:
            raise AuditConfigError(
                "Resource {0}: unknown audit.cadence '{1}'. "
                "Valid presets: {2}".format(
                    resource_label, cadence, ", ".join(sorted(_CADENCE_PRESETS))
                )
            )
        base["cadence"] = cadence

    # Explicit threshold overrides. Coerce to int; allow None for warning to
    # mean "no early warning band".
    for key in ("freshness_threshold_days", "warning_threshold_days"):
        if key in raw and raw[key] is not None:
            base[key] = int(raw[key])

    reason = raw.get("reason")
    if reason is not None:
        base["reason"] = str(reason).strip() or None

    anomaly = raw.get("anomaly")
    if isinstance(anomaly, dict):
        base["anomaly"] = anomaly

    # Required-reason validation. Any override of the source-level default
    # (preset OR explicit threshold) MUST be justified. This keeps the override
    # auditable -- you can grep for cadence: and see every exclusion's reason.
    has_override = (
        base["cadence"] is not None
        or base["freshness_threshold_days"] is not None
        or base["warning_threshold_days"] is not None
    )
    if has_override and not base["reason"]:
        raise AuditConfigError(
            "Resource {0}: audit override requires a 'reason' string "
            "(cadence={1}, freshness_threshold_days={2}, warning_threshold_days={3}). "
            "Add a one-line reason so future operators can tell intentional "
            "exclusions from mistakes.".format(
                resource_label,
                base["cadence"],
                base["freshness_threshold_days"],
                base["warning_threshold_days"],
            )
        )

    return base


def _resolve_thresholds(
    audit: Dict[str, Any],
    *,
    source_freshness: int,
    source_warning: int,
) -> Dict[str, Any]:
    """Resolve final (freshness, warning) thresholds from cadence + explicit overrides.

    Precedence (highest to lowest):
      1. Explicit ``audit.freshness_threshold_days`` / ``audit.warning_threshold_days``
      2. The ``audit.cadence`` preset's defaults
      3. Source-level ``audit:`` block defaults
      4. Module-level _DEFAULT_* constants (resolved upstream by caller)

    Warning is clamped to <= freshness so the WARNING band never overshoots
    FAIL (which would make WARNING unreachable). A None warning is preserved
    (means "no early warning -- PASS straight to FAIL"). The runner's
    determine_status treats warning == freshness as a no-WARNING band.
    """
    preset = _CADENCE_PRESETS.get(audit["cadence"]) if audit["cadence"] else None

    if audit["freshness_threshold_days"] is not None:
        freshness = audit["freshness_threshold_days"]
    elif preset is not None:
        freshness = int(preset["freshness_threshold_days"])
    else:
        freshness = source_freshness

    if audit["warning_threshold_days"] is not None:
        warning = audit["warning_threshold_days"]
    elif preset is not None:
        warning = preset["warning_threshold_days"]
    else:
        warning = source_warning

    # Clamp warning to <= freshness. None passes through unchanged.
    if warning is not None and warning > freshness:
        warning = freshness
    # When warning is None ("no warning band"), the existing status_rules
    # contract treats warning_threshold_days == freshness_threshold_days as
    # "go straight to FAIL". Normalize None -> freshness for downstream
    # consumers that expect an int.
    if warning is None:
        warning = freshness

    return {"freshness_threshold_days": freshness, "warning_threshold_days": warning}


def _resource_is_audited(resource: Dict[str, Any]) -> bool:
    """Return True if a resource is an independently-delivered production table.

    Excludes resources flagged ``active: false`` (disabled), ``active: child``
    (delivered through a parent table, not on its own), or ``audit: false`` /
    ``audit: {enabled: false}`` (explicitly excluded from auditing).
    """
    if resource.get("active") in _INACTIVE_ACTIVE_VALUES:
        return False
    return _audit_block(resource)["enabled"]


def _resolve_date_column(resource: Dict[str, Any]) -> str:
    """Pick the freshness date column for a resource.

    Preference order:
      1. an explicit ``audit.date_column`` override on the resource (e.g.
         identity_hub tables whose freshness column is ``last_seen``);
      2. the first column in ``incremental.fields`` -- the cursor the pipeline
         increments on, i.e. the freshest data-bearing date;
      3. ``extracted_at`` -- the ETL load timestamp present in every schema -- as
         a fallback for resources with no incremental date (strategy ``none`` or a
         ``via_parent`` child that still lands its own load timestamp).
    """
    override = _audit_block(resource)["date_column"]
    if override:
        return override
    incremental = resource.get("incremental") or {}
    fields = incremental.get("fields") or []
    if isinstance(fields, (list, tuple)) and fields:
        return str(fields[0])
    return _EXTRACTED_AT_FALLBACK


def _has_real_date_cursor(resource: Dict[str, Any]) -> bool:
    """Return True if the resource declares a genuine data-bearing date cursor.

    True when an explicit ``audit.date_column`` override names the column (the
    operator vouches for it as a real freshness signal, so the table is eligible
    for genuine FAIL), or when incremental.strategy is 'date' AND there is at
    least one incremental field. Tables with strategy 'none' / 'id' /
    'parent_join' / 'via_parent' have no data-date cursor: they are audited on
    extracted_at (the ETL load time) and their worst status is CAPPED at WARNING
    by the runner, so a stale load is visible without reading as a hard
    data-staleness FAIL.
    """
    if _audit_block(resource)["date_column"]:
        return True
    incremental = resource.get("incremental") or {}
    strategy = incremental.get("strategy")
    fields = incremental.get("fields") or []
    return strategy == "date" and bool(fields)


def build_targets_for_source(
    source: str,
    project_id: str,
    *,
    recipients: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Build audit target dicts for one source from its YAML config.

    Args:
        source: source name (e.g. 'facebook'); load_config(source) must resolve.
        project_id: GCP project id stamped onto each target (probe interpolates it).
        recipients: comma-separated recipient string applied to every target from
            this source (carried through to the email step). None is allowed.

    Returns:
        A list of config-shaped dicts (same keys the old BQ loader returned):
        config_id, project_id, dataset_name, table_name, date_column,
        partition_column, freshness_threshold_days, warning_threshold_days,
        source_system, table_owner, email_recipients, send_email_notification,
        is_active. Returns [] if the source has no production_dataset or no
        audited resources.
    """
    try:
        config = load_config(source)
    except FileNotFoundError:
        logger.warning("No config found for source %s -- skipping", source)
        return []

    pipeline = config.get("pipeline") or {}
    production_dataset = pipeline.get("production_dataset")
    if not production_dataset:
        logger.warning(
            "Source %s has no pipeline.production_dataset -- skipping", source
        )
        return []

    # Per-source threshold overrides (optional); default to the 3-day rule.
    audit_cfg = config.get("audit") or {}
    freshness_threshold = int(
        audit_cfg.get("freshness_threshold_days", _DEFAULT_FRESHNESS_THRESHOLD_DAYS)
    )
    warning_threshold = int(
        audit_cfg.get("warning_threshold_days", _DEFAULT_WARNING_THRESHOLD_DAYS)
    )

    resources = config.get("resources") or {}
    targets: List[Dict[str, Any]] = []
    skipped = 0
    for name, resource in resources.items():
        resource = resource or {}
        # Re-parse the audit block once with the resource_label so any
        # AuditConfigError (missing reason, unknown cadence) names the offending
        # resource. _resource_is_audited / _resolve_date_column also call
        # _audit_block internally but with the default label -- safe because we
        # parse it here first; their re-parse will raise the same error if any.
        audit = _audit_block(resource, resource_label="{0}.{1}".format(source, name))
        if resource.get("active") in _INACTIVE_ACTIVE_VALUES or not audit["enabled"]:
            skipped += 1
            continue
        table_name = resource.get("table_name") or name
        date_column = _resolve_date_column(resource)
        has_cursor = _has_real_date_cursor(resource)
        thresholds = _resolve_thresholds(
            audit,
            source_freshness=freshness_threshold,
            source_warning=warning_threshold,
        )
        targets.append(
            {
                # A stable, human-readable id; no DB sequence needed.
                "config_id": "{0}:{1}".format(source, table_name),
                "project_id": project_id,
                "dataset_name": production_dataset,
                "table_name": table_name,
                "date_column": date_column,
                "partition_column": date_column,
                "freshness_threshold_days": thresholds["freshness_threshold_days"],
                "warning_threshold_days": thresholds["warning_threshold_days"],
                "source_system": source,
                "table_owner": "data-team",
                "email_recipients": recipients,
                "send_email_notification": True,
                "is_active": True,
                # True only for a genuine data-date cursor (strategy: date). When
                # False, the table is audited on extracted_at and the runner caps
                # its worst status at WARNING (never FAIL).
                "has_date_cursor": has_cursor,
                # Declared primary key + schema (column -> type) from the resource,
                # consumed by the duplicate-key and schema-drift quality checks.
                # Empty when the resource declares none (checks skip the table).
                "primary_key": list(resource.get("primary_key") or []),
                "schema_columns": dict(resource.get("schema") or {}),
                # Surfaced on the result row + email so any non-default policy is
                # visible. None when the resource is audited under the source-
                # level default (no override).
                "audit_cadence": audit["cadence"],
                "audit_reason": audit["reason"],
                # True for cadence: static -- a within-threshold result is reported
                # as STATUS_STATIC (intentionally-unchanging reference data) rather
                # than PASS, while a static table past even its large threshold
                # still FAILs.
                "is_static": audit["cadence"] in _STATIC_CADENCES,
                # Raw anomaly-detector knobs passed through unchanged so the
                # anomaly audit (step 2) can read per-table tuning without
                # re-parsing the YAML.
                "anomaly_overrides": audit["anomaly"],
            }
        )

    logger.info(
        "Source %s: %d audited table(s), %d skipped (inactive/child)",
        source,
        len(targets),
        skipped,
    )
    return targets


def load_active_audit_sources(
    registry_path: Optional[Path] = None,
) -> List[str]:
    """Read configs/audit_sources.yaml and return the active source names.

    The registry is the single source of truth for which ETL sources the
    freshness AND anomaly audits scan. Sources flagged ``active: false`` (or
    omitted) are excluded.

    Args:
        registry_path: override the default path; primarily for tests.

    Returns:
        List of source names with ``active: true``, in registry declaration
        order. Returns an empty list (with a warning) if the registry is missing
        or malformed -- callers may decide how to handle "no sources" (the audit
        runners log it and return an empty target list, which is preferable to
        crashing).
    """
    path = registry_path or _AUDIT_SOURCES_PATH
    try:
        with open(path) as fh:
            registry = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        logger.warning(
            "Audit sources registry not found at %s -- no sources will be audited",
            path,
        )
        return []
    except yaml.YAMLError as exc:
        logger.error(
            "Audit sources registry at %s could not be parsed: %s -- no sources will be audited",
            path,
            exc,
        )
        return []

    sources = registry.get("sources") or {}
    if not isinstance(sources, dict):
        logger.warning(
            "Audit sources registry at %s: 'sources' must be a mapping, got %s -- no sources will be audited",
            path,
            type(sources).__name__,
        )
        return []

    active: List[str] = []
    inactive: List[str] = []
    for name, entry in sources.items():
        if not isinstance(entry, dict):
            logger.warning(
                "Audit sources registry: '%s' entry must be a mapping; skipping",
                name,
            )
            continue
        if entry.get("active") is True:
            active.append(name)
        else:
            inactive.append(name)

    if inactive:
        # Log the inactive list at debug so it does not add to operational
        # noise, but is available when diagnosing "why isn't X being audited".
        logger.debug(
            "Audit sources inactive (skipped): %s",
            ", ".join(inactive),
        )
    logger.info(
        "Audit sources registry: %d active, %d inactive",
        len(active),
        len(inactive),
    )
    return active


def build_all_targets(
    sources: List[str],
    project_id: str,
    *,
    recipients: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Build audit targets across every configured source.

    Args:
        sources: list of source names to audit (e.g. ['facebook', 'wordpress']).
        project_id: GCP project id stamped onto every target.
        recipients: comma-separated recipient string applied to all targets.

    Returns:
        A flat list of config-shaped target dicts across all sources. A source
        that fails to load is logged and skipped (never aborts the others).
    """
    all_targets: List[Dict[str, Any]] = []
    for source in sources:
        try:
            all_targets.extend(
                build_targets_for_source(source, project_id, recipients=recipients)
            )
        except AuditConfigError:
            # Reraise: a malformed audit: block (missing reason, unknown cadence)
            # is operator intent we will NOT silently honor. The whole point of
            # the missing-reason rule is to fail fast at startup; swallowing it
            # here would let a run report "success" while one source was silently
            # dropped from coverage. Run-level fatal.
            raise
        except FileNotFoundError as exc:
            # Source YAML genuinely absent (e.g. registry references a name that
            # was removed). Soft-skip with a warning -- not a config error.
            logger.warning(
                "No config file found for source %s -- skipping: %s",
                source,
                exc,
            )
        except Exception as exc:  # noqa: BLE001 - last-resort defense
            # Unexpected error parsing a single source. Log and continue so one
            # genuinely broken file does not block coverage of the others. This
            # is NOT the path for AuditConfigError (handled above).
            logger.warning(
                "Unexpected error building targets for source %s -- skipping: %s",
                source,
                exc,
            )
    logger.info(
        "Built %d freshness audit target(s) across %d source(s)",
        len(all_targets),
        len(sources),
    )
    return all_targets
