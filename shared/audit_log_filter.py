#!/usr/bin/env python3
# Context manager that temporarily raises noisy upstream loggers to ERROR for
# the duration of an audit run, so the operator-visible audit output is not
# drowned in deprecation warnings from shared.config_loader / shared.config_schema.
"""Audit-boundary log suppression.

The freshness and anomaly audits load every source's YAML at runtime. Each
load may emit dozens of ``[<source>] config deprecation: ...`` WARNING lines
from ``shared.config_loader`` / ``shared.config_schema`` (the v1 -> v2 key
migration compatibility shim). Those warnings are not actionable in the
context of an audit run: the operator running the audit usually has nothing
to do with the source YAML migration.

This helper installs a context manager that temporarily raises the upstream
loggers' levels to ERROR for the duration of an audit and restores them on
exit. The effect is scoped: other code running outside the context (a normal
ETL run, ad-hoc CLI invocation) still sees the deprecation warnings.

Use:

    from shared.audit_log_filter import suppress_config_deprecations

    with suppress_config_deprecations():
        summary = audit_runner.run_audit(client, config, audit_run_id)

ASCII-only logging. No SQL here.
"""

import logging
from contextlib import contextmanager
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

# Loggers whose WARNING-level deprecation noise should be suppressed during
# audit runs. Adding a new noisy logger is a one-line append; the helper is
# purely opt-in and never modifies any logger outside this list.
_DEFAULT_NOISY_LOGGERS = (
    "shared.config_loader",
    "shared.config_schema",
)


@contextmanager
def suppress_config_deprecations(
    logger_names: Optional[Iterable[str]] = None,
    level: int = logging.ERROR,
):
    """Temporarily raise the named upstream loggers to ``level`` for an audit run.

    Args:
        logger_names: iterable of logger names to suppress. Defaults to
            ``shared.config_loader`` and ``shared.config_schema``.
        level: the temporary log level. Defaults to ``logging.ERROR`` so genuine
            errors still surface while INFO/WARNING noise is suppressed.

    The original levels are captured on entry and restored on exit, even if the
    audit raises -- the contract is "scoped suppression," never permanent
    modification.
    """
    names = list(logger_names or _DEFAULT_NOISY_LOGGERS)
    saved: dict = {}
    for name in names:
        upstream = logging.getLogger(name)
        # getEffectiveLevel() walks up the parent chain when the logger has no
        # explicit level set; saving level (not getEffectiveLevel()) preserves
        # the "inherit from parent" state by restoring the raw value, including
        # NOTSET (0) which re-enables parent inheritance.
        saved[name] = upstream.level
        upstream.setLevel(level)
    try:
        yield
    finally:
        for name, original_level in saved.items():
            logging.getLogger(name).setLevel(original_level)
