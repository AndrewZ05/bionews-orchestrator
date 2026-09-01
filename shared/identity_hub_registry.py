"""YAML-driven Identity Hub connector registry.

Single source of truth: configs/identity_hub.yaml `connectors` keys that have a
matching IdentityHubBuilder.connect_<name> method. Enabled-but-unwired
connectors hard-fail at resolve time.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Set


# Connectors present in YAML but intentionally not implemented yet.
UNIMPLEMENTED_CONNECTORS: Set[str] = {
    "name_geo",  # configured but connect_name_geo not implemented yet
}


def discover_connect_methods(builder_cls) -> Set[str]:
    """Return connector names for which connect_<name> exists on the builder."""
    names = set()
    for attr in dir(builder_cls):
        if attr.startswith("connect_") and callable(getattr(builder_cls, attr, None)):
            names.add(attr[len("connect_") :])
    # Internal helpers that are not YAML connectors
    names.discard("existing_graph")
    return names


def wired_connectors_from_config(
    hub_config: Dict[str, Any],
    builder_cls=None,
) -> List[str]:
    """
    Ordered list of connector names that are both in YAML and wired in code.

    Excludes UNIMPLEMENTED_CONNECTORS even if YAML has them enabled.
    """
    connectors_cfg = (hub_config or {}).get("connectors") or {}
    yaml_keys = list(connectors_cfg.keys())
    if builder_cls is None:
        from shared.identity_hub import IdentityHubBuilder

        builder_cls = IdentityHubBuilder
    wired = discover_connect_methods(builder_cls)
    return [k for k in yaml_keys if k in wired and k not in UNIMPLEMENTED_CONNECTORS]


def assert_no_enabled_unwired(hub_config: Dict[str, Any], builder_cls=None) -> None:
    """Hard-fail if YAML enables a connector that has no connect_* method."""
    connectors_cfg = (hub_config or {}).get("connectors") or {}
    if builder_cls is None:
        from shared.identity_hub import IdentityHubBuilder

        builder_cls = IdentityHubBuilder
    wired = discover_connect_methods(builder_cls)
    bad = []
    for name, cfg in connectors_cfg.items():
        if not isinstance(cfg, dict):
            continue
        if not cfg.get("enabled", False):
            continue
        if name in UNIMPLEMENTED_CONNECTORS:
            continue
        if name not in wired:
            bad.append(name)
    if bad:
        raise RuntimeError(
            "Identity Hub YAML enables connector(s) with no connect_* method: "
            + ", ".join(sorted(bad))
        )


def resolve_connector_filter(
    tables: Optional[Iterable[str]],
    hub_config: Dict[str, Any],
    valid: Optional[List[str]] = None,
) -> Optional[List[str]]:
    """Validate --tables against the YAML/wired allowlist."""
    allow = valid or wired_connectors_from_config(hub_config)
    if not tables:
        return None
    tables = list(tables)
    unknown = [t for t in tables if t not in allow]
    if unknown:
        raise ValueError(
            f"Unknown identity hub connector(s): {', '.join(unknown)}. "
            f"Valid: {', '.join(allow)}"
        )
    return tables
