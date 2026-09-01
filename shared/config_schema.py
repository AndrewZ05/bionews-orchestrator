"""
Config contract v2: canonical key normalization, deprecation warnings, and
strict validation for sources that declare `config_version: 2`.

Canonical contract (v2):
  pipeline.parallel.max_workers / pipeline.parallel.batch_size
      (replaces flat pipeline.parallel_workers / pipeline.batch_size)
  resources.<table>.extraction_strategy
      (replaces resources.<table>.extraction_type)
  resources.<table>.primary_key
      (single source of merge keys; hash_merge.keys is a deprecated override)
  groups: + resources.<table>.group
      (every resource belongs to a group; a 'default' group containing all
       active resources is synthesized when a source declares none)
  pipeline.staging.{scope, archive.enabled, archive.ttl_days}
      (staging lifecycle, see shared/staging_lifecycle.py)

Versioning:
  config_version absent or 1  -> legacy: normalize + log deprecation warnings.
  config_version: 2           -> strict: legacy keys are validation errors.

normalize_config() maps legacy keys onto canonical keys AND mirrors canonical
keys back onto the legacy ones, so both old plugin code (reads legacy) and new
code (reads canonical) work with either config style during migration.
"""

import logging
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

SUPPORTED_CONFIG_VERSIONS = (1, 2)

DEFAULT_GROUP_NAME = "default"

# Known incremental strategies (informational; unknown values warn, not error,
# because plugins own the semantics).
KNOWN_INCREMENTAL_STRATEGIES = {
    "date",
    "id",
    "none",
    "via_parent",
    "parent_join",
    "full",
}


def get_config_version(config: Dict[str, Any]) -> int:
    """Declared config_version, defaulting to 1 (legacy)."""
    version = config.get("config_version", 1)
    try:
        return int(version)
    except (TypeError, ValueError):
        return 1


def normalize_config(
    config: Dict[str, Any], source_name: str
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Map legacy keys onto canonical v2 keys (and canonical back onto legacy for
    plugin backward compatibility). Mutates and returns the config plus a list
    of deprecation warnings describing legacy usage found.

    Safe to call on every config regardless of version.
    """
    warnings: List[str] = []
    pipeline = config.setdefault("pipeline", {})
    resources = config.get("resources", {}) or {}

    # --- parallel config: flat <-> nested -------------------------------
    parallel = pipeline.get("parallel")
    if not isinstance(parallel, dict):
        parallel = {}
        pipeline["parallel"] = parallel

    if "parallel_workers" in pipeline:
        if "max_workers" not in parallel:
            parallel["max_workers"] = pipeline["parallel_workers"]
        warnings.append(
            "pipeline.parallel_workers is deprecated; use pipeline.parallel.max_workers"
        )
    elif "max_workers" in parallel:
        # Mirror canonical onto legacy so old readers keep working.
        pipeline["parallel_workers"] = parallel["max_workers"]

    if "batch_size" in pipeline:
        if "batch_size" not in parallel:
            parallel["batch_size"] = pipeline["batch_size"]
        warnings.append(
            "pipeline.batch_size is deprecated; use pipeline.parallel.batch_size"
        )
    elif "batch_size" in parallel:
        pipeline["batch_size"] = parallel["batch_size"]

    # --- extraction strategy naming: extraction_type <-> extraction_strategy
    for table_name, table_config in resources.items():
        if not isinstance(table_config, dict):
            continue
        has_type = "extraction_type" in table_config
        has_strategy = "extraction_strategy" in table_config
        if has_type and not has_strategy:
            table_config["extraction_strategy"] = table_config["extraction_type"]
            warnings.append(
                f"resources.{table_name}.extraction_type is deprecated; "
                f"use extraction_strategy"
            )
        elif has_strategy and not has_type:
            table_config["extraction_type"] = table_config["extraction_strategy"]
        elif (
            has_type
            and has_strategy
            and (table_config["extraction_type"] != table_config["extraction_strategy"])
        ):
            warnings.append(
                f"resources.{table_name}: extraction_type and extraction_strategy "
                f"disagree ({table_config['extraction_type']!r} vs "
                f"{table_config['extraction_strategy']!r}); extraction_strategy wins"
            )
            table_config["extraction_type"] = table_config["extraction_strategy"]

    # --- merge keys: hash_merge.keys -> resources.<table>.primary_key ----
    hash_merge_keys = (config.get("hash_merge", {}) or {}).get("keys", {}) or {}
    for table_name, keys in hash_merge_keys.items():
        table_config = resources.get(table_name)
        if not isinstance(table_config, dict):
            continue
        keys_list = keys if isinstance(keys, list) else [keys]
        existing = table_config.get("primary_key")
        if not existing:
            table_config["primary_key"] = keys_list
        elif list(existing) != list(keys_list):
            warnings.append(
                f"resources.{table_name}.primary_key ({existing}) disagrees with "
                f"hash_merge.keys.{table_name} ({keys_list}); hash_merge.keys wins "
                f"at merge time, consolidate into primary_key"
            )
    if hash_merge_keys:
        warnings.append(
            "hash_merge.keys is a deprecated override; declare merge keys as "
            "resources.<table>.primary_key"
        )

    # --- groups: synthesize the default group ----------------------------
    groups = config.get("groups")
    if not isinstance(groups, dict):
        groups = {}
        config["groups"] = groups

    ungrouped = [
        name
        for name, table_config in resources.items()
        if isinstance(table_config, dict)
        and table_config.get("active", True) is not False
        and not table_config.get("group")
    ]
    if ungrouped:
        default_group = groups.setdefault(
            DEFAULT_GROUP_NAME,
            {
                "description": f"Auto-synthesized default group for {source_name} "
                f"(all active resources without an explicit group)",
                "synthesized": True,
            },
        )
        default_group.setdefault("tables", [])
        for name in ungrouped:
            resources[name]["group"] = DEFAULT_GROUP_NAME
            if name not in default_group["tables"]:
                default_group["tables"].append(name)

    # Ensure every group referenced by a resource exists at top level.
    for name, table_config in resources.items():
        if not isinstance(table_config, dict):
            continue
        group_name = table_config.get("group")
        if group_name and group_name not in groups:
            groups[group_name] = {
                "description": f"Auto-registered from resources.{name}.group",
                "synthesized": True,
            }

    return config, warnings


def validate_config_v2(config: Dict[str, Any], source_name: str) -> List[str]:
    """
    Strict checks for config_version: 2 sources. Returns error messages.

    Run AFTER normalize_config so structural normalization (group synthesis,
    key mirroring) has already happened; these checks look at what the YAML
    actually declared, so they are checked against pre-normalization markers
    where needed (normalize_config only fills missing keys, never removes the
    legacy declarations being checked here).
    """
    errors: List[str] = []
    pipeline = config.get("pipeline", {}) or {}
    resources = config.get("resources", {}) or {}

    # Legacy flat parallel keys are errors in v2 ONLY if declared in the
    # source YAML; normalize_config mirrors canonical onto them, so detect
    # genuine declarations via the deprecation pass instead of presence.
    # (load_config passes the pre-normalization warning list through.)

    # Staging lifecycle block must be well-formed when present.
    staging = pipeline.get("staging")
    if staging is not None:
        if not isinstance(staging, dict):
            errors.append("pipeline.staging must be a mapping")
        else:
            scope = staging.get("scope", "run")
            if scope not in ("run", "dataset"):
                errors.append(
                    f"pipeline.staging.scope must be 'run' or 'dataset', got {scope!r}"
                )
            archive = staging.get("archive", {}) or {}
            if not isinstance(archive, dict):
                errors.append("pipeline.staging.archive must be a mapping")
            else:
                ttl = archive.get("ttl_days")
                if ttl is not None and (not isinstance(ttl, int) or ttl < 1):
                    errors.append(
                        f"pipeline.staging.archive.ttl_days must be a positive "
                        f"integer, got {ttl!r}"
                    )

    # Every active resource must resolve to a group (normalize_config
    # guarantees this; a violation means resources is malformed).
    for name, table_config in resources.items():
        if not isinstance(table_config, dict):
            continue
        if table_config.get("active", True) is not False and not table_config.get(
            "group"
        ):
            errors.append(f"resources.{name} has no group (and synthesis failed)")

    # Merge keys must live on the resource in v2.
    if (config.get("hash_merge", {}) or {}).get("keys"):
        errors.append(
            "config_version 2 forbids hash_merge.keys; declare merge keys as "
            "resources.<table>.primary_key"
        )

    # Active extracted resources should declare primary_key (warn-level in v1,
    # error in v2 for tables that hash-merge).
    for name, table_config in resources.items():
        if not isinstance(table_config, dict):
            continue
        if table_config.get("active", True) is False:
            continue
        strategy = table_config.get("extraction_strategy") or table_config.get(
            "extraction_type"
        )
        if strategy == "via_parent":
            continue
        if not table_config.get("primary_key"):
            errors.append(
                f"resources.{name}.primary_key is required in config_version 2"
            )

    # Unknown incremental strategies are advisory only.
    for name, table_config in resources.items():
        if not isinstance(table_config, dict):
            continue
        incremental = table_config.get("incremental")
        if isinstance(incremental, dict):
            strategy = incremental.get("strategy")
            if strategy and strategy not in KNOWN_INCREMENTAL_STRATEGIES:
                logger.warning(
                    f"[{source_name}] resources.{name}.incremental.strategy "
                    f"{strategy!r} is not a known strategy "
                    f"{sorted(KNOWN_INCREMENTAL_STRATEGIES)}"
                )

    return errors


def check_v2_legacy_declarations(
    raw_source_config: Dict[str, Any],
) -> List[str]:
    """
    Errors for legacy keys declared in a v2 source YAML (checked against the
    raw, pre-merge source config so defaults.yaml mirroring does not trip it).
    """
    errors: List[str] = []
    pipeline = raw_source_config.get("pipeline", {}) or {}
    if "parallel_workers" in pipeline:
        errors.append(
            "config_version 2 forbids pipeline.parallel_workers; "
            "use pipeline.parallel.max_workers"
        )
    if "batch_size" in pipeline:
        errors.append(
            "config_version 2 forbids pipeline.batch_size; "
            "use pipeline.parallel.batch_size"
        )
    for name, table_config in (raw_source_config.get("resources", {}) or {}).items():
        if isinstance(table_config, dict) and "extraction_type" in table_config:
            errors.append(
                f"config_version 2 forbids resources.{name}.extraction_type; "
                f"use extraction_strategy"
            )
    if "table_groups" in raw_source_config:
        errors.append("config_version 2 forbids table_groups; use groups")
    return errors
