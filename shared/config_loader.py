#!/usr/bin/env python3
# Configuration loading with environment variable expansion and secret manager

import logging
import os
import re
import yaml
import threading
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

# Configuration cache (thread-safe)
_config_cache = {}
_cache_lock = threading.Lock()

# Project root - shared/ is one level below root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def expand_env_vars(config: Any) -> Any:
    # Recursively expand environment variables in config
    if isinstance(config, dict):
        return {key: expand_env_vars(value) for key, value in config.items()}
    elif isinstance(config, list):
        return [expand_env_vars(item) for item in config]
    elif isinstance(config, str):
        # Replace ${VAR_NAME} with secret manager or env variable
        pattern = r"\$\{([^}]+)\}"

        def replacer(match):
            try:
                from shared.secret_manager import get_secret_or_env
            except ImportError:
                from secret_manager import get_secret_or_env
            var_name = match.group(1)

            # Handle default value syntax ${VAR:-default}
            if ":-" in var_name:
                var_name, default = var_name.split(":-", 1)
                value = get_secret_or_env(var_name)
                return value if value else default
            else:
                value = get_secret_or_env(var_name)
                return value if value else match.group(0)

        return re.sub(pattern, replacer, config)
    else:
        return config


def load_config(source: str, use_cache: bool = True) -> Dict[str, Any]:
    """
    Load configuration with hierarchical merge and caching.

    Configuration Hierarchy (lowest to highest priority):
    1. configs/defaults.yaml - Global defaults for all sources
    2. configs/alerting.yaml - Global alerting overrides
    3. configs/{source}.yaml - Source-specific config (highest priority)

    Args:
        source: Source name (wordpress, facebook, etc.)
        use_cache: If True, use cached config if available (default: True)

    Returns:
        Merged configuration dictionary with env vars expanded

    Note:
        Config is cached per source for performance.
        Set use_cache=False to force reload from disk.
    """
    # Check cache first
    if use_cache:
        with _cache_lock:
            if source in _config_cache:
                return _config_cache[source].copy()

    # Step 1: Load global defaults
    defaults = {}
    defaults_path = _PROJECT_ROOT / "configs" / "defaults.yaml"
    if defaults_path.exists():
        with open(defaults_path, "r", encoding="utf-8") as f:
            defaults = yaml.safe_load(f) or {}

    # Step 2: Load global alerting config
    alerting_overrides = {}
    alerting_path = _PROJECT_ROOT / "configs" / "alerting.yaml"
    if alerting_path.exists():
        with open(alerting_path, "r", encoding="utf-8") as f:
            alerting_overrides = yaml.safe_load(f) or {}

    # Step 3: Load source-specific config
    config_paths = [
        _PROJECT_ROOT / "configs" / f"{source}.yaml",
        _PROJECT_ROOT / "configs" / f"{source}.yml",
        _PROJECT_ROOT / "config" / f"{source}.yaml",
        _PROJECT_ROOT / "config" / f"{source}.yml",
        Path(f"./{source}.yaml"),
        Path(f"./{source}.yml"),
    ]

    source_config = None
    for path in config_paths:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                source_config = yaml.safe_load(f) or {}
            break

    if source_config is None:
        raise FileNotFoundError(f"Configuration file not found for source: {source}")

    # Step 4: Merge configs (defaults <- alerting <- source)
    # Start with defaults
    config = defaults.copy()

    # Merge alerting overrides
    if alerting_overrides:
        config = merge_configs(config, alerting_overrides)

    # Merge source-specific config (highest priority)
    config = merge_configs(config, source_config)

    # Step 5: Expand environment variables and secrets
    config = expand_env_vars(config)

    # Step 6: Add source name if not present
    if "source" not in config:
        config["source"] = {"name": source}
    elif "name" not in config["source"]:
        config["source"]["name"] = source

    # Step 6.5: Normalize legacy keys onto the canonical v2 contract
    # (and mirror canonical keys back onto legacy readers). See config_schema.py.
    from shared.config_schema import (
        check_v2_legacy_declarations,
        get_config_version,
        normalize_config,
        validate_config_v2,
    )

    config_version = get_config_version(config)
    config, deprecations = normalize_config(config, source)

    # Step 7: Validate configuration
    is_valid, errors = validate_config(config, source)

    if config_version >= 2:
        # Strict contract: legacy declarations in the source YAML are errors.
        errors.extend(check_v2_legacy_declarations(source_config))
        errors.extend(validate_config_v2(config, source))
        is_valid = len(errors) == 0
    elif deprecations:
        # Legacy configs keep working; surface what migration to v2 will require.
        for warning in deprecations:
            logger.warning(f"[{source}] config deprecation: {warning}")

    if not is_valid:
        error_msg = f"Configuration validation failed for {source}:\n  " + "\n  ".join(
            errors
        )
        raise ValueError(error_msg)

    # Step 8: Cache the config
    with _cache_lock:
        _config_cache[source] = config.copy()

    return config


def clear_config_cache(source: str = None):
    """
    Clear configuration cache.

    Args:
        source: If provided, clear only that source. If None, clear all.

    Example:
        >>> clear_config_cache('facebook')  # Clear facebook config
        >>> clear_config_cache()  # Clear all cached configs
    """
    with _cache_lock:
        if source:
            if source in _config_cache:
                del _config_cache[source]
        else:
            _config_cache.clear()


def get_cache_info() -> Dict[str, Any]:
    """
    Get information about cached configurations.

    Returns:
        Dictionary with cache statistics

    Example:
        >>> info = get_cache_info()
        >>> print(f"Cached sources: {info['sources']}")
    """
    with _cache_lock:
        return {"sources": list(_config_cache.keys()), "count": len(_config_cache)}


def merge_configs(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two configuration dictionaries recursively.

    Args:
        base: Base configuration
        override: Override configuration (higher priority)

    Returns:
        Merged configuration
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_configs(result[key], value)
        else:
            result[key] = value

    return result


def validate_config(config: Dict[str, Any], source_name: str) -> tuple[bool, list[str]]:
    """
    Validate configuration structure and required fields.

    Args:
        config: Configuration dictionary to validate
        source_name: Source name (for error messages)

    Returns:
        Tuple of (is_valid, error_messages)

    Example:
        >>> config = load_config('facebook')
        >>> is_valid, errors = validate_config(config, 'facebook')
        >>> if not is_valid:
        ...     print(errors)
    """
    errors = []

    # Required top-level sections
    required_sections = ["source", "pipeline"]

    for section in required_sections:
        if section not in config:
            errors.append(f"Missing required section: '{section}'")
        elif not isinstance(config[section], dict):
            errors.append(
                f"Section '{section}' must be a dictionary, got {type(config[section]).__name__}"
            )

    # Validate source section
    if "source" in config and isinstance(config["source"], dict):
        source = config["source"]

        # Required source fields
        required_source_fields = ["name", "type"]
        for field in required_source_fields:
            if field not in source:
                errors.append(f"source.{field} is required")

        # Validate source type
        if "type" in source:
            valid_types = [
                "facebook",
                "wordpress",
                "mysql",
                "postgres",
                "generic",
                "rest_api",
                "mailchimp",
                "dcm",
                "limesurvey",
                "transform",
                "bigquery",
                "instagram",
                "threads",
                "screaming_frog",
                "onetrust",
                "geocoder",
                "profile_database",
            ]
            if source["type"] not in valid_types:
                errors.append(
                    f"source.type must be one of {valid_types}, got '{source['type']}'"
                )

    # Validate pipeline section
    if "pipeline" in config and isinstance(config["pipeline"], dict):
        pipeline = config["pipeline"]

        # Required pipeline fields
        required_pipeline_fields = ["staging_dataset", "production_dataset"]
        for field in required_pipeline_fields:
            if field not in pipeline:
                errors.append(f"pipeline.{field} is required")

        # Validate dataset names (must be valid BigQuery dataset IDs)
        for dataset_field in ["staging_dataset", "production_dataset", "test_dataset"]:
            if dataset_field in pipeline:
                dataset_name = pipeline[dataset_field]
                if not isinstance(dataset_name, str):
                    errors.append(
                        f"pipeline.{dataset_field} must be a string, got {type(dataset_name).__name__}"
                    )
                elif not dataset_name:
                    errors.append(f"pipeline.{dataset_field} cannot be empty")
                elif not _is_valid_dataset_name(dataset_name):
                    errors.append(
                        f"pipeline.{dataset_field} '{dataset_name}' is not a valid BigQuery dataset name"
                    )

        # Validate numeric fields
        numeric_fields = {"parallel_workers": (1, 100), "batch_size": (1, 100000)}
        for field, (min_val, max_val) in numeric_fields.items():
            if field in pipeline:
                value = pipeline[field]
                if not isinstance(value, int):
                    errors.append(
                        f"pipeline.{field} must be an integer, got {type(value).__name__}"
                    )
                elif value < min_val or value > max_val:
                    errors.append(
                        f"pipeline.{field} must be between {min_val} and {max_val}, got {value}"
                    )

    # Validate resources section (if present)
    if "resources" in config:
        if not isinstance(config["resources"], dict):
            errors.append(
                f"resources must be a dictionary, got {type(config['resources']).__name__}"
            )
        else:
            for table_name, table_config in config["resources"].items():
                if not isinstance(table_config, dict):
                    errors.append(
                        f"resources.{table_name} must be a dictionary, got {type(table_config).__name__}"
                    )

                # Validate schema if present
                if "schema" in table_config:
                    schema = table_config["schema"]
                    if not isinstance(schema, dict):
                        errors.append(
                            f"resources.{table_name}.schema must be a dictionary, got {type(schema).__name__}"
                        )
                    else:
                        # Reject legacy schema types (must use canonical BQ types)
                        _LEGACY_TYPES = {"INTEGER": "INT64", "FLOAT": "FLOAT64"}
                        for col_name, col_type in schema.items():
                            if isinstance(col_type, str) and col_type in _LEGACY_TYPES:
                                errors.append(
                                    f"resources.{table_name}.schema.{col_name}: "
                                    f"use '{_LEGACY_TYPES[col_type]}' instead of '{col_type}'"
                                )

                # Reject legacy incremental keys at resource root
                _LEGACY_INC_KEYS = [
                    "incremental_strategy",
                    "incremental_date_fields",
                    "incremental_field",
                    "incremental_lookback_days",
                ]
                for legacy_key in _LEGACY_INC_KEYS:
                    if legacy_key in table_config:
                        errors.append(
                            f"resources.{table_name}.{legacy_key} is deprecated; "
                            f"use nested 'incremental:' block instead"
                        )

    # Reject legacy table_groups key (must be 'groups')
    if "table_groups" in config:
        errors.append("'table_groups' is deprecated; rename to 'groups'")

    # Validate notifications section (if present)
    if "notifications" in config and isinstance(config["notifications"], dict):
        notif = config["notifications"]

        if "enabled" in notif and not isinstance(notif["enabled"], bool):
            errors.append(
                f"notifications.enabled must be a boolean, got {type(notif['enabled']).__name__}"
            )

        # Validate channels if present
        if "channels" in notif and isinstance(notif["channels"], dict):
            for channel_name, channel_config in notif["channels"].items():
                if not isinstance(channel_config, dict):
                    errors.append(
                        f"notifications.channels.{channel_name} must be a dictionary"
                    )

    is_valid = len(errors) == 0
    return is_valid, errors


def _is_valid_dataset_name(name: str) -> bool:
    """
    Check if a string is a valid BigQuery dataset name.

    BigQuery dataset names:
    - Must contain only letters, numbers, and underscores
    - Must start with a letter or underscore
    - Max 1024 characters

    Args:
        name: Dataset name to validate

    Returns:
        True if valid, False otherwise
    """
    if not name or len(name) > 1024:
        return False

    # Must start with letter or underscore
    if not (name[0].isalpha() or name[0] == "_"):
        return False

    # Must contain only letters, numbers, underscores
    for char in name:
        if not (char.isalnum() or char == "_"):
            return False

    return True
