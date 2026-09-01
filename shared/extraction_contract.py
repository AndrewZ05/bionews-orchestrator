"""
Post-extraction contract validation (backlog B2).

After a plugin's run_pipeline returns and BEFORE the orchestrator uploads to GCS,
validate the `table_files` it handed back. This catches a class of silent load
bugs at the seam -- with a clear, immediate error -- instead of letting a bad
entry surface deep in GCS upload / external-table / merge (or, worse, target the
wrong production table).

The contract a standard extractor must satisfy:
  table_files: { LOGICAL resource name -> local parquet path }
    * the key is a LOGICAL resource name present in config.resources -- NOT an
      affixed/physical table name (the orchestrator applies schema_prefix/suffix
      itself; an affixed key would miss the config lookup and mis-target).
    * the value is either:
        - a non-empty string path to a file that EXISTS locally, OR
        - None  -> the extractor is signalling "skip this table" (e.g. empty
          cache). None entries are filtered out, not errors -- this is an
          established convention (see instagram_extractor).

validate_table_files() returns the cleaned dict (None entries removed) and a
list of violations. The caller decides fatal-vs-warn; orchestrate.py fails fast
on any violation, since each is a real bug that would otherwise corrupt or
misdirect a load.
"""

import os
from typing import Any, Dict, List, Optional, Tuple


def validate_table_files(
    table_files: Dict[str, Any],
    config: Dict[str, Any],
    *,
    check_files_exist: bool = True,
) -> Tuple[Dict[str, str], List[str]]:
    """
    Validate a plugin's table_files against the standard contract.

    Returns (clean_table_files, violations):
      - clean_table_files: only the valid, non-None entries (safe to upload).
      - violations: human-readable strings; empty == fully valid.

    Never raises -- the caller decides how to act on violations.
    """
    violations: List[str] = []
    clean: Dict[str, str] = {}

    if table_files is None:
        return clean, violations
    if not isinstance(table_files, dict):
        return clean, [f"table_files must be a dict, got {type(table_files).__name__}"]

    resources = config.get("resources", {}) or {}
    # Physical/affixed names a key must NOT be. The logical key resolves to this
    # via resources[key].table_name (or {source}_{key}); a key that equals a
    # physical name means the plugin pre-affixed/renamed -- the contract says it
    # must hand back the logical resource name.
    source_name = (config.get("source", {}) or {}).get("name", "")
    physical_names = set()
    for logical, rcfg in resources.items():
        if isinstance(rcfg, dict):
            physical_names.add(rcfg.get("table_name", f"{source_name}_{logical}"))

    for key, path in table_files.items():
        # None -> documented "skip this table" signal; drop it silently.
        if path is None:
            continue

        # Key must be a logical resource name.
        if key not in resources:
            # Distinguish the common mistake (handed back a physical/affixed
            # name) from an outright unknown table, for a clearer message.
            if key in physical_names:
                violations.append(
                    f"table_files key '{key}' is a physical/affixed table name; "
                    f"return the LOGICAL resource name instead (the orchestrator "
                    f"applies affixes)"
                )
            else:
                violations.append(
                    f"table_files key '{key}' is not in config.resources "
                    f"(unknown table)"
                )
            continue

        # Value must be a non-empty string.
        if not isinstance(path, str) or not path.strip():
            violations.append(
                f"table_files['{key}'] must be a non-empty string path, got "
                f"{path!r}"
            )
            continue

        # File must exist locally (so GCS upload won't hand off a missing file --
        # the exact failure class from the Batch durable-copy bug).
        if check_files_exist and not os.path.exists(path):
            violations.append(
                f"table_files['{key}'] points to a path that does not exist: {path}"
            )
            continue

        clean[key] = path

    return clean, violations
