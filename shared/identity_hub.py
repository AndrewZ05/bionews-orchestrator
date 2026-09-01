#!/usr/bin/env python3
"""
Identity Hub v2 — Cross-Platform Identity Graph Builder

Creates a central bn_id linking individual-level identifiers across all
pipeline data stores. Queries BN_Acceptor.acceptor_events directly for
localStorage identity keys (bnfpvid, mc_euid, etc.) and bridges them to
cookies, emails, and platform-specific IDs.

Algorithm:
    1. Extract localStorage identifiers from acceptor_events (bnfpvid, mc_euid, etc.)
    2. Import existing cookie graph edges (visitor_id <-> cookie identifiers)
    3. Normalize cookies (strip GA version prefixes, dedup ga<->client_id)
    4. Run enabled connectors to produce cross-platform edges:
       - Email bridge (raw emails from MC, LS, WP)
       - UTM crosswalk (MC click URLs -> page_load_sessions via client_id)
       - IP + Device + Time (probabilistic MC/WP activity <-> sessions)
       - LimeSurvey session bridge (LS cookie <-> survey completion)
    5. Apply quality filters (cardinality thresholds)
    6. Run PriorityUnionFind on edges with confidence >= stitch_threshold
    7. Generate stable bn_id from canonical root (BN_ prefix + SHA256)
    8. Apply persistence (merge/split detection with audit logging)
    9. Write hub, xref, neighbors, persistence, merge_log, metrics tables

Output tables (in identity_hub_data dataset):
    - bn_id_hub              All edges with bn_id, confidence, metadata
    - bn_id_xref             identifier_key -> bn_id mapping
    - bn_id_neighbors        Cross-product neighbor pairs per bn_id
    - bn_id_persistence      Stable bn_id registry + merge redirects
    - bn_id_merge_log        Audit log of merge/split events
    - bn_id_metrics          Run statistics
    - cookie_normalization_log   Normalization audit trail

Usage:
    python shared/identity_hub.py --rebuild
    python shared/identity_hub.py --refresh --lookback 7
    python shared/identity_hub.py --rebuild --dry-run
    python shared/identity_hub.py --rebuild --connectors localstorage email_bridge

See configs/identity_hub.yaml for full configuration.
"""

import argparse
import base64
import hashlib
import io
import math
import os
import subprocess
import sys
import time
import uuid

# Suppress gRPC ALTS warning when not running on GCP VMs
os.environ.setdefault("GRPC_VERBOSITY", "ERROR")
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Set, Tuple, Optional

# Ensure project root is on sys.path for direct execution
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Fix Windows console encoding (skip when stdout is redirected, e.g. StringIO)
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Set credentials before importing google.cloud (cross-platform)
from shared.bigquery_client import setup_gcp_credentials

setup_gcp_credentials()

import yaml
from google.cloud import bigquery

from shared.cookie_normalizer import CookieNormalizer, normalize_ga_value


# Identifier types dropped from the graph (not person-level)
# Note: fbc, gcl_au, gcl_aw were removed (v3.1+ — they are now output_types
# per configs/identity_hub.yaml person_anchoring.output_types).
DROPPED_TYPES = {"fbclid", "limesurvey", "eoi", "gads"}


# =============================================================================
# CONFIGURATION
# =============================================================================


def load_config() -> Dict[str, Any]:
    """Load identity_hub config from identity_hub.yaml.

    Project resolution order:
      1. GCP_PROJECT_ID env var (set by orchestrator --env or manually)
      2. identity_hub.project in YAML
      3. source.connection.project in YAML
      4. Default: bi-data-391216 (prod)
    """
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "configs", "identity_hub.yaml"
    )
    config_path = os.path.normpath(config_path)

    env_project = os.environ.get("GCP_PROJECT_ID")

    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            full_config = yaml.safe_load(f)
            hub_config = full_config.get("identity_hub", {})
            # Env var takes precedence over YAML hardcoded project
            if env_project:
                hub_config["project"] = env_project
            elif "project" not in hub_config:
                hub_config["project"] = (
                    full_config.get("source", {}).get("connection", {}).get("project")
                    or "bi-data-391216"
                )
            return hub_config
    return {"project": env_project or "bi-data-391216"}


# =============================================================================
# EDGE DATA STRUCTURE
# =============================================================================


@dataclass(slots=True)
class HubEdge:
    """An edge in the identity hub graph."""

    identifier_a: str  # e.g., a bnfpvid value
    identifier_a_type: str  # e.g., 'bnfpvid'
    identifier_b: str  # e.g., raw email
    identifier_b_type: str  # e.g., 'email'
    source_system: (
        str  # 'acceptor', 'page_load', 'mailchimp', 'limesurvey', 'wordpress'
    )
    link_type: str  # 'deterministic', 'probabilistic'
    match_rule: str  # 'LOCALSTORAGE_COOCCURRENCE', 'EMAIL_EXACT', etc.
    confidence: float  # 0.0–1.0 observation strength (co-occurrence evidence).
    # Preserved in the hub for audit; never mutated by gates.
    first_seen: str  # ISO timestamp
    last_seen: str  # ISO timestamp
    base_confidence: float = -1.0  # Original connector confidence (-1 = not yet set)
    # v3.4: identity penalty applied by gates (conflict, shared-workstation override).
    # Cap on the identity-stitching confidence without overwriting observation strength.
    # Union-Find uses effective = confidence * obs_to_id_ratio * decay, then min(...,identity_cap).
    identity_cap: float = 1.0


# --- Fan-out filter semantics (shared by the BigQuery rebuild path and the Python
# incremental path; the unit tests assert both paths agree) ---
#
# A node whose degree exceeds its per-type threshold is "over-linked". Until
# 2026-08-23 the filter deleted EVERY edge of an over-linked node. A full
# rebuild counts a node's whole source window at once (Mailchimp click/IP
# bridges, UTM crosswalks, browser captures), so the most engaged subscribers
# blew past email's threshold of 10 and lost even their deterministic
# email<->mc_euid<->subscriber_hash edges: 21,087 emails (19,551 currently
# subscribed) vanished from the graph on the 2026-08-21 rebuild, and the
# profile rebuild that consumed it could not restore app fields or derive
# site_domain for them. Incremental runs only count the lookback window, so
# the same people never tripped it -- the two paths disagreed by construction.
#
# Now: an over-linked node loses its NON-anchor edges and keeps its anchor
# edges (deterministic edges between two person-type identifiers), unless its
# anchor degree is itself over threshold -- that is a genuinely shared
# identifier (a kiosk mc_euid carrying 15 emails) and is removed whole, as
# before. The explosion guard is intact: what survives is at most
# <threshold> deterministic person-level edges per over-linked node.

WP_USER_EMAIL_RULE = "WP_USER_EMAIL"


def is_anchor_edge(a_type: str, b_type: str, link_type: str, person_types) -> bool:
    """Deterministic edge between two person-type identifiers."""
    return (
        link_type == "deterministic"
        and a_type in person_types
        and b_type in person_types
    )


def _counts_toward_degree(node_type: str, match_rule) -> bool:
    # WordPress can legitimately link one email to many wp_user_id values
    # across sites; those edges never count as email fan-out.
    return not (node_type == "email" and (match_rule or "") == WP_USER_EMAIL_RULE)


def fanout_decisions(edges, threshold_for_type, person_types):
    """Classify over-linked nodes.

    Returns (remove_all, trim): node keys whose every edge goes, and node keys
    that keep only their anchor edges. `edges` yields objects with
    identifier_a/identifier_a_type/identifier_b/identifier_b_type/link_type/
    match_rule; `threshold_for_type(id_type) -> int`.
    """
    from collections import defaultdict

    degree = defaultdict(int)
    anchor_degree = defaultdict(int)
    for e in edges:
        anchor = is_anchor_edge(e.identifier_a_type, e.identifier_b_type, e.link_type, person_types)
        for node_type, key in (
            (e.identifier_a_type, f"{e.identifier_a_type}:{e.identifier_a}"),
            (e.identifier_b_type, f"{e.identifier_b_type}:{e.identifier_b}"),
        ):
            if _counts_toward_degree(node_type, e.match_rule):
                degree[key] += 1
                if anchor:
                    anchor_degree[key] += 1
    remove_all, trim = set(), set()
    for node, d in degree.items():
        thr = threshold_for_type(node.split(":", 1)[0])
        if d > thr:
            (remove_all if anchor_degree[node] > thr else trim).add(node)
    return remove_all, trim


def fanout_edge_removed(e, remove_all, trim, person_types) -> bool:
    """Does the per-type fan-out filter drop this edge?"""
    ka = f"{e.identifier_a_type}:{e.identifier_a}"
    kb = f"{e.identifier_b_type}:{e.identifier_b}"
    if ka in remove_all or kb in remove_all:
        return True
    if ka in trim or kb in trim:
        return not is_anchor_edge(e.identifier_a_type, e.identifier_b_type, e.link_type, person_types)
    return False


def _sql_in_list(values) -> str:
    return ", ".join(f"'{v}'" for v in sorted(values))


def build_fanout_nodes_sql(staging_table: str, nodes_table: str, threshold_table: str, person_types) -> str:
    """One row per over-linked node: degree, anchor_degree, threshold, remove_all."""
    pt = _sql_in_list(person_types)
    anchor = (
        "(link_type = 'deterministic' "
        f"AND identifier_a_type IN ({pt}) AND identifier_b_type IN ({pt}))"
    )
    not_wp = (
        "NOT (SPLIT(node_key, ':')[OFFSET(0)] = 'email' "
        f"AND IFNULL(match_rule, '') = '{WP_USER_EMAIL_RULE}')"
    )
    return f"""
        CREATE OR REPLACE TABLE `{nodes_table}` AS
        WITH ends AS (
          SELECT edge_key_a AS node_key, match_rule, {anchor} AS is_anchor
          FROM `{staging_table}`
          UNION ALL
          SELECT edge_key_b AS node_key, match_rule, {anchor} AS is_anchor
          FROM `{staging_table}`
        ),
        deg AS (
          SELECT node_key, SPLIT(node_key, ':')[OFFSET(0)] AS id_type,
                 COUNTIF({not_wp}) AS degree,
                 COUNTIF(is_anchor AND {not_wp}) AS anchor_degree
          FROM ends
          GROUP BY node_key, id_type
        )
        SELECT d.node_key, d.id_type, d.degree, d.anchor_degree, t.threshold,
               (d.anchor_degree > t.threshold) AS remove_all
        FROM deg d
        JOIN `{threshold_table}` t ON d.id_type = t.id_type
        WHERE d.degree > t.threshold
        """


def build_fanout_delete_sql(staging_table: str, nodes_table: str, person_types) -> str:
    """Delete non-anchor edges of trimmed nodes and every edge of remove_all nodes.

    Two EXISTS clauses keyed on an equality each: BigQuery refuses a correlated
    EXISTS whose only join condition is an OR of equalities.
    """
    pt = _sql_in_list(person_types)
    not_anchor = (
        "NOT (e.link_type = 'deterministic' "
        f"AND e.identifier_a_type IN ({pt}) AND e.identifier_b_type IN ({pt}))"
    )
    return f"""
        DELETE FROM `{staging_table}` e
        WHERE EXISTS (
          SELECT 1 FROM `{nodes_table}` f
          WHERE f.node_key = e.edge_key_a AND (f.remove_all OR {not_anchor})
        )
        OR EXISTS (
          SELECT 1 FROM `{nodes_table}` f
          WHERE f.node_key = e.edge_key_b AND (f.remove_all OR {not_anchor})
        )
        """


def build_fanout_preserved_sql(staging_table: str, nodes_table: str, person_types) -> str:
    """Count anchor edges that survive only because of trimming (run BEFORE delete)."""
    pt = _sql_in_list(person_types)
    return f"""
        SELECT COUNT(*) AS cnt
        FROM `{staging_table}` e
        WHERE e.link_type = 'deterministic'
          AND e.identifier_a_type IN ({pt}) AND e.identifier_b_type IN ({pt})
          AND (EXISTS (SELECT 1 FROM `{nodes_table}` f WHERE f.node_key = e.edge_key_a AND NOT f.remove_all)
               OR EXISTS (SELECT 1 FROM `{nodes_table}` f WHERE f.node_key = e.edge_key_b AND NOT f.remove_all))
          AND NOT EXISTS (SELECT 1 FROM `{nodes_table}` f WHERE f.node_key = e.edge_key_a AND f.remove_all)
          AND NOT EXISTS (SELECT 1 FROM `{nodes_table}` f WHERE f.node_key = e.edge_key_b AND f.remove_all)
        """
# =============================================================================
# PRIORITY UNION-FIND
# =============================================================================


class PriorityUnionFind:
    """
    Union-Find with source priority for canonical root selection.
    Prefers roots with best (lowest) source_priority.
    """

    def __init__(self, node_priority: Dict[str, int]):
        self.parent: Dict[str, str] = {}
        self.rank: Dict[str, int] = {}
        self.best_priority: Dict[str, int] = {}
        self.node_priority = node_priority

    def _ensure(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            self.best_priority[x] = self.node_priority.get(x, 99)

    def find(self, x: str) -> str:
        self._ensure(x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            next_x = self.parent[x]
            self.parent[x] = root
            x = next_x
        return root

    def union(self, x: str, y: str) -> bool:
        root_x = self.find(x)
        root_y = self.find(y)
        if root_x == root_y:
            return False

        pri_x = self.best_priority.get(root_x, 99)
        pri_y = self.best_priority.get(root_y, 99)

        # Higher priority (lower number) becomes root
        if pri_x > pri_y:
            root_x, root_y = root_y, root_x
        elif pri_x == pri_y and root_x > root_y:
            root_x, root_y = root_y, root_x

        self.parent[root_y] = root_x
        self.best_priority[root_x] = min(
            self.best_priority.get(root_x, 99), self.best_priority.get(root_y, 99)
        )
        self.rank[root_x] = max(self.rank.get(root_x, 0), self.rank.get(root_y, 0) + 1)
        return True

    def get_components(self) -> Dict[str, List[str]]:
        """Return {root: [members]}."""
        components = defaultdict(list)
        for node in self.parent:
            root = self.find(node)
            components[root].append(node)
        return dict(components)


# =============================================================================
# IDENTITY HUB UTILITIES
# =============================================================================


def _strip_plus_tag(email: str, allowed_domains: Set[str]) -> str:
    """RFC 5233 subaddressing: 'name+tag@host' delivers to 'name@host'.

    Only applied for domains in `allowed_domains`. For all other domains
    the tagged form is treated as a distinct address, since plus-addressing
    is not universally honored by mail systems.
    """
    if "+" not in email:
        return email
    local, _, domain = email.partition("@")
    if not domain or domain.lower() not in allowed_domains:
        return email
    base_local = local.split("+", 1)[0]
    return f"{base_local}@{domain.lower()}"


def _strip_dots_in_local(email: str, allowed_domains: Set[str]) -> str:
    """Remove dots from email local part for Gmail-like domains.

    Gmail treats 'john.smith@gmail.com' and 'johnsmith@gmail.com' as the same mailbox.
    Only applied for domains in `allowed_domains`. For other domains,
    dots may be semantically meaningful and should not be stripped.
    """
    if "." not in email:
        return email
    local, _, domain = email.partition("@")
    if not domain or domain.lower() not in allowed_domains:
        return email
    local_no_dots = local.replace(".", "")
    return f"{local_no_dots}@{domain.lower()}"


def compound_device_ip_edge_confidence(
    base_confidence: float,
    observations: int,
    min_observations: int = 2,
    rule_cap: float = 0.90,
) -> float:
    """Confidence for a COMPOUND_DEVICE_IP_TIME pair after observation aggregation.

    Pairs are pre-aggregated in SQL (one row per bnfpvid pair with an
    observation count). Apply the same log2 boost used by aggregate_confidence
    / BQ aggregation so that:
      - obs < min_observations → base confidence (audit-only after ratio)
      - obs >= min_observations → boosted toward rule_cap (stitchable after ratio)
    """
    if observations < 1:
        return 0.0
    if observations < min_observations:
        return float(base_confidence)
    return min(float(rule_cap), float(base_confidence) * math.log2(observations + 1))


# =============================================================================
# IDENTITY HUB BUILDER
# =============================================================================


class IdentityHubBuilder:
    """Builds the cross-platform identity hub graph."""

    def __init__(
        self,
        client: bigquery.Client,
        config: Dict[str, Any],
        start_date: str = None,
        end_date: str = None,
        incremental: bool = False,
        connector_filter: List[str] = None,
        project_override: str = None,
        force_overwrite: bool = False,
        output_dataset_override: str = None,
        graph_start_override: str = None,
    ):
        self.client = client
        self.config = config
        self.end_date = end_date
        self.incremental = incremental
        self.connector_filter = connector_filter

        # Run-level metadata (v3.3 — traceability + rollback)
        self.run_id = str(uuid.uuid4())
        self.build_mode = "incremental" if incremental else "full"
        self.config_version = self._compute_config_version()
        self.git_sha = self._compute_git_sha()
        print(
            f"  Run metadata: run_id={self.run_id[:8]}... mode={self.build_mode} "
            f"config_version={self.config_version[:8]}... git_sha={self.git_sha}"
        )
        # Safeguard: refuse to write output tables if new row count is dramatically
        # smaller than existing (protects against partial runs overwriting production).
        # Set to True to bypass (required when running with --connectors or after
        # schema changes that legitimately shrink the output).
        self.force_overwrite = force_overwrite
        # Threshold: abort if new size < 50% of existing
        self.shrink_abort_threshold = config.get("shrink_abort_threshold", 0.5)
        # Test isolation: when running against a test dataset, always bypass
        # the shrink safeguard (test tables are expected to be small/fresh).
        self._is_test_mode = bool(output_dataset_override)
        if self._is_test_mode:
            self.force_overwrite = True

        # Apply graph_start_date as floor for start_date.
        # graph_start_override (from --sample-days) takes precedence so tests
        # can process narrow date windows below the production floor.
        graph_start = graph_start_override or config.get("graph_start_date")
        if graph_start and (not start_date or start_date < graph_start):
            self.start_date = graph_start
        else:
            self.start_date = start_date

        # Project and datasets — orchestrator's project_override takes precedence
        self.project = project_override or config.get("project", "bi-data-391216")
        # Test isolation: output_dataset_override redirects writes to a test
        # dataset (e.g. identity_hub_data_test) without touching production.
        self.output_dataset = output_dataset_override or config.get(
            "output_dataset", "identity_hub_data"
        )

        # Staging dataset: temp tables during pipeline run
        if output_dataset_override:
            self.staging_dataset = output_dataset_override.replace("_data", "_staging")
        else:
            self.staging_dataset = config.get("staging_dataset", "identity_hub_staging")

        # Ensure output dataset exists
        dataset_ref = bigquery.DatasetReference(self.project, self.output_dataset)
        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            print(f"  Created dataset {self.project}.{self.output_dataset}")

        # Ensure staging dataset exists
        staging_ref = bigquery.DatasetReference(self.project, self.staging_dataset)
        try:
            self.client.get_dataset(staging_ref)
        except Exception:
            staging_ds = bigquery.Dataset(staging_ref)
            staging_ds.location = "US"
            self.client.create_dataset(staging_ds)
            print(f"  Created dataset {self.project}.{self.staging_dataset}")

        # Thresholds
        self.stitch_threshold = config.get("stitch_threshold", 0.80)
        self.max_visitors_per_identifier = config.get(
            "max_visitors_per_identifier", 125
        )
        self.max_identifiers_per_visitor = config.get(
            "max_identifiers_per_visitor", 125
        )
        self.min_identifier_length = config.get("min_identifier_length", 8)
        self.invalid_values = set(
            config.get(
                "invalid_values",
                ["null", "undefined", "false", "", "NA", "REDACTED", "$EMAIL_ADDRESS"],
            )
        )
        self.source_priorities = config.get("source_priorities", {})

        # Cookie normalization
        norm_config = config.get("cookie_normalization", {})
        self.normalizer = CookieNormalizer(
            merge_ga_client_id=norm_config.get("merge_ga_client_id", True)
        )
        self.normalize_cookies = norm_config.get("enabled", True)

        # Edge tiering
        tier_config = config.get("edge_tiers", {})
        self.person_types: Set[str] = set(
            tier_config.get(
                "person_types",
                [
                    "email",
                    "subscriber_hash",
                    "wp_user_id",
                    "participant_id",
                    "aim_dgid",
                    "bnfpvid",
                    "mc_euid",
                    "agile_crm_guid",
                    "kla_id",
                    "dmd_tag",
                    "dmd_vid",
                    "ref_pvid",
                ],
            )
        )
        self.browser_expiry_days: int = tier_config.get("browser_expiry_days", 365)

        # Person anchoring
        anchor_config = config.get("person_anchoring", {})
        self.person_anchoring_enabled = anchor_config.get("enabled", False)
        self.tier1_types: Set[str] = set(anchor_config.get("tier1_types", ["email"]))
        self.tier2_types: Set[str] = set(anchor_config.get("tier2_types", ["bnfpvid"]))
        self.anchor_types: Set[str] = self.tier1_types | self.tier2_types
        self.output_types: Set[str] = set(
            anchor_config.get("output_types", list(self.person_types))
        )
        self._bn_id_tiers: Dict[str, str] = {}

        # Edge aging (confidence decay)
        aging_config = config.get("edge_aging", {})
        self.edge_aging_enabled = aging_config.get("enabled", False)
        self.decay_schedule = aging_config.get(
            "decay_schedule",
            [
                {"max_age_days": 30, "weight": 1.0},
                {"max_age_days": 90, "weight": 0.8},
                {"max_age_days": 180, "weight": 0.6},
                {"max_age_days": 365, "weight": 0.4},
            ],
        )
        # Sort by max_age_days ascending for lookup
        self.decay_schedule.sort(key=lambda x: x["max_age_days"])
        self.type_influence_days: Dict[str, int] = aging_config.get(
            "type_influence_days", {}
        )

        # Per-type fanout thresholds — copy to avoid mutating shared config
        self.fanout_thresholds: Dict[str, int] = dict(
            config.get("fanout_thresholds", {})
        )
        self.default_fanout_threshold: int = self.fanout_thresholds.pop(
            "default", self.max_identifiers_per_visitor
        )

        # v3.3: Per-rule overrides
        # Format: {"identifier_type.match_rule": threshold_or_cap}
        self.fanout_thresholds_by_rule: Dict[str, int] = dict(
            config.get("fanout_thresholds_by_rule", {})
        )
        self.confidence_caps_by_rule: Dict[str, float] = dict(
            config.get("confidence_caps_by_rule", {})
        )

        # v3.4: observation-to-identity ratio separates "observed together"
        # from "same person" confidence.
        # Stored on the hub as edge.confidence (observation).
        # Union-Find uses confidence * obs_to_id_ratio (identity).
        self.observation_to_identity_ratio: Dict[str, float] = dict(
            config.get("observation_to_identity_ratio", {})
        )

        # Cluster size cap
        self.max_cluster_size: int = config.get("max_cluster_size", 200)

        # Blacklisted identifiers (known-bad values that cause false merges)
        self.blacklisted_identifiers: Set[str] = set(
            config.get("blacklisted_identifiers", [])
        )

        # Graph start date
        self.graph_start_date: Optional[str] = config.get("graph_start_date")

        # Bot detection
        bot_config = config.get("bot_detection", {})
        self.bot_detection_enabled = bot_config.get("enabled", False)
        self.bot_ua_patterns = bot_config.get("bot_ua_patterns", [])
        self.bot_headless_detection = bot_config.get("headless_detection", True)
        self.bot_cluster_size_threshold = bot_config.get(
            "bot_cluster_size_threshold", 50
        )
        self.bot_confirmed_action = bot_config.get("confirmed_bot_action", "remove")
        self._bot_bnfpvids: Set[str] = set()

        # Shared workstation detection — split flag from removal
        ws_config = config.get("shared_workstation", {})
        # Flag threshold: lower, flags more clusters for downstream awareness
        self.shared_ws_flag_min = ws_config.get(
            "flag_min_euids_per_bnfpvid", ws_config.get("min_euids_per_bnfpvid", 2)
        )
        # Removal threshold: higher, conservative edge deletion
        self.shared_ws_remove_min = ws_config.get(
            "remove_min_euids_per_bnfpvid", ws_config.get("min_euids_per_bnfpvid", 3)
        )
        # Legacy compat
        self.shared_ws_min_euids = self.shared_ws_remove_min

        # Source profile classification
        sp_config = config.get("source_profile", {})
        self.browser_sources = set(
            sp_config.get("browser_sources", ["acceptor", "page_load"])
        )
        self.ga4_sources = set(sp_config.get("ga4_sources", ["ga4"]))
        self.offline_sources = set(
            sp_config.get(
                "offline_sources",
                ["mailchimp", "npi_registry", "limesurvey", "wordpress"],
            )
        )

        # Connectors config
        self.connectors_config = config.get("connectors", {})

        # Cross-anchoring config (v3.0)
        xa_config = config.get("cross_anchoring", {})
        self.xa_localstorage_client_id = xa_config.get("localstorage_client_id", False)
        self.xa_aim_payload_client_id = xa_config.get("aim_payload_client_id", False)
        self.xa_ga4_extra_identifiers = xa_config.get("ga4_extra_identifiers", False)
        self.xa_limesurvey_session_client_id = xa_config.get(
            "limesurvey_session_client_id", False
        )

        # Plus-tag normalization config (v3.4)
        ptb_config = config.get("identity_hub", {}).get("plus_tag_bridge", {})
        self._plus_tag_bridge_enabled = ptb_config.get("enabled", False)
        # Load allowed_domains as a lowercase set for fast lookups
        allowed_domains_list = ptb_config.get("allowed_domains", [])
        self._plus_tag_allowed_domains: Set[str] = (
            set(d.lower() for d in allowed_domains_list)
            if self._plus_tag_bridge_enabled
            else set()
        )

        # Dot normalization config (v3.4)
        dnb_config = config.get("identity_hub", {}).get("dot_normalization_bridge", {})
        self._dot_normalization_bridge_enabled = dnb_config.get("enabled", False)
        # Load allowed_domains as a lowercase set for fast lookups
        allowed_domains_list_dots = dnb_config.get("allowed_domains", [])
        self._dot_normalization_allowed_domains: Set[str] = (
            set(d.lower() for d in allowed_domains_list_dots)
            if self._dot_normalization_bridge_enabled
            else set()
        )

        # Output table names
        tables = config.get("output_tables", {})
        self.hub_table = (
            f"{self.project}.{self.output_dataset}.{tables.get('hub', 'bn_id_hub')}"
        )
        self.xref_table = (
            f"{self.project}.{self.output_dataset}.{tables.get('xref', 'bn_id_xref')}"
        )
        self.neighbors_table = f"{self.project}.{self.output_dataset}.{tables.get('neighbors', 'bn_id_neighbors')}"
        self.persistence_table = f"{self.project}.{self.output_dataset}.{tables.get('persistence', 'bn_id_persistence')}"
        self.merge_log_table = f"{self.project}.{self.output_dataset}.{tables.get('merge_log', 'bn_id_merge_log')}"
        self.metrics_table = f"{self.project}.{self.output_dataset}.{tables.get('metrics', 'bn_id_metrics')}"
        self.norm_log_table = f"{self.project}.{self.output_dataset}.{tables.get('normalization_log', 'cookie_normalization_log')}"
        self.node_index_table = f"{self.project}.{self.output_dataset}.{tables.get('node_index', 'bn_id_node_index')}"

        # Legacy graph tables (bio_acceptor_data removed — stale test data)
        # self.bionews_lookup and self.bionews_xref no longer used

        # Edge staging pipeline (full rebuild only — zero Python memory):
        #   _staging_table  ("_staging_edges_*") — raw edges from all connectors
        #   _staging_aggregated ("_staging_agg_*") — after aggregate_confidence dedup
        #   _staging_filtered   ("_staging_filt_*") — after quality filters + gates
        # Union-Find reads from _staging_filtered. Hub is written from _staging_filtered.
        self._staging_table = (
            f"{self.project}.{self.staging_dataset}._staging_edges_{self.run_id[:8]}"
        )
        self._staging_aggregated = (
            f"{self.project}.{self.staging_dataset}._staging_agg_{self.run_id[:8]}"
        )
        self._staging_filtered = (
            f"{self.project}.{self.staging_dataset}._staging_filt_{self.run_id[:8]}"
        )

        # Legacy Python-side edge storage (kept for incremental subset path which
        # operates on small data). Full rebuild uses BQ staging exclusively.
        self._edge_agg: Dict[tuple, list] = {}
        self._edges_list: Optional[List[HubEdge]] = None
        self._edge_count_raw = 0
        self.stats: Dict[str, Any] = {}

    # ─── Edge management (online aggregation) ──────────────────

    @property
    def edges(self) -> List[HubEdge]:
        """Materialized edge list. Built from _edge_agg on first access."""
        if self._edges_list is None:
            self._materialize_edges()
        return self._edges_list

    @edges.setter
    def edges(self, value: List[HubEdge]) -> None:
        """Allow direct assignment (used by quality filters, incremental path)."""
        self._edges_list = value

    def _add_edge(self, edge: HubEdge) -> None:
        """Add an edge. Routes to BQ staging (full rebuild) or Python dict (incremental).

        When self._bq_staged is True: batches edges and flushes to BQ staging table.
        When False: stores compact tuples in _edge_agg dict (legacy Python path).
        """
        self._edge_count_raw += 1

        # Inline cookie normalization (ga -> client_id) before keying
        a_type = edge.identifier_a_type
        b_type = edge.identifier_b_type
        if self.normalize_cookies:
            if a_type == "ga":
                a_type, _ = self.normalizer.resolve("ga", edge.identifier_a)
            if b_type == "ga":
                b_type, _ = self.normalizer.resolve("ga", edge.identifier_b)

        # Early skip: don't store edges involving blacklisted/bot identifiers
        if self.blacklisted_identifiers:
            key_a_check = f"{a_type}:{edge.identifier_a}"
            key_b_check = f"{b_type}:{edge.identifier_b}"
            if (
                key_a_check in self.blacklisted_identifiers
                or key_b_check in self.blacklisted_identifiers
            ):
                return

        # BQ-staged path: batch rows and flush to BigQuery periodically
        if getattr(self, "_bq_staged", False):
            self._staging_batch.append(
                {
                    "identifier_a_type": a_type,
                    "identifier_a_value": edge.identifier_a,
                    "identifier_b_type": b_type,
                    "identifier_b_value": edge.identifier_b,
                    "source_system": edge.source_system,
                    "link_type": edge.link_type,
                    "match_rule": edge.match_rule,
                    "confidence": edge.confidence,
                    "base_confidence": edge.base_confidence
                    if edge.base_confidence >= 0
                    else edge.confidence,
                    "identity_cap": edge.identity_cap,
                    "first_seen": edge.first_seen or None,
                    "last_seen": edge.last_seen or None,
                }
            )
            if len(self._staging_batch) >= self._staging_flush_size:
                self._flush_staging_batch()
            if self._edge_count_raw % 2_000_000 == 0:
                print(f"    [bq] {self._edge_count_raw:,} raw edges staged", flush=True)
            return

        # Legacy Python dict path (used by incremental subset)
        if self._edge_count_raw % 2_000_000 == 0:
            try:
                import resource

                rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
            except Exception:
                rss = 0
            agg = len(self._edge_agg)
            print(
                f"    [mem] {self._edge_count_raw:,} raw edges | "
                f"{agg:,} unique pairs | {rss:,} MB RSS",
                flush=True,
            )

        a_type = sys.intern(a_type)
        b_type = sys.intern(b_type)
        source = sys.intern(edge.source_system)
        link = sys.intern(edge.link_type)
        rule = sys.intern(edge.match_rule)

        key_a = f"{a_type}:{edge.identifier_a}"
        key_b = f"{b_type}:{edge.identifier_b}"
        pair = (key_a, key_b) if key_a <= key_b else (key_b, key_a)

        slot = self._edge_agg.get(pair)
        if slot is None:
            # [conf, base_conf, identity_cap, source, link, rule, count, first_seen, last_seen]
            self._edge_agg[pair] = [
                edge.confidence,
                edge.base_confidence,
                edge.identity_cap,
                source,
                link,
                rule,
                1,
                edge.first_seen,
                edge.last_seen,
            ]
        else:
            slot[6] += 1  # count
            if edge.confidence > slot[0]:
                slot[0] = edge.confidence
                slot[1] = edge.base_confidence
                slot[2] = edge.identity_cap
                slot[3] = source
                slot[4] = link
                slot[5] = rule
            elif (
                edge.confidence == slot[0] and slot[3] == "prior" and source != "prior"
            ):
                slot[0] = edge.confidence
                slot[1] = edge.base_confidence
                slot[2] = edge.identity_cap
                slot[3] = source
                slot[4] = link
                slot[5] = rule
            if edge.first_seen and (not slot[7] or edge.first_seen < slot[7]):
                slot[7] = edge.first_seen
            if edge.last_seen and (not slot[8] or edge.last_seen > slot[8]):
                slot[8] = edge.last_seen

    def _materialize_edges(self) -> None:
        """Convert _edge_agg dict into HubEdge objects. Drains the dict to save memory."""
        self._edges_list = []
        append = self._edges_list.append
        while self._edge_agg:
            pair, slot = self._edge_agg.popitem()
            key_a, key_b = pair
            a_type, a_val = key_a.split(":", 1)
            b_type, b_val = key_b.split(":", 1)
            append(
                HubEdge(
                    identifier_a=a_val,
                    identifier_a_type=a_type,
                    identifier_b=b_val,
                    identifier_b_type=b_type,
                    source_system=slot[3],
                    link_type=slot[4],
                    match_rule=slot[5],
                    confidence=slot[0],
                    base_confidence=slot[1],
                    identity_cap=slot[2],
                    first_seen=slot[7],
                    last_seen=slot[8],
                )
            )
        print(
            f"    Materialized {len(self._edges_list):,} edges "
            f"(from {self._edge_count_raw:,} raw)",
            flush=True,
        )

    def _filter_edge_agg_fanout(self) -> int:
        """Remove over-linked nodes from _edge_agg before materialization.
        Runs fanout threshold checks on the compact dict to avoid creating
        HubEdge objects that would immediately be filtered out."""
        print("  Pre-materialization fanout filtering...", flush=True)
        node_degree: Dict[str, int] = defaultdict(int)
        for pair, slot in self._edge_agg.items():
            key_a, key_b = pair
            # slot: [conf, base_conf, id_cap, source, link, rule, count, first, last]
            rule = slot[5] if len(slot) > 5 else ""
            skip_email_fanout = rule == "WP_USER_EMAIL"
            a_type = key_a.split(":", 1)[0]
            b_type = key_b.split(":", 1)[0]
            if not (skip_email_fanout and a_type == "email"):
                node_degree[key_a] += 1
            if not (skip_email_fanout and b_type == "email"):
                node_degree[key_b] += 1

        bad_nodes = set()
        type_bad_counts: Dict[str, int] = defaultdict(int)
        for node, degree in node_degree.items():
            id_type = node.split(":", 1)[0]
            threshold = self._get_fanout_threshold(id_type)
            if degree > threshold:
                bad_nodes.add(node)
                type_bad_counts[id_type] += 1
        del node_degree

        if not bad_nodes:
            print("    No over-linked nodes found", flush=True)
            return 0

        # Build list of keys to remove (can't modify dict during iteration)
        keys_to_remove = [
            pair
            for pair in self._edge_agg
            if pair[0] in bad_nodes or pair[1] in bad_nodes
        ]
        for key in keys_to_remove:
            del self._edge_agg[key]

        type_summary = ", ".join(f"{t}={c}" for t, c in sorted(type_bad_counts.items()))
        print(
            f"    Removed {len(keys_to_remove):,} pairs ({len(bad_nodes):,} over-linked nodes: {type_summary})",
            flush=True,
        )
        return len(keys_to_remove)

    # ─── BQ edge staging (Phase B) ─────────────────────────────

    def _enable_bq_staging(self) -> None:
        """Switch _add_edge() to BQ staging mode (full rebuild)."""
        self._bq_staged = True
        self._staging_batch = []
        self._staging_flush_size = 500_000  # Flush every 500K rows

    def _disable_bq_staging(self) -> None:
        """Switch back to Python dict mode (incremental)."""
        self._bq_staged = False
        self._staging_batch = []

    def _flush_staging_batch(self) -> None:
        """Flush the current batch of staged edges to BQ."""
        if not self._staging_batch:
            return
        import pandas as pd

        df = pd.DataFrame(self._staging_batch)
        if "first_seen" in df.columns:
            df["first_seen"] = pd.to_datetime(
                df["first_seen"], utc=True, format="ISO8601", errors="coerce"
            )
        if "last_seen" in df.columns:
            df["last_seen"] = pd.to_datetime(
                df["last_seen"], utc=True, format="ISO8601", errors="coerce"
            )
        schema = [
            bigquery.SchemaField("identifier_a_type", "STRING"),
            bigquery.SchemaField("identifier_a_value", "STRING"),
            bigquery.SchemaField("identifier_b_type", "STRING"),
            bigquery.SchemaField("identifier_b_value", "STRING"),
            bigquery.SchemaField("source_system", "STRING"),
            bigquery.SchemaField("link_type", "STRING"),
            bigquery.SchemaField("match_rule", "STRING"),
            bigquery.SchemaField("confidence", "FLOAT64"),
            bigquery.SchemaField("base_confidence", "FLOAT64"),
            bigquery.SchemaField("identity_cap", "FLOAT64"),
            bigquery.SchemaField("first_seen", "TIMESTAMP"),
            bigquery.SchemaField("last_seen", "TIMESTAMP"),
        ]
        job_config = bigquery.LoadJobConfig(
            schema=schema, write_disposition="WRITE_APPEND"
        )
        self.client.load_table_from_dataframe(
            df, self._staging_table, job_config=job_config
        ).result()
        self._staging_batch.clear()

    def _create_staging_table(self) -> None:
        """Create the BQ staging table for edge collection. Zero Python memory."""
        query = f"""
        CREATE OR REPLACE TABLE `{self._staging_table}` (
          identifier_a_type STRING,
          identifier_a_value STRING,
          identifier_b_type STRING,
          identifier_b_value STRING,
          source_system STRING,
          link_type STRING,
          match_rule STRING,
          confidence FLOAT64,
          base_confidence FLOAT64,
          identity_cap FLOAT64,
          first_seen TIMESTAMP,
          last_seen TIMESTAMP
        )
        """
        self._run_query(query, "create_staging")

    def _staging_edge_count(self) -> int:
        """Count rows in the staging table."""
        try:
            query = f"SELECT COUNT(*) AS cnt FROM `{self._staging_table}`"
            result = list(self._run_query(query, "staging_count"))
            return result[0]["cnt"] if result else 0
        except Exception:
            return 0

    def _insert_connector_edges(self, select_sql: str, label: str) -> int:
        """INSERT connector edges into the staging table via BQ SQL.
        The select_sql must produce columns matching the staging schema.

        CONTRACT: This bypasses _add_edge() — so any logic in _add_edge that
        modifies edges (cookie normalization, early blacklist checks, _edge_count_raw
        accounting) is NOT applied. Callers must ensure:
          - Cookie normalization is handled in SQL or in later BQ aggregation
          - Blacklist filtering happens in BQ quality filters (already does)
          - The connector's SQL produces correct identifier types directly
        """
        insert_sql = f"""
        INSERT INTO `{self._staging_table}`
        (identifier_a_type, identifier_a_value, identifier_b_type, identifier_b_value,
         source_system, link_type, match_rule, confidence, base_confidence, identity_cap,
         first_seen, last_seen)
        {select_sql}
        """
        before = self._staging_edge_count()
        self._run_query(insert_sql, label)
        after = self._staging_edge_count()
        inserted = after - before
        # Update raw edge counter so _run_connector accounting stays correct
        self._edge_count_raw += inserted
        print(f"    [{label}] Inserted {inserted:,} edges into staging", flush=True)
        return inserted

    def _run_bq_aggregation(self) -> int:
        """Aggregate duplicate edges in BQ. Creates _staging_aggregated table."""
        print("  Aggregating edges in BigQuery...", flush=True)

        # Build rule_cap CASE expression from config
        cap_cases = []
        for rule, cap in self.confidence_caps_by_rule.items():
            cap_cases.append(f"WHEN match_rule = '{rule}' THEN {cap}")
        cap_expr = f"CASE {' '.join(cap_cases)} ELSE 1.0 END" if cap_cases else "1.0"

        query = f"""
        CREATE OR REPLACE TABLE `{self._staging_aggregated}` AS
        WITH edge_keys AS (
          SELECT *,
            LEAST(CONCAT(identifier_a_type, ':', identifier_a_value),
                  CONCAT(identifier_b_type, ':', identifier_b_value)) AS edge_key_a,
            GREATEST(CONCAT(identifier_a_type, ':', identifier_a_value),
                     CONCAT(identifier_b_type, ':', identifier_b_value)) AS edge_key_b
          FROM `{self._staging_table}`
        ),
        ranked AS (
          SELECT *,
            ROW_NUMBER() OVER (
              PARTITION BY edge_key_a, edge_key_b
              ORDER BY confidence DESC, source_system ASC
            ) AS rn,
            COUNT(*) OVER (PARTITION BY edge_key_a, edge_key_b) AS obs_count,
            MIN(first_seen) OVER (PARTITION BY edge_key_a, edge_key_b) AS min_first_seen,
            MAX(last_seen) OVER (PARTITION BY edge_key_a, edge_key_b) AS max_last_seen
          FROM edge_keys
        )
        SELECT
          identifier_a_type, identifier_a_value,
          identifier_b_type, identifier_b_value,
          source_system, link_type, match_rule,
          LEAST(
            {cap_expr},
            COALESCE(base_confidence, confidence) * LOG(obs_count + 1, 2)
          ) AS confidence,
          COALESCE(base_confidence, confidence) AS base_confidence,
          identity_cap,
          min_first_seen AS first_seen,
          max_last_seen AS last_seen,
          obs_count,
          edge_key_a, edge_key_b
        FROM ranked
        WHERE rn = 1
        """
        self._run_query(query, "bq_aggregation")
        count_result = list(
            self._run_query(
                f"SELECT COUNT(*) AS cnt FROM `{self._staging_aggregated}`", "agg_count"
            )
        )
        unique_pairs = count_result[0]["cnt"] if count_result else 0
        raw_count = self._staging_edge_count()
        print(
            f"    {raw_count:,} raw -> {unique_pairs:,} unique pairs "
            f"({raw_count - unique_pairs:,} duplicates merged)",
            flush=True,
        )
        return unique_pairs

    def _run_bq_quality_filters(self) -> int:
        """Apply quality filters in BQ. Creates _staging_filtered table."""
        print("  Applying quality filters in BigQuery...", flush=True)
        import pandas as pd

        # Upload blacklist as temp table (always create, even if empty)
        blacklist_table = (
            f"{self.project}.{self.staging_dataset}._tmp_blacklist_{self.run_id[:8]}"
        )
        bl_list = (
            list(self.blacklisted_identifiers)
            if self.blacklisted_identifiers
            else ["__EMPTY_PLACEHOLDER__"]
        )
        bl_df = pd.DataFrame({"identifier_key": bl_list})
        bl_schema = [bigquery.SchemaField("identifier_key", "STRING")]
        job_config = bigquery.LoadJobConfig(
            schema=bl_schema, write_disposition="WRITE_TRUNCATE"
        )
        self.client.load_table_from_dataframe(
            bl_df, blacklist_table, job_config=job_config
        ).result()
        del bl_df
        print(
            f"    Uploaded {len(self.blacklisted_identifiers or []):,} blacklisted identifiers",
            flush=True,
        )

        # Upload fanout thresholds as temp table
        threshold_table = (
            f"{self.project}.{self.staging_dataset}._tmp_thresholds_{self.run_id[:8]}"
        )
        threshold_rows = []
        for id_type in set(
            list(self.source_priorities.keys()) + list(self.fanout_thresholds.keys())
        ):
            threshold_rows.append(
                {
                    "id_type": id_type,
                    "threshold": self._get_fanout_threshold(id_type),
                }
            )
        if threshold_rows:
            th_df = pd.DataFrame(threshold_rows)
            th_schema = [
                bigquery.SchemaField("id_type", "STRING"),
                bigquery.SchemaField("threshold", "INT64"),
            ]
            job_config = bigquery.LoadJobConfig(
                schema=th_schema, write_disposition="WRITE_TRUNCATE"
            )
            self.client.load_table_from_dataframe(
                th_df, threshold_table, job_config=job_config
            ).result()
            del th_df

        # Step 1: Copy aggregated -> filtered, excluding blacklisted
        filter_sql = f"""
        CREATE OR REPLACE TABLE `{self._staging_filtered}` AS
        SELECT * FROM `{self._staging_aggregated}`
        WHERE edge_key_a NOT IN (SELECT identifier_key FROM `{blacklist_table}`)
          AND edge_key_b NOT IN (SELECT identifier_key FROM `{blacklist_table}`)
        """
        self._run_query(filter_sql, "bq_filter_blacklist")
        after_bl = list(
            self._run_query(
                f"SELECT COUNT(*) AS cnt FROM `{self._staging_filtered}`",
                "filter_bl_count",
            )
        )
        bl_removed = (
            list(
                self._run_query(
                    f"SELECT COUNT(*) AS cnt FROM `{self._staging_aggregated}`", ""
                )
            )[0]["cnt"]
            - after_bl[0]["cnt"]
        )
        print(f"    Blacklist: removed {bl_removed:,} edges", flush=True)

        # Step 2: Over-linked nodes (fanout threshold), anchor-aware.
        # See fanout_decisions() for the semantics and the 2026-08-21 incident.
        nodes_table = (
            f"{self.project}.{self.staging_dataset}._tmp_fanout_nodes_{self.run_id[:8]}"
        )
        self._run_query(
            build_fanout_nodes_sql(
                self._staging_filtered, nodes_table, threshold_table, self.person_types
            ),
            "bq_filter_fanout_nodes",
        )
        node_stats = list(
            self._run_query(
                f"""SELECT COUNT(*) AS over_n, COUNTIF(remove_all) AS removed_all,
                           COUNTIF(NOT remove_all) AS trimmed
                    FROM `{nodes_table}`""",
                "filter_fanout_nodes_count",
            )
        )
        fanout_nodes_over = node_stats[0]["over_n"] if node_stats else 0
        fanout_nodes_removed = node_stats[0]["removed_all"] if node_stats else 0
        fanout_nodes_trimmed = node_stats[0]["trimmed"] if node_stats else 0
        preserved = list(
            self._run_query(
                build_fanout_preserved_sql(
                    self._staging_filtered, nodes_table, self.person_types
                ),
                "filter_fanout_preserved",
            )
        )
        fanout_anchor_edges_preserved = preserved[0]["cnt"] if preserved else 0
        self._run_query(
            build_fanout_delete_sql(self._staging_filtered, nodes_table, self.person_types),
            "bq_filter_fanout",
        )
        after_fanout = list(
            self._run_query(
                f"SELECT COUNT(*) AS cnt FROM `{self._staging_filtered}`",
                "filter_fanout_count",
            )
        )
        fanout_removed = after_bl[0]["cnt"] - after_fanout[0]["cnt"]
        print(
            f"    Fanout: removed {fanout_removed:,} edges "
            f"({fanout_nodes_over:,} over-linked nodes: {fanout_nodes_trimmed:,} trimmed to "
            f"anchor edges, {fanout_nodes_removed:,} removed whole; "
            f"{fanout_anchor_edges_preserved:,} anchor edges preserved)",
            flush=True,
        )
        try:
            self.client.delete_table(nodes_table, not_found_ok=True)
        except Exception:
            pass

        # Step 3: Per-rule fanout caps (e.g., dmd_tag.LOCALSTORAGE_COOCCURRENCE)
        rule_removed = 0
        if self.fanout_thresholds_by_rule:
            import pandas as pd

            rule_threshold_table = f"{self.project}.{self.staging_dataset}._tmp_rule_thresholds_{self.run_id[:8]}"
            rule_rows = []
            for key, threshold in self.fanout_thresholds_by_rule.items():
                parts = key.split(".", 1)
                if len(parts) == 2:
                    rule_rows.append(
                        {
                            "id_type": parts[0],
                            "match_rule": parts[1],
                            "threshold": threshold,
                        }
                    )
            if rule_rows:
                rdf = pd.DataFrame(rule_rows)
                rschema = [
                    bigquery.SchemaField("id_type", "STRING"),
                    bigquery.SchemaField("match_rule", "STRING"),
                    bigquery.SchemaField("threshold", "INT64"),
                ]
                job_config = bigquery.LoadJobConfig(
                    schema=rschema, write_disposition="WRITE_TRUNCATE"
                )
                self.client.load_table_from_dataframe(
                    rdf, rule_threshold_table, job_config=job_config
                ).result()
                del rdf

                rule_fanout_sql = f"""
                DELETE FROM `{self._staging_filtered}` e
                WHERE EXISTS (
                  SELECT 1 FROM (
                    SELECT node_key, match_rule, COUNT(*) AS degree
                    FROM (
                      SELECT edge_key_a AS node_key, match_rule FROM `{self._staging_filtered}`
                      UNION ALL
                      SELECT edge_key_b, match_rule FROM `{self._staging_filtered}`
                    )
                    GROUP BY node_key, match_rule
                  ) d
                  JOIN `{rule_threshold_table}` rt
                    ON SPLIT(d.node_key, ':')[OFFSET(0)] = rt.id_type
                    AND d.match_rule = rt.match_rule
                  WHERE d.degree > rt.threshold
                    AND (d.node_key = e.edge_key_a OR d.node_key = e.edge_key_b)
                    AND d.match_rule = e.match_rule
                )
                """
                self._run_query(rule_fanout_sql, "bq_filter_rule_fanout")
                after_rule = list(
                    self._run_query(
                        f"SELECT COUNT(*) AS cnt FROM `{self._staging_filtered}`",
                        "filter_rule_count",
                    )
                )
                rule_removed = after_fanout[0]["cnt"] - after_rule[0]["cnt"]
                after_fanout = after_rule
                print(
                    f"    Per-rule fanout: removed {rule_removed:,} edges", flush=True
                )

                try:
                    self.client.delete_table(rule_threshold_table, not_found_ok=True)
                except Exception:
                    pass

        # Cleanup temp tables
        for t in [blacklist_table, threshold_table]:
            try:
                self.client.delete_table(t, not_found_ok=True)
            except Exception:
                pass

        final_result = list(
            self._run_query(
                f"SELECT COUNT(*) AS cnt FROM `{self._staging_filtered}`",
                "filter_final_count",
            )
        )
        final_count = final_result[0]["cnt"] if final_result else 0
        self.stats["quality_filter_bq"] = {
            "blacklisted_removed": bl_removed,
            "fanout_removed": fanout_removed,
            "fanout_nodes_over_threshold": fanout_nodes_over,
            "fanout_nodes_trimmed": fanout_nodes_trimmed,
            "fanout_nodes_removed_whole": fanout_nodes_removed,
            "fanout_anchor_edges_preserved": fanout_anchor_edges_preserved,
            "rule_fanout_removed": rule_removed,
            "final_edges": final_count,
        }
        return final_count

    def _run_bq_gates(self) -> None:
        """Apply shared-workstation and conflict gates in BQ."""
        print("  Applying gates in BigQuery...", flush=True)

        # Shared workstation gate: split flagging from removal.
        # Persist flagged pvids at FLAG threshold (2+) for cluster attribute labeling.
        # DELETE edges only at REMOVAL threshold (3+) — conservative.
        ws_flag_min = getattr(self, "shared_ws_flag_min", 2)
        ws_remove_min = getattr(self, "shared_ws_remove_min", 3)
        self._shared_ws_bq_table = (
            f"{self.project}.{self.staging_dataset}._tmp_shared_ws_{self.run_id[:8]}"
        )
        # Persist flagged pvids at the LOWER (flag) threshold for cluster attributes
        self._run_query(
            f"""
        CREATE OR REPLACE TABLE `{self._shared_ws_bq_table}` AS
        SELECT bnfpvid FROM (
          SELECT
            CASE WHEN identifier_a_type = 'bnfpvid' THEN identifier_a_value
                 WHEN identifier_b_type = 'bnfpvid' THEN identifier_b_value END AS bnfpvid,
            CASE WHEN identifier_a_type = 'mc_euid' THEN identifier_a_value
                 WHEN identifier_b_type = 'mc_euid' THEN identifier_b_value END AS mc_euid
          FROM `{self._staging_filtered}`
          WHERE match_rule = 'LOCALSTORAGE_COOCCURRENCE'
            AND 'mc_euid' IN (identifier_a_type, identifier_b_type)
            AND 'bnfpvid' IN (identifier_a_type, identifier_b_type)
        )
        WHERE bnfpvid IS NOT NULL AND mc_euid IS NOT NULL
        GROUP BY bnfpvid
        HAVING COUNT(DISTINCT mc_euid) >= {ws_flag_min}
        """,
            "bq_gate_persist_shared_ws",
        )
        ws_sql = f"""
        DELETE FROM `{self._staging_filtered}` e
        WHERE match_rule = 'LOCALSTORAGE_COOCCURRENCE'
          -- Only delete bnfpvid<->mc_euid edges (not bnfpvid<->client_id, etc.)
          AND 'bnfpvid' IN (identifier_a_type, identifier_b_type)
          AND 'mc_euid' IN (identifier_a_type, identifier_b_type)
          AND (
            edge_key_a IN (
              SELECT CONCAT('bnfpvid:', bnfpvid) FROM (
                SELECT
                  CASE WHEN identifier_a_type = 'bnfpvid' THEN identifier_a_value
                       WHEN identifier_b_type = 'bnfpvid' THEN identifier_b_value END AS bnfpvid,
                  CASE WHEN identifier_a_type = 'mc_euid' THEN identifier_a_value
                       WHEN identifier_b_type = 'mc_euid' THEN identifier_b_value END AS mc_euid
                FROM `{self._staging_filtered}`
                WHERE match_rule = 'LOCALSTORAGE_COOCCURRENCE'
                  AND 'mc_euid' IN (identifier_a_type, identifier_b_type)
                  AND 'bnfpvid' IN (identifier_a_type, identifier_b_type)
              )
              WHERE bnfpvid IS NOT NULL AND mc_euid IS NOT NULL
              GROUP BY bnfpvid
              HAVING COUNT(DISTINCT mc_euid) >= {ws_remove_min}
            )
            OR edge_key_b IN (
              SELECT CONCAT('bnfpvid:', bnfpvid) FROM (
                SELECT
                  CASE WHEN identifier_a_type = 'bnfpvid' THEN identifier_a_value
                       WHEN identifier_b_type = 'bnfpvid' THEN identifier_b_value END AS bnfpvid,
                  CASE WHEN identifier_a_type = 'mc_euid' THEN identifier_a_value
                       WHEN identifier_b_type = 'mc_euid' THEN identifier_b_value END AS mc_euid
                FROM `{self._staging_filtered}`
                WHERE match_rule = 'LOCALSTORAGE_COOCCURRENCE'
                  AND 'mc_euid' IN (identifier_a_type, identifier_b_type)
                  AND 'bnfpvid' IN (identifier_a_type, identifier_b_type)
              )
              WHERE bnfpvid IS NOT NULL AND mc_euid IS NOT NULL
              GROUP BY bnfpvid
              HAVING COUNT(DISTINCT mc_euid) >= {ws_remove_min}
            )
          )
        """
        before_ws = list(
            self._run_query(
                f"SELECT COUNT(*) AS cnt FROM `{self._staging_filtered}`",
                "ws_gate_before",
            )
        )[0]["cnt"]
        self._run_query(ws_sql, "bq_gate_shared_ws")
        after_ws = list(
            self._run_query(
                f"SELECT COUNT(*) AS cnt FROM `{self._staging_filtered}`",
                "ws_gate_after",
            )
        )[0]["cnt"]
        ws_removed = before_ws - after_ws
        print(f"    Shared workstation gate: {ws_removed:,} edges removed", flush=True)

        # Conflict score gate: per-type thresholds matching Python _compute_conflict_scores
        conflict_types = getattr(
            self, "conflict_types", {"email", "mc_euid", "npi_number", "bionews_uk"}
        )
        conflict_thresholds = getattr(
            self,
            "conflict_thresholds",
            {
                "email": 3,
                "npi_number": 2,
                "mc_euid": 3,
                "bionews_uk": 2,
            },
        )
        # For each anchor type, find bridge nodes exceeding the threshold
        for anchor_type, threshold in conflict_thresholds.items():
            conflict_sql = f"""
            UPDATE `{self._staging_filtered}` e
            SET identity_cap = LEAST(identity_cap, 0.5)
            WHERE edge_key_a IN (
              SELECT node_key FROM (
                SELECT node_key, COUNT(DISTINCT anchor_value) AS anchor_count FROM (
                  SELECT edge_key_a AS node_key,
                         CASE WHEN SPLIT(edge_key_b, ':')[OFFSET(0)] = '{anchor_type}'
                              THEN edge_key_b END AS anchor_value
                  FROM `{self._staging_filtered}`
                  UNION ALL
                  SELECT edge_key_b,
                         CASE WHEN SPLIT(edge_key_a, ':')[OFFSET(0)] = '{anchor_type}'
                              THEN edge_key_a END AS anchor_value
                  FROM `{self._staging_filtered}`
                )
                WHERE anchor_value IS NOT NULL
                GROUP BY node_key
                HAVING anchor_count >= {threshold}
              )
            )
            OR edge_key_b IN (
              SELECT node_key FROM (
                SELECT node_key, COUNT(DISTINCT anchor_value) AS anchor_count FROM (
                  SELECT edge_key_a AS node_key,
                         CASE WHEN SPLIT(edge_key_b, ':')[OFFSET(0)] = '{anchor_type}'
                              THEN edge_key_b END AS anchor_value
                  FROM `{self._staging_filtered}`
                  UNION ALL
                  SELECT edge_key_b,
                         CASE WHEN SPLIT(edge_key_a, ':')[OFFSET(0)] = '{anchor_type}'
                              THEN edge_key_a END AS anchor_value
                  FROM `{self._staging_filtered}`
                )
                WHERE anchor_value IS NOT NULL
                GROUP BY node_key
                HAVING anchor_count >= {threshold}
              )
            )
            """
            self._run_query(conflict_sql, f"bq_gate_conflict_{anchor_type}")
        # Count edges with identity_cap reduced by conflict gate
        conflict_capped = list(
            self._run_query(
                f"SELECT COUNTIF(identity_cap < 1.0) AS cnt FROM `{self._staging_filtered}`",
                "conflict_gate_count",
            )
        )[0]["cnt"]
        print(
            f"    Conflict score gate: {conflict_capped:,} edges capped ({len(conflict_thresholds)} anchor types)",
            flush=True,
        )

        self.stats["bq_gates"] = {
            "shared_ws_edges_removed": ws_removed,
            "conflict_edges_capped": conflict_capped,
        }

    def _bq_influence_window_filter(self) -> str:
        """Build a BQ WHERE clause fragment that excludes edges beyond their type influence window.
        Unknown types default to 365 days (matching Python _get_type_influence_days default).
        Types with -1 are permanent (never excluded). NULL last_seen edges are kept."""
        if not self.edge_aging_enabled or not self.type_influence_days:
            return ""
        # Build CASE for each type: -1 = permanent, positive = window, unknown = 365
        all_types = self.type_influence_days
        a_cases = []
        b_cases = []
        for t, days in all_types.items():
            a_cases.append(
                f"WHEN SPLIT(edge_key_a, ':')[OFFSET(0)] = '{t}' THEN {days}"
            )
            b_cases.append(
                f"WHEN SPLIT(edge_key_b, ':')[OFFSET(0)] = '{t}' THEN {days}"
            )
        # Default 365 for unknown types (not -1, matching Python)
        a_expr = f"CASE {' '.join(a_cases)} ELSE 365 END"
        b_expr = f"CASE {' '.join(b_cases)} ELSE 365 END"
        return f"""
          AND (
            last_seen IS NULL  -- Keep edges with no last_seen (matches Python: no exclusion)
            OR NOT (
              TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_seen, DAY) >
              CASE
                WHEN ({a_expr}) = -1 AND ({b_expr}) = -1 THEN 999999
                WHEN ({a_expr}) = -1 THEN {b_expr}
                WHEN ({b_expr}) = -1 THEN {a_expr}
                ELSE LEAST({a_expr}, {b_expr})
              END
            )
          )
        """

    def _bq_anchor_aware_decay_expr(
        self,
        last_seen_col: str = "last_seen",
        a_type_col: str = "identifier_a_type",
        b_type_col: str = "identifier_b_type",
    ) -> str:
        """Build a BQ CASE expression for anchor-aware decay.

        If EITHER endpoint type has influence_days = -1 (permanent), decay
        weight is 1.0 (no decay). Otherwise, apply the standard decay schedule.
        Matches the Python _get_effective_confidence anchor-aware logic exactly.
        """
        # Permanent types from config
        permanent_types = [
            t for t, days in self.type_influence_days.items() if days == -1
        ]
        permanent_sql = (
            ", ".join(f"'{t}'" for t in permanent_types)
            if permanent_types
            else "'__none__'"
        )

        # Standard decay schedule
        decay_cases = [f"WHEN {last_seen_col} IS NULL THEN 1.0"]
        for bucket in self.decay_schedule:
            decay_cases.append(
                f"WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), {last_seen_col}, DAY) <= {bucket['max_age_days']} "
                f"THEN {bucket['weight']}"
            )
        standard_decay = f"CASE {' '.join(decay_cases)} ELSE 0.0 END"

        # Anchor-aware: skip decay if either endpoint is permanent
        return (
            f"CASE WHEN {a_type_col} IN ({permanent_sql}) OR {b_type_col} IN ({permanent_sql}) "
            f"THEN 1.0 ELSE {standard_decay} END"
        )

    def _get_edge_count_and_threshold(self) -> Tuple[int, int]:
        """Get total stitchable edges and below-threshold count without materializing."""
        ratio_cases = []
        for rule, ratio in self.observation_to_identity_ratio.items():
            ratio_cases.append(f"WHEN match_rule = '{rule}' THEN {ratio}")
        ratio_expr = (
            f"CASE {' '.join(ratio_cases)} ELSE 1.0 END" if ratio_cases else "1.0"
        )
        decay_expr = self._bq_anchor_aware_decay_expr()
        influence_filter = self._bq_influence_window_filter()

        count_query = f"""
        SELECT
          COUNTIF(LEAST(identity_cap, confidence * {ratio_expr} * {decay_expr}) < {self.stitch_threshold}) AS below_threshold,
          COUNT(*) AS total
        FROM `{self._staging_filtered}`
        WHERE 1=1 {influence_filter}
        """
        counts = list(self._run_query(count_query, "edge_count"))
        below_threshold = counts[0]["below_threshold"] if counts else 0
        total = counts[0]["total"] if counts else 0
        stitchable = total - below_threshold
        return stitchable, below_threshold

    def _build_node_priorities_in_bq(self) -> Dict[str, int]:
        """Build node priorities from BQ without materializing all edges."""
        ratio_cases = []
        for rule, ratio in self.observation_to_identity_ratio.items():
            ratio_cases.append(f"WHEN match_rule = '{rule}' THEN {ratio}")
        ratio_expr = (
            f"CASE {' '.join(ratio_cases)} ELSE 1.0 END" if ratio_cases else "1.0"
        )
        decay_expr = self._bq_anchor_aware_decay_expr()
        influence_filter = self._bq_influence_window_filter()

        priority_query = f"""
        SELECT
          DISTINCT node,
          SPLIT(node, ':')[OFFSET(0)] AS node_type
        FROM (
          SELECT edge_key_a AS node FROM `{self._staging_filtered}`
          WHERE 1=1 {influence_filter} AND LEAST(identity_cap, confidence * {ratio_expr} * {decay_expr}) >= {self.stitch_threshold}
          UNION ALL
          SELECT edge_key_b AS node FROM `{self._staging_filtered}`
          WHERE 1=1 {influence_filter} AND LEAST(identity_cap, confidence * {ratio_expr} * {decay_expr}) >= {self.stitch_threshold}
        )
        """
        node_priority = {}
        for row in self._run_query(priority_query, "node_priorities"):
            node = row["node"]
            node_type = row["node_type"]
            priority = self.source_priorities.get(node_type, 99)
            node_priority[node] = min(node_priority.get(node, 99), priority)
        return node_priority

    def _export_union_find_tuples(self):
        """Stream edges for Union-Find without materializing all rows."""
        ratio_cases = []
        for rule, ratio in self.observation_to_identity_ratio.items():
            ratio_cases.append(f"WHEN match_rule = '{rule}' THEN {ratio}")
        ratio_expr = (
            f"CASE {' '.join(ratio_cases)} ELSE 1.0 END" if ratio_cases else "1.0"
        )
        decay_expr = self._bq_anchor_aware_decay_expr()
        influence_filter = self._bq_influence_window_filter()

        query = f"""
        SELECT
          edge_key_a AS key_a,
          edge_key_b AS key_b,
          LEAST(
            identity_cap,
            confidence * {ratio_expr} * {decay_expr}
          ) AS effective_confidence
        FROM `{self._staging_filtered}`
        WHERE 1=1
          {influence_filter}
          AND LEAST(
            identity_cap,
            confidence * {ratio_expr} * {decay_expr}
          ) >= {self.stitch_threshold}
        """

        # Stream results without materializing full result set (memory safe for large datasets)
        _suppress_stderr = hasattr(sys.stderr, "fileno")
        if _suppress_stderr:
            try:
                _stderr_fd = sys.stderr.fileno()
                _saved_fd = os.dup(_stderr_fd)
                _devnull = os.open(os.devnull, os.O_WRONLY)
                os.dup2(_devnull, _stderr_fd)
                os.close(_devnull)
            except (AttributeError, io.UnsupportedOperation):
                _suppress_stderr = False
        try:
            job = self.client.query(query)
            edge_count = 0
            for row in job.result(page_size=50_000):
                edge_count += 1
                yield row["key_a"], row["key_b"], row["effective_confidence"]
        finally:
            if _suppress_stderr:
                os.dup2(_saved_fd, _stderr_fd)
                os.close(_saved_fd)

    def _upload_assignments_to_bq(self, node_to_bn_id: Dict[str, str]) -> str:
        """Upload node_to_bn_id to a BQ temp table. Returns the table name."""
        import pandas as pd

        assignments_table = (
            f"{self.project}.{self.staging_dataset}._tmp_assignments_{self.run_id[:8]}"
        )

        CHUNK = 500_000
        first = True
        chunk = []
        total_rows = 0

        for k, v in node_to_bn_id.items():
            chunk.append({"identifier_key": k, "bn_id": v})
            total_rows += 1

            if len(chunk) >= CHUNK:
                df = pd.DataFrame(chunk)
                disposition = "WRITE_TRUNCATE" if first else "WRITE_APPEND"
                schema = [
                    bigquery.SchemaField("identifier_key", "STRING"),
                    bigquery.SchemaField("bn_id", "STRING"),
                ]
                job_config = bigquery.LoadJobConfig(
                    schema=schema, write_disposition=disposition
                )
                self.client.load_table_from_dataframe(
                    df, assignments_table, job_config=job_config
                ).result()
                first = False
                chunk = []

        # Upload final partial chunk
        if chunk:
            df = pd.DataFrame(chunk)
            disposition = "WRITE_TRUNCATE" if first else "WRITE_APPEND"
            schema = [
                bigquery.SchemaField("identifier_key", "STRING"),
                bigquery.SchemaField("bn_id", "STRING"),
            ]
            job_config = bigquery.LoadJobConfig(
                schema=schema, write_disposition=disposition
            )
            self.client.load_table_from_dataframe(
                df, assignments_table, job_config=job_config
            ).result()

        print(f"    Uploaded {total_rows:,} assignments to BQ", flush=True)
        return assignments_table

    def _write_hub_from_staging(self, assignments_table: str, target_table: str) -> int:
        """Write hub table from BQ staging + assignments JOIN. Zero Python memory."""
        print(f"  Writing hub table from staging...", flush=True)

        # Build tiers temp table
        import pandas as pd

        tiers_table = (
            f"{self.project}.{self.staging_dataset}._tmp_tiers_{self.run_id[:8]}"
        )
        tier_rows = [
            {"bn_id": k, "cluster_tier": v} for k, v in self._bn_id_tiers.items()
        ]
        if tier_rows:
            df = pd.DataFrame(tier_rows)
            schema = [
                bigquery.SchemaField("bn_id", "STRING"),
                bigquery.SchemaField("cluster_tier", "STRING"),
            ]
            job_config = bigquery.LoadJobConfig(
                schema=schema, write_disposition="WRITE_TRUNCATE"
            )
            self.client.load_table_from_dataframe(
                df, tiers_table, job_config=job_config
            ).result()
            del df, tier_rows

        # Build obs_to_id_ratio and anchor-aware decay CASE expressions
        ratio_cases = []
        for rule, ratio in self.observation_to_identity_ratio.items():
            ratio_cases.append(f"WHEN e.match_rule = '{rule}' THEN {ratio}")
        ratio_expr = (
            f"CASE {' '.join(ratio_cases)} ELSE 1.0 END" if ratio_cases else "1.0"
        )

        # Anchor-aware decay: must match _export_union_find_tuples and Python path
        decay_expr = self._bq_anchor_aware_decay_expr(
            last_seen_col="e.last_seen",
            a_type_col="e.identifier_a_type",
            b_type_col="e.identifier_b_type",
        )

        browser_expiry = self.browser_expiry_days
        person_types_str = ", ".join(f"'{t}'" for t in self.person_types)

        query = f"""
        CREATE OR REPLACE TABLE `{target_table}` AS
        SELECT
          a.bn_id,
          e.identifier_a_type, e.identifier_a_value,
          e.identifier_b_type, e.identifier_b_value,
          e.source_system, e.link_type, e.match_rule,
          e.base_confidence,
          e.confidence,
          LEAST(e.identity_cap, e.confidence * {ratio_expr} * {decay_expr}) AS effective_confidence,
          e.first_seen,
          e.last_seen,
          (e.identifier_a_type IN ({person_types_str}) OR e.identifier_b_type IN ({person_types_str})
           OR e.last_seen >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {browser_expiry} DAY)
          ) AS is_active,
          COALESCE(t.cluster_tier, 'unknown') AS cluster_tier
        FROM `{self._staging_filtered}` e
        JOIN (
          -- Pick one bn_id per edge: prefer the a-side assignment, fall back to b-side.
          -- This avoids duplicate rows from the OR condition.
          SELECT edge_key_a, edge_key_b, COALESCE(a1.bn_id, a2.bn_id) AS bn_id
          FROM `{self._staging_filtered}` s
          LEFT JOIN `{assignments_table}` a1 ON s.edge_key_a = a1.identifier_key
          LEFT JOIN `{assignments_table}` a2 ON s.edge_key_b = a2.identifier_key
          WHERE COALESCE(a1.bn_id, a2.bn_id) IS NOT NULL
        ) a ON e.edge_key_a = a.edge_key_a AND e.edge_key_b = a.edge_key_b
        LEFT JOIN `{tiers_table}` t ON a.bn_id = t.bn_id
        """
        self._run_query(query, "write_hub_from_staging")

        # Get row count
        count_result = list(
            self._run_query(
                f"SELECT COUNT(*) AS cnt FROM `{target_table}`", "hub_count"
            )
        )
        row_count = count_result[0]["cnt"] if count_result else 0
        print(f"    Wrote {row_count:,} hub rows", flush=True)

        # Cleanup tiers temp
        try:
            self.client.delete_table(tiers_table, not_found_ok=True)
        except Exception:
            pass

        self.stats["hub_table"] = {"rows": row_count, "mode": "bq_staging"}
        return row_count

    def _verify_output_contracts(self) -> None:
        """Post-rebuild contract checks.

        Verifies that the output tables are internally consistent and that
        stitching-only types were correctly excluded from published surfaces.
        Violations are logged as warnings (non-fatal) and stored in stats.
        """
        print("  Running output contract checks...")
        violations = []

        try:
            # Determine which types should NOT be in xref/neighbors
            all_output_types = set(
                self.config.get("person_anchoring", {}).get("output_types", [])
            )
            # All types that appear in the hub -- both edge sides. (The legacy
            # single-sided identifier_type column was removed 2026-08-20; this
            # union sees side-B-only types the old check silently missed.)
            hub_types_query = f"""
            SELECT identifier_a_type AS identifier_type FROM `{self.hub_table}`
            UNION DISTINCT
            SELECT identifier_b_type FROM `{self.hub_table}`
            """
            hub_types = {
                r["identifier_type"]
                for r in self._run_query(hub_types_query, "contract_hub_types")
            }

            # Types in xref that should be absent (stitching-only types)
            xref_types_query = f"""
            SELECT DISTINCT identifier_type
            FROM `{self.xref_table}`
            """
            xref_types = {
                r["identifier_type"]
                for r in self._run_query(xref_types_query, "contract_xref_types")
            }

            # Stitching-only types that leaked into xref
            stitching_only = hub_types - all_output_types
            leaked = stitching_only & xref_types
            if leaked:
                violations.append(
                    f"Stitching-only types found in xref: {sorted(leaked)}"
                )

            # Output types expected in xref but absent (possible config error)
            missing_output = all_output_types - xref_types
            if missing_output:
                # Some output types may simply not appear in the data (e.g., phone).
                # Only flag if a type is in output_types AND in hub_types but NOT in xref.
                truly_missing = missing_output & hub_types
                if truly_missing:
                    violations.append(
                        f"Output types in hub but missing from xref: {sorted(truly_missing)}"
                    )

            # bn_id count sanity
            xref_bn_count_query = (
                f"SELECT COUNT(DISTINCT bn_id) AS cnt FROM `{self.xref_table}`"
            )
            xref_bn_count = list(
                self._run_query(xref_bn_count_query, "contract_bn_count")
            )[0]["cnt"]
            uf_components = self.stats.get("union_find", {}).get("components", 0)
            if (
                uf_components > 0
                and abs(xref_bn_count - uf_components) > uf_components * 0.05
            ):
                violations.append(
                    f"bn_id count mismatch: xref has {xref_bn_count:,} but Union-Find produced {uf_components:,} "
                    f"(>{5}% drift)"
                )

            # wp_user_id format guard: must be site-qualified as `{site}:{id}`.
            # Raw integers indicate one of the WP/GA4/acceptor emit sites
            # regressed and is producing cross-site collisions again.
            wp_format_query = f"""
            SELECT COUNT(*) AS cnt
            FROM `{self.hub_table}`
            WHERE (identifier_a_type = 'wp_user_id' AND identifier_a_value NOT LIKE '%:%')
               OR (identifier_b_type = 'wp_user_id' AND identifier_b_value NOT LIKE '%:%')
            """
            wp_unqualified = list(
                self._run_query(wp_format_query, "contract_wp_user_id_format")
            )[0]["cnt"]
            if wp_unqualified > 0:
                violations.append(
                    f"{wp_unqualified:,} wp_user_id edges in hub are NOT site-qualified "
                    f"(expected `{{site}}:{{id}}` like `bnmyaprd:4629`). A connector is "
                    f"emitting bare integers — check Phase 0/0d/2c/11/11b."
                )

            if violations:
                print(f"    WARNING: {len(violations)} contract violations found:")
                for v in violations:
                    print(f"      - {v}")
            else:
                print(
                    f"    All contracts passed (xref types: {len(xref_types)}, "
                    f"hub types: {len(hub_types)}, bn_ids: {xref_bn_count:,})"
                )

        except Exception as e:
            violations.append(f"Contract check failed with error: {e}")
            print(f"    Contract check error (non-fatal): {e}")

        self.stats["contract_check"] = {
            "violations": violations,
            "passed": len(violations) == 0,
        }

    def _cleanup_staging(self) -> None:
        """Clean up all staging and temp tables from this run AND orphans from prior runs."""
        # Current run's known tables
        known = [self._staging_table, self._staging_aggregated, self._staging_filtered]
        if hasattr(self, "_shared_ws_bq_table"):
            known.append(self._shared_ws_bq_table)
        for table in known:
            try:
                self.client.delete_table(table, not_found_ok=True)
            except Exception:
                pass

        # Sweep orphaned tables from prior/crashed runs (>24 hours old)
        try:
            dataset_ref = f"{self.project}.{self.staging_dataset}"
            now = datetime.now(timezone.utc)
            orphans_dropped = 0
            for table in self.client.list_tables(dataset_ref):
                if table.table_id.startswith(("_staging_", "_tmp_")):
                    try:
                        full = self.client.get_table(table)
                        age_hours = (now - full.modified).total_seconds() / 3600
                        if age_hours > 24:
                            self.client.delete_table(table, not_found_ok=True)
                            orphans_dropped += 1
                    except Exception:
                        pass
            if orphans_dropped:
                print(
                    f"    Cleaned up {orphans_dropped} orphaned temp tables (>24h old)"
                )
        except Exception:
            pass  # Don't fail the pipeline on cleanup errors

    # ─── Helpers ──────────────────────────────────────────────

    def _fqt(self, dataset: str, table: str) -> str:
        """Fully-qualified table name."""
        return f"{self.project}.{dataset}.{table}"

    def _fallback_edge_ts(self) -> str:
        """Stable fallback when a connector has no observation timestamp.

        Prefer graph_start_date (reference/static edges) over wall-clock now so
        rebuilds don't stamp every missing edge with the run time.
        """
        if getattr(self, "start_date", None):
            return f"{self.start_date}T00:00:00"
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

    def _ts_iso(self, value, fallback: Optional[str] = None) -> str:
        """Normalize a BQ DATE/TIMESTAMP/str to ISO; never return empty."""
        fb = self._fallback_edge_ts() if fallback is None else fallback
        if value is None:
            return fb
        if hasattr(value, "isoformat"):
            s = value.isoformat()
            if "T" not in s:
                s = f"{s}T00:00:00"
            return s
        s = str(value).strip()
        return s if s else fb

    def _get_existing_row_count(self, fqt: str) -> Optional[int]:
        """Return row count of an existing table, or None if table doesn't exist."""
        try:
            table = self.client.get_table(fqt)
            return table.num_rows
        except Exception:
            return None

    def _check_shrink_safeguard(
        self, table_label: str, fqt: str, new_rows: int
    ) -> None:
        """
        Abort the write if the new row count is dramatically smaller than the
        existing table's row count. Bypass with force_overwrite=True.

        For full rebuilds with shadow writes, the table being written to is
        a fresh shadow table (empty). Use the pre-rebuild snapshot row counts
        instead, since those reflect the PRODUCTION state we're replacing.

        Raises RuntimeError on abort.
        """
        if self.force_overwrite:
            return
        # Use snapshot counts when available (full rebuild with shadow writes).
        # Map table_label to snapshot key.
        snapshot = getattr(self, "_pre_rebuild_snapshot", None)
        existing = None
        if snapshot:
            snapshot_key = table_label.replace("_table", "").replace("_", "")
            # Normalize: 'hub_table' -> 'hub', 'xref_table' -> 'xref', etc.
            for key in ["hub", "xref", "neighbors", "node_index"]:
                if key in table_label:
                    snapshot_entry = snapshot.get(key)
                    if snapshot_entry:
                        existing = snapshot_entry.get("rows")
                    break
        if existing is None:
            existing = self._get_existing_row_count(fqt)
        if existing is None or existing == 0:
            return  # First run or empty — nothing to protect
        threshold = self.shrink_abort_threshold
        if new_rows < existing * threshold:
            pct = (new_rows / existing) * 100 if existing else 0
            raise RuntimeError(
                f"\n" + "=" * 72 + "\n"
                f"ABORT: {table_label} shrink safeguard tripped.\n"
                f"  Existing rows:  {existing:>15,}\n"
                f"  New rows:       {new_rows:>15,}  ({pct:.1f}% of existing)\n"
                f"  Min threshold:  {threshold * 100:.0f}% of existing\n"
                f"\n"
                f"The pipeline refused to overwrite {fqt}\n"
                f"because the new result is dramatically smaller. This usually means\n"
                f"a partial run (--connectors filter) or a failed connector.\n"
                f"\n"
                f"If this shrink is intentional, re-run with --force-overwrite.\n"
                + "="
                * 72
            )

    # ─── Pre-rebuild safety guard ────────────────────────────────

    def _snapshot_production_state(self) -> Dict[str, Any]:
        """Capture current production table state before a full rebuild.
        Used to validate the new build before promoting."""
        snapshot = {}
        for label, fqt in [
            ("hub", self.hub_table),
            ("xref", self.xref_table),
            ("neighbors", self.neighbors_table),
            ("node_index", self.node_index_table),
        ]:
            rows = self._get_existing_row_count(fqt)
            snapshot[label] = {"table": fqt, "rows": rows}

        # Sample bn_ids from current xref for retention check
        try:
            sample_query = f"""
            SELECT DISTINCT bn_id
            FROM `{self.xref_table}`
            WHERE cluster_tier = 'tier1'
            ORDER BY bn_id
            LIMIT 10000
            """
            sample_bn_ids = set()
            for row in self._run_query(
                sample_query, "snapshot_bn_ids", page_size=10000
            ):
                sample_bn_ids.add(row["bn_id"])
            snapshot["sampled_bn_ids"] = sample_bn_ids
            snapshot["sample_size"] = len(sample_bn_ids)
        except Exception:
            snapshot["sampled_bn_ids"] = set()
            snapshot["sample_size"] = 0

        for label in ["hub", "xref", "neighbors", "node_index"]:
            rows = snapshot[label]["rows"]
            print(
                f"    {label}: {rows:,} rows" if rows else f"    {label}: (not found)",
                flush=True,
            )
        if snapshot["sample_size"]:
            print(
                f"    Sampled {snapshot['sample_size']:,} tier1 bn_ids for retention check",
                flush=True,
            )

        self._pre_rebuild_snapshot = snapshot
        return snapshot

    def _validate_before_publish(
        self, node_to_bn_id: Dict[str, str], components_by_bn_id: Dict[str, list]
    ) -> None:
        """Validate the new graph before writing to production.
        Raises RuntimeError if validation fails."""
        if not hasattr(self, "_pre_rebuild_snapshot"):
            return  # No snapshot = first run, skip validation

        snap = self._pre_rebuild_snapshot
        new_bn_ids = set(node_to_bn_id.values())
        new_components = len(components_by_bn_id)

        # Edge count: use BQ staging if available, else Python list
        if hasattr(self, "_staging_filtered"):
            try:
                result = list(
                    self._run_query(
                        f"SELECT COUNT(*) AS cnt FROM `{self._staging_filtered}`",
                        "validate_edge_count",
                    )
                )
                new_edges = result[0]["cnt"] if result else 0
            except Exception:
                new_edges = 0
        else:
            new_edges = len(self.edges) if self._edges_list else self._edge_count_raw

        # Check: do we have reasonable edge and component counts?
        prior_hub_rows = snap.get("hub", {}).get("rows") or 0
        if prior_hub_rows > 0 and new_edges < prior_hub_rows * 0.3:
            pct = (new_edges / prior_hub_rows) * 100
            raise RuntimeError(
                f"ABORT: New edge count ({new_edges:,}) is only {pct:.0f}% of prior "
                f"hub rows ({prior_hub_rows:,}). Suspected data loss."
            )

        # Check: bn_id retention — what % of sampled tier1 bn_ids survived?
        sampled = snap.get("sampled_bn_ids", set())
        if sampled:
            retained = sampled & new_bn_ids
            retention_pct = len(retained) / len(sampled) * 100
            lost = len(sampled) - len(retained)
            print(
                f"    bn_id retention: {len(retained):,}/{len(sampled):,} "
                f"({retention_pct:.1f}%) tier1 bn_ids retained",
                flush=True,
            )
            if retention_pct < 70:
                raise RuntimeError(
                    f"ABORT: Only {retention_pct:.0f}% of sampled tier1 bn_ids retained "
                    f"({lost:,} lost). Suspected graph corruption. "
                    f"Re-run with --force-overwrite to bypass."
                )
        else:
            print(f"    bn_id retention: no prior sample (first run)", flush=True)

        # Check: tier distribution sanity
        tier1 = sum(1 for t in self._bn_id_tiers.values() if t == "tier1")
        tier2 = sum(1 for t in self._bn_id_tiers.values() if t == "tier2")
        print(
            f"    New graph: {new_edges:,} edges, {new_components:,} components, "
            f"{tier1:,} tier1, {tier2:,} tier2",
            flush=True,
        )

        if tier1 == 0 and prior_hub_rows > 0:
            raise RuntimeError(
                "ABORT: Zero tier1 components in new graph. "
                "Suspected configuration or data error."
            )

    def _is_connector_enabled(self, name: str) -> bool:
        """Check if connector is enabled in config and not filtered out.
        In incremental mode, connectors marked static: true are skipped
        because their edges are already in the prior graph."""
        cfg = self.connectors_config.get(name, {})
        if not cfg.get("enabled", False):
            return False
        if self.connector_filter and name not in self.connector_filter:
            return False
        if self.incremental and cfg.get("static", False):
            print(f"  [{name}] Skipped (static -- edges in prior graph)", flush=True)
            return False
        return True

    def _raise_on_connector_failures(self) -> None:
        """Abort the run if any connector recorded an error in self.stats.

        Connectors historically caught exceptions and returned 0 edges. That
        masked missing bridges. Prefer failing closed: a partial identity graph
        is worse than no new publish when a configured connector breaks.
        """
        failures = []
        for name, payload in self.stats.items():
            if not isinstance(payload, dict):
                continue
            err = payload.get("error")
            if not err:
                continue
            # bot_detection is advisory infrastructure, not an edge connector
            if name == "bot_detection":
                continue
            failures.append(f"{name}: {err}")
        if failures:
            detail = "; ".join(failures[:10])
            more = f" (+{len(failures) - 10} more)" if len(failures) > 10 else ""
            raise RuntimeError(
                f"Identity Hub connector failure(s) — refusing to publish a "
                f"partial graph: {detail}{more}"
            )

    def _compute_config_version(self) -> str:
        """SHA-256 hash of the identity_hub config (hex-truncated to 16 chars).

        Used to distinguish output from different config versions without
        requiring file-system access to the YAML at query time.
        """
        try:
            import json

            config_json = json.dumps(self.config, sort_keys=True, default=str)
            return hashlib.sha256(config_json.encode("utf-8")).hexdigest()[:16]
        except Exception:
            return "unknown"

    def _compute_git_sha(self) -> str:
        """Current git HEAD SHA (short). Returns 'unknown' if not in a git repo."""
        try:
            repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                cwd=repo_root,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return "unknown"

    def _run_query(self, query: str, label: str = "", page_size: int = 0):
        """Run a BigQuery query with progress logging. Returns the result iterator."""
        tag = f"[{label}] " if label else ""
        # Throttle chatty batch labels (e.g. hub_remap_NNN) to once per 5 minutes
        _batch_prefix = label.rsplit("_", 1)[0] if label else ""
        _is_batch = _batch_prefix and label != _batch_prefix and label[-1:].isdigit()
        _now = time.time()
        _last_key = f"_run_query_last_print_{_batch_prefix}"
        _last = getattr(self, _last_key, 0)
        _throttled = _is_batch and (_now - _last) < 300
        if not _throttled:
            print(f"    {tag}Querying BigQuery...", flush=True)
            setattr(self, _last_key, _now)
        start = time.time()
        job = self.client.query(query)
        if page_size:
            result = job.result(page_size=page_size)
        else:
            result = job.result()
        elapsed = time.time() - start
        total_bytes = job.total_bytes_processed or 0
        gb = total_bytes / (1024**3)
        if not _throttled:
            print(
                f"    {tag}Query complete ({elapsed:.1f}s, {gb:.2f} GB processed)",
                flush=True,
            )
        return result

    def _generate_bn_id(self, canonical_root: str) -> str:
        """Generate bn_id from canonical root: BN_ + base64url(SHA256)[:16]."""
        hash_bytes = hashlib.sha256(canonical_root.encode("utf-8")).digest()
        b64 = base64.urlsafe_b64encode(hash_bytes).decode("ascii").rstrip("=")
        return f"BN_{b64[:16]}"

    def _save_prior_timestamps_to_bq(self) -> bool:
        """Save prior edge timestamps to a BQ staging table (zero Python memory).
        Called before connectors so the data survives the hub table TRUNCATE."""
        self._prior_ts_table = (
            f"{self.project}.{self.staging_dataset}._tmp_prior_ts_{self.run_id[:8]}"
        )
        try:
            query = f"""
            CREATE OR REPLACE TABLE `{self._prior_ts_table}` AS
            SELECT
              identifier_a_type, identifier_a_value,
              identifier_b_type, identifier_b_value,
              MIN(first_seen) AS first_seen,
              MAX(last_seen) AS last_seen
            FROM `{self.hub_table}`
            GROUP BY 1, 2, 3, 4
            """
            self._run_query(query, "save_prior_timestamps")
            return True
        except Exception as e:
            print(f"    No prior hub table (first run): {e}")
            return False

    def _apply_prior_timestamps_in_bq(self) -> None:
        """Apply prior timestamps to the just-written hub table via BQ UPDATE."""
        if not hasattr(self, "_prior_ts_table"):
            return
        try:
            query = f"""
            UPDATE `{self.hub_table}` AS curr
            SET curr.first_seen = COALESCE(
                  LEAST(curr.first_seen, prior.first_seen),
                  curr.first_seen,
                  prior.first_seen
                ),
                curr.last_seen = COALESCE(
                  GREATEST(curr.last_seen, prior.last_seen),
                  curr.last_seen,
                  prior.last_seen
                )
            FROM `{self._prior_ts_table}` AS prior
            WHERE curr.identifier_a_type = prior.identifier_a_type
              AND curr.identifier_a_value = prior.identifier_a_value
              AND curr.identifier_b_type = prior.identifier_b_type
              AND curr.identifier_b_value = prior.identifier_b_value
            """
            self._run_query(query, "apply_prior_timestamps")
        except Exception as e:
            print(f"    Warning: could not apply prior timestamps: {e}")
        finally:
            try:
                self.client.delete_table(self._prior_ts_table, not_found_ok=True)
            except Exception:
                pass

    # REMOVED: _load_prior_timestamps, _apply_prior_timestamps, _load_prior_edges,
    # _load_prior_edges_lightweight — replaced by BQ-based timestamp handling
    # and subset Union-Find. See _save_prior_timestamps_to_bq() and _run_subset_union_find().

    def _run_subset_union_find(self, dry_run: bool = False) -> dict:
        """
        Run Union-Find on ONLY the neighborhood of clusters affected by new edges.

        Instead of loading all 33.5M prior edges into memory, this method:
          1. Collects all identifier keys from new connector edges
          2. Looks up which bn_ids those keys belong to in the current xref
          3. Expands one hop — loads ALL identifiers for each affected bn_id
          4. Loads prior edges ONLY for those bn_ids from the hub table
          5. Runs standard Union-Find on the subset (prior neighborhood + new edges)
          6. Applies persistence for merge/split detection
          7. Writes results: append new edges to hub, MERGE updates into xref

        Correctness: Union-Find sees the complete subgraph for every touched cluster.
        If a new edge merges two existing clusters, both are fully loaded.

        Memory: ~4 GB peak (vs ~17+ GB for full prior edge load).
        """
        import pandas as pd

        # Read new edge count and keys directly from _edge_agg (do NOT access
        # self.edges — that triggers materialization which drains the dict and
        # would lose new edges when prior subset edges are added later).
        new_edge_pairs = len(self._edge_agg)
        stats = {
            "new_edges": self._edge_count_raw,
            "affected_bn_ids": 0,
            "prior_subset_edges": 0,
            "total_subset_edges": 0,
        }
        print(
            f"  Subset Union-Find: {new_edge_pairs:,} unique new edge pairs "
            f"(from {self._edge_count_raw:,} raw)",
            flush=True,
        )

        # ── Step 1: Collect identifier keys from _edge_agg keys (no materialization) ──
        new_keys = set()
        for key_a, key_b in self._edge_agg.keys():
            new_keys.add(key_a)
            new_keys.add(key_b)
        print(f"    {len(new_keys):,} unique identifier keys in new edges", flush=True)

        # ── Step 2: Find affected bn_ids via xref lookup ──
        # Write new keys to a temporary table, then JOIN against xref.
        # This avoids a massive IN() clause.
        print(f"    Finding affected bn_ids in xref...", flush=True)
        step2_start = time.time()

        # Upload keys as a temp table via load job (no streaming buffer delay)
        import pandas as pd

        temp_table_id = (
            f"{self.project}.{self.staging_dataset}._tmp_incr_keys_{self.run_id[:8]}"
        )
        schema = [bigquery.SchemaField("identifier_key", "STRING")]
        df = pd.DataFrame({"identifier_key": list(new_keys)})
        job_config = bigquery.LoadJobConfig(
            schema=schema, write_disposition="WRITE_TRUNCATE"
        )
        self.client.load_table_from_dataframe(
            df, temp_table_id, job_config=job_config
        ).result()
        del df

        # JOIN to find affected bn_ids
        affected_query = f"""
        SELECT DISTINCT x.bn_id
        FROM `{self.node_index_table}` x
        JOIN `{temp_table_id}` t ON x.identifier_key = t.identifier_key
        """
        affected_bn_ids = set()
        for row in self._run_query(affected_query, "subset_affected_bn_ids"):
            affected_bn_ids.add(row["bn_id"])

        stats["affected_bn_ids"] = len(affected_bn_ids)
        print(
            f"    {len(affected_bn_ids):,} affected bn_ids found ({time.time() - step2_start:.1f}s)",
            flush=True,
        )

        # ── Step 3: Expand clusters — load full xref for affected bn_ids ──
        # This captures the complete cluster boundary so Union-Find sees everything.
        print(f"    Expanding cluster neighborhoods...", flush=True)

        expanded_xref_count = 0
        bn_id_table_id = (
            f"{self.project}.{self.staging_dataset}._tmp_incr_bnids_{self.run_id[:8]}"
        )

        if affected_bn_ids:
            try:
                self.client.delete_table(bn_id_table_id, not_found_ok=True)
            except Exception:
                pass
            bn_schema = [bigquery.SchemaField("bn_id", "STRING")]
            df = pd.DataFrame({"bn_id": list(affected_bn_ids)})
            job_config = bigquery.LoadJobConfig(
                schema=bn_schema, write_disposition="WRITE_TRUNCATE"
            )
            self.client.load_table_from_dataframe(
                df, bn_id_table_id, job_config=job_config
            ).result()
            del df

            expand_query = f"""
            SELECT x.identifier_key, x.bn_id, x.identifier_type, x.identifier_value,
                   x.cluster_tier, x.cluster_size, x.is_hcp, x.is_shared_workstation,
                   x.last_seen, x.source_profile, x.is_bot
            FROM `{self.xref_table}` x
            JOIN `{bn_id_table_id}` t ON x.bn_id = t.bn_id
            """
            for row in self._run_query(expand_query, "subset_expand", page_size=50000):
                expanded_xref_count += 1

            print(
                f"    Expanded to {expanded_xref_count:,} xref rows across {len(affected_bn_ids):,} clusters",
                flush=True,
            )

        # ── Step 4: Load prior edges for affected bn_ids only ──
        prior_count = 0
        if affected_bn_ids:
            print(f"    Loading prior edges for affected clusters...", flush=True)
            prior_start = time.time()

            _prior = sys.intern("prior")
            _det = sys.intern("deterministic")

            # Preserve match_rule from the hub — Sprint 3/4 gates depend on it.
            # No DISTINCT — aggregate_confidence handles dedup in Python.
            # Removing DISTINCT avoids a BQ sort pass on 4M+ rows.
            prior_query = f"""
            SELECT
              identifier_a_type, identifier_a_value,
              identifier_b_type, identifier_b_value,
              match_rule,
              confidence,
              FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', first_seen) AS first_seen,
              FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', last_seen) AS last_seen
            FROM `{self.hub_table}` h
            JOIN `{bn_id_table_id}` t ON h.bn_id = t.bn_id
            """
            for row in self._run_query(
                prior_query, "subset_prior_edges", page_size=50000
            ):
                a_type = sys.intern(row["identifier_a_type"])
                b_type = sys.intern(row["identifier_b_type"])
                rule = sys.intern(row.get("match_rule") or "PRIOR_EDGE")
                self._add_edge(
                    HubEdge(
                        identifier_a=row["identifier_a_value"],
                        identifier_a_type=a_type,
                        identifier_b=row["identifier_b_value"],
                        identifier_b_type=b_type,
                        source_system=_prior,
                        link_type=_det,
                        match_rule=rule,
                        confidence=row["confidence"] or 0.0,
                        first_seen=row["first_seen"] or "",
                        last_seen=row["last_seen"] or "",
                        base_confidence=-1.0,
                    )
                )
                prior_count += 1
                if prior_count % 500_000 == 0:
                    print(
                        f"    ... {prior_count:,} prior subset edges loaded", flush=True
                    )

            stats["prior_subset_edges"] = prior_count
            print(
                f"    Loaded {prior_count:,} prior subset edges ({time.time() - prior_start:.1f}s)",
                flush=True,
            )

        total_agg_pairs = len(self._edge_agg)
        stats["total_subset_edges"] = total_agg_pairs
        print(
            f"    Total unique pairs for Union-Find: {total_agg_pairs:,} "
            f"({new_edge_pairs:,} new + {prior_count:,} prior subset, "
            f"after dedup)",
            flush=True,
        )

        # ── Step 5: Run standard aggregation + quality filters + Union-Find ──
        # aggregate_confidence drains _edge_agg and materializes _edges_list,
        # combining new connector edges and prior subset edges in one pass.
        self.resolve_cookie_normalization()
        self.aggregate_confidence()
        self.apply_quality_filters()

        if dry_run:
            print(f"\n  Total subset edges: {len(self.edges):,}")
            print("  DRY RUN complete -- no tables written")
            return stats

        node_to_bn_id, components_by_bn_id = self.run_union_find()

        # ── Step 6: Persistence (merge/split detection) ──
        node_to_bn_id = self.apply_persistence(node_to_bn_id, components_by_bn_id)

        # Persist redirects BEFORE mutating hub/xref so a crash mid-write cannot
        # leave remapped production state without durable merge redirects.
        self.write_merge_log(dry_run=dry_run)
        self.write_persistence_table(node_to_bn_id, dry_run=dry_run)

        # Rebuild components and tiers from final assignments (persistence may remap)
        components_by_bn_id = defaultdict(list)
        for node_key, bn_id in node_to_bn_id.items():
            components_by_bn_id[bn_id].append(node_key)
        components_by_bn_id = dict(components_by_bn_id)
        self._bn_id_tiers = {}
        for bn_id, members in components_by_bn_id.items():
            tier = self._classify_component(members)
            if tier:
                self._bn_id_tiers[bn_id] = tier

        # Compute cluster attributes for the subset
        cluster_attrs = self._compute_cluster_attributes(
            node_to_bn_id, components_by_bn_id
        )

        # ── Step 7: Write results ──
        # 7a: Append NEW edges to hub (filter out prior stubs)
        new_edges_only = [e for e in self.edges if e.source_system != "prior"]
        old_edges = self.edges
        self.edges = new_edges_only
        self._write_hub_append(node_to_bn_id)
        self.edges = old_edges

        # 7b: Update xref — delete affected bn_ids and rewrite with new assignments
        # Build the new xref rows from Union-Find results for affected nodes
        print(f"  Updating xref for affected clusters...", flush=True)
        xref_schema = [
            bigquery.SchemaField("identifier_key", "STRING"),
            bigquery.SchemaField("bn_id", "STRING"),
            bigquery.SchemaField("identifier_type", "STRING"),
            bigquery.SchemaField("identifier_value", "STRING"),
            bigquery.SchemaField("cluster_tier", "STRING"),
            bigquery.SchemaField("cluster_size", "INTEGER"),
            bigquery.SchemaField("is_hcp", "BOOLEAN"),
            bigquery.SchemaField("is_shared_workstation", "BOOLEAN"),
            bigquery.SchemaField("last_seen", "TIMESTAMP"),
            bigquery.SchemaField("source_profile", "STRING"),
            bigquery.SchemaField("is_bot", "BOOLEAN"),
            bigquery.SchemaField("cluster_health_score", "INT64"),
            bigquery.SchemaField("is_suspicious", "BOOLEAN"),
        ]

        # Build new xref rows from Union-Find output for all nodes in the subset
        new_xref_rows = []
        empty_attrs = {
            "cluster_size": None,
            "is_hcp": None,
            "is_shared_workstation": None,
            "last_seen": None,
            "source_profile": None,
            "is_bot": None,
            "cluster_health_score": 100,
            "is_suspicious": False,
        }
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())

        for node_key, bn_id in node_to_bn_id.items():
            id_type = node_key.split(":", 1)[0]
            # Skip non-output types
            if self.person_anchoring_enabled and id_type not in self.output_types:
                continue
            if id_type.startswith("_tmp_"):
                continue
            id_value = node_key.split(":", 1)[1] if ":" in node_key else node_key
            ca = cluster_attrs.get(bn_id, empty_attrs)
            new_xref_rows.append(
                {
                    "identifier_key": node_key,
                    "bn_id": bn_id,
                    "identifier_type": id_type,
                    "identifier_value": id_value,
                    "cluster_tier": self._bn_id_tiers.get(bn_id, "unknown"),
                    "cluster_size": ca.get("cluster_size"),
                    "is_hcp": ca.get("is_hcp"),
                    "is_shared_workstation": ca.get("is_shared_workstation"),
                    "last_seen": ca.get("last_seen") or now_iso,
                    "source_profile": ca.get("source_profile"),
                    "is_bot": ca.get("is_bot"),
                    "cluster_health_score": ca.get("cluster_health_score", 100),
                    "is_suspicious": ca.get("is_suspicious", False),
                }
            )

        # Atomic xref update: stage new rows, then MERGE (avoids partial state if process dies)
        # If new_xref_rows is empty but affected_bn_ids exist, delete stale rows
        xref_rows_written = 0
        if not new_xref_rows and affected_bn_ids:
            delete_sql = f"""
            DELETE FROM `{self.xref_table}`
            WHERE bn_id IN (SELECT bn_id FROM `{bn_id_table_id}`)
            """
            self._run_query(delete_sql, "xref_delete_stale")
            print(
                f"    Deleted stale xref rows for {len(affected_bn_ids):,} filtered bn_ids",
                flush=True,
            )
        elif new_xref_rows:
            xref_staging = f"{self.project}.{self.staging_dataset}._tmp_xref_staging_{self.run_id[:8]}"
            CHUNK_SIZE = 500_000
            first_chunk = True
            for i in range(0, len(new_xref_rows), CHUNK_SIZE):
                chunk = new_xref_rows[i : i + CHUNK_SIZE]
                df = pd.DataFrame(chunk)
                if "last_seen" in df.columns:
                    df["last_seen"] = pd.to_datetime(
                        df["last_seen"], utc=True, format="ISO8601", errors="coerce"
                    )
                disposition = "WRITE_TRUNCATE" if first_chunk else "WRITE_APPEND"
                job_config = bigquery.LoadJobConfig(
                    schema=xref_schema, write_disposition=disposition
                )
                self.client.load_table_from_dataframe(
                    df, xref_staging, job_config=job_config
                ).result()
                first_chunk = False
                xref_rows_written += len(chunk)

            # MERGE: upsert new/changed rows, delete stale rows for affected bn_ids
            if affected_bn_ids:
                # Delete stale xref rows for affected clusters not in staging
                delete_sql = f"""
                DELETE FROM `{self.xref_table}`
                WHERE bn_id IN (SELECT bn_id FROM `{bn_id_table_id}`)
                  AND identifier_key NOT IN (SELECT identifier_key FROM `{xref_staging}`)
                """
                self._run_query(delete_sql, "xref_delete_stale")

            # Upsert staged rows (works for both new-only and affected clusters)
            merge_sql = f"""
            MERGE `{self.xref_table}` AS target
            USING `{xref_staging}` AS source
            ON target.identifier_key = source.identifier_key
            WHEN MATCHED THEN UPDATE SET
                bn_id = source.bn_id,
                identifier_type = source.identifier_type,
                identifier_value = source.identifier_value,
                cluster_tier = source.cluster_tier,
                cluster_size = source.cluster_size,
                is_hcp = source.is_hcp,
                is_shared_workstation = source.is_shared_workstation,
                last_seen = source.last_seen,
                source_profile = source.source_profile,
                is_bot = source.is_bot,
                cluster_health_score = source.cluster_health_score,
                is_suspicious = source.is_suspicious
            WHEN NOT MATCHED THEN INSERT ROW
            """
            self._run_query(merge_sql, "xref_merge")
            print(
                f"    MERGED {xref_rows_written:,} xref rows for {len(affected_bn_ids):,} affected bn_ids",
                flush=True,
            )

            # Cleanup staging
            try:
                self.client.delete_table(xref_staging, not_found_ok=True)
            except Exception:
                pass

        # 7c: Update node index BEFORE deleting temp tables (stale delete needs bn_id_table_id)
        if not dry_run:
            import pandas as pd

            ni_staging = f"{self.project}.{self.staging_dataset}._tmp_node_index_{self.run_id[:8]}"
            ni_rows = [
                {
                    "identifier_key": k,
                    "bn_id": v,
                    "identifier_type": k.split(":", 1)[0],
                    "is_output": (
                        not self.person_anchoring_enabled
                        or k.split(":", 1)[0] in self.output_types
                    ),
                    "cluster_tier": self._bn_id_tiers.get(v, "unknown"),
                    "run_id": self.run_id,
                }
                for k, v in node_to_bn_id.items()
            ]
            # Delete stale node-index rows for affected bn_ids (even if all nodes dropped)
            if affected_bn_ids:
                delete_sql = f"""
                DELETE FROM `{self.node_index_table}`
                WHERE bn_id IN (SELECT bn_id FROM `{bn_id_table_id}`)
                """
                if ni_rows:
                    # If we have new rows, only delete those NOT in the new set
                    ni_schema = [
                        bigquery.SchemaField("identifier_key", "STRING"),
                        bigquery.SchemaField("bn_id", "STRING"),
                        bigquery.SchemaField("identifier_type", "STRING"),
                        bigquery.SchemaField("is_output", "BOOLEAN"),
                        bigquery.SchemaField("cluster_tier", "STRING"),
                        bigquery.SchemaField("run_id", "STRING"),
                    ]
                    df = pd.DataFrame(ni_rows)
                    job_config = bigquery.LoadJobConfig(
                        schema=ni_schema, write_disposition="WRITE_TRUNCATE"
                    )
                    self.client.load_table_from_dataframe(
                        df, ni_staging, job_config=job_config
                    ).result()
                    del df
                    delete_sql += f"""
                      AND identifier_key NOT IN (SELECT identifier_key FROM `{ni_staging}`)
                    """
                self._run_query(delete_sql, "node_index_delete_stale")

            # MERGE new rows if any
            if ni_rows:
                if not affected_bn_ids:
                    # Need to upload staging if we didn't already
                    ni_schema = [
                        bigquery.SchemaField("identifier_key", "STRING"),
                        bigquery.SchemaField("bn_id", "STRING"),
                        bigquery.SchemaField("identifier_type", "STRING"),
                        bigquery.SchemaField("is_output", "BOOLEAN"),
                        bigquery.SchemaField("cluster_tier", "STRING"),
                        bigquery.SchemaField("run_id", "STRING"),
                    ]
                    df = pd.DataFrame(ni_rows)
                    job_config = bigquery.LoadJobConfig(
                        schema=ni_schema, write_disposition="WRITE_TRUNCATE"
                    )
                    self.client.load_table_from_dataframe(
                        df, ni_staging, job_config=job_config
                    ).result()
                    del df
                merge_sql = f"""
                MERGE `{self.node_index_table}` AS target
                USING `{ni_staging}` AS source
                ON target.identifier_key = source.identifier_key
                WHEN MATCHED THEN UPDATE SET
                    bn_id = source.bn_id,
                    cluster_tier = source.cluster_tier,
                    run_id = source.run_id
                WHEN NOT MATCHED THEN INSERT ROW
                """
                self._run_query(merge_sql, "node_index_merge")
                try:
                    self.client.delete_table(ni_staging, not_found_ok=True)
                except Exception:
                    pass
            del ni_rows
            print(
                f"    Updated node index for {len(node_to_bn_id):,} nodes", flush=True
            )

        # 7d: Scoped neighbor update for affected clusters
        # Neighbors is a required product surface — must stay fresh every run.
        #
        # Scoping: use FINAL post-persistence bn_ids (not pre-run affected bn_ids).
        # After persistence/remap, some bn_ids merge (retired disappears, surviving
        # absorbs members) or split (new bn_ids appear). We need to cover:
        #   - All surviving bn_ids (may have new members from merges)
        #   - All retired bn_ids (old neighbor rows must be deleted)
        #   - Brand-new bn_ids from new clusters
        #
        # Error handling: failure is FATAL. A partial delete with no insert leaves
        # neighbors in a corrupt state — worse than not updating at all.

        # Build temp table of ALL final bn_ids from Union-Find output (post-remap)
        final_bn_ids = set(components_by_bn_id.keys())
        # Also include the original affected_bn_ids (some may have been retired
        # by persistence — their old neighbor rows still need to be deleted)
        all_neighbor_bn_ids = final_bn_ids | affected_bn_ids

        final_bn_id_table = f"{self.project}.{self.staging_dataset}._tmp_incr_final_bnids_{self.run_id[:8]}"
        print(
            f"  Updating neighbors for {len(all_neighbor_bn_ids):,} final+affected bn_ids...",
            flush=True,
        )
        neighbor_start = time.time()

        # Upload final bn_ids with their FULL component size (not xref-filtered size).
        # This ensures neighbor eligibility (2-50 members) matches the full rebuild path,
        # which counts total cluster membership, not just output-type members.
        bn_schema = [
            bigquery.SchemaField("bn_id", "STRING"),
            bigquery.SchemaField("component_size", "INT64"),
        ]
        self.client.delete_table(final_bn_id_table, not_found_ok=True)
        self.client.create_table(
            bigquery.Table(final_bn_id_table, bn_schema), exists_ok=True
        )
        UPLOAD_CHUNK = 50_000
        bn_rows = []
        for bn_id in all_neighbor_bn_ids:
            component_size = len(components_by_bn_id.get(bn_id, []))
            bn_rows.append({"bn_id": bn_id, "component_size": component_size})
        for i in range(0, len(bn_rows), UPLOAD_CHUNK):
            chunk = bn_rows[i : i + UPLOAD_CHUNK]
            errors = self.client.insert_rows_json(final_bn_id_table, chunk)
            if errors:
                raise RuntimeError(
                    f"Neighbor temp table upload failed: {len(errors)} rows rejected "
                    f"in chunk {i // UPLOAD_CHUNK + 1}. First error: {errors[0]}"
                )
        del bn_rows

        # Delete old neighbor rows for ALL affected + final bn_ids
        self.client.query(f"""
        DELETE FROM `{self.neighbors_table}`
        WHERE bn_id IN (SELECT bn_id FROM `{final_bn_id_table}`)
        """).result()

        # Regenerate neighbors for final clusters (SQL cross-product).
        # Eligibility uses FULL component_size (from components_by_bn_id) to match
        # the full rebuild path — not xref-filtered member count.
        # Neighbor pairs are still built from xref output members only.
        output_types_sql = (
            ", ".join(f"'{t}'" for t in self.output_types)
            if self.output_types
            else "'__none__'"
        )
        self.client.query(f"""
        INSERT INTO `{self.neighbors_table}`
          (bn_id, node_type, node_value, neighbor_type, neighbor_value,
           confidence, match_rule, source_system, first_seen, last_seen, cluster_tier)
        WITH cluster_members AS (
          SELECT x.bn_id, x.identifier_type, x.identifier_value, x.cluster_tier
          FROM `{self.xref_table}` x
          JOIN `{final_bn_id_table}` t ON x.bn_id = t.bn_id
          WHERE x.identifier_type IN ({output_types_sql})
        ),
        -- Use full component size from Union-Find (matches full rebuild eligibility)
        eligible AS (
          SELECT bn_id FROM `{final_bn_id_table}`
          WHERE component_size BETWEEN 2 AND 50
        ),
        pairs AS (
          SELECT
            c1.bn_id,
            c1.identifier_type AS node_type, c1.identifier_value AS node_value,
            c2.identifier_type AS neighbor_type, c2.identifier_value AS neighbor_value,
            c1.cluster_tier
          FROM cluster_members c1
          JOIN cluster_members c2
            ON c1.bn_id = c2.bn_id
            AND (c1.identifier_type < c2.identifier_type
                 OR (c1.identifier_type = c2.identifier_type AND c1.identifier_value < c2.identifier_value))
          JOIN eligible e ON c1.bn_id = e.bn_id
        )
        SELECT
          bn_id, node_type, node_value, neighbor_type, neighbor_value,
          0.0 AS confidence,
          'TRANSITIVE' AS match_rule,
          'transitive' AS source_system,
          CAST(NULL AS TIMESTAMP) AS first_seen,
          CAST(NULL AS TIMESTAMP) AS last_seen,
          cluster_tier
        FROM pairs
        """).result()

        # Fill metadata from hub (forward + reverse match)
        self.client.query(f"""
        UPDATE `{self.neighbors_table}` n
        SET n.confidence = h.confidence,
            n.match_rule = h.match_rule,
            n.source_system = h.source_system,
            n.first_seen = h.first_seen,
            n.last_seen = h.last_seen
        FROM `{self.hub_table}` h
        WHERE n.node_type = h.identifier_a_type
          AND n.node_value = h.identifier_a_value
          AND n.neighbor_type = h.identifier_b_type
          AND n.neighbor_value = h.identifier_b_value
          AND n.bn_id IN (SELECT bn_id FROM `{final_bn_id_table}`)
        """).result()
        self.client.query(f"""
        UPDATE `{self.neighbors_table}` n
        SET n.confidence = h.confidence,
            n.match_rule = h.match_rule,
            n.source_system = h.source_system,
            n.first_seen = h.first_seen,
            n.last_seen = h.last_seen
        FROM `{self.hub_table}` h
        WHERE n.node_type = h.identifier_b_type
          AND n.node_value = h.identifier_b_value
          AND n.neighbor_type = h.identifier_a_type
          AND n.neighbor_value = h.identifier_a_value
          AND n.bn_id IN (SELECT bn_id FROM `{final_bn_id_table}`)
        """).result()

        # Cleanup final bn_id temp table
        self.client.delete_table(final_bn_id_table, not_found_ok=True)

        neighbor_elapsed = time.time() - neighbor_start
        print(
            f"    Neighbors updated for {len(all_neighbor_bn_ids):,} clusters ({neighbor_elapsed:.1f}s)"
        )
        self.stats["incremental_neighbors"] = {
            "elapsed": neighbor_elapsed,
            "final_bn_ids": len(final_bn_ids),
            "affected_bn_ids": len(affected_bn_ids),
            "total_scoped": len(all_neighbor_bn_ids),
        }

        # Cleanup temp tables (after neighbor update which needs bn_id_table_id)
        for t in [temp_table_id, bn_id_table_id]:
            try:
                self.client.delete_table(t, not_found_ok=True)
            except Exception:
                pass

        # 7e: Write metrics (merge log + persistence already written after Step 6)
        self.write_metrics(dry_run=dry_run)

        # Keep bn_id_manifest fresh on incremental so downstream preflight /
        # ops dashboards don't look stuck on the last full-rebuild PROMOTED.
        try:
            from shared.identity_hub_promote import write_manifest

            manifest_table = f"{self.project}.{self.output_dataset}.bn_id_manifest"
            write_manifest(
                self.client,
                manifest_table,
                self.run_id,
                "INCREMENTAL_PROMOTED",
            )
            print(
                f"    Manifest INCREMENTAL_PROMOTED: run_id={self.run_id[:8]}",
                flush=True,
            )
        except Exception as e:
            print(f"    WARNING: incremental manifest update failed: {e}", flush=True)

        # Store stats
        uf_stats = self.stats.get("union_find", {})
        persist_stats = self.stats.get("persistence", {})
        stats.update(
            {
                "components": uf_stats.get("components", 0),
                "tier1": uf_stats.get("tier1_components", 0),
                "tier2": uf_stats.get("tier2_components", 0),
                "merges": persist_stats.get("merges", 0),
                "splits": persist_stats.get("splits", 0),
                "xref_rows_written": xref_rows_written,
            }
        )
        self.stats["subset_union_find"] = stats
        return stats

    # REMOVED: _run_incremental_merge (Sprint 5) — replaced by _run_subset_union_find
    def _classify_single_identifier(self, id_type: str) -> str:
        """Classify a single identifier type as tier1 or tier2."""
        if id_type in self.tier1_types:
            return "tier1"
        if id_type in self.tier2_types:
            return "tier2"
        return "tier2"  # Default to tier2 for behavioral identifiers

    def _classify_pair(self, edge) -> str:
        """Classify an edge's tier based on its identifier types."""
        types = {edge.identifier_a_type, edge.identifier_b_type}
        if types & self.tier1_types:
            return "tier1"
        if types & self.tier2_types:
            return "tier2"
        return "tier2"

    def _add_plus_tag_bridge_if_allowed(
        self,
        email: str,
        source_system: str,
        source_ts: str,
    ) -> bool:
        """Create a plus-tag bridge edge if the email has a plus-tag on an allowlisted domain.

        Returns True if a bridge was created, False otherwise.
        """
        if not self._plus_tag_bridge_enabled:
            return False
        base_email = _strip_plus_tag(email, self._plus_tag_allowed_domains)
        if base_email == email:
            return False
        self._add_edge(
            HubEdge(
                identifier_a=email,
                identifier_a_type="email",
                identifier_b=base_email,
                identifier_b_type="email",
                source_system=source_system,
                link_type="deterministic",
                match_rule="EMAIL_PLUS_TAG_BRIDGE",
                confidence=1.0,
                first_seen=source_ts,
                last_seen=source_ts,
            )
        )
        return True

    def _add_dot_normalization_bridge_if_allowed(
        self,
        email: str,
        source_system: str,
        source_ts: str,
    ) -> bool:
        """Create a dot-normalization bridge edge if email has dots on an allowlisted domain.

        Gmail-like systems treat 'john.smith@gmail.com' and 'johnsmith@gmail.com' as the same mailbox.
        Returns True if a bridge was created, False otherwise.
        """
        if not self._dot_normalization_bridge_enabled:
            return False
        normalized_email = _strip_dots_in_local(
            email, self._dot_normalization_allowed_domains
        )
        if normalized_email == email:
            return False
        self._add_edge(
            HubEdge(
                identifier_a=email,
                identifier_a_type="email",
                identifier_b=normalized_email,
                identifier_b_type="email",
                source_system=source_system,
                link_type="deterministic",
                match_rule="EMAIL_DOT_NORMALIZATION",
                confidence=1.0,
                first_seen=source_ts,
                last_seen=source_ts,
            )
        )
        return True

    def _timestamp_filter(self, field: str, indent: str = "    ") -> str:
        """Build a WHERE clause fragment for date filtering."""
        clauses = []
        if self.start_date:
            clauses.append(f"{indent}AND {field} >= TIMESTAMP('{self.start_date}')")
        if self.end_date:
            clauses.append(f"{indent}AND {field} < TIMESTAMP('{self.end_date}')")
        return "\n".join(clauses)

    def _dry_run_query(self, query: str, label: str) -> None:
        """Execute a dry-run query and print estimated bytes."""
        try:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            job = self.client.query(query, job_config=job_config)
            gb = (job.total_bytes_processed or 0) / 1e9
            print(f"    [{label}] Estimated: {gb:.2f} GB")
        except Exception as e:
            print(f"    [{label}] Dry-run error: {e}")

    # ─── Phase 0: localStorage extraction (NEW) ────────────────

    def connect_localstorage(self, dry_run: bool = False) -> int:
        """
        Extract identity keys from BN_Acceptor.acceptor_events.
        Sources: allLocalStorage JSON, userTracking top-level fields, allCookies string.
        Produces edges: bnfpvid <-> (mc_euid | client_id | fbp | clarity_user | dmd_tag | ...).
        """
        if not self._is_connector_enabled("localstorage"):
            print("  [localstorage] Skipped (disabled)")
            return 0

        print("  [localstorage] Extracting acceptor_events identifiers...")
        cfg = self.connectors_config["localstorage"]
        confidence = cfg.get("confidence", 1.0)
        source_ds = cfg.get("source_dataset", "BN_Acceptor")
        source_tbl = cfg.get("source_table", "acceptor_events")
        keys_config = cfg.get("keys", {})
        cookie_keys = cfg.get("cookie_keys", {})

        # Build JSON extraction for each localStorage/userTracking key
        json_extracts = []
        for key_name, key_cfg in keys_config.items():
            json_path = key_cfg["json_path"]
            split_char = key_cfg.get("split_char")
            if split_char:
                # Extract only the first segment before split_char
                # e.g., clarity "abc^2^xyz" -> "abc"
                json_extracts.append(
                    f"    SPLIT(JSON_VALUE(data, '{json_path}'), '{split_char}')[OFFSET(0)] AS {key_name}"
                )
            else:
                json_extracts.append(
                    f"    JSON_VALUE(data, '{json_path}') AS {key_name}"
                )

        # Build cookie extraction expressions (parsed from allCookies string)
        cookie_extracts = []
        for ck_name, ck_cfg in cookie_keys.items():
            cookie_name = ck_cfg["cookie_name"]
            split_char = ck_cfg.get("split_char")
            regex_post = ck_cfg.get("regex_extract")
            # Extract cookie value using regex: cookie_name=VALUE up to ; or end
            base_expr = (
                f"REGEXP_EXTRACT(JSON_VALUE(data, '$.userTracking.allCookies'), "
                f"r'(?:^|;\\s*){cookie_name}=([^;]+)')"
            )
            if split_char:
                # Take first segment before split_char (e.g., user_id^version^... -> user_id)
                base_expr = f"SPLIT({base_expr}, '{split_char}')[OFFSET(0)]"
            elif regex_post:
                # Apply a second regex to extract a sub-portion of the cookie value
                # e.g., _clck: 128s8it%5E2%5Eg4o -> extract '128s8it' via r'([^%^]+)'
                base_expr = f"REGEXP_EXTRACT({base_expr}, r'{regex_post}')"
            cookie_extracts.append(f"    {base_expr} AS {ck_name}")

        # Extra fields used to resolve WP site for the per-event wp_user_id
        # (visitor_id_ut). Derived from the acceptor JSON itself so we don't
        # need any GA4 lookup. siteName is the condition code (als/mya/par/...);
        # host + path disambiguate hcp / forum / main install.
        site_extracts = [
            "    LOWER(JSON_VALUE(data, '$.siteMetadata.siteName')) AS acceptor_site_name",
            "    LOWER(REGEXP_EXTRACT(JSON_VALUE(data, '$.pageDetails.url'), r'https?://([^/]+)/')) AS acceptor_host",
            "    LOWER(REGEXP_EXTRACT(JSON_VALUE(data, '$.pageDetails.url'), r'https?://[^/]+(/[^?#]*)')) AS acceptor_path",
        ]

        all_extracts = json_extracts + cookie_extracts + site_extracts
        extract_select = ",\n".join(all_extracts)
        ts_filter = self._timestamp_filter("ae.publish_time", indent="      ")

        # Build UNION ALL subqueries — one per key — to produce pre-deduplicated edges.
        union_parts = []

        # bnfpvid <-> each localStorage/userTracking key
        for key_name, key_cfg in keys_config.items():
            if key_name == "bnfpvid":
                continue
            id_type = key_cfg.get("id_type", key_name)
            invalid_vals = key_cfg.get("invalid_values", [])
            min_len = key_cfg.get("min_length", self.min_identifier_length)
            invalid_filter = ""
            if invalid_vals:
                quoted = ", ".join(f"'{v}'" for v in invalid_vals)
                invalid_filter = f"\n    AND {key_name} NOT IN ({quoted})"
            union_parts.append(f"""
  SELECT
    bnfpvid AS anchor_value,
    'bnfpvid' AS anchor_type,
    '{id_type}' AS id_type,
    {key_name} AS id_value,
    MIN(publish_time) AS first_seen,
    MAX(publish_time) AS last_seen
  FROM base
  WHERE bnfpvid IS NOT NULL AND {key_name} IS NOT NULL
    AND LENGTH(TRIM({key_name})) >= {min_len}{invalid_filter}
  GROUP BY bnfpvid, {key_name}""")

        # bnfpvid <-> each cookie key
        for ck_name, ck_cfg in cookie_keys.items():
            id_type = ck_cfg.get("id_type", ck_name)
            union_parts.append(f"""
  SELECT
    bnfpvid AS anchor_value,
    'bnfpvid' AS anchor_type,
    '{id_type}' AS id_type,
    {ck_name} AS id_value,
    MIN(publish_time) AS first_seen,
    MAX(publish_time) AS last_seen
  FROM base
  WHERE bnfpvid IS NOT NULL AND {ck_name} IS NOT NULL
    AND LENGTH(TRIM({ck_name})) >= {self.min_identifier_length}
  GROUP BY bnfpvid, {ck_name}""")

        # Cross-anchor: client_id <-> {all other identifiers available in base CTE}
        # This runs REGARDLESS of whether bnfpvid is present in the row — so
        # first-visit users without bnfpvid still get cross-linked via client_id.
        if self.xa_localstorage_client_id:
            # Build dynamic xa_targets from ALL keys + cookie_keys except bnfpvid/client_id itself
            xa_targets = {}
            for key_name, key_cfg in keys_config.items():
                if key_name in ("bnfpvid", "client_id_ut"):
                    continue
                id_type = key_cfg.get("id_type", key_name)
                min_len = key_cfg.get("min_length", self.min_identifier_length)
                xa_targets[key_name] = (id_type, min_len)
            for ck_name, ck_cfg in cookie_keys.items():
                xa_targets[ck_name] = (
                    ck_cfg.get("id_type", ck_name),
                    self.min_identifier_length,
                )
            for col_name, (id_type, min_len) in xa_targets.items():
                union_parts.append(f"""
  SELECT
    CAST(client_id_ut AS STRING) AS anchor_value,
    'client_id' AS anchor_type,
    '{id_type}' AS id_type,
    CAST({col_name} AS STRING) AS id_value,
    MIN(publish_time) AS first_seen,
    MAX(publish_time) AS last_seen
  FROM base
  WHERE client_id_ut IS NOT NULL AND {col_name} IS NOT NULL
    AND LENGTH(TRIM(CAST(client_id_ut AS STRING))) >= {self.min_identifier_length}
    AND LENGTH(TRIM(CAST({col_name} AS STRING))) >= {min_len}
  GROUP BY client_id_ut, {col_name}""")

        union_sql = "\n  UNION ALL\n".join(union_parts)

        # acceptor_events DOES carry hostname/site context — it's just buried
        # in JSON paths the original connector didn't extract. visitor_id
        # (= wp_user_id) is per-WP-site, so without site qualification the
        # same numeric id collapses different people across sites.
        #
        # Resolver: acceptor JSON itself, fields siteMetadata.siteName,
        # pageDetails.url. Verified 99.96% accurate against unambiguous
        # wp_user_id ground truth across 30 days of events.
        #
        # Site resolution rules (siteName is the condition code, e.g. 'als'):
        #   host hcp.X            -> bn{siteName}hcpprd
        #   host forums?.X        -> bn{siteName}forumprd
        #   path /forums/...      -> bn{siteName}forumprd
        #   bare/www X            -> bn{siteName}prd
        #
        # Two-tier lookup in qualified_pairs:
        #   1. acceptor-derived dominant site for (bnfpvid|client_id) — fires
        #      whenever the anchor has agreed-upon site context across events.
        #   2. wordpress_users wp_user_id-unambiguous fallback — fires for the
        #      ~35% of wp_user_ids that exist on exactly one WP install.
        # Unresolvable rows are dropped to prevent reintroducing collision.

        # CRITICAL memory optimization: wrap the UNION in an outer GROUP BY so
        # BigQuery deduplicates pairs on its side BEFORE streaming rows to
        # Python. Without this, acceptor_events produces ~34M edge rows for a
        # 90-day window, which causes MemoryError in Python. With the outer
        # GROUP BY, BigQuery collapses to ~3-5M unique pairs before transfer.
        query = f"""
-- Phase 0: Extract identity keys from acceptor_events
-- Sources: allLocalStorage, userTracking fields, allCookies
-- Produce pre-deduplicated edges anchored on bnfpvid (when present) OR client_id.
WITH base AS (
  SELECT
    ae.publish_time,
{extract_select}
  FROM `{self._fqt(source_ds, source_tbl)}` ae
  WHERE JSON_VALUE(ae.data, '$.eventMetadata.eventName') = 'page_load'
      {ts_filter}
),
all_pairs AS (
{union_sql}
),
-- Per-event WP site code derived from acceptor JSON (siteName + URL).
-- Each row of `base` maps to exactly one (bnfpvid, client_id, wp_user_id)
-- triple AND one wp_site, computed from siteName/host/path on that event.
base_with_site AS (
  SELECT
    *,
    CASE
      WHEN acceptor_site_name IS NULL OR acceptor_site_name IN ('', 'undefined', 'null') THEN NULL
      WHEN acceptor_host IS NOT NULL AND STARTS_WITH(acceptor_host, 'hcp.')
        THEN CONCAT('bn', acceptor_site_name, 'hcpprd')
      WHEN acceptor_host IS NOT NULL
           AND (STARTS_WITH(acceptor_host, 'forums.') OR STARTS_WITH(acceptor_host, 'forum.'))
        THEN CONCAT('bn', acceptor_site_name, 'forumprd')
      WHEN acceptor_path IS NOT NULL AND REGEXP_CONTAINS(acceptor_path, r'^/forums?/')
        THEN CONCAT('bn', acceptor_site_name, 'forumprd')
      ELSE CONCAT('bn', acceptor_site_name, 'prd')
    END AS event_wp_site
  FROM base
),
-- Resolver 1a: bnfpvid -> dominant WP site, from acceptor events themselves.
-- Only emits when ALL of a bnfpvid's events agree on a single site.
acceptor_bnfpvid_site AS (
  SELECT bnfpvid, ANY_VALUE(event_wp_site) AS wp_site
  FROM base_with_site
  WHERE bnfpvid IS NOT NULL AND event_wp_site IS NOT NULL
  GROUP BY bnfpvid
  HAVING COUNT(DISTINCT event_wp_site) = 1
),
-- Resolver 1b: client_id -> dominant WP site (same logic).
acceptor_client_id_site AS (
  SELECT CAST(client_id_ut AS STRING) AS client_id, ANY_VALUE(event_wp_site) AS wp_site
  FROM base_with_site
  WHERE client_id_ut IS NOT NULL AND event_wp_site IS NOT NULL
  GROUP BY client_id_ut
  HAVING COUNT(DISTINCT event_wp_site) = 1
),
-- Resolver 2: wp_user_id -> site fallback, when the integer is unambiguous
-- in wordpress_users (~35% of wp_user_ids).
wp_user_unambig AS (
  SELECT CAST(id AS STRING) AS wp_user_id, ANY_VALUE(LOWER(TRIM(site))) AS wp_site
  FROM `bi-data-391216.wordpress_data.wordpress_users`
  WHERE site IS NOT NULL AND TRIM(site) != ''
  GROUP BY id
  HAVING COUNT(DISTINCT site) = 1
),
qualified_pairs AS (
  SELECT
    p.anchor_value,
    p.anchor_type,
    p.id_type,
    CASE
      WHEN p.id_type != 'wp_user_id' THEN p.id_value
      WHEN p.anchor_type = 'bnfpvid' AND ab.wp_site IS NOT NULL
        THEN CONCAT(ab.wp_site, ':', p.id_value)
      WHEN p.anchor_type = 'client_id' AND ac.wp_site IS NOT NULL
        THEN CONCAT(ac.wp_site, ':', p.id_value)
      WHEN w.wp_site IS NOT NULL
        THEN CONCAT(w.wp_site, ':', p.id_value)
      ELSE NULL
    END AS id_value,
    p.first_seen,
    p.last_seen
  FROM all_pairs p
  LEFT JOIN acceptor_bnfpvid_site ab
    ON p.id_type = 'wp_user_id' AND p.anchor_type = 'bnfpvid' AND p.anchor_value = ab.bnfpvid
  LEFT JOIN acceptor_client_id_site ac
    ON p.id_type = 'wp_user_id' AND p.anchor_type = 'client_id' AND p.anchor_value = ac.client_id
  LEFT JOIN wp_user_unambig w
    ON p.id_type = 'wp_user_id' AND p.id_value = w.wp_user_id
)
-- Outer GROUP BY collapses duplicate (anchor,id) pairs across all UNION
-- subqueries. Pushes aggregation to BigQuery instead of Python memory.
SELECT
  anchor_value,
  anchor_type,
  id_type,
  id_value,
  MIN(first_seen) AS first_seen,
  MAX(last_seen) AS last_seen
FROM qualified_pairs
WHERE id_value IS NOT NULL  -- drop wp_user_id rows that couldn't be site-qualified
GROUP BY anchor_value, anchor_type, id_type, id_value
"""

        if dry_run:
            self._dry_run_query(query, "localstorage")
            return 0

        start = time.time()

        # Build invalid values filter for SQL
        invalid_sql = ", ".join(f"'{v}'" for v in self.invalid_values if v)

        # Full rebuild (BQ staging enabled): INSERT directly into staging table.
        # No Python round-trip — previously ~5300s, now ~120s.
        if getattr(self, "_bq_staged", False):
            select_sql = f"""
            SELECT
              anchor_type AS identifier_a_type,
              anchor_value AS identifier_a_value,
              id_type AS identifier_b_type,
              id_value AS identifier_b_value,
              'acceptor' AS source_system,
              'deterministic' AS link_type,
              'LOCALSTORAGE_COOCCURRENCE' AS match_rule,
              {confidence} AS confidence,
              {confidence} AS base_confidence,
              1.0 AS identity_cap,
              first_seen,
              last_seen
            FROM (
              {query}
            )
            WHERE anchor_value IS NOT NULL
              AND id_value IS NOT NULL
              AND LENGTH(anchor_value) >= {self.min_identifier_length}
              AND anchor_value NOT IN ({invalid_sql})
              AND id_value NOT IN ({invalid_sql})
            """
            try:
                count = self._insert_connector_edges(select_sql, "localstorage")
            except Exception as e:
                print(f"    Error staging localStorage edges: {e}")
                import traceback

                traceback.print_exc()
                return 0

            elapsed = time.time() - start
            self.stats["localstorage"] = {"edges": count, "elapsed": elapsed}
            print(
                f"    Staged {count:,} localStorage edges directly in BQ ({elapsed:.1f}s)"
            )
            return count

        # Incremental mode: stream through Python (smaller dataset — lookback window only).
        # The Python path is acceptable here because incremental lookback windows
        # produce far fewer rows than a full rebuild (~100K-500K vs ~5M).
        try:
            result_iter = self._run_query(query, page_size=50000)
        except Exception as e:
            print(f"    Error querying localStorage: {e}")
            return 0

        count = 0
        source_rows = 0
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

        for row in result_iter:
            source_rows += 1
            anchor_value = row["anchor_value"]
            anchor_type = row["anchor_type"]
            id_type = row["id_type"]
            id_value = row["id_value"]
            first_seen = row["first_seen"].isoformat() if row["first_seen"] else now_iso
            last_seen = row["last_seen"].isoformat() if row["last_seen"] else now_iso

            if (
                not anchor_value
                or len(anchor_value) < self.min_identifier_length
                or anchor_value in self.invalid_values
            ):
                continue
            if not id_value or id_value in self.invalid_values:
                continue

            self._add_edge(
                HubEdge(
                    identifier_a=anchor_value,
                    identifier_a_type=anchor_type,
                    identifier_b=id_value,
                    identifier_b_type=id_type,
                    source_system="acceptor",
                    link_type="deterministic",
                    match_rule="LOCALSTORAGE_COOCCURRENCE",
                    confidence=confidence,
                    first_seen=first_seen,
                    last_seen=last_seen,
                )
            )
            count += 1

            if source_rows % 1_000_000 == 0:
                _elapsed = time.time() - start
                _rate = int(source_rows / _elapsed) if _elapsed > 0 else 0
                try:
                    import resource

                    _rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
                except Exception:
                    _rss = 0
                print(
                    f"    Progress: {source_rows:,} rows | {count:,} edges | "
                    f"{_rate:,} rows/s | {_elapsed:.0f}s",
                    flush=True,
                )

        elapsed = time.time() - start
        self.stats["localstorage"] = {
            "edges": count,
            "source_rows": source_rows,
            "elapsed": elapsed,
        }
        print(
            f"    Imported {count:,} localStorage edges from {source_rows:,} events ({elapsed:.1f}s)"
        )
        return count

    # ─── Phase 0b: bio_acceptor flattened identity bridge ──────

    def connect_bio_acceptor(self, dry_run: bool = False) -> int:
        """
        Read flattened identity columns from df_warehouse_intermediate.int_bio_acceptor.

        This table has pre-parsed columns that are richer than the raw
        acceptor_events JSON extraction:
          - user_tracking_all_local_storage_bnfpvid  (INT64)
          - user_tracking_all_local_storage_mc_euid
          - user_tracking_aim_payload_npi_number  <- direct NPI<->bnfpvid link!
          - user_tracking_aim_payload_dgid
          - user_tracking_aim_payload_identity_type
          - user_tracking_clarity_user
          - user_tracking_fbp                     <- direct Meta pixel link
          - traffic_source_referring_pvid         <- cross-site bridge

        Produces bnfpvid-anchored edges for each (bnfpvid, other_id) combination.
        Partitioned by DATE(latest_publish_time), clustered by pvid.
        """
        if not self._is_connector_enabled("bio_acceptor"):
            print("  [bio_acceptor] Skipped (disabled)")
            return 0

        print("  [bio_acceptor] Extracting int_bio_acceptor flat identity columns...")
        cfg = self.connectors_config.get("bio_acceptor", {})
        confidence = cfg.get("confidence", 1.0)
        source_ds = cfg.get("source_dataset", "df_warehouse_intermediate")
        source_tbl = cfg.get("source_table", "int_bio_acceptor")
        fqt = self._fqt(source_ds, source_tbl)

        # Build date filter on latest_publish_time (partition column)
        date_clauses = []
        if self.start_date:
            date_clauses.append(f"AND DATE(latest_publish_time) >= '{self.start_date}'")
        if self.end_date:
            date_clauses.append(f"AND DATE(latest_publish_time) < '{self.end_date}'")
        date_filter = "\n    ".join(date_clauses)

        # Target columns with their id_types
        # (column_name, id_type, needs_cast_to_string)
        id_columns = [
            ("user_tracking_all_local_storage_mc_euid", "mc_euid", False),
            ("user_tracking_aim_payload_npi_number", "npi_number", False),
            ("user_tracking_aim_payload_dgid", "aim_dgid", False),
            ("user_tracking_clarity_user", "clarity_user", False),
            ("user_tracking_fbp", "fbp", False),
            (
                "traffic_source_referring_pvid",
                "bnfpvid",
                False,
            ),  # Referring pvid links sites
        ]

        # Anchor on bnfpvid (cast from INT64 to STRING for consistency with other connectors)
        anchor_col = "user_tracking_all_local_storage_bnfpvid"
        anchor_expr = f"CAST({anchor_col} AS STRING)"

        union_parts = []
        for col_name, id_type, _ in id_columns:
            union_parts.append(f"""
  SELECT
    {anchor_expr} AS anchor_value,
    'bnfpvid' AS anchor_type,
    '{id_type}' AS id_type,
    CAST({col_name} AS STRING) AS id_value,
    MIN(latest_publish_time) AS first_seen,
    MAX(latest_publish_time) AS last_seen
  FROM `{fqt}`
  WHERE {anchor_col} IS NOT NULL
    AND {col_name} IS NOT NULL
    AND LENGTH(TRIM(CAST({col_name} AS STRING))) >= {self.min_identifier_length}
    {date_filter}
  GROUP BY {anchor_expr}, {col_name}""")

        union_sql = "\n  UNION ALL\n".join(union_parts)

        query = f"""
-- Phase 0b: bio_acceptor flat identity columns
-- Sources: df_warehouse_intermediate.int_bio_acceptor (preprocessed acceptor_events)
-- Produces bnfpvid-anchored edges for all sibling identity columns.
{union_sql}
"""

        if dry_run:
            self._dry_run_query(query, "bio_acceptor")
            return 0

        start = time.time()
        invalid_sql = ", ".join(f"'{v}'" for v in self.invalid_values if v)

        # Full rebuild: direct BQ staging (skip Python round-trip)
        if getattr(self, "_bq_staged", False):
            select_sql = f"""
            SELECT
              anchor_type AS identifier_a_type,
              anchor_value AS identifier_a_value,
              id_type AS identifier_b_type,
              id_value AS identifier_b_value,
              'bio_acceptor' AS source_system,
              'deterministic' AS link_type,
              'BIO_ACCEPTOR_FLAT' AS match_rule,
              {confidence} AS confidence,
              {confidence} AS base_confidence,
              1.0 AS identity_cap,
              first_seen,
              last_seen
            FROM (
              {query}
            )
            WHERE anchor_value IS NOT NULL
              AND id_value IS NOT NULL
              AND LENGTH(anchor_value) >= {self.min_identifier_length}
              AND anchor_value NOT IN ({invalid_sql})
              AND id_value NOT IN ({invalid_sql})
            """
            try:
                count = self._insert_connector_edges(select_sql, "bio_acceptor")
            except Exception as e:
                print(f"    Error staging bio_acceptor edges: {e}")
                import traceback

                traceback.print_exc()
                return 0

            elapsed = time.time() - start
            self.stats["bio_acceptor"] = {"edges": count, "elapsed": elapsed}
            print(
                f"    Staged {count:,} bio_acceptor edges directly in BQ ({elapsed:.1f}s)"
            )
            return count

        # Incremental mode: stream through Python (smaller dataset)
        try:
            result_iter = self._run_query(query, page_size=50000)
        except Exception as e:
            print(f"    Error querying bio_acceptor: {e}")
            self.stats["bio_acceptor"] = {
                "edges": 0,
                "elapsed": 0,
                "error": str(e)[:200],
            }
            return 0

        count = 0
        source_rows = 0
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

        for row in result_iter:
            source_rows += 1
            anchor_value = row["anchor_value"]
            id_value = row["id_value"]
            first_seen = row["first_seen"].isoformat() if row["first_seen"] else now_iso
            last_seen = row["last_seen"].isoformat() if row["last_seen"] else now_iso

            if (
                not anchor_value
                or len(anchor_value) < self.min_identifier_length
                or anchor_value in self.invalid_values
            ):
                continue
            if not id_value or id_value in self.invalid_values:
                continue

            self._add_edge(
                HubEdge(
                    identifier_a=anchor_value,
                    identifier_a_type=row["anchor_type"],
                    identifier_b=id_value,
                    identifier_b_type=row["id_type"],
                    source_system="bio_acceptor",
                    link_type="deterministic",
                    match_rule="BIO_ACCEPTOR_FLAT",
                    confidence=confidence,
                    first_seen=first_seen,
                    last_seen=last_seen,
                )
            )
            count += 1

        elapsed = time.time() - start
        self.stats["bio_acceptor"] = {
            "edges": count,
            "source_rows": source_rows,
            "elapsed": elapsed,
        }
        print(
            f"    Imported {count:,} bio_acceptor edges from {source_rows:,} rows ({elapsed:.1f}s)"
        )
        return count

    # ─── Phase 0c: Gravity Forms -> GA4 session bridge ──────────

    def connect_form_session_bridge(self, dry_run: bool = False) -> int:
        """
        Link Gravity Forms submissions to concurrent GA4 web sessions via
        (ip_address, site, time_window) triple-match.

        Why this matters: users who submit a form on one of our sites provide
        their email in the form AND already have bnfpvid/client_id in their
        browser. The form submission and the page_view event share IP+UA+time.
        This is the most reliable non-web-only bridge from email -> bnfpvid.

        Matching logic:
          gf.ip = ga.ip
          AND gf_site = ga_site  (after normalizing bn<code>prd -> <code>)
          AND |gf.date_created - ga.event_timestamp| <= 5 minutes

        Emits: email <-> client_id (confidence 0.85)
        The identity graph then transitively connects email <-> bnfpvid via
        existing client_id <-> bnfpvid edges from the localstorage connector.
        """
        if not self._is_connector_enabled("form_session_bridge"):
            print("  [form_session_bridge] Skipped (disabled)")
            return 0

        print(
            "  [form_session_bridge] Matching Gravity Forms submissions to GA4 sessions..."
        )
        cfg = self.connectors_config.get("form_session_bridge", {})
        confidence = cfg.get("confidence", 0.85)
        window_minutes = cfg.get("time_window_minutes", 5)

        # Date filter on gf_entry.date_created (no partitioning, but reduces scan)
        gf_date_clauses = []
        if self.start_date:
            gf_date_clauses.append(
                f"AND gfe.date_created >= TIMESTAMP('{self.start_date}')"
            )
        if self.end_date:
            gf_date_clauses.append(
                f"AND gfe.date_created < TIMESTAMP('{self.end_date}')"
            )
        gf_date_filter = "\n    ".join(gf_date_clauses)

        # Date filter on output_ga4_joined.event_date (partition column)
        ga_date_clauses = []
        if self.start_date:
            ga_date_clauses.append(f"AND gj.event_date >= DATE('{self.start_date}')")
        if self.end_date:
            ga_date_clauses.append(f"AND gj.event_date < DATE('{self.end_date}')")
        ga_date_filter = "\n    ".join(ga_date_clauses)

        query = f"""
-- Phase 0c: Gravity Forms -> GA4 session bridge
-- Match form submissions to concurrent GA4 page_view events by IP+site+time
WITH gf_submissions AS (
  SELECT
    gfe.entry_id,
    gfe.date_created AS submit_time,
    gfe.ip,
    -- Normalize site: bn<code>prd -> <code>
    LOWER(REGEXP_EXTRACT(gfe.site, r'^bn(.+?)prd$')) AS site_code,
    LOWER(TRIM(gfm.meta_value)) AS email
  FROM `BN_Warehouse.gravity_forms_entry` gfe
  JOIN `BN_Warehouse.gravity_forms_entry_meta` gfm
    ON gfe.entry_id = gfm.entry_id AND gfe.site = gfm.site
  WHERE gfe.ip IS NOT NULL
    AND gfm.meta_key IN ('1', '4', '5')
    AND REGEXP_CONTAINS(LOWER(TRIM(gfm.meta_value)), r'^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{{2,}}$')
    AND gfe.site LIKE 'bn%prd'
    {gf_date_filter}
),
ga_sessions AS (
  SELECT
    gj.primary_user_id AS client_id,
    CAST(gj.user_tracking_all_local_storage_bnfpvid AS STRING) AS bnfpvid,
    gj.event_timestamp,
    gj.ip_address,
    LOWER(gj.site_nm) AS site_code
  FROM `df_warehouse_output.output_ga4_joined` gj
  WHERE gj.ip_address IS NOT NULL
    AND gj.primary_user_id IS NOT NULL
    AND gj.site_nm IS NOT NULL
    {ga_date_filter}
)
-- Match: same IP, same site, within 5-minute window
SELECT
  gf.email,
  ga.client_id,
  ga.bnfpvid,
  MIN(gf.submit_time) AS first_seen,
  MAX(gf.submit_time) AS last_seen
FROM gf_submissions gf
JOIN ga_sessions ga
  ON gf.ip = ga.ip_address
  AND gf.site_code = ga.site_code
  AND ABS(TIMESTAMP_DIFF(ga.event_timestamp, gf.submit_time, MINUTE)) <= {window_minutes}
WHERE gf.email IS NOT NULL AND ga.client_id IS NOT NULL
GROUP BY gf.email, ga.client_id, ga.bnfpvid
"""

        if dry_run:
            self._dry_run_query(query, "form_session_bridge")
            return 0

        start = time.time()
        try:
            result_iter = self._run_query(query, page_size=10000)
        except Exception as e:
            print(f"    Error querying form_session_bridge: {e}")
            self.stats["form_session_bridge"] = {
                "edges": 0,
                "elapsed": 0,
                "error": str(e)[:200],
            }
            return 0

        count = 0
        source_rows = 0
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

        for row in result_iter:
            source_rows += 1
            email = row["email"]
            client_id = row["client_id"]
            bnfpvid = row["bnfpvid"]
            first_seen = row["first_seen"].isoformat() if row["first_seen"] else now_iso
            last_seen = row["last_seen"].isoformat() if row["last_seen"] else now_iso

            if not email or email in self.invalid_values:
                continue
            if not client_id or client_id in self.invalid_values:
                continue

            # Emit email <-> client_id
            self._add_edge(
                HubEdge(
                    identifier_a=email,
                    identifier_a_type="email",
                    identifier_b=client_id,
                    identifier_b_type="client_id",
                    source_system="gravity_forms",
                    link_type="probabilistic",
                    match_rule="GF_IP_TIME_SITE",
                    confidence=confidence,
                    first_seen=first_seen,
                    last_seen=last_seen,
                )
            )
            count += 1

            # If bnfpvid also present, emit email <-> bnfpvid directly
            if bnfpvid and bnfpvid not in self.invalid_values:
                self._add_edge(
                    HubEdge(
                        identifier_a=email,
                        identifier_a_type="email",
                        identifier_b=bnfpvid,
                        identifier_b_type="bnfpvid",
                        source_system="gravity_forms",
                        link_type="probabilistic",
                        match_rule="GF_IP_TIME_SITE",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

        elapsed = time.time() - start
        self.stats["form_session_bridge"] = {
            "edges": count,
            "source_rows": source_rows,
            "elapsed": elapsed,
        }
        print(
            f"    Imported {count:,} form_session_bridge edges from {source_rows:,} matches ({elapsed:.1f}s)"
        )
        return count

    # ─── Phase 0e: Mailchimp click -> GA4 session bridge ────────

    def connect_mc_click_bridge(self, dry_run: bool = False) -> int:
        """
        Link Mailchimp email click events to concurrent GA4 web sessions via
        (ip, time_window) match.

        Every click row in campaign_email_activity carries email_address, ip,
        and activity_timestamp. When the user lands on our site after clicking,
        GA4 records a page_view from the same IP within seconds. Joining the
        two gives us a direct deterministic-ish bridge from email to
        client_id/bnfpvid without needing mc_euid to be captured in localStorage.

        We also emit email <-> subscriber_hash (= MD5(lower(email))) as a
        deterministic edge because email_id in campaign_email_activity is
        literally MD5(lower(email_address)). This creates a second identifier
        type that's useful downstream for cross-referencing.

        Output edges:
          email <-> subscriber_hash         deterministic (1.0)
          email <-> client_id               probabilistic (0.85, IP+time)
          email <-> bnfpvid                 probabilistic (0.85, IP+time)
        """
        if not self._is_connector_enabled("mc_click_bridge"):
            print("  [mc_click_bridge] Skipped (disabled)")
            return 0

        print("  [mc_click_bridge] Matching Mailchimp clicks to GA4 sessions...")
        cfg = self.connectors_config.get("mc_click_bridge", {})
        confidence = cfg.get("confidence", 0.85)
        window_minutes = cfg.get("time_window_minutes", 5)

        # Date filter on click activity_timestamp
        click_date_clauses = []
        if self.start_date:
            click_date_clauses.append(
                f"AND activity_timestamp >= TIMESTAMP('{self.start_date}')"
            )
        if self.end_date:
            click_date_clauses.append(
                f"AND activity_timestamp < TIMESTAMP('{self.end_date}')"
            )
        click_date_filter = "\n    ".join(click_date_clauses)

        # Date filter on output_ga4_joined.event_date (partition)
        ga_date_clauses = []
        if self.start_date:
            ga_date_clauses.append(f"AND gj.event_date >= DATE('{self.start_date}')")
        if self.end_date:
            ga_date_clauses.append(f"AND gj.event_date < DATE('{self.end_date}')")
        ga_date_filter = "\n    ".join(ga_date_clauses)

        query = f"""
-- Phase 0e: Mailchimp click -> GA4 session bridge
WITH mc_clicks AS (
  SELECT DISTINCT
    LOWER(TRIM(email_address)) AS email,
    email_id AS subscriber_hash,
    activity_timestamp AS click_time,
    ip AS click_ip
  FROM `mailchimp_data.campaign_email_activity`
  WHERE action = 'click'
    AND email_address IS NOT NULL
    AND ip IS NOT NULL
    AND REGEXP_CONTAINS(LOWER(TRIM(email_address)), r'^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{{2,}}$')
    {click_date_filter}
),
ga_sessions AS (
  SELECT
    gj.primary_user_id AS client_id,
    CAST(gj.user_tracking_all_local_storage_bnfpvid AS STRING) AS bnfpvid,
    gj.event_timestamp,
    gj.ip_address
  FROM `df_warehouse_output.output_ga4_joined` gj
  WHERE gj.ip_address IS NOT NULL
    AND gj.primary_user_id IS NOT NULL
    {ga_date_filter}
)
SELECT
  mc.email,
  mc.subscriber_hash,
  ga.client_id,
  ga.bnfpvid,
  MIN(mc.click_time) AS first_seen,
  MAX(mc.click_time) AS last_seen
FROM mc_clicks mc
JOIN ga_sessions ga
  ON mc.click_ip = ga.ip_address
  AND ABS(TIMESTAMP_DIFF(ga.event_timestamp, mc.click_time, MINUTE)) <= {window_minutes}
GROUP BY mc.email, mc.subscriber_hash, ga.client_id, ga.bnfpvid
"""

        if dry_run:
            self._dry_run_query(query, "mc_click_bridge")
            return 0

        start = time.time()
        try:
            result_iter = self._run_query(query, page_size=10000)
        except Exception as e:
            print(f"    Error querying mc_click_bridge: {e}")
            self.stats["mc_click_bridge"] = {
                "edges": 0,
                "elapsed": 0,
                "error": str(e)[:200],
            }
            return 0

        count = 0
        source_rows = 0
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

        for row in result_iter:
            source_rows += 1
            email = row["email"]
            subscriber_hash = row["subscriber_hash"]
            client_id = row["client_id"]
            bnfpvid = row["bnfpvid"]
            first_seen = row["first_seen"].isoformat() if row["first_seen"] else now_iso
            last_seen = row["last_seen"].isoformat() if row["last_seen"] else now_iso

            if not email or email in self.invalid_values:
                continue

            # Deterministic: email <-> subscriber_hash
            if subscriber_hash and subscriber_hash not in self.invalid_values:
                self._add_edge(
                    HubEdge(
                        identifier_a=email,
                        identifier_a_type="email",
                        identifier_b=subscriber_hash,
                        identifier_b_type="subscriber_hash",
                        source_system="mailchimp",
                        link_type="deterministic",
                        match_rule="MC_EMAIL_HASH",
                        confidence=1.0,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

            # Probabilistic: email <-> client_id via click IP+time
            if client_id and client_id not in self.invalid_values:
                self._add_edge(
                    HubEdge(
                        identifier_a=email,
                        identifier_a_type="email",
                        identifier_b=client_id,
                        identifier_b_type="client_id",
                        source_system="mailchimp",
                        link_type="probabilistic",
                        match_rule="MC_CLICK_IP_TIME",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

            # Probabilistic: email <-> bnfpvid via click IP+time
            if bnfpvid and bnfpvid not in self.invalid_values:
                self._add_edge(
                    HubEdge(
                        identifier_a=email,
                        identifier_a_type="email",
                        identifier_b=bnfpvid,
                        identifier_b_type="bnfpvid",
                        source_system="mailchimp",
                        link_type="probabilistic",
                        match_rule="MC_CLICK_IP_TIME",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

        elapsed = time.time() - start
        self.stats["mc_click_bridge"] = {
            "edges": count,
            "source_rows": source_rows,
            "elapsed": elapsed,
        }
        print(
            f"    Imported {count:,} mc_click_bridge edges from {source_rows:,} matches ({elapsed:.1f}s)"
        )
        return count

    # ─── Phase 0f: GA4 URL mc_euid extraction ──────────────────

    def connect_ga4_mc_euid_bridge(self, dry_run: bool = False) -> int:
        """
        Extract mc_euid values embedded in GA4 page_location URLs.

        When a user clicks a Mailchimp email link, Mailchimp's click tracker
        redirects through its own servers and appends the recipient's mc_euid
        to the final landing URL as a query parameter. GA4 captures the
        landing URL (page_location), and we can parse out the mc_euid.

        CRITICAL: acceptor_events only captures mc_euid when it's in
        localStorage (requires the site to have the Mailchimp tracking script
        that writes it). But GA4 captures mc_euid from the URL on the VERY
        FIRST page view — no script required. In the 3-day sample this
        produces 13,599 email->mc_euid pairs that overlap with Mailchimp
        members (vs ~2,500 from acceptor localstorage).

        Emits:
          mc_euid <-> client_id           deterministic (1.0)
          mc_euid <-> bnfpvid             deterministic (1.0, when present)
        The existing mc_euid_bridge already links mc_euid <-> email, so the
        chain closes automatically in Union-Find.
        """
        if not self._is_connector_enabled("ga4_mc_euid_bridge"):
            print("  [ga4_mc_euid_bridge] Skipped (disabled)")
            return 0

        print(
            "  [ga4_mc_euid_bridge] Extracting mc_euid from GA4 page_location URLs..."
        )
        cfg = self.connectors_config.get("ga4_mc_euid_bridge", {})
        confidence = cfg.get("confidence", 1.0)

        # Date filter on event_date (partition column)
        date_clauses = []
        if self.start_date:
            date_clauses.append(f"AND event_date >= DATE('{self.start_date}')")
        if self.end_date:
            date_clauses.append(f"AND event_date < DATE('{self.end_date}')")
        date_filter = "\n    ".join(date_clauses)

        query = f"""
-- Phase 0f: Extract mc_euid from GA4 page_location URLs
SELECT DISTINCT
  REGEXP_EXTRACT(page_location, r'[?&]mc_euid=([a-z0-9]{{10}})') AS mc_euid,
  primary_user_id AS client_id,
  CAST(user_tracking_all_local_storage_bnfpvid AS STRING) AS bnfpvid,
  MIN(event_timestamp) AS first_seen,
  MAX(event_timestamp) AS last_seen
FROM `df_warehouse_output.output_ga4_joined`
WHERE page_location LIKE '%mc_euid=%'
  AND REGEXP_EXTRACT(page_location, r'[?&]mc_euid=([a-z0-9]{{10}})') IS NOT NULL
  AND primary_user_id IS NOT NULL
  {date_filter}
GROUP BY mc_euid, client_id, bnfpvid
"""

        if dry_run:
            self._dry_run_query(query, "ga4_mc_euid_bridge")
            return 0

        start = time.time()
        try:
            result_iter = self._run_query(query, page_size=50000)
        except Exception as e:
            print(f"    Error querying ga4_mc_euid_bridge: {e}")
            self.stats["ga4_mc_euid_bridge"] = {
                "edges": 0,
                "elapsed": 0,
                "error": str(e)[:200],
            }
            return 0

        count = 0
        source_rows = 0
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

        for row in result_iter:
            source_rows += 1
            mc_euid = row["mc_euid"]
            client_id = row["client_id"]
            bnfpvid = row["bnfpvid"]
            first_seen = row["first_seen"].isoformat() if row["first_seen"] else now_iso
            last_seen = row["last_seen"].isoformat() if row["last_seen"] else now_iso

            if not mc_euid or mc_euid in self.invalid_values or len(mc_euid) != 10:
                continue
            if not client_id or client_id in self.invalid_values:
                continue

            # mc_euid <-> client_id (deterministic — the URL was clicked by this session)
            self._add_edge(
                HubEdge(
                    identifier_a=mc_euid,
                    identifier_a_type="mc_euid",
                    identifier_b=client_id,
                    identifier_b_type="client_id",
                    source_system="ga4",
                    link_type="deterministic",
                    match_rule="GA4_URL_MC_EUID",
                    confidence=confidence,
                    first_seen=first_seen,
                    last_seen=last_seen,
                )
            )
            count += 1

            # mc_euid <-> bnfpvid (when localStorage is populated)
            if bnfpvid and bnfpvid not in self.invalid_values:
                self._add_edge(
                    HubEdge(
                        identifier_a=mc_euid,
                        identifier_a_type="mc_euid",
                        identifier_b=bnfpvid,
                        identifier_b_type="bnfpvid",
                        source_system="ga4",
                        link_type="deterministic",
                        match_rule="GA4_URL_MC_EUID",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

        elapsed = time.time() - start
        self.stats["ga4_mc_euid_bridge"] = {
            "edges": count,
            "source_rows": source_rows,
            "elapsed": elapsed,
        }
        print(
            f"    Imported {count:,} ga4_mc_euid_bridge edges from {source_rows:,} rows ({elapsed:.1f}s)"
        )
        return count

    # ─── Phase 0d: WordPress users -> wp_user_id bridge ─────────

    def connect_wp_user_session_bridge(self, dry_run: bool = False) -> int:
        """
        Emit email <-> wp_user_id edges from wordpress_users across all sites.

        This is distinct from wp_mc4wp_email_bridge (which only captures users
        with the MC4WP plugin). Here we pull every WordPress user with a valid
        email. The transitive linkage to bnfpvid/client_id happens via the
        existing localstorage connector, which captures wp_user_id in the
        userTracking.visitorId field whenever a logged-in user loads a page.
        """
        if not self._is_connector_enabled("wp_user_session_bridge"):
            print("  [wp_user_session_bridge] Skipped (disabled)")
            return 0

        print("  [wp_user_session_bridge] Bridging WordPress users to wp_user_id...")
        cfg = self.connectors_config.get("wp_user_session_bridge", {})
        confidence = cfg.get("confidence", 1.0)

        # No date filter — this is reference data (user accounts). Static.
        # Use raw user_registered (NULL when missing, not CURRENT_TIMESTAMP)
        # wp_user_id is site-scoped: WordPress assigns the same numeric ID
        # independently per site, so id=4629 on bnmshcpprd is a different
        # person from id=4629 on bnmyaforumprd. Qualify as `{site}:{id}`.
        query = """
-- Phase 0d: WordPress users -> wp_user_id bridge
SELECT DISTINCT
  LOWER(TRIM(user_email)) AS email,
  CONCAT(LOWER(TRIM(site)), ':', CAST(ID AS STRING)) AS wp_user_id,
  CAST(user_registered AS TIMESTAMP) AS source_ts
FROM `wordpress_data.wordpress_users`
WHERE user_email IS NOT NULL
  AND LENGTH(TRIM(user_email)) > 5
  AND REGEXP_CONTAINS(LOWER(TRIM(user_email)), r'^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$')
  AND site IS NOT NULL AND TRIM(site) != ''
"""

        if dry_run:
            self._dry_run_query(query, "wp_user_session_bridge")
            return 0

        start = time.time()
        try:
            result_iter = self._run_query(query, page_size=50000)
        except Exception as e:
            print(f"    Error querying wp_user_session_bridge: {e}")
            self.stats["wp_user_session_bridge"] = {
                "edges": 0,
                "elapsed": 0,
                "error": str(e)[:200],
            }
            return 0

        count = 0

        for row in result_iter:
            email = row["email"]
            wp_user_id = row["wp_user_id"]
            source_ts = self._ts_iso(row.get("source_ts"))

            if not email or email in self.invalid_values:
                continue
            if not wp_user_id or wp_user_id in self.invalid_values or wp_user_id == "0":
                continue

            self._add_plus_tag_bridge_if_allowed(email, "wordpress", source_ts)
            self._add_dot_normalization_bridge_if_allowed(email, "wordpress", source_ts)

            self._add_edge(
                HubEdge(
                    identifier_a=email,
                    identifier_a_type="email",
                    identifier_b=wp_user_id,
                    identifier_b_type="wp_user_id",
                    source_system="wordpress",
                    link_type="deterministic",
                    match_rule="WP_USER_EMAIL",
                    confidence=confidence,
                    first_seen=source_ts,
                    last_seen=source_ts,
                )
            )
            count += 1

        elapsed = time.time() - start
        self.stats["wp_user_session_bridge"] = {
            "edges": count,
            "elapsed": elapsed,
        }
        print(f"    Imported {count:,} wp_user_session_bridge edges ({elapsed:.1f}s)")
        return count

    def connect_surveyengine_bridge(self, dry_run: bool = False) -> int:
        """
        Emit email <-> bnfpvid edges from SurveyEngine submissions and responses.

        SurveyEngine is the in-house Laravel forms engine (configs/
        surveyengine.yaml) and is becoming the primary registration path. It is
        a separate product from every other form source in this file and has its
        own connector; do not fold it into another one.

        The edge is deterministic at confidence 1.0 because SurveyEngine records
        email and bnfpvid on the SAME ROW -- the link is directly observed, with
        no IP/time correlation and no inference step.

        Deliberately NOT emitted:
          pvid -- page-level, unique per row. Treating it as a person
                  identifier would fuse everyone who shares a pageview.
          guid -- declared on four tables, 0% populated on all of them.

        email and bnfpvid are the two keys SurveyEngine contributes, by design
        (confirmed 2026-08-19). bionews_uk is deliberately not part of this
        bridge; the SSO key reaches the graph via the acceptor's cookie capture,
        not through SurveyEngine.
        """
        if not self._is_connector_enabled("surveyengine_bridge"):
            print("  [surveyengine_bridge] Skipped (disabled)")
            return 0

        print("  [surveyengine_bridge] Bridging SurveyEngine email <-> bnfpvid...")
        cfg = self.connectors_config.get("surveyengine_bridge", {})
        confidence = cfg.get("confidence", 1.0)

        # Both tables carry the pair. Submissions are completed forms;
        # question_responses are per-answer rows and cover more people (24
        # bnfpvid vs 9 as of 2026-08-18), so both are unioned.
        query = """
-- Phase 0d2: SurveyEngine -> email/bnfpvid bridge
SELECT DISTINCT email, bnfpvid, source_ts FROM (
  SELECT
    LOWER(TRIM(email)) AS email,
    CAST(bnfpvid AS STRING) AS bnfpvid,
    CAST(created_at AS TIMESTAMP) AS source_ts
  FROM `surveyengine_data.se_submissions`
  WHERE deleted_at IS NULL

  UNION ALL

  SELECT
    LOWER(TRIM(respondent_email)),
    CAST(bnfpvid AS STRING),
    CAST(created_at AS TIMESTAMP)
  FROM `surveyengine_data.se_question_responses`
  WHERE deleted_at IS NULL
)
WHERE email IS NOT NULL
  AND LENGTH(email) > 5
  AND REGEXP_CONTAINS(email, r'^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$')
  AND bnfpvid IS NOT NULL
  AND TRIM(bnfpvid) != ''
"""

        # guid: the identity service's user key. Empty today, so this query
        # returns nothing and the loop below is a no-op. It begins producing
        # deterministic email<->guid edges the moment the column populates.
        guid_query = """
-- Phase 0d2b: SurveyEngine -> email/guid bridge (dormant until guid populates)
SELECT DISTINCT email, guid, source_ts FROM (
  SELECT LOWER(TRIM(email)) AS email, CAST(guid AS STRING) AS guid,
         CAST(created_at AS TIMESTAMP) AS source_ts
  FROM `surveyengine_data.se_submissions` WHERE deleted_at IS NULL
  UNION ALL
  SELECT LOWER(TRIM(respondent_email)), CAST(guid AS STRING),
         CAST(created_at AS TIMESTAMP)
  FROM `surveyengine_data.se_question_responses` WHERE deleted_at IS NULL
)
WHERE email IS NOT NULL
  AND LENGTH(email) > 5
  AND guid IS NOT NULL
  AND TRIM(guid) != ''
"""

        if dry_run:
            self._dry_run_query(query, "surveyengine_bridge")
            return 0

        start = time.time()
        try:
            result_iter = self._run_query(query, page_size=50000)
        except Exception as e:
            print(f"    Error querying surveyengine_bridge: {e}")
            self.stats["surveyengine_bridge"] = {
                "edges": 0,
                "elapsed": 0,
                "error": str(e)[:200],
            }
            return 0

        count = 0

        for row in result_iter:
            email = row["email"]
            bnfpvid = row["bnfpvid"]
            source_ts = self._ts_iso(row.get("source_ts"))

            if not email or email in self.invalid_values:
                continue
            if not bnfpvid or bnfpvid in self.invalid_values or bnfpvid == "0":
                continue

            self._add_plus_tag_bridge_if_allowed(email, "surveyengine", source_ts)
            self._add_dot_normalization_bridge_if_allowed(
                email, "surveyengine", source_ts
            )

            self._add_edge(
                HubEdge(
                    identifier_a=email,
                    identifier_a_type="email",
                    identifier_b=bnfpvid,
                    identifier_b_type="bnfpvid",
                    source_system="surveyengine",
                    link_type="deterministic",
                    match_rule="SURVEYENGINE_DIRECT",
                    confidence=confidence,
                    first_seen=source_ts,
                    last_seen=source_ts,
                )
            )
            count += 1

        # ── guid edges: dormant until the identity service populates the column ──
        try:
            guid_rows = self._run_query(guid_query, page_size=50000)
        except Exception as e:
            print(f"    Error querying surveyengine guid bridge: {e}")
            guid_rows = []

        guid_count = 0
        for row in guid_rows:
            email = row["email"]
            guid = row["guid"]
            source_ts = self._ts_iso(row.get("source_ts"))
            if not email or email in self.invalid_values:
                continue
            if not guid or guid in self.invalid_values:
                continue
            self._add_edge(
                HubEdge(
                    identifier_a=email,
                    identifier_a_type="email",
                    identifier_b=guid,
                    identifier_b_type="surveyengine_guid",
                    source_system="surveyengine",
                    link_type="deterministic",
                    match_rule="SURVEYENGINE_DIRECT",
                    confidence=confidence,
                    first_seen=source_ts,
                    last_seen=source_ts,
                )
            )
            guid_count += 1

        if guid_count:
            print(
                f"    Imported {guid_count:,} email<->guid edges (identity service is live)"
            )
        count += guid_count

        elapsed = time.time() - start
        self.stats["surveyengine_bridge"] = {
            "edges": count,
            "guid_edges": guid_count,
            "elapsed": elapsed,
        }
        print(f"    Imported {count:,} surveyengine_bridge edges ({elapsed:.1f}s)")
        return count

    # ─── Phase 1: Import existing cookie graph ─────────────────

    def connect_existing_graph(self, dry_run: bool = False) -> int:
        """
        Import edges from the existing bionews identity_lookup table.
        These are visitor_id <-> cookie identifier pairs already extracted
        from page_load_sessions. Drops non-person identifiers.
        """
        if not self._is_connector_enabled("existing_graph"):
            # existing_graph doesn't have a config entry — always run unless filtered
            if self.connector_filter and "existing_graph" not in self.connector_filter:
                print("  [existing_graph] Skipped (filtered out)")
                return 0

        print("  [existing_graph] Importing cookie graph edges...")

        query = f"""
        SELECT
          visitor_id,
          identifier_value,
          identifier_type,
          source_priority,
          MIN(stitched_id) AS stitched_id
        FROM `{self.bionews_lookup}`
        WHERE identifier_value IS NOT NULL
          AND LENGTH(identifier_value) >= {self.min_identifier_length}
        GROUP BY visitor_id, identifier_value, identifier_type, source_priority
        """

        if dry_run:
            self._dry_run_query(query, "existing_graph")
            return 0

        start = time.time()
        try:
            result_iter = self._run_query(query, page_size=50000)
        except Exception as e:
            print(f"    Error querying existing graph: {e}")
            return 0

        count = 0
        dropped = 0
        source_rows = 0
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

        for row in result_iter:
            source_rows += 1
            id_type = row["identifier_type"]
            id_value = row["identifier_value"]

            # Drop non-person identifiers
            if id_type in DROPPED_TYPES:
                dropped += 1
                continue

            # Skip invalid values
            if id_value in self.invalid_values:
                continue

            # Normalize cookie values
            if self.normalize_cookies:
                id_type, id_value = self.normalizer.normalize_and_register(
                    id_type, id_value
                )

            self._add_edge(
                HubEdge(
                    identifier_a=row["visitor_id"],
                    identifier_a_type="visitor_id",
                    identifier_b=id_value,
                    identifier_b_type=id_type,
                    source_system="page_load",
                    link_type="deterministic",
                    match_rule="COOKIE_SESSION",
                    confidence=1.0,
                    first_seen=now_iso,
                    last_seen=now_iso,
                )
            )
            count += 1

            if source_rows % 1_000_000 == 0:
                _elapsed = time.time() - start
                try:
                    import resource

                    _rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
                except Exception:
                    _rss = 0
                _pairs = len(self._edge_agg)
                print(
                    f"    Progress: {source_rows:,} rows | {count:,} edges | "
                    f"{_pairs:,} unique | {_rss:,} MB | {_elapsed:.0f}s",
                    flush=True,
                )

        elapsed = time.time() - start
        self.stats["existing_graph"] = {
            "edges": count,
            "dropped": dropped,
            "source_rows": source_rows,
            "elapsed": elapsed,
        }
        print(
            f"    Imported {count:,} edges from {source_rows:,} lookup rows "
            f"(dropped {dropped:,} non-person) ({elapsed:.1f}s)"
        )
        return count

    # ─── Phase 2: Email bridge connector ───────────────────────

    def connect_email_bridge(self, dry_run: bool = False) -> int:
        """
        Extract raw emails from Mailchimp, LimeSurvey, WordPress users/comments/forms.
        Creates email nodes linked to source-specific identifiers.
        """
        if not self._is_connector_enabled("email_bridge"):
            print("  [email_bridge] Skipped (disabled)")
            return 0

        print("  [email_bridge] Extracting emails...")
        cfg = self.connectors_config["email_bridge"]
        confidence = cfg.get("confidence", 1.0)
        sources = cfg.get("sources", {})

        total_edges = 0
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

        for source_name, source_cfg in sources.items():
            dataset = source_cfg.get("dataset")
            table = source_cfg.get("table")
            email_field = source_cfg.get("email_field")
            id_field = source_cfg.get("id_field")
            id_type = source_cfg.get("id_type")
            extra_filter = source_cfg.get("filter", "1=1")
            ts_field = source_cfg.get("timestamp_field")

            if not dataset or not table or not email_field:
                print(f"    [{source_name}] Skipped (incomplete config)")
                continue

            fqt = self._fqt(dataset, table)

            # Build optional date filter (only for incremental — full rebuild loads all records)
            ts_clause = (
                self._timestamp_filter(ts_field, indent="                  ")
                if (ts_field and self.incremental and source_cfg.get("window", True))
                else ""
            )

            # Special handling for Gravity Forms meta (email in meta_value)
            # Build timestamp SELECT expression — use source timestamp when
            # available, NULL when not. Never stamp now_iso on reference data.
            ts_select = (
                f"CAST({ts_field} AS TIMESTAMP) AS source_ts"
                if ts_field
                else "CAST(NULL AS TIMESTAMP) AS source_ts"
            )

            if source_name == "wordpress_forms":
                meta_key_filter = source_cfg.get("meta_key_filter", "email")
                # JOIN to gravity_forms_entry for date_created timestamp
                entry_table = self._fqt("BN_Warehouse", "gravity_forms_entry")
                query = f"""
                SELECT DISTINCT
                  LOWER(TRIM(m.meta_value)) AS email,
                  CAST(m.entry_id AS STRING) AS source_id,
                  CAST(e.date_created AS TIMESTAMP) AS source_ts
                FROM `{fqt}` m
                LEFT JOIN `{entry_table}` e ON m.entry_id = e.entry_id AND m.site = e.site
                WHERE LOWER(m.meta_key) LIKE '%{meta_key_filter}%'
                  AND m.meta_value IS NOT NULL
                  AND REGEXP_CONTAINS(LOWER(TRIM(m.meta_value)), r'^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{{2,}}$')
                """
                source_id_type = "gf_entry_id"
            elif source_cfg.get("id_computed"):
                id_expr = source_cfg["id_computed"]
                query = f"""
                SELECT DISTINCT
                  LOWER(TRIM({email_field})) AS email,
                  CAST({id_expr} AS STRING) AS source_id,
                  {ts_select}
                FROM `{fqt}`
                WHERE {email_field} IS NOT NULL
                  AND LENGTH(TRIM({email_field})) > 5
                  AND REGEXP_CONTAINS(LOWER(TRIM({email_field})), r'^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{{2,}}$')
                  AND {extra_filter}
                  {ts_clause}
                """
                source_id_type = id_type
            elif id_field:
                query = f"""
                SELECT DISTINCT
                  LOWER(TRIM({email_field})) AS email,
                  CAST({id_field} AS STRING) AS source_id,
                  {ts_select}
                FROM `{fqt}`
                WHERE {email_field} IS NOT NULL
                  AND LENGTH(TRIM({email_field})) > 5
                  AND REGEXP_CONTAINS(LOWER(TRIM({email_field})), r'^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{{2,}}$')
                  AND {extra_filter}
                  {ts_clause}
                """
                source_id_type = id_type
            else:
                query = f"""
                SELECT DISTINCT
                  LOWER(TRIM({email_field})) AS email,
                  CAST(NULL AS STRING) AS source_id,
                  {ts_select}
                FROM `{fqt}`
                WHERE {email_field} IS NOT NULL
                  AND LENGTH(TRIM({email_field})) > 5
                  AND REGEXP_CONTAINS(LOWER(TRIM({email_field})), r'^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{{2,}}$')
                  AND {extra_filter}
                  {ts_clause}
                """
                source_id_type = None

            if dry_run:
                self._dry_run_query(query, source_name)
                continue

            try:
                start = time.time()
                source_rows = 0
                elapsed = time.time() - start

                source_system = source_name.split("_")[
                    0
                ]  # 'mailchimp', 'limesurvey', 'wordpress'
                count = 0

                for row in self._run_query(query, page_size=50000):
                    source_rows += 1
                    email = row["email"]
                    if not email:
                        continue

                    # Use source timestamp when available; never leave empty
                    # (empty coerced to NULL and can wipe hub first_seen on MERGE).
                    source_ts = self._ts_iso(row.get("source_ts"))

                    # Plus-tag bridge: create edge from tagged variant to base address if allowed
                    self._add_plus_tag_bridge_if_allowed(
                        email, source_system, source_ts
                    )
                    # Dot normalization bridge: create edge from dotted variant to normalized form if allowed
                    self._add_dot_normalization_bridge_if_allowed(
                        email, source_system, source_ts
                    )

                    # Edge: email <-> source-specific ID (raw email, not hashed)
                    if row["source_id"] and source_id_type:
                        self._add_edge(
                            HubEdge(
                                identifier_a=email,
                                identifier_a_type="email",
                                identifier_b=row["source_id"],
                                identifier_b_type=source_id_type,
                                source_system=source_system,
                                link_type="deterministic",
                                match_rule="EMAIL_EXACT",
                                confidence=confidence,
                                first_seen=source_ts,
                                last_seen=source_ts,
                            )
                        )
                        count += 1
                    else:
                        # No source ID — register the email as a standalone node
                        self._add_edge(
                            HubEdge(
                                identifier_a=email,
                                identifier_a_type="email",
                                identifier_b=email,
                                identifier_b_type="email",
                                source_system=source_system,
                                link_type="deterministic",
                                match_rule="EMAIL_EXACT",
                                confidence=confidence,
                                first_seen=source_ts,
                                last_seen=source_ts,
                            )
                        )
                        count += 1

                print(
                    f"    [{source_name}] {count:,} emails from {source_rows:,} rows ({elapsed:.1f}s)"
                )
                total_edges += count

            except Exception as e:
                print(f"    [{source_name}] Error: {e}")

        self.stats["email_bridge"] = {"edges": total_edges}
        print(f"    Total email bridge edges: {total_edges:,}")
        return total_edges

    # ─── Phase 2b: mc_euid Email Bridge ─────────────────────────

    def connect_mc_euid_bridge(self, dry_run: bool = False) -> int:
        """
        Bridge email <-> mc_euid via Mailchimp members table.
        mc_euid (unique_email_id) is stored in localStorage and linked to bnfpvid
        in Phase 0. This connector completes the chain: email <-> mc_euid <-> bnfpvid.
        """
        if not self._is_connector_enabled("mc_euid_bridge"):
            print("  [mc_euid_bridge] Skipped (disabled)")
            return 0

        print("  [mc_euid_bridge] Bridging email <-> mc_euid via Mailchimp members...")
        cfg = self.connectors_config["mc_euid_bridge"]
        confidence = cfg.get("confidence", 1.0)
        dataset = cfg.get("dataset", "mailchimp_data")
        table = cfg.get("table", "members")
        fqt = self._fqt(dataset, table)

        query = f"""
        SELECT DISTINCT
          LOWER(TRIM(email_address)) AS email,
          unique_email_id AS mc_euid,
          CAST(last_changed AS TIMESTAMP) AS source_ts
        FROM `{fqt}`
        WHERE email_address IS NOT NULL
          AND unique_email_id IS NOT NULL
          AND LENGTH(TRIM(email_address)) > 5
          AND REGEXP_CONTAINS(LOWER(TRIM(email_address)), r'^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{{2,}}$')
          AND LENGTH(TRIM(unique_email_id)) >= {self.min_identifier_length}
        """

        if dry_run:
            self._dry_run_query(query, "mc_euid_bridge")
            return 0

        try:
            start = time.time()
            source_rows = 0

            count = 0
            # Build validation dicts during streaming (no second pass)
            euid_to_emails: Dict[str, set] = {}
            email_to_euids: Dict[str, set] = {}

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                email = row["email"]
                mc_euid = row["mc_euid"]
                if not email or not mc_euid or mc_euid in self.invalid_values:
                    continue

                source_ts = row["source_ts"].isoformat() if row.get("source_ts") else ""

                # Track for validation
                euid_to_emails.setdefault(mc_euid, set()).add(email)
                email_to_euids.setdefault(email, set()).add(mc_euid)

                self._add_edge(
                    HubEdge(
                        identifier_a=email,
                        identifier_a_type="email",
                        identifier_b=mc_euid,
                        identifier_b_type="mc_euid",
                        source_system="mailchimp",
                        link_type="deterministic",
                        match_rule="MC_EUID_BRIDGE",
                        confidence=confidence,
                        first_seen=source_ts,
                        last_seen=source_ts,
                    )
                )
                count += 1

            elapsed = time.time() - start
            print(
                f"    {count:,} email<->mc_euid edges from {source_rows:,} members ({elapsed:.1f}s)"
            )

            # ── mc_euid validation checks ──
            multi_email_euids = {k: v for k, v in euid_to_emails.items() if len(v) > 1}
            if multi_email_euids:
                print(
                    f"    WARNING: {len(multi_email_euids)} mc_euids map to >1 email (expected 1:1)"
                )
                for euid, emails in sorted(
                    multi_email_euids.items(), key=lambda x: -len(x[1])
                )[:5]:
                    print(f"      {euid}: {len(emails)} emails")
            else:
                print(
                    f"    mc_euid uniqueness OK: all {len(euid_to_emails):,} mc_euids map to 1 email"
                )

            validation = {
                "unique_euids": len(euid_to_emails),
                "unique_emails": len(email_to_euids),
                "multi_email_euids": len(multi_email_euids),
            }
            self.stats["mc_euid_bridge"] = {
                "edges": count,
                "source_rows": source_rows,
                "elapsed": elapsed,
                "validation": validation,
            }
            return count

        except Exception as e:
            print(f"    [mc_euid_bridge] Error: {e}")
            self.stats["mc_euid_bridge"] = {"edges": 0, "error": str(e)}
            return 0

    # ─── Phase 2c: GA4 session co-occurrence ────────────────────

    def connect_ga4_identifiers(self, dry_run: bool = False) -> int:
        """
        Extract identity pairs from GA4 processed events.
        Produces edges: client_id <-> clarity_user, client_id <-> aim_dgid,
        client_id <-> npi_number. These supplement Phase 0 with GA4's
        higher fill rates for certain identifiers.
        """
        if not self._is_connector_enabled("ga4_identifiers"):
            print("  [ga4_identifiers] Skipped (disabled)")
            return 0

        print("  [ga4_identifiers] Extracting GA4 session co-occurrences...")
        cfg = self.connectors_config["ga4_identifiers"]
        confidence = cfg.get("confidence", 1.0)
        dataset = cfg.get("dataset", "df_warehouse_output")
        table = cfg.get("table", "output_ga4_joined")
        fqt = self._fqt(dataset, table)

        ts_filter = self._timestamp_filter("event_timestamp", indent="      ")
        # GA4 table is partitioned by event_date — must include partition filter
        date_filter = ""
        if self.start_date:
            date_filter = f"AND event_date >= '{self.start_date}'"

        # v3.0: Additional GA4 identity columns (bnfpvid, mc_euid, fbp, wp_user_id)
        ga4_extra_sql = ""
        if self.xa_ga4_extra_identifiers:
            ga4_extra_sql = f"""
          UNION ALL

          SELECT
            'bnfpvid' AS id_type,
            user_pseudo_id AS id_a,
            CAST(user_tracking_all_local_storage_bnfpvid AS STRING) AS id_b,
            event_timestamp AS first_seen,
            event_timestamp AS last_seen
          FROM `{fqt}`
          WHERE user_pseudo_id IS NOT NULL
            AND user_tracking_all_local_storage_bnfpvid IS NOT NULL
            AND LENGTH(TRIM(CAST(user_tracking_all_local_storage_bnfpvid AS STRING))) >= {self.min_identifier_length}
            {date_filter}
            {ts_filter}

          UNION ALL

          SELECT
            'mc_euid' AS id_type,
            user_pseudo_id AS id_a,
            CAST(user_tracking_all_local_storage_mc_euid AS STRING) AS id_b,
            event_timestamp AS first_seen,
            event_timestamp AS last_seen
          FROM `{fqt}`
          WHERE user_pseudo_id IS NOT NULL
            AND user_tracking_all_local_storage_mc_euid IS NOT NULL
            AND LENGTH(TRIM(CAST(user_tracking_all_local_storage_mc_euid AS STRING))) >= {self.min_identifier_length}
            {date_filter}
            {ts_filter}

          UNION ALL

          SELECT
            'fbp' AS id_type,
            user_pseudo_id AS id_a,
            CAST(user_tracking_fbp AS STRING) AS id_b,
            event_timestamp AS first_seen,
            event_timestamp AS last_seen
          FROM `{fqt}`
          WHERE user_pseudo_id IS NOT NULL
            AND user_tracking_fbp IS NOT NULL
            AND LENGTH(TRIM(CAST(user_tracking_fbp AS STRING))) >= {self.min_identifier_length}
            {date_filter}
            {ts_filter}

          -- wp_user_id intentionally NOT emitted from GA4: GA4's visitor_id
          -- is set per-session and includes both forum and main-site events
          -- under one user_pseudo_id, making site disambiguation unreliable
          -- (~57% accurate). The acceptor's localstorage connector covers
          -- the same population with per-event site context (~99.96% accurate),
          -- so emitting from GA4 here would only introduce contamination.

          UNION ALL

          SELECT
            'gbraid' AS id_type,
            user_pseudo_id AS id_a,
            session_gbraid AS id_b,
            event_timestamp AS first_seen,
            event_timestamp AS last_seen
          FROM `{fqt}`
          WHERE user_pseudo_id IS NOT NULL
            AND session_gbraid IS NOT NULL
            AND TRIM(session_gbraid) != ''
            AND LENGTH(TRIM(session_gbraid)) >= {self.min_identifier_length}
            {date_filter}
            {ts_filter}"""

        query = f"""
        -- Phase 2c: GA4 session co-occurrence identifiers
        -- client_id (user_pseudo_id) <-> clarity_user, aim_dgid, npi_number
        SELECT id_type, id_a, id_b, MIN(first_seen) AS first_seen, MAX(last_seen) AS last_seen
        FROM (
          SELECT
            'clarity_user' AS id_type,
            user_pseudo_id AS id_a,
            SPLIT(user_tracking_clarity_user, '^')[OFFSET(0)] AS id_b,
            event_timestamp AS first_seen,
            event_timestamp AS last_seen
          FROM `{fqt}`
          WHERE user_pseudo_id IS NOT NULL
            AND user_tracking_clarity_user IS NOT NULL
            AND TRIM(user_tracking_clarity_user) != ''
            AND LENGTH(SPLIT(user_tracking_clarity_user, '^')[OFFSET(0)]) >= {self.min_identifier_length}
            {date_filter}
            {ts_filter}

          UNION ALL

          SELECT
            'aim_dgid' AS id_type,
            user_pseudo_id AS id_a,
            AIM_dgid AS id_b,
            event_timestamp AS first_seen,
            event_timestamp AS last_seen
          FROM `{fqt}`
          WHERE user_pseudo_id IS NOT NULL
            AND AIM_dgid IS NOT NULL
            AND TRIM(AIM_dgid) != ''
            AND LENGTH(TRIM(AIM_dgid)) >= {self.min_identifier_length}
            {date_filter}
            {ts_filter}

          UNION ALL

          SELECT
            'npi_number' AS id_type,
            user_pseudo_id AS id_a,
            user_tracking_aim_payload_npi_number AS id_b,
            event_timestamp AS first_seen,
            event_timestamp AS last_seen
          FROM `{fqt}`
          WHERE user_pseudo_id IS NOT NULL
            AND user_tracking_aim_payload_npi_number IS NOT NULL
            AND TRIM(user_tracking_aim_payload_npi_number) != ''
            AND LENGTH(TRIM(user_tracking_aim_payload_npi_number)) >= {self.min_identifier_length}
            {date_filter}
            {ts_filter}
{ga4_extra_sql}
        )
        GROUP BY id_type, id_a, id_b
        """

        if dry_run:
            self._dry_run_query(query, "ga4_identifiers")
            return 0

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            count = 0
            type_counts = {}

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                id_type = row["id_type"]
                id_a = row["id_a"]  # client_id (user_pseudo_id)
                id_b = row["id_b"]
                if not id_a or not id_b or id_b in self.invalid_values:
                    continue

                first_seen = (
                    row["first_seen"].isoformat() if row["first_seen"] else now_iso
                )
                last_seen = (
                    row["last_seen"].isoformat() if row["last_seen"] else now_iso
                )

                self._add_edge(
                    HubEdge(
                        identifier_a=id_a,
                        identifier_a_type="client_id",
                        identifier_b=id_b,
                        identifier_b_type=id_type,
                        source_system="ga4",
                        link_type="deterministic",
                        match_rule="GA4_SESSION_COOCCURRENCE",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1
                type_counts[id_type] = type_counts.get(id_type, 0) + 1

            for t, c in sorted(type_counts.items()):
                print(f"    {t}: {c:,} edges")
            print(f"    Total: {count:,} GA4 edges ({elapsed:.1f}s)")
            self.stats["ga4_identifiers"] = {
                "edges": count,
                "source_rows": source_rows,
                "elapsed": elapsed,
                "type_counts": type_counts,
            }
            return count

        except Exception as e:
            print(f"    [ga4_identifiers] Error: {e}")
            self.stats["ga4_identifiers"] = {"edges": 0, "error": str(e)}
            return 0

    # ─── Phase 2d: NPI -> email bridge ─────────────────────────

    def connect_npi_email_bridge(self, dry_run: bool = False) -> int:
        """
        Bridge npi_number <-> email via NPI registry endpoints table.
        DIRECT endpoint type entries often contain practitioner email addresses.
        """
        if not self._is_connector_enabled("npi_email_bridge"):
            print("  [npi_email_bridge] Skipped (disabled)")
            return 0

        print("  [npi_email_bridge] Bridging NPI -> email via NPI registry...")
        cfg = self.connectors_config["npi_email_bridge"]
        confidence = cfg.get("confidence", 1.0)
        dataset = cfg.get("dataset", "npi_data")
        table = cfg.get("table", "npi_endpoints")
        fqt = self._fqt(dataset, table)

        # npi_endpoints has no CMS event dates; join npi_main for
        # provider_enumeration_date (first_seen) and last_update_date (last_seen).
        # Fallback to endpoint extracted_at when main dates are missing.
        npi_main = self._fqt(dataset, cfg.get("npi_main_table", "npi_main"))
        query = f"""
        SELECT
          e.npi AS npi_number,
          LOWER(TRIM(e.endpoint)) AS email,
          MIN(COALESCE(
            TIMESTAMP(m.provider_enumeration_date),
            e.extracted_at
          )) AS first_seen,
          MAX(COALESCE(
            TIMESTAMP(m.last_update_date),
            TIMESTAMP(m.provider_enumeration_date),
            e.extracted_at
          )) AS last_seen
        FROM `{fqt}` e
        LEFT JOIN `{npi_main}` m ON e.npi = m.npi
        WHERE e.endpoint IS NOT NULL
          AND REGEXP_CONTAINS(LOWER(TRIM(e.endpoint)),
              r'^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{{2,}}$')
          AND LENGTH(TRIM(e.npi)) >= {self.min_identifier_length}
        GROUP BY e.npi, LOWER(TRIM(e.endpoint))
        """

        if dry_run:
            self._dry_run_query(query, "npi_email_bridge")
            return 0

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            count = 0
            missing_ts = 0

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                npi = row["npi_number"]
                email = row["email"]
                if not npi or not email or email in self.invalid_values:
                    continue

                first_seen = (
                    row["first_seen"].isoformat() if row["first_seen"] else now_iso
                )
                last_seen = (
                    row["last_seen"].isoformat() if row["last_seen"] else first_seen
                )
                if not row["first_seen"]:
                    missing_ts += 1

                self._add_edge(
                    HubEdge(
                        identifier_a=npi,
                        identifier_a_type="npi_number",
                        identifier_b=email,
                        identifier_b_type="email",
                        source_system="npi_registry",
                        link_type="deterministic",
                        match_rule="NPI_REGISTRY_EMAIL",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

            print(
                f"    {count:,} npi<->email edges from {source_rows:,} endpoints "
                f"({missing_ts:,} used run-time fallback, {elapsed:.1f}s)"
            )
            self.stats["npi_email_bridge"] = {
                "edges": count,
                "source_rows": source_rows,
                "missing_source_ts": missing_ts,
                "elapsed": elapsed,
            }
            return count

        except Exception as e:
            print(f"    [npi_email_bridge] Error: {e}")
            self.stats["npi_email_bridge"] = {"edges": 0, "error": str(e)}
            return 0

    # ─── Phase 3: UTM Crosswalk connector ──────────────────────

    def connect_utm_crosswalk(self, dry_run: bool = False) -> int:
        """
        Match Mailchimp click mc_euid to GA4 page_location mc_euid.
        Mailchimp appends mc_euid to click URLs; GA4 captures the full URL
        in page_location. Joining on mc_euid + time window links email
        (from Mailchimp) to client_id (from GA4).
        """
        if not self._is_connector_enabled("utm_crosswalk"):
            print("  [utm_crosswalk] Skipped (disabled)")
            return 0

        print(
            "  [utm_crosswalk] Matching mc_euid in Mailchimp clicks -> GA4 page_location..."
        )
        cfg = self.connectors_config["utm_crosswalk"]
        confidence = cfg.get("confidence", 0.90)
        time_window = cfg.get("time_window_minutes", 5)
        source_ds = cfg.get("source_dataset", "mailchimp_data")
        source_tbl = cfg.get("source_table", "campaign_email_activity")
        target_ds = cfg.get("target_dataset", "df_warehouse_output")
        target_tbl = cfg.get("target_table", "output_ga4_joined")

        # GA4 table is partitioned by event_date — must include partition filter
        date_filter = ""
        if self.start_date:
            date_filter = f"AND ga.event_date >= '{self.start_date}'"

        query = f"""
-- UTM Crosswalk: Mailchimp click -> GA4 session via mc_euid
-- Mailchimp Activity API stores template URLs with literal *|UNIQID|* merge tag,
-- so we resolve mc_euid by joining clicks -> members (via email_id + list_id).
-- GA4 page_location has the resolved mc_euid from the browser's actual URL.
WITH mc_clicks AS (
  SELECT
    LOWER(TRIM(c.email_address)) AS email,
    m.unique_email_id AS mc_euid,
    c.activity_timestamp
  FROM `{self._fqt(source_ds, source_tbl)}` c
  JOIN `{self._fqt(source_ds, "members")}` m
    ON c.email_id = m.id AND c.list_id = m.list_id
  WHERE c.action = 'click'
    AND m.unique_email_id IS NOT NULL
    {self._timestamp_filter("c.activity_timestamp", indent="    ")}
),

-- GA4 events where page_location contains mc_euid
ga4_with_euid AS (
  SELECT
    user_pseudo_id AS client_id,
    REGEXP_EXTRACT(page_location, r'[?&]mc_euid=([^&]+)') AS mc_euid,
    event_timestamp
  FROM `{self._fqt(target_ds, target_tbl)}` ga
  WHERE page_location LIKE '%mc_euid=%'
    AND user_pseudo_id IS NOT NULL
    {date_filter}
    {self._timestamp_filter("ga.event_timestamp", indent="    ")}
),

-- Match on mc_euid + time window
matched AS (
  SELECT DISTINCT
    mc.email,
    ga.client_id,
    mc.activity_timestamp AS mc_time
  FROM mc_clicks mc
  JOIN ga4_with_euid ga
    ON mc.mc_euid = ga.mc_euid
    AND ABS(TIMESTAMP_DIFF(mc.activity_timestamp, ga.event_timestamp, MINUTE)) <= {time_window}
  WHERE mc.email IS NOT NULL
    AND ga.client_id IS NOT NULL
)

SELECT
  email,
  client_id,
  MIN(mc_time) AS first_seen,
  MAX(mc_time) AS last_seen
FROM matched
GROUP BY email, client_id
"""

        if dry_run:
            self._dry_run_query(query, "utm_crosswalk")
            return 0

        start = time.time()
        source_rows = 0
        elapsed = time.time() - start

        count = 0
        for row in self._run_query(query, page_size=50000):
            source_rows += 1
            email = row["email"]
            first_seen = row["first_seen"].isoformat() if row["first_seen"] else ""
            last_seen = row["last_seen"].isoformat() if row["last_seen"] else ""

            # Edge: email <-> client_id
            self._add_edge(
                HubEdge(
                    identifier_a=email,
                    identifier_a_type="email",
                    identifier_b=row["client_id"],
                    identifier_b_type="client_id",
                    source_system="mailchimp",
                    link_type="deterministic",
                    match_rule="UTM_CROSSWALK",
                    confidence=confidence,
                    first_seen=first_seen,
                    last_seen=last_seen,
                )
            )
            count += 1

        self.stats["utm_crosswalk"] = {"edges": count, "elapsed": elapsed}
        print(f"    {count:,} UTM crosswalk edges ({elapsed:.1f}s)")
        return count

    # ─── Phase 4: IP + Device + Time connector ─────────────────

    def connect_ip_device_time(self, dry_run: bool = False) -> int:
        """
        Probabilistic matching of Mailchimp click activity (ip, timestamp)
        to GA4 sessions within time window. Produces email <-> client_id edges.
        """
        if not self._is_connector_enabled("ip_device_time"):
            print("  [ip_device_time] Skipped (disabled)")
            return 0

        print("  [ip_device_time] Matching IP+time (MC clicks -> GA4)...")
        cfg = self.connectors_config["ip_device_time"]
        time_window = cfg.get("time_window_minutes", 5)
        max_visitors_per_ip = cfg.get("max_visitors_per_ip", 50)
        scoring = cfg.get("scoring", {})
        base_score = scoring.get("base", 0.50)
        ua_bonus = scoring.get("user_agent_match", 0.10)
        min_confidence = cfg.get("min_confidence", 0.55)
        source_ds = cfg.get("source_dataset", "mailchimp_data")
        source_tbl = cfg.get("source_table", "campaign_email_activity")
        target_ds = cfg.get("target_dataset", "df_warehouse_output")
        target_tbl = cfg.get("target_table", "output_ga4_joined")

        # GA4 table is partitioned by event_date — must include partition filter
        date_filter = ""
        if self.start_date:
            date_filter = f"AND event_date >= '{self.start_date}'"

        query = f"""
-- IP + Time: Mailchimp click activity -> GA4 session via IP + timestamp
WITH
-- Exclude high-cardinality IPs (corporate/VPN)
ip_counts AS (
  SELECT ip_address AS ip, COUNT(DISTINCT user_pseudo_id) AS visitor_count
  FROM `{self._fqt(target_ds, target_tbl)}`
  WHERE ip_address IS NOT NULL AND TRIM(ip_address) != ''
    {date_filter}
    {self._timestamp_filter("event_timestamp", indent="    ")}
  GROUP BY ip_address
  HAVING COUNT(DISTINCT user_pseudo_id) <= {max_visitors_per_ip}
),

-- Mailchimp click activities with IP
mc_activity AS (
  SELECT DISTINCT
    LOWER(TRIM(email_address)) AS email,
    ip,
    activity_timestamp
  FROM `{self._fqt(source_ds, source_tbl)}`
  WHERE action = 'click'
    AND ip IS NOT NULL AND TRIM(ip) != ''
    AND email_address IS NOT NULL
    {self._timestamp_filter("activity_timestamp", indent="    ")}
),

-- GA4 sessions with IP and user_agent
ga4_sessions AS (
  SELECT DISTINCT
    user_pseudo_id AS client_id,
    ip_address AS ip,
    user_agent,
    event_timestamp
  FROM `{self._fqt(target_ds, target_tbl)}`
  WHERE ip_address IS NOT NULL AND TRIM(ip_address) != ''
    AND user_pseudo_id IS NOT NULL
    {date_filter}
    {self._timestamp_filter("event_timestamp", indent="    ")}
),

-- Match on IP + time window
matched AS (
  SELECT
    mc.email,
    ga.client_id,
    {base_score} AS confidence,
    MIN(mc.activity_timestamp) OVER (PARTITION BY mc.email, ga.client_id) AS first_seen,
    MAX(mc.activity_timestamp) OVER (PARTITION BY mc.email, ga.client_id) AS last_seen
  FROM mc_activity mc
  JOIN ip_counts ic ON mc.ip = ic.ip
  JOIN ga4_sessions ga
    ON mc.ip = ga.ip
    AND ABS(TIMESTAMP_DIFF(mc.activity_timestamp, ga.event_timestamp, SECOND)) <= {time_window * 60}
)

SELECT DISTINCT
  email,
  client_id,
  MAX(confidence) AS confidence,
  MIN(first_seen) AS first_seen,
  MAX(last_seen) AS last_seen
FROM matched
WHERE confidence >= {min_confidence}
GROUP BY email, client_id
"""

        if dry_run:
            self._dry_run_query(query, "ip_device_time")
            return 0

        start = time.time()
        try:
            result_iter = self._run_query(query, page_size=50000)
        except Exception as e:
            print(f"    [ip_device_time] Error: {e}")
            self.stats["ip_device_time"] = {"edges": 0, "error": str(e)}
            return 0
        elapsed = time.time() - start

        count = 0
        source_rows = 0
        for row in result_iter:
            source_rows += 1
            email = row["email"]
            first_seen = row["first_seen"].isoformat() if row["first_seen"] else ""
            last_seen = row["last_seen"].isoformat() if row["last_seen"] else ""

            self._add_edge(
                HubEdge(
                    identifier_a=email,
                    identifier_a_type="email",
                    identifier_b=row["client_id"],
                    identifier_b_type="client_id",
                    source_system="mailchimp",
                    link_type="probabilistic",
                    match_rule="IP_DEVICE_TIME",
                    confidence=float(row["confidence"]),
                    first_seen=first_seen,
                    last_seen=last_seen,
                )
            )
            count += 1

        self.stats["ip_device_time"] = {"edges": count, "elapsed": elapsed}
        print(f"    {count:,} IP+time edges ({elapsed:.1f}s)")
        return count

    # ─── Phase 5: LimeSurvey session bridge ────────────────────

    def connect_limesurvey_session(self, dry_run: bool = False) -> int:
        """
        Match GA4 form_submit events on survey.* subdomains to LimeSurvey
        responses via survey_id + time window, producing:
          bnfpvid <-> ls_response_id
          (optionally) client_id <-> ls_response_id (cross-anchor, ~18% more edges)

        GA4 captures bnfpvid on survey.{site}.com pages via the BN acceptor
        tag's localStorage extraction.  Survey URLs contain the LimeSurvey
        survey_id (e.g. /index.php/141613 or /141613).  Joining GA4
        form_submit timestamps to lime_surveys_columnar.submitdate within a
        configurable time window links browser identity to survey response.

        email_bridge (Phase 2) provides email <-> participant_id from
        lime_participants, so Union-Find stitches:
          bnfpvid -> ls_response_id -> email (via survey responses).
        """
        if not self._is_connector_enabled("limesurvey_session"):
            print("  [limesurvey_session] Skipped (disabled)")
            return 0

        print(
            "  [limesurvey_session] Matching GA4 survey events to LimeSurvey responses..."
        )
        cfg = self.connectors_config["limesurvey_session"]
        confidence = cfg.get("confidence", 0.90)
        time_window = cfg.get("time_window_minutes", 5)
        ga4_ds = cfg.get("ga4_dataset", "df_warehouse_output")
        ga4_tbl = cfg.get("ga4_table", "output_ga4_joined")
        responses_ds = cfg.get("responses_dataset", "limesurvey_data")
        responses_tbl = cfg.get("responses_table", "lime_surveys_columnar")
        submit_event = cfg.get("submit_event", "form_submit")
        hostname_pattern = cfg.get("hostname_pattern", "survey.%")
        # Use pipeline lookback if set (incremental), else connector default
        if self.start_date:
            from datetime import datetime as _dt

            _days = (_dt.now() - _dt.strptime(self.start_date, "%Y-%m-%d")).days + 1
            lookback_days = max(_days, 7)  # minimum 7 days for survey completion lag
        else:
            lookback_days = cfg.get("lookback_days", 90)

        ga4_fqt = self._fqt(ga4_ds, ga4_tbl)
        responses_fqt = self._fqt(responses_ds, responses_tbl)
        min_id_len = self.min_identifier_length

        # CTE: extract bnfpvid + survey_id from GA4 form_submit events on survey.* subdomains
        cte = f"""
-- Phase 5: LimeSurvey Session Bridge — GA4 survey events -> response_id
WITH ga4_submits AS (
  SELECT DISTINCT
    CAST(user_tracking_all_local_storage_bnfpvid AS STRING) AS bnfpvid,
    user_pseudo_id,
    CAST(COALESCE(
      REGEXP_EXTRACT(page_location, r'/index\\.php/(\\d+)'),
      REGEXP_EXTRACT(page_location, r'survey\\.[^/]+/(\\d+)')
    ) AS INT64) AS survey_id,
    event_timestamp
  FROM `{ga4_fqt}`
  WHERE event_date > DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
    AND hostname LIKE '{hostname_pattern}'
    AND event_name = '{submit_event}'
    AND COALESCE(
      REGEXP_EXTRACT(page_location, r'/index\\.php/(\\d+)'),
      REGEXP_EXTRACT(page_location, r'survey\\.[^/]+/(\\d+)')
    ) IS NOT NULL
),
survey_completions AS (
  SELECT DISTINCT
    survey_id,
    response_id,
    submitdate
  FROM `{responses_fqt}`
  WHERE submitdate IS NOT NULL
    AND response_id IS NOT NULL
)
"""

        # Build UNION ALL parts for each anchor type
        union_parts = []

        # Anchor 1: bnfpvid <-> participant_id (survey_id + '_' + response_id)
        # The id_value format MUST match the email bridge's id_computed:
        # CONCAT(survey_id, '_', response_id) so Union-Find can stitch them.
        union_parts.append(f"""
SELECT
  g.bnfpvid AS anchor_value,
  'bnfpvid' AS anchor_type,
  'participant_id' AS id_type,
  CONCAT(CAST(sc.survey_id AS STRING), '_', CAST(sc.response_id AS STRING)) AS id_value,
  MIN(g.event_timestamp) AS first_seen,
  MAX(g.event_timestamp) AS last_seen
FROM ga4_submits g
JOIN survey_completions sc
  ON g.survey_id = sc.survey_id
  AND ABS(TIMESTAMP_DIFF(g.event_timestamp, sc.submitdate, MINUTE)) <= {time_window}
WHERE g.bnfpvid IS NOT NULL
  AND LENGTH(g.bnfpvid) >= {min_id_len}
GROUP BY g.bnfpvid, sc.survey_id, sc.response_id""")

        # Anchor 2: client_id <-> participant_id (cross-anchor)
        # GA4 user_pseudo_id IS the GA client_id (e.g. "92304857.1769968830"),
        # matching the 2.1M client_id values already in bn_id_xref.
        # This catches survey respondents who lack bnfpvid (~33% of submits).
        if self.xa_limesurvey_session_client_id:
            union_parts.append(f"""
SELECT
  g.user_pseudo_id AS anchor_value,
  'client_id' AS anchor_type,
  'participant_id' AS id_type,
  CONCAT(CAST(sc.survey_id AS STRING), '_', CAST(sc.response_id AS STRING)) AS id_value,
  MIN(g.event_timestamp) AS first_seen,
  MAX(g.event_timestamp) AS last_seen
FROM ga4_submits g
JOIN survey_completions sc
  ON g.survey_id = sc.survey_id
  AND ABS(TIMESTAMP_DIFF(g.event_timestamp, sc.submitdate, MINUTE)) <= {time_window}
WHERE g.user_pseudo_id IS NOT NULL
  AND LENGTH(g.user_pseudo_id) >= {min_id_len}
GROUP BY g.user_pseudo_id, sc.survey_id, sc.response_id""")

        query = cte + "\nUNION ALL\n".join(union_parts)

        if dry_run:
            self._dry_run_query(query, "limesurvey_session")
            return 0

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            count = 0
            type_counts: Dict[str, int] = {}

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                anchor_value = row["anchor_value"]
                anchor_type = row["anchor_type"]
                id_value = row["id_value"]
                if not anchor_value or not id_value or id_value in self.invalid_values:
                    continue

                first_seen = (
                    row["first_seen"].isoformat() if row["first_seen"] else now_iso
                )
                last_seen = (
                    row["last_seen"].isoformat() if row["last_seen"] else now_iso
                )

                self._add_edge(
                    HubEdge(
                        identifier_a=anchor_value,
                        identifier_a_type=anchor_type,
                        identifier_b=id_value,
                        identifier_b_type="ls_response_id",
                        source_system="limesurvey",
                        link_type="deterministic",
                        match_rule="GA4_SURVEY_SESSION",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1
                key = f"{anchor_type}<->ls_response_id"
                type_counts[key] = type_counts.get(key, 0) + 1

            for t, c in sorted(type_counts.items()):
                print(f"    {t}: {c:,} edges")
            print(f"    Total: {count:,} limesurvey_session edges ({elapsed:.1f}s)")
            self.stats["limesurvey_session"] = {
                "edges": count,
                "source_rows": source_rows,
                "elapsed": elapsed,
                "type_counts": type_counts,
            }
            return count

        except Exception as e:
            print(f"    [limesurvey_session] Error: {e}")
            self.stats["limesurvey_session"] = {"edges": 0, "error": str(e)}
            return 0

    # ─── Phase 5b: AIM payload extraction ─────────────────────

    def connect_aim_payload(self, dry_run: bool = False) -> int:
        """
        Extract aim tag_id and npi_number from BN_Acceptor aimPayload.
        aimPayload is a double-encoded JSON string inside userTracking.
        Produces edges: bnfpvid <-> aim_tag_id, bnfpvid <-> npi_number.
        """
        if not self._is_connector_enabled("aim_payload"):
            print("  [aim_payload] Skipped (disabled)")
            return 0

        print("  [aim_payload] Extracting tag_id + npi_number from aimPayload...")
        cfg = self.connectors_config["aim_payload"]
        confidence = cfg.get("confidence", 1.0)
        source_ds = cfg.get("source_dataset", "BN_Acceptor")
        source_tbl = cfg.get("source_table", "acceptor_events")
        fields_cfg = cfg.get("fields", {})

        fqt = self._fqt(source_ds, source_tbl)
        ts_filter = self._timestamp_filter("ae.publish_time", indent="    ")

        # aimPayload is double-encoded: JSON_VALUE returns a JSON string that
        # must be parsed again. Use JSON_VALUE(PARSE_JSON(...)) to extract fields.
        # Build UNION ALL for each configured field.
        union_parts = []
        for field_name, field_cfg in fields_cfg.items():
            json_key = field_cfg["json_key"]
            id_type = field_cfg["id_type"]
            union_parts.append(f"""
  SELECT
    JSON_VALUE(ae.data, '$.userTracking.allLocalStorage.bnfpvid') AS anchor_value,
    'bnfpvid' AS anchor_type,
    '{id_type}' AS id_type,
    JSON_VALUE(PARSE_JSON(JSON_VALUE(ae.data, '$.userTracking.aimPayload')), '$.{json_key}') AS id_value,
    MIN(ae.publish_time) AS first_seen,
    MAX(ae.publish_time) AS last_seen
  FROM `{fqt}` ae
  WHERE JSON_VALUE(ae.data, '$.eventMetadata.eventName') = 'page_load'
    AND JSON_VALUE(ae.data, '$.userTracking.allLocalStorage.bnfpvid') IS NOT NULL
    AND JSON_VALUE(ae.data, '$.userTracking.aimPayload') IS NOT NULL
    {ts_filter}
  GROUP BY anchor_value, id_value
  HAVING id_value IS NOT NULL AND LENGTH(TRIM(id_value)) >= {self.min_identifier_length}""")

        # v3.0: Cross-anchor client_id <-> {aim_tag_id, npi_number}
        if self.xa_aim_payload_client_id:
            for field_name, field_cfg in fields_cfg.items():
                json_key = field_cfg["json_key"]
                id_type = field_cfg["id_type"]
                union_parts.append(f"""
  SELECT
    JSON_VALUE(ae.data, '$.userTracking.clientId') AS anchor_value,
    'client_id' AS anchor_type,
    '{id_type}' AS id_type,
    JSON_VALUE(PARSE_JSON(JSON_VALUE(ae.data, '$.userTracking.aimPayload')), '$.{json_key}') AS id_value,
    MIN(ae.publish_time) AS first_seen,
    MAX(ae.publish_time) AS last_seen
  FROM `{fqt}` ae
  WHERE JSON_VALUE(ae.data, '$.eventMetadata.eventName') = 'page_load'
    AND JSON_VALUE(ae.data, '$.userTracking.clientId') IS NOT NULL
    AND JSON_VALUE(ae.data, '$.userTracking.aimPayload') IS NOT NULL
    {ts_filter}
  GROUP BY anchor_value, id_value
  HAVING id_value IS NOT NULL AND LENGTH(TRIM(id_value)) >= {self.min_identifier_length}""")

        union_sql = "\n  UNION ALL\n".join(union_parts)
        query = f"""
-- Phase 5b: AIM payload — extract tag_id + npi_number from double-encoded aimPayload
{union_sql}
"""

        if dry_run:
            self._dry_run_query(query, "aim_payload")
            return 0

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            count = 0
            type_counts = {}

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                anchor_value = row["anchor_value"]
                anchor_type = row["anchor_type"]
                id_type = row["id_type"]
                id_value = row["id_value"]
                if not anchor_value or not id_value or id_value in self.invalid_values:
                    continue
                if len(anchor_value) < self.min_identifier_length:
                    continue

                first_seen = (
                    row["first_seen"].isoformat() if row["first_seen"] else now_iso
                )
                last_seen = (
                    row["last_seen"].isoformat() if row["last_seen"] else now_iso
                )

                self._add_edge(
                    HubEdge(
                        identifier_a=anchor_value,
                        identifier_a_type=anchor_type,
                        identifier_b=id_value,
                        identifier_b_type=id_type,
                        source_system="acceptor",
                        link_type="deterministic",
                        match_rule="AIM_PAYLOAD_COOCCURRENCE",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1
                type_counts[f"{anchor_type}<->{id_type}"] = (
                    type_counts.get(f"{anchor_type}<->{id_type}", 0) + 1
                )

            for t, c in sorted(type_counts.items()):
                print(f"    {t}: {c:,} edges")
            print(f"    Total: {count:,} aim_payload edges ({elapsed:.1f}s)")
            self.stats["aim_payload"] = {
                "edges": count,
                "source_rows": source_rows,
                "elapsed": elapsed,
                "type_counts": type_counts,
            }
            return count

        except Exception as e:
            print(f"    [aim_payload] Error: {e}")
            self.stats["aim_payload"] = {"edges": 0, "error": str(e)}
            return 0

    # ─── Phase 6: Device stat_id clustering ─────────────────────

    def connect_device_stat_id(self, dry_run: bool = False) -> int:
        """
        Server-side device fingerprinting. Hashes 12 signals from
        userDeviceDetails into a stat_id, then links bnfpvids that
        share the same device fingerprint (fanout 2-20).
        Produces edges: bnfpvid <-> stat_id (probabilistic).
        """
        if not self._is_connector_enabled("device_stat_id"):
            print("  [device_stat_id] Skipped (disabled)")
            return 0

        print("  [device_stat_id] Hashing device signals -> stat_id...")
        cfg = self.connectors_config["device_stat_id"]
        confidence = cfg.get("confidence", 0.35)
        source_ds = cfg.get("source_dataset", "BN_Acceptor")
        source_tbl = cfg.get("source_table", "acceptor_events")
        max_visitors = cfg.get("max_visitors_per_stat_id", 20)
        min_visitors = cfg.get("min_visitors_per_stat_id", 2)
        hash_fields = cfg.get("hash_fields", [])
        bot_patterns = cfg.get("bot_ua_patterns", [])

        fqt = self._fqt(source_ds, source_tbl)
        ts_filter = self._timestamp_filter("ae.publish_time", indent="    ")

        # Build the CONCAT expression for hashing
        concat_parts = []
        for field in hash_fields:
            concat_parts.append(f"IFNULL(JSON_VALUE(ae.data, '{field}'), '')")
        hash_expr = "CONCAT(" + ", '|', ".join(concat_parts) + ")"

        # Build bot exclusion WHERE clauses
        ua_path = "JSON_VALUE(ae.data, '$.userDeviceDetails.userAgent')"
        bot_clauses = "\n    ".join(
            f"AND {ua_path} NOT LIKE '%{pattern}%'" for pattern in bot_patterns
        )

        query = f"""
-- Phase 6: Device stat_id — server-side fingerprint from 12 device signals
WITH device_fingerprints AS (
  SELECT
    JSON_VALUE(ae.data, '$.userTracking.allLocalStorage.bnfpvid') AS bnfpvid,
    CONCAT('stat_', SUBSTR(TO_HEX(SHA256({hash_expr})), 1, 16)) AS stat_id,
    MIN(ae.publish_time) AS first_seen,
    MAX(ae.publish_time) AS last_seen
  FROM `{fqt}` ae
  WHERE JSON_VALUE(ae.data, '$.eventMetadata.eventName') = 'page_load'
    AND JSON_VALUE(ae.data, '$.userTracking.allLocalStorage.bnfpvid') IS NOT NULL
    AND {ua_path} IS NOT NULL
    {bot_clauses}
    {ts_filter}
  GROUP BY bnfpvid, stat_id
),

-- Only keep stat_ids with useful fanout (2-{max_visitors} bnfpvids)
fanout_filtered AS (
  SELECT stat_id
  FROM device_fingerprints
  GROUP BY stat_id
  HAVING COUNT(DISTINCT bnfpvid) BETWEEN {min_visitors} AND {max_visitors}
)

SELECT df.bnfpvid, df.stat_id, df.first_seen, df.last_seen
FROM device_fingerprints df
JOIN fanout_filtered ff ON df.stat_id = ff.stat_id
"""

        if dry_run:
            self._dry_run_query(query, "device_stat_id")
            return 0

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            count = 0
            unique_stat_ids = set()

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                bnfpvid = row["bnfpvid"]
                stat_id = row["stat_id"]
                if not bnfpvid or not stat_id:
                    continue

                first_seen = (
                    row["first_seen"].isoformat() if row["first_seen"] else now_iso
                )
                last_seen = (
                    row["last_seen"].isoformat() if row["last_seen"] else now_iso
                )

                self._add_edge(
                    HubEdge(
                        identifier_a=bnfpvid,
                        identifier_a_type="bnfpvid",
                        identifier_b=stat_id,
                        identifier_b_type="stat_id",
                        source_system="acceptor",
                        link_type="probabilistic",
                        match_rule="DEVICE_STAT_ID",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1
                unique_stat_ids.add(stat_id)

            print(
                f"    {count:,} edges ({len(unique_stat_ids):,} unique stat_ids, {elapsed:.1f}s)"
            )
            self.stats["device_stat_id"] = {
                "edges": count,
                "unique_stat_ids": len(unique_stat_ids),
                "source_rows": source_rows,
                "elapsed": elapsed,
            }
            return count

        except Exception as e:
            print(f"    [device_stat_id] Error: {e}")
            self.stats["device_stat_id"] = {"edges": 0, "error": str(e)}
            return 0

    # ─── Phase 6b: Compound device+IP+time connector ────────────

    def connect_compound_device_ip(self, dry_run: bool = False) -> int:
        """
        Match bnfpvid pairs sharing the same stat_id (device fingerprint)
        AND same IP address within a 5-minute window. Three independent
        signals agreeing = strong cross-session evidence.

        Only pairs with 2+ observations produce edges above stitch threshold.
        Single observations are recorded as audit-only (confidence 0.7125).
        """
        if not self._is_connector_enabled("compound_device_ip"):
            print("  [compound_device_ip] Skipped (disabled)")
            return 0

        print("  [compound_device_ip] Matching stat_id+IP+time pairs...")
        cfg = self.connectors_config.get("compound_device_ip", {})
        confidence = float(cfg.get("confidence", 0.75))
        min_observations = int(cfg.get("min_observations", 2))
        source_ds = cfg.get("source_dataset", "BN_Acceptor")
        source_tbl = cfg.get("source_table", "acceptor_events")
        time_window = cfg.get("time_window_minutes", 5)
        max_ip_visitors = cfg.get("max_visitors_per_ip", 10)
        max_stat_visitors = cfg.get("max_visitors_per_stat_id", 20)
        rule_cap = float(
            self.confidence_caps_by_rule.get("COMPOUND_DEVICE_IP_TIME", 0.90)
        )
        fqt = self._fqt(source_ds, source_tbl)

        ts_filter = self._timestamp_filter("ae.publish_time", indent="      ")

        # Extract bnfpvid, stat_id (pre-computed by device_stat_id connector or
        # computed here), and IP from acceptor_events
        stat_id_hash = self._build_stat_id_hash_expr()

        query = f"""
-- Phase 6b: Compound device+IP+time cross-session matching
-- Three signals: same device fingerprint + same IP + same time window
WITH sessions AS (
  SELECT
    CAST(JSON_VALUE(ae.data, '$.userTracking.allLocalStorage.bnfpvid') AS STRING) AS bnfpvid,
    {stat_id_hash} AS stat_id,
    JSON_VALUE(ae.data, '$.userTracking.ipAddress') AS ip_address,
    ae.publish_time
  FROM `{fqt}` ae
  WHERE JSON_VALUE(ae.data, '$.eventMetadata.eventName') = 'page_load'
    AND JSON_VALUE(ae.data, '$.userTracking.allLocalStorage.bnfpvid') IS NOT NULL
    {ts_filter}
),
-- Filter out high-fanout IPs and stat_ids
ip_counts AS (
  SELECT ip_address, COUNT(DISTINCT bnfpvid) AS visitors
  FROM sessions WHERE ip_address IS NOT NULL
  GROUP BY 1 HAVING COUNT(DISTINCT bnfpvid) <= {max_ip_visitors}
),
stat_counts AS (
  SELECT stat_id, COUNT(DISTINCT bnfpvid) AS visitors
  FROM sessions WHERE stat_id IS NOT NULL
  GROUP BY 1 HAVING COUNT(DISTINCT bnfpvid) <= {max_stat_visitors}
),
filtered AS (
  SELECT s.*
  FROM sessions s
  JOIN ip_counts ic ON s.ip_address = ic.ip_address
  JOIN stat_counts sc ON s.stat_id = sc.stat_id
  WHERE s.stat_id IS NOT NULL AND s.ip_address IS NOT NULL
),
-- Find pairs sharing stat_id + IP within time window
pairs AS (
  SELECT
    f1.bnfpvid AS pvid_a,
    f2.bnfpvid AS pvid_b,
    COUNT(*) AS observations,
    MIN(f1.publish_time) AS first_seen,
    MAX(f2.publish_time) AS last_seen
  FROM filtered f1
  JOIN filtered f2
    ON f1.stat_id = f2.stat_id
    AND f1.ip_address = f2.ip_address
    AND f1.bnfpvid < f2.bnfpvid
    AND ABS(TIMESTAMP_DIFF(f1.publish_time, f2.publish_time, MINUTE)) <= {time_window}
  GROUP BY 1, 2
)
SELECT pvid_a, pvid_b, observations, first_seen, last_seen
FROM pairs
"""

        if dry_run:
            self._dry_run_query(query, "compound_device_ip")
            return 0

        start = time.time()
        count = 0
        source_rows = 0
        audit_only = 0
        below_min_obs = 0

        try:
            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                pvid_a = row["pvid_a"]
                pvid_b = row["pvid_b"]
                if not pvid_a or not pvid_b:
                    continue
                # Skip bot bnfpvids
                if pvid_a in self._bot_bnfpvids or pvid_b in self._bot_bnfpvids:
                    continue

                observations = int(row.get("observations") or 0)
                if observations < 1:
                    continue

                first_seen = (
                    row["first_seen"].isoformat() if row.get("first_seen") else ""
                )
                last_seen = row["last_seen"].isoformat() if row.get("last_seen") else ""

                # Pre-aggregated observation count: apply the same log2 boost
                # aggregate_confidence / BQ aggregation would apply if each
                # observation were inserted as its own staging row. Freeze the
                # boosted value into both confidence and base_confidence so a
                # later obs_count=1 aggregation pass does not undo it.
                #
                # Config contract (identity_hub.yaml + docstring):
                #   obs < min_observations → audit-only (below stitch after ratio)
                #   obs >= min_observations → stitchable after ratio/cap
                edge_conf = compound_device_ip_edge_confidence(
                    confidence, observations, min_observations, rule_cap
                )
                if observations < min_observations:
                    below_min_obs += 1
                    audit_only += 1
                else:
                    identity_ratio = self.observation_to_identity_ratio.get(
                        "COMPOUND_DEVICE_IP_TIME", 0.95
                    )
                    if edge_conf * identity_ratio < self.stitch_threshold:
                        audit_only += 1

                self._add_edge(
                    HubEdge(
                        identifier_a=pvid_a,
                        identifier_a_type="bnfpvid",
                        identifier_b=pvid_b,
                        identifier_b_type="bnfpvid",
                        source_system="acceptor",
                        link_type="probabilistic",
                        match_rule="COMPOUND_DEVICE_IP_TIME",
                        confidence=edge_conf,
                        base_confidence=edge_conf,
                        identity_cap=rule_cap,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

        except Exception as e:
            print(f"    Error in compound_device_ip: {e}")
            self.stats["compound_device_ip"] = {
                "edges": 0,
                "elapsed": 0,
                "error": str(e)[:200],
            }
            raise

        elapsed = time.time() - start
        stitchable = count - audit_only
        self.stats["compound_device_ip"] = {
            "edges": count,
            "source_rows": source_rows,
            "stitchable": stitchable,
            "audit_only": audit_only,
            "below_min_observations": below_min_obs,
            "min_observations": min_observations,
            "base_confidence": confidence,
            "stitch_threshold": self.stitch_threshold,
            "elapsed": elapsed,
        }
        # Publish probabilistic connector quality for ops tuning
        print(
            f"    {count:,} compound device+IP+time edges from {source_rows:,} pairs "
            f"(stitchable={stitchable:,} audit_only={audit_only:,}; "
            f"obs<{min_observations}={below_min_obs:,}; "
            f"base_conf={confidence} threshold={self.stitch_threshold}) "
            f"({elapsed:.1f}s)"
        )
        return count

    def _build_stat_id_hash_expr(self) -> str:
        """Build the SQL expression that computes stat_id from device signals.
        Matches the device_stat_id connector's hash logic."""
        cfg = self.connectors_config.get("device_stat_id", {})
        hash_fields = cfg.get(
            "hash_fields",
            [
                "$.userDeviceDetails.userAgent",
                "$.userDeviceDetails.screenWidth",
                "$.userDeviceDetails.screenHeight",
                "$.userDeviceDetails.devicePixelRatio",
                "$.userDeviceDetails.hardwareConcurrency",
                "$.userDeviceDetails.deviceMemory",
                "$.userDeviceDetails.colorDepth",
                "$.userDeviceDetails.platform",
                "$.userDeviceDetails.language",
                "$.userDeviceDetails.timeZone",
                "$.userDeviceDetails.maxTouchPoints",
                "$.userDeviceDetails.vendor",
            ],
        )
        concat_parts = ", ".join(
            f"COALESCE(JSON_VALUE(ae.data, '{f}'), '')" for f in hash_fields
        )
        return f"TO_HEX(SHA256(CONCAT({concat_parts})))"

    # ─── Phase 7: AIM Clickstream ─────────────────────────────

    def connect_aim_clickstream(self, dry_run: bool = False) -> int:
        """
        Extract aim_dgid <-> npi_number pairs from AIM clickstream data.
        147K unique pairs from 32M events across 2+ years of healthcare
        advertising events. Largest HCP identity bridge available.
        """
        if not self._is_connector_enabled("aim_clickstream"):
            print("  [aim_clickstream] Skipped (disabled)")
            return 0

        print("  [aim_clickstream] Extracting dgid <-> npi from AIM clickstream...")
        cfg = self.connectors_config["aim_clickstream"]
        confidence = cfg.get("confidence", 1.0)
        source_ds = cfg.get("source_dataset", "AIM_Clickstream")
        source_tbl = cfg.get("source_table", "AIM_bionews_bionews_weekly_webfeed")
        fqt = self._fqt(source_ds, source_tbl)

        query = f"""
-- Phase 7: AIM Clickstream — aim_dgid <-> npi_number
SELECT DISTINCT
  CAST(dgid AS STRING) AS aim_dgid,
  CAST(npi_number AS STRING) AS npi_number,
  MIN(event_timestamp) AS first_seen,
  MAX(event_timestamp) AS last_seen
FROM `{fqt}`
WHERE dgid IS NOT NULL AND npi_number IS NOT NULL
GROUP BY dgid, npi_number
"""

        if dry_run:
            self._dry_run_query(query, "aim_clickstream")
            return 0

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            count = 0
            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                aim_dgid = row["aim_dgid"]
                npi = row["npi_number"]
                if not aim_dgid or not npi or len(npi) != 10:
                    continue

                first_seen = (
                    row["first_seen"].isoformat() if row["first_seen"] else now_iso
                )
                last_seen = (
                    row["last_seen"].isoformat() if row["last_seen"] else now_iso
                )

                self._add_edge(
                    HubEdge(
                        identifier_a=aim_dgid,
                        identifier_a_type="aim_dgid",
                        identifier_b=npi,
                        identifier_b_type="npi_number",
                        source_system="aim_clickstream",
                        link_type="deterministic",
                        match_rule="AIM_CLICKSTREAM",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

            print(f"    {count:,} aim_dgid<->npi edges ({elapsed:.1f}s)")
            self.stats["aim_clickstream"] = {
                "edges": count,
                "source_rows": source_rows,
                "elapsed": elapsed,
            }
            return count

        except Exception as e:
            print(f"    [aim_clickstream] Error: {e}")
            self.stats["aim_clickstream"] = {"edges": 0, "error": str(e)}
            return 0

    # ─── Phase 8: DMD HCP Bridge ──────────────────────────────

    def connect_dmd_hcp_bridge(self, dry_run: bool = False) -> int:
        """
        Extract identity edges from DMD HCP Verified and Mailchimp DMD List.
        Produces edges: email <-> mc_euid, mc_euid <-> aim_dgid, aim_dgid <-> npi_number.
        The mc_euid <-> aim_dgid bridge is the critical tier1->HCP connector.
        """
        if not self._is_connector_enabled("dmd_hcp_bridge"):
            print("  [dmd_hcp_bridge] Skipped (disabled)")
            return 0

        print("  [dmd_hcp_bridge] Extracting DMD HCP verified identity links...")
        cfg = self.connectors_config["dmd_hcp_bridge"]
        confidence = cfg.get("confidence", 1.0)
        sources_cfg = cfg.get("sources", {})

        union_parts = []
        for src_name, src_cfg in sources_cfg.items():
            dataset = src_cfg["dataset"]
            table = src_cfg["table"]
            fqt = self._fqt(dataset, table)
            email_field = src_cfg["email_field"]
            mc_euid_field = src_cfg["mc_euid_field"]
            dgid_field = src_cfg["dgid_field"]
            npi_field = src_cfg["npi_field"]

            # Backtick-quote fields with spaces
            eq = f"`{email_field}`" if " " in email_field else email_field

            # email <-> mc_euid (carry NPI when present for timestamp join)
            union_parts.append(f"""
  SELECT
    'email_mc_euid' AS edge_type,
    LOWER(TRIM({eq})) AS id_a,
    'email' AS id_a_type,
    {mc_euid_field} AS id_b,
    'mc_euid' AS id_b_type,
    CAST({npi_field} AS STRING) AS npi_for_ts
  FROM `{fqt}`
  WHERE {eq} IS NOT NULL AND {mc_euid_field} IS NOT NULL
    AND TRIM({eq}) != '' AND TRIM({mc_euid_field}) != ''""")

            # mc_euid <-> aim_dgid
            union_parts.append(f"""
  SELECT
    'mc_euid_dgid' AS edge_type,
    {mc_euid_field} AS id_a,
    'mc_euid' AS id_a_type,
    {dgid_field} AS id_b,
    'aim_dgid' AS id_b_type,
    CAST({npi_field} AS STRING) AS npi_for_ts
  FROM `{fqt}`
  WHERE {mc_euid_field} IS NOT NULL AND {dgid_field} IS NOT NULL
    AND TRIM({mc_euid_field}) != '' AND TRIM({dgid_field}) != ''""")

            # aim_dgid <-> npi_number
            union_parts.append(f"""
  SELECT
    'dgid_npi' AS edge_type,
    {dgid_field} AS id_a,
    'aim_dgid' AS id_a_type,
    CAST({npi_field} AS STRING) AS id_b,
    'npi_number' AS id_b_type,
    CAST({npi_field} AS STRING) AS npi_for_ts
  FROM `{fqt}`
  WHERE {dgid_field} IS NOT NULL AND {npi_field} IS NOT NULL
    AND TRIM({dgid_field}) != ''""")

        npi_main = self._fqt(
            cfg.get("npi_main_dataset", "npi_data"),
            cfg.get("npi_main_table", "npi_main"),
        )
        union_sql = "\n  UNION ALL\n".join(union_parts)
        query = f"""
-- Phase 8: DMD HCP Bridge — email/mc_euid/aim_dgid/npi
SELECT
  e.edge_type, e.id_a, e.id_a_type, e.id_b, e.id_b_type,
  MIN(TIMESTAMP(m.provider_enumeration_date)) AS first_seen,
  MAX(COALESCE(
    TIMESTAMP(m.last_update_date),
    TIMESTAMP(m.provider_enumeration_date)
  )) AS last_seen
FROM (
{union_sql}
) e
LEFT JOIN `{npi_main}` m
  ON REGEXP_CONTAINS(TRIM(IFNULL(e.npi_for_ts, '')), r'^[0-9]{{10}}$')
 AND TRIM(e.npi_for_ts) = m.npi
WHERE e.id_a IS NOT NULL AND e.id_b IS NOT NULL
  AND TRIM(e.id_a) != '' AND TRIM(e.id_b) != ''
GROUP BY e.edge_type, e.id_a, e.id_a_type, e.id_b, e.id_b_type
"""

        if dry_run:
            self._dry_run_query(query, "dmd_hcp_bridge")
            return 0

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            count = 0
            type_counts = {}
            missing_ts = 0

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                id_a = row["id_a"]
                id_a_type = row["id_a_type"]
                id_b = row["id_b"]
                id_b_type = row["id_b_type"]
                edge_type = row["edge_type"]

                if not id_a or not id_b or id_b in self.invalid_values:
                    continue
                # Validate NPI is 10 digits
                if id_b_type == "npi_number" and (
                    len(id_b) != 10 or not id_b.isdigit()
                ):
                    continue

                first_seen = self._ts_iso(row.get("first_seen"))
                last_seen = self._ts_iso(row.get("last_seen"), fallback=first_seen)
                if not row.get("first_seen"):
                    missing_ts += 1

                self._add_edge(
                    HubEdge(
                        identifier_a=id_a,
                        identifier_a_type=id_a_type,
                        identifier_b=id_b,
                        identifier_b_type=id_b_type,
                        source_system="dmd_audiences",
                        link_type="deterministic",
                        match_rule="DMD_HCP_VERIFIED",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1
                type_counts[edge_type] = type_counts.get(edge_type, 0) + 1

            for t, c in sorted(type_counts.items()):
                print(f"    {t}: {c:,} edges")
            print(
                f"    Total: {count:,} DMD HCP edges "
                f"({missing_ts:,} used graph_start fallback, {elapsed:.1f}s)"
            )
            self.stats["dmd_hcp_bridge"] = {
                "edges": count,
                "source_rows": source_rows,
                "elapsed": elapsed,
                "type_counts": type_counts,
                "missing_source_ts": missing_ts,
            }
            return count

        except Exception as e:
            print(f"    [dmd_hcp_bridge] Error: {e}")
            self.stats["dmd_hcp_bridge"] = {"edges": 0, "error": str(e)}
            return 0

    # ─── Phase 9: NPI Phone Bridge ────────────────────────────

    def connect_npi_phone_bridge(self, dry_run: bool = False) -> int:
        """
        Extract npi_number <-> phone pairs from NPI doctor profiles.
        Introduces 'phone' as a new identifier type. 40K pairs.
        """
        if not self._is_connector_enabled("npi_phone_bridge"):
            print("  [npi_phone_bridge] Skipped (disabled)")
            return 0

        print("  [npi_phone_bridge] Extracting NPI <-> phone from doctor profiles...")
        cfg = self.connectors_config["npi_phone_bridge"]
        confidence = cfg.get("confidence", 1.0)
        source_ds = cfg.get("source_dataset", "npi_data")
        source_tbl = cfg.get("source_table", "doctor_profiles")
        fqt = self._fqt(source_ds, source_tbl)

        # doctor_profiles has no CMS dates; join npi_main for enumeration / last update.
        npi_main_ds = cfg.get("npi_main_dataset", source_ds)
        npi_main = self._fqt(npi_main_ds, cfg.get("npi_main_table", "npi_main"))
        query = f"""
-- Phase 9: NPI Phone Bridge — npi_number <-> phone
SELECT
  d.npi AS npi_number,
  d.phone AS raw_phone,
  MIN(TIMESTAMP(m.provider_enumeration_date)) AS first_seen,
  MAX(COALESCE(
    TIMESTAMP(m.last_update_date),
    TIMESTAMP(m.provider_enumeration_date)
  )) AS last_seen
FROM `{fqt}` d
LEFT JOIN `{npi_main}` m ON d.npi = m.npi
WHERE d.phone IS NOT NULL AND TRIM(d.phone) != ''
  AND LENGTH(TRIM(d.npi)) = 10
GROUP BY d.npi, d.phone
"""

        if dry_run:
            self._dry_run_query(query, "npi_phone_bridge")
            return 0

        from shared.cookie_normalizer import normalize_phone

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            count = 0
            rejected = 0
            missing_ts = 0

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                npi = row["npi_number"]
                phone = normalize_phone(row["raw_phone"])
                if not phone:
                    rejected += 1
                    continue

                first_seen = (
                    row["first_seen"].isoformat() if row["first_seen"] else now_iso
                )
                last_seen = (
                    row["last_seen"].isoformat() if row["last_seen"] else first_seen
                )
                if not row["first_seen"]:
                    missing_ts += 1

                self._add_edge(
                    HubEdge(
                        identifier_a=npi,
                        identifier_a_type="npi_number",
                        identifier_b=phone,
                        identifier_b_type="phone",
                        source_system="npi_registry",
                        link_type="deterministic",
                        match_rule="NPI_REGISTRY_PHONE",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

            print(
                f"    {count:,} npi<->phone edges ({rejected:,} rejected, "
                f"{missing_ts:,} used run-time fallback, {elapsed:.1f}s)"
            )
            self.stats["npi_phone_bridge"] = {
                "edges": count,
                "source_rows": source_rows,
                "rejected": rejected,
                "missing_source_ts": missing_ts,
                "elapsed": elapsed,
            }
            return count

        except Exception as e:
            print(f"    [npi_phone_bridge] Error: {e}")
            self.stats["npi_phone_bridge"] = {"edges": 0, "error": str(e)}
            return 0

    # ─── Phase 10: Mailchimp Phone Bridge ─────────────────────

    def connect_mailchimp_phone_bridge(self, dry_run: bool = False) -> int:
        """
        Extract identity pairs from Mailchimp member merge fields.
        - email <-> phone from MMERGE4/MMERGE5 (self-reported phone numbers)
        - email <-> npi_number from NPINUMBER/NPI_NUMBER (subscriber-declared HCP IDs)
        - email <-> aim_dgid from DGID (Doctor Group IDs)
        Combined with Phase 9, creates alternate path: email <-> phone <-> npi.
        """
        if not self._is_connector_enabled("mailchimp_phone_bridge"):
            print("  [mailchimp_phone_bridge] Skipped (disabled)")
            return 0

        print(
            "  [mailchimp_phone_bridge] Extracting email <-> phone from MC merge fields..."
        )
        cfg = self.connectors_config["mailchimp_phone_bridge"]
        confidence = cfg.get("confidence", 1.0)
        source_ds = cfg.get("source_dataset", "mailchimp_data")
        source_tbl = cfg.get("source_table", "members")
        # Scan ALL candidate merge fields. Mailchimp list configs vary — the
        # same "MMERGE7" slot could be "phone" in one list and "age" in another.
        # We scan broadly and let normalize_phone() validate.
        phone_fields = cfg.get(
            "phone_fields",
            [
                "MMERGE3",
                "MMERGE4",
                "MMERGE5",
                "MMERGE6",
                "MMERGE7",
                "MMERGE8",
                "MMERGE9",
                "MMERGE10",
                "MMERGE11",
                "MMERGE12",
                "MMERGE13",
                "MMERGE14",
                "MMERGE15",
                "MMERGE16",
                "MMERGE17",
                "MMERGE18",
                "MMERGE19",
                "MMERGE20",
                "PHONE",
            ],
        )
        fqt = self._fqt(source_ds, source_tbl)

        # UNPIVOT approach — emit one row per (email, field, value) so each
        # candidate phone gets validated independently by normalize_phone.
        select_parts = []
        for f in phone_fields:
            select_parts.append(
                f"SELECT LOWER(TRIM(email_address)) AS email, "
                f"'{f}' AS field, "
                f"NULLIF(JSON_VALUE(merge_fields, '$.{f}'), '') AS raw_phone, "
                f"timestamp_opt, timestamp_signup, last_changed, extracted_at "
                f"FROM `{fqt}` "
                f"WHERE email_address IS NOT NULL "
                f"AND JSON_VALUE(merge_fields, '$.{f}') IS NOT NULL"
            )
        union_sql = "\n  UNION ALL\n  ".join(select_parts)

        query = f"""
-- Phase 10: Mailchimp Phone Bridge — scan all MMERGE_N fields for phones
SELECT
  email,
  raw_phone,
  MIN(COALESCE(
    TIMESTAMP(timestamp_opt),
    TIMESTAMP(timestamp_signup),
    TIMESTAMP(last_changed),
    extracted_at
  )) AS first_seen,
  MAX(COALESCE(
    TIMESTAMP(last_changed),
    TIMESTAMP(timestamp_opt),
    TIMESTAMP(timestamp_signup),
    extracted_at
  )) AS last_seen
FROM (
  {union_sql}
)
WHERE raw_phone IS NOT NULL
  AND LENGTH(TRIM(raw_phone)) >= 7
  AND REGEXP_CONTAINS(raw_phone, r'[0-9]')
GROUP BY email, raw_phone
"""

        if dry_run:
            self._dry_run_query(query, "mailchimp_phone_bridge")
            return 0

        from shared.cookie_normalizer import normalize_phone

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            count = 0
            rejected = 0
            missing_ts = 0

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                email = row["email"]
                phone = normalize_phone(row["raw_phone"])
                if not email or not phone:
                    rejected += 1
                    continue

                first_seen = self._ts_iso(row.get("first_seen"))
                last_seen = self._ts_iso(row.get("last_seen"), fallback=first_seen)
                if not row.get("first_seen"):
                    missing_ts += 1

                self._add_edge(
                    HubEdge(
                        identifier_a=email,
                        identifier_a_type="email",
                        identifier_b=phone,
                        identifier_b_type="phone",
                        source_system="mailchimp",
                        link_type="deterministic",
                        match_rule="MAILCHIMP_PHONE",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

            print(
                f"    {count:,} email<->phone edges ({rejected:,} rejected, "
                f"{missing_ts:,} used graph_start fallback, {elapsed:.1f}s)"
            )

            # ── v3.1: Also extract email <-> npi_number and email <-> aim_dgid from merge fields ──
            hcp_fields = cfg.get("hcp_merge_fields", {})
            if hcp_fields:
                hcp_count = self._extract_mailchimp_hcp_merge_fields(
                    fqt, hcp_fields, confidence, dry_run=False
                )
                count += hcp_count

            self.stats["mailchimp_phone_bridge"] = {
                "edges": count,
                "source_rows": source_rows,
                "rejected": rejected,
                "missing_source_ts": missing_ts,
                "elapsed": elapsed,
            }
            return count

        except Exception as e:
            print(f"    [mailchimp_phone_bridge] Error: {e}")
            self.stats["mailchimp_phone_bridge"] = {"edges": 0, "error": str(e)}
            return 0

    def _extract_mailchimp_hcp_merge_fields(
        self, fqt: str, hcp_fields: dict, confidence: float, dry_run: bool = False
    ) -> int:
        """
        Extract email <-> npi_number and email <-> aim_dgid from Mailchimp merge fields.
        hcp_fields config format: {npi_fields: [NPINUMBER, NPI_NUMBER], dgid_fields: [DGID]}
        """
        npi_fields = hcp_fields.get("npi_fields", [])
        dgid_fields = hcp_fields.get("dgid_fields", [])

        union_parts = []
        for f in npi_fields:
            union_parts.append(f"""
  SELECT
    LOWER(TRIM(email_address)) AS email,
    TRIM(JSON_VALUE(merge_fields, '$.{f}')) AS id_value,
    'npi_number' AS id_type,
    timestamp_opt, timestamp_signup, last_changed, extracted_at
  FROM `{fqt}`
  WHERE email_address IS NOT NULL
    AND JSON_VALUE(merge_fields, '$.{f}') IS NOT NULL
    AND REGEXP_CONTAINS(TRIM(JSON_VALUE(merge_fields, '$.{f}')), r'^[0-9]{{10}}$')""")

        for f in dgid_fields:
            union_parts.append(f"""
  SELECT
    LOWER(TRIM(email_address)) AS email,
    TRIM(JSON_VALUE(merge_fields, '$.{f}')) AS id_value,
    'aim_dgid' AS id_type,
    timestamp_opt, timestamp_signup, last_changed, extracted_at
  FROM `{fqt}`
  WHERE email_address IS NOT NULL
    AND JSON_VALUE(merge_fields, '$.{f}') IS NOT NULL
    AND LENGTH(TRIM(JSON_VALUE(merge_fields, '$.{f}'))) >= {self.min_identifier_length}""")

        if not union_parts:
            return 0

        query = f"""
-- Phase 10b: Mailchimp HCP merge fields — email <-> npi_number, email <-> aim_dgid
SELECT
  email,
  id_value,
  id_type,
  MIN(COALESCE(
    TIMESTAMP(timestamp_opt),
    TIMESTAMP(timestamp_signup),
    TIMESTAMP(last_changed),
    extracted_at
  )) AS first_seen,
  MAX(COALESCE(
    TIMESTAMP(last_changed),
    TIMESTAMP(timestamp_opt),
    TIMESTAMP(timestamp_signup),
    extracted_at
  )) AS last_seen
FROM (
{chr(10).join("  UNION ALL" + p if i > 0 else p for i, p in enumerate(union_parts))}
)
GROUP BY email, id_value, id_type
"""
        if dry_run:
            self._dry_run_query(query, "mailchimp_hcp_fields")
            return 0

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            count = 0
            type_counts = {}
            missing_ts = 0

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                email = row["email"]
                id_value = row["id_value"]
                id_type = row["id_type"]
                if not email or not id_value:
                    continue

                first_seen = self._ts_iso(row.get("first_seen"))
                last_seen = self._ts_iso(row.get("last_seen"), fallback=first_seen)
                if not row.get("first_seen"):
                    missing_ts += 1

                self._add_edge(
                    HubEdge(
                        identifier_a=email,
                        identifier_a_type="email",
                        identifier_b=id_value,
                        identifier_b_type=id_type,
                        source_system="mailchimp",
                        link_type="deterministic",
                        match_rule="MAILCHIMP_HCP_MERGE",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1
                type_counts[id_type] = type_counts.get(id_type, 0) + 1

            for t, c in sorted(type_counts.items()):
                print(f"    {t}: {c:,} edges (from merge fields)")
            print(
                f"    Total: {count:,} HCP merge field edges "
                f"({missing_ts:,} used graph_start fallback, {elapsed:.1f}s)"
            )
            return count

        except Exception as e:
            print(f"    [mailchimp_hcp_fields] Error: {e}")
            return 0

    # ─── Phase 11: WordPress NPI Bridge ───────────────────────

    def connect_wp_npi_bridge(self, dry_run: bool = False) -> int:
        """
        Extract wp_user_id <-> npi_number from WordPress usermeta.
        Small volume (38 valid rows) but creates direct HCP bridge.
        """
        if not self._is_connector_enabled("wp_npi_bridge"):
            print("  [wp_npi_bridge] Skipped (disabled)")
            return 0

        print(
            "  [wp_npi_bridge] Extracting wp_user_id <-> npi from WordPress usermeta..."
        )
        cfg = self.connectors_config["wp_npi_bridge"]
        confidence = cfg.get("confidence", 1.0)
        source_ds = cfg.get("source_dataset", "wordpress_data")
        source_tbl = cfg.get("source_table", "wordpress_usermeta")
        meta_key = cfg.get("meta_key", "npi-number")
        fqt = self._fqt(source_ds, source_tbl)

        # wp_user_id is site-scoped — qualify as `{site}:{id}` to prevent
        # cross-site collisions (the same numeric id is a different person on
        # each WordPress install). Prefer NPI enumeration dates; fall back to
        # usermeta extracted_at.
        npi_main = self._fqt(
            cfg.get("npi_main_dataset", "npi_data"),
            cfg.get("npi_main_table", "npi_main"),
        )
        query = f"""
-- Phase 11: WordPress NPI Bridge — wp_user_id <-> npi_number
SELECT
  CONCAT(LOWER(TRIM(um.site)), ':', CAST(um.user_id AS STRING)) AS wp_user_id,
  TRIM(um.meta_value) AS npi_number,
  MIN(COALESCE(
    TIMESTAMP(m.provider_enumeration_date),
    um.extracted_at
  )) AS first_seen,
  MAX(COALESCE(
    TIMESTAMP(m.last_update_date),
    TIMESTAMP(m.provider_enumeration_date),
    um.extracted_at
  )) AS last_seen
FROM `{fqt}` um
LEFT JOIN `{npi_main}` m ON TRIM(um.meta_value) = m.npi
WHERE um.meta_key = '{meta_key}'
  AND REGEXP_CONTAINS(TRIM(um.meta_value), r'^[0-9]{{10}}$')
  AND um.site IS NOT NULL AND TRIM(um.site) != ''
GROUP BY 1, 2
"""

        if dry_run:
            self._dry_run_query(query, "wp_npi_bridge")
            return 0

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            count = 0
            missing_ts = 0

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                wp_user_id = row["wp_user_id"]
                npi = row["npi_number"]
                if not wp_user_id or not npi:
                    continue

                first_seen = self._ts_iso(row.get("first_seen"))
                last_seen = self._ts_iso(row.get("last_seen"), fallback=first_seen)
                if not row.get("first_seen"):
                    missing_ts += 1

                self._add_edge(
                    HubEdge(
                        identifier_a=wp_user_id,
                        identifier_a_type="wp_user_id",
                        identifier_b=npi,
                        identifier_b_type="npi_number",
                        source_system="wordpress",
                        link_type="deterministic",
                        match_rule="WORDPRESS_NPI",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

            print(
                f"    {count:,} wp_user_id<->npi edges "
                f"({missing_ts:,} used graph_start fallback, {elapsed:.1f}s)"
            )
            self.stats["wp_npi_bridge"] = {
                "edges": count,
                "source_rows": source_rows,
                "missing_source_ts": missing_ts,
                "elapsed": elapsed,
            }
            return count

        except Exception as e:
            print(f"    [wp_npi_bridge] Error: {e}")
            self.stats["wp_npi_bridge"] = {"edges": 0, "error": str(e)}
            return 0

    # ─── Phase 11b: WordPress MC4WP Email Bridge ────────────────

    def connect_wp_mc4wp_email_bridge(self, dry_run: bool = False) -> int:
        """
        Extract wp_user_id <-> email from WordPress MC4WP sync metadata.
        The MC4WP plugin syncs WP users to Mailchimp; this meta field stores
        the Mailchimp email address (which may differ from WP user_email).
        ~5,800 unique WP users with synced email addresses.
        """
        if not self._is_connector_enabled("wp_mc4wp_email_bridge"):
            print("  [wp_mc4wp_email_bridge] Skipped (disabled)")
            return 0

        print(
            "  [wp_mc4wp_email_bridge] Extracting wp_user_id <-> email from MC4WP sync..."
        )
        cfg = self.connectors_config["wp_mc4wp_email_bridge"]
        confidence = cfg.get("confidence", 1.0)
        source_ds = cfg.get("source_dataset", "wordpress_data")
        source_tbl = cfg.get("source_table", "wordpress_usermeta")
        meta_key = cfg.get("meta_key", "mc4wp_sync_remote_email_address")
        fqt = self._fqt(source_ds, source_tbl)

        # wp_user_id is site-scoped — qualify as `{site}:{id}` to prevent
        # cross-site collisions. Prefer user_registered from wordpress_users;
        # fall back to usermeta extracted_at.
        users_tbl = self._fqt(
            cfg.get("users_dataset", source_ds),
            cfg.get("users_table", "wordpress_users"),
        )
        query = f"""
-- Phase 11b: WordPress MC4WP Email Bridge — wp_user_id <-> email (from Mailchimp sync)
SELECT
  CONCAT(LOWER(TRIM(um.site)), ':', CAST(um.user_id AS STRING)) AS wp_user_id,
  LOWER(TRIM(um.meta_value)) AS email,
  MIN(COALESCE(
    CAST(u.user_registered AS TIMESTAMP),
    um.extracted_at
  )) AS first_seen,
  MAX(COALESCE(
    CAST(u.user_registered AS TIMESTAMP),
    um.extracted_at
  )) AS last_seen
FROM `{fqt}` um
LEFT JOIN `{users_tbl}` u
  ON um.user_id = u.ID
 AND LOWER(TRIM(um.site)) = LOWER(TRIM(u.site))
WHERE um.meta_key = '{meta_key}'
  AND um.meta_value IS NOT NULL
  AND TRIM(um.meta_value) != ''
  AND REGEXP_CONTAINS(TRIM(um.meta_value), r'^[^@]+@[^@]+\\.[^@]+$')
  AND um.site IS NOT NULL AND TRIM(um.site) != ''
GROUP BY 1, 2
"""

        if dry_run:
            self._dry_run_query(query, "wp_mc4wp_email_bridge")
            return 0

        try:
            start = time.time()
            source_rows = 0
            elapsed = time.time() - start

            count = 0
            missing_ts = 0

            for row in self._run_query(query, page_size=50000):
                source_rows += 1
                wp_user_id = row["wp_user_id"]
                email = row["email"]
                if not wp_user_id or not email or len(email) < 5:
                    continue

                first_seen = self._ts_iso(row.get("first_seen"))
                last_seen = self._ts_iso(row.get("last_seen"), fallback=first_seen)
                if not row.get("first_seen"):
                    missing_ts += 1

                self._add_edge(
                    HubEdge(
                        identifier_a=wp_user_id,
                        identifier_a_type="wp_user_id",
                        identifier_b=email,
                        identifier_b_type="email",
                        source_system="wordpress",
                        link_type="deterministic",
                        match_rule="WORDPRESS_MC4WP_EMAIL",
                        confidence=confidence,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1

            print(
                f"    {count:,} wp_user_id<->email edges "
                f"({missing_ts:,} used graph_start fallback, {elapsed:.1f}s)"
            )
            self.stats["wp_mc4wp_email_bridge"] = {
                "edges": count,
                "source_rows": source_rows,
                "missing_source_ts": missing_ts,
                "elapsed": elapsed,
            }
            return count

        except Exception as e:
            print(f"    [wp_mc4wp_email_bridge] Error: {e}")
            self.stats["wp_mc4wp_email_bridge"] = {"edges": 0, "error": str(e)}
            return 0

    def connect_onetrust_consent(self, dry_run: bool = False) -> int:
        """
        OneTrust consent subject → email identity edges.

        Disabled in YAML until onetrust_data extract is live. When enabled,
        emits deterministic email↔onetrust_subject edges for Union-Find.
        """
        if not self._is_connector_enabled("onetrust_consent"):
            print("  [onetrust_consent] Skipped (disabled)")
            return 0

        print("  [onetrust_consent] Linking OneTrust subjects to email...")
        cfg = self.connectors_config.get("onetrust_consent", {})
        confidence = float(cfg.get("confidence", 1.0))
        source_ds = cfg.get("source_dataset", "onetrust_data")
        source_tbl = cfg.get("source_table", "onetrust_data_subjects")
        fqt = self._fqt(source_ds, source_tbl)

        query = f"""
SELECT
  LOWER(TRIM(CAST(identifier AS STRING))) AS email,
  CAST(identifier AS STRING) AS subject_key,
  lastModifiedDate AS last_seen
FROM `{fqt}`
WHERE LOWER(IFNULL(CAST(identifierType AS STRING), 'email'))
      IN ('email', 'emailaddress', '')
  AND identifier IS NOT NULL
  AND LENGTH(TRIM(CAST(identifier AS STRING))) >= {self.min_identifier_length}
"""
        if dry_run:
            self._dry_run_query(query, "onetrust_consent")
            return 0

        start = time.time()
        count = 0
        try:
            for row in self._run_query(query, page_size=50000):
                email = row.get("email")
                if not email or "@" not in email:
                    continue
                last_seen = row["last_seen"].isoformat() if row.get("last_seen") else ""
                self._add_edge(
                    HubEdge(
                        identifier_a=email,
                        identifier_a_type="email",
                        identifier_b=f"ot:{row.get('subject_key') or email}",
                        identifier_b_type="onetrust_subject",
                        source_system="onetrust",
                        link_type="deterministic",
                        match_rule="ONETRUST_SUBJECT_EMAIL",
                        confidence=confidence,
                        base_confidence=confidence,
                        first_seen=last_seen,
                        last_seen=last_seen,
                    )
                )
                count += 1
        except Exception as e:
            print(f"    Error in onetrust_consent: {e}")
            self.stats["onetrust_consent"] = {"edges": 0, "error": str(e)[:200]}
            raise

        elapsed = time.time() - start
        self.stats["onetrust_consent"] = {"edges": count, "elapsed": elapsed}
        print(f"    {count:,} OneTrust consent edges ({elapsed:.1f}s)")
        return count

    # ─── Bot detection ──────────────────────────────────────────

    def _detect_bot_bnfpvids(self, dry_run: bool = False) -> int:
        """
        Query BN_Acceptor.acceptor_events to identify bnfpvids with bot
        user agents or headless browser signals. Populates self._bot_bnfpvids.
        Confirmed bots are added to self.blacklisted_identifiers before quality filtering.
        """
        if not self.bot_detection_enabled:
            print("  [bot_detection] Skipped (disabled)")
            return 0

        print("  [bot_detection] Scanning for bot bnfpvids...")
        fqt = self._fqt("BN_Acceptor", "acceptor_events")
        ua_path = "JSON_VALUE(ae.data, '$.userDeviceDetails.userAgent')"

        # Build UA pattern OR clauses
        ua_clauses = [f"{ua_path} LIKE '%{p}%'" for p in self.bot_ua_patterns]

        # Headless browser detection clause
        headless_clause = ""
        if self.bot_headless_detection:
            headless_clause = f"""
    OR ({ua_path} IS NOT NULL
        AND JSON_VALUE(ae.data, '$.userDeviceDetails.vendor') IS NULL
        AND SAFE_CAST(JSON_VALUE(ae.data, '$.userDeviceDetails.maxTouchPoints') AS INT64) = 0)"""

        where_parts = "\n    OR ".join(ua_clauses) + headless_clause

        # Limit scan to recent data — older bots are already blacklisted from prior runs
        bot_lookback = self.config.get("bot_detection", {}).get("lookback_days", 90)
        # Test mode: match bot detection window to the sample window
        if self._is_test_mode and self.start_date:
            try:
                sample_start = datetime.strptime(self.start_date, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                sample_days = max(1, (datetime.now(timezone.utc) - sample_start).days)
                bot_lookback = min(bot_lookback, sample_days)
            except Exception:
                pass

        query = f"""
-- Bot detection: find bnfpvids with bot UA or headless signals
SELECT DISTINCT
  JSON_VALUE(ae.data, '$.userTracking.allLocalStorage.bnfpvid') AS bnfpvid
FROM `{fqt}` ae
WHERE ae.publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {bot_lookback} DAY)
  AND JSON_VALUE(ae.data, '$.userTracking.allLocalStorage.bnfpvid') IS NOT NULL
  AND (
    {where_parts}
  )
"""

        if dry_run:
            self._dry_run_query(query, "bot_detection")
            return 0

        try:
            start = time.time()
            elapsed = time.time() - start

            for row in self._run_query(query, page_size=50000):
                pvid = row["bnfpvid"]
                if pvid:
                    self._bot_bnfpvids.add(pvid)

            print(
                f"    Found {len(self._bot_bnfpvids):,} bot bnfpvids ({elapsed:.1f}s)"
            )

            # Add to blacklist if confirmed_bot_action is 'remove'
            if self.bot_confirmed_action == "remove" and self._bot_bnfpvids:
                for pvid in self._bot_bnfpvids:
                    self.blacklisted_identifiers.add(f"bnfpvid:{pvid}")
                print(
                    f"    Blacklisted {len(self._bot_bnfpvids):,} bot bnfpvids for removal"
                )

            self.stats["bot_detection"] = {
                "bot_bnfpvids": len(self._bot_bnfpvids),
                "elapsed": elapsed,
                "failed": 0,
            }
            return len(self._bot_bnfpvids)

        except Exception as e:
            # Deliberate fail-open: bot detection is advisory infrastructure,
            # not an edge connector (the connector-failure checkpoint skips it).
            # Prior-run blacklist entries plus fanout/cluster caps bound the
            # damage, but this run stitches WITHOUT fresh bot filtering, so
            # make the degradation loud and queryable:
            #   bn_id_metrics metric_name = 'bot_detection.failed' (1 = failed)
            print("    " + "!" * 66)
            print(f"    [bot_detection] FAILED: {str(e)[:200]}")
            print(
                "    [bot_detection] Proceeding WITHOUT fresh bot filtering "
                "for this run."
            )
            print(
                "    [bot_detection] Prior-run blacklists still apply; new bots "
                "are NOT filtered."
            )
            print(
                "    [bot_detection] Alert metric: bot_detection.failed=1 in "
                "bn_id_metrics."
            )
            print("    " + "!" * 66)
            self.stats["bot_detection"] = {
                "bot_bnfpvids": 0,
                "error": str(e),
                "failed": 1,
            }
            return 0

    # ─── Cookie normalization resolution ───────────────────────

    def resolve_cookie_normalization(self) -> int:
        """Cookie normalization now applied inline in _add_edge(). Report stats only."""
        if not self.normalize_cookies:
            return 0
        norm_stats = self.normalizer.get_stats()
        self.stats["cookie_normalization"] = {
            "ga_merged_to_client_id": norm_stats.get("stripped_prefix", 0),
            **norm_stats,
        }
        print(
            f"  [normalization] {norm_stats['stripped_prefix']:,} prefixes stripped "
            f"(applied inline during edge collection)"
        )
        return norm_stats.get("stripped_prefix", 0)

    # ─── Confidence aggregation ──────────────────────────────────

    def aggregate_confidence(self) -> int:
        """
        Apply log2 confidence boost and materialize edges from _edge_agg.

        Online aggregation already deduplicated edges during connector execution.
        This method applies the confidence boost formula and materializes the
        edge list for downstream consumers (quality filters, Union-Find, writes).

        _edge_agg values are compact lists:
          [confidence, base_confidence, identity_cap, source_system,
           link_type, match_rule, count, first_seen, last_seen]

        Memory design: drains _edge_agg via popitem() to avoid holding both
        the dict and the list simultaneously.
        """
        print("  Aggregating edge confidence...")
        if not self._edge_agg:
            self._edges_list = []
            return 0

        original_count = self._edge_count_raw
        log2 = math.log2
        boosted = 0

        # Materialize edges by draining the dict (popitem avoids large key-list allocation)
        self._edges_list = []
        append = self._edges_list.append

        while self._edge_agg:
            pair, slot = self._edge_agg.popitem()
            # slot: [conf, base_conf, id_cap, source, link, rule, count, first_seen, last_seen]
            conf = slot[0]
            base_conf = slot[1]
            count = slot[6]

            if base_conf < 0:
                base_conf = conf

            if count > 1:
                rule_cap = self.confidence_caps_by_rule.get(slot[5], 1.0)
                new_conf = min(rule_cap, base_conf * log2(count + 1))
                if new_conf > conf:
                    conf = new_conf
                    boosted += 1
            else:
                rule_cap = self.confidence_caps_by_rule.get(slot[5], 1.0)
                if conf > rule_cap:
                    conf = rule_cap

            key_a, key_b = pair
            a_type, a_val = key_a.split(":", 1)
            b_type, b_val = key_b.split(":", 1)
            append(
                HubEdge(
                    identifier_a=a_val,
                    identifier_a_type=a_type,
                    identifier_b=b_val,
                    identifier_b_type=b_type,
                    source_system=slot[3],
                    link_type=slot[4],
                    match_rule=slot[5],
                    confidence=conf,
                    base_confidence=base_conf,
                    identity_cap=slot[2],
                    first_seen=slot[7],
                    last_seen=slot[8],
                )
            )

        deduped = original_count - len(self._edges_list)
        print(
            f"    {original_count:,} edges -> {len(self._edges_list):,} unique pairs "
            f"({deduped:,} duplicates merged, {boosted:,} confidence boosted)"
        )
        self.stats["confidence_aggregation"] = {
            "original_edges": original_count,
            "unique_pairs": len(self._edges_list),
            "duplicates_merged": deduped,
            "confidence_boosted": boosted,
        }
        # Force GC to reclaim dict memory before quality filters allocate more
        import gc

        gc.collect()
        return deduped

    # ─── Quality filters ───────────────────────────────────────

    def _get_fanout_threshold(self, id_type: str, match_rule: str = None) -> int:
        """Get the fanout threshold for a node.

        Lookup order (most specific wins):
          1. per-rule override: fanout_thresholds_by_rule['{id_type}.{match_rule}']
          2. per-type: fanout_thresholds[id_type]
          3. default: default_fanout_threshold
        """
        if match_rule and self.fanout_thresholds_by_rule:
            rule_key = f"{id_type}.{match_rule}"
            if rule_key in self.fanout_thresholds_by_rule:
                return self.fanout_thresholds_by_rule[rule_key]
        if self.fanout_thresholds:
            return self.fanout_thresholds.get(id_type, self.default_fanout_threshold)
        return self.max_identifiers_per_visitor

    def apply_quality_filters(self) -> int:
        """Remove edges violating per-type cardinality thresholds and blacklists.

        v3.3: Also enforces per-rule fanout caps. A node that exceeds the
        per-rule threshold only under a specific rule has its edges from that
        rule removed, while edges from other rules remain.
        """
        print("  Applying quality filters...")

        # Remove blacklisted identifiers (in-place to avoid doubling memory)
        blacklisted_removed = 0
        if self.blacklisted_identifiers:
            bl = self.blacklisted_identifiers
            write_idx = 0
            for edge in self._edges_list:
                if (
                    f"{edge.identifier_a_type}:{edge.identifier_a}" not in bl
                    and f"{edge.identifier_b_type}:{edge.identifier_b}" not in bl
                ):
                    self._edges_list[write_idx] = edge
                    write_idx += 1
                else:
                    blacklisted_removed += 1
            del self._edges_list[write_idx:]  # Trim in-place
            if blacklisted_removed:
                print(
                    f"    Removed {blacklisted_removed:,} edges from {len(bl)} blacklisted identifiers"
                )

        # Count degree per node globally AND per (node, rule) pair.
        # Only count per-rule degrees for rules that have configured caps
        # (avoids allocating ~80M tuples for unconfigured rules).
        #
        # WP_USER_EMAIL edges are excluded from email-node degree counts —
        # matching the BQ full-rebuild filter in _run_bq_quality_filters.
        # WordPress can legitimately link one email to many wp_user_id values
        # across sites; counting those edges as email fanout falsely drops
        # known identities on the incremental Python path.
        configured_rules = set()
        if self.fanout_thresholds_by_rule:
            for key in self.fanout_thresholds_by_rule:
                parts = key.split(".", 1)
                if len(parts) == 2:
                    configured_rules.add(parts[1])

        node_degree: Dict[str, int] = defaultdict(int)
        node_rule_degree: Dict[tuple, int] = defaultdict(int)
        for edge in self.edges:
            key_a = f"{edge.identifier_a_type}:{edge.identifier_a}"
            key_b = f"{edge.identifier_b_type}:{edge.identifier_b}"
            skip_email_fanout = edge.match_rule == "WP_USER_EMAIL"
            if not (skip_email_fanout and edge.identifier_a_type == "email"):
                node_degree[key_a] += 1
            if not (skip_email_fanout and edge.identifier_b_type == "email"):
                node_degree[key_b] += 1
            if configured_rules and edge.match_rule in configured_rules:
                node_rule_degree[(key_a, edge.match_rule)] += 1
                node_rule_degree[(key_b, edge.match_rule)] += 1

        # Find over-linked nodes using per-type thresholds (anchor-aware:
        # same semantics as the BigQuery rebuild path -- see fanout_decisions).
        del node_degree
        remove_all_nodes, trim_nodes = fanout_decisions(
            self.edges, self._get_fanout_threshold, self.person_types
        )
        bad_nodes = remove_all_nodes | trim_nodes
        type_bad_counts: Dict[str, int] = defaultdict(int)
        for node in bad_nodes:
            type_bad_counts[node.split(":", 1)[0]] += 1

        # Find (node, rule) pairs that exceed per-rule caps
        # These are narrower — only edges of the offending rule are removed,
        # not all edges for that node.
        bad_node_rules = set()
        rule_bad_counts: Dict[str, int] = defaultdict(int)
        if self.fanout_thresholds_by_rule:
            for (node, rule), degree in node_rule_degree.items():
                id_type = node.split(":", 1)[0]
                rule_key = f"{id_type}.{rule}"
                if rule_key in self.fanout_thresholds_by_rule:
                    threshold = self.fanout_thresholds_by_rule[rule_key]
                    if degree > threshold:
                        bad_node_rules.add((node, rule))
                        rule_bad_counts[rule_key] += 1
        del node_rule_degree

        if bad_nodes or bad_node_rules:
            original = len(self._edges_list)
            write_idx = 0
            for e in self._edges_list:
                a_key = f"{e.identifier_a_type}:{e.identifier_a}"
                b_key = f"{e.identifier_b_type}:{e.identifier_b}"
                if (
                    not fanout_edge_removed(
                        e, remove_all_nodes, trim_nodes, self.person_types
                    )
                    and (a_key, e.match_rule) not in bad_node_rules
                    and (b_key, e.match_rule) not in bad_node_rules
                ):
                    self._edges_list[write_idx] = e
                    write_idx += 1
            del self._edges_list[write_idx:]
            removed = original - len(self._edges_list)
            type_summary = ", ".join(
                f"{t}={c}" for t, c in sorted(type_bad_counts.items())
            )
            print(
                f"    Removed {removed:,} edges ({len(bad_nodes):,} over-linked nodes: {type_summary}; "
                f"{len(trim_nodes):,} trimmed to anchor edges, {len(remove_all_nodes):,} removed whole)"
            )
            if rule_bad_counts:
                rule_summary = ", ".join(
                    f"{r}={c}" for r, c in sorted(rule_bad_counts.items())
                )
                print(
                    f"    Additionally: {len(bad_node_rules):,} (node, rule) pairs over per-rule caps: {rule_summary}"
                )
            self.stats["quality_filter"] = {
                "removed_edges": removed,
                "bad_nodes": len(bad_nodes),
                "trimmed_nodes": len(trim_nodes),
                "removed_whole_nodes": len(remove_all_nodes),
                "bad_nodes_by_type": dict(type_bad_counts),
                "bad_node_rules": len(bad_node_rules),
                "bad_node_rules_by_key": dict(rule_bad_counts),
                "blacklisted_removed": blacklisted_removed,
            }
            return removed + blacklisted_removed

        print(f"    No over-linked nodes found (per-type thresholds)")
        self.stats["quality_filter"] = {
            "removed_edges": 0,
            "bad_nodes": 0,
            "blacklisted_removed": blacklisted_removed,
        }
        return blacklisted_removed

    # ─── Person anchoring ─────────────────────────────────────

    def _classify_component(self, members: List[str]) -> Optional[str]:
        """Classify a component as 'tier1', 'tier2', or None (drop)."""
        if not self.person_anchoring_enabled:
            return "tier1"
        member_types = {m.split(":", 1)[0] for m in members}
        if member_types & self.tier1_types:
            return "tier1"
        if member_types & self.tier2_types:
            return "tier2"
        return None

    # ─── Edge aging ─────────────────────────────────────────────

    def _get_edge_age_days(self, edge) -> float:
        """Calculate edge age in days from last_seen."""
        try:
            if isinstance(edge.last_seen, str):
                last_seen = datetime.fromisoformat(
                    edge.last_seen.replace("Z", "+00:00")
                )
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)
            else:
                last_seen = edge.last_seen
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - last_seen).total_seconds() / 86400
        except Exception:
            return 0  # If we can't parse, treat as fresh

    def _get_decay_weight(self, age_days: float) -> float:
        """Look up decay weight from the schedule based on edge age."""
        for bucket in self.decay_schedule:
            if age_days <= bucket["max_age_days"]:
                return bucket["weight"]
        return 0.0  # Beyond all buckets = excluded

    def _get_type_influence_days(self, id_type: str) -> int:
        """Get max influence window for a type. -1 = permanent."""
        return self.type_influence_days.get(id_type, 365)

    def _edge_excluded_by_aging(self, edge) -> bool:
        """Check if an edge should be excluded from stitching due to aging."""
        if not self.edge_aging_enabled:
            return False

        # Check per-type influence windows for both endpoints
        influence_a = self._get_type_influence_days(edge.identifier_a_type)
        influence_b = self._get_type_influence_days(edge.identifier_b_type)

        # Use the shorter influence window (more restrictive endpoint)
        # -1 means permanent
        if influence_a == -1 and influence_b == -1:
            return False  # Both permanent
        if influence_a == -1:
            effective_influence = influence_b
        elif influence_b == -1:
            effective_influence = influence_a
        else:
            effective_influence = min(influence_a, influence_b)

        age_days = self._get_edge_age_days(edge)
        if age_days > effective_influence:
            return True  # Beyond influence window

        return False

    def _get_effective_confidence(self, edge) -> float:
        """Return identity-stitching confidence for an edge.

        effective = min(identity_cap, confidence * obs_to_id_ratio * age_decay_weight)

        - confidence: observation strength ("we saw these two together")
        - obs_to_id_ratio: how well observation implies identity (default 1.0)
        - age_decay_weight: edge aging (1.0 if disabled, or 1.0 if one endpoint
          is a permanent anchor — known-identity edges don't decay)
        - identity_cap: per-edge ceiling from gates (conflict, etc.); default 1.0
        """
        # v3.4: apply observation-to-identity ratio (defaults to 1.0 for
        # deterministic bridges where observation = identity)
        identity_ratio = self.observation_to_identity_ratio.get(edge.match_rule, 1.0)
        base = edge.confidence * identity_ratio
        if self.edge_aging_enabled:
            # Skip decay if EITHER endpoint is a permanent anchor type.
            # If one side is email/mc_euid/npi_number/etc., the identity link
            # is established — only pure browser-to-browser edges should decay.
            influence_a = self._get_type_influence_days(edge.identifier_a_type)
            influence_b = self._get_type_influence_days(edge.identifier_b_type)
            has_permanent_anchor = influence_a == -1 or influence_b == -1
            if not has_permanent_anchor:
                age_days = self._get_edge_age_days(edge)
                decay_weight = self._get_decay_weight(age_days)
                base *= decay_weight
        # identity_cap applies after obs ratio + decay so it's the hardest
        # ceiling — used by gates to force an edge below stitch_threshold.
        cap = getattr(edge, "identity_cap", 1.0)
        return min(base, cap)

    # ─── Union-Find stitching ──────────────────────────────────

    def _gate_shared_workstation_edges(self) -> int:
        """v3.3: Remove bnfpvid<->mc_euid edges where the bnfpvid is a shared
        workstation (3+ mc_euids observed on the same device).

        Runs BEFORE Union-Find so the bad edges never stitch.
        Returns the number of edges removed.
        """
        if not self.shared_ws_flag_min or self.shared_ws_flag_min < 2:
            return 0

        # Count distinct mc_euids per bnfpvid via LOCALSTORAGE_COOCCURRENCE edges
        pvid_euids: Dict[str, Set[str]] = defaultdict(set)
        for edge in self.edges:
            if edge.match_rule != "LOCALSTORAGE_COOCCURRENCE":
                continue
            if (
                edge.identifier_a_type == "bnfpvid"
                and edge.identifier_b_type == "mc_euid"
            ):
                pvid_euids[edge.identifier_a].add(edge.identifier_b)
            elif (
                edge.identifier_b_type == "bnfpvid"
                and edge.identifier_a_type == "mc_euid"
            ):
                pvid_euids[edge.identifier_b].add(edge.identifier_a)

        # Flag at LOWER threshold (2+) — for is_shared_workstation cluster attribute
        flagged_pvids: Set[str] = {
            pvid
            for pvid, euids in pvid_euids.items()
            if len(euids) >= self.shared_ws_flag_min
        }
        # Remove at HIGHER threshold (3+) — conservative edge deletion
        remove_pvids: Set[str] = {
            pvid
            for pvid, euids in pvid_euids.items()
            if len(euids) >= self.shared_ws_remove_min
        }
        del pvid_euids

        # Persist the FLAGGED set (lower threshold) for cluster attribute marking.
        # _compute_cluster_attributes uses this to set is_shared_workstation=TRUE.
        self._shared_workstation_pvids = flagged_pvids

        if not remove_pvids:
            self.stats["shared_workstation_gate"] = {
                "flagged_pvids": len(flagged_pvids),
                "remove_pvids": 0,
                "edges_removed": 0,
            }
            if flagged_pvids:
                print(
                    f"    Shared-workstation: {len(flagged_pvids):,} flagged (>={self.shared_ws_flag_min}), "
                    f"0 removed (none at ≥{self.shared_ws_remove_min})"
                )
            return 0

        # Remove edges only for bnfpvids at the HIGHER threshold
        original = len(self.edges)
        self.edges = [
            e
            for e in self.edges
            if not (
                e.match_rule == "LOCALSTORAGE_COOCCURRENCE"
                and (
                    (
                        e.identifier_a_type == "bnfpvid"
                        and e.identifier_b_type == "mc_euid"
                        and e.identifier_a in remove_pvids
                    )
                    or (
                        e.identifier_b_type == "bnfpvid"
                        and e.identifier_a_type == "mc_euid"
                        and e.identifier_b in remove_pvids
                    )
                )
            )
        ]
        removed = original - len(self.edges)
        print(
            f"    Shared-workstation: {len(flagged_pvids):,} flagged (>={self.shared_ws_flag_min}), "
            f"{len(remove_pvids):,} removal candidates (≥{self.shared_ws_remove_min}) -> "
            f"{removed:,} edges suppressed"
        )
        self.stats["shared_workstation_gate"] = {
            "flagged_pvids": len(flagged_pvids),
            "remove_pvids": len(remove_pvids),
            "edges_removed": removed,
        }
        return removed

    def _compute_conflict_scores(self) -> int:
        """v3.4: Identify nodes that create impossible-identity clusters.

        An anchor identifier should never connect to multiple distinct values
        of another anchor identifier. For example:
          - One bnfpvid connected to 5 different emails (probably shared device)
          - One email connected to 3 different NPIs (probably data quality issue)
          - One npi_number connected to 2 different emails (sometimes legit)

        Strategy: for each bridge node (non-anchor), count how many distinct
        anchor values of each type it transitively touches via direct edges.
        If the count exceeds a threshold, mark the bridge node as conflicted
        and downweight its edges below stitch_threshold.

        Returns number of edges downweighted.
        """
        # Anchor types to check (conflict across these is suspicious)
        conflict_types = {"email", "mc_euid", "npi_number", "bionews_uk"}
        # Thresholds: N distinct anchor values through one bridge = suspicious
        conflict_thresholds: Dict[str, int] = {
            "email": 3,  # one bridge touching 3+ emails -> suspicious
            "npi_number": 2,  # one bridge touching 2+ NPIs -> suspicious
            "mc_euid": 3,
            "bionews_uk": 2,
        }

        # For each bridge node, collect sets of anchor values by type
        # Bridge = any non-anchor identifier appearing in edges with anchors
        bridge_to_anchors: Dict[str, Dict[str, Set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        for edge in self.edges:
            a_key = f"{edge.identifier_a_type}:{edge.identifier_a}"
            b_key = f"{edge.identifier_b_type}:{edge.identifier_b}"
            a_is_anchor = edge.identifier_a_type in conflict_types
            b_is_anchor = edge.identifier_b_type in conflict_types

            if a_is_anchor and not b_is_anchor:
                # Non-anchor b bridges to anchor a
                bridge_to_anchors[b_key][edge.identifier_a_type].add(edge.identifier_a)
            elif b_is_anchor and not a_is_anchor:
                # Non-anchor a bridges to anchor b
                bridge_to_anchors[a_key][edge.identifier_b_type].add(edge.identifier_b)
            elif (
                a_is_anchor
                and b_is_anchor
                and edge.identifier_a_type != edge.identifier_b_type
            ):
                # Both sides are anchors of DIFFERENT types (email <-> npi_number,
                # email <-> mc_euid, etc.). Each side acts as a bridge to the
                # other's anchor-type cluster. Counts "how many distinct NPIs
                # does this email connect to" directly.
                bridge_to_anchors[a_key][edge.identifier_b_type].add(edge.identifier_b)
                bridge_to_anchors[b_key][edge.identifier_a_type].add(edge.identifier_a)

        # Find bridges that exceed any threshold
        conflicted_bridges: Set[str] = set()
        conflict_counts: Dict[str, int] = defaultdict(int)
        for bridge, anchors_by_type in bridge_to_anchors.items():
            for atype, values in anchors_by_type.items():
                threshold = conflict_thresholds.get(atype, 999)
                if len(values) >= threshold:
                    conflicted_bridges.add(bridge)
                    conflict_counts[atype] += 1
                    break
        del bridge_to_anchors

        if not conflicted_bridges:
            self.stats["conflict_gate"] = {
                "conflicted_bridges": 0,
                "edges_downweighted": 0,
            }
            return 0

        # Cap the identity-stitching confidence on conflicted edges without
        # overwriting the observation confidence (which we want preserved in
        # the hub for audit). Union-Find sees 0.5 (below stitch_threshold);
        # hub still sees the true observation strength.
        CONFLICT_CAP = 0.5
        downweighted = 0
        for edge in self.edges:
            a_key = f"{edge.identifier_a_type}:{edge.identifier_a}"
            b_key = f"{edge.identifier_b_type}:{edge.identifier_b}"
            if a_key in conflicted_bridges or b_key in conflicted_bridges:
                if edge.identity_cap > CONFLICT_CAP:
                    edge.identity_cap = CONFLICT_CAP
                    downweighted += 1

        summary = ", ".join(f"{t}={c}" for t, c in sorted(conflict_counts.items()))
        print(
            f"    Conflict gate: {len(conflicted_bridges):,} bridges flagged "
            f"(conflicts: {summary}) -> {downweighted:,} edges downweighted"
        )
        self.stats["conflict_gate"] = {
            "conflicted_bridges": len(conflicted_bridges),
            "edges_downweighted": downweighted,
            "conflicts_by_type": dict(conflict_counts),
        }
        return downweighted

    def run_union_find(self) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
        """
        Run PriorityUnionFind on edges above stitch_threshold.
        Applies edge aging (confidence decay) and per-type influence windows.
        Returns (node_to_bn_id, components_by_bn_id).
        """
        # v3.3: suppress shared-workstation edges BEFORE stitching so they
        # don't cause false merges (cluster_attribute labeling was too late).
        self._gate_shared_workstation_edges()

        # v3.4: identify and downweight edges touching conflict bridges
        # (e.g., one bnfpvid connected to 5 different emails).
        self._compute_conflict_scores()

        print(
            f"  Running Union-Find (stitch_threshold={self.stitch_threshold}"
            f"{', edge_aging=ON' if self.edge_aging_enabled else ''})..."
        )
        start = time.time()

        # Build node priority from source_priorities config
        total_edges = len(self.edges)
        print(f"    Building node priorities ({total_edges:,} edges)...", flush=True)
        node_priority: Dict[str, int] = {}
        for edge in self.edges:
            key_a = f"{edge.identifier_a_type}:{edge.identifier_a}"
            key_b = f"{edge.identifier_b_type}:{edge.identifier_b}"
            pri_a = self.source_priorities.get(edge.identifier_a_type, 99)
            pri_b = self.source_priorities.get(edge.identifier_b_type, 99)
            node_priority[key_a] = min(node_priority.get(key_a, 99), pri_a)
            node_priority[key_b] = min(node_priority.get(key_b, 99), pri_b)
        print(f"    {len(node_priority):,} unique nodes", flush=True)

        uf = PriorityUnionFind(node_priority)

        # Compute browser edge expiry cutoff (legacy tiering, used when edge_aging disabled)
        browser_cutoff = (
            datetime.now(timezone.utc) - timedelta(days=self.browser_expiry_days)
        ).strftime("%Y-%m-%dT%H:%M:%S")

        # Union edges above confidence threshold
        print(f"    Stitching edges...", flush=True)
        unions = 0
        below_threshold = 0
        expired_browser = 0
        aged_out = 0
        for i, edge in enumerate(self.edges):
            if (i + 1) % 1_000_000 == 0:
                print(
                    f"    ... {i + 1:,}/{total_edges:,} edges processed, {unions:,} unions",
                    flush=True,
                )
            # Edge aging: check per-type influence window
            if self._edge_excluded_by_aging(edge):
                aged_out += 1
                continue

            # Apply confidence decay
            effective_conf = self._get_effective_confidence(edge)
            if effective_conf < self.stitch_threshold:
                below_threshold += 1
                continue

            # Legacy edge tiering (when edge_aging is disabled)
            if not self.edge_aging_enabled:
                is_person_edge = (
                    edge.identifier_a_type in self.person_types
                    or edge.identifier_b_type in self.person_types
                )
                if not is_person_edge and edge.last_seen < browser_cutoff:
                    expired_browser += 1
                    continue

            key_a = f"{edge.identifier_a_type}:{edge.identifier_a}"
            key_b = f"{edge.identifier_b_type}:{edge.identifier_b}"
            if key_a == key_b:
                continue  # Self-link
            if uf.union(key_a, key_b):
                unions += 1

        # Generate bn_id for each component
        node_to_bn_id: Dict[str, str] = {}
        components = uf.get_components()
        components_by_bn_id: Dict[str, List[str]] = {}

        dropped_components = 0
        dropped_nodes = 0
        oversized_components = 0
        oversized_nodes = 0
        tier_counts = {"tier1": 0, "tier2": 0}
        self._bn_id_tiers: Dict[str, str] = {}

        for root, members in components.items():
            # Cluster size hard cap
            if len(members) > self.max_cluster_size:
                oversized_components += 1
                oversized_nodes += len(members)
                continue

            tier = self._classify_component(members)
            if tier is None:
                dropped_components += 1
                dropped_nodes += len(members)
                continue
            bn_id = self._generate_bn_id(root)
            self._bn_id_tiers[bn_id] = tier
            tier_counts[tier] += 1
            components_by_bn_id[bn_id] = members
            for member in members:
                node_to_bn_id[member] = bn_id

        elapsed = time.time() - start
        self.stats["union_find"] = {
            "unions": unions,
            "components": len(components),
            "tier1_components": tier_counts["tier1"],
            "tier2_components": tier_counts["tier2"],
            "dropped_components": dropped_components,
            "dropped_nodes": dropped_nodes,
            "oversized_components": oversized_components,
            "oversized_nodes": oversized_nodes,
            "total_nodes": len(node_to_bn_id),
            "below_threshold": below_threshold,
            "expired_browser": expired_browser,
            "aged_out": aged_out,
            "elapsed": elapsed,
        }

        anchored = tier_counts["tier1"] + tier_counts["tier2"]
        print(
            f"    {unions:,} unions -> {len(components):,} components "
            f"({anchored:,} anchored: {tier_counts['tier1']:,} tier1, "
            f"{tier_counts['tier2']:,} tier2, {dropped_components:,} dropped"
            f"{f', {oversized_components:,} oversized (>{self.max_cluster_size})' if oversized_components else ''}) "
            f"({len(node_to_bn_id):,} nodes, {below_threshold:,} below threshold"
            f"{f', {aged_out:,} aged out' if aged_out else ''}"
            f"{f', {expired_browser:,} expired browser' if expired_browser else ''}) "
            f"({elapsed:.1f}s)"
        )

        return node_to_bn_id, components_by_bn_id

    # ─── Persistence: merge/split detection ────────────────────

    def apply_persistence(
        self, node_to_bn_id: Dict[str, str], components_by_bn_id: Dict[str, List[str]]
    ) -> Dict[str, str]:
        """
        Apply persistence logic via BigQuery SQL to minimize memory usage.

        Instead of loading the entire previous xref into Python memory,
        this method:
        1. Writes new assignments (identifier_key -> new_bn_id) to a temp table
        2. Runs a BigQuery SQL query that joins against the previous xref to
           detect merges, splits, and stable clusters
        3. Reads back only the remap dict (small — only changed bn_ids)
        4. Applies the remap to node_to_bn_id in Python
        """
        persistence_cfg = self.config.get("persistence", {})
        if not persistence_cfg.get("enabled", True):
            return node_to_bn_id

        print("  Applying persistence (SQL mode)...")
        start = time.time()

        # Check if previous xref exists
        try:
            check_query = f"SELECT COUNT(*) AS cnt FROM `{self.xref_table}` LIMIT 1"
            result = list(self._run_query(check_query, "persistence_check"))
            if not result or result[0]["cnt"] == 0:
                print("    No previous xref table found (first run)")
                return node_to_bn_id
        except Exception:
            print("    No previous xref table found (first run)")
            return node_to_bn_id

        # Step 1: Write new assignments to a temp table (chunked to avoid MemoryError)
        import pandas as pd

        temp_table = (
            f"{self.project}.{self.staging_dataset}._tmp_persist_{self.run_id[:8]}"
        )

        print("    Writing new assignments to temp table (chunked)...", flush=True)
        schema = [
            bigquery.SchemaField("identifier_key", "STRING"),
            bigquery.SchemaField("new_bn_id", "STRING"),
        ]

        # Process in 100k-row chunks to avoid materializing entire dict
        chunk_size = 100000
        items = list(node_to_bn_id.items())
        total_rows = len(items)
        write_disposition = "WRITE_TRUNCATE"

        for chunk_idx, i in enumerate(range(0, total_rows, chunk_size)):
            chunk_items = items[i : i + chunk_size]
            rows = [{"identifier_key": k, "new_bn_id": v} for k, v in chunk_items]
            df = pd.DataFrame(rows)
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=write_disposition,
            )
            self.client.load_table_from_dataframe(
                df, temp_table, job_config=job_config
            ).result()
            del df, rows  # Free memory immediately
            write_disposition = "WRITE_APPEND"  # Append subsequent chunks
            print(
                f"      Chunk {chunk_idx + 1}: rows {i + 1:,} - {min(i + chunk_size, total_rows):,}",
                flush=True,
            )

        del items  # Free the items list
        print(
            f"    Temp table written: {total_rows:,} rows ({time.time() - start:.1f}s)"
        )

        # Step 2: Run merge/split detection in BigQuery
        # This query:
        #   - Joins new assignments against previous xref
        #   - Groups by new_bn_id to find which old_bn_ids each cluster had
        #   - Detects merges (multiple old_bn_ids -> one new) and picks survivor
        #   - Detects splits (one old_bn_id -> multiple new) and picks keeper
        # self.config is already the identity_hub section (see load_config)
        anchor_types_list = list(
            set(self.config.get("person_anchoring", {}).get("tier1_types", ["email"]))
            | set(
                self.config.get("person_anchoring", {}).get("tier2_types", ["bnfpvid"])
            )
        )
        anchor_types_sql = ",".join(f"'{t}'" for t in anchor_types_list)

        remap_query = f"""
        WITH
        -- Join new assignments to previous xref
        joined AS (
            SELECT
                n.identifier_key,
                n.new_bn_id,
                x.bn_id AS old_bn_id
            FROM `{temp_table}` n
            LEFT JOIN `{self.xref_table}` x ON n.identifier_key = x.identifier_key
        ),
        -- For each new cluster, count how many members came from each old bn_id
        new_to_old AS (
            SELECT
                new_bn_id,
                old_bn_id,
                COUNT(*) AS member_count
            FROM joined
            WHERE old_bn_id IS NOT NULL
            GROUP BY new_bn_id, old_bn_id
        ),
        -- Count anchor identifiers per old bn_id (for tie-breaking)
        anchor_counts AS (
            SELECT bn_id AS old_bn_id, COUNT(*) AS anchor_count
            FROM `{self.xref_table}`
            WHERE SPLIT(identifier_key, ':')[OFFSET(0)] IN ({anchor_types_sql})
            GROUP BY bn_id
        ),
        -- Count distinct old bn_ids per new cluster
        merge_candidates AS (
            SELECT
                new_bn_id,
                COUNT(DISTINCT old_bn_id) AS old_count
            FROM new_to_old
            GROUP BY new_bn_id
        ),
        -- SPLIT detection (computed before stable to exclude split old_bn_ids)
        old_to_new AS (
            SELECT old_bn_id, new_bn_id, COUNT(*) AS member_count
            FROM joined
            WHERE old_bn_id IS NOT NULL
            GROUP BY old_bn_id, new_bn_id
        ),
        split_candidates AS (
            SELECT old_bn_id, COUNT(DISTINCT new_bn_id) AS new_count
            FROM old_to_new GROUP BY old_bn_id HAVING new_count > 1
        ),
        -- STABLE: new cluster has exactly one old bn_id AND that old bn_id
        -- is NOT a split candidate. Without the split exclusion, split-off
        -- pieces would all remap to the same old bn_id, undoing the split.
        stable AS (
            SELECT nto.new_bn_id, nto.old_bn_id AS surviving_bn_id
            FROM new_to_old nto
            JOIN merge_candidates mc ON nto.new_bn_id = mc.new_bn_id
            LEFT JOIN split_candidates sc ON nto.old_bn_id = sc.old_bn_id
            WHERE mc.old_count = 1
              AND sc.old_bn_id IS NULL
        ),
        -- MERGE: new cluster has multiple old bn_ids -> pick survivor
        merge_ranked AS (
            SELECT
                nto.new_bn_id,
                nto.old_bn_id,
                nto.member_count,
                COALESCE(ac.anchor_count, 0) AS anchor_count,
                ROW_NUMBER() OVER (
                    PARTITION BY nto.new_bn_id
                    ORDER BY COALESCE(ac.anchor_count, 0) DESC, nto.member_count DESC, nto.old_bn_id ASC
                ) AS rn
            FROM new_to_old nto
            JOIN merge_candidates mc ON nto.new_bn_id = mc.new_bn_id
            LEFT JOIN anchor_counts ac ON nto.old_bn_id = ac.old_bn_id
            WHERE mc.old_count > 1
        ),
        merge_survivors AS (
            SELECT new_bn_id, old_bn_id AS surviving_bn_id
            FROM merge_ranked WHERE rn = 1
        ),
        merge_retired AS (
            SELECT
                mr.new_bn_id,
                ms.surviving_bn_id,
                mr.old_bn_id AS retired_bn_id,
                mr.member_count AS node_count_moved
            FROM merge_ranked mr
            JOIN merge_survivors ms ON mr.new_bn_id = ms.new_bn_id
            WHERE mr.rn > 1
        ),
        -- old_to_new and split_candidates already defined above (before stable)
        -- Count anchor identifiers per NEW cluster (for split tie-breaking)
        new_anchor_counts AS (
            SELECT
                new_bn_id,
                COUNTIF(SPLIT(identifier_key, ':')[OFFSET(0)] IN ({anchor_types_sql})) AS anchor_count
            FROM `{temp_table}`
            GROUP BY new_bn_id
        ),
        split_ranked AS (
            SELECT
                otn.old_bn_id,
                otn.new_bn_id,
                otn.member_count,
                COALESCE(nac.anchor_count, 0) AS anchor_count,
                ROW_NUMBER() OVER (
                    PARTITION BY otn.old_bn_id
                    ORDER BY COALESCE(nac.anchor_count, 0) DESC,
                             otn.member_count DESC,
                             otn.new_bn_id ASC
                ) AS rn
            FROM old_to_new otn
            JOIN split_candidates sc ON otn.old_bn_id = sc.old_bn_id
            LEFT JOIN new_anchor_counts nac ON otn.new_bn_id = nac.new_bn_id
        ),
        split_keepers AS (
            SELECT old_bn_id, new_bn_id AS keeper_new_bn_id
            FROM split_ranked WHERE rn = 1
        ),
        -- Combine all remaps: stable + merge survivors + split keepers
        all_remaps AS (
            SELECT new_bn_id, surviving_bn_id FROM stable
            UNION ALL
            SELECT new_bn_id, surviving_bn_id FROM merge_survivors
            UNION ALL
            SELECT keeper_new_bn_id AS new_bn_id, old_bn_id AS surviving_bn_id FROM split_keepers
        )
        SELECT
            'remap' AS result_type,
            new_bn_id,
            surviving_bn_id AS value_bn_id,
            0 AS node_count
        FROM all_remaps

        UNION ALL

        -- Merge events for logging
        SELECT
            'merge' AS result_type,
            surviving_bn_id AS new_bn_id,
            retired_bn_id AS value_bn_id,
            node_count_moved AS node_count
        FROM merge_retired

        UNION ALL

        -- Split events for logging
        SELECT
            'split' AS result_type,
            old_bn_id AS new_bn_id,
            new_bn_id AS value_bn_id,
            member_count AS node_count
        FROM split_ranked
        WHERE rn > 1
        """

        print("    Running merge/split detection in BigQuery...", flush=True)
        query_start = time.time()
        results = list(self._run_query(remap_query, "persistence_sql"))
        print(
            f"    Query complete ({time.time() - query_start:.1f}s, {len(results):,} result rows)"
        )

        # Step 3: Parse results into remap dict + events
        remap: Dict[str, str] = {}
        merge_events = []
        split_events = []
        run_date = time.strftime("%Y-%m-%d", time.gmtime())

        for row in results:
            if row["result_type"] == "remap":
                remap[row["new_bn_id"]] = row["value_bn_id"]
            elif row["result_type"] == "merge":
                merge_events.append(
                    {
                        "event_date": run_date,
                        "event_type": "MERGE",
                        "surviving_bn_id": row["new_bn_id"],
                        "retired_bn_id": row["value_bn_id"],
                        "node_count_moved": row["node_count"],
                        "trigger_edge": "",
                    }
                )
            elif row["result_type"] == "split":
                split_events.append(
                    {
                        "event_date": run_date,
                        "event_type": "SPLIT",
                        "surviving_bn_id": row["new_bn_id"],
                        "retired_bn_id": row["value_bn_id"],
                        "node_count_moved": row["node_count"],
                        "trigger_edge": "",
                    }
                )

        del results  # Free query results

        # Populate trigger_edge on merge events.
        # The TRUE trigger is an edge whose two endpoints were in different
        # prior bn_ids (one in the retired, one in the surviving) but are
        # now both assigned to the same new cluster. We identify it by joining
        # the hub edges to the prior xref state (still present — xref is
        # rewritten LATER in the pipeline) and the new-assignment temp table.
        if merge_events:
            try:
                # Pass the (surviving, retired) event pairs directly so the
                # query only looks for bridges belonging to actual events.
                event_pairs = [
                    (ev["surviving_bn_id"], ev["retired_bn_id"])
                    for ev in merge_events
                    if ev.get("surviving_bn_id") and ev.get("retired_bn_id")
                ]
                if len(event_pairs) > 1000:
                    event_pairs = event_pairs[:1000]

                if event_pairs:
                    pairs_values = ",".join(
                        f"STRUCT('{s}' AS surviving, '{r}' AS retired)"
                        for s, r in event_pairs
                    )
                    trigger_query = f"""
                    WITH
                    -- The merge events we're looking to annotate
                    events AS (
                        SELECT * FROM UNNEST([{pairs_values}])
                    ),
                    -- Prior xref state (still valid — the xref table hasn't
                    -- been rewritten yet at this point in the pipeline)
                    prior_xref AS (
                        SELECT identifier_key, bn_id AS prior_bn_id
                        FROM `{self.xref_table}`
                    ),
                    -- For each hub edge, resolve the prior bn_id of each endpoint
                    edges_with_assignments AS (
                        SELECT
                            h.identifier_a_type, h.identifier_a_value,
                            h.identifier_b_type, h.identifier_b_value,
                            h.match_rule, h.confidence, h.last_seen,
                            xa.prior_bn_id AS a_prior_bn_id,
                            xb.prior_bn_id AS b_prior_bn_id
                        FROM `{self.hub_table}` h
                        JOIN prior_xref xa ON xa.identifier_key = CONCAT(h.identifier_a_type, ':', h.identifier_a_value)
                        JOIN prior_xref xb ON xb.identifier_key = CONCAT(h.identifier_b_type, ':', h.identifier_b_value)
                    ),
                    -- Find edges that bridge the retired and surviving clusters
                    bridges AS (
                        SELECT
                            e.surviving,
                            e.retired,
                            ea.identifier_a_type, ea.identifier_a_value,
                            ea.identifier_b_type, ea.identifier_b_value,
                            ea.match_rule, ea.confidence,
                            ROW_NUMBER() OVER (
                                PARTITION BY e.surviving, e.retired
                                ORDER BY ea.confidence DESC, ea.last_seen DESC
                            ) AS rn
                        FROM events e
                        JOIN edges_with_assignments ea
                          ON ((ea.a_prior_bn_id = e.surviving AND ea.b_prior_bn_id = e.retired)
                           OR (ea.a_prior_bn_id = e.retired AND ea.b_prior_bn_id = e.surviving))
                    )
                    SELECT surviving, retired, identifier_a_type, identifier_a_value,
                        identifier_b_type, identifier_b_value, match_rule, confidence
                    FROM bridges WHERE rn = 1
                    """
                    trigger_results = list(
                        self._run_query(trigger_query, "trigger_edge_lookup")
                    )
                    trigger_map = {}
                    for row in trigger_results:
                        key = (row["surviving"], row["retired"])
                        trigger_map[key] = (
                            f"{row['identifier_a_type']}:{row['identifier_a_value']}"
                            f"<->{row['identifier_b_type']}:{row['identifier_b_value']}"
                            f" ({row['match_rule']}, conf={row['confidence']:.2f})"
                        )
                    for ev in merge_events:
                        key = (ev.get("surviving_bn_id"), ev.get("retired_bn_id"))
                        if key in trigger_map:
                            ev["trigger_edge"] = trigger_map[key]
                    print(
                        f"    trigger_edge populated for {len(trigger_map)}/{len(event_pairs)} merges"
                    )
            except Exception as exc:
                print(f"    Warning: trigger_edge lookup failed (non-fatal): {exc}")

        # Step 4: Apply remapping to node_to_bn_id
        # Iterate items directly — safe because we only modify values, not keys.
        remapped = 0
        for node_key in node_to_bn_id:
            current_bn_id = node_to_bn_id[node_key]
            if current_bn_id in remap:
                new_bn_id = remap[current_bn_id]
                node_to_bn_id[node_key] = new_bn_id
                if (
                    current_bn_id in self._bn_id_tiers
                    and new_bn_id not in self._bn_id_tiers
                ):
                    self._bn_id_tiers[new_bn_id] = self._bn_id_tiers[current_bn_id]
                remapped += 1

        self._merge_events = merge_events
        self._split_events = split_events
        # Build retired -> surviving remap for hub row updates during incremental
        self._bn_id_remap = {
            evt["retired_bn_id"]: evt["surviving_bn_id"] for evt in merge_events
        }

        # Clean up temp table
        try:
            self.client.delete_table(temp_table, not_found_ok=True)
        except Exception:
            pass

        elapsed = time.time() - start
        print(
            f"    Reused {len(remap):,} previous bn_ids ({remapped:,} nodes remapped) ({elapsed:.1f}s)"
        )
        print(f"    Merges: {len(merge_events):,}, Splits: {len(split_events):,}")
        self.stats["persistence"] = {
            "remapped": remapped,
            "prev_ids_reused": len(remap),
            "merges": len(merge_events),
            "splits": len(split_events),
        }
        return node_to_bn_id

    # ─── Write output tables ───────────────────────────────────

    def _write_hub_append(self, node_to_bn_id: Dict[str, str]) -> int:
        """MERGE new edges into the hub table (idempotent). Used by incremental mode.
        Stages edges to a temp table, then MERGEs on the canonical edge key.
        Also updates bn_id on existing rows for affected clusters (handles merges)."""
        import pandas as pd

        print("  Merging edges into hub table (idempotent)...", flush=True)
        start = time.time()
        hub_staging = (
            f"{self.project}.{self.staging_dataset}._tmp_hub_staging_{self.run_id[:8]}"
        )

        browser_cutoff = (
            datetime.now(timezone.utc) - timedelta(days=self.browser_expiry_days)
        ).strftime("%Y-%m-%dT%H:%M:%S")

        schema = [
            bigquery.SchemaField("bn_id", "STRING"),
            bigquery.SchemaField("identifier_a_type", "STRING"),
            bigquery.SchemaField("identifier_a_value", "STRING"),
            bigquery.SchemaField("identifier_b_type", "STRING"),
            bigquery.SchemaField("identifier_b_value", "STRING"),
            bigquery.SchemaField("source_system", "STRING"),
            bigquery.SchemaField("link_type", "STRING"),
            bigquery.SchemaField("match_rule", "STRING"),
            bigquery.SchemaField("base_confidence", "FLOAT64"),
            bigquery.SchemaField("confidence", "FLOAT64"),
            bigquery.SchemaField("effective_confidence", "FLOAT64"),
            bigquery.SchemaField("first_seen", "TIMESTAMP"),
            bigquery.SchemaField("last_seen", "TIMESTAMP"),
            bigquery.SchemaField("is_active", "BOOLEAN"),
            bigquery.SchemaField("cluster_tier", "STRING"),
        ]

        CHUNK_SIZE = 500_000
        total_rows = 0
        chunk = []

        for edge in self.edges:
            key_a = f"{edge.identifier_a_type}:{edge.identifier_a}"
            bn_id = node_to_bn_id.get(key_a)
            if not bn_id:
                key_b = f"{edge.identifier_b_type}:{edge.identifier_b}"
                bn_id = node_to_bn_id.get(key_b)
            if not bn_id:
                continue

            is_person_edge = (
                edge.identifier_a_type in self.person_types
                or edge.identifier_b_type in self.person_types
            )
            is_active = is_person_edge or edge.last_seen >= browser_cutoff

            chunk.append(
                {
                    "bn_id": bn_id,
                    "identifier_a_type": edge.identifier_a_type,
                    "identifier_a_value": edge.identifier_a,
                    "identifier_b_type": edge.identifier_b_type,
                    "identifier_b_value": edge.identifier_b,
                    "source_system": edge.source_system,
                    "link_type": edge.link_type,
                    "match_rule": edge.match_rule,
                    "base_confidence": edge.base_confidence
                    if edge.base_confidence >= 0
                    else edge.confidence,
                    "confidence": edge.confidence,
                    "effective_confidence": self._get_effective_confidence(edge),
                    "first_seen": edge.first_seen or None,
                    "last_seen": edge.last_seen or None,
                    "is_active": is_active,
                    "cluster_tier": self._bn_id_tiers.get(bn_id, "unknown"),
                }
            )

            if len(chunk) >= CHUNK_SIZE:
                disposition = "WRITE_TRUNCATE" if total_rows == 0 else "WRITE_APPEND"
                job_config = bigquery.LoadJobConfig(
                    schema=schema, write_disposition=disposition
                )
                df = pd.DataFrame(chunk)
                df["first_seen"] = pd.to_datetime(
                    df["first_seen"], utc=True, format="ISO8601", errors="coerce"
                )
                df["last_seen"] = pd.to_datetime(
                    df["last_seen"], utc=True, format="ISO8601", errors="coerce"
                )
                self.client.load_table_from_dataframe(
                    df, hub_staging, job_config=job_config
                ).result()
                total_rows += len(chunk)
                print(f"    Chunk: {total_rows:,} edges staged...", flush=True)
                chunk = []

        if chunk:
            disposition = "WRITE_TRUNCATE" if total_rows == 0 else "WRITE_APPEND"
            job_config = bigquery.LoadJobConfig(
                schema=schema, write_disposition=disposition
            )
            df = pd.DataFrame(chunk)
            df["first_seen"] = pd.to_datetime(
                df["first_seen"], utc=True, format="ISO8601", errors="coerce"
            )
            df["last_seen"] = pd.to_datetime(
                df["last_seen"], utc=True, format="ISO8601", errors="coerce"
            )
            self.client.load_table_from_dataframe(
                df, hub_staging, job_config=job_config
            ).result()
            total_rows += len(chunk)

        # MERGE staged edges into hub (idempotent on edge key)
        if total_rows > 0:
            merge_sql = f"""
            MERGE `{self.hub_table}` AS target
            USING `{hub_staging}` AS source
            ON  target.identifier_a_type = source.identifier_a_type
            AND target.identifier_a_value = source.identifier_a_value
            AND target.identifier_b_type = source.identifier_b_type
            AND target.identifier_b_value = source.identifier_b_value
            WHEN MATCHED THEN UPDATE SET
                bn_id = source.bn_id,
                source_system = source.source_system,
                link_type = source.link_type,
                match_rule = source.match_rule,
                base_confidence = source.base_confidence,
                confidence = source.confidence,
                effective_confidence = source.effective_confidence,
                first_seen = COALESCE(
                    LEAST(target.first_seen, source.first_seen),
                    target.first_seen,
                    source.first_seen
                ),
                last_seen = COALESCE(
                    GREATEST(target.last_seen, source.last_seen),
                    target.last_seen,
                    source.last_seen
                ),
                is_active = source.is_active,
                cluster_tier = source.cluster_tier
            WHEN NOT MATCHED THEN INSERT ROW
            """
            self._run_query(merge_sql, "hub_merge")
            print(f"    MERGED {total_rows:,} edges into hub", flush=True)

            # Also update bn_id on OLD hub rows for affected clusters (handles merges).
            # Single batched UPDATE via join against a temp remap table — one full-table
            # scan instead of N (was ~9 GB × hundreds of remaps = multi-TB quota burn).
            if hasattr(self, "_bn_id_remap") and self._bn_id_remap:
                remap_rows = [
                    {"old_id": old_id, "new_id": new_id}
                    for old_id, new_id in self._bn_id_remap.items()
                ]
                remap_staging = (
                    f"{self.project}.{self.staging_dataset}."
                    f"_tmp_hub_remap_{self.run_id[:8]}"
                )
                remap_schema = [
                    bigquery.SchemaField("old_id", "STRING"),
                    bigquery.SchemaField("new_id", "STRING"),
                ]
                remap_job_config = bigquery.LoadJobConfig(
                    schema=remap_schema, write_disposition="WRITE_TRUNCATE"
                )
                remap_df = pd.DataFrame(remap_rows)
                self.client.load_table_from_dataframe(
                    remap_df, remap_staging, job_config=remap_job_config
                ).result()

                update_sql = f"""
                UPDATE `{self.hub_table}` AS t
                SET bn_id = r.new_id
                FROM `{remap_staging}` AS r
                WHERE t.bn_id = r.old_id
                """
                self._run_query(update_sql, "hub_remap_bulk")
                print(
                    f"    Updated bn_id on {len(remap_rows)} retired clusters in hub "
                    f"(single batched UPDATE)",
                    flush=True,
                )
                try:
                    self.client.delete_table(remap_staging, not_found_ok=True)
                except Exception:
                    pass

        # Cleanup staging
        try:
            self.client.delete_table(hub_staging, not_found_ok=True)
        except Exception:
            pass

        elapsed = time.time() - start
        print(f"    Merged {total_rows:,} edges into hub ({elapsed:.1f}s)", flush=True)
        self.stats["hub_table"] = {
            "rows": total_rows,
            "elapsed": elapsed,
            "mode": "merge",
        }
        return total_rows

    def write_hub_table(
        self, node_to_bn_id: Dict[str, str], dry_run: bool = False
    ) -> int:
        """Write the hub table: all edges with bn_id attached.

        v3.3: run_id and config_version are NOT stamped on every hub/xref row
        (would add ~150MB per million rows). Join to bn_id_metrics on run_date
        to trace which run produced a given hub state. Audit tables
        (merge_log, persistence) DO carry run metadata.
        """
        print("  Writing hub table...")

        if dry_run:
            print(f"    Would write {len(self.edges):,} edges to {self.hub_table}")
            return 0

        # Shrink safeguard — upper bound on rows is len(self.edges)
        self._check_shrink_safeguard("hub_table", self.hub_table, len(self.edges))

        start = time.time()
        import pandas as pd

        # Compute browser edge expiry cutoff for is_active flag
        browser_cutoff = (
            datetime.now(timezone.utc) - timedelta(days=self.browser_expiry_days)
        ).strftime("%Y-%m-%dT%H:%M:%S")

        schema = [
            bigquery.SchemaField("bn_id", "STRING"),
            bigquery.SchemaField("identifier_a_type", "STRING"),
            bigquery.SchemaField("identifier_a_value", "STRING"),
            bigquery.SchemaField("identifier_b_type", "STRING"),
            bigquery.SchemaField("identifier_b_value", "STRING"),
            bigquery.SchemaField("source_system", "STRING"),
            bigquery.SchemaField("link_type", "STRING"),
            bigquery.SchemaField("match_rule", "STRING"),
            bigquery.SchemaField("base_confidence", "FLOAT64"),
            bigquery.SchemaField("confidence", "FLOAT64"),
            bigquery.SchemaField("effective_confidence", "FLOAT64"),
            bigquery.SchemaField("first_seen", "TIMESTAMP"),
            bigquery.SchemaField("last_seen", "TIMESTAMP"),
            bigquery.SchemaField("is_active", "BOOLEAN"),
            bigquery.SchemaField("cluster_tier", "STRING"),
        ]

        CHUNK_SIZE = 500_000
        total_rows = 0
        inactive_count = 0
        skipped_dropped = 0
        chunk = []
        first_chunk = True
        total_edges = len(self.edges)

        for edge_idx, edge in enumerate(self.edges):
            if (edge_idx + 1) % 1_000_000 == 0:
                print(
                    f"    ... preparing {edge_idx + 1:,}/{total_edges:,} edges",
                    flush=True,
                )
            key_a = f"{edge.identifier_a_type}:{edge.identifier_a}"
            bn_id = node_to_bn_id.get(key_a)
            if not bn_id:
                key_b = f"{edge.identifier_b_type}:{edge.identifier_b}"
                bn_id = node_to_bn_id.get(key_b)
            if not bn_id:
                if self.person_anchoring_enabled:
                    skipped_dropped += 1
                    continue
                bn_id = "UNLINKED"

            # Person-level edges are always active; browser-level expire
            is_person_edge = (
                edge.identifier_a_type in self.person_types
                or edge.identifier_b_type in self.person_types
            )
            is_active = is_person_edge or edge.last_seen >= browser_cutoff
            if not is_active:
                inactive_count += 1

            chunk.append(
                {
                    "bn_id": bn_id,
                    "identifier_a_type": edge.identifier_a_type,
                    "identifier_a_value": edge.identifier_a,
                    "identifier_b_type": edge.identifier_b_type,
                    "identifier_b_value": edge.identifier_b,
                    "source_system": edge.source_system,
                    "link_type": edge.link_type,
                    "match_rule": edge.match_rule,
                    "base_confidence": edge.base_confidence
                    if edge.base_confidence >= 0
                    else edge.confidence,
                    "confidence": edge.confidence,
                    "effective_confidence": self._get_effective_confidence(edge),
                    "first_seen": edge.first_seen,
                    "last_seen": edge.last_seen,
                    "is_active": is_active,
                    "cluster_tier": self._bn_id_tiers.get(bn_id, "unknown"),
                }
            )

            if len(chunk) >= CHUNK_SIZE:
                disposition = "WRITE_TRUNCATE" if first_chunk else "WRITE_APPEND"
                job_config = bigquery.LoadJobConfig(
                    schema=schema, write_disposition=disposition
                )
                df = pd.DataFrame(chunk)
                df["first_seen"] = pd.to_datetime(
                    df["first_seen"], utc=True, format="ISO8601", errors="coerce"
                )
                df["last_seen"] = pd.to_datetime(
                    df["last_seen"], utc=True, format="ISO8601", errors="coerce"
                )
                self.client.load_table_from_dataframe(
                    df, self.hub_table, job_config=job_config
                ).result()
                total_rows += len(chunk)
                print(f"    Chunk: {total_rows:,} rows written...")
                chunk = []
                first_chunk = False

        # Final chunk
        if chunk:
            disposition = "WRITE_TRUNCATE" if first_chunk else "WRITE_APPEND"
            job_config = bigquery.LoadJobConfig(
                schema=schema, write_disposition=disposition
            )
            df = pd.DataFrame(chunk)
            df["first_seen"] = pd.to_datetime(
                df["first_seen"], utc=True, format="ISO8601", errors="coerce"
            )
            df["last_seen"] = pd.to_datetime(
                df["last_seen"], utc=True, format="ISO8601", errors="coerce"
            )
            self.client.load_table_from_dataframe(
                df, self.hub_table, job_config=job_config
            ).result()
            total_rows += len(chunk)

        elapsed = time.time() - start

        if skipped_dropped:
            print(f"    Skipped {skipped_dropped:,} edges from dropped components")
        print(
            f"    Wrote {total_rows:,} rows ({inactive_count:,} inactive) to {self.hub_table} ({elapsed:.1f}s)"
        )
        self.stats["hub_table"] = {
            "rows": total_rows,
            "inactive": inactive_count,
            "skipped_dropped": skipped_dropped,
            "elapsed": elapsed,
        }
        return total_rows

    def write_xref_table(
        self,
        node_to_bn_id: Dict[str, str],
        cluster_attrs: Dict[str, dict] = None,
        dry_run: bool = False,
    ) -> int:
        """Write xref table: identifier_key -> bn_id mapping with cluster attributes."""
        print("  Writing xref table...")

        if dry_run:
            print(
                f"    Would write {len(node_to_bn_id):,} mappings to {self.xref_table}"
            )
            return 0

        start = time.time()
        import pandas as pd

        if cluster_attrs is None:
            cluster_attrs = {}
        empty_attrs = {
            "cluster_size": None,
            "is_hcp": None,
            "is_shared_workstation": None,
            "last_seen": None,
            "source_profile": None,
            "is_bot": None,
            "cluster_health_score": 100,
            "is_suspicious": False,
        }

        schema = [
            bigquery.SchemaField("identifier_key", "STRING"),
            bigquery.SchemaField("bn_id", "STRING"),
            bigquery.SchemaField("identifier_type", "STRING"),
            bigquery.SchemaField("identifier_value", "STRING"),
            bigquery.SchemaField("cluster_tier", "STRING"),
            bigquery.SchemaField("cluster_size", "INTEGER"),
            bigquery.SchemaField("is_hcp", "BOOLEAN"),
            bigquery.SchemaField("is_shared_workstation", "BOOLEAN"),
            bigquery.SchemaField("last_seen", "TIMESTAMP"),
            bigquery.SchemaField("source_profile", "STRING"),
            bigquery.SchemaField("is_bot", "BOOLEAN"),
            bigquery.SchemaField("cluster_health_score", "INT64"),
            bigquery.SchemaField("is_suspicious", "BOOLEAN"),
        ]

        CHUNK_SIZE = 1_000_000  # Bumped from 500K — fewer BQ job round-trips
        total_rows = 0
        filtered_rows = 0
        chunk = []
        first_chunk = True

        # Pre-compute how many rows will be written for the shrink safeguard
        expected_rows = sum(
            1
            for key in node_to_bn_id
            if not self.person_anchoring_enabled
            or key.split(":", 1)[0] in self.output_types
        )
        self._check_shrink_safeguard("xref_table", self.xref_table, expected_rows)

        for key, bn_id in node_to_bn_id.items():
            id_type = key.split(":", 1)[0]
            # Skip non-output types (browser cookies, session IDs used for stitching only)
            if self.person_anchoring_enabled and id_type not in self.output_types:
                filtered_rows += 1
                continue
            ca = cluster_attrs.get(bn_id, empty_attrs)
            chunk.append(
                {
                    "identifier_key": key,
                    "bn_id": bn_id,
                    "identifier_type": id_type,
                    "identifier_value": key.split(":", 1)[1] if ":" in key else key,
                    "cluster_tier": self._bn_id_tiers.get(bn_id, "unknown"),
                    "cluster_size": ca.get("cluster_size"),
                    "is_hcp": ca.get("is_hcp"),
                    "is_shared_workstation": ca.get("is_shared_workstation"),
                    "last_seen": ca.get("last_seen"),
                    "source_profile": ca.get("source_profile"),
                    "is_bot": ca.get("is_bot"),
                    "cluster_health_score": ca.get("cluster_health_score", 100),
                    "is_suspicious": ca.get("is_suspicious", False),
                }
            )

            if len(chunk) >= CHUNK_SIZE:
                disposition = "WRITE_TRUNCATE" if first_chunk else "WRITE_APPEND"
                job_config = bigquery.LoadJobConfig(
                    schema=schema, write_disposition=disposition
                )
                df = pd.DataFrame(chunk)
                if "last_seen" in df.columns:
                    df["last_seen"] = pd.to_datetime(
                        df["last_seen"], utc=True, format="ISO8601", errors="coerce"
                    )
                self.client.load_table_from_dataframe(
                    df, self.xref_table, job_config=job_config
                ).result()
                total_rows += len(chunk)
                print(f"    Chunk: {total_rows:,} mappings written...")
                chunk = []
                first_chunk = False

        if chunk:
            disposition = "WRITE_TRUNCATE" if first_chunk else "WRITE_APPEND"
            job_config = bigquery.LoadJobConfig(
                schema=schema, write_disposition=disposition
            )
            df = pd.DataFrame(chunk)
            if "last_seen" in df.columns:
                df["last_seen"] = pd.to_datetime(
                    df["last_seen"], utc=True, format="ISO8601", errors="coerce"
                )
            self.client.load_table_from_dataframe(
                df, self.xref_table, job_config=job_config
            ).result()
            total_rows += len(chunk)

        elapsed = time.time() - start

        if filtered_rows:
            print(
                f"    Filtered {filtered_rows:,} stitching-only identifiers from output"
            )
        print(
            f"    Wrote {total_rows:,} mappings to {self.xref_table} ({elapsed:.1f}s)"
        )
        self.stats["xref_table"] = {
            "rows": total_rows,
            "filtered_rows": filtered_rows,
            "elapsed": elapsed,
        }
        return total_rows

    def write_node_index(
        self, node_to_bn_id: Dict[str, str], dry_run: bool = False
    ) -> int:
        """Write internal node index: ALL identifier_key -> bn_id mappings (unfiltered).
        Unlike xref (which is filtered to output_types), this includes every stitchable
        identifier so incrementals can discover affected clusters completely."""
        print("  Writing node index...")
        import pandas as pd

        if dry_run:
            print(
                f"    Would write {len(node_to_bn_id):,} nodes to {self.node_index_table}"
            )
            return 0

        start = time.time()
        schema = [
            bigquery.SchemaField("identifier_key", "STRING"),
            bigquery.SchemaField("bn_id", "STRING"),
            bigquery.SchemaField("identifier_type", "STRING"),
            bigquery.SchemaField("is_output", "BOOLEAN"),
            bigquery.SchemaField("cluster_tier", "STRING"),
            bigquery.SchemaField("run_id", "STRING"),
        ]

        CHUNK_SIZE = 1_000_000  # Bumped from 500K — fewer BQ job round-trips
        total_rows = 0
        chunk = []
        first_chunk = True

        for key, bn_id in node_to_bn_id.items():
            id_type = key.split(":", 1)[0]
            is_output = (
                not self.person_anchoring_enabled or id_type in self.output_types
            )
            chunk.append(
                {
                    "identifier_key": key,
                    "bn_id": bn_id,
                    "identifier_type": id_type,
                    "is_output": is_output,
                    "cluster_tier": self._bn_id_tiers.get(bn_id, "unknown"),
                    "run_id": self.run_id,
                }
            )

            if len(chunk) >= CHUNK_SIZE:
                disposition = "WRITE_TRUNCATE" if first_chunk else "WRITE_APPEND"
                job_config = bigquery.LoadJobConfig(
                    schema=schema, write_disposition=disposition
                )
                df = pd.DataFrame(chunk)
                self.client.load_table_from_dataframe(
                    df, self.node_index_table, job_config=job_config
                ).result()
                total_rows += len(chunk)
                chunk = []
                first_chunk = False

        if chunk:
            disposition = "WRITE_TRUNCATE" if first_chunk else "WRITE_APPEND"
            job_config = bigquery.LoadJobConfig(
                schema=schema, write_disposition=disposition
            )
            df = pd.DataFrame(chunk)
            self.client.load_table_from_dataframe(
                df, self.node_index_table, job_config=job_config
            ).result()
            total_rows += len(chunk)

        elapsed = time.time() - start
        print(
            f"    Wrote {total_rows:,} nodes to {self.node_index_table} ({elapsed:.1f}s)"
        )
        self.stats["node_index"] = {"rows": total_rows, "elapsed": elapsed}
        return total_rows

    def write_neighbors_table(
        self, node_to_bn_id: Dict[str, str], dry_run: bool = False
    ) -> int:
        """
        Write neighbor-pair table via pure BigQuery SQL.
        Cross-products the xref table (which is already written at this point)
        to generate all undirected pairs within each eligible cluster.
        """
        print("  Writing neighbors table (SQL path)...")

        if dry_run:
            print(f"    Would write neighbor pairs to {self.neighbors_table}")
            return 0

        start = time.time()

        MAX_CLUSTER = 50
        output_types_sql = (
            ", ".join(f"'{t}'" for t in self.output_types)
            if self.output_types
            else "'__none__'"
        )

        # Step 1: Generate cross-product pairs from xref (pure SQL)
        # Same logic as the old Python loop but 10-20x faster in BigQuery.
        print(f"    Generating cross-product pairs from xref...", flush=True)
        self._run_query(
            f"""
        CREATE OR REPLACE TABLE `{self.neighbors_table}` AS
        WITH cluster_members AS (
          SELECT bn_id, identifier_type, identifier_value, cluster_tier
          FROM `{self.xref_table}`
          WHERE identifier_type IN ({output_types_sql})
        ),
        cluster_sizes AS (
          SELECT bn_id, COUNT(*) AS sz FROM cluster_members GROUP BY 1
        ),
        eligible AS (
          SELECT bn_id FROM cluster_sizes WHERE sz BETWEEN 2 AND {MAX_CLUSTER}
        )
        SELECT
          c1.bn_id,
          c1.identifier_type AS node_type,
          c1.identifier_value AS node_value,
          c2.identifier_type AS neighbor_type,
          c2.identifier_value AS neighbor_value,
          0.0 AS confidence,
          'TRANSITIVE' AS match_rule,
          'transitive' AS source_system,
          CAST(NULL AS TIMESTAMP) AS first_seen,
          CAST(NULL AS TIMESTAMP) AS last_seen,
          c1.cluster_tier
        FROM cluster_members c1
        JOIN cluster_members c2
          ON c1.bn_id = c2.bn_id
          AND (c1.identifier_type < c2.identifier_type
               OR (c1.identifier_type = c2.identifier_type AND c1.identifier_value < c2.identifier_value))
        JOIN eligible e ON c1.bn_id = e.bn_id
        """,
            "neighbors_cross_product",
        )

        # Count results
        result = list(
            self._run_query(
                f"SELECT COUNT(*) AS cnt FROM `{self.neighbors_table}`",
                "neighbors_count",
            )
        )
        total_rows = result[0]["cnt"] if result else 0

        # Count skipped clusters
        skipped_result = list(
            self._run_query(
                f"""
        SELECT COUNT(*) AS cnt FROM (
          SELECT bn_id FROM `{self.xref_table}`
          WHERE identifier_type IN ({output_types_sql})
          GROUP BY 1 HAVING COUNT(*) > {MAX_CLUSTER}
        )""",
                "neighbors_skipped_count",
            )
        )
        skipped_large = skipped_result[0]["cnt"] if skipped_result else 0

        # Count filtered (non-output) pairs
        filtered_result = list(
            self._run_query(
                f"""
        SELECT COUNT(*) AS cnt FROM `{self.xref_table}`
        WHERE identifier_type NOT IN ({output_types_sql})
        """,
                "neighbors_filtered_count",
            )
        )
        filtered_pairs = filtered_result[0]["cnt"] if filtered_result else 0

        print(
            f"    Generated {total_rows:,} neighbor pairs ({skipped_large:,} large clusters skipped)",
            flush=True,
        )

        if total_rows == 0:
            print("    No neighbor pairs to write (all singletons)")
            return 0

        # Step 2: Fill edge metadata from hub (forward + reverse match)
        print(f"    Filling edge metadata from hub table...", flush=True)
        update_query = f"""
        UPDATE `{self.neighbors_table}` AS n
        SET n.confidence = h.confidence,
            n.match_rule = h.match_rule,
            n.source_system = h.source_system,
            n.first_seen = h.first_seen,
            n.last_seen = h.last_seen
        FROM `{self.hub_table}` AS h
        WHERE n.node_type = h.identifier_a_type
          AND n.node_value = h.identifier_a_value
          AND n.neighbor_type = h.identifier_b_type
          AND n.neighbor_value = h.identifier_b_value
        """
        self._run_query(update_query, "neighbors_metadata_fwd")
        update_query_rev = f"""
        UPDATE `{self.neighbors_table}` AS n
        SET n.confidence = h.confidence,
            n.match_rule = h.match_rule,
            n.source_system = h.source_system,
            n.first_seen = h.first_seen,
            n.last_seen = h.last_seen
        FROM `{self.hub_table}` AS h
        WHERE n.node_type = h.identifier_b_type
          AND n.node_value = h.identifier_b_value
          AND n.neighbor_type = h.identifier_a_type
          AND n.neighbor_value = h.identifier_a_value
          AND n.match_rule = 'TRANSITIVE'
        """
        self._run_query(update_query_rev, "neighbors_metadata_rev")

        elapsed = time.time() - start

        if skipped_large:
            print(f"    Skipped {skipped_large:,} clusters with >{MAX_CLUSTER} members")
        if filtered_pairs:
            print(f"    Filtered {filtered_pairs:,} stitching-only pairs from output")
        print(
            f"    Wrote {total_rows:,} neighbor pairs to {self.neighbors_table} ({elapsed:.1f}s)"
        )
        self.stats["neighbors_table"] = {
            "rows": total_rows,
            "skipped_large_clusters": skipped_large,
            "filtered_pairs": filtered_pairs,
            "elapsed": elapsed,
        }
        return total_rows

    def write_merge_log(self, dry_run: bool = False) -> int:
        """Write merge/split events to the audit log."""
        events = getattr(self, "_merge_events", []) + getattr(self, "_split_events", [])
        if not events:
            return 0

        print(f"  Writing merge log ({len(events)} events)...")

        if dry_run:
            for e in events:
                print(
                    f"    {e['event_type']}: {e['surviving_bn_id']} <- {e['retired_bn_id']} ({e['node_count_moved']} nodes)"
                )
            return 0

        try:
            import pandas as pd

            # v3.3: stamp run metadata on every event row
            for ev in events:
                ev.setdefault("run_id", self.run_id)
                ev.setdefault("build_mode", self.build_mode)
                ev.setdefault("config_version", self.config_version)
                ev.setdefault("git_sha", self.git_sha)
            df = pd.DataFrame(events)
            df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
            # Drop any internal tracking fields (e.g. _trigger_conf) before write
            df = df[[c for c in df.columns if not c.startswith("_")]]
            job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("event_date", "DATE"),
                    bigquery.SchemaField("event_type", "STRING"),
                    bigquery.SchemaField("surviving_bn_id", "STRING"),
                    bigquery.SchemaField("retired_bn_id", "STRING"),
                    bigquery.SchemaField("node_count_moved", "INTEGER"),
                    bigquery.SchemaField("trigger_edge", "STRING"),
                    bigquery.SchemaField("run_id", "STRING"),
                    bigquery.SchemaField("build_mode", "STRING"),
                    bigquery.SchemaField("config_version", "STRING"),
                    bigquery.SchemaField("git_sha", "STRING"),
                ],
                write_disposition="WRITE_APPEND",
                schema_update_options=["ALLOW_FIELD_ADDITION"],
            )
            self.client.load_table_from_dataframe(
                df, self.merge_log_table, job_config=job_config
            ).result()
            print(f"    Wrote {len(events)} merge/split events")
        except Exception as e:
            raise RuntimeError(
                f"Merge log write failed: {e}. "
                f"Cannot proceed without durable audit trail."
            )

        return len(events)

    def write_persistence_table(
        self, node_to_bn_id: Dict[str, str], dry_run: bool = False
    ) -> int:
        """Write persistence redirect rows for merged bn_ids."""
        events = getattr(self, "_merge_events", [])
        if not events:
            return 0

        print("  Writing persistence redirects...")

        if dry_run:
            print(f"    Would write {len(events)} redirect rows")
            return 0

        try:
            import pandas as pd

            rows = []
            for e in events:
                if e["event_type"] == "MERGE":
                    rows.append(
                        {
                            "old_bn_id": e["retired_bn_id"],
                            "current_bn_id": e["surviving_bn_id"],
                            # DDL contract: CREATE | MERGE | SPLIT (not MERGED).
                            # Downstream filters on 'MERGE' must see these rows.
                            "event_type": "MERGE",
                            "event_date": e["event_date"],
                            "run_id": self.run_id,
                            "build_mode": self.build_mode,
                            "config_version": self.config_version,
                            "git_sha": self.git_sha,
                        }
                    )

            # Idempotency: ledgers are written BEFORE hub/xref mutation, so a
            # crash between ledger write and mutation makes the next run
            # re-detect the same merges. Filter out (old_bn_id, current_bn_id)
            # pairs already present -- BQ does not enforce the PK.
            rows = self._filter_existing_persistence_pairs(rows)

            if rows:
                df = pd.DataFrame(rows)
                df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
                job_config = bigquery.LoadJobConfig(
                    schema=[
                        bigquery.SchemaField("old_bn_id", "STRING"),
                        bigquery.SchemaField("current_bn_id", "STRING"),
                        bigquery.SchemaField("event_type", "STRING"),
                        bigquery.SchemaField("event_date", "DATE"),
                        bigquery.SchemaField("run_id", "STRING"),
                        bigquery.SchemaField("build_mode", "STRING"),
                        bigquery.SchemaField("config_version", "STRING"),
                        bigquery.SchemaField("git_sha", "STRING"),
                    ],
                    write_disposition="WRITE_APPEND",
                    schema_update_options=["ALLOW_FIELD_ADDITION"],
                )
                self.client.load_table_from_dataframe(
                    df, self.persistence_table, job_config=job_config
                ).result()
                print(f"    Wrote {len(rows)} persistence redirects")
            else:
                print("    All redirect rows already present (idempotent skip)")

        except Exception as e:
            raise RuntimeError(
                f"Persistence write failed: {e}. "
                f"Cannot proceed without durable redirect table."
            )

        return len(rows) if rows else 0

    def _filter_existing_persistence_pairs(self, rows: List[dict]) -> List[dict]:
        """Drop redirect rows whose (old_bn_id, current_bn_id) pair already
        exists in the persistence table.

        Makes write_persistence_table idempotent across crash-window reruns.
        Also dedupes within the batch itself. Historical rows may carry
        event_type 'MERGED' (pre-normalization); the pair check is
        event_type-agnostic on purpose so those still count as present.
        """
        seen: Set[tuple] = set()
        unique_rows = []
        for r in rows:
            key = (r["old_bn_id"], r["current_bn_id"])
            if key in seen:
                continue
            seen.add(key)
            unique_rows.append(r)

        existing: Set[tuple] = set()
        olds = sorted({r["old_bn_id"] for r in unique_rows})
        chunk_size = 5000
        for i in range(0, len(olds), chunk_size):
            chunk = olds[i : i + chunk_size]
            query = (
                f"SELECT old_bn_id, current_bn_id "
                f"FROM `{self.persistence_table}` "
                f"WHERE old_bn_id IN UNNEST(@olds)"
            )
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("olds", "STRING", chunk)]
            )
            for row in self.client.query(query, job_config=job_config).result():
                existing.add((row.old_bn_id, row.current_bn_id))

        filtered = [
            r
            for r in unique_rows
            if (r["old_bn_id"], r["current_bn_id"]) not in existing
        ]
        dropped = len(rows) - len(filtered)
        if dropped:
            print(f"    Skipped {dropped:,} redirect rows already present (idempotent)")
        return filtered

    def write_metrics(self, dry_run: bool = False) -> None:
        """Write run metrics to bn_id_metrics table."""
        if dry_run:
            return

        print("  Writing metrics...")
        try:
            required_fields = {
                "run_date": "DATE",
                "run_id": "STRING",
                "build_mode": "STRING",
                "config_version": "STRING",
                "git_sha": "STRING",
                "metric_name": "STRING",
                "metric_value": "FLOAT64",
            }

            # Ensure table exists AND has all required columns. Existing prod
            # tables pre-Sprint-2 are missing run_id/build_mode/config_version/git_sha;
            # we must ALTER them or insert_rows_json() silently drops those columns.
            try:
                existing = self.client.get_table(self.metrics_table)
                existing_fields = {f.name for f in existing.schema}
                missing = [n for n in required_fields if n not in existing_fields]
                if missing:
                    print(
                        f"    Adding missing columns to {self.metrics_table}: {missing}"
                    )
                    new_schema = list(existing.schema) + [
                        bigquery.SchemaField(n, required_fields[n]) for n in missing
                    ]
                    existing.schema = new_schema
                    self.client.update_table(existing, ["schema"])
            except Exception:
                schema = [
                    bigquery.SchemaField(n, t) for n, t in required_fields.items()
                ]
                self.client.create_table(
                    bigquery.Table(self.metrics_table, schema=schema)
                )

            run_date = time.strftime("%Y-%m-%d", time.gmtime())
            metrics = []

            for section, section_stats in self.stats.items():
                if isinstance(section_stats, dict):
                    for key, value in section_stats.items():
                        if isinstance(value, (int, float)):
                            metrics.append(
                                {
                                    "run_date": run_date,
                                    "run_id": self.run_id,
                                    "build_mode": self.build_mode,
                                    "config_version": self.config_version,
                                    "git_sha": self.git_sha,
                                    "metric_name": f"{section}.{key}",
                                    "metric_value": float(value),
                                }
                            )

            if metrics:
                # insert_rows_json returns row errors; don't let them fail silently
                errors = self.client.insert_rows_json(self.metrics_table, metrics)
                if errors:
                    print(
                        f"    WARNING: {len(errors)} metric rows rejected by BigQuery"
                    )
                    for err in errors[:3]:
                        print(f"      {err}")
                    if len(errors) > 3:
                        print(f"      ... and {len(errors) - 3} more")
                else:
                    print(f"    Wrote {len(metrics)} metrics")

        except Exception as e:
            print(f"    Warning: Could not write metrics: {e}")

    def compute_graph_health_metrics(
        self, components_by_bn_id: Dict[str, List[str]]
    ) -> None:
        """
        Compute bnfpvid coverage and fragmentation metrics after Union-Find.
        Stores results in self.stats for write_metrics() to emit.
        """
        print("  Computing graph health metrics...")

        tier1_bn_ids = [bn for bn, tier in self._bn_id_tiers.items() if tier == "tier1"]
        tier2_bn_ids = [bn for bn, tier in self._bn_id_tiers.items() if tier == "tier2"]

        # ── bnfpvid coverage ──
        def has_type(bn_id, id_type):
            for node_key in components_by_bn_id.get(bn_id, []):
                if node_key.startswith(f"{id_type}:"):
                    return True
            return False

        tier1_with_bnfpvid = sum(1 for bn in tier1_bn_ids if has_type(bn, "bnfpvid"))
        tier2_with_bnfpvid = sum(1 for bn in tier2_bn_ids if has_type(bn, "bnfpvid"))

        pct_known = (
            (tier1_with_bnfpvid / len(tier1_bn_ids) * 100) if tier1_bn_ids else 0
        )
        pct_anon = (tier2_with_bnfpvid / len(tier2_bn_ids) * 100) if tier2_bn_ids else 0

        # Threshold alerts
        if pct_anon >= 95:
            status = "healthy"
        elif pct_anon >= 90:
            status = "warning"
            print(
                f"    ⚠ bnfpvid coverage WARNING: {pct_anon:.1f}% of anonymous clusters (target >95%)"
            )
        else:
            status = "alert"
            print(
                f"    ⚠ bnfpvid coverage ALERT: {pct_anon:.1f}% of anonymous clusters (target >95%)"
            )

        print(
            f"    bnfpvid coverage: {pct_known:.1f}% known, {pct_anon:.1f}% anonymous [{status}]"
        )

        self.stats["bnfpvid_coverage"] = {
            "pct_known_with_bnfpvid": round(pct_known, 2),
            "pct_anonymous_with_bnfpvid": round(pct_anon, 2),
            "tier1_total": len(tier1_bn_ids),
            "tier1_with_bnfpvid": tier1_with_bnfpvid,
            "tier2_total": len(tier2_bn_ids),
            "tier2_with_bnfpvid": tier2_with_bnfpvid,
        }

        # ── Fragmentation metrics ──
        output_types = set(
            self.config.get("person_anchoring", {}).get("output_types", [])
        )

        def count_output_identifiers(bn_id):
            return sum(
                1
                for node_key in components_by_bn_id.get(bn_id, [])
                if node_key.split(":")[0] in output_types
            )

        # "No browser link" = known user with email but no bnfpvid.
        # Previously labeled "email-only" which was misleading — a user with
        # email + mc_euid + subscriber_hash but no bnfpvid is NOT email-only.
        # High pct here means the mc_euid bridge isn't linking email clusters
        # to browser activity.
        tier1_no_browser = sum(
            1
            for bn in tier1_bn_ids
            if has_type(bn, "email") and not has_type(bn, "bnfpvid")
        )
        pct_no_browser = (
            (tier1_no_browser / len(tier1_bn_ids) * 100) if tier1_bn_ids else 0
        )

        tier1_id_counts = [count_output_identifiers(bn) for bn in tier1_bn_ids]
        tier2_id_counts = [count_output_identifiers(bn) for bn in tier2_bn_ids]

        avg_known = (
            (sum(tier1_id_counts) / len(tier1_id_counts)) if tier1_id_counts else 0
        )
        avg_anon = (
            (sum(tier2_id_counts) / len(tier2_id_counts)) if tier2_id_counts else 0
        )

        print(
            f"    Fragmentation: {pct_no_browser:.1f}% known-no-browser, "
            f"avg ids/known={avg_known:.1f}, avg ids/anon={avg_anon:.1f}"
        )

        self.stats["fragmentation"] = {
            "pct_known_no_browser": round(pct_no_browser, 2),
            # Keep old key for backward compat with existing dashboards
            "pct_known_email_only": round(pct_no_browser, 2),
            "avg_identifiers_per_known": round(avg_known, 2),
            "avg_identifiers_per_anonymous": round(avg_anon, 2),
            "tier1_no_browser_count": tier1_no_browser,
        }

    # ─── Cluster attribute computation ──────────────────────────

    def _compute_cluster_attrs_from_bq(
        self,
        attrs: Dict[str, dict],
        node_to_bn_id: Dict[str, str],
        components_by_bn_id: Dict[str, List[str]],
    ) -> None:
        """Compute edge-derived cluster attributes from BQ staging (zero Python edge memory).
        Must produce identical results to the Python Pass 2 path."""
        print("    Computing edge-derived attributes from BQ staging...", flush=True)

        assignments_table = self._upload_assignments_to_bq(node_to_bn_id)

        # Query 1: last_seen and source_systems per bn_id
        # Join on BOTH endpoints with COALESCE (same as hub write)
        query = f"""
        SELECT
          bn_id,
          MAX(last_seen) AS max_last_seen,
          ARRAY_AGG(DISTINCT source_system) AS source_systems
        FROM (
          SELECT
            COALESCE(a1.bn_id, a2.bn_id) AS bn_id,
            e.last_seen,
            e.source_system
          FROM `{self._staging_filtered}` e
          LEFT JOIN `{assignments_table}` a1 ON e.edge_key_a = a1.identifier_key
          LEFT JOIN `{assignments_table}` a2 ON e.edge_key_b = a2.identifier_key
          WHERE COALESCE(a1.bn_id, a2.bn_id) IS NOT NULL
        )
        GROUP BY bn_id
        """
        for row in self._run_query(query, "cluster_attrs_bq", page_size=50000):
            bn_id = row["bn_id"]
            if bn_id not in attrs:
                continue
            if row["max_last_seen"]:
                attrs[bn_id]["last_seen"] = row["max_last_seen"]
            sources = set(row["source_systems"]) if row["source_systems"] else set()
            has_browser = bool(sources & self.browser_sources)
            has_ga4 = bool(sources & self.ga4_sources)
            if has_browser and has_ga4:
                attrs[bn_id]["source_profile"] = "browser+ga4"
            elif has_browser:
                attrs[bn_id]["source_profile"] = "browser_only"
            elif has_ga4:
                attrs[bn_id]["source_profile"] = "ga4_only"
            else:
                attrs[bn_id]["source_profile"] = "offline_only"

        # Query 2: shared workstation detection
        # Use the pre-gate flagged bnfpvids table (created in _run_bq_gates BEFORE
        # the DELETE). The post-gate _staging_filtered has already had shared-workstation
        # edges removed, so recomputing from it would always find 0.
        shared_ws_table = getattr(self, "_shared_ws_bq_table", None)
        if shared_ws_table:
            ws_query = f"""
            SELECT DISTINCT a.bn_id
            FROM `{shared_ws_table}` ws
            JOIN `{assignments_table}` a
              ON a.identifier_key = CONCAT('bnfpvid:', ws.bnfpvid)
            WHERE a.bn_id IS NOT NULL
            """
            for row in self._run_query(
                ws_query, "cluster_shared_ws_bq", page_size=50000
            ):
                bn_id = row["bn_id"]
                if bn_id in attrs:
                    attrs[bn_id]["is_shared_workstation"] = True

        # Cleanup temp table
        try:
            self.client.delete_table(assignments_table, not_found_ok=True)
        except Exception:
            pass

        # ── Post-BQ logic that Python Pass 2 also runs ──

        # Flag oversized tier2 clusters as bots (same as Python path line 6725)
        bot_threshold = getattr(self, "bot_cluster_size_threshold", 200)
        for bn_id, a in attrs.items():
            if (
                a["cluster_size"] > bot_threshold
                and self._bn_id_tiers.get(bn_id) != "tier1"
            ):
                a["is_bot"] = True

        # Summary stats (same as Python path line 6731)
        from collections import Counter

        profile_dist = Counter(
            a.get("source_profile", "offline_only") for a in attrs.values()
        )
        n_shared = sum(1 for a in attrs.values() if a.get("is_shared_workstation"))
        n_hcp = sum(1 for a in attrs.values() if a.get("is_hcp"))
        n_bot = sum(1 for a in attrs.values() if a.get("is_bot"))
        sizes = [a["cluster_size"] for a in attrs.values()]
        sizes.sort()

        print(f"    source_profile: {dict(profile_dist)}")
        print(f"    is_shared_workstation=True: {n_shared:,} clusters")
        print(f"    is_hcp=True: {n_hcp:,} clusters")
        print(f"    is_bot=True: {n_bot:,} clusters")
        if sizes:
            p99_idx = int(len(sizes) * 0.99)
            print(
                f"    cluster_size: min={sizes[0]}, median={sizes[len(sizes) // 2]}, "
                f"p99={sizes[p99_idx]}, max={sizes[-1]}"
            )

        self.stats["cluster_attributes"] = {
            "source_profile": dict(profile_dist),
            "shared_workstations": n_shared,
            "hcp_clusters": n_hcp,
            "bot_clusters": n_bot,
        }
        print(
            f"    Done (BQ-derived attributes for {len(attrs):,} clusters)", flush=True
        )

    def check_incremental_parity(self, sample_size: int = 1000) -> Dict[str, Any]:
        """Compare incremental xref state to what full-scan edges would produce
        for a random sample of clusters. Detects transitive merge drift.

        This is a diagnostic tool — does not modify any tables.
        Call after an incremental run to verify it hasn't drifted from full-rebuild behavior.

        Returns dict with parity results and drift percentage.
        """
        print(
            f"  Running incremental parity check (sample={sample_size:,})...",
            flush=True,
        )
        start = time.time()

        # Sample random bn_ids from xref
        sample_query = f"""
        SELECT bn_id FROM (
          SELECT DISTINCT bn_id FROM `{self.xref_table}`
          WHERE cluster_tier = 'tier1'
          ORDER BY FARM_FINGERPRINT(bn_id)
          LIMIT {sample_size}
        )
        """
        sampled = set()
        for row in self._run_query(sample_query, "parity_sample"):
            sampled.add(row["bn_id"])

        if not sampled:
            print("    No bn_ids sampled -- skipping parity check")
            return {"status": "skipped", "reason": "no_samples"}

        # For each sampled bn_id, count its xref members
        # Then count all hub edges touching any member of that cluster
        # If a cluster has hub edges connecting to members of ANOTHER cluster,
        # that's a missed transitive merge
        parity_query = f"""
        WITH sampled AS (
          SELECT bn_id FROM UNNEST([{",".join(f"'{b}'" for b in list(sampled)[:100])}]) AS bn_id
        ),
        -- Get all member keys for sampled clusters
        members AS (
          SELECT x.bn_id, x.identifier_key
          FROM `{self.xref_table}` x
          JOIN sampled s ON x.bn_id = s.bn_id
        ),
        -- Find hub edges where one endpoint is in a sampled cluster
        -- and the other endpoint belongs to a DIFFERENT bn_id
        cross_cluster_edges AS (
          SELECT DISTINCT
            m.bn_id AS sampled_bn_id,
            x2.bn_id AS other_bn_id,
            h.confidence
          FROM `{self.hub_table}` h
          JOIN members m ON m.identifier_key = CONCAT(h.identifier_a_type, ':', h.identifier_a_value)
          JOIN `{self.xref_table}` x2 ON x2.identifier_key = CONCAT(h.identifier_b_type, ':', h.identifier_b_value)
          WHERE m.bn_id != x2.bn_id
            AND h.confidence >= {self.stitch_threshold}
        )
        SELECT
          COUNT(DISTINCT sampled_bn_id) AS clusters_with_cross_edges,
          COUNT(*) AS total_cross_edges,
          ROUND(AVG(confidence), 3) AS avg_cross_confidence
        FROM cross_cluster_edges
        """
        try:
            results = list(self._run_query(parity_query, "parity_check"))
            if results:
                cross_clusters = results[0]["clusters_with_cross_edges"] or 0
                cross_edges = results[0]["total_cross_edges"] or 0
                avg_conf = results[0]["avg_cross_confidence"] or 0
            else:
                cross_clusters = cross_edges = 0
                avg_conf = 0
        except Exception as e:
            print(f"    Parity check query failed: {e}")
            return {"status": "error", "error": str(e)[:200]}

        elapsed = time.time() - start
        drift_pct = (cross_clusters / min(len(sampled), 100)) * 100 if sampled else 0

        print(
            f"    Parity: {cross_clusters} of {min(len(sampled), 100)} sampled clusters "
            f"have cross-cluster edges ({drift_pct:.1f}% drift)"
        )
        if cross_edges:
            print(
                f"    {cross_edges:,} cross-cluster above-threshold edges found "
                f"(avg conf={avg_conf:.3f})"
            )
        print(f"    Parity check completed in {elapsed:.1f}s")

        result = {
            "status": "completed",
            "sampled_clusters": min(len(sampled), 100),
            "clusters_with_drift": cross_clusters,
            "cross_edges": cross_edges,
            "drift_pct": round(drift_pct, 2),
            "elapsed": round(elapsed, 1),
        }

        if drift_pct > 5:
            print(f"    WARNING: drift > 5% -- consider a full rebuild")
        elif drift_pct > 0:
            print(f"    INFO: minor drift detected -- within acceptable range")
        else:
            print(f"    OK: no drift detected in sample")

        self.stats["parity_check"] = result
        return result

    def _compute_cluster_health(
        self, attrs: Dict[str, dict], components_by_bn_id: Dict[str, List[str]]
    ) -> int:
        """Compute cluster_health_score (0-100) and is_suspicious for each cluster.

        Runs AFTER all other cluster attributes are computed (needs is_shared_workstation,
        is_bot). Called from both the Python and BQ-staged paths.

        Returns the number of suspicious clusters.
        """
        freemail_domains = {
            "gmail.com",
            "yahoo.com",
            "hotmail.com",
            "outlook.com",
            "aol.com",
            "icloud.com",
            "mail.com",
            "protonmail.com",
            "comcast.net",
            "att.net",
            "btinternet.com",
            "live.com",
            "msn.com",
            "ymail.com",
            "me.com",
        }
        n_suspicious = 0
        for bn_id, members in components_by_bn_id.items():
            if bn_id not in attrs:
                continue
            score = 100

            emails = [m.split(":", 1)[1] for m in members if m.startswith("email:")]
            bnfpvids = sum(1 for m in members if m.startswith("bnfpvid:"))
            npis = [m.split(":", 1)[1] for m in members if m.startswith("npi_number:")]

            # Multi-email penalty
            if len(emails) >= 5:
                score -= 40
            elif len(emails) >= 3:
                score -= 20
            elif len(emails) >= 2:
                score -= 5

            # Email domain diversity penalty
            if len(emails) >= 2:
                domains = {e.split("@")[1] for e in emails if "@" in e}
                non_free = domains - freemail_domains
                if len(non_free) >= 3:
                    score -= 30
                elif len(non_free) >= 2 and len(emails) >= 3:
                    score -= 15

            # Excessive bnfpvids for known users (shared device signal)
            if bnfpvids > 20 and self._bn_id_tiers.get(bn_id) == "tier1":
                score -= 20
            elif bnfpvids > 50:
                score -= 15

            # Multi-NPI penalty
            if len(npis) >= 3:
                score -= 30
            elif len(npis) >= 2:
                score -= 10

            # Shared workstation already flagged
            if attrs[bn_id].get("is_shared_workstation"):
                score -= 10

            # Bot already flagged
            if attrs[bn_id].get("is_bot"):
                score -= 20

            # Bonus for anchor diversity (email + bnfpvid + mc_euid = well-resolved)
            anchor_types = len(
                {m.split(":")[0] for m in members} & {"email", "bnfpvid", "mc_euid"}
            )
            if anchor_types >= 3:
                score += 10
            elif anchor_types >= 2:
                score += 5

            score = max(0, min(100, score))
            attrs[bn_id]["cluster_health_score"] = score
            attrs[bn_id]["is_suspicious"] = score < 60
            if score < 60:
                n_suspicious += 1

        return n_suspicious

    def _compute_cluster_attributes(
        self,
        node_to_bn_id: Dict[str, str],
        components_by_bn_id: Dict[str, List[str]],
    ) -> Dict[str, dict]:
        """
        Pre-compute cluster-level attributes for the xref table:
        cluster_size, is_hcp, is_shared_workstation, last_seen, source_profile, is_bot,
        cluster_health_score, is_suspicious.
        """
        print("  Computing cluster attributes...")
        start = time.time()

        attrs: Dict[str, dict] = {}

        # ── Pass 1: cluster membership (cluster_size, is_hcp, is_bot from bot pvids) ──
        hcp_types = {"npi_number", "aim_dgid"}
        bot_pvid_clusters: Dict[str, bool] = {}  # bn_id -> has any bot bnfpvid

        for bn_id, members in components_by_bn_id.items():
            member_types = set()
            has_bot_pvid = False
            for m in members:
                t = m.split(":", 1)[0]
                member_types.add(t)
                if t == "bnfpvid" and m.split(":", 1)[1] in self._bot_bnfpvids:
                    has_bot_pvid = True

            attrs[bn_id] = {
                "cluster_size": len(members),
                "is_hcp": bool(member_types & hcp_types),
                "is_shared_workstation": False,
                "last_seen": None,
                "source_profile": "offline_only",
                "is_bot": has_bot_pvid,
                "cluster_health_score": 100,
                "is_suspicious": False,
            }
            bot_pvid_clusters[bn_id] = has_bot_pvid

        # ── Pass 2: edge-derived attributes ──
        # BQ-staged mode: derive from staging table instead of self.edges
        if getattr(self, "_bq_staged", False) or (
            hasattr(self, "_staging_filtered") and not self._edges_list
        ):
            self._compute_cluster_attrs_from_bq(
                attrs, node_to_bn_id, components_by_bn_id
            )
            # Health scoring runs after BQ attrs (needs is_shared_workstation, is_bot)
            self._compute_cluster_health(attrs, components_by_bn_id)
            return attrs

        # Legacy Python path: iterate self.edges
        cluster_last_seen: Dict[str, datetime] = {}
        cluster_sources: Dict[str, set] = defaultdict(set)
        pvid_euids: Dict[str, set] = defaultdict(set)

        for edge in self.edges:
            key_a = f"{edge.identifier_a_type}:{edge.identifier_a}"
            bn_id = node_to_bn_id.get(key_a)
            if not bn_id:
                key_b = f"{edge.identifier_b_type}:{edge.identifier_b}"
                bn_id = node_to_bn_id.get(key_b)
            if not bn_id:
                continue

            # last_seen
            if edge.last_seen:
                try:
                    if isinstance(edge.last_seen, str):
                        ts = datetime.fromisoformat(
                            edge.last_seen.replace("Z", "+00:00")
                        )
                    else:
                        ts = edge.last_seen
                    cur = cluster_last_seen.get(bn_id)
                    if cur is None or ts > cur:
                        cluster_last_seen[bn_id] = ts
                except (ValueError, TypeError):
                    pass

            # source_profile
            cluster_sources[bn_id].add(edge.source_system)

            # (shared workstation detection removed from edge scan — the gate
            #  already deleted those edges. Use the pre-gate flagged set instead.)

        # ── Derive is_shared_workstation ──
        # Use the pre-gate flagged set persisted by _gate_shared_workstation_edges().
        # The gate removes bnfpvid<->mc_euid edges BEFORE Union-Find, so recomputing
        # from the post-gate edge set would always find 0 shared workstations.
        shared_pvids = getattr(self, "_shared_workstation_pvids", set())

        # Mark clusters containing shared workstation bnfpvids
        if shared_pvids:
            for bn_id, members in components_by_bn_id.items():
                for m in members:
                    if m.startswith("bnfpvid:") and m.split(":", 1)[1] in shared_pvids:
                        attrs[bn_id]["is_shared_workstation"] = True
                        break

        # ── Derive source_profile ──
        for bn_id, sources in cluster_sources.items():
            if bn_id not in attrs:
                continue
            has_browser = bool(sources & self.browser_sources)
            has_ga4 = bool(sources & self.ga4_sources)
            if has_browser and has_ga4:
                attrs[bn_id]["source_profile"] = "browser+ga4"
            elif has_browser:
                attrs[bn_id]["source_profile"] = "browser_only"
            elif has_ga4:
                attrs[bn_id]["source_profile"] = "ga4_only"
            else:
                attrs[bn_id]["source_profile"] = "offline_only"

        # ── Derive last_seen ──
        for bn_id, ts in cluster_last_seen.items():
            if bn_id in attrs:
                attrs[bn_id]["last_seen"] = ts

        # ── Flag oversized tier2 clusters as bots ──
        for bn_id, a in attrs.items():
            if (
                a["cluster_size"] > self.bot_cluster_size_threshold
                and self._bn_id_tiers.get(bn_id) != "tier1"
            ):
                a["is_bot"] = True

        # ── Cluster health scoring ──
        n_suspicious = self._compute_cluster_health(attrs, components_by_bn_id)

        # ── Summary stats ──
        from collections import Counter

        profile_dist = Counter(a["source_profile"] for a in attrs.values())
        n_shared = sum(1 for a in attrs.values() if a["is_shared_workstation"])
        n_hcp = sum(1 for a in attrs.values() if a["is_hcp"])
        n_bot = sum(1 for a in attrs.values() if a["is_bot"])
        sizes = [a["cluster_size"] for a in attrs.values()]
        sizes.sort()

        elapsed = time.time() - start
        print(f"    source_profile: {dict(profile_dist)}")
        print(f"    is_shared_workstation=True: {n_shared:,} clusters")
        print(f"    is_hcp=True: {n_hcp:,} clusters")
        print(f"    is_bot=True: {n_bot:,} clusters")
        print(f"    is_suspicious=True: {n_suspicious:,} clusters")
        health_scores = [a["cluster_health_score"] for a in attrs.values()]
        if health_scores:
            avg_health = sum(health_scores) / len(health_scores)
            poor = sum(1 for s in health_scores if s < 50)
            fair = sum(1 for s in health_scores if 50 <= s < 70)
            good = sum(1 for s in health_scores if 70 <= s < 90)
            excellent = sum(1 for s in health_scores if s >= 90)
            print(
                f"    cluster_health: avg={avg_health:.1f}, excellent={excellent:,}, "
                f"good={good:,}, fair={fair:,}, poor={poor:,}"
            )
        if sizes:
            p99_idx = int(len(sizes) * 0.99)
            print(
                f"    cluster_size: min={sizes[0]}, median={sizes[len(sizes) // 2]}, "
                f"p99={sizes[p99_idx]}, max={sizes[-1]}"
            )
        print(f"    Computed in {elapsed:.1f}s")

        self.stats["cluster_attributes"] = {
            "source_profile": dict(profile_dist),
            "shared_workstations": n_shared,
            "hcp_clusters": n_hcp,
            "bot_clusters": n_bot,
            "suspicious_clusters": n_suspicious,
            "avg_health_score": round(avg_health, 1) if health_scores else 0,
            "elapsed": elapsed,
        }

        return attrs

    # ─── Main pipeline orchestration ───────────────────────────

    def run(self, dry_run: bool = False) -> Dict[str, Any]:
        """Execute the full hub build pipeline."""
        print("\n" + "=" * 60)
        print("IDENTITY HUB v3 BUILD")
        print("=" * 60)

        mode = (
            "DRY RUN"
            if dry_run
            else ("INCREMENTAL" if self.incremental else "FULL REBUILD")
        )
        print(f"  Mode: {mode}")
        if self.start_date:
            print(f"  Date range: {self.start_date} to {self.end_date or 'now'}")
        if self.connector_filter:
            print(f"  Connectors: {', '.join(self.connector_filter)}")
        print()

        pipeline_start = time.time()

        # Ensure ledger tables exist (even for genesis with 0 events).
        # This makes "no events" distinguishable from "writer never ran."
        if not dry_run:
            for tbl, schema in [
                (
                    self.merge_log_table,
                    [
                        bigquery.SchemaField("event_date", "DATE"),
                        bigquery.SchemaField("event_type", "STRING"),
                        bigquery.SchemaField("surviving_bn_id", "STRING"),
                        bigquery.SchemaField("retired_bn_id", "STRING"),
                        bigquery.SchemaField("node_count_moved", "INTEGER"),
                        bigquery.SchemaField("trigger_edge", "STRING"),
                        bigquery.SchemaField("run_id", "STRING"),
                        bigquery.SchemaField("build_mode", "STRING"),
                        bigquery.SchemaField("config_version", "STRING"),
                        bigquery.SchemaField("git_sha", "STRING"),
                    ],
                ),
                (
                    self.persistence_table,
                    [
                        bigquery.SchemaField("old_bn_id", "STRING"),
                        bigquery.SchemaField("current_bn_id", "STRING"),
                        bigquery.SchemaField("event_type", "STRING"),
                        bigquery.SchemaField("event_date", "DATE"),
                        bigquery.SchemaField("run_id", "STRING"),
                        bigquery.SchemaField("build_mode", "STRING"),
                        bigquery.SchemaField("config_version", "STRING"),
                        bigquery.SchemaField("git_sha", "STRING"),
                    ],
                ),
            ]:
                try:
                    self.client.get_table(tbl)
                except Exception:
                    table_obj = bigquery.Table(tbl, schema=schema)
                    self.client.create_table(table_obj)
                    print(f"  Created ledger table: {tbl.split('.')[-1]}")

        import gc as _gc

        def _mem_mb():
            """Current process RSS in MB (peak on Linux, current on others)."""
            try:
                import resource

                return (
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
                )  # Linux: KB -> MB
            except Exception:
                try:
                    import psutil

                    return psutil.Process().memory_info().rss // (1024 * 1024)
                except Exception:
                    return 0

        def _phase(label):
            _gc.collect()  # Free unreachable objects before measuring
            elapsed = time.time() - pipeline_start
            mem = _mem_mb()
            agg_size = len(self._edge_agg) if self._edge_agg else 0
            edges_size = len(self._edges_list) if self._edges_list else 0
            mem_str = f" | {mem:,} MB" if mem else ""
            if agg_size:
                avg_bytes = (
                    int(mem * 1024 * 1024 / agg_size)
                    if mem and agg_size > 100000
                    else 0
                )
                avg_str = f" (~{avg_bytes} B/entry)" if avg_bytes else ""
                store_str = f" | agg: {agg_size:,} pairs{avg_str}"
            elif edges_size:
                store_str = f" | edges: {edges_size:,}"
            else:
                store_str = ""
            print(
                f"\n  [{elapsed:7.1f}s{mem_str}{store_str}] === {label} ===", flush=True
            )

        _prev_agg_size = [0]  # mutable for closure

        def _connector_done(name, raw_edges):
            """Print dedup summary after each connector."""
            new_agg_size = len(self._edge_agg)
            new_unique = new_agg_size - _prev_agg_size[0]
            dedup_pct = (1 - new_unique / raw_edges) * 100 if raw_edges > 0 else 0
            print(
                f"    [{name}] {raw_edges:,} raw -> {new_unique:,} new unique "
                f"({dedup_pct:.0f}% dedup) | total: {new_agg_size:,} pairs",
                flush=True,
            )
            _prev_agg_size[0] = new_agg_size

        # Bot detection first — so connectors can skip blacklisted bnfpvids
        _phase("Bot detection")
        self._detect_bot_bnfpvids(dry_run=dry_run)

        # Snapshot production state before full rebuild (for safety validation)
        if not dry_run and not self.incremental:
            _phase("Snapshot production state")
            self._snapshot_production_state()

        # Save prior timestamps to BQ before connectors (survives hub TRUNCATE)
        if not dry_run and not self.incremental:
            _phase("Saving prior timestamps to BQ")
            self._save_prior_timestamps_to_bq()

        # Full rebuild: enable BQ staging — edges go to BigQuery, not Python memory
        if not self.incremental:
            _phase("Creating BQ edge staging table")
            self._create_staging_table()
            self._enable_bq_staging()

        # Incremental mode NO LONGER loads all prior edges. Instead, after
        # connectors run, _run_subset_union_find() loads ONLY the neighborhood
        # of clusters affected by new edges. This keeps memory at ~4 GB.

        def _run_connector(name, method):
            """Run a connector and print dedup stats."""
            before = self._edge_count_raw
            method(dry_run=dry_run)
            raw_added = self._edge_count_raw - before
            if raw_added > 0:
                _connector_done(name, raw_added)

        _phase("Phase 0: localStorage/cookie co-occurrence")
        _run_connector("localstorage", self.connect_localstorage)

        _phase("Phase 0b: bio_acceptor flat identity columns")
        _run_connector("bio_acceptor", self.connect_bio_acceptor)

        _phase("Phase 0c: Gravity Forms -> GA4 session bridge")
        _run_connector("form_session", self.connect_form_session_bridge)

        _phase("Phase 0d: WordPress users -> wp_user_id bridge")
        _run_connector("wp_user_session", self.connect_wp_user_session_bridge)

        _phase("Phase 0d2: SurveyEngine -> email/bnfpvid bridge")
        _run_connector("surveyengine_bridge", self.connect_surveyengine_bridge)

        _phase("Phase 0e: Mailchimp click -> GA4 session bridge")
        _run_connector("mc_click", self.connect_mc_click_bridge)

        _phase("Phase 0f: GA4 URL mc_euid extraction")
        _run_connector("ga4_mc_euid", self.connect_ga4_mc_euid_bridge)

        # Phase 1: REMOVED — bio_acceptor_data.identity_lookup was stale test data

        _phase("Phase 2a: Email bridge (Mailchimp, WordPress, LimeSurvey, Gravity)")
        _run_connector("email_bridge", self.connect_email_bridge)

        _phase("Phase 2b: mc_euid bridge")
        _run_connector("mc_euid_bridge", self.connect_mc_euid_bridge)

        _phase("Phase 2c: GA4 session co-occurrence")
        _run_connector("ga4_identifiers", self.connect_ga4_identifiers)

        _phase("Phase 2d: NPI -> email bridge")
        _run_connector("npi_email", self.connect_npi_email_bridge)

        _phase("Phase 3: UTM crosswalk")
        _run_connector("utm_crosswalk", self.connect_utm_crosswalk)

        _phase("Phase 4: IP + Device + Time")
        _run_connector("ip_device_time", self.connect_ip_device_time)

        _phase("Phase 5: LimeSurvey session bridge")
        _run_connector("limesurvey", self.connect_limesurvey_session)

        _phase("Phase 5b: AIM payload")
        _run_connector("aim_payload", self.connect_aim_payload)

        _phase("Phase 6: Device stat_id clustering")
        _run_connector("device_stat_id", self.connect_device_stat_id)

        _phase("Phase 6b: Compound device+IP+time")
        _run_connector("compound_device_ip", self.connect_compound_device_ip)

        _phase("Phase 7: AIM Clickstream")
        _run_connector("aim_clickstream", self.connect_aim_clickstream)

        _phase("Phase 8: DMD HCP Bridge")
        _run_connector("dmd_hcp", self.connect_dmd_hcp_bridge)

        _phase("Phase 9: NPI Phone Bridge")
        _run_connector("npi_phone", self.connect_npi_phone_bridge)

        _phase("Phase 10: Mailchimp Phone Bridge")
        _run_connector("mc_phone", self.connect_mailchimp_phone_bridge)

        _phase("Phase 11: WordPress NPI Bridge")
        _run_connector("wp_npi", self.connect_wp_npi_bridge)

        _phase("Phase 11b: WordPress MC4WP Email Bridge")
        _run_connector("wp_mc4wp", self.connect_wp_mc4wp_email_bridge)

        _phase("Phase 12: OneTrust consent identity signal")
        _run_connector("onetrust_consent", self.connect_onetrust_consent)

        # Hard-fail if any connector recorded an error. Swallowing exceptions as
        # zero-edge outputs previously allowed silent loss of deterministic bridges.
        self._raise_on_connector_failures()

        # ── Post-connector processing ──
        # Incremental and full rebuild diverge here.

        if self.incremental and not dry_run:
            # ── INCREMENTAL: Subset Union-Find ──
            # Only loads the neighborhood of clusters affected by new edges.
            # Runs Union-Find on the subset for exact correctness.
            # Memory: ~4 GB (vs ~17+ GB for full prior edge load).
            _phase("Subset Union-Find (incremental)")
            subset_stats = self._run_subset_union_find(dry_run=dry_run)

            elapsed = time.time() - pipeline_start
            print(f"\n{'=' * 60}")
            print(f"  INCREMENTAL COMPLETE in {elapsed:.1f}s")
            print(f"  New edges from connectors: {subset_stats.get('new_edges', 0):,}")
            print(f"  Affected bn_ids: {subset_stats.get('affected_bn_ids', 0):,}")
            print(
                f"  Prior subset edges loaded: {subset_stats.get('prior_subset_edges', 0):,}"
            )
            print(f"  Union-Find components: {subset_stats.get('components', 0):,}")
            print(f"  Merges: {subset_stats.get('merges', 0):,}")
            print(f"  Splits: {subset_stats.get('splits', 0):,}")
            print(f"  Xref rows written: {subset_stats.get('xref_rows_written', 0):,}")
            print(f"{'=' * 60}\n")
            self.stats["total_elapsed"] = elapsed
            return self.stats

        # ── FULL REBUILD ──

        if getattr(self, "_bq_staged", False):
            # BQ-staged path: aggregation, filtering, gates all in BigQuery
            # Flush any remaining batch from connectors
            self._flush_staging_batch()
            self._disable_bq_staging()

            _phase("BQ aggregation")
            self._run_bq_aggregation()

            _phase("BQ quality filters")
            self._run_bq_quality_filters()

            _phase("BQ gates (shared workstation + conflict)")
            self._run_bq_gates()

            if dry_run:
                count = list(
                    self._run_query(
                        f"SELECT COUNT(*) AS cnt FROM `{self._staging_filtered}`", ""
                    )
                )[0]["cnt"]
                print(f"\n  Total edges after filtering: {count:,}")
                print("  DRY RUN complete -- no tables written")
                self._cleanup_staging()
                return self.stats

            _phase("Export edges for Union-Find")
            edge_count, below_threshold_count = self._get_edge_count_and_threshold()
            print(
                f"    Downloaded {edge_count:,} stitchable edges "
                f"({below_threshold_count:,} below threshold filtered in BQ)",
                flush=True,
            )

            # Build node priorities from BQ (avoids materializing all edges in Python)
            node_priority = self._build_node_priorities_in_bq()

            _phase(f"Union-Find ({edge_count:,} edges)")
            uf = PriorityUnionFind(node_priority)
            unions = 0

            # Stream edges and perform unions
            for key_a, key_b, _ in self._export_union_find_tuples():
                if uf.union(key_a, key_b):
                    unions += 1

            # Build node_to_bn_id and components_by_bn_id
            components_raw = uf.get_components()
            node_to_bn_id: Dict[str, str] = {}
            components_by_bn_id: Dict[str, List[str]] = {}
            self._bn_id_tiers = {}
            dropped = 0
            for root, members in components_raw.items():
                if len(members) > self.max_cluster_size:
                    continue
                tier = self._classify_component(members)
                if tier is None:
                    dropped += len(members)
                    continue
                bn_id = self._generate_bn_id(root)
                self._bn_id_tiers[bn_id] = tier
                components_by_bn_id[bn_id] = members
                for m in members:
                    node_to_bn_id[m] = bn_id
            del components_raw

            print(
                f"    {unions:,} unions, {len(components_by_bn_id):,} components, "
                f"{below_threshold_count:,} below threshold (filtered in BQ), "
                f"{dropped:,} dropped nodes",
                flush=True,
            )
            self.stats["union_find"] = {
                "unions": unions,
                "components": len(components_by_bn_id),
                "below_threshold": below_threshold_count,
                "total_nodes": len(node_to_bn_id),
            }

        else:
            # Legacy Python path (Phase A fallback)
            _phase("Pre-materialization fanout filtering")
            self._filter_edge_agg_fanout()
            import gc as _gc

            _gc.collect()

            _phase("Cookie normalization + confidence aggregation")
            self.resolve_cookie_normalization()
            self.aggregate_confidence()

            _phase("Quality filters")
            self.apply_quality_filters()

            if dry_run:
                print(f"\n  Total edges collected: {len(self.edges):,}")
                print("  DRY RUN complete -- no tables written")
                return self.stats

            _phase(f"Union-Find ({len(self.edges):,} edges)")
            node_to_bn_id, components_by_bn_id = self.run_union_find()

        _phase("Persistence (merge/split detection)")
        node_to_bn_id = self.apply_persistence(node_to_bn_id, components_by_bn_id)

        # Rebuild components and tiers from final node_to_bn_id after persistence
        # remapping (persistence can merge/split clusters, changing membership).
        components_by_bn_id = defaultdict(list)
        for node_key, bn_id in node_to_bn_id.items():
            components_by_bn_id[bn_id].append(node_key)
        components_by_bn_id = dict(components_by_bn_id)
        self._bn_id_tiers = {}
        for bn_id, members in components_by_bn_id.items():
            tier = self._classify_component(members)
            if tier:
                self._bn_id_tiers[bn_id] = tier
        print(
            f"    Rebuilt {len(components_by_bn_id):,} components, "
            f"{len(self._bn_id_tiers):,} tiered",
            flush=True,
        )

        _phase("Graph health metrics")
        self.compute_graph_health_metrics(components_by_bn_id)
        cluster_attrs = self._compute_cluster_attributes(
            node_to_bn_id, components_by_bn_id
        )

        # Validate new graph before writing
        _phase("Pre-publish validation")
        self._validate_before_publish(node_to_bn_id, components_by_bn_id)

        # ── Shadow-table publish: write to run-scoped tables, then promote ──
        # Save canonical table names and redirect writes to shadow tables
        _canon_hub = self.hub_table
        _canon_xref = self.xref_table
        _canon_neighbors = self.neighbors_table
        _canon_node_index = self.node_index_table
        run_suffix = f"__run_{self.run_id[:8]}"

        if not dry_run:
            # Shadow tables land in staging dataset, not output dataset
            _hub_name = _canon_hub.split(".")[-1]
            _xref_name = _canon_xref.split(".")[-1]
            _neighbors_name = _canon_neighbors.split(".")[-1]
            _node_index_name = _canon_node_index.split(".")[-1]
            self.hub_table = (
                f"{self.project}.{self.staging_dataset}.{_hub_name}{run_suffix}"
            )
            self.xref_table = (
                f"{self.project}.{self.staging_dataset}.{_xref_name}{run_suffix}"
            )
            self.neighbors_table = (
                f"{self.project}.{self.staging_dataset}.{_neighbors_name}{run_suffix}"
            )
            self.node_index_table = (
                f"{self.project}.{self.staging_dataset}.{_node_index_name}{run_suffix}"
            )
            print(f"    Writing to shadow tables (*{run_suffix})", flush=True)

        _phase("Writing hub table (shadow)")
        if hasattr(self, "_staging_filtered") and not dry_run:
            # BQ-staged: write hub from staging table JOIN (zero Python memory)
            assignments_table = self._upload_assignments_to_bq(node_to_bn_id)
            hub_rows = self._write_hub_from_staging(assignments_table, self.hub_table)
            self._check_shrink_safeguard("hub_table", _canon_hub, hub_rows)
            try:
                self.client.delete_table(assignments_table, not_found_ok=True)
            except Exception:
                pass
        else:
            self.write_hub_table(node_to_bn_id, dry_run=dry_run)
        # Apply prior timestamps to shadow hub table
        if not dry_run:
            _phase("Applying prior timestamps (BQ)")
            self._apply_prior_timestamps_in_bq()
        _phase("Writing xref table (shadow)")
        self.write_xref_table(
            node_to_bn_id, cluster_attrs=cluster_attrs, dry_run=dry_run
        )
        _phase("Writing node index")
        self.write_node_index(node_to_bn_id, dry_run=dry_run)
        _phase("Writing neighbors table (shadow)")
        self.write_neighbors_table(node_to_bn_id, dry_run=dry_run)

        # Run contract checks BEFORE promotion — if violations are found,
        # abort before overwriting production. At this point hub/xref/neighbors
        # point to shadow tables, so the checks validate the shadow state.
        if not dry_run:
            _phase("Pre-promotion contract checks")
            self._verify_output_contracts()
            contract = self.stats.get("contract_check", {})
            if not contract.get("passed", True):
                violations = contract.get("violations", [])
                print(
                    f"\n  ABORTING: {len(violations)} contract violations found on shadow tables:"
                )
                for v in violations:
                    print(f"    - {v}")
                print(
                    f"  Production tables NOT overwritten. Shadow tables preserved for inspection."
                )
                raise RuntimeError(
                    f"Contract check failed with {len(violations)} violations. "
                    f"Production unchanged. Inspect shadow tables."
                )

        # Promote shadow tables to production.
        # Order matters for correctness:
        #   1. Write merge/persistence ledgers FIRST so redirects exist even if
        #      a later promote copy fails mid-way.
        #   2. Mark manifest BUILDING so downstream preflight refuses to start
        #      against a mixed promotion.
        #   3. Copy shadow → production tables.
        #   4. Mark manifest PROMOTED only after all copies succeed.
        #   5. Write metrics last (observability, not correctness-critical).
        if not dry_run:
            _phase("Writing merge log + persistence (before promote)")
            self.write_merge_log(dry_run=dry_run)
            self.write_persistence_table(node_to_bn_id, dry_run=dry_run)

            _phase("Promoting shadow tables to production")

            manifest_table = f"{self.project}.{self.output_dataset}.bn_id_manifest"
            try:
                manifest_sql = f"""
                CREATE TABLE IF NOT EXISTS `{manifest_table}` (
                  active_run_id STRING, promoted_at TIMESTAMP, status STRING
                );
                """
                self._run_query(manifest_sql, "manifest_create")
            except Exception:
                pass  # Table may already exist

            # Record promotion intent BEFORE any production truncate/copy.
            # FATAL on failure: the BUILDING row is what lets downstream
            # preflight detect a mixed/failed promotion. Promoting without it
            # would silently remove that guarantee, so refuse to continue.
            try:
                building_sql = f"""
                INSERT INTO `{manifest_table}` (active_run_id, promoted_at, status)
                VALUES ('{self.run_id}', CURRENT_TIMESTAMP(), 'BUILDING')
                """
                self._run_query(building_sql, "manifest_building")
                print(f"    Manifest BUILDING: run_id={self.run_id[:8]}", flush=True)
            except Exception as e:
                raise RuntimeError(
                    f"Could not write BUILDING manifest row: {e}. "
                    f"Refusing to promote without promotion-intent record. "
                    f"Production tables are untouched. See "
                    f"docs/IDENTITY_HUB_PROMOTE_RUNBOOK.md."
                )

            promotion_order = [
                (self.hub_table, _canon_hub),
                (self.xref_table, _canon_xref),
                (self.node_index_table, _canon_node_index),
                (self.neighbors_table, _canon_neighbors),
            ]
            from shared.identity_hub_promote import (
                promote_tables_atomic,
                write_manifest,
            )

            try:
                promoted = promote_tables_atomic(
                    self.client,
                    promotion_order,
                    project=self.project,
                    ops_dataset=self.staging_dataset,
                    run_id=self.run_id,
                )
                write_manifest(self.client, manifest_table, self.run_id, "PROMOTED")
                print(f"    Manifest updated: run_id={self.run_id[:8]}", flush=True)
            except Exception as e:
                try:
                    write_manifest(
                        self.client,
                        manifest_table,
                        self.run_id,
                        f"FAILED_ROLLED_BACK:{e}",
                    )
                except Exception:
                    pass
                self.hub_table = _canon_hub
                self.xref_table = _canon_xref
                self.neighbors_table = _canon_neighbors
                self.node_index_table = _canon_node_index
                raise RuntimeError(
                    f"Shadow table promotion failed (rollback attempted). "
                    f"Check bn_id_manifest. Failed on: {e}"
                ) from e

            # Cleanup shadow tables (only after all promoted successfully)
            for shadow in [
                self.hub_table,
                self.xref_table,
                self.node_index_table,
                self.neighbors_table,
            ]:
                try:
                    self.client.delete_table(shadow, not_found_ok=True)
                except Exception:
                    pass

            # Restore canonical names for metrics (write to production directly)
            self.hub_table = _canon_hub
            self.xref_table = _canon_xref
            self.neighbors_table = _canon_neighbors
            self.node_index_table = _canon_node_index

        _phase("Writing metrics")
        self.write_metrics(dry_run=dry_run)

        elapsed = time.time() - pipeline_start

        # Summary
        uf_stats = self.stats.get("union_find", {})
        persist_stats = self.stats.get("persistence", {})
        print(f"\n{'=' * 60}")
        print(f"  COMPLETE in {elapsed:.1f}s")
        hub_rows = self.stats.get("hub_table", {}).get("rows", 0)
        if hub_rows:
            print(f"  Total hub rows: {hub_rows:,}")
        else:
            print(
                f"  Total edges: {len(self._edges_list) if self._edges_list else 0:,}"
            )
        print(f"  Components: {uf_stats.get('components', 0):,}")
        print(f"  BN IDs: {len(set(node_to_bn_id.values())):,}")
        if persist_stats.get("merges", 0) > 0:
            print(f"  Merges: {persist_stats['merges']:,}")
        if persist_stats.get("splits", 0) > 0:
            print(f"  Splits: {persist_stats['splits']:,}")
        contract = self.stats.get("contract_check", {})
        if contract.get("violations"):
            print(f"  CONTRACT VIOLATIONS: {len(contract['violations'])}")
            for v in contract["violations"]:
                print(f"    - {v}")
        print(f"{'=' * 60}\n")

        # Cleanup staging tables
        self._cleanup_staging()

        self.stats["total_elapsed"] = elapsed
        return self.stats


# =============================================================================
# CLI ENTRY POINT
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Identity Hub v2 — Cross-Platform Identity Graph Builder"
    )
    parser.add_argument(
        "--rebuild", action="store_true", help="Full rebuild of identity hub"
    )
    parser.add_argument("--refresh", action="store_true", help="Incremental refresh")
    parser.add_argument(
        "--dry-run", action="store_true", help="Estimate costs without writing"
    )
    parser.add_argument(
        "--lookback", type=int, default=None, help="Lookback days for incremental mode"
    )
    parser.add_argument("--start-date", default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--connectors",
        nargs="+",
        default=None,
        help="Connectors to run (e.g., localstorage email_bridge)",
    )
    parser.add_argument(
        "--force-overwrite",
        action="store_true",
        help="Bypass the shrink safeguard that aborts writes when "
        "new output is <50%% of existing table size. "
        "Required when running with --connectors.",
    )
    parser.add_argument(
        "--test-dataset",
        nargs="?",
        const="identity_hub_data_test",
        default=None,
        metavar="NAME",
        help="Write output to a test dataset (default: "
        "identity_hub_data_test) instead of production. "
        "Auto-creates the dataset and always --force-overwrite. "
        "Use with --sample-days for fast iteration.",
    )
    parser.add_argument(
        "--sample-days",
        type=int,
        default=None,
        metavar="N",
        help="Limit input data to the last N days (bypasses graph_start_date). "
        "Use with --test-dataset for fast iteration. "
        "Default test window is 3 days if --test-dataset is set without this.",
    )

    args = parser.parse_args()

    # Load config
    config = load_config()

    # Calculate dates
    start_date = args.start_date
    end_date = args.end_date
    incremental = args.refresh

    # --lookback implies --refresh (incremental)
    if args.lookback:
        incremental = True
        if not start_date:
            start_date = (
                datetime.now(timezone.utc) - timedelta(days=args.lookback)
            ).strftime("%Y-%m-%d")

    # ── Test harness: --test-dataset + --sample-days ──
    output_dataset_override = None
    graph_start_override = None
    if args.test_dataset:
        output_dataset_override = args.test_dataset
        # Default test window is 3 days if not specified
        sample_days = args.sample_days if args.sample_days is not None else 3
        test_start = (
            datetime.now(timezone.utc) - timedelta(days=sample_days)
        ).strftime("%Y-%m-%d")
        # Override graph_start_date floor so narrow windows work
        graph_start_override = test_start
        if not start_date:
            start_date = test_start
        # Test runs are always full rebuilds of the test dataset (no prior state)
        if incremental:
            print(
                "  [test-mode] Forcing full rebuild (incremental disabled for test runs)"
            )
            incremental = False
        print(f"  [test-mode] Output dataset: {output_dataset_override}")
        print(f"  [test-mode] Date window: {test_start} -> now ({sample_days} days)")
    elif args.sample_days is not None:
        # --sample-days without --test-dataset: refuse to touch production
        print(
            "Error: --sample-days requires --test-dataset (refusing to shrink production window)"
        )
        sys.exit(1)

    # Test mode implicitly triggers rebuild if neither flag was set
    if args.test_dataset and not args.rebuild and not incremental:
        args.rebuild = True

    if not args.rebuild and not incremental:
        print("Error: specify --rebuild, --refresh, or --lookback <days>")
        sys.exit(1)

    # Connector filter
    connector_filter = args.connectors

    # Initialize BQ client
    project = config.get("project", "bi-data-391216")
    client = bigquery.Client(project=project)

    # Build
    builder = IdentityHubBuilder(
        client=client,
        config=config,
        start_date=start_date,
        end_date=end_date,
        incremental=incremental,
        connector_filter=connector_filter,
        force_overwrite=args.force_overwrite,
        output_dataset_override=output_dataset_override,
        graph_start_override=graph_start_override,
    )

    result = builder.run(dry_run=args.dry_run)

    # Print stats summary
    print("\nStats:")
    for section, stats in result.items():
        if isinstance(stats, dict):
            print(f"  {section}:")
            for k, v in stats.items():
                if isinstance(v, float):
                    print(f"    {k}: {v:,.2f}")
                elif isinstance(v, int):
                    print(f"    {k}: {v:,}")
        elif isinstance(stats, (int, float)):
            print(f"  {section}: {stats}")


if __name__ == "__main__":
    main()
