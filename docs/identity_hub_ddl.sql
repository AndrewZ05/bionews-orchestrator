-- ============================================================================
-- Identity Hub ERD — BigQuery DDL
-- Dataset: bi-data-391216.identity_hub_data
-- Generated from shared/identity_hub.py schema definitions
-- ============================================================================

-- 1. bn_id_hub — All edges with bn_id, confidence, metadata
--    Primary edge store. Every observed relationship between two identifiers.
--    Partitioned by last_seen (DAY), clustered by bn_id, identifier_type.
CREATE TABLE identity_hub_data.bn_id_hub (
    bn_id                   STRING      NOT NULL,   -- Stable identity cluster key (BN_ + 16-char hash)
    identifier_a_type       STRING      NOT NULL,   -- Source identifier type (e.g. 'bnfpvid')
    identifier_a_value      STRING      NOT NULL,   -- Source identifier value
    identifier_b_type       STRING      NOT NULL,   -- Target identifier type (e.g. 'client_id')
    identifier_b_value      STRING      NOT NULL,   -- Target identifier value
    identifier_type         STRING,                 -- Denormalized: = identifier_b_type (for easy filtering)
    identifier_value        STRING,                 -- Denormalized: = identifier_b_value
    source_system           STRING,                 -- Which system: acceptor, mailchimp, ga4, npi_registry, wordpress, limesurvey, aim_clickstream, dmd_audiences
    link_type               STRING,                 -- 'deterministic' or 'probabilistic'
    match_rule              STRING,                 -- Connector rule: LOCALSTORAGE_COOCCURRENCE, EMAIL_EXACT, MC_EUID_BRIDGE, GA4_SESSION_COOCCURRENCE, NPI_REGISTRY_EMAIL, UTM_CROSSWALK, IP_DEVICE_TIME, SURVEY_SESSION, AIM_PAYLOAD_COOCCURRENCE, DEVICE_STAT_ID, AIM_CLICKSTREAM, DMD_HCP_VERIFIED, NPI_REGISTRY_PHONE, MAILCHIMP_PHONE, WORDPRESS_NPI, WORDPRESS_MC4WP_EMAIL
    base_confidence         FLOAT64,                -- Original confidence from connector (before aggregation)
    confidence              FLOAT64,                -- After aggregation: min(1.0, base * log2(count + 1))
    effective_confidence    FLOAT64,                -- After edge aging decay (what Union-Find uses)
    first_seen              TIMESTAMP,              -- Earliest observation of this edge
    last_seen               TIMESTAMP,              -- Most recent observation of this edge
    is_active               BOOLEAN,                -- FALSE if browser-level edge older than browser_expiry_days (365)
    cluster_tier            STRING                  -- 'tier1' (has known anchor) or 'tier2' (anonymous)
)
PARTITION BY DATE(last_seen)
CLUSTER BY bn_id, identifier_type;


-- 2. bn_id_xref — Fast lookup: identifier → bn_id
--    One row per identifier in the graph. Primary lookup table for downstream systems.
--    Clustered by bn_id, identifier_type.
CREATE TABLE identity_hub_data.bn_id_xref (
    identifier_key          STRING      NOT NULL,   -- Composite key: '{type}:{value}' (e.g. 'email:jdoe@example.com')
    bn_id                   STRING      NOT NULL,   -- FK → bn_id_hub.bn_id, bn_id_neighbors.bn_id, bn_id_persistence.current_bn_id
    identifier_type         STRING      NOT NULL,   -- Identifier type (email, bnfpvid, client_id, mc_euid, npi_number, ...)
    identifier_value        STRING      NOT NULL,   -- Identifier value
    cluster_tier            STRING,                 -- 'tier1' or 'tier2'
    cluster_size            INTEGER,                -- Number of identifiers in this bn_id cluster
    is_hcp                  BOOLEAN,                -- TRUE if cluster contains npi_number or aim_dgid
    is_shared_workstation   BOOLEAN,                -- TRUE if any bnfpvid has 3+ mc_euids (shared computer)
    last_seen               TIMESTAMP,              -- Most recent activity for any edge in this cluster
    source_profile          STRING,                 -- Comma-separated source categories: 'browser,ga4,offline'
    is_bot                  BOOLEAN                 -- TRUE if cluster flagged as bot (suspected, not removed)
)
CLUSTER BY bn_id, identifier_type;


-- 3. bn_id_neighbors — Pairwise relationships within clusters
--    Cross-product of all identifier pairs within each bn_id cluster.
--    Clustered by bn_id, node_type.
CREATE TABLE identity_hub_data.bn_id_neighbors (
    bn_id                   STRING      NOT NULL,   -- FK → bn_id_xref.bn_id
    node_type               STRING      NOT NULL,   -- Identifier type of node A
    node_value              STRING      NOT NULL,   -- Identifier value of node A
    neighbor_type           STRING      NOT NULL,   -- Identifier type of node B
    neighbor_value          STRING      NOT NULL,   -- Identifier value of node B
    confidence              FLOAT64,                -- Edge confidence (0.0 for transitive pairs with no direct edge)
    match_rule              STRING,                 -- 'TRANSITIVE' if no direct edge, else the connector match_rule
    source_system           STRING,                 -- 'transitive' if no direct edge, else the source system
    first_seen              TIMESTAMP,              -- From the direct edge, or NULL for transitive
    last_seen               TIMESTAMP,              -- From the direct edge, or NULL for transitive
    cluster_tier            STRING                  -- 'tier1' or 'tier2'
)
CLUSTER BY bn_id, node_type;


-- 4. bn_id_identity_changes — Merge/split audit log
--    Records every merge (two clusters combined) and split (one cluster divided).
--    Append-only — never truncated, permanent audit trail.
CREATE TABLE identity_hub_data.bn_id_identity_changes (
    event_date              DATE        NOT NULL,   -- Date the merge/split was detected
    event_type              STRING      NOT NULL,   -- 'merge' or 'split'
    surviving_bn_id         STRING      NOT NULL,   -- The bn_id that continues to exist
    retired_bn_id           STRING      NOT NULL,   -- The bn_id that was retired (merged into surviving or split off)
    node_count_moved        INTEGER,                -- Number of identifier nodes that moved
    trigger_edge            STRING                  -- Description of the edge that caused the merge/split
);


-- 5. bn_id_persistence — Stable bn_id registry + merge redirects
--    Maps old/retired bn_ids to their current bn_id after merges.
--    Downstream systems use this to resolve stale bn_id references.
CREATE TABLE identity_hub_data.bn_id_persistence (
    old_bn_id               STRING      NOT NULL,   -- The retired bn_id (no longer active)
    current_bn_id           STRING      NOT NULL,   -- FK → bn_id_xref.bn_id — the active bn_id it was merged into
    event_type              STRING,                 -- 'merge' or 'split'
    event_date              DATE                    -- Date the redirect was created
);


-- 6. bn_id_metrics — Run statistics
--    One row per metric per run. Captures connector edge counts, Union-Find stats,
--    quality filter stats, persistence stats, and graph health metrics.
CREATE TABLE identity_hub_data.bn_id_metrics (
    run_date                DATE        NOT NULL,   -- Date of the pipeline run
    metric_name             STRING      NOT NULL,   -- Metric key (e.g. 'localstorage.edges', 'union_find.components', 'bnfpvid_coverage.pct_anonymous')
    metric_value            FLOAT64                 -- Metric value
);


-- ============================================================================
-- RELATIONSHIPS (for ERD tools)
-- ============================================================================
--
--  bn_id_hub.bn_id              ──┐
--  bn_id_xref.bn_id             ──┤── Same bn_id links all tables
--  bn_id_neighbors.bn_id        ──┤
--  bn_id_persistence.current_bn_id ┘
--
--  bn_id_persistence.old_bn_id  →  bn_id_identity_changes.retired_bn_id
--  bn_id_identity_changes.surviving_bn_id  →  bn_id_xref.bn_id
--
--  Logical relationships:
--
--  bn_id_xref (1) ←──── (N) bn_id_hub
--      One bn_id has many edges in the hub table.
--
--  bn_id_xref (1) ←──── (N) bn_id_neighbors
--      One bn_id has many neighbor pairs.
--
--  bn_id_xref (1) ←──── (N) bn_id_persistence
--      One active bn_id may have many old bn_ids redirecting to it.
--
--  bn_id_xref (1) ←──── (N) bn_id_identity_changes
--      One surviving bn_id may have many merge/split events.
--
--  bn_id_metrics is standalone (no FK relationships).
--
-- ============================================================================
-- COMMON QUERIES
-- ============================================================================
--
-- Look up a person by email:
--   SELECT * FROM bn_id_xref WHERE identifier_type = 'email' AND identifier_value = 'jdoe@example.com';
--
-- Get all identifiers for a person:
--   SELECT * FROM bn_id_xref WHERE bn_id = 'BN_Xy7a2b3c4d5e6f';
--
-- Get all neighbors (pairwise links) for a person:
--   SELECT * FROM bn_id_neighbors WHERE bn_id = 'BN_Xy7a2b3c4d5e6f';
--
-- Get all edges (with confidence) for a person:
--   SELECT * FROM bn_id_hub WHERE bn_id = 'BN_Xy7a2b3c4d5e6f';
--
-- Resolve a stale bn_id:
--   SELECT current_bn_id FROM bn_id_persistence WHERE old_bn_id = 'BN_old123...';
--
-- Merge/split history for a bn_id:
--   SELECT * FROM bn_id_identity_changes WHERE surviving_bn_id = 'BN_Xy7a...' OR retired_bn_id = 'BN_Xy7a...';
