-- ============================================================================
-- Identity Hub — DDL for Dezign ERD
-- Dataset: bi-data-391216.identity_hub_data
-- Updated: 2026-04-23
--
-- Note: BigQuery does not enforce FK constraints but Dezign uses them
-- to render relationships. All constraints are REFERENCES only.
-- ============================================================================


-- 1. bn_id_xref — Fast lookup: identifier → bn_id
--    One row per identifier in the graph. Central lookup table; all other
--    tables relate back to bn_id_xref.bn_id.
CREATE TABLE identity_hub_data.bn_id_xref (
    identifier_key          STRING      NOT NULL,
    bn_id                   STRING      NOT NULL,
    identifier_type         STRING      NOT NULL,
    identifier_value        STRING      NOT NULL,
    cluster_tier            STRING,
    cluster_size            INTEGER,
    is_hcp                  BOOLEAN,
    is_shared_workstation   BOOLEAN,
    last_seen               TIMESTAMP,
    source_profile          STRING,
    is_bot                  BOOLEAN,
    cluster_health_score    INTEGER,
    is_suspicious           BOOLEAN,
    CONSTRAINT pk_xref PRIMARY KEY (identifier_key),
    CONSTRAINT uq_xref_bn_id UNIQUE (bn_id)
);


-- 2. bn_id_hub — All identity edges with confidence scores
--    Every observed relationship between two identifiers.
--    Partitioned by last_seen (DAY), clustered by bn_id, identifier_type.
CREATE TABLE identity_hub_data.bn_id_hub (
    bn_id                   STRING      NOT NULL,
    identifier_a_type       STRING      NOT NULL,
    identifier_a_value      STRING      NOT NULL,
    identifier_b_type       STRING      NOT NULL,
    identifier_b_value      STRING      NOT NULL,
    identifier_type         STRING,
    identifier_value        STRING,
    source_system           STRING,
    link_type               STRING,
    match_rule              STRING,
    base_confidence         FLOAT64,
    confidence              FLOAT64,
    effective_confidence    FLOAT64,
    first_seen              TIMESTAMP,
    last_seen               TIMESTAMP,
    is_active               BOOLEAN,
    cluster_tier            STRING,
    CONSTRAINT fk_hub_xref FOREIGN KEY (bn_id) REFERENCES identity_hub_data.bn_id_xref (bn_id)
);


-- 3. bn_id_neighbors — Pairwise relationships within clusters
--    Cross-product of all identifier pairs within each bn_id cluster.
--    Clustered by bn_id, node_type.
CREATE TABLE identity_hub_data.bn_id_neighbors (
    bn_id                   STRING      NOT NULL,
    node_type               STRING      NOT NULL,
    node_value              STRING      NOT NULL,
    neighbor_type           STRING      NOT NULL,
    neighbor_value          STRING      NOT NULL,
    confidence              FLOAT64,
    match_rule              STRING,
    source_system           STRING,
    first_seen              TIMESTAMP,
    last_seen               TIMESTAMP,
    cluster_tier            STRING,
    CONSTRAINT fk_neighbors_xref FOREIGN KEY (bn_id) REFERENCES identity_hub_data.bn_id_xref (bn_id)
);


-- 4. bn_id_persistence — Stable bn_id registry + merge redirects
--    Maps retired bn_ids to their current active bn_id after merges.
CREATE TABLE identity_hub_data.bn_id_persistence (
    old_bn_id               STRING      NOT NULL,
    current_bn_id           STRING      NOT NULL,
    event_type              STRING,
    event_date              DATE,
    run_id                  STRING,
    build_mode              STRING,
    config_version          STRING,
    git_sha                 STRING,
    CONSTRAINT pk_persistence PRIMARY KEY (old_bn_id),
    CONSTRAINT fk_persistence_xref FOREIGN KEY (current_bn_id) REFERENCES identity_hub_data.bn_id_xref (bn_id)
);


-- 5. bn_id_identity_changes — Merge/split audit log
--    Append-only permanent audit trail of every cluster merge and split.
CREATE TABLE identity_hub_data.bn_id_identity_changes (
    event_date              DATE        NOT NULL,
    event_type              STRING      NOT NULL,
    surviving_bn_id         STRING      NOT NULL,
    retired_bn_id           STRING      NOT NULL,
    node_count_moved        INTEGER,
    trigger_edge            STRING,
    run_id                  STRING,
    build_mode              STRING,
    config_version          STRING,
    git_sha                 STRING,
    CONSTRAINT fk_changes_surviving FOREIGN KEY (surviving_bn_id) REFERENCES identity_hub_data.bn_id_xref (bn_id),
    CONSTRAINT fk_changes_retired FOREIGN KEY (retired_bn_id) REFERENCES identity_hub_data.bn_id_persistence (old_bn_id)
);


-- 6. bn_id_metrics — Run statistics
--    One row per metric per pipeline run.
CREATE TABLE identity_hub_data.bn_id_metrics (
    run_date                DATE        NOT NULL,
    run_id                  STRING,
    build_mode              STRING,
    config_version          STRING,
    git_sha                 STRING,
    metric_name             STRING      NOT NULL,
    metric_value            FLOAT64
);


-- 7. bn_id_manifest — Active run promotion state
--    Single-row table tracking the last successfully promoted run.
CREATE TABLE identity_hub_data.bn_id_manifest (
    active_run_id           STRING      NOT NULL,
    promoted_at             TIMESTAMP   NOT NULL,
    status                  STRING      NOT NULL
);


-- 8. bn_id_node_index — Full node-to-bn_id mapping
--    Includes all nodes processed during a run, including non-output nodes.
--    Used for incremental rebuild targeting.
CREATE TABLE identity_hub_data.bn_id_node_index (
    identifier_key          STRING,
    bn_id                   STRING,
    identifier_type         STRING,
    is_output               BOOLEAN,
    cluster_tier            STRING,
    run_id                  STRING,
    CONSTRAINT fk_node_index_xref FOREIGN KEY (bn_id) REFERENCES identity_hub_data.bn_id_xref (bn_id)
);
