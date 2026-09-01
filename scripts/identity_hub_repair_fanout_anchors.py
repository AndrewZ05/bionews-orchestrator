"""
Repair the identity hub after the 2026-08-21 full rebuild dropped person anchors.

What happened: the fan-out quality filter deleted every edge of any node whose
degree exceeded its per-type threshold, so engaged subscribers lost even their
deterministic email<->mc_euid<->subscriber_hash edges and vanished from the
graph (21,087 emails; 19,551 currently subscribed). Fixed in the filter
(shared/identity_hub.py, anchor-aware fan-out). This script puts the lost
person identifiers back WITHOUT a rebuild and without changing any bn_id.

Source of truth: the pre-rebuild snapshot taken 2026-08-20 16:30 UTC
(platform_monthly_snapshots.bn_id_xref_202608 / bn_id_hub_202608).

For every pre-rebuild cluster that lost an email, restore the person-type
identifiers that are absent from the current xref, plus their deterministic
person-to-person edges from the snapshot hub, under one target bn_id chosen in
this order:
  1. the bn_id production profile_identifiers already uses for that email
     (profiles are the system of record downstream and must not split),
  2. the persistence redirect of the old bn_id (the cluster was merged away),
  3. the old bn_id itself (alive: rejoin it; gone: recreate it -- persistence
     reuses a prior bn_id when its members reappear, so this is stable).

Writes (apply mode only, after snapshotting all three tables):
  identity_hub_data.bn_id_xref        INSERT restored identifier rows, then
                                       recompute cluster_size / cluster_tier
                                       for touched clusters
  identity_hub_data.bn_id_node_index  INSERT (is_output TRUE, run_id = REPAIR_RUN_ID)
  identity_hub_data.bn_id_hub         INSERT deterministic anchor edges (is_active TRUE)

Idempotent: every INSERT is guarded by NOT EXISTS on the natural key.
Default is preview (counts only). Pass --apply to write.

    python scripts/identity_hub_repair_fanout_anchors.py --env prod
    python scripts/identity_hub_repair_fanout_anchors.py --env prod --apply
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.bigquery_client import get_bigquery_client, setup_gcp_credentials  # noqa: E402

PROJECT = "bi-data-391216"
HUB = "identity_hub_data"
SNAP = "platform_monthly_snapshots"
XREF_SNAP = f"{PROJECT}.{SNAP}.bn_id_xref_202608"
HUB_SNAP = f"{PROJECT}.{SNAP}.bn_id_hub_202608"
REPAIR_RUN_ID = "repair_fanout_anchors_20260823"
PERSON_TYPES = (
    "email",
    "subscriber_hash",
    "wp_user_id",
    "participant_id",
    "aim_dgid",
    "bnfpvid",
    "mc_euid",
    "npi_number",
    "bionews_uk",
)
RESTORE_TYPES = tuple(
    t for t in PERSON_TYPES if t != "bnfpvid"
)  # browser ids are not anchors
TIER1_TYPES = ("email", "mc_euid", "bionews_uk")


def sql_list(values):
    return ", ".join(f"'{v}'" for v in values)


def build_plan_sql(scratch: str) -> list[tuple[str, str]]:
    """Temp plan tables in the scratch dataset. Pure SELECT/CTAS; no production writes."""
    X = f"{PROJECT}.{HUB}.bn_id_xref"
    H = f"{PROJECT}.{HUB}.bn_id_hub"
    PERS = f"{PROJECT}.{HUB}.bn_id_persistence"
    PROD_IDS = f"{PROJECT}.profile_data.profile_identifiers"
    pt, rt, t1 = sql_list(PERSON_TYPES), sql_list(RESTORE_TYPES), sql_list(TIER1_TYPES)
    return [
        (
            "targets",
            f"""
        CREATE OR REPLACE TABLE `{scratch}.targets` AS
        WITH cur_emails AS (
          SELECT DISTINCT LOWER(identifier_value) AS email FROM `{X}` WHERE identifier_type = 'email'
        ),
        lost AS (
          SELECT DISTINCT LOWER(p.identifier_value) AS email, p.bn_id AS old_bn_id
          FROM `{XREF_SNAP}` p
          LEFT JOIN cur_emails c ON c.email = LOWER(p.identifier_value)
          WHERE p.identifier_type = 'email' AND c.email IS NULL AND NOT IFNULL(p.is_bot, FALSE)
        ),
        prod AS (
          SELECT LOWER(identifier_value) AS email, MIN(bn_id) AS prod_bn_id
          FROM `{PROD_IDS}` WHERE identifier_type = 'email' GROUP BY 1
        ),
        pers AS (
          SELECT old_bn_id, ARRAY_AGG(current_bn_id ORDER BY event_date DESC LIMIT 1)[OFFSET(0)] AS redirect_bn_id
          FROM `{PERS}` WHERE event_type = 'MERGE' AND event_date >= '2026-08-21' GROUP BY 1
        ),
        cur_bn AS (SELECT DISTINCT bn_id FROM `{X}`),
        per_email AS (
          SELECT l.email, l.old_bn_id,
                 COALESCE(prod.prod_bn_id, pers.redirect_bn_id, l.old_bn_id) AS target_bn_id,
                 CASE WHEN prod.prod_bn_id IS NOT NULL THEN 'production_profile'
                      WHEN pers.redirect_bn_id IS NOT NULL THEN 'persistence_redirect'
                      ELSE 'old_bn_id' END AS target_basis
          FROM lost l
          LEFT JOIN prod USING (email)
          LEFT JOIN pers ON pers.old_bn_id = l.old_bn_id
        ),
        -- one target per pre-rebuild cluster: the most common email target, ties by min
        per_cluster AS (
          SELECT old_bn_id, target_bn_id, target_basis
          FROM (
            SELECT old_bn_id, target_bn_id, ANY_VALUE(target_basis) AS target_basis, COUNT(*) AS n
            FROM per_email GROUP BY old_bn_id, target_bn_id
          )
          QUALIFY ROW_NUMBER() OVER (PARTITION BY old_bn_id ORDER BY n DESC, target_bn_id) = 1
        )
        SELECT pc.old_bn_id, pc.target_bn_id, pc.target_basis,
               (cb.bn_id IS NOT NULL) AS target_alive
        FROM per_cluster pc LEFT JOIN cur_bn cb ON cb.bn_id = pc.target_bn_id
        """,
        ),
        (
            "ids",
            f"""
        CREATE OR REPLACE TABLE `{scratch}.ids` AS
        SELECT t.target_bn_id AS bn_id, t.old_bn_id, t.target_basis, t.target_alive,
               s.identifier_key, s.identifier_type, s.identifier_value,
               s.is_hcp, s.is_shared_workstation, s.last_seen, s.source_profile,
               s.cluster_health_score, s.is_suspicious
        FROM `{XREF_SNAP}` s
        JOIN `{scratch}.targets` t ON t.old_bn_id = s.bn_id
        WHERE s.identifier_type IN ({rt})
          AND NOT IFNULL(s.is_bot, FALSE)
          AND NOT EXISTS (SELECT 1 FROM `{X}` x WHERE x.identifier_key = s.identifier_key)
        """,
        ),
        (
            "edges",
            f"""
        CREATE OR REPLACE TABLE `{scratch}.edges` AS
        -- an endpoint "lands in the target cluster" if it is being restored there
        -- or already sits in the current xref under that bn_id. Edges to nodes
        -- that now live in another cluster are NOT restored: the next
        -- incremental run would otherwise merge the two clusters on the strength
        -- of a snapshot, which is churn this repair must not cause.
        WITH keys AS (SELECT DISTINCT identifier_key FROM `{scratch}.ids`),
        landing AS (
          SELECT identifier_key, bn_id FROM `{scratch}.ids`
          UNION DISTINCT
          SELECT x.identifier_key, x.bn_id FROM `{X}` x
          WHERE x.bn_id IN (SELECT DISTINCT target_bn_id FROM `{scratch}.targets`)
        )
        SELECT t.target_bn_id AS bn_id,
               h.identifier_a_type, h.identifier_a_value, h.identifier_b_type, h.identifier_b_value,
               h.source_system, h.link_type, h.match_rule, h.base_confidence, h.confidence,
               h.effective_confidence, h.first_seen, h.last_seen, TRUE AS is_active
        FROM `{HUB_SNAP}` h
        JOIN `{scratch}.targets` t ON t.old_bn_id = h.bn_id
        WHERE h.link_type = 'deterministic'
          AND h.identifier_a_type IN ({pt}) AND h.identifier_b_type IN ({pt})
          AND (CONCAT(h.identifier_a_type, ':', h.identifier_a_value) IN (SELECT identifier_key FROM keys)
               OR CONCAT(h.identifier_b_type, ':', h.identifier_b_value) IN (SELECT identifier_key FROM keys))
          AND EXISTS (SELECT 1 FROM landing la WHERE la.identifier_key = CONCAT(h.identifier_a_type, ':', h.identifier_a_value) AND la.bn_id = t.target_bn_id)
          AND EXISTS (SELECT 1 FROM landing lb WHERE lb.identifier_key = CONCAT(h.identifier_b_type, ':', h.identifier_b_value) AND lb.bn_id = t.target_bn_id)
          AND NOT EXISTS (
            SELECT 1 FROM `{H}` c
            WHERE c.identifier_a_type = h.identifier_a_type AND c.identifier_a_value = h.identifier_a_value
              AND c.identifier_b_type = h.identifier_b_type AND c.identifier_b_value = h.identifier_b_value
              AND IFNULL(c.match_rule, '') = IFNULL(h.match_rule, '')
          )
        """,
        ),
    ]


def preview(client, scratch):
    q = lambda s: [dict(r) for r in client.query(s).result()]
    print("\n=== REPAIR PREVIEW ===")
    print("targets (pre-rebuild clusters that lost an email):")
    for r in q(
        f"SELECT target_basis, target_alive, COUNT(*) n FROM `{scratch}.targets` GROUP BY 1,2 ORDER BY 1,2"
    ):
        print(f"   {r['target_basis']:22s} alive={r['target_alive']!s:5s} {r['n']:,}")
    print("identifiers to restore:")
    for r in q(
        f"SELECT identifier_type, COUNT(*) n FROM `{scratch}.ids` GROUP BY 1 ORDER BY n DESC"
    ):
        print(f"   {r['identifier_type']:16s} {r['n']:,}")
    tot = q(
        f"SELECT COUNT(*) n, COUNT(DISTINCT bn_id) clusters, COUNTIF(is_suspicious) suspicious, COUNTIF(is_shared_workstation) ws FROM `{scratch}.ids`"
    )[0]
    print(
        f"   total {tot['n']:,} into {tot['clusters']:,} clusters (suspicious {tot['suspicious']:,}, shared_ws {tot['ws']:,})"
    )
    print("edges to restore:")
    for r in q(
        f"SELECT match_rule, COUNT(*) n FROM `{scratch}.edges` GROUP BY 1 ORDER BY n DESC"
    ):
        print(f"   {r['match_rule']:26s} {r['n']:,}")
    print("   total", f"{q(f'SELECT COUNT(*) n FROM `{scratch}.edges`')[0]['n']:,}")
    guard = q(
        f"""SELECT COUNT(*) n FROM `{scratch}.ids` i WHERE EXISTS (SELECT 1 FROM `{PROJECT}.{HUB}.bn_id_xref` x WHERE x.identifier_key = i.identifier_key)"""
    )[0]["n"]
    print(f"guard: identifiers already present in xref = {guard} (must be 0)")
    dup = q(f"SELECT COUNT(*) - COUNT(DISTINCT identifier_key) n FROM `{scratch}.ids`")[
        0
    ]["n"]
    print(f"guard: duplicate identifier keys in plan = {dup} (must be 0)")
    cross = q(
        f"""
        WITH landing AS (
          SELECT identifier_key, bn_id FROM `{scratch}.ids`
          UNION DISTINCT
          SELECT identifier_key, bn_id FROM `{PROJECT}.{HUB}.bn_id_xref`)
        SELECT COUNT(*) n FROM `{scratch}.edges` e
        WHERE NOT EXISTS (SELECT 1 FROM landing l WHERE l.identifier_key = CONCAT(e.identifier_a_type, ':', e.identifier_a_value) AND l.bn_id = e.bn_id)
           OR NOT EXISTS (SELECT 1 FROM landing l WHERE l.identifier_key = CONCAT(e.identifier_b_type, ':', e.identifier_b_value) AND l.bn_id = e.bn_id)"""
    )[0]["n"]
    print(
        f"guard: restored edges with an endpoint outside the target cluster = {cross} (must be 0)"
    )
    orphan = q(
        f"""SELECT COUNT(*) n FROM `{scratch}.ids` i WHERE i.identifier_type = 'email' AND NOT EXISTS (
        SELECT 1 FROM `{scratch}.edges` e WHERE e.bn_id = i.bn_id AND (CONCAT(e.identifier_a_type, ':', e.identifier_a_value) = i.identifier_key OR CONCAT(e.identifier_b_type, ':', e.identifier_b_value) = i.identifier_key))"""
    )[0]["n"]
    print(
        f"info : restored emails with no restored edge (xref-only, still valid anchors) = {orphan:,}"
    )
    return guard == 0 and dup == 0 and cross == 0


def apply(client, scratch):
    X = f"{PROJECT}.{HUB}.bn_id_xref"
    NI = f"{PROJECT}.{HUB}.bn_id_node_index"
    H = f"{PROJECT}.{HUB}.bn_id_hub"
    t1 = sql_list(TIER1_TYPES)
    run = lambda label, s: (
        print(f"   [{label}] ...", flush=True),
        client.query(s).result(),
    )[1]
    print("\n=== APPLY ===")
    for t in ("bn_id_xref", "bn_id_node_index", "bn_id_hub"):
        run(
            f"snapshot {t}",
            f"""
        CREATE SNAPSHOT TABLE IF NOT EXISTS `{PROJECT}.{SNAP}.{t}_prerepair_20260823`
        CLONE `{PROJECT}.{HUB}.{t}`
        OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY))""",
        )
    run(
        "insert xref",
        f"""
        INSERT INTO `{X}` (identifier_key, bn_id, identifier_type, identifier_value, cluster_tier, cluster_size,
                           is_hcp, is_shared_workstation, last_seen, source_profile, is_bot, cluster_health_score, is_suspicious)
        SELECT i.identifier_key, i.bn_id, i.identifier_type, i.identifier_value, 'tier1', 0,
               i.is_hcp, i.is_shared_workstation, i.last_seen, i.source_profile, FALSE, i.cluster_health_score, i.is_suspicious
        FROM `{scratch}.ids` i
        WHERE NOT EXISTS (SELECT 1 FROM `{X}` x WHERE x.identifier_key = i.identifier_key)""",
    )
    run(
        "recompute cluster_size/tier for touched clusters",
        f"""
        UPDATE `{X}` x
        SET cluster_size = s.n,
            cluster_tier = IF(s.has_tier1, 'tier1', x.cluster_tier)
        FROM (
          SELECT bn_id, COUNT(*) n, LOGICAL_OR(identifier_type IN ({t1})) has_tier1
          FROM `{X}` WHERE bn_id IN (SELECT DISTINCT bn_id FROM `{scratch}.ids`) GROUP BY 1
        ) s
        WHERE x.bn_id = s.bn_id""",
    )
    run(
        "insert node_index",
        f"""
        INSERT INTO `{NI}` (identifier_key, bn_id, identifier_type, is_output, cluster_tier, run_id)
        SELECT i.identifier_key, i.bn_id, i.identifier_type, TRUE, 'tier1', '{REPAIR_RUN_ID}'
        FROM `{scratch}.ids` i
        WHERE NOT EXISTS (SELECT 1 FROM `{NI}` n WHERE n.identifier_key = i.identifier_key)""",
    )
    run(
        "insert hub edges",
        f"""
        INSERT INTO `{H}` (bn_id, identifier_a_type, identifier_a_value, identifier_b_type, identifier_b_value, source_system,
                           link_type, match_rule, base_confidence, confidence, effective_confidence, first_seen, last_seen, is_active, cluster_tier)
        SELECT e.bn_id, e.identifier_a_type, e.identifier_a_value, e.identifier_b_type, e.identifier_b_value, e.source_system,
               e.link_type, e.match_rule, e.base_confidence, e.confidence, e.effective_confidence, e.first_seen, e.last_seen, TRUE, 'tier1'
        FROM `{scratch}.edges` e
        WHERE NOT EXISTS (
          SELECT 1 FROM `{H}` c
          WHERE c.identifier_a_type = e.identifier_a_type AND c.identifier_a_value = e.identifier_a_value
            AND c.identifier_b_type = e.identifier_b_type AND c.identifier_b_value = e.identifier_b_value
            AND IFNULL(c.match_rule, '') = IFNULL(e.match_rule, ''))""",
    )
    q = lambda s: [dict(r) for r in client.query(s).result()]
    print("\n=== VERIFY ===")
    v = q(f"""SELECT COUNTIF(x.identifier_key IS NOT NULL) in_xref, COUNTIF(n.identifier_key IS NOT NULL) in_node_index, COUNT(*) planned
              FROM `{scratch}.ids` i LEFT JOIN `{X}` x USING (identifier_key) LEFT JOIN `{NI}` n USING (identifier_key)""")[
        0
    ]
    print("   identifiers:", v)
    print(
        "   bn_id agreement xref vs node_index:",
        q(f"""SELECT COUNTIF(x.bn_id != n.bn_id) mismatches FROM `{scratch}.ids` i
          JOIN `{X}` x USING (identifier_key) JOIN `{NI}` n USING (identifier_key)""")[
            0
        ],
    )
    print(
        "   emails in xref now:",
        q(
            f"SELECT COUNT(DISTINCT LOWER(identifier_value)) n FROM `{X}` WHERE identifier_type='email'"
        )[0],
    )
    print(
        "   subscribed Mailchimp members absent from xref now:",
        q(f"""
        SELECT COUNT(DISTINCT m.email) n FROM (SELECT LOWER(email_address) email FROM `{PROJECT}.mailchimp_data.members` WHERE status='subscribed') m
        LEFT JOIN (SELECT DISTINCT LOWER(identifier_value) email FROM `{X}` WHERE identifier_type='email') x USING (email)
        WHERE x.email IS NULL AND EXISTS (SELECT 1 FROM `{PROJECT}.profile_data.profile_identifiers` p WHERE p.identifier_type='email' AND LOWER(p.identifier_value)=m.email)""")[
            0
        ],
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="prod")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument(
        "--keep-plan",
        action="store_true",
        help="leave the plan tables in the scratch dataset",
    )
    args = ap.parse_args()
    if args.env != "prod":
        sys.exit("only prod is wired")
    setup_gcp_credentials()
    client = get_bigquery_client(project=PROJECT)
    scratch = f"{PROJECT}.profile_staging"
    # plan tables carry a fixed prefix so reruns replace them
    scratch_tables = {
        k: f"{scratch}._tmp_hub_repair_{k}" for k in ("targets", "ids", "edges")
    }
    for label, sql in build_plan_sql(scratch):
        for k, full in scratch_tables.items():
            sql = sql.replace(f"`{scratch}.{k}`", f"`{full}`")
        print(f"   plan: {label} ...", flush=True)
        client.query(sql).result()

    class _S(str):
        pass

    # preview/apply use `{scratch}.<k>` names; map them the same way
    def _map(sql):
        for k, full in scratch_tables.items():
            sql = sql.replace(f"`{scratch}.{k}`", f"`{full}`")
        return sql

    orig_query = client.query
    client.query = lambda s, *a, **kw: orig_query(_map(s), *a, **kw)
    ok = preview(client, scratch)
    if args.apply:
        if not ok:
            sys.exit("guards failed; not applying")
        apply(client, scratch)
    else:
        print("\n(preview only -- pass --apply to write; snapshots are taken first)")
    if not args.keep_plan:
        for full in scratch_tables.values():
            client.delete_table(full, not_found_ok=True)


if __name__ == "__main__":
    main()
