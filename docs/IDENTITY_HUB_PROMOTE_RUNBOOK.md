# Identity Hub Promote Recovery Runbook

**When to use this:** an identity hub full rebuild failed during or after the
"Promoting shadow tables to production" phase, OR the profile database
preflight is blocked with `single_generation` reporting a non-PROMOTED
manifest status (e.g. `BUILDING`).

Fail-closed is intentional: a stale `BUILDING` row means the last promote may
have left production in a mixed state, and downstream refuses to build on it
until a human confirms which state production is actually in. This runbook is
how you confirm it and clear the block.

## How promotion works (context)

Full rebuild (`python shared/identity_hub.py --rebuild`) order of operations:

1. Merge log (`bn_id_identity_changes`) + persistence redirects
   (`bn_id_persistence`) are written to production ledgers FIRST.
2. A `BUILDING` row is inserted into `bn_id_manifest` (fatal if it fails --
   the run aborts with production untouched).
3. Shadow tables are copied over production, in order:
   `bn_id_hub`, `bn_id_xref`, `bn_id_neighbors`, `bn_id_node_index`.
4. A `PROMOTED` row is inserted into `bn_id_manifest`.
5. Metrics are written.

The profile preflight reads the NEWEST manifest row (by `promoted_at`) and
requires `status = 'PROMOTED'`. A crash anywhere in step 3 leaves `BUILDING`
as the newest row, which blocks downstream by design.

## Recovery procedure

### Step 1 -- Confirm the stuck state

```sql
SELECT active_run_id, status, promoted_at
FROM `bi-data-391216.identity_hub_data.bn_id_manifest`
ORDER BY promoted_at DESC
LIMIT 5;
```

If the newest row is `PROMOTED`, nothing is stuck -- stop here (the preflight
failure was something else, e.g. the freshness gate).

### Step 2 -- Determine how far the promote got

Check row counts and last-modified times of the four promoted tables:

```sql
SELECT table_id, row_count, TIMESTAMP_MILLIS(last_modified_time) AS modified
FROM `bi-data-391216.identity_hub_data.__TABLES__`
WHERE table_id IN ('bn_id_hub', 'bn_id_xref', 'bn_id_neighbors', 'bn_id_node_index');
```

Interpretation:
- **All four modified within the failed run's window** -> promote actually
  completed; only the PROMOTED stamp is missing. Go to Step 3a.
- **None modified within the run window** -> promote never started copying;
  production is entirely the previous generation. Go to Step 3b.
- **Some modified, some not** -> mixed state. Go to Step 3c.

Cross-check consistency: xref and hub should tell the same story --

```sql
SELECT
  (SELECT COUNT(DISTINCT bn_id) FROM `bi-data-391216.identity_hub_data.bn_id_xref`) AS xref_people,
  (SELECT COUNT(DISTINCT bn_id) FROM `bi-data-391216.identity_hub_data.bn_id_hub`)  AS hub_people;
```

A large divergence (more than a few percent) indicates mixed generations.

### Step 3a -- Promote completed, stamp missing

Insert a corrective PROMOTED row for the run that finished:

```sql
INSERT INTO `bi-data-391216.identity_hub_data.bn_id_manifest`
  (active_run_id, promoted_at, status)
VALUES ('<run_id of the completed run>', CURRENT_TIMESTAMP(), 'PROMOTED');
```

Downstream unblocks on the next preflight.

### Step 3b -- Promote never started

Production is the previous, still-consistent generation. Re-stamp it:

```sql
INSERT INTO `bi-data-391216.identity_hub_data.bn_id_manifest`
  (active_run_id, promoted_at, status)
VALUES ('<active_run_id of the previous PROMOTED row>', CURRENT_TIMESTAMP(), 'PROMOTED');
```

Then re-run the full rebuild when convenient. Note: the failed run already
wrote its merge/persistence ledger rows (step 1 happens before promote). That
is safe -- the redirect writes are idempotent, so the re-run will skip the
already-written pairs rather than duplicating them.

### Step 3c -- Mixed state

Do NOT stamp PROMOTED. The only clean recovery is to re-run the full rebuild:

```bash
python shared/identity_hub.py --rebuild
```

The rebuild rewrites all four tables from scratch, so a mixed intermediate
state is fully healed by a successful run. If the rebuild cannot be run
immediately and downstream pressure is high, the pre-promote CLONE backups
(if present from the failed run) can restore the previous generation -- check
`identity_hub_data` for `*_backup_*` tables before considering this path.

### Step 4 -- Verify

```sql
SELECT active_run_id, status, promoted_at
FROM `bi-data-391216.identity_hub_data.bn_id_manifest`
ORDER BY promoted_at DESC
LIMIT 1;
```

Newest row must be `PROMOTED`. Then run the profile preflight (or just the
next scheduled profile job) and confirm `single_generation` and
`manifest_freshness` both pass.

## Related gates

- **Freshness gate** (`manifest_freshness`): the profile preflight fails a
  rebuild (and warns on refresh) when the newest PROMOTED row is older than
  `PROFILE_HUB_MANIFEST_MAX_AGE_DAYS` (default 45). The fix is not a manifest
  edit -- it is running the overdue hub full rebuild.
- **Shrink safeguard**: the hub refuses to write output tables at less than
  50% of their previous row count. If a rebuild aborts on it, investigate
  source data before overriding with `--force-overwrite`.
- **Connector failure checkpoint**: any connector error aborts the run before
  post-processing (bot_detection excepted -- it degrades loudly instead).
  Re-run after fixing the connector; nothing needs manual cleanup because the
  abort happens before any production write.
