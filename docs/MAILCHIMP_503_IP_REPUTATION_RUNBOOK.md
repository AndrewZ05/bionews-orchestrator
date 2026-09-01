# Mailchimp 503 / IP Reputation Runbook

**When to use this:** Mailchimp extractions fail with
`too many 503 error responses` / `MaxRetryError` on `/` or `/campaigns` —
the first calls of a run, before any table work — and the same API key
succeeds from a different machine.

That last part is the whole diagnosis. If the key works elsewhere at the same
moment, nothing is wrong with the credential, the account, the config, or the
code. The prod VM's **egress IP** is being shed by Mailchimp's edge proxy.

## What is actually happening

Mailchimp fronts its API with an `istio-envoy` proxy (visible in the `Server`
response header). That layer does **abuse-protection load shedding**, which is
a different mechanism from the documented rate limit:

| | Documented rate limit | What we hit |
|---|---|---|
| Status | `429` | **`503`** |
| Scope | API key / account | **Source IP** |
| Trigger | >10 req/sec | Bursty retries, killed mid-flight runs |
| Clears in | ~1 second | **Minutes to hours** |
| `Retry-After` | Sent | Not sent |

**We have never observed a `429` from this pipeline.** Every failure on
2026-08-27/28 was a `503`. Probing from a healthy IP, 40 sequential calls in
8.7s and 60 concurrent calls at ~20/sec all returned `200` — Mailchimp did not
enforce the 10 req/sec limit with rejections at all.

So this is **IP reputation**, not rate accounting. Lowering
`requests_per_second` does not fix it. (We tried: 10.0 → 9.0 changed nothing.
That change was still correct on its own merits — running at exactly the
documented cap leaves no headroom for jitter — but it was not the cause.)

### What earns a bad reputation

The 2026-08-28 incident followed this sequence:

1. A ~2-hour `campaign_group` extraction was killed mid-flight.
2. Seven run attempts followed in under an hour.
3. Each failed in ~90 seconds because the retry policy gave up after ~3.5s,
   which invited another manual retry.

From the edge proxy, that reads as a misbehaving client. The impatient retry
policy turned every transient shed into an immediate hard failure, which drove
more retries — a feedback loop. Commit `5e7a11e` broke that loop (adapter
tolerance ~3.5s → ~62s), but it cannot un-shed an IP already flagged.

## Triage — is this actually an IP problem?

Run the same call from a machine on a different network (a laptop is fine):

```bash
curl -s -o /dev/null -w "%{http_code}\n" \
  --user "orchestrator:$MAILCHIMP_API_KEY" \
  "https://us5.api.mailchimp.com/3.0/"
```

- **200 elsewhere, 503 on the box** → IP reputation. Continue below.
- **503 everywhere** → Mailchimp-wide outage. Wait; nothing here applies.
- **401/403 anywhere** → credential problem, not this runbook.

## Immediate options

### Option A — Wait it out (default; do this first)

The block clears on Mailchimp's own timer. **Stop calling the API entirely**,
including `curl` health checks — every request plausibly refreshes the window.

Leave it alone 30–60 minutes, then check **once**. Repeat at 15-minute
intervals at most.

The nightly cron will fire regardless. Since `5e7a11e` it has ~62s of retry
tolerance instead of ~3.5s, so it stands a reasonable chance of riding through
a partial shed on its own.

### Option B — Restart the VM (fast, if urgent)

If the VM's external IP is **ephemeral**, a stop/start usually assigns a new
one, which resets reputation immediately.

```bash
gcloud compute instances stop  pipeline-20250926-02 --zone=us-east1-c --project=bi-data-391216
gcloud compute instances start pipeline-20250926-02 --zone=us-east1-c --project=bi-data-391216
# confirm the address actually changed
gcloud compute instances describe pipeline-20250926-02 --zone=us-east1-c \
  --project=bi-data-391216 \
  --format="value(networkInterfaces[0].accessConfigs[0].natIP)"
```

~5 minutes, keeps SSH access throughout, no lockout risk. **Check no pipeline
is mid-run first** (`python orchestrate.py --env prod --job-locks`). If the IP
is *static*, this changes nothing — go to Option C.

## Option C — Cloud NAT with a reserved static IP (durable fix)

**Do this as planned work, not as incident response.** It is a real network
change with a genuine lockout risk. It is the right long-term answer because
this box runs multi-hour Mailchimp extractions daily, so IP reputation matters
persistently.

### What changes

```
Today:      VM ──[ephemeral IP, can change without warning]──> Mailchimp
Cloud NAT:  VM (no external IP) ──> Cloud Router ──> Cloud NAT ──[static IP you own]──> Mailchimp
```

Two benefits:

1. **A stable reputation** that accrues over time, instead of starting from
   zero whenever the ephemeral IP changes.
2. **An allowlistable address** — if Mailchimp support ever needs to whitelist
   you, that requires a fixed IP.

Attaching a *newly reserved* IP also clears a current block immediately, since
the address has no history.

### ⚠️ Before you start: confirm you will not lock yourself out

Step 4 removes the VM's public address, which kills SSH-over-external-IP.
**Verify IAP SSH works first:**

```bash
gcloud compute ssh pipeline-20250926-02 --zone=us-east1-c \
  --project=bi-data-391216 --tunnel-through-iap
```

Requires `roles/iap.tunnelResourceAccessor` and a firewall rule allowing
`35.235.240.0/20` on tcp:22. If that command fails, **stop** — set up IAP or a
bastion host before going further.

Also confirm nothing needs to reach this VM *inbound*. Cloud NAT is
outbound-only. (Today this box only makes outbound API calls, so this is
expected to be fine — verify it is still true.)

### Procedure

```bash
PROJECT=bi-data-391216
REGION=us-east1
ZONE=us-east1-c
VM=pipeline-20250926-02

# 1. Reserve a static regional IP (region MUST match the VM's)
gcloud compute addresses create mailchimp-nat-ip \
  --region=$REGION --project=$PROJECT

gcloud compute addresses describe mailchimp-nat-ip \
  --region=$REGION --project=$PROJECT --format="value(address)"

# 2. Cloud Router in the same region + VPC as the VM
#    (confirm the network name first -- 'default' may not be correct)
gcloud compute routers create orchestrator-router \
  --network=default --region=$REGION --project=$PROJECT

# 3. NAT gateway pinned to the reserved IP
gcloud compute routers nats create orchestrator-nat \
  --router=orchestrator-router --region=$REGION \
  --nat-external-ip-pool=mailchimp-nat-ip \
  --nat-all-subnet-ip-ranges \
  --project=$PROJECT

# 4. POINT OF NO EASY RETURN -- removes the VM's external IP.
#    Confirm the access config name first:
gcloud compute instances describe $VM --zone=$ZONE --project=$PROJECT \
  --format="value(networkInterfaces[0].accessConfigs[0].name)"

gcloud compute instances delete-access-config $VM \
  --zone=$ZONE --access-config-name="external-nat" --project=$PROJECT
```

### Verify

From the box (via IAP SSH), confirm egress now uses the reserved IP:

```bash
curl -s https://ifconfig.me; echo        # must equal the reserved address
curl -s -o /dev/null -w "%{http_code}\n" \
  --user "orchestrator:$MAILCHIMP_API_KEY" \
  "https://us5.api.mailchimp.com/3.0/"   # expect 200
```

Then a real run:

```bash
python orchestrate.py --source mailchimp --group campaign_group --env prod --lookback 30
```

### Rollback

Re-attach an ephemeral external IP:

```bash
gcloud compute instances add-access-config $VM \
  --zone=$ZONE --access-config-name="external-nat" --project=$PROJECT
```

The NAT gateway and router can stay; a VM with its own external IP bypasses
NAT automatically. To fully remove, delete the NAT, then the router, then
release the address.

### Cost

A few dollars a month for the reserved IP plus NAT data-processing charges.
Negligible against the cost of failed daily extractions.

## Prevention

- **Never leave a killed run to be retried immediately.** If a Mailchimp run
  is killed mid-flight, wait several minutes before restarting. Back-to-back
  attempts are what built the bad reputation.
- **Do not lower `requests_per_second` in response to 503s.** It is unrelated;
  503s are reputation, 429s are rate. If you ever *do* see 429s, that is the
  knob — and `configs/mailchimp.yaml` should stay at 9.0, not 10.0.
- **Do not weaken the retry policy** in `shared/mailchimp_client.py`.
  `tests/unit/test_mailchimp_batch.py` pins `total >= 6`,
  `backoff_factor >= 2`, `respect_retry_after_header`, and — critically —
  `raise_on_status=False`. Without that last flag, urllib3 raises `RetryError`
  from inside `session.request()` and `mailchimp_request()`'s own 5xx/429
  backoff never sees the status code, making it dead code for 503s.

## History

- **2026-08-27** — 393 `503`s mid-run; the run still completed (207 min).
- **2026-08-28** — a killed 2-hour run plus ~7 rapid retries got the box's IP
  shed. Five consecutive `campaign_group` runs failed on their first call.
  Same key returned `200` from a laptop throughout, which is what identified
  the cause. Fixed forward with `5e7a11e` (retry patience); the block itself
  was waited out.
