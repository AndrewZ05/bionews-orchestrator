# Server Administration Guide

## GCP Compute Engine Backup & Restore

Orchestrator Pipeline Project — February 2026

---

## 1. Overview

The Server Administration tool provides automated backup, restore, and lifecycle management for the GCP Compute Engine instance that hosts the Orchestrator pipeline. It creates GCE machine images (full disk + metadata snapshots) on a scheduled or ad-hoc basis and enforces a configurable retention policy to control storage costs.

The tool is designed to run on Windows, macOS, or Linux — anywhere the Google Cloud SDK (gcloud CLI) is installed and authenticated.

---

## 2. Architecture

### 2.1 File Layout

| File | Purpose |
|------|---------|
| `ops/server_admin.yaml` | Configuration — project, instance, zone, backup prefix, retention |
| `ops/server_admin.py` | CLI tool — backup, restore, cleanup, list commands |
| `ops/__init__.py` | Python package marker for the ops/ directory |

### 2.2 How It Fits the Orchestrator

Although `server_admin.py` does not run ETL pipelines, it follows every convention established by the orchestrator codebase:

- Argparse with subparsers and `set_defaults(func=...)` dispatch (same as `pipeline_manager.py`)
- `--log-level` choices `[minimal, normal, verbose]` using `shared/log_config.configure_logging()`
- `--dry-run` on all state-changing commands; `--force` on destructive restore
- `def main()` returning int exit codes with `sys.exit(main())`
- try/except handling: `KeyboardInterrupt` → 130, `CalledProcessError` → 1, `Exception` → 1
- `shared/display_utils` for structured terminal output (`print_header`, `print_table`, etc.)
- Module-level `logger = logging.getLogger(__name__)`
- YAML config validation with collected errors before `sys.exit(1)`

### 2.3 Cross-Platform Design

The tool runs identically on Windows, macOS, and Linux:

- `shutil.which("gcloud")` locates the binary on any OS (no hardcoded paths)
- `pathlib.Path` throughout (no OS-specific path separators)
- `subprocess.run()` with list arguments (no `shell=True`)
- No Unix-only paths in the YAML configuration

### 2.4 Backup Naming Convention

Each machine image is named using the pattern:

```
{prefix}-{YYYYMMDD}-{HHMM}-{git-sha}
```

Example: `pipeline-auto-20260223-2200-a1b2c3d`

The git SHA ties each backup to the exact code revision deployed on the server, making it easy to correlate infrastructure state with code changes.

### 2.5 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    server_admin.py                          │
│                                                             │
│  backup ──► gcloud machine-images create ──► GCE Image     │
│              │                                              │
│              └──► cleanup (auto) ──► delete expired images  │
│                                                             │
│  restore ──► gcloud instances delete ──► instances create   │
│              (from machine image + static IP)               │
│                                                             │
│  cleanup ──► list images ──► delete where age > retention   │
│                                                             │
│  list    ──► gcloud machine-images list ──► formatted table │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Configuration

All settings live in `ops/server_admin.yaml`:

```yaml
project_id: bi-data-391216

instance:
  name: pipeline-20250926-02
  zone: us-east1-c
  region: us-east1
  static_ip_name: pipeline-ip-20250926

backup:
  prefix: pipeline-auto
  retention_days: 30
```

### 3.1 Configuration Reference

| Field | Type | Description |
|-------|------|-------------|
| `project_id` | string | GCP project ID |
| `instance.name` | string | Compute Engine instance name |
| `instance.zone` | string | GCE zone (e.g., us-east1-c) |
| `instance.region` | string | GCE region (informational) |
| `instance.static_ip_name` | string | Reserved static IP resource name (reattached on restore) |
| `backup.prefix` | string | Naming prefix for machine images |
| `backup.retention_days` | integer | Images older than this are deleted during cleanup |

---

## 4. Command Reference

### 4.1 backup — Create Machine Image

Creates a full machine image of the running instance and automatically cleans up expired images afterward.

**Syntax:**

```bash
python ops/server_admin.py backup [--dry-run] [--log-level {minimal,normal,verbose}]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--dry-run` | Log the gcloud commands that would execute without running them |
| `--log-level` | Set verbosity: minimal (warnings only), normal (default), verbose (debug) |

**Expected Output:**

```
======================================================================
                         Server Backup
======================================================================

Instance: pipeline-20250926-02
Zone: us-east1-c
Image name: pipeline-auto-20260223-2200-a1b2c3d
2026-02-23 22:00:01 - ops.server_admin - INFO - Running: gcloud compute machine-images create ...
2026-02-23 22:00:45 - ops.server_admin - INFO - Machine image created: pipeline-auto-20260223-2200-a1b2c3d

----------------------------------------------------------------------
Cleanup expired images
----------------------------------------------------------------------
Prefix filter: pipeline-auto
Retention: 30 days
2026-02-23 22:00:46 - ops.server_admin - INFO - Cleanup complete: 0 image(s) deleted.
```

**What happens behind the scenes:**

1. Reads instance name, zone, and project from `server_admin.yaml`
2. Generates image name: `{prefix}-{YYYYMMDD}-{HHMM}-{git-sha}`
3. Calls `gcloud compute machine-images create` (captures full disk, metadata, and config)
4. Runs automatic cleanup — lists all images with the configured prefix and deletes any older than `retention_days`

---

### 4.2 cleanup — Remove Expired Images

Deletes machine images older than the configured retention period. This runs automatically after each backup, but can also be invoked standalone.

**Syntax:**

```bash
python ops/server_admin.py cleanup [--dry-run]
```

**Expected Output:**

```
======================================================================
                      Cleanup Expired Images
======================================================================

Prefix filter: pipeline-auto
Retention: 30 days
2026-02-23 22:00:01 - ops.server_admin - INFO - Expired (45d old): pipeline-auto-20260109-0200-f8e9d0a
2026-02-23 22:00:01 - ops.server_admin - INFO - Running: gcloud compute machine-images delete ...
2026-02-23 22:00:15 - ops.server_admin - INFO - Cleanup complete: 1 image(s) deleted.
```

---

### 4.3 restore — Rebuild Instance from Image

Deletes the current Compute Engine instance and recreates it from a specified machine image, reattaching the static IP address. This is a destructive operation and requires either interactive confirmation (type YES) or the `--force` flag.

**Syntax:**

```bash
python ops/server_admin.py restore --image <name> [--force] [--dry-run]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--image` (required) | The machine image name to restore from |
| `--force` | Skip the interactive YES confirmation prompt |
| `--dry-run` | Log the gcloud commands without executing them |

**Expected Output:**

```
======================================================================
                         Server Restore
======================================================================

Instance: pipeline-20250926-02
Zone: us-east1-c
Source image: pipeline-auto-20260222-2200-a1b2c3d
Static IP: pipeline-ip-20250926

[WARNING] This will DELETE the running instance and recreate it.
Type YES to continue: YES

----------------------------------------------------------------------
Delete existing instance
----------------------------------------------------------------------
2026-02-23 10:00:01 - ops.server_admin - INFO - Running: gcloud compute instances delete ...

----------------------------------------------------------------------
Recreate from machine image
----------------------------------------------------------------------
2026-02-23 10:01:30 - ops.server_admin - INFO - Running: gcloud compute instances create ...
2026-02-23 10:02:45 - ops.server_admin - INFO - Instance pipeline-20250926-02 restored from pipeline-auto-20260222-2200-a1b2c3d.
```

**Restore sequence:**

1. Displays instance details and source image name
2. Prompts for YES confirmation (unless `--force` is passed)
3. Deletes the existing instance (`gcloud compute instances delete --quiet`)
4. Creates a new instance from the machine image with the static IP reattached
5. The new instance boots with the exact disk contents and metadata from the image

---

### 4.4 list — Show Available Images

Displays a formatted table of existing machine images, sorted newest first.

**Syntax:**

```bash
python ops/server_admin.py list [--limit N]
```

**Expected Output:**

```
======================================================================
                         Machine Images
======================================================================

Prefix filter: pipeline-auto
Name                                    | Created             | Age | Status
---------------------------------------------------------------------------
pipeline-auto-20260223-2200-a1b2c3d     | 2026-02-23T22:00:01 | 0d  | READY
pipeline-auto-20260222-2200-b2c3d4e     | 2026-02-22T22:00:01 | 1d  | READY
pipeline-auto-20260221-2200-c3d4e5f     | 2026-02-21T22:00:01 | 2d  | READY
```

---

## 5. Automated Scheduling (Cron)

The backup command should be scheduled via cron on any machine with gcloud authenticated (typically the pipeline server itself).

### 5.1 Crontab Entry

**IMPORTANT — two cron-environment gotchas on the pipeline VM (both cause silent, total failure):**

1. **Log path.** The cron user (`robert_macinnis_bionews_com`) cannot write to `/var/log/`. A redirect to `/var/log/...` makes the shell exit *before* Python runs -- the backup never happens and nothing is logged. All cron jobs on this VM log to `/home/orchestrator/logs/cron/`.
2. **PATH.** `gcloud` is installed at `/snap/bin/gcloud`, and cron's default PATH (`/usr/bin:/bin`) does not include `/snap/bin`. Without fixing PATH, `shutil.which("gcloud")` returns nothing and the tool exits 1 with "gcloud CLI not found on PATH." Prepend `PATH=/snap/bin:/usr/bin:/bin` (or add a `PATH=` header line at the top of the crontab).

Also use the **venv** interpreter (`/home/orchestrator/venv/bin/python`), not `/usr/bin/python3` -- the system Python lacks `pyyaml` and the `shared/` package imports.

**Server timezone is EST (US/Eastern) -- the working entry:**

```bash
00 23 * * * cd /home/orchestrator && PATH=/snap/bin:/usr/bin:/bin /home/orchestrator/venv/bin/python ops/server_admin.py --log-level minimal backup >> /home/orchestrator/logs/cron/server_backup.log 2>&1
```

**Verify it under cron's stripped environment before trusting the schedule** (safe -- `--dry-run` creates no image but still exercises gcloud auth via the cleanup listing):

```bash
env -i /bin/sh -c 'cd /home/orchestrator && PATH=/snap/bin:/usr/bin:/bin /home/orchestrator/venv/bin/python ops/server_admin.py --log-level minimal backup --dry-run; echo EXIT=$?'
```

The backup command automatically runs cleanup after creating the image, so a single cron entry handles both creation and retention enforcement.

### 5.2 Schedule Summary

| Parameter | Value |
|-----------|-------|
| Frequency | Daily at 10:00 PM EST |
| Retention | 30 days (configurable in YAML) |
| Auto-cleanup | Yes — runs after every backup |
| Log output | `/home/orchestrator/logs/cron/server_backup.log` |

---

## 6. Error Handling & Exit Codes

| Exit Code | Meaning |
|-----------|---------|
| 0 | Success |
| 1 | Error — gcloud command failed, config invalid, or unexpected exception |
| 130 | Operation interrupted by user (Ctrl+C) or restore aborted at confirmation |

In verbose mode (`--log-level verbose`), full Python tracebacks are printed on error. In normal/minimal mode, only the error message is shown.

---

## 7. Prerequisites

- Google Cloud SDK (gcloud) installed and on PATH
- Authenticated via `gcloud auth login` or a service account key
- IAM permissions: `compute.machineImages.create`, `compute.machineImages.delete`, `compute.machineImages.list`, `compute.instances.create`, `compute.instances.delete`
- Python 3.9+ with `pyyaml` package installed
- Static IP resource already reserved in the project (for restore)

---

## 8. Quick Reference Card

| Task | Command |
|------|---------|
| Create backup | `python ops/server_admin.py backup` |
| Preview backup (no changes) | `python ops/server_admin.py backup --dry-run` |
| List available images | `python ops/server_admin.py list` |
| Restore from image | `python ops/server_admin.py restore --image <name>` |
| Clean up expired images | `python ops/server_admin.py cleanup` |
