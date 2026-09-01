#!/usr/bin/env python3
"""
Server Administration CLI
=========================

GCP Compute Engine instance backup/restore via machine images.
Reads configuration from ops/server_admin.yaml.

Usage:
    python ops/server_admin.py backup [--dry-run]
    python ops/server_admin.py restore --image <name> [--force] [--dry-run]
    python ops/server_admin.py cleanup [--dry-run]
    python ops/server_admin.py list [--limit N]
"""

import argparse
import logging
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add project root for shared imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

import yaml
from shared.log_config import configure_logging
from shared.display_utils import print_header, print_section, print_key_value, print_table

# Module-level logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent / "server_admin.yaml"


# =============================================================================
# Configuration
# =============================================================================

def load_config():
    """Load and validate server_admin.yaml."""
    if not CONFIG_PATH.exists():
        logger.error(f"Config not found: {CONFIG_PATH}")
        sys.exit(1)

    with open(CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f)

    errors = []
    if not cfg.get("project_id"):
        errors.append("Missing required field: project_id")
    for section in ("instance", "backup"):
        if not isinstance(cfg.get(section), dict):
            errors.append(f"Missing required section: {section}")
    if isinstance(cfg.get("instance"), dict):
        for field in ("name", "zone", "static_ip_name"):
            if not cfg["instance"].get(field):
                errors.append(f"Missing required field: instance.{field}")
    if isinstance(cfg.get("backup"), dict):
        if not cfg["backup"].get("prefix"):
            errors.append("Missing required field: backup.prefix")
        if not isinstance(cfg["backup"].get("retention_days"), int):
            errors.append("backup.retention_days must be an integer")

    if errors:
        for e in errors:
            logger.error(f"Config validation: {e}")
        sys.exit(1)

    return cfg


# =============================================================================
# Utilities
# =============================================================================

def _find_gcloud():
    """Locate gcloud binary (cross-platform)."""
    gcloud = shutil.which("gcloud")
    if not gcloud:
        logger.error("gcloud CLI not found on PATH. Install the Google Cloud SDK.")
        sys.exit(1)
    return gcloud


def _run(cmd, dry_run=False):
    """Execute a shell command. In dry-run mode, log without executing."""
    display = " ".join(cmd)
    if dry_run:
        logger.info(f"[DRY RUN] Would run: {display}")
        return None
    logger.info(f"Running: {display}")
    return subprocess.run(cmd, check=True)


def _run_capture(cmd):
    """Execute a command and return stdout (always runs, even in dry-run)."""
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def _get_git_sha():
    """Return short git SHA for tagging, or 'nogit' if unavailable."""
    git = shutil.which("git")
    if not git:
        return "nogit"
    try:
        return subprocess.check_output(
            [git, "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL, text=True
        ).strip()
    except Exception:
        return "nogit"


# =============================================================================
# Commands
# =============================================================================

def cmd_backup(args, cfg):
    """Create a machine image of the current instance."""
    gcloud = _find_gcloud()
    prefix = cfg["backup"]["prefix"]
    instance = cfg["instance"]["name"]
    zone = cfg["instance"]["zone"]
    project = cfg["project_id"]

    sha = _get_git_sha()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    image_name = f"{prefix}-{timestamp}-{sha}"

    print_header("Server Backup")
    print_key_value("Instance", instance)
    print_key_value("Zone", zone)
    print_key_value("Image name", image_name)

    _run([
        gcloud, "compute", "machine-images", "create",
        image_name,
        "--project", project,
        f"--source-instance={instance}",
        f"--source-instance-zone={zone}",
    ], dry_run=args.dry_run)

    if not args.dry_run:
        logger.info(f"Machine image created: {image_name}")

    # Auto-cleanup expired images
    print_section("Cleanup expired images")
    _cleanup_expired(cfg, gcloud, args.dry_run)

    return 0


def cmd_cleanup(args, cfg):
    """Remove machine images older than retention_days."""
    gcloud = _find_gcloud()
    print_header("Cleanup Expired Images")
    _cleanup_expired(cfg, gcloud, args.dry_run)
    return 0


def _cleanup_expired(cfg, gcloud, dry_run):
    """Shared cleanup logic: delete images past retention."""
    prefix = cfg["backup"]["prefix"]
    retention = cfg["backup"]["retention_days"]
    project = cfg["project_id"]

    print_key_value("Prefix filter", prefix)
    print_key_value("Retention", f"{retention} days")

    try:
        output = _run_capture([
            gcloud, "compute", "machine-images", "list",
            "--project", project,
            "--filter", f"name~^{prefix}-",
            "--format", "value(name,creationTimestamp)",
        ])
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to list machine images: {e}")
        return

    if not output:
        logger.info("No machine images found matching prefix.")
        return

    now = datetime.now(timezone.utc)
    deleted = 0

    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        name, ts = parts[0], parts[1]
        try:
            created = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            logger.warning(f"Skipping image with unparseable timestamp: {name}")
            continue
        age_days = (now - created).days

        if age_days > retention:
            logger.info(f"Expired ({age_days}d old): {name}")
            _run([
                gcloud, "compute", "machine-images", "delete",
                name,
                "--project", project,
                "--quiet",
            ], dry_run=dry_run)
            deleted += 1

    logger.info(f"Cleanup complete: {deleted} image(s) {'would be ' if dry_run else ''}deleted.")


def cmd_restore(args, cfg):
    """Delete the current instance and recreate from a machine image."""
    gcloud = _find_gcloud()
    instance = cfg["instance"]["name"]
    zone = cfg["instance"]["zone"]
    static_ip = cfg["instance"]["static_ip_name"]
    project = cfg["project_id"]
    image_name = args.image

    print_header("Server Restore")
    print_key_value("Instance", instance)
    print_key_value("Zone", zone)
    print_key_value("Source image", image_name)
    print_key_value("Static IP", static_ip)

    if not args.force and not args.dry_run:
        print()
        print("[WARNING] This will DELETE the running instance and recreate it.")
        response = input("Type YES to continue: ")
        if response != "YES":
            logger.info("Restore aborted by user.")
            return 130

    # Delete existing instance
    print_section("Delete existing instance")
    _run([
        gcloud, "compute", "instances", "delete",
        instance,
        "--project", project,
        f"--zone={zone}",
        "--quiet",
    ], dry_run=args.dry_run)

    # Recreate from image
    print_section("Recreate from machine image")
    _run([
        gcloud, "compute", "instances", "create",
        instance,
        "--project", project,
        f"--zone={zone}",
        f"--source-machine-image={image_name}",
        f"--address={static_ip}",
        "--scopes=https://www.googleapis.com/auth/cloud-platform",
    ], dry_run=args.dry_run)

    if not args.dry_run:
        logger.info(f"Instance {instance} restored from {image_name}.")

    return 0


def cmd_list(args, cfg):
    """List existing machine images for this prefix."""
    gcloud = _find_gcloud()
    prefix = cfg["backup"]["prefix"]
    project = cfg["project_id"]
    limit = args.limit

    print_header("Machine Images")
    print_key_value("Prefix filter", prefix)

    try:
        output = _run_capture([
            gcloud, "compute", "machine-images", "list",
            "--project", project,
            "--filter", f"name~^{prefix}-",
            "--sort-by", "~creationTimestamp",
            "--limit", str(limit),
            "--format", "value(name,creationTimestamp,status)",
        ])
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to list machine images: {e}")
        return 1

    if not output:
        logger.info("No machine images found.")
        return 0

    rows = []
    now = datetime.now(timezone.utc)
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        name = parts[0]
        ts = parts[1] if len(parts) > 1 else "?"
        status = parts[2] if len(parts) > 2 else "?"
        try:
            created = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            age = f"{(now - created).days}d"
        except ValueError:
            age = "?"
        rows.append([name, ts[:19], age, status])

    print_table(["Name", "Created", "Age", "Status"], rows)
    return 0


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="GCP Compute Engine server backup/restore via machine images"
    )
    parser.add_argument(
        "--log-level",
        choices=["minimal", "normal", "verbose"],
        default="normal",
        help="Logging verbosity (default: normal)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # backup
    backup_parser = subparsers.add_parser("backup", help="Create a machine image backup")
    backup_parser.add_argument("--dry-run", action="store_true", help="Show what would happen without executing")
    backup_parser.set_defaults(func=cmd_backup)

    # cleanup
    cleanup_parser = subparsers.add_parser("cleanup", help="Remove expired machine images")
    cleanup_parser.add_argument("--dry-run", action="store_true", help="Show what would happen without executing")
    cleanup_parser.set_defaults(func=cmd_cleanup)

    # restore
    restore_parser = subparsers.add_parser("restore", help="Restore instance from a machine image")
    restore_parser.add_argument("--image", required=True, help="Machine image name to restore from")
    restore_parser.add_argument("--force", action="store_true", help="Skip interactive confirmation prompt")
    restore_parser.add_argument("--dry-run", action="store_true", help="Show what would happen without executing")
    restore_parser.set_defaults(func=cmd_restore)

    # list
    list_parser = subparsers.add_parser("list", help="List existing machine images")
    list_parser.add_argument("--limit", type=int, default=20, help="Maximum images to display (default: 20)")
    list_parser.set_defaults(func=cmd_list)

    args = parser.parse_args()

    # Configure logging (shared with all orchestrator tools)
    configure_logging(level=args.log_level)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    # Load config once
    cfg = load_config()

    try:
        return args.func(args, cfg)
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user.")
        return 130
    except subprocess.CalledProcessError as e:
        logger.error(f"gcloud command failed (exit {e.returncode}): {e.cmd}")
        return 1
    except Exception as e:
        logger.error(f"Command failed: {e}")
        if args.log_level == "verbose":
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
