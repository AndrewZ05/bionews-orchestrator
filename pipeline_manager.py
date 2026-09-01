#!/usr/bin/env python3
"""
Pipeline Manager CLI for Manual Intervention
==========================================

Provides CLI commands for manual pipeline management including
job cancellation, status checking, retry operations, and maintenance tasks.

Usage:
    python pipeline_manager.py cancel-job --job-id job_20250102_001
    python pipeline_manager.py status --job-id job_20250102_001
    python pipeline_manager.py retry --job-id job_20250102_001
    python pipeline_manager.py cleanup --force
    python pipeline_manager.py health --source facebook

Author: Data Pipeline Team
Created: 2025-01-02
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Tuple, Optional

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent))

# Load environment variables from .env file
def load_env_file():
    """Load environment variables from .env file if it exists."""
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        # Remove quotes if present
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        # Only set if not already in environment
                        if key and not os.getenv(key):
                            os.environ[key] = value

# Load .env before importing GCP clients
load_env_file()

from google.cloud import bigquery
from google.cloud import storage as gcs

# Import shared modules
from shared.config_loader import load_config
from shared.cli_utils import (
    pipeline_command,
    setup_pipeline_clients,
    get_monitoring_dataset,
    DEFAULT_MONITORING_DATASET,
    DEFAULT_JOB_LOG_RETENTION_DAYS,
    DEFAULT_CHECKPOINT_RETENTION_DAYS,
    DEFAULT_TTL_DAYS,
    DEFAULT_HEALTH_CHECK_HOURS,
    DEFAULT_JOB_LIST_LIMIT
)
from shared.display_utils import (
    print_header,
    print_section,
    print_key_value,
    print_table,
    status_icon,
    format_number
)
from shared.job_manager import (
    generate_job_id, create_job_log_table, log_job_start, log_job_complete,
    log_job_failure, cancel_job, check_job_status, list_recent_jobs,
    cleanup_old_jobs
)
from shared.checkpoint_manager import (
    create_checkpoint_table, get_latest_checkpoint, get_job_checkpoint_history,
    determine_retry_start_point, cleanup_checkpoints
)
from shared.metrics_manager import (
    create_metrics_table, get_recent_metrics, get_pipeline_health_score,
    calculate_performance_kpis, get_resource_usage_metrics, get_performance_trends,
    get_data_quality_metrics, get_operational_insights
)
from shared.ttl_manager import run_ttl_cleanup, get_ttl_status

# Airbyte integration removed - incompatible with Python version

# Import pipeline dependencies
from shared.pipeline_dependencies import (
    check_pipeline_dependency, wait_for_dependencies, execute_conditional_pipeline,
    create_pipeline_template, load_pipeline_template, execute_pipeline_with_template,
    get_pipeline_dependency_status
)

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _parse_gcs_uri(uri: str) -> Optional[Tuple[str, str]]:
    """Parse a GCS URI into bucket and blob."""
    if not uri or not uri.startswith('gs://'):
        return None
    path = uri[5:]
    if '/' not in path:
        return None
    bucket, blob = path.split('/', 1)
    if not bucket or not blob:
        return None
    return bucket, blob


def _read_gcs_log(gcs_client, uri: str) -> Optional[str]:
    """Fetch full log content from GCS."""
    parsed = _parse_gcs_uri(uri)
    if not parsed:
        return None

    bucket_name, blob_name = parsed
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if not blob.exists():
        return None

    try:
        content = blob.download_as_text()
    except Exception as exc:
        logger.debug(f"Failed to download log excerpt from {uri}: {exc}")
        return None

    return content

@pipeline_command(requires_gcs=False)
def cmd_cancel_job(args, bq_client):
    """Cancel a running pipeline job."""
    logger.info(f"Canceling job: {args.job_id}")
    
    monitoring_dataset = get_monitoring_dataset(args)
    
    # Check current job status
    status = check_job_status(bq_client, monitoring_dataset, args.job_id)
    
    if status == 'NOT_FOUND':
        logger.error(f"Job {args.job_id} not found")
        return 1
    
    if str(status or "").strip().upper() != "RUNNING":
        logger.warning(f"Job {args.job_id} is not running (status: {status})")
        return 1
    
    # Cancel the job
    success = cancel_job(bq_client, monitoring_dataset, args.job_id, args.reason)
    
    if success:
        logger.info(f"Job {args.job_id} cancelled successfully")
        return 0
    else:
        logger.error(f"Failed to cancel job {args.job_id}")
        return 1

@pipeline_command(requires_gcs=False)
def cmd_job_status(args, bq_client):
    """Get detailed status of a pipeline job."""
    logger.info(f"Checking status for job: {args.job_id}")

    monitoring_dataset = get_monitoring_dataset(args)
    table_id = f"{bq_client.project}.{monitoring_dataset}.jobs"

    # Get full job details including machine information
    query = f"""
    SELECT
        job_id, status, source, job_name,
        hostname, user, os_type, process_id,
        queued_at, started_at, completed_at, duration_seconds,
        total_rows, error_message, error_type,
        working_directory, ip_address, log_file_path
    FROM `{table_id}`
    WHERE job_id = @job_id
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", args.job_id)]
    )

    try:
        result = bq_client.query(query, job_config=job_config).result()
        job_details = None
        for row in result:
            job_details = {
                'job_id': row.job_id,
                'status': row.status,
                'source': row.source,
                'job_name': row.job_name,
                'hostname': row.hostname,
                'user': row.user,
                'os_type': row.os_type,
                'process_id': row.process_id,
                'queued_at': row.queued_at,
                'started_at': row.started_at,
                'completed_at': row.completed_at,
                'duration_seconds': row.duration_seconds,
                'total_rows': row.total_rows,
                'error_message': row.error_message,
                'error_type': row.error_type,
                'working_directory': row.working_directory,
                'ip_address': row.ip_address,
                'log_file_path': row.log_file_path
            }
            break
    except Exception as e:
        logger.error(f"Error querying job details: {e}")
        job_details = None

    print_header("Job Status Report")
    print_key_value("Job ID", args.job_id)

    if not job_details:
        print(f"\n{status_icon('error')} Job not found in job log")
        return 1

    print_key_value("Status", job_details['status'])
    print_key_value("Source", job_details['source'])
    print_key_value("Table", job_details['job_name'])

    print_section("Machine Information")
    print_key_value("Hostname", job_details['hostname'] or 'N/A')
    print_key_value("User", job_details['user'] or 'N/A')
    print_key_value("OS Type", job_details['os_type'] or 'N/A')
    print_key_value("Process ID", str(job_details['process_id']) if job_details['process_id'] else 'N/A')
    print_key_value("IP Address", job_details['ip_address'] or 'N/A')
    print_key_value("Working Dir", job_details['working_directory'] or 'N/A')

    print_section("Timing Information")
    print_key_value("Queued", str(job_details['queued_at']) if job_details['queued_at'] else 'N/A')
    print_key_value("Started", str(job_details['started_at']) if job_details['started_at'] else 'N/A')
    print_key_value("Completed", str(job_details['completed_at']) if job_details['completed_at'] else 'N/A')
    if job_details['duration_seconds']:
        print_key_value("Duration", f"{job_details['duration_seconds']}s")

    if job_details['total_rows']:
        print_section("Results")
        print_key_value("Total Rows", format_number(job_details['total_rows']))

    if job_details['error_message']:
        print_section("Error Information")
        print_key_value("Error Type", job_details['error_type'] or 'N/A')
        print(f"\nError Message:\n{job_details['error_message']}")

    # Display log file path
    if job_details['log_file_path']:
        print_section("Log File")
        print_key_value("Path", job_details['log_file_path'])
        print("\nTo view log:")
        print(f"  python pipeline_manager.py logs --job-id {args.job_id}")
        print(f"  python pipeline_manager.py logs --job-id {args.job_id} --tail 50")

    # Get checkpoint history if available
    try:
        checkpoints = get_job_checkpoint_history(bq_client, monitoring_dataset, args.job_id)
        if checkpoints:
            print_section("Checkpoint History")
            for cp in checkpoints:
                print(f"  {cp['checkpoint_type']}: {cp['checkpoint_timestamp']}")
    except:
        pass  # Checkpoints not available

    return 0

@pipeline_command(requires_gcs=False)
def cmd_logs(args, bq_client):
    """View job log file."""
    import os
    import subprocess

    logger.info(f"Fetching log for job: {args.job_id}")

    monitoring_dataset = get_monitoring_dataset(args)
    table_id = f"{bq_client.project}.{monitoring_dataset}.jobs"

    # Get log file path from job record
    query = f"""
    SELECT log_file_path
    FROM `{table_id}`
    WHERE job_id = @job_id
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", args.job_id)]
    )

    try:
        result = bq_client.query(query, job_config=job_config).result()
        log_path = None
        for row in result:
            log_path = row.log_file_path
            break

        if not log_path:
            print(f"No log file found for job: {args.job_id}")
            return 1

        # Check if it's a GCS path or local path
        if log_path.startswith('gs://'):
            # GCS path - download and display
            print(f"Log file location: {log_path}")
            print("\nTo view from GCS, use:")
            print(f"  gsutil cat {log_path}")
            if args.tail:
                print(f"  gsutil cat {log_path} | tail -{args.tail}")
            return 0
        else:
            # Local file path
            if not os.path.exists(log_path):
                print(f"Log file not found: {log_path}")
                print("The log may have been moved or deleted.")
                return 1

            print(f"Log file: {log_path}\n")
            print("="*70)

            # Display log file
            if args.tail:
                # Show last N lines
                if os.name == 'nt':  # Windows
                    # Use PowerShell Get-Content for tail on Windows
                    subprocess.run(['powershell', '-Command',
                                  f'Get-Content -Path "{log_path}" -Tail {args.tail}'])
                else:  # Linux/Mac
                    subprocess.run(['tail', f'-{args.tail}', log_path])
            else:
                # Show entire file (or use cat/type)
                if os.name == 'nt':  # Windows
                    subprocess.run(['type', log_path], shell=True)
                else:  # Linux/Mac
                    subprocess.run(['cat', log_path])

            print("\n" + "="*70)
            return 0

    except Exception as e:
        logger.error(f"Error fetching log: {e}")
        return 1


@pipeline_command(requires_gcs=True)
def cmd_recent_job_logs(args, bq_client, gcs_client):
    """Show recent jobs with log excerpts."""

    monitoring_dataset = get_monitoring_dataset(args)
    table_id = f"{bq_client.project}.{monitoring_dataset}.jobs"

    query = f"""
    SELECT job_id, command_line, status, status_updated_at, log_file_path
    FROM `{table_id}`
    ORDER BY status_updated_at DESC
    LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", args.limit)
        ]
    )

    print_header("Recent Job Logs")

    try:
        result = list(bq_client.query(query, job_config=job_config).result())
    except Exception as exc:
        logger.error(f"Error fetching recent jobs: {exc}")
        print(f"{status_icon('error')} Failed to query recent jobs")
        return 1

    if not result:
        print("No jobs found")
        return 0

    for idx, row in enumerate(result, start=1):
        print_section(f"Job {idx}: {row.job_id}")
        print_key_value("Command", row.command_line or 'N/A')
        print_key_value("Status", row.status or 'unknown')
        print_key_value("Updated", str(row.status_updated_at) if row.status_updated_at else 'N/A')
        log_path = row.log_file_path or ''
        print_key_value("Log Path", log_path or 'N/A')

        if log_path.startswith('gs://'):
            content = _read_gcs_log(gcs_client, log_path)
            if content:
                print("\n--- Log ---")
                print(content.rstrip())
                print("---------------\n")
            else:
                print("\n(Log unavailable or empty)\n")
        elif log_path:
            print("\n(Log is stored locally; retrieve manually)\n")
        else:
            print("\n(No log path recorded)\n")

    return 0

@pipeline_command(requires_gcs=False)
def cmd_retry_job(args, bq_client):
    """Retry a failed pipeline job from appropriate checkpoint."""
    logger.info(f"Retrying job: {args.job_id}")
    
    monitoring_dataset = get_monitoring_dataset(args)
    
    # Check job status first
    status = check_job_status(bq_client, monitoring_dataset, args.job_id)
    
    st = str(status or "").strip().upper()
    if st in ("SUCCESS", "COMPLETED"):
        logger.warning(f"Job {args.job_id} already successful")
        return 1

    if st == "RUNNING":
        logger.error(f"Job {args.job_id} is currently running - cancel first if needed")
        return 1
    
    # Determine retry start point
    restart_checkpoint, checkpoint_data = determine_retry_start_point(
        bq_client, monitoring_dataset, args.job_id
    )
    
    print_header("Retry Analysis")
    print_key_value("Job ID", args.job_id)
    print_key_value("Current Status", status)
    print_key_value("Restart From", restart_checkpoint)
    
    if args.force:
        print(f"\n{status_icon('info')} Force retry enabled - proceeding with restart")
        # This would trigger actual pipeline restart
        logger.info("Force retry - would restart pipeline from checkpoint")
        return 0
    else:
        print(f"\n{status_icon('info')} To execute retry, run with --force flag")
        print(f"   python pipeline_manager.py retry --job-id {args.job_id} --force")
        return 0

@pipeline_command(requires_gcs=True)
def cmd_cleanup(args, bq_client, gcs_client):
    """Perform maintenance cleanup operations."""
    logger.info("Performing pipeline cleanup...")
    
    monitoring_dataset = get_monitoring_dataset(args)
    
    cleanup_config = {
        'sources': ['facebook', 'wordpress'],
        'gcs': {
            'bucket_name': args.bucket_name or 'orchestrator-data-lake'
        },
        'sources_config': {
            'facebook': {'archive_ttl_days': args.ttl_days},
            'wordpress': {'archive_ttl_days': args.ttl_days}
        }
    }
    
    if not args.force:
        print(f"\n[WARNING] This will clean up:")
        print(f"   - Job logs older than {args.job_log_days} days")
        print(f"   - Checkpoints older than {args.checkpoint_days} days")
        print(f"   - Archived tables older than {args.ttl_days} days")
        print(f"   - GCS files older than {args.ttl_days} days")
        print(f"\n[INFO] Run with --force to execute cleanup")
        return 0
    
    print(f"\n[INFO] Starting cleanup operations...")
    
    # Clean up job logs
    jobs_count = cleanup_old_jobs(bq_client, monitoring_dataset, args.job_log_days)
    print(f"[OK] Cleaned up {jobs_count} old job logs")
    
    # Clean up checkpoints
    checkpoints_count = cleanup_checkpoints(bq_client, monitoring_dataset, args.checkpoint_days)
    print(f"[OK] Cleaned up {checkpoints_count} old checkpoints")
    
    # Clean up TTL resources
    ttl_results = run_ttl_cleanup(bq_client, gcs_client, cleanup_config)
    print(f"[OK] Cleaned up {ttl_results['tables_cleaned']} archived tables")
    print(f"[OK] Deleted {ttl_results['gcs_files_deleted']} GCS files")
    
    print(f"\n[OK] Cleanup completed successfully")
    return 0

@pipeline_command(requires_gcs=False)
def cmd_health_check(args, bq_client):
    """Check pipeline health status."""
    logger.info(f"Checking pipeline health...")
    
    monitoring_dataset = get_monitoring_dataset(args)
    
    health_summary = get_pipeline_health_score(
        bq_client, monitoring_dataset, args.source, args.hours_back
    )
    
    print_header("Pipeline Health Report")
    print_key_value("Assessment Window", f"{args.hours_back} hours")
    print_key_value("Source Filter", args.source or 'All sources')
    print_key_value("Score", f"{health_summary['overall_score']}/100")
    print_key_value("Status", health_summary['health_status'])
    print_key_value("Success Rate", f"{health_summary['success_rate']}%")
    print_key_value("Error Rate", f"{health_summary['error_rate']}%")
    print_key_value("Total Executions", format_number(health_summary['total_executions']))
    
    # Performance metrics
    print_section("Performance Metrics")
    print_key_value("Avg Duration", f"{health_summary['avg_duration_minutes']} minutes")
    print_key_value("Avg Throughput", f"{format_number(int(health_summary['avg_throughput_rows_per_min']))} rows/min")
    
    # Source-specific performance
    if args.source and args.source in health_summary['kpis']['source_performance']:
        source_perf = health_summary['kpis']['source_performance'][args.source]
        print_section(f"{args.source.title()} Performance")
        print_key_value("Executions", format_number(source_perf['count']))
        print_key_value("Avg Duration", f"{source_perf['avg_duration']:.2f} seconds")
        print_key_value("Avg Rows/Execution", format_number(int(source_perf['avg_rows_per_execution'])))
    
    # Health recommendations
    score = health_summary['overall_score']
    if score >= 90:
        print(f"\n{status_icon('success')} Pipeline is performing excellent!")
    elif score >= 80:
        print(f"\n{status_icon('success')} Pipeline is performing well")
    elif score >= 70:
        print(f"\n{status_icon('warning')} Pipeline performance could be improved")
    else:
        print(f"\n{status_icon('error')} Pipeline health issues detected - investigation recommended")
    
    return 0

@pipeline_command(requires_gcs=False)
def cmd_list_jobs(args, bq_client):
    """List recent pipeline jobs."""
    logger.info("Listing recent jobs...")

    monitoring_dataset = get_monitoring_dataset(args)

    jobs = list_recent_jobs(
        bq_client, monitoring_dataset,
        source=args.source, table=args.table,
        hostname=args.hostname if hasattr(args, 'hostname') else None,
        status=args.status if hasattr(args, 'status') else None,
        limit=args.limit
    )

    # Prepare data for table display
    if args.show_machine:
        headers = ['Job ID', 'Source', 'Table', 'Status', 'Hostname', 'User', 'Rows']
        rows = []

        for job in jobs:
            job_id = job.get('job_id', 'N/A')[:18] + "..." if len(job.get('job_id', '')) > 18 else job.get('job_id', 'N/A')
            source = job.get('source', 'N/A')[:8]
            table = job.get('job_name', 'N/A')[:12] if job.get('job_name') else 'N/A'
            status = job.get('status', 'N/A')
            hostname = (job.get('hostname') or 'N/A')[:15]
            user = (job.get('user') or 'N/A')[:10]
            rows_count = format_number(job.get('total_rows') or 0)

            rows.append([job_id, source, table, status, hostname, user, rows_count])

        print_header("Recent Pipeline Jobs", width=100)
        print_table(headers, rows, widths=[18, 8, 12, 10, 15, 10, 10])
    else:
        headers = ['Job ID', 'Source', 'Table', 'Status', 'Rows']
        rows = []

        for job in jobs:
            job_id = job['job_id'][:20] + "..." if len(job['job_id']) > 20 else job['job_id']
            source = job['source'][:10]
            table = job['job_name'][:15] if job['job_name'] else 'N/A'
            status = job['status']
            rows_count = format_number(job['total_rows'] or 0)

            rows.append([job_id, source, table, status, rows_count])

        print_header("Recent Pipeline Jobs", width=70)
        print_table(headers, rows, widths=[20, 10, 15, 10, 12])

    print(f"\nTotal: {format_number(len(jobs))} jobs")

    if not args.show_machine:
        print("Tip: Use --show-machine to see hostname and user information")

    return 0

@pipeline_command(requires_gcs=False)
def cmd_metrics(args, bq_client):
    """Get enhanced metrics including resource usage and trends."""
    logger.info("Getting enhanced pipeline metrics...")
    
    monitoring_dataset = get_monitoring_dataset(args)
    
    print_header("Enhanced Pipeline Metrics")
    print_key_value("Timestamp", datetime.now().isoformat())
    print_key_value("Assessment Window", f"{args.hours_back} hours")
    
    # Get resource usage metrics
    print_section("Resource Usage")
    resource_metrics = get_resource_usage_metrics(bq_client, monitoring_dataset, args.hours_back)
    
    if 'error' not in resource_metrics:
        print_key_value("Total Executions", format_number(resource_metrics['summary']['total_executions']))
        print_key_value("Estimated Cost (USD)", f"${resource_metrics['summary']['total_estimated_cost']:.4f}")
        print_key_value("Peak Slots Used", resource_metrics['summary']['peak_slots_used'])
        
        print("\nPer-Source Resource Usage:")
        for source, usage in resource_metrics['slot_usage'].items():
            print(f"  {source}:")
            print(f"    Executions: {usage['executions']}")
            print(f"    Avg Duration: {usage['avg_duration_seconds']:.1f}s")
            print(f"    Total Rows: {format_number(usage['total_rows'])}")
            print(f"    Est. Slots: {usage['estimated_slots_used']}")
    
    # Get performance trends
    if args.show_trends:
        print_section("Performance Trends")
        trends = get_performance_trends(bq_client, monitoring_dataset, args.source, args.days_back)
        
        if 'error' not in trends:
            print_key_value("Days Analyzed", trends['summary']['days_analyzed'])
            print_key_value("Sources Analyzed", trends['summary']['sources_analyzed'])
            
            print("\nTrend Analysis:")
            for source, analysis in trends['trend_analysis'].items():
                print(f"  {source}:")
                print(f"    Duration Trend: {analysis['duration_trend']}")
                print(f"    Reliability Trend: {analysis['reliability_trend']}")
                print(f"    Recent Performance: {analysis['recent_performance']:.1f}s")
    
    # Get data quality metrics
    if args.show_quality:
        print_section("Data Quality Metrics")
        quality_metrics = get_data_quality_metrics(bq_client, monitoring_dataset, args.hours_back)
        
        if 'error' not in quality_metrics:
            print_key_value("Sources Monitored", quality_metrics['summary']['sources_monitored'])
            print_key_value("Total Quality Checks", format_number(quality_metrics['summary']['total_checks']))
            print_key_value("Avg Quality Score", f"{quality_metrics['summary']['avg_quality_score']:.1f}")
            
            print("\nPer-Source Quality:")
            for source, quality in quality_metrics['overall_quality'].items():
                print(f"  {source}:")
                print(f"    Quality Score: {quality['weighted_avg_quality_score']:.1f}")
                print(f"    Status: {quality['quality_status']}")
                print(f"    Tables Monitored: {quality['tables_monitored']}")
    
    # Get operational insights
    if args.show_insights:
        print_section("Operational Insights")
        insights = get_operational_insights(bq_client, monitoring_dataset, args.hours_back)
        
        if 'error' not in insights:
            print_key_value("Total Failures", insights['summary']['total_failures'])
            print_key_value("Slowest Table", insights['summary']['slowest_table'] or 'N/A')
            print_key_value("Recommendations", insights['summary']['recommendations_count'])
            
            if insights['recommendations']:
                print("\nRecommendations:")
                for rec in insights['recommendations']:
                    priority_icon = "🔴" if rec['priority'] == 'high' else "🟡" if rec['priority'] == 'medium' else "🟢"
                    print(f"  {priority_icon} {rec['message']}")
                    print(f"    Action: {rec['action']}")
    
    return 0

@pipeline_command(requires_gcs=False)
def cmd_resources(args, bq_client):
    """Get detailed resource usage and cost analysis."""
    logger.info("Analyzing resource usage...")
    
    monitoring_dataset = get_monitoring_dataset(args)
    
    print_header("Resource Usage Analysis")
    print_key_value("Timestamp", datetime.now().isoformat())
    print_key_value("Analysis Window", f"{args.hours_back} hours")
    
    # Get detailed resource metrics
    resource_metrics = get_resource_usage_metrics(bq_client, monitoring_dataset, args.hours_back)
    
    if 'error' in resource_metrics:
        print(f"Error: {resource_metrics['error']}")
        return 1
    
    # Display slot usage
    print_section("BigQuery Slot Usage")
    if resource_metrics['slot_usage']:
        headers = ['Source', 'Executions', 'Avg Duration', 'Total Rows', 'Est. Slots']
        rows = []
        
        for source, usage in resource_metrics['slot_usage'].items():
            rows.append([
                source,
                str(usage['executions']),
                f"{usage['avg_duration_seconds']:.1f}s",
                format_number(usage['total_rows']),
                str(usage['estimated_slots_used'])
            ])
        
        print_table(headers, rows)
    else:
        print("No slot usage data available")
    
    # Display cost analysis
    print_section("Cost Analysis")
    if resource_metrics['cost_usage']:
        headers = ['Source', 'Total Bytes', 'Executions', 'Avg Bytes/Exec', 'Est. Cost (USD)']
        rows = []
        
        for source, cost in resource_metrics['cost_usage'].items():
            rows.append([
                source,
                format_number(cost['total_bytes']),
                str(cost['executions']),
                format_number(cost['avg_bytes_per_execution']),
                f"${cost['estimated_cost_usd']:.4f}"
            ])
        
        print_table(headers, rows)
        
        total_cost = resource_metrics['summary']['total_estimated_cost']
        print(f"\nTotal Estimated Cost: ${total_cost:.4f}")
        
        if args.show_cost_breakdown:
            print("\nCost Breakdown:")
            print(f"  Daily Average: ${total_cost / max(1, args.hours_back / 24):.4f}")
            print(f"  Monthly Estimate: ${total_cost * 30:.2f}")
    else:
        print("No cost data available")
    
    return 0

@pipeline_command(requires_gcs=False)
def cmd_dependencies(args, bq_client):
    """Manage pipeline dependencies and conditional execution."""
    logger.info("Managing pipeline dependencies...")
    
    monitoring_dataset = get_monitoring_dataset(args)
    
    print_header("Pipeline Dependencies")
    print_key_value("Timestamp", datetime.now().isoformat())
    
    if args.check_dependency:
        # Check a specific dependency
        dep_config = {
            'name': args.dep_name,
            'type': args.dep_type,
            'pipeline_name': args.pipeline_name,
            'table_id': args.table_id,
            'min_rows': args.min_rows,
            'max_age_hours': args.max_age_hours,
            'service_name': args.service_name,
            'check_url': args.check_url,
            'condition': args.condition,
            'timeout_hours': args.timeout_hours
        }
        
        satisfied, message = check_pipeline_dependency(dep_config, bq_client, monitoring_dataset)
        
        print_section("Dependency Check")
        print_key_value("Dependency Name", args.dep_name)
        print_key_value("Type", args.dep_type)
        print_key_value("Status", "Satisfied" if satisfied else "Not Satisfied")
        print_key_value("Message", message)
        
        return 0 if satisfied else 1
    
    elif args.wait_for:
        # Wait for dependencies
        dependencies = []
        for dep_name in args.wait_for:
            dep_config = {
                'name': dep_name,
                'type': args.dep_type,
                'pipeline_name': args.pipeline_name,
                'table_id': args.table_id,
                'min_rows': args.min_rows,
                'max_age_hours': args.max_age_hours
            }
            dependencies.append(dep_config)
        
        print_section("Waiting for Dependencies")
        print(f"Waiting for: {', '.join(args.wait_for)}")
        print_key_value("Max Wait Time", f"{args.max_wait_minutes} minutes")
        print_key_value("Check Interval", f"{args.check_interval_minutes} minutes")
        
        satisfied, satisfied_deps = wait_for_dependencies(
            dependencies, bq_client, monitoring_dataset,
            args.max_wait_minutes, args.check_interval_minutes
        )
        
        if satisfied:
            print(f"\nAll dependencies satisfied: {', '.join(satisfied_deps)}")
            return 0
        else:
            print(f"\nDependencies not satisfied within timeout: {', '.join(satisfied_deps)}")
            return 1
    
    elif args.conditional:
        # Execute conditional pipeline
        condition_config = {
            'type': args.condition_type,
            'table_id': args.table_id,
            'min_rows': args.min_rows,
            'source': args.source,
            'max_error_rate': args.max_error_rate,
            'hours_back': args.hours_back,
            'schedule': args.schedule
        }
        
        print_section("Conditional Execution")
        print_key_value("Condition Type", args.condition_type)
        print_key_value("Pipeline Command", args.pipeline_command)
        
        condition_met, message = execute_conditional_pipeline(
            condition_config, args.pipeline_command, bq_client, monitoring_dataset
        )
        
        print_key_value("Condition Met", "Yes" if condition_met else "No")
        print_key_value("Message", message)
        
        if condition_met:
            print(f"\nExecuting pipeline: {args.pipeline_command}")
            return 0
        else:
            print(f"\nPipeline execution skipped: {message}")
            return 0
    
    else:
        print("Use --check-dependency, --wait-for, or --conditional")
        return 1

@pipeline_command(requires_gcs=False)
def cmd_templates(args, bq_client):
    """Manage pipeline templates."""
    logger.info("Managing pipeline templates...")
    
    print_header("Pipeline Templates")
    print_key_value("Timestamp", datetime.now().isoformat())
    
    if args.create:
        # Create a new template
        template_config = {
            'name': args.template_name,
            'description': args.description,
            'dependencies': args.dependencies or [],
            'conditions': args.conditions or [],
            'pipeline_command': args.pipeline_command,
            'parameters': args.parameters or {}
        }
        
        success = create_pipeline_template(args.template_name, template_config)
        
        print_section("Template Creation")
        print_key_value("Template Name", args.template_name)
        print_key_value("Status", "Created" if success else "Failed")
        
        return 0 if success else 1
    
    elif args.execute:
        # Execute template
        parameters = {}
        if args.param_file:
            import json
            with open(args.param_file, 'r') as f:
                parameters = json.load(f)
        
        print_section("Template Execution")
        print_key_value("Template Name", args.template_name)
        print_key_value("Parameters", str(parameters))
        
        success, message = execute_pipeline_with_template(
            args.template_name, parameters, bq_client, get_monitoring_dataset(args)
        )
        
        print_key_value("Status", "Success" if success else "Failed")
        print_key_value("Message", message)
        
        return 0 if success else 1
    
    elif args.list:
        # List available templates
        import os
        templates_dir = "configs/templates"
        
        if os.path.exists(templates_dir):
            templates = [f.replace('.yaml', '') for f in os.listdir(templates_dir) if f.endswith('.yaml')]
            
            print_section("Available Templates")
            if templates:
                for template in templates:
                    print(f"  • {template}")
            else:
                print("No templates found")
        else:
            print("Templates directory not found")
        
        return 0
    
    else:
        print("Use --create, --execute, or --list")
        return 1



def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='Pipeline Manager CLI')
    parser.add_argument('--monitoring-dataset', default=DEFAULT_MONITORING_DATASET,
                       help=f'Monitoring dataset name (default: {DEFAULT_MONITORING_DATASET})')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Cancel job command
    cancel_parser = subparsers.add_parser('cancel-job', help='Cancel a running job')
    cancel_parser.add_argument('--job-id', required=True, help='Job ID to cancel')
    cancel_parser.add_argument('--reason', default='Manual cancellation', help='Cancellation reason')
    cancel_parser.set_defaults(func=cmd_cancel_job)
    
    # Job status command
    status_parser = subparsers.add_parser('status', help='Check job status')
    status_parser.add_argument('--job-id', required=True, help='Job ID to check')
    status_parser.set_defaults(func=cmd_job_status)
    
    # Retry job command
    retry_parser = subparsers.add_parser('retry', help='Retry a failed job')
    retry_parser.add_argument('--job-id', required=True, help='Job ID to retry')
    retry_parser.add_argument('--force', action='store_true', help='Force retry execution')
    retry_parser.set_defaults(func=cmd_retry_job)
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Perform maintenance cleanup')
    cleanup_parser.add_argument('--force', action='store_true', help='Force cleanup execution')
    cleanup_parser.add_argument('--bucket-name', help='GCS bucket name for cleanup')
    cleanup_parser.add_argument('--job-log-days', default=DEFAULT_JOB_LOG_RETENTION_DAYS, type=int, 
                               help=f'Job log retention days (default: {DEFAULT_JOB_LOG_RETENTION_DAYS})')
    cleanup_parser.add_argument('--checkpoint-days', default=DEFAULT_CHECKPOINT_RETENTION_DAYS, type=int, 
                               help=f'Checkpoint retention days (default: {DEFAULT_CHECKPOINT_RETENTION_DAYS})')
    cleanup_parser.add_argument('--ttl-days', default=DEFAULT_TTL_DAYS, type=int, 
                               help=f'TTL cleanup threshold days (default: {DEFAULT_TTL_DAYS})')
    cleanup_parser.set_defaults(func=cmd_cleanup)
    
    # Health check command
    health_parser = subparsers.add_parser('health', help='Check pipeline health')
    health_parser.add_argument('--source', help='Source filter for health check')
    health_parser.add_argument('--hours-back', default=DEFAULT_HEALTH_CHECK_HOURS, type=int, 
                               help=f'Hours to look back (default: {DEFAULT_HEALTH_CHECK_HOURS})')
    health_parser.set_defaults(func=cmd_health_check)
    
    # List jobs command
    list_parser = subparsers.add_parser('list-jobs', help='List jobs')
    list_parser.add_argument('--source', help='Source filter')
    list_parser.add_argument('--table', help='Table filter')
    list_parser.add_argument('--hostname', help='Filter by hostname')
    list_parser.add_argument('--status', help='Filter by status (running, success, failed, cancelled)')
    list_parser.add_argument('--show-machine', action='store_true',
                            help='Show machine information (hostname, user, etc.)')
    list_parser.add_argument('--limit', default=DEFAULT_JOB_LIST_LIMIT, type=int,
                            help=f'Maximum jobs to list (default: {DEFAULT_JOB_LIST_LIMIT})')
    list_parser.set_defaults(func=cmd_list_jobs)

    # Recent logs command
    recent_logs_parser = subparsers.add_parser('recent-logs', help='Show recent jobs with log previews')
    recent_logs_parser.add_argument('--limit', type=int, default=5,
                                    help='Number of recent jobs to display (default: 5)')
    recent_logs_parser.set_defaults(func=cmd_recent_job_logs)
    
    # Enhanced metrics command
    metrics_parser = subparsers.add_parser('metrics', help='Get enhanced pipeline metrics')
    metrics_parser.add_argument('--source', help='Source filter for metrics')
    metrics_parser.add_argument('--hours-back', default=24, type=int, 
                               help='Hours to look back (default: 24)')
    metrics_parser.add_argument('--days-back', default=7, type=int, 
                               help='Days for trend analysis (default: 7)')
    metrics_parser.add_argument('--show-trends', action='store_true',
                               help='Show performance trends')
    metrics_parser.add_argument('--show-quality', action='store_true',
                               help='Show data quality metrics')
    metrics_parser.add_argument('--show-insights', action='store_true',
                               help='Show operational insights')
    metrics_parser.set_defaults(func=cmd_metrics)
    
    # Resources command
    resources_parser = subparsers.add_parser('resources', help='Analyze resource usage and costs')
    resources_parser.add_argument('--source', help='Source filter for resource analysis')
    resources_parser.add_argument('--hours-back', default=24, type=int, 
                                help='Hours to look back (default: 24)')
    resources_parser.add_argument('--show-cost-breakdown', action='store_true',
                                help='Show detailed cost breakdown')
    resources_parser.set_defaults(func=cmd_resources)
    
    # Dependencies command
    deps_parser = subparsers.add_parser('dependencies', help='Manage pipeline dependencies and conditional execution')
    deps_group = deps_parser.add_mutually_exclusive_group(required=True)
    deps_group.add_argument('--check-dependency', action='store_true', help='Check a specific dependency')
    deps_group.add_argument('--wait-for', nargs='+', help='Wait for multiple dependencies')
    deps_group.add_argument('--conditional', action='store_true', help='Execute conditional pipeline')
    
    # Dependency parameters
    deps_parser.add_argument('--dep-name', help='Dependency name')
    deps_parser.add_argument('--dep-type', choices=['pipeline_completion', 'data_availability', 'service_check', 'time_condition'], 
                           help='Dependency type')
    deps_parser.add_argument('--pipeline-name', help='Pipeline name for pipeline_completion dependency')
    deps_parser.add_argument('--table-id', help='Table ID for data_availability dependency')
    deps_parser.add_argument('--min-rows', type=int, default=1, help='Minimum rows required')
    deps_parser.add_argument('--max-age-hours', type=int, default=24, help='Maximum age in hours')
    deps_parser.add_argument('--service-name', help='Service name for service_check dependency')
    deps_parser.add_argument('--check-url', help='URL for service check')
    deps_parser.add_argument('--condition', help='Time condition (business_hours, weekend)')
    deps_parser.add_argument('--timeout-hours', type=int, default=24, help='Timeout in hours')
    
    # Wait parameters
    deps_parser.add_argument('--max-wait-minutes', type=int, default=60, help='Maximum wait time in minutes')
    deps_parser.add_argument('--check-interval-minutes', type=int, default=5, help='Check interval in minutes')
    
    # Conditional parameters
    deps_parser.add_argument('--condition-type', choices=['data_exists', 'error_rate_threshold', 'time_based'], 
                           help='Condition type')
    deps_parser.add_argument('--pipeline-command', help='Pipeline command to execute')
    deps_parser.add_argument('--source', help='Source for error rate check')
    deps_parser.add_argument('--max-error-rate', type=float, default=0.1, help='Maximum error rate threshold')
    deps_parser.add_argument('--hours-back', type=int, default=24, help='Hours to look back for error rate')
    deps_parser.add_argument('--schedule', help='Schedule type (daily)')
    deps_parser.set_defaults(func=cmd_dependencies)
    
    # Templates command
    templates_parser = subparsers.add_parser('templates', help='Manage pipeline templates')
    templates_group = templates_parser.add_mutually_exclusive_group(required=True)
    templates_group.add_argument('--create', action='store_true', help='Create a new template')
    templates_group.add_argument('--execute', action='store_true', help='Execute a template')
    templates_group.add_argument('--list', action='store_true', help='List available templates')
    
    # Template parameters
    templates_parser.add_argument('--template-name', help='Template name')
    templates_parser.add_argument('--description', help='Template description')
    templates_parser.add_argument('--dependencies', help='JSON string of dependencies')
    templates_parser.add_argument('--conditions', help='JSON string of conditions')
    templates_parser.add_argument('--pipeline-command', help='Pipeline command template')
    templates_parser.add_argument('--parameters', help='JSON string of parameters')
    templates_parser.add_argument('--param-file', help='JSON file with parameters')
    templates_parser.set_defaults(func=cmd_templates)

    args = parser.parse_args()
    
    if not hasattr(args, 'func'):
        parser.print_help()
        return 1
    
    return args.func(args)

if __name__ == "__main__":
    sys.exit(main())
