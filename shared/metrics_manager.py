#!/usr/bin/env python3
"""
Metrics Manager for Pipeline Performance Monitoring  
==================================================

Provides comprehensive metrics collection and monitoring
for data extraction pipelines. Tracks performance, success rates,
resource utilization, and operational KPIs.

Author: Data Pipeline Team
Created: 2025-01-02
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from google.cloud import bigquery

# Configure logging
logger = logging.getLogger(__name__)

def create_metrics_table(client: bigquery.Client, dataset: str) -> None:
    """
    Create comprehensive metrics table for pipeline monitoring.
    
    Stores detailed metrics across multiple dimensions for
    analysis and alerting.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset name for metrics table
    """
    table_id = f"{client.project}.{dataset}._pipeline_metrics"
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
        metric_id STRING NOT NULL,
        job_id STRING NOT NULL,
        source STRING NOT NULL,
        table_name STRING NOT NULL,
        metric_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        
        -- Execution metrics
        execution_start_time TIMESTAMP,
        execution_end_time TIMESTAMP,
        execution_duration_seconds INT64,
        
        -- Data metrics
        total_rows_extracted INT64,
        total_rows_loaded INT64,
        total_bytes_processed INT64,
        
        -- Rate metrics
        extraction_rate_rows_per_second FLOAT64,
        loading_rate_rows_per_second FLOAT64,
        
        -- Error metrics
        error_count INT64,
        warning_count INT64,
        retry_attempts INT64,
        
        -- Resource metrics
        cpu_usage_percent FLOAT64,
        memory_usage_mb INT64,
        disk_usage_mb INT64,
        network_transfer_mb INT64,
        
        -- API metrics (source-specific)
        api_calls_total INT64,
        api_calls_failed INT64,
        rate_limit_hits INT64,
        
        -- Pipeline step metrics
        step_progress STRING,  -- JSON array of step completion
        checkpoint_count INT64,
        
        -- Performance metrics
        avg_row_size_bytes INT64,
        peak_memory_mb INT64,
        gc_collection_count INT64,
        
        -- Cost metrics (if applicable)
        estimated_cost_usd FLOAT64,
        resource_utilization_score FLOAT64,
        
        -- Metadata
        config_hash STRING,
        metrics_metadata STRING  -- JSON for additional metrics
    )
    PARTITION BY DATE(metric_timestamp)
    CLUSTER BY source, table_name, execution_start_time
    """
    
    try:
        client.query(create_sql).result()
        logger.info(f"Metrics table ready: {table_id}")
    except Exception as e:
        logger.error(f"Failed to create metrics table: {e}")
        raise

def collect_execution_metrics(start_time: datetime, end_time: datetime,
                            source: str, table: str, job_id: str,
                            stats: Dict[str, Any]) -> Dict[str, Any]:
    """
    Collect comprehensive execution metrics from pipeline run.
    
    Aggregates performance data across all pipeline dimensions
    for storage and analysis.
    
    Args:
        start_time: Pipeline execution start time
        end_time: Pipeline execution end time  
        source: Source system name
        table: Table name
        job_id: Job identifier
        stats: Pipeline execution statistics
    
    Returns:
        Comprehensive metrics dictionary
    """
    try:
        execution_duration = (end_time - start_time).total_seconds()
        
        # Calculate rates
        total_rows = stats.get('total_rows', 0)
        extraction_rate = total_rows / execution_duration if execution_duration > 0 else 0
        
        # Initialize metrics structure
        metrics = {
            'metric_id': f"metric_{job_id}_{int(start_time.timestamp())}",
            'job_id': job_id,
            'source': source,
            'table_name': table,
            'metric_timestamp': datetime.now(),
            
            # Execution metrics
            'execution_start_time': start_time,
            'execution_end_time': end_time,
            'execution_duration_seconds': execution_duration,
            
            # Data metrics
            'total_rows_extracted': stats.get('total_rows', 0),
            'total_rows_loaded': stats.get('total_rows', 0),  # Same as extracted for most cases
            'total_bytes_processed': stats.get('total_bytes', 0),
            
            # Rate metrics
            'extraction_rate_rows_per_second': extraction_rate,
            'loading_rate_rows_per_second': extraction_rate,  # Simplified
            'data_transfer_rate_mbps': stats.get('transfer_rate_mbps', 0),
            
            # Error metrics
            'error_count': stats.get('processing_errors', 0),
            'warning_count': stats.get('warnings', 0),
            'retry_attempts': stats.get('retry_count', 0),
            
            # Resource metrics (platform-dependent)
            'cpu_usage_percent': stats.get('cpu_usage_percent', 0),
            'memory_usage_mb': stats.get('memory_usage_mb', 0),
            'disk_usage_mb': stats.get('disk_usage_mb', 0),
            'network_transfer_mb': stats.get('network_transfer_mb', 0),
            
            # API metrics
            'api_calls_total': stats.get('api_calls', 0),
            'api_calls_failed': stats.get('api_calls_failed', 0),
            'rate_limit_hits': stats.get('rate_limit_hits', 0),
            
            # Pipeline metrics
            'step_progress': json.dumps(stats.get('step_completion', [])),
            'checkpoint_count': stats.get('checkpoint_count', 0),
            
            # Performance metrics
            'avg_row_size_bytes': stats.get('avg_row_size_bytes', 0),
            'peak_memory_mb': stats.get('peak_memory_mb', 0),
            'gc_collection_count': stats.get('gc_collections', 0),
            
            # Cost metrics
            'estimated_cost_usd': stats.get('estimated_cost_usd', 0),
            'resource_utilization_score': stats.get('utilization_score', 0),
            
            # Metadata
            'config_hash': stats.get('config_hash', ''),
            'metrics_metadata': json.dumps(stats.get('additional_metrics', {}))
        }
        
        logger.info(f"Collected metrics for {source}/{table}: {execution_duration:.2f}s, {total_rows:,} rows")
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to collect execution metrics: {e}")
        raise

def save_metrics(client: bigquery.Client, dataset: str, metrics: Dict[str, Any]) -> bool:
    """
    Save collected metrics to BigQuery metrics table.
    
    Inserts comprehensive metrics data for analysis and monitoring.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset containing metrics table
        metrics: Metrics dictionary from collect_execution_metrics()
    
    Returns:
        True if metrics saved successfully
    """
    try:
        table_id = f"{client.project}.{dataset}._pipeline_metrics"
        
        # Convert metrics to BigQuery row
        row_data = []
        for key, value in metrics.items():
            row_data.append(value)
        
        # Insert metrics row
        errors = client.insert_rows_json(table_id, [metrics])
        
        if errors:
            logger.error(f"Failed to insert metrics: {errors}")
            return False
        
        logger.info(f"Metrics saved: {metrics['metric_id']}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save metrics: {e}")
        return False

def get_recent_metrics(client: bigquery.Client, dataset: str,
                      source: str = None, table: str = None,
                      hours_back: int = 24, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Retrieve recent pipeline metrics with optional filtering.
    
    Useful for monitoring dashboards and real-time analysis.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset containing metrics table
        source: Optional source filter
        table: Optional table filter
        hours_back: Hours to look back for metrics
        limit: Maximum number of metrics to return
    
    Returns:
        List of metrics dictionaries
    """
    try:
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        where_clauses = [f"metric_timestamp >= '{cutoff_time.isoformat()}'"]
        
        if source:
            where_clauses.append(f"source = '{source}'")
        if table:
            where_clauses.append(f"table_name = '{table}'")
        
        where_sql = " AND ".join(where_clauses)
        
        query_sql = f"""
        SELECT *
        FROM `{client.project}.{dataset}._pipeline_metrics`
        WHERE {where_sql}
        ORDER BY execution_start_time DESC
        LIMIT {limit}
        """
        
        result = client.query(query_sql).result()
        metrics = []
        
        for row in result:
            metrics.append(dict(row))
        
        logger.info(f"Retrieved {len(metrics)} metrics records")
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get recent metrics: {e}")
        return []

def calculate_performance_kpis(metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate performance KPIs from metrics collection.
    
    Computes key performance indicators for operational monitoring
    and alerting.
    
    Args:
        metrics: List of metrics dictionaries
    
    Returns:
        Dictionary with calculated KPIs
    """
    try:
        if not metrics:
            return {'error': 'No metrics available'}
        
        # Initialize KPI calculations
        kpis = {
            'total_executions': len(metrics),
            'successful_executions': 0,
            'failed_executions': 0,
            'avg_execution_duration': 0,
            'avg_throughput': 0,
            'error_rate': 0,
            'peak_execution_time': 0,
            'resource_efficiency': 0,
            'cost_per_row': 0,
            'source_performance': {},
            'table_performance': {}
        }
        
        total_duration = 0
        total_rows = 0
        total_errors = 0
        
        # Aggregate metrics
        for metric in metrics:
            # Success/failure classification
            if metric.get('error_count', 0) == 0:
                kpis['successful_executions'] += 1
            else:
                kpis['failed_executions'] += 1
            
            # Performance aggregation
            duration = metric.get('execution_duration_seconds', 0)
            rows = metric.get('total_rows_extracted', 0)
            errors = metric.get('error_count', 0)
            
            total_duration += duration
            total_rows += rows
            total_errors += errors
            
            # Track performance by source
            source = metric.get('source')
            if source not in kpis['source_performance']:
                kpis['source_performance'][source] = {'count': 0, 'avg_duration': 0, 'total_rows': 0}
            
            kpis['source_performance'][source]['count'] += 1
            kpis['source_performance'][source]['avg_duration'] += duration
            kpis['source_performance'][source]['total_rows'] += rows
            
            # Track performance by table
            table = metric.get('table_name')
            if table not in kpis['table_performance']:
                kpis['table_performance'][table] = {'count': 0, 'avg_duration': 0, 'total_rows': 0}
            
            kpis['table_performance'][table]['count'] += 1
            kpis['table_performance'][table]['avg_duration'] += duration
            kpis['table_performance'][table]['total_rows'] += rows
        
        # Calculate averages and rates
        kpis['avg_execution_duration'] = total_duration / len(metrics) if metrics else 0
        kpis['avg_throughput'] = total_rows / (total_duration / 60) if total_duration > 0 else 0  # rows per minute
        kpis['error_rate'] = (total_errors / max(total_rows, 1)) * 100  # errors per 100 rows
        
        # Calculate source performance averages
        for source, perf in kpis['source_performance'].items():
            perf['avg_duration'] /= perf['count']
            perf['avg_rows_per_execution'] = perf['total_rows'] / perf['count']
        
        # Calculate table performance averages  
        for table, perf in kpis['table_performance'].items():
            perf['avg_duration'] /= perf['count']
            perf['avg_rows_per_execution'] = perf['total_rows'] / perf['count']
        
        logger.info(f"Calculated KPIs for {len(metrics)} metrics records")
        return kpis
        
    except Exception as e:
        logger.error(f"Failed to calculate performance KPIs: {e}")
        return {'error': str(e)}

def get_pipeline_health_score(client: bigquery.Client, dataset: str,
                             source: str = None, hours_back: int = 24) -> Dict[str, Any]:
    """
    Calculate overall pipeline health score.
    
    Provides single metric for pipeline health assessment
    based on recent performance.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset containing metrics table
        source: Optional source filter
        hours_back: Time window for health assessment
    
    Returns:
        Health score dictionary with score and details
    """
    try:
        metrics = get_recent_metrics(client, dataset, source, hours_back=hours_back)
        kpis = calculate_performance_kpis(metrics)
        
        # Calculate health components
        success_rate = (kpis['successful_executions'] / kpis['total_executions']) * 100 if kpis['total_executions'] > 0 else 0
        
        # Reliability score (0-100 based on success rate and error rate)
        reliability_score = max(0, 100 - kpis['error_rate'])
        
        # Performance score (0-100 based on throughput and duration)
        avg_throughput = kpis['avg_throughput']
        performance_score = min(100, (avg_throughput / 1000) * 100)  # Normalize to 1000 rows/min = 100
        
        # Overall health score (weighted average)
        weights = {
            'success_rate': 0.4,
            'reliability': 0.3,
            'performance': 0.3
        }
        
        health_score = (
            success_rate * weights['success_rate'] +
            reliability_score * weights['reliability'] +
            performance_score * weights['performance']
        )
        
        # Determine health status
        if health_score >= 90:
            health_status = "EXCELLENT"
        elif health_score >= 80:
            health_status = "GOOD"  
        elif health_score >= 70:
            health_status = "FAIR"
        elif health_score >= 60:
            health_status = "POOR"
        else:
            health_status = "CRITICAL"
        
        health_summary = {
            'overall_score': round(health_score, 2),
            'health_status': health_status,
            'success_rate': round(success_rate, 2),
            'reliability_score': round(reliability_score, 2),
            'performance_score': round(performance_score, 2),
            'total_executions': kpis['total_executions'],
            'error_rate': round(kpis['error_rate'], 2),
            'avg_duration_minutes': round(kpis['avg_execution_duration'] / 60, 2),
            'avg_throughput_rows_per_min': round(kpis['avg_throughput'], 2),
            'kpis': kpis,
            'timestamp': datetime.now().isoformat(),
            'assessment_window_hours': hours_back
        }
        
        logger.info(f"Pipeline health score: {health_score:.2f} ({health_status})")
        return health_summary
        
    except Exception as e:
        logger.error(f"Failed to calculate pipeline health score: {e}")
        return {'error': str(e)}

# Configuration constants
DEFAULT_METRICS_DATASET = "orchestrator_monitoring"
DEFAULT_HEALTH_WINDOW_HOURS = 24
DEFAULT_KPI_LOOKBACK_HOURS = 24

# Health score thresholds
HEALTH_THRESHOLDS = {
    'EXCELLENT': (90, 100),
    'GOOD': (80, 90),
    'FAIR': (70, 80), 
    'POOR': (60, 70),
    'CRITICAL': (0, 60)
}

#
# Enhanced Monitoring Functions
#
def get_resource_usage_metrics(client: bigquery.Client, dataset: str, 
                              hours_back: int = 24) -> Dict[str, Any]:
    # Get resource usage metrics including BigQuery slots and costs
    try:
        # Get BigQuery slot usage
        slot_query = f"""
        SELECT 
            source,
            COUNT(*) as executions,
            AVG(execution_duration_seconds) as avg_duration_seconds,
            SUM(total_rows_extracted) as total_rows,
            AVG(total_rows_extracted) as avg_rows_per_execution
        FROM `{client.project}.{dataset}._pipeline_metrics`
        WHERE metric_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
        GROUP BY source
        ORDER BY total_rows DESC
        """
        
        slot_results = client.query(slot_query).result()
        slot_usage = {}
        
        for row in slot_results:
            slot_usage[row.source] = {
                'executions': row.executions,
                'avg_duration_seconds': row.avg_duration_seconds,
                'total_rows': row.total_rows,
                'avg_rows_per_execution': row.avg_rows_per_execution,
                'estimated_slots_used': min(2000, row.avg_rows_per_execution / 1000)  # Rough estimate
            }
        
        # Get cost estimates (BigQuery charges by bytes processed)
        cost_query = f"""
        SELECT 
            source,
            SUM(estimated_bytes_processed) as total_bytes,
            COUNT(*) as executions,
            AVG(estimated_bytes_processed) as avg_bytes_per_execution
        FROM `{client.project}.{dataset}._pipeline_metrics`
        WHERE metric_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
          AND estimated_bytes_processed > 0
        GROUP BY source
        """
        
        cost_results = client.query(cost_query).result()
        cost_usage = {}
        
        for row in cost_results:
            # BigQuery pricing: $5 per TB processed
            estimated_cost = (row.total_bytes / (1024**4)) * 5  # Convert bytes to TB, multiply by $5
            cost_usage[row.source] = {
                'total_bytes': row.total_bytes,
                'executions': row.executions,
                'avg_bytes_per_execution': row.avg_bytes_per_execution,
                'estimated_cost_usd': round(estimated_cost, 4)
            }
        
        return {
            'slot_usage': slot_usage,
            'cost_usage': cost_usage,
            'summary': {
                'total_executions': sum(s['executions'] for s in slot_usage.values()),
                'total_estimated_cost': sum(c['estimated_cost_usd'] for c in cost_usage.values()),
                'peak_slots_used': max(s['estimated_slots_used'] for s in slot_usage.values()) if slot_usage else 0
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get resource usage metrics: {e}")
        return {'error': str(e)}

def get_performance_trends(client: bigquery.Client, dataset: str, 
                          source: str = None, days_back: int = 7) -> Dict[str, Any]:
    # Get performance trends over time
    try:
        source_filter = f"AND source = '{source}'" if source else ""
        
        trends_query = f"""
        SELECT 
            DATE(metric_timestamp) as execution_date,
            source,
            COUNT(*) as daily_executions,
            AVG(execution_duration_seconds) as avg_duration_seconds,
            AVG(total_rows_extracted) as avg_rows_per_execution,
            AVG(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) as success_rate,
            SUM(total_rows_extracted) as total_daily_rows
        FROM `{client.project}.{dataset}._pipeline_metrics`
        WHERE metric_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
        {source_filter}
        GROUP BY execution_date, source
        ORDER BY execution_date DESC, source
        """
        
        trends_results = client.query(trends_query).result()
        trends = {}
        
        for row in trends_results:
            date_key = row.execution_date.strftime('%Y-%m-%d')
            if date_key not in trends:
                trends[date_key] = {}
            
            trends[date_key][row.source] = {
                'executions': row.daily_executions,
                'avg_duration_seconds': row.avg_duration_seconds,
                'avg_rows_per_execution': row.avg_rows_per_execution,
                'success_rate': row.success_rate,
                'total_rows': row.total_daily_rows
            }
        
        # Calculate trend indicators
        trend_analysis = {}
        for source_name in set(row.source for row in trends_results):
            source_data = [trends[date][source_name] for date in trends.keys() if source_name in trends[date]]
            
            if len(source_data) >= 2:
                # Calculate trend direction
                recent_avg_duration = sum(d['avg_duration_seconds'] for d in source_data[:3]) / min(3, len(source_data))
                older_avg_duration = sum(d['avg_duration_seconds'] for d in source_data[-3:]) / min(3, len(source_data))
                
                duration_trend = "improving" if recent_avg_duration < older_avg_duration else "degrading"
                
                recent_success_rate = sum(d['success_rate'] for d in source_data[:3]) / min(3, len(source_data))
                older_success_rate = sum(d['success_rate'] for d in source_data[-3:]) / min(3, len(source_data))
                
                reliability_trend = "improving" if recent_success_rate > older_success_rate else "degrading"
                
                trend_analysis[source_name] = {
                    'duration_trend': duration_trend,
                    'reliability_trend': reliability_trend,
                    'recent_performance': recent_avg_duration,
                    'historical_performance': older_avg_duration
                }
        
        return {
            'daily_trends': trends,
            'trend_analysis': trend_analysis,
            'summary': {
                'days_analyzed': days_back,
                'sources_analyzed': len(trend_analysis),
                'total_data_points': len(trends_results)
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get performance trends: {e}")
        return {'error': str(e)}

def get_data_quality_metrics(client: bigquery.Client, dataset: str, 
                             hours_back: int = 24) -> Dict[str, Any]:
    # Get data quality metrics over time
    try:
        quality_query = f"""
        SELECT 
            source,
            table_name,
            COUNT(*) as quality_checks,
            AVG(CASE WHEN quality_score >= 90 THEN 1 ELSE 0 END) as high_quality_rate,
            AVG(quality_score) as avg_quality_score,
            SUM(CASE WHEN quality_issues > 0 THEN 1 ELSE 0 END) as checks_with_issues,
            AVG(quality_issues) as avg_quality_issues_per_check
        FROM `{client.project}.{dataset}._pipeline_metrics`
        WHERE metric_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
          AND quality_score IS NOT NULL
        GROUP BY source, table_name
        ORDER BY avg_quality_score ASC
        """
        
        quality_results = client.query(quality_query).result()
        quality_metrics = {}
        
        for row in quality_results:
            source_key = row.source
            if source_key not in quality_metrics:
                quality_metrics[source_key] = {}
            
            quality_metrics[source_key][row.table_name] = {
                'quality_checks': row.quality_checks,
                'high_quality_rate': row.high_quality_rate,
                'avg_quality_score': row.avg_quality_score,
                'checks_with_issues': row.checks_with_issues,
                'avg_quality_issues': row.avg_quality_issues_per_check
            }
        
        # Calculate overall quality summary
        overall_quality = {}
        for source, tables in quality_metrics.items():
            total_checks = sum(t['quality_checks'] for t in tables.values())
            weighted_avg_score = sum(t['avg_quality_score'] * t['quality_checks'] for t in tables.values()) / total_checks if total_checks > 0 else 0
            
            overall_quality[source] = {
                'total_quality_checks': total_checks,
                'weighted_avg_quality_score': weighted_avg_score,
                'tables_monitored': len(tables),
                'quality_status': 'excellent' if weighted_avg_score >= 95 else 'good' if weighted_avg_score >= 85 else 'needs_attention'
            }
        
        return {
            'table_quality': quality_metrics,
            'overall_quality': overall_quality,
            'summary': {
                'sources_monitored': len(quality_metrics),
                'total_checks': sum(o['total_quality_checks'] for o in overall_quality.values()),
                'avg_quality_score': sum(o['weighted_avg_quality_score'] for o in overall_quality.values()) / len(overall_quality) if overall_quality else 0
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get data quality metrics: {e}")
        return {'error': str(e)}

def get_operational_insights(client: bigquery.Client, dataset: str, 
                            hours_back: int = 24) -> Dict[str, Any]:
    # Get operational insights and recommendations
    try:
        # Get failure patterns
        failure_query = f"""
        SELECT 
            source,
            error_message,
            COUNT(*) as failure_count,
            AVG(execution_duration_seconds) as avg_duration_before_failure
        FROM `{client.project}.{dataset}._pipeline_metrics`
        WHERE metric_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
          AND execution_status = 'error'
          AND error_message IS NOT NULL
        GROUP BY source, error_message
        ORDER BY failure_count DESC
        LIMIT 10
        """
        
        failure_results = client.query(failure_query).result()
        failure_patterns = []
        
        for row in failure_results:
            failure_patterns.append({
                'source': row.source,
                'error_message': row.error_message[:100] + "..." if len(row.error_message) > 100 else row.error_message,
                'failure_count': row.failure_count,
                'avg_duration_before_failure': row.avg_duration_before_failure
            })
        
        # Get performance bottlenecks
        bottleneck_query = f"""
        SELECT 
            source,
            table_name,
            AVG(execution_duration_seconds) as avg_duration,
            AVG(total_rows_extracted) as avg_rows,
            COUNT(*) as executions,
            AVG(total_rows_extracted / NULLIF(execution_duration_seconds, 0)) as avg_throughput_rows_per_sec
        FROM `{client.project}.{dataset}._pipeline_metrics`
        WHERE metric_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
          AND execution_status = 'success'
        GROUP BY source, table_name
        HAVING executions >= 2
        ORDER BY avg_duration DESC
        LIMIT 10
        """
        
        bottleneck_results = client.query(bottleneck_query).result()
        bottlenecks = []
        
        for row in bottleneck_results:
            bottlenecks.append({
                'source': row.source,
                'table_name': row.table_name,
                'avg_duration_seconds': row.avg_duration,
                'avg_rows': row.avg_rows,
                'executions': row.executions,
                'avg_throughput_rows_per_sec': row.avg_throughput_rows_per_sec
            })
        
        # Generate recommendations
        recommendations = []
        
        if failure_patterns:
            top_failure = failure_patterns[0]
            recommendations.append({
                'type': 'reliability',
                'priority': 'high',
                'message': f"Most common failure: {top_failure['error_message']} ({top_failure['failure_count']} occurrences)",
                'action': 'Investigate and implement retry logic or error handling'
            })
        
        if bottlenecks:
            slowest_table = bottlenecks[0]
            recommendations.append({
                'type': 'performance',
                'priority': 'medium',
                'message': f"Slowest table: {slowest_table['table_name']} ({slowest_table['avg_duration_seconds']:.1f}s avg)",
                'action': 'Consider optimizing extraction logic or increasing batch size'
            })
        
        return {
            'failure_patterns': failure_patterns,
            'performance_bottlenecks': bottlenecks,
            'recommendations': recommendations,
            'summary': {
                'total_failures': sum(f['failure_count'] for f in failure_patterns),
                'slowest_table': bottlenecks[0]['table_name'] if bottlenecks else None,
                'recommendations_count': len(recommendations)
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get operational insights: {e}")
        return {'error': str(e)}
