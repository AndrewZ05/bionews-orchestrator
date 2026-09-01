#!/usr/bin/env python3
"""
Pipeline Dependencies and Conditional Execution Manager
=======================================================

Provides advanced pipeline management including dependencies,
conditional execution, and pipeline templates.

Author: Data Pipeline Team
Created: 2025-01-02
"""

import logging
import json
import subprocess
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from google.cloud import bigquery

# Configure logging
logger = logging.getLogger(__name__)

def check_pipeline_dependency(dependency_config: Dict[str, Any], 
                             bq_client: bigquery.Client, 
                             dataset: str) -> Tuple[bool, str]:
    # Check if a pipeline dependency is satisfied
    try:
        dep_type = dependency_config.get('type')
        dep_name = dependency_config.get('name')
        
        if dep_type == 'pipeline_completion':
            # Check if another pipeline completed successfully
            pipeline_name = dependency_config.get('pipeline_name')
            timeout_hours = dependency_config.get('timeout_hours', 24)
            
            query = f"""
            SELECT 
                execution_id,
                status,
                completed_at,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), completed_at, HOUR) as hours_ago
            FROM `{bq_client.project}.{dataset}._pipeline_executions`
            WHERE source = @pipeline_name
              AND status = 'success'
              AND completed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {timeout_hours} HOUR)
            ORDER BY completed_at DESC
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name)
                ]
            )
            
            result = bq_client.query(query, job_config=job_config).result()
            rows = list(result)
            
            if rows:
                hours_ago = rows[0].hours_ago
                return True, f"Pipeline {pipeline_name} completed {hours_ago} hours ago"
            else:
                return False, f"Pipeline {pipeline_name} has not completed successfully in the last {timeout_hours} hours"
        
        elif dep_type == 'data_availability':
            # Check if data exists in a table
            table_id = dependency_config.get('table_id')
            min_rows = dependency_config.get('min_rows', 1)
            max_age_hours = dependency_config.get('max_age_hours', 24)
            
            query = f"""
            SELECT 
                COUNT(*) as row_count,
                MAX(extracted_at) as latest_extraction,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(extracted_at), HOUR) as hours_since_extraction
            FROM `{table_id}`
            WHERE extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {max_age_hours} HOUR)
            """
            
            result = bq_client.query(query).result()
            row = list(result)[0]
            
            if row.row_count >= min_rows and row.hours_since_extraction <= max_age_hours:
                return True, f"Table {table_id} has {row.row_count} rows, last updated {row.hours_since_extraction} hours ago"
            else:
                return False, f"Table {table_id} has {row.row_count} rows (min: {min_rows}), last updated {row.hours_since_extraction} hours ago (max: {max_age_hours})"
        
        elif dep_type == 'service_check':
            # Check if a service is available
            service_name = dependency_config.get('service_name')
            check_url = dependency_config.get('check_url')
            
            if service_name == 'bigquery':
                try:
                    # Test BigQuery connection
                    test_query = "SELECT 1 as test"
                    bq_client.query(test_query).result()
                    return True, "BigQuery service is available"
                except Exception as e:
                    return False, f"BigQuery service check failed: {e}"
            
            elif service_name == 'gcs':
                try:
                    # Test GCS connection (would need GCS client)
                    return True, "GCS service is available"
                except Exception as e:
                    return False, f"GCS service check failed: {e}"
            
            elif check_url:
                try:
                    import requests
                    response = requests.get(check_url, timeout=10)
                    if response.status_code == 200:
                        return True, f"Service at {check_url} is responding"
                    else:
                        return False, f"Service at {check_url} returned status {response.status_code}"
                except Exception as e:
                    return False, f"Service check failed for {check_url}: {e}"
        
        elif dep_type == 'time_condition':
            # Check time-based conditions
            condition = dependency_config.get('condition')
            
            if condition == 'business_hours':
                now = datetime.now()
                if now.weekday() < 5 and 9 <= now.hour < 17:  # Monday-Friday, 9 AM - 5 PM
                    return True, "Currently within business hours"
                else:
                    return False, "Currently outside business hours"
            
            elif condition == 'weekend':
                now = datetime.now()
                if now.weekday() >= 5:  # Saturday-Sunday
                    return True, "Currently weekend"
                else:
                    return False, "Currently weekday"
        
        else:
            return False, f"Unknown dependency type: {dep_type}"
    
    except Exception as e:
        logger.error(f"Error checking dependency {dep_name}: {e}")
        return False, f"Dependency check failed: {e}"

def wait_for_dependencies(dependencies: List[Dict[str, Any]], 
                         bq_client: bigquery.Client, 
                         dataset: str,
                         max_wait_minutes: int = 60,
                         check_interval_minutes: int = 5) -> Tuple[bool, List[str]]:
    # Wait for all dependencies to be satisfied
    start_time = datetime.now()
    max_wait_time = start_time + timedelta(minutes=max_wait_minutes)
    
    satisfied_deps = []
    failed_deps = []
    
    while datetime.now() < max_wait_time:
        all_satisfied = True
        
        for dep in dependencies:
            dep_name = dep.get('name', 'unnamed')
            
            # Skip if already satisfied
            if dep_name in satisfied_deps:
                continue
            
            satisfied, message = check_pipeline_dependency(dep, bq_client, dataset)
            
            if satisfied:
                satisfied_deps.append(dep_name)
                logger.info(f"Dependency '{dep_name}' satisfied: {message}")
            else:
                all_satisfied = False
                logger.info(f"Dependency '{dep_name}' not satisfied: {message}")
        
        if all_satisfied:
            return True, satisfied_deps
        
        # Wait before next check
        logger.info(f"Waiting {check_interval_minutes} minutes before next dependency check...")
        time.sleep(check_interval_minutes * 60)
    
    # Timeout reached
    remaining_deps = [dep.get('name', 'unnamed') for dep in dependencies if dep.get('name', 'unnamed') not in satisfied_deps]
    return False, remaining_deps

def execute_conditional_pipeline(condition_config: Dict[str, Any], 
                                pipeline_command: str,
                                bq_client: bigquery.Client,
                                dataset: str) -> Tuple[bool, str]:
    # Execute pipeline based on conditions
    try:
        condition_type = condition_config.get('type')
        
        if condition_type == 'data_exists':
            # Only run if data exists
            table_id = condition_config.get('table_id')
            min_rows = condition_config.get('min_rows', 1)
            
            query = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
            result = bq_client.query(query).result()
            row_count = list(result)[0].row_count
            
            if row_count >= min_rows:
                logger.info(f"Condition met: {row_count} rows found in {table_id}")
                return True, f"Data condition satisfied ({row_count} rows)"
            else:
                logger.info(f"Condition not met: only {row_count} rows in {table_id} (min: {min_rows})")
                return False, f"Insufficient data ({row_count} rows, min: {min_rows})"
        
        elif condition_type == 'error_rate_threshold':
            # Only run if error rate is below threshold
            source = condition_config.get('source')
            max_error_rate = condition_config.get('max_error_rate', 0.1)  # 10%
            hours_back = condition_config.get('hours_back', 24)
            
            query = f"""
            SELECT 
                COUNT(*) as total_executions,
                SUM(CASE WHEN execution_status = 'error' THEN 1 ELSE 0 END) as error_executions
            FROM `{bq_client.project}.{dataset}._pipeline_metrics`
            WHERE source = @source
              AND metric_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("source", "STRING", source)
                ]
            )
            
            result = bq_client.query(query, job_config=job_config).result()
            row = list(result)[0]
            
            if row.total_executions > 0:
                error_rate = row.error_executions / row.total_executions
                if error_rate <= max_error_rate:
                    logger.info(f"Condition met: error rate {error_rate:.2%} <= {max_error_rate:.2%}")
                    return True, f"Error rate condition satisfied ({error_rate:.2%})"
                else:
                    logger.info(f"Condition not met: error rate {error_rate:.2%} > {max_error_rate:.2%}")
                    return False, f"Error rate too high ({error_rate:.2%} > {max_error_rate:.2%})"
            else:
                logger.info("No recent executions found, condition not applicable")
                return False, "No recent executions found"
        
        elif condition_type == 'time_based':
            # Time-based conditions
            schedule = condition_config.get('schedule')
            
            if schedule == 'daily':
                # Check if we've run today
                today = datetime.now().strftime('%Y-%m-%d')
                source = condition_config.get('source')
                
                query = f"""
                SELECT COUNT(*) as executions_today
                FROM `{bq_client.project}.{dataset}._pipeline_executions`
                WHERE source = @source
                  AND DATE(started_at) = @today
                  AND status = 'success'
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("source", "STRING", source),
                        bigquery.ScalarQueryParameter("today", "DATE", today)
                    ]
                )
                
                result = bq_client.query(query, job_config=job_config).result()
                executions_today = list(result)[0].executions_today
                
                if executions_today == 0:
                    logger.info("Condition met: no successful execution today")
                    return True, "No execution today, proceeding"
                else:
                    logger.info(f"Condition not met: {executions_today} executions already completed today")
                    return False, f"Already executed {executions_today} times today"
        
        else:
            return False, f"Unknown condition type: {condition_type}"
    
    except Exception as e:
        logger.error(f"Error checking condition: {e}")
        return False, f"Condition check failed: {e}"

def create_pipeline_template(template_name: str, 
                           template_config: Dict[str, Any]) -> bool:
    # Create a reusable pipeline template
    try:
        template_file = f"configs/templates/{template_name}.yaml"
        
        # Ensure templates directory exists
        import os
        os.makedirs("configs/templates", exist_ok=True)
        
        # Write template configuration
        import yaml
        with open(template_file, 'w') as f:
            yaml.dump(template_config, f, default_flow_style=False)
        
        logger.info(f"Created pipeline template: {template_file}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create template {template_name}: {e}")
        return False

def load_pipeline_template(template_name: str) -> Optional[Dict[str, Any]]:
    # Load a pipeline template
    try:
        template_file = f"configs/templates/{template_name}.yaml"
        
        import yaml
        with open(template_file, 'r') as f:
            template_config = yaml.safe_load(f)
        
        return template_config
        
    except Exception as e:
        logger.error(f"Failed to load template {template_name}: {e}")
        return None

def execute_pipeline_with_template(template_name: str, 
                                 parameters: Dict[str, Any],
                                 bq_client: bigquery.Client,
                                 dataset: str) -> Tuple[bool, str]:
    # Execute pipeline using a template with parameters
    try:
        # Load template
        template_config = load_pipeline_template(template_name)
        if not template_config:
            return False, f"Failed to load template {template_name}"
        
        # Check dependencies
        dependencies = template_config.get('dependencies', [])
        if dependencies:
            satisfied, failed_deps = wait_for_dependencies(dependencies, bq_client, dataset)
            if not satisfied:
                return False, f"Dependencies not satisfied: {failed_deps}"
        
        # Check conditions
        conditions = template_config.get('conditions', [])
        for condition in conditions:
            condition_met, message = execute_conditional_pipeline(condition, "", bq_client, dataset)
            if not condition_met:
                return False, f"Condition not met: {message}"
        
        # Execute pipeline command with parameters
        pipeline_command = template_config.get('pipeline_command', '')
        
        # Substitute parameters in command
        for param_name, param_value in parameters.items():
            pipeline_command = pipeline_command.replace(f"{{{param_name}}}", str(param_value))
        
        logger.info(f"Executing pipeline: {pipeline_command}")
        
        # Execute the command
        result = subprocess.run(pipeline_command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            return True, f"Pipeline executed successfully: {result.stdout}"
        else:
            return False, f"Pipeline failed: {result.stderr}"
    
    except Exception as e:
        logger.error(f"Error executing pipeline with template {template_name}: {e}")
        return False, f"Template execution failed: {e}"

def get_pipeline_dependency_status(dependencies: List[Dict[str, Any]], 
                                  bq_client: bigquery.Client, 
                                  dataset: str) -> Dict[str, Any]:
    # Get status of all pipeline dependencies
    status = {
        'total_dependencies': len(dependencies),
        'satisfied': 0,
        'failed': 0,
        'pending': 0,
        'details': []
    }
    
    for dep in dependencies:
        dep_name = dep.get('name', 'unnamed')
        satisfied, message = check_pipeline_dependency(dep, bq_client, dataset)
        
        dep_status = {
            'name': dep_name,
            'type': dep.get('type'),
            'satisfied': satisfied,
            'message': message
        }
        
        status['details'].append(dep_status)
        
        if satisfied:
            status['satisfied'] += 1
        else:
            status['failed'] += 1
    
    return status
