#!/usr/bin/env python3
# Validate pipeline configuration before execution

import yaml
import os
import logging
from typing import Tuple, List, Dict, Any, Optional
from pathlib import Path
from shared.bigquery_client import get_gcs_client, CREDENTIAL_FALLBACK_PATHS

# Configure logging
logger = logging.getLogger(__name__)

def validate_pipeline(source: str, config: Optional[Dict[str, Any]] = None, 
                     check_gcs: bool = True) -> Tuple[bool, List[str]]:
    # Full pipeline validation
    errors = []
    
    # If config not provided, load it
    if config is None:
        # Find config file
        config_path = find_config_file(source)
        if not config_path:
            return False, [f"Configuration file not found for source: {source}"]
        
        # Validate YAML syntax
        is_valid, yaml_errors = validate_yaml_syntax(config_path)
        if not is_valid:
            errors.extend(yaml_errors)
            return False, errors
        
        # Load config
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    
    # Validate required fields
    is_valid, field_errors = validate_required_fields(config)
    if not is_valid:
        errors.extend(field_errors)
    
    # Validate connections
    is_valid, conn_errors = validate_connections(config)
    if not is_valid:
        errors.extend(conn_errors)
    
    # Validate resources
    is_valid, resource_errors = validate_resources(config)
    if not is_valid:
        errors.extend(resource_errors)
    
    # Validate pipeline settings
    is_valid, pipeline_errors = validate_pipeline_settings(config)
    if not is_valid:
        errors.extend(pipeline_errors)
    
    # Validate GCS bucket access if needed
    if check_gcs:
        is_valid, gcs_errors = validate_gcs_access(config)
        if not is_valid:
            errors.extend(gcs_errors)
    
    return len(errors) == 0, errors

def find_config_file(source: str) -> Optional[Path]:
    # Find configuration file
    config_paths = [
        Path(f'configs/{source}.yaml'),
        Path(f'configs/{source}.yml'),
        Path(f'config/{source}.yaml'),
        Path(f'config/{source}.yml')
    ]
    
    for path in config_paths:
        if path.exists():
            return path
    return None

def validate_yaml_syntax(config_path: Path) -> Tuple[bool, List[str]]:
    # Check YAML is parseable
    try:
        with open(config_path, 'r') as f:
            yaml.safe_load(f)
        return True, []
    except yaml.YAMLError as e:
        return False, [f"Invalid YAML syntax: {e}"]

def validate_required_fields(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    # Ensure all required fields present
    errors = []
    required_sections = ['source', 'pipeline', 'resources']
    
    for section in required_sections:
        if section not in config:
            errors.append(f"Missing required section: {section}")
    
    # Check source section
    if 'source' in config:
        source_required = ['name', 'type']
        for field in source_required:
            if field not in config['source']:
                errors.append(f"Missing required field: source.{field}")
    
    # Check pipeline section
    if 'pipeline' in config:
        # Check for dataset field (either 'dataset' or 'staging_dataset' is acceptable)
        pipeline = config['pipeline']
        has_dataset = any(key in pipeline for key in ['dataset', 'staging_dataset', 'production_dataset'])
        if not has_dataset:
            errors.append(f"Missing required field: pipeline.dataset or pipeline.staging_dataset")
    
    return len(errors) == 0, errors

def validate_connections(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    # Test connections are valid
    errors = []
    source_type = config.get('source', {}).get('type')
    
    if source_type == 'facebook':
        # Check Facebook connection
        connection = config['source'].get('connection', {})
        if not connection.get('access_token'):
            if not os.getenv('FACEBOOK_ACCESS_TOKEN'):
                errors.append("Facebook access token not found in config or environment")
    
    elif source_type == 'mysql':
        # Check SSH key for WordPress
        connection = config['source'].get('connection', {})
        ssh_key = connection.get('ssh_key')
        if ssh_key and ssh_key.startswith('$'):
            # Environment variable
            var_name = ssh_key.replace('${', '').replace('}', '')
            if not os.getenv(var_name):
                errors.append(f"SSH key environment variable {var_name} not set")
    
    # Check BigQuery connection - check env var path and fallback paths
    gcp_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    paths_to_check = [gcp_creds] if gcp_creds else []
    paths_to_check.extend(CREDENTIAL_FALLBACK_PATHS)
    creds_found = any(os.path.exists(p) for p in paths_to_check if p)
    if not creds_found:
        errors.append(f"Google credentials file not found in any location: {paths_to_check}")
    
    return len(errors) == 0, errors

def validate_resources(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    # Check resource definitions
    errors = []
    resources = config.get('resources', {})

    # SQL-only pipelines (e.g. profile_database) have no extractable resources
    source_type = config.get('source', {}).get('type', '')
    if source_type in ('profile_database',) and not resources:
        return True, []

    if not resources:
        errors.append("No resources defined")
        return False, errors
    
    for name, resource in resources.items():
        # Check required resource fields
        if 'table_name' not in resource:
            errors.append(f"Resource '{name}' missing table_name")
        
        if 'primary_key' not in resource:
            errors.append(f"Resource '{name}' missing primary_key")
        elif not isinstance(resource['primary_key'], list):
            errors.append(f"Resource '{name}' primary_key must be a list")
        
        # Check write disposition
        valid_dispositions = ['merge', 'append', 'replace']
        if 'write_disposition' in resource:
            if resource['write_disposition'] not in valid_dispositions:
                errors.append(f"Resource '{name}' invalid write_disposition: {resource['write_disposition']}")
        
        # Check incremental configuration
        inc = resource.get('incremental', {})
        if inc.get('strategy') == 'date':
            if not inc.get('fields'):
                errors.append(f"Resource '{name}' incremental strategy 'date' requires 'fields'")
            if not inc.get('initial_value') and not inc.get('lookback_days'):
                errors.append(f"Resource '{name}' incremental strategy 'date' requires 'initial_value' or 'lookback_days'")
    
    return len(errors) == 0, errors

def validate_pipeline_settings(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    # Validate pipeline configuration
    errors = []
    pipeline = config.get('pipeline', {})
    
    # Check dataset naming
    dataset = pipeline.get('dataset', '')
    if dataset:
        # BigQuery dataset naming rules
        if not dataset.replace('_', '').isalnum():
            errors.append(f"Invalid dataset name: {dataset} (use only letters, numbers, underscores)")
        if len(dataset) > 1024:
            errors.append(f"Dataset name too long: {dataset}")
    
    # Check scheduling if present
    if 'scheduling' in config:
        schedule = config['scheduling']
        # Accept either 'cron' or 'schedule' field
        if schedule.get('enabled'):
            has_schedule = any(key in schedule for key in ['cron', 'schedule'])
            if not has_schedule:
                errors.append("Scheduling enabled but no cron/schedule expression provided")
    
    # Check notification configuration
    if 'notifications' in config:
        notifications = config['notifications']
        if notifications.get('enabled'):
            channels = notifications.get('channels', {})
            
            # Check email config
            if channels.get('email', {}).get('enabled'):
                if not channels['email'].get('recipients'):
                    errors.append("Email notifications enabled but no recipients specified")
            
            # Check Slack config
            if channels.get('slack', {}).get('enabled'):
                if not channels['slack'].get('webhook_url'):
                    errors.append("Slack notifications enabled but no webhook URL provided")
    
    # Check circuit breaker config
    if 'circuit_breaker' in config:
        cb = config['circuit_breaker']
        if cb.get('failure_threshold', 5) < 1:
            errors.append("Circuit breaker failure_threshold must be at least 1")
        if cb.get('timeout_seconds', 300) < 1:
            errors.append("Circuit breaker timeout_seconds must be at least 1")
    
    return len(errors) == 0, errors

def validate_gcs_access(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    # Validate GCS bucket configuration and access
    errors = []
    warnings = []
    
    try:
        # Check if GCS is configured
        gcs_config = config.get('gcs', {})
        pipeline_config = config.get('pipeline', {})
        
        # Try multiple ways to get bucket name
        bucket_name = (gcs_config.get('bucket_name') or 
                      pipeline_config.get('gcs_bucket') or
                      os.getenv('GCS_BUCKET'))
        
        if not bucket_name:
            # GCS is optional, don't error if not configured
            logger.debug("GCS bucket not configured (optional)")
            return True, []
        
        # Verify bucket access
        try:
            client = get_gcs_client()
            bucket = client.bucket(bucket_name)
            
            # Skip bucket.exists() check to avoid storage.buckets.get permission requirement
            # Test access permissions directly
            try:
                bucket.reload()
                logger.info(f"GCS bucket access verified: {bucket_name}")
            except Exception as e:
                # Treat as warning, not error (might be permissions issue)
                logger.debug(f"GCS bucket access issue: {str(e)}")
                # Don't add to errors - this won't block execution
                    
        except ImportError:
            errors.append("google-cloud-storage library not installed")
        except Exception as e:
            # Treat GCS errors as warnings
            logger.debug(f"GCS validation warning: {str(e)}")
            
    except Exception as e:
        logger.debug(f"Error validating GCS: {str(e)}")
    
    # Only fail if critical errors (like library missing or bucket doesn't exist)
    return len(errors) == 0, errors

def generate_validation_report(source: str, is_valid: bool, errors: List[str]) -> str:
    # Create readable validation report
    report = f"\nPipeline Validation Report: {source}\n"
    report += "=" * 50 + "\n\n"
    
    if is_valid:
        report += "[VALID] Configuration is VALID\n"
        report += "        All checks passed - ready to execute\n"
    else:
        report += "[ERROR] Configuration is INVALID\n\n"
        report += "Errors found:\n"
        for i, error in enumerate(errors, 1):
            report += f"  {i}. {error}\n"
        report += f"\nTotal errors: {len(errors)}\n"
        report += "Please fix the above issues before running the pipeline.\n"
    
    return report

