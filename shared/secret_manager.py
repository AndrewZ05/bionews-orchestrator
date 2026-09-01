#!/usr/bin/env python3
# Google Secret Manager integration with .env fallback

import os
from typing import Optional, List, Dict, Any
from pathlib import Path

def get_secret(secret_id: str, project_id: Optional[str] = None) -> Optional[str]:
    # Retrieve secret from Google Secret Manager
    try:
        from google.cloud import secretmanager
        
        if not project_id:
            project_id = os.getenv('GCP_PROJECT_ID')
        
        if not project_id:
            return None
        
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
        
    except Exception:
        # Secret Manager not available or secret not found
        return None

def get_secret_or_env(key: str) -> Optional[str]:
    # Try Secret Manager first, fall back to environment
    # First check if Secret Manager is configured
    use_secret_manager = os.getenv('USE_SECRET_MANAGER', 'false').lower() == 'true'
    
    if use_secret_manager:
        # Try to get from Secret Manager
        secret_value = get_secret(key)
        if secret_value:
            return secret_value
    
    # Fall back to environment variable
    env_value = os.getenv(key)
    if env_value:
        return env_value
    
    # Finally check .env file if it exists
    env_path = Path('.env')
    if env_path.exists():
        from dotenv import dotenv_values
        env_vars = dotenv_values('.env')
        return env_vars.get(key)
    
    return None

def list_secrets(project_id: Optional[str] = None) -> List[str]:
    # List available secrets in Secret Manager
    try:
        from google.cloud import secretmanager
        
        if not project_id:
            project_id = os.getenv('GCP_PROJECT_ID')
        
        if not project_id:
            return []
        
        client = secretmanager.SecretManagerServiceClient()
        parent = f"projects/{project_id}"
        
        secrets = []
        for secret in client.list_secrets(request={"parent": parent}):
            secret_name = secret.name.split('/')[-1]
            secrets.append(secret_name)
        
        return secrets
        
    except Exception:
        return []

def create_secret(secret_id: str, secret_value: str, 
                 project_id: Optional[str] = None) -> bool:
    # Create new secret in Secret Manager
    try:
        from google.cloud import secretmanager
        
        if not project_id:
            project_id = os.getenv('GCP_PROJECT_ID')
        
        if not project_id:
            return False
        
        client = secretmanager.SecretManagerServiceClient()
        parent = f"projects/{project_id}"
        
        # Create the secret
        secret = client.create_secret(
            request={
                "parent": parent,
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}}
            }
        )
        
        # Add secret version
        client.add_secret_version(
            request={
                "parent": secret.name,
                "payload": {"data": secret_value.encode("UTF-8")}
            }
        )
        
        return True
        
    except Exception as e:
        print(f"Failed to create secret: {e}")
        return False

def update_secret(secret_id: str, secret_value: str,
                 project_id: Optional[str] = None) -> bool:
    # Update existing secret with new version
    try:
        from google.cloud import secretmanager
        
        if not project_id:
            project_id = os.getenv('GCP_PROJECT_ID')
        
        if not project_id:
            return False
        
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}"
        
        # Add new version
        client.add_secret_version(
            request={
                "parent": name,
                "payload": {"data": secret_value.encode("UTF-8")}
            }
        )
        
        return True
        
    except Exception as e:
        print(f"Failed to update secret: {e}")
        return False

def migrate_env_to_secret_manager(env_file: str = '.env',
                                 project_id: Optional[str] = None) -> Dict[str, bool]:
    # Migrate .env file to Secret Manager
    from dotenv import dotenv_values
    
    env_vars = dotenv_values(env_file)
    results = {}
    
    for key, value in env_vars.items():
        if value:
            success = create_secret(key, value, project_id)
            results[key] = success
    
    return results

