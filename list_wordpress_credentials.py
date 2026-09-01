#!/usr/bin/env python3
"""
List WordPress site credentials by looping through all discovered instances.
Extracts site name, database name, username, and password from wp-config.php.
"""

import os
import sys
import logging
from typing import Dict, Any, List, Optional
from fabric import Connection

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.config_loader import load_config
from plugins.wordpress_extractor import (
    get_available_sites,
    extract_wp_constant,
    extract_wp_variable
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_wp_credentials(conn: Connection, site_name: str) -> Optional[Dict[str, Any]]:
    """
    Extract WordPress database credentials from wp-config.php via SSH.

    Args:
        conn: Fabric SSH connection
        site_name: Name of the WordPress site

    Returns:
        Dictionary with site_name, db_name, db_user, db_password or None on failure
    """
    config_paths = [
        f"sites/{site_name}/wp-config.php",
        f"~/sites/{site_name}/wp-config.php",
        "wp-config.php"
    ]

    for path in config_paths:
        try:
            result = conn.run(f'test -f "{path}"', hide=True, warn=True, timeout=10)
            if result.ok:
                result = conn.run(f'cat "{path}"', hide=True, warn=True, timeout=10)
                content = result.stdout

                # Handle BOM or other content before <?php
                php_start = content.find('<?php')
                if php_start > 0:
                    content = content[php_start:]

                db_name = extract_wp_constant(content, "DB_NAME")
                db_user = extract_wp_constant(content, "DB_USER")
                db_password = extract_wp_constant(content, "DB_PASSWORD")

                return {
                    'site_name': site_name,
                    'db_name': db_name,
                    'db_user': db_user,
                    'db_password': db_password
                }
        except Exception as e:
            logger.debug(f"Failed to read {path}: {e}")
            continue

    return None


def connect_and_get_credentials(site_name: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Establish SSH connection to a site and retrieve its database credentials.

    Args:
        site_name: Name of the WordPress site
        config: Configuration dictionary

    Returns:
        Credentials dictionary or None on failure
    """
    conn_patterns = config.get('connection_patterns', {}).get('wpengine', {})

    ssh_host = conn_patterns.get('ssh_host_pattern', '{site_name}.ssh.wpengine.net').format(site_name=site_name)
    ssh_user = conn_patterns.get('ssh_user_pattern', '{site_name}').format(site_name=site_name)
    ssh_key_path = conn_patterns.get('ssh_key') or os.getenv('WP_SSH_KEY_PATH')

    if not ssh_key_path:
        logger.error("SSH key path not configured. Set WP_SSH_KEY_PATH environment variable.")
        return None

    ssh_key_path = os.path.expanduser(ssh_key_path)

    if not os.path.exists(ssh_key_path):
        logger.error(f"SSH key not found: {ssh_key_path}")
        return None

    # Suppress Paramiko warnings
    import logging as py_logging
    paramiko_logger = py_logging.getLogger('paramiko')
    old_level = paramiko_logger.level
    paramiko_logger.setLevel(py_logging.ERROR)

    try:
        with Connection(
            host=ssh_host,
            user=ssh_user,
            connect_kwargs={
                'key_filename': ssh_key_path,
                'timeout': 30
            }
        ) as conn:
            credentials = get_wp_credentials(conn, site_name)
            return credentials
    except Exception as e:
        logger.error(f"Failed to connect to {site_name}: {e}")
        return None
    finally:
        paramiko_logger.setLevel(old_level)


def list_all_wordpress_credentials(source: str = 'wordpress') -> List[Dict[str, Any]]:
    """
    Loop through all WordPress instances and retrieve their credentials.

    Args:
        source: Source name for configuration (default: 'wordpress')

    Returns:
        List of credential dictionaries
    """
    # Load configuration
    config = load_config(source)

    # Get list of sites
    sites = get_available_sites(config)

    if not sites:
        logger.error("No WordPress sites discovered")
        return []

    logger.info(f"Found {len(sites)} WordPress sites")

    credentials_list = []

    for site_name in sites:
        logger.info(f"Processing site: {site_name}")

        credentials = connect_and_get_credentials(site_name, config)

        if credentials:
            credentials_list.append(credentials)
            logger.info(f"  ✓ Retrieved credentials for {site_name}")
        else:
            logger.warning(f"  ✗ Could not retrieve credentials for {site_name}")

    return credentials_list


def print_credentials_table(credentials_list: List[Dict[str, Any]], show_password: bool = False) -> None:
    """
    Print credentials in a formatted table.

    Args:
        credentials_list: List of credential dictionaries
        show_password: Whether to show passwords (default: masked)
    """
    if not credentials_list:
        print("\nNo credentials retrieved.")
        return

    # Calculate column widths
    site_width = max(len(c['site_name']) for c in credentials_list)
    db_width = max(len(c['db_name']) for c in credentials_list)
    user_width = max(len(c['db_user']) for c in credentials_list)

    site_width = max(site_width, 10)
    db_width = max(db_width, 13)
    user_width = max(user_width, 8)
    pass_width = 10

    # Print header
    print("\n" + "=" * (site_width + db_width + user_width + pass_width + 13))
    print(f"{'Site Name':<{site_width}} | {'Database Name':<{db_width}} | {'Username':<{user_width}} | {'Password':<{pass_width}}")
    print("=" * (site_width + db_width + user_width + pass_width + 13))

    # Print rows
    for cred in credentials_list:
        password_display = cred['db_password'] if show_password else '********'
        print(f"{cred['site_name']:<{site_width}} | {cred['db_name']:<{db_width}} | {cred['db_user']:<{user_width}} | {password_display:<{pass_width}}")

    print("=" * (site_width + db_width + user_width + pass_width + 13))
    print(f"\nTotal sites: {len(credentials_list)}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='List WordPress site credentials from all discovered instances'
    )
    parser.add_argument(
        '--source', '-c',
        default='wordpress',
        help='Source name for configuration (default: wordpress)'
    )
    parser.add_argument(
        '--show-password', '-p',
        action='store_true',
        help='Show passwords in plain text (default: masked)'
    )
    parser.add_argument(
        '--json', '-j',
        action='store_true',
        help='Output as JSON'
    )
    parser.add_argument(
        '--site', '-s',
        help='Query a specific site instead of all sites'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.site:
        # Query single site
        config = load_config(args.source)
        credentials = connect_and_get_credentials(args.site, config)
        credentials_list = [credentials] if credentials else []
    else:
        # Query all sites
        credentials_list = list_all_wordpress_credentials(args.source)

    if args.json:
        import json
        # Mask passwords unless --show-password is specified
        output = []
        for cred in credentials_list:
            c = cred.copy()
            if not args.show_password:
                c['db_password'] = '********'
            output.append(c)
        print(json.dumps(output, indent=2))
    else:
        print_credentials_table(credentials_list, show_password=args.show_password)


if __name__ == '__main__':
    main()
