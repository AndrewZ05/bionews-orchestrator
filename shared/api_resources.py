#!/usr/bin/env python3
"""
Generic API Resource Discovery and Filtering
=============================================

Provides source-agnostic utilities for discovering and filtering API resources.

This module handles:
- Resource/collection discovery with multiple strategies
- Token-based authentication detection
- Resource filtering based on user parameters
- Account/ID normalization

Can be used with any API that has discoverable resources (Facebook pages,
WordPress sites, generic REST collections, etc.)

Usage:
    from shared.api_resources import filter_resources, normalize_account_ids

    # Filter pages based on user's --sites parameter
    filtered_pages = filter_resources(
        resources=all_pages,
        filter_values=sites_param,
        resource_id_field='id',
        exclude_patterns=['act_', 'all']
    )

Author: Data Pipeline Team
Created: 2025-01-20
"""

import logging
from typing import List, Dict, Any, Optional, Callable, Set

logger = logging.getLogger(__name__)


def discover_resources_with_fallback(
    primary_discovery: Callable[[], List[Dict[str, Any]]],
    fallback_strategies: List[Callable[[], List[Dict[str, Any]]]],
    resource_name: str = 'resources'
) -> List[Dict[str, Any]]:
    """
    Discover resources using primary method with multiple fallback strategies.

    This handles cases where different authentication methods require different
    discovery approaches (e.g., User token vs Page token in Facebook).

    Args:
        primary_discovery: Primary discovery function
            Should return list of resource dictionaries
        fallback_strategies: List of fallback functions to try if primary fails
            Each should return list of resource dictionaries
        resource_name: Resource type name for logging

    Returns:
        List of discovered resource dictionaries

    Raises:
        Exception: If all strategies fail

    Example:
        def discover_via_user_token():
            return user.get_accounts()

        def discover_via_page_token():
            return [page.get_info() for page in page_ids]

        resources = discover_resources_with_fallback(
            primary_discovery=discover_via_user_token,
            fallback_strategies=[discover_via_page_token],
            resource_name='pages'
        )
    """
    all_errors = []

    # Try primary discovery
    try:
        logger.debug(f"  Attempting primary discovery for {resource_name}...")
        resources = primary_discovery()
        logger.info(
            f"  Primary discovery succeeded: found {len(resources)} {resource_name}"
        )
        return resources
    except Exception as primary_err:
        logger.debug(f"  Primary discovery failed: {primary_err}")
        all_errors.append(('primary', primary_err))

    # Try fallback strategies
    for idx, fallback_fn in enumerate(fallback_strategies, 1):
        try:
            logger.debug(
                f"  Attempting fallback strategy {idx}/{len(fallback_strategies)} "
                f"for {resource_name}..."
            )
            resources = fallback_fn()
            logger.info(
                f"  Fallback strategy {idx} succeeded: "
                f"found {len(resources)} {resource_name}"
            )
            return resources
        except Exception as fallback_err:
            logger.debug(f"  Fallback strategy {idx} failed: {fallback_err}")
            all_errors.append((f'fallback_{idx}', fallback_err))

    # All strategies failed
    error_summary = "\n".join([
        f"  - {name}: {str(err)[:100]}"
        for name, err in all_errors
    ])
    raise Exception(
        f"Failed to discover {resource_name} using all strategies:\n{error_summary}"
    )


def filter_resources(
    resources: List[Dict[str, Any]],
    filter_values: Optional[List[str]],
    resource_id_field: str = 'id',
    resource_name_field: Optional[str] = 'name',
    exclude_patterns: Optional[List[str]] = None,
    allow_all_keyword: str = 'all',
    allow_empty: bool = True
) -> List[Dict[str, Any]]:
    """
    Filter resources based on user-provided filter values.

    Handles common filtering patterns:
    - If filter_values is None/empty → return all resources
    - If filter_values contains 'all' → return all resources
    - If filter_values contains patterns to exclude → return all resources
    - Otherwise → filter to matching resources only

    Args:
        resources: List of resource dictionaries
        filter_values: List of resource IDs/names to filter by
        resource_id_field: Field name containing resource ID
        resource_name_field: Optional field name containing resource name
        exclude_patterns: Patterns that indicate "don't filter" (e.g., ['act_', 'all'])
        allow_all_keyword: Keyword that means "all resources" (default: 'all')
        allow_empty: Whether to allow empty results (if False, raises error)

    Returns:
        Filtered list of resources

    Raises:
        ValueError: If allow_empty=False and no resources match

    Example:
        # Filter to specific page IDs
        pages = filter_resources(
            resources=all_pages,
            filter_values=['123', '456'],
            resource_id_field='id',
            exclude_patterns=['act_']
        )

        # Return all (filter contains 'act_' pattern)
        pages = filter_resources(
            resources=all_pages,
            filter_values=['act_123'],  # Has 'act_' pattern
            exclude_patterns=['act_']
        )
        # Returns all_pages (not filtered)
    """
    # No filter specified - return all
    if not filter_values:
        logger.debug(
            f"  No filter specified - returning all {len(resources)} resources"
        )
        return resources

    # Check for exclude patterns
    if exclude_patterns:
        has_exclude_pattern = any(
            any(pattern in value for pattern in exclude_patterns)
            for value in filter_values
        )
        if has_exclude_pattern:
            logger.debug(
                f"  Filter contains exclude pattern - returning all {len(resources)} resources"
            )
            return resources

    # Check for 'all' keyword
    if allow_all_keyword in filter_values:
        logger.debug(
            f"  Filter contains '{allow_all_keyword}' - returning all {len(resources)} resources"
        )
        return resources

    # Filter to matching resources
    original_count = len(resources)
    filtered = []

    for resource in resources:
        resource_id = resource.get(resource_id_field)
        resource_name = resource.get(resource_name_field) if resource_name_field else None

        # Check if ID matches
        if resource_id and str(resource_id) in filter_values:
            filtered.append(resource)
            continue

        # Check if name matches (if name field provided)
        if resource_name and str(resource_name) in filter_values:
            filtered.append(resource)
            continue

    # Log filtering results
    if len(filtered) < original_count:
        logger.info(
            f"  Filtered to {len(filtered)} resources matching: {filter_values}"
        )

        if len(filtered) == 0:
            logger.warning(
                f"  No resources found matching filter: {filter_values}"
            )
            if original_count > 0:
                # Show available IDs for debugging
                sample_ids = [
                    r.get(resource_id_field)
                    for r in resources[:5]
                ]
                logger.warning(f"  Available IDs (sample): {sample_ids}")

            if not allow_empty:
                raise ValueError(
                    f"No resources match filter: {filter_values}. "
                    f"Available count: {original_count}"
                )

    return filtered


def normalize_account_id(
    account_id: str,
    prefix: str = 'act_',
    force_prefix: bool = True
) -> str:
    """
    Normalize account ID to ensure consistent format.

    Args:
        account_id: Account ID to normalize
        prefix: Expected prefix (e.g., 'act_' for Facebook ad accounts)
        force_prefix: Whether to add prefix if missing

    Returns:
        Normalized account ID

    Example:
        normalize_account_id('123456', prefix='act_')  # Returns: 'act_123456'
        normalize_account_id('act_123456', prefix='act_')  # Returns: 'act_123456'
    """
    if force_prefix and not account_id.startswith(prefix):
        return f'{prefix}{account_id}'
    return account_id


def normalize_account_ids(
    account_ids: List[str],
    prefix: str = 'act_',
    force_prefix: bool = True
) -> List[str]:
    """
    Normalize multiple account IDs.

    Args:
        account_ids: List of account IDs to normalize
        prefix: Expected prefix
        force_prefix: Whether to add prefix if missing

    Returns:
        List of normalized account IDs

    Example:
        ids = ['123', 'act_456', '789']
        normalize_account_ids(ids, prefix='act_')
        # Returns: ['act_123', 'act_456', 'act_789']
    """
    return [normalize_account_id(acc, prefix, force_prefix) for acc in account_ids]


def resolve_resource_identifiers(
    user_input: Optional[List[str]],
    available_resources: List[Dict[str, Any]],
    resource_id_field: str = 'id',
    resource_name_field: str = 'name',
    auto_discover: bool = True
) -> List[str]:
    """
    Resolve user input to actual resource identifiers.

    Handles various input scenarios:
    - User provides IDs → validate and return
    - User provides names → resolve to IDs
    - User provides 'all' → return all resource IDs
    - User provides nothing and auto_discover=True → return all resource IDs

    Args:
        user_input: User-provided identifiers (IDs or names)
        available_resources: List of available resource dictionaries
        resource_id_field: Field containing resource ID
        resource_name_field: Field containing resource name
        auto_discover: Whether to return all IDs if no input provided

    Returns:
        List of resolved resource IDs

    Example:
        resources = [
            {'id': '123', 'name': 'Page A'},
            {'id': '456', 'name': 'Page B'}
        ]

        # Resolve names to IDs
        ids = resolve_resource_identifiers(
            user_input=['Page A'],
            available_resources=resources
        )
        # Returns: ['123']

        # Auto-discover all
        ids = resolve_resource_identifiers(
            user_input=None,
            available_resources=resources,
            auto_discover=True
        )
        # Returns: ['123', '456']
    """
    # Auto-discover if no input
    if not user_input:
        if auto_discover:
            all_ids = [r.get(resource_id_field) for r in available_resources]
            logger.info(
                f"  Auto-discovered {len(all_ids)} resources: {all_ids[:5]}..."
                if len(all_ids) > 5 else f"  Auto-discovered {len(all_ids)} resources: {all_ids}"
            )
            return all_ids
        else:
            return []

    # 'all' keyword - return all IDs
    if 'all' in user_input:
        all_ids = [r.get(resource_id_field) for r in available_resources]
        logger.info(f"  'all' specified - using {len(all_ids)} resources")
        return all_ids

    # Build ID and name lookup maps
    id_to_resource = {
        str(r.get(resource_id_field)): r
        for r in available_resources
        if r.get(resource_id_field)
    }

    name_to_id = {
        str(r.get(resource_name_field)): r.get(resource_id_field)
        for r in available_resources
        if r.get(resource_name_field) and r.get(resource_id_field)
    }

    # Resolve each input value
    resolved_ids = []
    unresolved = []

    for value in user_input:
        value_str = str(value)

        # Try as ID first
        if value_str in id_to_resource:
            resolved_ids.append(value_str)
        # Try as name
        elif value_str in name_to_id:
            resolved_id = name_to_id[value_str]
            resolved_ids.append(str(resolved_id))
            logger.debug(f"  Resolved name '{value_str}' to ID '{resolved_id}'")
        else:
            unresolved.append(value_str)

    # Warn about unresolved values
    if unresolved:
        logger.warning(
            f"  Could not resolve {len(unresolved)} values: {unresolved}"
        )

    if resolved_ids:
        logger.info(f"  Resolved to {len(resolved_ids)} resource IDs")

    return resolved_ids


def group_resources_by_attribute(
    resources: List[Dict[str, Any]],
    attribute: str
) -> Dict[Any, List[Dict[str, Any]]]:
    """
    Group resources by a shared attribute.

    Useful for processing resources in batches based on shared characteristics
    (e.g., same access token, same account, same region).

    Args:
        resources: List of resource dictionaries
        attribute: Attribute name to group by

    Returns:
        Dictionary mapping attribute value to list of resources

    Example:
        pages = [
            {'id': '1', 'account_id': 'acc1'},
            {'id': '2', 'account_id': 'acc1'},
            {'id': '3', 'account_id': 'acc2'},
        ]
        groups = group_resources_by_attribute(pages, 'account_id')
        # Returns: {
        #   'acc1': [page1, page2],
        #   'acc2': [page3]
        # }
    """
    groups: Dict[Any, List[Dict[str, Any]]] = {}

    for resource in resources:
        key = resource.get(attribute)
        if key not in groups:
            groups[key] = []
        groups[key].append(resource)

    return groups


def deduplicate_resources(
    resources: List[Dict[str, Any]],
    unique_field: str = 'id',
    keep: str = 'first'
) -> List[Dict[str, Any]]:
    """
    Remove duplicate resources based on unique field.

    Args:
        resources: List of resource dictionaries
        unique_field: Field to use for uniqueness check
        keep: Which duplicate to keep ('first' or 'last')

    Returns:
        Deduplicated list of resources

    Example:
        resources = [
            {'id': '123', 'name': 'Page A'},
            {'id': '123', 'name': 'Page A (duplicate)'},
            {'id': '456', 'name': 'Page B'}
        ]
        unique = deduplicate_resources(resources, unique_field='id')
        # Returns: [{'id': '123', 'name': 'Page A'}, {'id': '456', 'name': 'Page B'}]
    """
    seen: Set[Any] = set()
    deduped = []

    if keep == 'last':
        resources = reversed(resources)

    for resource in resources:
        key = resource.get(unique_field)
        if key and key not in seen:
            seen.add(key)
            deduped.append(resource)

    if keep == 'last':
        deduped = list(reversed(deduped))

    original_count = len(resources) if keep == 'first' else len(list(resources))
    if len(deduped) < original_count:
        logger.info(
            f"  Removed {original_count - len(deduped)} duplicate resources "
            f"(based on '{unique_field}')"
        )

    return deduped
