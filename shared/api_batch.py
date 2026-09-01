#!/usr/bin/env python3
"""
Generic API Batch Request Utilities
====================================

Provides source-agnostic batch request execution for APIs that support batching.

This module handles:
- Batch request construction
- Automatic batching (splits large requests into chunks)
- Response processing with custom processors
- Error handling for individual requests
- Fallback handling for failed batches

Can be used with any batch-capable API (Facebook Batch API, etc.)

Usage:
    from shared.api_batch import execute_batch_requests

    def process_response(response_body, metadata):
        # Your response processing logic
        return processed_data

    results = execute_batch_requests(
        requests=[{'method': 'GET', 'relative_url': '/page/123'}],
        metadata=[{'page_id': '123'}],
        batch_executor=my_batch_fn,
        response_processor=process_response
    )

Author: Data Pipeline Team
Created: 2025-01-20
"""

import json
import logging
from typing import Callable, List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


def execute_batch_requests(
    requests: List[Dict[str, Any]],
    metadata: List[Dict[str, Any]],
    batch_executor: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]],
    response_processor: Callable[[Dict[str, Any], Dict[str, Any]], Any],
    batch_size: int = 50,
    fallback_handler: Optional[Callable[[Dict[str, Any], Dict[str, Any]], Any]] = None,
    resource_name: str = 'batch',
    success_code: int = 200
) -> List[Any]:
    """
    Execute batch API requests with automatic batching and error handling.

    This is a source-agnostic implementation that works with any batch API.
    The caller provides the actual batch execution function.

    Args:
        requests: List of request dictionaries
            Format depends on the API (e.g., Facebook uses {'method': 'GET', 'relative_url': '...'})
        metadata: Metadata for each request (same length as requests)
            Used by response_processor to add context
        batch_executor: Function that executes a batch of requests
            Signature: (batch: List[Dict]) -> List[Dict] (responses)
            Should return list of response dicts with 'code' and 'body' keys
        response_processor: Function to process each successful response
            Signature: (response_body: Dict, metadata: Dict) -> processed_result
            Return None to skip adding result to output
        batch_size: Maximum requests per batch (API-dependent limit)
        fallback_handler: Optional handler for failed individual requests
            Signature: (request: Dict, metadata: Dict) -> processed_result
            Called if batch execution fails for a request
        resource_name: Resource name for logging
        success_code: HTTP code that indicates success (default: 200)

    Returns:
        List of processed results from all successful requests

    Example:
        def execute_facebook_batch(batch):
            response = requests.post(
                'https://graph.facebook.com/v25.0',
                data={'access_token': token, 'batch': json.dumps(batch)}
            )
            return response.json()

        def process_insights(body, meta):
            return {
                'page_id': meta['page_id'],
                'impressions': body.get('data', [{}])[0].get('values', [{}])[0].get('value', 0)
            }

        results = execute_batch_requests(
            requests=batch_requests,
            metadata=request_metadata,
            batch_executor=execute_facebook_batch,
            response_processor=process_insights
        )
    """
    if len(requests) != len(metadata):
        raise ValueError(
            f"requests and metadata must have same length "
            f"(got {len(requests)} requests, {len(metadata)} metadata)"
        )

    total_requests = len(requests)
    total_batches = (total_requests + batch_size - 1) // batch_size
    all_results = []

    logger.info(
        f"  [{resource_name}] Executing {total_requests} requests "
        f"in {total_batches} batches (size: {batch_size})"
    )

    # Process in batches
    for batch_idx in range(0, total_requests, batch_size):
        batch_num = (batch_idx // batch_size) + 1
        batch_end = min(batch_idx + batch_size, total_requests)

        batch_requests = requests[batch_idx:batch_end]
        batch_metadata = metadata[batch_idx:batch_end]
        batch_count = len(batch_requests)

        logger.info(
            f"  [{resource_name}] Batch {batch_num}/{total_batches}: "
            f"Processing {batch_count} requests..."
        )

        try:
            # Execute batch using provided executor
            batch_responses = batch_executor(batch_requests)

            # Validate response structure
            if not isinstance(batch_responses, list):
                raise ValueError(
                    f"batch_executor must return a list, got {type(batch_responses)}"
                )

            if len(batch_responses) != len(batch_requests):
                logger.warning(
                    f"  [{resource_name}] Response count mismatch: "
                    f"expected {len(batch_requests)}, got {len(batch_responses)}"
                )

            # Process each response in the batch
            success_count = 0
            error_count = 0

            for i, response in enumerate(batch_responses):
                if i >= len(batch_metadata):
                    logger.warning(
                        f"  [{resource_name}] No metadata for response {i+1}, skipping"
                    )
                    continue

                req_metadata = batch_metadata[i]
                response_code = response.get('code')

                if response_code == success_code:
                    # Successful response - process it
                    try:
                        # Parse body if it's a string
                        body = response.get('body')
                        if isinstance(body, str):
                            body = json.loads(body)

                        # Call custom processor
                        processed = response_processor(body, req_metadata)

                        # Add to results if not None
                        if processed is not None:
                            all_results.append(processed)
                            success_count += 1

                    except Exception as process_err:
                        logger.warning(
                            f"  [{resource_name}] Failed to process response {i+1}: "
                            f"{process_err}"
                        )
                        error_count += 1

                else:
                    # Failed response
                    error_body = response.get('body', 'Unknown error')
                    if isinstance(error_body, dict):
                        error_body = json.dumps(error_body)

                    logger.warning(
                        f"  [{resource_name}] Request {i+1} failed: "
                        f"HTTP {response_code} - {str(error_body)[:150]}"
                    )
                    error_count += 1

            logger.info(
                f"  [{resource_name}] Batch {batch_num} complete: "
                f"{success_count} success, {error_count} errors"
            )

        except Exception as batch_err:
            # Entire batch failed
            logger.error(
                f"  [{resource_name}] Batch {batch_num} execution failed: {batch_err}"
            )

            # Try fallback if provided
            if fallback_handler:
                logger.info(
                    f"  [{resource_name}] Attempting fallback for {batch_count} requests..."
                )
                fallback_success = 0

                for i, (req, meta) in enumerate(zip(batch_requests, batch_metadata)):
                    try:
                        result = fallback_handler(req, meta)
                        if result is not None:
                            all_results.append(result)
                            fallback_success += 1
                    except Exception as fallback_err:
                        logger.warning(
                            f"  [{resource_name}] Fallback failed for request {i+1}: "
                            f"{fallback_err}"
                        )

                logger.info(
                    f"  [{resource_name}] Fallback complete: "
                    f"{fallback_success}/{batch_count} recovered"
                )
            else:
                logger.warning(
                    f"  [{resource_name}] No fallback handler - {batch_count} requests lost"
                )

    logger.info(
        f"  [{resource_name}] Batch execution complete: "
        f"{len(all_results)} total results from {total_requests} requests"
    )

    return all_results


def create_batch_request(
    method: str,
    relative_url: str,
    body: Optional[Dict[str, Any]] = None,
    omit_response_on_success: bool = False
) -> Dict[str, Any]:
    """
    Create a batch request dictionary (Facebook format).

    Args:
        method: HTTP method ('GET', 'POST', etc.)
        relative_url: Relative URL path (without base URL)
        body: Optional request body (for POST requests)
        omit_response_on_success: Whether to omit response body on success

    Returns:
        Batch request dictionary

    Example:
        request = create_batch_request(
            method='GET',
            relative_url='/page/123/insights'
        )
    """
    request = {
        'method': method,
        'relative_url': relative_url
    }

    if body:
        request['body'] = json.dumps(body) if isinstance(body, dict) else body

    if omit_response_on_success:
        request['omit_response_on_success'] = True

    return request


def batch_by_attribute(
    items: List[Dict[str, Any]],
    attribute: str,
    max_batch_size: int = 50
) -> List[Tuple[List[Dict[str, Any]], Any]]:
    """
    Group items into batches based on a shared attribute.

    Useful when batching requests that must share an attribute
    (e.g., same access token, same account).

    Args:
        items: List of items to batch
        attribute: Attribute name to group by
        max_batch_size: Maximum items per batch

    Returns:
        List of (batch_items, shared_value) tuples

    Example:
        # Batch requests by access_token
        items = [
            {'url': '/page/1', 'token': 'abc'},
            {'url': '/page/2', 'token': 'abc'},
            {'url': '/page/3', 'token': 'xyz'},
        ]
        batches = batch_by_attribute(items, 'token', max_batch_size=50)
        # Returns: [
        #   ([item1, item2], 'abc'),
        #   ([item3], 'xyz')
        # ]
    """
    # Group by attribute value
    groups: Dict[Any, List[Dict[str, Any]]] = {}
    for item in items:
        key = item.get(attribute)
        if key not in groups:
            groups[key] = []
        groups[key].append(item)

    # Split large groups into batches
    batches = []
    for key, group_items in groups.items():
        for i in range(0, len(group_items), max_batch_size):
            batch = group_items[i:i + max_batch_size]
            batches.append((batch, key))

    return batches


def merge_batch_results(
    base_record: Dict[str, Any],
    batch_results: List[Dict[str, Any]],
    merge_key: str,
    target_fields: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Merge multiple batch response results into a single record.

    Useful when making multiple batch requests for the same resource
    (e.g., fetching different metric groups for the same page).

    Args:
        base_record: Base record to merge into
        batch_results: List of results from batch responses
        merge_key: Key to match results to base record
        target_fields: Optional list of fields to merge (default: all fields)

    Returns:
        Merged record

    Example:
        base = {'page_id': '123', 'name': 'My Page'}
        results = [
            {'page_id': '123', 'impressions': 1000},
            {'page_id': '123', 'clicks': 50}
        ]
        merged = merge_batch_results(base, results, 'page_id')
        # Returns: {'page_id': '123', 'name': 'My Page', 'impressions': 1000, 'clicks': 50}
    """
    merged = base_record.copy()
    base_key_value = base_record.get(merge_key)

    for result in batch_results:
        if result.get(merge_key) == base_key_value:
            # Match found - merge fields
            for key, value in result.items():
                if key == merge_key:
                    continue  # Skip the merge key itself

                if target_fields and key not in target_fields:
                    continue  # Skip non-target fields

                merged[key] = value

    return merged
