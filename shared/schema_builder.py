#!/usr/bin/env python3
"""
Schema Builder for BigQuery Tables
==================================

Builds explicit BigQuery schemas with proper field ordering:
1. Primary keys first
2. Business/data fields
3. Tracking/metadata fields last

Author: Data Pipeline Team
Created: 2025-01-03
"""

from typing import List, Dict, Any, Optional
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)

# DEPRECATION NOTICE:
# This file is deprecated. Use shared.schema_registry instead.
# Kept for backward compatibility only.


def build_facebook_schema(table_name: str) -> List[bigquery.SchemaField]:
    """
    Build explicit schema for Facebook tables with proper field ordering.
    
    Args:
        table_name: Name of the table (campaigns, adsets, ads, etc.)
    
    Returns:
        List of BigQuery SchemaField objects in proper order
    """
    
    # Common Facebook schemas by table
    schemas = {
        'campaigns': [
            # PRIMARY KEYS FIRST
            bigquery.SchemaField("account_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),  # Normalized to STRING
            
            # FACEBOOK DATA FIELDS
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("effective_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("objective", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("bid_strategy", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("daily_budget", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("lifetime_budget", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("created_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("start_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("stop_time", "TIMESTAMP", mode="NULLABLE"),
            
            # TRACKING/METADATA FIELDS LAST
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("execution_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("row_hash", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ],
        
        'adsets': [
            # PRIMARY KEYS
            bigquery.SchemaField("account_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            
            # FACEBOOK DATA
            bigquery.SchemaField("campaign_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("effective_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("targeting", "STRING", mode="NULLABLE"),  # JSON string
            bigquery.SchemaField("daily_budget", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("lifetime_budget", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("bid_amount", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("bid_strategy", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("billing_event", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("optimization_goal", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("start_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("end_time", "TIMESTAMP", mode="NULLABLE"),
            
            # TRACKING/METADATA
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("execution_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("row_hash", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ],
        
        'ads': [
            # PRIMARY KEYS
            bigquery.SchemaField("account_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),

            # FACEBOOK DATA
            bigquery.SchemaField("campaign_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("adset_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("effective_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("configured_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("creative", "STRING", mode="NULLABLE"),  # JSON string
            bigquery.SchemaField("creative_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("bid_amount", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("bid_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_time", "TIMESTAMP", mode="NULLABLE"),

            # TRACKING/METADATA
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("execution_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("row_hash", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ],

        'campaign_insights': [
            # PRIMARY KEYS
            bigquery.SchemaField("account_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("campaign_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("date_start", "DATE", mode="NULLABLE"),

            # CAMPAIGN INFO
            bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("objective", "STRING", mode="NULLABLE"),

            # DATE FIELDS
            bigquery.SchemaField("date_stop", "DATE", mode="NULLABLE"),

            # PERFORMANCE METRICS
            bigquery.SchemaField("impressions", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("clicks", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("spend", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("reach", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("frequency", "FLOAT64", mode="NULLABLE"),

            # CALCULATED METRICS
            bigquery.SchemaField("cpm", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("cpc", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("ctr", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("conversions", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_conversion", "FLOAT64", mode="NULLABLE"),

            # COMPLEX FIELDS (JSON)
            bigquery.SchemaField("actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("action_values", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("action_values_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_action_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_action_type_json", "STRING", mode="NULLABLE"),

            # TRACKING/METADATA
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("execution_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("row_hash", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ],

        'ad_insights_actions': [
            # PRIMARY KEYS
            bigquery.SchemaField("account_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ad_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("date_start", "DATE", mode="NULLABLE"),

            # HIERARCHY
            bigquery.SchemaField("campaign_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("adset_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("adset_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ad_name", "STRING", mode="NULLABLE"),

            # DATES
            bigquery.SchemaField("date_stop", "DATE", mode="NULLABLE"),

            # BASIC METRICS
            bigquery.SchemaField("impressions", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("clicks", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("spend", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("reach", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("frequency", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("unique_clicks", "INT64", mode="NULLABLE"),

            # CALCULATED METRICS
            bigquery.SchemaField("cpc", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("cpm", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("cpp", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("ctr", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("unique_ctr", "FLOAT64", mode="NULLABLE"),

            # CONVERSIONS
            bigquery.SchemaField("conversions", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_conversion", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("conversion_rate_ranking", "STRING", mode="NULLABLE"),

            # ROAS METRICS
            bigquery.SchemaField("purchase_roas", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("website_purchase_roas", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("mobile_app_purchase_roas", "FLOAT64", mode="NULLABLE"),

            # ENGAGEMENT
            bigquery.SchemaField("outbound_clicks", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("unique_outbound_clicks", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("outbound_clicks_ctr", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("unique_outbound_clicks_ctr", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("inline_link_clicks", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("inline_link_click_ctr", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("unique_inline_link_clicks", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("unique_inline_link_click_ctr", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("inline_post_engagement", "INT64", mode="NULLABLE"),

            # QUALITY RANKINGS
            bigquery.SchemaField("engagement_rate_ranking", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("quality_ranking", "STRING", mode="NULLABLE"),

            # INSTANT EXPERIENCE
            bigquery.SchemaField("instant_experience_clicks_to_open", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("instant_experience_clicks_to_start", "INT64", mode="NULLABLE"),

            # OTHER
            bigquery.SchemaField("catalog_segment_value", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("website_ctr", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("social_spend", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("estimated_ad_recall_rate", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("estimated_ad_recallers", "INT64", mode="NULLABLE"),

            # COMPLEX FIELDS - Stored as JSON strings
            bigquery.SchemaField("actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("action_values", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("action_values_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_action_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_action_type_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_unique_action_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_unique_action_type_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("unique_actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("unique_actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("conversion_values", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("conversion_values_json", "STRING", mode="NULLABLE"),

            # VIDEO METRICS - Stored as JSON strings
            bigquery.SchemaField("video_play_actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_play_actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_p25_watched_actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_p25_watched_actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_p50_watched_actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_p50_watched_actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_p75_watched_actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_p75_watched_actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_p95_watched_actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_p95_watched_actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_p100_watched_actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_p100_watched_actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_avg_time_watched_actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_avg_time_watched_actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_time_watched_actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_time_watched_actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_thruplay_watched_actions", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_thruplay_watched_actions_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_thruplay", "FLOAT64", mode="NULLABLE"),

            # TRACKING/METADATA
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("execution_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("row_hash", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ]
    }

    schema = schemas.get(table_name)
    if schema:
        logger.info(f"Built explicit schema for {table_name} with {len(schema)} fields")
        return schema
    
    # Return minimal schema if table not defined
    logger.warning(f"No explicit schema for {table_name}, using minimal schema")
    return [
        bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("account_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("execution_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("row_hash", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
    ]


def get_schema_for_table(source: str, table_name: str) -> Optional[List[bigquery.SchemaField]]:
    """
    Get explicit schema for a given source and table.

    DEPRECATED: Use shared.schema_registry.get_bq_schema() instead.
    This function is kept for backward compatibility only.

    Args:
        source: Data source (facebook, wordpress, etc.)
        table_name: Table name

    Returns:
        Schema field list or None for auto-detect

    Migration:
        Old: schema = get_schema_for_table('facebook', 'campaigns')
        New: from shared.schema_registry import get_bq_schema
             schema = get_bq_schema('facebook', 'campaigns')
    """
    # Delegate to new schema registry
    try:
        from shared.schema_registry import get_bq_schema
        schema = get_bq_schema(source, table_name, include_etl_fields=True, use_cache=True)
        if schema:
            return schema
    except Exception as e:
        logger.debug(f"Schema registry not available, falling back to hardcoded schema: {e}")

    # Fallback to old hardcoded behavior for backward compatibility
    if source.lower() == 'facebook':
        return build_facebook_schema(table_name)

    # Add other sources as needed
    return None

