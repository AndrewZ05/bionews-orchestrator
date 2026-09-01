"""Shared pipeline orchestration helpers for extractors."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional, Set

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from shared.bigquery_utils import ensure_dataset_exists
from shared.extractor_utils import apply_table_affix


@dataclass
class PipelineEnvironment:
    """Normalized configuration for extractor pipelines."""

    project: str
    source_name: str
    staging_dataset: str
    production_dataset: str
    bucket_name: Optional[str]
    ttl_days: Optional[int]
    schema_prefix: Optional[str]
    schema_suffix: Optional[str]
    execution_id: Optional[str]
    rebuild: bool = False
    dropped_tables: Set[str] = field(default_factory=set)

    def format_table(
        self,
        table_name: str,
        *,
        prefix_separator: str = "",
        suffix_separator: str = "",
    ) -> str:
        return apply_table_affix(
            table_name,
            self.schema_prefix,
            self.schema_suffix,
            prefix_separator=prefix_separator,
            suffix_separator=suffix_separator,
        )


def initialize_pipeline_environment(
    config: dict,
    *,
    bq_client: bigquery.Client,
    source_default: str,
    schema_prefix: Optional[str],
    schema_suffix: Optional[str],
    execution_id: Optional[str],
    rebuild: bool,
    require_bucket: bool = False,
    default_bucket_env: str = "GCS_BUCKET",
    default_project_env: str = "GCP_PROJECT_ID",
    default_staging_suffix: str = "_staging",
    default_production_suffix: str = "_data",
    default_ttl: Optional[int] = None,
) -> PipelineEnvironment:
    """Create a normalized pipeline environment for an extractor."""

    pipeline_cfg = config.get("pipeline", {})
    source_name = config.get("source", {}).get("name", source_default)

    project = (
        pipeline_cfg.get("project")
        or os.getenv(default_project_env)
        or getattr(bq_client, "project", None)
    )
    if not project:
        raise ValueError("GCP project not configured for extractor")

    staging_dataset = (
        pipeline_cfg.get("staging_dataset")
        or f"{source_name}{default_staging_suffix}"
    )
    production_dataset = (
        pipeline_cfg.get("production_dataset")
        or pipeline_cfg.get("dataset")
        or f"{source_name}{default_production_suffix}"
    )

    ensure_dataset_exists(bq_client, staging_dataset)
    ensure_dataset_exists(bq_client, production_dataset)

    bucket_name = pipeline_cfg.get("gcs_bucket") or os.getenv(default_bucket_env)
    if require_bucket and not bucket_name:
        raise ValueError("GCS bucket not configured for extractor")

    ttl_days = pipeline_cfg.get("ttl_days", default_ttl)

    return PipelineEnvironment(
        project=project,
        source_name=source_name,
        staging_dataset=staging_dataset,
        production_dataset=production_dataset,
        bucket_name=bucket_name,
        ttl_days=ttl_days,
        schema_prefix=schema_prefix,
        schema_suffix=schema_suffix,
        execution_id=execution_id,
        rebuild=rebuild,
    )


def drop_production_table_if_needed(
    env: PipelineEnvironment,
    bq_client: bigquery.Client,
    table_name: str,
) -> None:
    """Drop production table once per run when rebuild mode is enabled."""

    if not env.rebuild:
        return

    if table_name in env.dropped_tables:
        return

    full_table_id = f"{env.project}.{env.production_dataset}.{table_name}"
    try:
        bq_client.delete_table(full_table_id)
    except NotFound:
        pass
    env.dropped_tables.add(table_name)


__all__ = [
    "PipelineEnvironment",
    "initialize_pipeline_environment",
    "drop_production_table_if_needed",
]
