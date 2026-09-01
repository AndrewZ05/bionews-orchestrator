#!/usr/bin/env python3
"""
Comprehensive review of lime_surveys_wide table.

Analyzes data quality, schema design, performance characteristics, and standardization coverage.
Generates actionable recommendations for improving the pipeline.

Usage:
    python shared/limesurvey_review_wide_table.py
    python shared/limesurvey_review_wide_table.py --data-quality
    python shared/limesurvey_review_wide_table.py --output report.md
"""

import os
import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import defaultdict

# Suppress gRPC warnings
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GLOG_minloglevel'] = '3'

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Load .env from project root
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'
WIDE_TABLE = f'{PROJECT_ID}.{DATASET}.lime_surveys_wide'
COLUMNAR_TABLE = f'{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed'

# Metadata columns that should be excluded from question analysis
METADATA_COLUMNS = [
    'survey_id', 'response_id', 'submitdate', 'lastpage', 'startdate',
    'datestamp', 'ipaddr', 'refurl', 'extracted_at', 'source', 'execution_id',
    'survey_gsid', 'survey_owner_id', 'survey_admin', 'survey_language'
]


def get_bq_client() -> bigquery.Client:
    """Get BigQuery client with service account credentials"""
    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_path and os.path.exists(creds_path):
        credentials = service_account.Credentials.from_service_account_file(creds_path)
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    return bigquery.Client(project=PROJECT_ID)


def run_query(client: bigquery.Client, query: str, description: str = "") -> Optional[List[Dict[str, Any]]]:
    """Run a query and return results as list of dicts"""
    try:
        if description:
            print(f"  Running: {description}")
        results = client.query(query)
        rows = []
        for row in results:
            rows.append(dict(row))
        return rows
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def get_question_columns(client: bigquery.Client) -> List[str]:
    """Get list of question column names (excluding metadata columns)"""
    query = f"""
    SELECT column_name
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'lime_surveys_wide'
      AND column_name NOT IN ({', '.join([f"'{c}'" for c in METADATA_COLUMNS])})
    ORDER BY ordinal_position
    """
    results = run_query(client, query, "Getting question columns")
    if results:
        return [r['column_name'] for r in results]
    return []


def analyze_data_quality(client: bigquery.Client) -> Dict[str, Any]:
    """Run completeness, duplicate, and multi-select checks"""
    print("\n" + "="*80)
    print(" DATA QUALITY ANALYSIS ".center(80))
    print("="*80)
    
    findings = {
        'completeness': {},
        'duplicates': {},
        'multiselect': {},
        'type_consistency': {}
    }
    
    # Row count validation
    print("\n[1] Row Count Validation")
    print("-"*80)
    query = f"""
    SELECT 
        COUNT(*) as wide_table_rows,
        (SELECT COUNT(DISTINCT CONCAT(CAST(survey_id AS STRING), '-', response_id))
         FROM `{COLUMNAR_TABLE}`) as source_rows
    FROM `{WIDE_TABLE}`
    """
    results = run_query(client, query, "Row count comparison")
    if results:
        row = results[0]
        wide_rows = row['wide_table_rows']
        source_rows = row['source_rows']
        diff = wide_rows - source_rows
        findings['completeness']['wide_table_rows'] = wide_rows
        findings['completeness']['source_rows'] = source_rows
        findings['completeness']['row_diff'] = diff
        findings['completeness']['match'] = diff == 0
        
        print(f"  Wide table rows: {wide_rows:,}")
        print(f"  Source unique (survey_id, response_id): {source_rows:,}")
        print(f"  Difference: {diff:,} {'✓ MATCH' if diff == 0 else '✗ MISMATCH'}")
    
    # NULL analysis in key columns
    print("\n[2] NULL Analysis in Key Columns")
    print("-"*80)
    query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNTIF(survey_id IS NULL) as null_survey_id,
        COUNTIF(response_id IS NULL) as null_response_id,
        COUNTIF(submitdate IS NULL) as null_submitdate,
        ROUND(COUNTIF(survey_id IS NULL) * 100.0 / COUNT(*), 2) as pct_null_survey_id,
        ROUND(COUNTIF(response_id IS NULL) * 100.0 / COUNT(*), 2) as pct_null_response_id,
        ROUND(COUNTIF(submitdate IS NULL) * 100.0 / COUNT(*), 2) as pct_null_submitdate
    FROM `{WIDE_TABLE}`
    """
    results = run_query(client, query, "NULL analysis")
    if results:
        row = results[0]
        findings['completeness']['null_analysis'] = row
        print(f"  Total rows: {row['total_rows']:,}")
        print(f"  NULL survey_id: {row['null_survey_id']:,} ({row['pct_null_survey_id']}%)")
        print(f"  NULL response_id: {row['null_response_id']:,} ({row['pct_null_response_id']}%)")
        print(f"  NULL submitdate: {row['null_submitdate']:,} ({row['pct_null_submitdate']}%)")
    
    # Question column completeness (top 20 by NULL count)
    print("\n[3] Question Column Completeness (Top 20 by NULL count)")
    print("-"*80)
    question_cols = get_question_columns(client)
    if question_cols:
        # Build dynamic query for NULL counts
        null_checks = []
        for col in question_cols[:50]:  # Limit to first 50 to avoid query size issues
            null_checks.append(f"COUNTIF({col} IS NULL) as null_{col}")
        
        if null_checks:
            query = f"""
            SELECT 
                {', '.join(null_checks)},
                COUNT(*) as total_rows
            FROM `{WIDE_TABLE}`
            """
            results = run_query(client, query, "Question column NULL analysis")
            if results and results[0]:
                row = results[0]
                total = row.get('total_rows', 0)
                null_counts = []
                for col in question_cols[:50]:
                    null_count = row.get(f'null_{col}', 0)
                    if null_count > 0:
                        pct = (null_count / total * 100) if total > 0 else 0
                        null_counts.append({
                            'column': col,
                            'null_count': null_count,
                            'pct_null': round(pct, 2)
                        })
                
                null_counts.sort(key=lambda x: x['null_count'], reverse=True)
                findings['completeness']['question_nulls'] = null_counts[:20]
                
                print(f"  Columns with NULLs: {len(null_counts)}")
                for item in null_counts[:10]:
                    print(f"    {item['column']}: {item['null_count']:,} ({item['pct_null']}%)")
    
    # Duplicate detection
    print("\n[4] Duplicate Detection")
    print("-"*80)
    query = f"""
    SELECT 
        survey_id,
        response_id,
        COUNT(*) as duplicate_count
    FROM `{WIDE_TABLE}`
    GROUP BY survey_id, response_id
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC
    LIMIT 10
    """
    results = run_query(client, query, "Duplicate (survey_id, response_id) pairs")
    if results:
        findings['duplicates']['duplicate_pairs'] = results
        if results:
            print(f"  Found {len(results)} duplicate (survey_id, response_id) pairs:")
            for row in results[:5]:
                print(f"    Survey {row['survey_id']}, Response {row['response_id']}: {row['duplicate_count']} rows")
        else:
            print("  ✓ No duplicates found")
    
    # Multi-select validation
    print("\n[5] Multi-Select Validation")
    print("-"*80)
    question_cols = get_question_columns(client)
    if question_cols:
        # Check for pipe-delimited values in each column
        multiselect_findings = []
        for col in question_cols[:30]:  # Check first 30 columns
            query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNTIF({col} LIKE '%|%') as multiselect_count,
                MAX(ARRAY_LENGTH(SPLIT({col}, '|'))) as max_selections
            FROM `{WIDE_TABLE}`
            WHERE {col} IS NOT NULL
            """
            results = run_query(client, query, f"Multi-select check: {col}")
            if results and results[0]:
                row = results[0]
                if row['multiselect_count'] > 0:
                    pct = (row['multiselect_count'] / row['total_rows'] * 100) if row['total_rows'] > 0 else 0
                    multiselect_findings.append({
                        'column': col,
                        'multiselect_count': row['multiselect_count'],
                        'total_rows': row['total_rows'],
                        'pct_multiselect': round(pct, 2),
                        'max_selections': row['max_selections']
                    })
        
        findings['multiselect']['columns'] = multiselect_findings
        if multiselect_findings:
            print(f"  Found {len(multiselect_findings)} columns with multi-select values:")
            for item in multiselect_findings[:10]:
                print(f"    {item['column']}: {item['multiselect_count']:,} rows ({item['pct_multiselect']}%), max {item['max_selections']} selections")
        else:
            print("  No multi-select patterns detected")
    
    return findings


def analyze_schema_design(client: bigquery.Client) -> Dict[str, Any]:
    """Review column naming, types, and schema evolution"""
    print("\n" + "="*80)
    print(" SCHEMA DESIGN ANALYSIS ".center(80))
    print("="*80)
    
    findings = {
        'column_naming': {},
        'schema_info': {},
        'column_count': 0
    }
    
    # Column naming quality
    print("\n[1] Column Naming Quality")
    print("-"*80)
    query = f"""
    SELECT 
        column_name,
        data_type,
        CASE 
            WHEN column_name LIKE 'q_%' THEN 'Reserved word conflict (q_ prefix)'
            WHEN LENGTH(column_name) - LENGTH(REPLACE(column_name, '_', '')) > 3 THEN 'Too many underscores'
            WHEN REGEXP_CONTAINS(column_name, r'[0-9]') THEN 'Contains numbers'
            ELSE 'Normal'
        END as naming_quality,
        LENGTH(column_name) as name_length
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'lime_surveys_wide'
      AND column_name NOT IN ({', '.join([f"'{c}'" for c in METADATA_COLUMNS])})
    ORDER BY name_length DESC
    """
    results = run_query(client, query, "Column naming analysis")
    if results:
        findings['column_naming']['columns'] = results
        issues = [r for r in results if r['naming_quality'] != 'Normal']
        print(f"  Total question columns: {len(results)}")
        print(f"  Columns with naming issues: {len(issues)}")
        if issues:
            print("  Issues found:")
            for row in issues[:10]:
                print(f"    {row['column_name']}: {row['naming_quality']}")
        else:
            print("  ✓ All column names are well-formed")
    
    # Schema info
    print("\n[2] Schema Information")
    print("-"*80)
    query = f"""
    SELECT 
        COUNT(*) as total_columns,
        COUNT(DISTINCT data_type) as distinct_types,
        STRING_AGG(DISTINCT data_type, ', ') as data_types
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'lime_surveys_wide'
    """
    results = run_query(client, query, "Schema summary")
    if results:
        row = results[0]
        findings['schema_info'] = row
        findings['column_count'] = row['total_columns']
        print(f"  Total columns: {row['total_columns']}")
        print(f"  Data types: {row['data_types']}")
        print(f"  Metadata columns: {len(METADATA_COLUMNS)}")
        print(f"  Question columns: {row['total_columns'] - len(METADATA_COLUMNS)}")
    
    # Column count over time (if execution_id tracking exists)
    print("\n[3] Column Count Over Time")
    print("-"*80)
    query = f"""
    SELECT 
        execution_id,
        MAX(extracted_at) as latest_extracted_at,
        COUNT(DISTINCT execution_id) as execution_count
    FROM `{WIDE_TABLE}`
    WHERE execution_id IS NOT NULL
    GROUP BY execution_id
    ORDER BY latest_extracted_at DESC
    LIMIT 5
    """
    results = run_query(client, query, "Execution tracking")
    if results:
        print(f"  Recent executions: {len(results)}")
        for row in results[:3]:
            print(f"    {row['execution_id']}: {row['latest_extracted_at']}")
    
    return findings


def analyze_performance(client: bigquery.Client) -> Dict[str, Any]:
    """Evaluate partitioning, clustering, and query patterns"""
    print("\n" + "="*80)
    print(" PERFORMANCE ANALYSIS ".center(80))
    print("="*80)
    
    findings = {
        'partitioning': {},
        'clustering': {},
        'query_patterns': {}
    }
    
    # Table size by partition
    print("\n[1] Table Size by Partition (Last 30 days)")
    print("-"*80)
    query = f"""
    SELECT 
        DATE(submitdate) as partition_date,
        COUNT(*) as row_count,
        COUNT(DISTINCT survey_id) as distinct_surveys,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_total
    FROM `{WIDE_TABLE}`
    WHERE submitdate IS NOT NULL
    GROUP BY partition_date
    ORDER BY partition_date DESC
    LIMIT 30
    """
    results = run_query(client, query, "Partition analysis")
    if results:
        findings['partitioning']['partitions'] = results
        total_rows = sum(r['row_count'] for r in results)
        print(f"  Total rows in last 30 partitions: {total_rows:,}")
        print(f"  Partitions analyzed: {len(results)}")
        if results:
            print("  Top 5 partitions by row count:")
            sorted_results = sorted(results, key=lambda x: x['row_count'], reverse=True)
            for row in sorted_results[:5]:
                print(f"    {row['partition_date']}: {row['row_count']:,} rows ({row['pct_of_total']}%)")
    
    # Clustering effectiveness
    print("\n[2] Clustering Effectiveness by survey_id")
    print("-"*80)
    query = f"""
    SELECT 
        survey_id,
        COUNT(*) as response_count,
        COUNT(DISTINCT DATE(submitdate)) as distinct_dates,
        MIN(submitdate) as earliest_response,
        MAX(submitdate) as latest_response
    FROM `{WIDE_TABLE}`
    GROUP BY survey_id
    ORDER BY response_count DESC
    LIMIT 20
    """
    results = run_query(client, query, "Clustering analysis")
    if results:
        findings['clustering']['top_surveys'] = results
        print(f"  Top 20 surveys by response count:")
        for row in results[:10]:
            print(f"    Survey {row['survey_id']}: {row['response_count']:,} responses, {row['distinct_dates']} dates")
    
    # Query pattern simulation
    print("\n[3] Query Pattern Simulation")
    print("-"*80)
    
    # Date range query
    query1 = f"""
    SELECT COUNT(*) as date_range_count
    FROM `{WIDE_TABLE}`
    WHERE DATE(submitdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """
    results1 = run_query(client, query1, "30-day date range query")
    if results1:
        findings['query_patterns']['date_range_30d'] = results1[0]['date_range_count']
        print(f"  30-day date range: {results1[0]['date_range_count']:,} rows")
    
    # Survey-specific query
    query2 = f"""
    SELECT COUNT(*) as survey_filter_count
    FROM `{WIDE_TABLE}`
    WHERE survey_id = (SELECT survey_id FROM `{WIDE_TABLE}` LIMIT 1)
    """
    results2 = run_query(client, query2, "Single survey filter query")
    if results2:
        findings['query_patterns']['single_survey'] = results2[0]['survey_filter_count']
        print(f"  Single survey filter: {results2[0]['survey_filter_count']:,} rows")
    
    return findings


def analyze_standardization(client: bigquery.Client) -> Dict[str, Any]:
    """Assess mapping coverage and quality"""
    print("\n" + "="*80)
    print(" STANDARDIZATION COVERAGE ANALYSIS ".center(80))
    print("="*80)
    
    findings = {
        'question_coverage': {},
        'unmapped_questions': {},
        'answer_quality': {}
    }
    
    # Question mapping coverage
    print("\n[1] Question Mapping Coverage")
    print("-"*80)
    query = f"""
    WITH wide_columns AS (
        SELECT column_name
        FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'lime_surveys_wide'
          AND column_name NOT IN ({', '.join([f"'{c}'" for c in METADATA_COLUMNS])})
    ),
    source_questions AS (
        SELECT DISTINCT standardized_question
        FROM `{COLUMNAR_TABLE}`
        WHERE standardized_question IS NOT NULL
    )
    SELECT 
        'In wide but not in source' as issue_type,
        wc.column_name as question_name
    FROM wide_columns wc
    LEFT JOIN source_questions sq ON wc.column_name = sq.standardized_question
    WHERE sq.standardized_question IS NULL

    UNION ALL

    SELECT 
        'In source but not in wide' as issue_type,
        sq.standardized_question as question_name
    FROM source_questions sq
    LEFT JOIN wide_columns wc ON sq.standardized_question = wc.column_name
    WHERE wc.column_name IS NULL
    """
    results = run_query(client, query, "Question coverage comparison")
    if results:
        in_wide_not_source = [r for r in results if r['issue_type'] == 'In wide but not in source']
        in_source_not_wide = [r for r in results if r['issue_type'] == 'In source but not in wide']
        findings['question_coverage']['in_wide_not_source'] = in_wide_not_source
        findings['question_coverage']['in_source_not_wide'] = in_source_not_wide
        
        print(f"  Columns in wide table but not in source: {len(in_wide_not_source)}")
        if in_wide_not_source:
            for row in in_wide_not_source[:5]:
                print(f"    {row['question_name']}")
        
        print(f"  Questions in source but not in wide table: {len(in_source_not_wide)}")
        if in_source_not_wide:
            for row in in_source_not_wide[:5]:
                print(f"    {row['question_name']}")
    
    # Unmapped questions in source
    print("\n[2] Unmapped Questions in Source")
    print("-"*80)
    query = f"""
    SELECT 
        raw_question_en,
        COUNT(*) as occurrence_count,
        COUNT(DISTINCT survey_id) as survey_count
    FROM `{COLUMNAR_TABLE}`
    WHERE standardized_question IS NULL
      AND raw_question_en IS NOT NULL
      AND TRIM(raw_question_en) != ''
    GROUP BY raw_question_en
    ORDER BY occurrence_count DESC
    LIMIT 20
    """
    results = run_query(client, query, "Unmapped questions")
    if results:
        findings['unmapped_questions']['top_unmapped'] = results
        print(f"  Top 20 unmapped questions:")
        for row in results[:10]:
            print(f"    {row['raw_question_en'][:60]}... ({row['occurrence_count']:,} occurrences, {row['survey_count']} surveys)")
    
    # Answer standardization quality
    print("\n[3] Answer Standardization Quality")
    print("-"*80)
    query = f"""
    SELECT 
        standardized_question,
        COUNT(DISTINCT standardized_answer) as distinct_answers,
        COUNT(DISTINCT raw_answer_en) as distinct_raw_answers,
        COUNT(*) as total_responses,
        ROUND(COUNT(DISTINCT standardized_answer) * 100.0 / NULLIF(COUNT(DISTINCT raw_answer_en), 0), 2) as standardization_ratio
    FROM `{COLUMNAR_TABLE}`
    WHERE standardized_question IS NOT NULL
      AND standardized_answer IS NOT NULL
    GROUP BY standardized_question
    HAVING distinct_raw_answers > 10
    ORDER BY total_responses DESC
    LIMIT 20
    """
    results = run_query(client, query, "Answer standardization analysis")
    if results:
        findings['answer_quality']['top_questions'] = results
        print(f"  Top 20 questions by response count:")
        for row in results[:10]:
            print(f"    {row['standardized_question']}: {row['distinct_standardized_answers']} standardized from {row['distinct_raw_answers']} raw ({row['standardization_ratio']}% ratio)")
    
    return findings


def generate_recommendations(
    data_quality: Dict[str, Any],
    schema_design: Dict[str, Any],
    performance: Dict[str, Any],
    standardization: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Create prioritized action items"""
    recommendations = []
    
    # Critical: Data integrity issues
    if data_quality.get('duplicates', {}).get('duplicate_pairs'):
        recommendations.append({
            'priority': 'Critical',
            'category': 'Data Quality',
            'issue': 'Duplicate (survey_id, response_id) pairs found',
            'count': len(data_quality['duplicates']['duplicate_pairs']),
            'action': 'Investigate duplicate generation in pivot logic. Ensure ROW_NUMBER() correctly selects one metadata row per response.'
        })
    
    if not data_quality.get('completeness', {}).get('match', True):
        diff = abs(data_quality.get('completeness', {}).get('row_diff', 0))
        if diff > 0:
            recommendations.append({
                'priority': 'Critical',
                'category': 'Data Quality',
                'issue': f'Row count mismatch: {diff} rows difference between wide table and source',
                'action': 'Verify pivot aggregation logic. Check for responses with no standardized questions.'
            })
    
    # High: Performance and coverage
    unmapped = standardization.get('unmapped_questions', {}).get('top_unmapped', [])
    if unmapped:
        total_unmapped = sum(r['occurrence_count'] for r in unmapped)
        if total_unmapped > 1000:
            recommendations.append({
                'priority': 'High',
                'category': 'Standardization',
                'issue': f'High number of unmapped questions: {total_unmapped:,} occurrences',
                'action': 'Review top unmapped questions and add manual mappings or improve NLP matching thresholds.'
            })
    
    # Schema issues
    naming_issues = schema_design.get('column_naming', {}).get('columns', [])
    issues = [c for c in naming_issues if c['naming_quality'] != 'Normal']
    if issues:
        recommendations.append({
            'priority': 'Medium',
            'category': 'Schema Design',
            'issue': f'{len(issues)} columns with naming quality issues',
            'action': 'Review column naming: reserved word conflicts, excessive underscores, numeric characters.'
        })
    
    # Multi-select handling
    multiselect = data_quality.get('multiselect', {}).get('columns', [])
    if multiselect:
        recommendations.append({
            'priority': 'Medium',
            'category': 'Data Quality',
            'issue': f'{len(multiselect)} columns contain multi-select values (pipe-delimited)',
            'action': 'Verify multi-select aggregation logic. Consider if BI tools need split columns or separate handling.'
        })
    
    return recommendations


def generate_report(
    data_quality: Dict[str, Any],
    schema_design: Dict[str, Any],
    performance: Dict[str, Any],
    standardization: Dict[str, Any],
    recommendations: List[Dict[str, Any]]
) -> str:
    """Generate structured markdown report"""
    report = []
    report.append("# LimeSurvey Wide Table Review Report")
    report.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"\nTable: `{WIDE_TABLE}`")
    
    # Executive Summary
    report.append("\n## Executive Summary")
    report.append("\n### Critical Issues")
    critical = [r for r in recommendations if r['priority'] == 'Critical']
    if critical:
        for rec in critical:
            report.append(f"- **{rec['issue']}**: {rec['action']}")
    else:
        report.append("- ✓ No critical issues found")
    
    report.append("\n### High Priority Issues")
    high = [r for r in recommendations if r['priority'] == 'High']
    if high:
        for rec in high:
            report.append(f"- **{rec['issue']}**: {rec['action']}")
    else:
        report.append("- ✓ No high priority issues found")
    
    # Data Quality Report
    report.append("\n## Data Quality Report")
    completeness = data_quality.get('completeness', {})
    report.append(f"\n### Completeness")
    report.append(f"- Wide table rows: {completeness.get('wide_table_rows', 'N/A'):,}")
    report.append(f"- Source unique responses: {completeness.get('source_rows', 'N/A'):,}")
    report.append(f"- Match: {'✓' if completeness.get('match') else '✗'}")
    
    null_analysis = completeness.get('null_analysis', {})
    if null_analysis:
        report.append(f"\n### NULL Analysis")
        report.append(f"- NULL survey_id: {null_analysis.get('null_survey_id', 0):,} ({null_analysis.get('pct_null_survey_id', 0)}%)")
        report.append(f"- NULL response_id: {null_analysis.get('null_response_id', 0):,} ({null_analysis.get('pct_null_response_id', 0)}%)")
        report.append(f"- NULL submitdate: {null_analysis.get('null_submitdate', 0):,} ({null_analysis.get('pct_null_submitdate', 0)}%)")
    
    # Schema Analysis
    report.append("\n## Schema Analysis")
    schema_info = schema_design.get('schema_info', {})
    report.append(f"- Total columns: {schema_info.get('total_columns', 'N/A')}")
    report.append(f"- Question columns: {schema_info.get('total_columns', 0) - len(METADATA_COLUMNS)}")
    report.append(f"- Data types: {schema_info.get('data_types', 'N/A')}")
    
    # Performance Metrics
    report.append("\n## Performance Metrics")
    partitions = performance.get('partitioning', {}).get('partitions', [])
    if partitions:
        report.append(f"- Partitions analyzed: {len(partitions)}")
        total_rows = sum(r['row_count'] for r in partitions)
        report.append(f"- Total rows in analyzed partitions: {total_rows:,}")
    
    # Standardization Coverage
    report.append("\n## Standardization Coverage")
    unmapped = standardization.get('unmapped_questions', {}).get('top_unmapped', [])
    if unmapped:
        total_unmapped = sum(r['occurrence_count'] for r in unmapped)
        report.append(f"- Unmapped question occurrences: {total_unmapped:,}")
        report.append(f"- Unique unmapped questions: {len(unmapped)}")
    
    # Recommendations
    report.append("\n## Prioritized Recommendations")
    for priority in ['Critical', 'High', 'Medium', 'Low']:
        recs = [r for r in recommendations if r['priority'] == priority]
        if recs:
            report.append(f"\n### {priority} Priority")
            for rec in recs:
                report.append(f"\n**{rec['category']}**: {rec['issue']}")
                report.append(f"\nAction: {rec['action']}")
    
    return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(
        description='Comprehensive review of lime_surveys_wide table'
    )
    parser.add_argument(
        '--data-quality',
        action='store_true',
        help='Run data quality analysis only'
    )
    parser.add_argument(
        '--schema',
        action='store_true',
        help='Run schema design analysis only'
    )
    parser.add_argument(
        '--performance',
        action='store_true',
        help='Run performance analysis only'
    )
    parser.add_argument(
        '--standardization',
        action='store_true',
        help='Run standardization coverage analysis only'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Save report to markdown file'
    )
    
    args = parser.parse_args()
    
    # Determine which analyses to run
    run_all = not any([args.data_quality, args.schema, args.performance, args.standardization])
    
    client = get_bq_client()
    
    print("="*80)
    print(" LIMESURVEY WIDE TABLE REVIEW ".center(80))
    print("="*80)
    print(f"\nTable: {WIDE_TABLE}")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET}")
    
    # Run analyses
    data_quality = {}
    schema_design = {}
    performance = {}
    standardization = {}
    
    if run_all or args.data_quality:
        data_quality = analyze_data_quality(client)
    
    if run_all or args.schema:
        schema_design = analyze_schema_design(client)
    
    if run_all or args.performance:
        performance = analyze_performance(client)
    
    if run_all or args.standardization:
        standardization = analyze_standardization(client)
    
    # Generate recommendations
    if run_all:
        print("\n" + "="*80)
        print(" GENERATING RECOMMENDATIONS ".center(80))
        print("="*80)
        recommendations = generate_recommendations(
            data_quality, schema_design, performance, standardization
        )
        
        print(f"\nFound {len(recommendations)} recommendations:")
        for rec in recommendations:
            print(f"  [{rec['priority']}] {rec['category']}: {rec['issue']}")
        
        # Generate and save report
        if args.output:
            report = generate_report(
                data_quality, schema_design, performance, standardization, recommendations
            )
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"\n✓ Report saved to: {args.output}")
        else:
            report = generate_report(
                data_quality, schema_design, performance, standardization, recommendations
            )
            print("\n" + "="*80)
            print(" REPORT ".center(80))
            print("="*80)
            print(report)
    
    print("\n" + "="*80)
    print(" REVIEW COMPLETE ".center(80))
    print("="*80)


if __name__ == "__main__":
    main()
