#!/usr/bin/env python3
"""
Survey Profile Builder - Clean, BI-Ready Survey Data Processing
================================================================

This module creates clean respondent profiles from LimeSurvey data for BI purposes.

Key features:
1. Proper answer code decoding (AO01 -> actual label)
2. HTML entity cleanup (&gt; -> >, &lt; -> <, etc.)
3. UMLS-based medical term standardization
4. Flattened output (no nested JSON for simple queries)
5. Semantic column naming based on question content

Architecture:
- Uses BigQuery for data transformation (efficient for large datasets)
- Post-processing in Python for complex standardization
- Caches UMLS lookups for performance

Author: Data Pipeline Team
Created: 2025-01-12
"""

import re
import html
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from google.cloud import bigquery

logger = logging.getLogger(__name__)


@dataclass
class ProfileColumn:
    """Defines a column in the profile output"""
    name: str
    question_ids: List[int]
    category: str  # 'demographic', 'medical', 'preference', 'other'
    is_multiple_choice: bool = False
    requires_umls: bool = False
    transform: str = None  # 'age_range', 'boolean', 'medical_term', etc.


class SurveyProfileBuilder:
    """
    Builds clean respondent profiles from LimeSurvey data.

    Approach:
    1. Define semantic categories (age, gender, diagnosis, treatment, etc.)
    2. Map questions to categories based on content analysis
    3. Transform answers to clean, standardized values
    4. Output flat, BI-ready tables
    """

    # Standard profile columns we want to extract
    # NOTE: The disease/condition is typically embedded in the "Which best describes you?"
    # questions (DEMQ01, G01Q01). The answer contains both role (patient/caregiver) AND disease.
    PROFILE_SCHEMA = {
        # Demographics - Age
        'age': {
            'patterns': [
                r'(what is|select|please select).*your age',
                r'how old are you(?! when)',  # Negative lookahead for "when"
                r'your age range',
            ],
            'exclude_patterns': [
                r'how old.*when.*diagnosed',
                r'how old.*when.*first',
                r'age.*at.*diagnosis',
                r'age.*at.*onset',
                r'current age.*loved one',
                r'caregiver.*family member.*age',
            ],
            'category': 'demographic',
            'transform': 'clean_age',
        },

        # Demographics - Gender
        'gender': {
            'patterns': [
                r'(what is|select).*your gender',
                r'gender identity',
            ],
            'exclude_patterns': [
                r'content',  # Exclude content preference questions
            ],
            'category': 'demographic',
            'transform': 'clean_gender',
        },

        # Role (patient vs caregiver vs HCP)
        # This is the key question - answers contain both role AND disease
        'respondent_type': {
            'patterns': [
                r'which best describes you',
                r'what best describes you',
                r'are you.*(patient|caregiver).*\?$',
                r'what is your relationship with',
            ],
            'exclude_patterns': [
                r'content',  # "What type of content"
                r'hcp',  # HCP-specific questions
                r'healthcare provider',
                r'full name',
                r'rank.*top',
            ],
            'category': 'demographic',
            'transform': 'extract_role',
        },

        # Age at diagnosis
        'age_at_diagnosis': {
            'patterns': [
                r'how old.*when.*diagnosed',
                r'age.*at.*diagnosis',
                r'age.*when.*diagnosed',
            ],
            'category': 'medical',
            'transform': 'clean_age',
        },

        # Age at symptom onset
        'age_at_onset': {
            'patterns': [
                r'how old.*when.*(first|noticed|symptoms)',
                r'age.*at.*onset',
                r'age.*symptoms.*started',
            ],
            'category': 'medical',
            'transform': 'clean_age',
        },

        # Who diagnosed - specialist type
        'diagnosed_by': {
            'patterns': [
                r'what type.*specialist.*diagnosed',
                r'who diagnosed',
                r'healthcare specialist diagnosed',
            ],
            'category': 'medical',
        },

        # Consent/Privacy
        'privacy_consent': {
            'patterns': [
                r'privacy.*policy',
                r'i agree.*terms',
                r'consent.*data',
            ],
            'exclude_patterns': [
                r'share.*email',  # Email sharing is different
            ],
            'category': 'consent',
            'transform': 'clean_boolean',
        },
    }

    # HTML entity replacements
    HTML_ENTITIES = {
        '&gt;': '>',
        '&lt;': '<',
        '&amp;': '&',
        '&nbsp;': ' ',
        '&quot;': '"',
        '&#39;': "'",
        '&apos;': "'",
    }

    def __init__(self, bq_client: bigquery.Client, umls_client=None):
        """
        Initialize profile builder.

        Args:
            bq_client: BigQuery client
            umls_client: Optional UMLSClient for medical term standardization
        """
        self.client = bq_client
        self.project = bq_client.project
        self.umls_client = umls_client

        # Table references
        self.questions_table = f"{self.project}.limesurvey_data.lime_questions"
        self.question_l10ns_table = f"{self.project}.limesurvey_data.lime_question_l10ns"
        self.answers_table = f"{self.project}.limesurvey_data.lime_answers"
        self.answer_l10ns_table = f"{self.project}.limesurvey_data.lime_answer_l10ns"
        self.columnar_table = f"{self.project}.limesurvey_data.lime_surveys_columnar"
        self.output_table = f"{self.project}.limesurvey_data.respondent_profiles"

        # Cache for question mappings
        self._question_cache = {}
        self._answer_cache = {}

    def clean_html(self, text: str) -> str:
        """Remove HTML tags and decode entities"""
        if not text:
            return text

        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Decode HTML entities
        for entity, replacement in self.HTML_ENTITIES.items():
            text = text.replace(entity, replacement)

        # Use html.unescape for any remaining entities
        text = html.unescape(text)

        # Clean up whitespace
        text = ' '.join(text.split())

        return text.strip()

    def clean_age(self, value: str) -> str:
        """
        Standardize age range values.

        Examples:
            "AO06" (with lookup) -> "55-64"
            "55 or older" -> "55+"
            "&gt;18" -> ">18"
            "Younger than 5 years" -> "<5"
        """
        if not value:
            return None

        value = self.clean_html(value)

        # Standardize "X or older" -> "X+"
        match = re.match(r'(\d+)\s*(or older|and older|\+)', value, re.IGNORECASE)
        if match:
            return f"{match.group(1)}+"

        # Standardize "younger than X" / "under X" / "less than X"
        match = re.match(r'(younger than|under|less than)\s*(\d+)', value, re.IGNORECASE)
        if match:
            return f"<{match.group(2)}"

        # Keep ranges as-is (e.g., "55-64", "18-24")
        if re.match(r'\d+-\d+', value):
            return value

        # Clean up common variations
        value = re.sub(r'\s*years?\s*', '', value, flags=re.IGNORECASE)

        return value

    def clean_gender(self, value: str) -> str:
        """Standardize gender values"""
        if not value:
            return None

        value = self.clean_html(value).lower()

        if 'female' in value or value == 'f':
            return 'Female'
        elif 'male' in value or value == 'm':
            return 'Male'
        elif 'non-binary' in value or 'nonbinary' in value:
            return 'Non-binary'
        elif 'prefer not' in value:
            return 'Prefer not to say'
        else:
            return value.title()

    def clean_boolean(self, value: str) -> bool:
        """Convert to boolean"""
        if not value:
            return None

        value = self.clean_html(value).lower()

        if any(x in value for x in ['yes', 'agree', 'consent', 'true', 'y']):
            return True
        elif any(x in value for x in ['no', 'disagree', 'false', 'n']):
            return False

        return None

    def extract_role(self, value: str) -> Dict[str, str]:
        """
        Extract role and disease from "Which best describes you?" answers.

        Examples:
            "Friedreich's ataxia (FA) patient" -> {role: 'patient', disease: 'Friedreich ataxia'}
            "Caregiver/family member for a ALS patient" -> {role: 'caregiver', disease: 'ALS'}
            "I am a caregiver/family member." -> {role: 'caregiver', disease: None}
            "I live with {site}." -> {role: 'patient', disease: None}  # disease comes from survey context

        Returns:
            Dict with 'role' and 'disease' keys
        """
        if not value:
            return {'role': None, 'disease': None}

        value = self.clean_html(value).lower()

        # Determine role
        role = None
        if any(x in value for x in ['patient', 'person with', 'i live with', 'i have']):
            role = 'patient'
        elif any(x in value for x in ['caregiver', 'family member', 'care for']):
            role = 'caregiver'
        elif any(x in value for x in ['healthcare', 'hcp', 'professional', 'provider']):
            role = 'healthcare_provider'
        elif 'neither' in value or 'none of the above' in value:
            role = 'other'

        # Try to extract disease name
        disease = None

        # Pattern: "Disease name (ABBREV) patient" or "Disease name patient"
        disease_patterns = [
            r'^(.+?)\s*\([^)]+\)\s*patient',  # "Friedreich's ataxia (FA) patient"
            r'^(.+?)\s+patient$',  # "Scleroderma patient"
            r'person with\s+(.+?)(?:\.|$)',  # "Person with MS"
            r'caregiver.*for\s+(?:a|an)\s+(.+?)\s+patient',  # "Caregiver for a DMD patient"
            r'family member.*(?:of|for)\s+(?:a|an)\s+(.+?)\s+patient',  # "Family member of a CF patient"
        ]

        for pattern in disease_patterns:
            match = re.search(pattern, value, re.IGNORECASE)
            if match:
                disease = match.group(1).strip()
                # Clean up the disease name
                disease = re.sub(r"['\u2019]s$", '', disease)  # Remove 's suffix
                disease = disease.title()
                break

        return {'role': role, 'disease': disease}

    def standardize_diagnosis(self, value: str) -> Tuple[str, Optional[str]]:
        """
        Standardize diagnosis using UMLS.

        Returns:
            (standardized_name, cui)
        """
        if not value:
            return (None, None)

        value = self.clean_html(value)

        if not self.umls_client:
            return (value, None)

        result = self.umls_client.normalize_term(value)
        if result:
            return (result.name, result.cui)

        return (value, None)

    def standardize_treatment(self, value: str) -> Tuple[str, Optional[str]]:
        """
        Standardize treatment using UMLS.

        Returns:
            (standardized_name, rxcui)
        """
        if not value:
            return (None, None)

        value = self.clean_html(value)

        if not self.umls_client:
            return (value, None)

        result = self.umls_client.normalize_term(value)
        if result:
            return (result.name, result.cui)

        return (value, None)

    def map_questions_to_profile(self) -> Dict[str, List[int]]:
        """
        Map questions to profile columns based on content patterns.

        Returns:
            Dict mapping profile column names to list of question IDs
        """
        logger.info("Mapping questions to profile columns...")

        # Load questions
        query = f"""
        SELECT
            q.qid,
            q.title,
            q.type,
            q.sid,
            ql.question as question_text
        FROM `{self.questions_table}` q
        JOIN `{self.question_l10ns_table}` ql
            ON q.qid = ql.qid AND ql.language = 'en'
        WHERE q.parent_qid = 0
        """

        questions = list(self.client.query(query))
        logger.info(f"Loaded {len(questions)} questions")

        # Map to profile columns
        mapping = {col: [] for col in self.PROFILE_SCHEMA}

        for q in questions:
            q_text = self.clean_html(q.question_text or '').lower()
            q_title = (q.title or '').lower()

            for col_name, col_def in self.PROFILE_SCHEMA.items():
                # Check exclude patterns first
                excluded = False
                for pattern in col_def.get('exclude_patterns', []):
                    if re.search(pattern, q_text, re.IGNORECASE):
                        excluded = True
                        break

                if excluded:
                    continue

                # Check include patterns
                for pattern in col_def['patterns']:
                    if re.search(pattern, q_text, re.IGNORECASE):
                        mapping[col_name].append(q.qid)
                        break

        # Log results
        for col, qids in mapping.items():
            if qids:
                logger.info(f"  {col}: {len(qids)} questions mapped")

        return mapping

    def build_answer_lookup_table(self) -> str:
        """
        Build a temporary table with answer code -> label mappings.

        Returns:
            Name of the temporary table
        """
        logger.info("Building answer lookup table...")

        temp_table = f"{self.project}.limesurvey_data._temp_answer_lookup"

        query = f"""
        CREATE OR REPLACE TABLE `{temp_table}` AS
        SELECT
            a.qid as question_id,
            a.code as answer_code,
            al.answer as answer_label
        FROM `{self.answers_table}` a
        JOIN `{self.answer_l10ns_table}` al
            ON a.aid = al.aid AND al.language = 'en'
        """

        self.client.query(query).result()
        logger.info("Answer lookup table created")

        return temp_table

    def build_profiles(self, force_rebuild: bool = False) -> Dict[str, Any]:
        """
        Build the respondent profiles table.

        Args:
            force_rebuild: If True, rebuild all mappings

        Returns:
            Dict with build statistics
        """
        logger.info("=" * 80)
        logger.info("BUILDING RESPONDENT PROFILES")
        logger.info("=" * 80)

        # Step 1: Map questions to profile columns
        mapping = self.map_questions_to_profile()

        # Step 2: Build answer lookup
        answer_table = self.build_answer_lookup_table()

        # Step 3: Build the profile query
        select_clauses = []

        # Metadata columns
        select_clauses.append("c.survey_id")
        select_clauses.append("c.response_id")
        select_clauses.append("ANY_VALUE(c.submitdate) as submit_date")
        select_clauses.append("ANY_VALUE(c.site) as site")

        # Profile columns
        for col_name, qids in mapping.items():
            if not qids:
                continue

            col_def = self.PROFILE_SCHEMA[col_name]
            qid_list = ','.join(str(q) for q in qids)

            if col_def.get('is_multiple_choice'):
                # Multiple choice: aggregate as array
                select_clauses.append(f"""
                    ARRAY_AGG(
                        DISTINCT IF(c.question_id IN ({qid_list}),
                                   COALESCE(al.answer_label, c.answer_value),
                                   NULL)
                        IGNORE NULLS
                    ) as {col_name}
                """)
            else:
                # Single value: take first non-null
                select_clauses.append(f"""
                    MAX(IF(c.question_id IN ({qid_list}),
                          COALESCE(al.answer_label, c.answer_value),
                          NULL)) as {col_name}
                """)

        # Build the full query
        query = f"""
        CREATE OR REPLACE TABLE `{self.output_table}` AS
        SELECT
            {','.join(select_clauses)}
        FROM `{self.columnar_table}` c
        LEFT JOIN `{answer_table}` al
            ON c.question_id = al.question_id
            AND c.answer_value = al.answer_code
        GROUP BY c.survey_id, c.response_id
        """

        logger.info("Executing profile build query...")
        self.client.query(query).result()

        # Get stats
        table = self.client.get_table(self.output_table)

        result = {
            'status': 'success',
            'rows': table.num_rows,
            'columns': len(table.schema),
            'table': self.output_table,
        }

        logger.info(f"Profile table created: {result['rows']:,} rows, {result['columns']} columns")

        return result

    def apply_transformations(self, batch_size: int = 10000) -> Dict[str, Any]:
        """
        Apply Python-based transformations (HTML cleanup, UMLS standardization).

        This is a post-processing step for values that need complex transformation.

        Args:
            batch_size: Number of rows to process at once

        Returns:
            Dict with transformation statistics
        """
        logger.info("Applying post-processing transformations...")

        stats = {
            'age_cleaned': 0,
            'gender_cleaned': 0,
            'diagnosis_standardized': 0,
            'html_entities_fixed': 0,
        }

        # For now, use SQL-based HTML entity cleanup (faster)
        cleanup_query = f"""
        UPDATE `{self.output_table}`
        SET
            age = REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(age, '&gt;', '>'),
                    '&lt;', '<'
                ),
                '&amp;', '&'
            )
        WHERE age IS NOT NULL
          AND (age LIKE '%&gt;%' OR age LIKE '%&lt;%' OR age LIKE '%&amp;%')
        """

        result = self.client.query(cleanup_query).result()

        # Get row count for modified rows
        count_query = f"""
        SELECT COUNT(*) as cleaned
        FROM `{self.output_table}`
        WHERE age LIKE '%>%' OR age LIKE '%<%'
        """

        for row in self.client.query(count_query):
            stats['age_cleaned'] = row.cleaned

        logger.info(f"Transformations applied: {stats}")

        return stats


def build_respondent_profiles(
    bq_client: bigquery.Client,
    umls_client=None,
    force_rebuild: bool = False
) -> Dict[str, Any]:
    """
    Main entry point for building respondent profiles.

    Args:
        bq_client: BigQuery client
        umls_client: Optional UMLSClient for medical standardization
        force_rebuild: If True, rebuild from scratch

    Returns:
        Dict with build results
    """
    builder = SurveyProfileBuilder(bq_client, umls_client)

    # Build base profiles
    result = builder.build_profiles(force_rebuild)

    # Apply transformations
    transform_stats = builder.apply_transformations()
    result['transformations'] = transform_stats

    return result


if __name__ == "__main__":
    import os
    import json
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO)

    client = bigquery.Client()

    # Try to load UMLS client
    umls = None
    try:
        from shared.umls_client import UMLSClient
        umls_db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'umls', 'umls.sqlite3')
        umls = UMLSClient(
            local_db_path=umls_db_path,
            prefer_local=True
        )
        logger.info("UMLS client loaded")
    except Exception as e:
        logger.warning(f"UMLS not available: {e}")

    result = build_respondent_profiles(client, umls_client=umls)

    print("\n" + "=" * 80)
    print("BUILD RESULT")
    print("=" * 80)
    print(json.dumps(result, indent=2, default=str))
