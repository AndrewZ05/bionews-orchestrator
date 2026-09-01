#!/usr/bin/env python3
"""
Analyze standardized questions to identify candidates for consolidation.

Identifies questions that are semantically similar and could be combined
to reduce wide table columns and improve data consistency.

Usage:
    python shared/limesurvey_analyze_question_consolidation.py
    python shared/limesurvey_analyze_question_consolidation.py --threshold 0.90
    python shared/limesurvey_analyze_question_consolidation.py --output recommendations.md
"""

import os
import argparse
import re
import yaml
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Set, Optional
from collections import defaultdict
from dataclasses import dataclass

# Suppress gRPC warnings
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GLOG_minloglevel'] = '3'

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# NLP libraries (same as question standardization)
try:
    from sentence_transformers import SentenceTransformer, util
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    SentenceTransformer = None
    util = None

try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    fuzz = None

# Load .env from project root
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'
COLUMNAR_TABLE = f'{PROJECT_ID}.{DATASET}.lime_surveys_columnar_completed'
QUESTIONS_LOOKUP = f'{PROJECT_ID}.{DATASET}.standardized_questions_lookup'


@dataclass
class QuestionStats:
    """Statistics for a standardized question"""
    question: str
    row_count: int
    survey_count: int
    category: str = None
    description: str = None
    sample_raw_questions: List[str] = None
    sample_answers: List[str] = None  # Sample standardized answers for compatibility check


def get_bq_client() -> bigquery.Client:
    """Get BigQuery client with service account credentials"""
    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_path and os.path.exists(creds_path):
        credentials = service_account.Credentials.from_service_account_file(creds_path)
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    return bigquery.Client(project=PROJECT_ID)


def get_all_standardized_questions(client: bigquery.Client) -> List[QuestionStats]:
    """Get all standardized questions with statistics"""
    query = f"""
    WITH question_stats AS (
        SELECT 
            standardized_question,
            COUNT(*) as row_count,
            COUNT(DISTINCT survey_id) as survey_count
        FROM `{COLUMNAR_TABLE}`
        WHERE standardized_question IS NOT NULL
        GROUP BY standardized_question
    ),
    question_metadata AS (
        SELECT 
            standardized_question,
            category,
            description
        FROM `{QUESTIONS_LOOKUP}`
    ),
    sample_questions AS (
        SELECT 
            standardized_question,
            ARRAY_AGG(DISTINCT raw_question_en ORDER BY raw_question_en LIMIT 5) as sample_raw
        FROM `{COLUMNAR_TABLE}`
        WHERE standardized_question IS NOT NULL
          AND raw_question_en IS NOT NULL
        GROUP BY standardized_question
    ),
    sample_answers AS (
        SELECT 
            standardized_question,
            ARRAY_AGG(DISTINCT standardized_answer ORDER BY standardized_answer LIMIT 10) as sample_ans
        FROM `{COLUMNAR_TABLE}`
        WHERE standardized_question IS NOT NULL
          AND standardized_answer IS NOT NULL
        GROUP BY standardized_question
    )
    SELECT 
        qs.standardized_question,
        qs.row_count,
        qs.survey_count,
        COALESCE(qm.category, 'unknown') as category,
        qm.description,
        sq.sample_raw as sample_raw_questions,
        sa.sample_ans as sample_answers
    FROM question_stats qs
    LEFT JOIN question_metadata qm ON qs.standardized_question = qm.standardized_question
    LEFT JOIN sample_questions sq ON qs.standardized_question = sq.standardized_question
    LEFT JOIN sample_answers sa ON qs.standardized_question = sa.standardized_question
    ORDER BY qs.row_count DESC
    """
    
    results = client.query(query)
    questions = []
    for row in results:
        questions.append(QuestionStats(
            question=row.standardized_question,
            row_count=row.row_count,
            survey_count=row.survey_count,
            category=row.category,
            description=row.description,
            sample_raw_questions=list(row.sample_raw_questions) if row.sample_raw_questions else [],
            sample_answers=list(row.sample_answers) if row.sample_answers else []
        ))
    
    return questions


def cleanup_text(text: str) -> str:
    """
    Clean text using same method as question standardization.
    HTML removal, encoding fixes, whitespace normalization.
    """
    if not text or not text.strip():
        return ""
    
    cleaned = text
    
    # Remove HTML tags (same as _cleanup_text in limesurvey_process_columnar.py)
    cleaned = re.sub(r'<[^>]+>', '', cleaned)
    
    # Fix encoding issues (same patterns)
    encoding_fixes = {
        r'â€™|â€˜|â€™': "'",
        r'â€œ|â€|â€|â€': '"',
        r'Â|Â ': ' ',
    }
    for pattern, replacement in encoding_fixes.items():
        cleaned = re.sub(pattern, replacement, cleaned)
    
    # Normalize whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned


def normalize_question_name(name: str) -> str:
    """Normalize question name for comparison (same as question standardization)"""
    # Clean text first
    normalized = cleanup_text(name.lower().strip())
    
    # Remove common patterns
    patterns_to_remove = [
        r'^q_',  # Reserved word prefix
        r'_$',   # Trailing underscore
        r'^_',   # Leading underscore
    ]
    
    for pattern in patterns_to_remove:
        normalized = re.sub(pattern, '', normalized)
    
    return normalized


def extract_keywords(name: str) -> Set[str]:
    """Extract meaningful keywords from question name"""
    # Split on underscores and common separators
    words = re.split(r'[_\s-]+', name.lower())
    
    # Filter out common stop words and short words
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
    keywords = {w for w in words if len(w) > 2 and w not in stop_words}
    
    return keywords


def calculate_name_similarity(name1: str, name2: str) -> float:
    """Calculate similarity between two question names using keyword overlap"""
    keywords1 = extract_keywords(name1)
    keywords2 = extract_keywords(name2)
    
    if not keywords1 or not keywords2:
        return 0.0
    
    # Jaccard similarity
    intersection = keywords1 & keywords2
    union = keywords1 | keywords2
    
    if not union:
        return 0.0
    
    return len(intersection) / len(union)


# Global transformer model (loaded once, reused)
_TRANSFORMER_MODEL = None

def get_transformer_model():
    """Get or initialize Sentence Transformer model (same as question standardization)"""
    global _TRANSFORMER_MODEL
    if _TRANSFORMER_MODEL is None and SENTENCE_TRANSFORMERS_AVAILABLE:
        try:
            EMBEDDING_MODEL = 'sentence-transformers/all-MiniLM-L6-v2'
            _TRANSFORMER_MODEL = SentenceTransformer(EMBEDDING_MODEL)
        except Exception as e:
            print(f"Warning: Could not load SentenceTransformer: {e}")
    return _TRANSFORMER_MODEL


def compare_raw_questions_semantically(
    questions1: List[str], 
    questions2: List[str]
) -> float:
    """
    Compare sample raw questions using Sentence Transformers (Layer 3 method from standardization).
    
    Uses same model and approach as question standardization process.
    """
    transformer_model = get_transformer_model()
    if not transformer_model or not questions1 or not questions2:
        return 0.0
    
    # Clean questions (same as question standardization)
    cleaned1 = [cleanup_text(q) for q in questions1 if q]
    cleaned2 = [cleanup_text(q) for q in questions2 if q]
    
    if not cleaned1 or not cleaned2:
        return 0.0
    
    # Generate embeddings (same as question standardization)
    try:
        embeddings1 = transformer_model.encode(cleaned1, convert_to_numpy=True)
        embeddings2 = transformer_model.encode(cleaned2, convert_to_numpy=True)
        
        # Calculate max similarity between any pair
        # (questions asking same thing should have high similarity)
        max_sim = 0.0
        for emb1 in embeddings1:
            similarities = util.cos_sim(emb1, embeddings2)[0].numpy()
            max_sim = max(max_sim, float(similarities.max()))
        
        return max_sim
    except Exception as e:
        print(f"Warning: Semantic comparison failed: {e}")
        return 0.0


def check_answer_compatibility(
    answers1: List[str], 
    answers2: List[str]
) -> Dict:
    """
    Check if answers are compatible (same question should have similar answers).
    
    Returns: {
        'compatible': bool,
        'overlap_ratio': float,
        'unique_answers_1': int,
        'unique_answers_2': int,
        'common_answers': int
    }
    """
    if not answers1 or not answers2:
        return {'compatible': False, 'overlap_ratio': 0.0}
    
    # Normalize answers for comparison
    set1 = {cleanup_text(str(a)).lower().strip() for a in answers1 if a}
    set2 = {cleanup_text(str(a)).lower().strip() for a in answers2 if a}
    
    if not set1 or not set2:
        return {'compatible': False, 'overlap_ratio': 0.0}
    
    intersection = set1 & set2
    union = set1 | set2
    
    overlap_ratio = len(intersection) / len(union) if union else 0.0
    
    # Consider compatible if >50% overlap
    compatible = overlap_ratio > 0.5
    
    return {
        'compatible': compatible,
        'overlap_ratio': overlap_ratio,
        'unique_answers_1': len(set1),
        'unique_answers_2': len(set2),
        'common_answers': len(intersection)
    }


def analyze_question_similarity(
    q1: QuestionStats, 
    q2: QuestionStats,
    check_answers: bool = True
) -> Tuple[float, str, Dict]:
    """
    Analyze similarity using applicable standardization methods.
    
    Applicable methods from question standardization:
    - Text cleanup (HTML removal, encoding fixes, whitespace normalization)
    - Sentence Transformers semantic similarity (Layer 3)
    - RapidFuzz token matching (Layer 6)
    - Answer compatibility checking
    
    Not applicable:
    - ML Supervised Model (trained on raw→standardized, not standardized→standardized)
    - UMLS (standardized names already normalized)
    - spaCy NER (for entity extraction, not name comparison)
    
    Returns: (similarity_score, method_name, metadata_dict)
    """
    name1 = q1.question.lower()
    name2 = q2.question.lower()
    metadata = {'category_match': q1.category == q2.category}
    
    # Method 1: Exact match (after normalization) - APPLICABLE
    norm1 = normalize_question_name(name1)
    norm2 = normalize_question_name(name2)
    if norm1 == norm2:
        return 1.0, "exact_match_after_normalization", metadata
    
    # Method 2: RapidFuzz token matching - APPLICABLE (same as Layer 6)
    if RAPIDFUZZ_AVAILABLE:
        # Use token_sort_ratio (same as question standardization)
        fuzz_score = fuzz.token_sort_ratio(name1, name2) / 100.0
        if fuzz_score >= 0.85:  # Same threshold as question standardization
            metadata['fuzz_score'] = fuzz_score
            return fuzz_score, "rapidfuzz_token_match", metadata
    
    # Method 3: Sentence Transformers on sample raw questions - APPLICABLE (same as Layer 3)
    if q1.sample_raw_questions and q2.sample_raw_questions:
        semantic_score = compare_raw_questions_semantically(
            q1.sample_raw_questions, 
            q2.sample_raw_questions
        )
        if semantic_score >= 0.85:  # Same threshold as question standardization
            metadata['semantic_score'] = semantic_score
            return semantic_score, "sentence_transformer_semantic", metadata
    
    # Method 4: Answer compatibility check - NEW (not in question standardization, but useful)
    if check_answers and q1.sample_answers and q2.sample_answers:
        answer_compat = check_answer_compatibility(q1.sample_answers, q2.sample_answers)
        metadata['answer_compatibility'] = answer_compat
        if answer_compat['compatible'] and answer_compat['overlap_ratio'] > 0.7:
            # High answer overlap suggests same question
            base_score = answer_compat['overlap_ratio'] * 0.9  # Slightly lower than semantic
            return base_score, "answer_compatibility", metadata
    
    # Method 5: Keyword overlap (fallback)
    keyword_sim = calculate_name_similarity(name1, name2)
    if keyword_sim > 0.7:
        return keyword_sim, "keyword_overlap", metadata
    
    # Method 6: Pattern matching (conservative, only for known patterns)
    patterns = [
        # Only very specific patterns to avoid false positives
        (r'^age$', r'^age_.*', 0.90, "age_variations"),
        (r'^gender$', r'^(sex|biological_sex)$', 0.90, "gender_variations"),
    ]
    
    for pattern1, pattern2, score, reason in patterns:
        if re.match(pattern1, name1) and re.match(pattern2, name2):
            return score, reason, metadata
        if re.match(pattern2, name1) and re.match(pattern1, name2):
            return score, reason, metadata
    
    return keyword_sim, "keyword_overlap", metadata


def find_semantic_similarities(
    questions: List[QuestionStats], 
    threshold: float = 0.70
) -> List[Tuple[QuestionStats, QuestionStats, float, str]]:
    """
    Find semantically similar questions using applicable standardization methods.
    
    Methods used (from question standardization):
    - Text cleanup (HTML removal, encoding fixes, whitespace normalization)
    - Sentence Transformers semantic similarity (Layer 3)
    - RapidFuzz token matching (Layer 6)
    - Answer compatibility checking
    """
    similarities = []
    
    # Group by category first (same category = more likely similar)
    by_category = defaultdict(list)
    for q in questions:
        by_category[q.category].append(q)
    
    # Check within categories (higher confidence)
    print("  Comparing questions within same categories...")
    for category, cat_questions in by_category.items():
        if len(cat_questions) < 2:
            continue
        for i, q1 in enumerate(cat_questions):
            for q2 in cat_questions[i+1:]:
                similarity, method, metadata = analyze_question_similarity(q1, q2, check_answers=True)
                
                # Add metadata to reason string
                reason = method
                if metadata.get('answer_compatibility'):
                    compat = metadata['answer_compatibility']
                    reason += f" (answer_overlap={compat['overlap_ratio']:.2f})"
                
                if similarity >= threshold:
                    similarities.append((q1, q2, similarity, reason))
    
    # Also check across categories (lower threshold, more conservative)
    print("  Comparing questions across categories...")
    cross_threshold = threshold * 0.90  # More conservative for cross-category
    for i, q1 in enumerate(questions):
        for q2 in questions[i+1:]:
            if q1.category != q2.category:
                similarity, method, metadata = analyze_question_similarity(q1, q2, check_answers=True)
                
                # Add metadata to reason string
                reason = method
                if metadata.get('answer_compatibility'):
                    compat = metadata['answer_compatibility']
                    reason += f" (answer_overlap={compat['overlap_ratio']:.2f})"
                
                # Only include if very high similarity across categories
                if similarity >= cross_threshold and similarity >= 0.85:
                    similarities.append((q1, q2, similarity, reason))
    
    # Sort by similarity descending
    similarities.sort(key=lambda x: x[2], reverse=True)
    
    return similarities


def find_consolidation_candidates(
    questions: List[QuestionStats],
    threshold: float = 0.75
) -> List[Dict]:
    """Find questions that could be consolidated"""
    similarities = find_semantic_similarities(questions, threshold)
    
    # Group into consolidation sets
    consolidation_sets = []
    processed = set()
    
    for q1, q2, sim, reason in similarities:
        # Skip if already processed
        if q1.question in processed or q2.question in processed:
            continue
        
        # Determine which should be the primary (keep the one with more rows)
        if q1.row_count >= q2.row_count:
            primary = q1
            secondary = q2
        else:
            primary = q2
            secondary = q1
        
        # Check if we can add to existing set
        added = False
        for group in consolidation_sets:
            # Check if primary matches this group's primary
            if primary.question == group['primary']:
                # Add secondary to this group
                group['questions'].append({
                    'question': secondary.question,
                    'row_count': secondary.row_count,
                    'survey_count': secondary.survey_count,
                    'similarity': sim,
                    'reason': reason
                })
                group['total_rows'] += secondary.row_count
                group['total_surveys'] = max(group['total_surveys'], secondary.survey_count)
                processed.add(secondary.question)
                added = True
                break
            # Check if either question is already in the questions list
            existing_questions = [q['question'] for q in group['questions']]
            if primary.question in existing_questions or secondary.question in existing_questions:
                # One of them is already in a group, skip to avoid duplicates
                processed.add(primary.question)
                processed.add(secondary.question)
                added = True
                break
        
        if not added:
            consolidation_sets.append({
                'primary': primary.question,
                'primary_stats': {
                    'row_count': primary.row_count,
                    'survey_count': primary.survey_count,
                    'category': primary.category
                },
                'questions': [{
                    'question': secondary.question,
                    'row_count': secondary.row_count,
                    'survey_count': secondary.survey_count,
                    'similarity': sim,
                    'reason': reason
                }],
                'total_rows': primary.row_count + secondary.row_count,
                'total_surveys': max(primary.survey_count, secondary.survey_count)
            })
            processed.add(primary.question)
            processed.add(secondary.question)
    
    return consolidation_sets


def analyze_question_patterns(questions: List[QuestionStats]) -> Dict:
    """Analyze patterns in question names to identify consolidation opportunities"""
    patterns = defaultdict(list)
    
    # Group by root word
    root_words = defaultdict(list)
    for q in questions:
        keywords = extract_keywords(q.question)
        if keywords:
            # Use longest keyword as root (likely the main concept)
            root = max(keywords, key=len)
            root_words[root].append(q)
    
    # Find roots with multiple questions
    multi_question_roots = {root: qs for root, qs in root_words.items() if len(qs) > 1}
    
    # Analyze common prefixes/suffixes
    prefixes = defaultdict(list)
    suffixes = defaultdict(list)
    
    for q in questions:
        parts = q.question.split('_')
        if len(parts) > 1:
            prefixes[parts[0]].append(q)
            suffixes[parts[-1]].append(q)
    
    return {
        'multi_question_roots': multi_question_roots,
        'common_prefixes': {p: qs for p, qs in prefixes.items() if len(qs) > 1},
        'common_suffixes': {s: qs for s, qs in suffixes.items() if len(qs) > 1}
    }


def generate_recommendations(
    consolidation_candidates: List[Dict],
    pattern_analysis: Dict,
    questions: List[QuestionStats]
) -> str:
    """Generate markdown report with recommendations"""
    report = []
    report.append("# Standardized Question Consolidation Recommendations")
    report.append(f"\nGenerated: {Path(__file__).name}")
    report.append(f"\nTotal standardized questions analyzed: {len(questions)}")
    report.append(f"\nConsolidation candidates found: {len(consolidation_candidates)}")
    
    # High-confidence consolidations
    high_confidence = [c for c in consolidation_candidates if any(
        q['similarity'] >= 0.85 for q in c['questions']
    )]
    
    report.append("\n## High-Confidence Consolidations (>=85% similarity)")
    report.append(f"\nFound {len(high_confidence)} high-confidence consolidation opportunities:\n")
    
    for i, candidate in enumerate(high_confidence, 1):
        report.append(f"\n### {i}. Primary: `{candidate['primary']}`")
        report.append(f"\n**Primary Stats:**")
        report.append(f"- Row count: {candidate['primary_stats']['row_count']:,}")
        report.append(f"- Survey count: {candidate['primary_stats']['survey_count']}")
        report.append(f"- Category: {candidate['primary_stats']['category']}")
        
        report.append(f"\n**Consolidate into this:**")
        for q in candidate['questions']:
            report.append(f"- `{q['question']}` ({q['row_count']:,} rows, {q['survey_count']} surveys)")
            report.append(f"  - Similarity: {q['similarity']:.1%}")
            report.append(f"  - Reason: {q['reason']}")
        
        report.append(f"\n**Impact:**")
        report.append(f"- Total rows affected: {candidate['total_rows']:,}")
        report.append(f"- Surveys affected: {candidate['total_surveys']}")
        report.append(f"- Columns reduced: {len(candidate['questions'])}")
    
    # Medium-confidence consolidations
    medium_confidence = [c for c in consolidation_candidates if not any(
        q['similarity'] >= 0.85 for q in c['questions']
    )]
    
    if medium_confidence:
        report.append("\n## Medium-Confidence Consolidations (70-84% similarity)")
        report.append(f"\nFound {len(medium_confidence)} medium-confidence consolidation opportunities:\n")
        
        for i, candidate in enumerate(medium_confidence[:10], 1):  # Top 10
            report.append(f"\n### {i}. Primary: `{candidate['primary']}`")
            for q in candidate['questions']:
                report.append(f"- `{q['question']}` ({q['similarity']:.1%} similarity, {q['reason']})")
    
    # Pattern-based recommendations
    report.append("\n## Pattern-Based Consolidation Opportunities")
    
    multi_roots = pattern_analysis.get('multi_question_roots', {})
    if multi_roots:
        report.append("\n### Questions Sharing Root Words")
        report.append("\nThese questions share the same root concept and may be candidates for consolidation:\n")
        
        for root, qs in sorted(multi_roots.items(), key=lambda x: len(x[1]), reverse=True)[:10]:
            if len(qs) > 1:
                report.append(f"\n**Root: `{root}`** ({len(qs)} questions)")
                for q in qs:
                    report.append(f"- `{q.question}` ({q.row_count:,} rows, {q.category})")
    
    # Common prefixes
    common_prefixes = pattern_analysis.get('common_prefixes', {})
    if common_prefixes:
        report.append("\n### Questions with Common Prefixes")
        report.append("\nThese questions share prefixes and may be related:\n")
        
        for prefix, qs in sorted(common_prefixes.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
            report.append(f"\n**Prefix: `{prefix}_`** ({len(qs)} questions)")
            for q in qs[:5]:
                report.append(f"- `{q.question}` ({q.row_count:,} rows)")
    
    # Implementation guidance
    report.append("\n## Implementation Guidance")
    report.append("\n### Before Consolidation")
    report.append("1. Review sample raw questions for each candidate pair")
    report.append("2. Verify answer formats are compatible")
    report.append("3. Check if answers can be normalized to common format")
    report.append("4. Test on sample data before full migration")
    
    report.append("\n### Consolidation Steps")
    report.append("1. Update `standardized_questions_lookup` to mark secondary questions as deprecated")
    report.append("2. Update mapping tables to point secondary → primary")
    report.append("3. Run answer standardization to normalize answer formats")
    report.append("4. Update `lime_surveys_columnar_completed` with new standardized_question values")
    report.append("5. Rebuild wide table to reflect consolidated columns")
    
    report.append("\n### Risks")
    report.append("- Data loss if answers are not compatible")
    report.append("- Breaking existing BI reports/queries")
    report.append("- Need to update downstream consumers")
    
    return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(
        description='Analyze standardized questions for consolidation opportunities'
    )
    parser.add_argument(
        '--threshold',
        type=float,
        default=0.75,
        help='Similarity threshold for consolidation (0.0-1.0, default: 0.75)'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Save recommendations to markdown file'
    )
    parser.add_argument(
        '--min-rows',
        type=int,
        default=100,
        help='Minimum row count to consider for consolidation (default: 100)'
    )
    
    args = parser.parse_args()
    
    client = get_bq_client()
    
    print("="*80)
    print(" STANDARDIZED QUESTION CONSOLIDATION ANALYSIS ".center(80))
    print("="*80)
    
    print("\n[1] Loading standardized questions...")
    questions = get_all_standardized_questions(client)
    print(f"  Found {len(questions)} standardized questions")
    
    # Filter by minimum rows
    filtered_questions = [q for q in questions if q.row_count >= args.min_rows]
    print(f"  After filtering (min {args.min_rows} rows): {len(filtered_questions)} questions")
    
    print("\n[2] Analyzing question patterns...")
    pattern_analysis = analyze_question_patterns(filtered_questions)
    
    multi_roots = pattern_analysis.get('multi_question_roots', {})
    print(f"  Found {len(multi_roots)} root words with multiple questions")
    
    print("\n[3] Finding consolidation candidates...")
    print("  Using applicable methods from question standardization:")
    print("    - Text cleanup (HTML removal, encoding fixes, whitespace normalization)")
    if SENTENCE_TRANSFORMERS_AVAILABLE:
        print("    - Sentence Transformers semantic similarity (Layer 3)")
    if RAPIDFUZZ_AVAILABLE:
        print("    - RapidFuzz token matching (Layer 6)")
    print("    - Answer compatibility checking")
    
    consolidation_candidates = find_consolidation_candidates(
        filtered_questions,
        threshold=args.threshold
    )
    print(f"  Found {len(consolidation_candidates)} consolidation opportunities")
    
    high_conf = [c for c in consolidation_candidates if any(
        q['similarity'] >= 0.85 for q in c['questions']
    )]
    print(f"  High-confidence (>=85%): {len(high_conf)}")
    print(f"  Medium-confidence (70-84%): {len(consolidation_candidates) - len(high_conf)}")
    
    print("\n[4] Generating recommendations...")
    recommendations = generate_recommendations(
        consolidation_candidates,
        pattern_analysis,
        filtered_questions
    )
    
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(recommendations)
        print(f"\n[OK] Recommendations saved to: {args.output}")
    else:
        print("\n" + "="*80)
        print(" RECOMMENDATIONS ".center(80))
        print("="*80)
        print(recommendations)
    
    print("\n" + "="*80)
    print(" ANALYSIS COMPLETE ".center(80))
    print("="*80)


if __name__ == "__main__":
    main()
