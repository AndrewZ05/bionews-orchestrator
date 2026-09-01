#!/usr/bin/env python3
"""
LimeSurvey Question Matching Diagnostic Tool

Traces through the EXACT matching logic to determine WHY a specific question
received a particular standardized_question assignment.

This helps identify bugs in the matching logic.

Usage:
    python shared/limesurvey_diagnose_matching.py "How are you treating hepatitis?"
    python shared/limesurvey_diagnose_matching.py --survey-id 123456 --question-id 7
"""

import argparse
import os
import sys
import pickle
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import numpy as np

# Suppress warnings
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GLOG_minloglevel'] = '3'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import warnings
warnings.filterwarnings('ignore')

from google.cloud import bigquery, storage
from sentence_transformers import SentenceTransformer, util

import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ID = 'bi-data-391216'
DATASET = 'limesurvey_data'
TABLE = 'lime_surveys_columnar_completed'
MODEL_BUCKET = 'limesurvey-model-artifacts'


@dataclass
class MatchStep:
    """A step in the matching process"""
    layer: str
    result: Optional[str]
    confidence: float
    reason: str
    matched_to: Optional[str] = None
    details: Optional[Dict] = None


class MatchingDiagnostic:
    """Traces through matching logic step by step"""

    def __init__(self):
        self.bq_client = bigquery.Client(project=PROJECT_ID)
        self.storage_client = storage.Client(project=PROJECT_ID)

        # Load models
        logger.info("Loading sentence transformer model...")
        self.transformer_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

        # Load NLP models
        self._load_nlp_models()

        # Load ML model
        self._load_ml_model()

        # Load known mappings
        self._load_mappings()

    def _load_nlp_models(self):
        """Load spaCy models"""
        import spacy

        logger.info("Loading spaCy models...")
        try:
            self.nlp = spacy.load("en_core_sci_lg")
            logger.info("[OK] Loaded en_core_sci_lg")
        except Exception as e:
            logger.warning(f"Could not load en_core_sci_lg: {e}")
            self.nlp = None

        try:
            self.nlp_ner = spacy.load("en_ner_bc5cdr_md")
            logger.info("[OK] Loaded en_ner_bc5cdr_md")
        except Exception as e:
            logger.warning(f"Could not load en_ner_bc5cdr_md: {e}")
            self.nlp_ner = None

    def _load_ml_model(self):
        """Load the trained ML model"""
        logger.info("Loading ML model from GCS...")
        try:
            bucket = self.storage_client.bucket(MODEL_BUCKET)

            model_blob = bucket.blob('question_classifier_model.pkl')
            encoder_blob = bucket.blob('question_classifier_encoder.pkl')

            if model_blob.exists() and encoder_blob.exists():
                self.ml_model = pickle.loads(model_blob.download_as_bytes())
                self.ml_encoder = pickle.loads(encoder_blob.download_as_bytes())
                logger.info("[OK] ML model loaded")
            else:
                logger.warning("ML model not found in GCS")
                self.ml_model = None
                self.ml_encoder = None
        except Exception as e:
            logger.warning(f"Could not load ML model: {e}")
            self.ml_model = None
            self.ml_encoder = None

    def _load_mappings(self):
        """Load known question mappings from BigQuery"""
        logger.info("Loading known question mappings...")

        query = f"""
        SELECT DISTINCT
            raw_question_en,
            standardized_question,
            COUNT(*) as usage_count
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE raw_question_en IS NOT NULL
          AND standardized_question IS NOT NULL
          AND TRIM(raw_question_en) != ''
        GROUP BY raw_question_en, standardized_question
        ORDER BY usage_count DESC
        """

        df = self.bq_client.query(query).to_dataframe()

        # Build mappings dict
        self.mappings = {}
        for _, row in df.iterrows():
            raw_q = row['raw_question_en'].strip()
            std_q = row['standardized_question']
            if raw_q not in self.mappings:
                self.mappings[raw_q] = std_q

        # Pre-compute embeddings for known questions
        self.known_questions = list(self.mappings.keys())
        logger.info(f"Computing embeddings for {len(self.known_questions)} known questions...")
        self.known_embeddings = self.transformer_model.encode(
            self.known_questions,
            convert_to_numpy=True,
            show_progress_bar=True
        )

        logger.info(f"[OK] Loaded {len(self.mappings)} known mappings")

    def diagnose(self, question: str) -> List[MatchStep]:
        """
        Trace through the full matching logic for a question

        Returns list of MatchStep showing what happened at each layer
        """
        steps = []
        cleaned = self._clean_question(question)

        print(f"\n{'='*80}")
        print(f"DIAGNOSING QUESTION MATCHING")
        print(f"{'='*80}")
        print(f"Original: \"{question}\"")
        print(f"Cleaned:  \"{cleaned}\"")
        print(f"{'='*80}\n")

        # Layer 1: Exact Match
        step = self._try_exact_match(cleaned)
        steps.append(step)
        self._print_step(step)
        if step.result:
            print(f"\n✓ MATCHED at Layer 1 (Exact Match)")
            return steps

        # Layer 2: ML Model
        step = self._try_ml_match(cleaned)
        steps.append(step)
        self._print_step(step)
        if step.result:
            print(f"\n✓ MATCHED at Layer 2 (ML Model)")
            return steps

        # Layer 3: Semantic Similarity
        step = self._try_semantic_match(cleaned)
        steps.append(step)
        self._print_step(step)
        if step.result:
            print(f"\n✓ MATCHED at Layer 3 (Semantic Similarity)")
            return steps

        # Layer 4: spaCy NER
        step = self._try_spacy_match(cleaned)
        steps.append(step)
        self._print_step(step)
        if step.result:
            print(f"\n✓ MATCHED at Layer 4 (spaCy NER)")
            return steps

        # Layer 5: Fuzzy Match
        step = self._try_fuzzy_match(cleaned)
        steps.append(step)
        self._print_step(step)
        if step.result:
            print(f"\n✓ MATCHED at Layer 5 (Fuzzy Match)")
            return steps

        print(f"\n✗ NO MATCH FOUND - would return None")
        return steps

    def _clean_question(self, question: str) -> str:
        """Clean and normalize question text"""
        import re
        # Remove HTML tags
        question = re.sub(r'<[^>]+>', '', question)
        # Normalize whitespace
        question = ' '.join(question.split())
        return question.strip()

    def _try_exact_match(self, question: str) -> MatchStep:
        """Layer 1: Exact match"""
        q_lower = question.lower().strip()

        for known_q, std_q in self.mappings.items():
            if known_q.lower().strip() == q_lower:
                return MatchStep(
                    layer="1. Exact Match",
                    result=std_q,
                    confidence=1.0,
                    reason="Question matches exactly (case-insensitive)",
                    matched_to=known_q
                )

        return MatchStep(
            layer="1. Exact Match",
            result=None,
            confidence=0.0,
            reason="No exact match found"
        )

    def _try_ml_match(self, question: str) -> MatchStep:
        """Layer 2: ML Model"""
        if not self.ml_model or not self.ml_encoder:
            return MatchStep(
                layer="2. ML Model",
                result=None,
                confidence=0.0,
                reason="ML model not available"
            )

        try:
            embedding = self.transformer_model.encode([question])[0]
            probs = self.ml_model.predict_proba([embedding])[0]

            # Get top 3 predictions
            top_indices = np.argsort(probs)[-3:][::-1]
            top_predictions = [
                (self.ml_encoder.inverse_transform([idx])[0], float(probs[idx]))
                for idx in top_indices
            ]

            best_std_q = top_predictions[0][0]
            best_conf = top_predictions[0][1]

            details = {
                "top_predictions": top_predictions,
                "total_classes": len(probs)
            }

            if best_conf >= 0.70:
                return MatchStep(
                    layer="2. ML Model",
                    result=best_std_q,
                    confidence=best_conf,
                    reason=f"ML predicted '{best_std_q}' with {best_conf:.2%} confidence",
                    details=details
                )
            else:
                return MatchStep(
                    layer="2. ML Model",
                    result=None,
                    confidence=best_conf,
                    reason=f"ML best prediction '{best_std_q}' below threshold ({best_conf:.2%} < 70%)",
                    details=details
                )
        except Exception as e:
            return MatchStep(
                layer="2. ML Model",
                result=None,
                confidence=0.0,
                reason=f"ML model error: {e}"
            )

    def _try_semantic_match(self, question: str) -> MatchStep:
        """Layer 3: Semantic similarity"""
        question_embedding = self.transformer_model.encode(question, convert_to_numpy=True)
        similarities = util.cos_sim(question_embedding, self.known_embeddings)[0].numpy()

        # Get top 5 matches
        top_indices = np.argsort(similarities)[-5:][::-1]
        top_matches = [
            (self.known_questions[idx], self.mappings[self.known_questions[idx]], float(similarities[idx]))
            for idx in top_indices
        ]

        details = {"top_matches": top_matches}

        best_q, best_std_q, best_score = top_matches[0]

        if best_score >= 0.85:
            return MatchStep(
                layer="3. Semantic Similarity",
                result=best_std_q,
                confidence=best_score,
                reason=f"Found similar question with {best_score:.2%} similarity",
                matched_to=best_q[:100],
                details=details
            )
        else:
            return MatchStep(
                layer="3. Semantic Similarity",
                result=None,
                confidence=best_score,
                reason=f"Best semantic match below threshold ({best_score:.2%} < 85%)",
                details=details
            )

    def _try_spacy_match(self, question: str) -> MatchStep:
        """Layer 4: spaCy NER matching - THE LIKELY CULPRIT"""
        question_lower = question.lower()
        details = {
            "medical_entities": [],
            "general_entities": [],
            "pattern_checks": []
        }

        # Extract medical entities
        if self.nlp_ner:
            doc_ner = self.nlp_ner(question)
            details["medical_entities"] = [(ent.text, ent.label_) for ent in doc_ner.ents]

        # Extract general entities
        if self.nlp:
            doc = self.nlp(question)
            details["general_entities"] = [(ent.text, ent.label_) for ent in doc.ents]

        disease_entities = [e[0].lower() for e in details["medical_entities"] if e[1] == 'DISEASE']
        chemical_entities = [e[0].lower() for e in details["medical_entities"] if e[1] == 'CHEMICAL']

        details["diseases_found"] = disease_entities
        details["chemicals_found"] = chemical_entities

        # Check disease entity matching
        if disease_entities:
            for disease in disease_entities:
                for known_q, std_q in self.mappings.items():
                    if disease in known_q.lower() or known_q.lower() in disease:
                        details["pattern_checks"].append({
                            "type": "disease_match",
                            "disease": disease,
                            "matched_known_q": known_q,
                            "std_q": std_q,
                            "match_type": "disease in known_q" if disease in known_q.lower() else "known_q in disease"
                        })
                        return MatchStep(
                            layer="4. spaCy NER (DISEASE)",
                            result=std_q,
                            confidence=0.92,
                            reason=f"DISEASE entity '{disease}' matched known question",
                            matched_to=known_q[:100],
                            details=details
                        )

        # Check chemical entity matching
        if chemical_entities:
            for chemical in chemical_entities:
                for known_q, std_q in self.mappings.items():
                    if chemical in known_q.lower() or 'treatment' in known_q.lower() or 'medication' in known_q.lower():
                        details["pattern_checks"].append({
                            "type": "chemical_match",
                            "chemical": chemical,
                            "matched_known_q": known_q,
                            "std_q": std_q
                        })
                        return MatchStep(
                            layer="4. spaCy NER (CHEMICAL)",
                            result=std_q,
                            confidence=0.90,
                            reason=f"CHEMICAL entity '{chemical}' triggered treatment/medication match",
                            matched_to=known_q[:100],
                            details=details
                        )

        # PROBLEMATIC RULE-BASED PATTERNS
        # Check for 'age' or 'old' in question
        if 'age' in question_lower or 'old' in question_lower:
            details["pattern_checks"].append({
                "type": "age_pattern",
                "triggered_by": "'age' in question" if 'age' in question_lower else "'old' in question"
            })
            for known_q, std_q in self.mappings.items():
                if 'age' in known_q.lower():
                    details["pattern_checks"].append({
                        "type": "age_rule_match",
                        "matched_known_q": known_q,
                        "std_q": std_q,
                        "BUG": "This rule matches ANY question containing 'age' to the FIRST known question with 'age'"
                    })
                    return MatchStep(
                        layer="4. spaCy NER (AGE RULE - BUGGY)",
                        result=std_q,
                        confidence=0.90,
                        reason=f"⚠️ BUG: Rule triggered because question contains 'age' or 'old'",
                        matched_to=known_q[:100],
                        details=details
                    )

        # Check for gender patterns
        gender_terms = ['sex', 'gender', 'male', 'female']
        for term in gender_terms:
            if term in question_lower:
                details["pattern_checks"].append({"type": "gender_pattern", "term": term})
                for known_q, std_q in self.mappings.items():
                    if any(t in known_q.lower() for t in gender_terms):
                        return MatchStep(
                            layer="4. spaCy NER (GENDER RULE)",
                            result=std_q,
                            confidence=0.88,
                            reason=f"Gender term '{term}' matched known question",
                            matched_to=known_q[:100],
                            details=details
                        )

        return MatchStep(
            layer="4. spaCy NER",
            result=None,
            confidence=0.0,
            reason="No NER patterns matched",
            details=details
        )

    def _try_fuzzy_match(self, question: str) -> MatchStep:
        """Layer 5: Fuzzy string matching"""
        from difflib import SequenceMatcher

        best_match = None
        best_score = 0.0

        for known_q in self.known_questions:
            score = SequenceMatcher(None, question.lower(), known_q.lower()).ratio()
            if score > best_score:
                best_score = score
                best_match = known_q

        if best_score >= 0.80:
            return MatchStep(
                layer="5. Fuzzy Match",
                result=self.mappings[best_match],
                confidence=best_score,
                reason=f"Fuzzy match with {best_score:.2%} similarity",
                matched_to=best_match[:100]
            )
        else:
            return MatchStep(
                layer="5. Fuzzy Match",
                result=None,
                confidence=best_score,
                reason=f"Best fuzzy match below threshold ({best_score:.2%} < 80%)"
            )

    def _print_step(self, step: MatchStep):
        """Pretty print a match step"""
        status = "✓" if step.result else "✗"
        print(f"\n{status} {step.layer}")
        print(f"   Result: {step.result or 'None'}")
        print(f"   Confidence: {step.confidence:.2%}")
        print(f"   Reason: {step.reason}")
        if step.matched_to:
            print(f"   Matched to: \"{step.matched_to}\"")
        if step.details:
            import json
            print(f"   Details: {json.dumps(step.details, indent=6, default=str)}")


def main():
    parser = argparse.ArgumentParser(
        description='Diagnose LimeSurvey question matching'
    )
    parser.add_argument(
        'question',
        nargs='?',
        type=str,
        help='Question text to diagnose'
    )
    parser.add_argument(
        '--lookup-mismatches',
        action='store_true',
        help='Find questions that may have matched incorrectly'
    )

    args = parser.parse_args()

    diagnostic = MatchingDiagnostic()

    if args.question:
        diagnostic.diagnose(args.question)
    elif args.lookup_mismatches:
        # Find questions currently mapped to 'age' that might be wrong
        query = f"""
        SELECT DISTINCT raw_question_en, COUNT(*) as cnt
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE standardized_question = 'age'
          AND raw_question_en IS NOT NULL
        GROUP BY raw_question_en
        ORDER BY cnt DESC
        LIMIT 20
        """
        print("\nQuestions currently mapped to 'age':\n")
        df = diagnostic.bq_client.query(query).to_dataframe()
        for _, row in df.iterrows():
            print(f"  [{row['cnt']}] {row['raw_question_en'][:80]}")
    else:
        # Default: diagnose the hepatitis question
        diagnostic.diagnose("How are you or the person in your care currently treating hepatitis?")


if __name__ == '__main__':
    main()
