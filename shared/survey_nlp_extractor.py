#!/usr/bin/env python3
"""
Survey NLP Extractor - Comprehensive extraction using biomedical NER and UMLS.

This module provides sophisticated NLP-based extraction for survey responses,
combining:
1. Biomedical NER (HuggingFace transformers)
2. UMLS entity linking (local database)
3. Survey-specific pattern recognition
4. Negation detection
"""

import logging
import sqlite3
import os
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from functools import lru_cache

logger = logging.getLogger(__name__)

# Lazy import to avoid loading heavy models until needed
_ner_model = None


def get_ner_model():
    """Lazy load the biomedical NER model."""
    global _ner_model
    if _ner_model is None:
        from transformers import pipeline
        logger.info("Loading biomedical NER model...")
        _ner_model = pipeline(
            "ner",
            model="d4data/biomedical-ner-all",
            aggregation_strategy="simple"
        )
        logger.info("Model loaded successfully")
    return _ner_model


@dataclass
class ExtractedProfile:
    """Structured extraction result from survey response."""
    role: Optional[str] = None
    family_relationship: Optional[str] = None
    disease: Optional[str] = None
    disease_umls_cui: Optional[str] = None
    is_negated: bool = False
    confidence: float = 0.0
    raw_entities: List[Dict] = field(default_factory=list)


class SurveyNLPExtractor:
    """
    NLP-based survey response extractor.

    Combines:
    - Biomedical NER for entity extraction
    - UMLS for medical term standardization
    - Pattern-based rules for role/relationship
    - Negation detection
    """

    # Role patterns in priority order (most specific first)
    ROLE_PATTERNS = [
        # Explicit negations
        (r'\b(neither|none of the above)\b', 'other', True),
        (r'\bnot a (patient|caregiver)\b', 'other', True),

        # Healthcare providers
        (r'\b(healthcare|health care|hcp|provider|physician|doctor|nurse|clinician|medical professional)\b', 'healthcare_provider', False),

        # Caregivers - must come before patient (since "caregiver for X patient" contains "patient")
        (r'\b(caregiver|care giver|care-giver)\b', 'caregiver', False),
        (r'\bfamily member\b(?!.*\bpatient$)', 'caregiver', False),  # "family member" not ending in "patient"
        (r'\b(grandparent|grandmother|grandfather)\b', 'caregiver', False),
        (r'\b(mother|father|parent)\s+(of|for)\b', 'caregiver', False),
        (r'\b(spouse|husband|wife|partner)\s+(of|for)\b', 'caregiver', False),
        (r'\b(sibling|brother|sister)\s+(of|for)\b', 'caregiver', False),
        (r'\b(son|daughter)\s+(of|for)\b', 'caregiver', False),  # e.g. "son of patient"
        (r'\bcare for\b', 'caregiver', False),

        # Patients
        (r'\bpatient\b', 'patient', False),
        (r'\bperson with\b', 'patient', False),
        (r'\bperson living with\b', 'patient', False),
        (r'\bi (have|live with)\b', 'patient', False),
        (r'\bdiagnosed with\b', 'patient', False),

        # Researchers
        (r'\bresearcher\b', 'researcher', False),

        # Other/explicit other responses
        (r'^other$', 'other', False),
        (r'^-oth-$', 'other', False),
        (r'^y$', 'other', False),  # Single letter responses
        (r'^yes$', 'other', False),
        (r'^no$', 'other', False),
    ]

    # Family relationship patterns
    FAMILY_PATTERNS = [
        (r'\b(spouse|husband|wife)\b', 'spouse'),
        (r'\b(grandmother|grandfather|grandparent)\b', 'grandparent'),
        (r'\bmother\b(?!.*\bgrand)', 'mother'),
        (r'\bfather\b(?!.*\bgrand)', 'father'),
        (r'\bparent\b(?!.*\bgrand)', 'parent'),
        (r'\b(sibling|brother|sister)\b', 'sibling'),
        (r'\b(son|daughter)\b(?!.*\bgrand)', 'child'),
        (r'\b(grandson|granddaughter|grandchild)\b', 'grandchild'),
        (r'\bfriend\b', 'friend'),
        (r'\bfamily member\b', 'family_member'),
    ]

    # Negation patterns
    NEGATION_PATTERNS = [
        r'\b(not|no|neither|nor|never)\b',
        r"\b(don't|doesn't|didn't|won't|wouldn't|haven't|hasn't)\b",
        r'\bnone of\b',
    ]

    # UMLS database path
    UMLS_DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'umls', 'umls.sqlite3')

    # Curated disease name mappings (abbreviations and common variations)
    DISEASE_MAPPINGS = {
        # Common abbreviations
        'als': 'Amyotrophic Lateral Sclerosis',
        'ms': 'Multiple Sclerosis',
        'eds': 'Ehlers-Danlos Syndrome',
        'copd': 'Chronic Obstructive Pulmonary Disease',
        'cf': 'Cystic Fibrosis',
        'md': 'Muscular Dystrophy',
        'dmd': 'Duchenne Muscular Dystrophy',
        'duchenne': 'Duchenne Muscular Dystrophy',
        'duchenne muscular dystrophy': 'Duchenne Muscular Dystrophy',
        'bmd': 'Becker Muscular Dystrophy',
        'becker': 'Becker Muscular Dystrophy',
        'becker muscular dystrophy': 'Becker Muscular Dystrophy',
        'sma': 'Spinal Muscular Atrophy',
        'cmt': 'Charcot-Marie-Tooth Disease',
        'mg': 'Myasthenia Gravis',
        'ph': 'Pulmonary Hypertension',
        'pf': 'Pulmonary Fibrosis',
        'ipf': 'Idiopathic Pulmonary Fibrosis',
        'pws': 'Prader-Willi Syndrome',
        'cad': 'Cold Agglutinin Disease',
        'aav': 'ANCA-Associated Vasculitis',
        'hae': 'Hereditary Angioedema',
        'itp': 'Immune Thrombocytopenia',
        'hd': 'Huntington Disease',
        'fa': 'Friedreich Ataxia',
        'sle': 'Systemic Lupus Erythematosus',
        'ahus': 'Atypical Hemolytic Uremic Syndrome',
        'fap': 'Familial Amyloid Polyneuropathy',
        'eb': 'Epidermolysis Bullosa',
        'pnh': 'Paroxysmal Nocturnal Hemoglobinuria',
        'aadc': 'AADC Deficiency',
        'nmosd': 'Neuromyelitis Optica Spectrum Disorder',
        'lems': 'Lambert-Eaton Myasthenic Syndrome',
        'aae': 'Acquired Angioedema',

        # Partial matches and variations
        'parkinson': 'Parkinson Disease',
        "parkinson's": 'Parkinson Disease',
        "parkinson's disease": 'Parkinson Disease',
        'alzheimer': 'Alzheimer Disease',
        "alzheimer's": 'Alzheimer Disease',
        'huntington': 'Huntington Disease',
        "huntington's": 'Huntington Disease',
        'rett': 'Rett Syndrome',
        'rett syndrome': 'Rett Syndrome',
        'sjogren': 'Sjogren Syndrome',
        "sjogren's": 'Sjogren Syndrome',
        'sjögren': 'Sjogren Syndrome',
        'sjögren': 'Sjogren Syndrome',  # Double-encoded UTF-8 variant
        "sjögren's": 'Sjogren Syndrome',  # Double-encoded with apostrophe
        'sjã¶gren': 'Sjogren Syndrome',  # Another encoding variant
        'ehlers-danlos': 'Ehlers-Danlos Syndrome',
        'ehlers danlos': 'Ehlers-Danlos Syndrome',
        'sclerosis': 'Multiple Sclerosis',  # Common partial for MS
        'lateral sclerosis': 'Amyotrophic Lateral Sclerosis',
        'amyotrophic lateral sclerosis': 'Amyotrophic Lateral Sclerosis',
        'multiple sclerosis': 'Multiple Sclerosis',
        'scleroderma': 'Scleroderma',
        'sarcoidosis': 'Sarcoidosis',
        'myasthenia': 'Myasthenia Gravis',
        'myasthenia gravis': 'Myasthenia Gravis',
        'cushing': 'Cushing Disease',
        "cushing's": 'Cushing Disease',
        'sickle cell': 'Sickle Cell Disease',
        'sickle cell disease': 'Sickle Cell Disease',
        'cystic fibrosis': 'Cystic Fibrosis',
        'muscular dystrophy': 'Muscular Dystrophy',
        'pulmonary fibrosis': 'Pulmonary Fibrosis',
        'pulmonary hypertension': 'Pulmonary Hypertension',
        'chronic obstructive pulmonary disease': 'Chronic Obstructive Pulmonary Disease',
        'lupus': 'Systemic Lupus Erythematosus',
        'angioedema': 'Hereditary Angioedema',
        'hemophilia': 'Hemophilia',
        'myeloma': 'Multiple Myeloma',
        'multiple myeloma': 'Multiple Myeloma',
        'pompe': 'Pompe Disease',
        'pompe disease': 'Pompe Disease',
        'fabry': 'Fabry Disease',
        'fabry disease': 'Fabry Disease',
        'angelman': 'Angelman Syndrome',
        'angelman syndrome': 'Angelman Syndrome',
        'dravet': 'Dravet Syndrome',
        'dravet syndrome': 'Dravet Syndrome',
        'batten': 'Batten Disease',
        'batten disease': 'Batten Disease',
        'friedreich': 'Friedreich Ataxia',
        'friedreich ataxia': 'Friedreich Ataxia',
        'charcot-marie-tooth': 'Charcot-Marie-Tooth Disease',
        'charcot marie tooth': 'Charcot-Marie-Tooth Disease',
        'prader-willi': 'Prader-Willi Syndrome',
        'prader willi': 'Prader-Willi Syndrome',
        'sanfilippo': 'Sanfilippo Syndrome',
        'sanfilippo syndrome': 'Sanfilippo Syndrome',
        'fragile x': 'Fragile X Syndrome',
        'fragile x syndrome': 'Fragile X Syndrome',
        'neuromyelitis optica': 'Neuromyelitis Optica Spectrum Disorder',
        'lambert-eaton': 'Lambert-Eaton Myasthenic Syndrome',
        'inflammatory bowel': 'Inflammatory Bowel Disease',
        'ankylosing spondylitis': 'Ankylosing Spondylitis',
        'porphyria': 'Porphyria',
        'hypoparathyroidism': 'Hypoparathyroidism',
        'anca': 'ANCA-Associated Vasculitis',
        'cold agglutinin': 'Cold Agglutinin Disease',

        # Additional disease variations found in data
        'atypical hemolytic uremic syndrome': 'Atypical Hemolytic Uremic Syndrome',
        'hemolytic uremic syndrome': 'Atypical Hemolytic Uremic Syndrome',
        'familial amyloid polyneuropathy': 'Familial Amyloid Polyneuropathy',
        'amyloid polyneuropathy': 'Familial Amyloid Polyneuropathy',
        'epidermolysis bullosa': 'Epidermolysis Bullosa',
        'paroxysmal nocturnal hemoglobinuria': 'Paroxysmal Nocturnal Hemoglobinuria',
        'aadc deficiency': 'AADC Deficiency',
        'aromatic l-amino acid decarboxylase': 'AADC Deficiency',
        'myotonic dystrophy': 'Myotonic Dystrophy',
        'myotonic': 'Myotonic Dystrophy',
        'fibromyalgia': 'Fibromyalgia',
        'liver disease': 'Liver Disease',
        'cancer': 'Cancer',
        'bleeding disorder': 'Bleeding Disorder',
        'rare disease': 'Rare Disease',

        # Additional mappings identified from unmapped analysis
        'neuromyeltis optica spectrum disorder': 'Neuromyelitis Optica Spectrum Disorder',
        'neuromyelitis optica spectrum disorder': 'Neuromyelitis Optica Spectrum Disorder',
        'nmo': 'Neuromyelitis Optica Spectrum Disorder',
        'igg4-rd': 'IgG4-Related Disease',
        'igg4 related disease': 'IgG4-Related Disease',
        'igg4-related disease': 'IgG4-Related Disease',
        'spinal muscular atrophy': 'Spinal Muscular Atrophy',
        'anca-associated vasculitis': 'ANCA-Associated Vasculitis',
        'anca associated vasculitis': 'ANCA-Associated Vasculitis',
        'granulomatosis with polyangiitis': 'Granulomatosis with Polyangiitis',
        'gpa': 'Granulomatosis with Polyangiitis',
        'wegener': 'Granulomatosis with Polyangiitis',
        "wegener's": 'Granulomatosis with Polyangiitis',
        'mpa': 'Microscopic Polyangiitis',
        'microscopic polyangiitis': 'Microscopic Polyangiitis',
        'autoimmune hepatitis': 'Autoimmune Hepatitis',
        'primary biliary cholangitis': 'Primary Biliary Cholangitis',
        'pbc': 'Primary Biliary Cholangitis',
        'primary sclerosing cholangitis': 'Primary Sclerosing Cholangitis',
        'psc': 'Primary Sclerosing Cholangitis',
        'gaucher': 'Gaucher Disease',
        'gaucher disease': 'Gaucher Disease',
        'niemann-pick': 'Niemann-Pick Disease',
        'niemann pick': 'Niemann-Pick Disease',
        'wilson disease': 'Wilson Disease',
        "wilson's disease": 'Wilson Disease',
        'marfan': 'Marfan Syndrome',
        'marfan syndrome': 'Marfan Syndrome',
        'loeys-dietz': 'Loeys-Dietz Syndrome',
        'loeys dietz': 'Loeys-Dietz Syndrome',
        'vascular eds': 'Vascular Ehlers-Danlos Syndrome',
        'hypermobile eds': 'Hypermobile Ehlers-Danlos Syndrome',
        'heds': 'Hypermobile Ehlers-Danlos Syndrome',
        'classical eds': 'Classical Ehlers-Danlos Syndrome',
        'kyphoscoliotic eds': 'Kyphoscoliotic Ehlers-Danlos Syndrome',
    }

    def __init__(self, use_ner: bool = True, use_umls: bool = True):
        """
        Initialize the extractor.

        Args:
            use_ner: Whether to use NER model (slower but more accurate)
            use_umls: Whether to use UMLS for standardization
        """
        self.use_ner = use_ner
        self.use_umls = use_umls
        self._umls_conn = None

    @property
    def umls_conn(self):
        """Lazy load UMLS connection."""
        if self._umls_conn is None and os.path.exists(self.UMLS_DB_PATH):
            self._umls_conn = sqlite3.connect(self.UMLS_DB_PATH)
        return self._umls_conn

    def extract_role(self, text: str) -> Tuple[Optional[str], bool]:
        """
        Extract role from text using pattern matching.

        Returns:
            Tuple of (role, is_negated)
        """
        if not text:
            return None, False

        text_lower = text.lower()

        for pattern, role, is_negation in self.ROLE_PATTERNS:
            if re.search(pattern, text_lower):
                return role, is_negation

        return None, False

    def extract_family_relationship(self, text: str) -> Optional[str]:
        """Extract family relationship from text."""
        if not text:
            return None

        text_lower = text.lower()

        for pattern, relationship in self.FAMILY_PATTERNS:
            if re.search(pattern, text_lower):
                return relationship

        return None

    def detect_negation(self, text: str) -> bool:
        """Detect if the statement is negated."""
        if not text:
            return False

        text_lower = text.lower()

        for pattern in self.NEGATION_PATTERNS:
            if re.search(pattern, text_lower):
                return True

        return False

    def extract_diseases_ner(self, text: str) -> List[Tuple[str, float]]:
        """
        Extract diseases using biomedical NER.

        Returns:
            List of (disease_name, confidence_score) tuples
        """
        if not text or not self.use_ner:
            return []

        try:
            ner = get_ner_model()
            entities = ner(text)

            disease_types = {'Disease_disorder', 'Family_history', 'History'}
            diseases = []

            for ent in entities:
                entity_type = ent.get('entity_group', '')
                if entity_type in disease_types:
                    word = ent['word'].strip()
                    # Clean tokenization artifacts
                    word = word.replace(' ##', '').replace('##', '')
                    word = re.sub(r"^' s ", "", word)
                    word = re.sub(r" ' s$", "'s", word)
                    score = float(ent.get('score', 0))

                    if word and len(word) > 2 and score > 0.3:
                        diseases.append((word, score))

            return diseases

        except Exception as e:
            logger.error(f"NER extraction failed: {e}")
            return []

    @lru_cache(maxsize=1000)
    def standardize_disease_umls(self, disease_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Standardize disease name using curated mappings first, then UMLS.

        Returns:
            Tuple of (standardized_name, cui)
        """
        if not disease_name:
            return None, None

        # Clean the disease name
        clean_name = disease_name.strip().lower()
        clean_name = re.sub(r'\s+', ' ', clean_name)  # Normalize whitespace
        clean_name = clean_name.replace(' - ', '-')   # Handle tokenization artifacts

        # 1. Check curated mappings first (highest priority)
        if clean_name in self.DISEASE_MAPPINGS:
            return self.DISEASE_MAPPINGS[clean_name], None

        # 2. Try partial match on curated mappings (only for longer terms)
        # Skip short abbreviations to avoid false positives
        for key, value in self.DISEASE_MAPPINGS.items():
            if len(key) >= 5:  # Only try partial match for terms >= 5 chars
                if key in clean_name or clean_name in key:
                    return value, None

        # 3. Skip UMLS fuzzy matching for now (causes too many errors)
        # Just return cleaned up version of the input
        # Title case the result
        if len(clean_name) <= 3:  # Keep abbreviations uppercase
            return clean_name.upper(), None
        else:
            return clean_name.title(), None

    def extract(self, text: str) -> ExtractedProfile:
        """
        Full extraction from survey response text.

        Args:
            text: Survey response text (e.g., respondent_type value)

        Returns:
            ExtractedProfile with all extracted fields
        """
        profile = ExtractedProfile()

        if not text:
            return profile

        # Extract role
        role, is_negation = self.extract_role(text)
        profile.role = role
        profile.is_negated = is_negation or self.detect_negation(text)

        # Extract family relationship
        profile.family_relationship = self.extract_family_relationship(text)

        # First try to extract disease from the original text using pattern matching
        # This handles cases where NER splits entities incorrectly
        text_lower = text.lower()
        for disease_pattern, standard_name in self.DISEASE_MAPPINGS.items():
            # Use word boundary matching to avoid false positives like "fa" in "for a"
            # For short patterns (abbreviations), require exact word match
            if len(disease_pattern) <= 4:
                pattern = r'\b' + re.escape(disease_pattern) + r'\b'
            else:
                # For longer patterns, allow substring match if pattern is at least 5 chars
                pattern = r'\b' + re.escape(disease_pattern)
            if re.search(pattern, text_lower):
                profile.disease = standard_name
                profile.confidence = 1.0
                return profile

        # Fall back to NER extraction
        # But skip NER if this is clearly a negation/exclusion response
        if profile.is_negated and profile.role == 'other':
            # This is likely "None of the above" or "Neither patient nor caregiver"
            return profile

        diseases = self.extract_diseases_ner(text)
        profile.raw_entities = [(d, s) for d, s in diseases]

        # Filter out obvious false positives
        diseases = [(d, s) for d, s in diseases
                    if d.lower() not in ('patient', 'caregiver', 'neither', 'none')]

        if diseases:
            # Try to combine adjacent entities that might be parts of the same disease
            # For example: "sickle" + "cell" + "disease"
            combined_text = ' '.join([d for d, _ in diseases])
            std_name, cui = self.standardize_disease_umls(combined_text)

            if std_name:
                profile.disease = std_name
                profile.disease_umls_cui = cui
                profile.confidence = max(s for _, s in diseases)
            else:
                # Take highest confidence single entity
                best_disease = max(diseases, key=lambda x: x[1])
                disease_name, confidence = best_disease
                profile.confidence = confidence

                std_name, cui = self.standardize_disease_umls(disease_name)
                profile.disease = std_name
                profile.disease_umls_cui = cui

        return profile


def test_extractor():
    """Test the survey NLP extractor."""
    print("Initializing Survey NLP Extractor...")
    extractor = SurveyNLPExtractor(use_ner=True, use_umls=True)

    test_texts = [
        # Standard patterns
        "Caregiver/family member for a Parkinson's disease patient",
        "Person with Multiple Sclerosis (MS)",
        "I am a caregiver of a female who has been diagnosed with Rett syndrome.",
        "Healthcare provider caring for ALS patients",
        "I have Ehlers-Danlos syndrome",
        "Neither a patient nor caregiver",
        "I am NOT a patient",
        "Grandmother of a child with sickle cell disease",
        "I live with chronic obstructive pulmonary disease",
        "None of the above",
        # Additional real-world patterns
        "Patient diagnosed with Duchenne muscular dystrophy",
        "Spouse of someone with Huntington's disease",
        "Mother of a child with Fragile X syndrome",
        "Father of son with DMD",
        "Person with CF (cystic fibrosis)",
        "Caregiver/family member for an SMA patient",
        "I am a patient living with IPF",
        "Sibling of patient with Pompe disease",
        "Provider treating patients with HAE",
        "Diagnosed with systemic lupus (SLE)",
    ]

    print("\nExtraction Results:")
    print("=" * 100)

    for text in test_texts:
        print(f"\nInput: {text}")
        result = extractor.extract(text)
        print(f"  Role: {result.role}")
        print(f"  Disease: {result.disease}")
        print(f"  Family: {result.family_relationship}")
        print(f"  Negated: {result.is_negated}")
        print(f"  Confidence: {result.confidence:.2f}")
        if result.raw_entities:
            print(f"  Raw entities: {result.raw_entities}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_extractor()
