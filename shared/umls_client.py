"""
UMLS Client - Unified Medical Language System API and Local Database Client

This module provides a hybrid approach to UMLS access:
- REST API for real-time lookups (always current)
- Local database for bulk processing (fast, offline capable)

Features:
- Medical term normalization (FA -> Friedreich Ataxia)
- Concept lookup by CUI
- Synonym retrieval
- Semantic type filtering
- Relationship traversal
- Caching for performance

Usage:
    from shared.umls_client import UMLSClient

    client = UMLSClient()

    # Normalize a term
    result = client.normalize_term("FA")
    print(result)  # {'name': 'Friedreich Ataxia', 'cui': 'C0016719', ...}

    # Get all synonyms
    synonyms = client.get_synonyms("C0016719")
    print(synonyms)  # ['FA', 'Friedreich Ataxia', 'FRDA', ...]
"""

import os
import logging
import requests
from functools import lru_cache
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class UMLSConcept:
    """Represents a UMLS concept"""
    cui: str
    name: str
    semantic_types: List[str]
    source: str  # 'API' or 'LOCAL'
    confidence: float = 0.95
    atom_count: int = 0
    synonyms: List[str] = None

    def to_dict(self) -> Dict:
        return {
            'cui': self.cui,
            'name': self.name,
            'semantic_types': self.semantic_types,
            'source': self.source,
            'confidence': self.confidence,
            'atom_count': self.atom_count,
            'synonyms': self.synonyms or []
        }


class UMLSClient:
    """
    UMLS Client with hybrid API + Local Database support.

    Provides medical terminology normalization for rare disease research.
    """

    API_BASE = "https://uts-ws.nlm.nih.gov/rest"

    # Semantic types relevant for rare disease research
    RELEVANT_SEMANTIC_TYPES = {
        'Disease or Syndrome',
        'Neoplastic Process',
        'Congenital Abnormality',
        'Mental or Behavioral Dysfunction',
        'Sign or Symptom',
        'Finding',
        'Pathologic Function',
        'Pharmacologic Substance',
        'Clinical Drug',
        'Therapeutic or Preventive Procedure',
        'Diagnostic Procedure',
        'Laboratory Procedure',
        'Body Part, Organ, or Organ Component',
        'Gene or Genome',
        'Amino Acid, Peptide, or Protein',
    }

    # Common rare disease abbreviations for priority matching
    RARE_DISEASE_ABBREVIATIONS = {
        'FA': 'Friedreich Ataxia',
        'FRDA': 'Friedreich Ataxia',
        'DMD': 'Duchenne Muscular Dystrophy',
        'BMD': 'Becker Muscular Dystrophy',
        'ALS': 'Amyotrophic Lateral Sclerosis',
        'SMA': 'Spinal Muscular Atrophy',
        'HD': 'Huntington Disease',
        'CF': 'Cystic Fibrosis',
        'PKU': 'Phenylketonuria',
        'MS': 'Multiple Sclerosis',
        'PD': 'Parkinson Disease',
        'AD': 'Alzheimer Disease',
        'MG': 'Myasthenia Gravis',
        'NF': 'Neurofibromatosis',
        'TSC': 'Tuberous Sclerosis Complex',
        'FSHD': 'Facioscapulohumeral Muscular Dystrophy',
        'CMT': 'Charcot-Marie-Tooth Disease',
        'LGMD': 'Limb-Girdle Muscular Dystrophy',
        'DM': 'Myotonic Dystrophy',
        'COPD': 'Chronic Obstructive Pulmonary Disease',
    }

    def __init__(
        self,
        api_key: str = None,
        local_db_path: str = None,
        prefer_local: bool = True,
        cache_size: int = 10000
    ):
        """
        Initialize UMLS Client.

        Args:
            api_key: UMLS API key (defaults to UMLS_API_KEY env var)
            local_db_path: Path to local UMLS SQLite database
            prefer_local: If True, try local DB first, then API
            cache_size: Maximum size of in-memory cache
        """
        self.api_key = api_key or os.environ.get('UMLS_API_KEY')
        self.prefer_local = prefer_local
        self.local_available = False
        self.local_cui = None

        # Try to load local database first
        if local_db_path and os.path.exists(local_db_path):
            self._load_local_db(local_db_path)

        # Only require API key if local DB not available or not preferred
        if not self.api_key and not (self.local_available and prefer_local):
            raise ValueError("UMLS API key required. Set UMLS_API_KEY environment variable.")

        # Set up API session with connection pooling (only if we have API key)
        self.session = None
        if self.api_key:
            self.session = requests.Session()
            self.session.headers.update({
                'Accept': 'application/json',
                'User-Agent': 'BioNews-UMLS-Client/1.0'
            })

        # Configure cache
        self._configure_cache(cache_size)

        logger.info(f"UMLS Client initialized (local_available={self.local_available})")

    def _load_local_db(self, db_path: str):
        """Load local UMLS database"""
        try:
            from owlready2 import default_world, get_ontology
            default_world.set_backend(filename=db_path)
            PYM = get_ontology("http://PYM/").load()
            self.local_cui = PYM["CUI"]
            self.local_available = True
            logger.info(f"Local UMLS database loaded: {db_path}")
        except ImportError:
            logger.warning("owlready2 not installed. Local UMLS database unavailable.")
        except Exception as e:
            logger.warning(f"Could not load local UMLS database: {e}")

    def _configure_cache(self, cache_size: int):
        """Configure LRU cache for API calls"""
        # Reconfigure the cache size
        self._search_api_cached = lru_cache(maxsize=cache_size)(self._search_api_impl)
        self._get_concept_cached = lru_cache(maxsize=cache_size)(self._get_concept_impl)

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def normalize_term(self, term: str, use_api_fallback: bool = True) -> Optional[UMLSConcept]:
        """
        Normalize a medical term to its standard UMLS form.

        This is the primary method for standardizing survey question terminology.

        Args:
            term: Medical term to normalize (e.g., "FA", "Friedreich Ataxia")
            use_api_fallback: If True, use API when local search fails

        Returns:
            UMLSConcept if found, None otherwise
        """
        if not term or not term.strip():
            return None

        term = term.strip()

        # Check rare disease abbreviations first (fast path)
        if term.upper() in self.RARE_DISEASE_ABBREVIATIONS:
            expanded = self.RARE_DISEASE_ABBREVIATIONS[term.upper()]
            result = self.search_term(expanded, use_api_fallback=use_api_fallback)
            if result:
                return result

        # Standard search
        return self.search_term(term, use_api_fallback=use_api_fallback)

    def search_term(self, term: str, use_api_fallback: bool = True) -> Optional[UMLSConcept]:
        """
        Search for a medical term in UMLS.

        Args:
            term: Search term
            use_api_fallback: Use API if local search fails

        Returns:
            Best matching UMLSConcept or None
        """
        # Try local first if available and preferred
        if self.local_available and self.prefer_local:
            result = self._search_local(term)
            if result:
                return result

        # Fallback to API
        if use_api_fallback and self.api_key:
            return self._search_api_cached(term)

        return None

    def get_concept(self, cui: str) -> Optional[UMLSConcept]:
        """
        Get detailed information about a UMLS concept by CUI.

        Args:
            cui: UMLS Concept Unique Identifier (e.g., "C0016719")

        Returns:
            UMLSConcept with full details
        """
        return self._get_concept_cached(cui)

    def get_synonyms(self, cui: str, max_results: int = 100) -> List[str]:
        """
        Get all synonyms/atoms for a UMLS concept.

        This is useful for matching survey questions that use different
        terminology for the same concept.

        Args:
            cui: UMLS Concept Unique Identifier
            max_results: Maximum number of synonyms to return

        Returns:
            List of synonym strings
        """
        try:
            url = f"{self.API_BASE}/content/current/CUI/{cui}/atoms"
            params = {
                'apiKey': self.api_key,
                'pageSize': max_results
            }
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            atoms = data.get('result', [])
            # Deduplicate and return
            return list(set([atom['name'] for atom in atoms if 'name' in atom]))

        except Exception as e:
            logger.warning(f"Error getting synonyms for {cui}: {e}")
            return []

    def get_related_concepts(
        self,
        cui: str,
        relation_types: List[str] = None,
        max_results: int = 50
    ) -> List[Dict]:
        """
        Get concepts related to a given CUI.

        Useful for finding:
        - Broader concepts (parent diseases)
        - Narrower concepts (subtypes)
        - Associated symptoms
        - Treatments

        Args:
            cui: UMLS Concept Unique Identifier
            relation_types: Filter by relation types (e.g., ['RB', 'RN', 'PAR', 'CHD'])
            max_results: Maximum number of results

        Returns:
            List of related concept dictionaries
        """
        try:
            url = f"{self.API_BASE}/content/current/CUI/{cui}/relations"
            params = {
                'apiKey': self.api_key,
                'pageSize': max_results
            }
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            relations = data.get('result', [])

            if relation_types:
                relations = [r for r in relations if r.get('relationLabel') in relation_types]

            return relations

        except Exception as e:
            logger.warning(f"Error getting relations for {cui}: {e}")
            return []

    def bulk_normalize(self, terms: List[str], use_api_fallback: bool = True) -> List[Optional[UMLSConcept]]:
        """
        Normalize multiple terms efficiently.

        For bulk processing of survey questions.

        Args:
            terms: List of terms to normalize
            use_api_fallback: Use API for terms not found locally

        Returns:
            List of UMLSConcept (or None for unmatched terms)
        """
        results = []
        api_needed = []
        api_indices = []

        # First pass: try local database
        for i, term in enumerate(terms):
            if self.local_available and self.prefer_local:
                result = self._search_local(term)
                if result:
                    results.append(result)
                    continue

            results.append(None)
            if use_api_fallback:
                api_needed.append(term)
                api_indices.append(i)

        # Second pass: API for remaining
        for term, idx in zip(api_needed, api_indices):
            result = self._search_api_cached(term)
            results[idx] = result

        return results

    def is_medical_term(self, term: str) -> bool:
        """
        Check if a term is a recognized medical term in UMLS.

        Args:
            term: Term to check

        Returns:
            True if term is found in UMLS
        """
        result = self.normalize_term(term, use_api_fallback=True)
        return result is not None

    def get_semantic_types(self, cui: str) -> List[str]:
        """
        Get semantic types for a concept.

        Semantic types categorize concepts (e.g., "Disease or Syndrome",
        "Pharmacologic Substance").

        Args:
            cui: UMLS Concept Unique Identifier

        Returns:
            List of semantic type names
        """
        concept = self.get_concept(cui)
        if concept:
            return concept.semantic_types
        return []

    # =========================================================================
    # PRIVATE IMPLEMENTATION
    # =========================================================================

    def _search_local(self, term: str) -> Optional[UMLSConcept]:
        """Search local UMLS database"""
        if not self.local_available:
            return None

        try:
            results = self.local_cui.search(term)
            if results:
                concept = results[0]
                return UMLSConcept(
                    cui=concept.name,
                    name=concept.label[0] if concept.label else term,
                    semantic_types=[],  # Local DB may not have this
                    source='LOCAL',
                    confidence=0.95
                )
        except Exception as e:
            logger.warning(f"Local search error for '{term}': {e}")

        return None

    def _search_api_impl(self, term: str) -> Optional[UMLSConcept]:
        """Search UMLS REST API (implementation, will be wrapped with cache)"""
        try:
            url = f"{self.API_BASE}/search/current"
            params = {
                'apiKey': self.api_key,
                'string': term,
                'searchType': 'words',
                'returnIdType': 'concept',
                'pageSize': 10
            }

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            results = data.get('result', {}).get('results', [])
            if results:
                # Filter by relevant semantic types if possible
                top = results[0]

                # Get full concept details for semantic types
                concept_details = self._get_concept_impl(top['ui'])
                if concept_details:
                    return concept_details

                # Fallback to basic info
                return UMLSConcept(
                    cui=top['ui'],
                    name=top['name'],
                    semantic_types=[],
                    source='API',
                    confidence=0.95
                )

        except requests.exceptions.Timeout:
            logger.warning(f"API timeout for '{term}'")
        except requests.exceptions.RequestException as e:
            logger.warning(f"API error for '{term}': {e}")
        except Exception as e:
            logger.warning(f"Unexpected error searching '{term}': {e}")

        return None

    def _get_concept_impl(self, cui: str) -> Optional[UMLSConcept]:
        """Get concept details from API (implementation, will be wrapped with cache)"""
        try:
            url = f"{self.API_BASE}/content/current/CUI/{cui}"
            params = {'apiKey': self.api_key}

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            result = data.get('result', {})
            if result:
                return UMLSConcept(
                    cui=cui,
                    name=result.get('name', ''),
                    semantic_types=[st['name'] for st in result.get('semanticTypes', [])],
                    source='API',
                    confidence=0.95,
                    atom_count=result.get('atomCount', 0)
                )

        except Exception as e:
            logger.warning(f"Error getting concept {cui}: {e}")

        return None


# =========================================================================
# CONVENIENCE FUNCTIONS
# =========================================================================

_default_client: Optional[UMLSClient] = None


def get_client() -> UMLSClient:
    """Get or create the default UMLS client instance."""
    global _default_client
    if _default_client is None:
        _default_client = UMLSClient()
    return _default_client


def normalize(term: str) -> Optional[Dict]:
    """
    Convenience function to normalize a medical term.

    Args:
        term: Medical term to normalize

    Returns:
        Dictionary with normalized term info, or None
    """
    client = get_client()
    result = client.normalize_term(term)
    return result.to_dict() if result else None


def get_synonyms(cui: str) -> List[str]:
    """
    Convenience function to get synonyms for a CUI.

    Args:
        cui: UMLS Concept Unique Identifier

    Returns:
        List of synonyms
    """
    client = get_client()
    return client.get_synonyms(cui)


# =========================================================================
# CLI TESTING
# =========================================================================

if __name__ == "__main__":
    import sys

    # Initialize client
    print("Initializing UMLS Client...")
    client = UMLSClient()
    print()

    # Test rare disease terms
    test_terms = [
        "FA",
        "Friedreich Ataxia",
        "DMD",
        "Duchenne Muscular Dystrophy",
        "ALS",
        "SMA",
        "gait ataxia",
        "cardiomyopathy",
        "muscle weakness",
        "age",
        "gender",
    ]

    print("=" * 80)
    print("UMLS TERM NORMALIZATION TEST")
    print("=" * 80)
    print()

    for term in test_terms:
        result = client.normalize_term(term)
        if result:
            print(f"{term:30s} -> {result.name:40s} (CUI: {result.cui})")
            if result.semantic_types:
                print(f"{'':30s}    Types: {', '.join(result.semantic_types[:3])}")
        else:
            print(f"{term:30s} -> NOT FOUND")
        print()

    # Test synonym retrieval
    print("=" * 80)
    print("SYNONYM TEST: Friedreich Ataxia (C0016719)")
    print("=" * 80)
    synonyms = client.get_synonyms("C0016719")
    print(f"Found {len(synonyms)} synonyms:")
    for syn in synonyms[:20]:
        print(f"  - {syn}")

    print()
    print("=" * 80)
    print("UMLS Client Test Complete")
    print("=" * 80)
