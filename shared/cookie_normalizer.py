#!/usr/bin/env python3
"""
Cookie Value Normalizer

Strips version prefixes from cookie-based identifiers so the underlying
user ID matches across sources. For example:
    GA1.2.123456.7890  →  123456.7890
    GA2.3.123456.7890  →  123456.7890
    client_id=123456.7890  →  123456.7890   (already raw)

After normalization, if a _ga cookie value matches a client_id value,
both get canonical type 'client_id' so Union-Find merges them automatically.

Usage:
    from shared.cookie_normalizer import CookieNormalizer

    normalizer = CookieNormalizer(merge_ga_client_id=True)
    canonical_type, normalized_value = normalizer.normalize('ga', 'GA1.2.123456.7890')
    # → ('ga', '123456.7890')

    # After collecting all identifiers, deduplicate:
    normalizer.register('client_id', '123456.7890')
    normalizer.register('ga', '123456.7890')
    # Now normalizer.resolve('ga', '123456.7890') → ('client_id', '123456.7890')
"""

import re
from typing import Dict, Tuple, Set, Optional


# Regex patterns for stripping version prefixes
# Each maps identifier_type → (pattern_to_strip, description)
STRIP_PATTERNS = {
    # _ga cookie: GA1.2.XXXXXXXXXX.XXXXXXXXXX → XXXXXXXXXX.XXXXXXXXXX
    # Also handles GS1.1. prefix (GA4 consent mode)
    'ga': (re.compile(r'^(?:GA|GS)\d+\.\d+\.'), 'GA version prefix'),

    # _fbp cookie: fb.1.1234567890.XXXXXXXXXX → XXXXXXXXXX
    # Format: fb.{subdomainIndex}.{creationTime}.{fbpValue}
    'fbp': (re.compile(r'^fb\.\d+\.\d+\.'), 'Facebook Pixel prefix'),

    # _fbc cookie: fb.1.1234567890.IwZXh0... → IwZXh0...
    # Format: fb.{subdomainIndex}.{creationTime}.{fbclid}
    # After stripping, the value is the raw fbclid — same as userTracking.fbclid
    'fbc': (re.compile(r'^fb\.\d+\.\d+\.'), 'Facebook Click ID prefix'),

    # _gcl_aw cookie: GCL.1234567890.Cj0KCQjw... → Cj0KCQjw...
    # Format: GCL.{timestamp}.{gclid}
    # After stripping, the value is the raw gclid
    'gcl_aw': (re.compile(r'^GCL\.\d+\.'), 'Google Ads Click ID prefix'),

    # _gcl_au cookie: 1.1.XXXXXXXXXX.XXXXXXXXXX — kept as-is (no canonical strip)
    # Format is version.version.random.timestamp — no prefix to strip

    # _clck cookie: URL-encoded {user_id}^{version}^{project}^{flags}^{count}
    # Split on ^ handled via split_char in config, not here
}

# Types that are already in canonical form (no stripping needed)
PASSTHROUGH_TYPES = {
    'client_id', 'clarity_user', 'bing_user', 'dmd_tag', 'dmd_vid',
    'kla_id', 'chartbeat', 'omappvp', 'agile_crm_guid',
    'aim_dgid', 'email', 'wp_user_id', 'participant_id',
    'bnfpvid', 'mc_euid', 'ref_pvid', 'tapad_id', 'subscriber_hash',
    'gcl_au', 'phone', 'npi_number', 'bionews_uk', 'gbraid',
}


class CookieNormalizer:
    """
    Normalizes cookie values by stripping version prefixes and deduplicating
    identifiers that resolve to the same underlying value.
    """

    def __init__(self, merge_ga_client_id: bool = True):
        """
        Args:
            merge_ga_client_id: If True, when a normalized _ga value matches
                                a client_id value, both get type 'client_id'.
        """
        self.merge_ga_client_id = merge_ga_client_id

        # Track known client_id values for ga→client_id dedup
        self._client_id_values: Set[str] = set()

        # Track all normalized values by type for dedup stats
        self._stats = {
            'total_normalized': 0,
            'stripped_prefix': 0,
            'ga_merged_to_client_id': 0,
            'passthrough': 0,
        }

    def normalize(self, identifier_type: str, value: str) -> Tuple[str, str]:
        """
        Normalize a cookie value by stripping its version prefix.

        Args:
            identifier_type: The type of identifier (e.g., 'ga', 'fbp', 'client_id')
            value: The raw cookie value

        Returns:
            Tuple of (canonical_type, normalized_value)
        """
        if not value:
            return identifier_type, value

        self._stats['total_normalized'] += 1

        # Passthrough types — already canonical
        if identifier_type in PASSTHROUGH_TYPES:
            self._stats['passthrough'] += 1
            return identifier_type, value

        # Apply strip pattern if one exists
        pattern_info = STRIP_PATTERNS.get(identifier_type)
        if pattern_info:
            pattern, _desc = pattern_info
            normalized = pattern.sub('', value)
            if normalized != value:
                self._stats['stripped_prefix'] += 1
            value = normalized

        return identifier_type, value

    def register(self, identifier_type: str, normalized_value: str) -> None:
        """
        Register a normalized value for cross-type deduplication.

        Call this after normalize() for every identifier to build the
        dedup index. Must be called for ALL identifiers before resolve().
        """
        if identifier_type == 'client_id' and normalized_value:
            self._client_id_values.add(normalized_value)

    def resolve(self, identifier_type: str, normalized_value: str) -> Tuple[str, str]:
        """
        Resolve final canonical type after cross-type deduplication.

        Call this after all identifiers have been register()'d.
        If a 'ga' value matches a known client_id, returns ('client_id', value).

        Args:
            identifier_type: The type after normalize()
            normalized_value: The value after normalize()

        Returns:
            Tuple of (final_type, normalized_value)
        """
        if not self.merge_ga_client_id:
            return identifier_type, normalized_value

        if identifier_type == 'ga' and normalized_value in self._client_id_values:
            self._stats['ga_merged_to_client_id'] += 1
            return 'client_id', normalized_value

        return identifier_type, normalized_value

    def normalize_and_register(self, identifier_type: str, value: str) -> Tuple[str, str]:
        """Convenience: normalize + register in one call."""
        canonical_type, normalized_value = self.normalize(identifier_type, value)
        self.register(canonical_type, normalized_value)
        return canonical_type, normalized_value

    def get_stats(self) -> Dict[str, int]:
        """Return normalization statistics."""
        return self._stats.copy()

    def reset(self) -> None:
        """Reset internal state for a fresh run."""
        self._client_id_values.clear()
        self._stats = {k: 0 for k in self._stats}


def normalize_ga_value(value: str) -> str:
    """
    Standalone helper: strip GA version prefix from a _ga cookie or _ga URL param.

    GA1.2.123456.7890 → 123456.7890
    GS1.1.123456.7890 → 123456.7890
    123456.7890        → 123456.7890  (already clean)
    """
    if not value:
        return value
    return re.sub(r'^(?:GA|GS)\d+\.\d+\.', '', value)


# Patterns that indicate test/invalid phone numbers
_PHONE_REJECT_PATTERNS = re.compile(
    r'^555[01]\d{6}$'       # 555-0XXX and 555-1XXX test numbers
    r'|^(\d)\1{9}$'         # All same digit (0000000000, 1111111111, etc.)
)


def normalize_phone(value: Optional[str]) -> Optional[str]:
    """
    Normalize a US phone number to a canonical 10-digit string.

    Strips all non-digit characters, drops a leading '1' if 11 digits,
    and validates the result is a 10-digit US number that is not a
    known test pattern.

    Examples:
        "(555) 123-4567"   → "5551234567"
        "+1-555-123-4567"  → "5551234567"
        "15551234567"      → "5551234567"
        "555-0100"         → None  (test number)
        "123"              → None  (too short)
        ""                 → None

    Returns:
        10-digit string, or None if invalid/test number.
    """
    if not value:
        return None

    # Strip all non-digit characters
    digits = re.sub(r'\D', '', value)

    # Drop leading country code '1' if 11 digits
    if len(digits) == 11 and digits[0] == '1':
        digits = digits[1:]

    # Must be exactly 10 digits
    if len(digits) != 10:
        return None

    # Reject test/invalid patterns
    if _PHONE_REJECT_PATTERNS.match(digits):
        return None

    return digits
