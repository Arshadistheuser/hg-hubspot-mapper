"""
Domain normalization and matching utilities.

Handles variations like:
  www.example.com, example.com, https://example.com,
  http://www.example.com/path, EXAMPLE.COM, etc.
"""

from typing import Optional
from urllib.parse import urlparse
import re


def normalize_domain(raw: str) -> Optional[str]:
    """
    Normalize a domain/URL string to a bare domain (no scheme, no www, no path).

    Examples:
        "https://www.example.com/about" -> "example.com"
        "WWW.EXAMPLE.COM"               -> "example.com"
        "example.com"                    -> "example.com"
        ""                               -> None
    """
    if not raw or not isinstance(raw, str):
        return None

    raw = raw.strip()
    if not raw:
        return None

    # Add scheme if missing so urlparse works correctly
    if not re.match(r'^https?://', raw, re.IGNORECASE):
        raw = 'https://' + raw

    try:
        parsed = urlparse(raw)
        domain = parsed.hostname  # lowercase by default
    except Exception:
        return None

    if not domain:
        return None

    # Strip leading "www."
    if domain.startswith('www.'):
        domain = domain[4:]

    # Basic validation: must contain at least one dot
    if '.' not in domain:
        return None

    return domain


def domains_match(domain_a: Optional[str], domain_b: Optional[str]) -> bool:
    """Compare two domains after normalization."""
    if domain_a is None or domain_b is None:
        return False
    norm_a = normalize_domain(domain_a)
    norm_b = normalize_domain(domain_b)
    if norm_a is None or norm_b is None:
        return False
    return norm_a == norm_b
