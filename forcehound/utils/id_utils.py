"""Salesforce ID utilities — 15-to-18 char conversion and hash-based IDs.

Salesforce record IDs come in two forms:
  - 15-character case-sensitive IDs
  - 18-character case-insensitive IDs (with 3-char checksum suffix)

ForceHound always stores the 18-character form.  For synthetic nodes that
have no native Salesforce ID (e.g., namespaced objects), we generate a
deterministic hash-based ID from the node kind and a unique identifier.
"""

import hashlib
from typing import Optional


# Lookup table for the 15→18 checksum algorithm.
# Each position in the 3-char suffix encodes which of the 5 chars in a
# 5-char chunk are uppercase, as a base-26 value (A-Z + 0-5).
_CHECKSUM_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"


def ensure_18_char_id(sf_id: Optional[str]) -> Optional[str]:
    """Convert a 15-character Salesforce ID to its 18-character form.

    If the input is already 18 characters or ``None``, it is returned as-is.
    Invalid lengths raise ``ValueError``.

    Args:
        sf_id: A 15- or 18-character Salesforce record ID, or ``None``.

    Returns:
        The 18-character case-insensitive ID, or ``None`` if input was ``None``.

    Raises:
        ValueError: If *sf_id* is not 15 or 18 characters long.
    """
    if sf_id is None:
        return None

    if len(sf_id) == 18:
        return sf_id

    if len(sf_id) != 15:
        raise ValueError(
            f"Salesforce ID must be 15 or 18 characters, got {len(sf_id)}: {sf_id!r}"
        )

    suffix = ""
    for chunk_start in range(0, 15, 5):
        flags = 0
        for bit_pos in range(5):
            char = sf_id[chunk_start + bit_pos]
            if char.isupper():
                flags |= 1 << bit_pos
        suffix += _CHECKSUM_CHARS[flags]

    return sf_id + suffix


def generate_hash_id(kind: str, identifier: str) -> str:
    """Create a deterministic 18-character uppercase hex ID from *kind* and *identifier*.

    Used for synthetic nodes (e.g., ``SF_NamespacedObject``) that have no
    native Salesforce record ID.

    Args:
        kind: The node kind (e.g., ``"SF_Object"``).
        identifier: A unique string within that kind (e.g., ``"Account"``).

    Returns:
        An 18-character uppercase hexadecimal string derived from the
        SHA-256 hash of ``"{kind}:{identifier}"``.
    """
    digest = hashlib.sha256(f"{kind}:{identifier}".encode()).hexdigest().upper()
    # Take the first 18 hex chars — sufficient uniqueness for our graph.
    return digest[:18]
