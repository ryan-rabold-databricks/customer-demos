"""Deterministic keys and column fingerprints.

make_key matches the make_key SQL function created by the setup job, so a key computed in Python
equals the key computed in SQL for the same inputs. A retried run therefore rebuilds the same keys.
"""
import hashlib
from typing import Iterable, Optional

_SEPARATOR = "\x1f"  # Unit separator: cannot appear in Unity Catalog names.
_NULL = "\x00"  # Distinct placeholder so NULL never collides with an empty string.


def make_key(prefix: str, parts: Iterable[Optional[str]]) -> str:
    joined = _SEPARATOR.join(_NULL if p is None else str(p) for p in parts)
    digest = hashlib.sha256(joined.encode("utf-8")).hexdigest()
    return f"{prefix}-{digest[:16].upper()}"


def column_fingerprint(
    catalog: str, schema: str, table: str, column: str, full_data_type: str, comment: Optional[str]
) -> str:
    """SHA-256 of the normalized column identity, full data type, and comment.

    Ordinal position is excluded so reordering columns does not trigger review.
    """
    normalized = "|".join(
        [
            catalog.strip().lower(),
            schema.strip().lower(),
            table.strip().lower(),
            column.strip().lower(),
            full_data_type.strip().upper(),
            (comment or "").strip().lower(),
        ]
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
