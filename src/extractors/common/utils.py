"""
Common utilities for data extraction and loading with full type safety.
"""
import hashlib
import json
from typing import Any, Dict, List, Optional, TypeVar

# Generic type for chunking any list type
T = TypeVar('T')


def compute_record_hash(
    record: Dict[str, Any],
    exclude_fields: Optional[List[str]] = None
) -> str:
    """
    Compute deterministic hash of a record for deduplication.

    Args:
        record: Record dictionary
        exclude_fields: Fields to exclude from hash (e.g., timestamps)

    Returns:
        SHA256 hex digest

    Example:
        >>> record = {"id": 1, "name": "John", "_extracted_at": "2024-01-01"}
        >>> hash1 = compute_record_hash(record)
        >>> hash2 = compute_record_hash(record)
        >>> hash1 == hash2
        True
    """
    if exclude_fields is None:
        exclude_fields = ['_extracted_at', '_loaded_at', '_record_hash']

    # Create sorted dict without excluded fields
    filtered = {
        k: v for k, v in sorted(record.items())
        if k not in exclude_fields
    }

    # Convert to deterministic JSON string
    json_str = json.dumps(filtered, sort_keys=True, default=str)

    # Hash it
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()


def add_metadata_fields(
    record: Dict[str, Any],
    resource: str,
    extracted_at: str
) -> Dict[str, Any]:
    """
    Add standard metadata fields to a record.

    Modifies the record in-place and returns it for chaining.

    Args:
        record: Original record (modified in-place)
        resource: Resource name (e.g., 'employees', 'projects')
        extracted_at: Extraction timestamp in ISO format

    Returns:
        Record with metadata fields added (_resource, _extracted_at, _record_hash)

    Example:
        >>> from datetime import datetime
        >>> record = {"id": 1, "name": "John"}
        >>> timestamp = datetime.utcnow().isoformat()
        >>> enriched = add_metadata_fields(record, "employees", timestamp)
        >>> '_resource' in enriched
        True
    """
    record['_resource'] = resource
    record['_extracted_at'] = extracted_at
    record['_record_hash'] = compute_record_hash(record)

    return record


def chunk_list(items: List[T], chunk_size: int) -> List[List[T]]:
    """
    Split list into chunks of specified size.

    Uses list slicing for efficient chunking without external dependencies.

    Args:
        items: List to chunk
        chunk_size: Maximum chunk size (must be > 0)

    Returns:
        List of chunks, each containing at most chunk_size items

    Raises:
        ValueError: If chunk_size <= 0

    Example:
        >>> items = [1, 2, 3, 4, 5]
        >>> chunks = chunk_list(items, 2)
        >>> chunks
        [[1, 2], [3, 4], [5]]
    """
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be positive, got {chunk_size}")

    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def merge_dicts(
    base: Dict[str, Any],
    update: Dict[str, Any],
    overwrite: bool = True
) -> Dict[str, Any]:
    """
    Merge two dictionaries with configurable overwrite behavior.

    Args:
        base: Base dictionary
        update: Dictionary with updates
        overwrite: If True, update values overwrite base values.
                  If False, only add new keys without overwriting existing ones.

    Returns:
        Merged dictionary (base is modified in-place)

    Example:
        >>> base = {"a": 1, "b": 2}
        >>> update = {"b": 3, "c": 4}
        >>> result = merge_dicts(base, update, overwrite=True)
        >>> result
        {'a': 1, 'b': 3, 'c': 4}
    """
    for key, value in update.items():
        if overwrite or key not in base:
            base[key] = value
    return base


def safe_get(
    data: Dict[str, Any],
    path: str,
    default: Any = None,
    separator: str = "."
) -> Any:
    """
    Safely get nested value from dictionary using dot notation.

    Args:
        data: Dictionary to query
        path: Dot-separated path (e.g., "user.address.city")
        default: Default value if path doesn't exist
        separator: Path separator (default: ".")

    Returns:
        Value at path or default if not found

    Example:
        >>> data = {"user": {"address": {"city": "NYC"}}}
        >>> safe_get(data, "user.address.city")
        'NYC'
        >>> safe_get(data, "user.phone", "N/A")
        'N/A'
    """
    keys = path.split(separator)
    current = data

    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default

    return current
