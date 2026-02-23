"""Lookup cache and dependency ordering for CRUD probing.

Builds a dependency graph from object field metadata (reference fields),
performs topological sort so parent objects are probed before children,
and caches record IDs for foreign-key resolution.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class LookupCache:
    """Cache of record IDs keyed by object API name.

    Populated during CRUD probing: when a record is created or discovered
    via ``get_items``, its ID is stored here so child objects can resolve
    their required reference fields.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, str] = {}  # object_name → record_id

    def put(self, object_name: str, record_id: str) -> None:
        """Store a record ID for the given object type."""
        self._cache[object_name] = record_id

    def get(self, object_name: str) -> Optional[str]:
        """Return a cached record ID, or ``None``."""
        return self._cache.get(object_name)

    def has(self, object_name: str) -> bool:
        """Return ``True`` if a record ID is cached for this object."""
        return object_name in self._cache

    def __repr__(self) -> str:
        return f"LookupCache({len(self._cache)} objects)"


def build_dependency_order(
    object_fields: Dict[str, List[Dict[str, Any]]],
    target_objects: Set[str],
) -> List[str]:
    """Return a topologically sorted list of object names.

    Objects with no dependencies come first. Objects that depend on
    others (via required reference fields) come after their parents.
    Circular dependencies are broken by dropping the back-edge.

    Args:
        object_fields: Mapping of ``object_name`` → field metadata list.
            Each field dict should have keys ``field_name``,
            ``is_reference``, ``reference_object``, and ``required``.
        target_objects: Set of object names to include in the ordering.

    Returns:
        List of object names in dependency order.
    """
    # Build adjacency list: object → set of objects it depends on
    deps: Dict[str, Set[str]] = defaultdict(set)
    for obj_name in target_objects:
        deps[obj_name]  # ensure entry exists even with no deps
        fields = object_fields.get(obj_name, [])
        for field in fields:
            if (
                field.get("is_reference")
                and field.get("required")
                and field.get("createable", True)
            ):
                ref_obj = field.get("reference_object")
                if ref_obj and ref_obj in target_objects and ref_obj != obj_name:
                    deps[obj_name].add(ref_obj)

    # Kahn's algorithm for topological sort
    in_degree: Dict[str, int] = {obj: 0 for obj in target_objects}
    reverse: Dict[str, Set[str]] = defaultdict(set)

    for obj, parents in deps.items():
        for parent in parents:
            reverse[parent].add(obj)
            in_degree[obj] = in_degree.get(obj, 0) + 1

    queue: deque[str] = deque(
        obj for obj in target_objects if in_degree.get(obj, 0) == 0
    )
    result: List[str] = []

    while queue:
        obj = queue.popleft()
        result.append(obj)
        for child in reverse.get(obj, set()):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    # Any remaining objects have circular deps — append them at the end
    remaining = target_objects - set(result)
    if remaining:
        logger.debug(
            "Circular dependencies detected for: %s — appending at end",
            remaining,
        )
        result.extend(sorted(remaining))

    return result
