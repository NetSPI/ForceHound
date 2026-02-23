"""Core graph data structures used throughout ForceHound.

Every collector produces :class:`CollectionResult` objects containing
:class:`GraphNode` and :class:`GraphEdge` instances.  The
:class:`GraphBuilder` merges these into the final OpenGraph v1 output.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple


@dataclass
class GraphNode:
    """A node in the BloodHound identity graph.

    Attributes:
        id: Unique identifier — either an 18-char Salesforce ID or a
            hash-based synthetic ID.
        kinds: Ordered list of type labels (e.g., ``["SF_User", "User"]``).
        properties: Arbitrary key/value metadata attached to the node.
    """

    id: str
    kinds: List[str]
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to the OpenGraph v1 node format.

        ``None`` property values are omitted — BloodHound CE's OpenGraph
        ingestor rejects ``null`` values in node properties.
        """
        return {
            "id": self.id,
            "kinds": list(self.kinds),
            "properties": {k: v for k, v in self.properties.items() if v is not None},
        }


@dataclass
class GraphEdge:
    """A directed edge in the BloodHound identity graph.

    Attributes:
        source: ID of the source node.
        target: ID of the target node.
        kind: Relationship label (e.g., ``"HasProfile"``, ``"MemberOf"``).
        properties: Optional edge metadata (e.g., ``access_level``).
    """

    source: str
    target: str
    kind: str
    properties: Dict[str, Any] = field(default_factory=dict)

    @property
    def dedup_key(self) -> Tuple[str, str, str]:
        """Tuple used to deduplicate edges during graph merging."""
        return (self.source, self.kind, self.target)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to the BloodHound edge format."""
        result: Dict[str, Any] = {
            "start": {"value": self.source, "match_by": "id"},
            "end": {"value": self.target, "match_by": "id"},
            "kind": self.kind,
        }
        if self.properties:
            result["properties"] = dict(self.properties)
        return result


@dataclass
class CollectionResult:
    """Output of a single collector run.

    Attributes:
        nodes: All graph nodes discovered during collection.
        edges: All graph edges discovered during collection.
        collector_type: ``"api"`` or ``"aura"``.
        org_id: The Salesforce Organization ID (18-char).
        metadata: Collector-specific summary data (counts, timing, etc.).
    """

    nodes: List[GraphNode]
    edges: List[GraphEdge]
    collector_type: str
    org_id: str
    metadata: Dict[str, Any] = field(default_factory=dict)
