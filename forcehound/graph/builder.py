"""Graph builder and OpenGraph v1 output formatter.

The :class:`GraphBuilder` accepts one or more :class:`CollectionResult`
objects (from the API collector, the Aura collector, or both) and merges
them into a single **OpenGraph v1** JSON structure::

    {
        "metadata": {"source_kind": "Salesforce"},
        "graph": {
            "nodes": [...],
            "edges": [...]
        }
    }

Merge semantics:
  - **Nodes** are merged by ``id``: ``kinds`` lists are unioned (order-
    preserving), and ``properties`` dicts are shallow-merged (last writer
    wins).
  - **Edges** are deduplicated by their ``(source, kind, target)`` tuple.
"""

from __future__ import annotations

import json
import logging
from collections import OrderedDict
from typing import Any, Dict, List

from forcehound.constants import CAPABILITY_TO_EDGE_KIND
from forcehound.models.base import CollectionResult, GraphEdge, GraphNode

logger = logging.getLogger(__name__)

_CAPABILITY_EDGE_KINDS: set = set(CAPABILITY_TO_EDGE_KIND.values())


class GraphBuilder:
    """Accumulates collector results and produces the final OpenGraph v1 output.

    Usage::

        builder = GraphBuilder()
        builder.add_result(api_result)
        builder.add_result(aura_result)
        output = builder.build()
        builder.save("output.json")
    """

    def __init__(self) -> None:
        self._nodes: Dict[str, GraphNode] = OrderedDict()
        self._edges: Dict[tuple, GraphEdge] = OrderedDict()
        self._results: List[CollectionResult] = []

    def add_result(self, result: CollectionResult) -> None:
        """Merge a :class:`CollectionResult` into the graph.

        Nodes with the same ``id`` are merged (kinds unioned, properties
        shallow-merged).  Edges with the same ``(source, kind, target)``
        are deduplicated — the first occurrence's properties are kept.
        """
        self._results.append(result)

        for node in result.nodes:
            self._merge_node(node)

        for edge in result.edges:
            key = edge.dedup_key
            if key not in self._edges:
                self._edges[key] = edge

    def build(self) -> Dict[str, Any]:
        """Return the OpenGraph v1 JSON-ready dict.

        Node kinds are truncated to 2 so that the auto-appended source_kind
        does not exceed the 3-kind maximum enforced by BloodHound CE.

        Edges referencing non-existent nodes are silently dropped with
        a warning — this prevents BloodHound CE ingestion failures
        caused by dangling references.
        """
        node_ids = set(self._nodes.keys())
        nodes = []
        for n in self._nodes.values():
            d = n.to_dict()
            d["kinds"] = d["kinds"][:2]
            # OpenGraph requires ``objectid`` on every node.
            d["properties"]["objectid"] = d["id"]
            nodes.append(d)

        valid_edges: list = []
        for edge in self._edges.values():
            if edge.source not in node_ids or edge.target not in node_ids:
                logger.warning(
                    "Dropping dangling edge: %s -[%s]-> %s",
                    edge.source,
                    edge.kind,
                    edge.target,
                )
                continue
            valid_edges.append(edge.to_dict())

        return {
            "metadata": {"source_kind": "Salesforce"},
            "graph": {"nodes": nodes, "edges": valid_edges},
        }

    def save(self, path: str, indent: int = 2) -> None:
        """Serialise the graph to *path* as pretty-printed JSON."""
        output = self.build()
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(output, fh, indent=indent)

    def get_summary(self) -> Dict[str, Any]:
        """Return a human-readable summary of the graph contents."""
        kind_counts: Dict[str, int] = {}
        for node in self._nodes.values():
            for kind in node.kinds:
                kind_counts[kind] = kind_counts.get(kind, 0) + 1

        edge_kind_counts: Dict[str, int] = {}
        for edge in self._edges.values():
            edge_kind_counts[edge.kind] = edge_kind_counts.get(edge.kind, 0) + 1

        return {
            "total_nodes": len(self._nodes),
            "total_edges": len(self._edges),
            "node_kinds": kind_counts,
            "edge_kinds": edge_kind_counts,
            "collectors": [r.collector_type for r in self._results],
        }

    def get_risk_summary(self) -> Dict[str, Any]:
        """Compute a per-user risk summary from the merged graph.

        Traverses ``SF_User → (HasProfile|HasPermissionSet) → entitlement
        → capability → SF_Organization`` paths to determine which dangerous
        capabilities each user holds and which entitlement grants them.

        Returns:
            A dict keyed by user node ID.  Each value contains ``name``,
            ``email``, ``is_active``, and ``capabilities`` — a dict mapping
            capability edge kinds to lists of ``(source_name, source_type)``
            tuples.
        """
        # Build an index: source_id → list of outgoing edges.
        edges_from: Dict[str, List[GraphEdge]] = {}
        for edge in self._edges.values():
            edges_from.setdefault(edge.source, []).append(edge)

        summary: Dict[str, Any] = {}

        for node in self._nodes.values():
            if "SF_User" not in node.kinds:
                continue

            capabilities: Dict[str, List[tuple]] = {}

            # Follow HasProfile / HasPermissionSet edges from this user.
            for edge in edges_from.get(node.id, []):
                if edge.kind not in ("HasProfile", "HasPermissionSet"):
                    continue

                entitlement = self._nodes.get(edge.target)
                if entitlement is None:
                    continue

                ent_name = entitlement.properties.get("name", entitlement.id)
                ent_type = self._classify_entitlement(entitlement)

                # Follow capability edges from this entitlement.
                source_entry = (ent_name, ent_type)
                for cap_edge in edges_from.get(entitlement.id, []):
                    if cap_edge.kind not in _CAPABILITY_EDGE_KINDS:
                        continue
                    cap_list = capabilities.setdefault(cap_edge.kind, [])
                    if source_entry not in cap_list:
                        cap_list.append(source_entry)

            if capabilities:
                summary[node.id] = {
                    "name": node.properties.get("name", node.id),
                    "email": node.properties.get("email", ""),
                    "is_active": node.properties.get("is_active", True),
                    "capabilities": capabilities,
                }

        return summary

    @staticmethod
    def _classify_entitlement(node: GraphNode) -> str:
        """Return a human label for an entitlement node type."""
        if "SF_Profile" in node.kinds:
            return "Profile"
        # PSGs are SF_PermissionSet nodes that have a 'label' property
        # (set by _create_psg_nodes with DeveloperName/MasterLabel).
        if "SF_PermissionSet" in node.kinds and node.properties.get("label"):
            return "PermissionSetGroup"
        if "SF_PermissionSet" in node.kinds:
            return "PermissionSet"
        return "Unknown"

    def _merge_node(self, node: GraphNode) -> None:
        """Merge *node* into the internal node map.

        If a node with the same ``id`` already exists:
          - ``kinds`` are unioned in insertion order.
          - ``properties`` are shallow-merged (new values overwrite old).
        """
        existing = self._nodes.get(node.id)
        if existing is None:
            self._nodes[node.id] = GraphNode(
                id=node.id,
                kinds=list(node.kinds),
                properties=dict(node.properties),
            )
            return

        # Union kinds — preserve insertion order.
        seen = set(existing.kinds)
        for kind in node.kinds:
            if kind not in seen:
                existing.kinds.append(kind)
                seen.add(kind)

        # Shallow-merge properties (last writer wins).
        existing.properties.update(node.properties)
