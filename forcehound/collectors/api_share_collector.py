"""Share-object sub-collector for the REST API backend.

Ported from ``collect_data_for_share_object_nodes.py``.  Discovers all
queryable Share objects via ``EntityDefinition``, then queries each to
build ``SF_Record`` nodes and ``ExplicitAccess`` / ``Owns`` /
``InheritsAccess`` edges.

AccountShare receives special handling:
  - Lateral access fields (``OpportunityAccessLevel``,
    ``CaseAccessLevel``, ``ContactAccessLevel``) are collected.
  - ``ControlledByParent`` values generate ``InheritsAccess`` edges to
    synthetic child-collection nodes.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceResourceNotFound

from forcehound.collectors.api_query_utils import audit_query
from forcehound.models.base import GraphEdge, GraphNode

if TYPE_CHECKING:
    from forcehound.audit import AuditLogger

logger = logging.getLogger(__name__)

# Fields that are always present on Share objects but carry no graph value.
_SYSTEM_FIELDS: Set[str] = {"Id", "CreatedById", "LastModifiedById", "LastModifiedDate"}


class ShareObjectCollector:
    """Discovers and queries Salesforce Share objects.

    Args:
        sf: An authenticated ``simple_salesforce.Salesforce`` instance.
        existing_node_ids: IDs already present as other node types
            (User, Group, etc.) to prevent label collisions.
        verbose: Emit progress messages to stdout.
    """

    def __init__(
        self,
        sf: Salesforce,
        existing_node_ids: Set[str],
        verbose: bool = False,
        audit_logger: Optional["AuditLogger"] = None,
    ) -> None:
        self._sf = sf
        self._existing_ids = existing_node_ids
        self._verbose = verbose
        self._audit_logger = audit_logger
        self.query_count: int = 0

    def _log(self, msg: str) -> None:
        if self._verbose:
            print(msg)

    def collect(self) -> Tuple[List[GraphNode], List[GraphEdge]]:
        """Discover Share objects, query records, and return nodes + edges."""
        share_map = self._query_share_objects()
        nodes = self._create_record_nodes(share_map)
        edges = self._create_sharing_edges(share_map)
        return nodes, edges

    def _audit_query(
        self,
        soql: str,
        resource_name: str = "",
    ) -> Dict[str, Any]:
        """Execute a SOQL query with optional audit logging.

        Delegates to :func:`~forcehound.collectors.api_query_utils.audit_query`.
        """
        return audit_query(
            self._sf, soql, self._audit_logger,
            resource_name=resource_name,
        )

    def _discover_share_object_names(self) -> List[str]:
        """Return all queryable Share object API names."""
        result = self._audit_query(
            "SELECT QualifiedApiName FROM EntityDefinition "
            "WHERE QualifiedApiName LIKE '%Share' AND IsQueryable=true",
            resource_name="EntityDefinition",
        )
        self.query_count += 1
        return [r["QualifiedApiName"] for r in result.get("records", [])]

    def _describe_fields(self, object_name: str) -> List[str]:
        """Return field names for *object_name*, excluding system fields."""
        try:
            describe = getattr(self._sf, object_name).describe()
            self.query_count += 1
        except (SalesforceResourceNotFound, Exception):
            return []

        return [
            f["name"]
            for f in describe.get("fields", [])
            if f["name"] not in _SYSTEM_FIELDS
        ]

    def _query_share_objects(self) -> Dict[str, List[Dict[str, Any]]]:
        """Query all Share objects and return ``{object_name: [records]}``."""
        names = self._discover_share_object_names()
        result: Dict[str, List[Dict[str, Any]]] = {}

        for name in names:
            fields = self._describe_fields(name)
            if not fields:
                continue

            parent_id_fields = [
                f for f in fields if f.endswith("Id") and f != "UserOrGroupId"
            ]
            if not parent_id_fields:
                continue

            access_fields = [
                f for f in fields if "AccessLevel" in f or f == "AccessLevel"
            ]
            if not access_fields:
                continue

            if name == "AccountShare":
                query = f"SELECT {', '.join(fields)} FROM {name}"
            else:
                parent_id_field = parent_id_fields[0]
                access_field = access_fields[0]
                query = (
                    f"SELECT {parent_id_field}, UserOrGroupId, "
                    f"{access_field}, RowCause FROM {name}"
                )

            try:
                qr = self._audit_query(query, resource_name=name)
                self.query_count += 1
                records = qr.get("records", [])
                if records:
                    result[name] = records
                    self._log(f"  {name}: {len(records)} records")
            except Exception as exc:
                logger.debug("Query failed for %s: %s", name, exc)

        return result

    def _create_record_nodes(
        self, share_map: Dict[str, List[Dict[str, Any]]]
    ) -> List[GraphNode]:
        """Create ``SF_Record`` nodes from Share records."""
        nodes: List[GraphNode] = []
        processed: Set[str] = set()

        for object_name, records in share_map.items():
            for record in records:
                parent_id = (
                    record.get("ParentId")
                    or record.get("AccountId")
                    or record.get("UserId")
                )
                if not parent_id or parent_id in self._existing_ids:
                    continue

                if parent_id not in processed:
                    base_name = object_name.replace("Share", "")
                    nodes.append(
                        GraphNode(
                            id=parent_id,
                            kinds=["SF_Record", f"SF_{base_name}"],
                            properties={
                                "name": f"{base_name} Record",
                                "id": parent_id,
                                "salesforce_id": parent_id,
                            },
                        )
                    )
                    processed.add(parent_id)

                # AccountShare synthetic child-collection nodes.
                if object_name == "AccountShare":
                    for child_type in ("Opportunity", "Case", "Contact"):
                        syn_id = f"{parent_id}_{child_type}_Collection"
                        if syn_id not in processed:
                            plural = (
                                "Opportunities"
                                if child_type == "Opportunity"
                                else f"{child_type}s"
                            )
                            nodes.append(
                                GraphNode(
                                    id=syn_id,
                                    kinds=["SF_Record", f"SF_{child_type}_Collection"],
                                    properties={
                                        "name": f"All {plural} for Account",
                                        "parent_account_id": parent_id,
                                    },
                                )
                            )
                            processed.add(syn_id)

        return nodes

    def _create_sharing_edges(
        self, share_map: Dict[str, List[Dict[str, Any]]]
    ) -> List[GraphEdge]:
        """Create Owns / ExplicitAccess / InheritsAccess edges."""
        edges: List[GraphEdge] = []

        for object_name, records in share_map.items():
            for record in records:
                parent_id = (
                    record.get("ParentId")
                    or record.get("AccountId")
                    or record.get("UserId")
                )
                user_or_group = record.get("UserOrGroupId")
                row_cause = record.get("RowCause")
                access_level = (
                    record.get("AccessLevel")
                    or record.get("AccountAccessLevel")
                    or record.get("UserAccessLevel")
                )

                if not parent_id or not user_or_group:
                    continue

                # Primary edge: Owns vs. ExplicitAccess.
                edge_kind = "Owns" if row_cause == "Owner" else "ExplicitAccess"
                edges.append(
                    GraphEdge(
                        source=user_or_group,
                        target=parent_id,
                        kind=edge_kind,
                        properties={
                            "access_level": access_level,
                            "row_cause": row_cause,
                        },
                    )
                )

                # AccountShare lateral access.
                if object_name == "AccountShare":
                    lateral_map = {
                        "OpportunityAccessLevel": "Opportunity",
                        "CaseAccessLevel": "Case",
                        "ContactAccessLevel": "Contact",
                    }
                    for field, child_type in lateral_map.items():
                        l_access = record.get(field)
                        if l_access and l_access != "None":
                            syn_id = f"{parent_id}_{child_type}_Collection"
                            if l_access == "ControlledByParent":
                                edges.append(
                                    GraphEdge(
                                        source=syn_id,
                                        target=parent_id,
                                        kind="InheritsAccess",
                                    )
                                )
                            else:
                                edges.append(
                                    GraphEdge(
                                        source=user_or_group,
                                        target=syn_id,
                                        kind="ExplicitAccess",
                                        properties={
                                            "access_level": l_access,
                                            "row_cause": row_cause,
                                        },
                                    )
                                )

        return edges
