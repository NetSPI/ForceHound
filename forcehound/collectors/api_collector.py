"""Privileged REST API collector using ``simple_salesforce``.

Ported from ``salesforce_collector.py``.  In full mode (``--collector api``)
executes up to 20 SOQL queries to collect Users, Profiles, PermissionSets,
PermissionSetGroups, Roles, Groups, GroupMembers, EntityDefinitions,
ObjectPermissions, FieldPermissions, Connected Applications, and Share
objects — then builds a complete BloodHound identity graph.

In **supplement mode** (``--collector both``), the Aura collector handles
Users, Profiles, Roles, Groups, and GroupMembers.  The API collector
only runs up to 12 queries Aura cannot cover: PermissionSets, PSGs, PSA,
EntityDefinitions, ObjectPermissions, FieldPermissions, OWD, Connected
Applications, and Share objects.

Requires either ``session_id`` or ``username + password`` authentication
with sufficient API privileges.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

if TYPE_CHECKING:
    from forcehound.audit import AuditLogger

from simple_salesforce import Salesforce

from forcehound.collectors.api_query_utils import audit_query
from forcehound.collectors.api_share_collector import ShareObjectCollector
from forcehound.collectors.base import BaseCollector
from forcehound.constants import (
    CAPABILITY_FIELDS,
    CAPABILITY_TO_EDGE_KIND,
    ENTITY_DEFINITION_FIELDS,
    FIELD_PERMISSION_FIELDS,
    FIELD_PERMISSION_TO_EDGE_KIND,
    NODE_KINDS,
    OBJECT_PERMISSION_FIELDS,
    OBJECT_PERMISSION_TO_EDGE_KIND,
    OWD_FIELDS,
    SALESFORCE_API_VERSION,
)
from forcehound.models.auth import AuthConfig
from forcehound.models.base import CollectionResult, GraphEdge, GraphNode
from forcehound.utils.id_utils import generate_hash_id

logger = logging.getLogger(__name__)


class APICollector(BaseCollector):
    """Privileged Salesforce collector via the REST API.

    Executes SOQL queries through ``simple_salesforce`` to build a
    comprehensive identity graph including Share-object access paths
    and field-level security (FLS).

    Args:
        auth: :class:`AuthConfig` with API credentials.
        verbose: Emit progress messages to stdout.
        supplement_only: When ``True``, skip queries already covered by
            the Aura collector (Users, Profiles, Roles, Groups,
            GroupMembers, Profile capabilities).  Used in ``both`` mode.
        known_node_ids: Node IDs from a prior Aura collection run.
            Passed to the Share collector so it avoids creating duplicate
            nodes for Users and Groups already in the graph.
        org_id: Organisation ID from a prior Aura collection run.
            When provided, the initial ``SELECT Id FROM Organization``
            query is skipped (saves one API call).
        skip_object_permissions: When ``True``, skip the
            ObjectPermissions query entirely.  Eliminates SF_Object
            nodes and all CRUD edges, dramatically reducing output size.
        skip_shares: When ``True``, skip Share-object discovery and
            queries.  Eliminates SF_Record nodes and all
            ExplicitAccess/Owns/InheritsAccess edges.
        skip_field_permissions: When ``True``, skip the
            FieldPermissions query entirely.  Eliminates SF_Field
            nodes and CanReadField/CanEditField/FieldOf edges.
    """

    def __init__(
        self,
        auth: AuthConfig,
        verbose: bool = False,
        supplement_only: bool = False,
        known_node_ids: Optional[Set[str]] = None,
        org_id: Optional[str] = None,
        skip_object_permissions: bool = False,
        active_only: bool = False,
        skip_shares: bool = False,
        skip_field_permissions: bool = False,
        skip_entity_definitions: bool = False,
        audit_logger: Optional["AuditLogger"] = None,
        proxy: Optional[str] = None,
        rate_limit: Optional[float] = None,
    ) -> None:
        super().__init__(auth, verbose, audit_logger=audit_logger, proxy=proxy, rate_limit=rate_limit)
        self._sf: Optional[Salesforce] = None
        self._query_count: int = 0
        self._supplement_only = supplement_only
        self._known_node_ids = known_node_ids or set()
        self._provided_org_id = org_id
        self._skip_object_permissions = skip_object_permissions
        self._active_only = active_only
        self._skip_shares = skip_shares
        self._skip_field_permissions = skip_field_permissions
        self._skip_entity_definitions = skip_entity_definitions

    async def collect(self) -> CollectionResult:
        """Execute the REST API collection flow.

        In full mode, runs up to 20 SOQL queries (18 base + ObjPerms + FLS).
        In supplement mode (``supplement_only=True``), skips the 7
        queries Aura already covers and only runs up to 12.
        """
        self.auth.validate_for_api()
        sf = self._connect()
        self._sf = sf

        if self._supplement_only:
            self._log("=== ForceHound API Collector (supplement mode) ===")
        else:
            self._log("=== ForceHound API Collector ===")

        if self._provided_org_id:
            org_id = self._provided_org_id
            self._log(f"Using org ID from Aura collector: {org_id}")
        else:
            self._log("Querying Organization ID...")
            org_id = sf.query("SELECT Id FROM Organization")["records"][0]["Id"]
            self._query_count += 1

        if self._supplement_only:
            self._log("Running supplement SOQL queries...")
            data = await asyncio.to_thread(self._run_supplement_queries, sf, org_id)
        else:
            self._log("Running SOQL queries...")
            data = await asyncio.to_thread(self._run_queries, sf, org_id)

        self._log("Building nodes...")
        nodes, share_edges, psg_shadow_map = self._build_all_nodes(data, sf, org_id)

        self._log("Building edges...")
        edges = self._build_all_edges(data, org_id, share_edges, psg_shadow_map)

        self._log(f"  Total nodes: {len(nodes)}")
        self._log(f"  Total edges: {len(edges)}")
        self._log(f"  Total queries: {self._query_count}")

        return CollectionResult(
            nodes=nodes,
            edges=edges,
            collector_type="api",
            org_id=org_id,
            metadata={
                "users": len(data["user_fields"]["records"]),
                "profiles": len(data["profile_fields"]["records"]),
                "permission_sets": len(data["permission_set_fields"]["records"]),
                "roles": len(data["role_hierarchy_fields"]["records"]),
                "groups": len(data["group_fields"]["records"]),
                "connected_apps": len(data["connected_app_fields"]["records"]),
                "queries": self._query_count,
            },
        )

    def _connect(self) -> Salesforce:
        """Create an authenticated ``simple_salesforce`` instance."""
        url = self.auth.instance_url
        # simple_salesforce expects the 'instance' without protocol.
        instance = url.replace("https://", "").replace("http://", "")

        proxies = None
        if self.proxy:
            proxies = {"http": self.proxy, "https": self.proxy}

        if self.auth.session_id:
            return Salesforce(
                instance=instance,
                session_id=self.auth.session_id,
                version=SALESFORCE_API_VERSION,
                proxies=proxies,
            )

        return Salesforce(
            username=self.auth.username,
            password=self.auth.password,
            security_token=self.auth.security_token,
            instance=instance,
            version=SALESFORCE_API_VERSION,
            proxies=proxies,
        )

    def _query(
        self,
        sf: Salesforce,
        soql: str,
        operation: str = "query_all",
        resource_name: str = "",
    ) -> Dict[str, Any]:
        """Execute a SOQL query with optional audit logging.

        Delegates to :func:`~forcehound.collectors.api_query_utils.audit_query`.
        """
        return audit_query(
            sf, soql, self.audit_logger,
            operation=operation, resource_name=resource_name,
        )

    def _run_queries(self, sf: Salesforce, org_id: str) -> Dict[str, Any]:
        """Execute all SOQL queries and return the results dict."""
        cap_fields = ", ".join(CAPABILITY_FIELDS)
        cap_where = " OR ".join(f"{f} = true" for f in CAPABILITY_FIELDS)

        profile_cap = self._query(
            sf, f"SELECT Id, {cap_fields} FROM Profile WHERE {cap_where}",
            resource_name="Profile",
        )
        ps_cap = self._query(
            sf,
            f"SELECT Id, {cap_fields} FROM PermissionSet "
            f"WHERE ({cap_where}) AND IsOwnedByProfile = false "
            f"AND Type != 'Group'",
            resource_name="PermissionSet",
        )
        psg_cap = self._query(
            sf,
            f"SELECT Id, PermissionSetGroupId, {cap_fields} "
            f"FROM PermissionSet WHERE Type = 'Group'",
            resource_name="PermissionSet",
        )

        owd_select = ", ".join(OWD_FIELDS)
        organization_fields = self._query(
            sf, f"SELECT Id, Name, {owd_select} FROM Organization",
            resource_name="Organization",
        )
        management_fields = self._query(
            sf, "SELECT Id, ManagerId FROM User WHERE ManagerId != null",
            resource_name="User",
        )
        role_hierarchy_fields = self._query(
            sf, "SELECT Id, Name, ParentRoleId, PortalType FROM UserRole",
            resource_name="UserRole",
        )
        psa_fields = self._query(
            sf,
            "SELECT AssigneeId, PermissionSetId, "
            "PermissionSet.IsOwnedByProfile, PermissionSet.ProfileId, "
            "PermissionSetGroupId FROM PermissionSetAssignment",
            resource_name="PermissionSetAssignment",
        )
        permission_set_fields = self._query(
            sf,
            "SELECT Id, Name, Label, Type FROM PermissionSet "
            "WHERE IsOwnedByProfile = false",
            resource_name="PermissionSet",
        )
        profile_fields = self._query(
            sf, "SELECT Id, Name, Description FROM Profile",
            resource_name="Profile",
        )
        if self._active_only:
            user_fields = self._query(
                sf,
                "SELECT Id, Name, Email, UserType, IsActive, UserRoleId FROM User "
                "WHERE IsActive = true",
                resource_name="User",
            )
        else:
            user_fields = self._query(
                sf,
                "SELECT Id, Name, Email, UserType, IsActive, UserRoleId FROM User",
                resource_name="User",
            )
        psg_fields = self._query(
            sf, "SELECT Id, DeveloperName, MasterLabel FROM PermissionSetGroup",
            resource_name="PermissionSetGroup",
        )
        psgc_fields = self._query(
            sf,
            "SELECT PermissionSetId, PermissionSetGroupId "
            "FROM PermissionSetGroupComponent",
            resource_name="PermissionSetGroupComponent",
        )
        group_fields = self._query(
            sf,
            "SELECT Id, Name, DeveloperName, Type, RelatedId FROM [Group]".replace(
                "[Group]", "Group"
            ),
            resource_name="Group",
        )
        group_member_fields = self._query(
            sf, "SELECT GroupId, UserOrGroupId FROM GroupMember",
            resource_name="GroupMember",
        )
        queue_sobject_fields = self._query(
            sf, "SELECT QueueId, SobjectType FROM QueueSobject",
            resource_name="QueueSobject",
        )
        self._resolve_queue_sobject_types(sf, queue_sobject_fields)

        empty: Dict[str, Any] = {"records": []}

        # EntityDefinition metadata for per-object sharing model enrichment.
        if self._skip_entity_definitions:
            entity_definition_fields = empty
        else:
            ed_select = ", ".join(ENTITY_DEFINITION_FIELDS)
            entity_definition_fields = self._query(
                sf,
                f"SELECT {ed_select} FROM EntityDefinition "
                f"WHERE IsDeprecatedAndHidden = false",
                resource_name="EntityDefinition",
            )

        # Object-level CRUD permissions per PermissionSet/Profile.
        # Source: supplemental_info HTTP capture confirmed query and field names.
        # Parent relationship fields resolve ownership:
        #   - IsOwnedByProfile + ProfileId: remap profile shadow PS → Profile node
        #   - Name + Label: used to create fallback nodes for ParentIds that don't
        #     match any collected PermissionSet (e.g. 0PL, 100 key prefixes)
        if self._skip_object_permissions:
            obj_perm_fields = empty
        else:
            obj_perm_select = ", ".join(OBJECT_PERMISSION_FIELDS)
            obj_perm_fields = self._query(
                sf,
                f"SELECT SobjectType, {obj_perm_select}, ParentId, "
                f"Parent.IsOwnedByProfile, Parent.ProfileId, "
                f"Parent.Name, Parent.Label "
                f"FROM ObjectPermissions",
                resource_name="ObjectPermissions",
            )

        # Field-level security (FLS) permissions per field per PermissionSet/Profile.
        if self._skip_field_permissions:
            field_perm_fields = empty
        else:
            field_perm_select = ", ".join(FIELD_PERMISSION_FIELDS)
            field_perm_fields = self._query(
                sf,
                f"SELECT SobjectType, Field, {field_perm_select}, ParentId, "
                f"Parent.IsOwnedByProfile, Parent.ProfileId, "
                f"Parent.Name, Parent.Label "
                f"FROM FieldPermissions",
                resource_name="FieldPermissions",
            )

        # Connected Applications and their access grants.
        connected_app_fields = self._query(
            sf,
            "SELECT Id, Name, CreatedById, OptionsAllowAdminApprovedUsersOnly, "
            "OptionsIsInternal, StartUrl, RefreshTokenValidityPeriod "
            "FROM ConnectedApplication",
            resource_name="ConnectedApplication",
        )
        connected_app_access_fields = self._query(
            sf,
            "SELECT SetupEntityId, ParentId, "
            "Parent.IsOwnedByProfile, Parent.ProfileId "
            "FROM SetupEntityAccess "
            "WHERE SetupEntityType = 'ConnectedApplication'",
            resource_name="SetupEntityAccess",
        )

        # Query count: 15 base + EntityDef(0|1) + ObjectPermissions(0|1)
        #   + FLS(0|1) + 2 ConnectedApp = 17..20
        query_count = 17
        if not self._skip_entity_definitions:
            query_count += 1
        if not self._skip_object_permissions:
            query_count += 1
        if not self._skip_field_permissions:
            query_count += 1
        self._query_count += query_count

        return {
            "profile_cap": profile_cap,
            "ps_cap": ps_cap,
            "psg_cap": psg_cap,
            "organization_fields": organization_fields,
            "management_fields": management_fields,
            "role_hierarchy_fields": role_hierarchy_fields,
            "psa_fields": psa_fields,
            "permission_set_fields": permission_set_fields,
            "profile_fields": profile_fields,
            "user_fields": user_fields,
            "psg_fields": psg_fields,
            "psgc_fields": psgc_fields,
            "group_fields": group_fields,
            "group_member_fields": group_member_fields,
            "queue_sobject_fields": queue_sobject_fields,
            "entity_definition_fields": entity_definition_fields,
            "obj_perm_fields": obj_perm_fields,
            "field_perm_fields": field_perm_fields,
            "connected_app_fields": connected_app_fields,
            "connected_app_access_fields": connected_app_access_fields,
        }

    def _run_supplement_queries(self, sf: Salesforce, org_id: str) -> Dict[str, Any]:
        """Run only the queries Aura cannot cover (up to 12 of 20).

        Skips: profile_cap, management_fields, role_hierarchy_fields,
        profile_fields, user_fields, group_fields, group_member_fields.
        These return ``{"records": []}`` so downstream builders produce
        no output for them.
        """
        cap_fields = ", ".join(CAPABILITY_FIELDS)
        cap_where = " OR ".join(f"{f} = true" for f in CAPABILITY_FIELDS)
        empty: Dict[str, Any] = {"records": []}

        # -- Queries that only the API can provide --
        ps_cap = self._query(
            sf,
            f"SELECT Id, {cap_fields} FROM PermissionSet "
            f"WHERE ({cap_where}) AND IsOwnedByProfile = false "
            f"AND Type != 'Group'",
            resource_name="PermissionSet",
        )
        psg_cap = self._query(
            sf,
            f"SELECT Id, PermissionSetGroupId, {cap_fields} "
            f"FROM PermissionSet WHERE Type = 'Group'",
            resource_name="PermissionSet",
        )
        owd_select = ", ".join(OWD_FIELDS)
        organization_fields = self._query(
            sf, f"SELECT Id, Name, {owd_select} FROM Organization",
            resource_name="Organization",
        )
        psa_fields = self._query(
            sf,
            "SELECT AssigneeId, PermissionSetId, "
            "PermissionSet.IsOwnedByProfile, PermissionSet.ProfileId, "
            "PermissionSetGroupId FROM PermissionSetAssignment",
            resource_name="PermissionSetAssignment",
        )
        permission_set_fields = self._query(
            sf,
            "SELECT Id, Name, Label, Type FROM PermissionSet "
            "WHERE IsOwnedByProfile = false",
            resource_name="PermissionSet",
        )
        psg_fields = self._query(
            sf, "SELECT Id, DeveloperName, MasterLabel FROM PermissionSetGroup",
            resource_name="PermissionSetGroup",
        )
        psgc_fields = self._query(
            sf,
            "SELECT PermissionSetId, PermissionSetGroupId "
            "FROM PermissionSetGroupComponent",
            resource_name="PermissionSetGroupComponent",
        )
        # EntityDefinition metadata for per-object sharing model enrichment.
        if self._skip_entity_definitions:
            entity_definition_fields = empty
        else:
            ed_select = ", ".join(ENTITY_DEFINITION_FIELDS)
            entity_definition_fields = self._query(
                sf,
                f"SELECT {ed_select} FROM EntityDefinition "
                f"WHERE IsDeprecatedAndHidden = false",
                resource_name="EntityDefinition",
            )

        if self._skip_object_permissions:
            obj_perm_fields = empty
        else:
            obj_perm_select = ", ".join(OBJECT_PERMISSION_FIELDS)
            obj_perm_fields = self._query(
                sf,
                f"SELECT SobjectType, {obj_perm_select}, ParentId, "
                f"Parent.IsOwnedByProfile, Parent.ProfileId, "
                f"Parent.Name, Parent.Label "
                f"FROM ObjectPermissions",
                resource_name="ObjectPermissions",
            )

        # Field-level security (FLS) permissions.
        if self._skip_field_permissions:
            field_perm_fields = empty
        else:
            field_perm_select = ", ".join(FIELD_PERMISSION_FIELDS)
            field_perm_fields = self._query(
                sf,
                f"SELECT SobjectType, Field, {field_perm_select}, ParentId, "
                f"Parent.IsOwnedByProfile, Parent.ProfileId, "
                f"Parent.Name, Parent.Label "
                f"FROM FieldPermissions",
                resource_name="FieldPermissions",
            )

        # Connected Applications and their access grants.
        connected_app_fields = self._query(
            sf,
            "SELECT Id, Name, CreatedById, OptionsAllowAdminApprovedUsersOnly, "
            "OptionsIsInternal, StartUrl, RefreshTokenValidityPeriod "
            "FROM ConnectedApplication",
            resource_name="ConnectedApplication",
        )
        connected_app_access_fields = self._query(
            sf,
            "SELECT SetupEntityId, ParentId, "
            "Parent.IsOwnedByProfile, Parent.ProfileId "
            "FROM SetupEntityAccess "
            "WHERE SetupEntityType = 'ConnectedApplication'",
            resource_name="SetupEntityAccess",
        )

        # Query count: 7 base + EntityDef(0|1) + ObjectPermissions(0|1)
        #   + FLS(0|1) + 2 ConnectedApp = 9..12
        query_count = 9
        if not self._skip_entity_definitions:
            query_count += 1
        if not self._skip_object_permissions:
            query_count += 1
        if not self._skip_field_permissions:
            query_count += 1
        self._query_count += query_count

        return {
            "profile_cap": empty,
            "ps_cap": ps_cap,
            "psg_cap": psg_cap,
            "organization_fields": organization_fields,
            "management_fields": empty,
            "role_hierarchy_fields": empty,
            "psa_fields": psa_fields,
            "permission_set_fields": permission_set_fields,
            "profile_fields": empty,
            "user_fields": empty,
            "psg_fields": psg_fields,
            "psgc_fields": psgc_fields,
            "group_fields": empty,
            "group_member_fields": empty,
            "queue_sobject_fields": empty,
            "entity_definition_fields": entity_definition_fields,
            "obj_perm_fields": obj_perm_fields,
            "field_perm_fields": field_perm_fields,
            "connected_app_fields": connected_app_fields,
            "connected_app_access_fields": connected_app_access_fields,
        }

    def _build_entity_definition_lookup(
        self, entity_def_data: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Build QualifiedApiName -> properties dict from EntityDefinition."""
        lookup: Dict[str, Dict[str, Any]] = {}
        for rec in entity_def_data["records"]:
            api_name = rec.get("QualifiedApiName")
            if not api_name:
                continue
            props: Dict[str, Any] = {}
            for field in ENTITY_DEFINITION_FIELDS:
                if field == "QualifiedApiName":
                    continue
                val = rec.get(field)
                if val is not None:
                    props[field] = val
            lookup[api_name] = props
        return lookup

    def _build_all_nodes(
        self, data: Dict[str, Any], sf: Salesforce, org_id: str
    ) -> tuple[List[GraphNode], List[GraphEdge], Dict[str, str]]:
        """Build all graph nodes from the SOQL results.

        Returns:
            A 3-tuple of:
              - All graph nodes.
              - Share edges (produced during node building because the
                share collector discovers nodes and edges together).
              - PSG shadow map (shadow PS ID -> PSG ID) used by
                edge-building methods to remap source IDs.
        """
        user_nodes = self._create_user_nodes(data["user_fields"])
        profile_nodes = self._create_profile_nodes(data["profile_fields"])
        ps_nodes = self._create_permission_set_nodes(data["permission_set_fields"])
        org_nodes = self._create_organization_nodes(data["organization_fields"])
        role_nodes = self._create_role_nodes(data["role_hierarchy_fields"])
        psg_nodes = self._create_psg_nodes(data["psg_fields"])
        group_nodes = self._create_group_nodes(data["group_fields"])

        # Share object nodes — use known_node_ids from Aura in supplement
        # mode, since user_fields/group_fields are empty.
        if self._skip_shares:
            share_nodes: List[GraphNode] = []
            share_edges: List[GraphEdge] = []
        else:
            existing_ids: Set[str] = set(self._known_node_ids)
            for rec in data["user_fields"]["records"]:
                existing_ids.add(rec["Id"])
            for rec in data["group_fields"]["records"]:
                existing_ids.add(rec["Id"])

            share_collector = ShareObjectCollector(
                sf, existing_ids, self._verbose, audit_logger=self.audit_logger
            )
            share_nodes, share_edges = share_collector.collect()
            self._query_count += share_collector.query_count

        entity_def_lookup = self._build_entity_definition_lookup(
            data["entity_definition_fields"]
        )
        object_nodes = self._create_object_nodes(
            data["obj_perm_fields"], entity_def_lookup
        )
        queue_object_nodes = self._create_queue_sobject_object_nodes(
            data["queue_sobject_fields"], entity_def_lookup
        )
        field_nodes, fls_object_nodes = self._create_field_nodes(
            data["field_perm_fields"], entity_def_lookup
        )
        connected_app_nodes = self._create_connected_app_nodes(
            data["connected_app_fields"]
        )

        # Build set of all known node IDs so we can detect orphan parents.
        known_ids: Set[str] = set()
        for node_list in (
            user_nodes,
            profile_nodes,
            ps_nodes,
            org_nodes,
            role_nodes,
            psg_nodes,
            group_nodes,
            share_nodes,
            object_nodes,
            queue_object_nodes,
            field_nodes,
            fls_object_nodes,
            connected_app_nodes,
        ):
            for n in node_list:
                known_ids.add(n.id)

        # Build PSG shadow PS -> PSG ID mapping for edge remapping.
        psg_shadow_map: Dict[str, str] = {}
        for rec in data["psg_cap"]["records"]:
            psg_shadow_map[rec["Id"]] = rec["PermissionSetGroupId"]

        # Create fallback nodes for ObjectPermissions and FieldPermissions
        # parents that aren't already represented (e.g. key prefixes 0PL, 100).
        fallback_nodes = self._create_fallback_parent_nodes(
            data["obj_perm_fields"],
            known_ids,
            psg_shadow_map,
        )
        for n in fallback_nodes:
            known_ids.add(n.id)
        fallback_fls_nodes = self._create_fallback_parent_nodes(
            data["field_perm_fields"],
            known_ids,
            psg_shadow_map,
        )

        all_nodes = (
            user_nodes
            + profile_nodes
            + ps_nodes
            + org_nodes
            + role_nodes
            + psg_nodes
            + group_nodes
            + share_nodes
            + object_nodes
            + queue_object_nodes
            + field_nodes
            + fls_object_nodes
            + connected_app_nodes
            + fallback_nodes
            + fallback_fls_nodes
        )

        return all_nodes, share_edges, psg_shadow_map

    def _build_all_edges(
        self,
        data: Dict[str, Any],
        org_id: str,
        share_edges: List[GraphEdge],
        psg_shadow_map: Dict[str, str],
    ) -> List[GraphEdge]:
        """Build all graph edges from the SOQL results."""
        role_edges = self._create_role_hierarchy_edges(data["role_hierarchy_fields"])
        has_role_edges = self._create_has_role_edges(data["user_fields"])
        mgmt_edges = self._create_management_edges(data["management_fields"])
        assign_edges = self._create_assignment_edges(data["psa_fields"])

        # Capability edges from profiles, permission sets, and PSG shadow PS.
        profile_cap = self._create_capability_edges(data["profile_cap"], org_id)
        ps_cap = self._create_capability_edges(data["ps_cap"], org_id)
        psg_cap = self._create_psg_capability_edges(data["psg_cap"], org_id)

        psgc_edges = self._create_psgc_edges(data["psgc_fields"])
        group_edges = self._create_group_member_edges(data["group_member_fields"])
        queue_sobject_edges = self._create_queue_sobject_edges(
            data["queue_sobject_fields"]
        )

        obj_perm_edges = self._create_object_permission_edges(
            data["obj_perm_fields"], psg_shadow_map
        )
        field_perm_edges = self._create_field_permission_edges(
            data["field_perm_fields"], psg_shadow_map
        )

        ca_access_edges = self._create_connected_app_access_edges(
            data["connected_app_access_fields"], psg_shadow_map
        )
        implicit_ca_edges = self._create_implicit_connected_app_edges(
            data["connected_app_fields"], data["profile_fields"]
        )
        ca_created_by_edges = self._create_connected_app_created_by_edges(
            data["connected_app_fields"]
        )

        # Share edges were produced during node building and passed in.

        return (
            role_edges
            + has_role_edges
            + mgmt_edges
            + assign_edges
            + profile_cap
            + ps_cap
            + psg_cap
            + psgc_edges
            + group_edges
            + queue_sobject_edges
            + share_edges
            + obj_perm_edges
            + field_perm_edges
            + ca_access_edges
            + implicit_ca_edges
            + ca_created_by_edges
        )

    # -- User nodes --

    def _create_user_nodes(self, user_data: Dict) -> List[GraphNode]:
        nodes: List[GraphNode] = []
        for rec in user_data["records"]:
            nodes.append(
                GraphNode(
                    id=rec["Id"],
                    kinds=list(NODE_KINDS["user"]),
                    properties={
                        "name": rec["Name"],
                        "email": rec["Email"],
                        "user_type": rec["UserType"],
                        "is_active": rec["IsActive"],
                        "is_system_user": rec["UserType"] != "Standard",
                        "id": rec["Id"],
                    },
                )
            )
        return nodes

    # -- Profile nodes --

    def _create_profile_nodes(self, profile_data: Dict) -> List[GraphNode]:
        nodes: List[GraphNode] = []
        for rec in profile_data["records"]:
            nodes.append(
                GraphNode(
                    id=rec["Id"],
                    kinds=list(NODE_KINDS["profile"]),
                    properties={"name": rec["Name"]},
                )
            )
        return nodes

    # -- PermissionSet nodes --

    def _create_permission_set_nodes(self, ps_data: Dict) -> List[GraphNode]:
        nodes: List[GraphNode] = []
        for rec in ps_data["records"]:
            # Skip PSG shadow PermissionSets — those are handled by
            # _create_psg_nodes via the PermissionSetGroup query.
            if rec.get("Type") == "Group":
                continue
            nodes.append(
                GraphNode(
                    id=rec["Id"],
                    kinds=list(NODE_KINDS["permission_set"]),
                    properties={"name": rec["Name"], "label": rec["Label"]},
                )
            )
        return nodes

    # -- Organization nodes --

    def _create_organization_nodes(self, org_data: Dict) -> List[GraphNode]:
        nodes: List[GraphNode] = []
        for rec in org_data["records"]:
            props: Dict[str, Any] = {"name": rec["Name"]}
            for field in OWD_FIELDS:
                if field in rec:
                    props[field] = rec[field]
            nodes.append(
                GraphNode(
                    id=rec["Id"],
                    kinds=list(NODE_KINDS["organization"]),
                    properties=props,
                )
            )
        return nodes

    # -- Role nodes --

    def _create_role_nodes(self, role_data: Dict) -> List[GraphNode]:
        nodes: List[GraphNode] = []
        for rec in role_data["records"]:
            raw_portal_type = rec.get("PortalType")
            # Salesforce returns the string "None" for internal roles
            # (it's a picklist value, not a null).
            if raw_portal_type is None or raw_portal_type == "None":
                portal_type = None
            else:
                portal_type = raw_portal_type
            nodes.append(
                GraphNode(
                    id=rec["Id"],
                    kinds=list(NODE_KINDS["role"]),
                    properties={
                        "name": rec["Name"],
                        "portal_type": portal_type,
                        "is_portal_role": portal_type is not None,
                    },
                )
            )
        return nodes

    # -- PermissionSetGroup nodes --

    def _create_psg_nodes(self, psg_data: Dict) -> List[GraphNode]:
        nodes: List[GraphNode] = []
        for rec in psg_data["records"]:
            nodes.append(
                GraphNode(
                    id=rec["Id"],
                    kinds=list(NODE_KINDS["permission_set_group"]),
                    properties={
                        "name": rec["DeveloperName"],
                        "label": rec["MasterLabel"],
                        "is_permission_set_group": True,
                    },
                )
            )
        return nodes

    # -- Group nodes --

    def _create_group_nodes(self, group_data: Dict) -> List[GraphNode]:
        nodes: List[GraphNode] = []
        for rec in group_data["records"]:
            nodes.append(
                GraphNode(
                    id=rec["Id"],
                    kinds=list(NODE_KINDS["group"]),
                    properties={
                        "name": rec["DeveloperName"],
                        "group_type": rec["Type"],
                    },
                )
            )
        return nodes

    # -- SF_Object nodes (from ObjectPermissions) --

    def _create_object_nodes(
        self,
        obj_perm_data: Dict,
        entity_def_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> List[GraphNode]:
        """Create one ``SF_Object`` node per unique SobjectType.

        IDs are deterministic hashes so that both-mode deduplication works.
        """
        seen: set[str] = set()
        nodes: List[GraphNode] = []
        for rec in obj_perm_data["records"]:
            sobject_type = rec["SobjectType"]
            if sobject_type in seen:
                continue
            seen.add(sobject_type)
            props: Dict[str, Any] = {"name": sobject_type}
            if entity_def_lookup and sobject_type in entity_def_lookup:
                props.update(entity_def_lookup[sobject_type])
            nodes.append(
                GraphNode(
                    id=generate_hash_id("SF_Object", sobject_type),
                    kinds=list(NODE_KINDS["object"]),
                    properties=props,
                )
            )
        return nodes

    def _create_queue_sobject_object_nodes(
        self,
        queue_sobject_data: Dict,
        entity_def_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> List[GraphNode]:
        """Create ``SF_Object`` nodes for SObject types referenced by QueueSobject.

        Ensures that ``CanOwnObjectType`` edge targets exist as nodes even
        when those object types don't appear in ObjectPermissions.
        """
        seen: set[str] = set()
        nodes: List[GraphNode] = []
        for rec in queue_sobject_data["records"]:
            sobject_type = rec["SobjectType"]
            if sobject_type in seen:
                continue
            seen.add(sobject_type)
            props: Dict[str, Any] = {"name": sobject_type}
            if entity_def_lookup and sobject_type in entity_def_lookup:
                props.update(entity_def_lookup[sobject_type])
            nodes.append(
                GraphNode(
                    id=generate_hash_id("SF_Object", sobject_type),
                    kinds=list(NODE_KINDS["object"]),
                    properties=props,
                )
            )
        return nodes

    # -- SF_Field nodes (from FieldPermissions) --

    def _create_field_nodes(
        self,
        field_perm_data: Dict,
        entity_def_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> tuple[List[GraphNode], List[GraphNode]]:
        """Create ``SF_Field`` nodes and associated ``SF_Object`` nodes.

        Returns a tuple of ``(field_nodes, object_nodes)`` so that
        SF_Object nodes are available even when ``--skip-object-permissions``
        is set.
        """
        seen_fields: set[str] = set()
        seen_objects: set[str] = set()
        field_nodes: List[GraphNode] = []
        object_nodes: List[GraphNode] = []
        for rec in field_perm_data["records"]:
            field_name = rec["Field"]
            sobject_type = rec["SobjectType"]

            if field_name not in seen_fields:
                seen_fields.add(field_name)
                # "Account.Industry" → short name "Industry"
                short_name = field_name.split(".", 1)[1] if "." in field_name else field_name
                field_nodes.append(
                    GraphNode(
                        id=generate_hash_id("SF_Field", field_name),
                        kinds=list(NODE_KINDS["field"]),
                        properties={
                            "name": field_name,
                            "field_name": short_name,
                            "object": sobject_type,
                            "is_custom": short_name.endswith("__c"),
                        },
                    )
                )

            if sobject_type not in seen_objects:
                seen_objects.add(sobject_type)
                props: Dict[str, Any] = {"name": sobject_type}
                if entity_def_lookup and sobject_type in entity_def_lookup:
                    props.update(entity_def_lookup[sobject_type])
                object_nodes.append(
                    GraphNode(
                        id=generate_hash_id("SF_Object", sobject_type),
                        kinds=list(NODE_KINDS["object"]),
                        properties=props,
                    )
                )
        return field_nodes, object_nodes

    # -- Connected Application nodes --

    def _create_connected_app_nodes(self, ca_data: Dict) -> List[GraphNode]:
        """Create one ``SF_ConnectedApp`` node per Connected Application."""
        nodes: List[GraphNode] = []
        for rec in ca_data["records"]:
            nodes.append(
                GraphNode(
                    id=rec["Id"],
                    kinds=list(NODE_KINDS["connected_app"]),
                    properties={
                        "name": rec["Name"],
                        "admin_approved_only": rec.get(
                            "OptionsAllowAdminApprovedUsersOnly", False
                        ),
                        "is_internal": rec.get("OptionsIsInternal", False),
                        "start_url": rec.get("StartUrl"),
                        "refresh_token_validity_period": rec.get(
                            "RefreshTokenValidityPeriod"
                        ),
                    },
                )
            )
        return nodes

    # -- Fallback parent nodes (for unknown ParentId prefixes) --

    def _create_fallback_parent_nodes(
        self,
        obj_perm_data: Dict,
        known_ids: Set[str],
        psg_shadow_map: Optional[Dict[str, str]] = None,
    ) -> List[GraphNode]:
        """Create nodes for permission parents not already in the graph.

        Some ParentId values use non-standard key prefixes (e.g. ``0PL``,
        ``100``) that don't correspond to any PermissionSet we collected.
        These are internal Salesforce permission entities (license permission
        sets, tab settings, etc.) that still grant object-level CRUD or FLS.

        We use ``Parent.Name`` / ``Parent.Label`` from the relationship
        fields to name these fallback nodes.
        """
        if psg_shadow_map is None:
            psg_shadow_map = {}
        seen: set[str] = set()
        nodes: List[GraphNode] = []
        for rec in obj_perm_data["records"]:
            parent_id = rec["ParentId"]
            parent_info = rec.get("Parent") or {}

            # Skip if this parent already has a node or will be remapped.
            if parent_info.get("IsOwnedByProfile") and parent_info.get("ProfileId"):
                continue
            if parent_id in psg_shadow_map:
                continue
            if parent_id in known_ids:
                continue
            if parent_id in seen:
                continue
            seen.add(parent_id)

            name = parent_info.get("Label") or parent_info.get("Name") or parent_id
            nodes.append(
                GraphNode(
                    id=parent_id,
                    kinds=list(NODE_KINDS["permission_set"]),
                    properties={"name": name, "label": name},
                )
            )
        if nodes:
            logger.info(
                "Created %d fallback parent nodes for ObjectPermissions", len(nodes)
            )
        return nodes

    def _create_role_hierarchy_edges(self, role_data: Dict) -> List[GraphEdge]:
        """Role → ParentRole (ReportsTo) edges."""
        edges: List[GraphEdge] = []
        for rec in role_data["records"]:
            if rec["ParentRoleId"] is not None:
                edges.append(
                    GraphEdge(
                        source=rec["Id"], target=rec["ParentRoleId"], kind="ReportsTo"
                    )
                )
        return edges

    def _create_management_edges(self, mgmt_data: Dict) -> List[GraphEdge]:
        """User → Manager (ManagedBy) edges."""
        edges: List[GraphEdge] = []
        for rec in mgmt_data["records"]:
            if rec["ManagerId"] is not None:
                edges.append(
                    GraphEdge(
                        source=rec["Id"], target=rec["ManagerId"], kind="ManagedBy"
                    )
                )
        return edges

    def _create_has_role_edges(self, user_data: Dict) -> List[GraphEdge]:
        """User → Role (HasRole) edges."""
        edges: List[GraphEdge] = []
        for rec in user_data["records"]:
            role_id = rec.get("UserRoleId")
            if role_id:
                edges.append(
                    GraphEdge(source=rec["Id"], target=role_id, kind="HasRole")
                )
        return edges

    def _create_assignment_edges(self, psa_data: Dict) -> List[GraphEdge]:
        """User → Profile / PermissionSet / PSG assignment edges."""
        edges: List[GraphEdge] = []
        for rec in psa_data["records"]:
            if rec["PermissionSetGroupId"]:
                edges.append(
                    GraphEdge(
                        source=rec["AssigneeId"],
                        target=rec["PermissionSetGroupId"],
                        kind="HasPermissionSet",
                    )
                )
            elif rec["PermissionSet"]["IsOwnedByProfile"]:
                edges.append(
                    GraphEdge(
                        source=rec["AssigneeId"],
                        target=rec["PermissionSet"]["ProfileId"],
                        kind="HasProfile",
                    )
                )
            else:
                edges.append(
                    GraphEdge(
                        source=rec["AssigneeId"],
                        target=rec["PermissionSetId"],
                        kind="HasPermissionSet",
                    )
                )
        return edges

    def _create_capability_edges(self, cap_data: Dict, org_id: str) -> List[GraphEdge]:
        """Profile / PermissionSet → Organization capability edges."""
        edges: List[GraphEdge] = []
        for rec in cap_data["records"]:
            for perm_field in CAPABILITY_FIELDS:
                if rec.get(perm_field):
                    edges.append(
                        GraphEdge(
                            source=rec["Id"],
                            target=org_id,
                            kind=CAPABILITY_TO_EDGE_KIND[perm_field],
                        )
                    )
        return edges

    def _create_psg_capability_edges(
        self, psg_cap_data: Dict, org_id: str
    ) -> List[GraphEdge]:
        """PSG shadow PermissionSet → Organization capability edges.

        The source is the ``PermissionSetGroupId`` (not the shadow PS Id),
        because the shadow PS's capabilities represent the PSG's effective
        permissions.
        """
        edges: List[GraphEdge] = []
        for rec in psg_cap_data["records"]:
            for perm_field in CAPABILITY_FIELDS:
                if rec.get(perm_field):
                    edges.append(
                        GraphEdge(
                            source=rec["PermissionSetGroupId"],
                            target=org_id,
                            kind=CAPABILITY_TO_EDGE_KIND[perm_field],
                        )
                    )
        return edges

    def _create_psgc_edges(self, psgc_data: Dict) -> List[GraphEdge]:
        """PermissionSet → PSG (IncludedIn) edges."""
        edges: List[GraphEdge] = []
        for rec in psgc_data["records"]:
            edges.append(
                GraphEdge(
                    source=rec["PermissionSetId"],
                    target=rec["PermissionSetGroupId"],
                    kind="IncludedIn",
                )
            )
        return edges

    def _create_group_member_edges(self, gm_data: Dict) -> List[GraphEdge]:
        """User/Group → Group (MemberOf) edges."""
        edges: List[GraphEdge] = []
        for rec in gm_data["records"]:
            edges.append(
                GraphEdge(
                    source=rec["UserOrGroupId"],
                    target=rec["GroupId"],
                    kind="MemberOf",
                )
            )
        return edges

    def _resolve_queue_sobject_types(
        self, sf: Salesforce, queue_sobject_data: Dict
    ) -> None:
        """Resolve QueueSobject.SobjectType EntityDefinition IDs to API names.

        QueueSobject.SobjectType stores EntityDefinition DurableIds (key
        prefix ``01I``) rather than string API names.  This method uses
        the Tooling API to batch-resolve them and rewrites the records
        in-place so downstream code sees API names like ``"Lead"``.
        """
        records = queue_sobject_data["records"]
        if not records:
            return

        # Collect unique SobjectType values that look like IDs (not already names).
        raw_ids: set[str] = set()
        for rec in records:
            val = rec["SobjectType"]
            # EntityDefinition DurableIds are 18-char IDs; API names never start with 0.
            if val and len(val) == 18 and val[:3].isalnum() and not val[0].isalpha():
                raw_ids.add(val)
            elif val and val.startswith("01I"):
                raw_ids.add(val)

        if not raw_ids:
            return  # Already string names — nothing to resolve.

        # Batch resolve via Tooling API using simple_salesforce.
        id_list = "','".join(raw_ids)
        tooling_soql = (
            f"SELECT DurableId, QualifiedApiName FROM EntityDefinition "
            f"WHERE DurableId IN ('{id_list}')"
        )
        try:
            tooling_result = sf.toolingexecute(
                f"query?q={tooling_soql.replace(' ', '+')}"
            )
            id_to_name: Dict[str, str] = {}
            for entity in tooling_result.get("records", []):
                id_to_name[entity["DurableId"]] = entity["QualifiedApiName"]
        except Exception:
            logger.warning(
                "Failed to resolve QueueSobject EntityDefinition IDs; "
                "edges will use raw IDs."
            )
            return

        # Rewrite records in-place.
        for rec in records:
            resolved = id_to_name.get(rec["SobjectType"])
            if resolved:
                rec["SobjectType"] = resolved

    def _create_queue_sobject_edges(
        self, queue_sobject_data: Dict
    ) -> List[GraphEdge]:
        """Queue → SF_Object (CanOwnObjectType) edges."""
        edges: List[GraphEdge] = []
        for rec in queue_sobject_data["records"]:
            edges.append(
                GraphEdge(
                    source=rec["QueueId"],
                    target=generate_hash_id("SF_Object", rec["SobjectType"]),
                    kind="CanOwnObjectType",
                )
            )
        return edges

    def _create_object_permission_edges(
        self, obj_perm_data: Dict, psg_shadow_map: Optional[Dict[str, str]] = None
    ) -> List[GraphEdge]:
        """PermissionSet/Profile → SF_Object CRUD edges.

        For each ObjectPermissions record, creates one edge per true
        permission boolean (e.g., PermissionsCreate → CanCreate).

        Source ID resolution priority:
          1. Profile-owned shadow PS → use ``Parent.ProfileId``
          2. PSG aggregate shadow PS → use the PSG's ``0PG`` ID
          3. Otherwise → use ``ParentId`` as-is (regular PS, 0PL, 100, etc.)
        """
        if psg_shadow_map is None:
            psg_shadow_map = {}
        edges: List[GraphEdge] = []
        for rec in obj_perm_data["records"]:
            target_id = generate_hash_id("SF_Object", rec["SobjectType"])

            # Resolve the source node ID.
            parent_info = rec.get("Parent") or {}
            parent_id = rec["ParentId"]
            if parent_info.get("IsOwnedByProfile") and parent_info.get("ProfileId"):
                source_id = parent_info["ProfileId"]
            elif parent_id in psg_shadow_map:
                source_id = psg_shadow_map[parent_id]
            else:
                source_id = parent_id

            for perm_field in OBJECT_PERMISSION_FIELDS:
                if rec.get(perm_field):
                    edges.append(
                        GraphEdge(
                            source=source_id,
                            target=target_id,
                            kind=OBJECT_PERMISSION_TO_EDGE_KIND[perm_field],
                        )
                    )
        return edges

    def _create_field_permission_edges(
        self, field_perm_data: Dict, psg_shadow_map: Optional[Dict[str, str]] = None
    ) -> List[GraphEdge]:
        """PermissionSet/Profile → SF_Field FLS edges + FieldOf edges.

        For each FieldPermissions record, creates one edge per true
        permission boolean (PermissionsRead → CanReadField,
        PermissionsEdit → CanEditField) plus a ``FieldOf`` edge from
        the SF_Field to its parent SF_Object.

        Source ID resolution priority mirrors ObjectPermissions:
          1. Profile-owned shadow PS → use ``Parent.ProfileId``
          2. PSG aggregate shadow PS → use the PSG's ``0PG`` ID
          3. Otherwise → use ``ParentId`` as-is
        """
        if psg_shadow_map is None:
            psg_shadow_map = {}
        edges: List[GraphEdge] = []
        seen_field_of: set[tuple[str, str]] = set()
        for rec in field_perm_data["records"]:
            field_id = generate_hash_id("SF_Field", rec["Field"])
            object_id = generate_hash_id("SF_Object", rec["SobjectType"])

            # Resolve the source node ID.
            parent_info = rec.get("Parent") or {}
            parent_id = rec["ParentId"]
            if parent_info.get("IsOwnedByProfile") and parent_info.get("ProfileId"):
                source_id = parent_info["ProfileId"]
            elif parent_id in psg_shadow_map:
                source_id = psg_shadow_map[parent_id]
            else:
                source_id = parent_id

            for perm_field in FIELD_PERMISSION_FIELDS:
                if rec.get(perm_field):
                    edges.append(
                        GraphEdge(
                            source=source_id,
                            target=field_id,
                            kind=FIELD_PERMISSION_TO_EDGE_KIND[perm_field],
                        )
                    )

            # FieldOf edge (deduplicated by field+object pair).
            key = (field_id, object_id)
            if key not in seen_field_of:
                seen_field_of.add(key)
                edges.append(
                    GraphEdge(source=field_id, target=object_id, kind="FieldOf")
                )
        return edges

    # -- Implicit Connected Application access edges --

    def _create_implicit_connected_app_edges(
        self, ca_data: Dict, profile_data: Dict
    ) -> List[GraphEdge]:
        """Create ``CanAccessApp`` edges from every Profile to open apps.

        Non-admin-approved Connected Apps (``OptionsAllowAdminApprovedUsersOnly
        = False``) can be authorized by **any** user regardless of entitlements.
        These synthetic edges make that universal access visible in the graph.
        """
        edges: List[GraphEdge] = []
        for ca_rec in ca_data["records"]:
            if ca_rec.get("OptionsAllowAdminApprovedUsersOnly", False):
                continue
            ca_id = ca_rec["Id"]
            for profile_rec in profile_data["records"]:
                edges.append(
                    GraphEdge(
                        source=profile_rec["Id"],
                        target=ca_id,
                        kind="CanAccessApp",
                    )
                )
        return edges

    # -- Connected Application access edges --

    def _create_connected_app_access_edges(
        self, ca_access_data: Dict, psg_shadow_map: Optional[Dict[str, str]] = None
    ) -> List[GraphEdge]:
        """Profile / PermissionSet → ConnectedApp (CanAccessApp) edges.

        Only present when a Connected App has
        ``OptionsAllowAdminApprovedUsersOnly = true`` — Salesforce then
        requires an explicit SetupEntityAccess row for each Profile or
        PermissionSet that may use the app.

        Source ID resolution mirrors ObjectPermissions:
          - Profile-owned shadow PS → use ``Parent.ProfileId``
          - PSG aggregate shadow PS → use the PSG's ``0PG`` ID
          - Otherwise → use ``ParentId`` as-is
        """
        if psg_shadow_map is None:
            psg_shadow_map = {}
        edges: List[GraphEdge] = []
        for rec in ca_access_data["records"]:
            parent_info = rec.get("Parent") or {}
            parent_id = rec["ParentId"]

            if parent_info.get("IsOwnedByProfile") and parent_info.get("ProfileId"):
                source_id = parent_info["ProfileId"]
            elif parent_id in psg_shadow_map:
                source_id = psg_shadow_map[parent_id]
            else:
                source_id = parent_id

            edges.append(
                GraphEdge(
                    source=source_id,
                    target=rec["SetupEntityId"],
                    kind="CanAccessApp",
                )
            )
        return edges

    def _create_connected_app_created_by_edges(
        self, ca_data: Dict
    ) -> List[GraphEdge]:
        """ConnectedApp → User (CreatedBy) edges."""
        edges: List[GraphEdge] = []
        for rec in ca_data["records"]:
            created_by = rec.get("CreatedById")
            if created_by:
                edges.append(
                    GraphEdge(
                        source=rec["Id"],
                        target=created_by,
                        kind="CreatedBy",
                    )
                )
        return edges

    # Expose verbose state
    @property
    def _verbose(self) -> bool:
        return self.verbose
