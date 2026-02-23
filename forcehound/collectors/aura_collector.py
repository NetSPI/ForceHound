"""Low-privilege Aura collector — identity-graph extraction.

This module is an async port of ``ForceHound_Prototype/bloodhound_collector.py``.
It enumerates Users, Profiles (with 15 permissions), PermissionSets, Roles,
Groups, GroupMembers, and Namespaced Objects using Salesforce Aura/Lightning
endpoints — requiring only a browser session (no REST API privileges).

Collection flow:
  1. Get User IDs via :meth:`AuraClient.get_items_graphql`
  2. Fetch User records with Profile permissions (relationship traversal)
  3. Enumerate PermissionSets → Profiles, standalone PSes, capabilities
  3b. Enumerate UserRoles → all Roles with hierarchy
  4. Get Group IDs
  5. Fetch Group records
  6. Get GroupMember IDs
  7. Fetch GroupMember records
  8. Discover namespaced objects via :meth:`AuraClient.get_config_data`
  9. Count accessible records per namespaced object
  10. Build nodes and edges → :class:`CollectionResult`
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

if TYPE_CHECKING:
    from forcehound.audit import AuditLogger

from forcehound.collectors.aura.client import AuraClient
from forcehound.collectors.base import BaseCollector
from forcehound.collectors.crud.prober import CrudProbeReport, CrudProber
from forcehound.constants import (
    CAPABILITY_FIELDS,
    CAPABILITY_TO_EDGE_KIND,
    CRUD_EDGE_KINDS,
    DEFAULT_MAX_WORKERS,
    EXCLUDED_SUFFIXES,
    INCLUDED_SUFFIXES,
    NODE_KINDS,
)
from forcehound.models.auth import AuthConfig
from forcehound.models.base import CollectionResult, GraphEdge, GraphNode

logger = logging.getLogger(__name__)


# =====================================================================
# Helper — filter out None values from property dicts
# =====================================================================


def _filter_none(d: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of *d* with ``None``-valued keys removed."""
    return {k: v for k, v in d.items() if v is not None}


# =====================================================================
# Namespaced-object utilities
# =====================================================================


def is_namespaced_object(object_name: str) -> bool:
    """Return ``True`` if *object_name* is a namespaced managed-package object.

    Criteria:
      - Ends with an included suffix (``__c``, ``__mdt``, ``__History``)
      - Does **not** end with an excluded suffix (``__ChangeEvent``, ``__Share``)
      - Has a namespace prefix (≥ 3 double-underscore segments)
    """
    for suffix in EXCLUDED_SUFFIXES:
        if object_name.endswith(suffix):
            return False

    if not any(object_name.endswith(s) for s in INCLUDED_SUFFIXES):
        return False

    parts = object_name.split("__")
    return len(parts) >= 3


def parse_namespaced_object(object_name: str) -> Dict[str, str]:
    """Parse a namespaced object name into its components.

    Example::

        >>> parse_namespaced_object("dfsle__BulkStatus__c")
        {"name": "dfsle__BulkStatus__c", "namespace": "dfsle",
         "base_name": "BulkStatus", "object_type": "CustomObject"}
    """
    parts = object_name.split("__")

    if object_name.endswith("__c"):
        object_type = "CustomObject"
    elif object_name.endswith("__mdt"):
        object_type = "CustomMetadata"
    elif object_name.endswith("__History"):
        object_type = "HistoryTracking"
    else:
        object_type = "Unknown"

    namespace = parts[0] if len(parts) >= 3 else ""
    base_name = (
        "__".join(parts[1:-1])
        if len(parts) >= 3
        else parts[0]
        if len(parts) >= 2
        else object_name
    )

    return {
        "name": object_name,
        "namespace": namespace,
        "base_name": base_name,
        "object_type": object_type,
    }


# =====================================================================
# Response parsing helpers
# =====================================================================


def _get_value(fields: Dict[str, Any], field_name: str) -> Any:
    """Extract a scalar value from an Aura ``fields`` dict."""
    return fields.get(field_name, {}).get("value")


def _get_nested_value(
    fields: Dict[str, Any], parent_field: str, child_field: str
) -> Any:
    """Extract a nested relationship value from an Aura ``fields`` dict.

    Navigates ``fields[parent_field].value.fields[child_field].value``.
    """
    parent_data = fields.get(parent_field, {})
    parent_value = parent_data.get("value")
    if parent_value is None or not isinstance(parent_value, dict):
        return None
    nested_fields = parent_value.get("fields", {})
    return nested_fields.get(child_field, {}).get("value")


def parse_user_response(return_value: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse a single User record response into structured data.

    Returns a dict with keys ``user``, ``profile``, ``profile_permissions``,
    ``role``, ``manager``, or ``None`` if *return_value* is unusable.
    """
    if return_value is None:
        return None

    fields = return_value.get("fields", {})

    user_data = {
        "Id": _get_value(fields, "Id"),
        "Name": _get_value(fields, "Name"),
        "Email": _get_value(fields, "Email"),
        "Username": _get_value(fields, "Username"),
        "UserType": _get_value(fields, "UserType"),
        "IsActive": _get_value(fields, "IsActive"),
        "ProfileId": _get_value(fields, "ProfileId"),
        "UserRoleId": _get_value(fields, "UserRoleId"),
        "ManagerId": _get_value(fields, "ManagerId"),
        "LastLoginDate": _get_value(fields, "LastLoginDate"),
        "CreatedDate": _get_value(fields, "CreatedDate"),
        "CreatedById": _get_value(fields, "CreatedById"),
    }

    profile_data = {
        "Id": _get_nested_value(fields, "Profile", "Id"),
        "Name": _get_nested_value(fields, "Profile", "Name"),
        "UserType": _get_nested_value(fields, "Profile", "UserType"),
    }

    profile_permissions: Dict[str, Any] = {}
    for perm in CAPABILITY_FIELDS:
        profile_permissions[perm] = _get_nested_value(fields, "Profile", perm)

    role_data = {
        "Id": _get_nested_value(fields, "UserRole", "Id"),
        "Name": _get_nested_value(fields, "UserRole", "Name"),
        "ParentRoleId": _get_nested_value(fields, "UserRole", "ParentRoleId"),
    }

    manager_data = {
        "Id": _get_nested_value(fields, "Manager", "Id"),
        "Name": _get_nested_value(fields, "Manager", "Name"),
    }

    return {
        "user": user_data,
        "profile": profile_data,
        "profile_permissions": profile_permissions,
        "role": role_data,
        "manager": manager_data,
    }


def parse_group_response(return_value: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse a single Group record response."""
    if return_value is None:
        return None
    fields = return_value.get("fields", {})
    return {
        "Id": _get_value(fields, "Id"),
        "Name": _get_value(fields, "Name"),
        "DeveloperName": _get_value(fields, "DeveloperName"),
        "Type": _get_value(fields, "Type"),
        "RelatedId": _get_value(fields, "RelatedId"),
        "DoesIncludeBosses": _get_value(fields, "DoesIncludeBosses"),
        "DoesSendEmailToMembers": _get_value(fields, "DoesSendEmailToMembers"),
    }


def parse_group_member_response(
    return_value: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Parse a single GroupMember record response."""
    if return_value is None:
        return None
    fields = return_value.get("fields", {})
    return {
        "Id": _get_value(fields, "Id"),
        "GroupId": _get_value(fields, "GroupId"),
        "UserOrGroupId": _get_value(fields, "UserOrGroupId"),
        "GroupName": _get_nested_value(fields, "Group", "Name"),
        "GroupType": _get_nested_value(fields, "Group", "Type"),
    }


def parse_role_response(
    return_value: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Parse a single UserRole record response.

    Returns a flat dict with ``Id``, ``Name``, ``DeveloperName``, and
    ``ParentRoleId``, or ``None`` if *return_value* is unusable.
    """
    if return_value is None:
        return None
    fields = return_value.get("fields", {})
    return {
        "Id": _get_value(fields, "Id"),
        "Name": _get_value(fields, "Name"),
        "DeveloperName": _get_value(fields, "DeveloperName"),
        "ParentRoleId": _get_value(fields, "ParentRoleId"),
    }


def parse_permission_set_response(
    return_value: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Parse a single PermissionSet record response into structured data.

    Returns a dict with keys ``permission_set``, ``capabilities``,
    ``profile``, or ``None`` if *return_value* is unusable.
    """
    if return_value is None:
        return None

    fields = return_value.get("fields", {})

    ps_data = {
        "Id": _get_value(fields, "Id"),
        "Name": _get_value(fields, "Name"),
        "Label": _get_value(fields, "Label"),
        "IsOwnedByProfile": _get_value(fields, "IsOwnedByProfile"),
        "ProfileId": _get_value(fields, "ProfileId"),
        "PermissionSetGroupId": _get_value(fields, "PermissionSetGroupId"),
        "IsCustom": _get_value(fields, "IsCustom"),
        "Type": _get_value(fields, "Type"),
    }

    capabilities: Dict[str, Any] = {}
    for perm in CAPABILITY_FIELDS:
        capabilities[perm] = _get_value(fields, perm)

    profile_data = {
        "Name": _get_nested_value(fields, "Profile", "Name"),
        "UserType": _get_nested_value(fields, "Profile", "UserType"),
    }

    return {
        "permission_set": ps_data,
        "capabilities": capabilities,
        "profile": profile_data,
    }


# =====================================================================
# Field-path builders
# =====================================================================


def build_user_field_paths() -> Tuple[List[str], List[str]]:
    """Return ``(required_fields, optional_fields)`` for User records."""
    required = ["User.Id"]

    user_fields = [
        "User.Name",
        "User.Email",
        "User.Username",
        "User.UserType",
        "User.IsActive",
        "User.ProfileId",
        "User.UserRoleId",
        "User.ManagerId",
        "User.LastLoginDate",
        "User.CreatedDate",
        "User.CreatedById",
    ]
    profile_fields = ["User.Profile.Id", "User.Profile.Name", "User.Profile.UserType"]
    profile_permissions = [f"User.Profile.{p}" for p in CAPABILITY_FIELDS]
    role_fields = [
        "User.UserRole.Id",
        "User.UserRole.Name",
        "User.UserRole.ParentRoleId",
    ]
    manager_fields = ["User.Manager.Id", "User.Manager.Name"]

    optional = (
        user_fields
        + profile_fields
        + profile_permissions
        + role_fields
        + manager_fields
    )
    return required, optional


def build_group_field_paths() -> Tuple[List[str], List[str]]:
    """Return ``(required_fields, optional_fields)`` for Group records."""
    required = ["Group.Id"]
    optional = [
        "Group.Name",
        "Group.DeveloperName",
        "Group.Type",
        "Group.RelatedId",
        "Group.DoesIncludeBosses",
        "Group.DoesSendEmailToMembers",
    ]
    return required, optional


def build_group_member_field_paths() -> Tuple[List[str], List[str]]:
    """Return ``(required_fields, optional_fields)`` for GroupMember records."""
    required = ["GroupMember.Id"]
    optional = [
        "GroupMember.GroupId",
        "GroupMember.UserOrGroupId",
        "GroupMember.Group.Id",
        "GroupMember.Group.Name",
        "GroupMember.Group.Type",
    ]
    return required, optional


def build_permission_set_field_paths() -> Tuple[List[str], List[str]]:
    """Return ``(required_fields, optional_fields)`` for PermissionSet records.

    Includes metadata fields, all 15 capability permission fields, and
    Profile relationship fields (for profile shadow PermissionSets).
    """
    required = ["PermissionSet.Id"]
    metadata = [
        "PermissionSet.Name",
        "PermissionSet.Label",
        "PermissionSet.IsOwnedByProfile",
        "PermissionSet.ProfileId",
        "PermissionSet.PermissionSetGroupId",
        "PermissionSet.IsCustom",
        "PermissionSet.Type",
    ]
    caps = [f"PermissionSet.{p}" for p in CAPABILITY_FIELDS]
    profile_rel = [
        "PermissionSet.Profile.Name",
        "PermissionSet.Profile.UserType",
    ]
    optional = metadata + caps + profile_rel
    return required, optional


def build_role_field_paths() -> Tuple[List[str], List[str]]:
    """Return ``(required_fields, optional_fields)`` for UserRole records."""
    required = ["UserRole.Id"]
    optional = [
        "UserRole.Name",
        "UserRole.DeveloperName",
        "UserRole.ParentRoleId",
    ]
    return required, optional


# =====================================================================
# AuraCollector
# =====================================================================


class AuraCollector(BaseCollector):
    """Low-privilege Salesforce collector using Aura/Lightning endpoints.

    Builds an identity graph of Users, Profiles, PermissionSets, Roles,
    Groups, GroupMembers, and Namespaced Objects — all accessible without
    REST API privileges.

    Args:
        auth: :class:`AuthConfig` with Aura credentials.
        verbose: Emit progress messages to stdout.
        max_workers: Maximum concurrent Aura requests.
    """

    def __init__(
        self,
        auth: AuthConfig,
        verbose: bool = False,
        max_workers: int = DEFAULT_MAX_WORKERS,
        page_size: Optional[int] = None,
        active_only: bool = False,
        aura_path: str = "/aura",
        crud: bool = False,
        aggressive: bool = False,
        crud_objects: Optional[Set[str]] = None,
        crud_dry_run: bool = False,
        crud_concurrency: int = 5,
        crud_max_records: Optional[int] = None,
        audit_logger: Optional["AuditLogger"] = None,
        unsafe: bool = False,
        proxy: Optional[str] = None,
        rate_limit: Optional[float] = None,
    ) -> None:
        super().__init__(auth, verbose, audit_logger=audit_logger, proxy=proxy, rate_limit=rate_limit)
        self.max_workers = max_workers
        self.page_size = page_size
        self.active_only = active_only
        self.aura_path = aura_path
        self.crud = crud
        self.aggressive = aggressive
        self.crud_objects = crud_objects
        self.crud_dry_run = crud_dry_run
        self.crud_concurrency = crud_concurrency
        self.crud_max_records = crud_max_records
        self.unsafe = unsafe
        self._client: Optional[AuraClient] = None
        self._crud_report: Optional[CrudProbeReport] = None

    async def collect(self) -> CollectionResult:
        """Execute the Aura collection flow."""
        self.auth.validate_for_aura()
        client = AuraClient(
            instance_url=self.auth.instance_url,
            session_id=self.auth.session_id,
            aura_context=self.auth.aura_context,
            aura_token=self.auth.aura_token,
            aura_path=self.aura_path,
            audit_logger=self.audit_logger,
            proxy=self.proxy,
            rate_limit=self.rate_limit,
        )
        self._client = client
        org_id = client.org_id
        sem = asyncio.Semaphore(self.max_workers)

        self._log("=== ForceHound Aura Collector ===")
        if self.page_size is not None:
            self._log(
                f"  GraphQL page size: {self.page_size} (pagination debug enabled)"
            )
        if self.active_only:
            self._log("  Active-only mode: filtering Users by IsActive=true")

        # -- Step 1: Get User IDs --
        user_where = None
        if self.active_only:
            user_where = "where:{IsActive:{eq:true}}"
            self._log("Step 1: Getting active User record IDs...")
        else:
            self._log("Step 1: Getting User record IDs...")
        user_ids = await self._get_ids(
            client,
            "User",
            where_clause=user_where,
            batch_size=self.page_size,
            debug_pagination=self.page_size is not None,
        )
        self._log(f"  Found {len(user_ids)} User records")

        # -- Step 2 & 3: Fetch + Parse Users --
        self._log("Step 2: Fetching User records with Profile permissions...")
        parsed_users = await self._fetch_and_parse_users(client, user_ids, sem)
        self._log(f"  Successfully parsed {len(parsed_users)} User records")

        if user_ids and not parsed_users:
            self._log(
                "  WARNING: Found user IDs but all record fetches failed. "
                "Your Aura session or token may have expired."
            )

        # -- Step 3: PermissionSet enumeration --
        self._log("Step 3: Getting PermissionSet record IDs...")
        ps_ids = await self._get_ids(client, "PermissionSet")
        self._log(f"  Found {len(ps_ids)} PermissionSet records")

        self._log("  Fetching PermissionSet records with capabilities...")
        parsed_ps = await self._fetch_and_parse_permission_sets(client, ps_ids, sem)
        self._log(f"  Successfully parsed {len(parsed_ps)} PermissionSet records")

        # -- Step 3b: UserRole enumeration --
        self._log("Step 3b: Getting UserRole record IDs...")
        role_ids = await self._get_ids(client, "UserRole")
        self._log(f"  Found {len(role_ids)} UserRole records")

        self._log("  Fetching UserRole records...")
        parsed_roles = await self._fetch_and_parse_roles(client, role_ids, sem)
        self._log(f"  Successfully parsed {len(parsed_roles)} UserRole records")

        # -- Step 4 & 5: Groups --
        self._log("Step 4: Getting Group record IDs...")
        group_ids = await self._get_ids(client, "Group")
        self._log(f"  Found {len(group_ids)} Group records")

        self._log("Step 5: Fetching Group records...")
        parsed_groups = await self._fetch_and_parse_groups(client, group_ids, sem)
        self._log(f"  Successfully parsed {len(parsed_groups)} Group records")

        # -- Step 6 & 7: GroupMembers --
        self._log("Step 6: Getting GroupMember record IDs...")
        member_ids = await self._get_ids(client, "GroupMember")
        self._log(f"  Found {len(member_ids)} GroupMember records")

        self._log("Step 7: Fetching GroupMember records...")
        parsed_members = await self._fetch_and_parse_members(client, member_ids, sem)
        self._log(f"  Successfully parsed {len(parsed_members)} GroupMember records")

        # -- Step 8 & 9: Namespaced Objects --
        self._log("Step 8: Discovering namespaced objects...")
        ns_objects = await self._get_namespaced_objects(client)
        self._log(f"  Found {len(ns_objects)} namespaced objects")

        self._log("Step 9: Counting accessible records...")
        ns_counts = await self._get_ns_record_counts(client, ns_objects, sem)
        ns_nodes = self._build_namespaced_object_nodes(ns_objects, ns_counts)
        self._log(f"  {len(ns_nodes)} objects with accessible records")

        # -- Step 10: CRUD probing (optional) --
        if self.crud:
            self._log("Step 10: CRUD probing...")
            if self.aggressive:
                if self.unsafe:
                    self._log(
                        "  Mode: AGGRESSIVE + UNSAFE "
                        "(edit all records, delete existing "
                        "+ self-created for protected objects)"
                    )
                else:
                    self._log(
                        "  Mode: AGGRESSIVE "
                        "(edit all records, delete existing "
                        "— protected objects excluded from deletion)"
                    )
            else:
                self._log("  Mode: Standard (create/edit/delete self-created only)")

            prober = CrudProber(
                client=client,
                aggressive=self.aggressive,
                crud_objects=self.crud_objects,
                dry_run=self.crud_dry_run,
                concurrency=self.crud_concurrency,
                max_records=self.crud_max_records,
                verbose=self.verbose,
                unsafe=self.unsafe,
            )
            self._crud_report = await prober.probe()
            self._log(f"  CRUD probing used {self._crud_report.total_requests} requests")

        # -- Step 11: Build graph --
        self._log("Step 11: Building graph...")
        nodes, edges = self._build_graph(
            parsed_users,
            parsed_groups,
            parsed_members,
            ns_nodes,
            org_id,
            parsed_ps=parsed_ps,
            parsed_roles=parsed_roles,
        )

        # Add CRUD probe edges — attribute to the actual session user
        if self._crud_report:
            try:
                crud_user_id = await client.get_current_user_id()
                self._log(f"  Session user ID: {crud_user_id}")
                if self.audit_logger and crud_user_id:
                    self.audit_logger.log_user_resolved(user_id=crud_user_id)
            except Exception as exc:
                logger.debug("Could not get current user ID: %s", exc)
                # Fallback: match by parsed users (best-effort)
                crud_user_id = parsed_users[0]["user"]["Id"] if parsed_users else ""
            if crud_user_id:
                crud_nodes, crud_edges = self._build_crud_graph(
                    self._crud_report, crud_user_id
                )
                nodes.extend(crud_nodes)
                edges.extend(crud_edges)

        self._log(f"  Total nodes: {len(nodes)}")
        self._log(f"  Total edges: {len(edges)}")
        self._log(f"  Total requests: {client.request_count}")

        metadata: Dict[str, Any] = {
            "users": len(parsed_users),
            "permission_sets": len(parsed_ps),
            "roles": len(parsed_roles),
            "groups": len(parsed_groups),
            "group_members": len(parsed_members),
            "namespaced_objects": len(ns_nodes),
            "requests": client.request_count,
        }
        if self._crud_report:
            results = self._crud_report.results
            metadata["crud_objects_probed"] = len(results)
            metadata["crud_can_read"] = sum(1 for r in results.values() if r.can_read)
            metadata["crud_can_create"] = sum(1 for r in results.values() if r.can_create)
            metadata["crud_can_edit"] = sum(1 for r in results.values() if r.can_edit)
            metadata["crud_can_delete"] = sum(1 for r in results.values() if r.can_delete)
            if self._crud_report.deletions:
                metadata["crud_deletions"] = self._crud_report.deletions

        return CollectionResult(
            nodes=nodes,
            edges=edges,
            collector_type="aura",
            org_id=org_id,
            metadata=metadata,
        )

    async def close(self) -> None:
        """Close the underlying Aura HTTP session."""
        if self._client:
            await self._client.close()

    # =================================================================
    # ID enumeration
    # =================================================================

    async def _get_ids(
        self,
        client: AuraClient,
        object_name: str,
        where_clause: Optional[str] = None,
        batch_size: Optional[int] = None,
        debug_pagination: bool = False,
    ) -> List[str]:
        """Get record IDs via GraphQL with ``get_items`` fallback.

        GraphQL (``executeGraphQL``) supports unlimited pagination but is
        not available on every org or edition.  When it fails, we silently
        fall back to ``get_items`` which works on all orgs but caps at
        2 000 records.
        """
        kwargs: Dict[str, Any] = {}
        if batch_size is not None:
            kwargs["batch_size"] = batch_size
        if where_clause:
            kwargs["where_clause"] = where_clause
        if debug_pagination:
            kwargs["debug_pagination"] = True

        try:
            return await client.get_items_graphql(object_name, **kwargs)
        except Exception as exc:
            logger.debug(
                "GraphQL unavailable for %s (%s), using getItems", object_name, exc
            )
            try:
                return await client.get_items(object_name)
            except Exception as exc2:
                self._log(f"  Error getting {object_name} IDs: {exc2}")
                return []

    # =================================================================
    # Fetch + Parse helpers
    # =================================================================

    async def _fetch_and_parse_users(
        self,
        client: AuraClient,
        user_ids: List[str],
        sem: asyncio.Semaphore,
    ) -> List[Dict[str, Any]]:
        """Fetch User records in parallel and parse the responses."""
        required, optional = build_user_field_paths()

        async def fetch_one(uid: str) -> Optional[Dict[str, Any]]:
            async with sem:
                try:
                    resp = await client.get_record_with_fields(uid, required, optional)
                    rv = resp.get("actions", [{}])[0].get("returnValue")
                    if rv is None:
                        return None
                    return parse_user_response(rv)
                except Exception as exc:
                    logger.debug("Error fetching user %s: %s", uid, exc)
                    return None

        results = await asyncio.gather(*(fetch_one(uid) for uid in user_ids))
        return [r for r in results if r is not None]

    async def _fetch_and_parse_groups(
        self,
        client: AuraClient,
        group_ids: List[str],
        sem: asyncio.Semaphore,
    ) -> List[Dict[str, Any]]:
        """Fetch Group records in parallel and parse the responses."""
        required, optional = build_group_field_paths()

        async def fetch_one(gid: str) -> Optional[Dict[str, Any]]:
            async with sem:
                try:
                    resp = await client.get_record_with_fields(gid, required, optional)
                    rv = resp.get("actions", [{}])[0].get("returnValue")
                    if rv is None:
                        return None
                    return parse_group_response(rv)
                except Exception as exc:
                    logger.debug("Error fetching group %s: %s", gid, exc)
                    return None

        results = await asyncio.gather(*(fetch_one(gid) for gid in group_ids))
        return [r for r in results if r is not None]

    async def _fetch_and_parse_members(
        self,
        client: AuraClient,
        member_ids: List[str],
        sem: asyncio.Semaphore,
    ) -> List[Dict[str, Any]]:
        """Fetch GroupMember records in parallel and parse the responses."""
        required, optional = build_group_member_field_paths()

        async def fetch_one(mid: str) -> Optional[Dict[str, Any]]:
            async with sem:
                try:
                    resp = await client.get_record_with_fields(mid, required, optional)
                    state = resp.get("actions", [{}])[0].get("state")
                    if state == "ERROR":
                        return None
                    rv = resp.get("actions", [{}])[0].get("returnValue")
                    if rv is None:
                        return None
                    return parse_group_member_response(rv)
                except Exception as exc:
                    logger.debug("Error fetching member %s: %s", mid, exc)
                    return None

        results = await asyncio.gather(*(fetch_one(mid) for mid in member_ids))
        return [r for r in results if r is not None]

    async def _fetch_and_parse_permission_sets(
        self,
        client: AuraClient,
        ps_ids: List[str],
        sem: asyncio.Semaphore,
    ) -> List[Dict[str, Any]]:
        """Fetch PermissionSet records in parallel and parse the responses."""
        required, optional = build_permission_set_field_paths()

        async def fetch_one(ps_id: str) -> Optional[Dict[str, Any]]:
            async with sem:
                try:
                    resp = await client.get_record_with_fields(
                        ps_id, required, optional
                    )
                    rv = resp.get("actions", [{}])[0].get("returnValue")
                    if rv is None:
                        return None
                    return parse_permission_set_response(rv)
                except Exception as exc:
                    logger.debug("Error fetching PS %s: %s", ps_id, exc)
                    return None

        results = await asyncio.gather(*(fetch_one(pid) for pid in ps_ids))
        return [r for r in results if r is not None]

    async def _fetch_and_parse_roles(
        self,
        client: AuraClient,
        role_ids: List[str],
        sem: asyncio.Semaphore,
    ) -> List[Dict[str, Any]]:
        """Fetch UserRole records in parallel and parse the responses."""
        required, optional = build_role_field_paths()

        async def fetch_one(role_id: str) -> Optional[Dict[str, Any]]:
            async with sem:
                try:
                    resp = await client.get_record_with_fields(
                        role_id, required, optional
                    )
                    rv = resp.get("actions", [{}])[0].get("returnValue")
                    if rv is None:
                        return None
                    return parse_role_response(rv)
                except Exception as exc:
                    logger.debug("Error fetching Role %s: %s", role_id, exc)
                    return None

        results = await asyncio.gather(*(fetch_one(rid) for rid in role_ids))
        return [r for r in results if r is not None]

    # =================================================================
    # Namespaced objects
    # =================================================================

    async def _get_namespaced_objects(self, client: AuraClient) -> List[str]:
        """Discover namespaced managed-package objects."""
        try:
            all_objects = await client.get_config_data()
            return [obj for obj in all_objects if is_namespaced_object(obj)]
        except Exception as exc:
            self._log(f"  Error discovering namespaced objects: {exc}")
            return []

    async def _get_ns_record_counts(
        self,
        client: AuraClient,
        object_names: List[str],
        sem: asyncio.Semaphore,
    ) -> Dict[str, int]:
        """Count accessible records for each namespaced object."""
        counts: Dict[str, int] = {}

        async def count_one(name: str) -> Tuple[str, int]:
            async with sem:
                try:
                    records = await client.get_items(name)
                    return (name, len(records))
                except Exception:
                    return (name, 0)

        results = await asyncio.gather(*(count_one(n) for n in object_names))
        for name, count in results:
            counts[name] = count
        return counts

    # =================================================================
    # Node builders
    # =================================================================

    def _build_user_nodes(self, parsed_users: List[Dict]) -> List[GraphNode]:
        """Create SF_User nodes from parsed user data."""
        nodes: List[GraphNode] = []
        for parsed in parsed_users:
            user = parsed["user"]
            if user["Id"] is None:
                continue
            props = _filter_none(
                {
                    "name": user["Name"],
                    "email": user["Email"],
                    "username": user["Username"],
                    "user_type": user["UserType"],
                    "is_active": user["IsActive"],
                    "is_system_user": user["UserType"] != "Standard"
                    if user["UserType"]
                    else False,
                    "last_login": user["LastLoginDate"],
                    "created_date": user["CreatedDate"],
                    "id": user["Id"],
                }
            )
            nodes.append(
                GraphNode(
                    id=user["Id"], kinds=list(NODE_KINDS["user"]), properties=props
                )
            )
        return nodes

    def _build_profile_nodes(
        self,
        parsed_users: List[Dict],
        parsed_ps: Optional[List[Dict]] = None,
    ) -> List[GraphNode]:
        """Create deduplicated SF_Profile nodes.

        Sources:
          1. User relationship traversal (``parsed_users``) — always present.
          2. PermissionSet enumeration (``parsed_ps``) — profile shadow PSes
             where ``IsOwnedByProfile=True`` contribute additional profiles
             not discoverable through user traversal.
        """
        seen: Dict[str, GraphNode] = {}
        # Source 1: profiles from User relationship traversal.
        for parsed in parsed_users:
            profile = parsed["profile"]
            pid = profile["Id"]
            if pid is None or pid in seen:
                continue
            props = _filter_none(
                {
                    "name": profile["Name"],
                    "user_type": profile["UserType"],
                    "id": pid,
                }
            )
            seen[pid] = GraphNode(
                id=pid, kinds=list(NODE_KINDS["profile"]), properties=props
            )

        # Source 2: profiles from PermissionSet enumeration (shadow PSes).
        if parsed_ps:
            for ps in parsed_ps:
                ps_data = ps["permission_set"]
                if not ps_data.get("IsOwnedByProfile"):
                    continue
                profile_id = ps_data.get("ProfileId")
                if not profile_id or profile_id in seen:
                    continue
                profile_meta = ps["profile"]
                props = _filter_none(
                    {
                        "name": profile_meta.get("Name"),
                        "user_type": profile_meta.get("UserType"),
                        "id": profile_id,
                    }
                )
                seen[profile_id] = GraphNode(
                    id=profile_id,
                    kinds=list(NODE_KINDS["profile"]),
                    properties=props,
                )

        return list(seen.values())

    def _build_role_nodes(
        self,
        parsed_users: List[Dict],
        parsed_roles: Optional[List[Dict]] = None,
    ) -> List[GraphNode]:
        """Create deduplicated SF_Role nodes.

        Roles discovered via User relationship traversal are supplemented
        by directly enumerated UserRole records (*parsed_roles*), which
        captures roles that have no assigned users.
        """
        seen: Dict[str, GraphNode] = {}
        # Roles from User traversal
        for parsed in parsed_users:
            role = parsed["role"]
            rid = role["Id"]
            if rid is None or rid in seen:
                continue
            props = _filter_none({"name": role["Name"], "id": rid})
            seen[rid] = GraphNode(
                id=rid, kinds=list(NODE_KINDS["role"]), properties=props
            )
        # Roles from direct enumeration
        if parsed_roles:
            for role in parsed_roles:
                rid = role["Id"]
                if rid is None or rid in seen:
                    continue
                props = _filter_none(
                    {
                        "name": role["Name"],
                        "developer_name": role["DeveloperName"],
                        "id": rid,
                    }
                )
                seen[rid] = GraphNode(
                    id=rid,
                    kinds=list(NODE_KINDS["role"]),
                    properties=props,
                )
        return list(seen.values())

    def _build_group_nodes(self, parsed_groups: List[Dict]) -> List[GraphNode]:
        """Create SF_PublicGroup / SF_Group nodes."""
        nodes: List[GraphNode] = []
        for group in parsed_groups:
            if group is None or group["Id"] is None:
                continue
            props = _filter_none(
                {
                    "name": group["Name"],
                    "developer_name": group["DeveloperName"],
                    "group_type": group["Type"],
                    "does_include_bosses": group["DoesIncludeBosses"],
                    "id": group["Id"],
                }
            )
            kinds = (
                list(NODE_KINDS["public_group"])
                if group["Type"] == "Regular"
                else list(NODE_KINDS["group"])
            )
            nodes.append(GraphNode(id=group["Id"], kinds=kinds, properties=props))
        return nodes

    def _build_organization_node(self, org_id: str) -> GraphNode:
        """Create the SF_Organization node."""
        return GraphNode(
            id=org_id,
            kinds=list(NODE_KINDS["organization"]),
            properties={"name": "Salesforce Organization", "id": org_id},
        )

    def _build_permission_set_nodes(self, parsed_ps: List[Dict]) -> List[GraphNode]:
        """Create SF_PermissionSet nodes from enumerated PermissionSets.

        Skips profile shadow PSes (handled by :meth:`_build_profile_nodes`)
        and PSG shadow PSes (handled by the API supplement).
        """
        nodes: List[GraphNode] = []
        for ps in parsed_ps:
            ps_data = ps["permission_set"]
            # Skip profile shadows — these become Profile nodes.
            if ps_data.get("IsOwnedByProfile"):
                continue
            # Skip PSG shadow PSes — API supplement handles PSGs.
            if ps_data.get("Type") == "Group" or ps_data.get("PermissionSetGroupId"):
                continue
            ps_id = ps_data.get("Id")
            if not ps_id:
                continue
            props = _filter_none(
                {
                    "name": ps_data.get("Name"),
                    "label": ps_data.get("Label"),
                    "is_custom": ps_data.get("IsCustom"),
                    "id": ps_id,
                }
            )
            nodes.append(
                GraphNode(
                    id=ps_id,
                    kinds=list(NODE_KINDS["permission_set"]),
                    properties=props,
                )
            )
        return nodes

    def _build_namespaced_object_nodes(
        self, object_names: List[str], counts: Dict[str, int]
    ) -> List[GraphNode]:
        """Create SF_NamespacedObject nodes for objects with accessible records."""
        nodes: List[GraphNode] = []
        for name in object_names:
            count = counts.get(name, 0)
            if count <= 0:
                continue
            parsed = parse_namespaced_object(name)
            props = _filter_none(
                {
                    "name": parsed["name"],
                    "namespace": parsed["namespace"],
                    "base_name": parsed["base_name"],
                    "object_type": parsed["object_type"],
                    "accessible_record_count": count,
                    "id": name,
                }
            )
            nodes.append(
                GraphNode(
                    id=name,
                    kinds=list(NODE_KINDS["namespaced_object"]),
                    properties=props,
                )
            )
        return nodes

    # =================================================================
    # Edge builders
    # =================================================================

    def _build_has_profile_edges(self, parsed_users: List[Dict]) -> List[GraphEdge]:
        """User → Profile edges."""
        edges: List[GraphEdge] = []
        for parsed in parsed_users:
            uid = parsed["user"]["Id"]
            pid = parsed["profile"]["Id"]
            if uid and pid:
                edges.append(GraphEdge(source=uid, target=pid, kind="HasProfile"))
        return edges

    def _build_has_role_edges(self, parsed_users: List[Dict]) -> List[GraphEdge]:
        """User → Role edges."""
        edges: List[GraphEdge] = []
        for parsed in parsed_users:
            uid = parsed["user"]["Id"]
            rid = parsed["role"]["Id"]
            if uid and rid:
                edges.append(GraphEdge(source=uid, target=rid, kind="HasRole"))
        return edges

    def _build_role_hierarchy_edges(
        self,
        parsed_users: List[Dict],
        parsed_roles: Optional[List[Dict]] = None,
    ) -> List[GraphEdge]:
        """Role → ParentRole (ReportsTo) edges (deduplicated).

        Combines hierarchy data from User relationship traversal with
        directly enumerated UserRole records.
        """
        edges: List[GraphEdge] = []
        seen: Set[str] = set()
        # From User traversal
        for parsed in parsed_users:
            role = parsed["role"]
            rid = role["Id"]
            parent = role["ParentRoleId"]
            if rid and parent and rid not in seen:
                seen.add(rid)
                edges.append(GraphEdge(source=rid, target=parent, kind="ReportsTo"))
        # From direct enumeration
        if parsed_roles:
            for role in parsed_roles:
                rid = role["Id"]
                parent = role["ParentRoleId"]
                if rid and parent and rid not in seen:
                    seen.add(rid)
                    edges.append(GraphEdge(source=rid, target=parent, kind="ReportsTo"))
        return edges

    def _build_manager_edges(self, parsed_users: List[Dict]) -> List[GraphEdge]:
        """User → Manager (ManagedBy) edges."""
        edges: List[GraphEdge] = []
        for parsed in parsed_users:
            uid = parsed["user"]["Id"]
            mid = parsed["user"]["ManagerId"]
            if uid and mid:
                edges.append(GraphEdge(source=uid, target=mid, kind="ManagedBy"))
        return edges

    def _build_capability_edges(
        self,
        parsed_users: List[Dict],
        org_id: str,
        parsed_ps: Optional[List[Dict]] = None,
    ) -> List[GraphEdge]:
        """Profile/PS → Organization capability edges (deduplicated).

        Sources:
          1. Profile permissions from User relationship traversal.
          2. PermissionSet enumeration — profile shadow PSes contribute
             additional profile capabilities; regular PSes contribute
             PermissionSet → Org capability edges.
        """
        edges: List[GraphEdge] = []
        processed: Dict[str, Set[str]] = {}

        # Source 1: Profile capabilities from User traversal.
        for parsed in parsed_users:
            pid = parsed["profile"]["Id"]
            if pid is None:
                continue
            if pid not in processed:
                processed[pid] = set()

            perms = parsed["profile_permissions"]
            for perm_field, edge_kind in CAPABILITY_TO_EDGE_KIND.items():
                if perms.get(perm_field) and edge_kind not in processed[pid]:
                    processed[pid].add(edge_kind)
                    edges.append(GraphEdge(source=pid, target=org_id, kind=edge_kind))

        # Source 2: capabilities from PermissionSet enumeration.
        if parsed_ps:
            for ps in parsed_ps:
                ps_data = ps["permission_set"]

                if ps_data.get("IsOwnedByProfile"):
                    # Profile shadow → capability edges from Profile ID.
                    source_id = ps_data.get("ProfileId")
                elif ps_data.get("Type") == "Group" or ps_data.get(
                    "PermissionSetGroupId"
                ):
                    # PSG shadow → skip (API supplement handles PSGs).
                    continue
                else:
                    # Regular PermissionSet → capability edges from PS ID.
                    source_id = ps_data.get("Id")

                if not source_id:
                    continue
                if source_id not in processed:
                    processed[source_id] = set()

                caps = ps["capabilities"]
                for perm_field, edge_kind in CAPABILITY_TO_EDGE_KIND.items():
                    if caps.get(perm_field) and edge_kind not in processed[source_id]:
                        processed[source_id].add(edge_kind)
                        edges.append(
                            GraphEdge(source=source_id, target=org_id, kind=edge_kind)
                        )

        return edges

    def _build_member_of_edges(
        self, parsed_members: List[Dict], user_ids_set: Set[str]
    ) -> List[GraphEdge]:
        """User → Group (MemberOf) edges."""
        edges: List[GraphEdge] = []
        for member in parsed_members:
            gid = member["GroupId"]
            uog = member["UserOrGroupId"]
            if gid and uog:
                if uog in user_ids_set or uog.startswith("005"):
                    edges.append(GraphEdge(source=uog, target=gid, kind="MemberOf"))
        return edges

    def _build_group_contains_edges(
        self, parsed_members: List[Dict], group_ids_set: Set[str]
    ) -> List[GraphEdge]:
        """Group → NestedGroup (Contains) edges."""
        edges: List[GraphEdge] = []
        for member in parsed_members:
            gid = member["GroupId"]
            uog = member["UserOrGroupId"]
            if gid and uog:
                if uog in group_ids_set or uog.startswith("00G"):
                    edges.append(GraphEdge(source=gid, target=uog, kind="Contains"))
        return edges

    def _build_can_access_edges(
        self, ns_nodes: List[GraphNode], current_user_id: str
    ) -> List[GraphEdge]:
        """User → NamespacedObject (CanAccess) edges."""
        if not current_user_id:
            return []
        return [
            GraphEdge(source=current_user_id, target=node.id, kind="CanAccess")
            for node in ns_nodes
        ]

    # =================================================================
    # Full graph assembly
    # =================================================================

    def _build_graph(
        self,
        parsed_users: List[Dict],
        parsed_groups: List[Dict],
        parsed_members: List[Dict],
        ns_nodes: List[GraphNode],
        org_id: str,
        parsed_ps: Optional[List[Dict]] = None,
        parsed_roles: Optional[List[Dict]] = None,
    ) -> Tuple[List[GraphNode], List[GraphEdge]]:
        """Assemble all nodes and edges from the collected data."""
        # Nodes
        user_nodes = self._build_user_nodes(parsed_users)
        profile_nodes = self._build_profile_nodes(parsed_users, parsed_ps)
        role_nodes = self._build_role_nodes(parsed_users, parsed_roles)
        group_nodes = self._build_group_nodes(parsed_groups)
        ps_nodes = self._build_permission_set_nodes(parsed_ps or [])
        org_node = self._build_organization_node(org_id)

        all_nodes = (
            user_nodes
            + profile_nodes
            + role_nodes
            + group_nodes
            + ps_nodes
            + ns_nodes
            + [org_node]
        )

        # ID sets for edge builders
        user_ids_set = {n.id for n in user_nodes}
        group_ids_set = {n.id for n in group_nodes}

        # Determine current user for CanAccess edges (first user if available).
        current_user_id = ""
        if user_nodes:
            # Use the session's org ID prefix to find "our" user.
            # Fallback: use first user in the list.
            current_user_id = user_nodes[0].id

        # Edges
        all_edges = (
            self._build_has_profile_edges(parsed_users)
            + self._build_has_role_edges(parsed_users)
            + self._build_role_hierarchy_edges(parsed_users, parsed_roles)
            + self._build_manager_edges(parsed_users)
            + self._build_capability_edges(parsed_users, org_id, parsed_ps)
            + self._build_member_of_edges(parsed_members, user_ids_set)
            + self._build_group_contains_edges(parsed_members, group_ids_set)
            + self._build_can_access_edges(ns_nodes, current_user_id)
        )

        self._log(f"  Users: {len(user_nodes)}")
        self._log(f"  Profiles: {len(profile_nodes)}")
        self._log(f"  PermissionSets: {len(ps_nodes)}")
        self._log(f"  Roles: {len(role_nodes)}")
        self._log(f"  Groups: {len(group_nodes)}")
        self._log(f"  Namespaced Objects: {len(ns_nodes)}")
        self._log("  Organization: 1")

        return all_nodes, all_edges

    # =================================================================
    # CRUD graph builders
    # =================================================================

    def _build_crud_graph(
        self,
        report: CrudProbeReport,
        current_user_id: str,
    ) -> Tuple[List[GraphNode], List[GraphEdge]]:
        """Build SF_Object nodes and CRUD edges from probe results.

        Creates an SF_Object node for each probed object and emits
        ``CrudCanRead``, ``CrudCanCreate``, ``CrudCanEdit``, and
        ``CrudCanDelete`` edges from the current user to each object.

        Args:
            report: The completed :class:`CrudProbeReport`.
            current_user_id: The ID of the user who ran the probe.

        Returns:
            A tuple of (nodes, edges).
        """
        from forcehound.utils.id_utils import generate_hash_id

        nodes: List[GraphNode] = []
        edges: List[GraphEdge] = []
        seen_objects: Set[str] = set()

        for obj_name, result in report.results.items():
            # Create SF_Object node (if not already in the graph)
            obj_id = generate_hash_id("SF_Object", obj_name)
            if obj_id not in seen_objects:
                seen_objects.add(obj_id)
                props = _filter_none({
                    "name": obj_name,
                    "id": obj_id,
                    "crud_read_count": result.read_count if result.can_read else None,
                })
                nodes.append(
                    GraphNode(
                        id=obj_id,
                        kinds=list(NODE_KINDS["object"]),
                        properties=props,
                    )
                )

            # CRUD edges
            if result.can_read:
                edges.append(GraphEdge(
                    source=current_user_id,
                    target=obj_id,
                    kind=CRUD_EDGE_KINDS["read"],
                    properties={"record_count": result.read_count},
                ))
            if result.can_create:
                edges.append(GraphEdge(
                    source=current_user_id,
                    target=obj_id,
                    kind=CRUD_EDGE_KINDS["create"],
                ))
            if result.can_edit:
                edges.append(GraphEdge(
                    source=current_user_id,
                    target=obj_id,
                    kind=CRUD_EDGE_KINDS["edit"],
                    properties={
                        "edit_success_count": result.edit_success_count,
                        "edit_fail_count": result.edit_fail_count,
                    },
                ))
            if result.can_delete:
                edges.append(GraphEdge(
                    source=current_user_id,
                    target=obj_id,
                    kind=CRUD_EDGE_KINDS["delete"],
                ))

        return nodes, edges
