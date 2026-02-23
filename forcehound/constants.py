"""Shared constants, enumerations, and capability mappings for ForceHound.

Merges constants from both the REST API collector (salesforce_collector.py) and
the Aura prototype (ForceHound_Prototype/bloodhound_collector.py) into a single
authoritative module.
"""

from enum import Enum
from typing import Dict, List, Set


# =============================================================================
# Enumerations
# =============================================================================


class CollectorMode(str, Enum):
    """Backend selection for the CLI ``--collector`` flag."""

    API = "api"
    AURA = "aura"
    BOTH = "both"


class ObjectType(str, Enum):
    """Salesforce object types collected by ForceHound."""

    USER = "User"
    PROFILE = "Profile"
    PERMISSION_SET = "PermissionSet"
    PERMISSION_SET_GROUP = "PermissionSetGroup"
    ROLE = "UserRole"
    GROUP = "Group"
    GROUP_MEMBER = "GroupMember"
    ORGANIZATION = "Organization"


class AccessLevel(str, Enum):
    """Salesforce OWD / Share record access levels."""

    READ = "Read"
    EDIT = "Edit"
    ALL = "All"
    NONE = "None"
    CONTROLLED_BY_PARENT = "ControlledByParent"


# =============================================================================
# Capability permissions (15 total — from Aura prototype)
# =============================================================================

CAPABILITY_FIELDS: List[str] = [
    "PermissionsModifyAllData",
    "PermissionsViewAllData",
    "PermissionsAuthorApex",
    "PermissionsManageUsers",
    "PermissionsCustomizeApplication",
    "PermissionsManageProfilesPermissionsets",
    "PermissionsAssignPermissionSets",
    "PermissionsManageRoles",
    "PermissionsManageSharing",
    "PermissionsManageInternalUsers",
    "PermissionsResetPasswords",
    "PermissionsApiEnabled",
    "PermissionsViewSetup",
    "PermissionsViewAllUsers",
    "PermissionsManageDataIntegrations",
]

CAPABILITY_TO_EDGE_KIND: Dict[str, str] = {
    "PermissionsModifyAllData": "ModifyAllData",
    "PermissionsViewAllData": "ViewAllData",
    "PermissionsAuthorApex": "AuthorApex",
    "PermissionsManageUsers": "ManageUsers",
    "PermissionsCustomizeApplication": "CustomizeApplication",
    "PermissionsManageProfilesPermissionsets": "ManageProfilesPermissionsets",
    "PermissionsAssignPermissionSets": "AssignPermissionSets",
    "PermissionsManageRoles": "ManageRoles",
    "PermissionsManageSharing": "ManageSharing",
    "PermissionsManageInternalUsers": "ManageInternalUsers",
    "PermissionsResetPasswords": "ResetPasswords",
    "PermissionsApiEnabled": "ApiEnabled",
    "PermissionsViewSetup": "ViewSetup",
    "PermissionsViewAllUsers": "ViewAllUsers",
    "PermissionsManageDataIntegrations": "ManageDataIntegrations",
}

# The 5 capabilities used by the REST API collector (subset of the 15 above)
API_CAPABILITY_FIELDS: List[str] = [
    "PermissionsModifyAllData",
    "PermissionsViewAllData",
    "PermissionsAuthorApex",
    "PermissionsManageUsers",
    "PermissionsCustomizeApplication",
]


# =============================================================================
# Organization-Wide Defaults (OWD) fields
# =============================================================================

OWD_FIELDS: List[str] = [
    "DefaultAccountAccess",
    "DefaultContactAccess",
    "DefaultOpportunityAccess",
    "DefaultLeadAccess",
    "DefaultCaseAccess",
    "DefaultCalendarAccess",
    "DefaultPricebookAccess",
    "DefaultCampaignAccess",
]


# =============================================================================
# EntityDefinition metadata fields (per-object sharing model enrichment)
# =============================================================================

ENTITY_DEFINITION_FIELDS: List[str] = [
    "QualifiedApiName",
    "Label",
    "PluralLabel",
    "KeyPrefix",
    "IsCustomSetting",
    "InternalSharingModel",
    "ExternalSharingModel",
    "IsEverCreatable",
    "IsEverUpdatable",
    "IsEverDeletable",
    "IsQueryable",
    "NamespacePrefix",
    "DeveloperName",
]


# =============================================================================
# ObjectPermissions CRUD fields
# Source: supplemental_info HTTP capture — SELECT FIELDS(ALL) FROM
#         ObjectPermissions on user's dev org confirmed these 7 booleans.
# =============================================================================

OBJECT_PERMISSION_FIELDS: List[str] = [
    "PermissionsCreate",
    "PermissionsRead",
    "PermissionsEdit",
    "PermissionsDelete",
    "PermissionsViewAllRecords",
    "PermissionsModifyAllRecords",
    "PermissionsViewAllFields",
]

OBJECT_PERMISSION_TO_EDGE_KIND: Dict[str, str] = {
    "PermissionsCreate": "CanCreate",
    "PermissionsRead": "CanRead",
    "PermissionsEdit": "CanEdit",
    "PermissionsDelete": "CanDelete",
    "PermissionsViewAllRecords": "CanViewAll",
    "PermissionsModifyAllRecords": "CanModifyAll",
    "PermissionsViewAllFields": "CanViewAllFields",
}


# =============================================================================
# FieldPermissions FLS fields
# =============================================================================

FIELD_PERMISSION_FIELDS: List[str] = [
    "PermissionsRead",
    "PermissionsEdit",
]

FIELD_PERMISSION_TO_EDGE_KIND: Dict[str, str] = {
    "PermissionsRead": "CanReadField",
    "PermissionsEdit": "CanEditField",
}


# =============================================================================
# Node kinds
# =============================================================================

NODE_KINDS: Dict[str, List[str]] = {
    "user": ["SF_User", "User", "SaaS_Identity"],
    "profile": ["SF_Profile", "SaaS_Container", "SaaS_Entitlement"],
    "permission_set": ["SF_PermissionSet", "SaaS_Entitlement"],
    "permission_set_group": ["SF_PermissionSet", "SaaS_Entitlement"],
    "role": ["SF_Role", "SaaS_Group"],
    "public_group": ["SF_PublicGroup", "SaaS_Group"],
    "group": ["SF_Group", "SaaS_Group"],
    "organization": ["SF_Organization", "Organization", "SaaS_Tenant"],
    "namespaced_object": ["SF_NamespacedObject", "SaaS_Resource"],
    "object": ["SF_Object", "SaaS_Resource"],
    "field": ["SF_Field", "SaaS_Resource"],
    "record": ["SF_Record"],
    "connected_app": ["SF_ConnectedApp", "SaaS_Application"],
}

CUSTOM_NODE_ICONS: Dict[str, Dict[str, str]] = {
    "SF_User":             {"name": "user",         "color": "#17A2B8"},
    "SF_Profile":          {"name": "id-badge",     "color": "#6F42C1"},
    "SF_PermissionSet":    {"name": "key",          "color": "#FFC107"},
    "SF_Role":             {"name": "sitemap",      "color": "#28A745"},
    "SF_PublicGroup":      {"name": "users",        "color": "#E83E8C"},
    "SF_Group":            {"name": "people-group", "color": "#FD7E14"},
    "SF_Organization":     {"name": "building",     "color": "#007BFF"},
    "SF_NamespacedObject": {"name": "cube",         "color": "#20C997"},
    "SF_Object":           {"name": "database",     "color": "#6C757D"},
    "SF_Field":            {"name": "tag",          "color": "#5C6BC0"},
    "SF_Record":           {"name": "file",         "color": "#ADB5BD"},
    "SF_ConnectedApp":     {"name": "plug",         "color": "#DC3545"},
}


# =============================================================================
# Edge kinds
# =============================================================================

EDGE_KINDS: Set[str] = {
    # Identity / assignment edges
    "HasProfile",
    "HasRole",
    "HasPermissionSet",
    "IncludedIn",
    # Hierarchy edges
    "ReportsTo",
    "ManagedBy",
    # Membership edges
    "MemberOf",
    "Contains",
    # Access edges
    "CanAccess",
    "CanAccessApp",
    "CreatedBy",
    "CanOwnObjectType",
    "Owns",
    "ExplicitAccess",
    "InheritsAccess",
    # Capability edges (15 from CAPABILITY_TO_EDGE_KIND values)
    *CAPABILITY_TO_EDGE_KIND.values(),
    # Object-level CRUD edges (7 from OBJECT_PERMISSION_TO_EDGE_KIND values)
    *OBJECT_PERMISSION_TO_EDGE_KIND.values(),
    # Field-level security edges (2 from FIELD_PERMISSION_TO_EDGE_KIND + FieldOf)
    *FIELD_PERMISSION_TO_EDGE_KIND.values(),
    "FieldOf",
    # CRUD probe edges (4 from CRUD_EDGE_KINDS values — added at module load)
    "CrudCanCreate",
    "CrudCanRead",
    "CrudCanEdit",
    "CrudCanDelete",
}


# =============================================================================
# Namespaced object filtering (from Aura prototype)
# =============================================================================

INCLUDED_SUFFIXES: List[str] = ["__c", "__mdt", "__History"]
EXCLUDED_SUFFIXES: List[str] = ["__ChangeEvent", "__Share"]


# =============================================================================
# CRUD probing constants
# =============================================================================

CRUD_SKIP_OBJECTS: Set[str] = {
    "Organization",
    "Profile",
    "PermissionSet",
    "PermissionSetAssignment",
    "PermissionSetGroup",
    "PermissionSetGroupComponent",
    "UserRole",
    "GroupMember",
    "SetupAuditTrail",
    "LoginHistory",
    "ApexLog",
    "AsyncApexJob",
    "CronTrigger",
    "CronJobDetail",
}
"""Objects skipped by CRUD probing — system/metadata objects that should
not be created, edited, or deleted during probing."""

CRUD_EXCLUDED_SUFFIXES: List[str] = [
    "__ChangeEvent",
    "__Share",
    "__Feed",
    "__History",
]
"""Object name suffixes excluded from CRUD probing."""

CRUD_DELETE_PROTECTED_OBJECTS: Set[str] = {
    # Identity & access control
    "User",
    "Profile",
    "PermissionSet",
    "PermissionSetGroup",
    "PermissionSetGroupComponent",
    "PermissionSetAssignment",
    "Group",
    "GroupMember",
    "UserRole",
    "Organization",
    "CustomPermission",
    "MutingPermissionSet",
    # Auth & integration
    "ConnectedApplication",
    "OauthToken",
    "AuthProvider",
    "NamedCredential",
    # Apex code
    "ApexClass",
    "ApexTrigger",
    "ApexComponent",
    "ApexPage",
    # Aura / LWC bundles
    "AuraDefinition",
    "AuraDefinitionBundle",
    # Visualforce
    "StaticResource",
    "FlexiPage",
    # Flows
    "FlowDefinitionView",
    "FlowRecord",
    "FlowRecordVersion",
    # Audit & logging
    "SetupAuditTrail",
    "LoginHistory",
}
"""Objects protected from aggressive deletion.

These objects are still probed for create/read/edit.  In ``--aggressive``
mode they are excluded from deletion entirely.  With ``--aggressive --unsafe``
only the self-created record is deleted — existing records are never touched.
Deleting these records can break org functionality or destroy code."""

CRUD_PROBE_CONCURRENCY_DEFAULT: int = 5
"""Default concurrency for CRUD probe operations."""

CRUD_EDGE_KINDS: Dict[str, str] = {
    "create": "CrudCanCreate",
    "read": "CrudCanRead",
    "edit": "CrudCanEdit",
    "delete": "CrudCanDelete",
}
"""Edge kinds emitted by the CRUD prober."""


# =============================================================================
# Concurrency defaults
# =============================================================================

DEFAULT_MAX_WORKERS: int = 30
"""Default number of concurrent requests for parallel fetches."""

SALESFORCE_API_VERSION: str = "63.0"
"""Salesforce REST API version.

simple_salesforce defaults to v59.0, which lacks newer fields such as
``PermissionsViewAllFields`` on ``ObjectPermissions`` (added in v63.0).
"""
