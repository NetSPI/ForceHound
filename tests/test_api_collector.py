"""Tests for forcehound.collectors.api_collector.APICollector."""

import pytest
from unittest.mock import MagicMock, patch

from forcehound.collectors.api_collector import APICollector
from forcehound.constants import (
    CAPABILITY_FIELDS,
    FIELD_PERMISSION_FIELDS,
    OBJECT_PERMISSION_FIELDS,
)
from forcehound.models.auth import AuthConfig
from forcehound.utils.id_utils import generate_hash_id


def _all_caps_false(**overrides):
    """Return a dict with all 15 capability fields set to False, with overrides."""
    caps = {f: False for f in CAPABILITY_FIELDS}
    caps.update(overrides)
    return caps


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def api_auth():
    return AuthConfig(
        instance_url="https://test.my.salesforce.com",
        session_id="00DXX000000XXXXX!AQEAQTEST",
    )


@pytest.fixture
def collector(api_auth):
    return APICollector(api_auth, verbose=False)


@pytest.fixture
def mock_sf():
    """Create a mock simple_salesforce.Salesforce instance."""
    sf = MagicMock()

    # Organization query
    sf.query.return_value = {"records": [{"Id": "00DXX0000008TEST"}]}

    # query_all results for each SOQL query
    sf.query_all.side_effect = _route_query

    return sf


def _route_query(query: str):
    """Route mock SOQL queries by substring matching (longer substrings first)."""
    q = query.upper()

    if "FROM ENTITYDEFINITION" in q:
        return {
            "records": [
                {
                    "QualifiedApiName": "Account",
                    "Label": "Account",
                    "PluralLabel": "Accounts",
                    "KeyPrefix": "001",
                    "IsCustomSetting": False,
                    "InternalSharingModel": "Private",
                    "ExternalSharingModel": "Private",
                    "IsEverCreatable": True,
                    "IsEverUpdatable": True,
                    "IsEverDeletable": True,
                    "IsQueryable": True,
                    "NamespacePrefix": None,
                    "DeveloperName": "Account",
                },
                {
                    "QualifiedApiName": "Contact",
                    "Label": "Contact",
                    "PluralLabel": "Contacts",
                    "KeyPrefix": "003",
                    "IsCustomSetting": False,
                    "InternalSharingModel": "ReadWrite",
                    "ExternalSharingModel": "Private",
                    "IsEverCreatable": True,
                    "IsEverUpdatable": True,
                    "IsEverDeletable": True,
                    "IsQueryable": True,
                    "NamespacePrefix": None,
                    "DeveloperName": "Contact",
                },
                {
                    "QualifiedApiName": "Lead",
                    "Label": "Lead",
                    "PluralLabel": "Leads",
                    "KeyPrefix": "00Q",
                    "IsCustomSetting": False,
                    "InternalSharingModel": "ReadWriteTransfer",
                    "ExternalSharingModel": "Private",
                    "IsEverCreatable": True,
                    "IsEverUpdatable": True,
                    "IsEverDeletable": True,
                    "IsQueryable": True,
                    "NamespacePrefix": None,
                    "DeveloperName": "Lead",
                },
                {
                    "QualifiedApiName": "Case",
                    "Label": "Case",
                    "PluralLabel": "Cases",
                    "KeyPrefix": "500",
                    "IsCustomSetting": False,
                    "InternalSharingModel": "ReadWriteTransfer",
                    "ExternalSharingModel": "Private",
                    "IsEverCreatable": True,
                    "IsEverUpdatable": True,
                    "IsEverDeletable": True,
                    "IsQueryable": True,
                    "NamespacePrefix": None,
                    "DeveloperName": "Case",
                },
            ]
        }

    if "FROM SETUPENTITYACCESS" in q:
        return {
            "records": [
                {
                    "SetupEntityId": "0H4XX000000APP01",
                    "ParentId": "0PSXX000000SHAD2",
                    "Parent": {
                        "IsOwnedByProfile": True,
                        "ProfileId": "00eXX000000PROF1",
                    },
                },
                {
                    "SetupEntityId": "0H4XX000000APP01",
                    "ParentId": "0PSXX000000PS02",
                    "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                },
            ]
        }

    if "FROM CONNECTEDAPPLICATION" in q:
        return {
            "records": [
                {
                    "Id": "0H4XX000000APP01",
                    "Name": "Test Connected App",
                    "CreatedById": "005XX000001USER1",
                    "OptionsAllowAdminApprovedUsersOnly": True,
                    "OptionsIsInternal": False,
                    "StartUrl": "https://example.com/callback",
                    "RefreshTokenValidityPeriod": 720,
                },
                {
                    "Id": "0H4XX000000APP02",
                    "Name": "Open App",
                    "CreatedById": "005XX000002USER2",
                    "OptionsAllowAdminApprovedUsersOnly": False,
                    "OptionsIsInternal": True,
                    "StartUrl": None,
                    "RefreshTokenValidityPeriod": None,
                },
            ]
        }

    if "FROM FIELDPERMISSIONS" in q:
        return {
            "records": [
                {
                    "SobjectType": "Account",
                    "Field": "Account.Industry",
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "ParentId": "0PSXX000000PS02",
                    "Parent": {
                        "IsOwnedByProfile": False,
                        "ProfileId": None,
                        "Name": "TestPS",
                        "Label": "Test Permission Set",
                    },
                },
                {
                    "SobjectType": "Account",
                    "Field": "Account.AnnualRevenue",
                    "PermissionsRead": True,
                    "PermissionsEdit": True,
                    "ParentId": "0PSXX000000SHAD2",
                    "Parent": {
                        "IsOwnedByProfile": True,
                        "ProfileId": "00eXX000000PROF1",
                        "Name": "StandardProfile",
                        "Label": "Standard Profile",
                    },
                },
                {
                    "SobjectType": "Contact",
                    "Field": "Contact.Email",
                    "PermissionsRead": True,
                    "PermissionsEdit": True,
                    "ParentId": "0PSXX000000PS02",
                    "Parent": {
                        "IsOwnedByProfile": False,
                        "ProfileId": None,
                        "Name": "TestPS",
                        "Label": "Test Permission Set",
                    },
                },
            ]
        }

    if "FROM OBJECTPERMISSIONS" in q:
        return {
            "records": [
                {
                    "SobjectType": "Account",
                    "PermissionsCreate": True,
                    "PermissionsRead": True,
                    "PermissionsEdit": True,
                    "PermissionsDelete": False,
                    "PermissionsViewAllRecords": True,
                    "PermissionsModifyAllRecords": False,
                    "PermissionsViewAllFields": False,
                    "ParentId": "0PSXX000000SHAD2",
                    "Parent": {
                        "IsOwnedByProfile": True,
                        "ProfileId": "00eXX000000PROF1",
                        "Name": "SysAdmin_Shadow",
                        "Label": "SysAdmin Shadow",
                    },
                },
                {
                    "SobjectType": "Contact",
                    "PermissionsCreate": True,
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "PermissionsDelete": False,
                    "PermissionsViewAllRecords": False,
                    "PermissionsModifyAllRecords": False,
                    "PermissionsViewAllFields": False,
                    "ParentId": "0PSXX000000PS02",
                    "Parent": {
                        "IsOwnedByProfile": False,
                        "ProfileId": None,
                        "Name": "TestPS",
                        "Label": "Test Permission Set",
                    },
                },
                {
                    "SobjectType": "Account",
                    "PermissionsCreate": True,
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "PermissionsDelete": False,
                    "PermissionsViewAllRecords": False,
                    "PermissionsModifyAllRecords": False,
                    "PermissionsViewAllFields": False,
                    "ParentId": "0PSXX000000SHAD",
                    "Parent": {
                        "IsOwnedByProfile": False,
                        "ProfileId": None,
                        "Name": "PSGShadow",
                        "Label": "PSG Shadow PS",
                    },
                },
                {
                    "SobjectType": "Lead",
                    "PermissionsCreate": True,
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "PermissionsDelete": False,
                    "PermissionsViewAllRecords": False,
                    "PermissionsModifyAllRecords": False,
                    "PermissionsViewAllFields": False,
                    "ParentId": "100XX000000LICPS",
                    "Parent": {
                        "IsOwnedByProfile": False,
                        "ProfileId": None,
                        "Name": "LicensePS",
                        "Label": "License Permission Set",
                    },
                },
            ]
        }

    if "FROM ORGANIZATION" in q:
        return {
            "records": [
                {
                    "Id": "00DXX0000008TEST",
                    "Name": "Test Org",
                    "DefaultAccountAccess": "Read",
                    "DefaultContactAccess": "ControlledByParent",
                    "DefaultOpportunityAccess": "Edit",
                    "DefaultLeadAccess": "ReadEditTransfer",
                    "DefaultCaseAccess": "ReadEditTransfer",
                    "DefaultCalendarAccess": "HideDetailsInsert",
                    "DefaultPricebookAccess": "ReadSelect",
                    "DefaultCampaignAccess": "All",
                }
            ]
        }

    if "FROM USERROLE" in q:
        return {
            "records": [
                {"Id": "00EXX000000ROLE1", "Name": "CEO", "ParentRoleId": None, "PortalType": "None"},
                {
                    "Id": "00EXX000000ROLE2",
                    "Name": "VP Sales",
                    "ParentRoleId": "00EXX000000ROLE1",
                    "PortalType": "None",
                },
            ]
        }

    if "PERMISSIONSETGROUPCOMPONENT" in q:
        return {
            "records": [
                {
                    "PermissionSetId": "0PSXX000000PS01",
                    "PermissionSetGroupId": "0PGXX000000PG01",
                },
            ]
        }

    if "FROM PERMISSIONSETGROUP" in q:
        return {
            "records": [
                {
                    "Id": "0PGXX000000PG01",
                    "DeveloperName": "TestPSG",
                    "MasterLabel": "Test PSG",
                },
            ]
        }

    if "FROM PERMISSIONSETASSIGNMENT" in q:
        return {
            "records": [
                {
                    "AssigneeId": "005XX000001USER1",
                    "PermissionSetId": "0PSXX000000PS01",
                    "PermissionSetGroupId": None,
                    "PermissionSet": {
                        "IsOwnedByProfile": True,
                        "ProfileId": "00eXX000000PROF1",
                    },
                },
                {
                    "AssigneeId": "005XX000001USER1",
                    "PermissionSetId": "0PSXX000000PS02",
                    "PermissionSetGroupId": None,
                    "PermissionSet": {"IsOwnedByProfile": False, "ProfileId": None},
                },
                {
                    "AssigneeId": "005XX000001USER1",
                    "PermissionSetId": "0PSXX000000PS03",
                    "PermissionSetGroupId": "0PGXX000000PG01",
                    "PermissionSet": {"IsOwnedByProfile": False, "ProfileId": None},
                },
            ]
        }

    if "TYPE = 'GROUP'" in q:
        return {
            "records": [
                {
                    "Id": "0PSXX000000SHAD",
                    "PermissionSetGroupId": "0PGXX000000PG01",
                    **_all_caps_false(PermissionsModifyAllData=True),
                },
            ]
        }

    if (
        "FROM PERMISSIONSET" in q
        and "ISOWNEDBYPROFILE = FALSE" in q
        and "PERMISSIONSMODIFY" in q
    ):
        # Capability query (has PermissionsModifyAllData etc. in SELECT)
        return {
            "records": [
                {
                    "Id": "0PSXX000000PS02",
                    **_all_caps_false(PermissionsViewAllData=True),
                },
            ]
        }

    if "FROM PERMISSIONSET" in q and "ISOWNEDBYPROFILE = FALSE" in q:
        # Node query (SELECT Id, Name, Label, Type)
        return {
            "records": [
                {
                    "Id": "0PSXX000000PS02",
                    "Name": "TestPS",
                    "Label": "Test Permission Set",
                    "Type": "Regular",
                },
                {
                    "Id": "0PSXX000000SHAD",
                    "Name": "ShadowPS",
                    "Label": "Shadow",
                    "Type": "Group",
                },
            ]
        }

    if "FROM PROFILE" in q and "PERMISSIONS" in q:
        return {
            "records": [
                {
                    "Id": "00eXX000000PROF1",
                    **_all_caps_false(
                        PermissionsModifyAllData=True, PermissionsViewAllData=True
                    ),
                },
            ]
        }

    if "FROM PROFILE" in q:
        return {
            "records": [
                {
                    "Id": "00eXX000000PROF1",
                    "Name": "System Administrator",
                    "Description": "Admin profile",
                },
            ]
        }

    if "FROM QUEUESOBJECT" in q:
        return {
            "records": [
                {"QueueId": "00GXX000001GRP1", "SobjectType": "Lead"},
                {"QueueId": "00GXX000001GRP1", "SobjectType": "Case"},
            ]
        }

    if "FROM GROUPMEMBER" in q:
        return {
            "records": [
                {"GroupId": "00GXX000001GRP1", "UserOrGroupId": "005XX000001USER1"},
            ]
        }

    if "FROM GROUP" in q:
        return {
            "records": [
                {
                    "Id": "00GXX000001GRP1",
                    "Name": "AllUsers",
                    "DeveloperName": "AllInternalUsers",
                    "Type": "Regular",
                    "RelatedId": None,
                },
            ]
        }

    if "MANAGERID" in q and "FROM USER" in q:
        return {
            "records": [
                {"Id": "005XX000001USER1", "ManagerId": "005XX000002USER2"},
            ]
        }

    if "FROM USER" in q:
        return {
            "records": [
                {
                    "Id": "005XX000001USER1",
                    "Name": "Test User",
                    "Email": "test@test.com",
                    "UserType": "Standard",
                    "IsActive": True,
                    "UserRoleId": "00EXX000001ROLE1",
                },
                {
                    "Id": "005XX000002USER2",
                    "Name": "Manager",
                    "Email": "mgr@test.com",
                    "UserType": "Standard",
                    "IsActive": True,
                    "UserRoleId": None,
                },
            ]
        }

    return {"records": []}


# =====================================================================
# Node builder tests
# =====================================================================


class TestAPICollectorNodeBuilders:
    def test_create_user_nodes(self, collector):
        data = {
            "records": [
                {
                    "Id": "005A",
                    "Name": "Alice",
                    "Email": "a@b.com",
                    "UserType": "Standard",
                    "IsActive": True,
                },
            ]
        }
        nodes = collector._create_user_nodes(data)
        assert len(nodes) == 1
        assert nodes[0].id == "005A"
        assert "SF_User" in nodes[0].kinds
        assert nodes[0].properties["name"] == "Alice"

    def test_create_profile_nodes(self, collector):
        data = {"records": [{"Id": "00eA", "Name": "Admin"}]}
        nodes = collector._create_profile_nodes(data)
        assert len(nodes) == 1
        assert "SF_Profile" in nodes[0].kinds

    def test_create_permission_set_nodes(self, collector):
        data = {
            "records": [
                {"Id": "0PSA", "Name": "TestPS", "Label": "Test PS", "Type": "Regular"},
                {
                    "Id": "0PSB",
                    "Name": "SessionPS",
                    "Label": "Session",
                    "Type": "Session",
                },
                {"Id": "0PSC", "Name": "ShadowPS", "Label": "Shadow", "Type": "Group"},
            ]
        }
        nodes = collector._create_permission_set_nodes(data)
        assert len(nodes) == 2  # Group type skipped
        names = {n.properties["name"] for n in nodes}
        assert "TestPS" in names
        assert "SessionPS" in names
        assert "ShadowPS" not in names

    def test_create_organization_nodes(self, collector):
        data = {"records": [{"Id": "00DA", "Name": "Test Org"}]}
        nodes = collector._create_organization_nodes(data)
        assert len(nodes) == 1
        assert "SF_Organization" in nodes[0].kinds

    def test_create_organization_nodes_owd_properties(self, collector):
        data = {
            "records": [
                {
                    "Id": "00DA",
                    "Name": "Test Org",
                    "DefaultAccountAccess": "Read",
                    "DefaultContactAccess": "ControlledByParent",
                    "DefaultOpportunityAccess": "Edit",
                    "DefaultLeadAccess": "ReadEditTransfer",
                    "DefaultCaseAccess": "ReadEditTransfer",
                    "DefaultCalendarAccess": "HideDetailsInsert",
                    "DefaultPricebookAccess": "ReadSelect",
                    "DefaultCampaignAccess": "All",
                }
            ]
        }
        nodes = collector._create_organization_nodes(data)
        assert len(nodes) == 1
        props = nodes[0].properties
        assert props["DefaultAccountAccess"] == "Read"
        assert props["DefaultContactAccess"] == "ControlledByParent"
        assert props["DefaultOpportunityAccess"] == "Edit"
        assert props["DefaultLeadAccess"] == "ReadEditTransfer"
        assert props["DefaultCaseAccess"] == "ReadEditTransfer"
        assert props["DefaultCalendarAccess"] == "HideDetailsInsert"
        assert props["DefaultPricebookAccess"] == "ReadSelect"
        assert props["DefaultCampaignAccess"] == "All"

    def test_create_object_nodes(self, collector):
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "ParentId": "P1",
                    **{f: False for f in OBJECT_PERMISSION_FIELDS},
                },
                {
                    "SobjectType": "Contact",
                    "ParentId": "P2",
                    **{f: False for f in OBJECT_PERMISSION_FIELDS},
                },
                {
                    "SobjectType": "Account",
                    "ParentId": "P3",
                    **{f: False for f in OBJECT_PERMISSION_FIELDS},
                },
            ]
        }
        nodes = collector._create_object_nodes(data)
        assert len(nodes) == 2  # Account deduplicated
        names = {n.properties["name"] for n in nodes}
        assert names == {"Account", "Contact"}
        assert "SF_Object" in nodes[0].kinds
        assert nodes[0].id == generate_hash_id("SF_Object", "Account")

    def test_create_role_nodes(self, collector):
        data = {"records": [{"Id": "00EA", "Name": "CEO"}]}
        nodes = collector._create_role_nodes(data)
        assert len(nodes) == 1
        assert "SF_Role" in nodes[0].kinds

    def test_create_psg_nodes(self, collector):
        data = {
            "records": [
                {"Id": "0PGA", "DeveloperName": "TestPSG", "MasterLabel": "Test PSG"}
            ]
        }
        nodes = collector._create_psg_nodes(data)
        assert len(nodes) == 1
        assert nodes[0].properties["name"] == "TestPSG"

    def test_create_group_nodes(self, collector):
        data = {
            "records": [
                {
                    "Id": "00GA",
                    "DeveloperName": "AllUsers",
                    "Type": "Regular",
                    "Name": "All",
                    "RelatedId": None,
                }
            ]
        }
        nodes = collector._create_group_nodes(data)
        assert len(nodes) == 1
        assert "SF_Group" in nodes[0].kinds


# =====================================================================
# Edge builder tests
# =====================================================================


class TestAPICollectorEdgeBuilders:
    def test_role_hierarchy_edges(self, collector):
        data = {
            "records": [
                {"Id": "00EA", "ParentRoleId": "00EB", "Name": "VP"},
                {"Id": "00EB", "ParentRoleId": None, "Name": "CEO"},
            ]
        }
        edges = collector._create_role_hierarchy_edges(data)
        assert len(edges) == 1
        assert edges[0].kind == "ReportsTo"
        assert edges[0].source == "00EA"
        assert edges[0].target == "00EB"

    def test_management_edges(self, collector):
        data = {"records": [{"Id": "005A", "ManagerId": "005B"}]}
        edges = collector._create_management_edges(data)
        assert len(edges) == 1
        assert edges[0].kind == "ManagedBy"
        assert edges[0].source == "005A"
        assert edges[0].target == "005B"

    def test_has_role_edges(self, collector):
        data = {
            "records": [
                {"Id": "005A", "UserRoleId": "00EA"},
                {"Id": "005B", "UserRoleId": None},
            ]
        }
        edges = collector._create_has_role_edges(data)
        assert len(edges) == 1
        assert edges[0].kind == "HasRole"
        assert edges[0].source == "005A"
        assert edges[0].target == "00EA"

    def test_assignment_edges_profile(self, collector):
        data = {
            "records": [
                {
                    "AssigneeId": "005A",
                    "PermissionSetId": "0PSA",
                    "PermissionSetGroupId": None,
                    "PermissionSet": {"IsOwnedByProfile": True, "ProfileId": "00eA"},
                }
            ]
        }
        edges = collector._create_assignment_edges(data)
        assert len(edges) == 1
        assert edges[0].kind == "HasProfile"
        assert edges[0].target == "00eA"

    def test_assignment_edges_permission_set(self, collector):
        data = {
            "records": [
                {
                    "AssigneeId": "005A",
                    "PermissionSetId": "0PSA",
                    "PermissionSetGroupId": None,
                    "PermissionSet": {"IsOwnedByProfile": False, "ProfileId": None},
                }
            ]
        }
        edges = collector._create_assignment_edges(data)
        assert len(edges) == 1
        assert edges[0].kind == "HasPermissionSet"
        assert edges[0].target == "0PSA"

    def test_assignment_edges_psg(self, collector):
        data = {
            "records": [
                {
                    "AssigneeId": "005A",
                    "PermissionSetId": "0PSA",
                    "PermissionSetGroupId": "0PGA",
                    "PermissionSet": {"IsOwnedByProfile": False, "ProfileId": None},
                }
            ]
        }
        edges = collector._create_assignment_edges(data)
        assert len(edges) == 1
        assert edges[0].kind == "HasPermissionSet"
        assert edges[0].target == "0PGA"

    def test_capability_edges(self, collector):
        data = {
            "records": [
                {
                    "Id": "00eA",
                    **_all_caps_false(
                        PermissionsModifyAllData=True, PermissionsViewAllData=True
                    ),
                }
            ]
        }
        edges = collector._create_capability_edges(data, "00DXX")
        assert len(edges) == 2
        kinds = {e.kind for e in edges}
        assert "ModifyAllData" in kinds
        assert "ViewAllData" in kinds

    def test_capability_edges_all_15(self, collector):
        """All 15 capabilities produce edges when True."""
        data = {
            "records": [
                {
                    "Id": "00eA",
                    **{f: True for f in CAPABILITY_FIELDS},
                }
            ]
        }
        edges = collector._create_capability_edges(data, "00DXX")
        assert len(edges) == 15

    def test_psg_capability_edges_uses_psg_id(self, collector):
        data = {
            "records": [
                {
                    "Id": "0PS_SHADOW",
                    "PermissionSetGroupId": "0PGA",
                    **_all_caps_false(PermissionsModifyAllData=True),
                }
            ]
        }
        edges = collector._create_psg_capability_edges(data, "00DXX")
        assert len(edges) == 1
        assert edges[0].source == "0PGA"  # PSG ID, not shadow PS ID

    def test_psgc_edges(self, collector):
        data = {
            "records": [
                {"PermissionSetId": "0PSA", "PermissionSetGroupId": "0PGA"},
            ]
        }
        edges = collector._create_psgc_edges(data)
        assert len(edges) == 1
        assert edges[0].kind == "IncludedIn"

    def test_group_member_edges(self, collector):
        data = {
            "records": [
                {"GroupId": "00GA", "UserOrGroupId": "005A"},
            ]
        }
        edges = collector._create_group_member_edges(data)
        assert len(edges) == 1
        assert edges[0].kind == "MemberOf"

    def test_object_permission_edges(self, collector):
        """All 7 CRUD permissions create edges; source uses ParentId for non-profile PS."""
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "ParentId": "0PSA",
                    "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                    "PermissionsCreate": True,
                    "PermissionsRead": True,
                    "PermissionsEdit": True,
                    "PermissionsDelete": True,
                    "PermissionsViewAllRecords": True,
                    "PermissionsModifyAllRecords": True,
                    "PermissionsViewAllFields": True,
                }
            ]
        }
        edges = collector._create_object_permission_edges(data)
        assert len(edges) == 7
        kinds = {e.kind for e in edges}
        assert kinds == {
            "CanCreate",
            "CanRead",
            "CanEdit",
            "CanDelete",
            "CanViewAll",
            "CanModifyAll",
            "CanViewAllFields",
        }
        target_id = generate_hash_id("SF_Object", "Account")
        assert all(e.source == "0PSA" for e in edges)
        assert all(e.target == target_id for e in edges)

    def test_object_permission_edges_resolves_profile(self, collector):
        """Profile-owned PS ParentId is resolved to the actual Profile ID."""
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "ParentId": "0PSXX_SHADOW",
                    "Parent": {"IsOwnedByProfile": True, "ProfileId": "00eA"},
                    "PermissionsCreate": True,
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "PermissionsDelete": False,
                    "PermissionsViewAllRecords": False,
                    "PermissionsModifyAllRecords": False,
                    "PermissionsViewAllFields": False,
                }
            ]
        }
        edges = collector._create_object_permission_edges(data)
        assert len(edges) == 2
        assert all(e.source == "00eA" for e in edges)  # Profile ID, not shadow PS

    def test_object_permission_edges_skips_false(self, collector):
        data = {
            "records": [
                {
                    "SobjectType": "Contact",
                    "ParentId": "0PSA",
                    "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                    "PermissionsCreate": False,
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "PermissionsDelete": False,
                    "PermissionsViewAllRecords": False,
                    "PermissionsModifyAllRecords": False,
                    "PermissionsViewAllFields": False,
                }
            ]
        }
        edges = collector._create_object_permission_edges(data)
        assert len(edges) == 1
        assert edges[0].kind == "CanRead"

    def test_object_permission_edges_remaps_psg_shadow(self, collector):
        """PSG shadow PS ParentId is remapped to the PSG's 0PG ID."""
        # Set up PSG shadow mapping (normally done by _build_all_nodes)
        psg_shadow_map = {"0PS_SHADOW": "0PG_TARGET"}
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "ParentId": "0PS_SHADOW",
                    "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                    "PermissionsCreate": True,
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "PermissionsDelete": False,
                    "PermissionsViewAllRecords": False,
                    "PermissionsModifyAllRecords": False,
                    "PermissionsViewAllFields": False,
                }
            ]
        }
        edges = collector._create_object_permission_edges(data, psg_shadow_map=psg_shadow_map)
        assert len(edges) == 2
        assert all(e.source == "0PG_TARGET" for e in edges)

    def test_fallback_parent_nodes(self, collector):
        """Unknown ParentId prefixes get fallback SF_PermissionSet nodes."""
        known_ids = {"0PSXX_EXISTING"}
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "ParentId": "100XX000000LIC01",
                    "Parent": {
                        "IsOwnedByProfile": False,
                        "ProfileId": None,
                        "Name": "LicPS1",
                        "Label": "License PS 1",
                    },
                    "PermissionsCreate": True,
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "PermissionsDelete": False,
                    "PermissionsViewAllRecords": False,
                    "PermissionsModifyAllRecords": False,
                    "PermissionsViewAllFields": False,
                },
                {
                    "SobjectType": "Contact",
                    "ParentId": "0PLXX000000LIC02",
                    "Parent": {
                        "IsOwnedByProfile": False,
                        "ProfileId": None,
                        "Name": "SetLic2",
                        "Label": "Set License 2",
                    },
                    "PermissionsCreate": True,
                    "PermissionsRead": False,
                    "PermissionsEdit": False,
                    "PermissionsDelete": False,
                    "PermissionsViewAllRecords": False,
                    "PermissionsModifyAllRecords": False,
                    "PermissionsViewAllFields": False,
                },
                {
                    "SobjectType": "Lead",
                    "ParentId": "0PSXX_EXISTING",
                    "Parent": {
                        "IsOwnedByProfile": False,
                        "ProfileId": None,
                        "Name": "Existing",
                        "Label": "Existing PS",
                    },
                    "PermissionsCreate": True,
                    "PermissionsRead": False,
                    "PermissionsEdit": False,
                    "PermissionsDelete": False,
                    "PermissionsViewAllRecords": False,
                    "PermissionsModifyAllRecords": False,
                    "PermissionsViewAllFields": False,
                },
                {
                    "SobjectType": "Opportunity",
                    "ParentId": "0PSXX_SHADOW",
                    "Parent": {"IsOwnedByProfile": True, "ProfileId": "00eXX_PROF"},
                    "PermissionsCreate": True,
                    "PermissionsRead": False,
                    "PermissionsEdit": False,
                    "PermissionsDelete": False,
                    "PermissionsViewAllRecords": False,
                    "PermissionsModifyAllRecords": False,
                    "PermissionsViewAllFields": False,
                },
            ]
        }
        nodes = collector._create_fallback_parent_nodes(data, known_ids)
        # Only 100XX and 0PLXX get fallback nodes; 0PSXX_EXISTING is known,
        # 0PSXX_SHADOW is profile-owned (skipped).
        assert len(nodes) == 2
        ids = {n.id for n in nodes}
        assert "100XX000000LIC01" in ids
        assert "0PLXX000000LIC02" in ids
        # Uses Label for name
        lic1 = next(n for n in nodes if n.id == "100XX000000LIC01")
        assert lic1.properties["name"] == "License PS 1"
        assert "SF_PermissionSet" in lic1.kinds


# =====================================================================
# Integration / collect flow tests
# =====================================================================


class TestAPICollectorCollect:
    @pytest.mark.asyncio
    async def test_collect_validates_auth(self):
        auth = AuthConfig(instance_url="")
        collector = APICollector(auth)
        with pytest.raises(ValueError):
            await collector.collect()

    @pytest.mark.asyncio
    async def test_collect_full_flow(self, api_auth, mock_sf):
        collector = APICollector(api_auth, verbose=False)

        # Mock ShareObjectCollector
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        assert result.collector_type == "api"
        assert result.org_id == "00DXX0000008TEST"
        assert len(result.nodes) > 0
        assert len(result.edges) > 0

    @pytest.mark.asyncio
    async def test_collect_has_expected_node_kinds(self, api_auth, mock_sf):
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        all_kinds = set()
        for node in result.nodes:
            all_kinds.update(node.kinds)

        assert "SF_User" in all_kinds
        assert "SF_Profile" in all_kinds
        assert "SF_Role" in all_kinds
        assert "SF_Organization" in all_kinds
        assert "SF_Object" in all_kinds

    @pytest.mark.asyncio
    async def test_collect_has_expected_edge_kinds(self, api_auth, mock_sf):
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        edge_kinds = {e.kind for e in result.edges}
        assert "ReportsTo" in edge_kinds
        assert "HasRole" in edge_kinds
        assert "ManagedBy" in edge_kinds
        assert "HasProfile" in edge_kinds
        assert "ModifyAllData" in edge_kinds
        assert "CanCreate" in edge_kinds
        assert "CanRead" in edge_kinds

    @pytest.mark.asyncio
    async def test_collect_owd_on_organization_node(self, api_auth, mock_sf):
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        org_nodes = [n for n in result.nodes if "SF_Organization" in n.kinds]
        assert len(org_nodes) == 1
        props = org_nodes[0].properties
        assert props["DefaultAccountAccess"] == "Read"
        assert props["DefaultCampaignAccess"] == "All"


# =====================================================================
# Supplement-only mode tests (--collector both)
# =====================================================================


class TestAPICollectorSupplementMode:
    @pytest.mark.asyncio
    async def test_supplement_skips_org_id_query(self, api_auth, mock_sf):
        """When org_id is provided, the initial Organization query is skipped."""
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        # sf.query() should NOT have been called (org_id was provided).
        mock_sf.query.assert_not_called()
        assert result.org_id == "00DXX0000008TEST"

    @pytest.mark.asyncio
    async def test_supplement_runs_only_10_queries(self, api_auth, mock_sf):
        """Supplement mode should run exactly 10 query_all() calls."""
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        # 12 supplement queries (no org_id query, no share queries in this mock)
        assert result.metadata["queries"] == 12

    @pytest.mark.asyncio
    async def test_supplement_no_user_nodes(self, api_auth, mock_sf):
        """Supplement mode should not produce User nodes (Aura handles those)."""
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        user_nodes = [n for n in result.nodes if "SF_User" in n.kinds]
        assert len(user_nodes) == 0

    @pytest.mark.asyncio
    async def test_supplement_no_role_or_group_nodes(self, api_auth, mock_sf):
        """Supplement mode should not produce Role, Profile, or Group nodes."""
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        all_kinds = set()
        for n in result.nodes:
            all_kinds.update(n.kinds)

        assert "SF_Role" not in all_kinds
        assert "SF_Group" not in all_kinds
        assert "SF_PublicGroup" not in all_kinds

    @pytest.mark.asyncio
    async def test_supplement_still_has_ps_psg_org_object(self, api_auth, mock_sf):
        """Supplement mode still produces PS, PSG, Organization, SF_Object nodes."""
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        all_kinds = set()
        for n in result.nodes:
            all_kinds.update(n.kinds)

        assert "SF_PermissionSet" in all_kinds
        assert "SF_Organization" in all_kinds
        assert "SF_Object" in all_kinds

    @pytest.mark.asyncio
    async def test_supplement_still_has_owd(self, api_auth, mock_sf):
        """Supplement mode still collects OWD fields on Organization."""
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        org_nodes = [n for n in result.nodes if "SF_Organization" in n.kinds]
        assert len(org_nodes) == 1
        assert org_nodes[0].properties["DefaultAccountAccess"] == "Read"

    @pytest.mark.asyncio
    async def test_supplement_no_profile_capability_edges(self, api_auth, mock_sf):
        """Supplement mode skips Profile capability edges (Aura handles those)."""
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        # No ReportsTo (roles), no HasRole, no ManagedBy (management chain),
        # no MemberOf (group members) — those come from Aura.
        edge_kinds = {e.kind for e in result.edges}
        assert "ReportsTo" not in edge_kinds
        assert "HasRole" not in edge_kinds
        assert "ManagedBy" not in edge_kinds
        assert "MemberOf" not in edge_kinds

    @pytest.mark.asyncio
    async def test_supplement_still_has_psa_and_crud_edges(self, api_auth, mock_sf):
        """Supplement mode still produces PSA assignment and CRUD edges."""
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        edge_kinds = {e.kind for e in result.edges}
        assert "HasProfile" in edge_kinds
        assert "HasPermissionSet" in edge_kinds
        assert "CanCreate" in edge_kinds
        assert "CanRead" in edge_kinds
        assert "IncludedIn" in edge_kinds

    @pytest.mark.asyncio
    async def test_supplement_uses_known_node_ids_for_share(self, api_auth, mock_sf):
        """known_node_ids are passed to ShareObjectCollector."""
        known = {"005XX_AURA_USER", "00GXX_AURA_GROUP"}
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
            known_node_ids=known,
        )
        mock_share_cls = MagicMock()
        mock_share_instance = MagicMock()
        mock_share_instance.collect.return_value = ([], [])
        mock_share_instance.query_count = 0
        mock_share_cls.return_value = mock_share_instance

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                mock_share_cls,
            ),
        ):
            await collector.collect()

        # ShareObjectCollector was instantiated with known IDs.
        call_args = mock_share_cls.call_args
        existing_ids_arg = call_args[0][1]  # second positional arg
        assert "005XX_AURA_USER" in existing_ids_arg
        assert "00GXX_AURA_GROUP" in existing_ids_arg


# =====================================================================
# --skip-object-permissions tests
# =====================================================================


class TestAPICollectorSkipObjectPermissions:
    @pytest.mark.asyncio
    async def test_skip_obj_perm_no_object_nodes(self, api_auth, mock_sf):
        """With skip_object_permissions + skip_field_permissions, no SF_Object nodes."""
        collector = APICollector(
            api_auth,
            skip_object_permissions=True,
            skip_field_permissions=True,
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        # SF_Object nodes may still exist from QueueSobject, but no
        # ObjectPermissions-sourced CRUD edges (checked in next test).

    @pytest.mark.asyncio
    async def test_skip_obj_perm_no_crud_edges(self, api_auth, mock_sf):
        """With skip_object_permissions, no CRUD edges are produced."""
        collector = APICollector(
            api_auth,
            skip_object_permissions=True,
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        crud_kinds = {
            "CanCreate",
            "CanRead",
            "CanEdit",
            "CanDelete",
            "CanViewAll",
            "CanModifyAll",
            "CanViewAllFields",
        }
        edge_kinds = {e.kind for e in result.edges}
        assert not edge_kinds & crud_kinds

    @pytest.mark.asyncio
    async def test_skip_obj_perm_still_has_identity_data(self, api_auth, mock_sf):
        """Skipping ObjectPermissions preserves identity/capability data."""
        collector = APICollector(
            api_auth,
            skip_object_permissions=True,
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        all_kinds = set()
        for n in result.nodes:
            all_kinds.update(n.kinds)
        assert "SF_User" in all_kinds
        assert "SF_Profile" in all_kinds
        assert "SF_PermissionSet" in all_kinds
        assert "SF_Organization" in all_kinds

        edge_kinds = {e.kind for e in result.edges}
        assert "HasProfile" in edge_kinds
        assert "ModifyAllData" in edge_kinds

    @pytest.mark.asyncio
    async def test_skip_obj_perm_reduces_query_count(self, api_auth, mock_sf):
        """Skipping ObjectPermissions runs 20 queries (1 org + 19) instead of 21."""
        collector = APICollector(
            api_auth,
            skip_object_permissions=True,
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        # 1 (org_id) + 19 (full queries minus ObjectPermissions) = 20
        assert result.metadata["queries"] == 20

    @pytest.mark.asyncio
    async def test_skip_obj_perm_supplement_mode(self, api_auth, mock_sf):
        """skip_object_permissions also works in supplement mode."""
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
            skip_object_permissions=True,
            skip_field_permissions=True,
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        # 10 queries (12 supplement minus ObjectPermissions minus FieldPermissions)
        assert result.metadata["queries"] == 10

        crud_kinds = {
            "CanCreate",
            "CanRead",
            "CanEdit",
            "CanDelete",
            "CanViewAll",
            "CanModifyAll",
            "CanViewAllFields",
        }
        edge_kinds = {e.kind for e in result.edges}
        assert not edge_kinds & crud_kinds

        # SF_Object nodes may still exist from QueueSobject — that's expected.


# =====================================================================
# --skip-shares tests
# =====================================================================


class TestAPICollectorSkipShares:
    @pytest.mark.asyncio
    async def test_skip_shares_no_share_nodes_or_edges(self, api_auth, mock_sf):
        """With skip_shares, no SF_Record nodes or share edges are produced."""
        collector = APICollector(api_auth, skip_shares=True)

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector"
            ) as ShareCls,
        ):
            result = await collector.collect()

        # ShareObjectCollector should never be instantiated.
        ShareCls.assert_not_called()

        all_kinds = set()
        for n in result.nodes:
            all_kinds.update(n.kinds)
        assert "SF_Record" not in all_kinds

        share_kinds = {"ExplicitAccess", "Owns", "InheritsAccess"}
        edge_kinds = {e.kind for e in result.edges}
        assert not edge_kinds & share_kinds

    @pytest.mark.asyncio
    async def test_skip_shares_preserves_identity_data(self, api_auth, mock_sf):
        """Skipping shares preserves identity, capability, and CRUD data."""
        collector = APICollector(api_auth, skip_shares=True)

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch("forcehound.collectors.api_collector.ShareObjectCollector"),
        ):
            result = await collector.collect()

        all_kinds = set()
        for n in result.nodes:
            all_kinds.update(n.kinds)
        assert "SF_User" in all_kinds
        assert "SF_Profile" in all_kinds
        assert "SF_Organization" in all_kinds
        assert "SF_Object" in all_kinds

        edge_kinds = {e.kind for e in result.edges}
        assert "HasProfile" in edge_kinds
        assert "ModifyAllData" in edge_kinds
        assert "CanCreate" in edge_kinds

    @pytest.mark.asyncio
    async def test_skip_shares_supplement_mode(self, api_auth, mock_sf):
        """skip_shares works in supplement mode too."""
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
            skip_shares=True,
        )

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector"
            ) as ShareCls,
        ):
            result = await collector.collect()

        ShareCls.assert_not_called()

        share_kinds = {"ExplicitAccess", "Owns", "InheritsAccess"}
        edge_kinds = {e.kind for e in result.edges}
        assert not edge_kinds & share_kinds

    @pytest.mark.asyncio
    async def test_skip_both_shares_and_obj_perms(self, api_auth, mock_sf):
        """All three skip flags eliminate shares, CRUD, and FLS."""
        collector = APICollector(
            api_auth,
            skip_shares=True,
            skip_object_permissions=True,
            skip_field_permissions=True,
        )

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector"
            ) as ShareCls,
        ):
            result = await collector.collect()

        ShareCls.assert_not_called()

        all_kinds = set()
        for n in result.nodes:
            all_kinds.update(n.kinds)
        assert "SF_Record" not in all_kinds
        # SF_Object nodes may still exist from QueueSobject — that's expected.

        excluded = {
            "ExplicitAccess",
            "Owns",
            "InheritsAccess",
            "CanCreate",
            "CanRead",
            "CanEdit",
            "CanDelete",
            "CanViewAll",
            "CanModifyAll",
            "CanViewAllFields",
        }
        edge_kinds = {e.kind for e in result.edges}
        assert not edge_kinds & excluded

        # Identity data still present
        assert "SF_User" in all_kinds
        assert "HasProfile" in edge_kinds


# =====================================================================
# Connected Application tests
# =====================================================================


class TestAPICollectorConnectedApps:
    @pytest.mark.asyncio
    async def test_connected_app_nodes_created(self, api_auth, mock_sf):
        """Connected App query produces SF_ConnectedApp nodes."""
        collector = APICollector(api_auth)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        ca_nodes = [n for n in result.nodes if "SF_ConnectedApp" in n.kinds]
        assert len(ca_nodes) == 2

        app1 = next(n for n in ca_nodes if n.id == "0H4XX000000APP01")
        assert app1.properties["name"] == "Test Connected App"
        assert app1.properties["admin_approved_only"] is True
        assert app1.properties["is_internal"] is False
        assert app1.properties["start_url"] == "https://example.com/callback"
        assert app1.properties["refresh_token_validity_period"] == 720

        app2 = next(n for n in ca_nodes if n.id == "0H4XX000000APP02")
        assert app2.properties["name"] == "Open App"
        assert app2.properties["admin_approved_only"] is False
        assert app2.properties["is_internal"] is True
        assert app2.properties["start_url"] is None
        assert app2.properties["refresh_token_validity_period"] is None

    @pytest.mark.asyncio
    async def test_connected_app_node_kinds(self, api_auth, mock_sf):
        """SF_ConnectedApp nodes have the correct kinds array."""
        collector = APICollector(api_auth)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        ca_nodes = [n for n in result.nodes if "SF_ConnectedApp" in n.kinds]
        for node in ca_nodes:
            assert node.kinds == ["SF_ConnectedApp", "SaaS_Application"]

    @pytest.mark.asyncio
    async def test_connected_app_access_edges(self, api_auth, mock_sf):
        """SetupEntityAccess produces CanAccessApp edges plus implicit edges."""
        collector = APICollector(api_auth)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        ca_edges = [e for e in result.edges if e.kind == "CanAccessApp"]
        # 2 explicit (SetupEntityAccess → APP01) + 1 implicit (PROF1 → APP02)
        assert len(ca_edges) == 3

    @pytest.mark.asyncio
    async def test_connected_app_access_profile_remapping(self, api_auth, mock_sf):
        """Profile shadow PS ParentId is remapped to Profile ID."""
        collector = APICollector(api_auth)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        ca_edges = [e for e in result.edges if e.kind == "CanAccessApp"]

        # First record: ParentId=0PSXX000000SHAD2 with IsOwnedByProfile=true,
        # ProfileId=00eXX000000PROF1 → source should be the Profile ID.
        profile_edge = next(
            (
                e
                for e in ca_edges
                if e.target == "0H4XX000000APP01" and e.source == "00eXX000000PROF1"
            ),
            None,
        )
        assert profile_edge is not None, (
            "Profile shadow PS should be remapped to Profile ID"
        )

        # Second record: ParentId=0PSXX000000PS02, IsOwnedByProfile=false
        # → source should be the PS ID as-is.
        ps_edge = next(
            (
                e
                for e in ca_edges
                if e.target == "0H4XX000000APP01" and e.source == "0PSXX000000PS02"
            ),
            None,
        )
        assert ps_edge is not None, "Regular PS should use ParentId as-is"

    @pytest.mark.asyncio
    async def test_connected_app_access_psg_remapping(self, api_auth, mock_sf):
        """PSG shadow PS ParentId is remapped to PSG ID."""
        collector = APICollector(api_auth)

        # Override SetupEntityAccess to return a PSG shadow PS
        def route_with_psg_shadow(query):
            q = query.upper()
            if "FROM SETUPENTITYACCESS" in q:
                return {
                    "records": [
                        {
                            "SetupEntityId": "0H4XX000000APP01",
                            "ParentId": "0PSXX000000SHAD",  # This is the PSG shadow PS
                            "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                        },
                    ]
                }
            return _route_query(query)

        mock_sf_copy = MagicMock()
        mock_sf_copy.query.return_value = {"records": [{"Id": "00DXX0000008TEST"}]}
        mock_sf_copy.query_all.side_effect = route_with_psg_shadow

        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf_copy),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        ca_edges = [e for e in result.edges if e.kind == "CanAccessApp"]
        # 1 explicit (PSG shadow → APP01) + 1 implicit (PROF1 → APP02)
        assert len(ca_edges) == 2
        # 0PSXX000000SHAD → PSG 0PGXX000000PG01 (via psg_shadow_map)
        psg_edge = next(e for e in ca_edges if e.target == "0H4XX000000APP01")
        assert psg_edge.source == "0PGXX000000PG01"

    @pytest.mark.asyncio
    async def test_connected_app_empty_access(self, api_auth, mock_sf):
        """When no SetupEntityAccess rows exist, nodes still created but no edges."""
        collector = APICollector(api_auth)

        def route_empty_access(query):
            q = query.upper()
            if "FROM SETUPENTITYACCESS" in q:
                return {"records": []}
            return _route_query(query)

        mock_sf_copy = MagicMock()
        mock_sf_copy.query.return_value = {"records": [{"Id": "00DXX0000008TEST"}]}
        mock_sf_copy.query_all.side_effect = route_empty_access

        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf_copy),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        ca_nodes = [n for n in result.nodes if "SF_ConnectedApp" in n.kinds]
        assert len(ca_nodes) == 2  # Nodes always created

        ca_edges = [e for e in result.edges if e.kind == "CanAccessApp"]
        # No explicit edges, but 1 implicit edge (PROF1 → APP02, the open app)
        assert len(ca_edges) == 1
        assert ca_edges[0].target == "0H4XX000000APP02"

    @pytest.mark.asyncio
    async def test_connected_app_full_mode_query_count(self, api_auth, mock_sf):
        """Full mode runs 21 queries (1 org_id + 20)."""
        collector = APICollector(api_auth)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        # 1 (org_id) + 20 (full queries) = 21
        assert result.metadata["queries"] == 21

    @pytest.mark.asyncio
    async def test_connected_app_supplement_mode(self, api_auth, mock_sf):
        """Connected Apps appear in supplement mode too."""
        collector = APICollector(
            api_auth,
            supplement_only=True,
            org_id="00DXX0000008TEST",
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        ca_nodes = [n for n in result.nodes if "SF_ConnectedApp" in n.kinds]
        assert len(ca_nodes) == 2

        ca_edges = [e for e in result.edges if e.kind == "CanAccessApp"]
        assert len(ca_edges) == 2

        assert result.metadata["connected_apps"] == 2

    @pytest.mark.asyncio
    async def test_connected_app_metadata(self, api_auth, mock_sf):
        """Metadata includes connected_apps count."""
        collector = APICollector(api_auth)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        assert result.metadata["connected_apps"] == 2


# =====================================================================
# Implicit Connected App edge tests
# =====================================================================


class TestImplicitConnectedAppEdges:
    """Tests for synthetic CanAccessApp edges on non-admin-approved apps."""

    TWO_PROFILES = {
        "records": [
            {"Id": "00eXX000000PROF1", "Name": "System Administrator"},
            {"Id": "00eXX000000PROF2", "Name": "Standard User"},
        ]
    }

    MIXED_APPS = {
        "records": [
            {
                "Id": "0H4XX000000APP01",
                "Name": "Admin-Approved App",
                "OptionsAllowAdminApprovedUsersOnly": True,
            },
            {
                "Id": "0H4XX000000APP02",
                "Name": "Open App",
                "OptionsAllowAdminApprovedUsersOnly": False,
            },
        ]
    }

    def test_implicit_edges_created_for_open_apps(self, collector):
        """Non-admin-approved apps get CanAccessApp edges from every Profile."""
        edges = collector._create_implicit_connected_app_edges(
            self.MIXED_APPS, self.TWO_PROFILES
        )
        # Only APP02 is open → 2 profiles × 1 open app = 2 edges
        assert len(edges) == 2
        assert all(e.kind == "CanAccessApp" for e in edges)
        assert all(e.target == "0H4XX000000APP02" for e in edges)
        sources = {e.source for e in edges}
        assert sources == {"00eXX000000PROF1", "00eXX000000PROF2"}

    def test_no_implicit_edges_for_admin_approved_apps(self, collector):
        """Admin-approved apps never get synthetic edges."""
        edges = collector._create_implicit_connected_app_edges(
            self.MIXED_APPS, self.TWO_PROFILES
        )
        targets = {e.target for e in edges}
        assert "0H4XX000000APP01" not in targets

    def test_implicit_edges_coexist_with_explicit(self, collector):
        """Explicit SetupEntityAccess edges and synthetic edges coexist."""
        explicit_edges = collector._create_connected_app_access_edges(
            {
                "records": [
                    {
                        "SetupEntityId": "0H4XX000000APP01",
                        "ParentId": "00eXX000000PROF1",
                        "Parent": {
                            "IsOwnedByProfile": True,
                            "ProfileId": "00eXX000000PROF1",
                        },
                    },
                ]
            }
        )
        implicit_edges = collector._create_implicit_connected_app_edges(
            self.MIXED_APPS, self.TWO_PROFILES
        )
        all_edges = explicit_edges + implicit_edges
        # 1 explicit (PROF1 → APP01) + 2 implicit (PROF1/PROF2 → APP02)
        assert len(all_edges) == 3
        explicit_targets = {e.target for e in explicit_edges}
        implicit_targets = {e.target for e in implicit_edges}
        assert "0H4XX000000APP01" in explicit_targets
        assert "0H4XX000000APP02" in implicit_targets

    def test_all_connected_apps_no_admin_approved(self, collector):
        """When all apps are open, every Profile gets edges to every app."""
        all_open = {
            "records": [
                {
                    "Id": "0H4XX000000APP01",
                    "Name": "App A",
                    "OptionsAllowAdminApprovedUsersOnly": False,
                },
                {
                    "Id": "0H4XX000000APP02",
                    "Name": "App B",
                    "OptionsAllowAdminApprovedUsersOnly": False,
                },
            ]
        }
        edges = collector._create_implicit_connected_app_edges(
            all_open, self.TWO_PROFILES
        )
        # 2 apps × 2 profiles = 4 edges
        assert len(edges) == 4
        targets = {e.target for e in edges}
        assert targets == {"0H4XX000000APP01", "0H4XX000000APP02"}

    def test_all_connected_apps_admin_approved(self, collector):
        """When all apps are admin-approved, no synthetic edges are created."""
        all_locked = {
            "records": [
                {
                    "Id": "0H4XX000000APP01",
                    "Name": "App A",
                    "OptionsAllowAdminApprovedUsersOnly": True,
                },
                {
                    "Id": "0H4XX000000APP02",
                    "Name": "App B",
                    "OptionsAllowAdminApprovedUsersOnly": True,
                },
            ]
        }
        edges = collector._create_implicit_connected_app_edges(
            all_locked, self.TWO_PROFILES
        )
        assert len(edges) == 0


# =====================================================================
# Field-Level Security (FLS) tests
# =====================================================================


class TestAPICollectorFieldPermissions:
    """Tests for SF_Field node and FLS edge creation."""

    FIELD_PERM_DATA = {
        "records": [
            {
                "SobjectType": "Account",
                "Field": "Account.Industry",
                "PermissionsRead": True,
                "PermissionsEdit": False,
                "ParentId": "0PSXX000000PS01AAA",
                "Parent": {
                    "IsOwnedByProfile": False,
                    "ProfileId": None,
                    "Name": "TestPS",
                    "Label": "Test Permission Set",
                },
            },
            {
                "SobjectType": "Account",
                "Field": "Account.AnnualRevenue",
                "PermissionsRead": True,
                "PermissionsEdit": True,
                "ParentId": "0PSXX000000PROF1AAA",
                "Parent": {
                    "IsOwnedByProfile": True,
                    "ProfileId": "00eXX000000PROF1AAA",
                    "Name": "StandardProfile",
                    "Label": "Standard Profile",
                },
            },
        ]
    }

    def test_creates_field_nodes(self, collector):
        """SF_Field nodes are created with correct IDs, kinds, and properties."""
        field_nodes, _ = collector._create_field_nodes(self.FIELD_PERM_DATA)
        assert len(field_nodes) == 2
        ids = {n.id for n in field_nodes}
        assert generate_hash_id("SF_Field", "Account.Industry") in ids
        assert generate_hash_id("SF_Field", "Account.AnnualRevenue") in ids
        for n in field_nodes:
            assert "SF_Field" in n.kinds
            assert "SaaS_Resource" in n.kinds
            assert "name" in n.properties
            assert "field_name" in n.properties
            assert "object" in n.properties
            assert "is_custom" in n.properties
            # Standard fields (Industry, AnnualRevenue) are not custom.
            assert n.properties["is_custom"] is False
            # field_name is the short name after the dot.
            assert "." not in n.properties["field_name"]

    def test_field_nodes_deduplicated(self, collector):
        """Same Field from multiple ParentIds produces only one node."""
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "Field": "Account.Industry",
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "ParentId": "0PS_A",
                    "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                },
                {
                    "SobjectType": "Account",
                    "Field": "Account.Industry",
                    "PermissionsRead": True,
                    "PermissionsEdit": True,
                    "ParentId": "0PS_B",
                    "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                },
            ]
        }
        field_nodes, _ = collector._create_field_nodes(data)
        assert len(field_nodes) == 1

    def test_field_node_custom_flag(self, collector):
        """is_custom is True for __c fields, False for standard fields."""
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "Field": "Account.Industry",
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "ParentId": "0PS_A",
                    "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                },
                {
                    "SobjectType": "Lead",
                    "Field": "Lead.ProductInterest__c",
                    "PermissionsRead": True,
                    "PermissionsEdit": True,
                    "ParentId": "0PS_A",
                    "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                },
            ]
        }
        field_nodes, _ = collector._create_field_nodes(data)
        by_name = {n.properties["name"]: n for n in field_nodes}
        assert by_name["Account.Industry"].properties["is_custom"] is False
        assert by_name["Account.Industry"].properties["field_name"] == "Industry"
        assert by_name["Lead.ProductInterest__c"].properties["is_custom"] is True
        assert by_name["Lead.ProductInterest__c"].properties["field_name"] == "ProductInterest__c"

    def test_creates_object_nodes_from_field_perms(self, collector):
        """SF_Object nodes are created for SobjectTypes in FLS data."""
        _, object_nodes = collector._create_field_nodes(self.FIELD_PERM_DATA)
        assert len(object_nodes) == 1  # Both records reference "Account"
        assert object_nodes[0].id == generate_hash_id("SF_Object", "Account")
        assert "SF_Object" in object_nodes[0].kinds

    def test_creates_can_read_field_edges(self, collector):
        """CanReadField edges for PermissionsRead=True records."""
        edges = collector._create_field_permission_edges(self.FIELD_PERM_DATA, psg_shadow_map={})
        read_edges = [e for e in edges if e.kind == "CanReadField"]
        assert len(read_edges) == 2  # Both records have PermissionsRead=True

    def test_creates_can_edit_field_edges(self, collector):
        """CanEditField edges for PermissionsEdit=True records."""
        edges = collector._create_field_permission_edges(self.FIELD_PERM_DATA, psg_shadow_map={})
        edit_edges = [e for e in edges if e.kind == "CanEditField"]
        assert len(edit_edges) == 1  # Only second record has PermissionsEdit=True

    def test_no_edge_for_false_permission(self, collector):
        """PermissionsEdit=False produces no CanEditField edge."""
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "Field": "Account.Industry",
                    "PermissionsRead": False,
                    "PermissionsEdit": False,
                    "ParentId": "0PS_A",
                    "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                },
            ]
        }
        edges = collector._create_field_permission_edges(data, psg_shadow_map={})
        perm_edges = [e for e in edges if e.kind != "FieldOf"]
        assert len(perm_edges) == 0

    def test_source_resolution_profile_owned(self, collector):
        """Parent.IsOwnedByProfile=True → source is Parent.ProfileId."""
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "Field": "Account.Industry",
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "ParentId": "0PS_SHADOW",
                    "Parent": {"IsOwnedByProfile": True, "ProfileId": "00e_PROF"},
                },
            ]
        }
        edges = collector._create_field_permission_edges(data, psg_shadow_map={})
        read_edges = [e for e in edges if e.kind == "CanReadField"]
        assert len(read_edges) == 1
        assert read_edges[0].source == "00e_PROF"

    def test_source_resolution_psg_shadow(self, collector):
        """ParentId in PSG shadow map → source is PSG ID."""
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "Field": "Account.Industry",
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "ParentId": "0PS_SHADOW",
                    "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                },
            ]
        }
        edges = collector._create_field_permission_edges(data, psg_shadow_map={"0PS_SHADOW": "0PG_TARGET"})
        read_edges = [e for e in edges if e.kind == "CanReadField"]
        assert len(read_edges) == 1
        assert read_edges[0].source == "0PG_TARGET"

    def test_source_resolution_regular_ps(self, collector):
        """Regular ParentId → source is ParentId as-is."""
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "Field": "Account.Industry",
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "ParentId": "0PS_REGULAR",
                    "Parent": {"IsOwnedByProfile": False, "ProfileId": None},
                },
            ]
        }
        edges = collector._create_field_permission_edges(data, psg_shadow_map={})
        read_edges = [e for e in edges if e.kind == "CanReadField"]
        assert len(read_edges) == 1
        assert read_edges[0].source == "0PS_REGULAR"

    def test_field_of_edges(self, collector):
        """FieldOf edges from SF_Field → SF_Object are created."""
        edges = collector._create_field_permission_edges(self.FIELD_PERM_DATA, psg_shadow_map={})
        field_of_edges = [e for e in edges if e.kind == "FieldOf"]
        # 2 unique fields → 2 FieldOf edges (both point to Account)
        assert len(field_of_edges) == 2
        account_obj_id = generate_hash_id("SF_Object", "Account")
        assert all(e.target == account_obj_id for e in field_of_edges)

    def test_fallback_nodes_for_field_perms(self, collector):
        """Unknown ParentIds in FLS data create fallback PermissionSet nodes."""
        known_ids = {"0PS_EXISTING"}
        data = {
            "records": [
                {
                    "SobjectType": "Account",
                    "Field": "Account.Industry",
                    "PermissionsRead": True,
                    "PermissionsEdit": False,
                    "ParentId": "100XX000000FLSLIC",
                    "Parent": {
                        "IsOwnedByProfile": False,
                        "ProfileId": None,
                        "Name": "FLS_Lic",
                        "Label": "FLS License PS",
                    },
                },
            ]
        }
        nodes = collector._create_fallback_parent_nodes(data, known_ids)
        assert len(nodes) == 1
        assert nodes[0].id == "100XX000000FLSLIC"
        assert nodes[0].properties["name"] == "FLS License PS"


class TestAPICollectorSkipFieldPermissions:
    """Tests for --skip-field-permissions behavior."""

    @pytest.mark.asyncio
    async def test_skip_fls_no_field_nodes(self, api_auth, mock_sf):
        """No SF_Field kind in any node when FLS is skipped."""
        collector = APICollector(
            api_auth, verbose=False, skip_field_permissions=True
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        all_kinds = set()
        for node in result.nodes:
            all_kinds.update(node.kinds)
        assert "SF_Field" not in all_kinds

    @pytest.mark.asyncio
    async def test_skip_fls_no_fls_edges(self, api_auth, mock_sf):
        """No CanReadField/CanEditField/FieldOf edges when FLS is skipped."""
        collector = APICollector(
            api_auth, verbose=False, skip_field_permissions=True
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        edge_kinds = {e.kind for e in result.edges}
        assert "CanReadField" not in edge_kinds
        assert "CanEditField" not in edge_kinds
        assert "FieldOf" not in edge_kinds

    @pytest.mark.asyncio
    async def test_skip_fls_preserves_identity(self, api_auth, mock_sf):
        """User, Profile, PS nodes still present when FLS is skipped."""
        collector = APICollector(
            api_auth, verbose=False, skip_field_permissions=True
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        all_kinds = set()
        for node in result.nodes:
            all_kinds.update(node.kinds)
        assert "SF_User" in all_kinds
        assert "SF_Profile" in all_kinds
        assert "SF_PermissionSet" in all_kinds

    @pytest.mark.asyncio
    async def test_skip_fls_preserves_object_perms(self, api_auth, mock_sf):
        """CanCreate/CanRead etc. still present when FLS is skipped."""
        collector = APICollector(
            api_auth, verbose=False, skip_field_permissions=True
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        edge_kinds = {e.kind for e in result.edges}
        assert "CanCreate" in edge_kinds
        assert "CanRead" in edge_kinds

    @pytest.mark.asyncio
    async def test_skip_fls_supplement_mode(self, api_auth, mock_sf):
        """Skip FLS works with supplement_only=True."""
        collector = APICollector(
            api_auth,
            verbose=False,
            supplement_only=True,
            org_id="00DXX0000008TEST",
            skip_field_permissions=True,
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        all_kinds = set()
        for node in result.nodes:
            all_kinds.update(node.kinds)
        assert "SF_Field" not in all_kinds

    @pytest.mark.asyncio
    async def test_skip_fls_reduces_query_count(self, api_auth, mock_sf):
        """Skipping FLS reduces query count by 1."""
        collector_with = APICollector(api_auth, verbose=False)
        collector_without = APICollector(
            api_auth, verbose=False, skip_field_permissions=True
        )

        for c in (collector_with, collector_without):
            mock_share = MagicMock()
            mock_share.collect.return_value = ([], [])
            mock_share.query_count = 0
            with (
                patch.object(c, "_connect", return_value=mock_sf),
                patch(
                    "forcehound.collectors.api_collector.ShareObjectCollector",
                    return_value=mock_share,
                ),
            ):
                await c.collect()

        assert (
            collector_with._query_count - collector_without._query_count == 1
        )


class TestAPICollectorFieldPermissionsIntegration:
    """Integration tests verifying FLS in the full collect flow."""

    @pytest.mark.asyncio
    async def test_collect_has_field_nodes(self, api_auth, mock_sf):
        """Full collect includes SF_Field nodes."""
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        all_kinds = set()
        for node in result.nodes:
            all_kinds.update(node.kinds)
        assert "SF_Field" in all_kinds

    @pytest.mark.asyncio
    async def test_collect_has_fls_edges(self, api_auth, mock_sf):
        """Full collect includes CanReadField and FieldOf edges."""
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        edge_kinds = {e.kind for e in result.edges}
        assert "CanReadField" in edge_kinds
        assert "FieldOf" in edge_kinds

    @pytest.mark.asyncio
    async def test_collect_query_count_includes_fls(self, api_auth, mock_sf):
        """Full collect query count is 21 (1 org + 20 queries)."""
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        assert result.metadata["queries"] == 21


# =====================================================================
# Feature: ConnectedApp CreatedBy edges
# =====================================================================


class TestConnectedAppCreatedByEdges:
    """Tests for ConnectedApp → User (CreatedBy) edges."""

    def test_connected_app_created_by_edges(self, collector):
        """CreatedBy edges link each app to its creator."""
        data = {
            "records": [
                {"Id": "0H4XX000000APP01", "CreatedById": "005XX000001USER1"},
                {"Id": "0H4XX000000APP02", "CreatedById": "005XX000002USER2"},
            ]
        }
        edges = collector._create_connected_app_created_by_edges(data)
        assert len(edges) == 2
        assert all(e.kind == "CreatedBy" for e in edges)
        assert edges[0].source == "0H4XX000000APP01"
        assert edges[0].target == "005XX000001USER1"
        assert edges[1].source == "0H4XX000000APP02"
        assert edges[1].target == "005XX000002USER2"

    def test_connected_app_created_by_none(self, collector):
        """No edge when CreatedById is None."""
        data = {
            "records": [
                {"Id": "0H4XX000000APP01", "CreatedById": None},
            ]
        }
        edges = collector._create_connected_app_created_by_edges(data)
        assert len(edges) == 0

    def test_connected_app_created_by_missing_field(self, collector):
        """No edge when CreatedById field is absent from record."""
        data = {
            "records": [
                {"Id": "0H4XX000000APP01"},
            ]
        }
        edges = collector._create_connected_app_created_by_edges(data)
        assert len(edges) == 0

    @pytest.mark.asyncio
    async def test_created_by_in_full_collect(self, api_auth, mock_sf):
        """CreatedBy edges appear in full collect results."""
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        created_by_edges = [e for e in result.edges if e.kind == "CreatedBy"]
        assert len(created_by_edges) == 2
        sources = {e.source for e in created_by_edges}
        assert sources == {"0H4XX000000APP01", "0H4XX000000APP02"}


# =====================================================================
# Feature: QueueSobject (CanOwnObjectType) edges
# =====================================================================


class TestAPICollectorQueueSobject:
    """Tests for Queue → SF_Object (CanOwnObjectType) edges."""

    def test_queue_sobject_edges(self, collector):
        """CanOwnObjectType edges from QueueId to SF_Object hashes."""
        data = {
            "records": [
                {"QueueId": "00GXX000001GRP1", "SobjectType": "Lead"},
                {"QueueId": "00GXX000001GRP1", "SobjectType": "Case"},
            ]
        }
        edges = collector._create_queue_sobject_edges(data)
        assert len(edges) == 2
        assert all(e.kind == "CanOwnObjectType" for e in edges)
        assert all(e.source == "00GXX000001GRP1" for e in edges)
        targets = {e.target for e in edges}
        assert generate_hash_id("SF_Object", "Lead") in targets
        assert generate_hash_id("SF_Object", "Case") in targets

    def test_queue_sobject_target_is_object_hash(self, collector):
        """Target IDs are hash-based, not raw SobjectType strings."""
        data = {
            "records": [
                {"QueueId": "00GXX000001GRP1", "SobjectType": "Lead"},
            ]
        }
        edges = collector._create_queue_sobject_edges(data)
        assert len(edges) == 1
        assert edges[0].target != "Lead"
        assert edges[0].target == generate_hash_id("SF_Object", "Lead")

    def test_queue_sobject_empty(self, collector):
        """No QueueSobject records → no edges."""
        data = {"records": []}
        edges = collector._create_queue_sobject_edges(data)
        assert len(edges) == 0

    @pytest.mark.asyncio
    async def test_queue_sobject_in_full_collect(self, api_auth, mock_sf):
        """CanOwnObjectType edges appear in full collect results."""
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        queue_edges = [e for e in result.edges if e.kind == "CanOwnObjectType"]
        assert len(queue_edges) == 2


# =====================================================================
# Feature: Portal Role Detection
# =====================================================================


class TestAPICollectorPortalRoles:
    """Tests for portal_type and is_portal_role on SF_Role nodes."""

    def test_internal_role_string_none(self, collector):
        """SF API returns string 'None' for internal roles — treated as no portal."""
        data = {
            "records": [
                {"Id": "00EXX000000ROLE1", "Name": "CEO", "PortalType": "None"},
            ]
        }
        nodes = collector._create_role_nodes(data)
        assert len(nodes) == 1
        assert nodes[0].properties["is_portal_role"] is False
        assert nodes[0].properties["portal_type"] is None

    def test_internal_role_python_none(self, collector):
        """Python None (e.g. from Aura/supplement) also treated as no portal."""
        data = {
            "records": [
                {"Id": "00EXX000000ROLE1", "Name": "CEO", "PortalType": None},
            ]
        }
        nodes = collector._create_role_nodes(data)
        assert len(nodes) == 1
        assert nodes[0].properties["is_portal_role"] is False
        assert nodes[0].properties["portal_type"] is None

    def test_portal_role_properties(self, collector):
        """Portal roles have is_portal_role=True and portal_type set."""
        data = {
            "records": [
                {
                    "Id": "00EXX000000ROLE3",
                    "Name": "Customer Portal Manager",
                    "PortalType": "CustomerPortal",
                },
            ]
        }
        nodes = collector._create_role_nodes(data)
        assert len(nodes) == 1
        assert nodes[0].properties["is_portal_role"] is True
        assert nodes[0].properties["portal_type"] == "CustomerPortal"

    def test_portal_type_preserved(self, collector):
        """Exact PortalType string is preserved (e.g. 'Partner')."""
        data = {
            "records": [
                {
                    "Id": "00EXX000000ROLE4",
                    "Name": "Partner Portal User",
                    "PortalType": "Partner",
                },
            ]
        }
        nodes = collector._create_role_nodes(data)
        assert nodes[0].properties["portal_type"] == "Partner"

    @pytest.mark.asyncio
    async def test_portal_role_in_full_collect(self, api_auth, mock_sf):
        """Role nodes in full collect have portal properties."""
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        role_nodes = [n for n in result.nodes if "SF_Role" in n.kinds]
        assert len(role_nodes) == 2
        for node in role_nodes:
            assert "portal_type" in node.properties
            assert "is_portal_role" in node.properties
            # All mock roles are internal
            assert node.properties["is_portal_role"] is False


# =====================================================================
# Feature: EntityDefinition enrichment on SF_Object nodes
# =====================================================================


class TestEntityDefinitionEnrichment:
    """Tests for per-object sharing model enrichment via EntityDefinition."""

    @pytest.mark.asyncio
    async def test_object_nodes_have_sharing_model(self, api_auth, mock_sf):
        """SF_Object nodes should have InternalSharingModel from EntityDefinition."""
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        obj_nodes = [n for n in result.nodes if "SF_Object" in n.kinds]
        account_node = next(
            (n for n in obj_nodes if n.properties["name"] == "Account"), None
        )
        assert account_node is not None
        assert account_node.properties["InternalSharingModel"] == "Private"
        assert account_node.properties["ExternalSharingModel"] == "Private"

    @pytest.mark.asyncio
    async def test_object_nodes_have_key_prefix(self, api_auth, mock_sf):
        """SF_Object nodes should have KeyPrefix from EntityDefinition."""
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        obj_nodes = [n for n in result.nodes if "SF_Object" in n.kinds]
        account_node = next(
            (n for n in obj_nodes if n.properties["name"] == "Account"), None
        )
        assert account_node is not None
        assert account_node.properties["KeyPrefix"] == "001"

    @pytest.mark.asyncio
    async def test_unenriched_object_graceful(self, api_auth, mock_sf):
        """SF_Object for a type NOT in EntityDefinition still has name."""
        collector = APICollector(api_auth, verbose=False)
        # Add a custom SobjectType not in EntityDefinition mock
        original_route = mock_sf.query_all.side_effect

        def custom_route(query):
            q = query.upper()
            if "FROM OBJECTPERMISSIONS" in q:
                result = original_route(query)
                result["records"].append(
                    {
                        "SobjectType": "CustomMissing__c",
                        "PermissionsCreate": True,
                        "PermissionsRead": True,
                        "PermissionsEdit": False,
                        "PermissionsDelete": False,
                        "PermissionsViewAllRecords": False,
                        "PermissionsModifyAllRecords": False,
                        "PermissionsViewAllFields": False,
                        "ParentId": "0PSXX000000PS02",
                        "Parent": {
                            "IsOwnedByProfile": False,
                            "ProfileId": None,
                            "Name": "TestPS",
                            "Label": "Test Permission Set",
                        },
                    }
                )
                return result
            return original_route(query)

        mock_sf.query_all.side_effect = custom_route
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        obj_nodes = [n for n in result.nodes if "SF_Object" in n.kinds]
        missing_node = next(
            (n for n in obj_nodes if n.properties["name"] == "CustomMissing__c"),
            None,
        )
        assert missing_node is not None
        assert missing_node.properties["name"] == "CustomMissing__c"
        assert "InternalSharingModel" not in missing_node.properties

    @pytest.mark.asyncio
    async def test_skip_entity_definitions(self, api_auth, mock_sf):
        """With skip_entity_definitions, SF_Object nodes have only name."""
        collector = APICollector(
            api_auth, verbose=False, skip_entity_definitions=True
        )
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        obj_nodes = [n for n in result.nodes if "SF_Object" in n.kinds]
        for node in obj_nodes:
            assert "InternalSharingModel" not in node.properties
            assert "KeyPrefix" not in node.properties

        # Query count should be 1 less than full (20 instead of 21)
        assert result.metadata["queries"] == 20

    def test_entity_def_lookup_build(self, collector):
        """Unit test for the lookup dict construction."""
        data = {
            "records": [
                {
                    "QualifiedApiName": "Account",
                    "Label": "Account",
                    "KeyPrefix": "001",
                    "InternalSharingModel": "Private",
                    "ExternalSharingModel": "Private",
                },
                {
                    "QualifiedApiName": "Lead",
                    "Label": "Lead",
                    "KeyPrefix": "00Q",
                    "InternalSharingModel": "ReadWriteTransfer",
                    "ExternalSharingModel": "Private",
                },
                {
                    "QualifiedApiName": None,
                    "Label": "Broken",
                },
            ]
        }
        lookup = collector._build_entity_definition_lookup(data)
        assert "Account" in lookup
        assert "Lead" in lookup
        assert len(lookup) == 2  # None key skipped
        assert lookup["Account"]["InternalSharingModel"] == "Private"
        assert lookup["Account"]["KeyPrefix"] == "001"
        assert "QualifiedApiName" not in lookup["Account"]
        assert lookup["Lead"]["InternalSharingModel"] == "ReadWriteTransfer"

    @pytest.mark.asyncio
    async def test_queue_sobject_nodes_enriched(self, api_auth, mock_sf):
        """QueueSobject-sourced SF_Object nodes are also enriched."""
        collector = APICollector(api_auth, verbose=False)
        mock_share = MagicMock()
        mock_share.collect.return_value = ([], [])
        mock_share.query_count = 0

        with (
            patch.object(collector, "_connect", return_value=mock_sf),
            patch(
                "forcehound.collectors.api_collector.ShareObjectCollector",
                return_value=mock_share,
            ),
        ):
            result = await collector.collect()

        obj_nodes = [n for n in result.nodes if "SF_Object" in n.kinds]
        # Lead comes from both ObjectPermissions and QueueSobject;
        # Case comes only from QueueSobject
        case_node = next(
            (n for n in obj_nodes if n.properties["name"] == "Case"), None
        )
        assert case_node is not None
        assert case_node.properties["KeyPrefix"] == "500"
        assert case_node.properties["InternalSharingModel"] == "ReadWriteTransfer"
