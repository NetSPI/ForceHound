"""Tests for forcehound.collectors.aura_collector."""

import pytest
from unittest.mock import AsyncMock, patch

from forcehound.collectors.aura_collector import (
    AuraCollector,
    is_namespaced_object,
    parse_namespaced_object,
    parse_user_response,
    parse_group_response,
    parse_group_member_response,
    parse_permission_set_response,
    parse_role_response,
    build_user_field_paths,
    build_group_field_paths,
    build_group_member_field_paths,
    build_permission_set_field_paths,
    build_role_field_paths,
    _filter_none,
    _get_value,
    _get_nested_value,
)
from forcehound.constants import CAPABILITY_FIELDS
from forcehound.models.auth import AuthConfig
from forcehound.models.base import GraphNode


# =====================================================================
# Helper function tests
# =====================================================================


class TestFilterNone:
    def test_removes_none_values(self):
        assert _filter_none({"a": 1, "b": None, "c": "x"}) == {"a": 1, "c": "x"}

    def test_empty_dict(self):
        assert _filter_none({}) == {}

    def test_all_none(self):
        assert _filter_none({"a": None, "b": None}) == {}

    def test_no_none(self):
        assert _filter_none({"a": 1, "b": 2}) == {"a": 1, "b": 2}

    def test_preserves_false_and_zero(self):
        result = _filter_none({"a": False, "b": 0, "c": ""})
        assert result == {"a": False, "b": 0, "c": ""}


class TestGetValue:
    def test_extracts_value(self):
        fields = {"Name": {"value": "Test"}}
        assert _get_value(fields, "Name") == "Test"

    def test_missing_field(self):
        assert _get_value({}, "Name") is None

    def test_none_value(self):
        fields = {"Name": {"value": None}}
        assert _get_value(fields, "Name") is None


class TestGetNestedValue:
    def test_extracts_nested(self):
        fields = {"Profile": {"value": {"fields": {"Name": {"value": "Admin"}}}}}
        assert _get_nested_value(fields, "Profile", "Name") == "Admin"

    def test_parent_missing(self):
        assert _get_nested_value({}, "Profile", "Name") is None

    def test_parent_value_none(self):
        fields = {"Profile": {"value": None}}
        assert _get_nested_value(fields, "Profile", "Name") is None

    def test_child_missing(self):
        fields = {"Profile": {"value": {"fields": {}}}}
        assert _get_nested_value(fields, "Profile", "Name") is None


# =====================================================================
# Namespaced object tests
# =====================================================================


class TestIsNamespacedObject:
    def test_custom_object(self):
        assert is_namespaced_object("dfsle__BulkStatus__c") is True

    def test_custom_metadata(self):
        assert is_namespaced_object("ns__Config__mdt") is True

    def test_history_tracking(self):
        assert is_namespaced_object("ns__MyObject__History") is True

    def test_excluded_change_event(self):
        assert is_namespaced_object("ns__MyObject__ChangeEvent") is False

    def test_excluded_share(self):
        assert is_namespaced_object("ns__MyObject__Share") is False

    def test_no_namespace(self):
        assert is_namespaced_object("Account") is False

    def test_standard_custom_no_namespace(self):
        assert is_namespaced_object("MyObj__c") is False

    def test_two_parts_only(self):
        assert is_namespaced_object("CustomObj__c") is False

    def test_three_parts(self):
        assert is_namespaced_object("ns__Obj__c") is True

    def test_four_parts(self):
        assert is_namespaced_object("ns__My__Obj__c") is True


class TestParseNamespacedObject:
    def test_custom_object(self):
        result = parse_namespaced_object("dfsle__BulkStatus__c")
        assert result["name"] == "dfsle__BulkStatus__c"
        assert result["namespace"] == "dfsle"
        assert result["base_name"] == "BulkStatus"
        assert result["object_type"] == "CustomObject"

    def test_custom_metadata(self):
        result = parse_namespaced_object("ns__Config__mdt")
        assert result["namespace"] == "ns"
        assert result["base_name"] == "Config"
        assert result["object_type"] == "CustomMetadata"

    def test_history_tracking(self):
        result = parse_namespaced_object("ns__MyObject__History")
        assert result["object_type"] == "HistoryTracking"

    def test_complex_name(self):
        result = parse_namespaced_object("ns__My__Complex__Obj__c")
        assert result["namespace"] == "ns"
        assert result["base_name"] == "My__Complex__Obj"
        assert result["object_type"] == "CustomObject"


# =====================================================================
# Response parsing tests
# =====================================================================


class TestParseUserResponse:
    def test_basic_parse(self, aura_user_response):
        rv = aura_user_response["actions"][0]["returnValue"]
        result = parse_user_response(rv)
        assert result is not None
        assert result["user"]["Id"] == "005XX000001ATEST"
        assert result["user"]["Name"] == "Test User"
        assert result["user"]["Email"] == "test@example.com"

    def test_profile_data(self, aura_user_response):
        rv = aura_user_response["actions"][0]["returnValue"]
        result = parse_user_response(rv)
        assert result["profile"]["Id"] == "00eXX000000TEST"
        assert result["profile"]["Name"] == "System Administrator"

    def test_profile_permissions(self, aura_user_response):
        rv = aura_user_response["actions"][0]["returnValue"]
        result = parse_user_response(rv)
        perms = result["profile_permissions"]
        assert perms["PermissionsModifyAllData"] is True
        assert perms["PermissionsManageRoles"] is False

    def test_all_15_permissions_present(self, aura_user_response):
        rv = aura_user_response["actions"][0]["returnValue"]
        result = parse_user_response(rv)
        for perm in CAPABILITY_FIELDS:
            assert perm in result["profile_permissions"]

    def test_role_data(self, aura_user_response):
        rv = aura_user_response["actions"][0]["returnValue"]
        result = parse_user_response(rv)
        assert result["role"]["Id"] == "00EXX000000TEST"
        assert result["role"]["Name"] == "CEO"

    def test_manager_data(self, aura_user_response):
        rv = aura_user_response["actions"][0]["returnValue"]
        result = parse_user_response(rv)
        assert result["manager"]["Id"] == "005XX000002MGRID"

    def test_none_input(self):
        assert parse_user_response(None) is None

    def test_empty_fields(self):
        result = parse_user_response({"fields": {}})
        assert result is not None
        assert result["user"]["Id"] is None


class TestParseGroupResponse:
    def test_basic_parse(self, aura_group_response):
        rv = aura_group_response["actions"][0]["returnValue"]
        result = parse_group_response(rv)
        assert result["Id"] == "00GXX000001GTEST"
        assert result["Name"] == "TestGroup"
        assert result["Type"] == "Regular"

    def test_none_input(self):
        assert parse_group_response(None) is None


class TestParseGroupMemberResponse:
    def test_basic_parse(self, aura_group_member_response):
        rv = aura_group_member_response["actions"][0]["returnValue"]
        result = parse_group_member_response(rv)
        assert result["GroupId"] == "00GXX000001GTEST"
        assert result["UserOrGroupId"] == "005XX000001ATEST"
        assert result["GroupName"] == "TestGroup"

    def test_none_input(self):
        assert parse_group_member_response(None) is None


# =====================================================================
# Field path builder tests
# =====================================================================


class TestBuildUserFieldPaths:
    def test_required_has_id(self):
        req, opt = build_user_field_paths()
        assert req == ["User.Id"]

    def test_optional_has_profile_permissions(self):
        req, opt = build_user_field_paths()
        for perm in CAPABILITY_FIELDS:
            assert f"User.Profile.{perm}" in opt

    def test_optional_has_role_fields(self):
        req, opt = build_user_field_paths()
        assert "User.UserRole.Id" in opt
        assert "User.UserRole.Name" in opt

    def test_optional_has_manager_fields(self):
        req, opt = build_user_field_paths()
        assert "User.Manager.Id" in opt
        assert "User.Manager.Name" in opt


class TestBuildGroupFieldPaths:
    def test_required_has_id(self):
        req, opt = build_group_field_paths()
        assert req == ["Group.Id"]

    def test_optional_has_type(self):
        req, opt = build_group_field_paths()
        assert "Group.Type" in opt


class TestBuildGroupMemberFieldPaths:
    def test_required_has_id(self):
        req, opt = build_group_member_field_paths()
        assert req == ["GroupMember.Id"]

    def test_optional_has_group_fields(self):
        req, opt = build_group_member_field_paths()
        assert "GroupMember.GroupId" in opt
        assert "GroupMember.UserOrGroupId" in opt


# =====================================================================
# AuraCollector node builder tests
# =====================================================================


class TestAuraCollectorNodeBuilders:
    @pytest.fixture
    def collector(self):
        auth = AuthConfig(
            instance_url="https://test.lightning.force.com",
            session_id="00DXX000000XXXXX!AQEAQTEST",
            aura_context='{"mode":"PRODDEBUG"}',
            aura_token="eyJtoken",
        )
        return AuraCollector(auth, verbose=False)

    def test_build_user_nodes(self, collector, parsed_user_data):
        nodes = collector._build_user_nodes([parsed_user_data])
        assert len(nodes) == 1
        assert nodes[0].id == "005XX000001ATEST"
        assert "SF_User" in nodes[0].kinds
        assert nodes[0].properties["name"] == "Test User"

    def test_build_user_nodes_skips_none_id(self, collector):
        data = {
            "user": {
                "Id": None,
                "Name": "X",
                "Email": None,
                "Username": None,
                "UserType": "Standard",
                "IsActive": True,
                "ProfileId": None,
                "UserRoleId": None,
                "ManagerId": None,
                "LastLoginDate": None,
                "CreatedDate": None,
                "CreatedById": None,
            },
            "profile": {"Id": None, "Name": None, "UserType": None},
            "profile_permissions": {},
            "role": {"Id": None, "Name": None, "ParentRoleId": None},
            "manager": {"Id": None, "Name": None},
        }
        assert collector._build_user_nodes([data]) == []

    def test_build_profile_nodes_deduplicates(self, collector, parsed_user_data):
        nodes = collector._build_profile_nodes([parsed_user_data, parsed_user_data])
        assert len(nodes) == 1

    def test_build_role_nodes(self, collector, parsed_user_data):
        nodes = collector._build_role_nodes([parsed_user_data])
        assert len(nodes) == 1
        assert nodes[0].id == "00EXX000000TEST"
        assert "SF_Role" in nodes[0].kinds

    def test_build_role_nodes_deduplicates(self, collector, parsed_user_data):
        nodes = collector._build_role_nodes([parsed_user_data, parsed_user_data])
        assert len(nodes) == 1

    def test_build_group_nodes_regular(self, collector, parsed_group_data):
        nodes = collector._build_group_nodes([parsed_group_data])
        assert len(nodes) == 1
        assert "SF_PublicGroup" in nodes[0].kinds

    def test_build_group_nodes_role_type(self, collector):
        group = {
            "Id": "00GXX000001",
            "Name": "RoleGroup",
            "DeveloperName": "RG",
            "Type": "Role",
            "RelatedId": None,
            "DoesIncludeBosses": False,
            "DoesSendEmailToMembers": False,
        }
        nodes = collector._build_group_nodes([group])
        assert "SF_Group" in nodes[0].kinds
        assert "SF_PublicGroup" not in nodes[0].kinds

    def test_build_organization_node(self, collector):
        node = collector._build_organization_node("00DXX0000008TEST")
        assert node.id == "00DXX0000008TEST"
        assert "SF_Organization" in node.kinds

    def test_build_namespaced_object_nodes(self, collector):
        names = ["ns__Obj__c", "ns__Obj2__c"]
        counts = {"ns__Obj__c": 10, "ns__Obj2__c": 0}
        nodes = collector._build_namespaced_object_nodes(names, counts)
        assert len(nodes) == 1  # Only ns__Obj__c has count > 0
        assert nodes[0].properties["accessible_record_count"] == 10


# =====================================================================
# AuraCollector edge builder tests
# =====================================================================


class TestAuraCollectorEdgeBuilders:
    @pytest.fixture
    def collector(self):
        auth = AuthConfig(
            instance_url="https://test.lightning.force.com",
            session_id="00DXX000000XXXXX!AQEAQTEST",
            aura_context='{"mode":"PRODDEBUG"}',
            aura_token="eyJtoken",
        )
        return AuraCollector(auth, verbose=False)

    def test_has_profile_edges(self, collector, parsed_user_data):
        edges = collector._build_has_profile_edges([parsed_user_data])
        assert len(edges) == 1
        assert edges[0].source == "005XX000001ATEST"
        assert edges[0].target == "00eXX000000TEST"
        assert edges[0].kind == "HasProfile"

    def test_has_role_edges(self, collector, parsed_user_data):
        edges = collector._build_has_role_edges([parsed_user_data])
        assert len(edges) == 1
        assert edges[0].kind == "HasRole"

    def test_role_hierarchy_edges(self, collector, parsed_user_data):
        edges = collector._build_role_hierarchy_edges([parsed_user_data])
        # ParentRoleId is None for CEO, so no edges
        assert len(edges) == 0

    def test_role_hierarchy_with_parent(self, collector):
        data = {
            "user": {"Id": "005A", "ManagerId": None},
            "profile": {"Id": "00eA"},
            "profile_permissions": {},
            "role": {"Id": "00EA", "Name": "VP", "ParentRoleId": "00EB"},
            "manager": {"Id": None, "Name": None},
        }
        edges = collector._build_role_hierarchy_edges([data])
        assert len(edges) == 1
        assert edges[0].source == "00EA"
        assert edges[0].target == "00EB"
        assert edges[0].kind == "ReportsTo"

    def test_role_hierarchy_deduplicates(self, collector):
        data = {
            "user": {"Id": "005A", "ManagerId": None},
            "profile": {"Id": "00eA"},
            "profile_permissions": {},
            "role": {"Id": "00EA", "Name": "VP", "ParentRoleId": "00EB"},
            "manager": {"Id": None, "Name": None},
        }
        edges = collector._build_role_hierarchy_edges([data, data])
        assert len(edges) == 1

    def test_manager_edges(self, collector, parsed_user_data):
        edges = collector._build_manager_edges([parsed_user_data])
        assert len(edges) == 1
        assert edges[0].source == "005XX000001ATEST"
        assert edges[0].target == "005XX000002MGRID"
        assert edges[0].kind == "ManagedBy"

    def test_capability_edges(self, collector, parsed_user_data):
        edges = collector._build_capability_edges([parsed_user_data], "00DXX")
        # Profile has 8 True permissions
        true_count = sum(
            1 for v in parsed_user_data["profile_permissions"].values() if v is True
        )
        assert len(edges) == true_count
        assert all(e.target == "00DXX" for e in edges)

    def test_capability_edges_deduplicates_per_profile(
        self, collector, parsed_user_data
    ):
        edges = collector._build_capability_edges(
            [parsed_user_data, parsed_user_data], "00DXX"
        )
        true_count = sum(
            1 for v in parsed_user_data["profile_permissions"].values() if v is True
        )
        assert len(edges) == true_count

    def test_member_of_edges(self, collector, parsed_group_member_data):
        user_ids = {"005XX000001ATEST"}
        edges = collector._build_member_of_edges([parsed_group_member_data], user_ids)
        assert len(edges) == 1
        assert edges[0].kind == "MemberOf"

    def test_member_of_skips_group_members(self, collector):
        member = {"GroupId": "00GA", "UserOrGroupId": "00GB"}
        edges = collector._build_member_of_edges([member], set())
        assert len(edges) == 0  # 00GB starts with 00G, not 005

    def test_group_contains_edges(self, collector):
        member = {"GroupId": "00GA", "UserOrGroupId": "00GB"}
        edges = collector._build_group_contains_edges([member], {"00GA", "00GB"})
        assert len(edges) == 1
        assert edges[0].kind == "Contains"

    def test_can_access_edges(self, collector):
        ns_nodes = [
            GraphNode(id="ns__Obj__c", kinds=["SF_NamespacedObject"], properties={}),
        ]
        edges = collector._build_can_access_edges(ns_nodes, "005XX000001ATEST")
        assert len(edges) == 1
        assert edges[0].kind == "CanAccess"

    def test_can_access_no_user(self, collector):
        ns_nodes = [
            GraphNode(id="ns__Obj__c", kinds=["SF_NamespacedObject"], properties={}),
        ]
        edges = collector._build_can_access_edges(ns_nodes, "")
        assert len(edges) == 0


# =====================================================================
# AuraCollector collect flow tests
# =====================================================================


class TestAuraCollectorCollectFlow:
    @pytest.mark.asyncio
    async def test_collect_basic_flow(
        self, aura_user_response, aura_group_response, aura_group_member_response
    ):
        auth = AuthConfig(
            instance_url="https://test.lightning.force.com",
            session_id="00DXX000000XXXXX!AQEAQTEST",
            aura_context='{"mode":"PRODDEBUG"}',
            aura_token="eyJtoken",
        )
        collector = AuraCollector(auth, verbose=False, max_workers=5)

        mock_client = AsyncMock()
        mock_client.org_id = "00DXX000000XXXXX"

        # Mock get_items_graphql to return IDs
        async def mock_get_items_graphql(obj_name, **kwargs):
            if obj_name == "User":
                return ["005XX000001ATEST"]
            elif obj_name == "PermissionSet":
                return ["0PSXX000000PS001"]
            elif obj_name == "UserRole":
                return ["00EXX000000RVPNA"]
            elif obj_name == "Group":
                return ["00GXX000001GTEST"]
            elif obj_name == "GroupMember":
                return ["011XX000001MTEST"]
            return []

        mock_client.get_items_graphql = mock_get_items_graphql

        # Mock get_record_with_fields
        async def mock_get_record(record_id, fields, optional_fields=None):
            if record_id.startswith("005"):
                return aura_user_response
            elif record_id.startswith("0PS"):
                # Regular PermissionSet
                return {
                    "actions": [
                        {
                            "returnValue": {
                                "fields": {
                                    "Id": {"value": record_id},
                                    "Name": {"value": "TestPS"},
                                    "Label": {"value": "Test PS"},
                                    "IsOwnedByProfile": {"value": False},
                                    "ProfileId": {"value": None},
                                    "PermissionSetGroupId": {"value": None},
                                    "IsCustom": {"value": True},
                                    "Type": {"value": "Regular"},
                                    "PermissionsApiEnabled": {"value": True},
                                    "Profile": {"value": None},
                                }
                            }
                        }
                    ]
                }
            elif record_id.startswith("00E"):
                # UserRole
                return {
                    "actions": [
                        {
                            "returnValue": {
                                "fields": {
                                    "Id": {"value": record_id},
                                    "Name": {"value": "VP, North American Sales"},
                                    "DeveloperName": {"value": "VPNorthAmericanSales"},
                                    "ParentRoleId": {"value": "00EXX000000RSVPM"},
                                }
                            }
                        }
                    ]
                }
            elif record_id.startswith("00G"):
                return aura_group_response
            elif record_id.startswith("011"):
                return aura_group_member_response
            return {"actions": [{"returnValue": None}]}

        mock_client.get_record_with_fields = mock_get_record

        # Mock get_config_data
        async def mock_config():
            return ["Account", "Contact", "ns__TestObj__c"]

        mock_client.get_config_data = mock_config

        # Mock get_items for namespaced object counts
        async def mock_get_items(obj_name):
            if obj_name == "ns__TestObj__c":
                return ["001A", "001B"]
            return []

        mock_client.get_items = mock_get_items
        mock_client.close = AsyncMock()

        # Patch AuraClient constructor
        with patch(
            "forcehound.collectors.aura_collector.AuraClient", return_value=mock_client
        ):
            result = await collector.collect()

        assert result.collector_type == "aura"
        assert result.org_id == "00DXX000000XXXXX"
        assert len(result.nodes) > 0
        assert len(result.edges) > 0

        # Verify node types
        node_kinds = set()
        for node in result.nodes:
            node_kinds.update(node.kinds)
        assert "SF_User" in node_kinds
        assert "SF_Profile" in node_kinds
        assert "SF_Role" in node_kinds
        assert "SF_PermissionSet" in node_kinds
        assert "SF_Organization" in node_kinds

    @pytest.mark.asyncio
    async def test_collect_validates_auth(self):
        auth = AuthConfig(instance_url="https://test.lightning.force.com")
        collector = AuraCollector(auth)
        with pytest.raises(ValueError):
            await collector.collect()

    @pytest.mark.asyncio
    async def test_get_ids_graphql_fallback(self):
        auth = AuthConfig(
            instance_url="https://test.lightning.force.com",
            session_id="00DXX!TOKEN",
            aura_context='{"mode":"PRODDEBUG"}',
            aura_token="eyJtoken",
        )
        collector = AuraCollector(auth, verbose=False)

        mock_client = AsyncMock()
        mock_client.get_items_graphql.side_effect = Exception("GraphQL not supported")
        mock_client.get_items.return_value = ["005A", "005B"]

        ids = await collector._get_ids(mock_client, "User")
        assert ids == ["005A", "005B"]

    @pytest.mark.asyncio
    async def test_get_ids_both_fail(self):
        auth = AuthConfig(
            instance_url="https://test.lightning.force.com",
            session_id="00DXX!TOKEN",
            aura_context='{"mode":"PRODDEBUG"}',
            aura_token="eyJtoken",
        )
        collector = AuraCollector(auth, verbose=False)

        mock_client = AsyncMock()
        mock_client.get_items_graphql.side_effect = Exception("fail")
        mock_client.get_items.side_effect = Exception("fail too")

        ids = await collector._get_ids(mock_client, "User")
        assert ids == []


# =====================================================================
# PermissionSet enumeration tests
# =====================================================================


class TestBuildPermissionSetFieldPaths:
    def test_required_has_id(self):
        required, _ = build_permission_set_field_paths()
        assert required == ["PermissionSet.Id"]

    def test_optional_has_metadata_fields(self):
        _, optional = build_permission_set_field_paths()
        assert "PermissionSet.Name" in optional
        assert "PermissionSet.Label" in optional
        assert "PermissionSet.IsOwnedByProfile" in optional
        assert "PermissionSet.ProfileId" in optional
        assert "PermissionSet.PermissionSetGroupId" in optional
        assert "PermissionSet.IsCustom" in optional
        assert "PermissionSet.Type" in optional

    def test_optional_has_all_capability_fields(self):
        _, optional = build_permission_set_field_paths()
        for cap in CAPABILITY_FIELDS:
            assert f"PermissionSet.{cap}" in optional

    def test_optional_has_profile_relationship(self):
        _, optional = build_permission_set_field_paths()
        assert "PermissionSet.Profile.Name" in optional
        assert "PermissionSet.Profile.UserType" in optional


class TestParsePermissionSetResponse:
    def test_parses_regular_ps(self, aura_permission_set_response):
        rv = aura_permission_set_response["actions"][0]["returnValue"]
        result = parse_permission_set_response(rv)
        assert result is not None
        assert result["permission_set"]["Id"] == "0PSXX000000PS001"
        assert result["permission_set"]["Name"] == "TestPermSet"
        assert result["permission_set"]["IsOwnedByProfile"] is False
        assert result["capabilities"]["PermissionsViewAllData"] is True
        assert result["capabilities"]["PermissionsApiEnabled"] is True
        assert result["capabilities"]["PermissionsModifyAllData"] is False
        assert result["profile"]["Name"] is None

    def test_parses_profile_shadow_ps(self, aura_profile_shadow_ps_response):
        rv = aura_profile_shadow_ps_response["actions"][0]["returnValue"]
        result = parse_permission_set_response(rv)
        assert result is not None
        assert result["permission_set"]["IsOwnedByProfile"] is True
        assert result["permission_set"]["ProfileId"] == "00eXX000000PROF2"
        assert result["profile"]["Name"] == "Standard User"
        assert result["profile"]["UserType"] == "Standard"

    def test_returns_none_for_none(self):
        assert parse_permission_set_response(None) is None

    def test_handles_empty_fields(self):
        result = parse_permission_set_response({"fields": {}})
        assert result is not None
        assert result["permission_set"]["Id"] is None


class TestAuraCollectorPermissionSetEnumeration:
    @pytest.fixture
    def collector(self):
        auth = AuthConfig(
            instance_url="https://test.lightning.force.com",
            session_id="00DXX000000XXXXX!AQEAQTEST",
            aura_context='{"mode":"PRODDEBUG"}',
            aura_token="eyJtoken",
        )
        return AuraCollector(auth, verbose=False)

    # -- Node builder tests --

    def test_build_permission_set_nodes_regular(
        self, collector, parsed_permission_set_data
    ):
        nodes = collector._build_permission_set_nodes([parsed_permission_set_data])
        assert len(nodes) == 1
        assert nodes[0].id == "0PSXX000000PS001"
        assert "SF_PermissionSet" in nodes[0].kinds
        assert nodes[0].properties["name"] == "TestPermSet"
        assert nodes[0].properties["label"] == "Test Permission Set"
        assert nodes[0].properties["is_custom"] is True

    def test_build_permission_set_nodes_skips_profile_shadow(
        self, collector, parsed_profile_shadow_ps_data
    ):
        nodes = collector._build_permission_set_nodes([parsed_profile_shadow_ps_data])
        assert len(nodes) == 0

    def test_build_permission_set_nodes_skips_psg_shadow(self, collector):
        psg_shadow = {
            "permission_set": {
                "Id": "0PSXX000000PSGSH",
                "Name": "PSGShadow",
                "Label": "PSG Shadow",
                "IsOwnedByProfile": False,
                "ProfileId": None,
                "PermissionSetGroupId": "0PGXX000000PSG01",
                "IsCustom": False,
                "Type": "Group",
            },
            "capabilities": {cap: False for cap in CAPABILITY_FIELDS},
            "profile": {"Name": None, "UserType": None},
        }
        nodes = collector._build_permission_set_nodes([psg_shadow])
        assert len(nodes) == 0

    def test_build_permission_set_nodes_skips_type_group(self, collector):
        """PSG shadow with Type='Group' but no PermissionSetGroupId."""
        psg_shadow = {
            "permission_set": {
                "Id": "0PSXX000000PSGSH",
                "Name": "PSGShadow",
                "Label": "PSG Shadow",
                "IsOwnedByProfile": False,
                "ProfileId": None,
                "PermissionSetGroupId": None,
                "IsCustom": False,
                "Type": "Group",
            },
            "capabilities": {cap: False for cap in CAPABILITY_FIELDS},
            "profile": {"Name": None, "UserType": None},
        }
        nodes = collector._build_permission_set_nodes([psg_shadow])
        assert len(nodes) == 0

    def test_build_permission_set_nodes_empty(self, collector):
        nodes = collector._build_permission_set_nodes([])
        assert nodes == []

    # -- Profile node builder with PS data --

    def test_build_profile_nodes_adds_from_ps(
        self, collector, parsed_profile_shadow_ps_data
    ):
        """Profile shadow PS creates a new Profile node."""
        nodes = collector._build_profile_nodes([], [parsed_profile_shadow_ps_data])
        assert len(nodes) == 1
        assert nodes[0].id == "00eXX000000PROF2"
        assert "SF_Profile" in nodes[0].kinds
        assert nodes[0].properties["name"] == "Standard User"
        assert nodes[0].properties["user_type"] == "Standard"

    def test_build_profile_nodes_dedup_user_traversal_and_ps(
        self, collector, parsed_user_data
    ):
        """Profile already found via User traversal is not duplicated by PS enum."""
        # parsed_user_data has profile ID "00eXX000000TEST"
        ps_shadow_same = {
            "permission_set": {
                "Id": "0PSXX000000SHADX",
                "Name": "Shadow",
                "Label": "Shadow",
                "IsOwnedByProfile": True,
                "ProfileId": "00eXX000000TEST",  # Same as user's profile
                "PermissionSetGroupId": None,
                "IsCustom": False,
                "Type": "Profile",
            },
            "capabilities": {cap: False for cap in CAPABILITY_FIELDS},
            "profile": {"Name": "System Administrator", "UserType": "Standard"},
        }
        nodes = collector._build_profile_nodes([parsed_user_data], [ps_shadow_same])
        # Should still be just 1 profile — deduplicated
        assert len(nodes) == 1
        assert nodes[0].id == "00eXX000000TEST"

    def test_build_profile_nodes_merges_both_sources(
        self, collector, parsed_user_data, parsed_profile_shadow_ps_data
    ):
        """User traversal profile + PS-discovered profile both appear."""
        nodes = collector._build_profile_nodes(
            [parsed_user_data], [parsed_profile_shadow_ps_data]
        )
        assert len(nodes) == 2
        ids = {n.id for n in nodes}
        assert "00eXX000000TEST" in ids  # From user traversal
        assert "00eXX000000PROF2" in ids  # From PS enumeration

    def test_build_profile_nodes_no_ps(self, collector, parsed_user_data):
        """Without PS data, behavior is unchanged from before."""
        nodes = collector._build_profile_nodes([parsed_user_data])
        assert len(nodes) == 1
        assert nodes[0].id == "00eXX000000TEST"

    # -- Capability edge builder with PS data --

    def test_capability_edges_from_regular_ps(
        self, collector, parsed_permission_set_data
    ):
        """Regular PS creates PS → Org capability edges."""
        edges = collector._build_capability_edges(
            [], "00DXX", [parsed_permission_set_data]
        )
        # parsed_permission_set_data has 2 True caps: ViewAllData, ApiEnabled
        assert len(edges) == 2
        kinds = {e.kind for e in edges}
        assert "ViewAllData" in kinds
        assert "ApiEnabled" in kinds
        assert all(e.source == "0PSXX000000PS001" for e in edges)
        assert all(e.target == "00DXX" for e in edges)

    def test_capability_edges_from_profile_shadow(
        self, collector, parsed_profile_shadow_ps_data
    ):
        """Profile shadow PS creates Profile → Org capability edges."""
        edges = collector._build_capability_edges(
            [], "00DXX", [parsed_profile_shadow_ps_data]
        )
        # Shadow has 2 True caps: ApiEnabled, ViewSetup
        assert len(edges) == 2
        kinds = {e.kind for e in edges}
        assert "ApiEnabled" in kinds
        assert "ViewSetup" in kinds
        # Source should be the Profile ID, not the shadow PS ID
        assert all(e.source == "00eXX000000PROF2" for e in edges)

    def test_capability_edges_psg_shadow_skipped(self, collector):
        """PSG shadow PSes do not create capability edges."""
        psg_shadow = {
            "permission_set": {
                "Id": "0PSXX000000PSGSH",
                "Name": "PSGShadow",
                "IsOwnedByProfile": False,
                "ProfileId": None,
                "PermissionSetGroupId": "0PGXX000000PSG01",
                "Type": "Group",
            },
            "capabilities": {"PermissionsApiEnabled": True},
            "profile": {"Name": None, "UserType": None},
        }
        edges = collector._build_capability_edges([], "00DXX", [psg_shadow])
        assert len(edges) == 0

    def test_capability_edges_dedup_user_and_ps(self, collector, parsed_user_data):
        """Profile capabilities from User traversal and PS enum are deduplicated."""
        # User traversal already creates edges for profile 00eXX000000TEST
        ps_shadow_same = {
            "permission_set": {
                "Id": "0PSXX000000SHADX",
                "Name": "Shadow",
                "IsOwnedByProfile": True,
                "ProfileId": "00eXX000000TEST",
                "PermissionSetGroupId": None,
                "Type": "Profile",
            },
            "capabilities": {
                "PermissionsModifyAllData": True,  # Same as user traversal
                "PermissionsApiEnabled": True,
            },
            "profile": {"Name": "System Administrator", "UserType": "Standard"},
        }
        edges_without_ps = collector._build_capability_edges(
            [parsed_user_data], "00DXX"
        )
        edges_with_ps = collector._build_capability_edges(
            [parsed_user_data], "00DXX", [ps_shadow_same]
        )
        # Should be the same count — PS data doesn't add new edges for same profile
        assert len(edges_with_ps) == len(edges_without_ps)

    # -- Full collect flow with PS --

    @pytest.mark.asyncio
    async def test_collect_includes_permission_sets(
        self,
        aura_user_response,
        aura_group_response,
        aura_group_member_response,
        aura_profile_shadow_ps_response,
    ):
        auth = AuthConfig(
            instance_url="https://test.lightning.force.com",
            session_id="00DXX000000XXXXX!AQEAQTEST",
            aura_context='{"mode":"PRODDEBUG"}',
            aura_token="eyJtoken",
        )
        collector = AuraCollector(auth, verbose=False, max_workers=5)

        mock_client = AsyncMock()
        mock_client.org_id = "00DXX000000XXXXX"
        mock_client.request_count = 0

        async def mock_get_items_graphql(obj_name, **kwargs):
            if obj_name == "User":
                return ["005XX000001ATEST"]
            elif obj_name == "PermissionSet":
                return ["0PSXX000000PS001", "0PSXX000000SHAD1"]
            elif obj_name == "UserRole":
                return ["00EXX000000RCEO1"]
            elif obj_name == "Group":
                return []
            elif obj_name == "GroupMember":
                return []
            return []

        mock_client.get_items_graphql = mock_get_items_graphql

        async def mock_get_record(record_id, fields, optional_fields=None):
            if record_id == "005XX000001ATEST":
                return aura_user_response
            elif record_id == "0PSXX000000PS001":
                return {
                    "actions": [
                        {
                            "returnValue": {
                                "fields": {
                                    "Id": {"value": "0PSXX000000PS001"},
                                    "Name": {"value": "CustomPS"},
                                    "Label": {"value": "Custom Permission Set"},
                                    "IsOwnedByProfile": {"value": False},
                                    "ProfileId": {"value": None},
                                    "PermissionSetGroupId": {"value": None},
                                    "IsCustom": {"value": True},
                                    "Type": {"value": "Regular"},
                                    "PermissionsApiEnabled": {"value": True},
                                    "Profile": {"value": None},
                                }
                            }
                        }
                    ]
                }
            elif record_id == "0PSXX000000SHAD1":
                return aura_profile_shadow_ps_response
            elif record_id.startswith("00E"):
                # UserRole — root role (no parent)
                return {
                    "actions": [
                        {
                            "returnValue": {
                                "fields": {
                                    "Id": {"value": record_id},
                                    "Name": {"value": "CEO"},
                                    "DeveloperName": {"value": "CEO"},
                                    "ParentRoleId": {"value": None},
                                }
                            }
                        }
                    ]
                }
            return {"actions": [{"returnValue": None}]}

        mock_client.get_record_with_fields = mock_get_record

        async def mock_config():
            return []

        mock_client.get_config_data = mock_config
        mock_client.close = AsyncMock()

        with patch(
            "forcehound.collectors.aura_collector.AuraClient", return_value=mock_client
        ):
            result = await collector.collect()

        node_kinds = set()
        for node in result.nodes:
            node_kinds.update(node.kinds)

        assert "SF_PermissionSet" in node_kinds
        assert "SF_Profile" in node_kinds

        # Should have 2 profiles: one from User traversal (00eXX000000TEST)
        # and one from PS shadow (00eXX000000PROF2)
        profile_nodes = [n for n in result.nodes if "SF_Profile" in n.kinds]
        assert len(profile_nodes) == 2

        # Should have 1 PS node (the regular one)
        ps_nodes = [n for n in result.nodes if "SF_PermissionSet" in n.kinds]
        assert len(ps_nodes) == 1
        assert ps_nodes[0].id == "0PSXX000000PS001"

        # Verify metadata
        assert result.metadata["permission_sets"] == 2

        # Verify role enumeration metadata
        assert result.metadata["roles"] == 1


# =====================================================================
# UserRole field paths tests
# =====================================================================


class TestBuildRoleFieldPaths:
    """Tests for build_role_field_paths()."""

    def test_required_has_id(self):
        required, _ = build_role_field_paths()
        assert required == ["UserRole.Id"]

    def test_optional_has_name(self):
        _, optional = build_role_field_paths()
        assert "UserRole.Name" in optional

    def test_optional_has_developer_name(self):
        _, optional = build_role_field_paths()
        assert "UserRole.DeveloperName" in optional

    def test_optional_has_parent_role_id(self):
        _, optional = build_role_field_paths()
        assert "UserRole.ParentRoleId" in optional


# =====================================================================
# UserRole parse response tests
# =====================================================================


class TestParseRoleResponse:
    """Tests for parse_role_response()."""

    def test_parses_role_with_parent(self, aura_role_response):
        rv = aura_role_response["actions"][0]["returnValue"]
        result = parse_role_response(rv)
        assert result["Id"] == "00EXX000000RVPNA"
        assert result["Name"] == "VP, North American Sales"
        assert result["DeveloperName"] == "VPNorthAmericanSales"
        assert result["ParentRoleId"] == "00EXX000000RSVPM"

    def test_parses_root_role(self):
        rv = {
            "fields": {
                "Id": {"value": "00EXX000000RCEO1"},
                "Name": {"value": "CEO"},
                "DeveloperName": {"value": "CEO"},
                "ParentRoleId": {"value": None},
            }
        }
        result = parse_role_response(rv)
        assert result["Id"] == "00EXX000000RCEO1"
        assert result["Name"] == "CEO"
        assert result["ParentRoleId"] is None

    def test_returns_none_for_none(self):
        assert parse_role_response(None) is None

    def test_handles_empty_fields(self):
        result = parse_role_response({"fields": {}})
        assert result["Id"] is None
        assert result["Name"] is None
        assert result["DeveloperName"] is None
        assert result["ParentRoleId"] is None


# =====================================================================
# UserRole enumeration tests
# =====================================================================


class TestAuraCollectorRoleEnumeration:
    """Tests for UserRole enumeration in the Aura collector."""

    @pytest.fixture
    def collector(self):
        auth = AuthConfig(
            instance_url="https://test.lightning.force.com",
            session_id="00DXX000000XXXXX!AQEAQTEST",
            aura_context='{"mode":"PRODDEBUG"}',
            aura_token="eyJtoken",
        )
        return AuraCollector(auth, verbose=False, max_workers=5)

    # -- _build_role_nodes with parsed_roles --

    def test_role_nodes_from_enumeration(self, collector):
        roles = [
            {"Id": "00EA", "Name": "CEO", "DeveloperName": "CEO", "ParentRoleId": None},
            {
                "Id": "00EB",
                "Name": "CFO",
                "DeveloperName": "CFO",
                "ParentRoleId": "00EA",
            },
        ]
        nodes = collector._build_role_nodes([], roles)
        assert len(nodes) == 2
        ids = {n.id for n in nodes}
        assert "00EA" in ids
        assert "00EB" in ids

    def test_role_nodes_dedup_user_traversal_and_enumeration(
        self, collector, parsed_user_data
    ):
        # Role from user traversal has Id=00EXX000000TEST
        roles = [
            {
                "Id": "00EXX000000TEST",
                "Name": "CEO",
                "DeveloperName": "CEO",
                "ParentRoleId": None,
            },
            {
                "Id": "00EB",
                "Name": "CFO",
                "DeveloperName": "CFO",
                "ParentRoleId": "00EXX000000TEST",
            },
        ]
        nodes = collector._build_role_nodes([parsed_user_data], roles)
        # Should have 2: CEO from user traversal, CFO from enumeration
        assert len(nodes) == 2

    def test_role_nodes_no_parsed_roles(self, collector, parsed_user_data):
        nodes = collector._build_role_nodes([parsed_user_data], None)
        assert len(nodes) == 1
        assert nodes[0].id == "00EXX000000TEST"

    def test_role_nodes_empty_parsed_roles(self, collector, parsed_user_data):
        nodes = collector._build_role_nodes([parsed_user_data], [])
        assert len(nodes) == 1

    def test_role_nodes_developer_name_property(self, collector):
        roles = [
            {
                "Id": "00EA",
                "Name": "SVP, Sales",
                "DeveloperName": "SVPSales",
                "ParentRoleId": None,
            },
        ]
        nodes = collector._build_role_nodes([], roles)
        assert nodes[0].properties["developer_name"] == "SVPSales"

    # -- _build_role_hierarchy_edges with parsed_roles --

    def test_hierarchy_edges_from_enumeration(self, collector):
        roles = [
            {"Id": "00EA", "Name": "CEO", "DeveloperName": "CEO", "ParentRoleId": None},
            {
                "Id": "00EB",
                "Name": "CFO",
                "DeveloperName": "CFO",
                "ParentRoleId": "00EA",
            },
            {
                "Id": "00EC",
                "Name": "COO",
                "DeveloperName": "COO",
                "ParentRoleId": "00EA",
            },
        ]
        edges = collector._build_role_hierarchy_edges([], roles)
        assert len(edges) == 2
        sources = {e.source for e in edges}
        assert "00EB" in sources
        assert "00EC" in sources
        for e in edges:
            assert e.kind == "ReportsTo"

    def test_hierarchy_edges_root_role_no_edge(self, collector):
        roles = [
            {"Id": "00EA", "Name": "CEO", "DeveloperName": "CEO", "ParentRoleId": None},
        ]
        edges = collector._build_role_hierarchy_edges([], roles)
        assert len(edges) == 0

    def test_hierarchy_edges_dedup(self, collector, parsed_user_data):
        # parsed_user_data role has Id=00EXX000000TEST, ParentRoleId=None (CEO)
        # So no edges from user traversal. Add enumerated role with same ID but parent.
        roles = [
            {"Id": "00EA", "Name": "VP", "DeveloperName": "VP", "ParentRoleId": "00EB"},
            {"Id": "00EA", "Name": "VP", "DeveloperName": "VP", "ParentRoleId": "00EB"},
        ]
        edges = collector._build_role_hierarchy_edges([], roles)
        # Dedup means only 1 edge
        assert len(edges) == 1

    def test_hierarchy_edges_no_parsed_roles(self, collector, parsed_user_data):
        edges = collector._build_role_hierarchy_edges([parsed_user_data], None)
        # CEO has no parent, so 0 edges
        assert len(edges) == 0

    def test_hierarchy_combined_sources(self, collector):
        user_data = {
            "user": {"Id": "005A", "ManagerId": None},
            "profile": {"Id": "00eA"},
            "profile_permissions": {},
            "role": {"Id": "00EA", "Name": "VP", "ParentRoleId": "00EB"},
            "manager": {"Id": None, "Name": None},
        }
        roles = [
            {
                "Id": "00EC",
                "Name": "CFO",
                "DeveloperName": "CFO",
                "ParentRoleId": "00EB",
            },
        ]
        edges = collector._build_role_hierarchy_edges([user_data], roles)
        assert len(edges) == 2
        sources = {e.source for e in edges}
        assert "00EA" in sources
        assert "00EC" in sources

    # -- Full collect flow --

    @pytest.mark.asyncio
    async def test_collect_includes_enumerated_roles(
        self,
        aura_user_response,
        aura_group_response,
        aura_group_member_response,
    ):
        auth = AuthConfig(
            instance_url="https://test.lightning.force.com",
            session_id="00DXX000000XXXXX!AQEAQTEST",
            aura_context='{"mode":"PRODDEBUG"}',
            aura_token="eyJtoken",
        )
        collector = AuraCollector(auth, verbose=False, max_workers=5)

        mock_client = AsyncMock()
        mock_client.org_id = "00DXX000000XXXXX"
        mock_client.request_count = 0

        async def mock_get_items_graphql(obj_name, **kwargs):
            if obj_name == "User":
                return ["005XX000001ATEST"]
            elif obj_name == "PermissionSet":
                return []
            elif obj_name == "UserRole":
                return ["00EXX000000RCEO1", "00EXX000000RCFO1"]
            elif obj_name == "Group":
                return []
            elif obj_name == "GroupMember":
                return []
            return []

        mock_client.get_items_graphql = mock_get_items_graphql

        async def mock_get_record(record_id, fields, optional_fields=None):
            if record_id.startswith("005"):
                return aura_user_response
            elif record_id == "00EXX000000RCEO1":
                return {
                    "actions": [
                        {
                            "returnValue": {
                                "fields": {
                                    "Id": {"value": "00EXX000000RCEO1"},
                                    "Name": {"value": "CEO"},
                                    "DeveloperName": {"value": "CEO"},
                                    "ParentRoleId": {"value": None},
                                }
                            }
                        }
                    ]
                }
            elif record_id == "00EXX000000RCFO1":
                return {
                    "actions": [
                        {
                            "returnValue": {
                                "fields": {
                                    "Id": {"value": "00EXX000000RCFO1"},
                                    "Name": {"value": "CFO"},
                                    "DeveloperName": {"value": "CFO"},
                                    "ParentRoleId": {"value": "00EXX000000RCEO1"},
                                }
                            }
                        }
                    ]
                }
            return {"actions": [{"returnValue": None}]}

        mock_client.get_record_with_fields = mock_get_record

        async def mock_config():
            return []

        mock_client.get_config_data = mock_config
        mock_client.close = AsyncMock()

        with patch(
            "forcehound.collectors.aura_collector.AuraClient", return_value=mock_client
        ):
            result = await collector.collect()

        role_nodes = [n for n in result.nodes if "SF_Role" in n.kinds]
        # User traversal finds 1 role (00EXX000000TEST from user data),
        # Enumeration adds CEO (00EXX000000RCEO1) + CFO (00EXX000000RCFO1)
        # = 3 total (different IDs, no dedup)
        assert len(role_nodes) == 3

        role_ids = {n.id for n in role_nodes}
        assert "00EXX000000TEST" in role_ids
        assert "00EXX000000RCEO1" in role_ids
        assert "00EXX000000RCFO1" in role_ids

        # Verify ReportsTo edge exists (CFO → CEO)
        reports_to = [e for e in result.edges if e.kind == "ReportsTo"]
        assert len(reports_to) >= 1
        cfo_edge = [e for e in reports_to if e.source == "00EXX000000RCFO1"]
        assert len(cfo_edge) == 1
        assert cfo_edge[0].target == "00EXX000000RCEO1"

        # Verify metadata
        assert result.metadata["roles"] == 2
