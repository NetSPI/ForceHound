"""Tests for forcehound.constants."""

import pytest
import pytest
from forcehound.constants import (
    CollectorMode,
    ObjectType,
    AccessLevel,
    CAPABILITY_FIELDS,
    CAPABILITY_TO_EDGE_KIND,
    API_CAPABILITY_FIELDS,
    ENTITY_DEFINITION_FIELDS,
    FIELD_PERMISSION_FIELDS,
    FIELD_PERMISSION_TO_EDGE_KIND,
    NODE_KINDS,
    EDGE_KINDS,
    INCLUDED_SUFFIXES,
    EXCLUDED_SUFFIXES,
    DEFAULT_MAX_WORKERS,
    CUSTOM_NODE_ICONS,
)


class TestCollectorMode:
    def test_api_value(self):
        assert CollectorMode.API.value == "api"

    def test_aura_value(self):
        assert CollectorMode.AURA.value == "aura"

    def test_both_value(self):
        assert CollectorMode.BOTH.value == "both"

    def test_from_string(self):
        assert CollectorMode("api") == CollectorMode.API
        assert CollectorMode("aura") == CollectorMode.AURA
        assert CollectorMode("both") == CollectorMode.BOTH

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            CollectorMode("invalid")


class TestObjectType:
    def test_user(self):
        assert ObjectType.USER.value == "User"

    def test_profile(self):
        assert ObjectType.PROFILE.value == "Profile"

    def test_permission_set(self):
        assert ObjectType.PERMISSION_SET.value == "PermissionSet"

    def test_role(self):
        assert ObjectType.ROLE.value == "UserRole"

    def test_group(self):
        assert ObjectType.GROUP.value == "Group"


class TestAccessLevel:
    def test_all_levels(self):
        assert AccessLevel.READ.value == "Read"
        assert AccessLevel.EDIT.value == "Edit"
        assert AccessLevel.ALL.value == "All"
        assert AccessLevel.NONE.value == "None"
        assert AccessLevel.CONTROLLED_BY_PARENT.value == "ControlledByParent"


class TestCapabilityFields:
    def test_count(self):
        assert len(CAPABILITY_FIELDS) == 15

    def test_starts_with_permissions(self):
        for field in CAPABILITY_FIELDS:
            assert field.startswith("Permissions"), (
                f"{field} doesn't start with Permissions"
            )

    def test_modify_all_data_present(self):
        assert "PermissionsModifyAllData" in CAPABILITY_FIELDS

    def test_view_all_data_present(self):
        assert "PermissionsViewAllData" in CAPABILITY_FIELDS

    def test_manage_data_integrations_present(self):
        assert "PermissionsManageDataIntegrations" in CAPABILITY_FIELDS


class TestCapabilityToEdgeKind:
    def test_count(self):
        assert len(CAPABILITY_TO_EDGE_KIND) == 15

    def test_all_fields_mapped(self):
        for field in CAPABILITY_FIELDS:
            assert field in CAPABILITY_TO_EDGE_KIND

    def test_modify_all_data_mapping(self):
        assert CAPABILITY_TO_EDGE_KIND["PermissionsModifyAllData"] == "ModifyAllData"

    def test_api_enabled_mapping(self):
        assert CAPABILITY_TO_EDGE_KIND["PermissionsApiEnabled"] == "ApiEnabled"

    def test_edge_kinds_are_strings(self):
        for kind in CAPABILITY_TO_EDGE_KIND.values():
            assert isinstance(kind, str)
            assert len(kind) > 0


class TestAPICapabilityFields:
    def test_count(self):
        assert len(API_CAPABILITY_FIELDS) == 5

    def test_subset_of_all_fields(self):
        for field in API_CAPABILITY_FIELDS:
            assert field in CAPABILITY_FIELDS


class TestNodeKinds:
    def test_user_kinds(self):
        assert NODE_KINDS["user"] == ["SF_User", "User", "SaaS_Identity"]

    def test_profile_kinds(self):
        assert NODE_KINDS["profile"] == [
            "SF_Profile",
            "SaaS_Container",
            "SaaS_Entitlement",
        ]

    def test_role_kinds(self):
        assert NODE_KINDS["role"] == ["SF_Role", "SaaS_Group"]

    def test_organization_kinds(self):
        assert NODE_KINDS["organization"] == [
            "SF_Organization",
            "Organization",
            "SaaS_Tenant",
        ]

    def test_all_values_are_lists(self):
        for key, kinds in NODE_KINDS.items():
            assert isinstance(kinds, list), f"{key} is not a list"
            assert len(kinds) > 0, f"{key} is empty"


class TestEdgeKinds:
    def test_has_profile(self):
        assert "HasProfile" in EDGE_KINDS

    def test_has_role(self):
        assert "HasRole" in EDGE_KINDS

    def test_member_of(self):
        assert "MemberOf" in EDGE_KINDS

    def test_capability_edges_included(self):
        for edge_kind in CAPABILITY_TO_EDGE_KIND.values():
            assert edge_kind in EDGE_KINDS

    def test_sharing_edges(self):
        assert "Owns" in EDGE_KINDS
        assert "ExplicitAccess" in EDGE_KINDS
        assert "InheritsAccess" in EDGE_KINDS

    def test_created_by_edge(self):
        assert "CreatedBy" in EDGE_KINDS

    def test_can_own_object_type_edge(self):
        assert "CanOwnObjectType" in EDGE_KINDS


class TestSuffixes:
    def test_included(self):
        assert "__c" in INCLUDED_SUFFIXES
        assert "__mdt" in INCLUDED_SUFFIXES
        assert "__History" in INCLUDED_SUFFIXES

    def test_excluded(self):
        assert "__ChangeEvent" in EXCLUDED_SUFFIXES
        assert "__Share" in EXCLUDED_SUFFIXES


class TestCustomNodeIcons:
    def test_all_sf_kinds_covered(self):
        """Every SF_* kind from NODE_KINDS has a CUSTOM_NODE_ICONS entry."""
        sf_kinds = set()
        for kinds in NODE_KINDS.values():
            for k in kinds:
                if k.startswith("SF_"):
                    sf_kinds.add(k)
        for kind in sf_kinds:
            assert kind in CUSTOM_NODE_ICONS, f"{kind} missing from CUSTOM_NODE_ICONS"

    def test_entries_have_name_and_color(self):
        for kind, icon in CUSTOM_NODE_ICONS.items():
            assert "name" in icon, f"{kind} missing 'name'"
            assert "color" in icon, f"{kind} missing 'color'"

    def test_colors_are_valid_hex(self):
        import re
        hex_re = re.compile(r"^#[0-9A-Fa-f]{6}$")
        for kind, icon in CUSTOM_NODE_ICONS.items():
            assert hex_re.match(icon["color"]), (
                f"{kind} color {icon['color']!r} is not valid hex"
            )

    def test_no_fa_prefix(self):
        for kind, icon in CUSTOM_NODE_ICONS.items():
            assert not icon["name"].startswith("fa-"), (
                f"{kind} name should not have 'fa-' prefix"
            )


class TestFieldPermissionFields:
    def test_count(self):
        assert len(FIELD_PERMISSION_FIELDS) == 2

    def test_entries(self):
        assert "PermissionsRead" in FIELD_PERMISSION_FIELDS
        assert "PermissionsEdit" in FIELD_PERMISSION_FIELDS


class TestFieldPermissionToEdgeKind:
    def test_count(self):
        assert len(FIELD_PERMISSION_TO_EDGE_KIND) == 2

    def test_read_mapping(self):
        assert FIELD_PERMISSION_TO_EDGE_KIND["PermissionsRead"] == "CanReadField"

    def test_edit_mapping(self):
        assert FIELD_PERMISSION_TO_EDGE_KIND["PermissionsEdit"] == "CanEditField"

    def test_all_fields_mapped(self):
        for field in FIELD_PERMISSION_FIELDS:
            assert field in FIELD_PERMISSION_TO_EDGE_KIND


class TestFieldNodeKinds:
    def test_field_kinds(self):
        assert NODE_KINDS["field"] == ["SF_Field", "SaaS_Resource"]


class TestFieldEdgeKinds:
    def test_can_read_field_in_edge_kinds(self):
        assert "CanReadField" in EDGE_KINDS

    def test_can_edit_field_in_edge_kinds(self):
        assert "CanEditField" in EDGE_KINDS

    def test_field_of_in_edge_kinds(self):
        assert "FieldOf" in EDGE_KINDS


class TestFieldCustomNodeIcons:
    def test_sf_field_icon_exists(self):
        assert "SF_Field" in CUSTOM_NODE_ICONS

    def test_sf_field_icon_has_name(self):
        assert "name" in CUSTOM_NODE_ICONS["SF_Field"]

    def test_sf_field_icon_has_color(self):
        assert "color" in CUSTOM_NODE_ICONS["SF_Field"]

    def test_sf_field_icon_values(self):
        assert CUSTOM_NODE_ICONS["SF_Field"]["name"] == "tag"
        assert CUSTOM_NODE_ICONS["SF_Field"]["color"] == "#5C6BC0"


class TestEntityDefinitionFields:
    def test_count(self):
        assert len(ENTITY_DEFINITION_FIELDS) == 13

    def test_sharing_model_fields_present(self):
        assert "InternalSharingModel" in ENTITY_DEFINITION_FIELDS
        assert "ExternalSharingModel" in ENTITY_DEFINITION_FIELDS

    def test_qualified_api_name_present(self):
        assert "QualifiedApiName" in ENTITY_DEFINITION_FIELDS

    def test_key_prefix_present(self):
        assert "KeyPrefix" in ENTITY_DEFINITION_FIELDS


class TestDefaults:
    def test_max_workers(self):
        assert DEFAULT_MAX_WORKERS == 30
