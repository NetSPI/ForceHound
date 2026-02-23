"""Shared pytest fixtures for ForceHound test suite."""

import pytest
from forcehound.models.auth import AuthConfig
from forcehound.models.base import GraphNode, GraphEdge, CollectionResult


@pytest.fixture
def api_auth():
    """AuthConfig for API mode testing."""
    return AuthConfig(
        instance_url="https://test.my.salesforce.com",
        session_id="00DXX000000XXXXX!AQEAQTEST",
        username="",
        password="",
        security_token="",
    )


@pytest.fixture
def aura_auth():
    """AuthConfig for Aura mode testing."""
    return AuthConfig(
        instance_url="https://test.lightning.force.com",
        session_id="00DXX000000XXXXX!AQEAQTEST",
        aura_context='{"mode":"PRODDEBUG","fwuid":"test123","app":"one:one","loaded":{},"dn":[],"globals":{},"uad":true}',
        aura_token="eyJtesttoken",
    )


@pytest.fixture
def sample_user_node():
    """A sample SF_User GraphNode."""
    return GraphNode(
        id="005XX000001ATEST",
        kinds=["SF_User", "User", "SaaS_Identity"],
        properties={
            "name": "Test User",
            "email": "test@example.com",
            "user_type": "Standard",
            "is_active": True,
            "id": "005XX000001ATEST",
        },
    )


@pytest.fixture
def sample_profile_node():
    """A sample SF_Profile GraphNode."""
    return GraphNode(
        id="00eXX000000TEST",
        kinds=["SF_Profile", "SaaS_Container", "SaaS_Entitlement"],
        properties={"name": "System Administrator", "id": "00eXX000000TEST"},
    )


@pytest.fixture
def sample_edge():
    """A sample HasProfile GraphEdge."""
    return GraphEdge(
        source="005XX000001ATEST",
        target="00eXX000000TEST",
        kind="HasProfile",
    )


@pytest.fixture
def sample_collection_result(sample_user_node, sample_profile_node, sample_edge):
    """A sample CollectionResult with one user, one profile, one edge."""
    return CollectionResult(
        nodes=[sample_user_node, sample_profile_node],
        edges=[sample_edge],
        collector_type="api",
        org_id="00DXX0000008TEST",
    )


# -- Aura response fixtures --


@pytest.fixture
def aura_user_response():
    """Raw Aura response for a User record with Profile permissions."""
    return {
        "actions": [
            {
                "state": "SUCCESS",
                "returnValue": {
                    "fields": {
                        "Id": {"value": "005XX000001ATEST"},
                        "Name": {"value": "Test User"},
                        "Email": {"value": "test@example.com"},
                        "Username": {"value": "test@example.com.sandbox"},
                        "UserType": {"value": "Standard"},
                        "IsActive": {"value": True},
                        "ProfileId": {"value": "00eXX000000TEST"},
                        "UserRoleId": {"value": "00EXX000000TEST"},
                        "ManagerId": {"value": "005XX000002MGRID"},
                        "LastLoginDate": {"value": "2025-01-15T10:00:00.000Z"},
                        "CreatedDate": {"value": "2024-01-01T00:00:00.000Z"},
                        "CreatedById": {"value": "005XX000001ADMIN"},
                        "Profile": {
                            "value": {
                                "fields": {
                                    "Id": {"value": "00eXX000000TEST"},
                                    "Name": {"value": "System Administrator"},
                                    "UserType": {"value": "Standard"},
                                    "PermissionsModifyAllData": {"value": True},
                                    "PermissionsViewAllData": {"value": True},
                                    "PermissionsAuthorApex": {"value": True},
                                    "PermissionsManageUsers": {"value": True},
                                    "PermissionsCustomizeApplication": {"value": True},
                                    "PermissionsManageProfilesPermissionsets": {
                                        "value": False
                                    },
                                    "PermissionsAssignPermissionSets": {"value": False},
                                    "PermissionsManageRoles": {"value": False},
                                    "PermissionsManageSharing": {"value": False},
                                    "PermissionsManageInternalUsers": {"value": False},
                                    "PermissionsResetPasswords": {"value": False},
                                    "PermissionsApiEnabled": {"value": True},
                                    "PermissionsViewSetup": {"value": True},
                                    "PermissionsViewAllUsers": {"value": True},
                                    "PermissionsManageDataIntegrations": {
                                        "value": False
                                    },
                                }
                            }
                        },
                        "UserRole": {
                            "value": {
                                "fields": {
                                    "Id": {"value": "00EXX000000TEST"},
                                    "Name": {"value": "CEO"},
                                    "ParentRoleId": {"value": None},
                                }
                            }
                        },
                        "Manager": {
                            "value": {
                                "fields": {
                                    "Id": {"value": "005XX000002MGRID"},
                                    "Name": {"value": "Manager User"},
                                }
                            }
                        },
                    }
                },
            }
        ]
    }


@pytest.fixture
def aura_group_response():
    """Raw Aura response for a Group record."""
    return {
        "actions": [
            {
                "state": "SUCCESS",
                "returnValue": {
                    "fields": {
                        "Id": {"value": "00GXX000001GTEST"},
                        "Name": {"value": "TestGroup"},
                        "DeveloperName": {"value": "TestGroup"},
                        "Type": {"value": "Regular"},
                        "RelatedId": {"value": None},
                        "DoesIncludeBosses": {"value": False},
                        "DoesSendEmailToMembers": {"value": False},
                    }
                },
            }
        ]
    }


@pytest.fixture
def aura_group_member_response():
    """Raw Aura response for a GroupMember record."""
    return {
        "actions": [
            {
                "state": "SUCCESS",
                "returnValue": {
                    "fields": {
                        "Id": {"value": "011XX000001MTEST"},
                        "GroupId": {"value": "00GXX000001GTEST"},
                        "UserOrGroupId": {"value": "005XX000001ATEST"},
                        "Group": {
                            "value": {
                                "fields": {
                                    "Name": {"value": "TestGroup"},
                                    "Type": {"value": "Regular"},
                                }
                            }
                        },
                    }
                },
            }
        ]
    }


@pytest.fixture
def parsed_user_data():
    """Parsed user data dict as returned by parse_user_response."""
    return {
        "user": {
            "Id": "005XX000001ATEST",
            "Name": "Test User",
            "Email": "test@example.com",
            "Username": "test@example.com.sandbox",
            "UserType": "Standard",
            "IsActive": True,
            "ProfileId": "00eXX000000TEST",
            "UserRoleId": "00EXX000000TEST",
            "ManagerId": "005XX000002MGRID",
            "LastLoginDate": "2025-01-15T10:00:00.000Z",
            "CreatedDate": "2024-01-01T00:00:00.000Z",
            "CreatedById": "005XX000001ADMIN",
        },
        "profile": {
            "Id": "00eXX000000TEST",
            "Name": "System Administrator",
            "UserType": "Standard",
        },
        "profile_permissions": {
            "PermissionsModifyAllData": True,
            "PermissionsViewAllData": True,
            "PermissionsAuthorApex": True,
            "PermissionsManageUsers": True,
            "PermissionsCustomizeApplication": True,
            "PermissionsManageProfilesPermissionsets": False,
            "PermissionsAssignPermissionSets": False,
            "PermissionsManageRoles": False,
            "PermissionsManageSharing": False,
            "PermissionsManageInternalUsers": False,
            "PermissionsResetPasswords": False,
            "PermissionsApiEnabled": True,
            "PermissionsViewSetup": True,
            "PermissionsViewAllUsers": True,
            "PermissionsManageDataIntegrations": False,
        },
        "role": {
            "Id": "00EXX000000TEST",
            "Name": "CEO",
            "ParentRoleId": None,
        },
        "manager": {
            "Id": "005XX000002MGRID",
            "Name": "Manager User",
        },
    }


@pytest.fixture
def aura_permission_set_response():
    """Raw Aura response for a regular PermissionSet record."""
    return {
        "actions": [
            {
                "state": "SUCCESS",
                "returnValue": {
                    "fields": {
                        "Id": {"value": "0PSXX000000PS001"},
                        "Name": {"value": "TestPermSet"},
                        "Label": {"value": "Test Permission Set"},
                        "IsOwnedByProfile": {"value": False},
                        "ProfileId": {"value": None},
                        "PermissionSetGroupId": {"value": None},
                        "IsCustom": {"value": True},
                        "Type": {"value": "Regular"},
                        "PermissionsModifyAllData": {"value": False},
                        "PermissionsViewAllData": {"value": True},
                        "PermissionsAuthorApex": {"value": False},
                        "PermissionsManageUsers": {"value": False},
                        "PermissionsCustomizeApplication": {"value": False},
                        "PermissionsManageProfilesPermissionsets": {"value": False},
                        "PermissionsAssignPermissionSets": {"value": False},
                        "PermissionsManageRoles": {"value": False},
                        "PermissionsManageSharing": {"value": False},
                        "PermissionsManageInternalUsers": {"value": False},
                        "PermissionsResetPasswords": {"value": False},
                        "PermissionsApiEnabled": {"value": True},
                        "PermissionsViewSetup": {"value": False},
                        "PermissionsViewAllUsers": {"value": False},
                        "PermissionsManageDataIntegrations": {"value": False},
                        "Profile": {"value": None},
                    }
                },
            }
        ]
    }


@pytest.fixture
def aura_profile_shadow_ps_response():
    """Raw Aura response for a profile shadow PermissionSet."""
    return {
        "actions": [
            {
                "state": "SUCCESS",
                "returnValue": {
                    "fields": {
                        "Id": {"value": "0PSXX000000SHAD1"},
                        "Name": {"value": "X00eXX000000PROF2"},
                        "Label": {"value": "Standard User"},
                        "IsOwnedByProfile": {"value": True},
                        "ProfileId": {"value": "00eXX000000PROF2"},
                        "PermissionSetGroupId": {"value": None},
                        "IsCustom": {"value": False},
                        "Type": {"value": "Profile"},
                        "PermissionsModifyAllData": {"value": False},
                        "PermissionsViewAllData": {"value": False},
                        "PermissionsAuthorApex": {"value": False},
                        "PermissionsManageUsers": {"value": False},
                        "PermissionsCustomizeApplication": {"value": False},
                        "PermissionsManageProfilesPermissionsets": {"value": False},
                        "PermissionsAssignPermissionSets": {"value": False},
                        "PermissionsManageRoles": {"value": False},
                        "PermissionsManageSharing": {"value": False},
                        "PermissionsManageInternalUsers": {"value": False},
                        "PermissionsResetPasswords": {"value": False},
                        "PermissionsApiEnabled": {"value": True},
                        "PermissionsViewSetup": {"value": True},
                        "PermissionsViewAllUsers": {"value": False},
                        "PermissionsManageDataIntegrations": {"value": False},
                        "Profile": {
                            "value": {
                                "fields": {
                                    "Name": {"value": "Standard User"},
                                    "UserType": {"value": "Standard"},
                                }
                            }
                        },
                    }
                },
            }
        ]
    }


@pytest.fixture
def parsed_permission_set_data():
    """Parsed regular PermissionSet data dict."""
    return {
        "permission_set": {
            "Id": "0PSXX000000PS001",
            "Name": "TestPermSet",
            "Label": "Test Permission Set",
            "IsOwnedByProfile": False,
            "ProfileId": None,
            "PermissionSetGroupId": None,
            "IsCustom": True,
            "Type": "Regular",
        },
        "capabilities": {
            "PermissionsModifyAllData": False,
            "PermissionsViewAllData": True,
            "PermissionsAuthorApex": False,
            "PermissionsManageUsers": False,
            "PermissionsCustomizeApplication": False,
            "PermissionsManageProfilesPermissionsets": False,
            "PermissionsAssignPermissionSets": False,
            "PermissionsManageRoles": False,
            "PermissionsManageSharing": False,
            "PermissionsManageInternalUsers": False,
            "PermissionsResetPasswords": False,
            "PermissionsApiEnabled": True,
            "PermissionsViewSetup": False,
            "PermissionsViewAllUsers": False,
            "PermissionsManageDataIntegrations": False,
        },
        "profile": {
            "Name": None,
            "UserType": None,
        },
    }


@pytest.fixture
def parsed_profile_shadow_ps_data():
    """Parsed profile shadow PermissionSet data dict."""
    return {
        "permission_set": {
            "Id": "0PSXX000000SHAD1",
            "Name": "X00eXX000000PROF2",
            "Label": "Standard User",
            "IsOwnedByProfile": True,
            "ProfileId": "00eXX000000PROF2",
            "PermissionSetGroupId": None,
            "IsCustom": False,
            "Type": "Profile",
        },
        "capabilities": {
            "PermissionsModifyAllData": False,
            "PermissionsViewAllData": False,
            "PermissionsAuthorApex": False,
            "PermissionsManageUsers": False,
            "PermissionsCustomizeApplication": False,
            "PermissionsManageProfilesPermissionsets": False,
            "PermissionsAssignPermissionSets": False,
            "PermissionsManageRoles": False,
            "PermissionsManageSharing": False,
            "PermissionsManageInternalUsers": False,
            "PermissionsResetPasswords": False,
            "PermissionsApiEnabled": True,
            "PermissionsViewSetup": True,
            "PermissionsViewAllUsers": False,
            "PermissionsManageDataIntegrations": False,
        },
        "profile": {
            "Name": "Standard User",
            "UserType": "Standard",
        },
    }


@pytest.fixture
def aura_role_response():
    """Raw Aura response for a UserRole record (VP with parent)."""
    return {
        "actions": [
            {
                "state": "SUCCESS",
                "returnValue": {
                    "fields": {
                        "Id": {"value": "00EXX000000RVPNA"},
                        "Name": {"value": "VP, North American Sales"},
                        "DeveloperName": {"value": "VPNorthAmericanSales"},
                        "ParentRoleId": {"value": "00EXX000000RSVPM"},
                    }
                },
            }
        ]
    }


@pytest.fixture
def parsed_role_data():
    """Parsed UserRole data dict (VP with parent)."""
    return {
        "Id": "00EXX000000RVPNA",
        "Name": "VP, North American Sales",
        "DeveloperName": "VPNorthAmericanSales",
        "ParentRoleId": "00EXX000000RSVPM",
    }


@pytest.fixture
def parsed_group_data():
    """Parsed group data dict."""
    return {
        "Id": "00GXX000001GTEST",
        "Name": "TestGroup",
        "DeveloperName": "TestGroup",
        "Type": "Regular",
        "RelatedId": None,
        "DoesIncludeBosses": False,
        "DoesSendEmailToMembers": False,
    }


@pytest.fixture
def parsed_group_member_data():
    """Parsed group member data dict."""
    return {
        "Id": "011XX000001MTEST",
        "GroupId": "00GXX000001GTEST",
        "UserOrGroupId": "005XX000001ATEST",
        "GroupName": "TestGroup",
        "GroupType": "Regular",
    }
