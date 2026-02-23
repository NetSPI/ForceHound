"""Typed dataclasses for REST API (SOQL) query results.

Each dataclass mirrors the shape of a Salesforce SOQL response record,
providing type safety and IDE auto-completion when building graph nodes
and edges from API collector results.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def _records(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract the ``records`` list from a ``simple_salesforce`` query result."""
    return result.get("records", [])


@dataclass
class SFOrganization:
    """Salesforce Organization record."""

    Id: str = ""
    Name: str = ""


@dataclass
class SFUser:
    """Salesforce User record."""

    Id: str = ""
    Name: str = ""
    Email: Optional[str] = None
    UserType: str = ""
    IsActive: bool = True


@dataclass
class SFProfile:
    """Salesforce Profile record."""

    Id: str = ""
    Name: str = ""
    Description: Optional[str] = None


@dataclass
class SFPermissionSet:
    """Salesforce PermissionSet record (non-profile-owned, regular type)."""

    Id: str = ""
    Name: str = ""
    Label: str = ""


@dataclass
class SFPermissionSetGroup:
    """Salesforce PermissionSetGroup record."""

    Id: str = ""
    DeveloperName: str = ""
    MasterLabel: str = ""


@dataclass
class SFRole:
    """Salesforce UserRole record."""

    Id: str = ""
    Name: str = ""
    ParentRoleId: Optional[str] = None


@dataclass
class SFGroup:
    """Salesforce Group record."""

    Id: str = ""
    Name: Optional[str] = None
    DeveloperName: Optional[str] = None
    Type: str = ""
    RelatedId: Optional[str] = None


@dataclass
class SFPermissionSetAssignment:
    """Salesforce PermissionSetAssignment record."""

    AssigneeId: str = ""
    PermissionSetId: str = ""
    PermissionSetGroupId: Optional[str] = None
    IsOwnedByProfile: bool = False
    ProfileId: Optional[str] = None


@dataclass
class SFGroupMember:
    """Salesforce GroupMember record."""

    GroupId: str = ""
    UserOrGroupId: str = ""


@dataclass
class SFPermissionSetGroupComponent:
    """Link between a PermissionSet and its parent PermissionSetGroup."""

    PermissionSetId: str = ""
    PermissionSetGroupId: str = ""


@dataclass
class SFShareRecord:
    """Generic Share object record (AccountShare, OpportunityShare, etc.).

    Field availability varies by Share object type, so most fields are
    optional.
    """

    Id: str = ""
    UserOrGroupId: str = ""
    RowCause: Optional[str] = None
    AccessLevel: Optional[str] = None
    AccountAccessLevel: Optional[str] = None
    OpportunityAccessLevel: Optional[str] = None
    CaseAccessLevel: Optional[str] = None
    ContactAccessLevel: Optional[str] = None
    ParentId: Optional[str] = None
    AccountId: Optional[str] = None
    UserId: Optional[str] = None
    UserAccessLevel: Optional[str] = None
