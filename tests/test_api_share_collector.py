"""Tests for forcehound.collectors.api_share_collector.ShareObjectCollector."""

import pytest
from unittest.mock import MagicMock

from forcehound.collectors.api_share_collector import ShareObjectCollector


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def mock_sf():
    """Create a mock simple_salesforce.Salesforce with Share object data."""
    sf = MagicMock()

    # EntityDefinition query returns two Share objects
    sf.query_all.side_effect = _route_share_queries

    # Describe for AccountShare
    account_share_describe = MagicMock()
    account_share_describe.describe.return_value = {
        "fields": [
            {"name": "Id"},
            {"name": "AccountId"},
            {"name": "UserOrGroupId"},
            {"name": "AccountAccessLevel"},
            {"name": "OpportunityAccessLevel"},
            {"name": "CaseAccessLevel"},
            {"name": "ContactAccessLevel"},
            {"name": "RowCause"},
            {"name": "CreatedById"},
            {"name": "LastModifiedById"},
            {"name": "LastModifiedDate"},
        ]
    }
    sf.AccountShare = account_share_describe

    # Describe for LeadShare
    lead_share_describe = MagicMock()
    lead_share_describe.describe.return_value = {
        "fields": [
            {"name": "Id"},
            {"name": "LeadId"},
            {"name": "UserOrGroupId"},
            {"name": "LeadAccessLevel"},
            {"name": "RowCause"},
            {"name": "CreatedById"},
            {"name": "LastModifiedById"},
            {"name": "LastModifiedDate"},
        ]
    }
    sf.LeadShare = lead_share_describe

    return sf


def _route_share_queries(query: str):
    q = query.upper()

    if "ENTITYDEFINITION" in q:
        return {
            "records": [
                {"QualifiedApiName": "AccountShare"},
                {"QualifiedApiName": "LeadShare"},
            ]
        }

    if "FROM ACCOUNTSHARE" in q:
        return {
            "records": [
                {
                    "AccountId": "001XX000001ACCT1",
                    "UserOrGroupId": "005XX000001USER1",
                    "AccountAccessLevel": "Edit",
                    "OpportunityAccessLevel": "Edit",
                    "CaseAccessLevel": "ControlledByParent",
                    "ContactAccessLevel": "None",
                    "RowCause": "Owner",
                    "attributes": {"type": "AccountShare"},
                },
                {
                    "AccountId": "001XX000001ACCT1",
                    "UserOrGroupId": "00GXX000001GRP1",
                    "AccountAccessLevel": "Read",
                    "OpportunityAccessLevel": "Read",
                    "CaseAccessLevel": "Read",
                    "ContactAccessLevel": "Read",
                    "RowCause": "Rule",
                    "attributes": {"type": "AccountShare"},
                },
            ]
        }

    if "FROM LEADSHARE" in q:
        return {
            "records": [
                {
                    "LeadId": "00QXX000001LEAD1",
                    "UserOrGroupId": "005XX000001USER1",
                    "LeadAccessLevel": "Edit",
                    "RowCause": "Owner",
                    "attributes": {"type": "LeadShare"},
                },
            ]
        }

    return {"records": []}


@pytest.fixture
def existing_ids():
    return {"005XX000001USER1", "00GXX000001GRP1"}


# =====================================================================
# Discovery tests
# =====================================================================


class TestShareDiscovery:
    def test_discovers_share_objects(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        names = collector._discover_share_object_names()
        assert "AccountShare" in names
        assert "LeadShare" in names

    def test_describe_fields(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        fields = collector._describe_fields("AccountShare")
        assert "AccountId" in fields
        assert "UserOrGroupId" in fields
        # System fields should be excluded
        assert "CreatedById" not in fields
        assert "LastModifiedById" not in fields


# =====================================================================
# Node creation tests
# =====================================================================


class TestShareRecordNodes:
    def test_creates_record_nodes(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        share_map = {
            "AccountShare": [
                {
                    "AccountId": "001XX000001ACCT1",
                    "UserOrGroupId": "005XX000001USER1",
                    "AccountAccessLevel": "Edit",
                    "RowCause": "Owner",
                },
            ]
        }
        nodes = collector._create_record_nodes(share_map)
        assert len(nodes) >= 1
        # Account node
        acct_nodes = [n for n in nodes if n.id == "001XX000001ACCT1"]
        assert len(acct_nodes) == 1
        assert "SF_Record" in acct_nodes[0].kinds
        assert "SF_Account" in acct_nodes[0].kinds

    def test_creates_synthetic_child_nodes_for_account_share(
        self, mock_sf, existing_ids
    ):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        share_map = {
            "AccountShare": [
                {
                    "AccountId": "001XX000001ACCT1",
                    "UserOrGroupId": "005XX000001USER1",
                    "AccountAccessLevel": "Edit",
                    "RowCause": "Owner",
                },
            ]
        }
        nodes = collector._create_record_nodes(share_map)
        ids = {n.id for n in nodes}
        assert "001XX000001ACCT1_Opportunity_Collection" in ids
        assert "001XX000001ACCT1_Case_Collection" in ids
        assert "001XX000001ACCT1_Contact_Collection" in ids

    def test_skips_existing_node_ids(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        share_map = {
            "LeadShare": [
                {
                    "UserId": "005XX000001USER1",  # This is an existing user ID
                    "UserOrGroupId": "00GXX000001GRP1",
                    "LeadAccessLevel": "Edit",
                    "RowCause": "Owner",
                },
            ]
        }
        nodes = collector._create_record_nodes(share_map)
        node_ids = {n.id for n in nodes}
        assert "005XX000001USER1" not in node_ids

    def test_deduplicates_record_nodes(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        share_map = {
            "LeadShare": [
                {
                    "ParentId": "00QXX",
                    "UserOrGroupId": "005A",
                    "AccessLevel": "Edit",
                    "RowCause": "Owner",
                },
                {
                    "ParentId": "00QXX",
                    "UserOrGroupId": "005B",
                    "AccessLevel": "Read",
                    "RowCause": "Rule",
                },
            ]
        }
        nodes = collector._create_record_nodes(share_map)
        lead_nodes = [n for n in nodes if n.id == "00QXX"]
        assert len(lead_nodes) == 1


# =====================================================================
# Edge creation tests
# =====================================================================


class TestShareEdges:
    def test_owner_edge(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        share_map = {
            "LeadShare": [
                {
                    "ParentId": "00QXX",
                    "UserOrGroupId": "005A",
                    "AccessLevel": "Edit",
                    "RowCause": "Owner",
                },
            ]
        }
        edges = collector._create_sharing_edges(share_map)
        owns_edges = [e for e in edges if e.kind == "Owns"]
        assert len(owns_edges) == 1
        assert owns_edges[0].source == "005A"
        assert owns_edges[0].target == "00QXX"

    def test_explicit_access_edge(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        share_map = {
            "LeadShare": [
                {
                    "ParentId": "00QXX",
                    "UserOrGroupId": "005A",
                    "AccessLevel": "Read",
                    "RowCause": "Rule",
                },
            ]
        }
        edges = collector._create_sharing_edges(share_map)
        access_edges = [e for e in edges if e.kind == "ExplicitAccess"]
        assert len(access_edges) == 1

    def test_account_share_lateral_access(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        share_map = {
            "AccountShare": [
                {
                    "AccountId": "001A",
                    "UserOrGroupId": "005A",
                    "AccountAccessLevel": "Edit",
                    "OpportunityAccessLevel": "Edit",
                    "CaseAccessLevel": "Read",
                    "ContactAccessLevel": "None",
                    "RowCause": "Owner",
                },
            ]
        }
        edges = collector._create_sharing_edges(share_map)
        # Primary edge (Owns)
        owns = [e for e in edges if e.kind == "Owns"]
        assert len(owns) == 1

        # Lateral edges for Opportunity (Edit) and Case (Read)
        lateral = [
            e for e in edges if e.kind == "ExplicitAccess" and "_Collection" in e.target
        ]
        assert len(lateral) == 2  # Opportunity + Case (Contact is "None")

    def test_controlled_by_parent_creates_inherits_access(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        share_map = {
            "AccountShare": [
                {
                    "AccountId": "001A",
                    "UserOrGroupId": "005A",
                    "AccountAccessLevel": "Edit",
                    "OpportunityAccessLevel": "ControlledByParent",
                    "CaseAccessLevel": "None",
                    "ContactAccessLevel": "None",
                    "RowCause": "Owner",
                },
            ]
        }
        edges = collector._create_sharing_edges(share_map)
        inherits = [e for e in edges if e.kind == "InheritsAccess"]
        assert len(inherits) == 1
        assert inherits[0].source == "001A_Opportunity_Collection"
        assert inherits[0].target == "001A"

    def test_skips_none_lateral_access(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        share_map = {
            "AccountShare": [
                {
                    "AccountId": "001A",
                    "UserOrGroupId": "005A",
                    "AccountAccessLevel": "Edit",
                    "OpportunityAccessLevel": "None",
                    "CaseAccessLevel": "None",
                    "ContactAccessLevel": "None",
                    "RowCause": "Owner",
                },
            ]
        }
        edges = collector._create_sharing_edges(share_map)
        lateral = [e for e in edges if "_Collection" in getattr(e, "target", "")]
        assert len(lateral) == 0


# =====================================================================
# Integration tests
# =====================================================================


class TestShareCollectorIntegration:
    def test_collect_returns_nodes_and_edges(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        nodes, edges = collector.collect()
        assert isinstance(nodes, list)
        assert isinstance(edges, list)
        assert len(nodes) > 0
        assert len(edges) > 0

    def test_collect_node_types(self, mock_sf, existing_ids):
        collector = ShareObjectCollector(mock_sf, existing_ids)
        nodes, edges = collector.collect()
        all_kinds = set()
        for node in nodes:
            all_kinds.update(node.kinds)
        assert "SF_Record" in all_kinds
