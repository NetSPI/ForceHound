"""Tests for graph merging behavior in both-mode scenarios.

When ``--collector both`` is used, the GraphBuilder merges results from
both the API and Aura collectors.  These tests verify that overlapping
nodes are merged correctly and edges are deduplicated.
"""

import pytest
from forcehound.graph.builder import GraphBuilder
from forcehound.models.base import CollectionResult, GraphEdge, GraphNode


class TestBothModeMerge:
    """Test merging of API + Aura results."""

    @pytest.fixture
    def api_result(self):
        """Simulated API collector result."""
        return CollectionResult(
            nodes=[
                GraphNode(
                    id="005XX000001USER1",
                    kinds=["SF_User", "User", "SaaS_Identity"],
                    properties={
                        "name": "Test User",
                        "email": "test@api.com",
                        "id": "005XX000001USER1",
                    },
                ),
                GraphNode(
                    id="00eXX000000PROF1",
                    kinds=["SF_Profile", "SaaS_Container", "SaaS_Entitlement"],
                    properties={"name": "System Administrator"},
                ),
                GraphNode(
                    id="00DXX0000008TEST",
                    kinds=["SF_Organization", "Organization", "SaaS_Tenant"],
                    properties={"name": "Test Org"},
                ),
                GraphNode(
                    id="0PSXX000000PS01",
                    kinds=["SF_PermissionSet", "SaaS_Entitlement"],
                    properties={"name": "TestPS", "label": "Test PS"},
                ),
            ],
            edges=[
                GraphEdge(
                    source="005XX000001USER1",
                    target="00eXX000000PROF1",
                    kind="HasProfile",
                ),
                GraphEdge(
                    source="00eXX000000PROF1",
                    target="00DXX0000008TEST",
                    kind="ModifyAllData",
                ),
                GraphEdge(
                    source="005XX000001USER1",
                    target="0PSXX000000PS01",
                    kind="HasPermissionSet",
                ),
            ],
            collector_type="api",
            org_id="00DXX0000008TEST",
        )

    @pytest.fixture
    def aura_result(self):
        """Simulated Aura collector result with overlapping user and profile."""
        return CollectionResult(
            nodes=[
                GraphNode(
                    id="005XX000001USER1",
                    kinds=["SF_User", "User", "SaaS_Identity"],
                    properties={
                        "name": "Test User",
                        "email": "test@aura.com",  # Different email (last writer wins)
                        "username": "test@example.com",  # Extra field
                        "id": "005XX000001USER1",
                    },
                ),
                GraphNode(
                    id="00eXX000000PROF1",
                    kinds=["SF_Profile", "SaaS_Container", "SaaS_Entitlement"],
                    properties={
                        "name": "System Administrator",
                        "user_type": "Standard",
                    },
                ),
                GraphNode(
                    id="00DXX0000008TEST",
                    kinds=["SF_Organization", "Organization", "SaaS_Tenant"],
                    properties={"name": "Test Org"},
                ),
                GraphNode(
                    id="00EXX000000ROLE1",
                    kinds=["SF_Role", "SaaS_Group"],
                    properties={"name": "CEO"},
                ),
                GraphNode(
                    id="ns__TestObj__c",
                    kinds=["SF_NamespacedObject", "SaaS_Resource"],
                    properties={"name": "ns__TestObj__c", "accessible_record_count": 5},
                ),
            ],
            edges=[
                GraphEdge(
                    source="005XX000001USER1",
                    target="00eXX000000PROF1",
                    kind="HasProfile",
                ),  # Duplicate
                GraphEdge(
                    source="005XX000001USER1", target="00EXX000000ROLE1", kind="HasRole"
                ),  # New
                GraphEdge(
                    source="00eXX000000PROF1",
                    target="00DXX0000008TEST",
                    kind="ModifyAllData",  # Duplicate
                ),
                GraphEdge(
                    source="00eXX000000PROF1",
                    target="00DXX0000008TEST",
                    kind="ViewAllData",  # New capability
                ),
                GraphEdge(
                    source="005XX000001USER1", target="ns__TestObj__c", kind="CanAccess"
                ),  # New
            ],
            collector_type="aura",
            org_id="00DXX0000008TEST",
        )

    def test_merged_node_count(self, api_result, aura_result):
        builder = GraphBuilder()
        builder.add_result(api_result)
        builder.add_result(aura_result)
        output = builder.build()
        nodes = output["graph"]["nodes"]
        # API: User, Profile, Org, PermissionSet (4)
        # Aura: User (merged), Profile (merged), Org (merged), Role (new), NS Object (new) → 2 new
        # Total: 4 + 2 = 6
        assert len(nodes) == 6

    def test_merged_edge_count(self, api_result, aura_result):
        builder = GraphBuilder()
        builder.add_result(api_result)
        builder.add_result(aura_result)
        output = builder.build()
        edges = output["graph"]["edges"]
        # API: HasProfile, ModifyAllData, HasPermissionSet (3)
        # Aura: HasProfile (dup), HasRole (new), ModifyAllData (dup), ViewAllData (new), CanAccess (new) → 3 new
        # Total: 3 + 3 = 6
        assert len(edges) == 6

    def test_user_node_properties_merged(self, api_result, aura_result):
        builder = GraphBuilder()
        builder.add_result(api_result)
        builder.add_result(aura_result)
        output = builder.build()

        user = next(
            n for n in output["graph"]["nodes"] if n["id"] == "005XX000001USER1"
        )
        # email should be from aura (last writer wins)
        assert user["properties"]["email"] == "test@aura.com"
        # username only in aura, should be present
        assert user["properties"]["username"] == "test@example.com"
        # name in both, last writer wins
        assert user["properties"]["name"] == "Test User"

    def test_user_node_kinds_unioned(self, api_result, aura_result):
        builder = GraphBuilder()
        builder.add_result(api_result)
        builder.add_result(aura_result)
        output = builder.build()

        user = next(
            n for n in output["graph"]["nodes"] if n["id"] == "005XX000001USER1"
        )
        # Both have same kinds, should be unioned without duplication
        # build() truncates to 2 kinds for OpenGraph compatibility
        assert user["kinds"] == ["SF_User", "User"]

    def test_profile_gets_extra_properties(self, api_result, aura_result):
        builder = GraphBuilder()
        builder.add_result(api_result)
        builder.add_result(aura_result)
        output = builder.build()

        profile = next(
            n for n in output["graph"]["nodes"] if n["id"] == "00eXX000000PROF1"
        )
        assert profile["properties"]["name"] == "System Administrator"
        # user_type comes from aura result
        assert profile["properties"]["user_type"] == "Standard"

    def test_aura_only_nodes_present(self, api_result, aura_result):
        builder = GraphBuilder()
        builder.add_result(api_result)
        builder.add_result(aura_result)
        output = builder.build()

        node_ids = {n["id"] for n in output["graph"]["nodes"]}
        assert "00EXX000000ROLE1" in node_ids  # Role from aura
        assert "ns__TestObj__c" in node_ids  # NS object from aura

    def test_api_only_nodes_present(self, api_result, aura_result):
        builder = GraphBuilder()
        builder.add_result(api_result)
        builder.add_result(aura_result)
        output = builder.build()

        node_ids = {n["id"] for n in output["graph"]["nodes"]}
        assert "0PSXX000000PS01" in node_ids  # PermissionSet from API only

    def test_summary_shows_both_collectors(self, api_result, aura_result):
        builder = GraphBuilder()
        builder.add_result(api_result)
        builder.add_result(aura_result)
        summary = builder.get_summary()
        assert summary["collectors"] == ["api", "aura"]

    def test_output_format_valid(self, api_result, aura_result):
        builder = GraphBuilder()
        builder.add_result(api_result)
        builder.add_result(aura_result)
        output = builder.build()
        assert output["metadata"]["source_kind"] == "Salesforce"
        assert "graph" in output
        assert "nodes" in output["graph"]
        assert "edges" in output["graph"]


class TestEmptyMerge:
    def test_empty_api_result(self):
        builder = GraphBuilder()
        builder.add_result(
            CollectionResult(
                nodes=[],
                edges=[],
                collector_type="api",
                org_id="00D",
            )
        )
        output = builder.build()
        assert output["graph"]["nodes"] == []
        assert output["graph"]["edges"] == []

    def test_single_result_no_merge(self):
        nodes = [
            GraphNode(id="005A", kinds=["SF_User"], properties={"name": "A"}),
            GraphNode(id="00eB", kinds=["SF_Profile"]),
        ]
        edge = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        builder = GraphBuilder()
        builder.add_result(
            CollectionResult(
                nodes=nodes,
                edges=[edge],
                collector_type="api",
                org_id="00D",
            )
        )
        output = builder.build()
        assert len(output["graph"]["nodes"]) == 2
        assert len(output["graph"]["edges"]) == 1


class TestEdgeDeduplication:
    def test_same_edge_different_properties(self):
        """First edge's properties should be kept."""
        nodes = [
            GraphNode(id="005A", kinds=["SF_User"]),
            GraphNode(id="00eB", kinds=["SF_Profile"]),
        ]
        e1 = GraphEdge(
            source="005A", target="00eB", kind="HasProfile", properties={"from": "api"}
        )
        e2 = GraphEdge(
            source="005A", target="00eB", kind="HasProfile", properties={"from": "aura"}
        )
        r1 = CollectionResult(
            nodes=list(nodes), edges=[e1], collector_type="api", org_id="00D"
        )
        r2 = CollectionResult(
            nodes=list(nodes), edges=[e2], collector_type="aura", org_id="00D"
        )

        builder = GraphBuilder()
        builder.add_result(r1)
        builder.add_result(r2)
        output = builder.build()
        edges = output["graph"]["edges"]
        assert len(edges) == 1
        # First occurrence properties should be kept (not mentioned in plan but natural behavior)

    def test_reverse_direction_not_deduplicated(self):
        """(A→B) and (B→A) are different edges."""
        nodes = [
            GraphNode(id="005A", kinds=["SF_User"]),
            GraphNode(id="00eB", kinds=["SF_Profile"]),
        ]
        e1 = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        e2 = GraphEdge(source="00eB", target="005A", kind="HasProfile")
        r1 = CollectionResult(
            nodes=nodes, edges=[e1, e2], collector_type="api", org_id="00D"
        )

        builder = GraphBuilder()
        builder.add_result(r1)
        output = builder.build()
        assert len(output["graph"]["edges"]) == 2
