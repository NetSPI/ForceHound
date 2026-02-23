"""Tests for forcehound.graph.builder."""

import json
import os
import tempfile

from forcehound.graph.builder import GraphBuilder
from forcehound.models.base import CollectionResult, GraphEdge, GraphNode


class TestGraphBuilderBasic:
    def test_empty_build(self):
        builder = GraphBuilder()
        output = builder.build()
        assert output["metadata"]["source_kind"] == "Salesforce"
        assert "graph" in output
        assert output["graph"]["nodes"] == []
        assert output["graph"]["edges"] == []

    def test_add_single_result(self, sample_collection_result):
        builder = GraphBuilder()
        builder.add_result(sample_collection_result)
        output = builder.build()
        assert len(output["graph"]["nodes"]) == 2
        assert len(output["graph"]["edges"]) == 1

    def test_node_to_dict_format(self, sample_collection_result):
        builder = GraphBuilder()
        builder.add_result(sample_collection_result)
        output = builder.build()
        node = output["graph"]["nodes"][0]
        assert "id" in node
        assert "kinds" in node
        assert "properties" in node

    def test_edge_to_dict_format(self, sample_collection_result):
        builder = GraphBuilder()
        builder.add_result(sample_collection_result)
        output = builder.build()
        edge = output["graph"]["edges"][0]
        assert "start" in edge
        assert edge["start"]["match_by"] == "id"
        assert "end" in edge
        assert edge["end"]["match_by"] == "id"
        assert "kind" in edge

    def test_opengraph_v1_meta(self):
        builder = GraphBuilder()
        output = builder.build()
        assert output["metadata"]["source_kind"] == "Salesforce"


class TestGraphBuilderMerge:
    def test_node_merge_by_id(self):
        """Nodes with same ID should be merged."""
        r1 = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "Alice"})
            ],
            edges=[],
            collector_type="api",
            org_id="00D",
        )
        r2 = CollectionResult(
            nodes=[
                GraphNode(
                    id="005A", kinds=["SaaS_Identity"], properties={"email": "a@b.com"}
                )
            ],
            edges=[],
            collector_type="aura",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r1)
        builder.add_result(r2)
        output = builder.build()

        nodes = output["graph"]["nodes"]
        assert len(nodes) == 1
        assert "SF_User" in nodes[0]["kinds"]
        assert "SaaS_Identity" in nodes[0]["kinds"]
        assert nodes[0]["properties"]["name"] == "Alice"
        assert nodes[0]["properties"]["email"] == "a@b.com"

    def test_node_merge_kinds_union_preserves_order(self):
        r1 = CollectionResult(
            nodes=[GraphNode(id="005A", kinds=["SF_User", "User"])],
            edges=[],
            collector_type="api",
            org_id="00D",
        )
        r2 = CollectionResult(
            nodes=[GraphNode(id="005A", kinds=["User", "SaaS_Identity"])],
            edges=[],
            collector_type="aura",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r1)
        builder.add_result(r2)
        output = builder.build()

        kinds = output["graph"]["nodes"][0]["kinds"]
        # build() truncates to 2 kinds for OpenGraph compatibility
        assert kinds == ["SF_User", "User"]

    def test_node_merge_properties_last_writer_wins(self):
        r1 = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "Alice"})
            ],
            edges=[],
            collector_type="api",
            org_id="00D",
        )
        r2 = CollectionResult(
            nodes=[GraphNode(id="005A", kinds=["SF_User"], properties={"name": "Bob"})],
            edges=[],
            collector_type="aura",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r1)
        builder.add_result(r2)
        output = builder.build()
        assert output["graph"]["nodes"][0]["properties"]["name"] == "Bob"

    def test_edge_dedup(self):
        """Duplicate edges (same source/kind/target) should be deduplicated."""
        nodes = [
            GraphNode(id="005A", kinds=["SF_User"]),
            GraphNode(id="00eB", kinds=["SF_Profile"]),
        ]
        edge1 = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        edge2 = GraphEdge(
            source="005A", target="00eB", kind="HasProfile", properties={"extra": True}
        )
        r1 = CollectionResult(
            nodes=nodes, edges=[edge1], collector_type="api", org_id="00D"
        )
        r2 = CollectionResult(
            nodes=nodes, edges=[edge2], collector_type="aura", org_id="00D"
        )

        builder = GraphBuilder()
        builder.add_result(r1)
        builder.add_result(r2)
        output = builder.build()
        assert len(output["graph"]["edges"]) == 1

    def test_different_edges_kept(self):
        nodes = [
            GraphNode(id="005A", kinds=["SF_User"]),
            GraphNode(id="00eB", kinds=["SF_Profile"]),
            GraphNode(id="00EX", kinds=["SF_Role"]),
        ]
        edge1 = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        edge2 = GraphEdge(source="005A", target="00EX", kind="HasRole")
        r1 = CollectionResult(
            nodes=nodes, edges=[edge1, edge2], collector_type="api", org_id="00D"
        )

        builder = GraphBuilder()
        builder.add_result(r1)
        output = builder.build()
        assert len(output["graph"]["edges"]) == 2


class TestGraphBuilderDanglingEdges:
    def test_build_strips_dangling_start(self):
        """Edge whose source node doesn't exist is dropped."""
        node = GraphNode(id="00eB", kinds=["SF_Profile"])
        edge = GraphEdge(source="MISSING", target="00eB", kind="HasProfile")
        r = CollectionResult(
            nodes=[node], edges=[edge], collector_type="api", org_id="00D"
        )
        builder = GraphBuilder()
        builder.add_result(r)
        output = builder.build()
        assert len(output["graph"]["edges"]) == 0

    def test_build_strips_dangling_end(self):
        """Edge whose target node doesn't exist is dropped."""
        node = GraphNode(id="005A", kinds=["SF_User"])
        edge = GraphEdge(source="005A", target="MISSING", kind="HasProfile")
        r = CollectionResult(
            nodes=[node], edges=[edge], collector_type="api", org_id="00D"
        )
        builder = GraphBuilder()
        builder.add_result(r)
        output = builder.build()
        assert len(output["graph"]["edges"]) == 0

    def test_build_keeps_valid_edges(self):
        """Edge with both endpoints present survives validation."""
        nodes = [
            GraphNode(id="005A", kinds=["SF_User"]),
            GraphNode(id="00eB", kinds=["SF_Profile"]),
        ]
        edge = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        r = CollectionResult(
            nodes=nodes, edges=[edge], collector_type="api", org_id="00D"
        )
        builder = GraphBuilder()
        builder.add_result(r)
        output = builder.build()
        assert len(output["graph"]["edges"]) == 1

    def test_build_mixed_valid_and_dangling(self):
        """Only valid edges survive; dangling edges are stripped."""
        nodes = [
            GraphNode(id="005A", kinds=["SF_User"]),
            GraphNode(id="00eB", kinds=["SF_Profile"]),
        ]
        valid_edge = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        dangling_edge = GraphEdge(source="005A", target="MISSING", kind="HasRole")
        r = CollectionResult(
            nodes=nodes,
            edges=[valid_edge, dangling_edge],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        output = builder.build()
        assert len(output["graph"]["edges"]) == 1
        assert output["graph"]["edges"][0]["kind"] == "HasProfile"


class TestBuildOutput:
    def test_build_metadata(self):
        builder = GraphBuilder()
        output = builder.build()
        assert output["metadata"]["source_kind"] == "Salesforce"
        assert "meta" not in output
        assert output["graph"]["nodes"] == []
        assert output["graph"]["edges"] == []

    def test_build_truncates_kinds(self):
        """Nodes with 3 kinds should be truncated to first 2."""
        r = CollectionResult(
            nodes=[
                GraphNode(
                    id="005A",
                    kinds=["SF_User", "User", "SaaS_Identity"],
                    properties={"name": "Alice"},
                )
            ],
            edges=[],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        output = builder.build()
        kinds = output["graph"]["nodes"][0]["kinds"]
        assert kinds == ["SF_User", "User"]

    def test_build_preserves_2_kinds(self):
        """Nodes with 2 kinds should be unchanged."""
        r = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_PermissionSet", "SaaS_Entitlement"])
            ],
            edges=[],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        output = builder.build()
        kinds = output["graph"]["nodes"][0]["kinds"]
        assert kinds == ["SF_PermissionSet", "SaaS_Entitlement"]

    def test_build_preserves_1_kind(self):
        """Nodes with 1 kind should be unchanged."""
        r = CollectionResult(
            nodes=[GraphNode(id="REC1", kinds=["SF_Record"])],
            edges=[],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        output = builder.build()
        kinds = output["graph"]["nodes"][0]["kinds"]
        assert kinds == ["SF_Record"]

    def test_build_injects_objectid(self):
        """Every node gets objectid in properties, set to the node's id."""
        r = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "Alice"}),
                GraphNode(
                    id="00eB", kinds=["SF_Profile"], properties={"name": "Admin"}
                ),
            ],
            edges=[],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        output = builder.build()
        for node in output["graph"]["nodes"]:
            assert "objectid" in node["properties"]
            assert node["properties"]["objectid"] == node["id"]

    def test_build_strips_dangling_edges(self):
        """Dangling edges are dropped during build."""
        node = GraphNode(id="005A", kinds=["SF_User"])
        edge = GraphEdge(source="005A", target="MISSING", kind="HasProfile")
        r = CollectionResult(
            nodes=[node], edges=[edge], collector_type="api", org_id="00D"
        )
        builder = GraphBuilder()
        builder.add_result(r)
        output = builder.build()
        assert len(output["graph"]["edges"]) == 0

class TestGraphBuilderSave:
    def test_save_creates_valid_json(self, sample_collection_result):
        builder = GraphBuilder()
        builder.add_result(sample_collection_result)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name

        try:
            builder.save(path)
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            assert data["metadata"]["source_kind"] == "Salesforce"
            assert len(data["graph"]["nodes"]) == 2
        finally:
            os.unlink(path)

    def test_save_with_indent(self, sample_collection_result):
        builder = GraphBuilder()
        builder.add_result(sample_collection_result)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name

        try:
            builder.save(path, indent=4)
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            assert "    " in content  # 4-space indent
        finally:
            os.unlink(path)


class TestGraphBuilderSummary:
    def test_summary_counts(self, sample_collection_result):
        builder = GraphBuilder()
        builder.add_result(sample_collection_result)
        summary = builder.get_summary()
        assert summary["total_nodes"] == 2
        assert summary["total_edges"] == 1

    def test_summary_node_kinds(self, sample_collection_result):
        builder = GraphBuilder()
        builder.add_result(sample_collection_result)
        summary = builder.get_summary()
        assert "SF_User" in summary["node_kinds"]
        assert "SF_Profile" in summary["node_kinds"]

    def test_summary_edge_kinds(self, sample_collection_result):
        builder = GraphBuilder()
        builder.add_result(sample_collection_result)
        summary = builder.get_summary()
        assert "HasProfile" in summary["edge_kinds"]

    def test_summary_collectors(self, sample_collection_result):
        builder = GraphBuilder()
        builder.add_result(sample_collection_result)
        summary = builder.get_summary()
        assert "api" in summary["collectors"]

    def test_summary_empty(self):
        builder = GraphBuilder()
        summary = builder.get_summary()
        assert summary["total_nodes"] == 0
        assert summary["total_edges"] == 0
        assert summary["collectors"] == []

    def test_summary_multiple_collectors(self):
        r1 = CollectionResult(nodes=[], edges=[], collector_type="api", org_id="00D")
        r2 = CollectionResult(nodes=[], edges=[], collector_type="aura", org_id="00D")
        builder = GraphBuilder()
        builder.add_result(r1)
        builder.add_result(r2)
        summary = builder.get_summary()
        assert summary["collectors"] == ["api", "aura"]


class TestRiskSummary:
    def test_empty_graph(self):
        builder = GraphBuilder()
        assert builder.get_risk_summary() == {}

    def test_user_with_no_capabilities(self):
        """User with a profile that has no capability edges."""
        r = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "Alice"}),
                GraphNode(
                    id="00eA",
                    kinds=["SF_Profile", "SaaS_Container"],
                    properties={"name": "Read Only"},
                ),
            ],
            edges=[GraphEdge(source="005A", target="00eA", kind="HasProfile")],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        assert builder.get_risk_summary() == {}

    def test_user_capability_via_profile(self):
        r = CollectionResult(
            nodes=[
                GraphNode(
                    id="005A",
                    kinds=["SF_User"],
                    properties={"name": "Alice", "email": "a@b.com", "is_active": True},
                ),
                GraphNode(
                    id="00eA",
                    kinds=["SF_Profile", "SaaS_Container"],
                    properties={"name": "SysAdmin"},
                ),
                GraphNode(
                    id="00D", kinds=["SF_Organization"], properties={"name": "Org"}
                ),
            ],
            edges=[
                GraphEdge(source="005A", target="00eA", kind="HasProfile"),
                GraphEdge(source="00eA", target="00D", kind="ModifyAllData"),
            ],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        risk = builder.get_risk_summary()

        assert "005A" in risk
        assert risk["005A"]["name"] == "Alice"
        assert "ModifyAllData" in risk["005A"]["capabilities"]
        sources = risk["005A"]["capabilities"]["ModifyAllData"]
        assert sources == [("SysAdmin", "Profile")]

    def test_user_capability_via_permission_set(self):
        r = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "Bob"}),
                GraphNode(
                    id="0PSA",
                    kinds=["SF_PermissionSet", "SaaS_Entitlement"],
                    properties={"name": "AdminPS"},
                ),
                GraphNode(
                    id="00D", kinds=["SF_Organization"], properties={"name": "Org"}
                ),
            ],
            edges=[
                GraphEdge(source="005A", target="0PSA", kind="HasPermissionSet"),
                GraphEdge(source="0PSA", target="00D", kind="ManageUsers"),
            ],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        risk = builder.get_risk_summary()

        assert "005A" in risk
        sources = risk["005A"]["capabilities"]["ManageUsers"]
        assert sources == [("AdminPS", "PermissionSet")]

    def test_user_capability_via_psg(self):
        r = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "Carol"}),
                GraphNode(
                    id="0PGA",
                    kinds=["SF_PermissionSet", "SaaS_Entitlement"],
                    properties={"name": "AdminPSG", "label": "Admin PSG"},
                ),
                GraphNode(
                    id="00D", kinds=["SF_Organization"], properties={"name": "Org"}
                ),
            ],
            edges=[
                GraphEdge(source="005A", target="0PGA", kind="HasPermissionSet"),
                GraphEdge(source="0PGA", target="00D", kind="ResetPasswords"),
            ],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        risk = builder.get_risk_summary()

        sources = risk["005A"]["capabilities"]["ResetPasswords"]
        assert sources == [("AdminPSG", "PermissionSetGroup")]

    def test_same_capability_multiple_sources(self):
        r = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "Dave"}),
                GraphNode(
                    id="00eA", kinds=["SF_Profile"], properties={"name": "SysAdmin"}
                ),
                GraphNode(
                    id="0PSA",
                    kinds=["SF_PermissionSet"],
                    properties={"name": "ExtraPS"},
                ),
                GraphNode(
                    id="00D", kinds=["SF_Organization"], properties={"name": "Org"}
                ),
            ],
            edges=[
                GraphEdge(source="005A", target="00eA", kind="HasProfile"),
                GraphEdge(source="005A", target="0PSA", kind="HasPermissionSet"),
                GraphEdge(source="00eA", target="00D", kind="ViewAllData"),
                GraphEdge(source="0PSA", target="00D", kind="ViewAllData"),
            ],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        risk = builder.get_risk_summary()

        sources = risk["005A"]["capabilities"]["ViewAllData"]
        assert len(sources) == 2
        assert ("SysAdmin", "Profile") in sources
        assert ("ExtraPS", "PermissionSet") in sources

    def test_inactive_user_flagged(self):
        r = CollectionResult(
            nodes=[
                GraphNode(
                    id="005A",
                    kinds=["SF_User"],
                    properties={"name": "Ex-Admin", "is_active": False},
                ),
                GraphNode(
                    id="00eA", kinds=["SF_Profile"], properties={"name": "SysAdmin"}
                ),
                GraphNode(
                    id="00D", kinds=["SF_Organization"], properties={"name": "Org"}
                ),
            ],
            edges=[
                GraphEdge(source="005A", target="00eA", kind="HasProfile"),
                GraphEdge(source="00eA", target="00D", kind="ModifyAllData"),
            ],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        risk = builder.get_risk_summary()

        assert risk["005A"]["is_active"] is False

    def test_multiple_users(self):
        r = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "Alice"}),
                GraphNode(id="005B", kinds=["SF_User"], properties={"name": "Bob"}),
                GraphNode(
                    id="00eA", kinds=["SF_Profile"], properties={"name": "Admin"}
                ),
                GraphNode(
                    id="00eB", kinds=["SF_Profile"], properties={"name": "ReadOnly"}
                ),
                GraphNode(
                    id="00D", kinds=["SF_Organization"], properties={"name": "Org"}
                ),
            ],
            edges=[
                GraphEdge(source="005A", target="00eA", kind="HasProfile"),
                GraphEdge(source="005B", target="00eB", kind="HasProfile"),
                GraphEdge(source="00eA", target="00D", kind="ModifyAllData"),
                # 00eB has no capability edges
            ],
            collector_type="api",
            org_id="00D",
        )
        builder = GraphBuilder()
        builder.add_result(r)
        risk = builder.get_risk_summary()

        assert "005A" in risk  # Alice has ModifyAllData
        assert "005B" not in risk  # Bob has no capabilities
