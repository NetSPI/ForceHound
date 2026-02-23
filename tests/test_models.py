"""Tests for forcehound.models.base and forcehound.models.auth."""

import pytest
from forcehound.models.base import GraphNode, GraphEdge, CollectionResult
from forcehound.models.auth import AuthConfig


# =====================================================================
# GraphNode tests
# =====================================================================


class TestGraphNode:
    def test_basic_creation(self):
        node = GraphNode(id="005TEST", kinds=["SF_User"], properties={"name": "Test"})
        assert node.id == "005TEST"
        assert node.kinds == ["SF_User"]
        assert node.properties == {"name": "Test"}

    def test_default_properties(self):
        node = GraphNode(id="005TEST", kinds=["SF_User"])
        assert node.properties == {}

    def test_to_dict(self):
        node = GraphNode(
            id="005TEST", kinds=["SF_User", "User"], properties={"name": "Test"}
        )
        d = node.to_dict()
        assert d == {
            "id": "005TEST",
            "kinds": ["SF_User", "User"],
            "properties": {"name": "Test"},
        }

    def test_to_dict_returns_copies(self):
        node = GraphNode(id="005TEST", kinds=["SF_User"], properties={"name": "Test"})
        d = node.to_dict()
        d["kinds"].append("EXTRA")
        d["properties"]["extra"] = True
        assert "EXTRA" not in node.kinds
        assert "extra" not in node.properties

    def test_multiple_kinds(self):
        node = GraphNode(id="005TEST", kinds=["SF_User", "User", "SaaS_Identity"])
        assert len(node.kinds) == 3

    def test_empty_properties(self):
        node = GraphNode(id="005TEST", kinds=["SF_User"], properties={})
        assert node.to_dict()["properties"] == {}

    def test_to_dict_filters_none_properties(self):
        node = GraphNode(
            id="005TEST",
            kinds=["SF_User"],
            properties={"name": "Test", "url": None, "count": 0, "active": False},
        )
        d = node.to_dict()
        assert d["properties"] == {"name": "Test", "count": 0, "active": False}
        assert "url" not in d["properties"]


# =====================================================================
# GraphEdge tests
# =====================================================================


class TestGraphEdge:
    def test_basic_creation(self):
        edge = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        assert edge.source == "005A"
        assert edge.target == "00eB"
        assert edge.kind == "HasProfile"

    def test_default_properties(self):
        edge = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        assert edge.properties == {}

    def test_dedup_key(self):
        edge = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        assert edge.dedup_key == ("005A", "HasProfile", "00eB")

    def test_dedup_key_different_for_different_kinds(self):
        e1 = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        e2 = GraphEdge(source="005A", target="00eB", kind="HasRole")
        assert e1.dedup_key != e2.dedup_key

    def test_dedup_key_same_for_same_edges(self):
        e1 = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        e2 = GraphEdge(
            source="005A", target="00eB", kind="HasProfile", properties={"extra": True}
        )
        assert e1.dedup_key == e2.dedup_key

    def test_to_dict_no_properties(self):
        edge = GraphEdge(source="005A", target="00eB", kind="HasProfile")
        d = edge.to_dict()
        assert d == {
            "start": {"value": "005A", "match_by": "id"},
            "end": {"value": "00eB", "match_by": "id"},
            "kind": "HasProfile",
        }
        assert "properties" not in d

    def test_to_dict_with_properties(self):
        edge = GraphEdge(
            source="005A",
            target="00eB",
            kind="ExplicitAccess",
            properties={"access_level": "Edit"},
        )
        d = edge.to_dict()
        assert d["start"] == {"value": "005A", "match_by": "id"}
        assert d["end"] == {"value": "00eB", "match_by": "id"}
        assert d["properties"] == {"access_level": "Edit"}

    def test_to_dict_returns_copy_of_properties(self):
        edge = GraphEdge(
            source="005A",
            target="00eB",
            kind="ExplicitAccess",
            properties={"access_level": "Edit"},
        )
        d = edge.to_dict()
        d["properties"]["extra"] = True
        assert "extra" not in edge.properties


# =====================================================================
# CollectionResult tests
# =====================================================================


class TestCollectionResult:
    def test_basic_creation(self):
        result = CollectionResult(
            nodes=[GraphNode(id="005A", kinds=["SF_User"])],
            edges=[GraphEdge(source="005A", target="00eB", kind="HasProfile")],
            collector_type="api",
            org_id="00DXX0000008TEST",
        )
        assert len(result.nodes) == 1
        assert len(result.edges) == 1
        assert result.collector_type == "api"
        assert result.org_id == "00DXX0000008TEST"

    def test_default_metadata(self):
        result = CollectionResult(
            nodes=[], edges=[], collector_type="aura", org_id="00D"
        )
        assert result.metadata == {}

    def test_with_metadata(self):
        result = CollectionResult(
            nodes=[],
            edges=[],
            collector_type="aura",
            org_id="00D",
            metadata={"users": 42},
        )
        assert result.metadata["users"] == 42

    def test_empty_result(self):
        result = CollectionResult(
            nodes=[], edges=[], collector_type="api", org_id="00D"
        )
        assert len(result.nodes) == 0
        assert len(result.edges) == 0


# =====================================================================
# AuthConfig tests
# =====================================================================


class TestAuthConfig:
    def test_basic_creation(self):
        auth = AuthConfig(
            instance_url="https://test.my.salesforce.com",
            session_id="00D!TOKEN",
        )
        assert auth.instance_url == "https://test.my.salesforce.com"
        assert auth.session_id == "00D!TOKEN"

    def test_strips_trailing_slash(self):
        auth = AuthConfig(instance_url="https://test.my.salesforce.com/")
        assert auth.instance_url == "https://test.my.salesforce.com"

    def test_strips_multiple_trailing_slashes(self):
        auth = AuthConfig(instance_url="https://test.my.salesforce.com///")
        assert auth.instance_url == "https://test.my.salesforce.com"

    def test_url_decode_aura_context(self):
        encoded = "%7B%22mode%22%3A%22PRODDEBUG%22%7D"
        auth = AuthConfig(aura_context=encoded)
        assert auth.aura_context == '{"mode":"PRODDEBUG"}'

    def test_url_decode_aura_token(self):
        encoded = "eyJ%3Dtest%3D"
        auth = AuthConfig(aura_token=encoded)
        assert auth.aura_token == "eyJ=test="

    def test_no_decode_when_no_percent(self):
        raw = '{"mode":"PRODDEBUG"}'
        auth = AuthConfig(aura_context=raw)
        assert auth.aura_context == raw

    def test_validate_for_api_success_session(self):
        auth = AuthConfig(
            instance_url="https://test.my.salesforce.com", session_id="TOKEN"
        )
        auth.validate_for_api()  # Should not raise

    def test_validate_for_api_success_creds(self):
        auth = AuthConfig(
            instance_url="https://test.my.salesforce.com",
            username="user",
            password="pass",
        )
        auth.validate_for_api()  # Should not raise

    def test_validate_for_api_missing_url(self):
        auth = AuthConfig(session_id="TOKEN")
        with pytest.raises(ValueError, match="instance_url"):
            auth.validate_for_api()

    def test_validate_for_api_missing_auth(self):
        auth = AuthConfig(instance_url="https://test.my.salesforce.com")
        with pytest.raises(ValueError, match="session_id or username"):
            auth.validate_for_api()

    def test_validate_for_aura_success(self):
        auth = AuthConfig(
            instance_url="https://test.lightning.force.com",
            session_id="TOKEN",
            aura_context='{"mode":"PRODDEBUG"}',
            aura_token="eyJtoken",
        )
        auth.validate_for_aura()  # Should not raise

    def test_validate_for_aura_missing_fields(self):
        auth = AuthConfig(instance_url="https://test.lightning.force.com")
        with pytest.raises(ValueError, match="session_id"):
            auth.validate_for_aura()

    def test_validate_for_aura_missing_context(self):
        auth = AuthConfig(
            instance_url="https://test.lightning.force.com",
            session_id="TOKEN",
            aura_token="eyJtoken",
        )
        with pytest.raises(ValueError, match="aura_context"):
            auth.validate_for_aura()

    def test_default_empty_strings(self):
        auth = AuthConfig()
        assert auth.instance_url == ""
        assert auth.session_id == ""
        assert auth.username == ""
        assert auth.password == ""
        assert auth.security_token == ""
        assert auth.aura_context == ""
        assert auth.aura_token == ""
