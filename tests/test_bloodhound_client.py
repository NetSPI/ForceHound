"""Tests for forcehound.bloodhound.client."""

import base64
import datetime
import hashlib
import hmac
import json
from unittest.mock import MagicMock, patch

import pytest

from forcehound.bloodhound.client import BloodHoundClient, BloodHoundAPIError


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def client():
    return BloodHoundClient(
        base_url="http://localhost:8080",
        token_id="test-token-id",
        token_key="dGVzdC10b2tlbi1rZXk=",  # base64("test-token-key")
    )


@pytest.fixture
def fixed_time():
    return datetime.datetime(2026, 2, 18, 12, 30, 45, tzinfo=datetime.timezone.utc)


# =====================================================================
# HMAC signing tests
# =====================================================================


class TestHMACSigning:
    def test_sign_returns_required_headers(self, client, fixed_time):
        with patch("forcehound.bloodhound.client.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = fixed_time
            mock_dt.timezone = datetime.timezone
            headers = client._sign("POST", "/api/v2/clear-database")

        assert "Authorization" in headers
        assert headers["Authorization"] == "bhesignature test-token-id"
        assert "RequestDate" in headers
        assert "Signature" in headers
        assert "User-Agent" in headers
        assert headers["User-Agent"] == "forcehound 0.1"

    def test_sign_request_date_format(self, client, fixed_time):
        with patch("forcehound.bloodhound.client.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = fixed_time
            mock_dt.timezone = datetime.timezone
            headers = client._sign("POST", "/api/v2/file-upload/start")

        assert headers["RequestDate"] == "2026-02-18T12:30:45Z"

    def test_sign_deterministic(self, client, fixed_time):
        """Same inputs produce same signature."""
        with patch("forcehound.bloodhound.client.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = fixed_time
            mock_dt.timezone = datetime.timezone
            sig1 = client._sign("POST", "/api/v2/clear-database")["Signature"]

        with patch("forcehound.bloodhound.client.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = fixed_time
            mock_dt.timezone = datetime.timezone
            sig2 = client._sign("POST", "/api/v2/clear-database")["Signature"]

        assert sig1 == sig2

    def test_sign_different_paths_differ(self, client, fixed_time):
        """Different paths produce different signatures."""
        with patch("forcehound.bloodhound.client.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = fixed_time
            mock_dt.timezone = datetime.timezone
            sig1 = client._sign("POST", "/api/v2/clear-database")["Signature"]

        with patch("forcehound.bloodhound.client.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = fixed_time
            mock_dt.timezone = datetime.timezone
            sig2 = client._sign("POST", "/api/v2/file-upload/start")["Signature"]

        assert sig1 != sig2

    def test_sign_with_body_differs_from_without(self, client, fixed_time):
        """Body content affects the signature."""
        body = b'{"test": true}'
        with patch("forcehound.bloodhound.client.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = fixed_time
            mock_dt.timezone = datetime.timezone
            sig_with = client._sign("POST", "/api/v2/test", body)["Signature"]

        with patch("forcehound.bloodhound.client.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = fixed_time
            mock_dt.timezone = datetime.timezone
            sig_without = client._sign("POST", "/api/v2/test")["Signature"]

        assert sig_with != sig_without

    def test_sign_chain_matches_reference(self, client, fixed_time):
        """Verify the 3-link HMAC chain produces expected output."""
        with patch("forcehound.bloodhound.client.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = fixed_time
            mock_dt.timezone = datetime.timezone
            headers = client._sign("POST", "/api/v2/clear-database", b'{"x":1}')

        # Manually compute expected signature
        key = client.token_key.encode("utf-8")
        d1 = hmac.new(key, digestmod=hashlib.sha256)
        d1.update(b"POST/api/v2/clear-database")
        d2 = hmac.new(d1.digest(), digestmod=hashlib.sha256)
        d2.update(b"2026-02-18T12")  # datetime[:13]
        d3 = hmac.new(d2.digest(), digestmod=hashlib.sha256)
        d3.update(b'{"x":1}')
        expected = base64.b64encode(d3.digest()).decode("utf-8")

        assert headers["Signature"] == expected


# =====================================================================
# clear_database tests
# =====================================================================


class TestClearDatabase:
    def test_clear_database_success(self, client):
        mock_resp = MagicMock()
        mock_resp.status = 204

        with patch(
            "forcehound.bloodhound.client.urlopen", return_value=mock_resp
        ) as mock_urlopen:
            client.clear_database()

        req = mock_urlopen.call_args[0][0]
        assert req.get_method() == "POST"
        assert req.full_url == "http://localhost:8080/api/v2/clear-database"
        body = json.loads(req.data.decode("utf-8"))
        assert body["deleteCollectedGraphData"] is True
        assert body["deleteFileIngestHistory"] is True
        assert body["deleteDataQualityHistory"] is True

    def test_clear_database_partial_flags(self, client):
        mock_resp = MagicMock()
        mock_resp.status = 204

        with patch(
            "forcehound.bloodhound.client.urlopen", return_value=mock_resp
        ) as mock_urlopen:
            client.clear_database(
                graph=True, ingest_history=False, quality_history=False
            )

        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        assert body["deleteCollectedGraphData"] is True
        assert body["deleteFileIngestHistory"] is False
        assert body["deleteDataQualityHistory"] is False

    def test_clear_database_failure(self, client):
        mock_resp = MagicMock()
        mock_resp.status = 401
        mock_resp.read.return_value = b"Unauthorized"

        with patch("forcehound.bloodhound.client.urlopen", return_value=mock_resp):
            with pytest.raises(BloodHoundAPIError, match="clear-database failed: 401"):
                client.clear_database()


# =====================================================================
# upload_graph tests
# =====================================================================


class TestUploadGraph:
    def test_upload_graph_success(self, client, tmp_path):
        graph_data = {
            "graph": {"nodes": [], "edges": []},
            "metadata": {"source_kind": "Salesforce"},
        }
        graph_file = tmp_path / "test_output.json"
        graph_file.write_text(json.dumps(graph_data))

        # Mock three sequential responses: start(201), upload(202), end(200)
        start_resp = MagicMock()
        start_resp.status = 201
        start_resp.read.return_value = json.dumps({"data": {"id": 42}}).encode("utf-8")

        upload_resp = MagicMock()
        upload_resp.status = 202

        end_resp = MagicMock()
        end_resp.status = 200

        with patch(
            "forcehound.bloodhound.client.urlopen",
            side_effect=[start_resp, upload_resp, end_resp],
        ) as mock_urlopen:
            job_id = client.upload_graph(str(graph_file))

        assert job_id == 42

        # Verify the three requests
        calls = mock_urlopen.call_args_list
        assert len(calls) == 3

        # Start request
        start_req = calls[0][0][0]
        assert start_req.get_method() == "POST"
        assert "/api/v2/file-upload/start" in start_req.full_url

        # Upload request
        upload_req = calls[1][0][0]
        assert upload_req.get_method() == "POST"
        assert "/api/v2/file-upload/42" in upload_req.full_url
        assert json.loads(upload_req.data.decode("utf-8")) == graph_data
        assert upload_req.get_header("X-file-upload-name") == "test_output.json"

        # End request
        end_req = calls[2][0][0]
        assert end_req.get_method() == "POST"
        assert "/api/v2/file-upload/42/end" in end_req.full_url

    def test_upload_graph_custom_file_name(self, client, tmp_path):
        graph_file = tmp_path / "test_output.json"
        graph_file.write_text("{}")

        start_resp = MagicMock()
        start_resp.status = 201
        start_resp.read.return_value = json.dumps({"data": {"id": 1}}).encode("utf-8")

        upload_resp = MagicMock()
        upload_resp.status = 202

        end_resp = MagicMock()
        end_resp.status = 200

        with patch(
            "forcehound.bloodhound.client.urlopen",
            side_effect=[start_resp, upload_resp, end_resp],
        ) as mock_urlopen:
            client.upload_graph(str(graph_file), file_name="MyOrg_Production.json")

        upload_req = mock_urlopen.call_args_list[1][0][0]
        assert upload_req.get_header("X-file-upload-name") == "MyOrg_Production.json"

    def test_upload_graph_start_failure(self, client, tmp_path):
        graph_file = tmp_path / "test.json"
        graph_file.write_text("{}")

        mock_resp = MagicMock()
        mock_resp.status = 500
        mock_resp.read.return_value = b"Internal Server Error"

        with patch("forcehound.bloodhound.client.urlopen", return_value=mock_resp):
            with pytest.raises(
                BloodHoundAPIError, match="file-upload/start failed: 500"
            ):
                client.upload_graph(str(graph_file))

    def test_upload_graph_upload_failure(self, client, tmp_path):
        graph_file = tmp_path / "test.json"
        graph_file.write_text("{}")

        start_resp = MagicMock()
        start_resp.status = 201
        start_resp.read.return_value = json.dumps({"data": {"id": 7}}).encode("utf-8")

        upload_resp = MagicMock()
        upload_resp.status = 400
        upload_resp.read.return_value = b"Bad Request"

        with patch(
            "forcehound.bloodhound.client.urlopen",
            side_effect=[start_resp, upload_resp],
        ):
            with pytest.raises(BloodHoundAPIError, match="file-upload/7 failed: 400"):
                client.upload_graph(str(graph_file))

    def test_upload_graph_end_failure(self, client, tmp_path):
        graph_file = tmp_path / "test.json"
        graph_file.write_text("{}")

        start_resp = MagicMock()
        start_resp.status = 201
        start_resp.read.return_value = json.dumps({"data": {"id": 7}}).encode("utf-8")

        upload_resp = MagicMock()
        upload_resp.status = 202

        end_resp = MagicMock()
        end_resp.status = 404
        end_resp.read.return_value = b"Not Found"

        with patch(
            "forcehound.bloodhound.client.urlopen",
            side_effect=[start_resp, upload_resp, end_resp],
        ):
            with pytest.raises(
                BloodHoundAPIError, match="file-upload/7/end failed: 404"
            ):
                client.upload_graph(str(graph_file))


# =====================================================================
# Base URL handling
# =====================================================================


class TestRegisterCustomNodes:
    def test_register_success(self, client):
        mock_resp = MagicMock()
        mock_resp.status = 201

        with patch(
            "forcehound.bloodhound.client.urlopen", return_value=mock_resp
        ) as mock_urlopen:
            result = client.register_custom_nodes()

        # Returns sorted list of type names
        assert result == sorted(result)
        assert "SF_User" in result
        assert "SF_Profile" in result
        assert len(result) == 12

        # Verify request body structure
        req = mock_urlopen.call_args[0][0]
        assert req.get_method() == "POST"
        assert req.full_url == "http://localhost:8080/api/v2/custom-nodes"
        body = json.loads(req.data.decode("utf-8"))
        assert "custom_types" in body
        assert body["custom_types"]["SF_User"]["icon"]["type"] == "font-awesome"
        assert body["custom_types"]["SF_User"]["icon"]["name"] == "user"
        assert body["custom_types"]["SF_User"]["icon"]["color"] == "#17A2B8"

    def test_register_conflict_treated_as_success(self, client):
        mock_resp = MagicMock()
        mock_resp.status = 409

        with patch("forcehound.bloodhound.client.urlopen", return_value=mock_resp):
            result = client.register_custom_nodes()

        assert isinstance(result, list)
        assert len(result) == 12

    def test_register_server_error_raises(self, client):
        mock_resp = MagicMock()
        mock_resp.status = 500
        mock_resp.read.return_value = b"Internal Server Error"

        with patch("forcehound.bloodhound.client.urlopen", return_value=mock_resp):
            with pytest.raises(
                BloodHoundAPIError, match="custom-nodes registration failed: 500"
            ):
                client.register_custom_nodes()


class TestBaseURL:
    def test_trailing_slash_stripped(self):
        c = BloodHoundClient("http://localhost:8080/", "id", "key")
        assert c.base_url == "http://localhost:8080"

    def test_custom_base_url(self):
        c = BloodHoundClient("https://bh.internal:9090", "id", "key")
        assert c.base_url == "https://bh.internal:9090"
