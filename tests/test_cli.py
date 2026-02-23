"""Tests for forcehound.cli."""

import json
import os

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from forcehound.cli import build_parser, run, main
from forcehound.models.base import CollectionResult, GraphNode, GraphEdge


# =====================================================================
# Argument parsing tests
# =====================================================================


class TestBuildParser:
    def test_default_collector(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.collector == "aura"

    def test_collector_api(self):
        parser = build_parser()
        args = parser.parse_args(["--collector", "api"])
        assert args.collector == "api"

    def test_collector_aura(self):
        parser = build_parser()
        args = parser.parse_args(["--collector", "aura"])
        assert args.collector == "aura"

    def test_collector_both(self):
        parser = build_parser()
        args = parser.parse_args(["--collector", "both"])
        assert args.collector == "both"

    def test_instance_url(self):
        parser = build_parser()
        args = parser.parse_args(["--instance-url", "https://test.my.salesforce.com"])
        assert args.instance_url == "https://test.my.salesforce.com"

    def test_session_id(self):
        parser = build_parser()
        args = parser.parse_args(["--session-id", "TOKEN123"])
        assert args.session_id == "TOKEN123"

    def test_username_password(self):
        parser = build_parser()
        args = parser.parse_args(
            ["--username", "user@test.com", "--password", "pass123"]
        )
        assert args.username == "user@test.com"
        assert args.password == "pass123"

    def test_aura_context(self):
        parser = build_parser()
        args = parser.parse_args(["--aura-context", '{"mode":"PRODDEBUG"}'])
        assert args.aura_context == '{"mode":"PRODDEBUG"}'

    def test_aura_token(self):
        parser = build_parser()
        args = parser.parse_args(["--aura-token", "eyJtoken"])
        assert args.aura_token == "eyJtoken"

    def test_output_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.output == "forcehound_output.json"

    def test_output_custom(self):
        parser = build_parser()
        args = parser.parse_args(["-o", "custom.json"])
        assert args.output == "custom.json"

    def test_verbose_flag(self):
        parser = build_parser()
        args = parser.parse_args(["-v"])
        assert args.verbose is True

    def test_verbose_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.verbose is False

    def test_max_workers(self):
        parser = build_parser()
        args = parser.parse_args(["--max-workers", "50"])
        assert args.max_workers == 50

    def test_max_workers_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.max_workers == 30

    def test_security_token(self):
        parser = build_parser()
        args = parser.parse_args(["--security-token", "SEC123"])
        assert args.security_token == "SEC123"

    def test_api_instance_url(self):
        parser = build_parser()
        args = parser.parse_args(
            ["--api-instance-url", "https://test.my.salesforce.com"]
        )
        assert args.api_instance_url == "https://test.my.salesforce.com"

    def test_api_instance_url_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.api_instance_url == ""

    def test_api_session_id(self):
        parser = build_parser()
        args = parser.parse_args(["--api-session-id", "API_TOKEN"])
        assert args.api_session_id == "API_TOKEN"

    def test_api_session_id_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.api_session_id == ""

    def test_risk_summary_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--risk-summary"])
        assert args.risk_summary is True

    def test_risk_summary_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.risk_summary is False

    def test_proxy_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--proxy", "http://127.0.0.1:8080"])
        assert args.proxy == "http://127.0.0.1:8080"

    def test_proxy_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.proxy == ""

    def test_rate_limit_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--rate-limit", "5"])
        assert args.rate_limit == 5.0

    def test_rate_limit_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.rate_limit is None

    def test_upload_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--upload"])
        assert args.upload is True

    def test_upload_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.upload is False

    def test_upload_file_name(self):
        parser = build_parser()
        args = parser.parse_args(["--upload-file-name", "MyOrg.json"])
        assert args.upload_file_name == "MyOrg.json"

    def test_upload_file_name_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.upload_file_name is None

    def test_clear_db_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--clear-db"])
        assert args.clear_db is True

    def test_clear_db_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.clear_db is False

    def test_bh_url_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.bh_url == "http://localhost:8080"

    def test_bh_url_custom(self):
        parser = build_parser()
        args = parser.parse_args(["--bh-url", "https://bh.internal:9090"])
        assert args.bh_url == "https://bh.internal:9090"

    def test_bh_token_id(self):
        parser = build_parser()
        args = parser.parse_args(["--bh-token-id", "my-uuid"])
        assert args.bh_token_id == "my-uuid"

    def test_bh_token_key(self):
        parser = build_parser()
        args = parser.parse_args(["--bh-token-key", "base64secret=="])
        assert args.bh_token_key == "base64secret=="

    def test_clear_db_only_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--clear-db-only"])
        assert args.clear_db_only is True

    def test_clear_db_only_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.clear_db_only is False

    def test_setup_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--setup"])
        assert args.setup is True

    def test_setup_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.setup is False


# =====================================================================
# Environment variable tests
# =====================================================================


class TestEnvVars:
    def test_instance_url_from_env(self, monkeypatch):
        monkeypatch.setenv("FORCEHOUND_INSTANCE_URL", "https://env.salesforce.com")
        parser = build_parser()
        args = parser.parse_args([])
        assert args.instance_url == "https://env.salesforce.com"

    def test_session_id_from_env(self, monkeypatch):
        monkeypatch.setenv("FORCEHOUND_SESSION_ID", "ENV_TOKEN")
        parser = build_parser()
        args = parser.parse_args([])
        assert args.session_id == "ENV_TOKEN"

    def test_cli_overrides_env(self, monkeypatch):
        monkeypatch.setenv("FORCEHOUND_INSTANCE_URL", "https://env.salesforce.com")
        parser = build_parser()
        args = parser.parse_args(["--instance-url", "https://cli.salesforce.com"])
        assert args.instance_url == "https://cli.salesforce.com"

    def test_aura_context_from_env(self, monkeypatch):
        monkeypatch.setenv("FORCEHOUND_AURA_CONTEXT", '{"mode":"PROD"}')
        parser = build_parser()
        args = parser.parse_args([])
        assert args.aura_context == '{"mode":"PROD"}'

    def test_aura_token_from_env(self, monkeypatch):
        monkeypatch.setenv("FORCEHOUND_AURA_TOKEN", "envtoken")
        parser = build_parser()
        args = parser.parse_args([])
        assert args.aura_token == "envtoken"

    def test_bh_url_from_env(self, monkeypatch):
        monkeypatch.setenv("FORCEHOUND_BH_URL", "https://bh.env:9090")
        parser = build_parser()
        args = parser.parse_args([])
        assert args.bh_url == "https://bh.env:9090"

    def test_bh_token_id_from_env(self, monkeypatch):
        monkeypatch.setenv("FORCEHOUND_BH_TOKEN_ID", "env-uuid")
        parser = build_parser()
        args = parser.parse_args([])
        assert args.bh_token_id == "env-uuid"

    def test_bh_token_key_from_env(self, monkeypatch):
        monkeypatch.setenv("FORCEHOUND_BH_TOKEN_KEY", "env-secret==")
        parser = build_parser()
        args = parser.parse_args([])
        assert args.bh_token_key == "env-secret=="


# =====================================================================
# Run function tests
# =====================================================================


class TestClearDbOnly:
    @pytest.mark.asyncio
    async def test_clear_db_only_success(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "--clear-db-only",
                "--bh-token-id",
                "test-id",
                "--bh-token-key",
                "test-key",
            ]
        )

        mock_client = MagicMock()
        mock_client.clear_database.return_value = None

        with patch(
            "forcehound.bloodhound.client.BloodHoundClient", return_value=mock_client
        ):
            exit_code = await run(args)

        assert exit_code == 0
        mock_client.clear_database.assert_called_once()

    @pytest.mark.asyncio
    async def test_clear_db_only_missing_token(self):
        parser = build_parser()
        args = parser.parse_args(["--clear-db-only"])

        exit_code = await run(args)
        assert exit_code == 1

    @pytest.mark.asyncio
    async def test_clear_db_only_api_error(self):
        from forcehound.bloodhound.client import BloodHoundAPIError

        parser = build_parser()
        args = parser.parse_args(
            [
                "--clear-db-only",
                "--bh-token-id",
                "test-id",
                "--bh-token-key",
                "test-key",
            ]
        )

        mock_client = MagicMock()
        mock_client.clear_database.side_effect = BloodHoundAPIError("test error")

        with patch(
            "forcehound.bloodhound.client.BloodHoundClient", return_value=mock_client
        ):
            exit_code = await run(args)

        assert exit_code == 1


class TestSetup:
    @pytest.mark.asyncio
    async def test_setup_success(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "--setup",
                "--bh-token-id",
                "test-id",
                "--bh-token-key",
                "test-key",
            ]
        )

        mock_client = MagicMock()
        mock_client.register_custom_nodes.return_value = [
            "SF_ConnectedApp",
            "SF_Group",
            "SF_User",
        ]

        with patch(
            "forcehound.bloodhound.client.BloodHoundClient", return_value=mock_client
        ):
            exit_code = await run(args)

        assert exit_code == 0
        mock_client.register_custom_nodes.assert_called_once()

    @pytest.mark.asyncio
    async def test_setup_missing_token(self):
        parser = build_parser()
        args = parser.parse_args(["--setup"])

        exit_code = await run(args)
        assert exit_code == 1

    @pytest.mark.asyncio
    async def test_setup_api_error(self):
        from forcehound.bloodhound.client import BloodHoundAPIError

        parser = build_parser()
        args = parser.parse_args(
            [
                "--setup",
                "--bh-token-id",
                "test-id",
                "--bh-token-key",
                "test-key",
            ]
        )

        mock_client = MagicMock()
        mock_client.register_custom_nodes.side_effect = BloodHoundAPIError(
            "registration failed"
        )

        with patch(
            "forcehound.bloodhound.client.BloodHoundClient", return_value=mock_client
        ):
            exit_code = await run(args)

        assert exit_code == 1


class TestRunFunction:
    @pytest.fixture
    def mock_aura_result(self):
        return CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "Test"}),
                GraphNode(
                    id="00DA", kinds=["SF_Organization"], properties={"name": "Org"}
                ),
            ],
            edges=[
                GraphEdge(source="005A", target="00DA", kind="ModifyAllData"),
            ],
            collector_type="aura",
            org_id="00DA",
            metadata={"users": 1, "requests": 5},
        )

    @pytest.mark.asyncio
    async def test_run_aura_mode(self, mock_aura_result, tmp_path):
        output_path = str(tmp_path / "output.json")
        parser = build_parser()
        args = parser.parse_args(
            [
                "--collector",
                "aura",
                "--instance-url",
                "https://test.lightning.force.com",
                "--session-id",
                "00DXX!TOKEN",
                "--aura-context",
                '{"mode":"PRODDEBUG"}',
                "--aura-token",
                "eyJtoken",
                "-o",
                output_path,
            ]
        )

        mock_collector = AsyncMock()
        mock_collector.collect.return_value = mock_aura_result
        mock_collector.close = AsyncMock()

        with patch("forcehound.cli.AuraCollector", return_value=mock_collector):
            exit_code = await run(args)

        assert exit_code == 0
        assert os.path.exists(output_path)

        with open(output_path, "r") as f:
            data = json.load(f)
        assert data["metadata"]["source_kind"] == "Salesforce"

    @pytest.mark.asyncio
    async def test_run_api_mode(self, tmp_path):
        output_path = str(tmp_path / "output.json")
        parser = build_parser()
        args = parser.parse_args(
            [
                "--collector",
                "api",
                "--instance-url",
                "https://test.my.salesforce.com",
                "--session-id",
                "00DXX!TOKEN",
                "-o",
                output_path,
            ]
        )

        mock_result = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "Test"})
            ],
            edges=[],
            collector_type="api",
            org_id="00DA",
        )
        mock_collector = AsyncMock()
        mock_collector.collect.return_value = mock_result

        with patch("forcehound.cli.APICollector", return_value=mock_collector):
            exit_code = await run(args)

        assert exit_code == 0

    @pytest.mark.asyncio
    async def test_run_both_mode(self, mock_aura_result, tmp_path):
        output_path = str(tmp_path / "output.json")
        parser = build_parser()
        args = parser.parse_args(
            [
                "--collector",
                "both",
                "--instance-url",
                "https://test.lightning.force.com",
                "--session-id",
                "00DXX!AURA_TOKEN",
                "--api-instance-url",
                "https://test.my.salesforce.com",
                "--api-session-id",
                "00DXX!API_TOKEN",
                "--aura-context",
                '{"mode":"PRODDEBUG"}',
                "--aura-token",
                "eyJtoken",
                "-o",
                output_path,
            ]
        )

        api_result = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "API User"})
            ],
            edges=[],
            collector_type="api",
            org_id="00DA",
        )
        mock_api = AsyncMock()
        mock_api.collect.return_value = api_result

        mock_aura = AsyncMock()
        mock_aura.collect.return_value = mock_aura_result
        mock_aura.close = AsyncMock()

        with (
            patch("forcehound.cli.APICollector", return_value=mock_api),
            patch("forcehound.cli.AuraCollector", return_value=mock_aura),
        ):
            exit_code = await run(args)

        assert exit_code == 0

        with open(output_path, "r") as f:
            data = json.load(f)
        # Should have merged nodes
        assert len(data["graph"]["nodes"]) >= 1

    @pytest.mark.asyncio
    async def test_run_both_mode_fallback_on_empty_aura(self, tmp_path):
        """When Aura returns 0 users, both-mode falls back to full API."""
        output_path = str(tmp_path / "output.json")
        parser = build_parser()
        args = parser.parse_args(
            [
                "--collector",
                "both",
                "--instance-url",
                "https://test.lightning.force.com",
                "--session-id",
                "00DXX!AURA_TOKEN",
                "--api-instance-url",
                "https://test.my.salesforce.com",
                "--api-session-id",
                "00DXX!API_TOKEN",
                "--aura-context",
                '{"mode":"PRODDEBUG"}',
                "--aura-token",
                "eyJtoken",
                "-o",
                output_path,
            ]
        )

        # Aura returns 0 users (expired session scenario)
        empty_aura = CollectionResult(
            nodes=[
                GraphNode(
                    id="00DA", kinds=["SF_Organization"], properties={"name": "Org"}
                )
            ],
            edges=[],
            collector_type="aura",
            org_id="00DA",
            metadata={"users": 0, "requests": 3},
        )
        mock_aura = AsyncMock()
        mock_aura.collect.return_value = empty_aura
        mock_aura.close = AsyncMock()

        # Full API should produce a complete result
        api_result = CollectionResult(
            nodes=[
                GraphNode(id="005A", kinds=["SF_User"], properties={"name": "User"}),
                GraphNode(
                    id="00DA", kinds=["SF_Organization"], properties={"name": "Org"}
                ),
            ],
            edges=[GraphEdge(source="005A", target="00DA", kind="ModifyAllData")],
            collector_type="api",
            org_id="00DA",
            metadata={"queries": 16},
        )
        mock_api = AsyncMock()
        mock_api.collect.return_value = api_result

        with (
            patch("forcehound.cli.APICollector") as ApiCls,
            patch("forcehound.cli.AuraCollector", return_value=mock_aura),
        ):
            ApiCls.return_value = mock_api
            exit_code = await run(args)

            # Verify API was created WITHOUT supplement_only (full mode)
            call_kwargs = ApiCls.call_args[1] if ApiCls.call_args[1] else {}
            assert call_kwargs.get("supplement_only", False) is False

        assert exit_code == 0
        with open(output_path, "r") as f:
            data = json.load(f)
        # Should have the API user node
        user_nodes = [n for n in data["graph"]["nodes"] if "SF_User" in n["kinds"]]
        assert len(user_nodes) >= 1

    @pytest.mark.asyncio
    async def test_run_validation_error(self, tmp_path):
        output_path = str(tmp_path / "output.json")
        parser = build_parser()
        args = parser.parse_args(
            [
                "--collector",
                "aura",
                "-o",
                output_path,
            ]
        )

        exit_code = await run(args)
        assert exit_code == 1

    @pytest.mark.asyncio
    async def test_run_collection_error(self, tmp_path):
        output_path = str(tmp_path / "output.json")
        parser = build_parser()
        args = parser.parse_args(
            [
                "--collector",
                "aura",
                "--instance-url",
                "https://test.lightning.force.com",
                "--session-id",
                "00DXX!TOKEN",
                "--aura-context",
                '{"mode":"PRODDEBUG"}',
                "--aura-token",
                "eyJtoken",
                "-o",
                output_path,
            ]
        )

        mock_collector = AsyncMock()
        mock_collector.collect.side_effect = RuntimeError("Connection failed")
        mock_collector.close = AsyncMock()

        with patch("forcehound.cli.AuraCollector", return_value=mock_collector):
            exit_code = await run(args)

        assert exit_code == 1


# =====================================================================
# Main function tests
# =====================================================================


class TestMainFunction:
    def test_main_calls_parser(self):
        """Test that main exits with SystemExit."""
        with pytest.raises(SystemExit):
            # --help triggers SystemExit(0)
            main(["--help"])
