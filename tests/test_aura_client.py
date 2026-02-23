"""Tests for forcehound.collectors.aura.client.AuraClient."""

import json
import pytest
import aiohttp
from unittest.mock import AsyncMock, MagicMock, patch

from forcehound.collectors.aura.client import AuraClient


# =====================================================================
# Mock helpers
# =====================================================================


def _mock_response(status=200, body="", payload=None):
    """Create a mock aiohttp response usable as an async context manager."""
    if payload is not None:
        body = json.dumps(payload)
    resp = AsyncMock()
    resp.status = status
    resp.text = AsyncMock(return_value=body)
    resp.headers = {}
    resp.raise_for_status = MagicMock()
    return resp


def _patch_post(response):
    """Patch aiohttp.ClientSession.post to return the given mock response."""
    ctx = AsyncMock()
    ctx.__aenter__.return_value = response
    ctx.__aexit__.return_value = False
    p = patch("aiohttp.ClientSession.post", return_value=ctx)
    return p


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def client():
    """Create an AuraClient for testing."""
    return AuraClient(
        instance_url="https://test.lightning.force.com",
        session_id="00DXX000000XXXXX!AQEAQTEST",
        aura_context='{"mode":"PRODDEBUG","fwuid":"testfwuid","app":"one:one","loaded":{},"dn":[],"globals":{},"uad":true}',
        aura_token="eyJtesttoken",
    )


@pytest.fixture
def aura_url():
    return "https://test.lightning.force.com/aura"


# =====================================================================
# Constructor tests
# =====================================================================


class TestAuraClientInit:
    def test_basic_init(self, client):
        assert client.instance_url == "https://test.lightning.force.com"
        assert client.session_id == "00DXX000000XXXXX!AQEAQTEST"
        assert client.aura_token == "eyJtesttoken"

    def test_strips_trailing_slash(self):
        c = AuraClient(
            instance_url="https://test.lightning.force.com/",
            session_id="SID",
            aura_context="{}",
            aura_token="T",
        )
        assert c.instance_url == "https://test.lightning.force.com"

    def test_url_decode_aura_context(self):
        encoded = "%7B%22mode%22%3A%22PRODDEBUG%22%7D"
        c = AuraClient(
            instance_url="https://test.lightning.force.com",
            session_id="SID",
            aura_context=encoded,
            aura_token="T",
        )
        assert c.aura_context == '{"mode":"PRODDEBUG"}'

    def test_url_decode_aura_token(self):
        encoded = "eyJ%3Dtest%3D"
        c = AuraClient(
            instance_url="https://test.lightning.force.com",
            session_id="SID",
            aura_context="{}",
            aura_token=encoded,
        )
        assert c.aura_token == "eyJ=test="

    def test_no_decode_when_no_percent(self):
        raw = '{"mode":"PRODDEBUG"}'
        c = AuraClient(
            instance_url="https://test.lightning.force.com",
            session_id="SID",
            aura_context=raw,
            aura_token="eyJtoken",
        )
        assert c.aura_context == raw

    def test_org_id_extracted(self, client):
        assert client.org_id == "00DXX000000XXXXX"

    def test_org_id_no_exclamation(self):
        c = AuraClient(
            instance_url="https://test.lightning.force.com",
            session_id="noseparator",
            aura_context="{}",
            aura_token="T",
        )
        assert c.org_id == ""

    def test_url_built_correctly(self, client):
        assert client._url == "https://test.lightning.force.com/aura"


# =====================================================================
# _make_request tests
# =====================================================================


class TestMakeRequest:
    @pytest.mark.asyncio
    async def test_returns_dict(self, client):
        response_body = {"actions": [{"state": "SUCCESS"}]}
        mock_resp = _mock_response(payload=response_body)
        with _patch_post(mock_resp):
            result = await client._make_request('{"actions":[]}')
            assert result == response_body
        await client.close()

    @pytest.mark.asyncio
    async def test_normalizes_list_response(self, client):
        response_body = [{"actions": [{"state": "SUCCESS"}]}]
        mock_resp = _mock_response(payload=response_body)
        with _patch_post(mock_resp):
            result = await client._make_request('{"actions":[]}')
            assert result == {"actions": [{"state": "SUCCESS"}]}
        await client.close()

    @pytest.mark.asyncio
    async def test_empty_list_returns_empty_dict(self, client):
        mock_resp = _mock_response(payload=[])
        with _patch_post(mock_resp):
            result = await client._make_request('{"actions":[]}')
            assert result == {}
        await client.close()

    @pytest.mark.asyncio
    async def test_invalid_json_raises(self, client):
        mock_resp = _mock_response(body="not json")
        with _patch_post(mock_resp):
            with pytest.raises(ValueError, match="Failed to parse"):
                await client._make_request('{"actions":[]}')
        await client.close()


# =====================================================================
# get_config_data tests
# =====================================================================


class TestGetConfigData:
    @pytest.mark.asyncio
    async def test_returns_object_names(self, client):
        response = {
            "actions": [
                {
                    "returnValue": {
                        "apiNamesToKeyPrefixes": {
                            "Account": "001",
                            "Contact": "003",
                            "User": "005",
                        }
                    }
                }
            ]
        }
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_config_data()
            assert "Account" in result
            assert "Contact" in result
            assert "User" in result
            assert len(result) == 3
        await client.close()

    @pytest.mark.asyncio
    async def test_empty_result(self, client):
        response = {"actions": [{"returnValue": {"apiNamesToKeyPrefixes": {}}}]}
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_config_data()
            assert result == []
        await client.close()


# =====================================================================
# get_object_info tests
# =====================================================================


class TestGetObjectInfo:
    @pytest.mark.asyncio
    async def test_returns_field_list(self, client):
        response = {
            "actions": [
                {
                    "returnValue": {
                        "fields": {
                            "Name": {
                                "apiName": "Name",
                                "reference": False,
                            },
                            "OwnerId": {
                                "apiName": "OwnerId",
                                "reference": True,
                                "relationshipName": "Owner",
                                "referenceToInfos": [{"apiName": "User"}],
                            },
                        }
                    }
                }
            ]
        }
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_object_info("Account")
            assert len(result) == 2
            name_field = next(f for f in result if f["field_name"] == "Name")
            assert name_field["is_reference"] is False
            owner_field = next(f for f in result if f["field_name"] == "OwnerId")
            assert owner_field["is_reference"] is True
            assert owner_field["relationship_name"] == "Owner"
            assert owner_field["reference_object"] == "User"
        await client.close()

    @pytest.mark.asyncio
    async def test_reference_no_infos(self, client):
        response = {
            "actions": [
                {
                    "returnValue": {
                        "fields": {
                            "WeirdRef": {
                                "apiName": "WeirdRef",
                                "reference": True,
                                "relationshipName": "Weird",
                                "referenceToInfos": [],
                            }
                        }
                    }
                }
            ]
        }
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_object_info("Custom__c")
            assert result[0]["reference_object"] is None
        await client.close()


# =====================================================================
# get_items tests
# =====================================================================


class TestGetItems:
    @pytest.mark.asyncio
    async def test_returns_record_ids(self, client):
        response = {
            "actions": [
                {
                    "returnValue": {
                        "result": [
                            {"record": {"Id": "001XX000001TEST"}},
                            {"record": {"Id": "001XX000002TEST"}},
                        ]
                    }
                }
            ]
        }
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_items("Account")
            assert len(result) == 2
            assert "001XX000001TEST" in result
            assert "001XX000002TEST" in result
        await client.close()

    @pytest.mark.asyncio
    async def test_filters_null_ids(self, client):
        response = {
            "actions": [
                {
                    "returnValue": {
                        "result": [
                            {"record": {"Id": "001XX000001TEST"}},
                            {"record": {"Id": None}},
                            {"record": {"Id": "000000000000000AAA"}},
                        ]
                    }
                }
            ]
        }
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_items("Account")
            assert len(result) == 1
            assert "001XX000001TEST" in result
        await client.close()

    @pytest.mark.asyncio
    async def test_deduplicates(self, client):
        response = {
            "actions": [
                {
                    "returnValue": {
                        "result": [
                            {"record": {"Id": "001XX000001TEST"}},
                            {"record": {"Id": "001XX000001TEST"}},
                        ]
                    }
                }
            ]
        }
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_items("Account")
            assert len(result) == 1
        await client.close()


# =====================================================================
# get_items_graphql tests
# =====================================================================


class TestGetItemsGraphQL:
    @pytest.mark.asyncio
    async def test_single_page(self, client):
        response = {
            "actions": [
                {
                    "returnValue": {
                        "data": {
                            "uiapi": {
                                "query": {
                                    "User": {
                                        "edges": [
                                            {"node": {"Id": "005XX000001TEST"}},
                                            {"node": {"Id": "005XX000002TEST"}},
                                        ],
                                        "pageInfo": {
                                            "hasNextPage": False,
                                            "endCursor": "abc",
                                            "hasPreviousPage": False,
                                        },
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        }
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_items_graphql("User")
            assert len(result) == 2
        await client.close()

    @pytest.mark.asyncio
    async def test_multi_page(self, client):
        page1 = {
            "actions": [
                {
                    "returnValue": {
                        "data": {
                            "uiapi": {
                                "query": {
                                    "User": {
                                        "edges": [{"node": {"Id": "005XX000001TEST"}}],
                                        "pageInfo": {
                                            "hasNextPage": True,
                                            "endCursor": "cursor1",
                                            "hasPreviousPage": False,
                                        },
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        }
        page2 = {
            "actions": [
                {
                    "returnValue": {
                        "data": {
                            "uiapi": {
                                "query": {
                                    "User": {
                                        "edges": [{"node": {"Id": "005XX000002TEST"}}],
                                        "pageInfo": {
                                            "hasNextPage": False,
                                            "endCursor": "cursor2",
                                            "hasPreviousPage": True,
                                        },
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        }
        mock_resp1 = _mock_response(payload=page1)
        mock_resp2 = _mock_response(payload=page2)
        ctx1 = AsyncMock()
        ctx1.__aenter__.return_value = mock_resp1
        ctx1.__aexit__.return_value = False
        ctx2 = AsyncMock()
        ctx2.__aenter__.return_value = mock_resp2
        ctx2.__aexit__.return_value = False
        with patch("aiohttp.ClientSession.post", side_effect=[ctx1, ctx2]):
            result = await client.get_items_graphql("User")
            assert len(result) == 2
            assert "005XX000001TEST" in result
            assert "005XX000002TEST" in result
        await client.close()

    @pytest.mark.asyncio
    async def test_filters_null_and_zero_ids(self, client):
        response = {
            "actions": [
                {
                    "returnValue": {
                        "data": {
                            "uiapi": {
                                "query": {
                                    "User": {
                                        "edges": [
                                            {"node": {"Id": "005XX000001TEST"}},
                                            {"node": {"Id": None}},
                                            {"node": {"Id": "000000000000000AAA"}},
                                        ],
                                        "pageInfo": {
                                            "hasNextPage": False,
                                            "endCursor": "",
                                            "hasPreviousPage": False,
                                        },
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        }
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_items_graphql("User")
            assert len(result) == 1
            assert "005XX000001TEST" in result
        await client.close()


# =====================================================================
# get_record_with_fields tests
# =====================================================================


class TestGetRecordWithFields:
    @pytest.mark.asyncio
    async def test_returns_raw_response(self, client):
        response = {
            "actions": [
                {
                    "state": "SUCCESS",
                    "returnValue": {
                        "fields": {
                            "Id": {"value": "005XX000001TEST"},
                            "Name": {"value": "Test User"},
                        }
                    },
                }
            ]
        }
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_record_with_fields(
                "005XX000001TEST", ["User.Id"], ["User.Name"]
            )
            assert result["actions"][0]["state"] == "SUCCESS"
        await client.close()

    @pytest.mark.asyncio
    async def test_default_optional_fields(self, client):
        response = {"actions": [{"state": "SUCCESS", "returnValue": {"fields": {}}}]}
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_record_with_fields("005TEST", ["User.Id"])
            assert result["actions"][0]["state"] == "SUCCESS"
        await client.close()

    @pytest.mark.asyncio
    async def test_multiple_fields(self, client):
        response = {"actions": [{"state": "SUCCESS", "returnValue": {"fields": {}}}]}
        mock_resp = _mock_response(payload=response)
        with _patch_post(mock_resp):
            result = await client.get_record_with_fields(
                "005TEST",
                ["User.Id"],
                ["User.Name", "User.Email", "User.Profile.Name"],
            )
            assert result is not None
        await client.close()


# =====================================================================
# Session lifecycle tests
# =====================================================================


class TestSessionLifecycle:
    @pytest.mark.asyncio
    async def test_close_internal_session(self, client):
        response_body = {"actions": [{"state": "SUCCESS"}]}
        mock_resp = _mock_response(payload=response_body)
        with _patch_post(mock_resp):
            await client._make_request('{"actions":[]}')
        await client.close()
        assert client._session is None or client._session.closed

    @pytest.mark.asyncio
    async def test_close_without_session(self, client):
        await client.close()  # Should not raise

    @pytest.mark.asyncio
    async def test_external_session_not_closed(self):
        session = aiohttp.ClientSession()
        c = AuraClient(
            instance_url="https://test.lightning.force.com",
            session_id="00DXX!TOKEN",
            aura_context="{}",
            aura_token="T",
            session=session,
        )
        await c.close()
        assert not session.closed
        await session.close()
