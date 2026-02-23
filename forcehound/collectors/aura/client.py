"""Async Aura/Lightning client for low-privilege Salesforce access.

This module is an async (``aiohttp``) port of the synchronous
``ForceHound_Prototype/aura_client.py``.  It provides five core methods
that interact with Salesforce Aura endpoints:

+--------------------------+------------------------------------------------------+
| Method                   | Aura action descriptor                               |
+==========================+======================================================+
| :meth:`get_config_data`  | ``HostConfigController/ACTION$getConfigData``         |
+--------------------------+------------------------------------------------------+
| :meth:`get_object_info`  | ``RecordUiController/ACTION$getObjectInfo``           |
+--------------------------+------------------------------------------------------+
| :meth:`get_items`        | ``SelectableListDataProviderController/ACTION$…``     |
+--------------------------+------------------------------------------------------+
| :meth:`get_items_graphql`| ``RecordUiController/ACTION$executeGraphQL``          |
+--------------------------+------------------------------------------------------+
| :meth:`get_record_with_fields` | ``RecordUiController/ACTION$getRecordWithFields``|
+--------------------------+------------------------------------------------------+

Key differences from the prototype:

* ``aura_context`` is accepted as a constructor parameter (not hardcoded)
  and is auto-decoded if URL-encoded.
* ``requests.post()`` → ``aiohttp.ClientSession.post()``.
* Concurrency is handled by the caller via ``asyncio.Semaphore`` +
  ``asyncio.gather``, replacing ``ThreadPoolExecutor``.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.parse
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import aiohttp

from forcehound.utils.id_utils import ensure_18_char_id

if TYPE_CHECKING:
    from forcehound.audit import AuditLogger

logger = logging.getLogger(__name__)


class AuraClient:
    """Async client for making requests to the Salesforce Aura API.

    Args:
        instance_url: The Lightning instance URL
            (e.g., ``https://myorg.lightning.force.com``).
        session_id: The ``sid`` session cookie value.
        aura_context: The ``aura.context`` JSON string.
            URL-encoded values are auto-decoded.
        aura_token: The ``aura.token`` JWT string.
            URL-encoded values are auto-decoded.
        session: Optional pre-existing ``aiohttp.ClientSession``.  If
            ``None``, a new session will be created on first use and
            closed by :meth:`close`.
    """

    def __init__(
        self,
        instance_url: str,
        session_id: str,
        aura_context: str,
        aura_token: str,
        session: Optional[aiohttp.ClientSession] = None,
        aura_path: str = "/aura",
        audit_logger: Optional["AuditLogger"] = None,
        proxy: Optional[str] = None,
        rate_limit: Optional[float] = None,
    ) -> None:
        self.instance_url = instance_url.rstrip("/")
        self.session_id = session_id
        self._audit_logger = audit_logger
        self._proxy = proxy
        self._rate_limit = rate_limit
        self._min_interval = 1.0 / rate_limit if rate_limit else 0.0
        self._last_request_time = 0.0

        # Auto-decode URL-encoded context / token.
        self.aura_context = (
            urllib.parse.unquote(aura_context) if "%" in aura_context else aura_context
        )
        self.aura_token = (
            urllib.parse.unquote(aura_token) if "%" in aura_token else aura_token
        )

        self._cookies = {"sid": session_id}
        self._url = f"{self.instance_url}{aura_path}"
        self._external_session = session is not None
        self._session = session
        self.request_count: int = 0

        # Extract org_id from the session_id (the prefix before the ``!``
        # delimiter is the org ID, typically 15 chars).  Convert to 18-char
        # form so it matches the API collector's Organization query.
        if "!" in session_id:
            raw_org = session_id.split("!")[0]
            try:
                self.org_id = ensure_18_char_id(raw_org)
            except ValueError:
                self.org_id = raw_org
        else:
            self.org_id = ""

    async def _get_session(self) -> aiohttp.ClientSession:
        """Return the underlying HTTP session, creating one if needed."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(cookies=self._cookies)
        return self._session

    async def close(self) -> None:
        """Close the HTTP session if it was created internally."""
        if self._session and not self._external_session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _make_request(
        self,
        message: str,
        context_label: str = "",
        resource_name: str = "",
    ) -> Dict[str, Any]:
        """POST to the Aura endpoint and return the parsed JSON response.

        The Aura API sometimes returns a JSON list rather than a single
        object; if so, the first element is returned for consistency.

        Args:
            message: The JSON ``message`` payload for the Aura action.
            context_label: Human-readable operation name for audit logging
                (e.g. ``"getObjectInfo"``).
            resource_name: Target resource name for audit logging
                (e.g. ``"Account"``).

        Returns:
            The normalised JSON response dict.

        Raises:
            aiohttp.ClientResponseError: On non-2xx HTTP status.
            ValueError: If the response body cannot be parsed as JSON.
        """
        t0 = time.monotonic() if self._audit_logger else None

        self.request_count += 1

        # Rate limiting — enforce minimum interval between requests.
        if self._min_interval > 0:
            import asyncio as _asyncio

            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self._min_interval:
                await _asyncio.sleep(self._min_interval - elapsed)
            self._last_request_time = time.monotonic()

        session = await self._get_session()

        post_data = {
            "message": message,
            "aura.context": self.aura_context,
            "aura.token": self.aura_token,
        }

        async with session.post(self._url, data=post_data, proxy=self._proxy) as resp:
            status_code = resp.status
            resp_headers = dict(resp.headers) if self._audit_logger and self._audit_logger.level >= 2 else {}
            resp.raise_for_status()
            text = await resp.text()

        # Salesforce Aura responses can be wrapped with JSON-hijacking
        # defenses:  "*/" prefix and "/*ERROR*/" or "/*" suffix.
        # Strip both before parsing.
        if text.startswith("*/"):
            text = text[2:].lstrip()
        if text.endswith("/*ERROR*/"):
            text = text[: -len("/*ERROR*/")].rstrip()
        elif text.endswith("/*"):
            text = text[:-2].rstrip()

        # Detect session expiry before parsing.
        is_error = False
        error_msg = None
        if "aura:invalidSession" in text:
            is_error = True
            error_msg = "Aura session is invalid or expired."

        # Parse JSON (manual parse to handle edge cases).
        response_state = None
        try:
            response_json = json.loads(text)
        except json.JSONDecodeError as exc:
            logger.debug("Aura response text (first 500 chars): %s", text[:500])
            is_error = True
            error_msg = f"Failed to parse Aura response: {exc}"
            # Audit log the failure before raising
            if self._audit_logger:
                duration_ms = (time.monotonic() - t0) * 1000 if t0 else None
                self._audit_logger.log_request(
                    method="POST",
                    url=self._url.replace(self.instance_url, ""),
                    status_code=status_code,
                    operation=context_label or "unknown",
                    resource_name=resource_name,
                    duration_ms=duration_ms,
                    response_headers=resp_headers or None,
                    request_body=message if self._audit_logger.level >= 3 else None,
                    response_body=text if self._audit_logger.level >= 3 else None,
                    error_message=error_msg,
                    is_error=True,
                )
            raise ValueError(error_msg) from exc

        # Normalise list responses.
        if isinstance(response_json, list):
            response_json = response_json[0] if response_json else {}

        # Extract response state for audit
        if isinstance(response_json, dict):
            actions = response_json.get("actions", [])
            if actions and isinstance(actions[0], dict):
                response_state = actions[0].get("state")

        # Audit log
        if self._audit_logger:
            duration_ms = (time.monotonic() - t0) * 1000 if t0 else None
            req_headers: Optional[Dict[str, str]] = None
            if self._audit_logger.level >= 2:
                req_headers = {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Cookie": f"sid={self.session_id[:20]}...",
                }

            self._audit_logger.log_request(
                method="POST",
                url=self._url.replace(self.instance_url, ""),
                status_code=status_code,
                operation=context_label or "unknown",
                resource_name=resource_name,
                duration_ms=duration_ms,
                request_headers=req_headers,
                response_headers=resp_headers or None,
                request_body=message if self._audit_logger.level >= 3 else None,
                response_body=text if self._audit_logger.level >= 3 else None,
                response_state=response_state,
                error_message=error_msg,
                is_error=is_error,
            )

        if is_error and error_msg:
            raise ValueError(
                "Aura session is invalid or expired. "
                "Obtain a fresh session ID and aura token."
            )

        return response_json

    async def get_config_data(self) -> List[str]:
        """Retrieve all Salesforce object API names visible to the current user.

        Uses the ``HostConfigController/ACTION$getConfigData`` descriptor.

        Returns:
            A list of object API name strings
            (e.g., ``["Account", "Contact", ...]``).
        """
        message = (
            '{"actions":[{"id":"1094;a","descriptor":'
            '"serviceComponent://ui.force.components.controllers.hostConfig.'
            'HostConfigController/ACTION$getConfigData",'
            '"callingDescriptor":"UNKNOWN","params":{},"storable":true}]}'
        )

        response = await self._make_request(
            message, context_label="getConfigData", resource_name="HostConfig"
        )
        api_names = response["actions"][0]["returnValue"]["apiNamesToKeyPrefixes"]
        return list(api_names.keys())

    async def get_object_info(self, object_name: str) -> List[Dict[str, Any]]:
        """Retrieve field metadata for a Salesforce object.

        Uses the ``RecordUiController/ACTION$getObjectInfo`` descriptor.

        Args:
            object_name: API name of the object (e.g., ``"Account"``).

        Returns:
            A list of field-metadata dicts, each containing:
            ``field_name``, ``is_reference``, ``relationship_name``,
            ``reference_object``, ``required``, ``createable``,
            ``updateable``, ``data_type``, ``length``, ``nillable``,
            ``picklist_values``, ``reference_to_infos``.
        """
        message = (
            '{"actions":[{"id":"1094;a","descriptor":'
            '"aura://RecordUiController/ACTION$getObjectInfo",'
            '"callingDescriptor":"UNKNOWN","params":{"objectApiName":"'
            + object_name
            + '"},"storable":true}]}'
        )

        response = await self._make_request(
            message, context_label="getObjectInfo", resource_name=object_name
        )
        fields_data = response["actions"][0]["returnValue"]["fields"]

        field_list: List[Dict[str, Any]] = []
        for field_details in fields_data.values():
            field_name = field_details["apiName"]
            is_reference = field_details["reference"]
            relationship_name: Optional[str] = None
            reference_object: Optional[str] = None

            if is_reference:
                relationship_name = field_details["relationshipName"]
                ref_infos = field_details.get("referenceToInfos", [])
                if ref_infos:
                    reference_object = ref_infos[0]["apiName"]

            field_list.append(
                {
                    "field_name": field_name,
                    "is_reference": is_reference,
                    "relationship_name": relationship_name,
                    "reference_object": reference_object,
                    "required": field_details.get("required", False),
                    "createable": field_details.get("createable", False),
                    "updateable": field_details.get("updateable", False),
                    "data_type": field_details.get("dataType", ""),
                    "length": field_details.get("length", 0),
                    "nillable": field_details.get("nillable", True),
                    "picklist_values": field_details.get("picklistValues", []),
                    "reference_to_infos": field_details.get("referenceToInfos", []),
                }
            )

        return field_list

    async def get_items(self, object_name: str, page_size: int = 2000) -> List[str]:
        """Retrieve accessible record IDs for an object (max 2 000).

        Uses the ``SelectableListDataProviderController/ACTION$getItems``
        descriptor.  For full enumeration beyond 2 000 records, use
        :meth:`get_items_graphql` instead.

        Args:
            object_name: API name of the object.
            page_size: Number of records to request (max 2 000).

        Returns:
            A list of record ID strings.
        """
        message = (
            '{"actions":[{"id":"123;a","descriptor":'
            '"serviceComponent://ui.force.components.controllers.lists.'
            "selectableListDataProvider."
            'SelectableListDataProviderController/ACTION$getItems",'
            '"callingDescriptor":"UNKNOWN","params":{"entityNameOrId":"'
            + object_name
            + '","layoutType":"FULL","pageSize":'
            + str(page_size)
            + ',"currentPage":0,"useTimeout":false,'
            '"getCount":false,"enableRowActions":false}}]}'
        )

        response = await self._make_request(
            message, context_label="getItems", resource_name=object_name
        )
        records = response["actions"][0]["returnValue"]["result"]

        record_ids: set[str] = set()
        for record in records:
            record_id = record["record"]["Id"]
            if record_id and record_id != "000000000000000AAA":
                record_ids.add(record_id)

        return list(record_ids)

    async def get_items_graphql(
        self,
        object_name: str,
        batch_size: int = 2000,
        where_clause: Optional[str] = None,
        debug_pagination: bool = False,
    ) -> List[str]:
        """Retrieve *all* accessible record IDs using cursor-based GraphQL.

        Uses the ``RecordUiController/ACTION$executeGraphQL`` descriptor
        with cursor pagination, bypassing the 2 000-record limit of
        :meth:`get_items`.

        Based on research by Mandiant (AuraInspector).

        Args:
            object_name: API name of the object.
            batch_size: Number of records per page (max 2 000).
            where_clause: Optional GraphQL ``where`` filter, e.g.
                ``"where:{IsActive:{eq:true}}"``.  Appended verbatim
                to the query arguments.
            debug_pagination: Print per-page progress to stdout.

        Returns:
            A list of record ID strings (complete enumeration).
        """
        record_ids: set[str] = set()
        cursor: Optional[str] = None
        page_num = 0

        while True:
            page_num += 1

            # Build argument clause: first, after, where.
            args_parts = [f"first:{batch_size}"]
            if cursor:
                args_parts.append(f'after:"{cursor}"')
            if where_clause:
                args_parts.append(where_clause)
            args_str = f"({','.join(args_parts)})"

            # Build the GraphQL query (spaces, not '+').
            query = (
                f"query getItems {{uiapi {{query {{{object_name}"
                f"{args_str}{{edges {{node {{Id}}}}"
                f"totalCount,pageInfo{{endCursor,hasNextPage,"
                f"hasPreviousPage}}}}}}}}}}"
            )

            message = json.dumps(
                {
                    "actions": [
                        {
                            "id": "GraphQL",
                            "descriptor": "aura://RecordUiController/ACTION$executeGraphQL",
                            "callingDescriptor": "UNKNOWN",
                            "params": {
                                "queryInput": {
                                    "operationName": "getItems",
                                    "query": query,
                                    "variables": {},
                                }
                            },
                        }
                    ]
                }
            )

            response = await self._make_request(
                message,
                context_label="getItemsGraphQL",
                resource_name=object_name,
            )

            # Navigate the response defensively with clear errors.
            action = response.get("actions", [{}])[0]
            state = action.get("state")
            if state == "ERROR":
                raw_error = action.get("error", [])
                # Aura errors can be a list of dicts or other structures.
                if isinstance(raw_error, list) and raw_error:
                    first = raw_error[0]
                    if isinstance(first, dict):
                        msg = (
                            first.get("message")
                            or first.get("exceptionMessage")
                            or json.dumps(first)
                        )
                    else:
                        msg = str(first)
                elif raw_error:
                    msg = str(raw_error)
                else:
                    msg = "No error details."
                logger.debug(
                    "GraphQL ERROR for %s — action keys: %s, raw error: %s",
                    object_name,
                    list(action.keys()),
                    raw_error,
                )
                raise RuntimeError(
                    f"GraphQL action error for {object_name}: {msg} "
                    f"[action keys: {list(action.keys())}]"
                )

            return_value = action.get("returnValue")
            if return_value is None:
                logger.debug(
                    "GraphQL returnValue is None for %s. Full action: %s",
                    object_name,
                    action,
                )
                raise RuntimeError(
                    f"GraphQL returned null for {object_name} (state={state})"
                )

            gql_data = return_value.get("data")
            if gql_data is None:
                gql_errors = return_value.get("errors", [])
                if gql_errors:
                    msg = gql_errors[0].get("message", "Unknown")
                    raise RuntimeError(f"GraphQL query error for {object_name}: {msg}")
                raise RuntimeError(f"GraphQL 'data' is null for {object_name}")

            uiapi = gql_data.get("uiapi")
            if uiapi is None:
                raise RuntimeError(
                    f"GraphQL 'uiapi' is null for {object_name}. "
                    f"Keys in data: {list(gql_data.keys())}"
                )

            query_data = uiapi.get("query")
            if query_data is None:
                raise RuntimeError(
                    f"GraphQL 'query' is null for {object_name}. "
                    f"Keys in uiapi: {list(uiapi.keys())}"
                )

            data = query_data.get(object_name)
            if data is None:
                raise RuntimeError(
                    f"GraphQL has no '{object_name}' key. "
                    f"Keys in query: {list(query_data.keys())}"
                )

            page_ids = []
            for edge in data["edges"]:
                record_id = edge["node"]["Id"]
                if record_id and record_id != "000000000000000AAA":
                    page_ids.append(record_id)
            record_ids.update(page_ids)

            total_count = data.get("totalCount")
            page_info = data["pageInfo"]
            has_next = page_info["hasNextPage"]

            if debug_pagination:
                print(
                    f"  [GraphQL] {object_name} page {page_num}: "
                    f"{len(page_ids)} records (total so far: {len(record_ids)}, "
                    f"server totalCount: {total_count}, hasNextPage: {has_next})"
                )

            if has_next:
                cursor = page_info["endCursor"]
            else:
                break

        if debug_pagination:
            print(
                f"  [GraphQL] {object_name} complete: "
                f"{len(record_ids)} total records across {page_num} page(s)"
            )
        return list(record_ids)

    async def get_record_with_fields(
        self,
        record_id: str,
        fields: List[str],
        optional_fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Retrieve field values for a record using relationship traversal.

        Uses the ``RecordUiController/ACTION$getRecordWithFields``
        descriptor.  Accepts dot-notation field paths
        (e.g., ``"User.Profile.PermissionsModifyAllData"``) to pull
        related-record data in a single call.

        Args:
            record_id: The 15- or 18-character Salesforce record ID.
            fields: Required field paths — the request will fail if any
                are invalid.
            optional_fields: Field paths that are silently ignored when
                the field does not exist.

        Returns:
            The raw JSON response dict from the Aura API.
        """
        if optional_fields is None:
            optional_fields = []

        fields_str = ", ".join(f'"{f}"' for f in fields)
        opt_fields_str = ", ".join(f'"{f}"' for f in optional_fields)

        message = (
            '{"actions":[{"id":"2840;a","descriptor":'
            '"aura://RecordUiController/ACTION$getRecordWithFields",'
            '"callingDescriptor":"UNKNOWN","params":{"recordId":"'
            + record_id
            + '","fields":['
            + fields_str
            + '],"optionalFields":['
            + opt_fields_str
            + "]}}]}"
        )

        return await self._make_request(
            message,
            context_label="getRecordWithFields",
            resource_name=record_id,
        )

    # =================================================================
    # DML operations (CRUD probing)
    # =================================================================

    async def create_record(
        self,
        object_name: str,
        fields: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create a record via the Aura ``createRecord`` descriptor.

        Args:
            object_name: API name of the object (e.g., ``"Account"``).
            fields: Field name → value mapping for the new record.

        Returns:
            The raw JSON response dict from the Aura API.
        """
        record_input = {
            "allowSaveOnDuplicate": False,
            "apiName": object_name,
            "fields": fields,
        }
        message = json.dumps(
            {
                "actions": [
                    {
                        "id": "crud;a",
                        "descriptor": "aura://RecordUiController/ACTION$createRecord",
                        "callingDescriptor": "UNKNOWN",
                        "params": {"recordInput": record_input},
                    }
                ]
            }
        )
        return await self._make_request(
            message, context_label="createRecord", resource_name=object_name
        )

    async def update_record(
        self,
        record_id: str,
        fields: Dict[str, Any],
        if_unmodified_since: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update a record via the Aura ``updateRecord`` descriptor.

        Args:
            record_id: The 18-character Salesforce record ID.
            fields: Field name → value mapping for the update. Must
                include the ``Id`` field.
            if_unmodified_since: Optional ``Last-Modified`` header value
                for optimistic concurrency control.

        Returns:
            The raw JSON response dict from the Aura API.
        """
        params: Dict[str, Any] = {
            "recordId": record_id,
            "recordInput": {
                "allowSaveOnDuplicate": False,
                "fields": fields,
            },
        }
        if if_unmodified_since:
            params["clientOptions"] = {
                "ifUnmodifiedSince": if_unmodified_since,
            }

        message = json.dumps(
            {
                "actions": [
                    {
                        "id": "crud;a",
                        "descriptor": "aura://RecordUiController/ACTION$updateRecord",
                        "callingDescriptor": "UNKNOWN",
                        "params": params,
                    }
                ]
            }
        )
        return await self._make_request(
            message, context_label="updateRecord", resource_name=record_id
        )

    async def get_current_user_id(self) -> str:
        """Return the 18-character User ID of the authenticated session owner.

        Uses ``RecordUiController/ACTION$getRecordCreateDefaults`` for
        Account, which returns the default ``OwnerId`` — always the
        current user.  This is a read-only call (no records created).

        Returns:
            The 18-character Salesforce User ID (``005…``).

        Raises:
            RuntimeError: If the user ID cannot be determined.
        """
        message = json.dumps(
            {
                "actions": [
                    {
                        "id": "uid;a",
                        "descriptor": (
                            "aura://RecordUiController/"
                            "ACTION$getRecordCreateDefaults"
                        ),
                        "callingDescriptor": "UNKNOWN",
                        "params": {
                            "objectApiName": "Account",
                            "formFactor": "LARGE",
                            "recordTypeId": None,
                            "optionalFields": [],
                        },
                    }
                ]
            }
        )
        response = await self._make_request(
            message,
            context_label="getRecordCreateDefaults",
            resource_name="Account",
        )
        actions = response.get("actions", [])
        if not actions or actions[0].get("state") != "SUCCESS":
            error = actions[0].get("error") if actions else "empty actions"
            raise RuntimeError(
                f"getRecordCreateDefaults failed: {error}"
            )

        fields = (
            actions[0]
            .get("returnValue", {})
            .get("record", {})
            .get("fields", {})
        )
        owner_id = fields.get("OwnerId", {}).get("value")
        if owner_id:
            return ensure_18_char_id(owner_id)

        raise RuntimeError(
            "Could not determine current user ID from "
            "getRecordCreateDefaults response"
        )

    async def delete_record(self, record_id: str) -> Dict[str, Any]:
        """Delete a record via the Aura ``deleteRecord`` descriptor.

        Uses the ``RecordGvpController`` (not ``RecordUiController``).

        Args:
            record_id: The 18-character Salesforce record ID.

        Returns:
            The raw JSON response dict from the Aura API.
        """
        message = json.dumps(
            {
                "actions": [
                    {
                        "id": "crud;a",
                        "descriptor": (
                            "serviceComponent://ui.force.components.controllers."
                            "recordGlobalValueProvider.RecordGvpController/"
                            "ACTION$deleteRecord"
                        ),
                        "callingDescriptor": "UNKNOWN",
                        "params": {"recordId": record_id},
                    }
                ]
            }
        )
        return await self._make_request(
            message, context_label="deleteRecord", resource_name=record_id
        )
