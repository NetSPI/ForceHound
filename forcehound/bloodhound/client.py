"""Portable BloodHound CE API client using only Python stdlib.

Implements HMAC-SHA256 authentication and the file-upload workflow
so ForceHound can automate database clearing and graph ingestion
without requiring the ``blood_hound_api_client`` or ``httpx`` packages.
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
import json
import logging
import os
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from forcehound.constants import CUSTOM_NODE_ICONS

logger = logging.getLogger(__name__)


class BloodHoundClient:
    """Synchronous, stdlib-only client for the BloodHound CE v2 API.

    Args:
        base_url: BloodHound CE base URL (e.g., ``http://localhost:8080``).
        token_id: API token ID (UUID).
        token_key: API token key (base64-encoded secret).
    """

    def __init__(self, base_url: str, token_id: str, token_key: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token_id = token_id
        self.token_key = token_key

    def clear_database(
        self,
        graph: bool = True,
        ingest_history: bool = True,
        quality_history: bool = True,
    ) -> None:
        """Clear the BloodHound database.

        Args:
            graph: Delete collected graph data.
            ingest_history: Delete file ingest history.
            quality_history: Delete data quality history.

        Raises:
            BloodHoundAPIError: On non-204 response.
        """
        body = json.dumps(
            {
                "deleteCollectedGraphData": graph,
                "deleteFileIngestHistory": ingest_history,
                "deleteDataQualityHistory": quality_history,
            }
        ).encode("utf-8")

        resp = self._request("POST", "/api/v2/clear-database", body)
        if resp.status != 204:
            raise BloodHoundAPIError(
                f"clear-database failed: {resp.status} {resp.read().decode()}"
            )

    def register_custom_nodes(self) -> List[str]:
        """Register ForceHound custom node types in BloodHound CE.

        POSTs icon definitions from ``CUSTOM_NODE_ICONS`` to
        ``/api/v2/custom-nodes`` so that SF_* nodes render with
        meaningful Font Awesome icons instead of generic circles.

        Returns:
            Sorted list of registered type names.

        Raises:
            BloodHoundAPIError: On non-201/409 response.
        """
        payload: Dict[str, Any] = {
            "custom_types": {
                kind: {
                    "icon": {
                        "type": "font-awesome",
                        "name": icon["name"],
                        "color": icon["color"],
                    }
                }
                for kind, icon in CUSTOM_NODE_ICONS.items()
            }
        }
        body = json.dumps(payload).encode("utf-8")
        resp = self._request("POST", "/api/v2/custom-nodes", body)

        if resp.status in (201, 409):
            return sorted(CUSTOM_NODE_ICONS.keys())

        raise BloodHoundAPIError(
            f"custom-nodes registration failed: {resp.status} "
            f"{resp.read().decode()}"
        )

    def upload_graph(self, path: str, file_name: Optional[str] = None) -> int:
        """Upload a graph JSON file to BloodHound CE.

        Executes the three-step file-upload workflow:
          1. ``POST /api/v2/file-upload/start`` → job ID
          2. ``POST /api/v2/file-upload/{id}`` → upload file
          3. ``POST /api/v2/file-upload/{id}/end`` → trigger ingestion

        Args:
            path: Path to the graph JSON file on disk.
            file_name: Display name for the file in BloodHound CE's
                File Ingest page.  Defaults to the basename of *path*.

        Returns:
            The upload job ID.

        Raises:
            BloodHoundAPIError: On any non-success response.
        """
        # Step 1: Create upload job
        resp = self._request("POST", "/api/v2/file-upload/start")
        if resp.status != 201:
            raise BloodHoundAPIError(
                f"file-upload/start failed: {resp.status} {resp.read().decode()}"
            )
        job_data = json.loads(resp.read().decode("utf-8"))
        job_id: int = job_data["data"]["id"]
        logger.info("Created upload job %d", job_id)

        # Step 2: Upload file contents
        with open(path, "rb") as fh:
            file_bytes = fh.read()

        display_name = file_name or os.path.basename(path)
        resp = self._request(
            "POST",
            f"/api/v2/file-upload/{job_id}",
            file_bytes,
            extra_headers={
                "X-File-Upload-Name": display_name,
            },
        )
        if resp.status != 202:
            raise BloodHoundAPIError(
                f"file-upload/{job_id} failed: {resp.status} {resp.read().decode()}"
            )
        logger.info("Uploaded file to job %d", job_id)

        # Step 3: End upload job (triggers ingestion)
        resp = self._request("POST", f"/api/v2/file-upload/{job_id}/end")
        if resp.status != 200:
            raise BloodHoundAPIError(
                f"file-upload/{job_id}/end failed: {resp.status} {resp.read().decode()}"
            )
        logger.info("Ended upload job %d — ingestion triggered", job_id)

        return job_id

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[bytes] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Send a signed HTTP request to the BloodHound API.

        Returns the :class:`http.client.HTTPResponse` object.
        """
        url = f"{self.base_url}{path}"
        headers = self._sign(method, path, body)
        if body is not None:
            headers["Content-Type"] = "application/json"
        if extra_headers:
            headers.update(extra_headers)

        req = Request(url, data=body, headers=headers, method=method)
        try:
            return urlopen(req)
        except HTTPError as exc:
            # HTTPError is a subclass of HTTPResponse — return it so
            # callers can inspect .status and .read() uniformly.
            return exc

    def _sign(
        self,
        method: str,
        path: str,
        body: Optional[bytes] = None,
    ) -> Dict[str, str]:
        """Compute the HMAC-SHA256 signature chain and return auth headers.

        The three-link chain mirrors the SpecterOps ``bhesignature`` scheme:

        1. HMAC(token_key, ``METHOD`` + ``path``)
        2. HMAC(digest₁, datetime truncated to hour)
        3. HMAC(digest₂, request body)
        """
        # Link 1: method + path
        digester = hmac.new(
            self.token_key.encode("utf-8"),
            digestmod=hashlib.sha256,
        )
        digester.update(f"{method}{path}".encode("utf-8"))

        # Link 2: datetime truncated to hour
        digester = hmac.new(digester.digest(), digestmod=hashlib.sha256)
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        datetime_formatted = now.isoformat(timespec="seconds").replace(
            "+00:00",
            "Z",
        )
        digester.update(datetime_formatted[:13].encode("utf-8"))

        # Link 3: body
        digester = hmac.new(digester.digest(), digestmod=hashlib.sha256)
        if body is not None:
            digester.update(body)

        signature = base64.b64encode(digester.digest()).decode("utf-8")

        return {
            "User-Agent": "forcehound 0.1",
            "Authorization": f"bhesignature {self.token_id}",
            "RequestDate": datetime_formatted,
            "Signature": signature,
        }


class BloodHoundAPIError(Exception):
    """Raised when a BloodHound CE API call fails."""
