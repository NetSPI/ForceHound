"""Forensic audit logging for ForceHound.

Produces OCSF-aligned (class_uid 6003 — API Activity) JSONL logs that
record every HTTP request made during a collection run.  Three verbosity
tiers control how much detail is captured per entry:

  Level 1 — Activity ledger (timestamp, operation, status)
  Level 2 — + headers, duration, error detail
  Level 3 — + full request/response bodies (forensic reconstruction)

Usage::

    logger = setup_audit_log(level=3, collector="aura",
                             instance_url="https://myorg.lightning.force.com",
                             org_id="00D...", cli_args="--collector aura -v")
    logger.log_request(method="POST", url="/aura", status_code=200, ...)
    logger.close()
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from forcehound import __version__

# OCSF activity_id mapping
ACTIVITY_MAP: Dict[str, int] = {
    "getConfigData": 2,
    "getObjectInfo": 2,
    "getItems": 2,
    "getItemsGraphQL": 2,
    "getRecordWithFields": 2,
    "getRecordCreateDefaults": 2,
    "getCurrentUserId": 2,
    "executeGraphQL": 2,
    "createRecord": 1,
    "updateRecord": 3,
    "deleteRecord": 4,
    "query_all": 2,
    "query": 2,
    "describe": 2,
}

ACTIVITY_NAME_MAP: Dict[int, str] = {
    1: "Create",
    2: "Read",
    3: "Update",
    4: "Delete",
    99: "Other",
}


def _utcnow_iso() -> str:
    """Return current UTC time in ISO 8601 with microseconds."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class AuditLogger:
    """Thread-safe JSONL audit logger with OCSF API Activity schema.

    Args:
        file_path: Path to the output ``.jsonl`` file.
        level: Verbosity tier (1, 2, or 3).
        collector: Collector backend name (``"aura"`` or ``"api"``).
        instance_url: Salesforce instance URL.
        org_id: 18-char Salesforce Org ID.
        cli_args: The full CLI invocation string.
    """

    def __init__(
        self,
        file_path: str,
        level: int,
        collector: str = "",
        instance_url: str = "",
        org_id: str = "",
        cli_args: str = "",
    ) -> None:
        self.file_path = file_path
        self.level = level
        self.collector = collector
        self.instance_url = instance_url
        self.org_id = org_id
        self.cli_args = cli_args

        self._lock = threading.Lock()
        self._entry_counter = 0
        self._start_time = time.monotonic()
        self._file = open(file_path, "w", buffering=1, encoding="utf-8")
        self._closed = False

        # Write session start event
        self._write_session_start()

    def _next_uid(self) -> int:
        """Return the next sequential entry number (thread-safe)."""
        with self._lock:
            self._entry_counter += 1
            return self._entry_counter

    def _write_line(self, obj: Dict[str, Any]) -> None:
        """Write a single JSON line to the log file (thread-safe)."""
        with self._lock:
            if not self._closed:
                self._file.write(json.dumps(obj, separators=(",", ":")) + "\n")

    def _write_session_start(self) -> None:
        """Write the session start event (first line)."""
        hostname = ""
        if self.instance_url:
            hostname = self.instance_url.replace("https://", "").replace("http://", "").rstrip("/")

        entry: Dict[str, Any] = {
            "class_uid": 6003,
            "class_name": "API Activity",
            "activity_id": 99,
            "activity_name": "Session Start",
            "severity_id": 1,
            "severity": "Informational",
            "time": _utcnow_iso(),
            "metadata": {
                "product": {
                    "name": "ForceHound",
                    "version": __version__,
                    "vendor_name": "NetSPI",
                },
                "version": "1.3.0",
                "log_level": self.level,
            },
            "actor": {
                "user": {"uid": "(pending)", "name": "(pending)"},
            },
            "dst_endpoint": {
                "url": self.instance_url,
                "instance_uid": self.org_id,
            },
            "message": "ForceHound audit log started",
            "unmapped": {
                "collector": self.collector,
                "cli_args": self.cli_args,
            },
        }

        if hostname:
            entry["cloud"] = {"provider": "Salesforce", "region": hostname.split(".")[0]}

        if self.level >= 3:
            entry["unmapped"]["credential_warning"] = (
                "This log contains a session ID. Treat as a credential artifact."
            )

        self._write_line(entry)

    def log_user_resolved(
        self,
        user_id: str,
        user_name: str = "",
        email: str = "",
    ) -> None:
        """Write a Session User Resolved event."""
        actor_user: Dict[str, Any] = {"uid": user_id}
        if user_name:
            actor_user["name"] = user_name
        if email:
            actor_user["email_addr"] = email

        entry: Dict[str, Any] = {
            "class_uid": 6003,
            "class_name": "API Activity",
            "activity_id": 99,
            "activity_name": "Session User Resolved",
            "severity_id": 1,
            "severity": "Informational",
            "time": _utcnow_iso(),
            "actor": {"user": actor_user},
            "message": "Session user identity resolved",
        }
        self._write_line(entry)

    def log_request(
        self,
        *,
        method: str = "POST",
        url: str = "",
        status_code: int = 0,
        operation: str = "",
        resource_name: str = "",
        resource_type: str = "SF_Object",
        duration_ms: Optional[float] = None,
        request_headers: Optional[Dict[str, str]] = None,
        response_headers: Optional[Dict[str, str]] = None,
        request_body: Optional[str] = None,
        response_body: Optional[str] = None,
        response_state: Optional[str] = None,
        error_message: Optional[str] = None,
        is_error: bool = False,
    ) -> None:
        """Log a single HTTP request/response pair.

        The ``level`` controls which fields are populated:

        - Level 1: timestamp, operation, resource, status, message
        - Level 2: + duration, headers, error detail, response state
        - Level 3: + full request body, full response body
        """
        uid = self._next_uid()

        activity_id = ACTIVITY_MAP.get(operation, 2)
        activity_name = ACTIVITY_NAME_MAP.get(activity_id, "Read")

        status_id = 2 if is_error else 1
        status = "Failure" if is_error else "Success"

        # Build human-readable message line
        resource_part = f"[{operation}({resource_name})]" if resource_name else f"[{operation}]"
        message = f"{method} {url} {resource_part} → {status_code}"

        # Resources array
        resources: List[Dict[str, str]] = []
        if resource_name:
            resources.append({"name": resource_name, "type": resource_type})

        # === Level 1 (always) ===
        entry: Dict[str, Any] = {
            "class_uid": 6003,
            "class_name": "API Activity",
            "activity_id": activity_id,
            "activity_name": activity_name,
            "time": _utcnow_iso(),
            "severity_id": 3 if is_error else 1,
            "api": {
                "operation": operation,
                "request": {"uid": str(uid)},
            },
            "resources": resources,
            "status_id": status_id,
            "status": status,
            "status_code": str(status_code),
            "message": message,
        }

        # === Level 2 (+ duration, headers, error) ===
        if self.level >= 2:
            if duration_ms is not None:
                entry["duration"] = round(duration_ms)

            hostname = ""
            if self.instance_url:
                hostname = (
                    self.instance_url.replace("https://", "")
                    .replace("http://", "")
                    .rstrip("/")
                )

            http_req: Dict[str, Any] = {
                "http_method": method,
                "url": {"url_string": url, "hostname": hostname},
            }

            if request_headers:
                http_req["http_headers"] = [
                    {"name": k, "value": v} for k, v in request_headers.items()
                ]

            entry["http_request"] = http_req

            unmapped: Dict[str, Any] = {}
            if response_state:
                unmapped["response_state"] = response_state
            if response_headers:
                unmapped["response_headers"] = dict(response_headers)
            if error_message:
                unmapped["error_message"] = error_message
            if unmapped:
                entry["unmapped"] = unmapped

        # === Level 3 (+ full bodies) ===
        if self.level >= 3:
            if request_body is not None:
                if "http_request" not in entry:
                    entry["http_request"] = {}
                entry["http_request"]["body"] = request_body
            if response_body is not None:
                entry.setdefault("unmapped", {})["response_body"] = response_body
                entry["raw_data"] = response_body

        self._write_line(entry)

    def close(self) -> None:
        """Write the session end event and close the file."""
        with self._lock:
            if self._closed:
                return
            self._closed = True

        elapsed_ms = (time.monotonic() - self._start_time) * 1000

        # Flush to ensure all prior entries are on disk before measuring size.
        self._file.flush()

        log_file_bytes = 0
        try:
            log_file_bytes = os.path.getsize(self.file_path)
        except OSError:
            pass

        entry: Dict[str, Any] = {
            "class_uid": 6003,
            "class_name": "API Activity",
            "activity_id": 99,
            "activity_name": "Session End",
            "severity_id": 1,
            "severity": "Informational",
            "time": _utcnow_iso(),
            "duration": round(elapsed_ms),
            "message": "ForceHound audit log complete",
            "unmapped": {
                "total_entries": self._entry_counter + 1,  # +1 for start event
                "total_requests": self._entry_counter,
                "log_file_bytes": log_file_bytes,
            },
        }
        self._file.write(json.dumps(entry, separators=(",", ":")) + "\n")
        self._file.flush()
        self._file.close()


def setup_audit_log(
    level: int,
    collector: str = "",
    instance_url: str = "",
    org_id: str = "",
    cli_args: str = "",
) -> AuditLogger:
    """Create and return an :class:`AuditLogger` with a timestamped filename.

    Args:
        level: Audit verbosity (1, 2, or 3).
        collector: Backend name (``"aura"`` or ``"api"``).
        instance_url: Salesforce instance URL.
        org_id: 18-char Org ID.
        cli_args: CLI invocation string.

    Returns:
        A ready-to-use :class:`AuditLogger` instance.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = f"forcehound_audit_{timestamp}.jsonl"
    return AuditLogger(
        file_path=file_path,
        level=level,
        collector=collector,
        instance_url=instance_url,
        org_id=org_id,
        cli_args=cli_args,
    )
