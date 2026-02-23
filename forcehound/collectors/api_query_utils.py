"""Shared audit-aware SOQL query helper for the REST API collectors.

Both :class:`APICollector` and :class:`ShareObjectCollector` execute SOQL
queries through ``simple_salesforce`` with optional audit logging.  This
module provides a single implementation to avoid duplication.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

    from forcehound.audit import AuditLogger

from forcehound.constants import SALESFORCE_API_VERSION


def audit_query(
    sf: Salesforce,
    soql: str,
    audit_logger: Optional["AuditLogger"],
    operation: str = "query_all",
    resource_name: str = "",
) -> Dict[str, Any]:
    """Execute a SOQL query via ``sf.query_all()`` with optional audit logging.

    Args:
        sf: Authenticated ``simple_salesforce.Salesforce`` instance.
        soql: SOQL query string.
        audit_logger: Optional :class:`AuditLogger`; ``None`` to skip logging.
        operation: Operation name for the audit entry.
        resource_name: Target resource for the audit entry.

    Returns:
        The query result dict from simple_salesforce.
    """
    t0 = time.monotonic() if audit_logger else None

    try:
        result = sf.query_all(soql)
    except Exception as exc:
        if audit_logger:
            duration_ms = (time.monotonic() - t0) * 1000 if t0 else None
            api_path = f"/services/data/v{SALESFORCE_API_VERSION}/query"
            audit_logger.log_request(
                method="GET",
                url=api_path,
                status_code=0,
                operation=operation,
                resource_name=resource_name,
                duration_ms=duration_ms,
                request_body=soql if audit_logger.level >= 3 else None,
                error_message=str(exc),
                is_error=True,
            )
        raise

    if audit_logger:
        duration_ms = (time.monotonic() - t0) * 1000 if t0 else None
        api_path = f"/services/data/v{SALESFORCE_API_VERSION}/query"
        record_count = len(result.get("records", []))
        audit_logger.log_request(
            method="GET",
            url=api_path,
            status_code=200,
            operation=operation,
            resource_name=resource_name,
            duration_ms=duration_ms,
            request_body=soql if audit_logger.level >= 3 else None,
            response_body=(
                json.dumps(result) if audit_logger.level >= 3 else None
            ),
            response_state=f"{record_count} records",
        )

    return result
