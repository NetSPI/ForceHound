"""Abstract base class for all ForceHound collectors.

Both the REST API collector and the Aura collector inherit from
:class:`BaseCollector` and implement the :meth:`collect` coroutine.
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Optional

from forcehound.models.auth import AuthConfig
from forcehound.models.base import CollectionResult

if TYPE_CHECKING:
    from forcehound.audit import AuditLogger


class BaseCollector(abc.ABC):
    """Abstract collector interface.

    Args:
        auth: Authentication configuration for the target Salesforce org.
        verbose: When ``True``, emit progress messages to stdout.
        audit_logger: Optional :class:`AuditLogger` for forensic logging.
    """

    def __init__(
        self,
        auth: AuthConfig,
        verbose: bool = False,
        audit_logger: Optional["AuditLogger"] = None,
        proxy: Optional[str] = None,
        rate_limit: Optional[float] = None,
    ) -> None:
        self.auth = auth
        self.verbose = verbose
        self.audit_logger = audit_logger
        self.proxy = proxy
        self.rate_limit = rate_limit

    @abc.abstractmethod
    async def collect(self) -> CollectionResult:
        """Execute the collection run and return the result.

        Subclasses must implement this coroutine.  It should:
          1. Connect to the Salesforce org using the credentials in
             ``self.auth``.
          2. Enumerate users, profiles, roles, groups, etc.
          3. Build :class:`GraphNode` and :class:`GraphEdge` objects.
          4. Return a :class:`CollectionResult`.
        """

    async def close(self) -> None:
        """Release any resources held by the collector.

        Subclasses may override this to close HTTP sessions, etc.
        The default implementation is a no-op.
        """

    def _log(self, message: str) -> None:
        """Print *message* if verbose mode is enabled."""
        if self.verbose:
            print(message)
