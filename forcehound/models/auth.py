"""Authentication configuration for ForceHound collectors.

Each backend requires different credentials:

- **API** (``simple_salesforce``): ``instance_url`` + (``session_id`` *or*
  ``username`` / ``password`` / ``security_token``)
- **Aura**: ``instance_url`` + ``session_id`` + ``aura_context`` + ``aura_token``
"""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass


@dataclass
class AuthConfig:
    """Container for all authentication parameters.

    Attributes:
        instance_url: Salesforce instance URL
            (e.g., ``https://myorg.my.salesforce.com``).
        session_id: Active session / access token.
        username: Salesforce username (API mode only).
        password: Salesforce password (API mode only).
        security_token: Salesforce security token (API mode only).
        aura_context: Aura framework context JSON string.
            Accepts URL-encoded values — they are auto-decoded.
        aura_token: Aura authentication token (JWT).
            Accepts URL-encoded values — they are auto-decoded.
    """

    instance_url: str = ""
    session_id: str = ""
    username: str = ""
    password: str = ""
    security_token: str = ""
    aura_context: str = ""
    aura_token: str = ""

    def __post_init__(self) -> None:
        # Strip trailing slashes from instance_url for consistency.
        self.instance_url = self.instance_url.rstrip("/")

        # Auto-decode URL-encoded aura_context and aura_token.
        if self.aura_context and "%" in self.aura_context:
            self.aura_context = urllib.parse.unquote(self.aura_context)
        if self.aura_token and "%" in self.aura_token:
            self.aura_token = urllib.parse.unquote(self.aura_token)

    def validate_for_api(self) -> None:
        """Raise ``ValueError`` if required API-mode fields are missing."""
        if not self.instance_url:
            raise ValueError("instance_url is required for API mode")

        has_session = bool(self.session_id)
        has_creds = bool(self.username and self.password)

        if not (has_session or has_creds):
            raise ValueError(
                "API mode requires either session_id or username + password"
            )

    def validate_for_aura(self) -> None:
        """Raise ``ValueError`` if required Aura-mode fields are missing."""
        missing = []
        if not self.instance_url:
            missing.append("instance_url")
        if not self.session_id:
            missing.append("session_id")
        if not self.aura_context:
            missing.append("aura_context")
        if not self.aura_token:
            missing.append("aura_token")

        if missing:
            raise ValueError(f"Aura mode requires: {', '.join(missing)}")
