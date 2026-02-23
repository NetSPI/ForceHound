"""Dummy value generation for CRUD probing.

Generates plausible field values for record creation and update
operations. Uses a hardcoded bank of known field names first, then
falls back to type-based defaults.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# =====================================================================
# Known field name → value bank
# =====================================================================

_KNOWN_FIELD_VALUES: Dict[str, Any] = {
    "Name": "FH_Test_Record",
    "FirstName": "FH_Test",
    "LastName": "FH_Record",
    "Phone": "5550000001",
    "Fax": "5550000002",
    "Email": "fh_probe@example.com",
    "Website": "https://forcehound.test",
    "Description": "ForceHound CRUD probe record",
    "Title": "FH Test",
    "Department": "FH Testing",
    "Industry": "Technology",
    "Company": "FH_Test_Company",
    "Street": "123 Test St",
    "City": "Test City",
    "State": "CA",
    "PostalCode": "90210",
    "Country": "US",
    "Subject": "FH_Test_Subject",
    "Status": None,  # Will use picklist fallback
    "Priority": None,  # Will use picklist fallback
    "Type": None,  # Will use picklist fallback
    "Rating": None,  # Will use picklist fallback
    "LeadSource": None,  # Will use picklist fallback
    "StageName": None,  # Will use picklist fallback
    "Amount": 1.0,
    "Probability": 10.0,
    "NumberOfEmployees": 1,
    "AnnualRevenue": 1.0,
    "Birthdate": "2000-01-01",
    "CloseDate": "2099-12-31",
}

# Prefix used by all generated record names for cleanup identification.
FH_PROBE_PREFIX = "FH_"


# =====================================================================
# Type-based fallback values
# =====================================================================

_TYPE_DEFAULTS: Dict[str, Any] = {
    "string": "FH_probe",
    "textarea": "FH_probe",
    "email": "fh_probe@example.com",
    "phone": "5550000001",
    "url": "https://forcehound.test",
    "double": 1.0,
    "currency": 1.0,
    "percent": 10.0,
    "int": 1,
    "boolean": False,
    "date": "2026-01-01",
    "datetime": "2026-01-01T00:00:00.000Z",
}


def generate_dummy_value(
    field_name: str,
    data_type: str,
    picklist_values: Optional[List[Dict[str, Any]]] = None,
    length: Optional[int] = None,
    reference_id: Optional[str] = None,
) -> Any:
    """Generate a plausible dummy value for a Salesforce field.

    Resolution order:
      1. If ``reference_id`` is provided (reference/lookup field), use it.
      2. If the field name is in the known bank and the value is not None, use it.
      3. If ``picklist_values`` are available, use the first active value.
      4. Fall back to type-based defaults.
      5. Return ``"FH_probe"`` as last resort.

    Args:
        field_name: The API name of the field.
        data_type: The Salesforce field data type (e.g., ``"string"``).
        picklist_values: Optional list of picklist value dicts from
            ``get_object_info``.
        length: Optional max field length — value will be truncated.
        reference_id: Optional record ID for reference/lookup fields.

    Returns:
        A value suitable for the field.
    """
    # 1. Reference field
    if reference_id is not None:
        return reference_id

    # 2. Known field bank
    if field_name in _KNOWN_FIELD_VALUES:
        known = _KNOWN_FIELD_VALUES[field_name]
        if known is not None:
            if length and isinstance(known, str) and len(known) > length:
                return known[:length]
            return known

    # 3. Picklist values
    if picklist_values:
        for pv in picklist_values:
            # Prefer active, non-default values
            val = pv.get("value")
            if val:
                return val

    # 4. Type-based default
    dt_lower = (data_type or "string").lower()
    value = _TYPE_DEFAULTS.get(dt_lower, "FH_probe")

    # Truncate strings to field length
    if length and isinstance(value, str) and len(value) > length:
        value = value[:length]

    return value
