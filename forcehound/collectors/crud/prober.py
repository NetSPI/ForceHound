"""CRUD prober — empirical permission testing via DML operations.

Probes Create, Read, Edit, and Delete permissions by attempting actual
DML operations against the Salesforce org through the Aura API.

Standard mode (``--crud``):
  - Read: ``get_items`` on each object
  - Create: dummy record from required fields + lookup cache
  - Edit: no-op update on one record (write same values back)
  - Delete: only deletes self-created records

Aggressive mode (``--crud --aggressive``):
  - Edit: attempts no-op update on *every* record (reveals record-level
    sharing differences)
  - Delete: deletes one random existing record per object type (full
    state captured to deletion log before deletion)
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from forcehound.collectors.aura.client import AuraClient
from forcehound.collectors.crud.dummy_values import (
    FH_PROBE_PREFIX,
    generate_dummy_value,
)
from forcehound.collectors.crud.lookup_cache import LookupCache, build_dependency_order
from forcehound.constants import (
    CRUD_DELETE_PROTECTED_OBJECTS,
    CRUD_EDGE_KINDS,
    CRUD_EXCLUDED_SUFFIXES,
    CRUD_PROBE_CONCURRENCY_DEFAULT,
    CRUD_SKIP_OBJECTS,
)

logger = logging.getLogger(__name__)


@dataclass
class CrudProbeResult:
    """Result of CRUD probing for a single object."""

    object_name: str
    can_read: bool = False
    can_create: bool = False
    can_edit: bool = False
    can_delete: bool = False
    read_count: int = 0
    created_record_id: Optional[str] = None
    edit_success_count: int = 0
    edit_fail_count: int = 0
    error_messages: List[str] = field(default_factory=list)


@dataclass
class CrudProbeReport:
    """Aggregate results from the CRUD prober."""

    results: Dict[str, CrudProbeResult] = field(default_factory=dict)
    created_record_ids: List[str] = field(default_factory=list)
    deletions: List[Dict[str, Any]] = field(default_factory=list)
    total_requests: int = 0


def _should_skip_object(object_name: str) -> bool:
    """Return True if this object should not be CRUD-probed.

    Filters out system/metadata objects from ``CRUD_SKIP_OBJECTS`` and
    objects with suffixes in ``CRUD_EXCLUDED_SUFFIXES`` (e.g. __ChangeEvent,
    __Share, __Feed, __History) that produce noise without useful results.
    """
    if object_name in CRUD_SKIP_OBJECTS:
        return True
    for suffix in CRUD_EXCLUDED_SUFFIXES:
        if object_name.endswith(suffix):
            return True
    return False


def _is_probe_success(response: Dict[str, Any]) -> bool:
    """Return True if the Aura DML response indicates success."""
    actions = response.get("actions", [])
    if not actions:
        return False
    return actions[0].get("state") == "SUCCESS"


def _get_error_message(response: Dict[str, Any]) -> str:
    """Extract a human-readable error from an Aura DML response."""
    actions = response.get("actions", [])
    if not actions:
        return "No actions in response"
    action = actions[0]
    errors = action.get("error", [])
    if isinstance(errors, list) and errors:
        first = errors[0]
        if isinstance(first, dict):
            return (
                first.get("message")
                or first.get("exceptionMessage")
                or json.dumps(first)
            )
        return str(first)
    return f"State: {action.get('state', 'UNKNOWN')}"


def _get_created_record_id(response: Dict[str, Any]) -> Optional[str]:
    """Extract the record ID from a successful createRecord response."""
    actions = response.get("actions", [])
    if not actions:
        return None
    rv = actions[0].get("returnValue")
    if rv is None:
        return None
    if isinstance(rv, dict):
        return rv.get("id")
    return None


class CrudProber:
    """Empirical CRUD permission prober using Aura DML operations.

    Args:
        client: An authenticated :class:`AuraClient`.
        aggressive: Enable aggressive mode (edit all records, delete existing).
        crud_objects: Optional set of specific objects to probe.
        dry_run: Log what would be done without executing DML.
        concurrency: Max concurrent probe requests.
        max_records: Max records to test per object in aggressive edit.
            ``None`` means no cap (test all records).
        verbose: Emit progress messages to stdout.
    """

    def __init__(
        self,
        client: AuraClient,
        aggressive: bool = False,
        crud_objects: Optional[Set[str]] = None,
        dry_run: bool = False,
        concurrency: int = CRUD_PROBE_CONCURRENCY_DEFAULT,
        max_records: Optional[int] = None,
        verbose: bool = False,
        unsafe: bool = False,
    ) -> None:
        self.client = client
        self.aggressive = aggressive
        self.crud_objects = crud_objects
        self.dry_run = dry_run
        self.concurrency = concurrency
        self.max_records = max_records
        self.verbose = verbose
        self.unsafe = unsafe
        self._lookup_cache = LookupCache()
        self._report = CrudProbeReport()
        self._object_fields: Dict[str, List[Dict[str, Any]]] = {}

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(msg)

    async def probe(self) -> CrudProbeReport:
        """Execute the full CRUD probe flow.

        Returns:
            A :class:`CrudProbeReport` with per-object results.
        """
        sem = asyncio.Semaphore(self.concurrency)

        # Step 1: Discover all accessible objects
        self._log("  CRUD Step 1: Discovering accessible objects...")
        all_objects = await self.client.get_config_data()
        target_objects = {
            obj for obj in all_objects if not _should_skip_object(obj)
        }

        if self.crud_objects:
            target_objects = target_objects & self.crud_objects

        self._log(f"    {len(target_objects)} objects eligible for CRUD probing")

        if self.aggressive and not self.unsafe:
            protected = target_objects & CRUD_DELETE_PROTECTED_OBJECTS
            if protected:
                self._log(
                    f"    {len(protected)} objects are delete-protected "
                    f"(pass --unsafe to override)"
                )

        # Step 2: Get field metadata for all target objects
        self._log("  CRUD Step 2: Fetching field metadata...")
        await self._fetch_all_object_info(target_objects, sem)
        self._log(f"    Got metadata for {len(self._object_fields)} objects")

        # Step 3: Build dependency order
        dep_order = build_dependency_order(self._object_fields, target_objects)
        self._log(f"    Dependency-ordered {len(dep_order)} objects")

        # Step 4: Probe each object in dependency order
        self._log("  CRUD Step 3: Probing objects...")
        for i, obj_name in enumerate(dep_order, 1):
            if self.verbose and i % 10 == 0:
                self._log(f"    Progress: {i}/{len(dep_order)} objects")
            await self._probe_object(obj_name, sem)

        # Step 5: Cleanup — delete self-created records (standard mode)
        if not self.aggressive and self._report.created_record_ids:
            self._log(
                f"  CRUD Step 4: Cleaning up {len(self._report.created_record_ids)}"
                f" self-created records..."
            )
            await self._cleanup_created_records(sem)

        self._report.total_requests = self.client.request_count

        # Summary
        results = self._report.results
        can_read = sum(1 for r in results.values() if r.can_read)
        can_create = sum(1 for r in results.values() if r.can_create)
        can_edit = sum(1 for r in results.values() if r.can_edit)
        can_delete = sum(1 for r in results.values() if r.can_delete)
        self._log(
            f"  CRUD Complete: Read={can_read}, Create={can_create}, "
            f"Edit={can_edit}, Delete={can_delete} "
            f"(of {len(results)} objects probed)"
        )

        return self._report

    async def _fetch_all_object_info(
        self, target_objects: Set[str], sem: asyncio.Semaphore
    ) -> None:
        """Fetch field metadata for all target objects in parallel."""

        async def fetch_one(obj_name: str) -> None:
            async with sem:
                try:
                    fields = await self.client.get_object_info(obj_name)
                    self._object_fields[obj_name] = fields
                except Exception as exc:
                    logger.debug("Failed to get object info for %s: %s", obj_name, exc)

        await asyncio.gather(*(fetch_one(obj) for obj in target_objects))

    async def _probe_object(
        self, object_name: str, sem: asyncio.Semaphore
    ) -> None:
        """Probe a single object for CRUD permissions."""
        result = CrudProbeResult(object_name=object_name)

        # --- READ ---
        try:
            record_ids = await self.client.get_items(object_name)
            result.can_read = True
            result.read_count = len(record_ids)

            # Populate lookup cache with first discovered record
            if record_ids:
                self._lookup_cache.put(object_name, record_ids[0])
        except Exception as exc:
            logger.debug("Read probe failed for %s: %s", object_name, exc)
            record_ids = []

        # --- CREATE ---
        if not self.dry_run:
            created_id = await self._probe_create(object_name)
            if created_id:
                result.can_create = True
                result.created_record_id = created_id
                self._report.created_record_ids.append(created_id)
                # Update lookup cache with our created record
                self._lookup_cache.put(object_name, created_id)

        # --- EDIT ---
        if not self.dry_run:
            if self.aggressive and record_ids:
                # Aggressive: edit every record (up to max_records)
                test_ids = record_ids
                if self.max_records is not None:
                    test_ids = record_ids[: self.max_records]

                success, fail = await self._probe_edit_all(
                    object_name, test_ids, sem
                )
                result.edit_success_count = success
                result.edit_fail_count = fail
                result.can_edit = success > 0
            else:
                # Standard: edit one record
                edit_target = (
                    result.created_record_id
                    or (record_ids[0] if record_ids else None)
                )
                if edit_target:
                    result.can_edit = await self._probe_edit_one(
                        object_name, edit_target
                    )
                    if result.can_edit:
                        result.edit_success_count = 1

        # --- DELETE ---
        if not self.dry_run:
            if self.aggressive and record_ids:
                if object_name in CRUD_DELETE_PROTECTED_OBJECTS:
                    if self.unsafe and result.created_record_id:
                        # --unsafe: only delete the record WE created, never
                        # existing records for protected objects.
                        resp = await self.client.delete_record(
                            result.created_record_id
                        )
                        if _is_probe_success(resp):
                            result.can_delete = True
                            self._report.deletions.append(
                                {
                                    "object": object_name,
                                    "record_id": result.created_record_id,
                                    "fields": {},
                                    "deleted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                }
                            )
                            # Remove from cleanup list — already deleted
                            if result.created_record_id in self._report.created_record_ids:
                                self._report.created_record_ids.remove(
                                    result.created_record_id
                                )
                    else:
                        logger.debug(
                            "Skipping aggressive delete for protected object %s "
                            "(use --unsafe to override)",
                            object_name,
                        )
                else:
                    # Aggressive: delete one existing record
                    # Pick a random record that isn't the one we created
                    candidates = [
                        rid
                        for rid in record_ids
                        if rid != result.created_record_id
                    ]
                    if candidates:
                        target_id = random.choice(candidates)
                        result.can_delete = await self._probe_delete_existing(
                            object_name, target_id
                        )
            elif result.created_record_id:
                # Standard: try to delete our own created record
                # (actual deletion happens in cleanup phase)
                # We'll test delete permission there
                pass

        self._report.results[object_name] = result

    async def _probe_create(self, object_name: str) -> Optional[str]:
        """Attempt to create a dummy record. Returns the created ID or None."""
        fields_meta = self._object_fields.get(object_name, [])
        if not fields_meta:
            return None

        # Build field values from required createable fields
        field_values: Dict[str, Any] = {}
        for f in fields_meta:
            if not f.get("createable", False):
                continue
            if not f.get("required", False):
                continue

            field_name = f["field_name"]
            # Skip Id — auto-assigned
            if field_name == "Id":
                continue

            # Resolve reference fields
            ref_id = None
            if f.get("is_reference") and f.get("reference_object"):
                ref_id = self._lookup_cache.get(f["reference_object"])
                if ref_id is None:
                    # Can't resolve required reference — skip this field
                    # and let Salesforce auto-default it (e.g. OwnerId
                    # defaults to the running user).  If the server truly
                    # requires it, the create call will return ERROR.
                    logger.debug(
                        "Skipping unresolvable ref %s.%s → %s (may auto-default)",
                        object_name,
                        field_name,
                        f["reference_object"],
                    )
                    continue

            value = generate_dummy_value(
                field_name=field_name,
                data_type=f.get("data_type", "string"),
                picklist_values=f.get("picklist_values"),
                length=f.get("length"),
                reference_id=ref_id,
            )
            field_values[field_name] = value

        if not field_values:
            # Object has no required createable fields — try with just Name
            name_field = next(
                (f for f in fields_meta if f["field_name"] == "Name" and f.get("createable")),
                None,
            )
            if name_field:
                field_values["Name"] = f"{FH_PROBE_PREFIX}Test_{object_name}"
            else:
                return None

        try:
            resp = await self.client.create_record(object_name, field_values)
            if _is_probe_success(resp):
                record_id = _get_created_record_id(resp)
                logger.debug("Created %s record: %s", object_name, record_id)
                return record_id
            else:
                logger.debug(
                    "Create failed for %s: %s",
                    object_name,
                    _get_error_message(resp),
                )
                return None
        except Exception as exc:
            logger.debug("Create exception for %s: %s", object_name, exc)
            return None

    async def _probe_edit_one(
        self, object_name: str, record_id: str
    ) -> bool:
        """Attempt a no-op edit on a single record (save/restore)."""
        fields_meta = self._object_fields.get(object_name, [])

        # Find an updateable field to do a no-op write
        update_field = next(
            (
                f
                for f in fields_meta
                if f.get("updateable")
                and f["field_name"] not in ("Id", "OwnerId")
                and not f.get("is_reference")
            ),
            None,
        )

        if update_field is None:
            # No updateable fields — try with Id only (no-op)
            try:
                resp = await self.client.update_record(
                    record_id, {"Id": record_id}
                )
                return _is_probe_success(resp)
            except Exception:
                return False

        # Read current value, then write it back (no-op)
        try:
            read_resp = await self.client.get_record_with_fields(
                record_id,
                [f"{object_name}.Id"],
                [f"{object_name}.{update_field['field_name']}"],
            )
            rv = read_resp.get("actions", [{}])[0].get("returnValue")
            if rv is None:
                return False

            current_fields = rv.get("fields", {})
            field_name = update_field["field_name"]
            current_value = current_fields.get(field_name, {}).get("value")

            # Write the same value back
            resp = await self.client.update_record(
                record_id, {"Id": record_id, field_name: current_value}
            )
            return _is_probe_success(resp)
        except Exception as exc:
            logger.debug("Edit probe failed for %s/%s: %s", object_name, record_id, exc)
            return False

    async def _probe_edit_all(
        self,
        object_name: str,
        record_ids: List[str],
        sem: asyncio.Semaphore,
    ) -> tuple:
        """Attempt no-op edits on all given records. Returns (success, fail)."""
        success = 0
        fail = 0

        async def edit_one(rid: str) -> bool:
            async with sem:
                return await self._probe_edit_one(object_name, rid)

        results = await asyncio.gather(*(edit_one(rid) for rid in record_ids))
        for r in results:
            if r:
                success += 1
            else:
                fail += 1

        return success, fail

    async def _probe_delete_existing(
        self, object_name: str, record_id: str
    ) -> bool:
        """Delete one existing record (aggressive mode).

        Captures full record state before deletion into the deletion log.
        """
        # Read full record state before deletion
        fields_meta = self._object_fields.get(object_name, [])
        field_names = [
            f"{object_name}.{f['field_name']}"
            for f in fields_meta
            if f["field_name"] != "Id"
        ]

        record_state: Dict[str, Any] = {}
        try:
            resp = await self.client.get_record_with_fields(
                record_id,
                [f"{object_name}.Id"],
                field_names,
            )
            rv = resp.get("actions", [{}])[0].get("returnValue")
            if rv:
                for fname, fdata in rv.get("fields", {}).items():
                    record_state[fname] = fdata.get("value")
        except Exception as exc:
            logger.debug(
                "Could not read record state before delete %s/%s: %s",
                object_name, record_id, exc,
            )

        try:
            resp = await self.client.delete_record(record_id)
            if _is_probe_success(resp):
                self._report.deletions.append(
                    {
                        "object": object_name,
                        "record_id": record_id,
                        "fields": record_state,
                        "deleted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    }
                )
                return True
            else:
                logger.debug(
                    "Delete failed for %s/%s: %s",
                    object_name, record_id, _get_error_message(resp),
                )
                return False
        except Exception as exc:
            logger.debug("Delete exception for %s/%s: %s", object_name, record_id, exc)
            return False

    async def _cleanup_created_records(
        self, sem: asyncio.Semaphore
    ) -> None:
        """Delete all self-created records (standard mode cleanup).

        Deletes in reverse creation order (children before parents) so that
        referential integrity constraints are respected.  Also tests delete
        permission as a side effect.
        """
        # Map created IDs back to their object types
        id_to_object: Dict[str, str] = {}
        for obj_name, result in self._report.results.items():
            if result.created_record_id:
                id_to_object[result.created_record_id] = obj_name

        # Reverse: creation order is parents-first (topological), so
        # reversed gives children-first — safe for foreign-key deps.
        ordered_ids = list(reversed(self._report.created_record_ids))

        results: Dict[str, bool] = {}
        for rid in ordered_ids:
            async with sem:
                try:
                    resp = await self.client.delete_record(rid)
                    results[rid] = _is_probe_success(resp)
                except Exception:
                    results[rid] = False

        for rid, success in results.items():
            obj_name = id_to_object.get(rid)
            if obj_name and obj_name in self._report.results:
                self._report.results[obj_name].can_delete = success

        cleaned = sum(1 for s in results.values() if s)
        self._log(
            f"    Cleaned up {cleaned}/{len(self._report.created_record_ids)} records"
        )
