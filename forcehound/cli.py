"""Unified command-line interface for ForceHound.

Usage examples::

    # REST API mode (privileged)
    python -m forcehound --collector api \\
        --instance-url https://myorg.my.salesforce.com \\
        --session-id 00D...!AQ...

    # Aura mode (low-privilege)
    python -m forcehound --collector aura \\
        --instance-url https://myorg.lightning.force.com \\
        --session-id 00D...!AQ... \\
        --aura-context '{"mode":"PRODDEBUG",...}' \\
        --aura-token 'eyJ...'

    # Both backends — results merged into one graph
    # The API and Aura collectors use different Salesforce domains,
    # so ``both`` mode requires separate URLs and session IDs:
    python -m forcehound --collector both \\
        --api-instance-url https://myorg.my.salesforce.com \\
        --api-session-id 00D...!AQ...(api_sid) \\
        --instance-url https://myorg.lightning.force.com \\
        --session-id 00D...!AQ...(aura_sid) \\
        --aura-context '...' --aura-token '...'

Environment variable fallbacks (checked when CLI flags are omitted):
  ``FORCEHOUND_INSTANCE_URL``, ``FORCEHOUND_SESSION_ID``,
  ``FORCEHOUND_USERNAME``, ``FORCEHOUND_PASSWORD``,
  ``FORCEHOUND_SECURITY_TOKEN``, ``FORCEHOUND_AURA_CONTEXT``,
  ``FORCEHOUND_AURA_TOKEN``, ``FORCEHOUND_API_INSTANCE_URL``,
  ``FORCEHOUND_API_SESSION_ID``.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from typing import List, Optional

from forcehound.collectors.api_collector import APICollector
from forcehound.collectors.aura_collector import AuraCollector
from forcehound.constants import CollectorMode
from forcehound.graph.builder import GraphBuilder
from forcehound.models.auth import AuthConfig


def build_parser() -> argparse.ArgumentParser:
    """Construct the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="forcehound",
        description="ForceHound — Unified Salesforce BloodHound Collector",
    )

    # -- Collector mode --
    parser.add_argument(
        "--collector",
        choices=["api", "aura", "both"],
        default="aura",
        help="Collection backend to use (default: aura)",
    )

    # -- Connection (shared / Aura) --
    parser.add_argument(
        "--instance-url",
        default=os.environ.get("FORCEHOUND_INSTANCE_URL", ""),
        help="Salesforce instance URL — Lightning domain for Aura, "
        "My Domain for API (env: FORCEHOUND_INSTANCE_URL)",
    )
    parser.add_argument(
        "--session-id",
        default=os.environ.get("FORCEHOUND_SESSION_ID", ""),
        help="Session ID / access token (env: FORCEHOUND_SESSION_ID)",
    )

    # -- Connection (API-specific, for 'both' mode) --
    parser.add_argument(
        "--api-instance-url",
        default=os.environ.get("FORCEHOUND_API_INSTANCE_URL", ""),
        help="API instance URL (*.my.salesforce.com) — only needed with "
        "--collector both (env: FORCEHOUND_API_INSTANCE_URL)",
    )
    parser.add_argument(
        "--api-session-id",
        default=os.environ.get("FORCEHOUND_API_SESSION_ID", ""),
        help="API session ID — only needed with --collector both when the "
        "API sid differs from the Aura sid (env: FORCEHOUND_API_SESSION_ID)",
    )
    parser.add_argument(
        "--username",
        default=os.environ.get("FORCEHOUND_USERNAME", ""),
        help="Salesforce username (API mode, env: FORCEHOUND_USERNAME)",
    )
    parser.add_argument(
        "--password",
        default=os.environ.get("FORCEHOUND_PASSWORD", ""),
        help="Salesforce password (API mode, env: FORCEHOUND_PASSWORD)",
    )
    parser.add_argument(
        "--security-token",
        default=os.environ.get("FORCEHOUND_SECURITY_TOKEN", ""),
        help="Security token (API mode, env: FORCEHOUND_SECURITY_TOKEN)",
    )
    parser.add_argument(
        "--aura-context",
        default=os.environ.get("FORCEHOUND_AURA_CONTEXT", ""),
        help="Aura context JSON (env: FORCEHOUND_AURA_CONTEXT)",
    )
    parser.add_argument(
        "--aura-token",
        default=os.environ.get("FORCEHOUND_AURA_TOKEN", ""),
        help="Aura token JWT (env: FORCEHOUND_AURA_TOKEN)",
    )

    # -- Output --
    parser.add_argument(
        "-o",
        "--output",
        default="forcehound_output.json",
        help="Output file path (default: forcehound_output.json)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose progress output",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=30,
        help="Maximum concurrent requests for Aura mode (default: 30)",
    )
    parser.add_argument(
        "--risk-summary",
        action="store_true",
        help="Print per-user risk summary showing capabilities and their sources",
    )
    # -- Network --
    parser.add_argument(
        "--proxy",
        default=os.environ.get("FORCEHOUND_PROXY", ""),
        help="HTTP/HTTPS proxy URL (e.g., http://127.0.0.1:8080 for Burp). "
        "Routes all traffic through the proxy (env: FORCEHOUND_PROXY).",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=None,
        metavar="REQ/S",
        help="Maximum requests per second (e.g., --rate-limit 5). "
        "Applies globally across all backends (env: FORCEHOUND_RATE_LIMIT).",
    )

    # -- Aura-specific tuning --
    parser.add_argument(
        "--page-size",
        type=int,
        default=None,
        help="GraphQL page size for Aura record enumeration (default: 2000). "
        "Set to a small value (e.g. 2) to test pagination on small orgs.",
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="(Aura) Only collect active User records via GraphQL where clause",
    )
    parser.add_argument(
        "--aura-path",
        default="/aura",
        help="Path to the Aura endpoint (default: /aura). For Digital "
        "Experience / Community sites, use /s/sfsites/aura or "
        "/<community-prefix>/s/sfsites/aura.",
    )

    # -- BloodHound CE API --
    parser.add_argument(
        "--bh-url",
        default=os.environ.get("FORCEHOUND_BH_URL", "http://localhost:8080"),
        help="BloodHound CE base URL (default: http://localhost:8080, "
        "env: FORCEHOUND_BH_URL)",
    )
    parser.add_argument(
        "--bh-token-id",
        default=os.environ.get("FORCEHOUND_BH_TOKEN_ID", ""),
        help="BloodHound API token ID — UUID (env: FORCEHOUND_BH_TOKEN_ID)",
    )
    parser.add_argument(
        "--bh-token-key",
        default=os.environ.get("FORCEHOUND_BH_TOKEN_KEY", ""),
        help="BloodHound API token key — base64-encoded secret "
        "(env: FORCEHOUND_BH_TOKEN_KEY)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Auto-upload output to BloodHound CE after collection. "
        "Requires --bh-token-id and --bh-token-key.",
    )
    parser.add_argument(
        "--upload-file-name",
        default=None,
        help="Display name for the uploaded file in BloodHound CE's "
        "File Ingest page. Defaults to the basename of -o/--output.",
    )
    parser.add_argument(
        "--clear-db",
        action="store_true",
        help="Clear BloodHound database before uploading (requires --upload).",
    )
    parser.add_argument(
        "--clear-db-only",
        action="store_true",
        help="Clear BloodHound database and exit — no collection. "
        "Requires --bh-token-id and --bh-token-key.",
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Register ForceHound custom node types in BloodHound CE "
        "and exit. Requires --bh-token-id and --bh-token-key.",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=60,
        help="Seconds to wait after clearing the database before uploading "
        "(default: 60). Use with --clear-db to let BH CE finish cleanup.",
    )

    # -- Collection scope --
    parser.add_argument(
        "--skip-object-permissions",
        action="store_true",
        help="Skip ObjectPermissions CRUD collection. This dramatically "
        "reduces output size (CRUD edges are typically 99%% of all "
        "edges) while preserving identity, capability, and share data.",
    )
    parser.add_argument(
        "--skip-shares",
        action="store_true",
        help="Skip Share-object discovery and queries. Eliminates "
        "SF_Record nodes and ExplicitAccess/Owns/InheritsAccess "
        "edges, which can be very large on orgs with many sharing rules.",
    )
    parser.add_argument(
        "--skip-field-permissions",
        action="store_true",
        help="Skip FieldPermissions (FLS) collection. Eliminates SF_Field "
        "nodes and CanReadField/CanEditField edges. Use this if field-level "
        "access paths are not needed.",
    )
    parser.add_argument(
        "--skip-entity-definitions",
        action="store_true",
        help="Skip EntityDefinition metadata query. Disables per-object "
        "sharing model enrichment (InternalSharingModel, ExternalSharingModel) "
        "on SF_Object nodes.",
    )

    # -- Audit logging --
    parser.add_argument(
        "--audit-log",
        type=int,
        choices=[1, 2, 3],
        default=None,
        metavar="{1,2,3}",
        help="Enable forensic audit logging at the given verbosity level. "
        "1=activity ledger, 2=+headers/duration, 3=+full request/response bodies. "
        "Output: forcehound_audit_<timestamp>.jsonl",
    )

    # -- CRUD probing (Aura-only) --
    parser.add_argument(
        "--crud",
        action="store_true",
        help="(Aura) Empirically probe CRUD permissions by attempting "
        "actual DML operations. Creates dummy records, edits, and "
        "deletes to determine effective access.",
    )
    parser.add_argument(
        "--aggressive",
        action="store_true",
        help="(Aura) Aggressive CRUD mode: edit every record (save/restore), "
        "delete one existing record per object type. Requires --crud. "
        "WARNING: This modifies and deletes real data.",
    )
    parser.add_argument(
        "--crud-objects",
        default=None,
        help="Comma-separated list of objects to probe (default: all). "
        "Example: --crud-objects Account,Contact,Opportunity",
    )
    parser.add_argument(
        "--crud-dry-run",
        action="store_true",
        help="(Aura) Log CRUD probe plan without executing DML operations.",
    )
    parser.add_argument(
        "--crud-concurrency",
        type=int,
        default=5,
        help="Max concurrent requests for CRUD probing (default: 5).",
    )
    parser.add_argument(
        "--crud-max-records",
        type=int,
        default=None,
        help="Max records to test per object in aggressive edit mode "
        "(default: no cap — all records tested).",
    )
    parser.add_argument(
        "--unsafe",
        action="store_true",
        help="(Aura) Allow CRUD delete-probing of protected identity/config "
        "objects (User, Profile, PermissionSet, Role, etc.) by deleting the "
        "self-created record. Without --unsafe, protected objects are never "
        "delete-probed in aggressive mode. Requires --aggressive.",
    )

    return parser


async def run(args: argparse.Namespace) -> int:
    """Execute the collection run and save the output.

    Returns:
        Exit code — 0 on success, 1 on error.
    """

    # -- Standalone database clear (no collection) --
    if args.clear_db_only:
        if not args.bh_token_id or not args.bh_token_key:
            print(
                "ERROR: --clear-db-only requires --bh-token-id and "
                "--bh-token-key (or FORCEHOUND_BH_TOKEN_ID / "
                "FORCEHOUND_BH_TOKEN_KEY env vars).",
                file=sys.stderr,
            )
            return 1

        from forcehound.bloodhound.client import BloodHoundClient, BloodHoundAPIError

        bh = BloodHoundClient(args.bh_url, args.bh_token_id, args.bh_token_key)
        try:
            bh.clear_database()
            print("BloodHound database cleared successfully.")
        except BloodHoundAPIError as exc:
            print(f"BloodHound API error: {exc}", file=sys.stderr)
            return 1
        return 0

    # -- Register custom node types and exit --
    if args.setup:
        if not args.bh_token_id or not args.bh_token_key:
            print(
                "ERROR: --setup requires --bh-token-id and "
                "--bh-token-key (or FORCEHOUND_BH_TOKEN_ID / "
                "FORCEHOUND_BH_TOKEN_KEY env vars).",
                file=sys.stderr,
            )
            return 1

        from forcehound.bloodhound.client import BloodHoundClient, BloodHoundAPIError

        bh = BloodHoundClient(args.bh_url, args.bh_token_id, args.bh_token_key)
        try:
            registered = bh.register_custom_nodes()
            print("Registered custom node types:")
            for name in registered:
                print(f"  {name}")
        except BloodHoundAPIError as exc:
            print(f"BloodHound API error: {exc}", file=sys.stderr)
            return 1
        return 0

    # Resolve --rate-limit env var fallback (argparse can't do this for
    # optional float args with None default).
    if args.rate_limit is None:
        env_rate = os.environ.get("FORCEHOUND_RATE_LIMIT", "")
        if env_rate:
            try:
                args.rate_limit = float(env_rate)
            except ValueError:
                pass

    proxy = args.proxy or None
    rate_limit = args.rate_limit

    mode = CollectorMode(args.collector)
    builder = GraphBuilder()

    # Build auth configs.  In 'both' mode the API collector may need a
    # different instance_url and session_id (*.my.salesforce.com) than
    # the Aura collector (*.lightning.force.com).
    aura_auth = AuthConfig(
        instance_url=args.instance_url,
        session_id=args.session_id,
        username=args.username,
        password=args.password,
        security_token=args.security_token,
        aura_context=args.aura_context,
        aura_token=args.aura_token,
    )

    if mode == CollectorMode.BOTH:
        api_url = args.api_instance_url or args.instance_url
        # Only inherit the Aura session_id for the API side when an
        # explicit --api-session-id was provided OR when username/password
        # auth is not available.  When the user supplies username+password
        # the API collector should use those instead of the Aura sid
        # cookie (which is typically scoped to the Lightning domain and
        # invalid for the REST API domain).
        if args.api_session_id:
            api_sid = args.api_session_id
        elif args.username and args.password:
            api_sid = ""
        else:
            api_sid = args.session_id
        api_auth = AuthConfig(
            instance_url=api_url,
            session_id=api_sid,
            username=args.username,
            password=args.password,
            security_token=args.security_token,
        )
    else:
        api_auth = aura_auth

    # Set up audit logging
    audit_logger = None
    if args.audit_log is not None:
        from forcehound.audit import setup_audit_log

        audit_logger = setup_audit_log(
            level=args.audit_log,
            collector=args.collector,
            instance_url=args.instance_url,
            org_id="",  # resolved later
            cli_args=" ".join(sys.argv[1:]),
        )
        print(f"  Audit log: {audit_logger.file_path} (level {args.audit_log})")

    # Validate --aggressive requires --crud
    if args.aggressive and not args.crud:
        print(
            "ERROR: --aggressive requires --crud.",
            file=sys.stderr,
        )
        if audit_logger:
            audit_logger.close()
        return 1

    # Validate --unsafe requires --aggressive
    if getattr(args, "unsafe", False) and not args.aggressive:
        print(
            "ERROR: --unsafe requires --aggressive.",
            file=sys.stderr,
        )
        if audit_logger:
            audit_logger.close()
        return 1

    # Parse --crud-objects into a set
    crud_objects_set = None
    if args.crud_objects:
        crud_objects_set = set(args.crud_objects.split(","))

    try:
        # API-only mode — full 15-query collection.
        if mode == CollectorMode.API:
            api = APICollector(
                api_auth,
                verbose=args.verbose,
                skip_object_permissions=args.skip_object_permissions,
                active_only=args.active_only,
                skip_shares=args.skip_shares,
                skip_field_permissions=args.skip_field_permissions,
                skip_entity_definitions=args.skip_entity_definitions,
                audit_logger=audit_logger,
                proxy=proxy,
                rate_limit=rate_limit,
            )
            result = await api.collect()
            builder.add_result(result)

        # Aura-only mode — full Aura collection.
        elif mode == CollectorMode.AURA:
            aura = AuraCollector(
                aura_auth,
                verbose=args.verbose,
                max_workers=args.max_workers,
                page_size=args.page_size,
                active_only=args.active_only,
                aura_path=args.aura_path,
                crud=args.crud,
                aggressive=args.aggressive,
                crud_objects=crud_objects_set,
                crud_dry_run=args.crud_dry_run,
                crud_concurrency=args.crud_concurrency,
                crud_max_records=args.crud_max_records,
                audit_logger=audit_logger,
                unsafe=getattr(args, "unsafe", False),
                proxy=proxy,
                rate_limit=rate_limit,
            )
            try:
                result = await aura.collect()
                builder.add_result(result)
            finally:
                await aura.close()

        # Both mode — Aura first, then API supplement.
        elif mode == CollectorMode.BOTH:
            # Step 1: Aura handles Users, Profiles, Roles, Groups,
            # GroupMembers, Namespaced Objects.
            aura = AuraCollector(
                aura_auth,
                verbose=args.verbose,
                max_workers=args.max_workers,
                page_size=args.page_size,
                active_only=args.active_only,
                aura_path=args.aura_path,
                crud=args.crud,
                aggressive=args.aggressive,
                crud_objects=crud_objects_set,
                crud_dry_run=args.crud_dry_run,
                crud_concurrency=args.crud_concurrency,
                crud_max_records=args.crud_max_records,
                audit_logger=audit_logger,
                unsafe=getattr(args, "unsafe", False),
                proxy=proxy,
                rate_limit=rate_limit,
            )
            try:
                aura_result = await aura.collect()
                builder.add_result(aura_result)
            finally:
                await aura.close()

            aura_user_count = aura_result.metadata.get("users", 0)

            if aura_user_count == 0:
                print(
                    "WARNING: Aura collector returned 0 users "
                    "(session may have expired). Falling back to "
                    "full API collection.",
                    file=sys.stderr,
                )
                api = APICollector(
                    api_auth,
                    verbose=args.verbose,
                    skip_object_permissions=args.skip_object_permissions,
                    active_only=args.active_only,
                    skip_shares=args.skip_shares,
                    skip_field_permissions=args.skip_field_permissions,
                    skip_entity_definitions=args.skip_entity_definitions,
                    audit_logger=audit_logger,
                    proxy=proxy,
                    rate_limit=rate_limit,
                )
                api_result = await api.collect()
                builder.add_result(api_result)
            else:
                known_ids = {n.id for n in aura_result.nodes}
                api = APICollector(
                    api_auth,
                    verbose=args.verbose,
                    supplement_only=True,
                    known_node_ids=known_ids,
                    org_id=aura_result.org_id,
                    skip_object_permissions=args.skip_object_permissions,
                    skip_shares=args.skip_shares,
                    skip_field_permissions=args.skip_field_permissions,
                    skip_entity_definitions=args.skip_entity_definitions,
                    audit_logger=audit_logger,
                    proxy=proxy,
                    rate_limit=rate_limit,
                )
                api_result = await api.collect()
                builder.add_result(api_result)

    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        if audit_logger:
            audit_logger.close()
        return 1
    except Exception as exc:
        print(f"Collection failed: {exc}", file=sys.stderr)
        if audit_logger:
            audit_logger.close()
        return 1

    # Save output
    builder.save(args.output)

    # Save deletion log if aggressive CRUD was used
    if args.crud and args.aggressive:
        _save_deletion_log(builder)

    # Print summary
    summary = builder.get_summary()
    total_requests = sum(
        r.metadata.get("queries", 0) or r.metadata.get("requests", 0)
        for r in builder._results
    )
    print("\nForceHound collection complete!")
    print(f"  Nodes: {summary['total_nodes']}")
    print(f"  Edges: {summary['total_edges']}")
    print(f"  Requests: {total_requests}")
    print(f"  Output: {args.output}")

    if args.verbose:
        print("\nNode kinds:")
        for kind, count in sorted(summary["node_kinds"].items()):
            print(f"  {kind}: {count}")
        print("\nEdge kinds:")
        for kind, count in sorted(summary["edge_kinds"].items()):
            print(f"  {kind}: {count}")

    if args.risk_summary:
        _print_risk_summary(builder)

    # Upload to BloodHound CE
    if args.upload:
        if not args.bh_token_id or not args.bh_token_key:
            print(
                "ERROR: --upload requires --bh-token-id and --bh-token-key "
                "(or FORCEHOUND_BH_TOKEN_ID / FORCEHOUND_BH_TOKEN_KEY env vars).",
                file=sys.stderr,
            )
            return 1

        from forcehound.bloodhound.client import BloodHoundClient, BloodHoundAPIError

        bh = BloodHoundClient(args.bh_url, args.bh_token_id, args.bh_token_key)
        try:
            if args.clear_db:
                bh.clear_database()
                print("  BloodHound database cleared.")
                if args.wait > 0:
                    print(f"  Waiting {args.wait}s...")
                    time.sleep(args.wait)

            job_id = bh.upload_graph(args.output, file_name=args.upload_file_name)
            print(f"  Uploaded to BloodHound CE (job {job_id}).")
        except BloodHoundAPIError as exc:
            print(f"BloodHound API error: {exc}", file=sys.stderr)
            return 1

    # Close audit log
    if audit_logger:
        audit_logger.close()
        print(f"  Audit log saved: {audit_logger.file_path}")

    return 0


def _save_deletion_log(builder: GraphBuilder) -> None:
    """Save a log of records deleted during aggressive CRUD probing."""
    import datetime
    import json

    all_deletions = []
    org_id = ""
    for result in builder._results:
        deletions = result.metadata.get("crud_deletions", [])
        if deletions:
            all_deletions.extend(deletions)
            org_id = org_id or result.org_id

    if not all_deletions:
        return

    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = f"forcehound_deletions_{timestamp}.json"
    log_data = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z",
        "org_id": org_id,
        "deletion_count": len(all_deletions),
        "deletions": all_deletions,
    }

    with open(log_path, "w") as f:
        json.dump(log_data, f, indent=2)

    print(f"  Deletion log saved: {log_path} ({len(all_deletions)} records)")


def _print_risk_summary(builder: GraphBuilder) -> None:
    """Print a per-user risk summary to stdout."""
    risk = builder.get_risk_summary()
    if not risk:
        print("\nRisk Summary: No users with dangerous capabilities found.")
        return

    print(f"\nRisk Summary ({len(risk)} user(s) with capabilities):")
    for user_id, info in risk.items():
        status = "active" if info["is_active"] else "inactive"
        email_part = f" ({info['email']})" if info["email"] else ""
        print(f"\n  {info['name']}{email_part} [{status}]:")
        for cap_kind, sources in sorted(info["capabilities"].items()):
            source_strs = [f"{name} ({stype})" for name, stype in sources]
            print(f"    {cap_kind} via: {', '.join(source_strs)}")


def main(argv: Optional[List[str]] = None) -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    exit_code = asyncio.run(run(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
