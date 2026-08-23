"""CLI for validating configs, scaffolding src, and generating pipelines."""

from __future__ import annotations

import argparse
import json
import sys

from pydantic import ValidationError

from util.bundle_config import (
    resolve_dest_schema_suffix,
    resolve_ipac_metadata_schema,
    resolve_num_of_tables_in_pipeline,
    resolve_pipeline_use_instance_pool,
    resolve_recon_cluster_tier,
    resolve_uc_catalog,
    pipeline_tag_var_ref,
    uc_catalog_var_ref,
)
from util.cluster_generator import write_ingestion_recon_job_yaml
from util.config_loader import (
    format_validation_error,
    get_client,
    list_active_clients,
    list_client_names,
    load_client_overrides,
    load_cluster_config,
    load_common_tables,
    load_client_registry,
    validate_all,
)
from util.paths import (
    client_sql_dir,
    generated_bundle_dir,
    generated_config_dir,
    generated_config_schema_dir,
    generated_schema_dir,
    resources_jobs_dir,
)
from util.schema_generator import write_client_schema_resource_yaml, write_ipac_metadata_schema_resource_yaml
from util.pipeline_generator import (
    chunk_tables,
    generate_client_pipelines_yaml,
    pipeline_resource_key,
    write_bundle_pipeline_yaml,
)
from util.metadata_table_generator import write_process_log_table_sql, write_recon_tables_sql
from util.pipeline_registry import write_pipeline_name_registry
from util.resolver import resolve_effective_tables
from util.sql_generator import write_source_replication_sql
from util.src_scaffold import (
    remove_client_generated_artifacts,
    remove_generated_pipeline_artifacts,
    remove_stale_client_dirs,
    scaffold_src_tree,
)


def _cmd_list_clients(args: argparse.Namespace) -> int:
    names = list_client_names(active_only=args.active_only)
    if not names:
        print("No clients in config/common/client.json")
        return 0
    for name in names:
        print(name)
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    client_names = args.client or list_client_names(active_only=True)
    if not client_names:
        print("No active clients to validate.", file=sys.stderr)
        return 1

    errors = 0
    for client_nm in client_names:
        try:
            validate_all(client_nm)
            print(f"OK  {client_nm}")
        except (ValidationError, ValueError, FileNotFoundError) as exc:
            errors += 1
            print(f"FAIL {client_nm}: {exc}", file=sys.stderr)
            if isinstance(exc, ValidationError):
                print(format_validation_error(exc), file=sys.stderr)

    return 1 if errors else 0


def _cmd_resolve(args: argparse.Namespace) -> int:
    catalog = load_common_tables()
    client = get_client(args.client)
    overrides = load_client_overrides(client.client_nm)
    tables = resolve_effective_tables(client, catalog, overrides)
    if args.json:
        print(json.dumps([t.model_dump() for t in tables], indent=2))
    else:
        for t in tables:
            cluster = f"cluster={t.lq_key}" if t.has_cluster_by else "no_cluster"
            print(
                f"{t.table_nm:<35}  scd={t.scd_type} recon={t.recon_type}  {cluster}  "
                f"select={t.select_cols}  [{t.source}]"
            )
    return 0


def _cmd_sync_src(args: argparse.Namespace) -> int:
    registry = load_client_registry()
    clients = (
        [get_client(args.client, registry)]
        if args.client
        else list_active_clients(registry)
    )
    created = scaffold_src_tree(clients, force_placeholders=args.force)
    if created:
        for path in created:
            print(f"Created {path}")
    else:
        print("Src tree already up to date.")
    return 0


def _log(msg: str) -> None:
    print(msg, flush=True)


def _cmd_generate(args: argparse.Namespace) -> int:
    registry = load_client_registry()
    catalog = load_common_tables()
    cluster_cfg = load_cluster_config()

    clients = (
        [get_client(args.client, registry)]
        if args.client
        else list_active_clients(registry)
    )
    if not clients:
        print("No active clients to generate.", file=sys.stderr)
        return 1

    client_names = ", ".join(c.client_nm for c in clients)
    _log(f"Generating for {len(clients)} client(s): {client_names}")

    if not args.stdout and not getattr(args, "no_clean", False):
        stale_dirs: list[str] = []
        if args.client:
            _log("Cleaning generated bundle/schema for selected client(s)...")
            removed: list[str] = []
            for c in clients:
                removed.extend(remove_client_generated_artifacts(c.client_nm))
        else:
            _log("Cleaning old generated pipeline/schema artifacts...")
            removed = remove_generated_pipeline_artifacts()
            stale_dirs = remove_stale_client_dirs({c.client_nm for c in clients})
        if removed:
            _log(f"Removed {len(removed)} old generated file(s).")
        if not args.client and stale_dirs:
            _log(f"Removed {len(stale_dirs)} stale client src folder(s).")

    _log("Scaffolding src tree...")
    scaffold_src_tree(clients)

    bundle_dir = args.output_dir or generated_bundle_dir()
    uc_catalog_ref = uc_catalog_var_ref()
    resolved_uc_catalog = resolve_uc_catalog(
        override=getattr(args, "uc_catalog", None),
        target=getattr(args, "target", None),
    )
    num_tables = resolve_num_of_tables_in_pipeline(
        override=getattr(args, "num_of_tables", None),
        target=getattr(args, "target", None),
    )
    dest_schema_suffix = resolve_dest_schema_suffix(
        override=getattr(args, "dest_schema_suffix", None),
        target=getattr(args, "target", None),
    )
    pipeline_tag_ref = pipeline_tag_var_ref()
    recon_cluster_tier = resolve_recon_cluster_tier(
        override=getattr(args, "recon_cluster_tier", None),
        target=getattr(args, "target", None),
    )
    metadata_schema = resolve_ipac_metadata_schema(
        override=getattr(args, "ipac_metadata_schema", None),
        target=getattr(args, "target", None),
    )
    use_instance_pool = resolve_pipeline_use_instance_pool(
        target=getattr(args, "target", None),
    )
    schema_dir = generated_schema_dir()
    config_schema_dir = generated_config_schema_dir()
    config_dir = generated_config_dir()
    generated_pipeline_names: list[str] = []
    errors = 0

    if not args.stdout:
        ipac_metadata_schema_path = write_ipac_metadata_schema_resource_yaml(
            config_schema_dir,
            uc_catalog_ref=uc_catalog_ref,
        )
        print(f"Generated {ipac_metadata_schema_path}", flush=True)
        recon_job_path = write_ingestion_recon_job_yaml(
            cluster_cfg,
            recon_cluster_tier,
            resources_jobs_dir() / "ingestion_recon_jobs.yml",
        )
        print(f"Generated {recon_job_path}", flush=True)

    for client in clients:
        try:
            _log(f"--- {client.client_nm}: resolving tables...")
            overrides = load_client_overrides(client.client_nm)
            tables = resolve_effective_tables(client, catalog, overrides)

            if args.stdout:
                print(
                    generate_client_pipelines_yaml(
                        client,
                        tables,
                        cluster_cfg,
                        uc_catalog_ref=uc_catalog_ref,
                        resolved_uc_catalog=resolved_uc_catalog,
                        num_of_tables_in_pipeline=num_tables,
                        dest_schema_suffix=dest_schema_suffix,
                        pipeline_tag_ref=pipeline_tag_ref,
                        use_instance_pool=use_instance_pool,
                    )
                )
                print()
                continue

            _log(f"--- {client.client_nm}: writing UC schema resource...")
            schema_resource_path = write_client_schema_resource_yaml(
                client,
                dest_schema_suffix=dest_schema_suffix,
                output_dir=config_schema_dir,
                uc_catalog_ref=uc_catalog_ref,
            )
            _log(f"--- {client.client_nm}: writing bundle pipeline YAML ({len(tables)} tables)...")
            bundle_path = write_bundle_pipeline_yaml(
                client,
                tables,
                bundle_dir,
                cluster_cfg,
                uc_catalog_ref=uc_catalog_ref,
                resolved_uc_catalog=resolved_uc_catalog,
                num_of_tables_in_pipeline=num_tables,
                dest_schema_suffix=dest_schema_suffix,
                pipeline_tag_ref=pipeline_tag_ref,
                metadata_schema=metadata_schema,
                use_instance_pool=use_instance_pool,
            )
            pipeline_batches = chunk_tables(tables, num_tables)
            generated_pipeline_names.extend(
                pipeline_resource_key(client.client_nm, serial)
                for serial in range(1, len(pipeline_batches) + 1)
            )
            _log(f"--- {client.client_nm}: writing SQL scripts...")
            enable_path, ct_grant_path, cdc_grant_path, status_path = write_source_replication_sql(
                client,
                tables,
                client_sql_dir(client.client_nm),
                principal_placeholder="<KEEP_USER_ID>",
            )
            print(f"Generated {schema_resource_path}", flush=True)
            print(
                f"Generated {bundle_path} ({len(tables)} tables, "
                f"{len(pipeline_batches)} pipeline(s), batch={num_tables})",
                flush=True,
            )
            print(f"Generated {enable_path}", flush=True)
            print(f"Generated {ct_grant_path}", flush=True)
            print(f"Generated {cdc_grant_path}", flush=True)
            print(f"Generated {status_path}", flush=True)
        except (ValidationError, ValueError, FileNotFoundError) as exc:
            errors += 1
            print(f"FAIL {client.client_nm}: {exc}", file=sys.stderr)

    if not args.stdout:
        _log("Writing metadata DDL and pipeline registry...")
        process_log_sql_path = write_process_log_table_sql(
            resolved_uc_catalog,
            metadata_schema,
            schema_dir,
        )
        recon_sql_path = write_recon_tables_sql(
            resolved_uc_catalog,
            metadata_schema,
            schema_dir,
        )
        registry_path = write_pipeline_name_registry(config_dir, generated_pipeline_names)
        print(
            "Metadata schema: generated/config/schema/ipac_metadata_schema.yml",
            flush=True,
        )
        print(
            "Client schemas: generated/config/schema/<client_nm>_schema.yml",
            flush=True,
        )
        print(f"Generated {process_log_sql_path}", flush=True)
        print(f"Generated {recon_sql_path}", flush=True)
        print(f"Generated {registry_path}", flush=True)
        _log("Done.")

    return 1 if errors else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ipac-delta-sync",
        description="JSON config/common-driven Lakeflow Connect scaffolding",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    list_p = sub.add_parser("list-clients", help="List clients from config/common/client.json")
    list_p.add_argument(
        "--active-only",
        action="store_true",
        help="Only list is_active clients",
    )

    validate_p = sub.add_parser("validate", help="Validate config/common JSON files")
    validate_p.add_argument("--client", action="append", help="Client_nm (default: all active)")

    resolve_p = sub.add_parser("resolve", help="Show effective tables for a client")
    resolve_p.add_argument("--client", required=True)
    resolve_p.add_argument("--json", action="store_true")

    sync_p = sub.add_parser("sync-src", help="Create src/common and src/<client> folders")
    sync_p.add_argument("--client", help="Single client_nm")
    sync_p.add_argument("--force", action="store_true", help="Overwrite placeholder __init__.py")

    generate_p = sub.add_parser("generate", help="Scaffold src + generate pipeline YAML")
    generate_p.add_argument("--client", help="Single client_nm")
    generate_p.add_argument("--output-dir", help="Bundle output dir (default: generated/bundle)")
    generate_p.add_argument("--stdout", action="store_true")
    generate_p.add_argument(
        "--uc-catalog",
        help="Override UC catalog literal for YAML comments (bundle uses ${var.uc_catalog})",
    )
    generate_p.add_argument(
        "--target",
        help="databricks.yml target name when resolving bundle variable defaults",
    )
    generate_p.add_argument(
        "--num-of-tables",
        type=int,
        help="Override num_of_tables_in_pipeline for this generate run (default from databricks.yml)",
    )
    generate_p.add_argument(
        "--no-clean",
        action="store_true",
        help="Skip deleting old generated pipeline/schema files before writing (overwrites in place)",
    )
    generate_p.add_argument(
        "--dest-schema-suffix",
        help="Override destination schema suffix (default from databricks.yml var.dest_schema_suffix)",
    )
    generate_p.add_argument(
        "--recon-cluster-tier",
        help="Override recon cluster tier j1/j2/j3 (default from databricks.yml var.recon_cluster_tier)",
    )
    generate_p.add_argument(
        "--pipeline-tag",
        help="Override pipeline tag literal for YAML comments (bundle uses ${var.pipeline_tag})",
    )
    generate_p.add_argument(
        "--ipac-metadata-schema",
        help="Override metadata schema name (default from databricks.yml var.ipac_metadata_schema)",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    commands = {
        "list-clients": _cmd_list_clients,
        "validate": _cmd_validate,
        "resolve": _cmd_resolve,
        "sync-src": _cmd_sync_src,
        "generate": _cmd_generate,
    }
    return commands[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
