"""`de-accel` CLI. Thin layer over the library (no business logic here).

Exit codes: 0 ok · 1 validation/configuration/user error · 2 concurrency conflict (retry after re-reading state) · 3 unexpected.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from de_accelerator.core.bootstrap import BootstrapConfig, load_bootstrap
from de_accelerator.core.errors import AcceleratorError, ActivationConflictError, ConfigError, ValidationFailedError
from de_accelerator.core.naming import ENVIRONMENTS
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.packs.bundle import PackBundle
from de_accelerator.metadata.reader.source import StoreConfigSource
from de_accelerator.metadata.release import PackLoader, ReleaseManager
from de_accelerator.metadata.release.history import ChangeDiffer
from de_accelerator.metadata.snapshot import SnapshotBuilder, SnapshotScope, SnapshotStore
from de_accelerator.metadata.store.base import TableStore
from de_accelerator.metadata.validation import validate_pack

StoreFactory = Callable[[BootstrapConfig], TableStore]
EngineFactory = Callable[[BootstrapConfig, TableStore], Any]


def default_engine_factory(cfg: BootstrapConfig, store: TableStore) -> Any:
    """Build the ingestion engine for a bootstrap config (secrets: Key Vault scope on Databricks, env vars locally)."""
    import de_accelerator.connectors.builtin  # noqa: F401  (registers connectors)
    from de_accelerator.core.secrets import DatabricksSecretProvider, EnvSecretProvider
    from de_accelerator.ingestion.engine import EngineConfig, IngestionEngine

    secrets = EnvSecretProvider(cfg.environment) if cfg.flavor == "LOCAL" else DatabricksSecretProvider(cfg.secret_scope or "")
    ecfg = EngineConfig(env=cfg.environment, catalog=cfg.catalog, flavor=cfg.flavor)
    return IngestionEngine(store, store.spark, secrets, ecfg)  # type: ignore[attr-defined]


def default_store_factory(cfg: BootstrapConfig) -> TableStore:
    from pyspark.sql import SparkSession

    from de_accelerator.metadata.store.spark import SparkDeltaTableStore, build_local_spark

    if cfg.flavor == "LOCAL":
        if not cfg.warehouse_dir:
            raise ConfigError("flavor LOCAL requires warehouse_dir in the bootstrap config")
        return SparkDeltaTableStore(build_local_spark(cfg.warehouse_dir), flavor="LOCAL")
    return SparkDeltaTableStore(SparkSession.builder.getOrCreate(), catalog=cfg.catalog, flavor="DATABRICKS")


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="de-accel", description="Metadata management CLI")
    p.add_argument("--bootstrap", help="base bootstrap YAML (needed for commands that touch the metadata store)")
    p.add_argument("--bootstrap-env", help="optional per-environment bootstrap overlay YAML")
    p.add_argument("--actor", default="cli", help="user/service principal recorded in audit trails")
    p.add_argument("--json", action="store_true", help="machine-readable output")
    sub = p.add_subparsers(dest="cmd", required=True)

    v = sub.add_parser("validate-config", help="validate a metadata pack (no side effects)")
    v.add_argument("pack")
    v.add_argument("--env", default="all", choices=[*ENVIRONMENTS, "all"])
    v.add_argument("--deployed-schema", type=int, default=None, help="deployed metadata schema version to check against")
    v.add_argument("--core-version", default=None)

    b = sub.add_parser("build-bundle", help="validate and build immutable per-environment bundles")
    b.add_argument("pack")
    b.add_argument("--env", default="all", choices=[*ENVIRONMENTS, "all"])
    b.add_argument("--out", required=True)

    m = sub.add_parser("migrate", help="apply/rollback metadata schema migrations")
    m.add_argument("--target", type=int)
    m.add_argument("--dry-run", action="store_true")
    m.add_argument("--rollback-to", type=int)
    m.add_argument("--allow-destroy", action="store_true")

    ld = sub.add_parser("load-pack", help="validate + stage a bundle/pack as an (inactive) release")
    ld.add_argument("source", help="bundle .json or pack directory")
    for f in ("git-sha", "git-tag", "approved-by", "ci-run-url", "ticket"):
        ld.add_argument(f"--{f}")
    ld.add_argument("--activate", action="store_true")

    a = sub.add_parser("activate", help="make a staged release active")
    a.add_argument("release_id")
    a.add_argument("--expected-seq", type=int)
    a.add_argument("--ticket")

    r = sub.add_parser("rollback", help="re-point to the previous (or given) release")
    r.add_argument("--to")
    r.add_argument("--reason", default="")
    r.add_argument("--ticket")
    r.add_argument("--expected-seq", type=int)

    sub.add_parser("releases", help="list releases")
    sub.add_parser("status", help="show active release + schema version")

    s = sub.add_parser("snapshot", help="build + persist an immutable config snapshot of the active release")
    s.add_argument("--project", action="append", default=[])
    s.add_argument("--object", action="append", default=[], dest="objects")
    s.add_argument("--execution")

    ig = sub.add_parser("ingest", help="run source->Bronze ingestion for an execution (or selected objects)")
    ig.add_argument("--execution")
    ig.add_argument("--objects", help="comma separated object codes")
    ig.add_argument("--project")
    ig.add_argument("--run-id")
    ig.add_argument("--concurrency", type=int)
    ig.add_argument("--restart", action="store_true", help="resume batches that failed fatally after the cause was fixed")

    bs = sub.add_parser("batch-status", help="show recent batches of an object")
    bs.add_argument("--object", required=True)
    bs.add_argument("--stream", default="live")
    bs.add_argument("--limit", type=int, default=10)

    ab = sub.add_parser("abandon-batch", help="abandon the open batch of an object (never if its Bronze data is committed)")
    ab.add_argument("--object", required=True)
    ab.add_argument("--stream", default="live")
    ab.add_argument("--reason", required=True)

    rp = sub.add_parser("reprocess", help="re-extract an explicit window into Bronze as a separate REPROCESS stream (live cursor untouched)")
    rp.add_argument("--object", required=True)
    rp.add_argument("--project", required=True)
    rp.add_argument("--from", dest="frm", required=True)
    rp.add_argument("--to", required=True)
    rp.add_argument("--reason", required=True)

    d = sub.add_parser("diff", help="row-level diff between two releases")
    d.add_argument("--from", dest="frm", default="none")
    d.add_argument("--to", required=True)
    from de_accelerator.cli import transform as _transform

    _transform.add_parsers(sub)
    return p


def _emit(args: argparse.Namespace, text: str, data: Any, out: Callable[[str], None]) -> None:
    out(json.dumps(data, indent=2, sort_keys=True, default=str) if args.json else text)


def _envs(env: str, pack: str | None = None) -> list[str]:
    """`all` = the environments the pack declares in pack.yaml (falls back to DEV/QA/PROD if it cannot be read)."""
    if env != "all":
        return [env]
    if pack:
        try:
            import yaml

            declared = yaml.safe_load((Path(pack) / "pack.yaml").read_text(encoding="utf-8")).get("environments")
            if declared:
                return [e for e in ENVIRONMENTS if e in declared]
        except Exception:  # noqa: BLE001 - unreadable manifest is reported by the validator itself
            pass
    return list(ENVIRONMENTS)


def run(argv: Sequence[str] | None = None, store_factory: StoreFactory | None = None, out: Callable[[str], None] = print,
        engine_factory: EngineFactory | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return _dispatch(args, store_factory or default_store_factory, out, engine_factory or default_engine_factory)
    except ValidationFailedError as exc:
        out(f"ERROR {exc} codes={exc.context.get('codes')}")
        return 1
    except ActivationConflictError as exc:
        out(f"CONFLICT {exc}")
        return 2
    except AcceleratorError as exc:
        out(f"ERROR {exc}")
        return 1
    except Exception as exc:  # unexpected: don't hide it behind a stack trace dump, but flag distinctly
        out(f"UNEXPECTED {type(exc).__name__}: {exc}")
        return 3


def _dispatch(args: argparse.Namespace, factory: StoreFactory, out: Callable[[str], None], engine_factory: EngineFactory) -> int:
    if args.cmd == "validate-config":
        ok, results = True, []
        for env in _envs(args.env, args.pack):
            report, _ = validate_pack(args.pack, env, core_version=args.core_version, deployed_schema_version=args.deployed_schema)
            ok &= report.ok
            results.append(report)
            if not args.json:
                out(report.to_text())
        if args.json:
            out(json.dumps([r.to_dict() for r in results], indent=2, sort_keys=True))
        return 0 if ok else 1

    if args.cmd == "build-bundle":
        outdir = Path(args.out)
        outdir.mkdir(parents=True, exist_ok=True)
        ok, built = True, {}
        for env in _envs(args.env, args.pack):
            report, bundle = validate_pack(args.pack, env)
            if not report.ok or bundle is None:
                ok = False
                out(report.to_text())
                continue
            path = outdir / f"bundle_{env}.json"
            bundle.write(path)
            built[env] = {"path": str(path), "sha256": bundle.bundle_sha256}
            if not args.json:
                out(f"built {path} sha256={bundle.bundle_sha256} warnings={len(report.warnings)}")
        if args.json:
            out(json.dumps(built, indent=2, sort_keys=True))
        return 0 if ok else 1

    # ---- commands that need the metadata store -------------------------------------------------
    if not args.bootstrap:
        raise ConfigError("--bootstrap is required for this command")
    cfg = load_bootstrap(args.bootstrap, args.bootstrap_env)
    store = factory(cfg)
    env = cfg.environment

    if args.cmd == "migrate":
        mig = Migrator(store, MIGRATIONS, actor=args.actor)
        if args.rollback_to is not None:
            mig.rollback(args.rollback_to, allow_destroy=args.allow_destroy)
            out(f"rolled back to schema version {args.rollback_to}")
            return 0
        stmts = mig.apply(args.target, dry_run=args.dry_run, catalog=cfg.catalog)
        out(("DRY RUN: would execute" if args.dry_run else "executed") + f" {len(stmts)} statements; schema version = {mig.current_version()}")
        if args.dry_run and not args.json:
            out("\n;\n".join(stmts))
        return 0

    rm = ReleaseManager(store, env, args.actor)
    if args.cmd == "load-pack":
        src = Path(args.source)
        if src.is_dir():
            report, bundle = validate_pack(src, env, deployed_schema_version=Migrator(store, MIGRATIONS).current_version() or None)
            if not report.ok or bundle is None:
                out(report.to_text())
                return 1
        else:
            bundle = PackBundle.read(src)
        loader = PackLoader(store, env, args.actor)
        rel = loader.stage(bundle, git_sha=args.git_sha, git_tag=args.git_tag, approved_by=args.approved_by,
                           ci_run_url=args.ci_run_url, ticket=args.ticket)  # fmt: skip
        out(f"staged release {rel['release_id']} status={rel['status']} bundle={rel['bundle_sha256']}")
        if args.activate:
            ptr = rm.activate(rel["release_id"], ticket=args.ticket)
            out(f"activated release {ptr['release_id']} seq={ptr['activation_seq']}")
        return 0
    if args.cmd == "activate":
        ptr = rm.activate(args.release_id, args.expected_seq, args.ticket)
        out(f"active release {ptr['release_id']} seq={ptr['activation_seq']}")
        return 0
    if args.cmd == "rollback":
        ptr = rm.rollback(args.to, args.reason, args.ticket, args.expected_seq)
        out(f"rolled back: active release {ptr['release_id']} seq={ptr['activation_seq']}")
        return 0
    if args.cmd == "releases":
        rows = [{k: r[k] for k in ("release_id", "pack_version", "status", "git_sha", "approved_by", "staged_ts")}
                for r in rm.list_releases()]  # fmt: skip
        _emit(args, "\n".join(f"{r['release_id']}  {r['status']:<11} v{r['pack_version']}  git={r['git_sha']}" for r in rows) or "(none)",
              rows, out)  # fmt: skip
        return 0
    if args.cmd == "status":
        p = rm.pointer()
        data = {"env": env, "active_release_id": p["release_id"] if p else None, "activation_seq": p["activation_seq"] if p else 0,
                "schema_version": Migrator(store, MIGRATIONS).current_version()}  # fmt: skip
        _emit(args, f"env={env} active_release={data['active_release_id']} seq={data['activation_seq']} schema={data['schema_version']}",
              data, out)  # fmt: skip
        return 0
    if args.cmd == "snapshot":
        snap = SnapshotBuilder(StoreConfigSource(store, env)).build(
            SnapshotScope(tuple(args.project), tuple(args.objects), args.execution))
        ref = SnapshotStore(store).persist(snap, args.actor)
        _emit(args, f"snapshot {ref.snapshot_id} release={ref.release_id}", {"snapshot_id": ref.snapshot_id, "release_id": ref.release_id}, out)
        return 0
    if args.cmd in ("ingest", "batch-status", "abandon-batch", "reprocess"):
        return _ingestion_commands(args, cfg, store, env, engine_factory, out)
    from de_accelerator.cli import transform as _transform

    if args.cmd in _transform.COMMANDS:
        return _transform.dispatch(args, cfg, store, env, out, _emit)
    if args.cmd == "diff":
        changes = ChangeDiffer(store).diff(None if args.frm == "none" else args.frm, args.to)
        rows = [{"table": c.table, "key": c.key, "change": c.change_type} for c in changes]
        _emit(args, "\n".join(f"{c['change']:<8} {c['table']} {c['key']}" for c in rows) or "(no changes)", rows, out)
        return 0
    raise ConfigError(f"unknown command {args.cmd}")


def _ingestion_commands(args: argparse.Namespace, cfg: BootstrapConfig, store: TableStore, env: str, engine_factory: EngineFactory,
                        out: Callable[[str], None]) -> int:
    from de_accelerator.ingestion.runner import IngestionRunner

    engine = engine_factory(cfg, store)
    if args.cmd == "ingest":
        objs = tuple(o for o in (args.objects or "").split(",") if o)
        s = IngestionRunner(engine, store, env).run(args.execution, objs, args.project, args.run_id, args.restart, "cli", args.concurrency)
        data = {"run_id": s.run_id, "status": s.status, "snapshot_id": s.snapshot.snapshot_id, "release_id": s.snapshot.release_id,
                "objects": [{"object": r.object_code, "status": r.status, "batch_seq": r.batch_seq, "rows_written": r.rows_written,
                             "attempts": r.attempts, "error": r.message if r.status == "FAILED" else None} for r in s.results]}  # fmt: skip
        lines = [f"run {s.run_id}: {s.status}  snapshot={s.snapshot.snapshot_id[:12]} release={s.snapshot.release_id[:8]}"]
        lines += [f"  {r.object_code:<28} {r.status:<8} batch={r.batch_seq} rows={r.rows_written} attempts={r.attempts}"
                  + (f"  [{r.error_class}] {r.message}" if r.status == "FAILED" else "") for r in s.results]  # fmt: skip
        _emit(args, "\n".join(lines), data, out)
        return 1 if s.status in ("FAILED", "PARTIAL") else 0
    if args.cmd == "batch-status":
        rows = []
        for m in engine.manifests.list(args.object, args.stream)[-args.limit:]:
            rc = engine.receipts.get(m.batch_id)
            rows.append({"seq": m.batch_seq, "state": engine.events.state(m.batch_id), "wm_from": m.wm_from, "wm_to": m.wm_to,
                         "rows": rc["rows_written"] if rc else None, "type": m.batch_type, "batch_id": m.batch_id})  # fmt: skip
        wm = engine.watermarks.get(args.object, args.stream)
        head = f"{args.object}/{args.stream}: cursor={wm.committed_value if wm else None} seq={wm.committed_seq if wm else 0}"
        _emit(args, head + "\n" + "\n".join(f"  #{r['seq']:<4} {r['state']:<18} {r['wm_from']} -> {r['wm_to']} rows={r['rows']}" for r in rows),
              {"cursor": wm.committed_value if wm else None, "batches": rows}, out)  # fmt: skip
        return 0
    if args.cmd == "abandon-batch":
        bid = engine.abandon_batch(args.object, args.stream, args.reason, args.actor)
        out(f"abandoned batch {bid}")
        return 0
    # reprocess
    from de_accelerator.audit.models import batch_id_for  # noqa: F401

    ref, reader = IngestionRunner(engine, store, env).snapshot_for(None, (args.project,), (args.object,))
    n = 1
    while engine.manifests.list(args.object, f"reprocess-{n}"):
        n += 1
    res = engine.ingest_object(reader, ref, args.project, args.object, args.run_id if hasattr(args, "run_id") and args.run_id else f"reprocess-{args.actor}",
                               stream_key=f"reprocess-{n}", batch_type="REPROCESS", window_override=(args.frm, args.to))  # fmt: skip
    out(f"reprocess {args.object} [{args.frm} .. {args.to}] ({args.reason}): {res.status} batch={res.batch_seq} rows={res.rows_written}")
    return 1 if res.status == "FAILED" else 0


def main() -> None:  # pragma: no cover - console entry point
    sys.exit(run())
