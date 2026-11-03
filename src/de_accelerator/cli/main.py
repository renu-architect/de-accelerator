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

    d = sub.add_parser("diff", help="row-level diff between two releases")
    d.add_argument("--from", dest="frm", default="none")
    d.add_argument("--to", required=True)
    return p


def _emit(args: argparse.Namespace, text: str, data: Any, out: Callable[[str], None]) -> None:
    out(json.dumps(data, indent=2, sort_keys=True, default=str) if args.json else text)


def _envs(env: str) -> list[str]:
    return list(ENVIRONMENTS) if env == "all" else [env]


def run(argv: Sequence[str] | None = None, store_factory: StoreFactory | None = None, out: Callable[[str], None] = print) -> int:
    args = _parser().parse_args(argv)
    try:
        return _dispatch(args, store_factory or default_store_factory, out)
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


def _dispatch(args: argparse.Namespace, factory: StoreFactory, out: Callable[[str], None]) -> int:
    if args.cmd == "validate-config":
        ok, results = True, []
        for env in _envs(args.env):
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
        for env in _envs(args.env):
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
    if args.cmd == "diff":
        changes = ChangeDiffer(store).diff(None if args.frm == "none" else args.frm, args.to)
        rows = [{"table": c.table, "key": c.key, "change": c.change_type} for c in changes]
        _emit(args, "\n".join(f"{c['change']:<8} {c['table']} {c['key']}" for c in rows) or "(no changes)", rows, out)
        return 0
    raise ConfigError(f"unknown command {args.cmd}")


def main() -> None:  # pragma: no cover - console entry point
    sys.exit(run())
