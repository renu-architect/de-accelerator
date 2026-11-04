"""CLI commands of the transformation engine: transform, transform-status, transform-replan (registered from cli/main.py)."""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from typing import Any

from de_accelerator.audit.layer_cursor import LayerCursorRepo
from de_accelerator.audit.repos import EventLog, ManifestRepo, ReceiptRepo
from de_accelerator.core.bootstrap import BootstrapConfig
from de_accelerator.core.errors import ConfigError
from de_accelerator.metadata.store.base import TableStore
from de_accelerator.processing.common.unit import UnitAudit
from de_accelerator.processing.runner import TransformRunner
from de_accelerator.processing.silver.engine import TransformConfig

COMMANDS = ("transform", "transform-status", "transform-replan")
_STREAM = {"SILVER": "SILVER:live", "GOLD": "GOLD:main"}


def add_parsers(sub: Any) -> None:
    t = sub.add_parser("transform", help="run Bronze->Silver and/or Silver->Gold for an execution (or selected objects)")
    t.add_argument("--execution")
    t.add_argument("--objects", help="comma separated object codes")
    t.add_argument("--layer", choices=("SILVER", "GOLD"), help="restrict to one layer (default: both, dependency ordered)")
    t.add_argument("--project")
    t.add_argument("--run-id")
    t.add_argument("--concurrency", type=int)
    s = sub.add_parser("transform-status", help="show the cursor and recent units of an object/layer")
    s.add_argument("--object", required=True)
    s.add_argument("--layer", choices=("SILVER", "GOLD"), default="SILVER")
    s.add_argument("--limit", type=int, default=10)
    r = sub.add_parser(
        "transform-replan", help="abandon the open (uncommitted) unit of an object so it is re-planned; skips no data"
    )
    r.add_argument("--object", required=True)
    r.add_argument("--layer", choices=("SILVER", "GOLD"), required=True)
    r.add_argument("--reason", required=True)


def _config(cfg: BootstrapConfig) -> TransformConfig:
    return TransformConfig(env=cfg.environment, catalog=cfg.catalog, flavor=cfg.flavor, transform_root=cfg.transform_root)


def dispatch(
    args: argparse.Namespace,
    cfg: BootstrapConfig,
    store: TableStore,
    env: str,
    out: Callable[[str], None],
    emit: Callable[[argparse.Namespace, str, Any, Callable[[str], None]], None],
) -> int:
    spark = getattr(store, "spark", None)
    if spark is None:
        raise ConfigError("the transformation engine needs a Spark-backed store")
    if args.cmd == "transform":
        runner = TransformRunner(store, spark, _config(cfg), env=env)
        objs = tuple(o for o in (args.objects or "").split(",") if o)
        s = runner.run(args.execution, objs, args.layer, args.project, args.run_id, "cli", args.concurrency)
        data = {
            "run_id": s.run_id,
            "status": s.status,
            "snapshot_id": s.snapshot.snapshot_id,
            "release_id": s.snapshot.release_id,
            "units": [
                {
                    "object": r.object_code,
                    "layer": r.layer,
                    "status": r.status,
                    "batch_seq": r.batch_seq,
                    "rows_in": r.rows_in,
                    "rows_applied": r.rows_applied,
                    "rows_rejected": r.rows_rejected,
                    "attempts": r.attempts,
                    "error": r.message if r.status in ("FAILED", "SKIPPED_UPSTREAM_FAILED") else None,
                }
                for r in s.results
            ],
        }
        lines = [f"run {s.run_id}: {s.status}  snapshot={s.snapshot.snapshot_id[:12]} release={s.snapshot.release_id[:8]}"]
        lines += [
            f"  {r.layer:<6} {r.object_code:<28} {r.status:<24} unit={r.batch_seq} "
            f"in={r.rows_in} applied={r.rows_applied} rejected={r.rows_rejected}"
            + (f"  [{r.error_class}] {r.message}" if r.status in ("FAILED", "SKIPPED_UPSTREAM_FAILED") else "")
            for r in s.results
        ]
        emit(args, "\n".join(lines), data, out)
        return 1 if s.status in ("FAILED", "PARTIAL") else 0
    stream = _STREAM[args.layer]
    if args.cmd == "transform-status":
        manifests, events, receipts = ManifestRepo(store), EventLog(store), ReceiptRepo(store)
        cur = LayerCursorRepo(store).get(args.object, args.layer)
        rows = []
        for m in manifests.list(args.object, stream)[-args.limit :]:
            rc = receipts.get(m.batch_id, args.layer)
            rows.append(
                {
                    "seq": m.batch_seq,
                    "state": events.state(m.batch_id),
                    "wm_from": m.wm_from,
                    "wm_to": m.wm_to,
                    "rows_written": rc["rows_written"] if rc else None,
                    "rows_rejected": rc["rows_rejected"] if rc else None,
                    "batch_id": m.batch_id,
                }
            )
        head = f"{args.object}/{args.layer}: cursor applied_seq={cur.applied_seq if cur else 0}"
        text = (
            head
            + "\n"
            + "\n".join(
                f"  #{r['seq']:<4} {r['state']:<18} {r['wm_from']} -> {r['wm_to']} "
                f"written={r['rows_written']} rejected={r['rows_rejected']}"
                for r in rows
            )
        )
        emit(
            args,
            text,
            {"cursor": cur.applied_seq if cur else 0, "input_versions": cur.input_versions if cur else None, "units": rows},
            out,
        )
        return 0
    bid = UnitAudit(store, _clock()).replan(args.object, stream, args.layer, args.reason, args.actor)
    out(f"abandoned unit {bid} (no data skipped; the next run re-plans the same input)")
    return 0


def _clock() -> Any:
    from de_accelerator.core.clock import SystemClock

    return SystemClock()


__all__ = ["COMMANDS", "add_parsers", "dispatch", "json"]
