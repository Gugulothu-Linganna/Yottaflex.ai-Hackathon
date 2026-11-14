"""CLI: python -m schema_drift {generate,run,reconcile,benchmark}"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from . import store
from .config import PipelineConfig
from .generate import build_dataset, write_dataset
from .pipeline import run_pipeline
from .reconcile import format_report, reconcile


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="schema_drift")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="write synthetic Parquet batches + separate expected labels")
    g.add_argument("--rows", type=int, default=200, help="baseline orders (>=30); use 100000 for a large run")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--out", default="data/sample")

    r = sub.add_parser("run", help="ingest all *.parquet batches from a directory")
    r.add_argument("--input-dir", default="data/sample/input")
    r.add_argument("--customers", default="data/sample/customers.parquet")
    r.add_argument("--db", default="output/warehouse.db")
    r.add_argument("--quarantine-dir", default="output/quarantine")
    r.add_argument("--report", default="output/run_report.json")
    r.add_argument("--reprocess-quarantined", action="store_true",
                   help="retry batches that were previously quarantined (e.g. after approving a new rule)")

    c = sub.add_parser("reconcile", help="run reconciliation checks on the warehouse")
    c.add_argument("--db", default="output/warehouse.db")
    c.add_argument("--report", default="output/reconciliation.md")

    b = sub.add_parser("benchmark", help="time the pipeline on a larger synthetic dataset")
    b.add_argument("--rows", type=int, default=100_000)
    b.add_argument("--parquet", action="store_true", help="include real Parquet I/O (needs pyarrow)")

    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if a.cmd == "generate":
        ds = build_dataset(a.rows, a.seed)
        write_dataset(ds, f"{a.out}/input", f"{a.out}/expected")
        print(f"wrote {len(ds.batches)} batches to {a.out}/input (labels in {a.out}/expected)")
        return 0
    if a.cmd == "run":
        cfg = PipelineConfig(a.input_dir, a.customers, a.db, a.quarantine_dir, a.report, a.reprocess_quarantined)
        summary = run_pipeline(cfg)
        print(json.dumps({k: summary[k] for k in ("run_id", "status", "totals")}, indent=2))
        return {"SUCCESS": 0, "SUCCESS_WITH_QUARANTINE": 2}.get(summary["status"], 1)
    if a.cmd == "reconcile":
        conn = store.connect(a.db)
        checks = reconcile(conn)
        text = format_report(checks)
        open(a.report, "w").write(text)
        print(text)
        return 0 if all(x["passed"] for x in checks) else 1
    if a.cmd == "benchmark":
        from .benchmark import run_benchmark
        print(json.dumps(run_benchmark(a.rows, a.parquet), indent=2))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
