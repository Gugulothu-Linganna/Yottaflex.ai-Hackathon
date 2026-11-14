"""Simple performance measurement: wall-clock and rows/sec per stage on a seeded dataset."""
from __future__ import annotations

import tempfile
import time
from pathlib import Path

from .config import PipelineConfig
from .generate import build_dataset, write_dataset
from .pipeline import run_pipeline
from .readers import InMemoryReader, ParquetReader


def run_benchmark(rows: int = 100_000, parquet: bool = False) -> dict:
    ds = build_dataset(rows, seed=42)
    with tempfile.TemporaryDirectory() as tmp:
        cfg = PipelineConfig(f"{tmp}/in", f"{tmp}/customers", f"{tmp}/wh.db", f"{tmp}/q")
        if parquet:
            write_dataset(ds, f"{tmp}/in", f"{tmp}/expected")
            cfg.customers_path = f"{tmp}/customers.parquet"
            reader = ParquetReader()
        else:
            reader = InMemoryReader(ds.batches, {"customers": ds.customers})
            cfg.customers_path = "customers"
        t = time.perf_counter()
        summary = run_pipeline(cfg, reader)
        wall = time.perf_counter() - t
        t = time.perf_counter()
        second = run_pipeline(cfg, reader)  # rerun: everything should be skipped
        rerun = time.perf_counter() - t
    loaded = [b for b in summary["batches"] if b["outcome"] == "LOADED"]
    stages = {k: round(sum(b["timings_s"][k] for b in loaded), 3) for k in ("read", "validate", "load")}
    rin = summary["totals"]["rows_in"]
    return {"rows_requested": rows, "rows_in_loaded_batches": rin, "io": "parquet" if parquet else "in-memory (no Parquet I/O)",
            "wall_seconds": round(wall, 2), "rows_per_second": int(rin / wall), "stage_seconds": stages,
            "rerun_wall_seconds": round(rerun, 2), "rerun_status": second["status"],
            "rerun_batches_skipped": second["totals"]["batches_skipped"], "totals": summary["totals"]}
