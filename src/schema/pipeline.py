"""Orchestration: discover -> fingerprint -> drift check -> quarantine | transform+load (one txn per batch)."""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from . import store
from .config import ISO_FMT, PipelineConfig
from .drift import BREAKING, Finding, detect_drift, has_breaking
from .readers import BatchUnreadable, ParquetReader, basename
from .validate import conform_frame, validate_rows

log = logging.getLogger("schema_drift")
COUNTER_KEYS = ("rows_in", "rows_inserted", "rows_updated", "rows_unchanged", "rows_stale", "rows_rejected")


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime(ISO_FMT)


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:6]


def _quarantine(conn, reader, cfg, run_id, name, sha, findings: List[Finding], started) -> dict:
    src = basename(name)
    qdir = Path(cfg.quarantine_dir)
    qdir.mkdir(parents=True, exist_ok=True)
    dest = qdir / f"{sha[:12]}__{src}"
    reader.copy_to(name, str(dest))  # idempotent overwrite
    now = utcnow()
    dest.with_name(dest.name + ".findings.json").write_text(json.dumps(
        {"source_file": src, "batch_sha256": sha, "run_id": run_id, "quarantined_at": now,
         "findings": [f.to_dict() for f in findings]}, indent=2))
    note = "; ".join(f"{f.kind}({f.column})" for f in findings if f.severity == BREAKING)
    conn.execute("BEGIN")
    try:
        store.log_drift(conn, findings, run_id, src, sha, now)
        store.record_batch(conn, run_id, src, sha, "QUARANTINED", {}, note, started, now)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    log.warning("[%s] QUARANTINED %s: %s", run_id, src, note)
    return {"source_file": src, "outcome": "QUARANTINED", "batch_sha256": sha, "note": note,
            "findings": [f.to_dict() for f in findings], **{k: 0 for k in COUNTER_KEYS}}


def process_batch(conn, reader, cfg: PipelineConfig, run_id: str, name: str) -> dict:
    src, started = basename(name), utcnow()
    t0 = time.perf_counter()
    sha = reader.fingerprint(name)

    prior = store.prior_outcome(conn, sha)
    if prior == "LOADED" or (prior == "QUARANTINED" and not cfg.reprocess_quarantined):
        note = f"already {prior.lower()} (same content hash)"
        store.record_batch(conn, run_id, src, sha, "SKIPPED", {}, note, started, utcnow())
        log.info("[%s] SKIPPED %s: %s", run_id, src, note)
        return {"source_file": src, "outcome": "SKIPPED", "batch_sha256": sha, "note": note, "findings": [],
                **{k: 0 for k in COUNTER_KEYS}}

    try:
        schema = reader.schema(name)
    except BatchUnreadable as exc:
        return _quarantine(conn, reader, cfg, run_id, name, sha,
                           [Finding("UNREADABLE_FILE", "*", BREAKING, str(exc))], started)

    registry = store.load_registry(conn)
    findings = detect_drift(schema, {c: t for c, (t, _) in registry.items()},
                            {c for c, (_, r) in registry.items() if r})
    if has_breaking(findings):
        return _quarantine(conn, reader, cfg, run_id, name, sha, findings, started)

    try:
        df = reader.frame(name)
    except BatchUnreadable as exc:
        return _quarantine(conn, reader, cfg, run_id, name, sha,
                           [Finding("UNREADABLE_FILE", "*", BREAKING, str(exc))], started)
    t_read = time.perf_counter()

    now = utcnow()
    conn.execute("BEGIN")
    try:
        store.apply_additions(conn, findings, run_id, now)
        reg = store.load_registry(conn)
        source_cols = {c: t for c, (t, _) in reg.items()}
        conformed = conform_frame(df, source_cols)
        valid, rejects = validate_rows(conformed, store.customer_ids(conn))
        t_val = time.perf_counter()
        counts = store.upsert_orders(conn, valid, source_cols, run_id, src, now)
        counts["rows_rejected"] = store.insert_rejects(conn, rejects, run_id, src, sha, now)
        counts["rows_in"] = len(df)
        if counts["rows_in"] != sum(counts[k] for k in COUNTER_KEYS[1:]):  # batch-level reconciliation
            raise RuntimeError(f"row accounting mismatch for {src}: {counts}")
        if cfg.fault_hook:
            cfg.fault_hook("after_upsert", src)
        store.log_drift(conn, findings, run_id, src, sha, now)
        note = "empty batch" if len(df) == 0 else None
        store.record_batch(conn, run_id, src, sha, "LOADED", counts, note, started, utcnow())
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    t_end = time.perf_counter()
    log.info("[%s] LOADED %s in=%d ins=%d upd=%d unch=%d stale=%d rej=%d", run_id, src, counts["rows_in"],
             counts["rows_inserted"], counts["rows_updated"], counts["rows_unchanged"], counts["rows_stale"],
             counts["rows_rejected"])
    return {"source_file": src, "outcome": "LOADED", "batch_sha256": sha, "note": note,
            "findings": [f.to_dict() for f in findings], **counts,
            "timings_s": {"read": round(t_read - t0, 4), "validate": round(t_val - t_read, 4),
                          "load": round(t_end - t_val, 4)}}


def run_pipeline(cfg: PipelineConfig, reader=None, run_id: Optional[str] = None) -> Dict:
    reader = reader or ParquetReader()
    run_id = run_id or new_run_id()
    conn = store.connect(cfg.db_path)
    store.init_db(conn)
    started = utcnow()
    store.start_run(conn, run_id, started)
    totals = {k: 0 for k in COUNTER_KEYS}
    totals.update(batches_seen=0, batches_loaded=0, batches_quarantined=0, batches_skipped=0, batches_failed=0)
    batches: List[dict] = []
    error, status = None, "SUCCESS"
    log.info("[%s] run started", run_id)
    try:
        ref = reader.frame(cfg.customers_path)
        ids = ref["customer_id"].astype("string").str.strip().dropna().unique().tolist()
        store.load_customers(conn, ids)
        for name in reader.list_batches(cfg.input_dir):
            totals["batches_seen"] += 1
            try:
                res = process_batch(conn, reader, cfg, run_id, name)
            except Exception as exc:  # isolated failure: txn already rolled back; later batches still run
                msg = f"{type(exc).__name__}: {exc}"
                log.error("[%s] FAILED %s: %s", run_id, basename(name), msg)
                res = {"source_file": basename(name), "outcome": "FAILED", "note": msg, "findings": [],
                       **{k: 0 for k in COUNTER_KEYS}}
                try:
                    store.record_batch(conn, run_id, basename(name), reader.fingerprint(name), "FAILED", {}, msg,
                                       started, utcnow())
                except Exception:
                    pass
            batches.append(res)
            key = {"LOADED": "batches_loaded", "QUARANTINED": "batches_quarantined",
                   "SKIPPED": "batches_skipped", "FAILED": "batches_failed"}[res["outcome"]]
            totals[key] += 1
            for k in COUNTER_KEYS:
                totals[k] += res.get(k, 0)
        if totals["batches_failed"]:
            status = "PARTIAL_FAILURE"
        elif totals["batches_quarantined"]:
            status = "SUCCESS_WITH_QUARANTINE"
    except Exception as exc:
        status, error = "FAILED", f"{type(exc).__name__}: {exc}"
        log.exception("[%s] run failed", run_id)
    finished = utcnow()
    store.finish_run(conn, run_id, finished, status, totals, error)
    conn.close()
    summary = {"run_id": run_id, "status": status, "started_at": started, "finished_at": finished,
               "error": error, "totals": totals, "batches": batches}
    if cfg.report_path:
        Path(cfg.report_path).parent.mkdir(parents=True, exist_ok=True)
        Path(cfg.report_path).write_text(json.dumps(summary, indent=2, default=str))
    log.info("[%s] run finished status=%s totals=%s", run_id, status, totals)
    return summary
