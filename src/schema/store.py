"""SQLite warehouse: DDL, schema registry, idempotent upsert, rejects and run bookkeeping."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from .config import ISO_FMT, LINEAGE_COLUMNS, OPTIONAL_COLUMNS, REQUIRED_COLUMNS, SNAKE_CASE, SQL_TYPES
from .drift import Finding
from .validate import to_db_rows

DDL = [
    """CREATE TABLE IF NOT EXISTS dim_customers(customer_id TEXT PRIMARY KEY)""",
    """CREATE TABLE IF NOT EXISTS schema_registry(
        column_name TEXT PRIMARY KEY, logical_type TEXT NOT NULL, is_required INTEGER NOT NULL,
        origin TEXT NOT NULL, added_run_id TEXT, added_at TEXT)""",
    """CREATE TABLE IF NOT EXISTS orders(
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL REFERENCES dim_customers(customer_id),
        order_ts TEXT NOT NULL, amount REAL NOT NULL CHECK(amount >= 0), status TEXT NOT NULL,
        updated_at TEXT NOT NULL, currency TEXT,
        order_date TEXT NOT NULL, amount_cents INTEGER NOT NULL,
        run_id TEXT NOT NULL, source_file TEXT NOT NULL, loaded_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS orders_history(
        order_id TEXT NOT NULL, updated_at TEXT NOT NULL, status TEXT NOT NULL, amount REAL NOT NULL,
        source_file TEXT NOT NULL, run_id TEXT NOT NULL, PRIMARY KEY(order_id, updated_at))""",
    """CREATE TABLE IF NOT EXISTS rejects(
        reject_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, source_file TEXT NOT NULL, batch_sha256 TEXT NOT NULL,
        source_row_number INTEGER NOT NULL, order_id TEXT, reason_code TEXT NOT NULL, reason_detail TEXT,
        record_json TEXT NOT NULL, rejected_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS schema_drift_log(
        drift_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, source_file TEXT NOT NULL, batch_sha256 TEXT NOT NULL,
        kind TEXT NOT NULL, column_name TEXT NOT NULL, severity TEXT NOT NULL, detail TEXT, detected_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS batch_runs(
        run_id TEXT NOT NULL, source_file TEXT NOT NULL, batch_sha256 TEXT NOT NULL, outcome TEXT NOT NULL,
        rows_in INTEGER NOT NULL DEFAULT 0, rows_inserted INTEGER NOT NULL DEFAULT 0,
        rows_updated INTEGER NOT NULL DEFAULT 0, rows_unchanged INTEGER NOT NULL DEFAULT 0,
        rows_stale INTEGER NOT NULL DEFAULT 0, rows_rejected INTEGER NOT NULL DEFAULT 0,
        note TEXT, started_at TEXT NOT NULL, finished_at TEXT NOT NULL, PRIMARY KEY(run_id, source_file))""",
    """CREATE TABLE IF NOT EXISTS pipeline_runs(
        run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL,
        batches_seen INTEGER DEFAULT 0, batches_loaded INTEGER DEFAULT 0, batches_quarantined INTEGER DEFAULT 0,
        batches_skipped INTEGER DEFAULT 0, batches_failed INTEGER DEFAULT 0, rows_in INTEGER DEFAULT 0,
        rows_inserted INTEGER DEFAULT 0, rows_updated INTEGER DEFAULT 0, rows_unchanged INTEGER DEFAULT 0,
        rows_stale INTEGER DEFAULT 0, rows_rejected INTEGER DEFAULT 0, error TEXT)""",
    "CREATE INDEX IF NOT EXISTS ix_batch_runs_sha ON batch_runs(batch_sha256, outcome)",
]


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None)  # explicit BEGIN/COMMIT
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    for stmt in DDL:
        conn.execute(stmt)
    if conn.execute("SELECT COUNT(*) FROM schema_registry").fetchone()[0] == 0:
        conn.execute("BEGIN")
        for c, t in REQUIRED_COLUMNS.items():
            conn.execute("INSERT INTO schema_registry VALUES(?,?,?,?,NULL,NULL)", (c, t, 1, "contract"))
        for c, t in OPTIONAL_COLUMNS.items():
            conn.execute("INSERT INTO schema_registry VALUES(?,?,?,?,NULL,NULL)", (c, t, 0, "contract"))
        conn.execute("COMMIT")


def load_registry(conn) -> Dict[str, Tuple[str, int]]:
    rows = conn.execute("SELECT column_name, logical_type, is_required FROM schema_registry ORDER BY rowid")
    return {c: (t, r) for c, t, r in rows}


def apply_additions(conn, findings: List[Finding], run_id: str, now: str) -> None:
    """Apply approved ADD_COLUMN findings: ALTER TABLE + registry row (inside caller's transaction)."""
    for f in findings:
        if f.kind != "ADD_COLUMN":
            continue
        if not SNAKE_CASE.match(f.column):  # defence in depth before interpolating an identifier
            raise ValueError(f"refusing to add column {f.column!r}")
        conn.execute(f'ALTER TABLE orders ADD COLUMN "{f.column}" {SQL_TYPES[f.logical_type]}')
        conn.execute("INSERT INTO schema_registry VALUES(?,?,0,'evolved',?,?)", (f.column, f.logical_type, run_id, now))


def load_customers(conn, ids: Iterable[str]) -> int:
    conn.execute("BEGIN")
    conn.executemany("INSERT OR IGNORE INTO dim_customers VALUES(?)", [(i,) for i in ids])
    conn.execute("COMMIT")
    return conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]


def customer_ids(conn) -> set:
    return {r[0] for r in conn.execute("SELECT customer_id FROM dim_customers")}


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def upsert_orders(conn, valid: pd.DataFrame, source_cols: Dict[str, str], run_id: str, source_file: str, now: str) -> Dict[str, int]:
    """Idempotent, order-independent upsert: a row only replaces the stored one if updated_at is newer."""
    counts = {"rows_inserted": 0, "rows_updated": 0, "rows_unchanged": 0, "rows_stale": 0}
    if valid.empty:
        return counts
    cols, rows = to_db_rows(valid, source_cols, {"run_id": run_id, "source_file": source_file, "loaded_at": now})
    ix = {c: i for i, c in enumerate(cols)}
    ids = [r[ix["order_id"]] for r in rows]
    existing: Dict[str, str] = {}
    for chunk in _chunks(ids, 500):
        q = ",".join("?" * len(chunk))
        existing.update(conn.execute(f"SELECT order_id, updated_at FROM orders WHERE order_id IN ({q})", chunk).fetchall())
    to_write = []
    for r in rows:
        old = existing.get(r[ix["order_id"]])
        new = r[ix["updated_at"]]
        if old is None:
            counts["rows_inserted"] += 1
            to_write.append(r)
        elif new > old:
            counts["rows_updated"] += 1
            to_write.append(r)
        elif new == old:
            counts["rows_unchanged"] += 1
        else:
            counts["rows_stale"] += 1  # late-arriving older version: history only
    qcols = ",".join(f'"{c}"' for c in cols)
    upd = ",".join(f'"{c}"=excluded."{c}"' for c in cols if c != "order_id")
    conn.executemany(
        f"INSERT INTO orders({qcols}) VALUES({','.join('?' * len(cols))}) ON CONFLICT(order_id) DO UPDATE SET {upd}",
        to_write)
    conn.executemany(
        "INSERT OR IGNORE INTO orders_history(order_id, updated_at, status, amount, source_file, run_id) VALUES(?,?,?,?,?,?)",
        [(r[ix["order_id"]], r[ix["updated_at"]], r[ix["status"]], r[ix["amount"]], source_file, run_id) for r in rows])
    return counts


def insert_rejects(conn, rejects: pd.DataFrame, run_id: str, source_file: str, sha: str, now: str) -> int:
    if rejects.empty:
        return 0
    rows = []
    for r in rejects.itertuples(index=False):
        rid = hashlib.sha1(f"{sha}|{r.source_row_number}|{r.reason_code}".encode()).hexdigest()[:16]
        rows.append((rid, run_id, source_file, sha, int(r.source_row_number), r.order_id, r.reason_code,
                     r.reason_detail, r.record_json, now))
    conn.executemany("INSERT OR IGNORE INTO rejects VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
    return len(rows)


def log_drift(conn, findings: List[Finding], run_id: str, source_file: str, sha: str, now: str) -> None:
    rows = [(hashlib.sha1(f"{run_id}|{sha}|{f.kind}|{f.column}".encode()).hexdigest()[:16], run_id, source_file, sha,
             f.kind, f.column, f.severity, f.detail, now) for f in findings]
    conn.executemany("INSERT OR IGNORE INTO schema_drift_log VALUES(?,?,?,?,?,?,?,?,?)", rows)


def record_batch(conn, run_id, source_file, sha, outcome, counts: Dict[str, int], note, started, finished) -> None:
    c = {k: counts.get(k, 0) for k in
         ("rows_in", "rows_inserted", "rows_updated", "rows_unchanged", "rows_stale", "rows_rejected")}
    conn.execute(
        "INSERT OR REPLACE INTO batch_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (run_id, source_file, sha, outcome, c["rows_in"], c["rows_inserted"], c["rows_updated"], c["rows_unchanged"],
         c["rows_stale"], c["rows_rejected"], note, started, finished))


def prior_outcome(conn, sha: str):
    """'LOADED' / 'QUARANTINED' if this exact batch content was already handled, else None."""
    rows = {r[0] for r in conn.execute(
        "SELECT outcome FROM batch_runs WHERE batch_sha256=? AND outcome IN ('LOADED','QUARANTINED')", (sha,))}
    return "LOADED" if "LOADED" in rows else ("QUARANTINED" if rows else None)


def start_run(conn, run_id: str, now: str) -> None:
    conn.execute("INSERT INTO pipeline_runs(run_id, started_at, status) VALUES(?,?,'RUNNING')", (run_id, now))


def finish_run(conn, run_id: str, now: str, status: str, totals: Dict[str, int], error) -> None:
    conn.execute(
        """UPDATE pipeline_runs SET finished_at=?, status=?, batches_seen=?, batches_loaded=?, batches_quarantined=?,
           batches_skipped=?, batches_failed=?, rows_in=?, rows_inserted=?, rows_updated=?, rows_unchanged=?,
           rows_stale=?, rows_rejected=?, error=? WHERE run_id=?""",
        (now, status, totals["batches_seen"], totals["batches_loaded"], totals["batches_quarantined"],
         totals["batches_skipped"], totals["batches_failed"], totals["rows_in"], totals["rows_inserted"],
         totals["rows_updated"], totals["rows_unchanged"], totals["rows_stale"], totals["rows_rejected"], error, run_id))
