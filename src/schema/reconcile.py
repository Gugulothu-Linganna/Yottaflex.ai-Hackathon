"""Post-load reconciliation checks over the warehouse (read-only)."""
from __future__ import annotations

import json
from typing import Dict, List


def reconcile(conn) -> List[Dict]:
    q = lambda sql: conn.execute(sql).fetchone()[0]
    checks: List[Dict] = []

    def add(name, passed, detail):
        checks.append({"check": name, "passed": bool(passed), "detail": detail})

    bad = q("""SELECT COUNT(*) FROM batch_runs WHERE outcome='LOADED' AND rows_in !=
               rows_inserted+rows_updated+rows_unchanged+rows_stale+rows_rejected""")
    add("batch_row_accounting", bad == 0, f"{bad} LOADED batches where rows_in != sum of outcomes")

    n_orders, n_ins = q("SELECT COUNT(*) FROM orders"), q("SELECT COALESCE(SUM(rows_inserted),0) FROM batch_runs WHERE outcome='LOADED'")
    add("orders_equals_inserted_keys", n_orders == n_ins, f"orders={n_orders} sum(rows_inserted)={n_ins}")

    n_rej, s_rej = q("SELECT COUNT(*) FROM rejects"), q("SELECT COALESCE(SUM(rows_rejected),0) FROM batch_runs WHERE outcome='LOADED'")
    add("rejects_traceable_to_batches", n_rej == s_rej, f"rejects={n_rej} sum(rows_rejected)={s_rej}")

    orphan = q("SELECT COUNT(*) FROM orders o LEFT JOIN dim_customers c USING(customer_id) WHERE c.customer_id IS NULL")
    add("referential_integrity_customer", orphan == 0, f"{orphan} orders with unknown customer_id")

    stale_cur = q("""SELECT COUNT(*) FROM orders o WHERE o.updated_at !=
                     (SELECT MAX(updated_at) FROM orders_history h WHERE h.order_id=o.order_id)""")
    add("current_row_is_latest_version", stale_cur == 0, f"{stale_cur} orders not at their latest history version")

    amt_o = q("SELECT ROUND(COALESCE(SUM(amount),0),2) FROM orders")
    amt_h = q("""SELECT ROUND(COALESCE(SUM(h.amount),0),2) FROM orders o
                 JOIN orders_history h ON h.order_id=o.order_id AND h.updated_at=o.updated_at""")
    add("amount_aggregate_matches_history", amt_o == amt_h, f"orders={amt_o} history(latest)={amt_h}")

    cents = q("SELECT COUNT(*) FROM orders WHERE amount_cents != CAST(ROUND(amount*100) AS INTEGER)")
    add("derived_amount_cents_consistent", cents == 0, f"{cents} rows with inconsistent amount_cents")

    leak = q("""SELECT COUNT(*) FROM orders WHERE source_file IN (
                SELECT source_file FROM batch_runs q WHERE outcome='QUARANTINED' AND NOT EXISTS (
                  SELECT 1 FROM batch_runs l WHERE l.batch_sha256=q.batch_sha256 AND l.outcome='LOADED'))""")
    add("quarantined_batches_not_in_target", leak == 0, f"{leak} orders sourced from quarantined batches")

    reg = {r[0] for r in conn.execute("SELECT column_name FROM schema_registry")}
    cols = {r[1] for r in conn.execute("PRAGMA table_info(orders)")}
    add("registry_matches_orders_columns", reg <= cols, f"registry columns missing from orders: {sorted(reg - cols)}")
    return checks


def format_report(checks: List[Dict]) -> str:
    lines = ["# Reconciliation report", "", "| check | result | detail |", "|---|---|---|"]
    lines += [f"| {c['check']} | {'PASS' if c['passed'] else 'FAIL'} | {c['detail']} |" for c in checks]
    return "\n".join(lines) + "\n"
