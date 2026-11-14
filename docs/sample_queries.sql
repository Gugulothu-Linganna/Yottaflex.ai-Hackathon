-- 1. Per-batch outcome and row accounting (every input row lands in exactly one bucket)
SELECT source_file, outcome, rows_in, rows_inserted, rows_updated, rows_unchanged, rows_stale, rows_rejected, note
FROM batch_runs WHERE run_id = (SELECT MIN(run_id) FROM pipeline_runs) ORDER BY source_file;

-- 2. Schema drift history (what changed, when, severity, and which batch)
SELECT source_file, severity, kind, column_name, detail FROM schema_drift_log ORDER BY source_file, severity, kind;

-- 3. Rejects by reason, traceable to file + row
SELECT reason_code, COUNT(*) AS n FROM rejects GROUP BY reason_code ORDER BY n DESC;
SELECT source_file, source_row_number, order_id, reason_code, reason_detail FROM rejects ORDER BY source_file, source_row_number LIMIT 5;

-- 4. Evolved target schema
SELECT column_name, logical_type, origin, added_run_id IS NOT NULL AS has_run_id FROM schema_registry;

-- 5. Revenue by status (current state only) and order-version history for one updated order
SELECT status, COUNT(*) AS orders, ROUND(SUM(amount),2) AS revenue FROM orders GROUP BY status ORDER BY revenue DESC;
SELECT h.order_id, h.updated_at, h.status, h.amount, h.source_file FROM orders_history h
WHERE h.order_id = (SELECT order_id FROM orders_history GROUP BY order_id HAVING COUNT(*) > 1 ORDER BY order_id LIMIT 1)
ORDER BY h.updated_at;
