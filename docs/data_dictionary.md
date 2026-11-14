# Data dictionary

## Source: orders batches (`*.parquet`) — contract
| column | logical type | required | notes |
|---|---|---|---|
| order_id | string | yes | business key, `O########`; trimmed |
| customer_id | string | yes | must exist in `dim_customers`; trimmed |
| order_ts | timestamp (UTC) | yes | creation time |
| amount | float | yes | ≥ 0 and finite; rounded to 2 dp; int source widened |
| status | string | yes | one of created, paid, shipped, cancelled, refunded (lower-cased) |
| updated_at | timestamp (UTC) | yes | version stamp; must be ≥ `order_ts`; drives upsert/late-data handling |
| currency | string | no | upper-cased; NULL if absent |
| *(any other snake_case column)* | supported type | no | auto-added as nullable (`ADD_COLUMN`) |

Reference: `customers.parquet` → `customer_id` (string, unique), `country`.

## Target tables (SQLite `warehouse.db`)
| table | grain / key | purpose |
|---|---|---|
| orders | PK `order_id` | current version of each order. Source columns + `order_date` (UTC date), `amount_cents` (int, derived) + lineage `run_id`, `source_file`, `loaded_at` + evolved columns |
| orders_history | PK (`order_id`, `updated_at`) | append-only version history incl. late/stale versions |
| dim_customers | PK `customer_id` | reference for referential integrity (FK enforced) |
| rejects | PK `reject_id` | rejected rows: file, 0-based row number, order_id, reason, detail, raw record JSON |
| schema_registry | PK `column_name` | current accepted columns, logical type, required flag, origin (`contract`/`evolved`), run that added it |
| schema_drift_log | PK `drift_id` | every drift finding (COMPATIBLE and BREAKING) per batch |
| batch_runs | PK (`run_id`, `source_file`) | per-batch outcome (LOADED/QUARANTINED/SKIPPED/FAILED), counters, note; also the idempotency ledger (by `batch_sha256`) |
| pipeline_runs | PK `run_id` | run status, start/end, totals, error |

Invariant per LOADED batch: `rows_in = rows_inserted + rows_updated + rows_unchanged + rows_stale + rows_rejected`.
