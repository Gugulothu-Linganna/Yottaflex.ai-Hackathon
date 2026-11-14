# Design & trade-off note
1. **SQLite target, Parquet in.** Zero-setup, transactional DDL (needed so a schema evolution and its data commit or roll back together),
   easy SQL evidence. Trade-off: single writer, not a lakehouse.
2. **Schema check before data read.** Only the Parquet footer is read to classify drift, so breaking batches are rejected cheaply and
   atomically as a *whole file* (a half-conforming batch would silently corrupt the target).
3. **Quarantine whole batch, not rows, for schema problems; reject rows for value problems.** Schema breaks are a producer contract failure
   (needs a human/rule change); value problems are expected noise and are traceable per row.
4. **Idempotency in two layers.** File-hash ledger (cheap skip) + semantic upsert on `(order_id, updated_at)` (safe when bytes differ).
5. **Versioned, order-independent merge.** Newest `updated_at` wins; history is append-only. Makes backfills and out-of-order delivery safe.
6. **Reader interface.** All Parquet I/O is behind 5 methods so logic is testable without pyarrow and swappable (S3, API) later.
7. **Conservative evolution.** Auto-approve only additions/widenings that cannot lose data; everything else is a human decision.
8. **Known weaknesses:** see README "Known limitations".
