# Parquet Schema Drift Pipeline

A local, batch data pipeline. It reads a series of Parquet files whose schema changes over time and does four things:

* **detects schema drift** in each file against the current target schema,
* **applies approved evolution rules**: new nullable columns, lossless int→float widening, and missing optional columns,
* **quarantines breaking changes**: whole files for schema or format problems, single rows for data-quality problems,
* **publishes clean data** into an `orders` current-state table and an append-only `orders_history` table. Every step is idempotent and logged.

It is a complete vertical slice. It needs only Python, `pandas` and `pyarrow` (the target is SQLite from the standard library), runs in seconds on a laptop, and has 45 automated tests (44 run without `pyarrow`; 1 real-Parquet test auto-skips if `pyarrow` is absent).

```
input/*.parquet ──► fingerprint ──► read schema ──► detect drift ──► validate rows ──► publish ──► orders / orders_history
                        │               │                │                 │
                        ▼               ▼                ▼                 ▼
                 SKIPPED (same     UNREADABLE_FILE   BREAKING finding   rejects (per row: reason + raw record)
                 content hash)          └───────────────┴──► quarantine/ (whole file + .findings.json)
```

## Contents
1. [Quick start](#1-quick-start)
2. [Problem, scope and assumptions](#2-problem-scope-and-assumptions)
3. [Architecture](#3-architecture)
4. [Schema drift and evolution rules](#4-schema-drift-and-evolution-rules)
5. [Data quality rules and quarantine](#5-data-quality-rules-and-quarantine)
6. [Idempotency, retry, restart and backfill](#6-idempotency-retry-restart-and-backfill)
7. [Observability](#7-observability)
8. [Sample data and expected results](#8-sample-data-and-expected-results)
9. [Testing and verification](#9-testing-and-verification)
10. [Performance](#10-performance)
11. [Naming conventions](#11-naming-conventions)
12. [Project structure](#12-project-structure)
13. [Known limitations and next steps](#13-known-limitations-and-next-steps)

More detail: [data dictionary](docs/data_dictionary.md) · [design and trade-off notes](docs/DESIGN.md) · [sample queries](docs/sample_queries.sql) · [sample outputs and test evidence](sample_output/)

---

## 1. Quick start
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=src

python -m schema_drift generate --rows 200 --out data/sample          # seeded sample (use --rows 100000 for a large run)
python -m schema_drift run --input-dir data/sample/input --customers data/sample/customers.parquet
python -m schema_drift reconcile                                      # writes output/reconciliation.md
python -m schema_drift run --input-dir data/sample/input --customers data/sample/customers.parquet   # rerun: all SKIPPED
python -m schema_drift benchmark --rows 100000
python -m unittest discover -s tests -v                               # or: pytest
```
`run` exit codes: `0` success · `2` success but at least one batch quarantined (alertable) · `1` failure or partial failure.
Outputs: `output/warehouse.db`, `output/run_report.json`, `output/quarantine/`, `output/reconciliation.md`.

## 2. Problem, scope and assumptions
**Problem.** A producer drops Parquet batches of `orders` into a folder. Over time columns get added, dropped, retyped or renamed, and some files are corrupt. The pipeline must keep the analytical target correct and explain everything it did not load.

**Scope.** Local batch run; one source entity (`orders`) plus a `customers` reference; SQLite target; no external services or credentials.

**Assumptions.**
* `orders` grain is one *current* row per `order_id`; each input row is a complete version of that order, stamped by `updated_at`.
* `customers.parquet` is the authoritative reference for `customer_id`.
* File content (SHA-256) identifies a batch; file names are not trusted.
* Timestamps are UTC. Data is synthetic and generated from a fixed seed.

## 3. Architecture
Per file, in sorted-name order:
1. **Fingerprint** the bytes; if this hash was already `LOADED` or `QUARANTINED`, record `SKIPPED`.
2. **Read the Parquet footer only** and classify drift against `schema_registry`. Unreadable files and breaking drift are quarantined without reading row data.
3. **One transaction per batch**: apply approved `ADD_COLUMN` (`ALTER TABLE` + registry row) → conform and cast → validate rows → upsert `orders` (newer `updated_at` wins) and append `orders_history` → write rejects → assert row accounting → record `LOADED` → `COMMIT`. Any exception rolls everything back, including the schema change.

Parquet I/O is isolated behind a 5-method reader interface (`readers.py`), so the core logic is testable without `pyarrow`.

## 4. Schema drift and evolution rules
| Change in batch | Class | Action |
|---|---|---|
| new snake_case column, supported type | COMPATIBLE `ADD_COLUMN` | `ALTER TABLE` + registry; nullable |
| optional / previously added column absent | COMPATIBLE `MISSING_OPTIONAL_COLUMN` | NULL-filled |
| int source for float target | COMPATIBLE `WIDEN_INT_TO_FLOAT` | cast |
| int32↔int64, float32↔float64, column reorder | silent | cast / ignored |
| required column absent (this includes renames of required columns) | BREAKING `MISSING_REQUIRED_COLUMN` | quarantine file |
| type mismatch, float→int narrowing, string for timestamp | BREAKING `TYPE_MISMATCH` / `NARROWING_FLOAT_TO_INT` | quarantine file |
| new column not snake_case / reserved name / unsupported type | BREAKING | quarantine file |
| corrupt or non-Parquet file | BREAKING `UNREADABLE_FILE` | quarantine file |

Every finding (compatible or breaking) is logged to `schema_drift_log`; the evolved schema lives in `schema_registry`.

## 5. Data quality rules and quarantine
**File level.** Breaking drift or an unreadable file → copied to `quarantine/<sha12>__<name>` with a `.findings.json` sidecar; nothing from it reaches the target.

**Row level** (precedence order; each rejected row keeps one reason and its raw record in `rejects`):
`MISSING_REQUIRED_VALUE` → `INVALID_AMOUNT` (non-finite or < 0) → `INVALID_STATUS` → `UNKNOWN_CUSTOMER` (invalid reference key) → `UPDATED_BEFORE_CREATED` → `DUPLICATE_IN_BATCH` (newest `updated_at` wins; tie → later row).

**Deterministic transforms:** trim strings, blank→NULL, `status` lower-case, `currency` upper-case, `amount` rounded to 2 dp, derived `order_date` and `amount_cents`.

## 6. Idempotency, retry, restart and backfill
* **Rerun, same input:** hash ledger in `batch_runs` → `SKIPPED`; the database is unchanged (tested).
* **Same rows, different bytes:** upsert compares `updated_at`: insert / update if newer / `unchanged` if equal / `stale` if older. No duplicates.
* **Partial failure:** a failing batch rolls back and is recorded `FAILED`; other batches continue; run status `PARTIAL_FAILURE`. **Retry is simply a rerun.**
* **Late / out-of-order data and backfill:** the final state depends on `updated_at`, not file order (tested by reversing load order). Late older versions go to history only.
* **After approving a new rule:** `run --reprocess-quarantined` re-evaluates previously quarantined files.
* **Crash:** the run's `pipeline_runs` row stays `RUNNING`; the next run is safe regardless.

## 7. Observability
* Log lines carry the `run_id`.
* `pipeline_runs`: status, start/end, totals, error. `batch_runs`: per-batch outcome and counters (`rows_in`, inserted, updated, unchanged, stale, rejected), note or failure reason.
* `schema_drift_log`, `rejects` (file, row number, reason, raw JSON), `run_report.json`.
* **Reconciliation** (`reconcile.py`, 9 checks): per-batch row accounting, orders == inserted keys, rejects == rejected counts, no orphan customers, current row == latest history version, amount aggregate vs history, derived-field consistency, no quarantined data in target, registry == table columns.

## 8. Sample data and expected results
`generate` writes 9 seeded batches (baseline with boundary values and labelled invalid rows, compatible add-column, byte-identical replay, missing optional column, rename, type change, empty file, malformed file, bad column name). Expected-answer labels are written **separately** to `expected/` and never appear in input files.

Sample run (200 orders, seed 42): 4 LOADED, 4 QUARANTINED, 1 SKIPPED; 239 orders inserted, 20 updated, 4 unchanged, 8 stale, 28 rejected; 9/9 reconciliation checks pass; rerun skips 9/9. See `sample_output/` (`run_report.json`, `reconciliation.md`, `sample_query_results.txt`, `expected/`).

## 9. Testing and verification
`python -m unittest discover -s tests -v` → **45 tests: 44 pass, 1 skipped** (`sample_output/test_run.txt`).
Coverage: drift rules (12), row validation (12), happy path / acceptance vs. labels, empty batch and empty input, malformed file, duplicates, nulls, invalid reference key, late data, rerun, differently encoded re-export, reverse load order, partial failure + retry (including rollback of `ALTER TABLE`), reprocess flag, reconciliation + tamper detection, generator determinism.

**Verification caveat:** the build environment had no `pyarrow`, so `test_parquet_roundtrip.py` (real Parquet files) was **not executed there**. All other tests run the same pipeline through `InMemoryReader`. Run the full suite once with `pyarrow` installed.

## 10. Performance
`python -m schema_drift benchmark --rows 100000` (seed 42, 148,003 input rows, **no Parquet I/O**): ~6.3 s wall (~23.6k rows/s). Stages: read/fingerprint 1.4 s (mostly the test double), validate 1.5 s, load 3.0 s. Rerun: 1.6 s, 9/9 skipped. Hardware-dependent.

**One reasonable optimisation (not implemented or measured):** replace per-row Python classification and chunked `IN (...)` lookups with a temp table of batch keys joined in SQL, and use `INSERT … ON CONFLICT DO UPDATE … WHERE excluded.updated_at > orders.updated_at`. Already applied: vectorised validation, batched `executemany`, footer-only schema reads.

## 11. Naming conventions
snake_case tables and columns · ISO-8601 UTC timestamps (`YYYY-MM-DDTHH:MM:SS.ffffffZ`, fixed width) · `order_id = O########`, `customer_id = C######` · `run_id = <UTC timestamp with microseconds>-<6 hex>` · deterministic `reject_id` / `drift_id` (SHA-1 prefix) so retries never duplicate them · lineage columns `run_id`, `source_file`, `loaded_at` on `orders`.

## 12. Project structure
```
src/schema_drift/  config.py drift.py validate.py store.py pipeline.py reconcile.py
                   generate.py readers.py benchmark.py cli.py
tests/             test_drift.py test_validate.py test_pipeline.py
                   test_reconcile_generator.py test_parquet_roundtrip.py helpers.py
docs/              data_dictionary.md DESIGN.md sample_queries.sql
sample_output/     run_report.json reconciliation.md sample_query_results.txt test_run.txt expected/ ...
```

## 13. Known limitations and next steps
* Same `(order_id, updated_at)` with **different** payloads is not flagged: first writer wins. Next: payload hash + `CONFLICTING_VERSION` reject.
* An absent optional column overwrites the stored value with NULL on update (each row is a full version).
* No approved-rename mapping: a renamed required column is a breaking change. Next: a config file of approved renames.
* No row-level quality threshold that quarantines a whole file when too many rows fail. Next: configurable max reject ratio.
* SQLite is single-writer; production would use a lakehouse table format with `MERGE` and an orchestrator.
* `ADD_COLUMN` is auto-approved by pattern; a real deployment may want an allow-list.
* Type rules are conservative (no string→timestamp parsing, no decimal/date types).
* The quarantine file copy is a filesystem side effect outside the DB transaction (idempotent overwrite).
