"""Seeded synthetic dataset generator. Expected-answer labels are returned/written SEPARATELY from inputs.

Scenario files (sorted order == processing order):
  001 baseline                 clean rows + boundary values + labelled invalid rows + in-batch duplicates
  002 compatible_add_column    new column `coupon_code`, int32 `amount`, updates, stale (late) rows, unchanged rows
  002b replay_of_002           byte-identical copy of 002  -> skipped by batch fingerprint
  003 missing_optional         no `currency`, shuffled column order, float32 amount, padded ids
  004 breaking_rename          customer_id renamed cust_id -> quarantine
  005 breaking_type            amount delivered as string  -> quarantine
  006 empty                    zero rows, valid schema
  007 malformed                not a Parquet file          -> quarantine
  008 breaking_name            extra column "Discount%"    -> quarantine
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Union

import numpy as np
import pandas as pd

from .validate import (DUPLICATE_IN_BATCH, INVALID_AMOUNT, INVALID_STATUS, MISSING_REQUIRED_VALUE,
                       UNKNOWN_CUSTOMER, UPDATED_BEFORE_CREATED)

T0 = pd.Timestamp("2026-01-01", tz="UTC")
STATUSES = ["created", "paid", "shipped", "cancelled", "refunded"]
B001 = "orders_batch_001_baseline.parquet"
B002 = "orders_batch_002_compatible_add_column.parquet"
B002R = "orders_batch_002b_replay_of_002.parquet"
B003 = "orders_batch_003_missing_optional.parquet"
B004 = "orders_batch_004_breaking_rename.parquet"
B005 = "orders_batch_005_breaking_type.parquet"
B006 = "orders_batch_006_empty.parquet"
B007 = "orders_batch_007_malformed.parquet"
B008 = "orders_batch_008_breaking_name.parquet"


@dataclass
class Dataset:
    customers: pd.DataFrame
    batches: Dict[str, Union[pd.DataFrame, bytes]]
    labels: pd.DataFrame      # batch, source_row_number, expected   (NOT part of the input files)
    expected: Dict[str, dict]  # per-batch expected outcome summary


def _ts(rng, n, lo, hi):
    return pd.Series(T0 + pd.to_timedelta(rng.integers(lo, hi, n), unit="s")).dt.as_unit("us")


def _orders(rng, start, n, cust_ids) -> pd.DataFrame:
    order_ts = _ts(rng, n, 0, 90 * 86400)
    gap = pd.to_timedelta(rng.integers(3600, 2 * 86400, n), unit="s")
    return pd.DataFrame({
        "order_id": [f"O{i:08d}" for i in range(start, start + n)],
        "customer_id": rng.choice(cust_ids, n),
        "order_ts": order_ts,
        "amount": np.round(np.clip(rng.lognormal(3.5, 1.0, n), 0.01, 5000), 2),
        "status": rng.choice(STATUSES, n),
        "updated_at": (order_ts + gap).dt.as_unit("us"),
        "currency": rng.choice(["USD", "EUR", "INR"], n),
    })


def _summ(labels: pd.DataFrame, batch: str) -> dict:
    lab = labels[labels.batch == batch].expected
    ok = {"INSERT": "rows_inserted", "UPDATE": "rows_updated", "UNCHANGED": "rows_unchanged", "STALE": "rows_stale"}
    out = {"outcome": "LOADED", "rows_in": int(len(lab)), **{v: int((lab == k).sum()) for k, v in ok.items()}}
    rej = lab[~lab.isin(ok)]
    out["rows_rejected"] = int(len(rej))
    out["rejects_by_reason"] = {k: int(v) for k, v in rej.value_counts().items()}
    return out


def build_dataset(n_orders: int = 200, seed: int = 42) -> Dataset:
    if n_orders < 30:
        raise ValueError("n_orders must be >= 30")
    rng = np.random.default_rng(seed)
    n_cust = max(50, n_orders // 20)
    cust_ids = np.array([f"C{i:06d}" for i in range(1, n_cust + 1)])
    customers = pd.DataFrame({"customer_id": cust_ids, "country": rng.choice(["IN", "US", "DE", "GB"], n_cust)})
    labels = []

    def lab(batch, exp):
        labels.append(pd.DataFrame({"batch": batch, "source_row_number": np.arange(len(exp)), "expected": exp}))

    # ---- 001 baseline -------------------------------------------------------------------------
    b1 = _orders(rng, 1, n_orders, cust_ids)
    exp1 = np.array(["INSERT"] * n_orders, dtype=object)
    b1.loc[0, "amount"] = 0.0                        # boundary: zero is valid
    b1.loc[1, "amount"] = 0.01
    b1.loc[2, "amount"] = 5000.0
    b1.loc[3, "updated_at"] = b1.loc[3, "order_ts"]  # boundary: updated_at == order_ts is valid
    b1["status"] = b1["status"].astype(object)
    b1.loc[4, "status"] = " PAID "                   # normalised to 'paid', still valid
    k = max(1, n_orders // 50)
    pool = list(rng.permutation(np.arange(5, n_orders)))
    take = lambda: [pool.pop() for _ in range(k)]
    miss, neg, bad_status, unk, early, dups = take(), take(), take(), take(), take(), take()
    b1["customer_id"] = b1["customer_id"].astype(object)
    b1.loc[miss, "customer_id"] = None
    b1.loc[neg, "amount"] = -5.0
    b1.loc[bad_status, "status"] = "teleported"
    b1.loc[unk, "customer_id"] = "C999999"
    b1.loc[early, "updated_at"] = b1.loc[early, "order_ts"] - pd.Timedelta(hours=1)
    for idx, code in ((miss, MISSING_REQUIRED_VALUE), (neg, INVALID_AMOUNT), (bad_status, INVALID_STATUS),
                      (unk, UNKNOWN_CUSTOMER), (early, UPDATED_BEFORE_CREATED)):
        exp1[idx] = code
    dup_rows = b1.loc[dups].copy()
    dup_rows["updated_at"] = dup_rows["updated_at"] + pd.Timedelta(hours=1)  # newer duplicate event wins
    exp1[dups] = DUPLICATE_IN_BATCH                                           # original is superseded
    b1 = pd.concat([b1, dup_rows], ignore_index=True)
    exp1 = np.concatenate([exp1, np.array(["INSERT"] * k, dtype=object)])
    lab(B001, exp1)
    clean = list(pool)  # untouched baseline rows (current state == generated row)

    # ---- 002 compatible add column ------------------------------------------------------------
    n_new, n_upd, n_stale, n_same = max(5, n_orders // 5), max(3, n_orders // 10), max(2, n_orders // 25), max(2, n_orders // 50)
    pick = lambda m: [clean.pop() for _ in range(m)]
    upd_i, stale_i, same_i = pick(n_upd), pick(n_stale), pick(n_same)
    new = _orders(rng, n_orders + 1, n_new, cust_ids)
    upd = b1.loc[upd_i].copy()
    upd["updated_at"] = upd["updated_at"] + pd.Timedelta(days=1)
    upd["status"] = "shipped"
    upd["amount"] = np.round(upd["amount"] * 1.1, 2)
    stale = b1.loc[stale_i].copy()
    stale["updated_at"] = stale["updated_at"] - pd.Timedelta(minutes=30)
    b1.loc[same_i, "amount"] = b1.loc[same_i, "amount"].round()  # whole amounts survive the int32 cast in 002
    same = b1.loc[same_i].copy()
    badc = _orders(rng, n_orders + n_new + 1, 2, cust_ids)
    badc["customer_id"] = "C999999"
    nullid = _orders(rng, n_orders + n_new + 3, 1, cust_ids)
    nullid["order_id"] = None
    b2 = pd.concat([new, upd, stale, same, badc, nullid], ignore_index=True)
    exp2 = (["INSERT"] * n_new + ["UPDATE"] * n_upd + ["STALE"] * n_stale + ["UNCHANGED"] * n_same
            + [UNKNOWN_CUSTOMER] * 2 + [MISSING_REQUIRED_VALUE])
    b2["amount"] = b2["amount"].round().astype("int32")
    b2["coupon_code"] = pd.Series([None] * len(b2), dtype="object")
    b2.loc[: max(2, n_new // 2), "coupon_code"] = "WELCOME10"
    lab(B002, np.array(exp2, dtype=object))

    # ---- 003 missing optional column, reordered, float32 ----------------------------------------
    n3 = max(5, n_orders // 10)
    start3 = n_orders + n_new + 4
    b3 = _orders(rng, start3, n3, cust_ids).drop(columns=["currency"])
    b3["customer_id"] = b3["customer_id"].astype(object)
    b3.loc[0, "customer_id"] = " " + b3.loc[0, "customer_id"] + " "  # padded id: normalised, valid
    b3.loc[1, "amount"] = -1.0
    exp3 = ["INSERT"] * n3
    exp3[1] = INVALID_AMOUNT
    b3["amount"] = b3["amount"].astype("float32")
    b3 = b3[list(rng.permutation(b3.columns))]
    lab(B003, np.array(exp3, dtype=object))

    # ---- breaking / degenerate batches ------------------------------------------------------------
    b4 = _orders(rng, start3 + n3, 5, cust_ids).rename(columns={"customer_id": "cust_id"})
    b5 = _orders(rng, start3 + n3 + 5, 5, cust_ids)
    b5["amount"] = b5["amount"].map(lambda v: f"{v:.2f}")
    b6 = b3.iloc[0:0].copy()
    b8 = _orders(rng, start3 + n3 + 10, 5, cust_ids)
    b8["Discount%"] = 5.0
    batches = {B001: b1, B002: b2, B002R: b2.copy(), B003: b3, B004: b4, B005: b5, B006: b6,
               B007: b"PAR1 this is definitely not a parquet file", B008: b8}

    labels_df = pd.concat(labels, ignore_index=True)
    expected = {B001: _summ(labels_df, B001), B002: _summ(labels_df, B002), B003: _summ(labels_df, B003)}
    expected[B001]["drift_kinds"] = []
    expected[B002]["drift_kinds"] = ["ADD_COLUMN", "WIDEN_INT_TO_FLOAT"]
    expected[B003]["drift_kinds"] = ["MISSING_OPTIONAL_COLUMN"]
    expected[B002R] = {"outcome": "SKIPPED"}
    expected[B004] = {"outcome": "QUARANTINED", "drift_kinds": ["MISSING_REQUIRED_COLUMN", "ADD_COLUMN"]}
    expected[B005] = {"outcome": "QUARANTINED", "drift_kinds": ["TYPE_MISMATCH"]}
    expected[B006] = {"outcome": "LOADED", "rows_in": 0, "rows_inserted": 0, "rows_rejected": 0,
                      "drift_kinds": ["MISSING_OPTIONAL_COLUMN"]}
    expected[B007] = {"outcome": "QUARANTINED", "drift_kinds": ["UNREADABLE_FILE"]}
    expected[B008] = {"outcome": "QUARANTINED", "drift_kinds": ["INVALID_COLUMN_NAME"]}
    return Dataset(customers, batches, labels_df, expected)


def write_dataset(ds: Dataset, out_dir: str, expected_dir: str) -> None:
    """Write Parquet inputs (needs pyarrow) and, separately, the expected-answer labels."""
    inp = Path(out_dir)
    inp.mkdir(parents=True, exist_ok=True)
    ds.customers.to_parquet(inp.parent / "customers.parquet", index=False)
    for name, obj in ds.batches.items():
        if isinstance(obj, bytes):
            (inp / name).write_bytes(obj)
        else:
            obj.to_parquet(inp / name, index=False)
    exp = Path(expected_dir)
    exp.mkdir(parents=True, exist_ok=True)
    ds.labels.to_csv(exp / "labels.csv", index=False)
    (exp / "expected_outcomes.json").write_text(json.dumps(ds.expected, indent=2, sort_keys=True))
