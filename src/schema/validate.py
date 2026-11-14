"""Row-level conformance, validation and deterministic transformations (vectorised)."""
from __future__ import annotations

import json
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

from .config import ISO_FMT, REQUIRED_COLUMNS, VALID_STATUSES

# Reject reason codes, in precedence order (first failing rule wins).
MISSING_REQUIRED_VALUE = "MISSING_REQUIRED_VALUE"
INVALID_AMOUNT = "INVALID_AMOUNT"
INVALID_STATUS = "INVALID_STATUS"
UNKNOWN_CUSTOMER = "UNKNOWN_CUSTOMER"
UPDATED_BEFORE_CREATED = "UPDATED_BEFORE_CREATED"
DUPLICATE_IN_BATCH = "DUPLICATE_IN_BATCH"


def _cast(s: pd.Series, ltype: str, col: str) -> pd.Series:
    if ltype == "string":
        s = s.astype("string").str.strip()
        s = s.mask(s.fillna("x") == "")  # blank string == missing
        if col == "status":
            s = s.str.lower()
        elif col == "currency":
            s = s.str.upper()
        return s
    if ltype == "float":
        return pd.to_numeric(s, errors="coerce").astype("float64")
    if ltype == "int":
        return pd.to_numeric(s, errors="coerce").astype("Int64")
    if ltype == "timestamp":
        return pd.to_datetime(s, utc=True, errors="coerce")
    if ltype == "bool":
        return s.astype("boolean")
    raise ValueError(f"unsupported logical type {ltype}")


def conform_frame(df: pd.DataFrame, target_cols: Dict[str, str]) -> pd.DataFrame:
    """Project to target columns (order = target order), cast types, NULL-fill absent columns."""
    df = df.reset_index(drop=True)
    out = pd.DataFrame(index=range(len(df)))
    for col, ltype in target_cols.items():
        s = df[col] if col in df.columns else pd.Series([None] * len(df), dtype="object")
        out[col] = _cast(s, ltype, col)
    out["source_row_number"] = np.arange(len(df), dtype="int64")
    return out


def _json_safe(v):
    if v is None or v is pd.NA or (isinstance(v, float) and np.isnan(v)) or v is pd.NaT:
        return None
    if isinstance(v, pd.Timestamp):
        return v.strftime(ISO_FMT)
    if isinstance(v, np.generic):
        return v.item()
    return v


def validate_rows(df: pd.DataFrame, valid_customers: Iterable[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return (valid rows incl. derived fields, rejects). valid + rejects == input rows."""
    reason = pd.Series([None] * len(df), index=df.index, dtype="object")
    detail = pd.Series([None] * len(df), index=df.index, dtype="object")

    def flag(mask: pd.Series, code: str, text=None):
        m = mask.fillna(False).astype(bool) & reason.isna()
        if m.any():
            reason[m] = code
            detail[m] = text if text is not None else code

    req = list(REQUIRED_COLUMNS)
    nulls = df[req].isna()
    miss = nulls.any(axis=1)
    if miss.any():
        txt = nulls[miss].apply(lambda r: "null: " + ",".join(c for c, v in r.items() if v), axis=1)
        reason[miss] = MISSING_REQUIRED_VALUE
        detail[miss] = txt
    flag(~np.isfinite(df["amount"].astype("float64")) | (df["amount"] < 0), INVALID_AMOUNT, "amount must be finite and >= 0")
    flag(~df["status"].isin(list(VALID_STATUSES)), INVALID_STATUS, "status not in allowed set")
    flag(~df["customer_id"].isin(list(valid_customers)), UNKNOWN_CUSTOMER, "customer_id not in dim_customers")
    flag(df["updated_at"] < df["order_ts"], UPDATED_BEFORE_CREATED, "updated_at earlier than order_ts")

    # Duplicates within the batch: newest updated_at wins; tie -> later row in file wins.
    ok = df[reason.isna()].sort_values(["updated_at", "source_row_number"], kind="stable")
    winners = ok.drop_duplicates("order_id", keep="last")
    losers = ok.index.difference(winners.index)
    if len(losers):
        win_row = winners.set_index("order_id")["source_row_number"]
        reason[losers] = DUPLICATE_IN_BATCH
        detail[losers] = "superseded by row " + df.loc[losers, "order_id"].map(win_row).astype(int).astype(str)

    valid = df[reason.isna()].copy()
    valid["amount"] = valid["amount"].round(2)
    valid["order_date"] = valid["order_ts"].dt.strftime("%Y-%m-%d")
    valid["amount_cents"] = (valid["amount"] * 100).round().astype("int64")

    bad = df[reason.notna()]
    rejects = pd.DataFrame({
        "source_row_number": bad["source_row_number"].astype("int64"),
        "order_id": bad["order_id"].astype(object).where(bad["order_id"].notna(), None),
        "reason_code": reason[bad.index],
        "reason_detail": detail[bad.index],
        "record_json": [json.dumps({k: _json_safe(v) for k, v in r.items()}, default=str, sort_keys=True)
                        for r in bad.to_dict("records")],
    })
    return valid, rejects.reset_index(drop=True)


def to_db_rows(valid: pd.DataFrame, source_cols: Dict[str, str], extra: Dict[str, object]) -> Tuple[List[str], List[list]]:
    """Convert validated rows to python-native lists for sqlite. Returns (columns, rows)."""
    out = pd.DataFrame(index=valid.index)
    for col, lt in source_cols.items():
        s = valid[col]
        out[col] = s.dt.strftime(ISO_FMT) if lt == "timestamp" else s
    for c in ("order_date", "amount_cents"):
        out[c] = valid[c]
    for c, v in extra.items():
        out[c] = v
    cols = list(out.columns)
    obj = out.astype(object)
    rows = obj.where(out.notna(), None).values.tolist()
    return cols, rows
