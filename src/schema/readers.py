"""Batch readers. Parquet I/O lives only here so the core logic is testable without pyarrow."""
from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Dict, List, Union

import numpy as np
import pandas as pd


class BatchUnreadable(Exception):
    """The file is not a readable batch (corrupt / not Parquet)."""


def basename(name: str) -> str:
    return Path(name).name


class ParquetReader:
    def list_batches(self, input_dir: str) -> List[str]:
        return sorted(str(p) for p in Path(input_dir).glob("*.parquet"))

    def fingerprint(self, name: str) -> str:
        h = hashlib.sha256()
        with open(name, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()

    def schema(self, name: str) -> Dict[str, str]:
        try:
            import pyarrow.parquet as pq
            return {f.name: str(f.type) for f in pq.read_schema(name)}
        except Exception as exc:  # corrupt footer, wrong magic bytes, ...
            raise BatchUnreadable(f"{type(exc).__name__}: {exc}") from exc

    def frame(self, name: str) -> pd.DataFrame:
        try:
            return pd.read_parquet(name)
        except Exception as exc:
            raise BatchUnreadable(f"{type(exc).__name__}: {exc}") from exc

    def copy_to(self, name: str, dest: str) -> None:
        shutil.copyfile(name, dest)


def _physical(series: pd.Series) -> str:
    dt = series.dtype
    if pd.api.types.is_bool_dtype(dt):
        return "bool"
    if pd.api.types.is_integer_dtype(dt):
        return str(dt).lower()
    if pd.api.types.is_float_dtype(dt):
        return "float" if dt == np.float32 else "double"
    if pd.api.types.is_datetime64_any_dtype(dt):
        return "timestamp[us, tz=UTC]"
    if dt == object or pd.api.types.is_string_dtype(dt):
        return "string"
    return str(dt)


class InMemoryReader:
    """Test double / benchmark reader: entries are DataFrames or raw bytes (=> unreadable)."""

    def __init__(self, batches: Dict[str, Union[pd.DataFrame, bytes]], reference: Dict[str, pd.DataFrame] = None):
        self.batches = dict(batches)
        self.reference = dict(reference or {})

    def list_batches(self, input_dir: str = "") -> List[str]:
        return sorted(self.batches)

    def _get(self, name):
        return self.batches[name] if name in self.batches else self.reference[name]

    def fingerprint(self, name: str) -> str:
        obj = self._get(name)
        if isinstance(obj, bytes):
            return hashlib.sha256(obj).hexdigest()
        schema = "|".join(f"{c}:{_physical(obj[c])}" for c in obj.columns)
        return hashlib.sha256((schema + obj.to_csv(index=False)).encode()).hexdigest()

    def schema(self, name: str) -> Dict[str, str]:
        obj = self._get(name)
        if isinstance(obj, bytes):
            raise BatchUnreadable("not a Parquet file (bad magic bytes)")
        return {c: _physical(obj[c]) for c in obj.columns}

    def frame(self, name: str) -> pd.DataFrame:
        obj = self._get(name)
        if isinstance(obj, bytes):
            raise BatchUnreadable("not a Parquet file (bad magic bytes)")
        return obj.copy()

    def copy_to(self, name: str, dest: str) -> None:
        obj = self._get(name)
        if isinstance(obj, bytes):
            Path(dest).write_bytes(obj)
        else:
            obj.to_csv(dest, index=False)
