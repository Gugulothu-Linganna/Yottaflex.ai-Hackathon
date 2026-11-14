"""Schema drift detection and the approved evolution rules.

Approved (COMPATIBLE) changes - applied automatically:
  ADD_COLUMN               new snake_case column with a supported type -> added to target as NULLable
  MISSING_OPTIONAL_COLUMN  optional / previously-added column absent -> filled with NULL
  WIDEN_INT_TO_FLOAT       int source for a float target column -> cast
  (column order changes and int32->int64 / float32->float64 are silently compatible)

Everything else is BREAKING and the whole batch is quarantined.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Set

from .config import RESERVED_NAMES, SNAKE_CASE

COMPATIBLE, BREAKING = "COMPATIBLE", "BREAKING"


@dataclass(frozen=True)
class Finding:
    kind: str
    column: str
    severity: str
    detail: str
    logical_type: Optional[str] = None  # set for ADD_COLUMN

    def to_dict(self) -> dict:
        return asdict(self)


def logical_type(physical: str) -> str:
    """Map a physical (Arrow-style) type string to a logical type."""
    p = physical.lower()
    if p.startswith(("int", "uint")):
        return "int"
    if p.startswith("float") or p in ("double", "halffloat"):
        return "float"
    if p in ("string", "large_string", "utf8", "large_utf8"):
        return "string"
    if p.startswith("timestamp"):
        return "timestamp"
    if p in ("bool", "boolean"):
        return "bool"
    return "other"


def detect_drift(batch_schema: Dict[str, str], target: Dict[str, str], required: Set[str]) -> List[Finding]:
    """Compare a batch schema (col -> physical type) with the target (col -> logical type)."""
    out: List[Finding] = []
    for col, tl in target.items():
        if col not in batch_schema:
            if col in required:
                out.append(Finding("MISSING_REQUIRED_COLUMN", col, BREAKING, f"required column '{col}' absent"))
            else:
                out.append(Finding("MISSING_OPTIONAL_COLUMN", col, COMPATIBLE, f"'{col}' absent; will be NULL"))
            continue
        phys = batch_schema[col]
        bl = logical_type(phys)
        if bl == tl:
            continue
        if tl == "float" and bl == "int":
            out.append(Finding("WIDEN_INT_TO_FLOAT", col, COMPATIBLE, f"{phys} -> float"))
        elif tl == "int" and bl == "float":
            out.append(Finding("NARROWING_FLOAT_TO_INT", col, BREAKING, f"{phys} would narrow to int"))
        else:
            out.append(Finding("TYPE_MISMATCH", col, BREAKING, f"expected {tl}, got {phys}"))
    for col, phys in batch_schema.items():
        if col in target:
            continue
        if not SNAKE_CASE.match(col):
            out.append(Finding("INVALID_COLUMN_NAME", col, BREAKING, f"'{col}' is not snake_case"))
        elif col in RESERVED_NAMES:
            out.append(Finding("RESERVED_COLUMN_NAME", col, BREAKING, f"'{col}' collides with a pipeline column"))
        elif logical_type(phys) == "other":
            out.append(Finding("UNSUPPORTED_TYPE", col, BREAKING, f"type {phys} not supported"))
        else:
            out.append(Finding("ADD_COLUMN", col, COMPATIBLE, f"new nullable column ({phys})", logical_type(phys)))
    return out


def has_breaking(findings: List[Finding]) -> bool:
    return any(f.severity == BREAKING for f in findings)
