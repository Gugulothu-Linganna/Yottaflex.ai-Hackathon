"""Target contract, naming rules and run configuration."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Optional

ISO_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"  # fixed-width UTC => lexicographic order == time order

# Target contract for `orders`. Logical types: string | int | float | timestamp | bool
REQUIRED_COLUMNS = {
    "order_id": "string",
    "customer_id": "string",
    "order_ts": "timestamp",
    "amount": "float",
    "status": "string",
    "updated_at": "timestamp",
}
OPTIONAL_COLUMNS = {"currency": "string"}

DERIVED_COLUMNS = ["order_date", "amount_cents"]
LINEAGE_COLUMNS = ["run_id", "source_file", "loaded_at"]
RESERVED_NAMES = frozenset(DERIVED_COLUMNS + LINEAGE_COLUMNS + ["source_row_number"])

VALID_STATUSES = frozenset({"created", "paid", "shipped", "cancelled", "refunded"})
SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
SQL_TYPES = {"string": "TEXT", "int": "INTEGER", "float": "REAL", "timestamp": "TEXT", "bool": "INTEGER"}


@dataclass
class PipelineConfig:
    input_dir: str
    customers_path: str
    db_path: str
    quarantine_dir: str
    report_path: Optional[str] = None
    reprocess_quarantined: bool = False
    # Test hook: called as hook(stage, source_file); may raise to simulate a crash.
    fault_hook: Optional[Callable[[str, str], None]] = None
