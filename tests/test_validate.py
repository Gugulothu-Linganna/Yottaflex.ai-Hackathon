import unittest

import numpy as np
import pandas as pd

from schema_drift.config import OPTIONAL_COLUMNS, REQUIRED_COLUMNS
from schema_drift.validate import conform_frame, validate_rows

TARGET = {**REQUIRED_COLUMNS, **OPTIONAL_COLUMNS}
CUST = {"C1", "C2"}
T = pd.Timestamp("2026-02-01T10:00:00Z")


def row(**kw):
    base = dict(order_id="O1", customer_id="C1", order_ts=T, amount=10.0, status="paid",
                updated_at=T + pd.Timedelta(hours=1), currency="USD")
    base.update(kw)
    return base


def run(rows):
    return validate_rows(conform_frame(pd.DataFrame(rows), TARGET), CUST)


class ValidateTests(unittest.TestCase):
    def test_happy_path_and_derived_fields(self):
        v, r = run([row(amount=12.344)])
        self.assertEqual((len(v), len(r)), (1, 0))
        self.assertEqual(v.iloc[0]["amount"], 12.34)
        self.assertEqual(v.iloc[0]["order_date"], "2026-02-01")
        self.assertEqual(v.iloc[0]["amount_cents"], 1234)

    def test_empty_input(self):
        v, r = validate_rows(conform_frame(pd.DataFrame({c: [] for c in TARGET}), TARGET), CUST)
        self.assertEqual((len(v), len(r)), (0, 0))

    def test_null_and_blank_required_values(self):
        v, r = run([row(order_id="O1", customer_id=None), row(order_id="O2", status="  "), row(order_id="O3", amount=np.nan)])
        self.assertEqual(len(v), 0)
        self.assertEqual(set(r.reason_code), {"MISSING_REQUIRED_VALUE"})
        self.assertIn("customer_id", r.reason_detail.iloc[0])

    def test_malformed_values(self):
        v, r = run([row(order_id="O1", amount=-0.01), row(order_id="O2", amount=np.inf),
                    row(order_id="O3", status="teleported")])
        self.assertEqual(list(r.reason_code), ["INVALID_AMOUNT", "INVALID_AMOUNT", "INVALID_STATUS"])

    def test_boundaries_are_valid(self):
        v, r = run([row(order_id="O1", amount=0.0), row(order_id="O2", updated_at=T)])
        self.assertEqual((len(v), len(r)), (2, 0))  # amount == 0 and updated_at == order_ts are allowed

    def test_normalisation_is_deterministic(self):
        v, _ = run([row(customer_id=" C1 ", status=" PAID ", currency="usd")])
        self.assertEqual((v.iloc[0].customer_id, v.iloc[0].status, v.iloc[0].currency), ("C1", "paid", "USD"))

    def test_invalid_reference_key(self):
        _, r = run([row(customer_id="C9")])
        self.assertEqual(list(r.reason_code), ["UNKNOWN_CUSTOMER"])

    def test_updated_before_created(self):
        _, r = run([row(updated_at=T - pd.Timedelta(seconds=1))])
        self.assertEqual(list(r.reason_code), ["UPDATED_BEFORE_CREATED"])

    def test_duplicates_newest_wins_and_traceable(self):
        v, r = run([row(amount=1.0), row(amount=2.0, updated_at=T + pd.Timedelta(hours=2)), row(amount=3.0, updated_at=T + pd.Timedelta(hours=1, minutes=30))])
        self.assertEqual(list(v.amount), [2.0])
        self.assertEqual(sorted(r.source_row_number), [0, 2])
        self.assertTrue(all(r.reason_code == "DUPLICATE_IN_BATCH"))
        self.assertTrue(all(r.reason_detail == "superseded by row 1"))

    def test_duplicate_tie_later_row_wins(self):
        v, r = run([row(amount=1.0), row(amount=2.0)])
        self.assertEqual(list(v.amount), [2.0])
        self.assertEqual(list(r.source_row_number), [0])

    def test_rows_conserved(self):
        rows = [row(order_id=f"O{i}", amount=-1.0 if i % 3 == 0 else 5.0) for i in range(30)]
        v, r = run(rows)
        self.assertEqual(len(v) + len(r), 30)

    def test_reject_keeps_source_record(self):
        _, r = run([row(order_id="OX", status="nope")])
        self.assertIn('"order_id": "OX"', r.record_json.iloc[0])
