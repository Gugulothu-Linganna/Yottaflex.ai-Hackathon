import unittest

from schema_drift.config import OPTIONAL_COLUMNS, REQUIRED_COLUMNS
from schema_drift.drift import detect_drift, has_breaking

TARGET = {**REQUIRED_COLUMNS, **OPTIONAL_COLUMNS}
REQ = set(REQUIRED_COLUMNS)
BASE = {"order_id": "string", "customer_id": "string", "order_ts": "timestamp[us, tz=UTC]", "amount": "double",
        "status": "string", "updated_at": "timestamp[us, tz=UTC]", "currency": "string"}


def kinds(schema, target=TARGET):
    return {f.kind for f in detect_drift(schema, target, REQ)}


class DriftTests(unittest.TestCase):
    def test_identical_schema_no_findings(self):
        self.assertEqual(detect_drift(BASE, TARGET, REQ), [])

    def test_column_reorder_is_not_drift(self):
        self.assertEqual(detect_drift(dict(reversed(list(BASE.items()))), TARGET, REQ), [])

    def test_physical_width_changes_are_compatible(self):
        self.assertEqual(detect_drift({**BASE, "amount": "float"}, TARGET, REQ), [])

    def test_new_column_is_compatible_add(self):
        f = detect_drift({**BASE, "coupon_code": "string"}, TARGET, REQ)
        self.assertEqual([(x.kind, x.logical_type) for x in f], [("ADD_COLUMN", "string")])
        self.assertFalse(has_breaking(f))

    def test_missing_optional_is_compatible(self):
        s = {k: v for k, v in BASE.items() if k != "currency"}
        f = detect_drift(s, TARGET, REQ)
        self.assertEqual([x.kind for x in f], ["MISSING_OPTIONAL_COLUMN"])
        self.assertFalse(has_breaking(f))

    def test_int_to_float_widening_is_compatible(self):
        f = detect_drift({**BASE, "amount": "int32"}, TARGET, REQ)
        self.assertEqual([x.kind for x in f], ["WIDEN_INT_TO_FLOAT"])
        self.assertFalse(has_breaking(f))

    def test_missing_required_is_breaking(self):
        s = {k: v for k, v in BASE.items() if k != "customer_id"}
        self.assertIn("MISSING_REQUIRED_COLUMN", kinds(s))
        self.assertTrue(has_breaking(detect_drift(s, TARGET, REQ)))

    def test_rename_is_breaking(self):
        s = {("cust_id" if k == "customer_id" else k): v for k, v in BASE.items()}
        self.assertEqual(kinds(s), {"MISSING_REQUIRED_COLUMN", "ADD_COLUMN"})
        self.assertTrue(has_breaking(detect_drift(s, TARGET, REQ)))

    def test_string_for_numeric_is_breaking(self):
        self.assertIn("TYPE_MISMATCH", kinds({**BASE, "amount": "string"}))

    def test_float_for_int_target_is_narrowing(self):
        self.assertIn("NARROWING_FLOAT_TO_INT", kinds({"n": "double"}, {"n": "int"}))

    def test_string_timestamp_is_breaking(self):
        self.assertIn("TYPE_MISMATCH", kinds({**BASE, "order_ts": "string"}))

    def test_bad_new_column_names_are_breaking(self):
        self.assertIn("INVALID_COLUMN_NAME", kinds({**BASE, "Discount%": "double"}))
        self.assertIn("INVALID_COLUMN_NAME", kinds({**BASE, "CamelCase": "double"}))
        self.assertIn("RESERVED_COLUMN_NAME", kinds({**BASE, "run_id": "string"}))

    def test_unsupported_new_type_is_breaking(self):
        self.assertIn("UNSUPPORTED_TYPE", kinds({**BASE, "ship_date": "date32[day]"}))
