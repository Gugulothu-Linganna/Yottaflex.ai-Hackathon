import unittest

from helpers import PipelineCase
from schema_drift.generate import build_dataset
from schema_drift.reconcile import reconcile


class ReconcileTests(PipelineCase):
    def test_all_checks_pass_after_scenario_run(self):
        self.run_it()
        with self.db() as c:
            checks = reconcile(c)
        failed = [x for x in checks if not x["passed"]]
        self.assertEqual(failed, [])
        self.assertGreaterEqual(len(checks), 8)

    def test_detects_tampering(self):
        self.run_it()
        with self.db() as c:
            c.execute("DELETE FROM orders WHERE order_id=(SELECT MIN(order_id) FROM orders)")
            c.execute("UPDATE orders SET amount_cents = amount_cents + 1 WHERE order_id=(SELECT MAX(order_id) FROM orders)")
            failed = {x["check"] for x in reconcile(c) if not x["passed"]}
        self.assertIn("orders_equals_inserted_keys", failed)
        self.assertIn("derived_amount_cents_consistent", failed)


class GeneratorTests(unittest.TestCase):
    def test_deterministic_for_same_seed(self):
        a, b = build_dataset(100, 7), build_dataset(100, 7)
        for k in a.batches:
            if not isinstance(a.batches[k], bytes):
                self.assertTrue(a.batches[k].equals(b.batches[k]), k)
        self.assertTrue(a.labels.equals(b.labels))

    def test_different_seed_differs(self):
        self.assertFalse(build_dataset(100, 1).batches["orders_batch_001_baseline.parquet"].equals(
            build_dataset(100, 2).batches["orders_batch_001_baseline.parquet"]))

    def test_labels_are_separate_and_aligned(self):
        ds = build_dataset(100, 3)
        for name, frame in ds.batches.items():
            if isinstance(frame, bytes):
                continue
            self.assertNotIn("expected", frame.columns)
            n = int((ds.labels.batch == name).sum())
            if n:
                self.assertEqual(n, len(frame), name)

    def test_scales_and_validates_size(self):
        self.assertEqual(len(build_dataset(5000, 1).batches["orders_batch_001_baseline.parquet"]), 5000 + 5000 // 50)
        with self.assertRaises(ValueError):
            build_dataset(10)
