import json
import unittest
from pathlib import Path

from helpers import PipelineCase
from schema_drift.generate import B001, B002, B002R, B003, B004, B005, B006, B007, B008
from schema_drift.pipeline import run_pipeline
from schema_drift.readers import InMemoryReader

COUNT_KEYS = ["rows_in", "rows_inserted", "rows_updated", "rows_unchanged", "rows_stale", "rows_rejected"]


def orders_snapshot(case):
    cols = [r[1] for r in case.q("PRAGMA table_info(orders)")]
    keep = [c for c in cols if c not in ("run_id", "source_file", "loaded_at")]
    q = ",".join(f'"{c}"' for c in sorted(keep))
    return case.q(f"SELECT {q} FROM orders ORDER BY order_id")


class AcceptanceTests(PipelineCase):
    def test_outcomes_match_expected_labels(self):
        s = self.run_it()
        by_file = {b["source_file"]: b for b in s["batches"]}
        for name, exp in self.ds.expected.items():
            got = by_file[name]
            self.assertEqual(got["outcome"], exp["outcome"], name)
            for k in COUNT_KEYS:
                if k in exp:
                    self.assertEqual(got[k], exp[k], f"{name}:{k}")
            kinds = {f["kind"] for f in got["findings"]}
            self.assertTrue(set(exp.get("drift_kinds", [])) <= kinds, f"{name}: {kinds}")
            if "rejects_by_reason" in exp:
                rows = self.q("SELECT reason_code, COUNT(*) FROM rejects WHERE source_file=? GROUP BY 1", name)
                self.assertEqual(dict(rows), exp["rejects_by_reason"], name)
        self.assertEqual(s["status"], "SUCCESS_WITH_QUARANTINE")

    def test_totals_reconcile_with_database(self):
        s = self.run_it()
        t = s["totals"]
        self.assertEqual(t["rows_in"], t["rows_inserted"] + t["rows_updated"] + t["rows_unchanged"] + t["rows_stale"] + t["rows_rejected"])
        self.assertEqual(self.q("SELECT COUNT(*) FROM orders")[0][0], t["rows_inserted"])
        self.assertEqual(self.q("SELECT COUNT(*) FROM rejects")[0][0], t["rows_rejected"])
        run = self.q("SELECT status, rows_in, batches_quarantined FROM pipeline_runs")[0]
        self.assertEqual(run, (s["status"], t["rows_in"], 4))

    def test_updates_applied_and_stale_ignored(self):
        self.run_it()
        self.assertEqual(self.q("SELECT COUNT(*) FROM orders WHERE status='shipped' AND source_file=?", B002)[0][0] >= 20, True)
        # stale (late) versions are kept only in history, never as current
        self.assertEqual(self.q("""SELECT COUNT(*) FROM orders o WHERE updated_at <
                                   (SELECT MAX(updated_at) FROM orders_history h WHERE h.order_id=o.order_id)""")[0][0], 0)
        self.assertGreater(self.q("SELECT COUNT(*) FROM orders_history")[0][0], self.q("SELECT COUNT(*) FROM orders")[0][0])


class SchemaEvolutionTests(PipelineCase):
    def test_added_column_is_evolved_into_target(self):
        self.run_it()
        cols = [r[1] for r in self.q("PRAGMA table_info(orders)")]
        self.assertIn("coupon_code", cols)
        self.assertEqual(self.q("SELECT origin, added_run_id IS NOT NULL FROM schema_registry WHERE column_name='coupon_code'"), [("evolved", 1)])
        self.assertGreater(self.q("SELECT COUNT(*) FROM orders WHERE coupon_code='WELCOME10'")[0][0], 0)
        # batch 003 lacks coupon_code and currency -> NULL, not an error
        self.assertEqual(self.q("SELECT COUNT(*) FROM orders WHERE source_file=? AND currency IS NOT NULL", B003)[0][0], 0)

    def test_breaking_batches_quarantined_with_findings_and_not_loaded(self):
        self.run_it()
        for name, kind in [(B004, "MISSING_REQUIRED_COLUMN"), (B005, "TYPE_MISMATCH"), (B007, "UNREADABLE_FILE"), (B008, "INVALID_COLUMN_NAME")]:
            self.assertEqual(self.q("SELECT outcome FROM batch_runs WHERE source_file=?", name), [("QUARANTINED",)])
            self.assertGreater(self.q("SELECT COUNT(*) FROM schema_drift_log WHERE source_file=? AND kind=? AND severity='BREAKING'", name, kind)[0][0], 0)
            self.assertEqual(self.q("SELECT COUNT(*) FROM orders WHERE source_file=?", name)[0][0], 0)
            files = list(Path(self.cfg.quarantine_dir).glob(f"*__{name}"))
            self.assertEqual(len(files), 1)
            side = json.loads(Path(str(files[0]) + ".findings.json").read_text())
            self.assertTrue(any(f["kind"] == kind for f in side["findings"]))

    def test_empty_batch_loads_zero_rows(self):
        self.run_it()
        self.assertEqual(self.q("SELECT outcome, rows_in, note FROM batch_runs WHERE source_file=?", B006), [("LOADED", 0, "empty batch")])

    def test_empty_input_directory(self):
        s = self.run_it(self.make_reader({}))
        self.assertEqual((s["status"], s["totals"]["batches_seen"]), ("SUCCESS", 0))
        self.assertEqual(self.q("SELECT COUNT(*) FROM orders")[0][0], 0)


class IdempotencyTests(PipelineCase):
    def dump(self):
        return {t: sorted(self.q(f"SELECT * FROM {t}"), key=repr) for t in
                ("orders", "orders_history", "rejects", "dim_customers", "schema_registry")}

    def test_rerun_same_input_changes_nothing(self):
        self.run_it()
        before = self.dump()
        s2 = self.run_it()
        self.assertEqual(self.dump(), before)
        self.assertEqual(s2["totals"]["batches_skipped"], 9)
        self.assertEqual(s2["totals"]["rows_inserted"], 0)

    def test_same_rows_in_differently_encoded_file_creates_no_duplicates(self):
        self.run_it()
        before = self.dump()
        b3 = self.ds.batches[B003]
        reexport = b3[list(reversed(b3.columns))]  # different bytes/fingerprint, same rows
        r = self.make_reader({"orders_batch_009_reexport.parquet": reexport})
        s = self.run_it(r)
        b = s["batches"][0]
        self.assertEqual((b["outcome"], b["rows_inserted"], b["rows_updated"]), ("LOADED", 0, 0))
        self.assertEqual(b["rows_unchanged"], 19)
        self.assertEqual(self.dump()["orders"], before["orders"])

    def test_reprocess_quarantined_flag(self):
        self.run_it()
        s = self.run_it()
        self.assertEqual({b["outcome"] for b in s["batches"] if "breaking" in b["source_file"]}, {"SKIPPED"})
        self.cfg.reprocess_quarantined = True
        s = self.run_it()
        outs = {b["source_file"]: b["outcome"] for b in s["batches"]}
        self.assertEqual(outs[B004], "QUARANTINED")  # still breaking, but was re-evaluated
        self.assertEqual(outs[B001], "SKIPPED")

    def test_load_order_does_not_change_final_state(self):
        self.run_it()
        forward = orders_snapshot(self)
        rev_cfg = type(self.cfg)("in", "customers", str(self.tmp / "rev.db"), str(self.tmp / "q2"))
        # reversed processing order: rename files so sort order flips
        flipped = {f"z{len(self.ds.batches) - i:02d}_{k}": v for i, (k, v) in enumerate(self.ds.batches.items())}
        run_pipeline(rev_cfg, self.make_reader(flipped))
        self.cfg.db_path = rev_cfg.db_path
        self.assertEqual(orders_snapshot(self), forward)


class RecoveryTests(PipelineCase):
    def test_partial_failure_rolls_back_and_retry_succeeds(self):
        def boom(stage, src):
            if src in (B002, B002R):  # the replay carries identical content, so fail both copies
                raise RuntimeError("simulated crash after upsert")
        self.cfg.fault_hook = boom
        s = self.run_it(run_id="run-1")
        self.assertEqual(s["status"], "PARTIAL_FAILURE")
        failed = [b for b in s["batches"] if b["outcome"] == "FAILED"]
        self.assertEqual([b["source_file"] for b in failed], [B002, B002R])
        # rolled back: no 002 rows, no evolved column, no registry entry; other batches unaffected
        self.assertEqual(self.q("SELECT COUNT(*) FROM orders WHERE source_file=?", B002)[0][0], 0)
        self.assertNotIn("coupon_code", [r[1] for r in self.q("PRAGMA table_info(orders)")])
        self.assertEqual(self.q("SELECT COUNT(*) FROM schema_registry WHERE column_name='coupon_code'")[0][0], 0)
        self.assertEqual(self.q("SELECT outcome FROM batch_runs WHERE source_file=?", B002), [("FAILED",)])
        self.assertGreater(self.q("SELECT COUNT(*) FROM orders WHERE source_file=?", B001)[0][0], 0)
        # retry without the fault: failed batch is loaded, everything else skipped; state == clean run
        self.cfg.fault_hook = None
        s2 = self.run_it(run_id="run-2")
        self.assertEqual(s2["status"], "SUCCESS")  # quarantined batches were already handled in run 1
        self.assertEqual(self.q("SELECT outcome FROM batch_runs WHERE source_file=? ORDER BY run_id DESC LIMIT 1", B002), [("LOADED",)])
        self.assertEqual(self.q("SELECT outcome FROM batch_runs WHERE source_file=? ORDER BY run_id DESC LIMIT 1", B002R), [("SKIPPED",)])
        retried = orders_snapshot(self)
        clean = type(self.cfg)("in", "customers", str(self.tmp / "clean.db"), str(self.tmp / "q3"))
        run_pipeline(clean, self.make_reader(self.ds.batches))
        self.cfg.db_path = clean.db_path
        self.assertEqual(orders_snapshot(self), retried)

    def test_missing_reference_file_fails_run_clearly(self):
        s = self.run_it(InMemoryReader(self.ds.batches, {}))
        self.assertEqual(s["status"], "FAILED")
        self.assertIn("KeyError", s["error"])
        self.assertEqual(self.q("SELECT status FROM pipeline_runs"), [("FAILED",)])


if __name__ == "__main__":
    unittest.main()
