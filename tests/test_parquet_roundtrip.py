"""Real Parquet I/O. Skipped automatically when pyarrow is not installed."""
import importlib.util
import json
import unittest
from pathlib import Path

from helpers import PipelineCase
from schema_drift.config import PipelineConfig
from schema_drift.generate import write_dataset
from schema_drift.pipeline import run_pipeline
from schema_drift.readers import ParquetReader


@unittest.skipUnless(importlib.util.find_spec("pyarrow"), "pyarrow not installed")
class ParquetRoundTrip(PipelineCase):
    def test_end_to_end_on_real_parquet_files(self):
        write_dataset(self.ds, str(self.tmp / "d/input"), str(self.tmp / "d/expected"))
        cfg = PipelineConfig(str(self.tmp / "d/input"), str(self.tmp / "d/customers.parquet"),
                             str(self.tmp / "p.db"), str(self.tmp / "pq"))
        s = run_pipeline(cfg, ParquetReader())
        exp = json.loads((self.tmp / "d/expected/expected_outcomes.json").read_text())
        got = {b["source_file"]: b for b in s["batches"]}
        for name, e in exp.items():
            self.assertEqual(got[name]["outcome"], e["outcome"], name)
            for k in ("rows_in", "rows_inserted", "rows_updated", "rows_unchanged", "rows_stale", "rows_rejected"):
                if k in e:
                    self.assertEqual(got[name][k], e[k], f"{name}:{k}")
        self.assertEqual(run_pipeline(cfg, ParquetReader())["totals"]["batches_skipped"], 9)


if __name__ == "__main__":
    unittest.main()
