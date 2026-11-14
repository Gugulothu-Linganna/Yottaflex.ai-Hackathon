import logging
import sqlite3
import tempfile
import unittest
from pathlib import Path

from schema_drift.config import PipelineConfig
from schema_drift.generate import build_dataset
from schema_drift.pipeline import run_pipeline
from schema_drift.readers import InMemoryReader

logging.getLogger("schema_drift").setLevel(logging.CRITICAL)


class PipelineCase(unittest.TestCase):
    """Fresh temp dir + generated dataset (seed 42, 200 orders) per test."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.ds = build_dataset(200, 42)
        self.cfg = PipelineConfig("in", "customers", str(self.tmp / "wh.db"), str(self.tmp / "quarantine"))
        self.reader = self.make_reader(self.ds.batches)

    def make_reader(self, batches):
        return InMemoryReader(batches, {"customers": self.ds.customers})

    def run_it(self, reader=None, cfg=None, **kw):
        return run_pipeline(cfg or self.cfg, reader or self.reader, **kw)

    def db(self):
        return sqlite3.connect(self.cfg.db_path)

    def q(self, sql, *args):
        with self.db() as c:
            return c.execute(sql, args).fetchall()
