import gzip
import json
import tempfile
import unittest
from pathlib import Path

from pipeline.services.unit_snapshot_service import UnitSnapshotService
from pipeline.services.unit_snapshot_sync import (
    records_from_payload,
    sync_unit_snapshot_from_payload,
)


def _make_payload(units: dict, dump_ts: int = 1700000000) -> dict:
    return {
        "version": 1,
        "dump_ts": dump_ts,
        "items_by_id": {str(uid): raw for uid, raw in units.items()},
    }


class UnitSnapshotSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.snapshot_path = root / "unit_snapshot.json.gz"
        self.legacy_path = root / "legacy.json.gz"
        self.service = UnitSnapshotService(snapshot_path=self.snapshot_path, legacy_path=self.legacy_path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_records_from_payload(self):
        payload = _make_payload(
            {
                1: {"nm": "Unit 1", "sens": [{"id": 10, "n": "Fuel", "t": "fuel_level"}]},
                2: {"nm": "Unit 2"},
            },
            dump_ts=180,
        )
        records, dump_ts = records_from_payload(payload)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].name, "Unit 1")
        self.assertEqual(dump_ts, 180)

    def test_sync_unit_snapshot(self):
        payload = _make_payload({1: {"nm": "Unit"}})
        changed = sync_unit_snapshot_from_payload(payload, self.service, force=False)
        self.assertTrue(changed)
        bundle = self.service.load_bundle()
        self.assertEqual(bundle.source_kind, "unit_cache")
        self.assertIn(1, bundle.units)

    def test_sync_skips_when_dump_ts_not_newer(self):
        payload = _make_payload({1: {"nm": "Unit"}}, dump_ts=100)
        sync_unit_snapshot_from_payload(payload, self.service, force=True)
        changed = sync_unit_snapshot_from_payload(payload, self.service, force=False)
        self.assertFalse(changed)


if __name__ == "__main__":
    unittest.main()
