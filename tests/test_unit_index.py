import tempfile
import unittest
from pathlib import Path

from pipeline.services.unit_index import UnitIndex
from pipeline.services.unit_snapshot_service import UnitSnapshotRecord, UnitSnapshotService


class UnitIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        snapshot_path = Path(self.tmp.name) / "unit_snapshot.json.gz"
        self.service = UnitSnapshotService(snapshot_path=snapshot_path, legacy_path=snapshot_path)
        self.service.refresh(
            [
                UnitSnapshotRecord(unit_id=100, name="Tractor Alpha", reg_number="ABC123", contacts={"phone": "+123"}),
                UnitSnapshotRecord(unit_id=200, name="Truck Beta"),
            ],
            source_kind="test",
            dump_ts=1,
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_search_by_name_and_id(self):
        index = UnitIndex(self.service)
        by_name = index.search("tractor")
        self.assertEqual(len(by_name), 1)
        self.assertEqual(by_name[0]["id"], 100)
        by_id = index.search("200")
        self.assertEqual(len(by_id), 1)
        self.assertEqual(by_id[0]["id"], 200)


if __name__ == "__main__":
    unittest.main()
