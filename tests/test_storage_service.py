import tempfile
import unittest
from pathlib import Path

from pipeline.config.defaults import PipelineConfig
from pipeline.events import Event
from pipeline.services import unit_index
from pipeline.services.storage_service import PipelineStorageService
from pipeline.services.unit_snapshot_service import UnitSnapshotRecord, UnitSnapshotService
from pipeline.storage.latest_metrics import LatestTelemetryStore
from pipeline.storage.raw_storage import RawStorage


class StorageServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.raw = RawStorage(root)
        self.latest = LatestTelemetryStore(root)
        config = PipelineConfig(storage_root=root)
        self.service = PipelineStorageService(config=config, raw_storage=self.raw, latest_store=self.latest)

        # Prepare unit snapshot for UnitIndex
        snapshot_path = root / "unit_snapshot.json.gz"
        snapshot_service = UnitSnapshotService(snapshot_path=snapshot_path, legacy_path=snapshot_path)
        snapshot_service.refresh(
            [
                UnitSnapshotRecord(unit_id=1, name="Alpha"),
                UnitSnapshotRecord(unit_id=2, name="Beta"),
            ],
            source_kind="test",
            dump_ts=1,
        )
        unit_index._INDEX = unit_index.UnitIndex(snapshot_service)

    def tearDown(self) -> None:
        self.tmp.cleanup()
        unit_index._INDEX = None

    def test_find_nearest_units(self):
        events = [
            Event(
                unit_id=1,
                device_ts=100,
                received_ts=100,
                latitude=53.0,
                longitude=39.0,
                speed=None,
                course=None,
                params={},
                source="test",
                raw_payload={},
            ),
            Event(
                unit_id=2,
                device_ts=100,
                received_ts=100,
                latitude=53.0005,
                longitude=39.0005,
                speed=None,
                course=None,
                params={},
                source="test",
                raw_payload={},
            ),
        ]
        self.latest.update_from_events(events)

        nearest = self.service.find_nearest_units(53.0, 39.0, max_distance_m=200, limit=5, exclude_unit_id=1)
        self.assertEqual(len(nearest), 1)
        self.assertEqual(nearest[0]["id"], 2)


if __name__ == "__main__":
    unittest.main()
