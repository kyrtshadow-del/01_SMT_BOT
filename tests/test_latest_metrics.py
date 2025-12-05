import json
import tempfile
import unittest
from pathlib import Path

from pipeline.events import Event
from pipeline.storage.latest_metrics import LatestTelemetryStore


class LatestTelemetryStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LatestTelemetryStore(Path(self.tmp.name))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_updates_and_persists(self):
        events = [
            Event(
                unit_id=1,
                device_ts=100,
                received_ts=105,
                latitude=53.0,
                longitude=39.0,
                speed=None,
                course=None,
                params={"sats": 7},
                source="test",
                raw_payload={},
            )
        ]
        self.store.update_from_events(events)

        data = json.loads((Path(self.tmp.name) / "latest_metrics.json").read_text(encoding="utf-8"))
        self.assertIn("1", data)
        self.assertEqual(data["1"]["params"]["sats"], 7)
        self.assertEqual(data["1"]["lat"], 53.0)

        entry = self.store.get_latest(1)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["params"]["sats"], 7)
        self.assertEqual(entry["lat"], 53.0)

        items = list(self.store.iter_latest())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0][0], 1)


if __name__ == "__main__":
    unittest.main()
