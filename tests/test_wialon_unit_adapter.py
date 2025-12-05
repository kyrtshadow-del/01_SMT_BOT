import json
import tempfile
import unittest
from pathlib import Path

from pipeline.adapters.wialon.unit_config_adapter import WialonUnitConfigAdapter
from pipeline.cli import import_unit_config as import_cli


FIXTURE = Path(__file__).with_name("fixtures") / "wialon_unit_sample.wlp"


class WialonUnitAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.adapter = WialonUnitConfigAdapter()
        self.payload = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_from_wlp_dict(self):
        config = self.adapter.from_wlp_dict(self.payload, unit_id=77, dump_ts=1700000000)
        self.assertEqual(config.unit_id, 77)
        self.assertEqual(config.general.name, "Demo Unit")
        self.assertEqual(len(config.sensors), 1)
        sensor = config.sensors[0]
        self.assertEqual(sensor.calibration.segments[1].a, 0.25)
        self.assertEqual(sensor.validation.mode, "2")
        self.assertEqual(config.advanced["monitoring_sensor"], "10")


class ImportUnitConfigCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.storage = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_cli_creates_unit_config_file(self):
        return_code = import_cli.main(
            [
                "--wlp",
                str(FIXTURE),
                "--unit-id",
                "42",
                "--dump-ts",
                "1700001000",
                "--storage-root",
                str(self.storage),
            ]
        )
        self.assertEqual(return_code, 0)
        path = self.storage / "unit_configs" / "42.json"
        self.assertTrue(path.exists())
        payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload["unit_id"], 42)
        self.assertEqual(payload["source_kind"], "wialon")


if __name__ == "__main__":
    unittest.main()
