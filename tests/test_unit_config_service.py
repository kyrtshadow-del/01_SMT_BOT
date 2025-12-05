import tempfile
import unittest
from pathlib import Path

from pipeline.config.unit_config import (
    CalibrationPoint,
    CalibrationSegment,
    GeneralConfig,
    SensorCalibration,
    SensorConfig,
    SensorValidation,
    UnitConfig,
)
from pipeline.config.unit_config_service import (
    UnitConfigService,
    UnitConfigValidationError,
)


def build_sample_config(unit_id: int = 101) -> UnitConfig:
    sensor = SensorConfig(
        sensor_id=1,
        name="Fuel tank A",
        type="fuel_level",
        units="l",
        description="Main tank",
        parameters={"p1": "param"},
        expression=None,
        validation=SensorValidation(mode="range", min_value=0, max_value=100, hysteresis=1.5),
        calibration=SensorCalibration(
            sensor_id=1,
            tank_id="A",
            points=[
                CalibrationPoint(raw=0, value=0),
                CalibrationPoint(raw=100, value=200),
            ],
            segments=[
                CalibrationSegment(start_x=0, a=2.0, b=0.0),
            ],
        ),
        meta={"kind": "fuel"},
    )
    return UnitConfig(
        unit_id=unit_id,
        source_kind="wialon",
        dump_ts=1700000000,
        general=GeneralConfig(name="Test unit", uid="123"),
        sensors=[sensor],
    )


class UnitConfigServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.service = UnitConfigService(root, schema_path=None)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_save_and_load_roundtrip(self):
        original = build_sample_config(500)
        path = self.service.save(original)
        self.assertTrue(path.exists())

        loaded = self.service.load(500)
        self.assertEqual(original, loaded)

    def test_list_configs(self):
        self.service.save(build_sample_config(1))
        self.service.save(build_sample_config(2))

        summaries = self.service.list_configs()
        unit_ids = sorted(summary.unit_id for summary in summaries)
        self.assertEqual(unit_ids, [1, 2])

    def test_invalid_unit_id_fails_validation(self):
        bad = build_sample_config()  # type: ignore[assignment]
        bad.unit_id = "oops"  # type: ignore[attr-defined]
        with self.assertRaises(UnitConfigValidationError):
            self.service.save(bad)


if __name__ == "__main__":
    unittest.main()
