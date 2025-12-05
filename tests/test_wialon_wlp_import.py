import json
import tempfile
from pathlib import Path

import pytest

from pipeline.adapters.wialon_wlp_import import import_wlp_to_service, load_wlp_unit_config
from pipeline.config.unit_config import UnitConfig
from pipeline.config.unit_config_service import UnitConfigService


SAMPLE_WLP = Path("Карточка объекта/LONKING CDM6225W - 3848уе23 1195 ВСЗ.wlp")


@pytest.mark.skipif(not SAMPLE_WLP.exists(), reason="Sample WLP file is required")
def test_load_wlp_unit_config_basic_fields() -> None:
    config = load_wlp_unit_config(SAMPLE_WLP, unit_id=101)
    assert isinstance(config, UnitConfig)
    assert config.unit_id == 101
    assert config.general is not None
    assert config.general.name.startswith("LONKING")
    assert config.general.uid == "863051063363567"
    assert config.hw_config is not None
    assert config.hw_config.hardware == "Galileosky 7x"
    assert config.sensors
    sensor_names = {sensor.name for sensor in config.sensors}
    assert "Водитель" in sensor_names or any("fuel" in name.lower() for name in sensor_names)


@pytest.mark.skipif(not SAMPLE_WLP.exists(), reason="Sample WLP file is required")
def test_import_wlp_to_service_roundtrip(tmp_path: Path) -> None:
    service = UnitConfigService(tmp_path, schema_path=None)
    result_path = import_wlp_to_service(SAMPLE_WLP, unit_id=202, service=service)
    assert result_path.exists()
    loaded = service.load(202)
    assert loaded.unit_id == 202
    assert loaded.general and loaded.general.name
    assert loaded.sensors
    # Ensure JSON is valid
    data = json.loads(result_path.read_text(encoding="utf-8"))
    assert "unit_id" in data and data["unit_id"] == 202
