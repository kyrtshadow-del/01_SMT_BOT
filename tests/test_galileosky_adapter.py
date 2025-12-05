import json
from pathlib import Path

import pytest

from pipeline.adapters.galileosky.stream import GalileoskyStreamAdapter
from pipeline.storage.raw_storage import RawStorage


def make_adapter(tmp_path: Path) -> GalileoskyStreamAdapter:
    # Use tmp_path both for raw storage and health report to avoid touching real data/logs.
    return GalileoskyStreamAdapter(
        raw_storage=RawStorage(tmp_path),
        latest_store=None,
        health_report_path=tmp_path / "galileosky_ingest_health.json",
        metrics_interval=0,
    )


def test_build_event_from_json_payload(tmp_path: Path) -> None:
    adapter = make_adapter(tmp_path)
    # Force stable unit_id resolution for the test.
    adapter._resolve_unit_id = lambda uid: 16095  # type: ignore[assignment]

    payload = {
        "uid": "864495031920561",
        "ts": 1732213260,
        "lat": 52.133928,
        "lon": 42.089836,
        "speed": 12.0,
        "course": 180.0,
        "params": {
            "pwr_ext": 12.238,
            "hdop": 0.6,
            "nsat": 7,
        },
    }

    ev = adapter._build_event(payload)
    assert ev is not None
    assert ev.unit_id == 16095
    assert ev.latitude == pytest.approx(52.133928)
    assert ev.longitude == pytest.approx(42.089836)
    assert ev.speed == pytest.approx(12.0)
    assert ev.course == pytest.approx(180.0)
    assert ev.params["pwr_ext"] == pytest.approx(12.238)
    # Source marker is important for downstream diagnostics.
    assert ev.source == "galileosky"


def test_health_snapshot_written(tmp_path: Path) -> None:
    adapter = make_adapter(tmp_path)
    adapter._resolve_unit_id = lambda uid: 16095  # type: ignore[assignment]

    payload = {
        "uid": "864495031920561",
        "ts": 1732213260,
        "lat": 52.1339,
        "lon": 42.0898,
        "speed": 12.0,
        "course": 180.0,
        "params": {},
    }

    ev = adapter._build_event(payload)
    assert ev is not None
    adapter._persist([ev])
    adapter._after_persist(1)

    health_path = tmp_path / "galileosky_ingest_health.json"
    assert health_path.exists()
    data = json.loads(health_path.read_text(encoding="utf-8"))
    assert data["events"] >= 1
    assert "eps" in data

