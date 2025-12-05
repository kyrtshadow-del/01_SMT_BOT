import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from detector import detect_short_drains


def _make_sample(
    ts: int,
    sensor_values,
    *,
    speed: float = 0.0,
    ignition: bool = False,
) -> dict:
    sensors = {}
    total = 0.0
    for idx, value in enumerate(sensor_values, start=1):
        sensors[idx] = {
            "sensor_id": idx,
            "param": f"dut{idx}",
            "name": f"dut{idx}",
            "liters": float(value),
        }
        total += float(value)
    return {
        "ts": ts,
        "lat": 55.0,
        "lon": 37.0,
        "speed": float(speed),
        "ignition": ignition,
        "ignition_raw": int(ignition),
        "valid": True,
        "sats": 12,
        "total_liters": total,
        "liters_count": len(sensor_values),
        "sensors": sensors,
    }


def test_detects_sparse_drain_between_two_messages():
    samples = [
        _make_sample(0, (300.0, 300.0), speed=0.0),
        _make_sample(300, (270.0, 270.0)),
    ]
    events = detect_short_drains({}, samples=samples)
    assert len(events) == 1
    event = events[0]
    assert event["samples"] == 2
    assert pytest.approx(event["total_drop_l"], rel=1e-9) == 60.0
    assert pytest.approx(event["rate_lpm"], rel=1e-9) == 12.0
    per_sensor = {drop["name"]: drop["drop_l"] for drop in event["per_sensor"]}
    assert pytest.approx(per_sensor["dut1"], rel=1e-9) == 30.0
    assert pytest.approx(per_sensor["dut2"], rel=1e-9) == 30.0


def test_ignition_on_with_data_gap_is_rejected():
    samples = [
        _make_sample(0, (300.0, 300.0), ignition=True),
        _make_sample(300, (270.0, 270.0), ignition=True),
    ]
    assert detect_short_drains({}, samples=samples) == []


def test_gap_beyond_stationary_limit_not_joined():
    samples = [
        _make_sample(0, (300.0, 300.0), speed=3.0),
        _make_sample(900, (240.0, 240.0)),
    ]
    assert detect_short_drains({}, samples=samples) == []


def test_starting_with_movement_is_rejected():
    samples = [
        _make_sample(0, (300.0, 300.0), speed=4.0),
        _make_sample(180, (260.0, 260.0)),
    ]
    assert detect_short_drains({}, samples=samples) == []


def test_small_tank_drop_passes_reduced_threshold():
    samples = [
        _make_sample(0, (120.0,), speed=0.0),
        _make_sample(60, (118.5,), speed=0.0),
        _make_sample(180, (113.5,), speed=0.0),
    ]
    events = detect_short_drains({}, samples=samples)
    assert len(events) == 1
    assert events[0]["total_drop_l"] == pytest.approx(6.5, rel=1e-6)


def test_large_total_drop_respects_sensor_scale():
    samples = [
        _make_sample(0, (260.0, 260.0, 260.0), speed=0.0),
        _make_sample(120, (250.0, 250.0, 260.0), speed=0.0),
    ]
    events = detect_short_drains({}, samples=samples)
    assert len(events) == 1
    event = events[0]
    assert event["total_drop_l"] == pytest.approx(20.0, rel=1e-9)
    per_sensor = {drop["name"]: drop["drop_l"] for drop in event["per_sensor"]}
    assert per_sensor["dut1"] == pytest.approx(10.0, rel=1e-9)
    assert per_sensor["dut2"] == pytest.approx(10.0, rel=1e-9)
