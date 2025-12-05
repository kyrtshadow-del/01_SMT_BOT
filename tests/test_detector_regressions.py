import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from detector import (
    PreparedSample,
    SensorProfile,
    _get_sensor_liters,
    detect_short_drains,
)
from tests.event_samples import (
    CLG_EVENT_MAY26,
    CLG_EVENT_MAY27,
    JOHN_EVENT_SEP26_A,
    JOHN_EVENT_SEP26_B,
)


@pytest.mark.parametrize(
    "samples, expected_start, expected_end, expected_drop",
    [
        (CLG_EVENT_MAY26, 1748243789, 1748244311, 190.0),
        (CLG_EVENT_MAY27, 1748334184, 1748334456, 257.0),
        (JOHN_EVENT_SEP26_A, 1758913025, 1758913625, 32.0),
        (JOHN_EVENT_SEP26_B, 1758917428, 1758917728, 200.0),
    ],
)
def test_regression_drains_detected(samples, expected_start, expected_end, expected_drop):
    events = detect_short_drains({}, samples=samples)
    assert len(events) == 1
    event = events[0]
    assert event["start_ts"] == expected_start
    assert event["end_ts"] == expected_end
    assert event["total_drop_l"] == pytest.approx(expected_drop, rel=1e-6)


def test_negative_recomputed_values_are_discarded():
    profile = SensorProfile(key=123, sensor_id=123, param="dut1", name="ДУТ1")
    profile.converter = lambda raw: -200.0

    sample = PreparedSample(
        ts=1_700_000_000,
        speed=0.0,
        lat=None,
        lon=None,
        ignition=None,
        ignition_raw=None,
        acc_trigger=None,
        valid=None,
        sats=None,
        gsm_status=None,
        total_liters=None,
        liters_count=0,
        sensors={
            123: {
                "sensor_id": 123,
                "param": "dut1",
                "name": "ДУТ1",
                "raw": 37.0,
                "liters": None,
            }
        },
        distance_prev_m=0.0,
    )

    value = _get_sensor_liters(sample, profile)

    assert value is None
    assert sample.sensors[123].get("liters") is None
