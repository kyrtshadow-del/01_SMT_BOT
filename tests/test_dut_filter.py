import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from detector_dut_filter import (
    SensorMeta,
    explain_rejection,
    list_dut_candidates,
    select_primary_dut,
)


@pytest.fixture
def unit_item_multi():
    return {
        "sens": {
            "1": {"id": 1, "n": "ДУТ", "t": "fuel level", "last_value": 45.2},
            "2": {"id": 2, "n": "Температура", "t": "temperature", "last_value": 12.0},
            "3": {"id": 3, "n": "DUT2", "t": "custom", "last_value": 0.0, "p": "adc5"},
        },
        "lmsg": {"p": {"adc5": 0}},
    }


def test_candidates_and_primary(unit_item_multi):
    candidates = list_dut_candidates(unit_item_multi)
    assert {meta.id for meta in candidates} == {1, 3}
    primary = select_primary_dut(candidates)
    assert isinstance(primary, SensorMeta)
    assert primary.id == 1
    assert any(meta.is_primary for meta in candidates)
    zero = next(meta for meta in candidates if meta.id == 3)
    assert zero.is_zero_value is True
    assert zero.zero_reason is not None


def test_rejections(unit_item_multi):
    reasons = explain_rejection(unit_item_multi)
    reason_by_id = {r.id: r for r in reasons}
    assert 2 in reason_by_id
    assert reason_by_id[2].reason in {"regex_mismatch", "invalid_range"}


def test_alt_name_and_value_range():
    unit_item = {
        "sens": {
            "10": {"id": 10, "n": "Fuel level main", "t": "custom", "last_value": 123.4},
            "11": {"id": 11, "n": "Pressure", "t": "analog", "last_value": 5},
        }
    }
    candidates = list_dut_candidates(unit_item)
    assert [meta.id for meta in candidates] == [10]
    primary = select_primary_dut(candidates)
    assert primary.id == 10
    reasons = explain_rejection(unit_item)
    assert any(r.id == 11 for r in reasons)
