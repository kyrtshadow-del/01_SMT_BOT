import time

from pipeline.engine import status as st


def _now() -> int:
    return int(time.time())


def test_health_down_marks_no_source():
    now = _now()
    res = st.compute_status({}, {"device_ts": now - 60}, health_ok=False, now=now)
    assert res["online"] is False
    assert res["reason"] == "no_source"
    assert "Нет связи с источником" in res["status_label"]


def test_fresh_data_ok():
    now = _now()
    res = st.compute_status({}, {"device_ts": now - 60, "speed": 0}, health_ok=True, now=now)
    assert res["online"] is True
    assert res["reason"] == "ok"
    assert "мин" in res["status_label"]


def test_offline_reason_no_connection():
    now = _now()
    # age deliberately больше динамического порога (>= ONLINE_MIN_SEC=300)
    res = st.compute_status({}, {"device_ts": now - 1800, "speed": 0}, health_ok=True, now=now)
    assert res["online"] is False
    assert res["reason"] == "no_connection"
    assert "Нет связи" in res["status_label"]


def test_stop_when_no_duration():
    now = _now()
    res = st.compute_status({}, {"device_ts": now - 60, "speed": 0}, health_ok=True, now=now)
    assert res["online"] is True
    assert res["status"] == "stop"


def test_no_timestamp_marks_no_data():
    res = st.compute_status({}, {}, health_ok=True, now=_now())
    assert res["online"] is False
    assert res["reason"] in ("no_data", "no_source")
    assert "Нет данных" in res["status_label"]
