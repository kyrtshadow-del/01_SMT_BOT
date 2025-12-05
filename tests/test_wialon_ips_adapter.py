import pytest

from pipeline.adapters.wialon_ips.stream import WialonIPSStreamAdapter
from pipeline.storage.raw_storage import RawStorage
from pathlib import Path


def make_adapter():
    # dummy storage; we won't write in tests
    return WialonIPSStreamAdapter(raw_storage=RawStorage(Path("/tmp")), latest_store=None)


def test_coord_parsing_ddmm_to_decimal():
    ad = make_adapter()
    lat = ad._parse_coord("5207.7237", "N")
    lon = ad._parse_coord("04205.7684", "E")
    assert pytest.approx(lat, rel=1e-6) == 52.1287283
    assert pytest.approx(lon, rel=1e-6) == 42.09614


def test_tail_params_parsing():
    ad = make_adapter()
    body = (
        "221125;091246;5207.7237;N;04205.7684;E;0;349;181;12;0.6;NA;973144064;"
        "0.264000,0.000000;NA;"
        "gsm_status:1:3,acc_trigger:1:1,dev_status:1:14849,pwr_ext:2:12.238000,"
        "pwr_int:2:3.718000,rs485_fls02:2:0.000000,rs485_fls12:2:3970.000000,"
        "rs485_fls22:2:3773.000000,rs485ex_0_lvl:1:3996,rs485ex_0_tmp:1:28"
    )
    ev = ad._parse_data(body, "864495031920561")
    assert ev is not None
    p = ev.params
    assert p["pwr_ext"] == pytest.approx(12.238)
    assert p["rs485_fls12"] == pytest.approx(3970.0)
    assert p["rs485ex_0_tmp"] == 28
    # derived flags
    assert p["gsm_level"] == 3
    assert p["gsm_roaming"] is False
    assert p["dev_status_flags"]["ignition"] in (True, False)  # presence check

