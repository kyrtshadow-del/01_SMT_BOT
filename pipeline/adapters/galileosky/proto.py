"""Minimal Galileosky binary protocol parser (TCP gateway side).

This implements only the parts we need to ingest live telemetry:
- Frame layout: [type:1][len:1][cnt:1][payload:len][crc16:2]
- CRC16: Modbus/IBM (poly 0xA001, init 0xFFFF, LE in frame)
- Payload is TLV: [tag_id:1][tag_len:1][tag_value...]*

Supported tags (subset, enough for cards and fuel):
  0x03 imei            str
  0x02 firmware        int
  0x10 msg_id          int
  0x20 time            uint32 (unix)
  0x30 navigation      nsat+source, lat/lon *1e-6
  0x33 velocity        speed/course /10
  0x34 height          int16
  0x35 hdop            /10
  0x40 status          int
  0x41 voltage_ext     mV -> V
  0x42 voltage_batt    mV -> V
  0x45 outputs_status  int
  0x46 inputs_status   int
  0x60-0x6F rs485_fuel uint16 (liters, raw)
  0x8A-0x8F rs485_t    int8  (°C)
  0x90 ibutton1        uint32
  0xD4 mileage         uint32 (meters)

Everything unknown is returned as hex in "unknown_tags".
Handshakes (type=0x01) are surfaced as {'imei': ...} for logging, but
do not create Events upstream.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional


def crc16_modbus(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


@dataclass
class Frame:
    type: int
    counter: int
    payload: bytes
    crc_ok: bool


def split_frames(buffer: bytes) -> List[Frame]:
    frames: List[Frame] = []
    i = 0
    n = len(buffer)
    while i + 4 <= n:  # minimum: type len cnt crc
        ptype = buffer[i]
        length = buffer[i + 1]
        cnt = buffer[i + 2]
        total = 1 + 1 + 1 + length + 2
        if i + total > n:
            break
        payload = buffer[i + 3 : i + 3 + length]
        crc_frame = buffer[i + total - 2 : i + total]
        crc_calc = crc16_modbus(buffer[i : i + 3 + length])
        crc_ok = crc_calc == struct.unpack('<H', crc_frame)[0]
        frames.append(Frame(ptype, cnt, payload, crc_ok))
        i += total
    return frames


# ---- Tag decoders ----

def _u8(b: bytes) -> int: return struct.unpack('<B', b)[0]
def _i8(b: bytes) -> int: return struct.unpack('<b', b)[0]
def _u16(b: bytes) -> int: return struct.unpack('<H', b)[0]
def _u32(b: bytes) -> int: return struct.unpack('<I', b)[0]
def _i16(b: bytes) -> int: return struct.unpack('<h', b)[0]
def _i32(b: bytes) -> int: return struct.unpack('<i', b)[0]


def decode_tag(tag: int, val: bytes) -> Dict[str, object]:
    l = len(val)
    if tag == 0x03 and l == 15:
        return {'imei': val.decode(errors='ignore')}
    if tag == 0x02 and l >= 1:
        return {'firmware': _u8(val[:1])}
    if tag == 0x10 and l == 2:
        return {'msg_id': _u16(val)}
    if tag == 0x20 and l == 4:
        return {'device_ts': _u32(val)}
    if tag == 0x30 and l == 9:
        nsat_src, lat, lon = struct.unpack('<Bii', val)
        return {
            'nsat': nsat_src & 0x0F,
            'nav_src': (nsat_src & 0xF0) >> 4,
            'lat': lat / 1_000_000,
            'lon': lon / 1_000_000,
        }
    if tag == 0x30 and l == 12:
        # Some Galileosky packets embed lat/lon/speed/course without nsat/nav_src.
        lat, lon, speed, course = struct.unpack('<iiHH', val)
        return {
            'lat': lat / 1_000_000,
            'lon': lon / 1_000_000,
            'speed': speed / 10,
            'course': course / 10,
        }
    if tag == 0x33 and l == 4:
        speed, course = struct.unpack('<HH', val)
        return {'speed': speed / 10, 'course': course / 10}
    if tag == 0x34 and l == 2:
        return {'alt': _i16(val)}
    if tag == 0x35 and l == 1:
        return {'hdop': _u8(val) / 10}
    if tag == 0x40 and l == 2:
        return {'dev_status': _u16(val)}
    if tag == 0x41 and l == 2:
        return {'pwr_ext': _u16(val) / 1000}
    if tag == 0x42 and l == 2:
        return {'pwr_int': _u16(val) / 1000}
    if tag == 0x45 and l == 2:
        return {'outputs_status': _u16(val)}
    if tag == 0x46 and l == 2:
        return {'inputs_status': _u16(val)}
    if 0x60 <= tag <= 0x6F and l == 2:
        idx = tag - 0x60
        return {f'rs485_fuel{idx}': _u16(val)}
    if 0x8A <= tag <= 0x8F and l == 1:
        idx = tag - 0x8A
        return {f'rs485_t{idx}': _i8(val)}
    if tag == 0x90 and l == 4:
        return {'ibutton_code': _u32(val)}
    if tag == 0xD4 and l == 4:
        return {'mileage_m': _u32(val)}
    return {f'unknown_0x{tag:02X}': val.hex()}


def parse_tlv(payload: bytes) -> Dict[str, object]:
    res: Dict[str, object] = {}
    i = 0
    n = len(payload)
    while i + 2 <= n:
        tag = payload[i]
        ln = payload[i + 1]
        if ln == 0 or i + 2 + ln > n:  # corrupt/garbage length — try to resync
            i += 1
            continue
        val = payload[i + 2 : i + 2 + ln]
        res.update(decode_tag(tag, val))
        i += 2 + ln
    return res


def parse_frame(frame: Frame) -> Dict[str, object]:
    # returns dict of parsed tags; special-case handshake (type 0x01)
    if frame.type == 0x01 and frame.payload and frame.payload[0] == 0x03:
        # handshake format: [0x03][15 bytes IMEI], length usually 0x10
        if len(frame.payload) >= 16:
            return {'imei': frame.payload[1:16].decode(errors='ignore')}
    return parse_tlv(frame.payload)
