"""Stream adapter that accepts telemetry directly from Galileosky devices.

Design goals:
- Minimal working gateway to receive packets without Wialon.
- Defaults to HTTP JSON ingestion (one message or array per request).
- Can be extended to TCP/line-delimited protocol by adjusting `_start_server`.

Current assumptions (until протокол уточнён):
- Сообщение приходит в JSON и содержит хотя бы `uid` (IMEI/unique_id), `ts`
  (unix seconds), `lat`, `lon`. Дополнительно: `speed`, `course`, `params`.
- Аутентификация по заголовку `X-Api-Key` (если `auth_token` задан).

Поведение:
- Каждое валидное сообщение нормализуется в `Event` и кладётся в очередь.
- Метод `subscribe()` отдаёт события из очереди и пишет их в storage/latest.

Это «скелет», чтобы быстро подключить Galileosky: если формат отличается, нужно
поменять `_parse_message` и, при необходимости, протокол транспорта.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import contextlib
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Mapping, Optional, Sequence, Tuple

from pipeline.events import RawPacket
from pipeline.services.location_enricher import LocationEnricher
from pipeline.monitoring.health import HealthReporter
from pipeline.storage.raw_storage import RawStorage
from pipeline.storage.latest_metrics import LatestTelemetryStore
from pipeline.adapters.galileosky import proto
from pipeline.adapters.base import StreamAdapter, MetricsMixin
from logging.handlers import RotatingFileHandler

log = logging.getLogger(__name__)

# Keys we consider meaningful when scanning raw payloads byte-by-byte.
# Expanded with power/io states and RS-485 fuel/temperature channels so that
# even при битых TLV‑длинах мы всё равно вытаскиваем датчики.
KNOWN_TAG_KEYS = {
    "lat",
    "lon",
    "speed",
    "course",
    "nsat",
    "nav_src",
    "pwr_ext",
    "pwr_int",
    "mileage_m",
    "dev_status",
    "inputs_status",
    "outputs_status",
    "ibutton_code",
    "hdop",
}
KNOWN_TAG_KEYS.update({f"rs485_fuel{i}" for i in range(16)})
KNOWN_TAG_KEYS.update({f"rs485_t{i}" for i in range(16)})


@dataclass
class GalileoskyStreamAdapter(StreamAdapter, MetricsMixin):
    raw_storage: RawStorage
    latest_store: LatestTelemetryStore | None
    host: str = "0.0.0.0"
    port: int = 8088
    auth_token: str | None = None
    metrics_interval: int = 60
    location_enricher: LocationEnricher | None = None
    health_report_path: Path | None = None

    _queue: asyncio.Queue[RawPacket] = field(default_factory=asyncio.Queue, init=False, repr=False)
    _health_reporter: HealthReporter | None = field(default=None, init=False, repr=False)
    _server: asyncio.base_events.Server | None = field(default=None, init=False, repr=False)
    _last_metrics_ts: float = field(default_factory=time.perf_counter, init=False, repr=False)
    _metrics_count: int = field(default=0, init=False, repr=False)
    _last_uid: str | None = field(default=None, init=False, repr=False)
    _last_event_ts: int | None = field(default=None, init=False, repr=False)
    _synthetic_events: int = field(default=0, init=False, repr=False)
    _anomaly_events: int = field(default=0, init=False, repr=False)
    _max_speed: float = field(default=400.0, init=False, repr=False)

    def __post_init__(self) -> None:
        MetricsMixin.__init__(self, prom_path="logs/galileosky_ingest.prom")
        if self.health_report_path is None:
            self.health_report_path = Path("logs/galileosky_ingest_health.json")
        self._health_reporter = HealthReporter(self.health_report_path)
        self._setup_logging()
        self._load_dynamic_limits()

    def _setup_logging(self) -> None:
        # Separate rotating log for raw payload diagnostics
        log_path = Path("logs/galileosky_ingest.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler.setFormatter(formatter)
        ingest_logger = logging.getLogger("galileosky.ingest")
        ingest_logger.setLevel(logging.INFO)
        if not ingest_logger.handlers:
            ingest_logger.addHandler(handler)
        self._ingest_logger = ingest_logger

    def _load_dynamic_limits(self) -> None:
        import os

        try:
            self._max_speed = float(os.getenv("GALILEOSKY_MAX_SPEED", self._max_speed))
        except ValueError:
            # keep default on bad env
            self._max_speed = 400.0

    async def subscribe(self) -> AsyncIterator[RawPacket]:
        await self._start_server()
        log.info("galileosky: listener started on %s:%s", self.host, self.port)
        try:
            while True:
                event = await self._queue.get()
                yield event
                self._after_persist(1)
        except asyncio.CancelledError:
            log.info("galileosky: subscribe cancelled")
            raise
        finally:
            await self._stop_server()

    async def _start_server(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, host=self.host, port=self.port)

    async def _stop_server(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        try:
            context_imei: str | None = None
            # Read in a loop to keep TCP session open; some devices send handshake then data frames.
            while True:
                data = await reader.read(512 * 1024)  # 512KB cap per chunk
                if not data:
                    break
                # log raw snippet (hex + utf-8 preview)
                preview_hex = data[:64].hex()
                preview_txt = data[:200].decode(errors="ignore")
                self._ingest_logger.info(
                    "peer=%s bytes=%s hex=%s preview=%s", peer, len(data), preview_hex, preview_txt
                )
                binary_handled, responses, binary_replies, context_imei = self._process_binary(
                    data, context_imei
                )
                if binary_handled:
                    for reply in binary_replies:
                        writer.write(reply)
                    await writer.drain()
                    continue

                payload = data.decode(errors="ignore")
                responses = []
                for message in self._split_messages(payload):
                    ok, result = self._process_message(message)
                    responses.append(result if ok else {"error": result})
                writer.write(json.dumps(responses, ensure_ascii=False).encode())
                await writer.drain()
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("galileosky: client %s error: %s", peer, exc)
        finally:
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()

    def _split_messages(self, raw: str) -> List[str]:
        # Try JSON array first
        raw = raw.strip()
        if not raw:
            return []
        if raw.startswith("["):
            try:
                arr = json.loads(raw)
                if isinstance(arr, list):
                    return [json.dumps(item, ensure_ascii=False) for item in arr]
            except Exception:
                pass
        # Fallback: newline-delimited JSON or single JSON object
        if "\n" in raw:
            return [line for line in raw.splitlines() if line.strip()]
        return [raw]

    def _process_message(self, raw_message: str) -> tuple[bool, Mapping[str, Any]]:
        try:
            payload = json.loads(raw_message)
        except Exception as exc:
            return False, {"reason": "invalid_json", "detail": str(exc)}

        if self.auth_token:
            token = None
            if isinstance(payload, dict):
                token = payload.pop("auth", None) or payload.get("token")
            # If token missing or mismatched — reject
            if token != self.auth_token:
                return False, {"reason": "unauthorized"}

        events = self._parse_payload(payload)
        if not events:
            return False, {"reason": "no_events"}
        for event in events:
            self._queue.put_nowait(event)
        return True, {"accepted": len(events)}

    def _process_binary(
        self, data: bytes, context_imei: str | None = None
    ) -> tuple[bool, List[Mapping[str, Any]], List[bytes], str | None]:
        """Try to parse Galileosky binary frames; enqueue Events if found.

        Returns handled flag, responses, replies and cached IMEI (from handshake) to
        allow attaching it to subsequent data frames in the same TCP session.
        """
        responses: List[Mapping[str, Any]] = []
        binary_replies: List[bytes] = []
        handled = False

        # 1) ASCII protocol (#L#, #D#) – keep TCP session context IMEI.
        if data.startswith(b"#"):
            handled = True
            text = data.decode(errors="ignore")
            for line in text.splitlines():
                line = line.strip()
                if not line or not line.startswith("#"):
                    continue
                if line.startswith("#L#"):
                    parts = line[3:].split(";")
                    if parts and parts[0]:
                        context_imei = parts[0].strip()
                        self._last_uid = context_imei
                elif line.startswith("#D#"):
                    parts = line[3:].split(";")
                    if len(parts) < 8:
                        continue
                    date_s, time_s, lat_s, lat_h, lon_s, lon_h = parts[:6]
                    try:
                        lat_raw = float(lat_s)
                        lon_raw = float(lon_s)
                        lat = self._degmin_to_deg(lat_raw, lat_h)
                        lon = self._degmin_to_deg(lon_raw, lon_h)
                    except Exception:
                        continue
                    try:
                        device_ts = self._parse_dt(date_s, time_s)
                    except Exception:
                        device_ts = int(time.time())
                    speed_val = None
                    course_val = None
                    try:
                        speed_val = float(parts[6])
                    except Exception:
                        pass
                    try:
                        course_val = float(parts[7])
                    except Exception:
                        pass
                    tail_fields = parts[8:]
                    payload = {
                        "imei": context_imei,
                        "device_ts": device_ts,
                        "lat": lat,
                        "lon": lon,
                        "speed": speed_val,
                        "course": course_val,
                    }
                    params_tail: Dict[str, Any] = {}
                    if tail_fields:
                        tail_raw = ";".join(tail_fields)
                        # tokens like pwr_ext:2:12.706000 separated by comma
                        for token in tail_raw.replace(";", ",").split(","):
                            if ":" not in token:
                                continue
                            try:
                                key, _type, val = token.split(":", 2)
                            except ValueError:
                                continue
                            key = key.strip()
                            val = val.strip()
                            if not key or not val:
                                continue
                            if key.startswith("rs485"):
                                try:
                                    params_tail[key] = float(val)
                                except Exception:
                                    params_tail[key] = val
                                continue
                            if key in {"pwr_ext", "pwr_int", "dev_status", "acc_trigger", "inputs_status", "outputs_status", "firmware", "soft", "ignition"}:
                                try:
                                    params_tail[key] = float(val) if key.startswith("pwr_") else int(val)
                                except Exception:
                                    params_tail[key] = val
                    if params_tail:
                        payload["params"] = params_tail
                    ev = self._build_event(payload, fallback_uid=context_imei)
                    if ev:
                        self._queue.put_nowait(ev)
                        responses.append({"accepted": 1})
            return handled, responses, binary_replies, context_imei

        # 2) Binary TLV frames
        frames = proto.split_frames(data)
        if not frames:
            # log as potential binary but unparsable
            self._ingest_logger.info(
                "binary_no_frames len=%s hex=%s",
                len(data),
                data[:256].hex(),
            )
            return handled, responses, binary_replies, context_imei

        handled = True
        for frame in frames:
            parsed = proto.parse_frame(frame)
            parsed["crc_ok"] = frame.crc_ok
            parsed["packet_type"] = frame.type
            parsed["packet_counter"] = frame.counter
            # handshake: only IMEI
            if frame.type == 0x01 and "imei" in parsed:
                context_imei = parsed.get("imei") or context_imei
                responses.append({"handshake": parsed})
                # send ACK: type=0x01, len=1, cnt=same, payload=0x01
                ack_hdr = bytes([0x01, 0x01, frame.counter, 0x01])
                crc = struct.pack("<H", proto.crc16_modbus(ack_hdr))
                binary_replies.append(ack_hdr + crc)
                continue
            # data packet
            # If frame lacks IMEI, reuse one from the same TCP session
            if "imei" not in parsed and context_imei:
                parsed["imei"] = context_imei
            elif "imei" in parsed and not context_imei:
                context_imei = parsed["imei"]

            # enrich parsed dict with fixed-layout hints for 0x10 records
            fixed = self._decode_fixed_layout(frame.payload)
            parsed.update(fixed)
            ev = self._build_event(parsed, fallback_uid=context_imei, raw_hex=frame.payload.hex())
            if ev:
                self._queue.put_nowait(ev)
                # track last seen identity / timestamp for health snapshots
                uid_for_state = parsed.get("imei") or context_imei or parsed.get("uid") or parsed.get("id")
                if uid_for_state is not None:
                    self._last_uid = str(uid_for_state)
                self._last_event_ts = ev.device_ts
                responses.append({"accepted": 1})
            else:
                responses.append({"error": "no_event_built", "parsed": parsed})
        return handled, responses, binary_replies, context_imei

    @staticmethod
    def _degmin_to_deg(value: float, hemi: str) -> float:
        """Convert DDMM.MMMM + hemisphere to decimal degrees."""
        deg = int(value // 100)
        minutes = value - deg * 100
        dec = deg + minutes / 60.0
        if hemi.upper() in {"S", "W"}:
            dec = -dec
        return dec

    @staticmethod
    def _parse_dt(date_s: str, time_s: str) -> int:
        """Parse ddmmyy and hhmmss to unix timestamp (UTC)."""
        from datetime import datetime, timezone

        if len(date_s) != 6 or len(time_s) < 6:
            return int(time.time())
        day = int(date_s[:2])
        month = int(date_s[2:4])
        year = 2000 + int(date_s[4:6])
        hour = int(time_s[:2])
        minute = int(time_s[2:4])
        second = int(time_s[4:6])
        dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
        return int(dt.timestamp())

    def _parse_payload(self, payload: Any) -> List[RawPacket]:
        if isinstance(payload, list):
            events: List[RawPacket] = []
            for item in payload:
                ev = self._build_event(item)
                if ev:
                    events.append(ev)
            return events
        ev = self._build_event(payload)
        return [ev] if ev else []

    def _build_event(
        self,
        payload: Any,
        fallback_uid: str | None = None,
        raw_hex: str | None = None,
    ) -> Optional[Event]:
        if not isinstance(payload, dict):
            return None
        uid = payload.get("uid") or payload.get("imei") or payload.get("id") or fallback_uid
        ts = payload.get("ts") or payload.get("time") or payload.get("device_ts") or payload.get("device_ts_fixed")
        if ts is None:
            device_ts = int(time.time())
        else:
            try:
                device_ts = int(ts)
            except Exception:
                device_ts = int(time.time())
        lat = self._safe_float(payload.get("lat") or payload.get("y"))
        lon = self._safe_float(payload.get("lon") or payload.get("x"))
        speed = self._safe_float(payload.get("speed") or payload.get("s"))
        course = self._safe_float(payload.get("course") or payload.get("c"))
        if speed is None and payload.get("speed_fixed") is not None:
            speed = float(payload["speed_fixed"])
        if course is None and payload.get("course_fixed") is not None:
            course = float(payload["course_fixed"])
        params_raw = payload.get("params") or payload.get("p") or {}
        params: Dict[str, Any] = params_raw if isinstance(params_raw, dict) else {}
        if payload.get("altitude_fixed") is not None:
            params.setdefault("altitude_m", payload["altitude_fixed"])

        # Heuristic: scan raw bytes for embedded TLV tags even если основная
        # TLV‑структура битая. Это позволяет достать nav и RS‑485 датчики.
        if raw_hex:
            try:
                raw_bytes = bytes.fromhex(raw_hex)
                extracted = self._extract_known_tags(raw_bytes)
                lat = lat or extracted.get("lat")
                lon = lon or extracted.get("lon")
                speed = speed or extracted.get("speed")
                course = course or extracted.get("course")
                for k, v in extracted.items():
                    if k in {"lat", "lon", "speed", "course"}:
                        continue
                    params.setdefault(k, v)
            except Exception:
                pass

        # merge all other parsed keys into params
        for k, v in payload.items():
            # Skip identifiers/coords and also the nested params themselves to avoid self‑references.
            if k in {
                "uid",
                "imei",
                "id",
                "ts",
                "time",
                "device_ts",
                "lat",
                "lon",
                "y",
                "x",
                "speed",
                "s",
                "course",
                "c",
                "params",
                "p",
            }:
                continue
            params.setdefault(k, v)
        if raw_hex:
            params.setdefault("raw_hex", raw_hex)

        # Fallback: if uid is missing but imei is present in params, use it
        if uid is None and "imei" in params:
            uid = params["imei"]
        if uid is None:
            log.debug("galileosky: skip message (no uid/imei)")
            return None

        # Track last seen UID / ts for health snapshots
        uid_for_state = params.get("imei") or uid
        if uid_for_state is not None:
            self._last_uid = str(uid_for_state)
        self._last_event_ts = device_ts

        # Enrich location (address/geofences) if we have coords
        if lat is not None and lon is not None and self.location_enricher:
            try:
                enriched = self.location_enricher.enrich(lat, lon)
                if enriched.get("address"):
                    params.setdefault("address", enriched["address"])
                if enriched.get("geofences"):
                    params.setdefault("geofences", enriched["geofences"])
            except Exception:
                pass

        # Lightweight anomaly scoring (no hard filtering, только пометка события)
        reasons: List[str] = []
        score = 0
        if speed is not None and speed > self._max_speed:
            score += 3
            reasons.append("speed_over_limit")
        elif speed is not None and speed > 150:
            score += 1
            reasons.append("speed_high")
        hdop_val = params.get("hdop")
        if isinstance(hdop_val, (int, float)) and hdop_val > 5:
            score += 1
            reasons.append("hdop_high")
        sat_val = params.get("satellites") or params.get("nsat")
        if isinstance(sat_val, (int, float)) and sat_val < 3:
            score += 1
            reasons.append("low_sats")
        if score > 0:
            params["anomaly_score"] = score
            params["anomaly_flags"] = reasons
            self._anomaly_events += 1
            self.incr_metric("galileosky_anomaly_events_total", 1)

        if self.location_enricher and lat is not None and lon is not None:
            enrichment = self.location_enricher.enrich(float(lat), float(lon), existing_address=params.get("address"))
            if enrichment.get("address"):
                params.setdefault("address", enrichment["address"])
            if enrichment.get("geofences"):
                params.setdefault("geofences", enrichment["geofences"])

        return RawPacket(
            protocol="galileosky",
            uid=str(uid),
            device_ts=device_ts,
            received_ts=int(payload.get("received_ts") or time.time()),
            latitude=lat,
            longitude=lon,
            speed=speed,
            course=course,
            params=params,
            raw_payload=payload,
            ip=payload.get("ip"),
        )

    def _persist(self, events: List[RawPacket]) -> None:
        # Legacy hook (not used in RawPacket mode)
        return

    def _after_persist(self, count: int) -> None:
        self._metrics_count += count
        now = time.perf_counter()
        if now - self._last_metrics_ts >= self.metrics_interval:
            eps = self._metrics_count / max(1.0, now - self._last_metrics_ts)
            snapshot = {
                "ts": int(time.time()),
                "events": self._metrics_count,
                "eps": round(eps, 2),
                "last_uid": self._last_uid,
                "last_event_ts": self._last_event_ts,
                "synthetic_events": self._synthetic_events,
                "anomaly_events": self._anomaly_events,
            }
            reporter = self._health_reporter
            if reporter:
                reporter.report(snapshot)
            # Export Prometheus-style counters (textfile)
            current_total = self.metrics.get("galileosky_events_total", 0)
            self.set_metric("galileosky_events_total", current_total + self._metrics_count)
            self.set_metric("galileosky_eps", eps)
            syn_total = self.metrics.get("galileosky_synthetic_total", 0)
            self.set_metric("galileosky_synthetic_total", syn_total + self._synthetic_events)
            anom_total = self.metrics.get("galileosky_anomaly_total", 0)
            self.set_metric("galileosky_anomaly_total", anom_total + self._anomaly_events)
            self._metrics_count = 0
            self._last_metrics_ts = now
            self._synthetic_events = 0
            self._anomaly_events = 0

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            f = float(value)
            if f != f:  # NaN check
                return None
            return f
        except Exception:
            return None

    @staticmethod
    def _day_key(ts: int) -> str:
        return time.strftime("%Y-%m-%d", time.gmtime(ts))

    @staticmethod
    def _extract_known_tags(raw: bytes) -> Dict[str, Any]:
        """Scan raw payload for embedded TLV tags and decode known ones."""
        out: Dict[str, Any] = {}
        i = 0
        n = len(raw)
        while i + 2 <= n:
            tag = raw[i]
            ln = raw[i + 1]
            if ln == 0 or i + 2 + ln > n:
                i += 1
                continue
            slice_ = raw[i + 2 : i + 2 + ln]
            decoded = proto.decode_tag(tag, slice_)
            # store only known keys to avoid noise
            for k, v in decoded.items():
                if k in KNOWN_TAG_KEYS:
                    out[k] = v
            i += 2 + ln
        return out

    @staticmethod
    def _decode_fixed_layout(payload: bytes) -> Dict[str, Any]:
        """Best-effort decoder for frames that are not TLV but fixed layout (observed 26-byte records).

        Layout (hypothesis):
        - [0]   msg id (0x10)
        - [4:8] device_ts (LE, unix seconds)
        - [16:18] speed_raw (uint16, tenths of km/h)
        - [18:20] course_raw (uint16, tenths of degree)
        - pattern byte 0x33 may precede speed/course, 0x34 altitude
        """
        res: Dict[str, Any] = {}
        if len(payload) >= 8 and payload[0] == 0x10:
            res["device_ts_fixed"] = int.from_bytes(payload[4:8], "little", signed=False)
            # search for 0x33 marker
            try:
                idx = payload.index(0x33)
                if idx + 4 < len(payload):
                    sp = int.from_bytes(payload[idx + 1 : idx + 3], "little", signed=False)
                    crs = int.from_bytes(payload[idx + 3 : idx + 5], "little", signed=False)
                    res["speed_fixed"] = sp / 10.0
                    res["course_fixed"] = crs / 10.0
                    # altitude tag 0x34 may follow
                    if idx + 5 < len(payload) and payload[idx + 5] == 0x34 and idx + 7 < len(payload):
                        alt = int.from_bytes(payload[idx + 6 : idx + 8], "little", signed=True)
                        res["altitude_fixed"] = alt
            except ValueError:
                pass
            # navigation block variant: 0x30 <len=12> <lat><lon><speed><course>
            if len(payload) >= 22 and payload[8] == 0x30 and payload[9] >= 8:
                nav_len = payload[9]
                val_start = 10  # points to start of nav value
                end = val_start + nav_len
                if end <= len(payload):
                    # Observed layout: lat (4) + lon (4) + [nsat?] (+ speed/course if len>=12)
                    if nav_len >= 8:
                        lat = int.from_bytes(payload[val_start : val_start + 4], "little", signed=True) / 1_000_000
                        lon = int.from_bytes(payload[val_start + 4 : val_start + 8], "little", signed=True) / 1_000_000
                        res.setdefault("lat", lat)
                        res.setdefault("lon", lon)
                    if nav_len >= 12:
                        sp_raw = int.from_bytes(payload[val_start + 8 : val_start + 10], "little", signed=False)
                        cr_raw = int.from_bytes(payload[val_start + 10 : val_start + 12], "little", signed=False)
                        nav_speed = sp_raw / 10.0
                        nav_course = cr_raw / 10.0
                        if res.get("speed_fixed") is None or res.get("speed_fixed") == 0:
                            res["speed_fixed"] = nav_speed
                        if res.get("course_fixed") is None or res.get("course_fixed") == 0:
                            res["course_fixed"] = nav_course
        return res

    @staticmethod
    def _is_imei_ping(data: bytes) -> bool:
        """
        Heuristic for short 21-byte packets we see:
        01 10 00 03 <IMEI ASCII...>
        """
        return (
            isinstance(data, (bytes, bytearray))
            and len(data) == 21
            and data.startswith(b"\x01\x10\x00\x03")
            and all(48 <= b <= 57 for b in data[4:])  # ASCII digits
        )
