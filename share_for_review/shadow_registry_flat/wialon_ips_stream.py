"""Stream adapter for Wialon IPS retranslator (TCP).

This ingests plain Wialon IPS packets (`#L#`, `#D#`, `#SD#`), normalises them
to `Event` and writes to RawStorage/latest_metrics.

Supported/assumed format (common in the field):
    #SD#DDMMYY;HHMMSS;lat;lon;speed;course;height;sats;hdop;inputs;outputs;adc1;adc2;adc3;odometer;ibutton;params...<CR><LF>

The parser is tolerant: it uses only the fields that parse successfully and
keeps the rest in `params`. Login packet `#L#imei;password` is optional; if it's
present we cache IMEI per TCP session and use it as `uid`.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import contextlib
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Mapping, Optional

from pipeline.events import Event, RawPacket
from pipeline.services.location_enricher import LocationEnricher
from pipeline.monitoring.health import HealthReporter
from pipeline.adapters.base import StreamAdapter, MetricsMixin
from logging.handlers import RotatingFileHandler

log = logging.getLogger(__name__)


@dataclass
class WialonIPSStreamAdapter(StreamAdapter, MetricsMixin):
    raw_storage: object  # kept for compatibility, not used in raw mode
    latest_store: object | None  # kept for compatibility, not used in raw mode
    host: str = "0.0.0.0"
    port: int = 18081
    password: str | None = None
    metrics_interval: int = 60
    location_enricher: LocationEnricher | None = None
    health_report_path: Path | None = None

    _queue: asyncio.Queue[Event] = field(default_factory=asyncio.Queue, init=False, repr=False)
    _health_reporter: HealthReporter | None = field(default=None, init=False, repr=False)
    _server: asyncio.base_events.Server | None = field(default=None, init=False, repr=False)
    _last_metrics_ts: float = field(default=0.0, init=False, repr=False)
    _metrics_count: int = field(default=0, init=False, repr=False)
    _last_uid: str | None = field(default=None, init=False, repr=False)
    _last_event_ts: int | None = field(default=None, init=False, repr=False)
    _synthetic_events: int = field(default=0, init=False, repr=False)
    _anomaly_events: int = field(default=0, init=False, repr=False)
    _prom_path: Path | None = field(default=None, init=False, repr=False)
    _max_speed: float = field(default=400.0, init=False, repr=False)
    _max_future_sec: int = field(default=7200, init=False, repr=False)  # 2h guard after timezone normalization
    _max_age_sec: int = field(default=86400 * 30, init=False, repr=False)
    _storage_root: Path = field(default_factory=lambda: Path("data/pipeline_storage"), init=False, repr=False)
    _tz_warned_uids: set[str] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        MetricsMixin.__init__(self, prom_path="logs/wialon_ips_ingest.prom")
        if self.health_report_path is None:
            self.health_report_path = Path("logs/wialon_ips_ingest_health.json")
        self._health_reporter = HealthReporter(self.health_report_path)
        self._setup_logging()
        self._prom_path = Path("logs/wialon_ips_ingest.prom")
        self._load_dynamic_limits()
        raw_root = getattr(self.raw_storage, "root", None)
        if raw_root:
            self._storage_root = Path(raw_root)

    def _load_dynamic_limits(self) -> None:
        import os
        self._max_speed = float(os.getenv("IPS_MAX_SPEED", self._max_speed))
        self._max_future_sec = int(os.getenv("IPS_MAX_FUTURE_SEC", self._max_future_sec))
        self._max_age_sec = int(os.getenv("IPS_MAX_AGE_SEC", self._max_age_sec))

    async def subscribe(self) -> AsyncIterator[Event]:
        await self._start_server()
        log.info("wialon-ips: listener started on %s:%s", self.host, self.port)
        try:
            while True:
                event = await self._queue.get()
                yield event
                self._after_emit(1)
        except asyncio.CancelledError:
            log.info("wialon-ips: subscribe cancelled")
            raise
        finally:
            await self._stop_server()

    def _setup_logging(self) -> None:
        log_path = Path("logs/wialon_ips_ingest.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler.setFormatter(formatter)
        ingest_logger = logging.getLogger("wialon_ips.ingest")
        ingest_logger.setLevel(logging.INFO)
        if not ingest_logger.handlers:
            ingest_logger.addHandler(handler)
        self._ingest_logger = ingest_logger

    async def _start_server(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, host=self.host, port=self.port)

    async def _stop_server(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        peer_ip = peer[0] if isinstance(peer, tuple) and peer else None
        context_uid: str | None = None
        buffer = ""
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                buffer += data.decode(errors="ignore")
                # Messages are typically separated by CR/LF
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    self._ingest_logger.info("peer=%s line=%s", peer, line)
                    reply, context_uid = self._process_line(line, context_uid, peer_ip)
                    if reply:
                        writer.write(reply.encode())
                        await writer.drain()
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("wialon-ips: client %s error: %s", peer, exc)
        finally:
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()

    def _process_line(self, line: str, context_uid: str | None, peer_ip: str | None) -> tuple[str, str | None]:
        # Login
        if line.startswith("#L#"):
            parts = line[3:].split(";")
            uid = parts[0] if parts else None
            pwd = parts[1] if len(parts) > 1 else None
            if self.password and pwd != self.password:
                return "AL#\r\n", context_uid
            context_uid = uid
            return "OK\r\n", context_uid

        if line.startswith("#P#"):
            # Ping packet, just acknowledge
            return "OK\r\n", context_uid

        if line.startswith("#D#") or line.startswith("#SD#"):
            body = line[3:] if line.startswith("#D#") else line[4:]
            event = self._parse_data(body, context_uid, peer_ip)
            if event:
                self._queue.put_nowait(event)
                self._last_uid = context_uid or event.params.get("imei")
                self._last_event_ts = event.device_ts
            return "OK\r\n", context_uid

        # Unknown packet: acknowledge to keep device happy
        return "OK\r\n", context_uid

    def _parse_data(self, body: str, context_uid: str | None, peer_ip: str | None = None) -> Optional[Event]:
        tokens = [t for t in body.split(";") if t != ""]
        if len(tokens) < 4:
            return None
        try:
            date_s, time_s = tokens[0], tokens[1]
        except Exception:
            return None

        # Coordinates in Wialon IPS: DDMM.MMMM + hemisphere
        lat = self._parse_coord(tokens[2] if len(tokens) > 2 else None, tokens[3] if len(tokens) > 3 else None)
        lon = self._parse_coord(tokens[4] if len(tokens) > 4 else None, tokens[5] if len(tokens) > 5 else None)
        if lat is not None and (abs(lat) > 90 or abs(lat) < 0.0001):
            lat = None
        if lon is not None and (abs(lon) > 180 or abs(lon) < 0.0001):
            lon = None

        device_ts = self._parse_ts(date_s, time_s)
        speed = self._safe_float(tokens[6]) if len(tokens) > 6 else None
        course = self._safe_float(tokens[7]) if len(tokens) > 7 else None
        altitude = self._safe_float(tokens[8]) if len(tokens) > 8 else None
        sats = self._safe_int(tokens[9]) if len(tokens) > 9 else None

        params: Dict[str, Any] = {}
        if altitude is not None:
            params["altitude_m"] = altitude
        if sats is not None:
            params["satellites"] = sats
        if len(tokens) > 10:
            params["hdop"] = self._safe_float(tokens[10])
        if len(tokens) > 11:
            val = self._safe_int(tokens[11])
            if val is not None:
                params["inputs_status"] = val
        # outputs / odometer block
        if len(tokens) > 12:
            out_val = self._safe_int(tokens[12])
            if out_val is not None:
                if out_val <= 0xFFFF:
                    params["outputs_status"] = out_val
                else:
                    params["odometer_m"] = out_val
        # ADC / analog list (comma-separated)
        if len(tokens) > 13 and tokens[13]:
            analog = tokens[13].split(",")
            for idx, av in enumerate(analog, start=1):
                cast = self._try_cast(av)
                if cast is None or cast == "NA":
                    continue
                params[f"adc{idx}"] = cast
        # ibutton / driver id
        if len(tokens) > 14 and tokens[14] not in {"", "NA"}:
            params["ibutton_code"] = self._try_cast(tokens[14])
        # tail params key:type:value or key=value; also flatten legacy lists separated by commas
        for part in tokens[15:]:
            if ":" in part:
                pieces = part.split(",")
                for p in pieces:
                    if ":" in p:
                        key, *rest = p.split(":")
                        val = rest[-1] if rest else ""
                        if key:
                            casted = self._try_cast(val)
                            params[key] = casted
                            # simple bit/flag decoding hooks
                            if key == "dev_status" and isinstance(casted, int):
                                params["dev_status_flags"] = {
                                    "ignition": bool(casted & 0x1),
                                    "gps_ok": bool(casted & 0x2),
                                    "accel_alarm": bool(casted & 0x100),
                                }
                            if key == "gsm_status" and isinstance(casted, int):
                                params["gsm_level"] = casted & 0x0F
                                params["gsm_roaming"] = bool(casted & 0x10)
            elif "=" in part:
                key, val = part.split("=", 1)
                key = key.strip()
                if key:
                    params[key] = self._try_cast(val.strip())
            elif "," in part:
                # fallback: comma list of numbers -> adc tail
                for idx, av in enumerate(part.split(","), start=1):
                    cast = self._try_cast(av)
                    if cast is None or cast == "NA":
                        continue
                    params[f"adc_tail{idx}"] = cast

        # Anomaly scoring
        reasons = []
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
        sat_val = params.get("satellites")
        if isinstance(sat_val, (int, float)) and sat_val < 3:
            score += 1
            reasons.append("low_sats")
        if score > 0:
            params["anomaly_score"] = score
            params["anomaly_flags"] = reasons
            self._anomaly_events += 1
            self.incr_metric("wialon_ips_anomaly_events_total", 1)

        uid = params.get("imei") or context_uid
        if not uid:
            return None

        # keep original device ts for audit/logging
        raw_device_ts = device_ts
        if device_ts:
            device_ts, tz_offset = self._normalize_timestamp(device_ts, uid)
            if tz_offset:
                params.setdefault("tz_offset_applied", tz_offset)
                params.setdefault("raw_device_ts", raw_device_ts)

        unit_id = self._synthetic_unit_id(str(uid))
        params.setdefault("synthetic_unit", True)
        params.setdefault("imei", uid)

        # Anomaly filters: drop obviously bad timestamps or speeds
        now_ts = int(dt.datetime.utcnow().timestamp())
        if device_ts:
            if device_ts > now_ts + self._max_future_sec:  # future
                self._ingest_logger.warning(
                    "drop: ts_future device_ts=%s now=%s max_future=%s uid=%s",
                    device_ts,
                    now_ts,
                    self._max_future_sec,
                    uid,
                )
                return None
            if device_ts < now_ts - self._max_age_sec:  # too old
                self._ingest_logger.warning(
                    "drop: ts_old device_ts=%s now=%s max_age=%s uid=%s",
                    device_ts,
                    now_ts,
                    self._max_age_sec,
                    uid,
                )
                return None
        if speed is not None and speed > self._max_speed * 1.5:  # unrealistically high
            self._ingest_logger.warning(
                "drop: speed_unreal speed=%s max=%s uid=%s",
                speed,
                self._max_speed,
                uid,
            )
            return None

        if self.location_enricher and lat is not None and lon is not None:
            enrichment = self.location_enricher.enrich(float(lat), float(lon), existing_address=params.get("address"))
            if enrichment.get("address"):
                params.setdefault("address", enrichment["address"])
            if enrichment.get("geofences"):
                params.setdefault("geofences", enrichment["geofences"])

        # increment event counter for prometheus textfile
        self.incr_metric("wialon_ips_events_total", 1)

        return Event(
            unit_id=unit_id,
            device_ts=device_ts or int(dt.datetime.utcnow().timestamp()),
            received_ts=int(dt.datetime.utcnow().timestamp()),
            latitude=lat,
            longitude=lon,
            speed=speed,
            course=course,
            params=params,
            source="wialon_ips",
            raw_payload={
                "raw": body,
                "tokens": tokens,
                "uid": uid,
                "ip": peer_ip,
                "raw_device_ts": raw_device_ts,
            },
        )

    def _normalize_timestamp(self, device_ts: int, uid: str | None) -> tuple[int, int]:
        """Shift device_ts only when it is in the future due to timezone misconfig.

        Returns (normalized_ts, applied_offset_seconds). Past timestamps are not altered
        to preserve buffered history from black boxes.
        """

        now_ts = int(dt.datetime.utcnow().timestamp())
        diff = device_ts - now_ts

        # trust past and near-present data (incl. small clock drift)
        if diff < 900:  # 15 minutes
            return device_ts, 0

        timezone_step = 1800  # 30 minutes
        tolerance = 300  # 5 minutes tolerance around step
        remainder = diff % timezone_step
        if remainder < tolerance or remainder > (timezone_step - tolerance):
            rounded_offset = int(round(diff / timezone_step) * timezone_step)
            corrected_ts = device_ts - rounded_offset
            if uid and uid not in self._tz_warned_uids:
                self._ingest_logger.warning(
                    "ts_normalize: uid=%s diff=%s offset=%s device_ts=%s corrected=%s",
                    uid,
                    diff,
                    rounded_offset,
                    device_ts,
                    corrected_ts,
                )
                self._tz_warned_uids.add(uid)
            return corrected_ts, rounded_offset

        # leave as-is; downstream future guard may drop obviously bogus timestamps
        return device_ts, 0

    def _after_emit(self, count: int) -> None:
        self._metrics_count += count
        now = asyncio.get_event_loop().time()
        if now - self._last_metrics_ts >= self.metrics_interval:
            eps = self._metrics_count / max(1.0, now - self._last_metrics_ts)
            snapshot = {
                "ts": int(dt.datetime.utcnow().timestamp()),
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
            # Export Prometheus-style counters
            self.set_metric("wialon_ips_events_total", self.metrics.get("wialon_ips_events_total", 0) + self._metrics_count)
            self.set_metric("wialon_ips_eps", eps)
            self.set_metric("wialon_ips_synthetic_total", self.metrics.get("wialon_ips_synthetic_total", 0) + self._synthetic_events)
            self.set_metric("wialon_ips_anomaly_total", self.metrics.get("wialon_ips_anomaly_total", 0) + self._anomaly_events)
            self._metrics_count = 0
            self._last_metrics_ts = now
            self._synthetic_events = 0
            self._anomaly_events = 0

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            f = float(value)
            if f != f:  # NaN
                return None
            return f
        except Exception:
            return None

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            return int(float(value))
        except Exception:
            return None

    @staticmethod
    def _try_cast(val: str) -> Any:
        for cast in (int, float):
            try:
                return cast(val)
            except Exception:
                continue
        return val

    @staticmethod
    def _parse_ts(date_s: str, time_s: str) -> Optional[int]:
        # expect DDMMYY and HHMMSS
        if len(date_s) in {6, 8} and len(time_s) == 6 and date_s.isdigit() and time_s.isdigit():
            try:
                fmt = "%d%m%y%H%M%S" if len(date_s) == 6 else "%d%m%Y%H%M%S"
                dt_obj = dt.datetime.strptime(date_s + time_s, fmt)
                return int(dt_obj.replace(tzinfo=dt.timezone.utc).timestamp())
            except Exception:
                return None
        return None

    @staticmethod
    def _parse_coord(value: str | None, hemi: str | None) -> Optional[float]:
        if not value or value == "NA":
            return None
        try:
            v = float(value)
        except Exception:
            return None
        deg = int(v // 100)
        minutes = v - deg * 100
        decimal = deg + minutes / 60.0
        hemi = (hemi or "").upper()
        if hemi in {"S", "W"}:
            decimal = -decimal
        return decimal

    @staticmethod
    def _day_key(ts: int) -> str:
        return dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")

    @staticmethod
    def _synthetic_unit_id(uid: str) -> int:
        import zlib
        return zlib.crc32(uid.encode()) & 0x7FFFFFFF

    def _persist(self, events: list[Event]) -> None:
        if not events:
            return
        day_key = self._day_key(events[0].device_ts)
        root = self._storage_root
        try:
            if hasattr(self.raw_storage, "append"):
                written = self.raw_storage.append(day_key, events)  # type: ignore[attr-defined]
                self._ingest_logger.info(
                    "persist: day=%s root=%s count=%s written=%s",
                    day_key,
                    root,
                    len(events),
                    written,
                )
                if written != len(events):
                    self._ingest_logger.debug(
                        "persist: dedup day=%s written=%s/%s root=%s",
                        day_key,
                        written,
                        len(events),
                        root,
                    )
            else:
                self._persist_fallback(root, day_key, events)
        except Exception as exc:
            self._ingest_logger.error(
                "persist: failed day=%s root=%s err=%s",
                day_key,
                root,
                exc,
                exc_info=True,
            )
            return

        if self.latest_store:
            try:
                self.latest_store.update_from_events(events)
            except Exception as exc:
                self._ingest_logger.error(
                    "persist: latest_store update failed day=%s root=%s err=%s",
                    day_key,
                    root,
                    exc,
                    exc_info=True,
                )

    def _persist_fallback(self, root: Path, day_key: str, events: list[Event]) -> None:
        """File append path when RawStorage is not available."""
        import json

        day_dir = Path(root) / day_key
        day_dir.mkdir(parents=True, exist_ok=True)
        fp = day_dir / "events.jsonl"
        payload = "\n".join(json.dumps(ev.as_dict(), ensure_ascii=False) for ev in events) + "\n"
        with fp.open("a", encoding="utf-8") as fh:
            fh.write(payload)
        self._ingest_logger.info("persist: fallback day=%s root=%s count=%s", day_key, root, len(events))
