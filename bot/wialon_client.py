from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import queue
import re
import threading
import time
import weakref
from functools import lru_cache
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from datetime import datetime, timezone

import httpx
import requests
from requests.adapters import HTTPAdapter
from pipeline.config.defaults import load_from_env as load_pipeline_config
from detector_dut_filter import SensorMeta, list_dut_candidates, select_primary_dut

log = logging.getLogger("wialon_cf_bot")
geo_log = logging.getLogger("geo")
snapshot_log = logging.getLogger("unit_snapshot_watch")
cancel_log = logging.getLogger("cancel_trace")
_GEOCODE_CLIENTS: weakref.WeakValueDictionary[str, "WialonClient"] = weakref.WeakValueDictionary()


def _env_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return float(default)
    try:
        return float(raw.strip())
    except ValueError:
        return float(default)


HTTP_POOL_MAX = int(_env_str("HTTP_POOL_MAX", "64") or 64)
HTTP_TIMEOUT_CONN = float(_env_str("HTTP_TIMEOUT_CONN", "5.0") or 5.0)
HTTP_TIMEOUT_READ = float(_env_str("HTTP_TIMEOUT_READ", "20.0") or 20.0)
REPORT_TEMPLATES_TTL = 900
GEOCODE_CACHE_MAX = 512
GEOCODE_TTL_SEC_BUCKET = 300
UNIT_FLAGS_BASE = 1
UNIT_FLAGS_BASE_PLUS_FIELDS = UNIT_FLAGS_BASE + 8
UNIT_FLAGS_ADMIN_FIELDS = 128
UNIT_FLAGS_DEVICE_META = 0x100
UNIT_FLAGS_SENSORS = UNIT_FLAGS_BASE | 4096
UNIT_FLAGS_STATS = UNIT_FLAGS_BASE + 4096 + 1024 + 2097152 + 8 + UNIT_FLAGS_DEVICE_META
UNIT_FLAGS_SNAPSHOT = UNIT_FLAGS_BASE | UNIT_FLAGS_DEVICE_META
HW_TYPES_CACHE_TTL = 3600
ZONE_DATA_CACHE_TTL = 300
DUT_BATCH_RETRY_INITIAL_DELAY = 0.5
DUT_BATCH_RETRY_MAX_DELAY = 8.0
UNIT_DEVICE_BATCH_SIZE = int(_env_str("UNIT_DEVICE_BATCH_SIZE", "16") or 16)
UNIT_DEVICE_BATCH_RETRIES = int(_env_str("UNIT_DEVICE_BATCH_RETRIES", "5") or 5)
UNIT_DEVICE_BATCH_BACKOFF = float(_env_str("UNIT_DEVICE_BATCH_BACKOFF", "2.0") or 2.0)
UNIT_DEVICE_BATCH_BACKOFF_MAX = float(_env_str("UNIT_DEVICE_BATCH_BACKOFF_MAX", "30.0") or 30.0)
UNIT_DEVICE_WORKERS = int(_env_str("UNIT_DEVICE_WORKERS", "4") or 4)
UNIT_DEVICE_TOKENS_LIMIT = int(_env_str("UNIT_DEVICE_TOKENS_LIMIT", "6") or 6)

__all__ = [
    "WialonClient",
    "AsyncWialonClient",
    "AsyncWialonSessionPool",
    "AsyncWialonClientBuilder",
    "WialonSessionPool",
    "HTTP_POOL_MAX",
    "HTTP_TIMEOUT_CONN",
    "HTTP_TIMEOUT_READ",
    "UNIT_FLAGS_DEVICE_META",
    "UNIT_FLAGS_SNAPSHOT",
    "UNIT_FLAGS_BASE_PLUS_FIELDS",
    "UNIT_FLAGS_ADMIN_FIELDS",
    "UNIT_FLAGS_SENSORS",
    "UNIT_FLAGS_STATS",
    "HW_TYPES_CACHE_TTL",
    "ZONE_DATA_CACHE_TTL",
    "REPORT_TEMPLATES_TTL",
    "GEOCODE_CACHE_MAX",
    "GEOCODE_TTL_SEC_BUCKET",
    "DUT_BATCH_RETRY_INITIAL_DELAY",
    "DUT_BATCH_RETRY_MAX_DELAY",
    "UNIT_DEVICE_BATCH_SIZE",
    "UNIT_DEVICE_BATCH_RETRIES",
    "UNIT_DEVICE_BATCH_BACKOFF",
    "UNIT_DEVICE_BATCH_BACKOFF_MAX",
    "UNIT_DEVICE_WORKERS",
    "UNIT_DEVICE_TOKENS_LIMIT",
    "fetch_device_meta_with_batches",
    "apply_device_meta_fields",
    "collect_device_clients",
]

_time_module = time

class _GeoTimer:
    def __init__(self, label: str):
        self.label = label
        self.started = _time_module.perf_counter()

    def done(self) -> Tuple[str, float]:
        return self.label, _time_module.perf_counter() - self.started


def _geo_scrub_params(params: Any) -> str:
    try:
        payload = json.dumps(params, ensure_ascii=False)
        payload = re.sub(r'"sid"\s*:\s*"[^"]+"', '"sid":"***"', payload)
        payload = re.sub(r'"token"\s*:\s*"[^"]+"', '"token":"***"', payload)
        return payload
    except Exception:
        return str(params)

class WialonSessionPool:
    """Manage a limited set of independent Wialon sessions for parallel loading."""

    def __init__(
        self,
        clients: List["WialonClient"],
        owns_clients: bool,
        requested_size: int,
    ):
        self._queue: asyncio.Queue["WialonClient"] = asyncio.Queue()
        for client in clients:
            self._queue.put_nowait(client)
        self._clients = clients
        self._owns_clients = owns_clients
        self._requested_size = max(1, requested_size)
        self._closed = False

    @classmethod
    async def create(
        cls,
        base_client: "WialonClient",
        desired_size: int,
    ) -> "WialonSessionPool":
        desired = max(1, desired_size)
        clones: List["WialonClient"] = []
        for idx in range(desired):
            try:
                clone = base_client.spawn_subsession()
            except Exception as exc:
                log.warning(
                    "drain_analysis: failed to spawn subsession %s/%s: %s",
                    idx + 1,
                    desired,
                    exc,
                )
                break
            clones.append(clone)
        if not clones:
            log.warning(
                "drain_analysis: falling back to single-session mode for message loading"
            )
            pool = cls([base_client], owns_clients=False, requested_size=desired)
        else:
            pool = cls(clones, owns_clients=True, requested_size=desired)
        return pool

    @property
    def size(self) -> int:
        return len(self._clients)

    @property
    def requested_size(self) -> int:
        return self._requested_size

    @property
    def owns_clients(self) -> bool:
        return self._owns_clients

    async def acquire(self) -> "WialonClient":
        if self._closed:
            raise RuntimeError("WialonSessionPool is closed")
        client = await self._queue.get()
        return client

    def release(self, client: "WialonClient") -> None:
        if self._closed:
            if self._owns_clients:
                client.close()
            return
        self._queue.put_nowait(client)

    def _replace_client(self, old: "WialonClient", new: "WialonClient") -> None:
        replaced = False
        for idx, existing in enumerate(self._clients):
            if existing is old:
                self._clients[idx] = new
                replaced = True
                break
        if not replaced:
            self._clients.append(new)
        with contextlib.suppress(Exception):
            old.close()
        self._queue.put_nowait(new)

    def recycle(self, client: "WialonClient", *, broken: bool = False) -> None:
        if self._closed:
            if self._owns_clients:
                with contextlib.suppress(Exception):
                    client.close()
            return
        if broken and self._owns_clients:
            replacement: Optional["WialonClient"] = None
            try:
                replacement = client.spawn_subsession()
            except Exception as exc:
                log.warning(
                    "drain_analysis: failed to respawn session after error: %s",
                    exc,
                )
            if replacement is not None:
                self._replace_client(client, replacement)
                return
        self.release(client)

    @contextlib.asynccontextmanager
    async def session(self) -> Iterator["WialonClient"]:
        client = await self.acquire()
        try:
            yield client
        finally:
            self.release(client)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_clients:
            for client in self._clients:
                with contextlib.suppress(Exception):
                    client.close()
        while not self._queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()


def fetch_device_meta_with_batches(
    base_client: "WialonClient",
    clients: List[Tuple[str, "WialonClient"]],
    unit_ids: Sequence[int],
    *,
    chunk_size: int = UNIT_DEVICE_BATCH_SIZE,
    retries: int = UNIT_DEVICE_BATCH_RETRIES,
    backoff: float = UNIT_DEVICE_BATCH_BACKOFF,
    max_backoff: float = UNIT_DEVICE_BATCH_BACKOFF_MAX,
    workers: int = UNIT_DEVICE_WORKERS,
) -> Dict[int, Dict[str, Any]]:
    if not unit_ids or not clients:
        return {}
    chunk_size = max(1, int(chunk_size))
    retries = max(1, int(retries))
    backoff = max(0.1, float(backoff))
    max_backoff = max(backoff, float(max_backoff))
    workers = max(1, int(workers))

    task_queue: queue.Queue[Tuple[int, List[int]]] = queue.Queue()
    chunks = list(_chunked(unit_ids, chunk_size))
    total_chunks = len(chunks)
    for idx, chunk in enumerate(chunks, 1):
        task_queue.put((idx, list(chunk)))
    results: Dict[int, Dict[str, Any]] = {}
    results_lock = threading.Lock()

    device_flags = 1 | UNIT_FLAGS_DEVICE_META

    def _execute_batch(label: str, client: "WialonClient", chunk_index: int, chunk: List[int]) -> None:
        processed = 0
        for uid in chunk:
            delay = backoff
            attempt = 0
            while True:
                attempt += 1
                try:
                    data = client.request("core/search_item", {"id": uid, "flags": device_flags})
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    snapshot_log.warning(
                        "unit_snapshot: device chunk=%s/%s token=%s unit=%s attempt=%s err=%s",
                        chunk_index,
                        total_chunks,
                        label,
                        uid,
                        attempt,
                        exc,
                    )
                    if attempt >= retries:
                        break
                    client.abort_pending_requests()
                    base_client._sleep_with_cancel(min(delay, max_backoff))
                    delay = min(delay * 2, max_backoff)
                    continue

                if isinstance(data, dict) and data.get("error"):
                    snapshot_log.debug(
                        "unit_snapshot: device response error unit=%s err=%s",
                        uid,
                        data.get("error"),
                    )
                    break

                item = data.get("item") if isinstance(data, dict) else None
                if not isinstance(item, dict):
                    break
                details = base_client._extract_device_details(item)
                if details:
                    with results_lock:
                        results[uid] = details
                    processed += 1
                break
        if processed:
            snapshot_log.debug(
                "unit_snapshot: device chunk=%s/%s token=%s processed=%s",
                chunk_index,
                total_chunks,
                label,
                processed,
            )

    def _worker(label: str, client: "WialonClient") -> None:
        while True:
            try:
                chunk_index, chunk = task_queue.get_nowait()
            except queue.Empty:
                break
            try:
                _execute_batch(label, client, chunk_index, chunk)
            finally:
                task_queue.task_done()

    snapshot_log.info(
        "unit_snapshot: device details via batch tokens=%s workers=%s chunk_size=%s total_chunks=%s",
        len(clients),
        workers,
        chunk_size,
        total_chunks,
    )
    threads: List[threading.Thread] = []
    try:
        for label, client in clients[:workers]:
            thread = threading.Thread(
                target=_worker,
                name=f"device-meta-{label}",
                args=(label, client),
                daemon=True,
            )
            thread.start()
            threads.append(thread)
        task_queue.join()
    finally:
        for thread in threads:
            thread.join(timeout=0.1)

    snapshot_log.info(
        "unit_snapshot: device details fetched units=%s/%s",
        len(results),
        len(unit_ids),
    )
    return results


def apply_device_meta_fields(entry: Dict[str, Any], meta: Mapping[str, Any]) -> None:
    if not isinstance(entry, dict) or not isinstance(meta, Mapping):
        return
    device_block = entry.get("device")
    if isinstance(device_block, dict):
        target_device = device_block
    else:
        target_device = {}
        entry["device"] = target_device

    device_uid = meta.get("device_uid")
    if device_uid:
        entry["uid"] = device_uid
        target_device["uid"] = device_uid

    device_type_id = meta.get("device_type_id")
    if device_type_id is not None:
        entry["hw"] = device_type_id
        target_device["hw"] = device_type_id

    device_type_name = meta.get("device_type_name")
    if device_type_name:
        entry["hardware"] = device_type_name
        target_device["hardware"] = device_type_name


def collect_device_clients(base_client: "WialonClient") -> Tuple[List[Tuple[str, "WialonClient"]], List["WialonClient"]]:
    cfg = load_pipeline_config()
    raw_tokens: List[Tuple[str, str]] = []
    primary_token = getattr(cfg, "wialon_token", None)
    if primary_token:
        raw_tokens.append(("primary", primary_token))
    for idx, token in enumerate(getattr(cfg, "wialon_extra_tokens", ()), 1):
        if token:
            raw_tokens.append((f"extra-{idx}", token))
    deduped: List[Tuple[str, str]] = []
    seen: set[str] = set()
    for label, token in raw_tokens:
        token = token.strip()
        if not token or token in seen:
            continue
        seen.add(token)
        deduped.append((label, token))
    if not deduped:
        return [], []
    clients: List[Tuple[str, WialonClient]] = []
    extra_clients: List[WialonClient] = []
    base_token = getattr(base_client, "token", None)
    for label, token in deduped[: UNIT_DEVICE_TOKENS_LIMIT or len(deduped)]:
        if base_token and token == base_token and all(c is not base_client for _, c in clients):
            clients.append((label, base_client))
        else:
            new_client = WialonClient(base_client.host, token)
            clients.append((label, new_client))
            extra_clients.append(new_client)
    if not clients:
        clients.append(("primary", base_client))
    return clients, extra_clients


def _normalize_wialon_endpoints(host_value: str) -> Tuple[str, str]:
    raw = (host_value or "").strip()
    if not raw:
        raise ValueError("Wialon host must not be empty")
    if not raw.startswith(("http://", "https://")):
        normalized = f"https://{raw}"
    else:
        normalized = raw
    normalized = normalized.strip()
    lower = normalized.lower().rstrip("/")
    ajax_suffix = "/ajax.html"
    full_suffix = "/wialon/ajax.html"
    wialon_suffix = "/wialon"
    if lower.endswith(full_suffix):
        host_prefix = normalized[: -len(ajax_suffix)].rstrip("/")
        base_url = normalized.rstrip("/")
    elif lower.endswith(ajax_suffix):
        host_prefix = normalized[: -len(ajax_suffix)].rstrip("/")
        base_url = normalized.rstrip("/")
    elif lower.endswith(wialon_suffix):
        host_prefix = normalized.rstrip("/")
        base_url = f"{host_prefix}/ajax.html"
    else:
        host_prefix = normalized.rstrip("/")
        base_url = f"{host_prefix}/wialon/ajax.html"
    return host_prefix.rstrip("/"), base_url.rstrip("/")


def _chunked(sequence: Sequence[int], size: int) -> Iterator[List[int]]:
    chunk: List[int] = []
    for item in sequence:
        chunk.append(int(item))
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class AsyncWialonClient:
    """Asynchronous subset of the Wialon API used for message loading routines."""

    def __init__(
        self,
        host: str,
        token: str,
        *,
        zone_strategy: Optional[str] = None,
        allowed_rids_cache: Optional[Tuple[float, List[int]]] = None,
    ) -> None:
        self.host, self.base_url = _normalize_wialon_endpoints(host)
        self.token = token
        self.sid: Optional[str] = None
        self._zone_all_strategy = zone_strategy
        self._allowed_rids_cache = allowed_rids_cache or (0.0, [])
        self._http_timeout = httpx.Timeout(
            connect=HTTP_TIMEOUT_CONN,
            read=HTTP_TIMEOUT_READ,
            write=HTTP_TIMEOUT_READ,
            pool=None,
        )
        self._http_limits = httpx.Limits(
            max_connections=HTTP_POOL_MAX,
            max_keepalive_connections=HTTP_POOL_MAX,
        )
        self._client = self._create_client()
        self._auth_lock = asyncio.Lock()
        self._cancel_predicate: Optional[Callable[[], bool]] = None
        self._last_calc_series_raw: Any = None

    def _create_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=self._http_timeout, limits=self._http_limits)

    async def aclose(self) -> None:
        with contextlib.suppress(Exception):
            await self._client.aclose()

    async def spawn_subsession(self) -> "AsyncWialonClient":
        clone = AsyncWialonClient(
            self.host,
            self.token,
            zone_strategy=self._zone_all_strategy,
            allowed_rids_cache=self._allowed_rids_cache,
        )
        clone.sid = self.sid
        clone._cancel_predicate = self._cancel_predicate
        return clone

    def set_cancel_predicate(self, predicate: Callable[[], bool]) -> None:
        self._cancel_predicate = predicate

    def clear_cancel_predicate(self) -> None:
        self._cancel_predicate = None

    def _check_cancelled(self) -> None:
        if not self._cancel_predicate:
            return
        cancelled = False
        try:
            cancelled = bool(self._cancel_predicate())
        except Exception as exc:  # pragma: no cover - defensive logging
            log.debug("AsyncWialonClient cancel predicate raised: %s", exc)
        if cancelled:
            raise asyncio.CancelledError()

    async def _sleep_with_cancel(self, seconds: float) -> None:
        if seconds <= 0:
            return
        deadline = time.monotonic() + seconds
        interval = min(0.25, seconds)
        while True:
            self._check_cancelled()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(min(interval, max(0.0, remaining)))

    async def abort_pending_requests(self) -> None:
        cancelled = False
        try:
            self._check_cancelled()
        except asyncio.CancelledError:
            cancelled = True
        log.info("AsyncWialonClient[%s]: abort_pending_requests triggered cancel_flag=%s", id(self), cancelled)
        await self._client.aclose()
        self._client = self._create_client()
        if cancelled:
            cancel_log.info("async_client abort_pending_requests closed after cancel id=%s", id(self))

    async def login(self) -> None:
        async with self._auth_lock:
            if self.sid:
                return
            data = await self._call_rpc("token/login", {"token": self.token}, include_sid=False)
            self.sid = data.get("eid") or data.get("sid")
            if not self.sid:
                raise RuntimeError(f"Не удалось получить SID при аутентификации: {data}")

    async def request(
        self,
        svc: str,
        params: Dict[str, Any],
        *,
        include_sid: bool = True,
        retry_on_session_error: bool = True,
    ) -> Dict[str, Any]:
        if include_sid and not self.sid:
            await self.login()
        data = await self._call_rpc(svc, params, include_sid=include_sid)
        if (
            include_sid
            and retry_on_session_error
            and isinstance(data, dict)
            and data.get("error") in (1, 4, 6)
        ):
            self.sid = None
            await self.login()
            data = await self._call_rpc(svc, params, include_sid=True)
        return data

    async def _call_rpc(self, svc: str, params: Dict[str, Any], *, include_sid: bool) -> Dict[str, Any]:
        timer = _GeoTimer(f"rpc:{svc}")
        try:
            data = await self._request_raw(svc, params, include_sid=include_sid)
        except Exception as exc:
            label, elapsed = timer.done()
            geo_log.error(
                "%s FAIL in %.3fs err=%r params=%s",
                label,
                elapsed,
                exc,
                _geo_scrub_params(params)[:400],
            )
            raise
        else:
            label, elapsed = timer.done()
            err = data.get("error") if isinstance(data, dict) else None
            geo_log.debug(
                "%s ok in %.3fs err=%s params=%s",
                label,
                elapsed,
                err,
                _geo_scrub_params(params)[:400],
            )
            return data

    async def _request_raw(self, svc: str, params: Dict[str, Any], *, include_sid: bool) -> Dict[str, Any]:
        try:
            params_str = params if isinstance(params, str) else json.dumps(params, ensure_ascii=False)
        except Exception as exc:
            raise RuntimeError(f"Не удалось сериализовать params для {svc}: {exc}") from exc

        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        max_attempts = 5
        attempts = 0
        backoff = 0.5
        last_error: Optional[Exception] = None

        while attempts < max_attempts:
            self._check_cancelled()
            attempts += 1
            form = {"svc": svc, "params": params_str}
            target_url = self.base_url.rstrip("/") or self.base_url
            if include_sid and self.sid:
                form["sid"] = self.sid

            try:
                response = await self._client.post(target_url, data=form, headers=headers)
                response.raise_for_status()
                try:
                    return response.json()
                except ValueError as exc:
                    content_type = response.headers.get("Content-Type", "")
                    snippet = response.text[:500] if response.text else ""
                    last_error = RuntimeError(
                        "Не удалось декодировать JSON от Wialon "
                        f"(HTTP {response.status_code}, {content_type}): {snippet}"
                    )
                    if attempts >= max_attempts:
                        raise last_error
                    sleep_time = backoff
                    backoff = min(backoff * 2, 8.0)
                    log.warning(
                        "rpc %s JSON decode failed attempt=%s/%s sleep=%.1fs: %s",
                        svc,
                        attempts,
                        max_attempts,
                        sleep_time,
                        exc,
                    )
                    await self._sleep_with_cancel(sleep_time)
            except asyncio.CancelledError:
                raise
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code if exc.response else None
                last_error = exc
                if status_code in (401, 403):
                    log.warning("rpc %s received %s, re-login", svc, status_code)
                    self.sid = None
                    try:
                        await self.login()
                    except Exception as auth_exc:
                        last_error = auth_exc
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after HTTP error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                await self._sleep_with_cancel(sleep_time)
            except httpx.HTTPError as exc:
                last_error = exc
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after network error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                await self._sleep_with_cancel(sleep_time)
            except Exception as exc:
                last_error = exc
                if isinstance(exc, RuntimeError) and "client has been closed" in str(exc):
                    log.warning("rpc %s detected closed HTTP client, recreating", svc)
                    with contextlib.suppress(Exception):
                        await self._client.aclose()
                    self._client = self._create_client()
                    if include_sid:
                        self.sid = None
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after unexpected error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                await self._sleep_with_cancel(sleep_time)

        if last_error:
            raise last_error
        raise RuntimeError(f"Не удалось выполнить запрос Wialon для {svc}")

    async def load_messages_interval(
        self,
        unit_id: int,
        time_from: int,
        time_to: int,
        *,
        flags: int = 0x0000,
        flags_mask: int = 0xFF00,
        load_count: int = 0xFFFFFFFF,
    ) -> Dict[str, Any]:
        params = {
            "itemId": int(unit_id),
            "timeFrom": int(time_from),
            "timeTo": int(time_to),
            "flags": int(flags),
            "flagsMask": int(flags_mask),
            "loadCount": int(load_count),
        }
        return await self.request("messages/load_interval", params)

    async def get_loaded_messages(self, unit_id: int, index_from: int, index_to: int) -> Any:
        params = {
            "itemId": int(unit_id),
            "indexFrom": int(max(0, index_from)),
            "indexTo": int(max(0, index_to)),
        }
        return await self.request("messages/get_messages", params)

    async def unload_messages(self, unit_id: Optional[int] = None) -> None:
        params: Dict[str, Any] = {}
        if unit_id is not None:
            try:
                params["itemId"] = int(unit_id)
            except Exception:
                params["itemId"] = unit_id
        try:
            await self.request("messages/unload", params)
        except Exception as exc:
            log.debug("messages/unload failed: %s", exc)

    async def calc_sensor_series(
        self,
        unit_id: int,
        sensor_id: Optional[int],
        *,
        width: Optional[int] = None,
        index_from: int = 0,
        index_to: Optional[int] = None,
    ) -> Union[List[Tuple[int, float]], Any]:
        try:
            idx_from = max(0, int(index_from))
        except Exception:
            idx_from = 0
        try:
            idx_to = int(index_to) if index_to is not None else 0
        except Exception:
            idx_to = 0

        params: Dict[str, Any] = {
            "source": "",
            "unitId": int(unit_id),
            "indexFrom": idx_from,
            "indexTo": idx_to,
        }
        if isinstance(sensor_id, (list, tuple, set)):
            raise ValueError("calc_sensor_series does not accept multiple sensor IDs; use sensorId=0 instead")
        if sensor_id is None:
            params["sensorId"] = 0
        else:
            params["sensorId"] = int(sensor_id)
        if width is not None:
            try:
                params["width"] = max(1, int(width))
            except Exception:
                params["width"] = width
        self._last_calc_series_params = dict(params)
        data = await self.request("unit/calc_sensors", params)
        self._last_calc_series_raw = data
        sensor_id_value = params.get("sensorId")
        if sensor_id_value == 0 or isinstance(sensor_id_value, list):
            return data
        if isinstance(data, list):
            cleaned: List[Tuple[int, float]] = []
            for entry in data:
                if isinstance(entry, list) and entry:
                    try:
                        ts = int(entry[0])
                        val = float(entry[1])
                    except Exception:
                        continue
                    cleaned.append((ts, val))
            return cleaned
        return []


class AsyncWialonSessionPool:
    """Pool wrapper over AsyncWialonClient instances."""

    def __init__(
        self,
        clients: List[AsyncWialonClient],
        owns_clients: bool,
        requested_size: int,
    ) -> None:
        self._queue: asyncio.Queue[AsyncWialonClient] = asyncio.Queue()
        for client in clients:
            self._queue.put_nowait(client)
        self._clients = clients
        self._owns_clients = owns_clients
        self._requested_size = max(1, requested_size)
        self._closed = False

    @classmethod
    async def create_from_sync(cls, base_client: "WialonClient", desired_size: int) -> "AsyncWialonSessionPool":
        desired = max(1, desired_size)
        base_async = await AsyncWialonClientBuilder.from_sync(base_client)
        clients: List[AsyncWialonClient] = [base_async]
        for idx in range(desired - 1):
            try:
                clone = await base_async.spawn_subsession()
            except Exception as exc:
                log.warning(
                    "message_cache: failed to spawn async subsession %s/%s: %s",
                    idx + 1,
                    desired - 1,
                    exc,
                )
                break
            clients.append(clone)
        return cls(clients, owns_clients=True, requested_size=desired)

    @property
    def size(self) -> int:
        return len(self._clients)

    @property
    def requested_size(self) -> int:
        return self._requested_size

    @property
    def owns_clients(self) -> bool:
        return self._owns_clients

    async def acquire(self) -> AsyncWialonClient:
        if self._closed:
            raise RuntimeError("AsyncWialonSessionPool is closed")
        return await self._queue.get()

    def release(self, client: AsyncWialonClient) -> None:
        if self._closed:
            if self._owns_clients:
                asyncio.create_task(client.aclose())
            return
        self._queue.put_nowait(client)

    async def recycle(self, client: AsyncWialonClient, *, broken: bool = False) -> None:
        if self._closed:
            if self._owns_clients:
                await client.aclose()
            return
        if broken and self._owns_clients:
            replacement: Optional[AsyncWialonClient] = None
            try:
                replacement = await client.spawn_subsession()
            except Exception as exc:
                log.warning("message_cache: failed to respawn async session after error: %s", exc)
            if replacement is not None:
                await client.aclose()
                self._clients.append(replacement)
                self._queue.put_nowait(replacement)
                return
        self.release(client)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_clients:
            await asyncio.gather(*(client.aclose() for client in self._clients), return_exceptions=True)
        while not self._queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()


class AsyncWialonClientBuilder:
    """Factory helpers for AsyncWialonClient."""

    @staticmethod
    async def from_sync(base_client: "WialonClient") -> AsyncWialonClient:
        async_client = AsyncWialonClient(
            base_client.host,
            base_client.token,
            zone_strategy=getattr(base_client, "_zone_all_strategy", None),
            allowed_rids_cache=getattr(base_client, "_allowed_rids_cache", (0.0, [])),
        )
        async_client.sid = base_client.sid
        async_client._cancel_predicate = getattr(base_client, "_cancel_predicate", None)
        if not async_client.sid:
            await async_client.login()
        return async_client

class WialonClient:
    # env-переключатель (опционально): omit|null|empty
    GEO_ZONE_ALL_STRATEGY = ""

    def __init__(self, host: str, token: str, session: Optional[requests.Session] = None):
        self.host, self.base_url = _normalize_wialon_endpoints(host)
        self.token = token
        self.sid: Optional[str] = None
        self._http_adapter_kwargs = {
            "pool_connections": HTTP_POOL_MAX,
            "pool_maxsize": HTTP_POOL_MAX,
            "max_retries": 0,
        }
        self.session = session or requests.Session()
        adapter = HTTPAdapter(**self._http_adapter_kwargs)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self._auth_lock = threading.Lock()
        self._session_external = session is not None
        self._session_lock: Optional[threading.Lock] = threading.Lock() if self._session_external else None
        self._session_local = threading.local()
        self._session_local.session = self.session
        self._report_templates_cache: Tuple[float, List[Dict[str, Any]]] = (0.0, [])
        self._zone_all_strategy: Optional[str] = None  # 'omit'|'null'|'empty'
        self._allowed_rids_cache: Tuple[float, List[int]] = (0.0, [])
        self._last_zone_details: Dict[Tuple[int, int], Dict[str, Any]] = {}
        self._geocode_cache_id = id(self)
        self._hw_types_cache: Tuple[float, Dict[int, str]] = (0.0, {})
        _GEOCODE_CLIENTS[self._geocode_cache_id] = self
        self._last_calc_series_raw: Any = None
        self._last_rpc_response: Any = None
        self._cancel_predicate: Optional[Callable[[], bool]] = None

    def spawn_subsession(self) -> "WialonClient":
        """Create a new client instance sharing the same host and token."""

        clone = WialonClient(self.host, self.token)
        clone._zone_all_strategy = self._zone_all_strategy  # type: ignore[attr-defined]
        clone._cancel_predicate = self._cancel_predicate
        return clone

    def set_cancel_predicate(self, predicate: Callable[[], bool]) -> None:
        self._cancel_predicate = predicate

    def clear_cancel_predicate(self) -> None:
        self._cancel_predicate = None

    def _check_cancelled(self) -> None:
        predicate = self._cancel_predicate
        if not predicate:
            return
        cancelled = False
        try:
            cancelled = bool(predicate())
        except Exception as exc:
            log.debug("WialonClient cancel predicate raised: %s", exc)
        if cancelled:
            log.info("WialonClient[%s]: cancellation detected", id(self))
            raise asyncio.CancelledError()

    def _sleep_with_cancel(self, seconds: float) -> None:
        if seconds <= 0:
            return
        deadline = time.monotonic() + seconds
        interval = min(0.25, seconds)
        while True:
            self._check_cancelled()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(interval, max(0.0, remaining)))

    def close(self) -> None:
        """Close the underlying HTTP session if it was created internally."""

        if self._session_external:
            return
        session = getattr(self._session_local, "session", None)
        if session is None:
            session = self.session
        try:
            session.close()
        except Exception:
            pass
        with contextlib.suppress(Exception):
            _GEOCODE_CLIENTS.pop(self._geocode_cache_id, None)


    def abort_pending_requests(self) -> None:
        cancelled = False
        try:
            self._check_cancelled()
        except asyncio.CancelledError:
            cancelled = True
        log.info("WialonClient[%s]: abort_pending_requests triggered cancel_flag=%s", id(self), cancelled)
        session = getattr(self._session_local, "session", None)
        if session is None:
            session = self.session
        with contextlib.suppress(Exception):
            session.close()
        self._session_local.session = self._create_session()
        self.session = self._session_local.session
        if cancelled:
            cancel_log.info("sync_client abort_pending_requests closed after cancel id=%s", id(self))

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        adapter = HTTPAdapter(**self._http_adapter_kwargs)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    @contextlib.contextmanager
    def _session_scope(self) -> Iterator[requests.Session]:
        if self._session_external:
            lock = self._session_lock
            if lock:
                lock.acquire()
            try:
                yield self.session
            finally:
                if lock:
                    lock.release()
            return

        session = getattr(self._session_local, "session", None)
        if session is None:
            session = self._create_session()
            self._session_local.session = session
        yield session

    def _request_raw(self, svc: str, params: Dict[str, Any], include_sid: bool = True) -> Dict[str, Any]:
        # Wialon Remote API ожидает form-urlencoded:
        # svc=<svc>&params=<json-string>&sid=<sid>
        try:
            params_str = params if isinstance(params, str) else json.dumps(params, ensure_ascii=False)
        except Exception as exc:
            raise RuntimeError(f"Не удалось сериализовать params для {svc}: {exc}") from exc

        # гарантируем корректный Content-Type
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}

        max_attempts = 5
        attempts = 0
        backoff = 0.5
        last_error: Optional[Exception] = None

        while attempts < max_attempts:
            self._check_cancelled()
            attempts += 1
            form = {"svc": svc, "params": params_str}
            target_url = self.base_url.rstrip("/") or self.base_url
            if include_sid and self.sid:
                form["sid"] = self.sid

            try:
                with self._session_scope() as session:
                    resp = session.post(
                        target_url,
                        data=form,  # ВАЖНО: не json=..., а data=...
                        headers=headers,
                        timeout=(HTTP_TIMEOUT_CONN, HTTP_TIMEOUT_READ),
                    )
                resp.raise_for_status()
                try:
                    return resp.json()
                except ValueError as exc:
                    content_type = resp.headers.get("Content-Type", "")
                    snippet = resp.text[:500] if resp.text else ""
                    last_error = RuntimeError(
                        "Не удалось получить JSON от Wialon "
                        f"(HTTP {resp.status_code}, {content_type}): {snippet}"
                    )
                    if attempts >= max_attempts:
                        raise last_error
                    sleep_time = backoff
                    backoff = min(backoff * 2, 8.0)
                    log.warning(
                        "rpc %s JSON decode failed attempt=%s/%s sleep=%.1fs: %s",
                        svc,
                        attempts,
                        max_attempts,
                        sleep_time,
                        exc,
                    )
                    self._sleep_with_cancel(sleep_time)
                else:
                    break
            except asyncio.CancelledError:
                raise
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response else None
                last_error = exc
                if status_code in (401, 403):
                    log.warning("rpc %s received %s, re-login", svc, status_code)
                    self.sid = None
                    try:
                        self.login()
                    except Exception as auth_exc:
                        last_error = auth_exc
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after HTTP error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                self._sleep_with_cancel(sleep_time)
                continue
            except requests.RequestException as exc:
                last_error = exc
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after network error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                self._sleep_with_cancel(sleep_time)
                continue
            except Exception as exc:
                last_error = exc
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after unexpected error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                self._sleep_with_cancel(sleep_time)
                continue

        if last_error:
            raise last_error
        raise RuntimeError(f"Не удалось выполнить запрос Wialon для {svc}")

    def _call_rpc(self, svc: str, params: Dict[str, Any], include_sid: bool = True) -> Dict[str, Any]:
        timer = _GeoTimer(f"rpc:{svc}")
        try:
            data = self._request_raw(svc, params, include_sid=include_sid)
        except Exception as exc:
            label, elapsed = timer.done()
            geo_log.error(
                "%s FAIL in %.3fs err=%r params=%s",
                label,
                elapsed,
                exc,
                _geo_scrub_params(params)[:400],
            )
            raise
        else:
            label, elapsed = timer.done()
            err = data.get("error") if isinstance(data, dict) else None
            geo_log.debug(
                "%s ok in %.3fs err=%s params=%s",
                label,
                elapsed,
                err,
                _geo_scrub_params(params)[:400],
            )
            return data

    def login(self) -> None:
        with self._auth_lock:
            data = self._call_rpc("token/login", {"token": self.token}, include_sid=False)
            self.sid = data.get("eid") or data.get("sid")
            if not self.sid:
                raise RuntimeError(f"Не удалось получить SID при авторизации: {data}")

    def request(self, svc: str, params: Dict[str, Any], retry_on_session_error: bool = True) -> Dict[str, Any]:
        if not self.sid:
            self.login()
        data = self._call_rpc(svc, params, include_sid=True)
        self._last_rpc_response = data
        if isinstance(data, dict) and data.get("error") in (1, 4, 6) and retry_on_session_error:
            self.login()
            data = self._call_rpc(svc, params, include_sid=True)
            self._last_rpc_response = data
        return data

    def _get_hw_types_map(self, use_cache: bool = True) -> Dict[int, str]:
        now = time.time()
        cache_ts, cached = self._hw_types_cache
        if use_cache and cached and (now - cache_ts) < HW_TYPES_CACHE_TTL:
            return dict(cached)

        try:
            data = self.request("core/get_hw_types", {})
        except Exception:
            if cached:
                return dict(cached)
            raise

        raw_items: List[Dict[str, Any]] = []
        if isinstance(data, list):
            raw_items = [dict(item) for item in data if isinstance(item, dict)]
        elif isinstance(data, dict):
            candidate_lists = []
            for key in ("items", "hwtypes", "hwTypes", "types", "list", "values"):
                value = data.get(key)
                if isinstance(value, list):
                    candidate_lists.append(value)
            if candidate_lists:
                for lst in candidate_lists:
                    raw_items.extend(dict(item) for item in lst if isinstance(item, dict))
            elif all(isinstance(val, dict) for val in data.values()):
                for key, val in data.items():
                    entry = dict(val)
                    if "id" not in entry:
                        entry["id"] = key
                    raw_items.append(entry)

        mapping: Dict[int, str] = {}
        for entry in raw_items:
            hw_id_raw = entry.get("id") or entry.get("i") or entry.get("hw") or entry.get("hwid")
            try:
                hw_id = int(hw_id_raw)
            except Exception:
                continue
            name = entry.get("name") or entry.get("nm") or entry.get("n") or entry.get("title")
            if isinstance(name, str):
                mapping[hw_id] = name.strip()
            elif hw_id not in mapping:
                mapping[hw_id] = ""

        if mapping:
            self._hw_types_cache = (now, {k: v for k, v in mapping.items() if v})
            return {k: v for k, v in mapping.items() if v}

        if cached:
            return dict(cached)
        return {}

    def _extract_device_details(self, item: Mapping[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        unit_raw = item.get("id")
        try:
            result["unit_id"] = int(unit_raw)
        except Exception:
            if unit_raw is not None:
                result["unit_id"] = unit_raw

        uid_value = item.get("uid") or item.get("unique_id") or item.get("uniqueId")
        device_uid: Optional[str] = None
        if isinstance(uid_value, (int, float)):
            device_uid = str(int(uid_value))
        elif isinstance(uid_value, str) and uid_value.strip():
            device_uid = uid_value.strip()
        if device_uid:
            result["device_uid"] = device_uid

        hw_entry = item.get("hw")
        hw_id: Optional[int] = None
        hw_name: Optional[str] = None
        if isinstance(hw_entry, dict):
            hw_raw = hw_entry.get("id") or hw_entry.get("i") or hw_entry.get("hw")
            try:
                hw_id = int(hw_raw)
            except Exception:
                hw_id = None
            name_val = hw_entry.get("name") or hw_entry.get("nm") or hw_entry.get("n")
            if isinstance(name_val, str) and name_val.strip():
                hw_name = name_val.strip()
        else:
            try:
                hw_id = int(hw_entry)
            except Exception:
                hw_id = None

        if hw_id is not None:
            result["device_type_id"] = hw_id
            if not hw_name:
                try:
                    hw_name = self._get_hw_types_map().get(hw_id)
                except Exception:
                    hw_name = None
        if hw_name:
            result["device_type_name"] = hw_name

        return result

    def get_unit_device_details(
        self,
        unit_id: int,
        *,
        timeout: float = 20.0,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        params = {"id": int(unit_id), "flags": 1 | 0x100}
        attempt = 0
        while True:
            attempt += 1
            try:
                data = self.request("core/search_item", params)
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if attempt >= max_retries:
                    raise
                snapshot_log.debug(
                    "unit_snapshot: device details retry unit=%s attempt=%s err=%s",
                    unit_id,
                    attempt,
                    exc,
                )
                self._sleep_with_cancel(min(timeout, 2.0))
        item = data.get("item") if isinstance(data, dict) else {}
        if not isinstance(item, dict):
            return {}

        return self._extract_device_details(item)

    def get_unit_assigned_drivers(self, unit_id: int) -> List[Dict[str, Any]]:
        params = {"unitId": int(unit_id)}
        data = self.request("resource/get_unit_drivers", params)

        raw_entries: List[Dict[str, Any]] = []
        if isinstance(data, list):
            raw_entries = [dict(item) for item in data if isinstance(item, dict)]
        elif isinstance(data, dict):
            for key in ("drivers", "items", "list", "result", "value"):
                value = data.get(key)
                if isinstance(value, list):
                    raw_entries.extend(dict(item) for item in value if isinstance(item, dict))
            for key, val in data.items():
                if isinstance(val, list) and key not in {"drivers", "items", "list", "result", "value"}:
                    raw_entries.extend(
                        dict(item) for item in val if isinstance(item, dict)
                    )
            if not raw_entries and all(isinstance(val, dict) for val in data.values()):
                for key, val in data.items():
                    entry = dict(val)
                    if "id" not in entry:
                        entry["id"] = key
                    raw_entries.append(entry)

        drivers: List[Dict[str, Any]] = []
        for entry in raw_entries:
            driver_id_raw = entry.get("id") or entry.get("i") or entry.get("driver_id")
            try:
                driver_id = int(driver_id_raw)
            except Exception:
                driver_id = None
            name = entry.get("name") or entry.get("nm") or entry.get("n") or entry.get("title")
            code = entry.get("code") or entry.get("c") or entry.get("driver_code")
            drivers.append(
                {
                    "id": driver_id,
                    "name": name.strip() if isinstance(name, str) else None,
                    "code": code.strip() if isinstance(code, str) else None,
                }
            )

        drivers.sort(
            key=lambda d: (
                (d.get("name") or "").casefold(),
                (d.get("code") or "").casefold(),
                d.get("id") or 0,
            )
        )
        return drivers

    def list_units_with_pos(self) -> List[Dict[str, Any]]:
        spec = {
            "itemsType": "avl_unit",
            "propName": "sys_name",
            "propValueMask": "*",
            "sortType": "sys_name",
        }
        params = {"spec": spec, "force": 1, "flags": 1 + 1024, "from": 0, "to": 0}
        data = self.request("core/search_items", params)
        items = (data or {}).get("items") or []
        out = []
        for it in items:
            if not isinstance(it, dict):
                continue
            pos = it.get("pos") or {}
            lat = pos.get("y") if isinstance(pos, dict) else None
            if lat is None and isinstance(pos, dict):
                lat = pos.get("lat")
            lon = pos.get("x") if isinstance(pos, dict) else None
            if lon is None and isinstance(pos, dict):
                lon = pos.get("lon")
            out.append(
                {
                    "id": it.get("id"),
                    "nm": it.get("nm"),
                    "pos": {
                        "lat": lat,
                        "lon": lon,
                        "t": (pos.get("t") if isinstance(pos, dict) else None),
                    },
                }
            )
        return out

    def list_accessible_resource_ids(self) -> List[int]:
        params = {
            "spec": {
                "itemsType": "resource",
                "propName": "sys_name",
                "propValueMask": "*",
                "sortType": "sys_name",
            },
            "force": 1,
            "flags": 1,
            "from": 0,
            "to": 0,
        }
        data = self.request("core/search_items", params)
        items = (data or {}).get("items") or []
        result: List[int] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            rid = item.get("id") or item.get("i")
            if isinstance(rid, int):
                result.append(rid)
                continue
            try:
                result.append(int(rid))
            except Exception:
                continue
        if result:
            return result
        try:
            legacy = self.list_resource_ids()
        except Exception:
            legacy = []
        return list(legacy)

    def get_allowed_rids(self, force_refresh: bool = False) -> List[int]:
        env_raw = ""
        if env_raw:
            allowed: List[int] = []
            for part in env_raw.split(","):
                token = part.strip()
                if token.isdigit():
                    try:
                        allowed.append(int(token))
                    except Exception:
                        continue
            return allowed

        cache_ts, cached = self._allowed_rids_cache
        if cached and not force_refresh:
            if time.time() - cache_ts < 60:
                return list(cached)

        try:
            accessible = self.list_accessible_resource_ids()
        except Exception as exc:
            geo_log.error("[zones] failed to list accessible resources: %s", exc)
            accessible = []
        self._allowed_rids_cache = (time.time(), list(accessible))
        return list(accessible)

    def find_zones_for_unit(
        self, unit_id: int, lat: float, lon: float
    ) -> Dict[int, List[int]]:
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except Exception:
            return {}
        if not math.isfinite(lat_f) or not math.isfinite(lon_f):
            return {}

        allowed_rids = self.get_allowed_rids()
        used_rids = [rid for rid in allowed_rids if isinstance(rid, int)]
        sid_short = (self.sid or "-")[:8]
        geo_log.info(
            "[zones] SID=%s allowed_rids=%s used_rids=%s lat,lon=(%.6f,%.6f)",
            sid_short,
            len(allowed_rids),
            len(used_rids),
            lat_f,
            lon_f,
        )
        if not used_rids:
            geo_log.info("[zones] no allowed rids for this SID -> none")
            return {}

        zone_map = {str(rid): [] for rid in used_rids}
        params = {"spec": {"lat": lat_f, "lon": lon_f, "zoneId": zone_map}}
        timer = _GeoTimer("zones:get_zones_by_point")
        try:
            data = self.request("resource/get_zones_by_point", params)
        except Exception:
            _, elapsed = timer.done()
            geo_log.error(
                "[zones] get_zones_by_point failed in %.3fs for uid=%s", elapsed, unit_id
            )
            raise

        _, elapsed = timer.done()
        hits: Dict[int, List[int]] = {}
        if isinstance(data, dict):
            for rid_key, zids in data.items():
                if rid_key in {"error", "total"}:
                    continue
                try:
                    rid = int(rid_key)
                except Exception:
                    continue
                if not isinstance(zids, list):
                    continue
                cleaned: List[int] = []
                for zid in zids:
                    try:
                        cleaned.append(int(zid))
                    except Exception:
                        continue
                if cleaned:
                    hits[rid] = cleaned
        hits_total = sum(len(zones) for zones in hits.values())
        hits_summary = {rid: len(zones) for rid, zones in hits.items()}
        geo_log.info(
            "[zones] get_zones_by_point -> hits_total=%s by_res=%s in %.3fs",
            hits_total,
            hits_summary,
            elapsed,
        )
        return hits

    def resolve_zone_names(self, hits: Dict[int, List[int]]) -> List[str]:
        names: List[str] = []
        seen: Set[str] = set()
        self._last_zone_details = {}
        if not hits:
            geo_log.info("[zones] resolve -> no hits")
            return names

        timer = _GeoTimer("zones:get_zone_data")
        for rid, zone_ids in hits.items():
            if not zone_ids:
                continue
            params = {
                "itemId": int(rid),
                "col": [int(zid) for zid in zone_ids],
                "flags": 16,
            }
            try:
                data = self.request("resource/get_zone_data", params)
            except Exception as exc:
                geo_log.debug(
                    "[zones] resolve: get_zone_data failed rid=%s ids=%s err=%s",
                    rid,
                    len(zone_ids),
                    exc,
                )
                continue
            parsed = self._parse_zone_data_response(data)
            if parsed:
                self._update_zone_cache(rid, parsed)
            for zid, payload in parsed.items():
                if not isinstance(payload, dict):
                    payload = {"n": payload}
                raw_name = (payload.get("n") or payload.get("name") or "").strip()
                name = raw_name or f"ID {zid}"
                name_cf = name.casefold()
                center_lat, center_lon = self._extract_zone_center(payload)
                entry = {
                    "resource_id": rid,
                    "zone_id": zid,
                    "name": name,
                    "lat": center_lat,
                    "lon": center_lon,
                    "payload": dict(payload),
                }
                self._last_zone_details[(rid, zid)] = entry
                if name_cf not in seen:
                    names.append(name)
                    seen.add(name_cf)
        _, elapsed = timer.done()
        geo_log.info(
            "[zones] get_zone_data resolve -> names=%s in %.3fs",
            len(names),
            elapsed,
        )
        geo_log.debug(
            "[zones] names=%s",
            ", ".join(names[:20]) if names else "—",
        )
        return names

    def _ensure_geo_file_updater(self) -> None:
        try:
            ensure_geo_file_updater(self)
        except Exception as exc:
            geo_log.debug("file_cache: ensure updater failed: %s", exc)

    def _ensure_unit_snapshot_updater(self) -> None:
        try:
            ensure_unit_snapshot_updater(self)
        except Exception as exc:
            log.debug("unit_snapshot: ensure updater failed: %s", exc)

    def list_resource_ids(self) -> List[int]:
        params = {
            "spec": {
                "itemsType": "avl_resource",
                "propName": "sys_name",
                "propValueMask": "*",
                "sortType": "sys_name",
            },
            "force": 1,
            "flags": 1,
            "from": 0,
            "to": 0,
        }
        started = time.perf_counter()
        data = self.request("core/search_items", params)
        duration = time.perf_counter() - started
        items = data.get("items", []) if isinstance(data, dict) else []
        resource_ids: List[int] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            rid = item.get("id")
            try:
                resource_ids.append(int(rid))
            except Exception:
                continue
        geo_log.debug(
            "file_cache: search_items took %.3fs resources=%s",
            duration,
            len(resource_ids),
        )
        return resource_ids

    def list_report_templates(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Возвращает список шаблонов отчётов из всех ресурсов."""
        if use_cache:
            cache_ts, cached = self._report_templates_cache
            if cached and (time.time() - cache_ts) < REPORT_TEMPLATES_TTL:
                return [dict(tpl) for tpl in cached]

        spec = {
            "itemsType": "avl_resource",
            "propName": "sys_name",
            "propValueMask": "*",
            "sortType": "sys_name",
        }
        params = {"spec": spec, "force": 1, "flags": 8193, "from": 0, "to": 0}
        data = self.request("core/search_items", params)
        items = data.get("items", []) if isinstance(data, dict) else []
        templates: List[Dict[str, Any]] = []
        for item in items:
            rep = item.get("rep") or {}
            resource_id = item.get("id")
            resource_name = item.get("nm") or ""
            if not isinstance(rep, dict):
                continue
            for tpl in rep.values():
                if not isinstance(tpl, dict):
                    continue
                tpl_copy: Dict[str, Any] = dict(tpl)
                tpl_copy["resource_id"] = resource_id
                tpl_copy["resource_name"] = resource_name
                templates.append(tpl_copy)

        for tpl in templates:
            tpl["name"] = (tpl.get("name") or tpl.get("n") or "").strip()
            tpl["resource_name"] = (
                tpl.get("resource_name")
                or tpl.get("res_name")
                or tpl.get("rname")
                or ""
            ).strip()
            try:
                tpl["template_id"] = int(
                    tpl.get("template_id") or tpl.get("id") or tpl.get("i") or 0
                )
            except Exception:
                tpl["template_id"] = 0
            try:
                tpl["resource_id"] = int(tpl.get("resource_id") or tpl.get("rid") or 0)
            except Exception:
                tpl["resource_id"] = 0

        templates.sort(key=_report_template_sort_key)

        self._report_templates_cache = (time.time(), [dict(tpl) for tpl in templates])
        return templates

    @staticmethod
    def _resolve_format(fmt: str) -> Tuple[str, int, str]:
        fmt_cf = (fmt or "").strip().lower()
        if fmt_cf == "pdf":
            return "pdf", 2, "pdf"
        if fmt_cf in ("excel", "xlsx"):
            return "xlsx", 8, "xlsx"
        if fmt_cf == "xls":
            return "xls", 4, "xls"
        raise ValueError("Неподдерживаемый формат отчёта")

    def _build_export_params(self, fmt: str, output_name: str) -> Dict[str, Any]:
        _, format_code, _ = self._resolve_format(fmt)
        params = {
            "format": format_code,
            "pageWidth": 0,
            "headings": 1,
            "compress": 0,
            "attachMap": 0,
            "hideMapBasis": 0,
            "coding": "utf8",
            "outputFileName": output_name,
        }
        return params

    @staticmethod
    def _parse_disposition_filename(disposition: str) -> Optional[str]:
        if not disposition:
            return None
        match_utf = re.search(r"filename\*=(?:UTF-8''|utf-8'')([^;]+)", disposition)
        if match_utf:
            try:
                return requests.utils.unquote(match_utf.group(1))
            except Exception:
                return match_utf.group(1)
        match = re.search(r'filename="?([^";]+)"?', disposition)
        if match:
            return match.group(1)
        return None

    def generate_report_file(
        self,
        resource_id: int,
        template_id: int,
        interval_from: int,
        interval_to: int,
        fmt: str,
        cancel_event: Optional[threading.Event] = None,
        object_id: Optional[int] = None,
    ) -> Tuple[str, bytes]:
        fmt_cf = (fmt or "").strip().lower()
        canonical_fmt, _, extension = self._resolve_format(fmt_cf)

        if cancel_event and cancel_event.is_set():
            raise ReportCancelledError()

        report_context = {
            "resource_id": int(resource_id),
            "template_id": int(template_id),
            "object_id": int(object_id) if object_id is not None else 0,
            "from": int(interval_from),
            "to": int(interval_to),
            "sid": (self.sid or "")[-8:],
        }

        try:
            cleanup_params = {
                "reportResourceId": report_context["resource_id"],
                "reportTemplateId": report_context["template_id"],
            }
            cleanup_resp = self.request("report/cleanup_result", cleanup_params)
            if isinstance(cleanup_resp, dict) and cleanup_resp.get("error") not in (None, 0):
                log.warning(
                    "report/cleanup_result returned error %s for context %s",
                    cleanup_resp.get("error"),
                    report_context,
                )
        except Exception as exc:
            log.warning("report/cleanup_result failed for %s: %s", report_context, exc)

        params_exec = {
            "reportResourceId": report_context["resource_id"],
            "reportTemplateId": report_context["template_id"],
            "reportObjectId": report_context["object_id"],
            "reportObjectSecId": 0,
            "interval": {
                "from": report_context["from"],
                "to": report_context["to"],
                "flags": 0,
            },
        }
        exec_result = self.request("report/exec_report", params_exec)
        if isinstance(exec_result, dict) and exec_result.get("error"):
            raise RuntimeError(
                f"Wialon error {exec_result['error']} in report/exec_report: {exec_result}"
            )

        if cancel_event and cancel_event.is_set():
            raise ReportCancelledError()

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_name = f"report_{template_id}_{timestamp}"
        export_params = self._build_export_params(canonical_fmt, output_name)
        export_params.update(
            {
                "reportResourceId": report_context["resource_id"],
                "reportTemplateId": report_context["template_id"],
                "reportObjectId": report_context["object_id"],
            }
        )

        wait_messages = {"report not ready", "report has not been saved"}
        deadline = time.monotonic() + 60
        delay = 0.5
        attempt = 0

        while time.monotonic() < deadline:
            if cancel_event and cancel_event.is_set():
                raise ReportCancelledError()

            attempt += 1
            params = {
                "svc": "report/export_result",
                "params": json.dumps(export_params, ensure_ascii=False),
                "sid": self.sid or "",
            }

            with self._session_scope() as session:
                response = session.get(
                    self.base_url,
                    params=params,
                    timeout=(HTTP_TIMEOUT_CONN, max(HTTP_TIMEOUT_READ, 60.0)),
                )
            content_type = (response.headers.get("Content-Type") or "").lower()
            disposition = response.headers.get("Content-Disposition") or ""
            log.debug(
                "report/export_result attempt %s, sid=%s, content_type=%s, disposition=%s",
                attempt,
                (self.sid or "")[-8:],
                content_type,
                disposition,
            )

            if "attachment" in disposition.lower() or (
                "application" in content_type and "json" not in content_type and "html" not in content_type
            ) or any(ext in content_type for ext in ("pdf", "ms-excel", "spreadsheet")):
                filename = self._parse_disposition_filename(disposition)
                if not filename:
                    filename = f"{output_name}.{extension}"
                if cancel_event and cancel_event.is_set():
                    raise ReportCancelledError()
                return filename, response.content

            text_payload = (response.text or "").strip()
            lower_text = text_payload.lower()
            if any(msg in lower_text for msg in wait_messages):
                log.info(
                    "report/export_result not ready yet (attempt %s) for %s: %s",
                    attempt,
                    report_context,
                    text_payload[:200],
                )
            elif "json" in content_type or (text_payload.startswith("{") and text_payload.endswith("}")):
                try:
                    payload = response.json()
                except ValueError:
                    payload = {}
                error_code = payload.get("error") if isinstance(payload, dict) else None
                if error_code in (1001, 1002, 1003):
                    log.info(
                        "report/export_result waiting (error %s) attempt %s for %s",
                        error_code,
                        attempt,
                        report_context,
                    )
                elif error_code:
                    raise RuntimeError(
                        f"Wialon error {error_code} in report/export_result: {payload}"
                    )
                else:
                    log.info(
                        "report/export_result returned JSON without file on attempt %s: %s",
                        attempt,
                        payload,
                    )
            else:
                log.info(
                    "Unexpected report/export_result response (attempt %s): ct=%s body=%s",
                    attempt,
                    content_type,
                    text_payload[:200],
                )

            now = time.monotonic()
            if now >= deadline:
                break
            remaining = max(0.0, deadline - now)
            sleep_time = min(delay, remaining)
            delay = min(delay * 2, 5.0)
            if cancel_event:
                if cancel_event.wait(sleep_time):
                    raise ReportCancelledError()
            else:
                time.sleep(sleep_time)

        raise RuntimeError(
            "Не удалось подготовить файл отчёта за отведённое время."
        )

    # ---- Поиск юнитов ----
    def search_units(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        spec = {
            "itemsType": "avl_unit",
            "propName": "sys_name,profilefield",
            "propValueMask": f"*{query}*,*{query}*",
            "sortType": "sys_name",
            "propType": "property,profilefield",
            "or_logic": 1,
        }
        # 5121 = base (1) + last message (1024) + sensors (4096)
        params = {"spec": spec, "force": 1, "flags": 5121, "from": 0, "to": max(1, limit)}
        data = self.request("core/search_items", params)
        items = data.get("items", []) if isinstance(data, dict) else []
        results: List[Dict[str, Any]] = []
        for it in items:
            entry: Dict[str, Any] = {"id": it.get("id"), "nm": it.get("nm")}
            pos = it.get("pos") if isinstance(it, dict) else None
            if isinstance(pos, dict):
                entry["pos"] = dict(pos)
            lmsg = it.get("lmsg") if isinstance(it, dict) else None
            if isinstance(lmsg, dict):
                entry["lmsg"] = dict(lmsg)
            results.append(entry)
        return results

    # ---- Для CF ----
    def get_unit_with_fields(self, unit_id: int) -> Dict[str, Any]:
        data = self.request("core/search_item", {"id": unit_id, "flags": UNIT_FLAGS_BASE_PLUS_FIELDS})
        item = data.get("item", {}) if isinstance(data, dict) else {}
        return {"id": item.get("id"), "nm": item.get("nm"), "flds": item.get("flds", {})}

    @staticmethod
    def _normalize_custom_fields_block(raw_block: Any) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        if isinstance(raw_block, dict):
            for seq_key, payload in raw_block.items():
                if not isinstance(payload, dict):
                    continue
                entry: Dict[str, Any] = {"seq": str(seq_key)}
                raw_id = payload.get("id")
                try:
                    entry["id"] = int(raw_id)
                except (TypeError, ValueError):
                    entry["id"] = raw_id
                raw_name = payload.get("n")
                if isinstance(raw_name, str):
                    name = raw_name.strip()
                elif raw_name is None:
                    name = ""
                else:
                    name = str(raw_name).strip()
                entry["name"] = name
                raw_value = payload.get("v")
                if isinstance(raw_value, str):
                    value = raw_value.strip()
                elif raw_value is None:
                    value = ""
                else:
                    value = str(raw_value)
                entry["value"] = value
                entries.append(entry)

        def _sort_key(item: Dict[str, Any]) -> Tuple[int, Any]:
            seq_text = item.get("seq")
            try:
                return (0, int(seq_text))
            except (TypeError, ValueError):
                return (1, seq_text or "")

        entries.sort(key=_sort_key)
        return entries

    def get_unit_custom_fields_snapshot(
        self, unit_id: int, *, include_admin: bool = True
    ) -> Dict[str, Any]:
        flags = UNIT_FLAGS_BASE_PLUS_FIELDS | (UNIT_FLAGS_ADMIN_FIELDS if include_admin else 0)
        data = self.request("core/search_item", {"id": unit_id, "flags": flags})
        item = data.get("item", {}) if isinstance(data, dict) else {}
        return {
            "unit_id": item.get("id"),
            "unit_name": item.get("nm"),
            "custom_fields": self._normalize_custom_fields_block(item.get("flds")),
            "admin_fields": self._normalize_custom_fields_block(item.get("aflds")) if include_admin else [],
        }

    # ---- Для статистики ----
    def get_unit_full_for_stats(self, unit_id: int) -> Dict[str, Any]:
        data = self.request("core/search_item", {"id": unit_id, "flags": UNIT_FLAGS_STATS})
        return data.get("item", {}) if isinstance(data, dict) else {}

    def get_unit_sensors_detailed(self, unit_id: int) -> List[Dict[str, Any]]:
        payload = self.request("core/search_item", {"id": unit_id, "flags": UNIT_FLAGS_SENSORS})
        item = payload.get("item", {}) if isinstance(payload, dict) else {}
        sens_block = item.get("sens") or []
        if isinstance(sens_block, dict):
            iterable = sens_block.values()
        elif isinstance(sens_block, list):
            iterable = sens_block
        else:
            iterable = []
        sensors: List[Dict[str, Any]] = []
        for entry in iterable:
            if isinstance(entry, dict):
                sensors.append(dict(entry))
        return sensors

    def _geocode_host_component(self) -> str:
        parsed = urlsplit(self.host)
        netloc = parsed.netloc
        path = (parsed.path or "").strip("/")
        if netloc:
            if path:
                return f"{netloc}/{path}"
            return netloc
        host = (parsed.path or parsed.geturl() or self.host or "").strip("/")
        return host

    @staticmethod
    def _extract_address_from_geocoder(data: Any) -> Optional[str]:
        candidates: List[str] = []

        def _collect(obj: Any) -> None:
            if isinstance(obj, dict):
                for key in ("address", "addr", "name", "result", "value", "label"):
                    val = obj.get(key)
                    if isinstance(val, str) and val.strip():
                        candidates.append(val.strip())
                for key in ("objects", "list", "items", "value", "results"):
                    nested = obj.get(key)
                    if isinstance(nested, (list, tuple)):
                        for item in nested:
                            _collect(item)
            elif isinstance(obj, (list, tuple)):
                for item in obj:
                    _collect(item)
            elif isinstance(obj, str) and obj.strip():
                candidates.append(obj.strip())

        _collect(data)
        return candidates[0] if candidates else None

    def _reverse_geocode_uncached(self, lat: float, lon: float) -> str:
        if not self.sid:
            self.login()
        host_component = self._geocode_host_component()
        if not host_component:
            return ""
        coords_payload = json.dumps([{"lon": lon, "lat": lat}], ensure_ascii=False)
        url = f"https://geocode-maps.wialon.com/{host_component}/gis_geocode"
        params = {"coords": coords_payload, "uid": self.sid}
        geocode_started = time.perf_counter()
        try:
            with self._session_scope() as session:
                resp = session.get(
                    url,
                    params=params,
                    timeout=(HTTP_TIMEOUT_CONN, HTTP_TIMEOUT_READ),
                )
        except Exception as exc:
            log.debug(
                "gis_geocode request failed after %.3fs: %s",
                time.perf_counter() - geocode_started,
                exc,
            )
            return ""
        try:
            resp.raise_for_status()
        except Exception as exc:
            log.debug(
                "gis_geocode http error after %.3fs: %s",
                time.perf_counter() - geocode_started,
                exc,
            )
            return ""
        try:
            data = resp.json()
        except ValueError as exc:
            log.debug(
                "gis_geocode decode failed after %.3fs: %s",
                time.perf_counter() - geocode_started,
                exc,
            )
            return ""
        duration = time.perf_counter() - geocode_started
        result = self._extract_address_from_geocoder(data) or ""
        log.debug("gis_geocode completed in %.3fs (result=%s)", duration, bool(result))
        return result

    def reverse_geocode(self, lat: float, lon: float) -> Optional[str]:
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (TypeError, ValueError):
            return None
        if not (math.isfinite(lat_f) and math.isfinite(lon_f)):
            return None
        bucket_lat, bucket_lon, bucket = _geocode_bucket_key(lat_f, lon_f)
        cache_key = (self._geocode_cache_id, bucket_lat, bucket_lon, bucket)
        _GEOCODE_CLIENTS[self._geocode_cache_id] = self
        try:
            cached = _geocode_cached(cache_key)
        except Exception:
            cached = self._reverse_geocode_uncached(bucket_lat, bucket_lon)
        return cached or None

    @staticmethod
    def _parse_zone_data_response(data: Any) -> Dict[int, Dict[str, Any]]:
        result: Dict[int, Dict[str, Any]] = {}
        if not data:
            return result
        containers: List[Dict[Any, Any]] = []
        if isinstance(data, dict):
            for key in ("zones", "zl", "items"):
                value = data.get(key)
                if isinstance(value, dict):
                    containers.append(value)
            if not containers:
                containers.append(data)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    zid = item.get("id")
                    try:
                        zid_int = int(zid)
                    except Exception:
                        continue
                    result[zid_int] = item
            return result
        for container in containers:
            for key, value in container.items():
                try:
                    zid_int = int(key)
                except Exception:
                    continue
                if isinstance(value, dict):
                    result[zid_int] = value
                else:
                    result[zid_int] = {"n": value}
        return result

    @staticmethod
    def _extract_zone_center(zone_data: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        if not isinstance(zone_data, dict):
            return None, None
        candidates = []
        for key in ("ct", "c", "center"):
            val = zone_data.get(key)
            if isinstance(val, dict):
                candidates.append(val)
        for center in candidates:
            lat = center.get("y") if isinstance(center, dict) else None
            lon = center.get("x") if isinstance(center, dict) else None
            if lat is None and isinstance(center, dict):
                lat = center.get("lat")
            if lon is None and isinstance(center, dict):
                lon = center.get("lon")
            try:
                if lat is not None and lon is not None:
                    return float(lat), float(lon)
            except Exception:
                continue
        points = zone_data.get("p") or zone_data.get("points")
        if isinstance(points, list) and points:
            sum_lat = 0.0
            sum_lon = 0.0
            count = 0
            for pt in points:
                if not isinstance(pt, dict):
                    continue
                lat = pt.get("y") if isinstance(pt, dict) else None
                lon = pt.get("x") if isinstance(pt, dict) else None
                if lat is None and isinstance(pt, dict):
                    lat = pt.get("lat")
                if lon is None and isinstance(pt, dict):
                    lon = pt.get("lon")
                try:
                    if lat is not None and lon is not None:
                        sum_lat += float(lat)
                        sum_lon += float(lon)
                        count += 1
                except Exception:
                    continue
            if count:
                return sum_lat / count, sum_lon / count
        return None, None

    def _zone_all_params(
        self, resource_id: int, zone_ids: Optional[List[int]], flags: int
    ) -> Dict[str, Any]:
        if zone_ids:
            return {
                "itemId": int(resource_id),
                "col": [int(z) for z in zone_ids],
                "flags": int(flags),
            }

        strategy = self._zone_all_strategy or self.GEO_ZONE_ALL_STRATEGY or "omit"
        if strategy == "omit":
            return {"itemId": int(resource_id), "flags": int(flags)}
        if strategy == "null":
            return {"itemId": int(resource_id), "col": None, "flags": int(flags)}
        return {"itemId": int(resource_id), "col": [], "flags": int(flags)}

    def _detect_zone_all_strategy(self, sample_resource_id: int, flags: int) -> str:
        trials = [
            ("omit", {"itemId": int(sample_resource_id), "flags": int(flags)}),
            (
                "null",
                {"itemId": int(sample_resource_id), "col": None, "flags": int(flags)},
            ),
            (
                "empty",
                {"itemId": int(sample_resource_id), "col": [], "flags": int(flags)},
            ),
        ]
        for name, params in trials:
            try:
                data = self.request("resource/get_zone_data", params)
                parsed = self._parse_zone_data_response(data)
                geo_log.info(
                    "zone_detect: strategy=%s zones=%s", name, len(parsed)
                )
                if parsed:
                    return name
            except Exception as exc:
                geo_log.warning("zone_detect: strategy=%s err=%r", name, exc)
        return "omit"

    def _request_zone_data(
        self, resource_id: int, zone_ids: Optional[List[int]], flags: int
    ) -> Dict[int, Dict[str, Any]]:
        params = self._zone_all_params(resource_id, zone_ids, flags)
        strategy_label = "explicit"
        if not zone_ids:
            if "col" not in params:
                strategy_label = "omit"
            else:
                col_value = params.get("col")
                if col_value is None:
                    strategy_label = "null"
                elif col_value == []:
                    strategy_label = "empty"
        started = time.perf_counter()
        resp = self.request("resource/get_zone_data", params)
        duration = time.perf_counter() - started
        parsed = self._parse_zone_data_response(resp)
        geo_log.debug(
            "zone_cache: get_zone_data item=%s strategy=%s flags=%s zones=%s in %.3fs",
            resource_id,
            strategy_label if not zone_ids else "explicit-list",
            flags,
            len(parsed),
            duration,
        )
        return parsed

    def _filter_zone_cache_by_allowed(
        self, cache: Dict[int, Dict[int, Dict[str, Any]]]
    ) -> Dict[int, Dict[int, Dict[str, Any]]]:
        if not cache:
            return {}
        allowed_raw = [rid for rid in self.get_allowed_rids() if isinstance(rid, int)]
        allowed: Set[int] = {int(rid) for rid in allowed_raw}
        if not allowed:
            geo_log.info("[zones] zone_cache filter -> no allowed rids")
            return {}
        filtered = {rid: dict(zones) for rid, zones in cache.items() if rid in allowed}
        if len(filtered) != len(cache):
            geo_log.debug(
                "[zones] zone_cache filter: total=%s allowed=%s kept=%s",
                len(cache),
                len(allowed),
                len(filtered),
            )
        return filtered

    def _load_zone_cache(self) -> Optional[Dict[int, Dict[int, Dict[str, Any]]]]:
        global _ZONE_CACHE_TS, _ZONE_CACHE_DATA
        self._ensure_geo_file_updater()
        now = time.time()
        with _ZONE_CACHE_LOCK:
            cache_ts = _ZONE_CACHE_TS
            existing_cache = {
                rid: dict(zones)
                for rid, zones in _ZONE_CACHE_DATA.items()
                if isinstance(zones, dict)
            }
        cache_age = now - cache_ts if cache_ts else None
        if existing_cache and cache_age is not None and cache_age < ZONE_DATA_CACHE_TTL:
            return self._filter_zone_cache_by_allowed(existing_cache)

        payload = ZONE_STORE.load_if_present()
        if isinstance(payload, dict):
            zone_cache: Dict[int, Dict[int, Dict[str, Any]]] = {}
            zone_ids_by_resource = payload.get("zone_ids_by_resource")
            zone_meta_raw = payload.get("zone_meta")
            zone_meta = zone_meta_raw if isinstance(zone_meta_raw, dict) else {}
            if isinstance(zone_ids_by_resource, dict):
                for rid_key, zone_ids in zone_ids_by_resource.items():
                    try:
                        rid_int = int(rid_key)
                    except Exception:
                        continue
                    zones_map: Dict[int, Dict[str, Any]] = {}
                    if isinstance(zone_ids, list):
                        for zid in zone_ids:
                            try:
                                zid_int = int(zid)
                            except Exception:
                                continue
                            meta_key = f"{rid_int}:{zid_int}"
                            meta = zone_meta.get(meta_key) if isinstance(zone_meta, dict) else {}
                            name_val = None
                            ct_val = None
                            if isinstance(meta, dict):
                                name_val = meta.get("n") or meta.get("name")
                                ct_val = meta.get("ct")
                            if not isinstance(name_val, str) or not name_val.strip():
                                name_val = f"ID {zid_int}"
                            zones_map[zid_int] = {"n": name_val.strip(), "ct": ct_val}
                    if zones_map:
                        zone_cache[rid_int] = zones_map
            strategy_val = payload.get("strategy")
            if isinstance(strategy_val, str) and strategy_val:
                self._zone_all_strategy = strategy_val

            with _ZONE_CACHE_LOCK:
                _ZONE_CACHE_DATA = {
                    rid: dict(zones) for rid, zones in zone_cache.items()
                }
                _ZONE_CACHE_TS = time.time()
                refreshed = {
                    rid: dict(zones)
                    for rid, zones in _ZONE_CACHE_DATA.items()
                    if isinstance(zones, dict)
                }
            if refreshed:
                return self._filter_zone_cache_by_allowed(refreshed)
            return {}

        if existing_cache:
            return self._filter_zone_cache_by_allowed(existing_cache)
        return None

    def _update_zone_cache(self, resource_id: int, updates: Dict[int, Dict[str, Any]]) -> None:
        if not updates:
            return
        global _ZONE_CACHE_TS, _ZONE_CACHE_DATA
        with _ZONE_CACHE_LOCK:
            resource_cache = _ZONE_CACHE_DATA.setdefault(resource_id, {})
            for zid, payload in updates.items():
                if isinstance(payload, dict):
                    resource_cache[zid] = payload
            _ZONE_CACHE_TS = max(_ZONE_CACHE_TS, time.time())

    def get_zone_center(self, resource_id: int, zone_id: int) -> Optional[Tuple[float, float]]:
        try:
            rid_int = int(resource_id)
            zid_int = int(zone_id)
        except Exception:
            return None
        cached_zone: Optional[Dict[str, Any]] = None
        with _ZONE_CACHE_LOCK:
            resource_cache = _ZONE_CACHE_DATA.get(rid_int, {})
            zone_info = resource_cache.get(zid_int)
            if isinstance(zone_info, dict):
                cached_zone = dict(zone_info)
        if isinstance(cached_zone, dict):
            lat, lon = self._extract_zone_center(cached_zone)
            if lat is not None and lon is not None:
                return lat, lon
        try:
            fetched = self._request_zone_data(rid_int, [zid_int], 20)
        except Exception as exc:
            geo_log.debug(
                "Failed to fetch zone center for resource %s zone %s: %s",
                rid_int,
                zid_int,
                exc,
            )
            return None
        if not fetched:
            return None
        self._update_zone_cache(rid_int, fetched)
        zone_data = fetched.get(zid_int)
        if not isinstance(zone_data, dict):
            return None
        lat, lon = self._extract_zone_center(zone_data)
        if lat is None or lon is None:
            return None
        return lat, lon

    def get_unit_geozone_details(
        self, unit_id: int, lat: Optional[float], lon: Optional[float]
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {"names": [], "hits": {}, "entries": []}
        if lat is None or lon is None:
            return result

        hits = self.find_zones_for_unit(unit_id, lat, lon)
        result["hits"] = hits
        if not hits:
            return result

        names = self.resolve_zone_names(hits)
        result["names"] = names

        entries_by_name: Dict[str, Dict[str, Any]] = {}
        for (rid, zid), entry in self._last_zone_details.items():
            if not isinstance(entry, dict):
                continue
            name = (entry.get("name") or "").strip()
            if not name:
                continue
            name_cf = name.casefold()
            if name_cf not in entries_by_name:
                entries_by_name[name_cf] = {
                    "resource_id": rid,
                    "zone_id": zid,
                    "name": name,
                    "lat": entry.get("lat"),
                    "lon": entry.get("lon"),
                }

        ordered_entries: List[Dict[str, Any]] = []
        for name in names:
            name_cf = name.casefold()
            entry = entries_by_name.get(name_cf)
            if entry:
                ordered_entries.append(dict(entry))

        result["entries"] = ordered_entries
        return result

    # ---- Custom fields helpers ----
    def _find_custom_field_id(self, flds: Dict[str, Any], name_lower: str) -> Optional[int]:
        if not flds:
            return None
        for _, fld in flds.items():
            n = (fld.get("n") or "").strip().lower()
            if n == name_lower:
                return int(fld.get("id"))
        return None

    def update_custom_field(self, unit_id: int, name: str, value: str) -> Tuple[str, int]:
        unit = self.get_unit_with_fields(unit_id)
        existing_id = self._find_custom_field_id(unit.get("flds", {}), name.strip().lower())

        if existing_id is None:
            params = {"itemId": unit_id, "id": 0, "callMode": "create", "n": name, "v": value}
            data = self.request("item/update_custom_field", params)
            if isinstance(data, list) and data:
                return "create", int(data[0])
            if isinstance(data, dict) and data.get("error") == 7:
                raise PermissionError("Недостаточно прав ADF_ACL_ITEM_EDIT_CFIELDS.")
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(f"Wialon error {data['error']} in item/update_custom_field create")
            raise RuntimeError(f"Неожиданный ответ при создании поля: {data}")
        else:
            params = {"itemId": unit_id, "id": existing_id, "callMode": "update", "n": name, "v": value}
            data = self.request("item/update_custom_field", params)
            if isinstance(data, list) and data:
                return "update", existing_id
            if isinstance(data, dict) and data.get("error") == 7:
                raise PermissionError("Недостаточно прав ADF_ACL_ITEM_EDIT_CFIELDS.")
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(f"Wialon error {data['error']} in item/update_custom_field update")
            raise RuntimeError(f"Неожиданный ответ при обновлении поля: {data}")

    def delete_custom_field(self, unit_id: int, field_id: int) -> None:
        params = {"itemId": unit_id, "id": field_id, "callMode": "delete"}
        data = self.request("item/update_custom_field", params)
        if isinstance(data, dict) and data.get("error"):
            if data.get("error") == 7:
                raise PermissionError("Недостаточно прав ADF_ACL_ITEM_EDIT_CFIELDS.")
            raise RuntimeError(f"Wialon error {data['error']} in item/update_custom_field delete")

    # ---- Статистика: ДУТ ----
    @staticmethod
    def _pick_fuel_sensor(unit_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        candidates = list_dut_candidates(unit_item or {})
        primary = select_primary_dut(candidates)
        chosen: Optional[SensorMeta] = None
        if primary:
            chosen = primary
        elif candidates:
            chosen = next((meta for meta in candidates if not meta.is_zero_value), candidates[0])
        return chosen.raw_sensor if isinstance(chosen, SensorMeta) else None

    def load_last_message(self, unit_id: int) -> None:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        params = {"itemId": unit_id, "lastTime": now_ts, "lastCount": 1, "flags": 0, "flagsMask": 0, "loadCount": 1}
        data = self.request("messages/load_last", params)
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(f"Wialon error {data['error']} in messages/load_last")

    def calc_sensor_value(self, unit_id: int, sensor_id: int) -> Optional[float]:
        params = {"source": "", "indexFrom": 0, "indexTo": 1, "unitId": unit_id, "sensorId": int(sensor_id)}
        data = self.request("unit/calc_sensors", params)
        if not isinstance(data, list) or not data:
            return None
        values_map = data[0] if isinstance(data[0], dict) else {}
        key = str(sensor_id)
        val = values_map.get(key)
        try:
            return float(val) if val is not None else None
        except Exception:
            return None

    def load_messages_interval(
        self,
        unit_id: int,
        time_from: int,
        time_to: int,
        *,
        flags: int = 0x0000,
        flags_mask: int = 0xFF00,
        load_count: int = 4294967295,
    ) -> Dict[str, Any]:
        params = {
            "itemId": int(unit_id),
            "timeFrom": int(time_from),
            "timeTo": int(time_to),
            "flags": int(flags),
            "flagsMask": int(flags_mask),
            "loadCount": int(load_count),
        }
        return self.request("messages/load_interval", params)

    def get_loaded_messages(
        self,
        unit_id: int,
        index_from: int,
        index_to: int,
    ) -> Any:
        params = {
            "itemId": int(unit_id),
            "indexFrom": int(max(0, index_from)),
            "indexTo": int(max(0, index_to)),
        }
        return self.request("messages/get_messages", params)

    def unload_messages(self, unit_id: Optional[int] = None) -> None:
        params: Dict[str, Any] = {}
        if unit_id is not None:
            try:
                params["itemId"] = int(unit_id)
            except Exception:
                params["itemId"] = unit_id
        try:
            self.request("messages/unload", params)
        except Exception as exc:
            log.debug("messages/unload failed: %s", exc)

    def calc_sensor_series(
        self,
        unit_id: int,
        sensor_id: Optional[int],
        *,
        width: Optional[int] = None,
        index_from: int = 0,
        index_to: Optional[int] = None,
    ) -> Union[List[Tuple[int, float]], Any]:
        try:
            idx_from = max(0, int(index_from))
        except Exception:
            idx_from = 0
        try:
            idx_to = int(index_to) if index_to is not None else 0
        except Exception:
            idx_to = 0
        params: Dict[str, Any] = {
            "source": "",
            "indexFrom": idx_from,
            "indexTo": idx_to,
            "unitId": int(unit_id),
        }
        if isinstance(sensor_id, (list, tuple, set)):
            raise ValueError("calc_sensor_series does not accept multiple sensor IDs; use sensorId=0 instead")
        if sensor_id is None:
            params["sensorId"] = 0
        else:
            params["sensorId"] = int(sensor_id)
        if width is not None:
            try:
                params["width"] = max(1, int(width))
            except Exception:
                params["width"] = width
        self._last_calc_series_params = dict(params)
        data = self.request("unit/calc_sensors", params)
        self._last_calc_series_raw = data
        sensor_id_value = params.get("sensorId")
        if sensor_id_value == 0:
            return data
        return self._extract_sensor_series(data, int(sensor_id_value))

    @staticmethod
    def _extract_sensor_series(data: Any, sensor_id: int) -> List[Tuple[int, float]]:
        points: Dict[int, float] = {}

        def _normalize_timestamp(value: Any, depth: int = 0) -> Optional[int]:
            if depth > 3:
                return None
            if isinstance(value, (int, float)):
                try:
                    return int(float(value))
                except Exception:
                    return None
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return None
                try:
                    return int(float(text))
                except Exception:
                    return None
            if isinstance(value, dict):
                for key in ("x", "t", "time", "ts", "timestamp", "left", "right"):
                    if key in value:
                        ts_candidate = _normalize_timestamp(value[key], depth + 1)
                        if ts_candidate is not None:
                            return ts_candidate
                for inner in value.values():
                    ts_candidate = _normalize_timestamp(inner, depth + 1)
                    if ts_candidate is not None:
                        return ts_candidate
            if isinstance(value, (list, tuple)):
                for item in value:
                    ts_candidate = _normalize_timestamp(item, depth + 1)
                    if ts_candidate is not None:
                        return ts_candidate
            return None

        def _normalize_value(value: Any, depth: int = 0) -> Optional[float]:
            if depth > 3:
                return None
            if isinstance(value, (int, float)):
                try:
                    return float(value)
                except Exception:
                    return None
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return None
                if "," in text and "." not in text:
                    text = text.replace(",", ".")
                try:
                    return float(text)
                except Exception:
                    return None
            if isinstance(value, dict):
                for key in (
                    "v",
                    "value",
                    "val",
                    "avg",
                    "y",
                    "top",
                    "bottom",
                    "min",
                    "max",
                    "1",
                    str(sensor_id),
                ):
                    if key in value:
                        candidate = _normalize_value(value[key], depth + 1)
                        if candidate is not None:
                            return candidate
                for inner in value.values():
                    candidate = _normalize_value(inner, depth + 1)
                    if candidate is not None:
                        return candidate
            if isinstance(value, (list, tuple)):
                for item in value:
                    candidate = _normalize_value(item, depth + 1)
                    if candidate is not None:
                        return candidate
            return None

        def _coerce_point(candidate: Any) -> None:
            if isinstance(candidate, (list, tuple)):
                if len(candidate) < 2:
                    return
                ts_raw, val_raw = candidate[0], candidate[1]
            elif isinstance(candidate, dict):
                ts_raw = (
                    candidate.get("x")
                    or candidate.get("t")
                    or candidate.get("time")
                    or candidate.get("ts")
                )
                if ts_raw is None:
                    ts_raw = candidate
                val_raw = (
                    candidate.get("y")
                    or candidate.get("v")
                    or candidate.get("value")
                    or candidate.get("val")
                )
                if val_raw is None:
                    val_raw = candidate
            else:
                return

            ts_val = _normalize_timestamp(ts_raw)
            if ts_val is None:
                return
            val_val = _normalize_value(val_raw)
            if val_val is None:
                return
            points[ts_val] = val_val

        def _walk(obj: Any) -> None:
            if obj is None:
                return
            if isinstance(obj, (list, tuple, set)):
                _coerce_point(obj)
                for item in obj:
                    _walk(item)
                return
            if isinstance(obj, dict):
                sid_key = str(sensor_id)
                if sid_key in obj and isinstance(obj[sid_key], (list, tuple, dict)):
                    _walk(obj[sid_key])
                _coerce_point(obj)
                for key in (
                    "values",
                    "value",
                    "result",
                    "results",
                    "data",
                    "items",
                    "list",
                    "series",
                    "segments",
                ):
                    if key in obj:
                        _walk(obj[key])
                for key in ("left", "right", "bottom", "top", "avg", "min", "max", "first", "last"):
                    if key in obj:
                        _walk(obj[key])
                for key, value in obj.items():
                    if key in {"error", "sensorId", "unitId", "type", "name", "sensor", "u"}:
                        continue
                    if isinstance(key, str) and key.isdigit() and int(key) != sensor_id:
                        continue
                    if isinstance(value, (list, tuple, dict, set)):
                        _walk(value)
                return
            _coerce_point(obj)

        _walk(data)
        ordered = sorted(points.items(), key=lambda item: item[0])
        return [(ts, val) for ts, val in ordered]

    def batch_calc_sensor_values(
        self,
        pairs: List[Tuple[int, int]],
        *,
        chunk_size: int = 50,
    ) -> Dict[Tuple[int, int], Optional[float]]:
        setattr(self, "_last_batch_calc_call_count", 0)
        setattr(self, "_last_batch_calc_total_pairs", len(pairs))
        if not pairs:
            return {}
        try:
            chunk_size = max(1, int(chunk_size))
        except Exception:
            chunk_size = 50

        results: Dict[Tuple[int, int], Optional[float]] = {}
        total_pairs = len(pairs)
        chunk_index = 0
        batch_requests = 0
        for start in range(0, total_pairs, chunk_size):
            self._check_cancelled()
            chunk_index += 1
            chunk = pairs[start : start + chunk_size]
            calls: List[Dict[str, Any]] = []
            for unit_id, sensor_id in chunk:
                try:
                    uid = int(unit_id)
                    sid = int(sensor_id)
                except Exception:
                    results[(unit_id, sensor_id)] = None
                    continue
                calls.append(
                    {
                        "svc": "unit/calc_sensors",
                        "params": {
                            "source": "",
                            "indexFrom": 0,
                            "indexTo": 1,
                            "unitId": uid,
                            "sensorId": sid,
                        },
                    }
                )

            if not calls:
                continue

            attempt = 0
            backoff = DUT_BATCH_RETRY_INITIAL_DELAY
            data: Any = None
            while True:
                self._check_cancelled()
                attempt += 1
                timer = _GeoTimer(f"dut:batch_calc[{chunk_index}]#{attempt}")
                try:
                    batch_requests += 1
                    data = self.request("core/batch", {"calls": calls})
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    label, elapsed = timer.done()
                    sleep_time = min(backoff, DUT_BATCH_RETRY_MAX_DELAY)
                    log.warning(
                        "dut batch chunk %s retry attempt=%s sleep=%.1fs err=%s",
                        chunk_index,
                        attempt,
                        sleep_time,
                        exc,
                    )
                    self.sid = None
                    try:
                        self.login()
                    except Exception as auth_exc:
                        log.debug(
                            "dut batch chunk %s login retry failed: %s",
                            chunk_index,
                            auth_exc,
                        )
                    self._sleep_with_cancel(sleep_time)
                    backoff = min(backoff * 2, DUT_BATCH_RETRY_MAX_DELAY)
                    self._check_cancelled()
                    continue
                else:
                    label, elapsed = timer.done()
                    log.info(
                        "dut batch chunk %s: size=%s elapsed=%.3fs attempt=%s",
                        chunk_index,
                        len(calls),
                        elapsed,
                        attempt,
                    )
                    break

            if data is None:
                continue

            if not isinstance(data, list):
                log.debug(
                    "dut batch chunk %s unexpected response type: %s",
                    chunk_index,
                    type(data).__name__,
                )
                for entry in chunk:
                    results.setdefault((entry[0], entry[1]), None)
                continue

            for idx, pair in enumerate(chunk):
                unit_id, sensor_id = pair
                value: Optional[float] = None
                entry = data[idx] if idx < len(data) else None
                if isinstance(entry, dict) and entry.get("error") not in (None, 0):
                    log.debug(
                        "dut batch element error unit=%s sensor=%s err=%s",
                        unit_id,
                        sensor_id,
                        entry.get("error"),
                    )
                elif isinstance(entry, list) and entry:
                    values_map = entry[0] if isinstance(entry[0], dict) else {}
                    raw = values_map.get(str(sensor_id)) if isinstance(values_map, dict) else None
                    try:
                        value = float(raw) if raw is not None else None
                    except Exception:
                        value = None
                elif isinstance(entry, dict) and "result" in entry:
                    payload = entry.get("result")
                    if isinstance(payload, list) and payload:
                        values_map = payload[0] if isinstance(payload[0], dict) else {}
                        raw = values_map.get(str(sensor_id)) if isinstance(values_map, dict) else None
                        try:
                            value = float(raw) if raw is not None else None
                        except Exception:
                            value = None
                else:
                    log.debug(
                        "dut batch element unexpected entry unit=%s sensor=%s type=%s",
                        unit_id,
                        sensor_id,
                        type(entry).__name__ if entry is not None else None,
                    )
                results[(unit_id, sensor_id)] = value

        log.info(
            "dut batch finished: total_pairs=%s chunks=%s",
            total_pairs,
            chunk_index,
        )
        setattr(self, "_last_batch_calc_call_count", batch_requests)
        return results

    # ---- Команды ----
    def send_custom_tcp(self, unit_id: int, text: str, timeout: int = 10) -> Dict[str, Any]:
        params = {
            "itemId": int(unit_id),
            "commandType": "custom_msg",
            "commandName": "custom_msg",
            "linkType": "tcp",
            "param": str(text),  # без жёсткой проверки на пустоту
            "timeout": int(timeout),
            "flags": 0,
        }
        data = self.request("unit/send_cmd", params)
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError("send_cmd_failed")
        return data
