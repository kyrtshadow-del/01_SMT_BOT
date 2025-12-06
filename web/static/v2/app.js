const MAP_KEY = "smt_map_state_v2";
const TILE_URL_DEFAULT = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png";
const TILE_OPTS_DEFAULT = {
  attribution:
    '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
  subdomains: "abcd",
  maxZoom: 20,
};
const SESSION_KEY = "smt_session_v2";
const SESSION_INFO_KEY = "smt_session_info_v1";
const ROW_H = 60;
const OVERSCAN = 6;
const MIN_POOL = 40;
const MAX_POOL = 160;
const FIXED_CARD_KEYS = ["location"];
const CARD_SECTIONS = [
  { key: "sensors", title: "Датчики" },
  { key: "connectivity", title: "Подключение" },
  { key: "counters", title: "Счётчики" },
  { key: "params", title: "Параметры" },
  { key: "custom_fields", title: "Пользовательские поля" },
  { key: "crew", title: "Водители" },
  { key: "nearby", title: "Ближайшие" },
  { key: "recent_events", title: "Последние сообщения" },
];
const CARD_SETTINGS_KEY = "smt_card_sections_v1";
const CARD_ORDER_KEY = "smt_card_order_v1";
const DETAIL_CACHE_TTL_MS = 60000; // longer cache to keep card data warm
const ACTIVE_TAB_KEY = "smt_active_tab_v1";
const TAB_STATE_KEY = "smt_tab_state_v1";
const SIDEBAR_WIDTH_KEY = "smt_sidebar_width_v1";
const RENDER_CARD_KEYS = [...FIXED_CARD_KEYS, ...CARD_SECTIONS.map((s) => s.key)];
const DEFAULT_CARD_SECTIONS = CARD_SECTIONS.reduce((acc, s) => {
  acc[s.key] = true;
  return acc;
}, {});
const DEFAULT_CARD_ORDER = [...RENDER_CARD_KEYS];
// History / track state
let histLayerGroup = null;
let histUnitId = null;
let histDate = new Date();
let trackPointsCache = [];
let trackCursorMarker = null;
function renderKvGrid(items, opts = {}) {
  if (!items || !items.length) return "—";
  return `<div class="kv-grid">${items
    .map((item) => {
      const label = item.label ? `<span class="kv-label">${item.label}</span>` : "";
      const divider = item.label ? '<span class="kv-divider"></span>' : "";
      const value = `<span class="kv-value">${item.value != null ? item.value : "—"}</span>`;
      return `<div class="kv-pill">${label}${divider}${value}</div>`;
    })
    .join("")}</div>`;
}
const DEFAULT_TABS = [{ id: "work", name: "Рабочий", filters: { worklist: true } }];
const FEED_INTERVAL_MS = 10000;
const SIDEBAR_MIN = 260;
const SIDEBAR_MAX = 640;
const quickFilters = {}; // legacy holder; ignition filters вынесены в отдельный state
let connectionFilter = null; // "online" | "stale" | "offline" | null
let motionFilter = null; // "moving" | "stop" | "parking" | null
let ignitionFilter = null; // "on" | "off" | null

function stripIgnitionText(text = "") {
  return text.replace(/,?\s*зажиг\.?\s*(вкл|выкл)/gi, "").trim();
}

function formatUid(uid) {
  if (!uid) return "";
  const s = String(uid);
  if (s.length <= 8) return s;
  return `${s.slice(0, 4)}…${s.slice(-4)}`;
}

function cardSectionTitle(key) {
  const item = CARD_SECTIONS.find((s) => s.key === key);
  if (item) return item.title;
  if (key === "nearby") return "Ближайшие";
  return key;
}

let map;
let cluster;
let markerById = new Map();
let units = [];
let unitsById = new Map();
let worklist = new Set();
let current = [];
let listEl, innerEl;
let pool = [];
let poolSize = 0;
let selectedId = null;
let lastFeedTick = Date.now();
let modalState = { filtered: [], selected: new Set(), showOnMap: false };
let isAdmin = false;
let modalRowById = new Map();
let tooltipEl = null;
let cardEl = null;
let cardSettings = loadCardSettings();
let cardOrder = loadCardOrder();
let orderRowByKey = new Map();
let panelTabs = [...DEFAULT_TABS];
let activeTabId = loadActiveTab();
let tabState = loadTabState();
let currentCardDetail = null;
let panelSettingsReady = false;
let liveTimer = null;
let liveInFlight = false;
let feedSince = null;
let lastLiveUpdated = null;
let sidebarWidth = loadSidebarWidth();
let resizerPointerId = null;
let tabFilterRows = [];
let expandedIndex = null;
let cardHeight = 0;
let cardVisible = false;
let cardNeedsAnchor = false;
const CARD_GAP = 6;
const CARD_ANCHOR_OFFSET = 8;
const DETAIL_CACHE_LIMIT = 300;
let nearbyCardState = { unitId: null, visible: false, list: [] };
const detailCache = new Map(); // unitId -> { ts, data }
let historyChart = null; // Chart.js instance for history graph
let calendarInstance = null;
let calendarCache = {}; // month => {YYYY-MM-DD: dist_m}
let eventMarkersLayer = null;
// Player state
let playerState = {
  animFrame: null,
  idx: 0,
  isPlaying: false,
  marker: null,
  speedMultiplier: 5,
  lastTick: 0,
};
let loginOverlayEl = null;
let loginErrorEl = null;
let loginLoginInput = null;
let loginPasswordInput = null;
// Shadow/Inbox
let mode = "monitor"; // "monitor" | "inbox"
let shadowUnits = [];
let selectedShadow = null;
let shadowLayer = null;
let shadowMarkerByKey = new Map();
let shadowBindSearchTimer = null;
const SHADOW_API_PATHS = ["/web/api/unknown_devices", "/api/unknown_devices"];
// Trip detector modal elements
let tripModal = null;
let tripModeInput = null;
let tripMinSpeedInput = null;
let tripMinParkingInput = null;
let tripMinTripTimeInput = null;
let tripMinTripDistanceInput = null;
let tripMaxGapSecInput = null;
let tripMaxGapMInput = null;
let tripErrorEl = null;
let tripUnitId = null;
let loginSubmitBtn = null;
let loginInitDone = false;
let userLabelEl = null;
let adminBtn = null;
let logoutBtn = null;
let shadowToggleBtn = null;
let adminModal = null;
let adminUsersEl = null;
let adminNodesEl = null;
let adminUnassignedEl = null;
let adminAssignNodeInput = null;
let adminAssignUnitsInput = null;
let adminMsgEl = null;
let feedAlertEl = null;
let cardLayoutEdit = false;
let cardDragKey = null;
let cardPreviewById = new Map();
let detailPrefetchInFlight = new Set();
let detailCacheKeys = [];
let statusPopupsWired = false;
let loginFormEl = null;
let shadowToggleInFlight = false;
let shadowDebugSeq = 0;

function getUnitIconUrl(kind) {
  switch (kind) {
    case "bus":
      return "/static/icons/bus.svg";
    case "van":
      return "/static/icons/van.svg";
    case "truck":
      return "/static/icons/truck.svg";
    case "tractor":
      return "/static/icons/tractor.svg";
    case "combine":
      return "/static/icons/combine.svg";
    case "loader":
      return "/static/icons/loader.svg";
    case "car":
    default:
      return "/static/icons/car.svg";
  }
}

function loadCardSettings() {
  try {
    const raw = localStorage.getItem(CARD_SETTINGS_KEY);
    if (!raw) return { ...DEFAULT_CARD_SECTIONS };
    const parsed = JSON.parse(raw);
    const clean = {};
    Object.keys(DEFAULT_CARD_SECTIONS).forEach((k) => {
      clean[k] = parsed[k] !== undefined ? parsed[k] : DEFAULT_CARD_SECTIONS[k];
    });
    return clean;
  } catch {
    return { ...DEFAULT_CARD_SECTIONS };
  }
}

function saveCardSettings() {
  localStorage.setItem(CARD_SETTINGS_KEY, JSON.stringify(cardSettings));
}

function loadCardOrder() {
  try {
    const raw = localStorage.getItem(CARD_ORDER_KEY);
    if (raw) {
      const arr = JSON.parse(raw);
      if (Array.isArray(arr)) {
        const filtered = arr.filter((k) => DEFAULT_CARD_ORDER.includes(k));
        return filtered.concat(DEFAULT_CARD_ORDER.filter((k) => !filtered.includes(k)));
      }
    }
  } catch {}
  return [...DEFAULT_CARD_ORDER];
}

function saveCardOrder(order) {
  try {
    localStorage.setItem(CARD_ORDER_KEY, JSON.stringify(order));
  } catch {}
}

function buildPreviewDetail(item) {
  const statusDetails = {
    online: item.online,
    status: item.status,
    status_label: item.status_label,
    last_ts: item.last_ts,
    ignition: item.ignition,
    speed: item.speed,
  };
  const paramsObj = item.params || {};
  const paramsArr = Object.entries(paramsObj).slice(0, 8).map(([key, value]) => ({ key, value }));
  return {
    item,
    latest: {
      lat: item.lat,
      lon: item.lon,
      params: paramsObj,
      last_ts: item.last_ts,
    },
    snapshot: {
      lat: item.lat,
      lon: item.lon,
      meta: item.address ? { address: item.address } : {},
      device: { hardware: item.hw },
      uid: item.uid,
    },
    status_details: statusDetails,
    card_data: {
      status: statusDetails,
      location: { lat: item.lat, lon: item.lon, address: item.address || "—" },
      connectivity: { uid: item.uid, hardware: item.hw },
      params: paramsArr,
    },
  };
}

function hydratePreview(preview, item) {
  // Превью из /api/units уже содержит card_data, sensors, counters — используем их без ожидания /units/{id}
  if (!preview || typeof preview !== "object") {
    return buildPreviewDetail(item);
  }
  const cardData = preview.card_data || {};
  const status = preview.status || cardData.status || {
    online: item.online,
    status: item.status,
    status_label: item.status_label,
    last_ts: item.last_ts,
    ignition: item.ignition,
    speed: item.speed,
  };
  const latestParams =
    (preview.latest && preview.latest.params) ||
    Object.fromEntries((cardData.params || []).map((p) => [p.key, p.value]));
  const latest = {
    lat: (preview.latest && preview.latest.lat) || cardData.location?.lat || item.lat,
    lon: (preview.latest && preview.latest.lon) || cardData.location?.lon || item.lon,
    speed: (preview.latest && preview.latest.speed) || status.speed || item.speed,
    params: latestParams,
    last_ts: (preview.latest && preview.latest.last_ts) || status.last_ts || item.last_ts,
  };
  const snapshot = preview.snapshot || {
    lat: latest.lat,
    lon: latest.lon,
    meta: cardData.location?.address ? { address: cardData.location.address } : {},
  };
  return {
    item,
    latest,
    snapshot,
    status_details: status,
    card_data: cardData,
  };
}

function cacheDetail(id, data) {
  const now = Date.now();
  if (detailCache.has(id)) {
    detailCache.delete(id);
    detailCacheKeys = detailCacheKeys.filter((k) => k !== id);
  }
  detailCache.set(id, { ts: now, data });
  detailCacheKeys.push(id);
  if (detailCacheKeys.length > DETAIL_CACHE_LIMIT) {
    const oldest = detailCacheKeys.shift();
    if (oldest !== undefined) {
      detailCache.delete(oldest);
    }
  }
}

function resetCardSettings() {
  cardSettings = { ...DEFAULT_CARD_SECTIONS };
  cardOrder = [...DEFAULT_CARD_ORDER];
  saveCardSettings();
  saveCardOrder(cardOrder);
  persistPanelSettings();
  renderCardOrderList();
  if (currentCardDetail) renderCard(currentCardDetail);
}

function getAllTabs() {
  if (!panelTabs.length) {
    panelTabs = [...DEFAULT_TABS];
  }
  return panelTabs;
}

function ensureTabExists(id) {
  return getAllTabs().some((t) => t.id === id);
}

function loadActiveTab() {
  const saved = localStorage.getItem(ACTIVE_TAB_KEY);
  if (saved && ensureTabExists(saved)) {
    return saved;
  }
  return "work";
}

function setActiveTab(id) {
  if (!ensureTabExists(id)) return;
  activeTabId = id;
  localStorage.setItem(ACTIVE_TAB_KEY, id);
  renderTabs();
  restoreSearchForActiveTab();
  applyFilters();
  persistPanelSettings();
}

function removeTab(id) {
  if (id === "work") return;
  panelTabs = panelTabs.filter((t) => t.id !== id);
  deleteTabState(id);
  if (activeTabId === id) {
    activeTabId = panelTabs[0]?.id || "work";
    localStorage.setItem(ACTIVE_TAB_KEY, activeTabId);
  }
  renderTabs();
  restoreSearchForActiveTab();
  applyFilters();
  persistPanelSettings();
}

function getActiveTab() {
  const tabs = getAllTabs();
  return tabs.find((t) => t.id === activeTabId) || tabs[0];
}

async function fetchPanelSettings() {
  try {
    const res = await authFetch("/web/api/settings/panel");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (data.card_sections) {
      const merged = { ...DEFAULT_CARD_SECTIONS, ...data.card_sections };
      cardSettings = {};
      Object.keys(DEFAULT_CARD_SECTIONS).forEach((k) => {
        cardSettings[k] = merged[k] !== undefined ? merged[k] : DEFAULT_CARD_SECTIONS[k];
      });
      saveCardSettings();
    }
    if (Array.isArray(data.tabs)) {
      panelTabs = ensureWorkTabList(data.tabs);
    }
    if (data.active_tab_id) {
      activeTabId = data.active_tab_id;
      localStorage.setItem(ACTIVE_TAB_KEY, activeTabId);
    }
  } catch (err) {
    console.warn("panel settings fetch failed", err);
  } finally {
    panelSettingsReady = true;
    panelTabs = ensureWorkTabList(panelTabs);
    if (!ensureTabExists(activeTabId)) {
      activeTabId = panelTabs[0]?.id || "work";
      localStorage.setItem(ACTIVE_TAB_KEY, activeTabId);
    }
  }
}

async function persistPanelSettings() {
  if (!panelSettingsReady) return;
  const payload = {
    tabs: panelTabs,
    active_tab_id: activeTabId,
    card_sections: cardSettings,
  };
  try {
    await authFetch("/web/api/settings/panel", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    console.warn("panel settings save failed", err);
  }
}

async function ensureSession() {
  const cached = localStorage.getItem(SESSION_KEY);
  if (cached) return cached;
  // Всегда используем WEB‑логин (токен из бота больше не нужен).
  const sid = await ensureLoginSession();
  return sid;
}

async function authFetch(url, opts = {}) {
  const sid = await ensureSession();
  const headers = { ...(opts.headers || {}), "X-Session-Id": sid };
  return fetch(url, { ...opts, headers });
}

function logClientEvent(event, detail = {}) {
  authFetch("/web/api/logs/client", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ event, detail }),
  }).catch(() => {});
}

function setStatus(text) {
  const el = document.getElementById("status");
  if (el) el.textContent = text;
}

function initLoginModal() {
  if (loginInitDone) return;
  loginInitDone = true;
  loginOverlayEl = document.getElementById("login-overlay");
  loginErrorEl = document.getElementById("login-error");
  loginLoginInput = document.getElementById("login-login");
  loginPasswordInput = document.getElementById("login-password");
  loginSubmitBtn = document.getElementById("login-submit");
  loginFormEl = document.getElementById("login-form");
  if (!loginOverlayEl || !loginLoginInput || !loginPasswordInput || !loginSubmitBtn) {
    return;
  }
  const submitHandler = async () => {
    const login = loginLoginInput.value.trim();
    const password = loginPasswordInput.value;
    if (!login || !password) {
      showLoginError("Укажите логин и пароль");
      return;
    }
    loginSubmitBtn.disabled = true;
    showLoginError("");
    try {
      const res = await fetch("/web/api/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ login, password }),
      });
      if (!res.ok) {
        if (res.status === 401) {
          showLoginError("Неверный логин или пароль");
        } else {
          showLoginError("Ошибка входа, попробуйте позже");
        }
        return;
      }
      const data = await res.json();
      if (data.session_id) {
        localStorage.setItem(SESSION_KEY, data.session_id);
        try {
          localStorage.setItem(SESSION_INFO_KEY, JSON.stringify(data));
        } catch {}
        hideLoginModal();
      } else {
        showLoginError("Неверный ответ сервера");
      }
    } catch (err) {
      console.error("login error", err);
      showLoginError("Ошибка сети при входе");
    } finally {
      loginSubmitBtn.disabled = false;
    }
  };
  loginSubmitBtn.addEventListener("click", submitHandler);
  if (loginFormEl && !loginFormEl._wired) {
    loginFormEl._wired = true;
    loginFormEl.addEventListener("submit", (e) => {
      e.preventDefault();
      submitHandler();
    });
  }
  [loginLoginInput, loginPasswordInput].forEach((el) => {
    el.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        submitHandler();
      }
    });
  });
}

function showLoginError(msg) {
  if (!loginErrorEl) return;
  if (!msg) {
    loginErrorEl.style.display = "none";
    loginErrorEl.textContent = "";
  } else {
    loginErrorEl.textContent = msg;
    loginErrorEl.style.display = "block";
  }
}

function showLoginModal() {
  initLoginModal();
  if (!loginOverlayEl) return;
  loginOverlayEl.classList.remove("hidden");
  showLoginError("");
  if (loginLoginInput) {
    loginLoginInput.focus();
  }
}

function hideLoginModal() {
  if (!loginOverlayEl) return;
  loginOverlayEl.classList.add("hidden");
}

async function ensureLoginSession() {
  const existing = localStorage.getItem(SESSION_KEY);
  if (existing) return existing;
  return new Promise((resolve, reject) => {
    showLoginModal();
    const start = Date.now();
    const tick = () => {
      const sid = localStorage.getItem(SESSION_KEY);
      if (sid) {
        resolve(sid);
        return;
      }
      if (Date.now() - start > 10 * 60 * 1000) {
        reject(new Error("login timeout"));
        return;
      }
      setTimeout(tick, 300);
    };
    tick();
  });
}

async function fetchSessionInfo() {
  try {
    const res = await authFetch("/web/api/session");
    if (res.status === 401) {
      // сессия протухла или отсутствует — чистим и показываем логин
      localStorage.removeItem(SESSION_KEY);
      localStorage.removeItem(SESSION_INFO_KEY);
      showLoginModal();
      return;
    }
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();
    localStorage.setItem(SESSION_INFO_KEY, JSON.stringify(data));
    applySessionInfo(data);
  } catch (e) {
    console.warn("session info load failed", e);
  }
}

function applySessionInfo(info) {
  if (!info) {
    userLabelEl && (userLabelEl.textContent = "—");
    if (adminBtn) adminBtn.style.display = "none";
    if (shadowToggleBtn) shadowToggleBtn.style.display = "none";
    isAdmin = false;
    return;
  }
  isAdmin = !!info.is_admin;
  if (userLabelEl) {
    const name = info.display_name || info.login || "—";
    userLabelEl.textContent = `${name}${info.is_admin ? " (admin)" : ""}`;
  }
  if (adminBtn) {
    adminBtn.style.display = info.is_admin ? "inline-flex" : "none";
  }
  if (shadowToggleBtn) {
    shadowToggleBtn.style.display = info.is_admin ? "inline-flex" : "none";
  }
}

async function logout() {
  const sid = localStorage.getItem(SESSION_KEY);
  localStorage.removeItem(SESSION_KEY);
  localStorage.removeItem(SESSION_INFO_KEY);
  try {
    if (sid) {
      await fetch("/web/api/logout", { method: "POST", headers: { "X-Session-Id": sid } });
    }
  } catch (_) {}
  window.location.reload();
}

async function openAdmin() {
  if (!adminModal) return;
  try {
    const res = await authFetch("/web/api/admin/summary");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    adminUsersEl.textContent = (data.users || [])
      .map((u) => `${u.id}: ${u.login} (${u.display_name}) admin=${u.is_admin} node=${u.node_id ?? "-"}`)
      .join("\n");
    adminNodesEl.textContent = (data.nodes || [])
      .map((n) => `${n.id}: ${n.name} parent=${n.parent_id ?? "-"} order=${n.order}`)
      .join("\n");
    adminUnassignedEl.textContent = (data.unassigned_units || []).join(", ") || "—";
    adminMsgEl.textContent = "";
    adminModal.classList.remove("hidden");
  } catch (e) {
    console.error("admin summary", e);
    showToast("Не удалось загрузить админку");
  }
}

async function assignOwners() {
  const nodeId = Number(adminAssignNodeInput.value);
  if (!Number.isFinite(nodeId)) {
    adminMsgEl.textContent = "Укажите node_id";
    return;
  }
  const ids = (adminAssignUnitsInput.value || "")
    .split(",")
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n));
  if (!ids.length) {
    adminMsgEl.textContent = "Укажите unit ids";
    return;
  }
  try {
    const res = await authFetch("/web/api/admin/units_meta", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ unit_ids: ids, node_id: nodeId }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    adminMsgEl.textContent = "Назначено";
    await openAdmin();
    await loadUnits({ restartLive: true });
  } catch (e) {
    adminMsgEl.textContent = "Ошибка назначения";
  }
}

async function loadUnits({ restartLive = false } = {}) {
  const res = await authFetch("/web/api/units");
  if (!res.ok) throw new Error("units http " + res.status);
  units = await res.json();
  // сохраним превью, если сервер прислал
  cardPreviewById.clear();
  units.forEach((u) => {
    if (u.card_preview) {
      cardPreviewById.set(u.id, u.card_preview);
      delete u.card_preview;
    }
  });
  rebuildUnitIndex();
  sortUnits();
  applyFilters();
  lastFeedTick = Date.now();
  updateFeedAlert();
  if (restartLive) {
    feedSince = null;
    scheduleLiveUpdates(true);
  }
}

async function loadWorklist() {
  try {
    const res = await authFetch("/web/api/worklist");
    if (!res.ok) return;
    worklist = new Set(await res.json());
    applyFilters();
  } catch (e) {
    console.warn("worklist load failed", e);
  }
}

async function enterShadowMode() {
  hideCard();
  mode = "inbox";
  selectedShadow = null;
  clearShadowMarkers();
   // В Inbox‑режиме обычные маркеры убираем, чтобы фокус был на новых
   // устройствах (Ghost units). Возвращаем при выходе из режима.
   syncMarkers([]);
  hideShadowCard();
  try {
    await loadShadowInbox();
  } catch (e) {
    console.error(e);
    setStatus("Inbox: ошибка загрузки");
  }
}

async function exitShadowMode({ refreshUnits = false } = {}) {
  mode = "monitor";
  selectedShadow = null;
  clearShadowMarkers();
  hideShadowCard();
  if (cardEl && innerEl && !innerEl.contains(cardEl)) {
    innerEl.appendChild(cardEl);
  }
  if (refreshUnits) {
    try {
      await loadUnits({ restartLive: true });
      return;
    } catch (e) {
      console.error(e);
      setStatus("Мониторинг: ошибка обновления");
    }
  }
  applyFilters();
}

function applyFilters() {
  // Inbox-режим: показываем Shadow-устройства вместо обычных юнитов
  if (mode === "inbox") {
    logClientEvent("shadow_apply_filters", {
      seq: ++shadowDebugSeq,
      mode,
      shadow_count: shadowUnits.length,
      units_count: units.length,
    });
    renderShadowSidebar();
    renderShadowMarkers();
    const msg =
      shadowUnits.length > 0
        ? `Inbox: новых устройств ${shadowUnits.length}`
        : "Inbox: новых устройств нет";
    setStatus(msg);
    return;
  }
  logClientEvent("shadow_apply_filters", {
    seq: ++shadowDebugSeq,
    mode,
    shadow_count: shadowUnits.length,
    units_count: units.length,
  });
  const searchInput = document.getElementById("search");
  const rawSearch = (searchInput?.value || "").trim();
  const searchById = !!document.getElementById("search-by-id")?.checked;
  const tokens = rawSearch
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);
  if (activeTabId) {
    setTabState(activeTabId, { search: rawSearch });
  }
  const view = getActiveTab();
  let list = units;
  if (view.filters?.worklist) {
    // Если рабочий список пустой — показываем все доступные юниты, чтобы не было «пустого экрана».
    if (worklist.size) {
      list = list.filter((u) => worklist.has(u.id));
    }
  }
  if (view.filters?.online && !view.filters?.offline) {
    list = list.filter((u) => u.online);
  } else if (view.filters?.offline && !view.filters?.online) {
    list = list.filter((u) => !u.online);
  }
  if (view.filters?.hasFuel) {
    list = list.filter((u) => u.has_fuel);
  }
  if (view.filters?.regions && view.filters.regions.length) {
    list = list.filter((u) => {
      if (!u.region) return false;
      const region = u.region.toLowerCase();
      return view.filters.regions.some((r) => region.includes(r));
    });
  }
  const sensorQuery = (view.filters?.sensorQuery || "").trim().toLowerCase();
  if (sensorQuery) {
    const tokens = sensorQuery.split(/\s+/).filter(Boolean);
    if (tokens.length) {
      list = list.filter((u) => {
        const tags = Array.isArray(u.sensor_tags) ? u.sensor_tags.map((tag) => String(tag).toLowerCase()) : [];
        if (!tags.length) return false;
        return tokens.every((token) => tags.some((tag) => tag.includes(token)));
      });
    }
  }
  if (tokens.length) {
    list = list.filter((u) => {
      const baseHaystacks = [u.name, u.reg_number, u.hw, u.address]
        .filter(Boolean)
        .map((s) => String(s).toLowerCase());
      const uidHaystacks = searchById ? [u.uid] : []; // UID учитываем только когда включён ID‑режим, чтобы не ловить ID при совпадении чисел
      const idHaystacks = searchById && u.id ? [String(u.id)] : [];
      const haystacks = [...baseHaystacks, ...uidHaystacks.map((s) => String(s).toLowerCase()), ...idHaystacks];
      return tokens.every((t) => haystacks.some((h) => h.includes(t)));
    });
  }
  // Filters by status groups
  if (connectionFilter === "online") {
    list = list.filter((u) => u.online);
  } else if (connectionFilter === "stale") {
    list = list.filter((u) => u.online && u.reason === "no_data");
  } else if (connectionFilter === "offline") {
    list = list.filter((u) => !u.online);
  }
  if (motionFilter === "moving") {
    list = list.filter((u) => u.status === "moving");
  } else if (motionFilter === "stop") {
    list = list.filter(
      (u) => u.status === "stop" || u.status === "park_ign_on" || u.status === "park_ign_off"
    );
  } else if (motionFilter === "parking") {
    list = list.filter((u) => u.status === "stopped");
  }
  if (ignitionFilter === "on") {
    list = list.filter((u) => u.ignition === true);
  } else if (ignitionFilter === "off") {
    list = list.filter((u) => u.ignition === false);
  }
  current = list;
  let shouldRenderList = true;
  if (selectedId) {
    const exists = current.some((u) => u.id === selectedId);
    if (!exists) {
      // юнит отсутствует в глобальном списке (удалён) — закрываем карточку
      hideCard();
      shouldRenderList = false;
    }
  }
  if (shouldRenderList) {
    renderList();
  }
  syncMarkers(current);
  updateFooterStatus();
}

function getWatchIds() {
  const ids = [];
  if (selectedId) ids.push(selectedId);
  const viewport = listEl?.clientHeight || 600;
  const rawScrollTop = listEl?.scrollTop || 0;
  const scrollTop = adjustScrollTop(rawScrollTop);
  const start = Math.max(0, Math.floor(scrollTop / ROW_H) - OVERSCAN);
  const end = Math.min(current.length, start + Math.ceil(viewport / ROW_H) + OVERSCAN * 2);
  for (let i = start; i < end; i++) {
    const id = current[i]?.id;
    if (id) ids.push(id);
  }
  worklist.forEach((id) => ids.push(id));
  return Array.from(new Set(ids));
}

function updateFooterStatus() {
  const total = units.length;
  const online = units.filter((u) => u.online).length;
  const fuel = units.filter((u) => u.has_fuel).length;
  const active = getActiveTab();
  setStatus(`Онлайн: ${online}/${total} • Вкладка: ${active.name}`);
}

function ensureListDom() {
  if (listEl) return;
  listEl = document.getElementById("unit-list");
  if (!listEl) {
    console.error("unit-list element not found");
    return;
  }
  innerEl = document.createElement("div");
  innerEl.className = "unit-list-inner";
  listEl.appendChild(innerEl);
  if (cardEl && !innerEl.contains(cardEl)) {
    innerEl.appendChild(cardEl);
  }
  listEl.addEventListener("scroll", () => requestAnimationFrame(renderList));
  const searchInput = document.getElementById("search");
  if (searchInput) searchInput.addEventListener("input", applyFilters);
  const searchByIdToggle = document.getElementById("search-by-id");
  if (searchByIdToggle) searchByIdToggle.addEventListener("change", applyFilters);
  initStatusFilters();
  const refreshBtn = document.getElementById("btn-refresh");
  if (refreshBtn) {
    refreshBtn.addEventListener("click", () => {
      loadUnits({ restartLive: true }).catch((e) => console.error(e));
    });
  }
  const shadowBtn = document.getElementById("btn-shadow");
  if (shadowBtn) {
    shadowBtn.addEventListener("click", async () => {
      const before = mode;
      logClientEvent("shadow_toggle_click", { mode_before: before });
      if (shadowToggleInFlight) {
        console.warn("shadow toggle: click ignored, in-flight", { mode });
        logClientEvent("shadow_toggle_ignored", { mode_current: mode });
        return;
      }
      shadowToggleInFlight = true;
      try {
        if (mode === "monitor") {
          await enterShadowMode();
        } else {
          await exitShadowMode();
        }
        logClientEvent("shadow_toggle_done", { mode_before: before, mode_after: mode });
      } catch (err) {
        console.error("shadow toggle failed", err);
        setStatus("Inbox: ошибка переключения режима");
        logClientEvent("shadow_toggle_error", {
          mode_before: before,
          mode_after: mode,
          message: err?.message || String(err),
        });
      } finally {
        shadowToggleInFlight = false;
      }
    });
  }
  const btnAdd = document.getElementById("btn-add");
  if (btnAdd) btnAdd.addEventListener("click", openModal);
}

function initStatusFilters() {
  if (statusPopupsWired) return;
  statusPopupsWired = true;
  const popups = document.querySelectorAll(".status-popup");
  const hideAll = () => popups.forEach((p) => p.classList.add("hidden"));
  document.addEventListener("click", hideAll);

  document.querySelectorAll(".status-toggle").forEach((btn) => {
    const group = btn.dataset.group;
    const popup = document.querySelector(`.status-popup[data-popup="${group}"]`);
    if (!popup) return;

    btn.addEventListener("mouseenter", () => {
      hideAll();
      popup.classList.remove("hidden");
    });
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      // клик по активной кнопке сбрасывает фильтр
      if (group === "connection" && connectionFilter) {
        connectionFilter = null;
        updateStatusToggleUI();
        applyFilters();
        hideAll();
        return;
      }
      if (group === "motion" && motionFilter) {
        motionFilter = null;
        updateStatusToggleUI();
        applyFilters();
        hideAll();
        return;
      }
      if (group === "ignition" && ignitionFilter) {
        ignitionFilter = null;
        updateStatusToggleUI();
        applyFilters();
        hideAll();
        return;
      }
      // если фильтр не активен — просто показать popup
      popup.classList.toggle("hidden");
    });

    popup.addEventListener("mouseleave", () => {
      popup.classList.add("hidden");
    });

    popup.querySelectorAll("button").forEach((item) => {
      item.addEventListener("click", (e) => {
        const value = item.dataset.value;
        if (group === "connection") connectionFilter = value;
        if (group === "motion") motionFilter = value;
        if (group === "ignition") ignitionFilter = value;
        updateStatusToggleUI();
        applyFilters();
        hideAll();
        e.stopPropagation();
      });
    });
  });

  updateStatusToggleUI();
}

function updateStatusToggleUI() {
  const connBtn = document.querySelector('.status-toggle[data-group="connection"]');
  const motionBtn = document.querySelector('.status-toggle[data-group="motion"]');
  const ignBtn = document.querySelector('.status-toggle[data-group="ignition"]');
  if (connBtn) {
    const icon = connBtn.querySelector(".icon");
    connBtn.classList.toggle("active", !!connectionFilter);
    if (connectionFilter === "online") icon.textContent = "🟢";
    else if (connectionFilter === "stale") icon.textContent = "🟡";
    else if (connectionFilter === "offline") icon.textContent = "🔴";
    else icon.textContent = "●";
  }
  if (motionBtn) {
    const icon = motionBtn.querySelector(".icon");
    motionBtn.classList.toggle("active", !!motionFilter);
    if (motionFilter === "moving") icon.textContent = "🚗";
    else if (motionFilter === "stop") icon.textContent = "🛑";
    else if (motionFilter === "parking") icon.textContent = "🅿️";
    else icon.textContent = "🚘";
  }
  if (ignBtn) {
    const icon = ignBtn.querySelector(".icon");
    ignBtn.classList.toggle("active", !!ignitionFilter);
    if (ignitionFilter === "on") icon.textContent = "🔑";
    else if (ignitionFilter === "off") icon.textContent = "🗝️";
    else icon.textContent = "🔑";
  }
  document.querySelectorAll('.status-popup[data-popup="connection"] button').forEach((b) => {
    b.classList.toggle("active", b.dataset.value === connectionFilter);
  });
  document.querySelectorAll('.status-popup[data-popup="motion"] button').forEach((b) => {
    b.classList.toggle("active", b.dataset.value === motionFilter);
  });
  document.querySelectorAll('.status-popup[data-popup="ignition"] button').forEach((b) => {
    b.classList.toggle("active", b.dataset.value === ignitionFilter);
  });
}

function ensurePool() {
  const viewport = listEl.clientHeight || 600;
  const desired = Math.min(MAX_POOL, Math.max(MIN_POOL, Math.ceil(viewport / ROW_H) + OVERSCAN * 2));
  if (poolSize === desired) return;
  poolSize = desired;
  const cardNode = cardEl && innerEl.contains(cardEl) ? cardEl : null;
  innerEl.innerHTML = "";
  pool = [];
  for (let i = 0; i < desired; i++) {
    const row = document.createElement("div");
    row.className = "unit-row";
    const name = document.createElement("div");
    name.className = "unit-name";
     const nameIcon = document.createElement("img");
     nameIcon.className = "unit-type-icon";
     const nameSpan = document.createElement("span");
     nameSpan.className = "unit-title";
     name.appendChild(nameIcon);
     name.appendChild(nameSpan);
    const meta = document.createElement("div");
    meta.className = "unit-meta";
    const speed = document.createElement("div");
    speed.className = "unit-meta";
    const star = document.createElement("div");
    star.className = "unit-meta unit-star";
    row.appendChild(name);
    row.appendChild(speed);
    row.appendChild(star);
    row._name = nameSpan;
    row._icon = nameIcon;
    row._meta = meta;
    row._speed = speed;
    row._star = star;
    row.appendChild(meta);
    pool.push(row);
    innerEl.appendChild(row);
    row.addEventListener("click", () => selectUnit(row._id));
  }
  if (cardNode) {
    innerEl.appendChild(cardNode);
  }
}

function renderTabs() {
  const container = document.getElementById("view-tabs");
  if (!container) return;
  const tabs = getAllTabs();
  container.innerHTML = "";
  tabs.forEach((tab) => {
    const btn = document.createElement("button");
    btn.className = `tab-btn${tab.id === activeTabId ? " active" : ""}`;
    btn.dataset.id = tab.id;
    btn.textContent = tab.name;
    btn.addEventListener("click", () => setActiveTab(tab.id));
    if (tab.id !== "work") {
      const close = document.createElement("button");
      close.className = "tab-close";
      close.textContent = "×";
      close.title = "Удалить вкладку";
      close.addEventListener("click", (e) => {
        e.stopPropagation();
        removeTab(tab.id);
      });
      btn.appendChild(close);
    }
    container.appendChild(btn);
  });
}

function openTabModal() {
  const modal = document.getElementById("tab-modal");
  document.getElementById("tab-name").value = "";
  document.getElementById("tab-worklist").checked = false;
  document.getElementById("tab-online").checked = false;
  document.getElementById("tab-offline").checked = false;
  document.getElementById("tab-has-fuel").checked = false;
  document.getElementById("tab-regions").value = "";
  tabFilterRows = [{ kind: "sensor", value: "" }];
  renderTabFilterRows();
  modal.classList.remove("hidden");
}

function closeTabModal() {
  document.getElementById("tab-modal").classList.add("hidden");
}

function saveTabFromModal() {
  const name = document.getElementById("tab-name").value.trim();
  if (!name) {
    alert("Укажите название вкладки");
    return;
  }
  const filters = {
    worklist: document.getElementById("tab-worklist").checked,
    online: document.getElementById("tab-online").checked,
    offline: document.getElementById("tab-offline").checked,
    hasFuel: document.getElementById("tab-has-fuel").checked,
    regions: document
      .getElementById("tab-regions")
      .value.split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean),
  };
  const advanced = buildFiltersFromRows(tabFilterRows);
  Object.assign(filters, advanced);
  const tab = {
    id: `tab-${Date.now().toString(36)}`,
    name,
    filters,
  };
  panelTabs = [...panelTabs, tab];
  closeTabModal();
  setActiveTab(tab.id);
  persistPanelSettings();
}

// Trip detector defaults/helpers
const TRIP_DEFAULTS = {
  mode: "ignition",
  min_speed_kph: 3,
  min_parking_time_sec: 300,
  min_trip_time_sec: 60,
  min_trip_distance_m: 300,
  max_gap_sec: 300,
  max_gap_m: 10000,
};

function showTripError(msg) {
  if (tripErrorEl) tripErrorEl.textContent = msg || "";
}

function fillTripForm(cfg) {
  const data = { ...TRIP_DEFAULTS, ...(cfg || {}) };
  if (tripModeInput) tripModeInput.value = data.mode || "ignition";
  if (tripMinSpeedInput) tripMinSpeedInput.value = data.min_speed_kph ?? "";
  if (tripMinParkingInput) tripMinParkingInput.value = data.min_parking_time_sec ?? "";
  if (tripMinTripTimeInput) tripMinTripTimeInput.value = data.min_trip_time_sec ?? "";
  if (tripMinTripDistanceInput) tripMinTripDistanceInput.value = data.min_trip_distance_m ?? "";
  if (tripMaxGapSecInput) tripMaxGapSecInput.value = data.max_gap_sec ?? "";
  if (tripMaxGapMInput) tripMaxGapMInput.value = data.max_gap_m ?? "";
}

async function openTripModal(unitId) {
  if (!isAdmin) {
    showToast("Доступно только админам");
    return;
  }
  tripUnitId = unitId;
  if (!tripModal) return;
  tripModal.classList.remove("hidden");
  showTripError("");
  fillTripForm(TRIP_DEFAULTS);
  try {
    const res = await authFetch(`/web/api/admin/units/${unitId}/trip-config`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    fillTripForm(data?.effective || TRIP_DEFAULTS);
  } catch (e) {
    showTripError("Не удалось загрузить настройки");
  }
}

function closeTripModal() {
  tripUnitId = null;
  if (tripModal) tripModal.classList.add("hidden");
}

function readNumber(val) {
  if (val === "" || val === null || val === undefined) return null;
  const num = Number(val);
  return Number.isFinite(num) ? num : null;
}

async function saveTripModal() {
  if (!tripUnitId) {
    showTripError("Юнит не выбран");
    return;
  }
  const payload = {
    mode: tripModeInput?.value || "ignition",
    min_speed_kph: readNumber(tripMinSpeedInput?.value),
    min_parking_time_sec: readNumber(tripMinParkingInput?.value),
    min_trip_time_sec: readNumber(tripMinTripTimeInput?.value),
    min_trip_distance_m: readNumber(tripMinTripDistanceInput?.value),
    max_gap_sec: readNumber(tripMaxGapSecInput?.value),
    max_gap_m: readNumber(tripMaxGapMInput?.value),
  };
  Object.keys(payload).forEach((k) => payload[k] === null && delete payload[k]);
  try {
    const res = await authFetch(`/web/api/admin/units/${tripUnitId}/trip-config`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const text = await res.text();
      let msg = `Ошибка сохранения (${res.status})`;
      try {
        const err = JSON.parse(text);
        msg = err.detail || msg;
      } catch (_) {}
      showTripError(msg);
      return;
    }
    await res.json();
    closeTripModal();
    showToast("Настройки детектора поездок сохранены");
  } catch (e) {
    showTripError("Не удалось сохранить");
  }
}

function formatStatusWithIcon(unit) {
  const rawLabel = unit.status_label ? stripIgnitionText(unit.status_label) : "";
  if (!rawLabel) return "";
  const status = unit.status;
  const reason = unit.reason;
  // Для спец-причин («нет данных», «нет источника») иконки не добавляем
  if (reason === "no_data" || reason === "no_source") {
    return rawLabel;
  }
  if (status === "moving") return `🚗 ${rawLabel}`;
  if (status === "stop" || status === "park_ign_on" || status === "park_ign_off") return `🛑 ${rawLabel}`;
  if (status === "stopped") return `🅿️ ${rawLabel}`;
  return rawLabel;
}

function formatAgeShort(sec) {
  if (sec == null) return "";
  const s = Number(sec);
  if (!Number.isFinite(s) || s < 0) return "";
  if (s < 60) return "<1 мин";
  const mins = Math.round(s / 60);
  if (mins < 60) return `${mins} мин`;
  const hrs = s / 3600;
  if (hrs < 24) return `${Number(hrs.toFixed(1))} ч`;
  const days = hrs / 24;
  return `${Number(days.toFixed(1))} дн`;
}

function buildMetaText(unit) {
  const segments = [];
  if (unit.hw) segments.push(unit.hw);
  let statusPart = formatStatusWithIcon(unit);
  // Добавляем длительность стоянки/остановки:
  // сначала берём stop_duration_s (если backend его отдаёт),
  // иначе падаем обратно на возраст последнего сообщения.
  if (
    statusPart &&
    (unit.status === "stop" || unit.status === "park_ign_on" || unit.status === "park_ign_off" || unit.status === "stopped") &&
    !/\d/.test(statusPart)
  ) {
    let durBase = unit.stop_duration_s;
    if (durBase == null) {
      durBase = unit.last_ts_age_sec;
    }
    const dur = formatAgeShort(durBase);
    if (dur) statusPart = `${statusPart} · ${dur}`;
  }
  // Если офлайн/нет данных и цифр нет — подставим возраст последнего пакета
  const needsAge =
    (!statusPart || !/\d/.test(statusPart)) &&
    (unit.status === "offline" || unit.reason === "no_connection" || unit.reason === "no_data");
  if (needsAge) {
    const ageText = formatAgeShort(unit.last_ts_age_sec);
    if (ageText) {
      statusPart = statusPart ? `${statusPart}, ${ageText}` : ageText;
    }
  }
  if (statusPart) segments.push(statusPart);
  if (unit.reason === "no_data") {
    segments.push("нет данных");
  } else if (unit.reason === "no_source") {
    segments.push("нет связи с источником");
  }
  if (!segments.length) {
    segments.push("—");
  }
  return segments.join(" · ");
}

function updateRow(node, unit, idx, translateY) {
  node._id = unit.id;
  node.dataset.id = String(unit.id);
  node.dataset.index = String(idx);
  node.style.transform = `translateY(${translateY}px)`;
  node.classList.toggle("is-selected", unit.id === selectedId);
  node.classList.remove("status-warn", "status-source");
  if (unit.reason === "no_data") node.classList.add("status-warn");
  if (unit.reason === "no_source") node.classList.add("status-source");
  const iconUrl = getUnitIconUrl(unit.icon_kind);
  if (node._icon) {
    if (iconUrl) {
      node._icon.src = iconUrl;
      node._icon.style.display = "";
    } else {
      node._icon.style.display = "none";
    }
  }
  node._name.textContent = unit.name;
  const ign = unit.ignition;
  const ignHtml = ign === true ? `<span class="ign-chip">🔑 вкл</span>` : "";
  let dotClass = unit.online ? "online" : "offline";
  if (unit.reason === "no_data") dotClass = "warn";
  if (unit.reason === "no_source") dotClass = "source";
  let reasonBadge = "";
  if (unit.reason === "no_data") reasonBadge = '<span class="pill pill-warn">нет данных</span>';
  else if (unit.reason === "no_source") reasonBadge = '<span class="pill pill-source">нет источника</span>';
  node._meta.innerHTML = `<span class="dot ${dotClass}"></span>${buildMetaText(unit)} ${reasonBadge} ${ignHtml}`;
  node._speed.textContent = unit.speed != null ? `${Math.round(unit.speed)} км/ч` : "";
  node._star.textContent = ""; // оставляем столбец под звезду пустым, чтобы верстка не смещалась
  node.onmouseenter = (e) => {
    showTooltip(unit, e.clientX, e.clientY);
    // лёгкий prefetch деталей, чтобы клик был мгновенным
    if (node._prefetchTimer) clearTimeout(node._prefetchTimer);
    node._prefetchTimer = setTimeout(() => prefetchDetail(unit.id), 180);
  };
  node.onmousemove = (e) => showTooltip(unit, e.clientX, e.clientY);
  node.onmouseleave = hideTooltip;
}

async function prefetchDetail(id) {
  const cached = detailCache.get(id);
  const now = Date.now();
  if (cached && now - cached.ts < DETAIL_CACHE_TTL_MS) return;
  if (detailPrefetchInFlight.has(id)) return;
  if (detailPrefetchInFlight.size >= 3) return;
  detailPrefetchInFlight.add(id);
  try {
    const res = await authFetch(`/web/api/units/${id}`);
    if (!res.ok) throw new Error(`prefetch http ${res.status}`);
    const data = await res.json();
    cacheDetail(id, data);
  } catch (err) {
    console.warn("prefetch failed", err);
  } finally {
    detailPrefetchInFlight.delete(id);
  }
}

function renderList() {
  // В режиме Inbox список и карточка рисуются отдельной логикой без виртуализации.
  if (mode === "inbox") {
    renderShadowSidebar();
    return;
  }
  ensureListDom();
  ensurePool();
  const total = current.length;
  const viewport = listEl.clientHeight || 600;
  const rawScrollTop = listEl.scrollTop || 0;
  const scrollTop = adjustScrollTop(rawScrollTop);
  const start = Math.max(0, Math.floor(scrollTop / ROW_H) - OVERSCAN);
  const end = Math.min(total, start + Math.ceil(viewport / ROW_H) + OVERSCAN * 2);
  const extraHeight = cardVisible ? cardHeight : 0;
  innerEl.style.height = `${total * ROW_H + extraHeight}px`;
  const visible = end - start;
  for (let i = 0; i < pool.length; i++) {
    if (i < visible) {
      const u = current[start + i];
      const idx = start + i;
      let translateY = idx * ROW_H;
      if (cardVisible && expandedIndex !== null && idx > expandedIndex) {
        translateY += cardHeight;
      }
      updateRow(pool[i], u, idx, translateY);
      pool[i].style.display = "grid";
    } else {
      pool[i].style.display = "none";
    }
  }
  updateCardPlacement();
}

async function selectUnit(id) {
  // toggle: if already opened on this unit, close fast
  if (cardVisible && currentCardDetail?.item?.id === id) {
    hideCard();
    return;
  }
  const t0 = performance.now ? performance.now() : Date.now();
  selectedId = id;
  logClientEvent("card_select", { unitId: id });
  hideTooltip();
  const listItem = unitsById.get(id);
  if (listItem) {
    const previewRaw = cardPreviewById.get(id);
    const preview = previewRaw ? hydratePreview(previewRaw, listItem) : buildPreviewDetail(listItem);
    currentCardDetail = preview;
    resetNearbyState(listItem.id);
    cardNeedsAnchor = true;
    renderCard(preview);
  }
  try {
    const cached = detailCache.get(id);
    const now = Date.now();
    let data;
    let fetchMs = 0;
    let fromCache = false;
    if (cached && now - cached.ts < DETAIL_CACHE_TTL_MS) {
      fromCache = true;
      data = cached.data;
    } else {
      const tf0 = performance.now ? performance.now() : Date.now();
      const res = await authFetch(`/web/api/units/${id}`);
      if (!res.ok) throw new Error("detail http " + res.status);
      data = await res.json();
      cacheDetail(id, data);
      const tf1 = performance.now ? performance.now() : Date.now();
      fetchMs = tf1 - tf0;
    }
    const tr0 = performance.now ? performance.now() : Date.now();
    currentCardDetail = data;
    resetNearbyState(data.item.id);
    renderCard(data);
    focusMarker(data.item);
    const tr1 = performance.now ? performance.now() : Date.now();
    const totalMs = tr1 - t0;
    logClientEvent("card_perf", {
      unitId: id,
      fromCache,
      fetchMs,
      renderMs: tr1 - tr0,
      totalMs,
      cacheAgeMs: fromCache ? now - (cached?.ts || now) : null,
    });
    logClientEvent("card_detail_loaded", {
      unitId: id,
      hasCard: !!data.card_data,
      hasConfig: !!data.unit_config,
    });
  } catch (e) {
    console.error(e);
    const t1 = performance.now ? performance.now() : Date.now();
    logClientEvent("card_perf", {
      unitId: id,
      error: e?.message || String(e),
      totalMs: t1 - t0,
    });
    logClientEvent("card_detail_failed", { unitId: id, message: e?.message || String(e) });
  }
}


function renderCard(detail) {
  if (!cardEl) return;
  try {
    if (!detail || !detail.item) {
      console.warn("renderCard: empty detail", detail);
      return;
    }
    const { item } = detail;
    if (nearbyCardState.unitId !== item.id) {
      resetNearbyState(item.id);
    }
    const showNearby = nearbyCardState.visible && nearbyCardState.unitId === item.id;
    const nearbyList = showNearby ? nearbyCardState.list : [];
    const cardData = detail.card_data || {};
    const fallbackLatest = detail.latest || {};
    const fallbackSnapshot = detail.snapshot || {};
    const fallbackStatus = detail.status_details || {};
    const sectionBlocks = [];
    const statusBlock = cardData.status || {
      online: item.online,
      status_label: item.status_label,
      last_ts: fallbackStatus.last_ts,
      speed: fallbackLatest.speed,
      ignition: fallbackStatus.ignition,
      moving: (() => {
        const s = cardData.status?.speed ?? fallbackLatest.speed;
        if (s != null && !Number.isNaN(Number(s))) return Number(s) > 1;
        return fallbackStatus.status === "moving";
      })(),
      status: cardData.status?.status || fallbackStatus.status,
    };
    const statusChips = [];
    if (statusBlock.status === "moving" || statusBlock.moving) {
      statusChips.push("🚗 Движение");
    } else if (
      statusBlock.status === "stop" ||
      statusBlock.status === "park_ign_on" ||
      statusBlock.status === "park_ign_off"
    ) {
      statusChips.push("🛑 Остановка");
    } else if (statusBlock.status === "stopped") {
      statusChips.push("🅿️ Стоянка");
    } else if (statusBlock.online) {
      statusChips.push("🟢 Онлайн");
    }
    if (statusBlock.ignition === true) {
      // ключ только для включённого зажигания
      statusChips.push("🔑 вкл");
    }

    // Убираем дублирующее упоминание зажигания в строке статуса, раз уже есть чип.
    let statusLabelClean = statusBlock.status_label || "";
    statusLabelClean = stripIgnitionText(statusLabelClean);
    if (statusLabelClean.endsWith("—")) statusLabelClean = statusLabelClean.slice(0, -1).trim();

    const locationBlock = cardData.location || {};
    const address = locationBlock.address || (fallbackSnapshot.meta && (fallbackSnapshot.meta.address || fallbackSnapshot.meta.addr)) || "—";
    const latVal = locationBlock.lat ?? item.lat;
    const lonVal = locationBlock.lon ?? item.lon;
    const coords = `${formatCoord(latVal)}, ${formatCoord(lonVal)}`;
    const geofences = Array.isArray(locationBlock.geofences) ? locationBlock.geofences : [];
    const geofenceHtml = geofences.length ? `<div class="geofence-list">${geofences.map((name) => `<span class="geofence-chip">${name}</span>`).join("")}</div>` : "";
    const coordsLink =
      latVal != null && lonVal != null
        ? `<span class="kv-value-link" data-action="copy-coords" data-coords="${latVal}, ${lonVal}" title="Скопировать координаты">${coords}</span>`
        : coords;
    const addressLink =
      latVal != null && lonVal != null
        ? `<a class="kv-value-link" href="https://yandex.ru/maps/?pt=${lonVal},${latVal}&z=17&l=map" target="_blank" rel="noopener">${address}</a>`
        : address;
  sectionBlocks.push({
    key: "location",
    title: "Местоположение",
    body: `
        ${renderKvGrid([
          { label: "Адрес", value: addressLink },
          { label: "Координаты", value: coordsLink },
          ...(geofences.length ? [{ label: "Геозоны", value: geofences.join(", ") }] : []),
        ])}
        ${geofenceHtml}
        `,
  });

    const showBlock = (key) => cardLayoutEdit || cardSettings[key];

    if (showBlock("sensors")) {
      const fallbackSensors = detail.unit_config?.sensors || fallbackSnapshot.sensors || [];
      const block = cardData.sensors && cardData.sensors.length ? cardData.sensors : fallbackSensors.slice(0, 6);
      const sensorsHtml = renderKvGrid(
        block.map((s) => ({
          label: s.name || s.type || "датчик",
          value: s.value != null ? `${s.value}${s.units ? ` ${s.units}` : ""}` : "—",
        }))
      );
      sectionBlocks.push({
        key: "sensors",
        title: "Датчики",
        body: sensorsHtml,
      });
    }

    const connectivityBlock = cardData.connectivity || {};

    if (showBlock("connectivity")) {
      const block = connectivityBlock;
      let firmwareValue = block.firmware;
      const firmwareNum = Number(block.firmware);
      if (!Number.isNaN(firmwareNum)) firmwareValue = firmwareNum.toFixed(2);
      const rows = [
        { label: "ID", value: item.id != null ? item.id : "—" },
        { label: "UID", value: block.uid || item.uid || "—" },
        { label: "Устройство", value: block.hardware || item.hw || "—" },
      ];
      if (block.firmware) rows.push({ label: "Прошивка", value: firmwareValue });
      if (block.phones?.length) rows.push({ label: "Телефоны", value: block.phones.join(", ") });
      sectionBlocks.push({
        key: "connectivity",
        title: "Подключение",
        body: renderKvGrid(rows),
      });
    }

    if (showBlock("counters")) {
      const block = cardData.counters || {};
      sectionBlocks.push({
        key: "counters",
        title: "Счётчики",
        body: renderKvGrid([
          { label: "Пробег", value: block.mileage != null ? block.mileage : "—" },
          { label: "Моточасы", value: block.engine_hours != null ? block.engine_hours : "—" },
        ]),
      });
    }

    if (showBlock("params")) {
      const shouldShowParam = (key) => {
        const k = String(key || "").toLowerCase();
        if (k.startsWith("raw_")) return false;
        if (k.startsWith("unknown_")) return false;
        if (k.startsWith("crc_")) return false;
        if (k.startsWith("packet_")) return false;
        return true;
      };
      const paramsSource =
        cardData.params && cardData.params.length
          ? cardData.params
          : Object.entries(fallbackLatest.params || {}).map(([k, v]) => ({ key: k, value: v }));
      const params = paramsSource.filter((p) => shouldShowParam(p.key));
      const html = renderKvGrid(params.slice(0, 40).map((p) => ({ label: p.key, value: p.value })));
      sectionBlocks.push({
        key: "params",
        title: "Параметры",
        body: `<div>${html}</div>`,
      });
    }

    if (showBlock("recent_events")) {
      const rows = cardData.recent_events
        ?.map((e) => {
          const ts = e.device_ts ? new Date(e.device_ts * 1000).toLocaleString() : "—";
          const addr = e.params?.address || "";
          const speed = e.speed != null ? `${Math.round(e.speed)} км/ч` : "";
          return `<div class="recent-row"><div>${ts}</div><div>${speed}</div><div>${addr}</div></div>`;
        })
        .join("");
      sectionBlocks.push({
        key: "recent_events",
        title: "Последние сообщения",
        body: rows || "—",
      });
    }

    if (showBlock("custom_fields")) {
      const fields = cardData.custom_fields || [];
      const rows = fields.length ? renderKvGrid(fields.map((f) => ({ label: f.key, value: f.value }))) : "";
      sectionBlocks.push({
        key: "custom_fields",
        title: "Пользовательские поля",
        body: rows || "—",
      });
    }

    if (showBlock("crew")) {
      const drivers = Array.isArray(cardData.drivers) ? cardData.drivers : [];
      const trailers = Array.isArray(cardData.trailers) ? cardData.trailers : [];
      const rows = [];
      if (drivers.length) {
        rows.push(
          ...drivers.map((d) => `<div>${d.name || "Водитель"}${d.phone ? ` (${d.phone})` : ""}</div>`)
        );
      }
      if (trailers.length) {
        rows.push(...trailers.map((t) => `<div>${t.name || "Прицеп"}</div>`));
      }
      sectionBlocks.push({
        key: "crew",
        title: "Водители и прицепы",
        body: rows.length ? rows.join("") : "—",
      });
    }

    const offlineLine = item.offline_reason ? `<span class="card-status-pill">${item.offline_reason}</span>` : "";
    if (showNearby || cardLayoutEdit) {
      const rows =
        showNearby && nearbyList && nearbyList.length
          ? nearbyList
              .map(
                (entry) => `
            <button class="nearby-row" data-action="card-select" data-unit-id="${entry.id}">
              <span>${entry.name}</span>
              <span class="muted">${entry.distance}</span>
            </button>`
              )
              .join("")
          : `<div class="muted">Нажмите компас, чтобы показать ближайшие объекты</div>`;
      sectionBlocks.unshift({
        key: "nearby",
        title: "Ближайшие",
        body: `<div class="card-nearby-list">${rows}</div>`,
      });
    }

    const statusReason = statusBlock.reason || item.reason;
    const statusLabel = statusBlock.status_label || "Нет связи";
    const lastTs = statusBlock.last_ts ?? fallbackStatus.last_ts ?? fallbackLatest.last_ts;
    const statusCaption = `Последнее сообщение: ${formatRelativeTime(lastTs)}`;
    let statusDotClass = item.online ? "online" : "offline";
    if (statusReason === "no_data") statusDotClass = "warn";
    if (statusReason === "no_source") statusDotClass = "source";
    const statusTitle = `<span class="card-status-title ${statusDotClass}">${statusLabel}</span>`;
    const statusRow = `
      <div class="card-status-row">
        <span class="card-status-dot ${statusDotClass}"></span>
        <div class="card-status-body">
          <div class="card-status-title">${statusTitle}</div>
          <div class="card-status-caption">${statusCaption}</div>
        </div>
        ${offlineLine}
      </div>
    `;
    cardEl.dataset.unitId = String(item.id);
    currentCardDetail = detail;
    const idx = current.findIndex((u) => u.id === item.id);
    if (idx === -1) {
      hideCard();
      return;
    }
    selectedId = item.id;
    expandedIndex = idx;
    cardVisible = true;
    cardEl.classList.remove("hidden");
    const order = loadCardOrder();
    const orderIndex = (k) => {
      const v = order.indexOf(k);
      return v === -1 ? 999 : v;
    };
    const sortedBlocks = sectionBlocks
      .filter((b) => cardLayoutEdit || cardSettings[b.key] || FIXED_CARD_KEYS.includes(b.key))
      .sort((a, b) => orderIndex(a.key) - orderIndex(b.key));

    const buildSectionHtml = (block) => {
      const editable = cardLayoutEdit && !FIXED_CARD_KEYS.includes(block.key);
      const hidden = !cardSettings[block.key];
      const handle = editable ? `<span class="section-handle" data-section-key="${block.key}">☰</span>` : "";
      const toggleBtn = editable
        ? `<button class="ghost icon section-toggle" data-action="card-section-toggle" data-section-key="${block.key}" title="${hidden ? "Показать" : "Скрыть"}">${hidden ? "🚫" : "👁"}</button>`
        : "";
      const body = cardLayoutEdit ? "" : block.body;
      return `
      <section class="card-block${cardLayoutEdit ? " card-block-edit" : ""}" data-section-key="${block.key}" ${editable ? 'draggable="true"' : ""}>
        <div class="card-block-header">
          <div class="card-block-title">
            ${handle}
            <span class="label">${block.title || cardSectionTitle(block.key)}</span>
          </div>
          ${toggleBtn}
        </div>
        <div class="card-block-body">${body}</div>
      </section>`;
    };
    const titleName = item.name || "Объект";
    const titleIdSuffix = item.id != null ? ` · #${item.id}` : "";
    const subtitleParts = [];
    if (item.id != null) subtitleParts.push(`#${item.id}`);
    if (item.uid) subtitleParts.push(`UID ${formatUid(item.uid)}`);
    const hwLabel = connectivityBlock.hardware || item.hw;
    if (hwLabel) subtitleParts.push(hwLabel);
    const titleSubtitle = subtitleParts.join(" · ");
    cardEl.innerHTML = `
    <div class="card-head">
      <div class="card-head-main">
        <div class="card-head-name">${titleName}${titleIdSuffix}</div>
        <div class="card-head-subtitle">${titleSubtitle || "\u00a0"}</div>
      </div>
      <div class="card-actions">
        ${cardLayoutEdit ? `<button class="ghost icon" data-action="card-layout-reset" title="Сбросить по умолчанию">↺</button>` : ""}
        <button class="ghost icon${cardLayoutEdit ? " edit-active" : ""}" data-action="card-layout-toggle" title="Настроить карточку">☰</button>
        <button class="ghost icon" data-action="card-nearby" title="Ближайшие объекты">${showNearby ? "⦿" : "◎"}</button>
        <button class="ghost icon" data-action="open-history" title="История треков">📅</button>
        ${isAdmin ? `<button class="ghost icon" data-action="trip-settings" title="Детектор поездок">🚗</button>` : ""}
      </div>
    </div>
    ${statusRow}
    <div class="card-grid">
      ${sortedBlocks.map((b) => buildSectionHtml(b)).join("")}
    </div>
  `;
    // вычисляем общую ширину label в карточке
    updateKvLabelWidth();
    if (cardLayoutEdit) {
      cardEl.classList.add("card-layout-edit");
      setupCardDrag();
    } else {
      cardEl.classList.remove("card-layout-edit");
    }
    updateKvLabelWidth();
    requestAnimationFrame(() => {
      if (!cardEl || !cardVisible) return;
      cardHeight = cardEl.offsetHeight + CARD_GAP;
      updateCardPlacement();
      if (cardNeedsAnchor) {
        ensureCardAnchorVisible();
        cardNeedsAnchor = false;
      }
      renderList();
    });
  } catch (err) {
    console.error("renderCard error", err);
    logClientEvent("card_render_error", {
      unitId: detail?.item?.id,
      message: err?.message || String(err),
      stack: err?.stack || null,
    });
    showToast("Не удалось отобразить карточку");
  }
}


function initMap() {
  const saved = localStorage.getItem(MAP_KEY);
  let center = [55.75, 37.61];
  let zoom = 5;
  if (saved) {
    try {
      const obj = JSON.parse(saved);
      center = obj.center || center;
      zoom = obj.zoom || zoom;
    } catch {}
  }
  // Достаточно указать путь к каталогу с иконками; сами имена Leaflet подставит
  L.Icon.Default.imagePath = "/static/vendor/leaflet/images/";
  map = L.map("map", { zoomControl: true }).setView(center, zoom);

  // Тёмная тема по умолчанию; при сбое откатываемся на стандартный OSM
  const tileUrl = window.OFFLINE_TILE_URL || window.MAP_TILE_URL || TILE_URL_DEFAULT;
  const tileOpts = window.OFFLINE_TILE_URL
    ? { maxZoom: 19, attribution: "" }
    : { ...TILE_OPTS_DEFAULT };

  const baseLayer = L.tileLayer(tileUrl, tileOpts).addTo(map);
  baseLayer.on("tileerror", () => {
    if (baseLayer._fallbackApplied) return;
    baseLayer._fallbackApplied = true;
    map.removeLayer(baseLayer);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap",
    }).addTo(map);
  });

  map.on("moveend", () => {
    const c = map.getCenter();
    localStorage.setItem(MAP_KEY, JSON.stringify({ center: [c.lat, c.lng], zoom: map.getZoom() }));
  });
}

function ensureCluster() {
  if (!map) return;
  if (!cluster && L.markerClusterGroup) {
    cluster = L.markerClusterGroup({
      chunkedLoading: true,
      maxClusterRadius: 80,
      disableClusteringAtZoom: 16,
      spiderfyOnEveryZoom: false,
    });
    map.addLayer(cluster);
  }
}

function detachMarker(id) {
  const marker = markerById.get(id);
  if (!marker) return;
  if (cluster) {
    cluster.removeLayer(marker);
  } else if (map && map.hasLayer(marker)) {
    map.removeLayer(marker);
  }
  markerById.delete(id);
}

function wireMarker(unit) {
  const marker = L.marker([unit.lat, unit.lon]);
  marker.on("click", () => selectUnit(unit.id));
  marker.on("mouseover", (e) => showTooltip(unit, e.originalEvent.clientX, e.originalEvent.clientY));
  marker.on("mousemove", (e) => showTooltip(unit, e.originalEvent.clientX, e.originalEvent.clientY));
  marker.on("mouseout", hideTooltip);
  markerById.set(unit.id, marker);
  if (cluster) cluster.addLayer(marker);
  else marker.addTo(map);
  return marker;
}

function syncMarkers(list) {
  if (!map) return;
  ensureCluster();
  const nextVisible = new Set();
  list.forEach((u) => {
    if (u.lat == null || u.lon == null) {
      detachMarker(u.id);
      return;
    }
    nextVisible.add(u.id);
    const existing = markerById.get(u.id);
    if (existing) {
      existing.setLatLng([u.lat, u.lon]);
      return;
    }
    wireMarker(u);
  });
  Array.from(markerById.keys()).forEach((id) => {
    if (!nextVisible.has(id)) {
      detachMarker(id);
    }
  });
  if (!nextVisible.size) {
    hideTooltip();
  }
}

function focusMarker(item) {
  if (!item || !map) return;
  const marker = markerById.get(item.id);
  if (marker && cluster) {
    cluster.zoomToShowLayer(marker, () => {
      map.setView([item.lat, item.lon], Math.max(map.getZoom(), 14));
    });
  } else if (item.lat && item.lon) {
    map.setView([item.lat, item.lon], Math.max(map.getZoom(), 14));
  }
}

/* Tooltip */
function showTooltip(unit, x, y) {
  if (!tooltipEl) return;
  const data = unit.tooltip_data || {};
  const online = data.online ?? unit.online;
  const statusLabel = data.status_label || unit.status_label || "";
  const status = online ? "🟢 Онлайн" : "🔴 Оффлайн";
  const last = formatRelativeTime(data.last_ts ?? unit.last_ts);
  const speed = data.speed ?? unit.speed;
  const address = data.address || "";
  const geofences = Array.isArray(data.geofences) ? data.geofences : [];
  const parts = [`<div><strong>${unit.name}</strong></div>`, `<div>${status} — ${statusLabel}</div>`, `<div>Последнее: ${last}</div>`];
  if (data.reason === "no_source") parts.push("<div>Нет связи с источником</div>");
  if (data.reason === "no_data") parts.push("<div>Нет данных</div>");
  if (!online && unit.offline_reason) parts.push(`<div>${unit.offline_reason}</div>`);
  if (speed != null) parts.push(`<div>Скорость: ${Math.round(speed)} км/ч</div>`);
  if (address) parts.push(`<div>${address}</div>`);
  if (geofences.length) parts.push(`<div>Геозоны: ${geofences.join(", ")}</div>`);
  tooltipEl.innerHTML = parts.join("");
  tooltipEl.classList.remove("hidden");
  const width = tooltipEl.offsetWidth || 220;
  const height = tooltipEl.offsetHeight || 100;
  const left = Math.min(x + 16, window.innerWidth - width - 10);
  const top = Math.min(y + 16, window.innerHeight - height - 10);
  tooltipEl.style.left = `${left}px`;
  tooltipEl.style.top = `${top}px`;
}

function hideTooltip() {
  if (tooltipEl) tooltipEl.classList.add("hidden");
}

function formatRelativeTime(ts) {
  if (!ts) return "нет данных";
  const numericTs = Number(ts);
  if (!Number.isFinite(numericTs)) return "нет данных";
  const now = Date.now() / 1000;
  const diff = Math.max(0, Math.floor(now - numericTs));
  if (diff < 60) return `${diff} c назад`;
  if (diff < 3600) return `${Math.floor(diff / 60)} мин назад`;
  if (diff < 86400) return `${Math.floor(diff / 3600)} ч назад`;
  return `${Math.floor(diff / 86400)} дн назад`;
}

function formatCoord(value) {
  if (value == null) return "—";
  return Number(value).toFixed(5);
}

function updateFeedAlert() {
  if (!feedAlertEl) return;
  const lag = Date.now() - lastFeedTick;
  const threshold = FEED_INTERVAL_MS * 2.5;
  if (lag > threshold) {
    const secs = Math.floor(lag / 1000);
    feedAlertEl.textContent = `Нет связи с сервером · ${secs} сек`;
    feedAlertEl.classList.remove("hidden");
  } else {
    feedAlertEl.classList.add("hidden");
  }
}

function initClock() {
  const el = document.getElementById("clock");
  setInterval(() => {
    el.textContent = new Date().toLocaleString();
    updateFeedAlert();
  }, 1000);
}

function initModal() {
  ["modal-name", "modal-uid", "modal-hw"].forEach((id) => {
    const el = document.getElementById(id);
    el.addEventListener("input", applyModalFilter);
  });
  document.getElementById("modal-show-map").addEventListener("change", (e) => {
    modalState.showOnMap = e.target.checked;
  });
  document.getElementById("modal-close").addEventListener("click", closeModal);
  document.getElementById("modal-cancel").addEventListener("click", closeModal);
  document.getElementById("modal-add-selected").addEventListener("click", () => {
    addUnits(Array.from(modalState.selected), false);
  });
  document.getElementById("modal-add-found").addEventListener("click", () => {
    addUnits(modalState.filtered.map((u) => u.id), false);
  });
  document.getElementById("modal-replace").addEventListener("click", () => {
    addUnits(modalState.filtered.map((u) => u.id), true);
  });
}

/* Modal: добавление объектов */
function openModal() {
  modalState.selected = new Set();
  modalState.showOnMap = false;
  document.getElementById("modal-name").value = "";
  document.getElementById("modal-uid").value = "";
  document.getElementById("modal-hw").value = "";
  document.getElementById("modal-show-map").checked = false;
  applyModalFilter();
  document.getElementById("modal-overlay").classList.remove("hidden");
}

function closeModal() {
  document.getElementById("modal-overlay").classList.add("hidden");
}

function applyModalFilter() {
  const nameQ = document.getElementById("modal-name").value.trim().toLowerCase();
  const uidQ = document.getElementById("modal-uid").value.trim().toLowerCase();
  const hwQ = document.getElementById("modal-hw").value.trim().toLowerCase();
  modalState.filtered = units.filter((u) => {
    const nameHit = !nameQ || u.name.toLowerCase().includes(nameQ);
    const uidHit = !uidQ || (u.uid || "").toLowerCase().includes(uidQ);
    const hwHit = !hwQ || (u.hw || "").toLowerCase().includes(hwQ);
    return nameHit && uidHit && hwHit;
  });
  renderModalList(true);
}

function renderModalList(rebuild = false) {
  const box = document.getElementById("modal-list");
  if (rebuild) {
    box.innerHTML = "";
    modalRowById.clear();
    modalState.filtered.forEach((u) => {
      const row = document.createElement("div");
      row.className = "modal-row";
      row.dataset.id = String(u.id);
      const checked = modalState.selected.has(u.id);
      row.innerHTML = `
        <input type="checkbox" ${checked ? "checked" : ""}>
        <div>
          <div class="unit-name">${u.name}</div>
          <div class="unit-meta">${u.uid || "—"}</div>
        </div>
        <div class="unit-meta">${u.hw || ""}</div>
        <div class="unit-meta">${u.online ? "🟢" : "🔴"}</div>
      `;
      const cb = row.querySelector("input");
      cb.addEventListener("click", (e) => {
        e.stopPropagation();
        toggleSelect(u.id, !modalState.selected.has(u.id));
      });
      row.addEventListener("click", () => toggleSelect(u.id, !modalState.selected.has(u.id)));
      row.addEventListener("mouseenter", (e) => {
        showTooltip(u, e.clientX, e.clientY);
      });
      row.addEventListener("mousemove", (e) => {
        showTooltip(u, e.clientX, e.clientY);
      });
      row.addEventListener("mouseleave", hideTooltip);
      row.addEventListener("dblclick", async () => {
        await addUnits([u.id], false);
        if (modalState.showOnMap) focusMarker(u);
      });
      row.classList.toggle("checked", checked);
      modalRowById.set(u.id, row);
      box.appendChild(row);
    });
  } else {
    modalState.filtered.forEach((u) => {
      const row = modalRowById.get(u.id);
      if (!row) return;
      const cb = row.querySelector("input");
      const checked = modalState.selected.has(u.id);
      cb.checked = checked;
      row.classList.toggle("checked", checked);
    });
  }
  const addSel = document.getElementById("modal-add-selected");
  const addFound = document.getElementById("modal-add-found");
  const replaceBtn = document.getElementById("modal-replace");
  addSel.disabled = modalState.selected.size === 0;
  addFound.disabled = modalState.filtered.length === 0;
  replaceBtn.disabled = modalState.filtered.length === 0;
  modalStatus();
}

function toggleSelect(id, on) {
  if (on) modalState.selected.add(id);
  else modalState.selected.delete(id);
  const row = modalRowById.get(id);
  if (row) {
    const cb = row.querySelector("input");
    cb.checked = on;
    row.classList.toggle("checked", on);
  }
  renderModalList(false);
}

async function addUnits(ids, replace) {
  if (!ids.length) {
    showToast("Выберите объекты");
    return;
  }
  const url = "/web/api/worklist";
  const method = replace ? "PUT" : "POST";
  const res = await authFetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ unit_ids: ids }),
  });
  if (!res.ok) {
    showToast("Не удалось изменить рабочий список");
    return;
  }
  const data = await res.json();
  worklist = new Set(data);
  applyFilters();
  if (modalState.showOnMap && ids.length > 0) {
    const u = units.find((x) => x.id === ids[0]);
    if (u) focusMarker(u);
  }
  showToast(replace ? "Список заменён" : "Добавлено");
  renderModalList(false);
}

function showToast(text) {
  setStatus(text);
}

function modalStatus() {
  const el = document.getElementById("modal-status");
  if (!el) return;
  el.textContent = `Найдено: ${modalState.filtered.length}, выбрано: ${modalState.selected.size}`;
}

async function main() {
  tooltipEl = document.getElementById("unit-tooltip");
  cardEl = document.getElementById("unit-card");
  feedAlertEl = document.getElementById("feed-alert");
  userLabelEl = document.getElementById("user-label");
  adminBtn = document.getElementById("btn-admin");
  shadowToggleBtn = document.getElementById("btn-shadow");
  logoutBtn = document.getElementById("btn-logout");
  if (shadowToggleBtn) shadowToggleBtn.style.display = "none";
  adminModal = document.getElementById("admin-modal");
  adminUsersEl = document.getElementById("admin-users");
  adminNodesEl = document.getElementById("admin-nodes");
  adminUnassignedEl = document.getElementById("admin-unassigned");
  adminAssignNodeInput = document.getElementById("admin-assign-node");
  adminAssignUnitsInput = document.getElementById("admin-assign-units");
  adminMsgEl = document.getElementById("admin-msg");
  if (logoutBtn) logoutBtn.addEventListener("click", logout);
  if (adminBtn) {
    adminBtn.addEventListener("click", openAdmin);
  }
  const adminClose = document.getElementById("admin-close");
  if (adminClose) adminClose.addEventListener("click", () => adminModal.classList.add("hidden"));
  const adminAssignBtn = document.getElementById("admin-assign-btn");
  if (adminAssignBtn) adminAssignBtn.addEventListener("click", assignOwners);
  if (cardEl) {
    cardEl.classList.add("hidden");
    cardEl.style.top = "-9999px";
  }
  ensureListDom();
  initMap();
  initHistory();
  initClock();
  initModal();
  initTabModalFilters();
  initLayout();
  const btnAddTab = document.getElementById("btn-add-tab");
  if (btnAddTab) btnAddTab.addEventListener("click", openTabModal);
  const tabClose = document.getElementById("tab-close");
  if (tabClose) tabClose.addEventListener("click", closeTabModal);
  const tabCancel = document.getElementById("tab-cancel");
  if (tabCancel) tabCancel.addEventListener("click", closeTabModal);
  const tabSave = document.getElementById("tab-save");
  if (tabSave) tabSave.addEventListener("click", saveTabFromModal);
  const btnCardSettings = document.getElementById("btn-card-settings");
  if (btnCardSettings) btnCardSettings.addEventListener("click", openCardSettingsModal);
  const csClose = document.getElementById("card-settings-close");
  if (csClose) csClose.addEventListener("click", closeCardSettingsModal);
  const csCloseBottom = document.getElementById("card-settings-close-bottom");
  if (csCloseBottom) csCloseBottom.addEventListener("click", closeCardSettingsModal);
  const csReset = document.getElementById("card-settings-reset");
  if (csReset) csReset.addEventListener("click", resetCardSettings);
  // Trip modal init
  tripModal = document.getElementById("trip-modal");
  tripModeInput = document.getElementById("trip-mode");
  tripMinSpeedInput = document.getElementById("trip-min-speed");
  tripMinParkingInput = document.getElementById("trip-min-parking");
  tripMinTripTimeInput = document.getElementById("trip-min-trip-time");
  tripMinTripDistanceInput = document.getElementById("trip-min-trip-distance");
  tripMaxGapSecInput = document.getElementById("trip-max-gap-sec");
  tripMaxGapMInput = document.getElementById("trip-max-gap-m");
  tripErrorEl = document.getElementById("trip-error");
  const tripClose = document.getElementById("trip-close");
  if (tripClose) tripClose.addEventListener("click", closeTripModal);
  const tripCancel = document.getElementById("trip-cancel");
  if (tripCancel) tripCancel.addEventListener("click", closeTripModal);
  const tripSave = document.getElementById("trip-save");
  if (tripSave) tripSave.addEventListener("click", saveTripModal);
  if (cardEl) {
    cardEl.addEventListener("click", handleCardClick);
  }
  initLoginModal();
  await ensureSession();
  try {
    const cachedInfo = localStorage.getItem(SESSION_INFO_KEY);
    if (cachedInfo) applySessionInfo(JSON.parse(cachedInfo));
  } catch {}
  fetchSessionInfo().catch(() => {});
  await fetchPanelSettings();
  renderTabs();
  restoreSearchForActiveTab();
  await loadWorklist();
  await loadUnits({ restartLive: true });
  renderList();
}

main().catch((e) => {
  console.error(e);
  setStatus("Ошибка инициализации");
});

function openCardSettingsModal() {
  renderCardOrderList();
  document.getElementById("card-settings-modal").classList.remove("hidden");
}

function closeCardSettingsModal() {
  document.getElementById("card-settings-modal").classList.add("hidden");
}

function renderCardOrderList() {
  const list = document.getElementById("card-order-list");
  if (!list) return;
  const keys = loadCardOrder();
  cardOrder = keys;
  const mutableKeys = keys.filter((k) => !FIXED_CARD_KEYS.includes(k));
  // build rows if not cached
  mutableKeys.forEach((k) => {
    if (orderRowByKey.has(k)) return;
    const row = document.createElement("div");
    row.className = "order-item";
    row.dataset.key = k;
    row.draggable = true;
    row.innerHTML = `
      <span class="handle" aria-hidden="true">☰</span>
      <span class="title">
        <label class="checkbox-inline">
          <input type="checkbox" data-order-key="${k}" ${cardSettings[k] ? "checked" : ""}/>
          <span class="label-text">${cardSectionTitle(k)}</span>
        </label>
      </span>
    `;
    row.addEventListener("dragstart", onOrderDragStart);
    row.addEventListener("dragover", onOrderDragOver);
    row.addEventListener("dragleave", onOrderDragLeave);
    row.addEventListener("drop", onOrderDrop);
    row.addEventListener("keydown", onOrderKeyDown);
    orderRowByKey.set(k, row);
  });

  // sync checkbox states
  orderRowByKey.forEach((row, key) => {
    const cb = row.querySelector("input[type=checkbox][data-order-key]");
    if (cb) {
      cb.checked = !!cardSettings[key];
      cb.disabled = false;
      cb.removeEventListener("change", onOrderCheckboxChange);
      cb.addEventListener("change", onOrderCheckboxChange);
    }
  });

  list.innerHTML = "";
  mutableKeys.forEach((k) => {
    const el = orderRowByKey.get(k);
    if (el) list.appendChild(el);
  });
  if (!list.dataset.wired) {
    list.addEventListener("dragover", onOrderListDragOver);
    list.addEventListener("drop", onOrderListDrop);
    list.dataset.wired = "1";
  }
}

let orderDragKey = null;
let orderDropTarget = null; // {type:'before', key} | {type:'end'}
function onOrderDragStart(e) {
  orderDragKey = e.currentTarget.dataset.key;
  e.dataTransfer.effectAllowed = "move";
  const list = document.getElementById("card-order-list");
  if (list) list.classList.add("dragging");
}

// ---------------- Shadow / Inbox (sidebar + card) ----------------

function updateShadowBadge(countOverride) {
  const badge = document.getElementById("shadow-badge");
  if (!badge) return;
  const count = typeof countOverride === "number" ? countOverride : shadowUnits.length;
  if (count > 0) {
    badge.textContent = count > 99 ? "99+" : String(count);
    badge.classList.remove("hidden");
  } else {
    badge.textContent = "0";
    badge.classList.add("hidden");
  }
}

async function loadShadowInbox() {
  setStatus("Inbox: загрузка…");
  try {
    const res = await fetchShadowApi("");
    if (!res.ok) throw new Error(`http ${res.status}`);
    shadowUnits = await res.json();
    updateShadowBadge(shadowUnits.length);
    if (mode !== "inbox") {
      // уже вышли из Inbox к моменту ответа — не перерисовываем sidebar
      logClientEvent("shadow_load_skipped", {
        seq: ++shadowDebugSeq,
        mode,
        shadow_count: shadowUnits.length,
      });
      return;
    }
    renderShadowSidebar();
    renderShadowMarkers();
    const msg =
      shadowUnits.length > 0
        ? `Inbox: новых устройств ${shadowUnits.length}`
        : "Inbox: новых устройств нет";
    setStatus(msg);
  } catch (err) {
    console.error("loadShadowInbox failed", err);
    setStatus("Inbox: ошибка загрузки");
  }
}

function renderShadowSidebar() {
  if (mode !== "inbox") {
    logClientEvent("shadow_render_sidebar_skipped", {
      seq: ++shadowDebugSeq,
      mode,
      shadow_count: shadowUnits.length,
    });
    return;
  }
  logClientEvent("shadow_render_sidebar", {
    seq: ++shadowDebugSeq,
    mode,
    shadow_count: shadowUnits.length,
  });
  ensureListDom();
  if (!innerEl) return;

  // Inbox полностью перерисовывает контейнер: сбрасываем пул виртуализации,
  // чтобы при возврате в monitor ensurePool пересоздал unit-row DOM.
  pool = [];
  poolSize = 0;

  const cardNode = cardEl && innerEl.contains(cardEl) ? cardEl : null;
  innerEl.innerHTML = "";
  if (cardNode) innerEl.appendChild(cardNode);

  if (!shadowUnits.length) {
    const empty = document.createElement("div");
    empty.className = "shadow-empty";
    empty.innerHTML = `
        <div style="text-align:center; padding: 40px 20px; color: var(--muted);">
            <div style="font-size: 40px; margin-bottom: 10px;">📡</div>
            <div>Новых устройств нет</div>
            <div style="font-size: 12px; margin-top:8px;">Здесь появятся трекеры, которые шлют данные,<br>но ещё не созданы в системе.</div>
        </div>
    `;
    innerEl.appendChild(empty);
    hideShadowCard();
    return;
  }

  shadowUnits.forEach((d) => {
    const row = document.createElement("div");
    row.className = "shadow-row";

    const isSelected = selectedShadow && selectedShadow.protocol === d.protocol && selectedShadow.uid === d.uid;
    if (isSelected) row.classList.add("selected");

    const ageSec = d.last_seen_ts ? Math.max(0, Math.floor(Date.now() / 1000 - d.last_seen_ts)) : null;
    const ago = ageSec != null ? formatAgeShort(ageSec) : "—";
    const protoLabel = (d.protocol || "").replace("_ips", "").toUpperCase();

    row.innerHTML = `
      <div class="shadow-row-icon">?</div>
      <div class="shadow-row-body">
        <div class="shadow-row-top">
          <span class="shadow-uid" title="${d.uid}">${d.uid}</span>
          <span class="shadow-ago">${ago}</span>
        </div>
        <div class="shadow-row-bottom">
          <span class="shadow-proto">${protoLabel}</span>
          <span class="shadow-ip">${d.last_ip || ""}</span>
        </div>
      </div>
    `;

    row.addEventListener("click", () => {
      selectShadow(d);
    });
    innerEl.appendChild(row);
  });

  if (cardEl && !innerEl.contains(cardEl)) {
    innerEl.appendChild(cardEl);
  }
}

function selectShadow(device) {
  selectedShadow = device;
  renderShadowSidebar();
  focusShadowMarker(device);
  renderShadowCard();
}

function clearShadowSelection() {
  selectedShadow = null;
  renderShadowSidebar();
  hideShadowCard();
}

function renderShadowCard() {
  if (!cardEl || !selectedShadow) {
    hideShadowCard();
    return;
  }
  const ageSec = selectedShadow.last_seen_ts
    ? Math.max(0, Math.floor(Date.now() / 1000 - selectedShadow.last_seen_ts))
    : null;
  const ago = ageSec != null ? formatAgeShort(ageSec) : "—";
  const coord =
    selectedShadow.lat != null && selectedShadow.lon != null
      ? `${Number(selectedShadow.lat).toFixed(5)}, ${Number(selectedShadow.lon).toFixed(5)}`
      : "—";
  const uidStr = String(selectedShadow.uid || "");
  const shortUidSuffix = uidStr ? uidStr.slice(-4) : "";

  cardEl.innerHTML = `
    <div class="card-head">
      <div class="card-head-main">
        <div class="card-head-name" style="font-size:18px;">${uidStr}</div>
        <div class="card-head-subtitle">
           ${selectedShadow.protocol} · ${selectedShadow.last_ip || "No IP"} · ${ago}${coord !== "—" ? " · " + coord : ""}
        </div>
      </div>
      <div class="card-actions">
        <button class="ghost icon" data-action="shadow-card-close" title="Закрыть">✕</button>
      </div>
    </div>
    <div class="shadow-tabs" style="margin-top: 0;">
      <button class="shadow-tab active" data-shadow-tab="create">Новый объект</button>
      <button class="shadow-tab" data-shadow-tab="bind">Привязать к старому</button>
    </div>
    <div class="shadow-tab-body shadow-tab-create">
      <div class="shadow-form">
        <div style="font-size:13px; color:var(--muted); margin-bottom:8px;">
            Будет создан новый юнит и привязан к этому трекеру.
        </div>
        <label>Имя объекта
          <input id="shadow-create-name" type="text" value="Unit ${shortUidSuffix}" />
        </label>
        <input id="shadow-create-priority" type="hidden" value="0" />
        <button id="shadow-create-btn" class="primary" style="margin-top:8px;">Создать</button>
      </div>
    </div>
    <div class="shadow-tab-body shadow-tab-bind hidden">
      <div class="shadow-form">
        <label>Поиск по имени/ID/UID
          <input id="shadow-bind-search" type="text" placeholder="Начните вводить..." />
        </label>
        <div id="shadow-bind-results" class="shadow-bind-results"></div>
        <div class="shadow-bind-selected">
          <label>Выбранный unit_id
            <input id="shadow-bind-unit" type="number" placeholder="Выберите из списка выше" />
          </label>
          <input id="shadow-bind-priority" type="hidden" value="0" />
        </div>
        <div class="shadow-actions-row">
          <button id="shadow-bind-btn" class="primary">Привязать</button>
          <button id="shadow-ignore-btn" class="ghost danger">Игнорировать</button>
        </div>
      </div>
    </div>
  `;

  cardEl.classList.remove("hidden");
  cardEl.style.top = `${CARD_ANCHOR_OFFSET}px`;

  const tabs = cardEl.querySelectorAll(".shadow-tab");
  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      const target = tab.dataset.shadowTab || "create";
      tabs.forEach((t) => t.classList.toggle("active", t === tab));
      cardEl.querySelectorAll(".shadow-tab-body").forEach((body) => {
        body.classList.toggle("hidden", !body.classList.contains(`shadow-tab-${target}`));
      });
    });
  });

  const createName = document.getElementById("shadow-create-name");
  if (createName && !createName.value) createName.value = `Unit ${selectedShadow.uid}`;
  renderShadowBindResults([]);

  const createBtn = document.getElementById("shadow-create-btn");
  if (createBtn && !createBtn._wired) {
    createBtn._wired = true;
    createBtn.addEventListener("click", async () => {
      try {
        await shadowCreate(selectedShadow.protocol, selectedShadow.uid);
      } catch (err) {
        alert(err.message);
      }
    });
  }

  const bindBtn = document.getElementById("shadow-bind-btn");
  if (bindBtn && !bindBtn._wired) {
    bindBtn._wired = true;
    bindBtn.addEventListener("click", async () => {
      try {
        await shadowBind(selectedShadow.protocol, selectedShadow.uid);
      } catch (err) {
        alert(err.message);
      }
    });
  }

  const ignoreBtn = document.getElementById("shadow-ignore-btn");
  if (ignoreBtn && !ignoreBtn._wired) {
    ignoreBtn._wired = true;
    ignoreBtn.addEventListener("click", async () => {
      try {
        await shadowIgnore(selectedShadow.protocol, selectedShadow.uid);
      } catch (err) {
        alert(err.message);
      }
    });
  }

  const bindSearch = document.getElementById("shadow-bind-search");
  if (bindSearch && !bindSearch._wired) {
    bindSearch._wired = true;
    bindSearch.addEventListener("input", () => {
      const q = bindSearch.value.trim();
      if (shadowBindSearchTimer) clearTimeout(shadowBindSearchTimer);
      shadowBindSearchTimer = setTimeout(() => searchUnitsForBind(q), 200);
    });
  }
}

function hideShadowCard() {
  if (!cardEl) return;
  cardEl.classList.add("hidden");
  cardEl.style.top = "-9999px";
}

function renderShadowBindResults(list) {
  const box = document.getElementById("shadow-bind-results");
  if (!box) return;
  box.innerHTML = "";
  list.forEach((u) => {
    const row = document.createElement("div");
    row.className = "shadow-bind-row";
    row.innerHTML = `
      <div class="unit-name">${u.name || "(без имени)"} · #${u.id}</div>
      <div class="unit-meta">${u.uid || ""}</div>
    `;
    row.addEventListener("click", () => {
      const bindUnit = document.getElementById("shadow-bind-unit");
      if (bindUnit) bindUnit.value = u.id;
    });
    box.appendChild(row);
  });
}

async function searchUnitsForBind(q) {
  if (!q) {
    renderShadowBindResults([]);
    return;
  }
  try {
    const res = await authFetch(`/web/api/admin/units?q=${encodeURIComponent(q)}`);
    if (!res.ok) throw new Error(`http ${res.status}`);
    const data = await res.json();
    renderShadowBindResults(data || []);
  } catch (err) {
    console.error("bind search failed", err);
  }
}

async function shadowCreate(protocol, uid) {
  const nameInput = document.getElementById("shadow-create-name");
  const priorityInput = document.getElementById("shadow-create-priority");
  const name = nameInput?.value?.trim();
  const priority = priorityInput ? Number(priorityInput.value) || 0 : 0;
  if (!name) throw new Error("Укажите имя юнита");
  const res = await fetchShadowApi(`/${protocol}/${uid}/create`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, priority }),
  });
  if (!res.ok) throw new Error(`http ${res.status}`);
  const data = await res.json(); // { status: "ok", unit_id, device_id }
  await loadShadowInbox();
  // сразу добавляем созданный юнит в рабочий список текущего пользователя
  if (data?.unit_id) {
    try {
      await addUnits([data.unit_id], false);
      showToast(`Юнит ${name} добавлен`);
    } catch (e) {
      console.error("Auto-add to worklist failed", e);
    }
  }
  await exitShadowMode({ refreshUnits: true });
}

async function shadowBind(protocol, uid) {
  const unitField = document.getElementById("shadow-bind-unit");
  const priorityInput = document.getElementById("shadow-bind-priority");
  if (!unitField || !unitField.value) throw new Error("Укажите unit_id");
  const unit_id = Number(unitField.value);
  if (!Number.isFinite(unit_id)) throw new Error("unit_id должен быть числом");
  const priority = priorityInput ? Number(priorityInput.value) || 0 : 0;
  const res = await fetchShadowApi(`/${protocol}/${uid}/bind`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ unit_id, priority }),
  });
  if (!res.ok) throw new Error(`http ${res.status}`);
  await loadShadowInbox();
  // привязанный (возможно скрытый) юнит добавляем в рабочий список, чтобы сразу увидеть
  try {
    await addUnits([unit_id], false);
  } catch (e) {
    console.error("Auto-add to worklist failed", e);
  }
  await exitShadowMode({ refreshUnits: true });
}

async function shadowIgnore(protocol, uid) {
  const res = await fetchShadowApi(`/${protocol}/${uid}/ignore`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({}),
  });
  if (!res.ok) throw new Error(`http ${res.status}`);
  await loadShadowInbox();
  await exitShadowMode();
}

async function fetchShadowApi(path, opts = {}) {
  let lastErr = null;
  for (const base of SHADOW_API_PATHS) {
    const url = `${base}${path}`;
    try {
      const res = await authFetch(url, opts);
      if (res.status !== 404) return res;
      lastErr = res;
    } catch (e) {
      lastErr = e;
    }
  }
  if (lastErr) {
    if (lastErr instanceof Response) {
      throw new Error(`http ${lastErr.status}`);
    }
    throw lastErr;
  }
  throw new Error("Shadow API unavailable");
}

function renderShadowMarkers() {
  if (!map) return;
  if (!shadowLayer) {
    shadowLayer = L.layerGroup().addTo(map);
  }
  shadowLayer.clearLayers();
  shadowMarkerByKey.clear();
  shadowUnits.forEach((d) => {
    if (d.lat == null || d.lon == null) return;
    const lat = Number(d.lat);
    const lon = Number(d.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
    const key = `${d.protocol}:${d.uid}`;
    const marker = L.circleMarker([lat, lon], {
      radius: 8,
      color: "#9aa7b8",
      fillColor: "#9aa7b8",
      fillOpacity: 0.8,
      weight: 1,
    }).addTo(shadowLayer);
    marker.bindPopup(`${d.protocol}:${d.uid}`);
    marker.on("click", () => {
      selectShadow(d);
    });
    shadowMarkerByKey.set(key, marker);
  });
}

function focusShadowMarker(device) {
  if (!map || !device) return;
  if (device.lat == null || device.lon == null) return;
  map.setView([Number(device.lat), Number(device.lon)], Math.max(map.getZoom(), 9));
}

function clearShadowMarkers() {
  if (shadowLayer) {
    shadowLayer.clearLayers();
    map?.removeLayer(shadowLayer);
    shadowLayer = null;
  }
  shadowMarkerByKey.clear();
}


function onOrderDragOver(e) {
  e.preventDefault();
  const row = e.currentTarget;
  clearDragHighlights();
  orderDropTarget = { type: "before", key: row.dataset.key };
  row.classList.add("drag-over");
}
function onOrderDragLeave(e) {
  e.currentTarget.classList.remove("drag-over");
}
function onOrderDrop(e) {
  e.preventDefault();
  const row = e.currentTarget;
  row.classList.remove("drag-over");
  if (!orderDragKey || !orderDropTarget) {
    clearDragHighlights();
    return;
  }
  const mutable = cardOrder.filter((k) => !FIXED_CARD_KEYS.includes(k));
  const fromIdx = mutable.indexOf(orderDragKey);
  if (fromIdx === -1) {
    clearDragHighlights();
    return;
  }
  mutable.splice(fromIdx, 1);
  if (orderDropTarget.type === "before") {
    const toIdx = mutable.indexOf(orderDropTarget.key);
    if (toIdx === -1) mutable.push(orderDragKey);
    else mutable.splice(toIdx, 0, orderDragKey);
  } else {
    mutable.push(orderDragKey);
  }
  applyOrder(mutable);
  clearDragHighlights();
}

function onOrderCheckboxChange(e) {
  const key = e.target.dataset.orderKey;
  cardSettings[key] = e.target.checked;
  saveCardSettings();
  persistPanelSettings();
  if (currentCardDetail) renderCard(currentCardDetail);
}

function onOrderKeyDown(e) {
  const key = e.currentTarget.dataset.key;
  if (!key) return;
  if (!e.ctrlKey) return;
  const mutable = cardOrder.filter((k) => !FIXED_CARD_KEYS.includes(k));
  const idx = mutable.indexOf(key);
  if (idx === -1) return;
  if (e.key === "Home") {
    mutable.splice(idx, 1);
    mutable.unshift(key);
    applyOrder(mutable);
    e.preventDefault();
  } else if (e.key === "End") {
    mutable.splice(idx, 1);
    mutable.push(key);
    applyOrder(mutable);
    e.preventDefault();
  }
}

function clearDragHighlights() {
  document.querySelectorAll(".order-item.drag-over").forEach((el) => el.classList.remove("drag-over"));
  orderDropTarget = null;
  const list = document.getElementById("card-order-list");
  if (list) list.classList.remove("dragging");
}

function onOrderListDragOver(e) {
  e.preventDefault();
  orderDropTarget = { type: "end" };
}

function onOrderListDrop(e) {
  e.preventDefault();
  if (!orderDragKey) return;
  orderDropTarget = { type: "end" };
  onOrderDrop(e);
}

function applyOrder(mutableKeys) {
  cardOrder = [...FIXED_CARD_KEYS, ...mutableKeys];
  saveCardOrder(cardOrder);
  persistPanelSettings();
  if (currentCardDetail) renderCard(currentCardDetail);
}

// --- Inline card drag ---
function setupCardDrag() {
  if (!cardEl) return;
  const blocks = Array.from(cardEl.querySelectorAll(".card-grid > .card-block")).filter(
    (el) => !FIXED_CARD_KEYS.includes(el.dataset.sectionKey)
  );
  const grid = cardEl.querySelector(".card-grid");
  blocks.forEach((el) => {
    el.addEventListener("dragstart", onCardDragStart);
    el.addEventListener("dragend", onCardDragEnd);
    el.addEventListener("dragover", onCardDragOver);
    el.addEventListener("dragleave", onCardDragLeave);
    el.addEventListener("drop", onCardDrop);
  });
  if (grid) {
    grid.addEventListener("dragover", (e) => {
      if (!cardLayoutEdit || !cardDragKey) return;
      e.preventDefault();
    });
    grid.addEventListener("drop", (e) => {
      if (!cardLayoutEdit || !cardDragKey) return;
      if (e.target.closest(".card-block")) return; // block-level drop handles reorder
      const draggedEl = grid.querySelector(`[data-section-key="${cardDragKey}"]`);
      if (draggedEl) {
        grid.appendChild(draggedEl);
        applyOrderFromDom();
      }
    });
  }
}

function onCardDragStart(e) {
  const key = e.currentTarget.dataset.sectionKey || e.target.dataset.sectionKey;
  if (!key || FIXED_CARD_KEYS.includes(key)) {
    e.preventDefault();
    return;
  }
  cardDragKey = key;
  e.dataTransfer.effectAllowed = "move";
  e.currentTarget.classList.add("dragging");
}

function onCardDragEnd() {
  if (cardLayoutEdit && cardDragKey) {
    applyOrderFromDom();
  }
  cardDragKey = null;
  clearCardHighlights();
  document.querySelectorAll(".card-block.dragging").forEach((el) => el.classList.remove("dragging"));
}

function onCardDragOver(e) {
  if (!cardLayoutEdit || !cardDragKey) return;
  e.preventDefault();
  const key = e.currentTarget.dataset.sectionKey;
  if (!key || key === cardDragKey || FIXED_CARD_KEYS.includes(key)) return;
  const grid = cardEl?.querySelector(".card-grid");
  const draggedEl = grid?.querySelector(`[data-section-key="${cardDragKey}"]`);
  const targetEl = grid?.querySelector(`[data-section-key="${key}"]`);
  if (!grid || !draggedEl || !targetEl || draggedEl === targetEl) return;

  // текущий порядок
  const order = Array.from(grid.children);
  const dragIdx = order.indexOf(draggedEl);
  const targetIdx = order.indexOf(targetEl);
  if (dragIdx === -1 || targetIdx === -1) return;

  // dead‑zone для стабилизации: реагируем только если зашли в верхние 30% (before) или нижние 30% (after)
  const rect = targetEl.getBoundingClientRect();
  const ratio = (e.clientY - rect.top) / rect.height;
  const moveBefore = ratio < 0.4;
  const moveAfter = ratio > 0.6;

  if (dragIdx < targetIdx) {
    // двигаем вниз, вставляем только если зашли в нижнюю зону или есть блоки между
    if (moveAfter || dragIdx < targetIdx - 1) {
      grid.insertBefore(draggedEl, targetEl.nextSibling);
    }
  } else if (dragIdx > targetIdx) {
    // двигаем вверх, вставляем если в верхнюю зону или есть блоки между
    if (moveBefore || dragIdx > targetIdx + 1) {
      grid.insertBefore(draggedEl, targetEl);
    }
  }

  highlightTarget(targetEl);
}

function onCardDragLeave(e) {
  e.currentTarget.classList.remove("drag-over");
}

function onCardDrop(e) {
  if (!cardLayoutEdit || !cardDragKey) return;
  e.preventDefault();
  clearCardHighlights();
}

function applyOrderFromDom() {
  if (!cardEl) return;
  const keys = Array.from(cardEl.querySelectorAll(".card-grid > .card-block"))
    .map((el) => el.dataset.sectionKey)
    .filter((k) => k && !FIXED_CARD_KEYS.includes(k));
  applyOrder(keys);
}

function clearCardHighlights() {
  document.querySelectorAll(".card-block.drag-over").forEach((el) => el.classList.remove("drag-over"));
}

function highlightTarget(el) {
  clearCardHighlights();
  el.classList.add("drag-over");
}

function handleCardClick(e) {
  const action = e.target.dataset.action;
  if (!action) return;
  if (action === "shadow-card-close") {
    clearShadowSelection();
  } else if (action === "card-close") {
    hideCard();
  } else if (action === "card-nearby") {
    toggleNearbySection();
  } else if (action === "card-layout-toggle") {
    cardLayoutEdit = !cardLayoutEdit;
    if (currentCardDetail) renderCard(currentCardDetail);
  } else if (action === "card-layout-reset") {
    resetCardSettings();
    cardLayoutEdit = true;
    if (currentCardDetail) renderCard(currentCardDetail);
  } else if (action === "trip-settings") {
    if (currentCardDetail?.item?.id) {
      openTripModal(currentCardDetail.item.id);
    } else {
      showToast("Юнит не выбран");
    }
  } else if (action === "card-select") {
    const targetId = Number(e.target.dataset.unitId || e.target.closest("button")?.dataset.unitId);
    if (targetId) {
      selectUnit(targetId);
    }
  } else if (action === "copy-coords") {
    const dataEl = e.target.closest("[data-coords]");
    const coordsStr = dataEl?.dataset?.coords;
    if (coordsStr) {
      copyText(coordsStr);
      showToast("Координаты скопированы");
      return;
    }
    const lat = currentCardDetail?.item?.lat;
    const lon = currentCardDetail?.item?.lon;
    if (lat != null && lon != null) {
      copyText(`${lat}, ${lon}`);
      showToast("Координаты скопированы");
    } else {
      showToast("Координаты недоступны");
    }
  } else if (action === "card-section-toggle") {
    const key = e.target.dataset.sectionKey;
    if (!key) return;
    cardSettings[key] = !cardSettings[key];
    saveCardSettings();
    persistPanelSettings();
    if (currentCardDetail) renderCard(currentCardDetail);
  } else if (action === "open-history") {
    if (currentCardDetail?.item?.id) {
      openHistoryModal(currentCardDetail.item.id);
    } else {
      showToast("Юнит не выбран");
    }
  }
}

function hideCard() {
  cardVisible = false;
  expandedIndex = null;
  cardHeight = 0;
  selectedId = null;
  currentCardDetail = null;
  cardNeedsAnchor = false;
  resetNearbyState(null);
  if (cardEl) {
    cardEl.classList.add("hidden");
    cardEl.style.top = "-9999px";
  }
  renderList();
  logClientEvent("card_hidden", {});
}

function updateCardPlacement() {
  if (!cardEl) return;
  if (!cardVisible || expandedIndex == null) {
    cardEl.classList.add("hidden");
    return;
  }
  const base = (expandedIndex + 1) * ROW_H + CARD_ANCHOR_OFFSET;
  cardEl.style.top = `${base}px`;
  cardEl.classList.remove("hidden");
}

function ensureCardAnchorVisible() {
  if (!listEl || expandedIndex == null || !cardVisible) return;
  const viewport = listEl.clientHeight || 600;
  const currentScroll = listEl.scrollTop || 0;
  const rowTop = expandedIndex * ROW_H;
  const rowBottom = rowTop + ROW_H;
  let newScroll = currentScroll;
  // гарантируем, что строка с выбранным юнитом целиком видна
  if (rowTop < currentScroll) {
    newScroll = rowTop;
  } else if (rowBottom > currentScroll + viewport) {
    newScroll = rowBottom - viewport;
  }
  if (newScroll < 0) newScroll = 0;
  if (newScroll !== currentScroll) listEl.scrollTop = newScroll;
}

function updateKvLabelWidth() {
  if (!cardEl) return;
  const labels = cardEl.querySelectorAll(".kv-label");
  let max = 0;
  labels.forEach((el) => {
    const w = el.offsetWidth;
    if (w > max) max = w;
  });
  cardEl.style.setProperty("--kv-label-width", `${max}px`);
}

function copyText(text) {
  if (!text) return;
  navigator.clipboard?.writeText(text).catch(() => {});
}

async function runUnitAction(action) {
  if (!selectedId) {
    showToast("Сначала выберите объект");
    return;
  }
  try {
    const res = await authFetch(`/web/api/units/${selectedId}/actions/${action}`, { method: "POST" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (data.result?.device_ts) {
      const when = new Date((data.result.device_ts || 0) * 1000).toLocaleString();
      showToast(`Последнее сообщение: ${when}`);
    } else {
      showToast("Действие выполнено");
    }
  } catch (err) {
    console.error("runUnitAction", err);
    showToast("Не удалось выполнить действие");
  }
}

function rebuildUnitIndex() {
  unitsById = new Map();
  units.forEach((u) => {
    unitsById.set(u.id, u);
  });
}

function sortUnits() {
  // Оставляем исходный порядок (по имени) только один раз при загрузке,
  // далее порядок не трогаем, чтобы элементы не прыгали.
  units.sort((a, b) => a.name.localeCompare(b.name, "ru", { sensitivity: "base" }));
}

function scheduleLiveUpdates(reset = false) {
  if (reset && liveTimer) {
    clearTimeout(liveTimer);
    liveTimer = null;
  }
  if (liveTimer) return;
  liveTimer = setTimeout(fetchLiveUpdates, FEED_INTERVAL_MS);
}

async function fetchLiveUpdates() {
  if (liveInFlight) {
    scheduleLiveUpdates();
    return;
  }
  liveInFlight = true;
  try {
    const watchIds = getWatchIds().slice(0, 400);
    const qsParts = [];
    if (feedSince) qsParts.push(`since=${encodeURIComponent(feedSince)}`);
    if (watchIds.length) qsParts.push(`watch_ids=${watchIds.join(",")}`);
    const qs = qsParts.length ? `?${qsParts.join("&")}` : "";
    const res = await authFetch(`/web/api/units/feed${qs}`);
    if (!res.ok) throw new Error("feed http " + res.status);
    const data = await res.json();
    lastFeedTick = Date.now();
    if (typeof data.shadow_count === "number") {
      updateShadowBadge(data.shadow_count);
    }
    if (typeof data.ts === "number") {
      feedSince = data.ts;
    }
    if (data.reset) {
      await loadUnits({ restartLive: true });
      return;
    }
    if (Array.isArray(data.updates) && data.updates.length) {
      applyFeedUpdates(data.updates);
    }
  } catch (err) {
    console.warn("live updates failed", err);
  } finally {
    liveInFlight = false;
    liveTimer = null;
    scheduleLiveUpdates();
  }
}

function applyFeedUpdates(updates) {
  let changed = false;
  const touched = new Set();
  updates.forEach((patch) => {
    const unit = unitsById.get(patch.id);
    if (!unit) {
      return;
    }
    touched.add(patch.id);
    changed = mergeUnitPatch(unit, patch) || changed;
  });
  if (!changed) return;
  lastLiveUpdated = Date.now();
  // Не пересортировываем список, чтобы объекты не прыгали.
  applyFilters();
  if (selectedId && touched.has(selectedId)) {
    refreshCurrentCard();
  }
}

function mergeUnitPatch(unit, patch) {
  let mutated = false;
  [
    "online",
    "status",
    "status_label",
    "last_ts",
    "last_ts_age_sec",
    "stop_duration_s",
    "ignition",
    "lat",
    "lon",
    "speed",
    "has_fuel",
    "offline_reason",
    "tooltip_data",
  ].forEach((key) => {
    if (Object.prototype.hasOwnProperty.call(patch, key) && unit[key] !== patch[key]) {
      unit[key] = patch[key];
      mutated = true;
    }
  });
  return mutated;
}

async function refreshCurrentCard() {
  if (!selectedId) return;
  try {
    const res = await authFetch(`/web/api/units/${selectedId}`);
    if (!res.ok) return;
    const data = await res.json();
    currentCardDetail = data;
    renderCard(data);
  } catch (err) {
    console.warn("card refresh failed", err);
  }
}

function loadTabState() {
  try {
    const raw = localStorage.getItem(TAB_STATE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") return parsed;
    return {};
  } catch {
    return {};
  }
}

function saveTabState() {
  localStorage.setItem(TAB_STATE_KEY, JSON.stringify(tabState));
}

function setTabState(tabId, patch) {
  if (!tabId) return;
  const prev = tabState[tabId] || {};
  tabState[tabId] = { ...prev, ...patch };
  saveTabState();
}

function deleteTabState(tabId) {
  if (!tabId || !tabState[tabId]) return;
  delete tabState[tabId];
  saveTabState();
}

function restoreSearchForActiveTab() {
  const input = document.getElementById("search");
  if (!input) return;
  const state = tabState[activeTabId];
  const target = state?.search ?? "";
  if (input.value !== target) {
    input.value = target;
  }
}

function ensureWorkTabList(tabs) {
  let result = Array.isArray(tabs) ? tabs.filter(Boolean).map((tab) => ({ ...tab })) : [];
  if (!result.some((tab) => tab.id === "work")) {
    result = [Object.assign({}, DEFAULT_TABS[0]), ...result];
  }
  return result;
}

function loadSidebarWidth() {
  const raw = localStorage.getItem(SIDEBAR_WIDTH_KEY);
  const parsed = Number.parseInt(raw || "", 10);
  if (Number.isFinite(parsed)) {
    return clampSidebarWidth(parsed);
  }
  return 380;
}

function clampSidebarWidth(value) {
  return Math.min(SIDEBAR_MAX, Math.max(SIDEBAR_MIN, value));
}

function applySidebarWidth(width) {
  sidebarWidth = clampSidebarWidth(width);
  document.documentElement.style.setProperty("--sidebar-width", `${sidebarWidth}px`);
}

function resetNearbyState(unitId) {
  if (unitId == null) {
    nearbyCardState = { unitId: null, visible: false, list: [] };
  } else {
    nearbyCardState = { unitId, visible: false, list: [] };
  }
}

function toggleNearbySection() {
  if (!currentCardDetail?.item) return;
  const unitId = currentCardDetail.item.id;
  if (nearbyCardState.visible) {
    nearbyCardState = { unitId, visible: false, list: [] };
    renderCard(currentCardDetail);
    return;
  }
  const list = computeNearbyUnits(currentCardDetail.item);
  if (!list.length) {
    showToast("Нет координат для расчёта");
    return;
  }
  nearbyCardState = { unitId, visible: true, list };
  renderCard(currentCardDetail);
}

function computeNearbyUnits(baseItem) {
  const baseLat = Number(baseItem.lat);
  const baseLon = Number(baseItem.lon);
  if (!Number.isFinite(baseLat) || !Number.isFinite(baseLon)) {
    return [];
  }
  return units
    .map((u) => ({ ...u, latNum: Number(u.lat), lonNum: Number(u.lon) }))
    .filter((u) => u.id !== baseItem.id && Number.isFinite(u.latNum) && Number.isFinite(u.lonNum))
    .map((u) => {
      const distKm = haversineKm(baseLat, baseLon, u.latNum, u.lonNum);
      const formatted = formatDistance(distKm);
      return {
        id: u.id,
        name: u.name,
        distance: formatted.label,
        distanceVal: formatted.value,
      };
    })
    .sort((a, b) => a.distanceVal - b.distanceVal)
    .slice(0, 5);
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function formatDistance(km) {
  if (!Number.isFinite(km)) {
    return { label: "—", value: Infinity };
  }
  if (km < 1) {
    const meters = Math.round(km * 1000);
    return { label: `${meters} м`, value: km };
  }
  const fixed = km < 10 ? km.toFixed(1) : km.toFixed(0);
  return { label: `${fixed} км`, value: km };
}

function initLayout() {
  applySidebarWidth(sidebarWidth);
  const resizer = document.getElementById("sidebar-resizer");
  if (!resizer) return;
  resizer.addEventListener("pointerdown", startResize);
}

let resizeStartX = 0;
let resizeStartWidth = sidebarWidth;

function startResize(event) {
  event.preventDefault();
  resizerPointerId = event.pointerId;
  resizeStartX = event.clientX;
  resizeStartWidth = sidebarWidth;
  const resizer = document.getElementById("sidebar-resizer");
  if (resizer) {
    resizer.classList.add("dragging");
    resizer.setPointerCapture(resizerPointerId);
  }
  window.addEventListener("pointermove", onResizeMove);
  window.addEventListener("pointerup", stopResize);
}

function onResizeMove(event) {
  if (resizerPointerId !== event.pointerId) return;
  const delta = event.clientX - resizeStartX;
  applySidebarWidth(resizeStartWidth + delta);
}

function stopResize(event) {
  if (resizerPointerId !== event.pointerId) return;
  const resizer = document.getElementById("sidebar-resizer");
  if (resizer) {
    resizer.classList.remove("dragging");
    resizer.releasePointerCapture(resizerPointerId);
  }
  window.removeEventListener("pointermove", onResizeMove);
  window.removeEventListener("pointerup", stopResize);
  resizerPointerId = null;
  localStorage.setItem(SIDEBAR_WIDTH_KEY, String(sidebarWidth));
}

function filterRowTemplate(row, index, total, sensorListId) {
  const kind = row.kind || "sensor";
  const value = row.value || "";
  const valueControl =
    kind === "status"
      ? buildStatusSelect(value)
      : `<input class="filter-value" type="text" placeholder="Название датчика" ${sensorListId ? `list="${sensorListId}"` : ""} value="${value}" />`;
  return `
    <div class="filter-row" data-index="${index}">
      <select class="filter-kind">
        <option value="sensor" ${kind === "sensor" ? "selected" : ""}>Датчик</option>
        <option value="status" ${kind === "status" ? "selected" : ""}>Статус</option>
      </select>
      ${valueControl}
      <button class="ghost icon" data-action="filter-remove" ${total <= 1 ? "disabled" : ""} title="Удалить условие">✕</button>
    </div>
  `;
}

function buildStatusSelect(value) {
  return `
    <select class="filter-value">
      <option value="online" ${value === "online" ? "selected" : ""}>Только онлайн</option>
      <option value="offline" ${value === "offline" ? "selected" : ""}>Только оффлайн</option>
      <option value="has_fuel" ${value === "has_fuel" ? "selected" : ""}>Есть ДУТ</option>
    </select>
  `;
}

function initTabModalFilters() {
  const rowsBox = document.getElementById("tab-filter-rows");
  const addBtn = document.getElementById("tab-filter-add");
  if (!rowsBox || !addBtn) return;
  if (!tabFilterRows.length) {
    tabFilterRows = [{ kind: "sensor", value: "" }];
  }
  rowsBox.addEventListener("input", handleTabFilterInput);
  rowsBox.addEventListener("change", handleTabFilterInput);
  rowsBox.addEventListener("click", handleTabFilterClick);
  addBtn.addEventListener("click", () => {
    tabFilterRows.push({ kind: "sensor", value: "" });
    renderTabFilterRows();
  });
  renderTabFilterRows();
}

function renderTabFilterRows() {
  const rowsBox = document.getElementById("tab-filter-rows");
  if (!rowsBox) return;
  const total = tabFilterRows.length || 1;
  rowsBox.innerHTML = tabFilterRows.map((row, idx) => filterRowTemplate(row, idx, total, "")).join("");
  rowsBox.querySelectorAll(".filter-row").forEach((row) => {
    const btn = row.querySelector("[data-action=\"filter-remove\"]");
    if (btn) {
      if (total <= 1) btn.setAttribute("disabled", "disabled");
      else btn.removeAttribute("disabled");
    }
  });
}

function handleTabFilterInput(event) {
  const rowEl = event.target.closest(".filter-row");
  if (!rowEl) return;
  const index = Number(rowEl.dataset.index);
  if (!Number.isFinite(index) || !tabFilterRows[index]) return;
  if (event.target.classList.contains("filter-kind")) {
    tabFilterRows[index].kind = event.target.value;
    if (tabFilterRows[index].kind === "status" && !tabFilterRows[index].value) {
      tabFilterRows[index].value = "online";
    }
    renderTabFilterRows();
    return;
  }
  if (event.target.classList.contains("filter-value")) {
    tabFilterRows[index].value = event.target.value;
    return;
  }
  renderTabFilterRows();
}

function handleTabFilterClick(event) {
  if (event.target.dataset.action !== "filter-remove") return;
  const rowEl = event.target.closest(".filter-row");
  if (!rowEl) return;
  const index = Number(rowEl.dataset.index);
  if (!Number.isFinite(index) || tabFilterRows.length <= 1) return;
  tabFilterRows.splice(index, 1);
  renderTabFilterRows();
}

function buildFiltersFromRows(rows) {
  const filters = {};
  rows.forEach((row) => {
    if (row.kind === "sensor") {
      const value = (row.value || "").trim().toLowerCase();
      if (value) {
        filters.sensorQuery = filters.sensorQuery ? `${filters.sensorQuery} ${value}` : value;
      }
    } else if (row.kind === "status") {
      if (row.value === "online") {
        filters.online = true;
        filters.offline = false;
      } else if (row.value === "offline") {
        filters.offline = true;
        filters.online = false;
      } else if (row.value === "has_fuel") {
        filters.hasFuel = true;
      }
    }
  });
  return filters;
}

function adjustScrollTop(scrollTop) {
  if (!cardVisible || expandedIndex == null || cardHeight <= 0) {
    return scrollTop;
  }
  const cardTop = (expandedIndex + 1) * ROW_H + CARD_ANCHOR_OFFSET;
  if (scrollTop <= cardTop) {
    return scrollTop;
  }
  const past = Math.min(scrollTop - cardTop, cardHeight);
  return scrollTop - past;
}

/* ================= HISTORY MODULE (треки) ================= */

function initHistory() {
  const backBtn = document.getElementById("history-back");
  backBtn?.addEventListener("click", exitHistoryMode);
  document.getElementById("hist-prev")?.addEventListener("click", () => shiftHistoryDate(-1));
  document.getElementById("hist-next")?.addEventListener("click", () => shiftHistoryDate(1));
  document.getElementById("history-refresh-btn")?.addEventListener("click", loadHistoryTrips);
  initHistoryCalendar();
  // курсор по треку
  if (map) {
    map.on("mousemove", onMapMouseMove);
  }

  initHistoryChart();
  initTrackPlayer();
}

function shiftHistoryDate(deltaDays) {
  histDate.setDate(histDate.getDate() + deltaDays);
  syncHistoryDateInputs(false);
  loadHistoryTrips();
}

function openHistoryModal(unitId) {
  const main = document.getElementById("sidebar-main");
  const hist = document.getElementById("sidebar-history");
  main?.classList.add("hidden");
  hist?.classList.remove("hidden");
  hideCard();
  histUnitId = unitId;
  const u = unitsById.get(unitId);
  const nameEl = document.getElementById("history-unit-name");
  if (nameEl) nameEl.textContent = u ? u.name : `Unit #${unitId}`;
  histDate = new Date();
  syncHistoryDateInputs(true);
  calendarCache = {};
  refreshCalendarMonth();
  clearHistoryMap();
  trackPointsCache = [];
  loadHistoryTrips();
}

function exitHistoryMode() {
  const main = document.getElementById("sidebar-main");
  const hist = document.getElementById("sidebar-history");
  main?.classList.remove("hidden");
  hist?.classList.add("hidden");
  clearHistoryMap();
  trackPointsCache = [];
  hideTrackCursor();
}

function syncHistoryDateInputs(setInputValue) {
  const picker = document.getElementById("hist-date-picker");
  if (picker && setInputValue) {
    picker.value = histDate.toISOString().slice(0, 10);
  }
  if (calendarInstance) {
    calendarInstance.setDate(histDate, false);
  }
}

function refreshCalendarMonth() {
  if (!calendarInstance) return;
  updateCalendarActivity(calendarInstance.currentYear, calendarInstance.currentMonth, calendarInstance);
}

function initHistoryCalendar() {
  const input = document.getElementById("hist-date-picker");
  if (!input || !window.flatpickr) {
    // fallback: keep native date input behavior
    input && (input.valueAsDate = histDate);
    input?.addEventListener("change", (e) => {
      if (e.target.valueAsDate) {
        histDate = e.target.valueAsDate;
        loadHistoryTrips();
      }
    });
    return;
  }

  calendarInstance = flatpickr(input, {
    locale: "ru",
    dateFormat: "d.m.Y",
    disableMobile: true,
    defaultDate: histDate,
    onChange: (selectedDates) => {
      if (selectedDates[0]) {
        histDate = selectedDates[0];
        loadHistoryTrips();
      }
    },
    onMonthChange: (_sel, _str, inst) => {
      updateCalendarActivity(inst.currentYear, inst.currentMonth, inst);
    },
    onOpen: (_sel, _str, inst) => {
      updateCalendarActivity(inst.currentYear, inst.currentMonth, inst);
    },
    onDayCreate: (_dObj, _dStr, fp, dayElem) => decorateCalendarDay(dayElem),
  });
}

async function updateCalendarActivity(year, monthIndex, instance) {
  if (!histUnitId) return;
  const monthStr = `${year}-${String(monthIndex + 1).padStart(2, "0")}`;
  if (!calendarCache[monthStr]) {
    try {
      const res = await authFetch(`/web/api/history/calendar?unit_id=${histUnitId}&month=${monthStr}`);
      if (res.ok) {
        calendarCache[monthStr] = await res.json();
      }
    } catch (e) {
      console.error("calendar fetch error", e);
    }
  }
  if (instance) instance.redraw();
}

function decorateCalendarDay(dayElem) {
  if (!dayElem?.dateObj) return;
  const y = dayElem.dateObj.getFullYear();
  const m = String(dayElem.dateObj.getMonth() + 1).padStart(2, "0");
  const d = String(dayElem.dateObj.getDate()).padStart(2, "0");
  const monthKey = `${y}-${m}`;
  const key = `${y}-${m}-${d}`;
  const monthData = calendarCache[monthKey];
  if (!monthData || !monthData[key]) return;
  const dist = monthData[key];
  const dot = document.createElement("span");
  dot.className = "event-dot";
  if (dist > 300_000) dot.style.background = "#4caf50"; // >300 км
  else if (dist > 50_000) dot.style.background = "#ff9800"; // >50 км
  else dot.style.background = "#9e9e9e";
  dayElem.appendChild(dot);
}

async function loadHistoryTrips() {
  if (!histUnitId) return;
  const listEl = document.getElementById("history-timeline");
  if (listEl) listEl.innerHTML = '<div class="muted" style="padding:20px; text-align:center">Загрузка...</div>';

  const from = new Date(histDate);
  from.setHours(0, 0, 0, 0);
  const to = new Date(histDate);
  to.setHours(23, 59, 59, 999);
  const fromTs = Math.floor(from.getTime() / 1000);
  const toTs = Math.floor(to.getTime() / 1000);

  try {
    const res = await authFetch(`/web/api/history/trips?unit_id=${histUnitId}&from_ts=${fromTs}&to_ts=${toTs}`);
    if (!res.ok) throw new Error(`http ${res.status}`);
    const trips = await res.json();
    renderTimeline(trips);
    calculateDailyStats(trips);
    drawTripMarkers(trips);
  } catch (e) {
    console.error("history trips error", e);
    if (listEl) listEl.innerHTML = '<div class="muted" style="padding:20px; text-align:center;color:#ef5350">Ошибка загрузки</div>';
    setStatus("История: ошибка загрузки");
  }
}

function calculateDailyStats(trips) {
  let totalDist = 0;
  let maxSpd = 0;
  let moveSec = 0;
  trips?.forEach((t) => {
    if (t.type === "trip") {
      totalDist += t.distance_m || 0;
      if (t.max_speed > maxSpd) maxSpd = t.max_speed;
      moveSec += Math.max(0, (t.end_ts || 0) - (t.start_ts || 0));
    }
  });

  const distKm = (totalDist / 1000).toFixed(1);
  const h = Math.floor(moveSec / 3600);
  const m = Math.floor((moveSec % 3600) / 60);
  const timeStr = h ? `${h}ч ${m}м` : `${m} мин`;
  const avgSpeed = moveSec > 0 ? Math.round((totalDist / moveSec) * 3.6) : 0;

  const distEl = document.getElementById("hs-dist");
  const timeEl = document.getElementById("hs-time");
  const maxEl = document.getElementById("hs-max");
  const avgEl = document.getElementById("hs-avg");
  if (distEl) distEl.textContent = distKm;
  if (timeEl) timeEl.textContent = timeStr;
  if (maxEl) maxEl.textContent = maxSpd;
  if (avgEl) avgEl.textContent = avgSpeed;
}

function renderTimeline(trips) {
  const listEl = document.getElementById("history-timeline");
  if (!listEl) return;
  listEl.innerHTML = "";
  if (!trips || !trips.length) {
    listEl.innerHTML = '<div class="muted" style="padding:20px; text-align:center">Нет поездок за день</div>';
    return;
  }
  trips.forEach((t) => {
    const el = document.createElement("div");
    const isTrip = t.type === "trip";
    const isStop = t.type === "stop";
    const typeClass = isTrip ? "trip" : isStop ? "stop" : "stay";
    el.className = `timeline-item ${typeClass}`;
    el.dataset.id = t.id;
    const start = new Date(t.start_ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    const end = new Date(t.end_ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

    let iconContent = "";
    let title = "";
    let meta = "";

    if (isTrip) {
      iconContent = "🚗";
      const km = (t.distance_m / 1000).toFixed(1);
      title = `Поездка ${km} км`;
      meta = `макс ${t.max_speed} км/ч`;
    } else if (isStop) {
      iconContent = "🛑";
      title = "Остановка";
      meta = "двигатель работал";
    } else {
      iconContent = "🅿️";
      title = "Стоянка";
      meta = "двигатель выкл";
    }

    const addr = t.start_address || "Адрес определяется...";
    el.innerHTML = `
      <div class="t-icon-box">${iconContent}</div>
      <div class="t-body">
        <div class="t-header">
          <span>${title}</span>
          <span class="t-time-range">${start} — ${end}</span>
        </div>
        <div class="t-meta">
          <span>${t.duration_str}</span>
          ${meta ? `<span>• ${meta}</span>` : ""}
        </div>
        <div class="t-addr" title="${addr}">${addr}</div>
      </div>
    `;
    el.addEventListener("click", () => {
      listEl.querySelectorAll(".timeline-item").forEach((x) => x.classList.remove("active"));
      el.classList.add("active");
      loadTrackOnMap(t);
    });
    listEl.appendChild(el);
  });
}

async function loadTrackOnMap(trip) {
  clearHistoryMap();
  if (!map) return;
  document.getElementById("track-cursor-info")?.classList.add("hidden");
  try {
    const res = await authFetch(`/web/api/history/track?unit_id=${trip.unit_id}&from_ts=${trip.start_ts}&to_ts=${trip.end_ts}`);
    if (!res.ok) throw new Error(`http ${res.status}`);
    const points = await res.json();
    if (!points.length) {
      showToast("Нет точек трека");
      return;
    }
    trackPointsCache = points;
    histLayerGroup = L.layerGroup().addTo(map);
    drawColoredTrack(points, histLayerGroup);
    const startPt = [points[0].lat, points[0].lon];
    const endPt = [points[points.length - 1].lat, points[points.length - 1].lon];
    createMarker(startPt, "A", "#4caf50").addTo(histLayerGroup);
    createMarker(endPt, "B", "#f44336").addTo(histLayerGroup);
    drawArrows(points, histLayerGroup);
    const latlngs = points.map((p) => [p.lat, p.lon]);
    map.fitBounds(L.latLngBounds(latlngs), { padding: [50, 50] });

    attachTrackClickLayer(latlngs);
    initPlayerSlider(points);
    renderHistoryChart(points);
    showToast("Трек загружен");
  } catch (e) {
    console.error("history track error", e);
    showToast("Ошибка загрузки трека");
  }
}

function getSpeedColor(speed) {
  if (speed < 5) return "#3b82f6"; // стоянка/медленно
  if (speed < 60) return "#4caf50"; // город
  if (speed < 90) return "#ff9800"; // трасса
  return "#f44336"; // быстро
}

// Bearing between two lat/lon points (deg, 0..360)
function getBearing(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const toDeg = (r) => (r * 180) / Math.PI;
  const y = Math.sin(toRad(lon2 - lon1)) * Math.cos(toRad(lat2));
  const x =
    Math.cos(toRad(lat1)) * Math.sin(toRad(lat2)) -
    Math.sin(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.cos(toRad(lon2 - lon1));
  return (toDeg(Math.atan2(y, x)) + 360) % 360;
}

function initHistoryChart() {
  const ctx = document.getElementById("history-chart");
  const wrapper = document.getElementById("history-chart-wrapper");
  if (!ctx || historyChart) return;

  const verticalLinePlugin = {
    id: "historyVerticalLine",
    afterDraw: (chart) => {
      const active = chart.tooltip?._active;
      if (active && active.length) {
        const x = active[0].element.x;
        const { top, bottom } = chart.scales.y;
        const c = chart.ctx;
        c.save();
        c.beginPath();
        c.moveTo(x, top);
        c.lineTo(x, bottom);
        c.lineWidth = 1;
        c.setLineDash([5, 5]);
        c.strokeStyle = "rgba(255,255,255,0.5)";
        c.stroke();
        c.restore();
      }
    },
  };

  historyChart = new Chart(ctx, {
    type: "line",
    data: { labels: [], datasets: [] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "rgba(17,24,39,0.9)",
          borderColor: "#4caf50",
          borderWidth: 1,
          titleColor: "#fff",
          bodyColor: "#cbd5e1",
        },
      },
      scales: {
        x: { display: false },
        y: {
          beginAtZero: true,
          grid: { color: "rgba(255,255,255,0.08)" },
          ticks: { color: "#9ca3af", font: { size: 10 } },
        },
      },
      onHover: (evt, elements) => {
        if (elements && elements.length > 0) {
          const idx = elements[0].index;
          if (trackPointsCache && trackPointsCache[idx]) {
            showTrackCursor(trackPointsCache[idx]);
          }
        }
      },
    },
    plugins: [verticalLinePlugin],
  });

  if (wrapper) wrapper.classList.add("hidden");
}

function renderHistoryChart(points) {
  const wrapper = document.getElementById("history-chart-wrapper");
  if (!wrapper) return;
  if (!points || points.length < 2) {
    wrapper.classList.add("hidden");
    return;
  }

  if (!historyChart) initHistoryChart();
  if (!historyChart) return;

  wrapper.classList.remove("hidden");

  const labels = points.map((p) => new Date(p.ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }));
  const speeds = points.map((p) => Math.round(p.speed || 0));

  historyChart.data.labels = labels;
  historyChart.data.datasets = [
    {
      label: "Скорость (км/ч)",
      data: speeds,
      borderColor: "#4caf50",
      borderWidth: 2,
      fill: true,
      backgroundColor: (ctx) => {
        const g = ctx.chart.ctx.createLinearGradient(0, 0, 0, 200);
        g.addColorStop(0, "rgba(76,175,80,0.35)");
        g.addColorStop(1, "rgba(76,175,80,0)");
        return g;
      },
      pointRadius: 0,
      pointHoverRadius: 4,
      tension: 0.2,
    },
  ];

  historyChart.update();
}

// -------------------- Плеер трека --------------------

function initTrackPlayer() {
  const btn = document.getElementById("track-play-btn");
  const slider = document.getElementById("track-slider");
  const speedSel = document.getElementById("player-speed");

  if (btn) btn.onclick = togglePlay;

  if (slider) {
    slider.oninput = (e) => {
      stopPlayer();
      const btnPlay = document.getElementById("track-play-btn");
      if (btnPlay) {
        btnPlay.textContent = "▶";
        btnPlay.classList.remove("playing");
      }
      playerState.idx = Number(e.target.value);
      updatePlayerVisuals(false);
    };
  }

  if (speedSel) {
    speedSel.onchange = (e) => {
      playerState.speedMultiplier = Number(e.target.value) / 10;
    };
  }
}

function initPlayerSlider(points) {
  const slider = document.getElementById("track-slider");
  if (slider) {
    slider.max = points.length - 1;
    slider.value = 0;
    slider.disabled = false;
  }
  const speedSel = document.getElementById("player-speed");
  playerState.idx = 0;
  playerState.speedMultiplier = (speedSel ? Number(speedSel.value) : 100) / 10;
}

function togglePlay() {
  const btn = document.getElementById("track-play-btn");
  if (!btn) return;

  if (playerState.isPlaying) {
    stopPlayer();
    btn.textContent = "▶";
    btn.classList.remove("playing");
    return;
  }

  if (!trackPointsCache || trackPointsCache.length < 2) {
    showToast("Нет трека для воспроизведения");
    return;
  }

  if (playerState.idx >= trackPointsCache.length - 1) playerState.idx = 0;
  playerState.isPlaying = true;
  btn.textContent = "⏸";
  btn.classList.add("playing");

  playerLoop();
}

function stopPlayer() {
  playerState.isPlaying = false;
  if (playerState.animFrame) cancelAnimationFrame(playerState.animFrame);
}

function playerLoop() {
  if (!playerState.isPlaying) return;
  playerState.idx += playerState.speedMultiplier * 0.2;

  if (playerState.idx >= trackPointsCache.length - 1) {
    playerState.idx = trackPointsCache.length - 1;
    updatePlayerVisuals(true);
    stopPlayer();
    const btn = document.getElementById("track-play-btn");
    if (btn) {
      btn.textContent = "▶";
      btn.classList.remove("playing");
    }
    return;
  }

  updatePlayerVisuals(true);
  playerState.animFrame = requestAnimationFrame(playerLoop);
}

function updatePlayerVisuals(shouldPan = false) {
  if (!trackPointsCache.length) return;
  const i = Math.floor(playerState.idx);
  const pt = trackPointsCache[i];
  if (!pt) return;

  if (!playerState.marker) {
    playerState.marker = L.marker([pt.lat, pt.lon], {
      icon: L.divIcon({
        className: "player-marker-icon",
        html: '<div class="nav-arrow"></div>',
        iconSize: [30, 30],
        iconAnchor: [15, 15],
      }),
      zIndexOffset: 1000,
    }).addTo(map);
  }

  playerState.marker.setLatLng([pt.lat, pt.lon]);

  let nextPt = trackPointsCache[i + 5] || trackPointsCache[trackPointsCache.length - 1];
  if (nextPt && nextPt !== pt) {
    const angle = getBearing(pt.lat, pt.lon, nextPt.lat, nextPt.lon);
    const iconEl = playerState.marker.getElement();
    if (iconEl) {
      const inner = iconEl.querySelector(".nav-arrow");
      if (inner) inner.style.transform = `rotate(${angle}deg)`;
    }
  }

  if (shouldPan && map && !map.getBounds().pad(-0.1).contains([pt.lat, pt.lon])) {
    map.panTo([pt.lat, pt.lon], { animate: true, duration: 0.5 });
  }

  const slider = document.getElementById("track-slider");
  const timeLbl = document.getElementById("player-time-lbl");
  if (slider) slider.value = i;
  if (timeLbl) timeLbl.textContent = new Date(pt.ts * 1000).toLocaleTimeString();

  showTrackCursor(pt);

  if (historyChart) {
    const meta = historyChart.getDatasetMeta(0);
    if (meta?.data?.[i]) {
      const el = meta.data[i];
      historyChart.tooltip.setActiveElements(
        [{ datasetIndex: 0, index: i }],
        { x: el.x, y: el.y }
      );
      historyChart.update();
    }
  }
}

function drawColoredTrack(points, layerGroup) {
  if (points.length < 2) return;
  let segment = [];
  let currentColor = getSpeedColor(points[0].speed || 0);

  for (let i = 0; i < points.length - 1; i++) {
    const p = points[i];
    const color = getSpeedColor(p.speed || 0);
    segment.push([p.lat, p.lon]);
    const colorChanged = color !== currentColor;
    if (colorChanged) {
      L.polyline(segment, {
        color: currentColor,
        weight: 5,
        opacity: 0.9,
        lineJoin: "round",
        lineCap: "round",
      }).addTo(layerGroup);
      segment = [[p.lat, p.lon]];
      currentColor = color;
    }
  }

  // хвост + последняя точка
  const last = points[points.length - 1];
  segment.push([last.lat, last.lon]);
  L.polyline(segment, {
    color: currentColor,
    weight: 5,
    opacity: 0.9,
    lineJoin: "round",
    lineCap: "round",
  }).addTo(layerGroup);
}

// Клик по линии трека для телепорта плеера
function attachTrackClickLayer(latlngs) {
  const clickLine = L.polyline(latlngs, {
    color: "transparent",
    weight: 20,
    opacity: 0,
  }).addTo(histLayerGroup);

  clickLine.on("click", (e) => {
    if (!trackPointsCache?.length) return;
    const clat = e.latlng.lat;
    const clon = e.latlng.lng;
    let minDist = Infinity;
    let closest = 0;
    for (let i = 0; i < trackPointsCache.length; i += 5) {
      const p = trackPointsCache[i];
      const d = Math.abs(p.lat - clat) + Math.abs(p.lon - clon);
      if (d < minDist) {
        minDist = d;
        closest = i;
      }
    }
    playerState.idx = closest;
    stopPlayer();
    updatePlayerVisuals(false);
  });
}

// Маркеры остановок/стоянок
function drawTripMarkers(trips) {
  if (!map) return;
  if (!eventMarkersLayer) {
    eventMarkersLayer = L.layerGroup().addTo(map);
  } else {
    eventMarkersLayer.clearLayers();
  }

  trips.forEach((t) => {
    if (t.type !== "stop" && t.type !== "stay") return;
    const lat = t.start_lat || t.lat || t.start_latitude;
    const lon = t.start_lon || t.lon || t.start_longitude;
    if (!lat || !lon) return;
    const isStay = t.type === "stay";
    const label = isStay ? "P" : "S";
    const color = isStay ? "#3b82f6" : "#ef5350";
    const icon = L.divIcon({
      className: "map-stop-icon",
      html: `<div style="width:100%;height:100%;border-radius:50%;background:${color};display:flex;align-items:center;justify-content:center;">${label}</div>`,
      iconSize: [20, 20],
      iconAnchor: [10, 10],
    });
    const marker = L.marker([lat, lon], { icon }).bindTooltip(
      `${isStay ? "Стоянка" : "Остановка"} (${t.duration_str || ""})`,
      { offset: [0, -10], direction: "top" }
    );
    marker.on("click", () => scrollToTimelineItem(t.id));
    marker.addTo(eventMarkersLayer);
  });
}

function scrollToTimelineItem(id) {
  const el = document.querySelector(`.timeline-item[data-id='${id}']`);
  if (!el) return;
  el.scrollIntoView({ behavior: "smooth", block: "center" });
  el.classList.add("highlight-flash");
  setTimeout(() => el.classList.remove("highlight-flash"), 1000);
}

function createMarker(latlng, label, color) {
  return L.marker(latlng, {
    icon: L.divIcon({
      className: "map-label-icon",
      html: `<div style="background:${color}; width:24px; height:24px; border-radius:50%; border:2px solid #fff; color:#fff; font-weight:bold; display:flex; align-items:center; justify-content:center; box-shadow:0 2px 5px rgba(0,0,0,0.5); font-size:12px;">${label}</div>`,
      iconSize: [24, 24],
      iconAnchor: [12, 12],
    }),
  });
}

function drawArrows(points, layerGroup) {
  if (points.length < 2) return;
  const step = Math.max(10, Math.floor(points.length / 15));
  for (let i = 0; i < points.length - 1; i += step) {
    const p1 = points[i];
    const p2 = points[i + 1];
    const pp1 = map.latLngToLayerPoint([p1.lat, p1.lon]);
    const pp2 = map.latLngToLayerPoint([p2.lat, p2.lon]);
    const dx = pp2.x - pp1.x;
    const dy = pp2.y - pp1.y;
    if (dx * dx + dy * dy < 20) continue;
    const angle = (Math.atan2(dy, dx) * 180) / Math.PI;
    L.marker([p2.lat, p2.lon], {
      icon: L.divIcon({
        className: "",
        html: `<div class="arrow-icon" style="transform: rotate(${angle}deg);">➤</div>`,
        iconSize: [20, 20],
        iconAnchor: [10, 10],
      }),
      interactive: false,
    }).addTo(layerGroup);
  }
}

function clearHistoryMap() {
  if (histLayerGroup) {
    map.removeLayer(histLayerGroup);
    histLayerGroup = null;
  }
  if (trackCursorMarker) {
    map.removeLayer(trackCursorMarker);
    trackCursorMarker = null;
  }
  stopPlayer();
  playerState.idx = 0;
  playerState.isPlaying = false;
  if (eventMarkersLayer) eventMarkersLayer.clearLayers();
  if (playerState.marker) {
    map.removeLayer(playerState.marker);
    playerState.marker = null;
  }

  const btn = document.getElementById("track-play-btn");
  if (btn) {
    btn.textContent = "▶";
    btn.classList.remove("playing");
  }

  const slider = document.getElementById("track-slider");
  const timeLbl = document.getElementById("player-time-lbl");
  if (slider) {
    slider.value = 0;
    slider.disabled = true;
  }
  if (timeLbl) timeLbl.textContent = "--:--";

  if (historyChart) {
    historyChart.destroy();
    historyChart = null;
  }
  const wrapper = document.getElementById("history-chart-wrapper");
  if (wrapper) wrapper.classList.add("hidden");

  document.getElementById("hs-dist")?.replaceChildren(document.createTextNode("—"));
  document.getElementById("hs-time")?.replaceChildren(document.createTextNode("—"));
  document.getElementById("hs-max")?.replaceChildren(document.createTextNode("—"));
  document.getElementById("hs-avg")?.replaceChildren(document.createTextNode("—"));

  document.getElementById("track-cursor-info")?.classList.add("hidden");
}

function onMapMouseMove(e) {
  if (!trackPointsCache.length || !histLayerGroup) return;
  let minDist = Infinity;
  let closest = null;
  const mousePoint = map.latLngToLayerPoint(e.latlng);
  for (const pt of trackPointsCache) {
    const layerPoint = map.latLngToLayerPoint([pt.lat, pt.lon]);
    const dx = layerPoint.x - mousePoint.x;
    const dy = layerPoint.y - mousePoint.y;
    const distSq = dx * dx + dy * dy;
    if (distSq < minDist) {
      minDist = distSq;
      closest = pt;
    }
  }
  if (closest && minDist < 400) {
    showTrackCursor(closest);
  } else {
    hideTrackCursor();
  }
}

function showTrackCursor(pt) {
  const infoBox = document.getElementById("track-cursor-info");
  if (!infoBox) return;
  infoBox.querySelector(".ti-time").textContent = new Date(pt.ts * 1000).toLocaleTimeString();
  infoBox.querySelector(".ti-speed").textContent = `${Math.round(pt.speed || 0)} км/ч`;
  infoBox.classList.remove("hidden");
  if (!trackCursorMarker) {
    trackCursorMarker = L.circleMarker([pt.lat, pt.lon], {
      radius: 6,
      color: "#fff",
      fillColor: "#4caf50",
      fillOpacity: 1,
      weight: 2,
      interactive: false,
    }).addTo(map);
  } else {
    trackCursorMarker.setLatLng([pt.lat, pt.lon]);
    trackCursorMarker.bringToFront();
  }
}

function hideTrackCursor() {
  document.getElementById("track-cursor-info")?.classList.add("hidden");
  if (trackCursorMarker) {
    map.removeLayer(trackCursorMarker);
    trackCursorMarker = null;
  }
}
