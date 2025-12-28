// ---------- Configuration ----------
const api = {
  taxi: (id) => `/api/taxi/${encodeURIComponent(id)}`,
  alerts: (order = 'desc', limit = 50) => `/api/alerts?order=${order}&limit=${limit}`
};

// Default IDs to display something immediately (Demo)
const DEFAULT_IDS = ["100", "10002", "10004", "200"]; 
let refreshHandle = null;

// ---------- Utilities ----------
const qs = (s, el = document) => el.querySelector(s);

const kmfmt = (n) => (n == null || Number.isNaN(+n)) ? "—" : (+n).toFixed(3);
const spdfmt = (n) => (n == null || Number.isNaN(+n)) ? "—" : (+n).toFixed(1);
const tsfmt = (t) => {
  if (!t) return "—";
  try {
    if (typeof t === 'string' && t.includes('T')) return new Date(t).toLocaleTimeString();
    const num = +t;
    return new Date(num > 1e12 ? num : num * 1000).toLocaleTimeString();
  } catch { return "—"; }
};

const statusForDist = (d) => {
  const dist = +d || 0;
  if (dist > 15) return { cls: 'disc', label: 'DISCARD (>15km)' };
  if (dist > 10) return { cls: 'warn', label: 'WARN (10-15km)' };
  return { cls: 'ok', label: 'OK (<10km)' };
};

// ---------- MAP LOGIC (Leaflet) ----------
let map = null;
const markers = {}; // Cache: { id: LeafletMarker }
const FC_COORDS = [39.9163, 116.3972]; // Forbidden City

function initMap() {
  if (map) return;

  // 1. Create map
  map = L.map('map-container').setView(FC_COORDS, 13);

  // 2. OpenStreetMap base layer
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '© OpenStreetMap'
  }).addTo(map);

  // 3. Geofence circles (SpeedBolt logic visualization)
  L.circle(FC_COORDS, { color: '#f59e0b', fill: false, weight: 2, dashArray: '5, 5', radius: 10000 }).addTo(map);
  L.circle(FC_COORDS, { color: '#ef4444', fill: false, weight: 2, dashArray: '5, 5', radius: 15000 }).addTo(map);
}

function updateMarker(id, data) {
  if (!map) return;

  // Try to read lat/lon in various possible ways for robustness
  const lat = parseFloat(data.lat || data.latitude);
  const lon = parseFloat(data.lon || data.longitude);
  const speed = parseFloat(data.speed_kmh || data.speed || 0);

  // If coordinates are not valid, do nothing
  if (isNaN(lat) || isNaN(lon)) return;

  const st = statusForDist(data.dist_km_center);

  if (markers[id]) {
    // UPDATE: Move existing marker
    markers[id].setLatLng([lat, lon]);
    markers[id].setPopupContent(`<b>Taxi ${id}</b><br>${st.label}<br>Speed: ${speed.toFixed(1)} km/h`);
  } else {
    // CREATE: New marker
    const m = L.marker([lat, lon]).addTo(map);
    m.bindPopup(`<b>Taxi ${id}</b><br>${st.label}<br>Speed: ${speed.toFixed(1)} km/h`);
    markers[id] = m;
  }
}

// ---------- API & UI ----------
async function fetchTaxi(id) {
  const res = await fetch(api.taxi(id));
  if (!res.ok) throw new Error(`Taxi ${id} missing`);
  return res.json();
}

function taxiCardDOM(id, data) {
  const dist = parseFloat(data.dist_km_center);
  const speed = parseFloat(data.speed_kmh);
  const avg = parseFloat(data.avg_speed_kmh);
  const st = statusForDist(dist);

  const card = document.createElement('div');
  card.className = `card ${st.cls}`;
  card.id = `taxi-${id}`;
  
  card.innerHTML = `
    <div class="title">
      <span class="dot ${st.cls}"></span>
      <span>taxi:${id}</span>
      <span class="badge time">${tsfmt(data.ts)}</span>
    </div>
    <div class="metrics">
      <div class="kv"><div class="k">Dist. Center</div><div class="v">${kmfmt(dist)} km</div></div>
      <div class="kv"><div class="k">Speed</div><div class="v">${spdfmt(speed)} km/h</div></div>
      <div class="kv"><div class="k">Avg (Window)</div><div class="v">${spdfmt(avg)} km/h</div></div>
      <div class="kv"><div class="k">Total Dist</div><div class="v">${kmfmt(data.distance_km_total)} km</div></div>
    </div>
  `;
  return card;
}

async function refreshAll() {
  const ids = getIds();
  
  // 1. Taxis
  const taxiJobs = ids.map(async (id) => {
    try {
      const data = await fetchTaxi(id);
      
      // Update DOM
      const wrap = qs('#taxis');
      if (wrap) {
        const old = qs(`#taxi-${id}`, wrap);
        const fresh = taxiCardDOM(id, data);
        old ? wrap.replaceChild(fresh, old) : wrap.appendChild(fresh);
      }

      // Update Map
      updateMarker(id, data);

    } catch (e) {
      // If it fails, clean up map marker if it exists
      if (markers[id]) {
        map.removeLayer(markers[id]);
        delete markers[id];
      }
    }
  });

  // 2. Alertas
  const alertJob = loadAlerts();

  await Promise.all([...taxiJobs, alertJob]);
}

// ---------- Alerts ----------
async function loadAlerts() {
  try {
    const res = await fetch(api.alerts('desc', 20));
    const data = await res.json();
    renderAlerts(data.items || []);
  } catch (e) { console.error(e); }
}

function renderAlerts(items) {
  const list = qs('#alerts-list');
  if (!list) return;
  list.innerHTML = '';
  
  items.forEach(a => {
    const li = document.createElement('li');
    li.className = 'alert-item';
    const typeClass = a.type === 'speeding' ? 'tag-red' : 'tag-amber';
    li.innerHTML = `
      <div class="alert-time">${tsfmt(a.timestamp)}</div>
      <div class="alert-content">
        <strong>taxi:${a.taxi_id}</strong>
        <span class="tag ${typeClass}">${a.type}</span>
        <span class="alert-val">${a.value ? parseFloat(a.value).toFixed(2) : ''}</span>
      </div>
    `;
    list.appendChild(li);
  });
}

// ---------- Notifications ----------
function showNotification(msg) {
  const container = document.getElementById('toast-container');
  if (!container) return;

  const el = document.createElement('div');
  el.className = 'toast';
  
  // Simple parsing to determine type for styling
  if (msg.includes('speeding')) el.classList.add('speeding');
  if (msg.includes('left_area')) el.classList.add('left_area');

  el.innerHTML = `<strong>New Alert!</strong><br>${msg}`;
  container.appendChild(el);

  // Auto remove
  setTimeout(() => {
    el.style.opacity = '0';
    setTimeout(() => el.remove(), 300);
  }, 5000);
}

let ws = null;

function connectWs() {
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/api/ws/alerts`;
  console.log("Connecting to WS:", wsUrl);
  
  ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    console.log("WS Connected");
    const wsStatus = qs('#ws-status');
    if (wsStatus) wsStatus.textContent = "WS: Connected 🟢";
  };

  ws.onmessage = (event) => {
    console.log("WS Message:", event.data);
    showNotification(event.data);
    loadAlerts(); 
  };

  ws.onerror = (err) => {
    console.error("WS Error:", err);
    const wsStatus = qs('#ws-status');
    if (wsStatus) wsStatus.textContent = "WS: Error 🔴";
  };

  ws.onclose = () => {
    console.log("WS Closed, retrying...");
    const wsStatus = qs('#ws-status');
    if (wsStatus) wsStatus.textContent = "WS: Reconnecting... 🟠";
    ws = null;
    setTimeout(connectWs, 5000);
  };
}

// ---------- Main Loop ----------
function getIds() {
  const input = qs('#ids'); // Hidden or visible input depending on design
  // If there is no input or it is empty, use the defaults
  if (input && input.value.trim()) {
      return input.value.split(',').map(s => s.trim()).filter(Boolean);
  }
  return DEFAULT_IDS;
}

async function init() {
  initMap();
  connectWs();
  await refreshAll();

  // Apply button and Loop configuration
  const btn = qs('#btn-apply');
  if(btn) btn.addEventListener('click', () => {
     if (refreshHandle) clearInterval(refreshHandle);
     startLoop();
  });

  startLoop();
}

function startLoop() {
  const inputRate = qs('#refresh-rate');
  const sec = inputRate ? Math.max(0.5, parseFloat(inputRate.value)) : 1;
  refreshHandle = setInterval(refreshAll, sec * 1000);
}

document.addEventListener('DOMContentLoaded', init);