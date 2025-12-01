// ---------- Config ----------
const api = {
  taxi: (id) => `/api/taxi/${encodeURIComponent(id)}`,
  alerts: (order = 'desc', limit = 200) => `/api/alerts?order=${order}&limit=${limit}`
};

let refreshHandle = null;

// ---------- Utilities ----------
const qs = (s, el=document) => el.querySelector(s);
const qsa = (s, el=document) => [...el.querySelectorAll(s)];

const kmfmt = (n) => (n == null || Number.isNaN(+n)) ? "—" : (+n).toFixed(3);
const spdfmt = (n) => (n == null || Number.isNaN(+n)) ? "—" : (+n).toFixed(1);
const tsfmt = (t) => {
  if (!t) return "—";
  // /taxi returns ISO at _ts_iso_utc if you added it; fall back to epoch seconds/ms
  try {
    if (typeof t === 'string' && t.includes('T')) return new Date(t).toLocaleString();
    const num = +t;
    return new Date(num > 1e12 ? num : num * 1000).toLocaleString();
  } catch { return "—"; }
};

const statusForDist = (d) => {
  const dist = +d || 0;
  if (dist > 15) return { cls: 'disc', label: 'DISCARD' };
  if (dist > 10) return { cls: 'warn', label: 'WARN' };
  return { cls: 'ok', label: 'OK' };
};

// ---------- Taxi Cards ----------
async function fetchTaxi(id) {
  const res = await fetch(api.taxi(id));
  if (!res.ok) throw new Error(`Taxi ${id} not found`);
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
      <span class="badge">${data.lat ?? "?"}, ${data.lon ?? "?"}</span>
      <span class="badge">${data._ts_iso_utc ? new Date(data._ts_iso_utc).toLocaleTimeString() : tsfmt(data.ts)}</span>
    </div>

    <div class="metrics">
      <div class="kv"><div class="k">Distance to center</div><div class="v">${kmfmt(dist)} km</div></div>
      <div class="kv"><div class="k">Status</div><div class="v">${st.label}</div></div>
      <div class="kv"><div class="k">Speed</div><div class="v">${spdfmt(speed)} km/h</div></div>
      <div class="kv"><div class="k">Avg speed</div><div class="v">${spdfmt(avg)} km/h</div></div>
      <div class="kv"><div class="k">Meters since last</div><div class="v">${spdfmt(data.meters_since_last)} m</div></div>
      <div class="kv"><div class="k">Total distance</div><div class="v">${kmfmt(data.distance_km_total)} km</div></div>
    </div>

    <div class="badges">
      <span class="badge">outside_15km: ${String(data.outside_15km)}</span>
      <span class="badge">ts: ${data.ts ?? "—"}</span>
    </div>
  `;
  return card;
}

async function renderTaxis(ids) {
  const wrap = qs('#taxis');
  wrap.innerHTML = '';
  const jobs = ids.map(async (id) => {
    try {
      const data = await fetchTaxi(id);
      wrap.appendChild(taxiCardDOM(id, data));
    } catch (e) {
      const err = document.createElement('div');
      err.className = 'card';
      err.innerHTML = `<div class="title"><span class="dot disc"></span><span>taxi:${id}</span></div>
        <div class="sub">Not found or error.</div>`;
      wrap.appendChild(err);
    }
  });
  await Promise.allSettled(jobs);
}

async function refreshTaxis(ids) {
  const wrap = qs('#taxis');
  const jobs = ids.map(async (id) => {
    try {
      const data = await fetchTaxi(id);
      const old = qs(`#taxi-${id}`, wrap);
      const fresh = taxiCardDOM(id, data);
      old ? wrap.replaceChild(fresh, old) : wrap.appendChild(fresh);
    } catch {/* ignore individual errors */}
  });
  await Promise.allSettled(jobs);
}

// ---------- Alerts ----------
async function loadAlerts() {
  const res = await fetch(api.alerts('desc', 200));
  const { items } = await res.json();
  renderAlerts(items || []);
}

function renderAlerts(items) {
  const body = qs('#alertsBody');
  body.innerHTML = '';
  for (const a of items) {
    const tr = document.createElement('tr');
    const cls = a.type === 'speeding' ? 'type-speeding' :
                a.type === 'left_area_10km' ? 'type-left' : '';
    tr.innerHTML = `
      <td>${a.time ?? tsfmt(a.timestamp)}</td>
      <td>taxi:${a.taxi_id ?? ''}</td>
      <td><span class="type-badge ${cls}">${a.type}</span></td>
      <td>${a.value ?? ''}</td>
      <td class="hide-sm"><code>${a.raw ?? ''}</code></td>
    `;
    body.appendChild(tr);
  }
}

// ---------- Controls & Loop ----------
function getIds() {
  return qs('#ids').value.split(',').map(s => s.trim()).filter(Boolean);
}

function setStatus(msg) {
  qs('#status').textContent = msg;
}

async function init() {
  const ids = getIds();
  await renderTaxis(ids);
  await loadAlerts();

  // start loop
  const sec = Math.max(1, parseInt(qs('#interval').value || '1', 10));
  if (refreshHandle) clearInterval(refreshHandle);
  refreshHandle = setInterval(async () => {
    const idsNow = getIds();
    await Promise.all([refreshTaxis(idsNow), loadAlerts()]);
    setStatus(`Last update: ${new Date().toLocaleTimeString()}`);
  }, sec * 1000);
}

qs('#apply').addEventListener('click', init);

// initial run
init().catch(err => setStatus(`Error: ${err.message}`));
