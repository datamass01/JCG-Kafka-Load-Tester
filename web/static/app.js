'use strict';

const PX_PER_POINT = 14;

// ── Auth ────────────────────────────────────────────────
const TOKEN_KEY = 'kafka_agent_token';
let _authToken = null;

async function ensureAuth() {
  try {
    const res = await fetch('/auth-config');
    const cfg = await res.json();
    if (!cfg.auth_required) return;
    _authToken = localStorage.getItem(TOKEN_KEY) || '';
    if (!_authToken) {
      _authToken = window.prompt('Dashboard auth token:') || '';
      if (_authToken) localStorage.setItem(TOKEN_KEY, _authToken);
    }
  } catch (e) {
    // /auth-config unreachable — proceed without auth headers
  }
}

// authFetch wraps fetch() to attach the bearer token when configured and
// handles 401 by clearing the cached token so the next reload re-prompts.
async function authFetch(url, init = {}) {
  if (_authToken) {
    init.headers = Object.assign({}, init.headers || {}, {
      'Authorization': 'Bearer ' + _authToken,
    });
  }
  const res = await fetch(url, init);
  if (res.status === 401) {
    localStorage.removeItem(TOKEN_KEY);
    _authToken = null;
  }
  return res;
}

// ── Producer charts ──────────────────────────────────────
const rateData    = { labels: [], datasets: [{ label: 'msg/sec', data: [], borderColor: '#00d2ff', backgroundColor: 'rgba(0,210,255,.08)', tension: .3, fill: true, pointRadius: 0 }] };
const latencyData = { labels: [], datasets: [{ label: 'p99 ms',  data: [], borderColor: '#fc5a5a', backgroundColor: 'rgba(252,90,90,.08)',  tension: .3, fill: true, pointRadius: 0 }] };

// ── Consumer charts ──────────────────────────────────────
const crateData = { labels: [], datasets: [{ label: 'consumed/sec', data: [], borderColor: '#3adb76', backgroundColor: 'rgba(58,219,118,.08)', tension: .3, fill: true, pointRadius: 0 }] };
const clagData  = { labels: [], datasets: [{ label: 'lag',           data: [], borderColor: '#f5c518', backgroundColor: 'rgba(245,197,24,.08)',  tension: .3, fill: true, pointRadius: 0 }] };

const chartOpts = (yLabel) => ({
  responsive: true, maintainAspectRatio: false, animation: false,
  plugins: { legend: { display: false } },
  scales: {
    x: { ticks: { color: '#6b7280', maxRotation: 0, autoSkip: true, maxTicksLimit: 10 }, grid: { color: 'rgba(255,255,255,.05)' } },
    y: { ticks: { color: '#6b7280' }, grid: { color: 'rgba(255,255,255,.05)' }, title: { display: true, text: yLabel, color: '#6b7280', font: { size: 11 } } }
  }
});

let chartRate, chartLatency, chartCRate, chartCLag;
window.addEventListener('DOMContentLoaded', async () => {
  chartRate    = new Chart(document.getElementById('chart-rate'),    { type: 'line', data: rateData,    options: chartOpts('msg/sec') });
  chartLatency = new Chart(document.getElementById('chart-latency'), { type: 'line', data: latencyData, options: chartOpts('ms') });
  chartCRate   = new Chart(document.getElementById('chart-crate'),   { type: 'line', data: crateData,   options: chartOpts('msg/sec') });
  chartCLag    = new Chart(document.getElementById('chart-clag'),    { type: 'line', data: clagData,    options: chartOpts('messages') });

  // Bind UI controls (CSP forbids inline onclick handlers).
  document.querySelectorAll('.tab-btn[data-tab]').forEach(btn =>
    btn.addEventListener('click', () => switchTab(btn.dataset.tab)));
  document.getElementById('btn-connect-brokers').addEventListener('click', connectBrokers);
  document.getElementById('btn-clear-log').addEventListener('click', clearLog);
  document.getElementById('btn-start').addEventListener('click', startTest);
  document.getElementById('btn-stop').addEventListener('click', stopTest);
  document.getElementById('cbtn-start').addEventListener('click', startConsumer);
  document.getElementById('cbtn-stop').addEventListener('click', stopConsumer);
  document.getElementById('btn-history-refresh').addEventListener('click', loadHistory);
  document.getElementById('btn-consumer-history-refresh').addEventListener('click', loadConsumerHistory);

  await ensureAuth();
  connectWS();
  loadHistory();
  loadConsumerHistory();
});

// ── Tab switching ─────────────────────────────────────────
function switchTab(name) {
  document.getElementById('tab-producer').style.display = name === 'producer' ? 'contents' : 'none';
  document.getElementById('tab-consumer').style.display = name === 'consumer' ? 'contents' : 'none';
  document.getElementById('tab-btn-producer').classList.toggle('active', name === 'producer');
  document.getElementById('tab-btn-consumer').classList.toggle('active', name === 'consumer');
  // Charts initialised while the pane was hidden have 0 size — resize after revealing
  if (name === 'producer') {
    chartRate.resize();
    chartLatency.resize();
  } else {
    chartCRate.resize();
    chartCLag.resize();
  }
}

// ── Charts ────────────────────────────────────────────────
function clearCharts() {
  rateData.labels = [];    rateData.datasets[0].data = [];
  latencyData.labels = []; latencyData.datasets[0].data = [];
  setChartWidth(0);
  chartRate.update();
  chartLatency.update();
}

function clearConsumerCharts() {
  crateData.labels = []; crateData.datasets[0].data = [];
  clagData.labels = [];  clagData.datasets[0].data = [];
  setConsumerChartWidth(0);
  chartCRate.update();
  chartCLag.update();
}

function setChartWidth(n) {
  const rateWrap    = document.getElementById('rate-inner');
  const latencyWrap = document.getElementById('latency-inner');
  const minW = rateWrap.parentElement.clientWidth;
  const w    = Math.max(minW, n * PX_PER_POINT);
  rateWrap.style.width    = w + 'px';
  latencyWrap.style.width = w + 'px';
}

function setConsumerChartWidth(n) {
  const crateWrap = document.getElementById('crate-inner');
  const clagWrap  = document.getElementById('clag-inner');
  const minW = crateWrap.parentElement.clientWidth;
  const w    = Math.max(minW, n * PX_PER_POINT);
  crateWrap.style.width = w + 'px';
  clagWrap.style.width  = w + 'px';
}

// ── Event Log ────────────────────────────────────────────
const LOG_MAX = 500;
let logLines = 0;

function logMsg(text, level = 'info') {
  const el = document.getElementById('event-log');
  const ts  = new Date().toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
  const line = document.createElement('div');
  line.className = 'log-line';
  const tsSpan = document.createElement('span');
  tsSpan.className = 'log-ts';
  tsSpan.textContent = ts;
  const msgSpan = document.createElement('span');
  msgSpan.className = 'log-msg ' + String(level).replace(/[^a-zA-Z0-9_-]/g, '');
  msgSpan.textContent = text;
  line.appendChild(tsSpan);
  line.appendChild(msgSpan);
  el.appendChild(line);
  logLines++;
  if (logLines > LOG_MAX) {
    el.removeChild(el.firstChild);
    logLines--;
  }
  el.scrollTop = el.scrollHeight;
}

function logDivider(label) {
  const bar = '─'.repeat(20);
  logMsg(`${bar} ${label} ${bar}`, 'divider');
}

function clearLog() {
  const el = document.getElementById('event-log');
  el.textContent = '';
  logLines = 0;
  logMsg('Log cleared', 'info');
}

// ── Kafka Instance Selector ──────────────────────────────
let _connectedBroker = null;

async function connectBrokers() {
  const val = document.getElementById('instance-manual').value.trim();
  if (!val) return;
  logMsg(`Connecting to brokers: ${val}`, 'info');
  try {
    const res = await authFetch('/api/kafka/connect', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ brokers: val }),
    });
    if (!res.ok) {
      const msg = await res.text();
      logMsg(`Connect failed: ${msg.trim()}`, 'error');
      return;
    }
    _connectedBroker = val;
    document.getElementById('instance-manual').value = '';
    renderConnectedBroker();
    logMsg(`Connected to ${val}`, 'success');
  } catch (e) {
    logMsg(`Connect error: ${e}`, 'error');
  }
}

function renderConnectedBroker() {
  const el = document.getElementById('connected-broker');
  el.textContent = '';
  if (!_connectedBroker) return;
  const chip = document.createElement('span');
  chip.className = 'connected-chip';
  const dot = document.createElement('span');
  dot.className = 'dot connected';
  chip.appendChild(dot);
  chip.appendChild(document.createTextNode(' ' + _connectedBroker + ' '));
  const btn = document.createElement('button');
  btn.className = 'chip-disconnect';
  btn.title = 'Disconnect';
  btn.textContent = '✕';
  btn.addEventListener('click', disconnectBroker);
  chip.appendChild(btn);
  el.appendChild(chip);
}

async function disconnectBroker() {
  logMsg(`Disconnecting from: ${_connectedBroker}`, 'info');
  try {
    const res = await authFetch('/api/kafka/disconnect', { method: 'POST' });
    if (!res.ok) {
      const msg = await res.text();
      logMsg(`Disconnect failed: ${msg.trim()}`, 'error');
      return;
    }
    logMsg(`Disconnected from ${_connectedBroker}`, 'warn');
    _connectedBroker = null;
    renderConnectedBroker();
  } catch (e) {
    logMsg(`Disconnect error: ${e}`, 'error');
  }
}

// ── WebSocket ────────────────────────────────────────────
let ws, wsRetry = 1000;

function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const qs = _authToken ? ('?token=' + encodeURIComponent(_authToken)) : '';
  ws = new WebSocket(`${proto}://${location.host}/ws/metrics${qs}`);

  ws.onopen = () => {
    setDot('connected', 'Connected');
    logMsg('WebSocket connected to server', 'success');
    wsRetry = 1000;
  };
  ws.onclose = () => {
    setDot('error', 'Disconnected – retrying…');
    logMsg(`WebSocket disconnected — retrying in ${Math.round(wsRetry / 1000)}s`, 'warn');
    setTimeout(connectWS, wsRetry);
    wsRetry = Math.min(wsRetry * 1.5, 15000);
  };
  ws.onerror = () => ws.close();

  ws.onmessage = (ev) => {
    const d = JSON.parse(ev.data);
    if (d.type === 'update') handleUpdate(d);
    else if (d.type === 'log') logMsg(`[server] ${d.message}`, d.level || 'info');
    else if (d.type === 'run_completed') loadHistory();
    else if (d.type === 'consumer_run_completed') loadConsumerHistory();
  };
}

function setDot(cls, label) {
  const dot = document.getElementById('ws-dot');
  dot.className = `dot ${cls}`;
  document.getElementById('ws-label').textContent = label;
}

// ── Update handler ───────────────────────────────────────
let _chartRunning = false;
let _consumerChartRunning = false;

function handleUpdate(d) {
  const isRunning         = d.status === 'running';
  const isConsumerRunning = d.consumer_status === 'running';

  if (isRunning && !_chartRunning)                 clearCharts();
  if (isConsumerRunning && !_consumerChartRunning) clearConsumerCharts();

  updateStatus(d.status, d.elapsed);
  updateMetrics(d.metrics);
  updateKafka(d.kafka);
  updateConsumerStatus(d.consumer_status, d.consumer_elapsed);
  updateConsumerMetrics(d.consumer_metrics);

  if (isRunning) {
    pushChart(rateData,    d.metrics?.msg_per_sec    ?? 0);
    pushChart(latencyData, d.metrics?.latency_p99_ms ?? 0);
    setChartWidth(rateData.labels.length);
    document.querySelector('#rate-inner').parentElement.scrollLeft    = 999999;
    document.querySelector('#latency-inner').parentElement.scrollLeft = 999999;
    chartRate.update();
    chartLatency.update();
  }

  if (isConsumerRunning) {
    pushChart(crateData, d.consumer_metrics?.msg_per_sec   ?? 0);
    pushChart(clagData,  d.consumer_metrics?.lag_messages  ?? 0);
    setConsumerChartWidth(crateData.labels.length);
    document.querySelector('#crate-inner').parentElement.scrollLeft = 999999;
    document.querySelector('#clag-inner').parentElement.scrollLeft  = 999999;
    chartCRate.update();
    chartCLag.update();
  }

  _chartRunning         = isRunning;
  _consumerChartRunning = isConsumerRunning;
}

function pushChart(dataset, value) {
  const now = new Date().toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
  dataset.labels.push(now);
  dataset.datasets[0].data.push(Math.round(value * 10) / 10);
}

// ── Producer status ──────────────────────────────────────
let _lastStatus = null;
let _lastElapsed = 0;

function updateStatus(status, elapsed) {
  const badge = document.getElementById('test-status');
  badge.textContent = status.charAt(0).toUpperCase() + status.slice(1);
  badge.className = `status-badge ${status}`;

  const running = status === 'running';
  document.getElementById('btn-start').disabled = running;
  document.getElementById('btn-stop').disabled  = !running;
  document.getElementById('elapsed-label').textContent = running ? `Elapsed: ${fmtDur(elapsed)}` : '';

  if (running && elapsed > 0) _lastElapsed = elapsed;

  if (status !== _lastStatus) {
    if (status === 'running') {
      _lastElapsed = 0;
      logDivider('LOAD TEST STARTED');
      const t = document.getElementById('cfg-topic').value;
      const w = document.getElementById('cfg-workers').value;
      const r = document.getElementById('cfg-rate').value;
      const s = document.getElementById('cfg-size').value;
      const d = document.getElementById('cfg-duration').value;
      logMsg(`Topic: ${t}  |  Workers: ${w}  |  Target: ${r} msg/s  |  Size: ${s}B  |  Duration: ${d == 0 ? '∞' : d + 's'}`, 'info');
    } else if (_lastStatus === 'running') {
      logDivider('LOAD TEST STOPPED');
      logMsg(`Elapsed: ${fmtDur(_lastElapsed)}`, 'info');
    }
    _lastStatus = status;
  }
}

// ── Consumer status ──────────────────────────────────────
let _lastConsumerStatus = null;
let _lastConsumerElapsed = 0;

function updateConsumerStatus(status, elapsed) {
  if (!status) return;
  const badge = document.getElementById('consumer-status');
  badge.textContent = status.charAt(0).toUpperCase() + status.slice(1);
  badge.className = `status-badge ${status}`;

  const running = status === 'running';
  document.getElementById('cbtn-start').disabled = running;
  document.getElementById('cbtn-stop').disabled  = !running;
  document.getElementById('consumer-elapsed-label').textContent = running ? `Elapsed: ${fmtDur(elapsed)}` : '';

  if (running && elapsed > 0) _lastConsumerElapsed = elapsed;

  if (status !== _lastConsumerStatus) {
    if (status === 'running') {
      _lastConsumerElapsed = 0;
      logDivider('CONSUMER STARTED');
      const t = document.getElementById('ccfg-topic').value;
      const g = document.getElementById('ccfg-group').value;
      const o = document.getElementById('ccfg-offset').value;
      const d = document.getElementById('ccfg-duration').value;
      logMsg(`Topic: ${t}  |  Group: ${g}  |  Offset: ${o}  |  Duration: ${d == 0 ? '∞' : d + 's'}`, 'info');
    } else if (_lastConsumerStatus === 'running') {
      logDivider('CONSUMER STOPPED');
      logMsg(`Elapsed: ${fmtDur(_lastConsumerElapsed)}`, 'info');
    }
    _lastConsumerStatus = status;
  }
}

function fmtDur(s) {
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = Math.floor(s % 60);
  return [h, m, sec].map(n => String(n).padStart(2, '0')).join(':');
}

// ── Producer metrics ─────────────────────────────────────
let _lastLoggedError = null;

function updateMetrics(m) {
  if (!m) return;
  setText('s-total',  fmtNum(m.total_messages_sent));
  setText('s-rate',   fmtNum(Math.round(m.msg_per_sec)));
  setText('s-mb',     m.mb_per_sec.toFixed(2));
  setText('s-errors', fmtNum(m.total_errors));
  setText('s-errpct', m.error_rate_pct.toFixed(2) + '%');
  setText('s-p50',    m.latency_p50_ms.toFixed(1));
  setText('s-p95',    m.latency_p95_ms.toFixed(1));
  setText('s-p99',    m.latency_p99_ms.toFixed(1));

  const errBox = document.getElementById('last-error');
  if (m.last_error) {
    errBox.style.display = 'block';
    errBox.textContent = 'Last error: ' + m.last_error;
    if (m.last_error !== _lastLoggedError) {
      logMsg(`ERROR: ${m.last_error}`, 'error');
      _lastLoggedError = m.last_error;
    }
  } else {
    errBox.style.display = 'none';
  }
}

// ── Consumer metrics ─────────────────────────────────────
let _lastLoggedConsumerError = null;

function updateConsumerMetrics(m) {
  if (!m) return;
  setText('cs-total',  fmtNum(m.total_messages_consumed));
  setText('cs-rate',   fmtNum(Math.round(m.msg_per_sec)));
  setText('cs-mb',     m.mb_per_sec.toFixed(2));
  setText('cs-errors', fmtNum(m.total_errors));
  setText('cs-errpct', m.error_rate_pct.toFixed(2) + '%');
  setText('cs-lag',    fmtNum(m.lag_messages));
  setText('cs-p50',    m.latency_p50_ms.toFixed(1));
  setText('cs-p95',    m.latency_p95_ms.toFixed(1));
  setText('cs-p99',    m.latency_p99_ms.toFixed(1));

  const errBox = document.getElementById('consumer-last-error');
  if (m.last_error) {
    errBox.style.display = 'block';
    errBox.textContent = 'Last error: ' + m.last_error;
    if (m.last_error !== _lastLoggedConsumerError) {
      logMsg(`CONSUMER ERROR: ${m.last_error}`, 'error');
      _lastLoggedConsumerError = m.last_error;
    }
  } else {
    errBox.style.display = 'none';
  }
}

// ── Kafka health ─────────────────────────────────────────
let _brokerState = {};

function updateKafka(kafka) {
  if (!kafka) return;
  const brokerEl = document.getElementById('broker-list');
  brokerEl.textContent = '';
  if (kafka.brokers && kafka.brokers.length) {
    kafka.brokers.forEach(b => {
      const chip = document.createElement('span');
      chip.className = 'broker-chip ' + (b.connected ? 'up' : 'down');
      chip.appendChild(document.createTextNode('node-' + b.id));
      if (b.is_controller) {
        chip.appendChild(document.createTextNode(' '));
        const badge = document.createElement('span');
        badge.className = 'leader-badge';
        badge.textContent = 'leader';
        chip.appendChild(badge);
      }
      chip.appendChild(document.createTextNode(' · ' + b.host + ' ' + (b.connected ? '✓' : '✗')));
      brokerEl.appendChild(chip);

      const prev = _brokerState[b.host];
      if (prev === undefined && b.connected) {
        logMsg(`Broker connected: ${b.host}`, 'success');
      } else if (prev === true && !b.connected) {
        logMsg(`Broker unreachable: ${b.host}`, 'error');
      } else if (prev === false && b.connected) {
        logMsg(`Broker recovered: ${b.host}`, 'success');
      }
      _brokerState[b.host] = b.connected;
    });
  } else {
    const empty = document.createElement('span');
    empty.style.color = 'var(--muted)';
    empty.textContent = 'No brokers found';
    brokerEl.appendChild(empty);
  }

  const tbody = document.getElementById('topic-body');
  tbody.textContent = '';
  if (kafka.topics && kafka.topics.length) {
    kafka.topics.forEach(t => {
      const totalOffset = Object.values(t.offsets_by_partition || {}).reduce((a, b) => a + b, 0);
      const tr = document.createElement('tr');
      tr.appendChild(td(t.name));
      tr.appendChild(td(t.partitions));
      tr.appendChild(td(t.replication_factor));
      tr.appendChild(td(fmtNum(totalOffset)));
      tbody.appendChild(tr);
    });
  } else {
    const tr = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 4;
    cell.style.color = 'var(--muted)';
    cell.textContent = 'No user topics';
    tr.appendChild(cell);
    tbody.appendChild(tr);
  }

  if (kafka.collection_error) {
    const errSpan = document.createElement('span');
    errSpan.style.color = 'var(--red)';
    errSpan.style.fontSize = '.75rem';
    errSpan.style.marginLeft = '.5rem';
    errSpan.textContent = kafka.collection_error;
    brokerEl.appendChild(errSpan);
    if (kafka.collection_error !== _lastKafkaError) {
      logMsg(`Kafka collection error: ${kafka.collection_error}`, 'error');
      _lastKafkaError = kafka.collection_error;
    }
  } else {
    _lastKafkaError = null;
  }
}

function td(text) {
  const cell = document.createElement('td');
  cell.textContent = text;
  return cell;
}
let _lastKafkaError = null;

// ── Producer load test controls ──────────────────────────
async function applyConfig() {
  const cfg = {
    topic:              document.getElementById('cfg-topic').value,
    partitions:         3,
    replication_factor: 1,
    workers:            parseInt(document.getElementById('cfg-workers').value, 10),
    target_msg_per_sec: parseInt(document.getElementById('cfg-rate').value, 10),
    message_size_bytes: parseInt(document.getElementById('cfg-size').value, 10),
    duration_seconds:   parseInt(document.getElementById('cfg-duration').value, 10),
    key_strategy:       document.getElementById('cfg-key').value,
    value_strategy:     'random',
  };
  await authFetch('/api/config', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg) });
}

async function startTest() {
  logMsg('Applying config and sending start request…', 'info');
  await applyConfig();
  const res = await authFetch('/api/load/start', { method: 'POST' });
  if (!res.ok) {
    const msg = await res.text();
    logMsg(`Start failed: ${msg.trim()}`, 'error');
    alert(msg);
  }
}

async function stopTest() {
  logMsg('Stop requested by user', 'warn');
  await authFetch('/api/load/stop', { method: 'POST' });
}

// ── Consumer test controls ───────────────────────────────
async function applyConsumerConfig() {
  const cfg = {
    topic:            document.getElementById('ccfg-topic').value,
    consumer_group:   document.getElementById('ccfg-group').value,
    duration_seconds: parseInt(document.getElementById('ccfg-duration').value, 10),
    offset_reset:     document.getElementById('ccfg-offset').value,
  };
  await authFetch('/api/consumer/config', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg) });
}

async function startConsumer() {
  logMsg('Applying consumer config and sending start request…', 'info');
  await applyConsumerConfig();
  const res = await authFetch('/api/load/consumer/start', { method: 'POST' });
  if (!res.ok) {
    const msg = await res.text();
    logMsg(`Consumer start failed: ${msg.trim()}`, 'error');
    alert(msg);
  }
}

async function stopConsumer() {
  logMsg('Consumer stop requested by user', 'warn');
  await authFetch('/api/load/consumer/stop', { method: 'POST' });
}

// ── History ──────────────────────────────────────────────
function placeholderRow(tbody, text, color) {
  tbody.textContent = '';
  const tr = document.createElement('tr');
  const cell = document.createElement('td');
  cell.colSpan = 10;
  cell.style.textAlign = 'center';
  if (color) cell.style.color = color;
  cell.textContent = text;
  tr.appendChild(cell);
  tbody.appendChild(tr);
}

function appendRow(tbody, cells) {
  const tr = document.createElement('tr');
  cells.forEach(v => tr.appendChild(td(v)));
  tbody.appendChild(tr);
}

async function loadHistory() {
  const tbody = document.getElementById('history-body');
  placeholderRow(tbody, 'Loading…', 'var(--muted)');
  try {
    const res = await authFetch('/api/metrics/history');
    const runs = await res.json();
    if (!runs || !runs.length) {
      placeholderRow(tbody, 'No runs yet', 'var(--muted)');
      return;
    }
    tbody.textContent = '';
    runs.forEach(r => {
      const dur = (new Date(r.stopped_at) - new Date(r.started_at)) / 1000;
      appendRow(tbody, [
        new Date(r.started_at).toLocaleString(),
        fmtDur(dur),
        r.topic,
        r.workers,
        fmtNum(r.target_msg_per_sec),
        fmtNum(r.total_messages_sent),
        fmtNum(r.total_errors),
        r.avg_msg_per_sec.toFixed(0),
        r.avg_mb_per_sec.toFixed(2),
        r.avg_latency_p99_ms.toFixed(1),
      ]);
    });
  } catch (e) {
    placeholderRow(tbody, String(e), 'var(--red)');
  }
}

async function loadConsumerHistory() {
  const tbody = document.getElementById('consumer-history-body');
  placeholderRow(tbody, 'Loading…', 'var(--muted)');
  try {
    const res = await authFetch('/api/metrics/consumer/history');
    const runs = await res.json();
    if (!runs || !runs.length) {
      placeholderRow(tbody, 'No runs yet', 'var(--muted)');
      return;
    }
    tbody.textContent = '';
    runs.forEach(r => {
      const dur = (new Date(r.stopped_at) - new Date(r.started_at)) / 1000;
      appendRow(tbody, [
        new Date(r.started_at).toLocaleString(),
        fmtDur(dur),
        r.topic,
        r.consumer_group,
        r.offset_reset,
        fmtNum(r.total_messages_consumed),
        fmtNum(r.total_errors),
        r.avg_msg_per_sec.toFixed(0),
        r.avg_mb_per_sec.toFixed(2),
        r.avg_latency_p99_ms.toFixed(1),
      ]);
    });
  } catch (e) {
    placeholderRow(tbody, String(e), 'var(--red)');
  }
}

// ── Helpers ──────────────────────────────────────────────
function setText(id, val) { document.getElementById(id).textContent = val; }
function fmtNum(n) { return Number(n).toLocaleString(); }
