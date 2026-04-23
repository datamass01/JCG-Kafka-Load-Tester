'use strict';

const PX_PER_POINT = 14;

const rateData    = { labels: [], datasets: [{ label: 'msg/sec', data: [], borderColor: '#00d2ff', backgroundColor: 'rgba(0,210,255,.08)', tension: .3, fill: true, pointRadius: 0 }] };
const latencyData = { labels: [], datasets: [{ label: 'p99 ms',  data: [], borderColor: '#fc5a5a', backgroundColor: 'rgba(252,90,90,.08)',  tension: .3, fill: true, pointRadius: 0 }] };

const chartOpts = (yLabel) => ({
  responsive: true, maintainAspectRatio: false, animation: false,
  plugins: { legend: { display: false } },
  scales: {
    x: { ticks: { color: '#6b7280', maxRotation: 0, autoSkip: true, maxTicksLimit: 10 }, grid: { color: 'rgba(255,255,255,.05)' } },
    y: { ticks: { color: '#6b7280' }, grid: { color: 'rgba(255,255,255,.05)' }, title: { display: true, text: yLabel, color: '#6b7280', font: { size: 11 } } }
  }
});

let chartRate, chartLatency;
window.addEventListener('DOMContentLoaded', () => {
  chartRate    = new Chart(document.getElementById('chart-rate'),    { type: 'line', data: rateData,    options: chartOpts('msg/sec') });
  chartLatency = new Chart(document.getElementById('chart-latency'), { type: 'line', data: latencyData, options: chartOpts('ms') });
  connectWS();
  loadHistory();
});

function clearCharts() {
  rateData.labels = [];    rateData.datasets[0].data = [];
  latencyData.labels = []; latencyData.datasets[0].data = [];
  setChartWidth(0);
  chartRate.update();
  chartLatency.update();
}

function setChartWidth(n) {
  const rateWrap    = document.getElementById('rate-inner');
  const latencyWrap = document.getElementById('latency-inner');
  const minW = rateWrap.parentElement.clientWidth;
  const w    = Math.max(minW, n * PX_PER_POINT);
  rateWrap.style.width    = w + 'px';
  latencyWrap.style.width = w + 'px';
}

// ── Event Log ────────────────────────────────────────────
const LOG_MAX = 500;
let logLines = 0;

function logMsg(text, level = 'info') {
  const el = document.getElementById('event-log');
  const ts  = new Date().toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
  const line = document.createElement('div');
  line.className = 'log-line';
  line.innerHTML = `<span class="log-ts">${ts}</span><span class="log-msg ${level}">${text}</span>`;
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
  el.innerHTML = '';
  logLines = 0;
  logMsg('Log cleared', 'info');
}

// ── WebSocket ────────────────────────────────────────────
let ws, wsRetry = 1000;

function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws/metrics`);

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
  };
}

function setDot(cls, label) {
  const dot = document.getElementById('ws-dot');
  dot.className = `dot ${cls}`;
  document.getElementById('ws-label').textContent = label;
}

// ── Update handler ───────────────────────────────────────
let _chartRunning = false;

function handleUpdate(d) {
  const isRunning = d.status === 'running';

  // Clear charts at the moment a new run starts
  if (isRunning && !_chartRunning) {
    clearCharts();
  }

  updateStatus(d.status, d.elapsed);
  updateMetrics(d.metrics);
  updateKafka(d.kafka);

  // Only record data points while the test is active
  if (isRunning) {
    pushChart(rateData,    d.metrics?.msg_per_sec ?? 0);
    pushChart(latencyData, d.metrics?.latency_p99_ms ?? 0);
    setChartWidth(rateData.labels.length);
    // Auto-scroll both charts to the right edge
    document.querySelector('#rate-inner').parentElement.scrollLeft = 999999;
    document.querySelector('#latency-inner').parentElement.scrollLeft = 999999;
    chartRate.update();
    chartLatency.update();
  }

  _chartRunning = isRunning;
}

function pushChart(dataset, value) {
  const now = new Date().toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
  dataset.labels.push(now);
  dataset.datasets[0].data.push(Math.round(value * 10) / 10);
}

// ── Status ───────────────────────────────────────────────
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

  // Keep the last known elapsed while running so we can log it on stop
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

function fmtDur(s) {
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = Math.floor(s % 60);
  return [h, m, sec].map(n => String(n).padStart(2, '0')).join(':');
}

// ── Metrics ──────────────────────────────────────────────
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
let _lastLoggedError = null;

// ── Kafka health ─────────────────────────────────────────
let _brokerState = {};

function updateKafka(kafka) {
  if (!kafka) return;
  const brokerEl = document.getElementById('broker-list');
  if (kafka.brokers && kafka.brokers.length) {
    brokerEl.innerHTML = kafka.brokers.map(b =>
      `<span class="broker-chip ${b.connected ? 'up' : 'down'}">${b.host} ${b.connected ? '✓' : '✗'}</span>`
    ).join('');
    kafka.brokers.forEach(b => {
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
    brokerEl.innerHTML = '<span style="color:var(--muted)">No brokers found</span>';
  }

  const tbody = document.getElementById('topic-body');
  if (kafka.topics && kafka.topics.length) {
    tbody.innerHTML = kafka.topics.map(t => {
      const totalOffset = Object.values(t.offsets_by_partition || {}).reduce((a, b) => a + b, 0);
      return `<tr><td>${t.name}</td><td>${t.partitions}</td><td>${t.replication_factor}</td><td>${fmtNum(totalOffset)}</td></tr>`;
    }).join('');
  } else {
    tbody.innerHTML = '<tr><td colspan="4" style="color:var(--muted)">No user topics</td></tr>';
  }

  if (kafka.collection_error) {
    brokerEl.innerHTML += `<span style="color:var(--red);font-size:.75rem;margin-left:.5rem">${kafka.collection_error}</span>`;
    if (kafka.collection_error !== _lastKafkaError) {
      logMsg(`Kafka collection error: ${kafka.collection_error}`, 'error');
      _lastKafkaError = kafka.collection_error;
    }
  } else {
    _lastKafkaError = null;
  }
}
let _lastKafkaError = null;

// ── Load test controls ───────────────────────────────────
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
  await fetch('/api/config', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg) });
}

async function startTest() {
  logMsg('Applying config and sending start request…', 'info');
  await applyConfig();
  const res = await fetch('/api/load/start', { method: 'POST' });
  if (!res.ok) {
    const msg = await res.text();
    logMsg(`Start failed: ${msg.trim()}`, 'error');
    alert(msg);
  }
}

async function stopTest() {
  logMsg('Stop requested by user', 'warn');
  await fetch('/api/load/stop', { method: 'POST' });
}

// ── History ──────────────────────────────────────────────
async function loadHistory() {
  const tbody = document.getElementById('history-body');
  tbody.innerHTML = '<tr><td colspan="10" style="text-align:center;color:var(--muted)">Loading…</td></tr>';
  try {
    const res = await fetch('/api/metrics/history');
    const runs = await res.json();
    if (!runs || !runs.length) {
      tbody.innerHTML = '<tr><td colspan="10" style="text-align:center;color:var(--muted)">No runs yet</td></tr>';
      return;
    }
    tbody.innerHTML = runs.map(r => {
      const dur = (new Date(r.stopped_at) - new Date(r.started_at)) / 1000;
      return `<tr>
        <td>${new Date(r.started_at).toLocaleString()}</td>
        <td>${fmtDur(dur)}</td>
        <td>${r.topic}</td>
        <td>${r.workers}</td>
        <td>${fmtNum(r.target_msg_per_sec)}</td>
        <td>${fmtNum(r.total_messages_sent)}</td>
        <td>${fmtNum(r.total_errors)}</td>
        <td>${r.avg_msg_per_sec.toFixed(0)}</td>
        <td>${r.avg_mb_per_sec.toFixed(2)}</td>
        <td>${r.avg_latency_p99_ms.toFixed(1)}</td>
      </tr>`;
    }).join('');
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="10" style="color:var(--red)">${e}</td></tr>`;
  }
}

// ── Helpers ──────────────────────────────────────────────
function setText(id, val) { document.getElementById(id).textContent = val; }
function fmtNum(n) { return Number(n).toLocaleString(); }
