// Backend state is authoritative; browser staging is not the transfer scheduler.
function connectSocket(path, onMessage, options = {}) {
  let socket, timer, stopped = false, delay = 500;
  function connect() {
    if (stopped) return;
    socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}${path}`);
    const current = socket;
    socket.onopen = () => { delay = 500; if (options.onStatus) options.onStatus('Connected'); };
    socket.onmessage = event => {
      try { onMessage(JSON.parse(event.data)); }
      catch (error) { if (options.onError) options.onError(error); }
    };
    socket.onerror = () => current.close();
    socket.onclose = () => {
      if (stopped) return;
      if (options.onStatus) options.onStatus('Reconnecting…');
      timer = setTimeout(connect, delay); delay = Math.min(delay * 2, 10000);
    };
  }
  function close() { stopped = true; clearTimeout(timer); if (socket) socket.close(); }
  connect();
  if (typeof window !== 'undefined') window.addEventListener('pagehide', close, {once: true});
  return {close};
}
function connectEventSocket(onMessage) { return connectSocket('/api/logs', onMessage); }
function connectTransferEventSocket(onEvent, options = {}) {
  const query = options.side ? `?side=${encodeURIComponent(options.side)}` : '';
  return connectSocket('/api/transfers/events' + query, onEvent, options);
}
const MAX_LOG_LINES = 300;
function renderLogLine(container, data) {
  const line = document.createElement('div'); line.className = 'log-line';
  line.textContent = `${data.ts || ''} [${data.level || 'INFO'}] ${data.message || ''}`;
  container.appendChild(line);
  while (container.children.length > MAX_LOG_LINES) container.removeChild(container.firstChild);
  container.scrollTop = container.scrollHeight;
}
function escapeHtml(value) {
  const div = document.createElement('div'); div.textContent = String(value ?? ''); return div.innerHTML;
}
async function api(path, options = {}) {
  const response = await fetch(path, {...options, headers: {'Content-Type': 'application/json', ...options.headers}});
  let data;
  try { data = await response.json(); }
  catch (_) { throw new Error(`Request failed (${response.status})`); }
  if (!response.ok && !data.error) data.error = typeof data.detail === 'string' ? data.detail : `Request failed (${response.status})`;
  if (!response.ok) data.ok = false;
  return data;
}
function formatBytesPerSec(bps) {
  if (!bps || bps <= 0) return '';
  if (bps < 1024) return `${bps} B/s`;
  if (bps < 1048576) return `${(bps / 1024).toFixed(1)} KB/s`;
  return `${(bps / 1048576).toFixed(1)} MB/s`;
}
const TRANSFER_STATE_LABELS = {
  staging: 'Staging…', queued: 'Queued', active: 'Active', pausing: 'Pausing…',
  paused: 'Paused', cancelled: 'Cancelled', completed: 'Done', failed: 'Failed',
  retrying: 'Retrying…', connection_lost: 'Connection lost',
};
const TERMINAL_TRANSFER_STATES = new Set(['completed', 'cancelled', 'failed']);

function createTransferQueueView(container, options = {}) {
  const {showControls = true, onPause, onResume, onCancel, onRetry, onDownload,
    autoRemoveDelay = 1500, emptyMessage = null, keyForEvent = event => event.id} = options;
  const rows = new Map(), dismissed = new Map();
  let streamId = null, scope = null, connected = true;
  function showEmpty() {
    if (emptyMessage && rows.size === 0) {
      container.replaceChildren();
      const message = document.createElement('div'); message.className = 'muted';
      message.textContent = emptyMessage; container.appendChild(message);
    }
  }
  function removeRow(id, remember = false) {
    const entry = rows.get(id); if (!entry) return;
    clearTimeout(entry.timer); entry.row.remove(); rows.delete(id);
    if (remember) {
      dismissed.set(id, {revision: entry.data.revision ?? -1, generation: entry.data.generation, state: entry.data.state,
                         retired: new Set(entry.retiredGenerations)});
      if (dismissed.size > 2000) dismissed.delete(dismissed.keys().next().value);
    }
    showEmpty();
  }
  function ensureRow(id) {
    if (rows.has(id)) return rows.get(id);
    if (rows.size === 0) container.replaceChildren();
    const row = document.createElement('div'); row.className = 'transfer-row';
    row.innerHTML = '<div class="transfer-label"><span class="left-group"><span class="dir"></span><span class="name"></span><span class="state muted"></span></span><span class="right-group"><span class="pct muted"></span><span class="speed muted"></span><span class="btns"></span></span></div><div class="transfer-track" role="progressbar" aria-valuemin="0" aria-valuemax="100"><div class="transfer-bar"></div></div><div class="action-error" role="alert"></div>';
    container.appendChild(row);
    const entry = {row, data: {}, pending: false, timer: null, retiredGenerations: new Set()};
    for (const name of ['dir', 'name', 'state', 'pct', 'speed', 'btns', 'transfer-bar', 'transfer-track', 'action-error']) entry[name] = row.querySelector('.' + name);
    rows.set(id, entry); return entry;
  }
  function button(entry, id, label, action, cls = 'secondary') {
    if (!action) return;
    const b = document.createElement('button'); b.textContent = label; b.className = cls; b.disabled = entry.pending;
    b.onclick = async () => {
      if (entry.pending) return;
      entry.pending = true; entry['action-error'].textContent = ''; render(id, entry);
      try {
        const result = await action(entry.data.id);
        if (result && result.ok === false) throw new Error(result.error || 'Action was rejected');
        if (result && result.transfer) applyEvent({...result.transfer, scope: entry.data.scope, side: entry.data.side, peer_id: entry.data.peer_id});
      } catch (error) { entry['action-error'].textContent = error.message || String(error); }
      finally { entry.pending = false; if (rows.get(id) === entry) render(id, entry); }
    };
    entry.btns.appendChild(b);
  }
  function render(id, entry) {
    const data = entry.data, state = data.state || 'queued';
    entry.row.setAttribute('data-state', state);
    entry.name.setAttribute('title', data.filename || '');
    entry.name.textContent = data.filename || ''; entry.dir.textContent = data.direction === 'download' ? '⬇' : '⬆';
    entry.state.textContent = data.resume_requested ? 'Resuming… (waiting for a slot)' : (state === 'failed' && data.error ? `Failed: ${data.error}` : TRANSFER_STATE_LABELS[state] || state);
    const raw = state === 'completed' ? 100 : data.total_size > 0 ? 100 * (data.offset || 0) / data.total_size : 0;
    const percent = Math.max(0, Math.min(100, Number.isFinite(raw) ? raw : 0));
    entry['transfer-bar'].style.width = percent + '%'; entry.pct.textContent = Math.round(percent) + '%';
    entry['transfer-track'].setAttribute('aria-valuenow', String(Math.round(percent)));
    entry['transfer-track'].setAttribute('aria-label', data.filename || 'Transfer');
    entry['transfer-track'].setAttribute('aria-valuetext', `${Math.round(percent)}%, ${TRANSFER_STATE_LABELS[state] || state}`);
    entry.speed.textContent = state === 'active' ? formatBytesPerSec(data.bytes_per_sec) : '';
    entry.state.classList.toggle('is-failed', state === 'failed'); entry.state.classList.toggle('is-done', state === 'completed');
    entry['transfer-bar'].classList.toggle('is-failed', state === 'failed'); entry['transfer-bar'].classList.toggle('is-cancelled', state === 'cancelled');
    const buttonKey = `${state}:${Boolean(data.resume_requested)}:${entry.pending}:${connected}`;
    if (entry.buttonKey === buttonKey) return;
    entry.buttonKey = buttonKey;
    entry.btns.replaceChildren();
    if (showControls) {
      if (connected && (['queued', 'active'].includes(state) ||
          (state === 'paused' && data.resume_requested))) button(entry, id, 'Pause', onPause);
      if (connected && state === 'paused' && !data.resume_requested) button(entry, id, 'Resume', onResume);
      if (connected && state === 'failed') button(entry, id, 'Retry', onRetry);
      if (!TERMINAL_TRANSFER_STATES.has(state)) button(entry, id, 'Cancel', onCancel, 'danger');
      if (state === 'completed' && data.direction === 'download') button(entry, id, 'Save file', onDownload);
      if (TERMINAL_TRANSFER_STATES.has(state)) button(entry, id, 'Dismiss', () => removeRow(id, true));
    }
  }
  function applyEvent(event) {
    if (!event || !event.id) return;
    if (event.stream_id && streamId && event.stream_id !== streamId) return;
    const id = keyForEvent(event);
    const dismissedEntry = dismissed.get(id);
    if (dismissedEntry && dismissedEntry.retired.has(event.generation)) return;
    // Cancellation is terminal. Polling, refresh and late progress must not
    // recreate its activity row; retain the backend journal for protocol safety.
    if (dismissedEntry && dismissedEntry.state === 'cancelled' && event.generation === dismissedEntry.generation) return;
    if (dismissedEntry && event.generation === dismissedEntry.generation &&
        (event.revision ?? -1) <= dismissedEntry.revision) return;
    const entry = ensureRow(id);
    if (!entry.data.generation && dismissedEntry && dismissedEntry.generation &&
        event.generation !== dismissedEntry.generation) entry.retiredGenerations.add(dismissedEntry.generation);
    if (event.generation && entry.data.generation && event.generation !== entry.data.generation) {
      if (entry.retiredGenerations.has(event.generation)) return;
      entry.retiredGenerations.add(entry.data.generation);
      if (entry.retiredGenerations.size > 4) entry.retiredGenerations.delete(entry.retiredGenerations.values().next().value);
      clearTimeout(entry.timer); entry.timer = null; entry.data = {}; entry.buttonKey = null;
    }
    if (event.revision != null && entry.data.revision != null && event.revision <= entry.data.revision) return;
    if (event.type === 'TRANSFER_PROGRESS' && TERMINAL_TRANSFER_STATES.has(entry.data.state)) return;
    if (event.state && entry.data.state !== event.state) entry['action-error'].textContent = '';
    entry.data = {...entry.data, ...event};
    if (entry.data.state === 'cancelled') { removeRow(id, true); return; }
    render(id, entry);
    if (entry.data.state !== 'completed') { clearTimeout(entry.timer); entry.timer = null; }
    else if (autoRemoveDelay != null && entry.timer == null && !(onDownload && entry.data.direction === 'download')) {
      const revision = entry.data.revision;
      entry.timer = setTimeout(() => { if (rows.get(id) === entry && entry.data.state === 'completed' && entry.data.revision === revision) removeRow(id, true); }, autoRemoveDelay);
    }
  }
  function syncSnapshot(snapshot) {
    if ((snapshot.stream_id && streamId && streamId !== snapshot.stream_id) ||
        (scope != null && snapshot.scope !== undefined && scope !== snapshot.scope)) {
      for (const [id, entry] of rows) if (!entry.data.local) removeRow(id);
      dismissed.clear();
    }
    streamId = snapshot.stream_id || streamId;
    if (snapshot.scope !== undefined) scope = snapshot.scope;
    const present = new Set((snapshot.transfers || []).map(keyForEvent));
    for (const [id, entry] of rows) {
      if (!entry.data.local && !present.has(id) &&
          (entry.data.event_id == null || (snapshot.event_id != null && snapshot.event_id >= entry.data.event_id))) removeRow(id);
    }
    for (const data of snapshot.transfers || []) applyEvent(data);
  }
  function renameRow(oldId, newId) {
    const entry = rows.get(oldId); if (!entry) return;
    if (rows.has(newId)) { removeRow(oldId); return; }
    rows.delete(oldId); rows.set(newId, entry); entry.data.id = newId; entry.data.local = false;
  }
  function setConnected(value) { connected = Boolean(value); for (const [id, entry] of rows) render(id, entry); }
  showEmpty();
  return {applyEvent, syncSnapshot, removeRow, renameRow, setConnected, hasRow: id => rows.has(id)};
}
