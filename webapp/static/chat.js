// Direct peer chat. Message text is always rendered with textContent.
(() => {
  const side = document.body.classList.contains('host-page') ? 'host' : 'client';
  const root = document.createElement('div');
  root.className = 'chat-widget';
  root.innerHTML = `
    <button class="chat-launcher" aria-controls="chatPanel" aria-expanded="false" aria-label="Open device chat"><span aria-hidden="true"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M5 3h14a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2H9l-6 4V5a2 2 0 0 1 2-2Z"/><path d="M7 8h10M7 12h6"/></svg></span> Chat <span class="chat-badge" hidden></span></button>
    <aside id="chatPanel" class="chat-panel" aria-label="Device chat" hidden>
      <div class="chat-header">
        <div class="chat-grip" tabindex="0" role="group" aria-label="Move chat panel. Drag or use arrow keys."><strong>Device chat</strong><small>Drag to move · arrow keys work too</small></div>
        <button class="chat-reset" title="Reset chat position" aria-label="Reset chat position">↺</button>
        <button class="chat-close" aria-label="Minimize chat">−</button>
      </div>
      <div class="chat-contact"><label for="chatPeer">Conversation</label><select id="chatPeer" aria-label="Chat recipient"></select><p class="chat-status" role="status">Connect to a device to start chatting.</p></div>
      <div class="chat-messages" role="log" aria-label="Chat messages" aria-live="polite" aria-relevant="additions"></div>
      <div class="chat-empty"><span aria-hidden="true">✦</span><strong>A quick word, while files move.</strong><p>Messages go directly to the other device.<br>Recent messages only · not saved to disk.</p></div>
      <button class="chat-latest secondary" hidden>↓ Latest messages</button>
      <form class="chat-compose">
        <label class="chat-sr-only" for="chatText">Message</label>
        <textarea id="chatText" rows="2" maxlength="2000" placeholder="Write a message…" disabled></textarea>
        <div class="chat-compose-footer"><span class="chat-counter">0 / 2,000</span><button class="chat-send" type="submit" disabled>Send <span aria-hidden="true">↑</span></button></div>
        <div class="chat-error" role="alert"></div>
        <div class="chat-hint">Enter to send · Shift + Enter for a new line</div>
      </form>
    </aside>`;
  document.body.appendChild(root);
  const $ = selector => root.querySelector(selector);
  const panel = $('.chat-panel'), launcher = $('.chat-launcher'), badge = $('.chat-badge');
  const peerPicker = $('#chatPeer'), text = $('#chatText'), send = $('.chat-send');
  const messages = $('.chat-messages'), errorBox = $('.chat-error'), status = $('.chat-status');
  let opened = false, stopped = false, polling = false, sending = false, timer;
  let selected = null, conversation = null, peers = [], revision = -1, renderedChannel = null, selectionVersion = 0;
  let peerSignature = '', dragging = null, position = null;
  const rows = new Map(), drafts = new Map(), controllers = new Set();
  const positionKey = `filetransfer.chat.position.${side}`, readKey = `filetransfer.chat.read.${side}`;
  let read = {};
  try {
    const saved = JSON.parse(localStorage.getItem(readKey) || '{}');
    if (saved && typeof saved === 'object' && !Array.isArray(saved)) read = saved;
    const point = JSON.parse(localStorage.getItem(positionKey) || 'null');
    if (point && Number.isFinite(point.x) && Number.isFinite(point.y)) position = point;
  } catch (_) {}

  async function request(path, options = {}) {
    const controller = new AbortController(); controllers.add(controller);
    const timeout = setTimeout(() => controller.abort(), 12000);
    try {
      const response = await fetch(path, {...options, signal:controller.signal, headers:{'Content-Type':'application/json'}});
      const data = await response.json();
      if (!response.ok || data.ok === false) throw new Error(data.error || 'Chat request failed. Please try again.');
      return data;
    } finally { clearTimeout(timeout); controllers.delete(controller); }
  }
  function draftKey() { return conversation ? `${selected}:${conversation.channel_id}` : ''; }
  function draft() {
    const key = draftKey();
    if (!drafts.has(key)) drafts.set(key, {text:'', id:null});
    // Bound drafts when hosts see many different peers over a long app run.
    if (drafts.size > 100) drafts.delete(drafts.keys().next().value);
    return drafts.get(key);
  }
  function updateControls() {
    text.disabled = !conversation?.available || sending;
    send.disabled = text.disabled || !text.value.trim();
    send.classList.toggle('busy', sending);
    peerPicker.disabled = sending || !peers.length;
    $('.chat-counter').textContent = `${text.value.length.toLocaleString()} / 2,000`;
  }
  function unread(peer) { return Math.max(0, peer.incoming - (Number(read[peer.channel_id]) || 0)); }
  function refreshBadge() {
    const count = peers.reduce((sum, peer) => sum + unread(peer), 0);
    badge.hidden = !count; badge.textContent = count > 99 ? '99+' : String(count);
    launcher.setAttribute('aria-label', `${opened ? 'Minimize' : 'Open'} device chat${count ? `, ${count} unread messages` : ''}`);
    const signature = JSON.stringify(peers.map(peer => [peer.peer_id, peer.label, peer.available, unread(peer)]));
    if (signature !== peerSignature) {
      peerSignature = signature;
      peerPicker.replaceChildren();
      for (const peer of peers) {
        const option = document.createElement('option'); option.value = peer.peer_id;
        option.textContent = `${peer.label}${peer.connected ? '' : ' (offline)'}${unread(peer) ? ` · ${unread(peer)} unread` : ''}`;
        peerPicker.appendChild(option);
      }
      if (!peers.length) {
        const option = document.createElement('option'); option.textContent = 'No device connected'; peerPicker.appendChild(option);
      }
    }
    if (selected) peerPicker.value = selected;
  }
  function atBottom() { return messages.scrollHeight - messages.scrollTop - messages.clientHeight < 35; }
  function markRead() {
    if (!opened || !conversation || document.visibilityState !== 'visible' || !document.hasFocus() || !atBottom()) return;
    read[conversation.channel_id] = conversation.incoming;
    const entries = Object.entries(read); if (entries.length > 100) read = Object.fromEntries(entries.slice(-100));
    try { localStorage.setItem(readKey, JSON.stringify(read)); } catch (_) {}
    refreshBadge();
  }
  function scrollLatest() { messages.scrollTop = messages.scrollHeight; $('.chat-latest').hidden = true; markRead(); }
  function render(list) {
    const bottom = atBottom();
    const present = new Set();
    for (const message of list) {
      const key = `${message.direction}:${message.id}`; present.add(key);
      let row = rows.get(key);
      if (!row) {
        row = document.createElement('article');
        row.className = `chat-message ${message.direction === 'outgoing' ? 'is-mine' : 'is-theirs'}`;
        const body = document.createElement('p'); body.className = 'chat-message-text'; body.textContent = message.text;
        const meta = document.createElement('div'); meta.className = 'chat-message-meta';
        row.append(body, meta); rows.set(key, row); messages.appendChild(row);
      }
      const meta = row.querySelector('.chat-message-meta');
      if (row.dataset.status !== message.status) {
        row.dataset.status = message.status;
        const time = new Date(message.timestamp);
        const clock = Number.isNaN(time.getTime()) ? '' : time.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
        const label = message.direction === 'incoming' ? 'Received' : ({sending:'Sending…', delivered:'Delivered', unconfirmed:'Not confirmed'})[message.status] || message.status;
        meta.textContent = `${clock} · ${label}`;
        if (message.status === 'unconfirmed') {
          const retry = document.createElement('button'); retry.type = 'button'; retry.className = 'chat-retry'; retry.textContent = 'Retry';
          retry.onclick = () => sendMessage(message.text, message.id); meta.appendChild(retry);
        }
      }
    }
    for (const [key, row] of rows) if (!present.has(key)) { row.remove(); rows.delete(key); }
    $('.chat-empty').hidden = rows.size > 0;
    if (bottom) scrollLatest(); else $('.chat-latest').hidden = atBottom();
  }
  async function poll() {
    if (polling || stopped) return;
    polling = true;
    const requestedSelection = selectionVersion;
    try {
      const query = new URLSearchParams({side, revision:String(revision)});
      if (selected) query.set('peer_id', selected);
      if (renderedChannel) query.set('channel_id', renderedChannel);
      const data = await request('/api/chat?' + query);
      if (requestedSelection !== selectionVersion) return;
      peers = data.peers || [];
      const oldKey = draftKey();
      conversation = data.conversation || null;
      if (!conversation && peers.length && selected) {
        selected = null; revision = -1; renderedChannel = null;
      } else selected = conversation?.peer_id || null;
      if (renderedChannel !== (conversation?.channel_id || null)) {
        messages.replaceChildren(); rows.clear(); revision = -1;
        renderedChannel = conversation?.channel_id || null;
        $('.chat-empty').hidden = false;
      }
      if (oldKey !== draftKey()) { text.value = draft().text; errorBox.textContent = ''; }
      refreshBadge();
      if (conversation) {
        if (Array.isArray(data.messages)) render(data.messages);
        revision = conversation.revision;
        status.textContent = conversation.available ? 'Connected · direct device chat' : conversation.connected ? 'This device needs the chat-enabled update.' : 'Disconnected · reconnect to send messages';
      } else status.textContent = side === 'host' ? 'Approve a client connection to start chatting.' : 'Connect to a host to start chatting.';
      markRead(); updateControls();
    } catch (_) {
      status.textContent = 'Chat unavailable · retrying connection…';
      if (conversation) conversation.available = false;
      updateControls();
    } finally {
      polling = false;
      if (!stopped) { clearTimeout(timer); timer = setTimeout(poll, document.hidden ? 5000 : 1000); }
    }
  }
  async function sendMessage(body, messageId) {
    if (sending || !conversation?.available || !body.trim()) return;
    const target = selected, key = draftKey(), activeDraft = draft();
    if (!messageId) {
      if (!activeDraft.id) {
        const bytes = crypto.getRandomValues(new Uint8Array(16));
        activeDraft.id = Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('');
      }
      messageId = activeDraft.id;
    }
    sending = true; errorBox.textContent = ''; updateControls();
    try {
      await request('/api/chat/messages', {method:'POST', body:JSON.stringify({side, peer_id:target, text:body, message_id:messageId})});
      if (activeDraft.text.trim() === body.trim()) {
        activeDraft.text = ''; activeDraft.id = null;
        if (draftKey() === key) text.value = '';
      }
      revision = -1;
    } catch (error) {
      errorBox.textContent = error.name === 'AbortError' ? 'Delivery not confirmed. Retry to check the same message.' : error.message;
      revision = -1;
    } finally {
      sending = false; updateControls();
      if (opened && !text.disabled) text.focus();
      clearTimeout(timer); poll();
    }
  }
  text.addEventListener('input', () => { const value = draft(); value.text = text.value; value.id = null; updateControls(); });
  $('.chat-compose').onsubmit = event => { event.preventDefault(); sendMessage(text.value); };
  text.addEventListener('keydown', event => {
    if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) { event.preventDefault(); sendMessage(text.value); }
  });
  peerPicker.onchange = () => {
    selectionVersion++;
    selected = peerPicker.value; conversation = null; revision = -1; renderedChannel = null;
    messages.replaceChildren(); rows.clear(); text.value = ''; updateControls();
    clearTimeout(timer); poll();
  };
  messages.addEventListener('scroll', () => { $('.chat-latest').hidden = atBottom(); markRead(); });
  $('.chat-latest').onclick = scrollLatest;

  function setPosition(point, save = false) {
    position = {x:Math.max(8, Math.min(innerWidth - panel.offsetWidth - 8, point.x)),
                y:Math.max(8, Math.min(innerHeight - panel.offsetHeight - 8, point.y))};
    panel.style.left = position.x + 'px'; panel.style.top = position.y + 'px';
    panel.style.right = 'auto'; panel.style.bottom = 'auto';
    if (save) try { localStorage.setItem(positionKey, JSON.stringify(position)); } catch (_) {}
  }
  function toggle(open) {
    opened = open; panel.hidden = !open; launcher.setAttribute('aria-expanded', String(open));
    if (open) {
      if (position) setPosition(position);
      scrollLatest(); clearTimeout(timer); poll();
      if (!text.disabled) text.focus(); else $('.chat-grip').focus();
    } else launcher.focus();
    refreshBadge();
  }
  launcher.onclick = () => toggle(!opened);
  $('.chat-close').onclick = () => toggle(false);
  panel.addEventListener('keydown', event => { if (event.key === 'Escape') { event.preventDefault(); toggle(false); } });
  $('.chat-reset').onclick = () => {
    position = null; panel.style.left = panel.style.top = panel.style.right = panel.style.bottom = '';
    try { localStorage.removeItem(positionKey); } catch (_) {}
  };
  const grip = $('.chat-grip');
  grip.addEventListener('pointerdown', event => {
    if (event.button !== 0) return;
    const rect = panel.getBoundingClientRect();
    dragging = {id:event.pointerId, x:event.clientX - rect.left, y:event.clientY - rect.top};
    grip.setPointerCapture(event.pointerId); grip.focus(); event.preventDefault();
  });
  grip.addEventListener('pointermove', event => {
    if (dragging?.id === event.pointerId) setPosition({x:event.clientX - dragging.x, y:event.clientY - dragging.y});
  });
  function stopDrag(event) {
    if (dragging?.id !== event.pointerId) return;
    dragging = null; if (position) setPosition(position, true);
    if (grip.hasPointerCapture(event.pointerId)) grip.releasePointerCapture(event.pointerId);
  }
  grip.addEventListener('pointerup', stopDrag); grip.addEventListener('pointercancel', stopDrag);
  grip.addEventListener('keydown', event => {
    const directions = {ArrowLeft:[-1,0], ArrowRight:[1,0], ArrowUp:[0,-1], ArrowDown:[0,1]};
    if (!directions[event.key]) return;
    event.preventDefault(); const rect = panel.getBoundingClientRect(), delta = event.shiftKey ? 50 : 16;
    setPosition({x:rect.left + directions[event.key][0] * delta, y:rect.top + directions[event.key][1] * delta}, true);
  });
  window.addEventListener('resize', () => { if (opened && position) setPosition(position); });
  window.addEventListener('focus', markRead);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) { clearTimeout(timer); poll(); } });
  window.addEventListener('pagehide', () => { stopped = true; clearTimeout(timer); controllers.forEach(controller => controller.abort()); }, {once:true});
  poll();
})();
