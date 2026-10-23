/* Local UI only: host authority and archive validation live in Python. */
(() => {
  const $ = id => document.getElementById(id);
  async function request(url, options = {}) {
    const response = await fetch(url, {headers: {'Content-Type': 'application/json'}, ...options});
    const data = await response.json();
    if (!response.ok || data.ok === false) throw new Error(data.detail || data.error || 'Request failed');
    return data;
  }
  const post = (url, data = {}) => request(url, {method: 'POST', body: JSON.stringify(data)});
  function element(tag, text, className) {
    const node = document.createElement(tag); node.textContent = text;
    if (className) node.className = className;
    return node;
  }
  function celebrate(text) {
    document.querySelector('.success-toast')?.remove();
    const toast = element('div', '', 'success-toast'); toast.setAttribute('role', 'status');
    toast.append(element('strong', '✓'), document.createTextNode(text));
    document.body.append(toast); setTimeout(() => toast.remove(), 3500);
  }
  const notices = [];
  let showingNotice = false;
  function finishGroup(card, state) {
    card.remove();
    if (state === 'cancelled') return;
    notices.push(`Group ${state}. Select the files again to try again.`);
    showNextNotice();
  }
  function showNextNotice() {
    if (showingNotice || !notices.length) return;
    showingNotice = true;
    const previousFocus = document.activeElement;
    const dialog = element('dialog', '', 'group-notice');
    dialog.setAttribute('role', 'alertdialog');
    dialog.setAttribute('aria-label', 'File group update');
    const message = element('p', notices.shift());
    const ok = element('button', 'OK'); ok.type = 'button'; ok.autofocus = true;
    ok.onclick = () => dialog.close();
    dialog.append(message, ok); document.body.append(dialog);
    dialog.addEventListener('close', () => {
      dialog.remove(); showingNotice = false;
      if (previousFocus?.isConnected) previousFocus.focus();
      showNextNotice();
    }, {once: true});
    dialog.showModal();
  }
  const queue = $('transferQueue');
  if (queue) {
    const completed = new WeakSet();
    new MutationObserver(() => {
      queue.querySelectorAll('[data-state="completed"]').forEach(row => {
        if (!completed.has(row)) { completed.add(row); celebrate('Transfer complete. All set!'); }
      });
    }).observe(queue, {subtree: true, attributes: true, childList: true});
  }
  if (document.body.classList.contains('client-page')) {
    window.selectHost = (ip, name) => {
      ip = String(ip || '').trim(); if (!ip) return;
      $('hostInput').value = ip; $('selectedHost').textContent = `${name || 'Selected host'} · ${ip}`;
      $('pinStep').hidden = false; $('manualConnect').open = false;
      $('pinInput').focus();
    };
    let connected = false;
    function syncConnection() {
      const next = $('statusDot').classList.contains('on');
      document.body.classList.toggle('is-connected', next);
      if (next && !connected) celebrate('Connected securely. Ready to share.');
      if (!next && connected) $('pinStep').hidden = true;
      connected = next;
    }
    new MutationObserver(syncConnection).observe($('statusDot'), {attributes: true});
    syncConnection();
    // Reloading the page must not strand a selection already waiting on the host.
    request('/api/client/batches').then(data => {
      (data.batches || []).forEach(group => {
        const card = element('div', '', 'batch-card');
        const message = element('p', group.status === 'preparing' ? 'Checking files and preparing your selection…' : `Waiting for host approval · ${group.count} files`);
        const button = element('button', 'Cancel group', 'secondary'); card.append(message, button); $('batchProgress').append(card);
        let stopped = false, polling = false;
        button.onclick = async () => {
          try { await request(`/api/client/batches/${group.batch_id}`, {method: 'DELETE'}); stopped = true; card.remove(); }
          catch (error) { message.textContent = error.message; }
        };
        const timer = setInterval(async () => {
          if (stopped) { clearInterval(timer); return; }
          if (polling) return;
          polling = true;
          try {
            const result = await request(`/api/client/batches/${group.batch_id}`);
            if (stopped) return;
            message.textContent = result.status === 'preparing'
              ? `Checking / compressing · ${((result.prepared_bytes || 0) / 1048576).toFixed(1)} MB processed…`
              : `Waiting for host approval · ${group.count} files`;
            if (!['preparing','pending','approved'].includes(result.status)) {
              stopped = true; clearInterval(timer); button.remove(); message.textContent = `Group ${result.status}.`;
              if (result.status === 'sending') setTimeout(() => card.remove(), 6000);
              else finishGroup(card, result.status);
            }
          } catch (error) { message.textContent = error.message; }
          finally { polling = false; }
        }, 1500);
      });
    }).catch(() => {});
    window.sendSelection = async files => {
      if (!files.length) return;
      const card = element('div', '', 'batch-card');
      const message = element('p', `Preparing ${files.length} file${files.length === 1 ? '' : 's'}…`);
      const cancel = element('button', 'Cancel group', 'secondary'); card.append(message, cancel);
      $('batchProgress').append(card);
      let batchId, cancelled = false;
      const controller = new AbortController();
      cancel.onclick = async () => {
        cancelled = true; controller.abort(); cancel.disabled = true;
        try {
          if (batchId) await request(`/api/client/batches/${batchId}`, {method: 'DELETE'});
          card.remove();
        } catch (error) { message.textContent = `Could not cancel: ${error.message}`; cancel.disabled = false; }
      };
      try {
        const group = await post('/api/client/batches', {files: files.map(f => ({filename: f.name, size: f.size})), zipped: $('zipSelection').checked});
        batchId = group.batch_id;
        if (cancelled) { await request(`/api/client/batches/${batchId}`, {method: 'DELETE'}); return; }
        let done = 0;
        for (let index = 0; index < files.length; index++) {
          const file = files[index], chunkSize = 4 * 1024 * 1024;
          const chunks = Math.max(1, Math.ceil(file.size / chunkSize));
          for (let chunk = 0; chunk < chunks; chunk++) {
            await request(`/api/client/batches/${batchId}/files/${index}?chunk_index=${chunk}&final=${chunk === chunks - 1}`, {
              method: 'PUT', headers: {'Content-Type': 'application/octet-stream'},
              body: file.slice(chunk * chunkSize, (chunk + 1) * chunkSize), signal: controller.signal});
          }
          message.textContent = `Prepared ${++done} of ${files.length} files…`;
        }
        message.textContent = 'Checking files and preparing your selection…';
        await post(`/api/client/batches/${batchId}/request`);
        while (!cancelled) {
          await new Promise(resolve => setTimeout(resolve, 1200));
          if (cancelled) break;
          const result = await request(`/api/client/batches/${batchId}`);
          if (cancelled) break;
          message.textContent = result.status === 'preparing'
            ? `Checking / compressing · ${((result.prepared_bytes || 0) / 1048576).toFixed(1)} MB processed…`
            : `Waiting for host approval · ${files.length} files`;
          if (result.status === 'sending') {
            message.textContent = '✓ Group approved. Follow progress in Your transfers.';
            cancel.remove(); setTimeout(() => card.remove(), 6000); break;
          }
          if (['rejected', 'expired', 'cancelled', 'failed'].includes(result.status)) {
            finishGroup(card, result.status); break;
          }
        }
      } catch (error) {
        if (!cancelled) {
          message.textContent = error.message; cancel.textContent = 'Discard group';
          // Keep the explicit cleanup action, including on network interruption.
        }
      }
    };
  }
  if (!$('rulesEditor')) return;
  const editor = $('rulesEditor');
  editor.innerHTML = `
    <p class="intro">Choose which files can enter this device. Every group also needs your approval.</p>
    <h3>1. Transfer permission</h3>
    <div class="permission-options"><label><input type="radio" name="permission" value="allow"> Is allowed</label><label><input type="radio" name="permission" value="deny" checked> Is not allowed</label></div>
    <h3>2. File extensions</h3><div id="extensionChips" class="extension-chips"></div>
    <details class="extension-picker"><summary>Select file extensions…</summary>
      <label class="field-label" for="extensionSearch">Search or type a custom extension</label>
      <input id="extensionSearch" type="search" placeholder="Search .pdf, .docx, .custom">
      <div id="extensionOptions" class="extension-options"></div><button id="addExtension" class="secondary" type="button">Add custom extension</button>
    </details>
    <h3>3. Maximum File Size Allowed</h3><label class="field-label" for="sizeNumber">Maximum per file (including the ZIP container)</label>
    <div class="size-controls"><input id="sizeNumber" type="number" min="1" step="any"><select id="sizeUnit" aria-label="Size unit"><option value="1">MB</option><option value="1024">GB</option></select></div>
    <input id="sizeSlider" type="range" min="1" step="1" aria-label="Maximum file size"><div class="size-endpoints"><span>1 MB</span><span id="diskAvailable"></span></div>
    <label class="zip-option"><input id="allowRemoteDelete" type="checkbox"> Allow connected clients to delete shared files (all clients; off by default)</label>
    <div id="ruleSummary" class="rule-summary" role="status"></div><button id="saveRules">Save rules</button><p id="rulesStatus" role="status" class="muted"></p>`;
  const common = ['.pdf','.doc','.docx','.txt','.csv','.xlsx','.pptx','.jpg','.jpeg','.png','.gif','.svg','.webp','.mp3','.wav','.mp4','.mov','.zip','.7z','.rar','.json','.py','.exe','.bat','.sh'];
  const selected = new Set(); let maxMB = 1, limitMB = 1;
  const permission = () => editor.querySelector('input[name=permission]:checked').value;
  const format = mb => mb >= 1024 ? `${(mb / 1024).toFixed(2)} GB` : `${Number(mb.toFixed(2))} MB`;
  function summary() {
    const names = [...selected].sort().join(', ');
    $('ruleSummary').textContent = `${permission() === 'allow' ? 'Only allow' : 'Block'}: ${names || (permission() === 'deny' ? 'no extensions (all types accepted)' : 'select at least one extension')}. Maximum ${format(limitMB)} per file. Each group requires approval. Changes apply after saving.`;
  }
  function renderExtensions() {
    $('extensionChips').replaceChildren();
    [...selected].sort().forEach(ext => {
      const chip = element('button', `${ext} ×`); chip.setAttribute('aria-label', `Remove ${ext}`);
      chip.onclick = () => { selected.delete(ext); renderExtensions(); }; $('extensionChips').append(chip);
    });
    $('extensionOptions').replaceChildren();
    const search = $('extensionSearch').value.toLowerCase().trim();
    [...new Set([...common, ...selected])].sort().filter(ext => ext.includes(search)).forEach(ext => {
      const label = element('label', ''), checkbox = document.createElement('input');
      checkbox.type = 'checkbox'; checkbox.checked = selected.has(ext);
      checkbox.onchange = () => { checkbox.checked ? selected.add(ext) : selected.delete(ext); renderExtensions(); };
      label.append(checkbox, document.createTextNode(ext)); $('extensionOptions').append(label);
    }); summary();
  }
  $('extensionSearch').oninput = renderExtensions;
  $('addExtension').onclick = () => {
    const ext = '.' + $('extensionSearch').value.trim().toLowerCase().replace(/^\.+/, '');
    if (!/^\.[a-z0-9][a-z0-9._+\-]{0,30}$/.test(ext)) { $('rulesStatus').textContent = 'Enter an extension such as .custom'; return; }
    selected.add(ext); $('extensionSearch').value = ''; $('rulesStatus').textContent = ''; renderExtensions();
  };
  $('extensionSearch').onkeydown = event => { if (event.key === 'Enter') { event.preventDefault(); $('addExtension').click(); } };
  editor.querySelectorAll('input[name=permission]').forEach(input => input.onchange = summary);
  function syncSize(from) {
    const unit = Number($('sizeUnit').value);
    if (from === 'slider') limitMB = Number($('sizeSlider').value);
    else if (from === 'number') {
      const value = Number($('sizeNumber').value) * unit;
      if (!Number.isFinite(value) || value < 1 || value > maxMB) {
        $('sizeNumber').setCustomValidity(`Choose between 1 MB and ${format(maxMB)}`); return;
      }
      limitMB = value;
    }
    $('sizeNumber').setCustomValidity(''); $('sizeNumber').min = 1 / unit; $('sizeNumber').max = maxMB / unit;
    $('sizeNumber').value = Number((limitMB / unit).toFixed(6)); $('sizeSlider').value = limitMB; summary();
  }
  $('sizeSlider').oninput = () => syncSize('slider'); $('sizeNumber').oninput = () => syncSize('number'); $('sizeUnit').onchange = () => syncSize('unit');
  async function loadRules() {
    const data = await request('/api/host/settings'); maxMB = Math.max(1, Math.floor(data.free_bytes / 1048576));
    $('allowRemoteDelete').checked = data.allow_remote_delete === true;
    limitMB = Math.min(maxMB, data.max_upload_mb || maxMB); $('sizeSlider').max = maxMB;
    $('diskAvailable').textContent = `${format(maxMB)} available on host`;
    editor.querySelector(`input[value="${data.permission === 'allow' ? 'allow' : 'deny'}"]`).checked = true;
    selected.clear(); (data.extensions || []).forEach(ext => selected.add(ext)); renderExtensions(); syncSize('load');
  }
  $('saveRules').onclick = async () => {
    if (!$('sizeNumber').reportValidity()) return;
    $('saveRules').disabled = true;
    try { await post('/api/host/settings', {permission: permission(), extensions: [...selected], max_upload_mb: limitMB, allow_remote_delete: $('allowRemoteDelete').checked}); $('rulesStatus').textContent = '✓ Rules saved. Applied to new incoming transfers.'; }
    catch (error) { $('rulesStatus').textContent = error.message; }
    finally { $('saveRules').disabled = false; }
  };
  loadRules().catch(error => $('rulesStatus').textContent = error.message);
  let polling = false, previous = '';
  async function pollBatches() {
    if (polling) return; polling = true;
    try {
      const data = await request('/api/host/batches'); const signature = JSON.stringify(data);
      if (signature === previous) return; previous = signature; $('batchRequests').replaceChildren();
      if (!data.batches.length) $('batchRequests').textContent = 'No groups awaiting approval.';
      data.batches.forEach(batch => {
        const card = element('div', '', 'batch-card'), files = batch.members || batch.files;
        card.append(element('strong', `${files.length} files${batch.members ? ' · Compressed ZIP' : ''}`), element('p', `From device ${batch.peer_label || batch.peer_id.slice(0, 8)}`, 'muted'));
        const details = element('details', ''), list = element('ul', '');
        details.append(element('summary', 'Review selected files'));
        files.forEach(file => list.append(element('li', `${file.filename} · ${format(file.size / 1048576)}`)));
        details.append(list); card.append(details);
        ['accept', 'reject'].forEach(action => {
          const button = element('button', action === 'accept' ? 'Receive group' : 'Reject group', action === 'reject' ? 'secondary' : '');
          button.onclick = async () => {
            card.querySelectorAll('button').forEach(b => b.disabled = true);
            try { await post(`/api/host/batches/${batch.peer_id}/${batch.batch_id}/${action}`); previous = ''; await pollBatches(); }
            catch (error) { card.append(element('p', error.message)); card.querySelectorAll('button').forEach(b => b.disabled = false); }
          }; card.append(button);
        }); $('batchRequests').append(card);
      });
    } catch (error) { $('batchRequests').textContent = error.message; previous = ''; }
    finally { polling = false; }
  }
  pollBatches(); setInterval(pollBatches, 1500);
})();
