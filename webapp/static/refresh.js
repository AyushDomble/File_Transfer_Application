/* Refresh data, never reload the page or recreate a transfer session. */
(() => {
  const button = document.getElementById('refreshPage');
  const status = document.getElementById('refreshStatus');
  if (!button) return;
  button.addEventListener('click', async () => {
    if (button.disabled) return;
    button.disabled = true; button.classList.add('busy');
    button.setAttribute('aria-busy', 'true'); status.textContent = 'Refreshing…';
    try {
      if (document.body.classList.contains('host-page')) {
        const info = await api('/api/host/info');
        if (info.ok === false) throw new Error(info.error || 'Could not refresh host');
        setRunningUI(info.running, info);
        await Promise.all([refreshPending(), refreshPeers(), ...(info.running ? [refreshFiles()] : [])]);
        const saved = await api('/api/host/transfers');
        if (saved.ok === false) throw new Error(saved.error || 'Could not refresh transfers');
        transferQueue.syncSnapshot({...saved, transfers: (saved.transfers || []).map(t => ({...t, side: 'host'}))});
        if (info.running) refreshLoop();
        // The rules editor is deliberately untouched: preserve unsaved edits.
      } else {
        await syncSavedTransfers();
        if (document.getElementById('statusDot').classList.contains('on')) await refreshFiles();
        else await scanForHosts();
      }
      status.textContent = 'Refreshed just now';
    } catch (error) {
      status.textContent = error.message || 'Refresh failed. Try again.';
    } finally {
      button.disabled = false; button.classList.remove('busy'); button.removeAttribute('aria-busy');
    }
  });
})();
