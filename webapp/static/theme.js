// Resolve before paint; no network or external assets are needed.
(() => {
  const key = 'filetransfer.theme';
  const system = window.matchMedia('(prefers-color-scheme: dark)');
  let preference = 'system';
  try { preference = localStorage.getItem(key) || 'system'; } catch (_) {}
  if (!['system', 'light', 'dark'].includes(preference)) preference = 'system';
  function apply() {
    document.documentElement.dataset.theme = preference === 'system' ? (system.matches ? 'dark' : 'light') : preference;
    const picker = document.getElementById('themeSelect');
    if (picker) picker.value = preference;
  }
  apply();
  system.addEventListener('change', apply);
  window.addEventListener('storage', event => {
    if (event.key === key || event.key === null) {
      preference = ['light', 'dark'].includes(event.newValue) ? event.newValue : 'system';
      apply();
    }
  });
  document.addEventListener('DOMContentLoaded', () => {
    apply();
    if (document.body.classList.contains('client-page')) {
      const dot = document.getElementById('statusDot');
      const updateStatus = () => {
        document.getElementById('connectionLabel').textContent = dot.classList.contains('on') ? 'Connected to host' : 'Not connected';
      };
      new MutationObserver(updateStatus).observe(dot, {attributes:true, attributeFilter:['class']});
      updateStatus();
    }
    document.getElementById('themeSelect')?.addEventListener('change', event => {
      preference = event.target.value;
      try { localStorage.setItem(key, preference); } catch (_) {}
      apply();
    });
  });
})();
