// Runs on the TalentPulse dashboard. Marks the page so it knows the extension is installed, and
// passes "open this search" requests to background.js, which opens Sales Navigator in a background tab.
document.documentElement.dataset.talentpulseExt = '1';
// The dashboard compares this with its own copy and offers an update when it is older
document.documentElement.dataset.talentpulseExtVersion = chrome.runtime.getManifest().version;
window.addEventListener('message', (e) => {
  if (e.source !== window || !e.data || e.data.source !== 'talentpulse-dashboard' || e.data.type !== 'open') return;
  if (typeof e.data.url === 'string' && e.data.url.startsWith('https://www.linkedin.com/sales/')) {
    const jobId = typeof e.data.jobId === 'string' ? e.data.jobId.slice(0, 64) : '';
    chrome.runtime.sendMessage({ type: 'open', url: e.data.url, jobId });
  }
});
