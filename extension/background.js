// Talks to the TalentPulse server (content scripts on https pages can't call it directly).
// The server address is set on the extension's options page; it defaults to the local dashboard.
const DEFAULT_API = 'http://127.0.0.1:8000';
const LOCAL_ORIGINS = ['http://127.0.0.1:8000', 'http://localhost:8000'];  // covered by the manifest
// Dashboards the manifest already reaches: a search launched from one of them reports back to it
const knownDashboard = (origin) => LOCAL_ORIGINS.includes(origin) || /^https:\/\/[a-z0-9-]+\.onrender\.com$/.test(origin);
let searchTabId = null;

const getApi = async () => {
  const { serverUrl } = await chrome.storage.sync.get({ serverUrl: DEFAULT_API });
  return (serverUrl || DEFAULT_API).replace(/\/+$/, '');
};

// Which search each Sales Navigator window belongs to, so its results reach the tester who launched it.
// Kept in session storage because the service worker can be stopped between messages.
const tabJobs = async () => (await chrome.storage.session.get({ tabJobs: {} })).tabJobs;
const jobFor = async (tabId) => (await tabJobs())[tabId] || '';
const setJob = async (tabId, jobId) => {
  const all = await tabJobs();
  all[tabId] = jobId;
  await chrome.storage.session.set({ tabJobs: all });
};

// The dashboard page needs dashboard_bridge.js. The manifest covers the local addresses; a hosted
// address is registered here once the user has allowed it on the options page.
async function syncBridge() {
  try { await chrome.scripting.unregisterContentScripts({ ids: ['tp-bridge'] }); } catch (e) {}
  let origin;
  try { origin = new URL(await getApi()).origin; } catch (e) { return; }
  if (knownDashboard(origin)) return;
  if (!(await chrome.permissions.contains({ origins: [origin + '/*'] }))) return;
  await chrome.scripting.registerContentScripts([
    { id: 'tp-bridge', matches: [origin + '/*'], js: ['dashboard_bridge.js'], runAt: 'document_start' }]);
}
chrome.runtime.onInstalled.addListener(syncBridge);
chrome.runtime.onStartup.addListener(syncBridge);
chrome.storage.onChanged.addListener((changes) => { if (changes.serverUrl) syncBridge(); });
chrome.tabs.onRemoved.addListener(async (tabId) => {
  const all = await tabJobs();
  if (tabId in all) { delete all[tabId]; await chrome.storage.session.set({ tabJobs: all }); }
});

chrome.runtime.onMessage.addListener((msg, sender, reply) => {
  if (msg.type === 'open') {
    // Sales Navigator opens in a background tab of the dashboard's window, folded into a collapsed
    // "TalentPulse" tab group: the user stays on the dashboard and no page or window appears.
    // The fast path only reuses the page's own results request, which works without the page on screen.
    const open = () => chrome.tabs.create({ url: msg.url, active: false, windowId: sender.tab ? sender.tab.windowId : undefined },
      async (t) => {
        searchTabId = t.id;
        setJob(t.id, msg.jobId || '');
        try {
          const group = await chrome.tabs.group({ tabIds: t.id });
          await chrome.tabGroups.update(group, { title: 'TalentPulse', color: 'green', collapsed: true });
        } catch (e) {}  // without tab groups the tab still stays in the background
      });
    // Results go back to the dashboard that launched the search (no need to set the address by hand)
    let from = '';
    try { from = new URL(sender.url || '').origin; } catch (e) {}
    const remember = knownDashboard(from) ? chrome.storage.sync.set({ serverUrl: from }) : Promise.resolve();
    // Close the previous search window, if any, then open a fresh one
    if (searchTabId !== null) chrome.tabs.remove(searchTabId, () => void chrome.runtime.lastError);
    remember.then(open);
    return false;
  }
  if (msg.type === 'done') {
    // The search tab closes itself once the run has finished
    if (sender.tab && sender.tab.id === searchTabId) { chrome.tabs.remove(searchTabId); searchTabId = null; }
    return false;
  }
  (async () => {
    const api = await getApi();
    try {
      const jobId = sender.tab ? await jobFor(sender.tab.id) : '';
      const post = (path, body) => fetch(api + path, { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...body, job_id: jobId || undefined }) });
      const r = msg.type === 'refine' ? await post('/api/search/refine', { regions: msg.regions, titles: msg.titles || [] })
        : msg.type === 'ingest' ? await post('/api/ingest', { page_url: msg.pageUrl, data: msg.data })
        : await fetch(`${api}/api/ingest/state${jobId ? '?job_id=' + encodeURIComponent(jobId) : ''}`);
      if (!r.ok) return reply({ offline: true, server: api, error: `server answered HTTP ${r.status}` });
      reply(await r.json());
    } catch (e) {
      reply({ offline: true, server: api, error: String(e && e.message || e) });
    }
  })();
  return true;  // async reply
});
