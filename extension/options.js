// Settings: which TalentPulse server the extension sends Sales Navigator results to.
const DEFAULT_API = 'http://127.0.0.1:8000';
const $ = (id) => document.getElementById(id);
const say = (text, ok) => { $('msg').textContent = text; $('msg').className = ok ? 'ok' : 'err'; };

// Only the address itself (scheme, host, port); https everywhere except this computer
function parse(text) {
  let u;
  try { u = new URL(text.trim()); } catch (e) { return { error: 'That is not a web address. Example: https://talentpulse.example.com' }; }
  const local = ['127.0.0.1', 'localhost'].includes(u.hostname);
  if (u.protocol !== 'https:' && !(u.protocol === 'http:' && local)) return { error: 'Use an https:// address (http:// only works for this computer).' };
  return { origin: u.origin };
}

async function test(origin) {
  try {
    const r = await fetch(origin + '/api/ingest/state', { cache: 'no-store' });
    if (!r.ok) return `the server answered HTTP ${r.status}`;
    const s = await r.json();
    return s && 'waiting' in s ? '' : 'that address is not a TalentPulse server';
  } catch (e) {
    return 'could not connect (' + (e.message || e) + ')';
  }
}

async function save(text) {
  const p = parse(text);
  if (p.error) return say(p.error, false);
  // Permission for this address (asked once, by Chrome); the local addresses are already allowed
  if (!['http://127.0.0.1:8000', 'http://localhost:8000'].includes(p.origin)) {
    const granted = await chrome.permissions.request({ origins: [p.origin + '/*'] });
    if (!granted) return say('Chrome permission for this address was not given, so it was not saved.', false);
  }
  await chrome.storage.sync.set({ serverUrl: p.origin });
  $('server').value = p.origin;
  say('Saved. Testing…', true);
  const problem = await test(p.origin);
  say(problem ? `Saved, but ${problem}. Check the address and that the server is running.`
              : 'Saved and connected. Reload the TalentPulse dashboard tab.', !problem);
}

chrome.storage.sync.get({ serverUrl: DEFAULT_API }).then(({ serverUrl }) => { $('server').value = serverUrl; });
$('save').addEventListener('click', () => save($('server').value));
$('server').addEventListener('keydown', (e) => { if (e.key === 'Enter') save($('server').value); });
$('local').addEventListener('click', () => save(DEFAULT_API));
