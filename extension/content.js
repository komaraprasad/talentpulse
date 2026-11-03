// Small TalentPulse panel on Sales Navigator search pages.
// While a search is waiting in the dashboard, it reads the people shown on each results page (the same
// cards you see), sends them to the dashboard, and clicks Sales Navigator's own Next button at a human
// pace (5-8 s) until the dashboard has enough qualified people or the results run out. Then the tab closes.
(() => {
  const ask = (msg) => new Promise((ok) => chrome.runtime.sendMessage(msg, ok));
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  let auto = true, busy = false, lastKey = '';

  const box = document.createElement('div');
  box.style.cssText = 'position:fixed;right:18px;bottom:90px;z-index:2147483647;width:270px;padding:14px;border-radius:12px;'
    + 'background:#14170f;color:#eef2e6;font:13px/1.45 system-ui,sans-serif;box-shadow:0 10px 30px rgba(0,0,0,.35);border:1px solid #2c3324';
  box.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
      <b style="color:#B6F24A">TalentPulse</b><span id="tpX" style="cursor:pointer;opacity:.6">✕</span></div>
    <div id="tpMsg">Checking dashboard…</div>
    <div style="display:flex;gap:6px;margin-top:10px">
      <button id="tpAuto" style="flex:1;padding:7px;border:0;border-radius:8px;background:#B6F24A;color:#14170f;font-weight:600;cursor:pointer">Resume</button>
      <button id="tpStop" style="padding:7px 10px;border:1px solid #3a4230;border-radius:8px;background:transparent;color:#eef2e6;cursor:pointer">Pause</button>
    </div>`;
  const mount = () => document.body ? document.body.appendChild(box) : setTimeout(mount, 300);
  mount();
  const msg = (t) => { box.querySelector('#tpMsg').textContent = t; };
  box.querySelector('#tpX').onclick = () => { auto = false; box.remove(); };
  box.querySelector('#tpStop').onclick = () => { auto = false; msg('Paused.'); };
  box.querySelector('#tpAuto').onclick = () => { auto = true; lastKey = ''; };

  // The server address comes from the extension's options page
  const offlineText = (r) => `Can't reach TalentPulse at ${(r && r.server) || 'the server'}`
    + `${r && r.error ? ' (' + r.error + ')' : ''}. Check that it is running and the address in the extension's options.`;
  const finished = (text) => { auto = false; msg(text); setTimeout(() => ask({ type: 'done' }), 3000); };
  const txt = (root, sel) => ((root.querySelector(sel) || {}).innerText || '').trim();
  const nextButton = () => document.querySelector('button.artdeco-pagination__button--next, button[aria-label="Next"]');
  const cards = () => [...document.querySelectorAll('li.artdeco-list__item, [data-x-search-result="LEAD"]')]
    .filter((li) => li.querySelector('a[href*="/sales/lead/"]'));
  // "No matches found" pages still list suggested people below; those are not search results
  const noResults = () => /no matches found|no results found|leads available if you remove/i
    .test((document.querySelector('main') || document.body).innerText.slice(0, 4000));

  // Sales Navigator draws cards as you scroll, so scroll through the list to have all 25 on screen
  // Zoomed out, all 25 cards fit on screen at once, so they are drawn without scrolling page by page
  async function loadAllCards() {
    document.documentElement.style.zoom = '0.25';
    for (let i = 0; i < 10; i++) {
      const list = cards();
      if (list.length) list[list.length - 1].scrollIntoView({ block: 'end' });
      await sleep(250);
      if (cards().length >= 25 || (i > 3 && cards().length === list.length && list.length)) break;
    }
    await sleep(300);  // let the last cards fill in their text
  }

  function readCard(li) {
    const link = li.querySelector('a[href*="/sales/lead/"]');
    const name = txt(li, '[data-anonymize="person-name"]') || (link.innerText || '').trim();
    const all = (li.innerText || '').replace(/\s+/g, ' ');
    return {
      fullName: name,
      profileUrl: link.href.split('?')[0],
      currentPositions: [{ title: txt(li, '[data-anonymize="title"]'), companyName: txt(li, '[data-anonymize="company-name"]') }],
      geoRegion: txt(li, '[data-anonymize="location"]'),
      headline: txt(li, '[data-anonymize="person-blurb"]') || all.slice(0, 300),
      openToWork: /open to work|#opentowork|actively (looking|seeking)/i.test(all),
    };
  }

  // Sales Navigator's own code for a city: the same lookup its Geography filter makes when you type a place
  async function regionCode(place) {
    const csrf = (document.cookie.match(/JSESSIONID="?([^";]+)/) || [])[1];
    const city = place.split(',')[0].trim();
    const r = await fetch(`/sales-api/salesApiFacetTypeahead?q=query&type=BING_GEO&query=${encodeURIComponent(city)}&count=10&start=0`,
      { credentials: 'include', headers: { 'csrf-token': csrf || '', 'x-restli-protocol-version': '2.0.0' } });
    if (!r.ok) return null;
    const els = ((await r.json()) || {}).elements || [];
    const want = place.toLowerCase().split(',').map((x) => x.trim());
    const score = (t) => want.filter((w) => t.toLowerCase().includes(w)).length;
    const best = els.map((e) => ({ id: String(e.id || e.value || '').split(':').pop(), text: e.displayValue || e.text || '' }))
      .filter((e) => /^\d+$/.test(e.id) && e.text.toLowerCase().includes(city.toLowerCase()))
      .sort((a, b) => score(b.text) - score(a.text))[0];
    return best || null;
  }

  // LinkedIn's standard job title for the role: the same lookup its Current job title filter makes
  async function titleCode(role) {
    const csrf = (document.cookie.match(/JSESSIONID="?([^";]+)/) || [])[1];
    const r = await fetch(`/sales-api/salesApiFacetTypeahead?q=query&type=TITLE&query=${encodeURIComponent(role)}&count=10&start=0`,
      { credentials: 'include', headers: { 'csrf-token': csrf || '', 'x-restli-protocol-version': '2.0.0' } });
    if (!r.ok) return null;
    const els = ((await r.json()) || {}).elements || [];
    const want = role.trim().toLowerCase();
    const list = els.map((e) => ({ id: String(e.id || e.value || '').split(':').pop(), text: e.displayValue || e.text || '' }))
      .filter((e) => /^\d+$/.test(e.id));
    // Only an exact standard title; a near one (e.g. "DevOps Manager") would change who is searched
    return list.find((e) => e.text.trim().toLowerCase() === want) || null;
  }

  // Fast path: take the results request this page itself just made, and ask for the next 25 the same way
  // (only "start" changes), as Next would. About 1.5 s per 25 people, no drawing or scrolling.
  let fastTried = false;
  async function fastRun(startAt) {
    fastTried = true;
    let base = null;
    for (let i = 0; i < 20 && !base; i++) {
      base = performance.getEntriesByType('resource').map((e) => e.name).filter((u) => /salesApiLeadSearch/.test(u)).pop();
      if (!base) await sleep(500);
    }
    if (!base) return false;
    const csrf = (document.cookie.match(/JSESSIONID="?([^";]+)/) || [])[1] || '';
    let start = startAt || 0, total = Infinity, sent = 0, wrapped = !start;
    while (auto && start < total && start < 2500) {
      const url = /([?&(,])start[:=]\d+/.test(base)
        ? base.replace(/([?&(,]start[:=])\d+/, `$1${start}`)
        : base + (base.includes('?') ? '&' : '?') + `start=${start}`;
      const r = await fetch(url, { credentials: 'include', headers: { 'csrf-token': csrf, 'x-restli-protocol-version': '2.0.0', accept: 'application/json' } });
      if (!r.ok) return sent > 0;  // fall back to reading the screen if the first call fails
      const data = await r.json();
      const n = (data.elements || []).length;
      if (!n && !wrapped && start > 0) { start = 0; wrapped = true; continue; }  // past the end: back to the top
      total = (data.paging && data.paging.total) || (n ? Infinity : 0);
      if (!n) { if (!sent) data.paging = { start: 0, count: 0, total: 0 }; }
      await ask({ type: 'ingest', pageUrl: location.href, data });
      sent += n;
      const s = await ask({ type: 'state' });
      if (!s || !s.waiting) { finished(`Done: ${s ? s.processed : ''} people. Results are on the dashboard.`); return true; }
      msg(`${s.processed} / ${s.limit} people collected…`);
      if (!n) break;
      start += n;
      if (start >= total && !wrapped) { start = 0; wrapped = true; }  // reached the end: check the top for new people
      await sleep(1200 + Math.random() * 800);  // short pause between requests
    }
    finished('Search finished. Results are on the dashboard.');
    return true;
  }

  let refineTried = false;
  async function refine(places, roles) {
    refineTried = true;
    msg('Matching your search to LinkedIn\u2019s own title and location…');
    const titles = [];
    for (const role of roles) {
      try { const t = await titleCode(role); if (t) titles.push({ ...t, role }); } catch (e) {}
      await sleep(600);
    }
    const found = [];
    for (const p of places) {
      try { const c = await regionCode(p); if (c) found.push({ ...c, place: p }); } catch (e) {}
      await sleep(800);
    }
    const r = await ask({ type: 'refine', regions: found, titles });
    if (r && r.url) { location.href = r.url; return true; }
    return false;  // keep the country-wide search; the dashboard still keeps only your location
  }

  async function tick() {
    if (busy || !auto || !/\/sales\/search\/people/.test(location.pathname)) return;
    busy = true;
    try {
      const s = await ask({ type: 'state' });
      if (!s || s.offline) return msg(offlineText(s));
      if (!s.waiting) return msg('No search waiting. Click "Launch search" in TalentPulse first.');
      if (((s.refine && s.refine.length) || (s.refine_titles && s.refine_titles.length)) && !refineTried
          && await refine(s.refine || [], s.refine_titles || [])) return;
      if (noResults()) {
        await ask({ type: 'ingest', pageUrl: location.href, data: { elements: [], paging: { start: 0, count: 0, total: 0 } } });
        return finished('Sales Navigator found no matches for this search.');
      }
      if (!fastTried) {
        msg('Collecting results…');
        if (await fastRun(s.start_at)) return;
      }
      if (!cards().length) return msg('Waiting for results to load…');
      // One read per results page: the page is identified by its number, not by card text that is still loading
      const pageNo = ((document.querySelector('.artdeco-pagination__indicator--number.active, [aria-current="true"]') || {}).innerText || '').trim();
      const key = location.pathname + location.search.replace(/[?&]page=\d+/, '') + '#' + (pageNo || (location.search.match(/page=(\d+)/) || [])[1] || '1');
      if (key === lastKey) return;  // still on the page already sent
      msg('Reading this page…');
      await loadAllCards();
      const people = cards().map(readCard).filter((p) => p.fullName);
      const next = nextButton();
      const last = !next || next.disabled;
      lastKey = key;
      const data = { elements: people };
      if (last) data.paging = { start: 0, count: people.length, total: people.length };
      const r = await ask({ type: 'ingest', pageUrl: location.href, data });
      if (!r || r.offline) return msg(offlineText(r));
      await sleep(600);
      const after = await ask({ type: 'state' });
      if (!after || !after.waiting) return finished(`Done: ${after ? after.processed : ''} people. Results are on the dashboard.`);
      if (last) return finished('Last page reached. Results are on the dashboard.');
      msg(`${after.processed} / ${after.limit} people collected. Next page soon…`);
      await sleep(2000 + Math.random() * 1500);  // still a human-like pause between pages
      if (auto) {
        next.click();
        // Wait for the next page number to show before reading again
        for (let i = 0; i < 20; i++) {
          await sleep(300);
          const now = ((document.querySelector('.artdeco-pagination__indicator--number.active, [aria-current="true"]') || {}).innerText || '').trim();
          if (now && now !== pageNo) break;
        }
      }
    } finally {
      busy = false;
    }
  }

  setInterval(tick, 1000);
  tick();
})();
