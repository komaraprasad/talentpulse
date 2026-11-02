// Runs inside the Sales Navigator page. It only *reads* the lead-search responses the page itself
// requested, and hands them to content.js. It never sends requests to LinkedIn.
(() => {
  const isSearch = (url) => typeof url === 'string' && /salesApiLeadSearch|LeadSearch|salesApiPeopleSearch/i.test(url);
  const forward = (url, text) => {
    try { window.postMessage({ source: 'talentpulse', url, data: JSON.parse(text) }, '*'); } catch (e) {}
  };

  const origFetch = window.fetch;
  window.fetch = async function (...args) {
    const res = await origFetch.apply(this, args);
    try {
      const url = typeof args[0] === 'string' ? args[0] : (args[0] && args[0].url);
      if (isSearch(url) && res.ok) res.clone().text().then((t) => forward(url, t));
    } catch (e) {}
    return res;
  };

  const origOpen = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function (method, url, ...rest) {
    this.__tpUrl = url;
    return origOpen.call(this, method, url, ...rest);
  };
  const origSend = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function (...args) {
    if (isSearch(String(this.__tpUrl))) {
      this.addEventListener('load', () => {
        try {
          if (this.status >= 200 && this.status < 300) {
            const text = this.responseType === '' || this.responseType === 'text' ? this.responseText : JSON.stringify(this.response);
            forward(String(this.__tpUrl), text);
          }
        } catch (e) {}
      });
    }
    return origSend.apply(this, args);
  };
})();
