# TalentPulse AI — Open-to-Work Sourcing Console

An in-house, zero-subscription tool that finds **candidates who are open to work** on LinkedIn Sales Navigator, filters out false matches, finds their work email, and exports a recruiter-ready CSV.

You pick a **role**, a **location** and (optionally) a **seniority**; the tool searches Sales Navigator live through your logged-in session, checks every lead, and streams results to a web dashboard.

---

## Project Structure

```
sales_nav_pipeline/
├── app.py                   # Web dashboard (FastAPI + live updates via SSE) and its UI
├── sales_nav_extractor.py   # Search, cleaning, qualification gates, email finder, database, CSV, CLI
├── job_titles.py            # Standard job-title catalogue + typeahead for the Role box
├── locations.py             # Location names -> LinkedIn geo IDs (aliases, parent-region fallback)
├── world_geo.db             # Offline world locations for the location autocomplete
├── sales_nav_leads.db       # Candidate database (created automatically)
├── requirements.txt
├── .env.example             # Template for the two LinkedIn session cookies
└── tests/test_extractor.py  # Automated tests
```

---

## Setup

```bash
pip install -r requirements.txt
```

Create a `.env` file (copy `.env.example`) with the cookies from a browser logged into **Sales Navigator**
(DevTools → Application → Cookies → `https://www.linkedin.com`):

```env
LI_AT=AQED...
JSESSIONID="ajax:1234567890123456789"
```

Treat these like a password. Logging out of LinkedIn invalidates them; copy fresh ones if you see an HTTP 401.

## Live searches: the TalentPulse browser extension
The server **never contacts LinkedIn itself**. LinkedIn ends sessions whose cookies are reused outside the browser.
Live results come from your own Sales Navigator tab through the extension in `extension/`, which only reads the
results the page already loaded (no extra LinkedIn requests). The cookies in `.env` are not needed for this.

Install once (Edge or Chrome): open `edge://extensions` → turn on **Developer mode** → **Load unpacked** → pick the
`extension` folder.

Each search:
1. Start the dashboard (`python app.py`), fill **New Search**, click **Launch search**: Sales Navigator opens in a new tab
   with your filters.
2. In the **TalentPulse** panel on that page, click **Auto-page**. It clicks Next at a human pace (8–14 s) until the lead
   limit or the last page. You can also page manually; each page you open is sent.
3. Results appear live in the dashboard, checked by gates A–G, with emails and CSV export as before.

## Run the dashboard

```bash
python app.py
```

Open **http://localhost:8000**. The server only accepts connections from this computer.

## Run the tests

```bash
python -m pytest -q tests
```

---

## Using the dashboard

### New Search
| Field | Notes |
|---|---|
| Role / job title | Required. Typeahead like LinkedIn's title filter: suggestions come from LinkedIn's own standard-title list when cookies are set (`/sales-api/salesApiFacetTypeahead`), otherwise from the built-in catalogue in `job_titles.py` (~380 standard titles across all job families, each with the variants people write). Partial words, any word order, short forms (`SDE`, `BDE`, `CA`) and typos work. One title per search (picking another replaces it), or keep your own words. A standard title also accepts its variants (Software Engineer → SDE, Software Developer). |
| Location | Works like LinkedIn's location filter: add one or more places (up to 10, OR'd — people in any of them are found), each picked from suggestions (LinkedIn's own list when cookies are set, otherwise `world_geo.db`). Everyday names work (Bangalore, Gurgaon, Bombay, USA, UK, NCR). Every place is resolved to a LinkedIn geo ID; a place without one is searched within its nearest region that has one, then Gate G keeps only candidates in the place itself. |
| Seniority | Optional, one level: Entry-level, Senior, Manager, Director or Executive. |
| Skills | Optional words added to the search (e.g. `Python, Spring Boot`). |
| How many leads | 25, 50, 100, 250 or 500. |

Every search is an open-to-work search. Runs pause 2.5–4.5 s between pages and 15 s every 10 pages to protect the account.

### Overview
Total candidates and open-to-work counts, the live job card (status, progress, processed / qualified / filtered, **Stop safely**, **Export run CSV**), the live feed for the current run, and recent runs.

**Stop safely** finishes the page in progress, then stops before the next one.

### Lead Explorer
Search, filter (role, batch, open to work, qualification, contact, sort, incl. by experience) and export (**filtered** or **all**). Each row has re-check, details and LinkedIn buttons; clicking a row opens the candidate drawer.

### Candidate drawer
Open-to-work evidence, qualification result and reasons, title/company/**experience**/location, skills, contact details with copy buttons and email status, and **Mark open to work / Mark not looking** overrides.

---

## How each lead is checked

| Gate | Rejects when |
|---|---|
| A — Current role | The role appears only in a past position |
| B — Company check | The keyword matched the company name, not the title |
| C — Recruiters | The profile is a recruiter / talent acquisition / hiring post |
| D — Open to work | No #OpenToWork badge or job-seeking language (marked **Needs review**) |
| E — Seniority | Title doesn't match the chosen seniority |
| F — Experience | Calculated experience is outside a requested range (API/CLI only) |
| G — Location | Only when the place had no LinkedIn ID of its own: the candidate's location doesn't mention it |

## Years of experience
Calculated from position start/end dates in the LinkedIn data; overlapping jobs are counted once. Shown as `—` when dates are missing. If LinkedIn only returns the current job, the figure undercounts.

## Contact details
- **Phone** — only when the candidate wrote one in their headline or bio (labelled numbers, `+country` numbers, and bare 10-digit Indian mobiles). Sales Navigator does not expose phone numbers.
- **Work email** — for Qualified / Needs-review leads without a profile email, the built-in finder:
  1. resolves the company's email domain (must have a mail server),
  2. builds common formats (`first.last@`, `flast@`, …),
  3. asks the company mail server which address exists — **no email is sent**.

| Email Status | Meaning |
|---|---|
| Verified | Mail server confirmed the address |
| From profile | Written by the candidate in their headline/bio |
| Risky (catch-all) | Server accepts any address; most likely format shown |
| Risky (shared name) | Two candidates with the same name at the same company got the same address; it may belong to the other person |
| Unverified | Server couldn't be checked; best guess |
| Not found | Nothing could be confirmed — no address is invented |

Requires outbound port 25. Companies whose domain differs from their name (e.g. Tata Consultancy Services → tcs.com) come back as Not found.

---

## Safety limits
- **Daily quota:** 2,000 leads per rolling 24 hours (shown in the sidebar).
- Stops immediately on HTTP 401 (expired session), 429 (rate limited) or 999 (LinkedIn checkpoint).
- Sales Navigator returns at most 2,500 results per search.

## CSV export (16 columns)
| Column | What it holds |
|---|---|
| Candidate Name, LinkedIn Profile URL | Who it is (URL is the de-duplication key) |
| Current Job Title, Headline, Current Company | Their role; the headline often states notice period / "immediate joiner" |
| Years of Experience, Location | From job dates and LinkedIn location |
| Open-to-Work Status | `No`, or how it's known: LinkedIn #OpenToWork badge / #OpenToWork stated in profile / job-seeking language / confirmed by recruiter |
| Qualification Status, Qualification Reason | Qualified / Needs Review / Rejected, with a short reason ("All checks passed", "Matched company name, not job title") |
| Mobile Number, Email Address | Phone shown as `(+91) 98765 43210`; best email found |
| Relevant Skills / Keywords | LinkedIn skills, or skills spotted in headline/bio (all job families) |
| First Found (IST), Last Seen (IST) | When the candidate first appeared, and the latest search that found them |

The file is UTF-8 with a BOM so Excel shows all names correctly. Cells starting with `=`, `+`, `-`, `@`, tab or carriage return are prefixed with `'` so spreadsheets don't execute them. Records are de-duplicated by LinkedIn URL.

---

## Command line (optional)

```bash
# Offline simulation with sample profiles — no cookies, no LinkedIn calls
python sales_nav_extractor.py --dry-run

# Live search
python sales_nav_extractor.py --role "Java Developer" --location "India" --pages 2

# Live search from a Sales Navigator URL
python sales_nav_extractor.py --url "https://www.linkedin.com/sales/search/people?query=..." --pages 4
```

Options: `--role`, `--location`, `--keywords` (default `Open to work`), `--pages` (25 leads each), `--url`, `--db-path`, `--csv-path`, `--dry-run`, `--no-jitter`, `--max-leads`.

---

## Known limits
- Seniority and experience filter IDs follow Sales Navigator's codes but have not yet been confirmed against a live account; a pasted URL (CLI) always uses LinkedIn's own codes.
- "Re-check live status" reads the public profile page and depends on LinkedIn's page markup.
- Using LinkedIn's internal search with session cookies is against LinkedIn's user agreement and carries some account risk; keep volumes modest and use a dedicated recruiting seat.
- The database stores personal data; agree retention and access rules with HR/legal (GDPR / DPDP Act).
