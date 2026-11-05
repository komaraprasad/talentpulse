# TalentPulse — Open-to-Work Sourcing Console

An in-house tool that finds **candidates who are open to work** on LinkedIn Sales Navigator and exports a
recruiter-ready Excel/CSV file, with work emails where they can be found.

You pick one or more **roles** and **locations**; the search runs in your own logged-in Sales Navigator through
a small browser extension, and the candidates stream into the TalentPulse dashboard.

---

## How it works

```
 Dashboard (this server)  ──postMessage──▶  TalentPulse extension  ──▶  Sales Navigator, in a hidden
       ▲                                    (in each user's browser)    background tab of that browser
       └──────── results (POST /api/ingest) ◀───────────┘
```

- The **server never contacts LinkedIn** and stores no LinkedIn logins. All LinkedIn access happens in the
  user's own browser, under their own Sales Navigator seat.
- The search opens in a background tab inside a collapsed "TalentPulse" tab group: the user stays on the
  dashboard. The extension reads the result pages Sales Navigator itself loads (25 people per page) and sends
  them to the server.
- After a search, the extension looks up each **new employer's website** once (at most 30 per search, with
  pauses), so emails use the real company domain.

## Project structure

```
app.py                  Web server (FastAPI) and the dashboard page
sales_nav_extractor.py  Cleaning, open-to-work check, email finder, database (SQLite or PostgreSQL), export
job_titles.py           Job-title catalogue for the Role box
locations.py            Place names -> LinkedIn location IDs (aliases, nearest-region fallback)
world_geo.db            Offline world locations for the Location box
extension/              The TalentPulse browser extension (Edge / Chrome, Manifest V3)
tests/                  Automated tests
Dockerfile              Container image for AWS
deploy/aws/             ECS and EKS configuration and the AWS deployment guide
.env.example            Optional server settings
```

---

## Run it on your computer

```bash
pip install -r requirements.txt
python app.py
```

Open **http://127.0.0.1:8000**. Locally the data is kept in `sales_nav_leads.db` (SQLite) in this folder.

Install the extension once: click **Download extension** in the dashboard's banner (or use the `extension`
folder directly), open `edge://extensions` (Chrome: `chrome://extensions`), turn on **Developer mode**, click
**Load unpacked** and choose the folder that contains `manifest.json`. Stay logged in to Sales Navigator in the
same browser.

## Run the tests

```bash
python -m pytest -q tests
```

The PostgreSQL tests run when `TALENTPULSE_TEST_DATABASE_URL` points at a throwaway database
(its tables are dropped); otherwise they are skipped.

## Deploy on AWS

See **[deploy/aws/README.md](deploy/aws/README.md)**: one container (port 8000, health check `/api/status`),
PostgreSQL (RDS) through `DATABASE_URL`, an Application Load Balancer, and **exactly one replica**.
Server settings are listed in [.env.example](.env.example).

---

## Using the dashboard

### New Search
| Field | Notes |
|---|---|
| Role / job title | Required. Up to 5 roles; people with any of them are found. Suggestions from the built-in title catalogue; any title can be typed as written. |
| Location | Up to 10 places; people in any of them are found. Everyday names work (Bangalore, Gurgaon, USA). A place without its own LinkedIn ID is searched within its region and then filtered to the place. |
| Keywords | Optional, up to 10, "Match any" or "Match all". Searched in the profile, like Sales Navigator's keyword box. |
| How many leads | 25, 50, 75 or 100 per search. |

Every search looks for open-to-work people: the keyword "Open to work" is always included.

### Overview
Totals, the running search (progress, **Stop safely**, **Export to Excel**), the live feed and recent searches.
Several people can search at once (up to 5 searches); each sees its own running search.

### Lead Explorer
Every saved candidate, with filters (role, batch, open to work, experience, contact, sort) and Excel export of the
filtered list or everything. Clicking a row opens the candidate's details, with **Mark open to work** /
**Mark not looking** overrides.

---

## How "open to work" is decided

1. Sales Navigator returns only people whose profile contains "Open to work" (the search keyword).
2. Each person's headline and About text is then checked:
   - `#OpenToWork` / "open to work" → **Yes - #OpenToWork stated in profile**
   - "actively looking", "seeking new opportunities", "open to new roles", "available for hire"… →
     **Yes - job-seeking language in profile**
   - none of these → **No** (the phrase was somewhere the check can't see, e.g. an old job description)

Sales Navigator does not show LinkedIn's green #OpenToWork photo frame, and nobody outside LinkedIn Recruiter can
see the private "recruiters only" setting; those people can't be found by any Sales Navigator tool.

## Contact details

- **Phone**: only when the candidate wrote one in their headline or About text (about 1 in 100). LinkedIn
  never shares phone numbers.
- **Email**: from the profile text when written there; otherwise the finder
  1. takes the employer's real website (looked up by the extension), or guesses the domain from the company name,
  2. builds the common formats (`first.last@`, `flast@`, …) from the cleaned name,
  3. where outgoing port 25 is open, asks the company's mail server which address exists (**no email is sent**).

  No email is made for "Self", "Student", "Freelance", "Open to Work" and similar non-employers.

| Email Status (export) | Meaning |
|---|---|
| Confirmed | The company's mail server accepted this exact address |
| From profile | The candidate wrote it in their profile |
| Likely (unconfirmed) | Right company domain; the server accepts any address, so the exact name can't be proved |
| Guess | The most common format, not checked |

## Excel / CSV export (14 columns)

Candidate Name · LinkedIn Profile URL · Current Job Title · Headline · Current Company · Years of Experience ·
Location · Open-to-Work Status · Mobile Number · Email Address · Email Status · Relevant Skills / Keywords ·
First Found (IST) · Last Seen (IST)

Records are de-duplicated by LinkedIn profile. Cells starting with `=`, `+`, `-`, `@`, tab or carriage return are
prefixed with `'` so spreadsheets don't run them as formulas.

## Safety limits

- 2,000 candidates per rolling 24 hours (shown in the sidebar); at most 100 per search.
- Short random pauses between result pages; company lookups only after the search, at most 30, and they stop at
  once if LinkedIn asks to slow down.
- Sales Navigator returns at most 2,500 results per search.

## Command line (offline demo)

```bash
python sales_nav_extractor.py --dry-run   # sample profiles, no LinkedIn access
```

## Known limits

- Emails are best guesses unless marked Confirmed or From profile; check before a campaign.
- The database holds personal data: agree retention and access rules with HR/legal (India's DPDP Act, GDPR),
  use emails only for relevant job outreach, and delete data on request.
