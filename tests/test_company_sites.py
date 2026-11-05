"""Company websites for emails: the server names new employers for the extension to look up, stores the
websites it sends back, and looks up again the emails that were guessed before the website arrived.

Runs app.py in a separate process with a throwaway data folder (never the real database), using SQLite or,
when TALENTPULSE_TEST_DATABASE_URL is set, Postgres. The email finder is replaced by a fake, so no DNS or mail
traffic happens.
"""
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SCRIPT = r'''
import os, sys, time
sys.path.insert(0, sys.argv[1])
os.chdir(sys.argv[1])
if os.environ.get("DATABASE_URL"):  # a throwaway Postgres: start from empty tables, like test_postgres.py
    import psycopg
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS leads")
        c.execute("DROP TABLE IF EXISTS company_sites")
import app

calls = []
def fake_find(first, last, company, website=None):
    calls.append(website)
    time.sleep(0.2)
    host = (website or "").replace("https://", "").replace("http://", "").replace("www.", "").strip("/")
    return (f"{first.lower()}.{last.lower()}@{host or 'guessed-' + company.lower().replace(' ', '') + '.com'}",
            "Unverified")
app.email_finder.find = fake_find

def person(n, cid, company):
    return {"fullName": f"Site Person{n}", "firstName": "Site", "lastName": f"Person{n}",
            "entityUrn": f"urn:li:fs_salesProfile:(ACwSITE{n},NAME_SEARCH,x)",
            "currentPositions": [{"title": "Java Developer", "companyName": company,
                                  "companyUrn": f"urn:li:fs_salesCompany:{cid}",
                                  "company": {"entityUrn": f"urn:li:fs_salesCompany:{cid}", "name": company}}],
            "geoRegion": "Bengaluru, Karnataka, India", "openToWork": True}

def email_of(n):
    from contextlib import closing
    with closing(app.LeadDatabase(app.DB_PATH)._get_connection()) as conn:
        row = conn.execute("SELECT email FROM leads WHERE linkedin_url LIKE ?", (f"%ACwSITE{n},%",)).fetchone()
    return row[0] if row else None

def domain_of(n):
    e = email_of(n)
    return e.split("@", 1)[1] if e else None

def wait(cond, t=15):
    end = time.time() + t
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.1)
    return False

# 1. The company id is read from Sales Navigator's position data
assert app._company_id(person(1, 1283, "Infosys")) == "1283"
assert app._company_id({"currentPositions": [{"companyName": "No Id"}]}) == ""
assert app._company_id({"currentPositions": []}) == ""

# 2. A search: the ingest answer names each new employer once, at most 10 per page
job = app.prepare_search(app.StartJobRequest(role_titles=[{"text": "Java Developer"}],
                         locations=[{"text": "Bengaluru, Karnataka, India"}], target_leads=25))["job_id"]
els = [person(1, 1001, "Tata Consultancy Services"), person(2, 1001, "Tata Consultancy Services"),
       person(3, 1002, "Acme Labs")] + [person(10 + i, 2000 + i, f"Firm {i}") for i in range(12)]
r = app.ingest(app.IngestRequest(job_id=job, page_url="test", data={"elements": els, "paging": {"start": 0, "count": 15, "total": 40}}),
               app.BackgroundTasks())
ids = [c["id"] for c in r["companies"]]
assert ids[:2] == ["1001", "1002"] and len(ids) == 10 and len(set(ids)) == 10, ids
app._ingest_worker(els, "test", False, job)

# 3. Emails are guessed at once, as before (no website yet)
assert wait(lambda: email_of(1) and email_of(3)), "emails were not guessed"
assert domain_of(1) == "guessed-tataconsultancyservices.com", email_of(1)

# 4. The extension sends the websites; guessed emails are looked up again with the real domain
saved = app.save_company_sites(app.CompanySitesRequest(companies=[
    {"id": "1001", "name": "Tata Consultancy Services", "website": "https://www.tcs.com"},
    {"id": "1002", "name": "Acme Labs", "website": ""},          # looked up, no website listed
    {"id": "abc", "website": "x.com"},                            # bad id: ignored
    {"id": "1003", "website": "not a website"}]))                 # bad website: ignored
assert saved == {"saved": 2}, saved
assert wait(lambda: domain_of(1) == "tcs.com" and domain_of(2) == "tcs.com"), (email_of(1), email_of(2))
time.sleep(0.5)
assert domain_of(3) == "guessed-acmelabs.com", email_of(3)  # no website: the guess stays

# 5. Known companies are never asked about again, and later people use the website straight away
r = app.ingest(app.IngestRequest(job_id=job, page_url="test", data={"elements": [person(4, 1001, "Tata Consultancy Services")],
               "paging": {"start": 15, "count": 1, "total": 40}}), app.BackgroundTasks())
assert all(c["id"] not in ("1001", "1002") for c in r["companies"]), r["companies"]
calls.clear()
app._ingest_worker([person(4, 1001, "Tata Consultancy Services")], "test", False, job)
assert wait(lambda: domain_of(4) == "tcs.com"), email_of(4)
assert calls == ["https://www.tcs.com"], calls

# 6. The websites survive a restart (stored in the database, not only in memory)
app._company_sites.clear()
assert app._company_website("1001") == "https://www.tcs.com" and app._company_website("1002") == ""
assert app._company_website("999999") is None
print("ok")
'''


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_company_websites_improve_emails(tmp_path, backend):
    env = dict(os.environ, TALENTPULSE_DATA_DIR=str(tmp_path), TALENTPULSE_FIND_EMAILS="1",
               TALENTPULSE_SMTP_CHECK="0", PYTHONUTF8="1", TALENTPULSE_DEBUG_INGEST="0")
    env.pop("DATABASE_URL", None)
    if backend == "postgres":
        url = os.environ.get("TALENTPULSE_TEST_DATABASE_URL", "")
        if not url:
            pytest.skip("set TALENTPULSE_TEST_DATABASE_URL to run the Postgres variant")
        env["DATABASE_URL"] = url
    res = subprocess.run([sys.executable, "-c", SCRIPT, ROOT], cwd=tmp_path, env=env,
                         capture_output=True, text=True, timeout=180)
    assert res.returncode == 0 and res.stdout.strip().endswith("ok"), res.stdout[-3000:] + res.stderr[-3000:]
