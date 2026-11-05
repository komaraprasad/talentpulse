"""Postgres storage (DATABASE_URL), as used on AWS with RDS.

Runs only when TALENTPULSE_TEST_DATABASE_URL points at a throwaway Postgres database; the `leads` table
there is dropped first. Skipped otherwise, so the normal test run needs no Postgres.
"""
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PG_URL = os.environ.get("TALENTPULSE_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not PG_URL, reason="set TALENTPULSE_TEST_DATABASE_URL to run the Postgres tests")

SCRIPT = r'''
import os, sys, threading
sys.path.insert(0, sys.argv[1])
import psycopg
psycopg.connect(os.environ["DATABASE_URL"], autocommit=True).execute("DROP TABLE IF EXISTS leads")
import sales_nav_extractor as s

def rec(i, email=None):
    return s.LeadRecord(full_name=f"P{i}", first_name="P", last_name=str(i), current_title="AI Engineer",
                        company_name="Acme", location="Hyderabad", is_open_to_work=True, is_qualified=True,
                        linkedin_url=f"https://www.linkedin.com/in/p{i}", email=email)

db = s.LeadDatabase("unused.db")
assert db.is_postgres and not os.path.exists("unused.db")
errors = []
def guard(f):
    try:
        f()
    except Exception as e:
        errors.append(repr(e))
threads = ([threading.Thread(target=guard, args=(lambda i=i: s.LeadDatabase("unused.db").upsert_lead(rec(i)),)) for i in range(100)]
           + [threading.Thread(target=guard, args=(lambda i=i: s.LeadDatabase("unused.db").update_lead_email(
               f"https://www.linkedin.com/in/p{i}", f"p{i}@acme.com", "Unverified"),)) for i in range(10)])
[t.start() for t in threads]
[t.join() for t in threads]
assert errors == [], errors
assert db.get_total_count() == 100
# Saving the same person again updates, never duplicates; a known email is kept
db.upsert_lead(rec(1))
assert db.get_total_count() == 100
lead = db.get_lead("https://www.linkedin.com/in/p1")
assert lead["email"] == "p1@acme.com", lead["email"]
# LIKE ignores case, as in SQLite
with db._get_connection() as conn:
    assert conn.execute("SELECT COUNT(*) FROM leads WHERE location LIKE ?", ("%hyder%",)).fetchone()[0] == 100
# Export works from Postgres
out = os.path.join(sys.argv[2], "pg_export.csv")
db.export_to_csv(out)
assert len(open(out, encoding="utf-8-sig").read().strip().splitlines()) == 101
print("ok")
'''


def test_postgres_storage(tmp_path):
    env = dict(os.environ, DATABASE_URL=PG_URL, PYTHONUTF8="1")
    res = subprocess.run([sys.executable, "-c", SCRIPT, ROOT, str(tmp_path)], cwd=tmp_path, env=env,
                         capture_output=True, text=True, timeout=180)
    assert res.returncode == 0 and res.stdout.strip().endswith("ok"), res.stdout + res.stderr
