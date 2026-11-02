"""Hosting checks: a real server on a spare port with a throwaway data folder (never the real database).

Covers per-search isolation, the extension's job_id, older clients without one, exports, dry runs,
hosted-mode switches and request limits. No LinkedIn or email traffic: email finding is turned off.
"""
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

import pytest
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _person(n):
    return {"fullName": f"Test Person{n}", "firstName": "Test", "lastName": f"Person{n}",
            "entityUrn": f"urn:li:fs_salesProfile:(ACwAATEST{n},NAME_SEARCH,x)",
            "currentPositions": [{"title": "DevOps Engineer", "companyName": "Acme Test"}],
            "geoRegion": "Bengaluru, Karnataka, India", "openToWork": True}


def _page(numbers, total=100):
    return {"elements": [_person(n) for n in numbers], "paging": {"start": 0, "count": 25, "total": total}}


def _start_server(data_dir, hosted):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    env = dict(os.environ, PORT=str(port), HOST="127.0.0.1", TALENTPULSE_DATA_DIR=data_dir,
               TALENTPULSE_FIND_EMAILS="0", TALENTPULSE_SMTP_CHECK="0", PYTHONUTF8="1",
               TALENTPULSE_HOSTED="1" if hosted else "")
    proc = subprocess.Popen([sys.executable, "app.py"], cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    base = f"http://127.0.0.1:{port}"
    for _ in range(80):
        try:
            if requests.get(base + "/api/ingest/state", timeout=1).ok:
                return proc, base
        except requests.RequestException:
            time.sleep(0.25)
    proc.kill()
    pytest.fail("server did not start")


@pytest.fixture(scope="module")
def hosted():
    data_dir = tempfile.mkdtemp(prefix="tp_hosted_test_")
    proc, base = _start_server(data_dir, hosted=True)
    yield base, data_dir
    proc.kill()


@pytest.fixture(scope="module")
def local():
    data_dir = tempfile.mkdtemp(prefix="tp_local_test_")
    proc, base = _start_server(data_dir, hosted=False)
    yield base, data_dir
    proc.kill()


def _prepare(base, role, replace=None):
    r = requests.post(base + "/api/search/prepare", json={
        "role_titles": [{"text": role}], "locations": [{"text": "Bengaluru, Karnataka, India"}],
        "target_leads": 25, "replace_job_id": replace}).json()
    assert not r.get("error"), r
    return r["job_id"]


def _wait(cond, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.2)
    return False


def test_dashboard_and_read_endpoints(hosted):
    base, _ = hosted
    page = requests.get(base + "/")
    assert page.status_code == 200 and "myJobId" in page.text and "kwBox" in page.text
    for path in ("/api/status", "/api/leads", "/api/batches", "/api/ingest/state",
                 "/api/geo/search?q=Beng", "/api/titles/search?q=devops"):
        assert requests.get(base + path).status_code == 200, path


def test_searches_are_kept_apart(hosted):
    base, data_dir = hosted
    a = _prepare(base, "DevOps Engineer")
    b = _prepare(base, "Data Engineer")
    assert a != b
    # Extension results tagged with search A only count for A
    r = requests.post(base + "/api/ingest", json={"job_id": a, "page_url": "test", "data": _page([1, 2, 3])}).json()
    assert r["accepted"] == 3 and r["job_id"] == a
    assert _wait(lambda: requests.get(base + f"/api/ingest/state?job_id={a}").json()["processed"] == 3)
    assert requests.get(base + f"/api/ingest/state?job_id={b}").json()["processed"] == 0
    sa = requests.get(base + f"/api/status?job_id={a}").json()
    sb = requests.get(base + f"/api/status?job_id={b}").json()
    assert sa["job"]["job_id"] == a and len(sa["session_leads"]) == 3
    assert sb["job"]["job_id"] == b and sb["session_leads"] == [] and sb["job"]["status"] == "waiting"
    # A dashboard with no search of its own sees none
    assert requests.get(base + "/api/status?job_id=").json()["job"]["status"] == "idle"
    # An older extension (no job_id) feeds the newest waiting search, as before
    r = requests.post(base + "/api/ingest", json={"page_url": "test", "data": _page([4])}).json()
    assert r["job_id"] == b
    assert _wait(lambda: requests.get(base + f"/api/ingest/state?job_id={b}").json()["processed"] == 1)
    # Stopping A leaves B running
    requests.post(base + f"/api/stop?job_id={a}")
    assert requests.get(base + f"/api/ingest/state?job_id={a}").json()["waiting"] is False
    assert requests.get(base + f"/api/ingest/state?job_id={b}").json()["waiting"] is True
    # Results for a stopped or unknown search are refused, not mixed into another one
    assert requests.post(base + "/api/ingest", json={"job_id": a, "data": _page([9])}).json()["waiting"] is False
    assert requests.post(base + "/api/ingest", json={"job_id": "job-nope", "data": _page([9])}).json()["waiting"] is False
    requests.post(base + f"/api/stop?job_id={b}")
    with sqlite3.connect(os.path.join(data_dir, "sales_nav_leads.db")) as conn:
        assert conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0] == 4


def test_relaunch_replaces_own_waiting_search(hosted):
    base, _ = hosted
    first = _prepare(base, "QA Engineer")
    second = _prepare(base, "QA Engineer", replace=first)
    assert requests.get(base + f"/api/ingest/state?job_id={first}").json()["status"] == "stopped"
    assert requests.get(base + f"/api/ingest/state?job_id={second}").json()["status"] == "waiting"
    requests.post(base + f"/api/stop?job_id={second}")


def test_active_search_cap(hosted):
    base, _ = hosted
    ids = [_prepare(base, f"Role {i}") for i in range(5)]
    r = requests.post(base + "/api/search/prepare", json={"role_titles": [{"text": "One too many"}]}).json()
    assert "already running" in r.get("error", "")
    for i in ids:
        requests.post(base + f"/api/stop?job_id={i}")


def test_concurrent_exports(hosted):
    base, _ = hosted
    out = {}

    def get(fmt):
        out[fmt] = requests.get(base + f"/api/export-csv?export_all=true&format={fmt}")
    threads = [threading.Thread(target=get, args=(f,)) for f in ("csv", "xlsx")]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert out["csv"].status_code == 200 and out["csv"].text.lstrip("﻿").startswith("Candidate Name")
    assert out["xlsx"].status_code == 200 and out["xlsx"].content[:2] == b"PK"
    leftovers = [f for f in os.listdir(tempfile.gettempdir()) if f.startswith("talentpulse_export_")]
    assert _wait(lambda: not [f for f in os.listdir(tempfile.gettempdir())
                              if f.startswith("talentpulse_export_") and f in leftovers], 5)


def test_hosted_switches(hosted):
    base, data_dir = hosted
    assert requests.get(base + "/docs").status_code == 404
    r = requests.post(base + "/api/start", json={"role": "x", "dry_run": True}).json()
    assert "turned off" in r.get("error", "")
    assert not os.path.exists(os.path.join(data_dir, "ingest_debug.jsonl"))
    big = requests.post(base + "/api/ingest", data=b"x" * (6 * 1024 * 1024), headers={"Content-Type": "application/json"})
    assert big.status_code == 413
    assert requests.post(base + "/api/leads/refresh", json={"linkedin_url": "x", "action": "drop"}).json()["error"]


def test_events_stream_starts(hosted):
    base, _ = hosted
    with requests.get(base + "/api/events", stream=True, timeout=5) as r:
        first = next(r.iter_lines(decode_unicode=True))
    assert first.startswith("data:") and '"init"' in first


def test_dry_run_never_touches_real_database(local):
    base, data_dir = local
    db = os.path.join(data_dir, "sales_nav_leads.db")
    requests.get(base + "/api/status")
    with sqlite3.connect(db) as conn:
        before = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    r = requests.post(base + "/api/start", json={"role": "DevOps Engineer", "dry_run": True, "target_leads": 25}).json()
    assert r.get("job_id"), r
    assert _wait(lambda: requests.get(base + "/api/status").json()["job"]["status"] in ("completed", "error", "stopped"), 30)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0] == before
    assert requests.get(base + "/docs").status_code == 200  # local mode keeps the API explorer
