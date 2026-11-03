import sqlite3
#!/usr/bin/env python3
"""
TalentPulse AI - Real-Time Production SaaS Dashboard Server
===========================================================
Production-grade FastAPI application providing:
1. Real-time extraction control (Start, Stop Safely, Dry-Run)
2. Server-Sent Events (SSE) streaming live logs and lead discoveries
3. Enterprise SaaS dashboard (Linear/Stripe-inspired, warm neutral theme, rich Lead Explorer)
4. Formula-injection sanitized CSV & JSON export
"""

import os
import json
import time
import queue
import logging
import threading
from contextlib import closing
import tempfile
import uuid
from dataclasses import asdict
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, BackgroundTasks, Query
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse, JSONResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel

from sales_nav_extractor import (
    SalesNavExtractor,
    SalesNavURLParser,
    SearchFilterParams,
    LeadDatabase,
    LeadRecord,
    DataCleaner,
    EMAIL_STATUS_PROFILE,
    load_credentials,
    create_sales_nav_session,
    DEFAULT_MAX_DAILY_LEADS,
    SENIORITY_ID_MAP,
)
import job_titles
import locations

# ---------------------------------------------------------------------------
# Server State & Event Broadcasting
# ---------------------------------------------------------------------------
# Hosting: TALENTPULSE_HOSTED=1 on a cloud host turns off local-only helpers (dry runs, the raw
# payload log and the /docs API explorer). Candidate data lives in TALENTPULSE_DATA_DIR, which should be
# a persistent disk on the host; locally it is this folder, as before.
HOSTED = os.environ.get("TALENTPULSE_HOSTED", "") == "1"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("TALENTPULSE_DATA_DIR") or BASE_DIR
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "sales_nav_leads.db")
CSV_PATH = os.path.join(DATA_DIR, "sales_nav_leads.csv")
INGEST_DEBUG_FILE = os.path.join(DATA_DIR, "ingest_debug.jsonl")
INGEST_DEBUG = os.environ.get("TALENTPULSE_DEBUG_INGEST", "0" if HOSTED else "1") == "1"
INGEST_DEBUG_MAX_BYTES = 5 * 1024 * 1024  # then the log starts over (one older copy is kept)

app = FastAPI(title="TalentPulse AI Dashboard", version="2.2.0",
              docs_url=None if HOSTED else "/docs", redoc_url=None if HOSTED else "/redoc",
              openapi_url=None if HOSTED else "/openapi.json")


class RequestLimits:
    """Caps POST body size and how often one address can POST, so an open testing URL can't be flooded."""
    MAX_BODY_BYTES = 5 * 1024 * 1024
    PER_MINUTE = int(os.environ.get("TALENTPULSE_POST_LIMIT_PER_MIN", "240"))

    def __init__(self, asgi_app):
        self.app = asgi_app
        self.hits: Dict[str, List[float]] = {}
        self.lock = threading.Lock()

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["method"] == "POST":
            headers = dict(scope.get("headers") or [])
            try:
                size = int(headers.get(b"content-length", b"0") or 0)
            except ValueError:
                size = 0
            if size > self.MAX_BODY_BYTES:
                await JSONResponse({"error": "Request too large."}, status_code=413)(scope, receive, send)
                return
            # Behind the host's proxy the caller's address is the first X-Forwarded-For entry
            fwd = headers.get(b"x-forwarded-for", b"").decode("latin-1").split(",")[0].strip()
            ip = fwd or (scope.get("client") or ("?",))[0]
            now = time.time()
            with self.lock:
                recent = [t for t in self.hits.get(ip, []) if now - t < 60]
                recent.append(now)
                self.hits[ip] = recent
                if len(self.hits) > 5000:
                    self.hits = {k: v for k, v in self.hits.items() if v and now - v[-1] < 60}
            if len(recent) > self.PER_MINUTE:
                await JSONResponse({"error": "Too many requests. Wait a minute and try again."},
                                   status_code=429)(scope, receive, send)
                return
        await self.app(scope, receive, send)


app.add_middleware(RequestLimits)

event_subscribers: List[queue.Queue] = []
subscribers_lock = threading.Lock()

# Thread-safe in-memory job state & log history
current_job = {
    "job_id": None,
    "status": "idle",      # idle, running, stopping, completed, error
    "mode": "live",        # live, dry_run
    "total_processed": 0,
    "qualified": 0,
    "current_page": 0,
    "max_pages": 1,
    "error_message": None,
    "started_at": None,
    "completed_at": None,
    "search_description": None,
}

recent_jobs: List[Dict[str, Any]] = []
current_session_leads: List[Dict[str, Any]] = []  # Session-isolated candidates for the active search
activity_logs: List[Dict[str, Any]] = []  # Log entries: {time, level, message}
MAX_LOGS = 200

job_lock = threading.Lock()
stop_flag = threading.Event()

# The server never calls LinkedIn itself: LinkedIn ends sessions whose cookies are reused outside the
# browser. Live data arrives from the TalentPulse browser extension (/api/ingest) instead.
SERVER_LINKEDIN_CALLS = os.environ.get("TALENTPULSE_SERVER_LINKEDIN_CALLS", "") == "1"
# Search waiting for the extension: filters used by the qualification gates
pending_search: Dict[str, Any] = {}

# Each extension search keeps its own state, so testers searching at the same time don't overwrite each
# other. current_job / pending_search / current_session_leads above stay for the server-side worker and
# for requests that name no search (older extension or dashboard).
ext_jobs: Dict[str, Dict[str, Any]] = {}  # job_id -> {"job", "pending", "leads", "stop"}
MAX_ACTIVE_SEARCHES = int(os.environ.get("TALENTPULSE_MAX_ACTIVE_SEARCHES", "5"))
SEARCH_IDLE_TIMEOUT = 30 * 60  # a search that hears nothing from its browser for 30 min is closed
IDLE_JOB = dict(current_job)


def _new_slot(job: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"job": job or dict(IDLE_JOB), "pending": {}, "leads": [], "stop": threading.Event()}


def _slot(job_id: Optional[str]) -> Dict[str, Any]:
    """The search a request belongs to (call with job_lock held). Without a job id: the newest search
    still waiting or running, as before searches were kept apart. An unknown id gets an idle search."""
    if job_id:
        return ext_jobs.get(job_id) or _new_slot(dict(IDLE_JOB, job_id=job_id))
    active = [s for s in ext_jobs.values() if s["job"]["status"] in ("waiting", "running")]
    if active:
        return active[-1]
    if ext_jobs:
        return list(ext_jobs.values())[-1]
    return {"job": current_job, "pending": pending_search, "leads": current_session_leads, "stop": stop_flag}


def _expire_idle_searches() -> None:
    """Closes searches whose browser went quiet and keeps the newest 50 (call with job_lock held)."""
    now = time.time()
    for s in ext_jobs.values():
        p = s["pending"]
        if s["job"]["status"] in ("waiting", "running") and now - p.get("touched", p.get("started", now)) > SEARCH_IDLE_TIMEOUT:
            s["job"].update(status="stopped", completed_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                            error_message="Closed: no results arrived from the browser for 30 minutes.")
            p.clear()
    for old in list(ext_jobs)[:-50]:
        if ext_jobs[old]["job"]["status"] not in ("waiting", "running"):
            del ext_jobs[old]


def add_activity_log(level: str, message: str) -> None:
    entry = {
        "time": time.strftime("%H:%M:%S"),
        "level": level,  # info, success, warning, error
        "message": message,
    }
    with job_lock:
        activity_logs.append(entry)
        if len(activity_logs) > MAX_LOGS:
            activity_logs.pop(0)
    broadcast_event("activity_log", entry)


def broadcast_event(event_type: str, data: Dict[str, Any]) -> None:
    message = json.dumps({"type": event_type, "data": data})
    with subscribers_lock:
        dead = []
        for q in event_subscribers:
            try:
                q.put_nowait(message)
            except Exception:
                dead.append(q)
        for q in dead:
            event_subscribers.remove(q)


# ---------------------------------------------------------------------------
# Background Extraction Worker
# ---------------------------------------------------------------------------
def run_extraction_worker(
    filters: SearchFilterParams,
    max_pages: int,
    dry_run: bool,
    db_path: str,
    csv_path: str,
    job_id: str,
    search_desc: str,
) -> None:
    stop_flag.clear()
    start_time = time.time()

    with job_lock:
        current_session_leads.clear()
        current_job.update({
            "job_id": job_id,
            "status": "running",
            "mode": "dry_run" if dry_run else "live",
            "total_processed": 0,
            "qualified": 0,
            "current_page": 0,
            "max_pages": max_pages,
            "error_message": None,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "completed_at": None,
            "search_description": search_desc,
        })

    broadcast_event("job_status", current_job)
    add_activity_log("info", f"Initiated extraction job [{job_id}] — {search_desc} (Pages: {max_pages}, Mode: {'Dry Run' if dry_run else 'Live'})")

    db = LeadDatabase(db_path=db_path)
    session = None

    if not dry_run:
        li_at, jsessionid = load_credentials()
        if not li_at or not jsessionid:
            msg = "Missing LI_AT or JSESSIONID cookies in .env configuration."
            with job_lock:
                current_job["status"] = "error"
                current_job["error_message"] = msg
                current_job["completed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            broadcast_event("job_status", current_job)
            add_activity_log("error", msg)
            return

        try:
            session = create_sales_nav_session(li_at, jsessionid)
            add_activity_log("success", "Authenticated LinkedIn Sales Navigator session established.")
        except Exception as e:
            msg = f"Failed to initialize LinkedIn session: {e}"
            with job_lock:
                current_job["status"] = "error"
                current_job["error_message"] = msg
                current_job["completed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            broadcast_event("job_status", current_job)
            add_activity_log("error", msg)
            return
    else:
        add_activity_log("info", "Executing offline mock payload simulation (no LinkedIn cookies required).")

    # Rolling 24-hour account safety quota (dry runs never touch LinkedIn)
    quota_remaining = DEFAULT_MAX_DAILY_LEADS
    if not dry_run:
        quota_remaining = DEFAULT_MAX_DAILY_LEADS - db.count_extracted_since()
        if quota_remaining <= 0:
            msg = f"Daily quota reached ({DEFAULT_MAX_DAILY_LEADS:,} leads in the last 24 hours). Try again later."
            with job_lock:
                current_job["status"] = "error"
                current_job["error_message"] = msg
                current_job["completed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            broadcast_event("job_status", current_job)
            add_activity_log("error", msg)
            return
        add_activity_log("info", f"Daily quota: {quota_remaining:,} of {DEFAULT_MAX_DAILY_LEADS:,} leads remaining in the last 24 hours.")

    extractor = SalesNavExtractor(
        session=session,
        db=db,
        max_daily_leads=quota_remaining,
        enable_jitter=not dry_run,
        dry_run=dry_run,
    )

    def on_lead_discovered(record: LeadRecord, progress: Dict[str, Any]) -> None:
        with job_lock:
            current_job["total_processed"] = progress["processed"]
            current_job["qualified"] = progress["qualified"]
            current_job["current_page"] = progress["page"]

        lead_data = {
            "linkedin_url": record.linkedin_url,
            "first_name": record.first_name,
            "last_name": record.last_name,
            "full_name": record.full_name,
            "current_title": record.current_title,
            "company_name": record.company_name,
            "location": record.location,
            "is_open_to_work": record.is_open_to_work,
            "is_qualified": record.is_qualified,
            "no_match_reasons": record.no_match_reasons,
            "discovered_at": record.discovered_at,
            "phone": record.phone,
            "email": record.email,
            "contact_source": record.contact_source,
            "contact_verified": record.contact_verified,
            "skills": record.skills,
            "otw_status": record.otw_status,
            "signal_type": record.signal_type,
            "signal_description": record.signal_description,
            "signal_source_url": record.signal_source_url,
            "signal_date": record.signal_date,
            "qualification_status": record.qualification_status,
            "qualification_reason": record.qualification_reason,
            "target_role_match": record.target_role_match,
            "data_source": record.data_source,
            "updated_at": record.updated_at,
            "job_id": record.job_id or job_id,
            "search_role": record.search_role or (filters.role if filters else None),
            "years_experience": record.years_experience,
            "email_status": record.email_status,
        }

        with job_lock:
            current_session_leads.append(lead_data)

        broadcast_event("lead", {
            "lead": lead_data,
            "job": current_job,
            "db_total": progress["db_total"],
            "session_leads_count": len(current_session_leads),
        })

        status_tag = "QUALIFIED" if record.is_qualified else f"FILTERED ({'; '.join(record.no_match_reasons[:1]) or 'Gatekeeper'})"
        add_activity_log("success" if record.is_qualified else "info", f"Extracted: {record.full_name} ({record.current_title} @ {record.company_name}) -> {status_tag}")

    try:
        processed, qualified = extractor.run(
            filters=filters,
            max_pages=max_pages,
            lead_callback=on_lead_discovered,
            job_id=job_id,
            should_stop=stop_flag.is_set,
        )
        db.export_to_csv(csv_path=csv_path)

        duration_sec = round(time.time() - start_time, 1)
        completed_time = time.strftime("%Y-%m-%d %H:%M:%S")

        with job_lock:
            final_status = "stopped" if stop_flag.is_set() else "completed"
            current_job["status"] = final_status
            current_job["total_processed"] = processed
            current_job["qualified"] = qualified
            current_job["completed_at"] = completed_time

            job_summary = {
                "job_id": job_id,
                "search_description": search_desc,
                "status": final_status,
                "started_at": current_job["started_at"],
                "duration": f"{duration_sec}s",
                "total_processed": processed,
                "qualified": qualified,
                "disqualified": processed - qualified,
            }
            recent_jobs.insert(0, job_summary)
            if len(recent_jobs) > 10:
                recent_jobs.pop()

        broadcast_event("job_status", current_job)
        add_activity_log("success", f"Job [{job_id}] finished in {duration_sec}s. Processed {processed} candidates ({qualified} qualified, {processed - qualified} filtered).")

    except Exception as e:
        completed_time = time.strftime("%Y-%m-%d %H:%M:%S")
        with job_lock:
            current_job["status"] = "error"
            current_job["error_message"] = str(e)
            current_job["completed_at"] = completed_time
            recent_jobs.insert(0, {
                "job_id": job_id,
                "search_description": search_desc,
                "status": "error",
                "started_at": current_job["started_at"],
                "duration": f"{round(time.time() - start_time, 1)}s",
                "total_processed": current_job["total_processed"],
                "qualified": current_job["qualified"],
                "disqualified": current_job["total_processed"] - current_job["qualified"],
            })
        broadcast_event("job_status", current_job)
        add_activity_log("error", f"Extraction error in job [{job_id}]: {e}")


# ---------------------------------------------------------------------------
# Comprehensive Global Location & LinkedIn Sales Navigator Geo Directory
# ---------------------------------------------------------------------------
WORLD_GEO_DB = os.path.join(BASE_DIR, "world_geo.db")

LINKEDIN_GEO_DIRECTORY = [
    # Top Countries
    {"id": "102713980", "name": "India", "type": "Country", "country": "India"},
    {"id": "103644278", "name": "United States", "type": "Country", "country": "United States"},
    {"id": "101165590", "name": "United Kingdom", "type": "Country", "country": "United Kingdom"},
    {"id": "101174742", "name": "Canada", "type": "Country", "country": "Canada"},
    {"id": "101452733", "name": "Australia", "type": "Country", "country": "Australia"},
    {"id": "101282230", "name": "Germany", "type": "Country", "country": "Germany"},
    {"id": "102454443", "name": "Singapore", "type": "Country", "country": "Singapore"},
    {"id": "104305787", "name": "United Arab Emirates", "type": "Country", "country": "United Arab Emirates"},

    # Tamil Nadu & South India
    {"id": "103135898", "name": "Coimbatore, Tamil Nadu, India", "type": "City", "country": "India"},
    {"id": "106832467", "name": "Chennai, Tamil Nadu, India", "type": "Metro Area", "country": "India"},
    {"id": "105658603", "name": "Madurai, Tamil Nadu, India", "type": "City", "country": "India"},
    {"id": "101831818", "name": "Tiruchirappalli, Tamil Nadu, India", "type": "City", "country": "India"},
    {"id": "104085465", "name": "Salem, Tamil Nadu, India", "type": "City", "country": "India"},
    {"id": "105214831", "name": "Bengaluru, Karnataka, India", "type": "Metro Area", "country": "India"},
    {"id": "100998375", "name": "Mysuru, Karnataka, India", "type": "City", "country": "India"},
    {"id": "105556991", "name": "Hyderabad, Telangana, India", "type": "Metro Area", "country": "India"},
    {"id": "103099905", "name": "Kochi, Kerala, India", "type": "City", "country": "India"},
    {"id": "101861214", "name": "Thiruvananthapuram, Kerala, India", "type": "City", "country": "India"},
    {"id": "101073846", "name": "Kozhikode, Kerala, India", "type": "City", "country": "India"},
    {"id": "103445831", "name": "Visakhapatnam, Andhra Pradesh, India", "type": "City", "country": "India"},
    {"id": "101036814", "name": "Vijayawada, Andhra Pradesh, India", "type": "City", "country": "India"},

    # North, West & Central India Tech Hubs
    {"id": "106155005", "name": "Mumbai, Maharashtra, India", "type": "Metro Area", "country": "India"},
    {"id": "103503525", "name": "Pune, Maharashtra, India", "type": "Metro Area", "country": "India"},
    {"id": "104113949", "name": "Delhi, India", "type": "Metro Area", "country": "India"},
    {"id": "106888324", "name": "Noida, Uttar Pradesh, India", "type": "City", "country": "India"},
    {"id": "104869687", "name": "Gurugram, Haryana, India", "type": "City", "country": "India"},
    {"id": "103856277", "name": "Ahmedabad, Gujarat, India", "type": "Metro Area", "country": "India"},
    {"id": "101443685", "name": "Surat, Gujarat, India", "type": "City", "country": "India"},
    {"id": "104621945", "name": "Vadodara, Gujarat, India", "type": "City", "country": "India"},
    {"id": "103671728", "name": "Jaipur, Rajasthan, India", "type": "City", "country": "India"},
    {"id": "104910382", "name": "Kolkata, West Bengal, India", "type": "Metro Area", "country": "India"},
    {"id": "104153106", "name": "Indore, Madhya Pradesh, India", "type": "City", "country": "India"},
    {"id": "103986016", "name": "Chandigarh, India", "type": "Metro Area", "country": "India"},
    {"id": "104768315", "name": "Lucknow, Uttar Pradesh, India", "type": "City", "country": "India"},
    {"id": "104845422", "name": "Nagpur, Maharashtra, India", "type": "City", "country": "India"},
    {"id": "101676673", "name": "Bhubaneswar, Odisha, India", "type": "City", "country": "India"},

    # Top US & Americas
    {"id": "90000084", "name": "San Francisco Bay Area", "type": "Metro Area", "country": "United States"},
    {"id": "90000070", "name": "Greater New York City Area", "type": "Metro Area", "country": "United States"},
    {"id": "90000091", "name": "Greater Seattle Area", "type": "Metro Area", "country": "United States"},
    {"id": "90000049", "name": "Austin, Texas Metropolitan Area", "type": "Metro Area", "country": "United States"},
    {"id": "90000052", "name": "Greater Boston", "type": "Metro Area", "country": "United States"},
    {"id": "90000045", "name": "Greater Los Angeles Area", "type": "Metro Area", "country": "United States"},
    {"id": "90000047", "name": "Greater Chicago Area", "type": "Metro Area", "country": "United States"},
    {"id": "90000087", "name": "Dallas-Fort Worth Metroplex", "type": "Metro Area", "country": "United States"},
    {"id": "90000078", "name": "Greater Toronto Area, Canada", "type": "Metro Area", "country": "Canada"},
    {"id": "90000081", "name": "Greater Vancouver Area, Canada", "type": "Metro Area", "country": "Canada"},

    # Top Europe & Global
    {"id": "90009495", "name": "London Area, United Kingdom", "type": "Metro Area", "country": "United Kingdom"},
    {"id": "90009699", "name": "Berlin Metropolitan Area, Germany", "type": "Metro Area", "country": "Germany"},
    {"id": "90009765", "name": "Munich Metropolitan Area, Germany", "type": "Metro Area", "country": "Germany"},
    {"id": "90009581", "name": "Amsterdam Area, Netherlands", "type": "Metro Area", "country": "Netherlands"},
    {"id": "90009569", "name": "Paris Area, France", "type": "Metro Area", "country": "France"},
    {"id": "90009575", "name": "Greater Dublin Area, Ireland", "type": "Metro Area", "country": "Ireland"},
    {"id": "90010467", "name": "Dubai, United Arab Emirates", "type": "City", "country": "United Arab Emirates"},
    {"id": "90009800", "name": "Greater Sydney Area, Australia", "type": "Metro Area", "country": "Australia"},
]


@app.get("/api/geo/search")
def search_geo(q: str = Query(..., min_length=1)):
    """
    Sub-millisecond global location search across all 158,000+ world cities,
    states/provinces, countries, and LinkedIn Sales Navigator Geo regions.
    """
    import sqlite3
    query = q.strip().replace('"', '""')
    if not query:
        return {"results": []}

    results = []
    seen = set()

    # LinkedIn's own location list first (exact geo IDs) when a Sales Navigator session is set
    for r in _linkedin_typeahead("BING_GEO", q.strip()[:80]):
        parts = [p.strip() for p in r["name"].split(",")]
        seen.add((r["name"].lower(), "linkedin"))
        results.append({"id": r["id"], "name": r["name"], "type": "LinkedIn location",
                        "country": parts[-1] if parts else "", "has_geo_id": bool(r["id"])})

    # Query full-text indexed world database if available
    if os.path.exists(WORLD_GEO_DB):
        try:
            with sqlite3.connect(WORLD_GEO_DB) as conn:
                cur = conn.cursor()
                # Clean query term for FTS5 prefix matching
                clean_term = "".join(c for c in query if c.isalnum() or c.isspace()).strip()
                if clean_term:
                    words = clean_term.split()
                    fts_query = " ".join(f"{w}*" for w in words)
                    cur.execute(
                        """
                        SELECT DISTINCT name, type, country, geo_id,
                               CASE 
                                   WHEN geo_id IS NOT NULL THEN 0
                                   WHEN lower(name) LIKE ? AND type = 'Country' THEN 1
                                   WHEN lower(name) LIKE ? AND type = 'State / Province' THEN 2
                                   WHEN lower(name) LIKE ? THEN 3
                                   WHEN type = 'Country' THEN 4
                                   WHEN type = 'State / Province' THEN 5
                                   WHEN type = 'Metro Area' THEN 6
                                   ELSE 7
                               END as rank_order
                        FROM locations_fts
                        WHERE locations_fts MATCH ?
                        ORDER BY rank_order ASC, length(name) ASC
                        LIMIT 15
                        """,
                        (f"{clean_term.lower()}%", f"{clean_term.lower()}%", f"{clean_term.lower()}%", fts_query)
                    )
                    for row in cur.fetchall():
                        name, l_type, country, geo_id, _ = row
                        key = (name.lower(), l_type.lower())
                        if key not in seen:
                            seen.add(key)
                            results.append({
                                "id": geo_id or "",
                                "name": name,
                                "type": l_type,
                                "country": country,
                                "has_geo_id": bool(geo_id),
                            })
        except Exception as e:
            logging.error(f"Error querying world_geo.db: {e}")

    # Fallback to curated directory if DB returned fewer results
    if len(results) < 8:
        q_lower = query.lower()
        for item in LINKEDIN_GEO_DIRECTORY:
            if q_lower in item["name"].lower():
                key = (item["name"].lower(), item["type"].lower())
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "id": item["id"],
                        "name": item["name"],
                        "type": item["type"],
                        "country": item["country"],
                        "has_geo_id": True,
                    })

    return {"results": _polish_geo_results(query, results)}


def _directory_entry(name: str) -> Optional[Dict[str, str]]:
    return next((d for d in LINKEDIN_GEO_DIRECTORY if d["name"].lower() == name.lower()), None)


def _polish_geo_results(query: str, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    LinkedIn-style list: everyday names first (Bangalore -> Bengaluru), LinkedIn places before
    look-alikes, no duplicate 'Hyderabad, India' under 'Hyderabad, Telangana, India', and each
    place without its own LinkedIn ID says which region the search will use.
    """
    out: List[Dict[str, Any]] = []
    for name in locations.alias_suggestions(query):
        d = _directory_entry(name)
        if d:
            out.append({"id": d["id"], "name": d["name"], "type": d["type"], "country": d["country"], "has_geo_id": True})
    out += [r for r in results if r["has_geo_id"]] + [r for r in results if not r["has_geo_id"]]

    final: List[Dict[str, Any]] = []
    linked_cities = set()
    for r in out:
        city = r["name"].split(",")[0].strip().lower()
        if any(f["name"].lower() == r["name"].lower() for f in final):
            continue
        if not r["has_geo_id"] and (city, r["country"].lower()) in linked_cities:
            continue  # 'Hyderabad, India' duplicates the LinkedIn 'Hyderabad, Telangana, India'
        if r["has_geo_id"]:
            linked_cities.add((city, r["country"].lower()))
            r["search_as"] = ""
        else:
            res = locations.resolve(r["name"], None, LINKEDIN_GEO_DIRECTORY)
            r["id"], r["has_geo_id"] = res["id"], bool(res["id"]) and res["exact"]
            r["search_as"] = "" if res["exact"] else (res["name"] if res["id"] else "")
        final.append(r)
    return final[:10]


# ---------------------------------------------------------------------------
# Job-title typeahead (LinkedIn's own title list when logged in, catalogue otherwise)
# ---------------------------------------------------------------------------
LINKEDIN_TITLE_TYPEAHEAD = "https://www.linkedin.com/sales-api/salesApiFacetTypeahead"
_title_cache: Dict[str, List[Dict[str, Any]]] = {}
_title_live = {"session": None, "disabled_until": 0.0}


def _linkedin_title_suggestions(q: str) -> List[Dict[str, Any]]:
    """LinkedIn standardized titles with ids. Returns [] on any problem (caller falls back)."""
    return _linkedin_typeahead("TITLE", q)


def _linkedin_typeahead(facet: str, q: str) -> List[Dict[str, Any]]:
    """Sales Navigator's own typeahead (TITLE, BING_GEO). Returns [] on any problem."""
    if not SERVER_LINKEDIN_CALLS:
        return []  # suggestions come from the offline catalogues
    if time.time() < _title_live["disabled_until"]:
        return []
    key = f"{facet}:{q.lower()}"
    if key in _title_cache:
        return _title_cache[key]
    try:
        if _title_live["session"] is None:
            li_at, jsession = load_credentials()
            if not li_at or not jsession:
                _title_live["disabled_until"] = time.time() + 300
                return []
            _title_live["session"] = create_sales_nav_session(li_at, jsession)
        resp = _title_live["session"].get(
            LINKEDIN_TITLE_TYPEAHEAD,
            params={"q": "query", "type": facet, "query": q, "count": 10, "start": 0},
            timeout=4,
        )
        if resp.status_code != 200:
            # 401/403/429/999: stop asking LinkedIn for a while, use the catalogue
            _title_live["disabled_until"] = time.time() + 300
            return []
        data = resp.json()
        elements = data.get("elements") or data.get("data", {}).get("elements") or []
        out = []
        for el in elements:
            name = el.get("displayValue") or el.get("text") or el.get("name")
            tid = str(el.get("id") or el.get("value") or "").rsplit(":", 1)[-1]
            if name:
                out.append({"name": name, "id": tid if tid.isdigit() else "", "matched": "",
                            "aliases": [], "source": "linkedin"})
        _title_cache[key] = out
        return out
    except Exception as e:
        logging.info(f"LinkedIn {facet} typeahead unavailable, using offline list: {e}")
        _title_live["disabled_until"] = time.time() + 300
        return []


@app.get("/api/titles/search")
def search_titles(q: str = Query(..., min_length=1)):
    """Typeahead for the Role box: standard job titles, best match first."""
    q = q.strip()[:80]
    if not q:
        return {"results": []}
    live = _linkedin_title_suggestions(q)
    seen = {r["name"].lower() for r in live}
    results = live + [r for r in job_titles.search(q, limit=10) if r["name"].lower() not in seen]
    return {"results": results[:10], "live": bool(live)}


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------
class StartJobRequest(BaseModel):
    url: Optional[str] = None
    role: Optional[str] = None
    role_titles: Optional[List[Dict[str, str]]] = None  # [{"text": ..., "id": ...}] from the typeahead
    location: Optional[str] = None
    locations: Optional[List[Dict[str, str]]] = None  # several places [{"text": ..., "id": ...}], OR'd
    geo_id: Optional[str] = None
    seniority_levels: Optional[List[str]] = None
    experience_ranges: Optional[List[str]] = None  # "<1", "1-2", "3-5", "6-10", "10+"
    keywords: Optional[str] = "Open to work"
    extra_keywords: Optional[List[str]] = None  # more keywords from the form
    keyword_mode: Optional[str] = "any"  # "any" (OR) or "all" (AND)
    experience: Optional[List[str]] = None  # Sales Navigator YEARS_OF_EXPERIENCE ids: 1 <1y, 2 1-2y, 3 3-5y, 4 6-10y, 5 10+y
    target_leads: Optional[int] = None
    pages: Optional[int] = None
    dry_run: bool = False
    replace_job_id: Optional[str] = None  # the dashboard's previous search, replaced if still waiting


class RefreshLeadRequest(BaseModel):
    linkedin_url: str
    action: Optional[str] = "verify"  # "verify", "mark_not_looking", "mark_open_to_work"


@app.get("/api/status")
def get_status(job_id: Optional[str] = None):
    db = LeadDatabase(DB_PATH)
    total_db = db.get_total_count()
    qualified_db = db.get_qualified_count()
    otw_db = 0
    with db._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM leads WHERE is_open_to_work = 1;")
        row = cursor.fetchone()
        otw_db = row[0] if row else 0

    with job_lock:
        # The dashboard asks for its own search; job_id="" means it has none yet
        slot = _new_slot() if job_id == "" else _slot(job_id)
        return {
            "job": slot["job"],
            "recent_jobs": recent_jobs,
            "activity_logs": activity_logs[-30:],
            "session_leads": list(slot["leads"]),
            "stats": {
                "db_total": total_db,
                "db_qualified": qualified_db,
                "db_disqualified": max(0, total_db - qualified_db),
                "db_otw": otw_db,
                "daily_cap": DEFAULT_MAX_DAILY_LEADS,
                "daily_used": db.count_extracted_since(),
            }
        }


@app.get("/api/leads")
def get_leads(
    status_filter: Optional[str] = "all",           # legacy support: all, qualified, disqualified, otw
    otw_status: Optional[str] = "all",              # all, explicit, possible, unknown
    qualification_status: Optional[str] = "all",    # all, qualified, rejected, needs_review
    contact_filter: Optional[str] = "all",          # all, has_phone, has_email, has_any
    location: Optional[str] = None,
    sort_by: Optional[str] = "date_desc",
    limit: int = 250,
    search: Optional[str] = None,
    job_id: Optional[str] = None,
    search_role: Optional[str] = None,
    experience: Optional[str] = None,                # 0-1, 1-3, 3-5, 5-10, 10+ years
):
    limit = max(1, min(int(limit or 250), 5000))
    db = LeadDatabase(DB_PATH)
    with db._get_connection() as conn:
        cursor = conn.cursor()
        query = (
            "SELECT linkedin_url, first_name, last_name, full_name, current_title, "
            "company_name, location, is_open_to_work, is_qualified, no_match_reasons, discovered_at, "
            "phone, email, contact_source, contact_verified, "
            "skills, otw_status, signal_type, signal_description, "
            "signal_source_url, signal_date, qualification_status, "
            "qualification_reason, target_role_match, data_source, updated_at, "
            "job_id, search_role, years_experience, email_status, headline, first_found_at "
            "FROM leads"
        )
        params: List[Any] = []
        conditions: List[str] = []

        if search:
            conditions.append(
                "(full_name LIKE ? OR company_name LIKE ? OR current_title LIKE ? OR location LIKE ? OR skills LIKE ?)"
            )
            term = f"%{search.strip()}%"
            params.extend([term, term, term, term, term])

        if location:
            conditions.append("location LIKE ?")
            params.append(f"%{location.strip()}%")

        exp_sql, exp_params = experience_condition(experience)
        if exp_sql:
            conditions.append(exp_sql)
            params.extend(exp_params)

        if job_id and job_id != "all":
            conditions.append("job_id = ?")
            params.append(job_id)

        if search_role and search_role != "all":
            conditions.append("(search_role LIKE ? OR current_title LIKE ? OR target_role_match LIKE ?)")
            term_role = f"%{search_role.strip()}%"
            params.extend([term_role, term_role, term_role])

        # Open to Work status filtering (Binary Yes/No)
        if otw_status in ("yes", "explicit"):
            conditions.append("(is_open_to_work = 1 OR otw_status IN ('Yes', 'Explicitly Indicated'))")
        elif otw_status in ("no", "unknown"):
            conditions.append("(is_open_to_work = 0 AND (otw_status IS NULL OR otw_status NOT IN ('Yes', 'Explicitly Indicated')))")
        elif otw_status == "possible":
            conditions.append("(is_open_to_work = 1 OR otw_status IN ('Yes', 'Possible Signal'))")

        # Qualification status filtering
        if qualification_status == "qualified":
            conditions.append("(qualification_status = 'Qualified' OR (qualification_status IS NULL AND is_qualified = 1))")
        elif qualification_status == "rejected":
            conditions.append("(qualification_status = 'Rejected' OR (qualification_status IS NULL AND is_qualified = 0))")
        elif qualification_status == "needs_review":
            conditions.append("qualification_status = 'Needs Review'")

        # Contact details filter
        if contact_filter == "has_phone":
            conditions.append("phone IS NOT NULL AND phone != ''")
        elif contact_filter == "has_email":
            conditions.append("email IS NOT NULL AND email != ''")
        elif contact_filter == "has_any":
            conditions.append("((phone IS NOT NULL AND phone != '') OR (email IS NOT NULL AND email != ''))")

        # Legacy filter backward compatibility
        if status_filter == "qualified" and qualification_status == "all":
            conditions.append("(is_qualified = 1 OR qualification_status = 'Qualified')")
        elif status_filter == "disqualified" and qualification_status == "all":
            conditions.append("(is_qualified = 0 OR qualification_status = 'Rejected')")
        elif status_filter == "otw" and otw_status == "all":
            conditions.append("(is_open_to_work = 1 OR otw_status = 'Explicitly Indicated')")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Sorting
        order_clause = "ORDER BY discovered_at DESC"
        if sort_by == "date_asc":
            order_clause = "ORDER BY discovered_at ASC"
        elif sort_by == "name_asc":
            order_clause = "ORDER BY full_name ASC"
        elif sort_by == "name_desc":
            order_clause = "ORDER BY full_name DESC"
        elif sort_by == "location_asc":
            order_clause = "ORDER BY location ASC"
        elif sort_by == "signal_date_desc":
            order_clause = "ORDER BY signal_date DESC, discovered_at DESC"
        elif sort_by == "experience_desc":
            order_clause = "ORDER BY years_experience IS NULL, years_experience DESC"
        elif sort_by == "experience_asc":
            order_clause = "ORDER BY years_experience IS NULL, years_experience ASC"

        query += f" {order_clause} LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        leads = [
            {
                "linkedin_url": r[0] or "",
                "first_name": r[1] or "",
                "last_name": r[2] or "",
                "full_name": r[3] or "Not available",
                "current_title": r[4] or "Not available",
                "company_name": r[5] or "Not available",
                "location": r[6] or "Not available",
                "is_open_to_work": bool(r[7]),
                "is_qualified": bool(r[8]),
                "no_match_reasons": r[9].split("; ") if r[9] else [],
                "discovered_at": r[10] or "",
                "phone": r[11] or None,
                "email": r[12] or None,
                "contact_source": r[13] or "Not available",
                "contact_verified": bool(r[14]),
                "skills": [s.strip() for s in r[15].split(";") if s.strip()] if r[15] else [],
                "otw_status": "Yes" if (r[16] in ("Yes", "Explicitly Indicated") or bool(r[7])) else "No",
                "signal_type": r[17] or ("Open to Work" if bool(r[7]) else "None"),
                "signal_description": r[18] or ("Explicit #OpenToWork indicator verified." if bool(r[7]) else "No job-seeking signals detected."),
                "signal_source_url": r[19] or r[0] or "",
                "signal_date": r[20] or "Not available",
                "qualification_status": r[21] or ("Qualified" if bool(r[8]) else "Rejected"),
                "qualification_reason": r[22] or ("Passed all qualification gates" if bool(r[8]) else (r[9] or "Filtered")),
                "target_role_match": r[23] or ("Role Criteria Satisfied" if bool(r[8]) else "Role Mismatch"),
                "data_source": r[24] or "LinkedIn Sales Navigator",
                "updated_at": r[25] or r[10] or "",
                "job_id": r[26] if len(r) > 26 and r[26] else "batch-init",
                "search_role": r[27] if len(r) > 27 and r[27] else "AI Engineer",
                "years_experience": r[28] if len(r) > 28 else None,
                "email_status": (r[29] if len(r) > 29 and r[29] else (EMAIL_STATUS_PROFILE if r[12] else "")),
                "headline": r[30] if len(r) > 30 and r[30] else "",
                "first_found_at": (r[31] if len(r) > 31 and r[31] else r[10]) or "",
            }
            for r in rows
        ]
        return {"leads": leads, "total": len(leads)}


@app.post("/api/start")
def start_extraction(req: StartJobRequest, background_tasks: BackgroundTasks):
    with job_lock:
        if current_job["status"] == "running":
            return {"error": "An extraction job is already in progress. Please stop it or wait for completion."}

    has_role = bool((req.role or "").strip()) or any((t.get("text") or "").strip() for t in (req.role_titles or []))
    if not req.url and not has_role:
        return {"error": "Enter the role or job title you are hiring for."}

    if not req.dry_run and not SERVER_LINKEDIN_CALLS:
        return {"error": "Live searches run through the TalentPulse browser extension. Use Launch search on the New Search page."}

    if req.url:
        filters = SalesNavURLParser.parse_url(req.url)
        if req.role and not filters.role:
            filters.role = req.role
        if req.location and not filters.location:
            filters.location = req.location
        if not filters.keywords:
            filters.keywords = "Open to work"
        search_desc = f"Sales Nav URL Search ({filters.role or 'All Roles'})"
    else:
        titles = [t for t in (req.role_titles or []) if (t.get("text") or "").strip()][:1]  # one title per search
        role = " | ".join(t["text"].strip() for t in titles) if titles else req.role
        filters = SearchFilterParams(
            role=role,
            role_titles=[{"text": t["text"].strip(), "id": str(t.get("id") or "")} for t in titles],
            location=req.location,
            geo_id=req.geo_id,
            seniority_levels=req.seniority_levels or [],
            # (location / geo_id / location_terms are resolved just below)
            experience_ranges=req.experience_ranges or [],
            keywords=req.keywords or "Open to work",
        )
        # LinkedIn filters location by geo ID: resolve the box to one (or to the nearest region
        # with an ID, then keep only leads actually in the requested place via Gate G)
        places = [p for p in (req.locations or []) if (p.get("text") or "").strip()][:10]
        if places:
            multi = locations.resolve_many(places, LINKEDIN_GEO_DIRECTORY)
            place_text = "; ".join(p["text"].strip() for p in places)
            filters.location = "; ".join(multi["names"]) or None
            filters.geo_id = None
            filters.regions = list(multi["regions"])
            filters.location_terms = list(multi["local_terms"])
        else:
            place_text = (req.location or "").strip()
            geo = locations.resolve(place_text, req.geo_id, LINKEDIN_GEO_DIRECTORY)
            filters.location = geo["name"] or None
            filters.geo_id = geo["id"] or None
            filters.location_terms = list(geo["local_terms"])
        sen_text = f" [{', '.join(req.seniority_levels)}]" if req.seniority_levels else ""
        exp_text = f" · {', '.join(req.experience_ranges)} yrs" if req.experience_ranges else ""
        search_desc = f"{filters.role or 'Candidates'}{sen_text} in {place_text or 'Global'}{exp_text}"

    # Calculate pages from target_leads (default 50 leads = 2 pages)
    if req.target_leads:
        calculated_pages = max(1, (req.target_leads + 24) // 25)
    elif req.pages:
        calculated_pages = max(1, req.pages)
    else:
        calculated_pages = 2  # default 50 leads

    job_id = f"job-{int(time.time() * 1000) % 1000000}"
    db_path, csv_path = DB_PATH, CSV_PATH
    if req.dry_run:
        if HOSTED:
            return {"error": "Dry runs are turned off on the hosted server."}
        # Mock candidates go to a throwaway database, never the real one
        tmp = tempfile.mkdtemp(prefix="talentpulse_dry_run_")
        db_path, csv_path = os.path.join(tmp, "dry_run.db"), os.path.join(tmp, "dry_run.csv")

    background_tasks.add_task(
        run_extraction_worker,
        filters=filters,
        max_pages=calculated_pages,
        dry_run=req.dry_run,
        db_path=db_path,
        csv_path=csv_path,
        job_id=job_id,
        search_desc=search_desc,
    )
    return {"message": "Extraction initiated successfully.", "job_id": job_id, "search_description": search_desc}


@app.post("/api/stop")
def stop_extraction(job_id: Optional[str] = None):
    with job_lock:
        slot = _slot(job_id)
        current_job, pending_search, stop_flag = slot["job"], slot["pending"], slot["stop"]
        if current_job["status"] == "waiting":
            current_job["status"] = "stopped"
            current_job["completed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            pending_search.clear()
            broadcast_event("job_status", current_job)
            return {"message": "Search cancelled."}
        if current_job["status"] not in ("running", "stopping"):
            return {"message": "No search is running."}
        stop_flag.set()
        if current_job.get("mode") == "extension":
            # Results arrive in batches from the browser; nothing is in progress between them, so stop now.
            # The extension sees the search is no longer waiting and closes its window.
            current_job["status"] = "stopped"
            current_job["completed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            pending_search.clear()
            collected = current_job["total_processed"]
        else:
            current_job["status"] = "stopping"
            collected = None
    broadcast_event("job_status", current_job)
    if collected is not None:
        add_activity_log("warning", f"Search stopped. {collected} people collected.")
        return {"message": "Search stopped."}
    add_activity_log("warning", "Stop signal received. Finishing the current page, then stopping.")
    return {"message": "Stop signal transmitted to extraction engine."}


@app.get("/api/batches")
def get_batches():
    db = LeadDatabase(DB_PATH)
    with db._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT job_id, MAX(search_role), COUNT(*), MAX(discovered_at) "
            "FROM leads "
            "WHERE job_id IS NOT NULL AND job_id != '' "
            "GROUP BY job_id ORDER BY MAX(discovered_at) DESC;"
        )
        batch_rows = cursor.fetchall()
        batches = [
            {
                "job_id": b[0],
                "search_role": b[1] or "Unspecified Role",
                "lead_count": b[2],
                "latest_date": b[3] or "",
            }
            for b in batch_rows
        ]

        cursor.execute(
            "SELECT DISTINCT search_role FROM leads "
            "WHERE search_role IS NOT NULL AND search_role != '' "
            "ORDER BY search_role ASC;"
        )
        role_rows = cursor.fetchall()
        roles = [r[0] for r in role_rows if r[0]]

    return {"batches": batches, "roles": roles}


# Column widths (characters) for the Excel export, by header
XLSX_WIDTHS = {"Candidate Name": 24, "LinkedIn Profile URL": 16, "Current Job Title": 34, "Headline": 60,
               "Current Company": 26, "Years of Experience": 11, "Location": 28, "Open-to-Work Status": 22,
               "Mobile Number": 18, "Email Address": 32, "Relevant Skills / Keywords": 36,
               "First Found (IST)": 21, "Last Seen (IST)": 21}


def _remove_files(*paths: str) -> None:
    for p in paths:
        try:
            os.remove(p)
        except OSError:
            pass


def _csv_to_xlsx(csv_path: str) -> str:
    """The same rows as the CSV, laid out for Excel: sized columns, bold frozen header, filters,
    clickable profile links, wrapped long text."""
    import csv as _csv
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter
    with open(csv_path, encoding="utf-8-sig", newline="") as fh:
        rows = list(_csv.reader(fh))
    wb = Workbook()
    ws = wb.active
    ws.title = "Candidates"
    if not rows:
        rows = [[]]
    headers = rows[0]
    ws.append(headers)
    for r in rows[1:]:
        out = []
        for h, v in zip(headers, r):
            v = v[1:] if v.startswith("'") and v[1:2] in ("=", "+", "-", "@") else v  # CSV-only formula guard
            if h == "Years of Experience" and v:
                try:
                    v = float(v)
                except ValueError:
                    pass
            elif h == "Headline" and len(v) > 300:
                v = v[:300].rsplit(" ", 1)[0] + "..."
            out.append(v)
        ws.append(out)
    head_fill = PatternFill("solid", fgColor="1F3A12")
    for col, h in enumerate(headers, 1):
        c = ws.cell(row=1, column=col)
        c.font = Font(bold=True, color="FFFFFF")
        c.fill = head_fill
        c.alignment = Alignment(vertical="center", wrap_text=True)
        ws.column_dimensions[get_column_letter(col)].width = XLSX_WIDTHS.get(h, 18)
    wrap_cols = {i for i, h in enumerate(headers, 1) if h in ("Headline", "Relevant Skills / Keywords", "Current Job Title")}
    url_col = headers.index("LinkedIn Profile URL") + 1 if "LinkedIn Profile URL" in headers else 0
    for row in ws.iter_rows(min_row=2):
        for c in row:
            c.alignment = Alignment(vertical="top", wrap_text=c.column in wrap_cols)
            if isinstance(c.value, str) and c.value[:1] in ("=", "+", "-", "@"):
                c.data_type = "s"  # always text, never a formula
        if url_col and row[url_col - 1].value:
            link = row[url_col - 1]
            link.hyperlink, link.value, link.font = link.value, "View profile", Font(color="0563C1", underline="single")
    ws.row_dimensions[1].height = 30
    ws.freeze_panes = "B2"
    ws.auto_filter.ref = ws.dimensions
    xlsx_path = csv_path[:-4] + ".xlsx"
    wb.save(xlsx_path)
    return xlsx_path


@app.get("/api/export-csv")
def export_csv(
    status_filter: Optional[str] = None,
    otw_status: Optional[str] = None,
    qualification_status: Optional[str] = None,
    contact_filter: Optional[str] = None,
    location: Optional[str] = None,
    search: Optional[str] = None,
    job_id: Optional[str] = None,
    search_role: Optional[str] = None,
    experience: Optional[str] = None,
    export_all: bool = False,
    format: str = "xlsx",                            # xlsx (formatted for Excel) or csv

):
    # Each export gets its own file, so two people exporting at once never get each other's rows
    csv_path = os.path.join(tempfile.gettempdir(), f"talentpulse_export_{uuid.uuid4().hex}.csv")
    db = LeadDatabase(DB_PATH)
    ts = int(time.time())

    if export_all:
        db.export_to_csv(csv_path, filters=None)
        filename = f"talentpulse_master_all_candidates_{ts}.csv"
    else:
        filters = {
            "status_filter": status_filter,
            "otw_status": otw_status,
            "qualification_status": qualification_status,
            "contact_filter": contact_filter,
            "location": location,
            "search": search,
            "job_id": job_id,
            "search_role": search_role,
            "experience": experience,
        }
        active_filters = {k: v for k, v in filters.items() if v and v != "all"}
        db.export_to_csv(csv_path, filters=active_filters if active_filters else None)
        if job_id and job_id != "all":
            clean_job = "".join(c for c in job_id if c.isalnum() or c in ("-", "_"))
            filename = f"talentpulse_batch_{clean_job}_{ts}.csv"
        elif search_role and search_role != "all":
            clean_role = "".join(c for c in search_role if c.isalnum() or c in ("-", "_")).lower()
            filename = f"talentpulse_{clean_role}_{ts}.csv"
        else:
            filename = f"talentpulse_candidates_filtered_{ts}.csv"

    cleanup = BackgroundTask(_remove_files, csv_path, csv_path[:-4] + ".xlsx")
    if os.path.exists(csv_path) and format != "csv":
        xlsx_path = _csv_to_xlsx(csv_path)
        return FileResponse(xlsx_path, filename=filename[:-4] + ".xlsx", background=cleanup,
                            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    if os.path.exists(csv_path):
        return FileResponse(
            csv_path,
            media_type="text/csv",
            filename=filename,
            background=cleanup,
        )
    return {"error": "CSV file could not be generated."}


@app.post("/api/leads/refresh")
def refresh_lead(req: RefreshLeadRequest):
    if req.action not in ("verify", "mark_not_looking", "mark_open_to_work"):
        return {"error": "Unknown action."}
    db = LeadDatabase(DB_PATH)
    normalized_url = DataCleaner.normalize_linkedin_url(req.linkedin_url)
    lead = db.get_lead(normalized_url)
    if not lead:
        lead = db.get_lead(req.linkedin_url)
        if not lead:
            return {"error": "Candidate record not found in database."}
        normalized_url = req.linkedin_url

    now = time.strftime("%Y-%m-%d %H:%M:%S")

    if req.action == "mark_not_looking":
        is_otw = False
        otw_status = "No"
        sig_type = "Recruiter Verified"
        sig_desc = "Recruiter confirmed candidate is no longer actively looking for opportunities (OTW: No)."
        db.update_lead_status(
            normalized_url,
            is_open_to_work=is_otw,
            otw_status=otw_status,
            signal_type=sig_type,
            signal_description=sig_desc,
            updated_at=now,
        )
        add_activity_log("info", f"Updated [{lead['full_name']}]: Marked Open to Work as No")
    elif req.action == "mark_open_to_work":
        is_otw = True
        otw_status = "Yes"
        sig_type = "Recruiter Verified"
        sig_desc = "Recruiter confirmed candidate is actively looking for new opportunities (OTW: Yes)."
        db.update_lead_status(
            normalized_url,
            is_open_to_work=is_otw,
            otw_status=otw_status,
            signal_type=sig_type,
            signal_description=sig_desc,
            updated_at=now,
        )
        add_activity_log("success", f"Updated [{lead['full_name']}]: Marked Open to Work as Yes")
    else:  # "verify"
        li_at, jsessionid = load_credentials() if SERVER_LINKEDIN_CALLS else ("", "")
        verified_live = False
        failure_reason = ("LinkedIn cookies are not configured in .env" if SERVER_LINKEDIN_CALLS else
                          "automatic LinkedIn checks are off to protect the account; open the profile on LinkedIn to check")
        if li_at and jsessionid:
            try:
                session = create_sales_nav_session(li_at, jsessionid)
                resp = session.get(normalized_url, timeout=15, headers={"accept": "text/html"})
                final_url = resp.url.lower()
                if resp.status_code != 200:
                    failure_reason = f"LinkedIn returned HTTP {resp.status_code}"
                elif any(marker in final_url for marker in ("authwall", "/login", "checkpoint")):
                    failure_reason = "LinkedIn session expired or blocked (redirected to login)"
                else:
                    verified_live = True
                    text_content = resp.text.lower()
                    # Only the #OpenToWork frame markers count; plain "open to work"
                    # text appears in LinkedIn page chrome and gives false positives.
                    has_otw = any(
                        marker in text_content
                        for marker in ("#opentowork", "open_to_work", "opentowork")
                    )
                    is_otw = bool(has_otw)
                    otw_status = "Yes" if has_otw else "No"
                    sig_type = "Live LinkedIn Inspection" if has_otw else "None"
                    sig_desc = (
                        "Verified active Open-to-Work badge on live LinkedIn profile."
                        if has_otw
                        else "Live re-verification confirmed candidate closed Open-to-Work status."
                    )
                    db.update_lead_status(
                        normalized_url,
                        is_open_to_work=is_otw,
                        otw_status=otw_status,
                        signal_type=sig_type,
                        signal_description=sig_desc,
                        updated_at=now,
                    )
                    add_activity_log("success", f"Live Verified [{lead['full_name']}]: OTW is {otw_status}")
            except Exception as e:
                failure_reason = f"Could not reach LinkedIn: {e}"

        if not verified_live:
            # Leave the stored record untouched; report why the live check failed
            add_activity_log("warning", f"Live check skipped for [{lead['full_name']}]: {failure_reason}.")
            return {
                "message": f"Live check not possible: {failure_reason}. Stored status unchanged.",
                "verified_live": False,
                "lead": lead,
            }

    updated_lead = db.get_lead(normalized_url)
    return {"message": "Candidate status refreshed successfully.", "verified_live": req.action == "verify", "lead": updated_lead}


SALESNAV_COUNTRY_IDS = {
    "india": "102713980", "united states": "103644278", "usa": "103644278", "united kingdom": "101165590",
    "uk": "101165590", "canada": "101174742", "australia": "101452733", "germany": "101282230",
    "united arab emirates": "104305776", "uae": "104305776", "singapore": "102454443", "france": "105015875",
    "netherlands": "102890719", "ireland": "104738515", "new zealand": "105490917", "saudi arabia": "100459316",
}


def _salesnav_search_url(role: Any, regions: List[Dict[str, str]], seniority: List[str], keywords: str,
                         exact: bool = False, experience: Optional[List[str]] = None, title_id: str = "") -> str:
    """Sales Navigator people-search link for the form, in Sales Navigator's own URL format."""
    from urllib.parse import quote
    v = lambda t: quote(t, safe="")  # value escaping inside the query
    # With LinkedIn's standard title id, related wordings match too (Senior DevOps Engineer, DevOps Engineer II...)
    # role: one title, or several [{"text", "id"}] that Sales Navigator ORs together
    titles = role if isinstance(role, list) else [{"text": role, "id": title_id}]
    tid = lambda t: f"id:{t['id']}," if str(t.get("id") or "").isdigit() else ""
    filters = ["(type:CURRENT_TITLE,values:List(" + ",".join(
        f"({tid(t)}text:{v(t['text'])},selectionType:INCLUDED)" for t in titles) + "))"]
    # City-level region codes don't always match Sales Navigator's, which then finds nobody. Search the
    # country with LinkedIn's standard country code instead; Gate G keeps only people in the chosen places.
    ids, seen = ([r for r in regions if str(r.get("id") or "").isdigit()] if exact else []), set()
    for r in ([] if exact else regions):
        country = (r.get("text") or "").split(",")[-1].strip().lower()
        cid = SALESNAV_COUNTRY_IDS.get(country)
        if cid and cid not in seen:
            seen.add(cid)
            ids.append({"id": cid, "text": country.title() if len(country) > 3 else country.upper()})
    if ids:
        filters.append("(type:REGION,values:List(" + ",".join(
            f"(id:{r['id']},text:{v(r.get('text', ''))},selectionType:INCLUDED)" for r in ids) + "))")
    sen_ids = [i for lvl in seniority for i in SENIORITY_ID_MAP.get(lvl.strip().lower().replace("-", "").replace(" ", ""), [])]
    if sen_ids:
        filters.append("(type:SENIORITY_LEVEL,values:List(" + ",".join(
            f"(id:{i},selectionType:INCLUDED)" for i in sen_ids) + "))")
    exp_ids = [e for e in (experience or []) if e in {"1", "2", "3", "4", "5"}]
    if exp_ids:
        filters.append("(type:YEARS_OF_EXPERIENCE,values:List(" + ",".join(
            f"(id:{e},selectionType:INCLUDED)" for e in exp_ids) + "))")
    query = f"(filters:List({','.join(filters)}),keywords:{v(keywords)})"
    return "https://www.linkedin.com/sales/search/people?query=" + quote(query, safe="(),:")


def _keyword_query(base: str, extra: List[str], mode: str) -> str:
    """Sales Navigator keyword text: the base words plus the form's keywords, in its Boolean syntax."""
    words = [" ".join(k.replace('"', " ").replace("(", " ").replace(")", " ").split()) for k in extra]
    words = list(dict.fromkeys(w for w in words if w))[:10]
    if not words:
        return base
    quoted = [f'"{w}"' if " " in w else w for w in words]
    joined = (" AND " if mode == "all" else " OR ").join(quoted)
    return f"{base} AND ({joined})" if base else joined


def _profile_key(url: str) -> str:
    """The person's LinkedIn member id from a profile link. Sales Navigator adds a code after it
    (",NAME_SEARCH,Gwu1") that changes between searches, so it is not used to tell people apart."""
    tail = (url or "").rstrip("/").split("/sales/lead/")[-1].split("/in/")[-1]
    return tail.split(",")[0].split("?")[0].lower()


PROGRESS_FILE = os.path.join(DATA_DIR, "search_progress.json")


def _search_progress() -> Dict[str, int]:
    """How far into Sales Navigator's results each search got last time, so a repeat starts there."""
    try:
        with open(PROGRESS_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def _known_profiles() -> set:
    """People already in the database (from any earlier run), by member id."""
    try:
        with closing(LeadDatabase(DB_PATH)._get_connection()) as conn:
            return {_profile_key(r[0]) for r in conn.execute("SELECT linkedin_url FROM leads WHERE linkedin_url IS NOT NULL").fetchall()}
    except Exception:
        return set()


@app.post("/api/search/prepare")
def prepare_search(req: StartJobRequest):
    """Validates the form, remembers its filters for the gates and returns the Sales Navigator link."""
    titles = [t for t in (req.role_titles or []) if (t.get("text") or "").strip()][:5]
    roles = list(dict.fromkeys(t["text"].strip() for t in titles)) or ([req.role.strip()] if (req.role or "").strip() else [])
    role = ", ".join(roles)
    if not role:
        return {"error": "Enter the role or job title you are hiring for."}
    places = [p for p in (req.locations or []) if (p.get("text") or "").strip()][:10]
    multi = locations.resolve_many(places, LINKEDIN_GEO_DIRECTORY) if places else {"regions": [], "local_terms": [], "names": []}
    regions = multi["regions"] or [{"id": p.get("id", ""), "text": p["text"]} for p in places if str(p.get("id") or "").isdigit()]
    base_keywords = (req.keywords or "Open to work").strip()
    keywords = _keyword_query(base_keywords, req.extra_keywords or [], req.keyword_mode or "any")
    # Sales Navigator is searched by country, so Gate G narrows to the chosen cities (and their other names)
    terms = list(multi["local_terms"])
    for r in regions:
        parts = [x.strip() for x in (r.get("text") or "").split(",") if x.strip()]
        if len(parts) > 1 or parts[0].lower() not in SALESNAV_COUNTRY_IDS:
            canon = r.get("text") or ""
            terms += [parts[0].lower()] + [k for k, v in locations.ALIASES.items() if v == canon and len(k) > 3]
    filters = SearchFilterParams(role=role, location="; ".join(multi["names"]) or None, regions=list(regions),
                                 location_terms=list(dict.fromkeys(terms)),
                                 seniority_levels=[],  # Sales Navigator applies seniority itself, from LinkedIn's data
                                 keywords=base_keywords)
    job_id = f"job-{int(time.time() * 1000) % 1000000}"
    place_text = "; ".join(p["text"] for p in places) or "Global"
    desc = f"{role}{' [' + ', '.join(req.seniority_levels) + ']' if req.seniority_levels else ''} in {place_text}"
    limit = max(25, min(int(req.target_leads or 50), 500))
    with job_lock:
        _expire_idle_searches()
        mine = ext_jobs.get(req.replace_job_id or "")
        if mine and mine["job"]["status"] == "running":
            return {"error": "A search is already in progress. Stop it or wait for it to finish."}
        if mine and mine["job"]["status"] == "waiting":  # launched again before Sales Navigator answered
            mine["job"].update(status="stopped", completed_at=time.strftime("%Y-%m-%d %H:%M:%S"))
            mine["pending"].clear()
        if sum(s["job"]["status"] in ("waiting", "running") for s in ext_jobs.values()) >= MAX_ACTIVE_SEARCHES:
            return {"error": f"{MAX_ACTIVE_SEARCHES} searches are already running. Try again in a minute."}
        while job_id in ext_jobs:  # two launches in the same millisecond
            job_id = f"job-{uuid.uuid4().int % 1000000:06d}"
        slot = ext_jobs[job_id] = _new_slot()
        current_job, pending_search, current_session_leads = slot["job"], slot["pending"], slot["leads"]
        # Cities whose Sales Navigator code is already known are searched directly (no second page load)
        cities = [r.get("text", "") for r in regions
                  if (r.get("text") or "").split(",")[0].strip().lower() not in SALESNAV_COUNTRY_IDS]
        cache = _geo_cache()
        known = [cache[c.strip().lower()] for c in cities if c.strip().lower() in cache]
        exact = bool(cities) and len(known) == len(cities)
        role_codes = [{"text": r, "id": cache.get("title:" + r.lower(), {}).get("id", "")} for r in roles]
        pending_search.clear()
        # People collected in earlier runs are skipped, and a repeated search starts where the last one stopped
        search_key = "|".join([";".join(sorted(r.lower() for r in roles)), ";".join(sorted(r.get("text", "").lower() for r in regions)),
                               ",".join(req.seniority_levels or []).lower(), keywords.lower()])
        pending_search.update({"search_key": search_key, "start_at": _search_progress().get(search_key, 0),
                               "filters": filters, "job_id": job_id, "desc": desc, "limit": limit, "seen": _known_profiles(),
                               "started": time.time(), "role": role, "seniority": req.seniority_levels or [],
                               "keywords": keywords, "experience": req.experience or [],
                               "refine": [] if exact else cities,
                               "roles": role_codes, "refine_titles": [c["text"] for c in role_codes if not c["id"]],
                               "known_regions": known if exact else []})
        current_session_leads.clear()
        current_job.update({"job_id": job_id, "status": "waiting", "mode": "extension", "total_processed": 0,
                            "qualified": 0, "current_page": 0, "max_pages": (limit + 24) // 25, "error_message": None,
                            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"), "completed_at": None,
                            "search_description": desc, "target_leads": limit, "skipped_known": 0})
    broadcast_event("job_status", current_job)
    add_activity_log("info", f"Search [{job_id}] ready: {desc}. Waiting for results from the browser extension.")
    return {"job_id": job_id, "search_description": desc, "limit": limit,
            "url": _salesnav_search_url(role_codes, known if exact else regions, req.seniority_levels or [], keywords,
                                        exact=exact, experience=req.experience or [])}


def _start_from_tenure(tenure: Any) -> Optional[Dict[str, int]]:
    """Start month from a "1 yr 5 mos" style duration, for positions listed without a start date."""
    if not isinstance(tenure, dict) or not (tenure.get("numYears") or tenure.get("numMonths")):
        return None
    now = time.gmtime()
    months = now.tm_year * 12 + now.tm_mon - 1 - int(tenure.get("numYears") or 0) * 12 - int(tenure.get("numMonths") or 0)
    return {"year": months // 12, "month": months % 12 + 1}


def _normalize_salesnav_element(el: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """One Sales Navigator search result -> the raw-lead shape process_raw_lead understands."""
    if not isinstance(el, dict) or not (el.get("fullName") or el.get("firstName")):
        return None
    positions = [p for p in (el.get("currentPositions") or []) if isinstance(p, dict)]
    cur = positions[0] if positions else {}
    urn = str(el.get("entityUrn") or el.get("objectUrn") or "")
    inner = urn.split(":", 3)[-1].strip("()") if urn else ""
    blob = json.dumps({k: el.get(k) for k in ("spotlightBadges", "badges", "openToWork", "openToWorkBadge",
                                               "profilePictureDisplayImage", "memorialized")}).upper()
    raw = dict(el)
    raw.update({
        "title": cur.get("title") or el.get("title") or "",
        "companyName": cur.get("companyName") or el.get("companyName") or "",
        "location": el.get("geoRegion") or el.get("location") or "",
        "profileUrl": el.get("profileUrl") or (f"https://www.linkedin.com/sales/lead/{inner}" if inner else ""),
        "openToWork": bool(el.get("openToWork")) or "OPEN_TO_WORK" in blob or "OPENTOWORK" in blob,
        "headline": el.get("headline") or el.get("summary") or "",
        "currentPositions": [{"title": p.get("title", ""), "company": p.get("companyName", ""),
                              "startedOn": p.get("startedOn") or _start_from_tenure(p.get("tenureAtPosition")),
                              "endedOn": p.get("endedOn")} for p in positions],
        "pastPositions": [{"title": p.get("title", ""), "company": p.get("companyName", ""),
                           "startedOn": p.get("startedOn"), "endedOn": p.get("endedOn")}
                          for p in (el.get("pastPositions") or []) if isinstance(p, dict)],
    })
    return raw


class IngestRequest(BaseModel):
    page_url: Optional[str] = None
    data: Dict[str, Any] = {}
    job_id: Optional[str] = None  # the search these results belong to (older extensions leave it out)


MAX_INGEST_PAGES = 40  # Sales Navigator shows 25 people per page


ingest_lock = threading.Lock()  # one batch at a time, so the lead limit can't be overshot
# Checking an email with the company's mail server takes ~4 s per person, so it runs alongside the
# search, 10 people at a time, instead of holding up each result
from concurrent.futures import ThreadPoolExecutor
from sales_nav_extractor import EmailFinder, experience_condition
email_pool = ThreadPoolExecutor(max_workers=10)
FIND_EMAILS = os.environ.get("TALENTPULSE_FIND_EMAILS", "1") != "0"
email_finder = EmailFinder()


def _find_email_later(record: Any) -> None:
    try:
        found, status = email_finder.find(record.first_name, record.last_name, record.company_name, None)
        db = LeadDatabase(DB_PATH)
        db.update_lead_email(record.linkedin_url, found, status)
        broadcast_event("lead_email", {"linkedin_url": record.linkedin_url, "email": found, "email_status": status})
    except Exception as e:
        logging.warning(f"Email lookup failed for {record.linkedin_url}: {e}")


def _ingest_worker(elements: List[Dict[str, Any]], page_url: str, last_page: bool = False,
                   job_id: Optional[str] = None) -> None:
    with ingest_lock:
        _ingest_batch(elements, page_url, last_page, job_id)


def _ingest_batch(elements: List[Dict[str, Any]], page_url: str, last_page: bool = False,
                  job_id: Optional[str] = None) -> None:
    with job_lock:
        slot = _slot(job_id)
        current_job, pending_search, current_session_leads, stop_flag = slot["job"], slot["pending"], slot["leads"], slot["stop"]
        ctx = dict(pending_search) if current_job["status"] in ("waiting", "running") else {}
    if not ctx:
        add_activity_log("warning", "Results received but no search is waiting. Start it from New Search first.")
        return
    filters, job_id = ctx["filters"], ctx["job_id"]
    db = LeadDatabase(DB_PATH)
    extractor = SalesNavExtractor(session=None, db=db, enable_jitter=False, dry_run=False, find_emails=False)
    with job_lock:
        current_job["status"] = "running"
        current_job["current_page"] += 1
    broadcast_event("job_status", current_job)
    quota_left = DEFAULT_MAX_DAILY_LEADS - db.count_extracted_since()
    for el in elements:
        if stop_flag.is_set():
            break
        with job_lock:
            if current_job["total_processed"] >= ctx["limit"] or quota_left <= 0:
                break
        raw = _normalize_salesnav_element(el)
        if not raw:
            continue
        record = extractor.process_raw_lead(raw, filters, job_id=job_id)
        if not record.linkedin_url or _profile_key(record.linkedin_url) in pending_search.get("seen", set()):
            with job_lock:
                current_job["skipped_known"] = current_job.get("skipped_known", 0) + 1
            continue
        with job_lock:
            # Check and count in one step
            if current_job["total_processed"] >= ctx["limit"]:
                break
            pending_search.setdefault("seen", set()).add(_profile_key(record.linkedin_url))
            current_job["total_processed"] += 1
        db.upsert_lead(record)
        if FIND_EMAILS and not record.email:
            email_pool.submit(_find_email_later, record)
        quota_left -= 1
        with job_lock:
            current_job["qualified"] += 1 if record.is_qualified else 0
            lead_data = asdict(record)
            current_session_leads.append(lead_data)
        broadcast_event("lead", {"lead": lead_data, "job": current_job, "db_total": db.get_total_count(),
                                 "session_leads_count": len(current_session_leads)})
    with job_lock:
        # The run stops once it has the number of people asked for (page cap keeps it bounded)
        done = (last_page or current_job["total_processed"] >= ctx["limit"] or current_job["current_page"] >= MAX_INGEST_PAGES
                or stop_flag.is_set() or quota_left <= 0)
        if done:
            current_job["status"] = "stopped" if stop_flag.is_set() else "completed"
            current_job["completed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            recent_jobs.insert(0, {"job_id": job_id, "search_description": ctx["desc"], "status": current_job["status"],
                                   "started_at": current_job["started_at"],
                                   "duration": f"{round(time.time() - ctx['started'], 1)}s",
                                   "total_processed": current_job["total_processed"], "qualified": current_job["qualified"],
                                   "disqualified": current_job["total_processed"] - current_job["qualified"]})
            del recent_jobs[10:]
            pending_search.clear()
    if done:
        db.export_to_csv(csv_path=CSV_PATH)
        if last_page and current_job["total_processed"] < ctx["limit"] and not stop_flag.is_set():
            old = current_job.get("skipped_known", 0)
            note = (f"Only {current_job['total_processed']} new people left for these filters (you asked for {ctx['limit']})"
                    + (f"; {old} others were already collected in earlier runs" if old else "")
                    + ". For more, add related roles or more locations, or remove keywords.")
            current_job["error_message"] = note
            add_activity_log("warning", note)
    broadcast_event("job_status", current_job)
    if not elements:
        add_activity_log("warning", "Sales Navigator returned 0 matches for this search. Try a wider location, "
                                    "a broader role, or clear the 'Open to work' keyword.")
        return
    add_activity_log("success", f"Captured page from Sales Navigator: {len(elements)} results ({current_job['total_processed']} so far).")


def _find_leads(data: Any) -> List[Dict[str, Any]]:
    """The people in a Sales Navigator response, wherever the response nests them."""
    found: List[Dict[str, Any]] = []
    def walk(node: Any, depth: int = 0) -> None:
        if depth > 8:
            return
        if isinstance(node, list):
            people = [i for i in node if isinstance(i, dict) and (i.get("fullName") or i.get("firstName"))
                      and (i.get("entityUrn") or i.get("objectUrn") or i.get("currentPositions") is not None)]
            if people:
                found.extend(people)
                return
            for i in node:
                walk(i, depth + 1)
        elif isinstance(node, dict):
            for v in node.values():
                walk(v, depth + 1)
    walk(data)
    seen, unique = set(), []
    for p in found:
        key = p.get("entityUrn") or p.get("objectUrn") or p.get("fullName")
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def _find_paging(data: Any, depth: int = 0) -> Dict[str, Any]:
    if isinstance(data, dict):
        p = data.get("paging")
        if isinstance(p, dict) and "total" in p:
            return p
        for v in data.values():
            r = _find_paging(v, depth + 1) if depth < 6 else {}
            if r:
                return r
    return {}


@app.post("/api/ingest")
def ingest(req: IngestRequest, background_tasks: BackgroundTasks):
    """Results the user's own Sales Navigator tab received, forwarded by the extension."""
    data = req.data or {}
    # Keep the last few raw responses so the parser can be checked against what Sales Navigator really sends
    # (local only by default: they hold candidate data)
    if INGEST_DEBUG:
        try:
            if os.path.exists(INGEST_DEBUG_FILE) and os.path.getsize(INGEST_DEBUG_FILE) > INGEST_DEBUG_MAX_BYTES:
                os.replace(INGEST_DEBUG_FILE, INGEST_DEBUG_FILE + ".1")
            with open(INGEST_DEBUG_FILE, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"page_url": req.page_url, "data": data})[:400000] + "\n")
        except Exception:
            pass
    with job_lock:
        slot = _slot(req.job_id)
        slot["pending"]["touched"] = time.time()
    current_job, pending_search = slot["job"], slot["pending"]
    elements, paging = _find_leads(data)[:100], _find_paging(data)
    if elements and paging.get("start") is not None and pending_search.get("search_key"):
        reached = int(paging.get("start") or 0) + len(elements)
        prog = _search_progress()
        # Past the end of the results, the next run starts again from the top (where new people appear)
        prog[pending_search["search_key"]] = 0 if paging.get("total") and reached >= paging["total"] else reached
        try:
            with open(PROGRESS_FILE, "w", encoding="utf-8") as fh:
                json.dump(prog, fh)
        except Exception:
            pass
    with job_lock:
        state = {"status": current_job["status"], "job_id": current_job.get("job_id"),
                 "processed": current_job["total_processed"], "limit": pending_search.get("limit", 0)}
    if state["status"] not in ("waiting", "running"):
        return {"accepted": 0, "waiting": False, "message": "No search is waiting. Click Launch search in TalentPulse first."}
    if not elements and paging.get("total") != 0:
        # Not a results page (counts, facets, etc.); ignore it and keep waiting for the real one
        return {"accepted": 0, "ignored": True, "waiting": True, **state}
    last_page = not elements or bool(paging.get("total") is not None and paging.get("start", 0) + paging.get("count", 25) >= paging["total"])
    background_tasks.add_task(_ingest_worker, elements, (req.page_url or "")[:2000], last_page, state["job_id"])
    return {"accepted": len(elements), "waiting": True, **state}


GEO_CACHE_FILE = os.path.join(DATA_DIR, "salesnav_geo_cache.json")


def _geo_cache() -> Dict[str, Dict[str, str]]:
    """City codes the extension has already looked up in Sales Navigator, keyed by place name."""
    try:
        with open(GEO_CACHE_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


class RefineRequest(BaseModel):
    regions: List[Dict[str, str]] = []
    title: Dict[str, str] = {}
    titles: List[Dict[str, str]] = []  # [{"role", "id", "text"}], one per role looked up
    job_id: Optional[str] = None


@app.post("/api/search/refine")
def refine_search(req: RefineRequest):
    """The extension found Sales Navigator's own codes for the chosen cities; search those cities directly."""
    with job_lock:
        slot = _slot(req.job_id)
        current_job, pending_search = slot["job"], slot["pending"]
        if current_job["status"] not in ("waiting", "running") or not (pending_search.get("refine") or pending_search.get("refine_titles")):
            return {"url": None}
        cities_wanted = pending_search.get("refine") or []
        pending_search["refine"], pending_search["refine_titles"] = [], []
        found = [r for r in req.regions if str(r.get("id") or "").isdigit()]
        got = {(t.get("role") or "").strip().lower(): t for t in req.titles if str(t.get("id") or "").isdigit()}
        cache = _geo_cache()
        for r in found:
            key = (r.get("place") or r.get("text") or "").strip().lower()
            if key:
                cache[key] = {"id": r["id"], "text": r.get("text", "")}
        for c in pending_search["roles"]:
            hit = got.get(c["text"].lower())
            if hit and not c["id"]:
                c["id"] = hit["id"]
                cache["title:" + c["text"].lower()] = {"id": hit["id"], "text": hit.get("text", "")}
        try:
            with open(GEO_CACHE_FILE, "w", encoding="utf-8") as fh:
                json.dump(cache, fh, indent=1)
        except Exception:
            pass
        filt = pending_search["filters"]
        # Exact city codes if known (just found, or saved earlier); otherwise the country-wide search
        regions_now = found or pending_search.get("known_regions") or list(filt.regions or [])
        exact = bool(found or pending_search.get("known_regions"))
        if not found and not got:
            return {"url": None}
        url = _salesnav_search_url(pending_search["roles"], regions_now, pending_search["seniority"],
                                   pending_search["keywords"], exact=exact, experience=pending_search.get("experience"))
    add_activity_log("info", "Matched the search to LinkedIn's own title and location: "
                     + "; ".join([t.get("text", "") for t in got.values()] + [r.get("text", "") for r in regions_now]))
    return {"url": url}


@app.get("/api/ingest/state")
def ingest_state(job_id: Optional[str] = None):
    """Lets the extension know whether to keep paging."""
    with job_lock:
        slot = _slot(job_id)
        current_job, pending_search = slot["job"], slot["pending"]
        if current_job["status"] in ("waiting", "running"):
            pending_search["touched"] = time.time()
        return {"status": current_job["status"], "job_id": current_job.get("job_id"),
                "processed": current_job["total_processed"], "qualified": current_job["qualified"],
                "limit": pending_search.get("limit", 0), "refine": pending_search.get("refine", []),
                "refine_titles": pending_search.get("refine_titles", []), "start_at": pending_search.get("start_at", 0),
                "waiting": current_job["status"] in ("waiting", "running")}


@app.get("/api/events")
def sse_events():
    def event_stream():
        client_q: queue.Queue = queue.Queue(maxsize=150)
        with subscribers_lock:
            event_subscribers.append(client_q)
        try:
            # Send initial ping & state
            with job_lock:
                first = json.dumps({'type': 'init', 'data': _slot(None)["job"]})
            yield f"data: {first}\n\n"
            while True:
                try:
                    msg = client_q.get(timeout=25.0)
                    yield f"data: {msg}\n\n"
                except queue.Empty:
                    yield ": heartbeat\n\n"
        finally:
            with subscribers_lock:
                if client_q in event_subscribers:
                    event_subscribers.remove(client_q)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Redesigned Enterprise B2B SaaS Dashboard UI (Single-Page App)
# ---------------------------------------------------------------------------
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TalentPulse · Sourcing Console</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,500;12..96,600;12..96,700&family=Geist:wght@400;500;600&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root, [data-theme="dark"] {
  --bg: #0B0D10; --bg-grad: #0E1116; --surface: #12151A; --elevated: #181C22; --hover: #1D222A;
  --line: rgba(255,255,255,0.07); --line-strong: rgba(255,255,255,0.13);
  --text: #EDEFF2; --text-2: #9AA3AE; --muted: #5F6873;
  --lime: #B6F24A; --lime-ink: #0B0D10; --lime-soft: rgba(182,242,74,0.12); --lime-glow: rgba(182,242,74,0.35);
  --sky: #5EC8FF; --sky-soft: rgba(94,200,255,0.12);
  --ember: #FF7A59; --ember-soft: rgba(255,122,89,0.12);
  --amber: #F5C04A; --amber-soft: rgba(245,192,74,0.12);
  --shadow: 0 24px 60px rgba(0,0,0,0.45);
  --highlight: inset 0 1px 0 rgba(255,255,255,0.04);
}
[data-theme="light"] {
  --bg: #F4F5F1; --bg-grad: #ECEEE7; --surface: #FFFFFF; --elevated: #F8F9F5; --hover: #F1F3EC;
  --line: rgba(15,20,10,0.08); --line-strong: rgba(15,20,10,0.16);
  --text: #12150F; --text-2: #4D5547; --muted: #8A9283;
  --lime: #5C8F00; --lime-ink: #FFFFFF; --lime-soft: rgba(92,143,0,0.10); --lime-glow: rgba(92,143,0,0.25);
  --sky: #0277B6; --sky-soft: rgba(2,119,182,0.09);
  --ember: #D9481F; --ember-soft: rgba(217,72,31,0.09);
  --amber: #A86F00; --amber-soft: rgba(168,111,0,0.10);
  --shadow: 0 24px 60px rgba(20,30,10,0.14);
  --highlight: inset 0 1px 0 rgba(255,255,255,0.8);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; }
body {
  font-family: 'Geist', system-ui, sans-serif; font-size: 14px; color: var(--text);
  background: radial-gradient(1200px 600px at 85% -10%, var(--lime-soft), transparent 60%), linear-gradient(180deg, var(--bg-grad), var(--bg) 40%);
  background-color: var(--bg); background-attachment: fixed;
  -webkit-font-smoothing: antialiased; transition: background-color .3s, color .3s;
}
button, input, select, textarea { font: inherit; color: inherit; }
a { color: var(--sky); text-decoration: none; }
a:hover { text-decoration: underline; }
.mono { font-family: 'JetBrains Mono', monospace; }
.label { font-family: 'JetBrains Mono', monospace; font-size: 10.5px; letter-spacing: .09em; text-transform: uppercase; color: var(--muted); font-weight: 500; }
.display { font-family: 'Bricolage Grotesque', sans-serif; letter-spacing: -0.02em; }
.hidden { display: none !important; }

/* ---------- Shell ---------- */
.app { display: grid; grid-template-columns: 248px 1fr; min-height: 100vh; }
.sidebar {
  position: sticky; top: 0; height: 100vh; display: flex; flex-direction: column; justify-content: space-between;
  border-right: 1px solid var(--line); background: color-mix(in srgb, var(--surface) 70%, transparent); backdrop-filter: blur(14px); padding: 20px 14px;
}
.brand { display: flex; align-items: center; gap: 11px; padding: 4px 8px 22px; }
.brand-mark { width: 36px; height: 36px; border-radius: 10px; background: var(--lime); display: grid; place-items: center; position: relative; box-shadow: 0 0 24px var(--lime-glow); }
.brand-mark svg { width: 20px; height: 20px; color: var(--lime-ink); }
.brand-name { font-family: 'Bricolage Grotesque'; font-weight: 700; font-size: 18px; letter-spacing: -0.02em; }
.brand-sub { font-size: 10px; }
.nav { display: flex; flex-direction: column; gap: 3px; }
.nav-btn {
  display: flex; align-items: center; gap: 11px; padding: 10px 12px; border-radius: 9px; border: 1px solid transparent;
  background: none; cursor: pointer; color: var(--text-2); font-weight: 500; text-align: left; transition: all .18s; position: relative;
}
.nav-btn svg { width: 17px; height: 17px; flex-shrink: 0; }
.nav-btn:hover { color: var(--text); background: var(--hover); }
.nav-btn.active { color: var(--text); background: var(--elevated); border-color: var(--line); }
.nav-btn.active::before { content: ''; position: absolute; left: -14px; top: 9px; bottom: 9px; width: 3px; border-radius: 0 3px 3px 0; background: var(--lime); box-shadow: 0 0 12px var(--lime-glow); }
.nav-count { margin-left: auto; font-family: 'JetBrains Mono'; font-size: 10.5px; color: var(--muted); }
.side-foot { display: flex; flex-direction: column; gap: 12px; padding: 0 6px; }
.usage-bar { height: 4px; border-radius: 4px; background: var(--line); overflow: hidden; margin-top: 7px; }
.usage-bar > i { display: block; height: 100%; background: var(--lime); border-radius: 4px; transition: width .6s cubic-bezier(.2,.8,.2,1); }
.conn { display: flex; align-items: center; gap: 8px; padding: 8px 10px; border-radius: 9px; border: 1px solid var(--line); background: var(--surface); font-size: 12px; color: var(--text-2); }

.main { min-width: 0; display: flex; flex-direction: column; }
.topbar {
  position: sticky; top: 0; z-index: 20; display: flex; align-items: center; justify-content: space-between; gap: 16px;
  padding: 18px 32px; border-bottom: 1px solid var(--line); background: color-mix(in srgb, var(--bg) 75%, transparent); backdrop-filter: blur(14px);
}
.topbar h1 { font-family: 'Bricolage Grotesque'; font-size: 22px; font-weight: 600; letter-spacing: -0.02em; }
.top-actions { display: flex; align-items: center; gap: 8px; }
.content { padding: 28px 32px 48px; max-width: 1480px; width: 100%; }
.btn.menu-btn { display: none; }
.card, .job-body, .split > *, .form-grid > * { min-width: 0; }

/* ---------- Primitives ---------- */
.btn {
  display: inline-flex; align-items: center; justify-content: center; gap: 8px; height: 38px; padding: 0 16px; border-radius: 9px;
  border: 1px solid var(--line-strong); background: var(--surface); cursor: pointer; font-weight: 500; font-size: 13px; white-space: nowrap;
  transition: transform .12s, background .18s, border-color .18s, box-shadow .18s, opacity .18s;
}
.btn svg { width: 15px; height: 15px; }
.btn:hover:not(:disabled) { background: var(--hover); border-color: var(--line-strong); }
.btn:active:not(:disabled) { transform: scale(.97); }
.btn:disabled { opacity: .4; cursor: not-allowed; }
.btn-primary { background: var(--lime); color: var(--lime-ink); border-color: transparent; font-weight: 600; box-shadow: 0 0 0 0 var(--lime-glow); }
.btn-primary:hover:not(:disabled) { background: var(--lime); box-shadow: 0 0 26px var(--lime-glow); }
.btn-danger { color: var(--ember); border-color: color-mix(in srgb, var(--ember) 40%, transparent); background: transparent; }
.btn-danger:hover:not(:disabled) { background: var(--ember-soft); }
.btn-ghost { background: transparent; border-color: transparent; color: var(--text-2); }
.btn-ghost:hover:not(:disabled) { color: var(--text); }
.btn-icon { width: 38px; padding: 0; }
.btn-sm { height: 30px; padding: 0 10px; font-size: 12px; border-radius: 7px; }
.btn-sm.btn-icon { width: 30px; }

.card {
  background: var(--surface); border: 1px solid var(--line); border-radius: 14px; box-shadow: var(--highlight); position: relative;
}
.card-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 18px 20px; border-bottom: 1px solid var(--line); }
.card-title { font-family: 'Bricolage Grotesque'; font-weight: 600; font-size: 16px; letter-spacing: -0.01em; }
.card-sub { color: var(--text-2); font-size: 12.5px; margin-top: 2px; }

.chip {
  display: inline-flex; align-items: center; gap: 6px; height: 24px; padding: 0 10px; border-radius: 99px; font-size: 11.5px; font-weight: 500; white-space: nowrap;
  border: 1px solid transparent;
}
.chip .dot { width: 6px; height: 6px; border-radius: 50%; background: currentColor; }
.chip-lime { color: var(--lime); background: var(--lime-soft); border-color: color-mix(in srgb, var(--lime) 25%, transparent); }
.chip-sky { color: var(--sky); background: var(--sky-soft); border-color: color-mix(in srgb, var(--sky) 25%, transparent); }
.chip-ember { color: var(--ember); background: var(--ember-soft); border-color: color-mix(in srgb, var(--ember) 25%, transparent); }
.chip-amber { color: var(--amber); background: var(--amber-soft); border-color: color-mix(in srgb, var(--amber) 25%, transparent); }
.chip-muted { color: var(--text-2); background: var(--elevated); border-color: var(--line); }
.pulse .dot { animation: pulse 1.6s ease-out infinite; }
@keyframes pulse { 0% { box-shadow: 0 0 0 0 currentColor; } 70% { box-shadow: 0 0 0 6px transparent; } 100% { box-shadow: 0 0 0 0 transparent; } }

.field { display: flex; flex-direction: column; gap: 8px; }
.field > label { font-size: 12.5px; font-weight: 500; color: var(--text-2); display: flex; align-items: center; justify-content: space-between; }
.input, .select, textarea.input {
  height: 42px; width: 100%; padding: 0 14px; border-radius: 9px; border: 1px solid var(--line-strong); background: var(--bg);
  outline: none; transition: border-color .18s, box-shadow .18s; font-size: 13.5px;
}
textarea.input { height: auto; min-height: 96px; padding: 12px 14px; resize: vertical; font-family: 'JetBrains Mono'; font-size: 12.5px; line-height: 1.6; }
.input::placeholder, textarea.input::placeholder { color: var(--muted); }
.input:focus, .select:focus { border-color: var(--lime); box-shadow: 0 0 0 3px var(--lime-soft); }
.select { appearance: none; cursor: pointer; padding-right: 34px; background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' fill='none' stroke='%239AA3AE' stroke-width='2' viewBox='0 0 24 24'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E"); background-repeat: no-repeat; background-position: right 12px center; }
.select-sm { height: 34px; font-size: 12.5px; }
.input-icon { position: relative; }
.input-icon svg { position: absolute; left: 12px; top: 50%; transform: translateY(-50%); width: 15px; height: 15px; color: var(--muted); pointer-events: none; z-index: 1; }
.input-icon:focus-within svg { color: var(--lime); }
.input-icon .input { padding-left: 36px; }

/* ---------- Views ---------- */
.view { animation: viewIn .45s cubic-bezier(.2,.8,.2,1); }
@keyframes viewIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: none; } }
.stack { display: flex; flex-direction: column; gap: 20px; }

/* KPIs */
.kpis { display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; }
.kpi { padding: 20px; overflow: hidden; }
.kpi-top { display: flex; align-items: center; justify-content: space-between; }
.kpi-icon { width: 30px; height: 30px; border-radius: 8px; display: grid; place-items: center; background: var(--elevated); border: 1px solid var(--line); color: var(--text-2); }
.kpi-icon svg { width: 15px; height: 15px; }
.kpi-value { font-family: 'Bricolage Grotesque'; font-size: 40px; font-weight: 600; letter-spacing: -0.03em; line-height: 1; margin: 18px 0 8px; font-variant-numeric: tabular-nums; }
.kpi-foot { font-size: 12px; color: var(--text-2); }
.kpi.is-lime { border-color: color-mix(in srgb, var(--lime) 30%, transparent); }
.kpi.is-lime::after { content: ''; position: absolute; inset: auto -40px -60px auto; width: 180px; height: 180px; background: radial-gradient(circle, var(--lime-glow), transparent 65%); opacity: .5; pointer-events: none; }
.kpi.is-lime .kpi-value { color: var(--lime); }
.kpi.is-lime .kpi-icon { color: var(--lime); background: var(--lime-soft); border-color: transparent; }
.kpi.is-ember .kpi-icon { color: var(--ember); background: var(--ember-soft); border-color: transparent; }
.kpi.is-sky .kpi-icon { color: var(--sky); background: var(--sky-soft); border-color: transparent; }
.kpi-meter { height: 3px; border-radius: 3px; background: var(--line); margin-top: 12px; overflow: hidden; }
.kpi-meter i { display: block; height: 100%; border-radius: 3px; transition: width .8s cubic-bezier(.2,.8,.2,1); }

/* Live job */
.job { display: grid; grid-template-columns: 220px 1fr; gap: 28px; padding: 24px; overflow: hidden; }
.radar { width: 200px; height: 200px; border-radius: 50%; position: relative; margin: auto;
  background: radial-gradient(circle, transparent 0 29%, var(--line) 29.5% 30%, transparent 30.5% 59%, var(--line) 59.5% 60%, transparent 60.5% 89%, var(--line-strong) 89.5% 90%, transparent 90.5%);
}
.radar::before, .radar::after { content: ''; position: absolute; background: var(--line); }
.radar::before { left: 50%; top: 5%; bottom: 5%; width: 1px; }
.radar::after { top: 50%; left: 5%; right: 5%; height: 1px; }
.radar-sweep { position: absolute; inset: 5%; border-radius: 50%; background: conic-gradient(from 0deg, transparent 0 270deg, var(--lime-soft) 320deg, var(--lime-glow) 360deg); opacity: 0; transition: opacity .4s; }
.radar-core { position: absolute; left: 50%; top: 50%; width: 12px; height: 12px; margin: -6px; border-radius: 50%; background: var(--muted); z-index: 2; transition: background .3s, box-shadow .3s; }
.radar-blip { position: absolute; width: 7px; height: 7px; margin: -3.5px; border-radius: 50%; background: var(--lime); box-shadow: 0 0 10px var(--lime); z-index: 2; animation: blip 2.6s ease-out forwards; }
@keyframes blip { 0% { transform: scale(0); opacity: 0; } 10% { transform: scale(1.6); opacity: 1; } 100% { transform: scale(1); opacity: 0; } }
.job.is-running .radar-sweep { opacity: 1; animation: sweep 2.8s linear infinite; }
.job.is-running .radar-core { background: var(--lime); box-shadow: 0 0 0 6px var(--lime-soft), 0 0 22px var(--lime-glow); }
.job.is-stopping .radar-sweep { opacity: .5; animation: sweep 6s linear infinite; }
.job.is-error .radar-core { background: var(--ember); box-shadow: 0 0 0 6px var(--ember-soft); }
@keyframes sweep { to { transform: rotate(360deg); } }
.job-body { display: flex; flex-direction: column; gap: 18px; min-width: 0; }
.job-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
.job-desc { font-family: 'Bricolage Grotesque'; font-size: 24px; font-weight: 600; letter-spacing: -0.02em; line-height: 1.2; margin-top: 8px; overflow-wrap: anywhere; }
.job-stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1px; background: var(--line); border: 1px solid var(--line); border-radius: 11px; overflow: hidden; }
.job-stat { background: var(--surface); padding: 12px 14px; }
.job-stat b { display: block; font-family: 'Bricolage Grotesque'; font-size: 22px; font-weight: 600; letter-spacing: -0.02em; margin-top: 4px; font-variant-numeric: tabular-nums; }
.progress { height: 8px; border-radius: 8px; background: var(--elevated); border: 1px solid var(--line); overflow: hidden; position: relative; }
.progress > i { display: block; height: 100%; width: 0; border-radius: 8px; background: var(--lime); transition: width .6s cubic-bezier(.2,.8,.2,1); position: relative; overflow: hidden; }
.job.is-running .progress > i::after { content: ''; position: absolute; inset: 0; background: linear-gradient(90deg, transparent, rgba(255,255,255,.45), transparent); animation: shimmer 1.4s infinite; }
@keyframes shimmer { from { transform: translateX(-100%); } to { transform: translateX(100%); } }
.job-actions { display: flex; gap: 10px; flex-wrap: wrap; }

.split { display: grid; grid-template-columns: minmax(0, 1fr); gap: 20px; }

/* Tables */
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; }
thead th { position: sticky; top: 0; text-align: left; padding: 11px 16px; background: var(--surface); border-bottom: 1px solid var(--line); white-space: nowrap; }
tbody td { padding: 12px 16px; border-bottom: 1px solid var(--line); vertical-align: middle; font-size: 13px; }
tbody tr { transition: background .15s; }
tbody tr:last-child td { border-bottom: none; }
tbody tr.clickable { cursor: pointer; }
tbody tr.clickable:hover { background: var(--elevated); }
tr.is-new { animation: rowIn .7s cubic-bezier(.2,.8,.2,1), rowGlow 2.4s ease-out; }
@keyframes rowIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: none; } }
@keyframes rowGlow { 0% { background: var(--lime-soft); } 100% { background: transparent; } }
.person { display: flex; align-items: center; gap: 12px; min-width: 200px; }
.avatar { width: 36px; height: 36px; border-radius: 50%; flex-shrink: 0; display: grid; place-items: center; font-family: 'Bricolage Grotesque'; font-weight: 600; font-size: 13px; background: var(--elevated); border: 1px solid var(--line-strong); color: var(--text-2); position: relative; }
.avatar.otw { border: 2px solid var(--lime); color: var(--lime); box-shadow: 0 0 0 3px var(--lime-soft); }
.avatar.otw::after { content: '#OTW'; position: absolute; bottom: -7px; left: 50%; transform: translateX(-50%); font-family: 'JetBrains Mono'; font-size: 7px; font-weight: 600; background: var(--lime); color: var(--lime-ink); padding: 1px 3px; border-radius: 3px; letter-spacing: .02em; }
.person-name { font-weight: 600; line-height: 1.3; }
.person-meta { font-family: 'JetBrains Mono'; font-size: 10.5px; color: var(--muted); margin-top: 2px; }
.cell-title { line-height: 1.35; max-width: 280px; }
.cell-title small { display: block; color: var(--text-2); font-size: 12px; margin-top: 2px; }
.cell-muted { color: var(--text-2); }
.trunc { max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.contact-icons { display: flex; gap: 6px; }
.ci { width: 26px; height: 26px; border-radius: 7px; display: grid; place-items: center; border: 1px solid var(--line); color: var(--muted); }
.ci svg { width: 13px; height: 13px; }
.ci.on { color: var(--sky); background: var(--sky-soft); border-color: transparent; }
.ci.warn { color: var(--amber); background: var(--amber-soft); border-color: transparent; }
.row-actions { display: flex; gap: 6px; justify-content: flex-end; }
.empty { padding: 56px 20px; text-align: center; color: var(--text-2); }
.empty svg { width: 34px; height: 34px; color: var(--muted); margin-bottom: 12px; }
.empty b { display: block; color: var(--text); font-family: 'Bricolage Grotesque'; font-size: 16px; margin-bottom: 4px; }
.spin { animation: spin 0.9s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }

/* Log */
.log { height: 420px; overflow-y: auto; padding: 14px 16px; font-family: 'JetBrains Mono'; font-size: 11.5px; line-height: 1.7; }
.log-line { display: grid; grid-template-columns: 58px 10px 1fr; gap: 8px; align-items: baseline; animation: rowIn .4s ease-out; }
.log-line time { color: var(--muted); }
.log-line .lv { width: 6px; height: 6px; border-radius: 50%; background: var(--text-2); transform: translateY(-1px); }
.log-line.success .lv { background: var(--lime); } .log-line.warning .lv { background: var(--amber); } .log-line.error .lv { background: var(--ember); } .log-line.info .lv { background: var(--sky); }
.log-line span { color: var(--text-2); overflow-wrap: anywhere; }
.log-line.error span { color: var(--ember); }

/* Search form */
.form-grid { display: grid; grid-template-columns: minmax(0, 1fr) 340px; gap: 20px; align-items: start; }
.form-body { padding: 24px; display: flex; flex-direction: column; gap: 24px; }
.tabs { display: inline-flex; padding: 4px; border-radius: 11px; background: var(--bg); border: 1px solid var(--line); gap: 4px; }
.tab { height: 34px; padding: 0 14px; border-radius: 8px; border: none; background: none; cursor: pointer; color: var(--text-2); font-weight: 500; font-size: 13px; display: inline-flex; align-items: center; gap: 8px; transition: all .18s; }
.tab svg { width: 14px; height: 14px; }
.tab.active { background: var(--elevated); color: var(--text); box-shadow: 0 1px 0 var(--line-strong) inset, 0 4px 14px rgba(0,0,0,.18); }
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.seg { display: grid; grid-template-columns: repeat(5, 1fr); gap: 8px; }
.seg button, .pill-opt {
  height: 46px; border-radius: 10px; border: 1px solid var(--line-strong); background: var(--bg); cursor: pointer; font-family: 'Bricolage Grotesque'; font-weight: 600; font-size: 16px; transition: all .18s;
}
.seg button small { display: block; font-family: 'JetBrains Mono'; font-size: 9.5px; color: var(--muted); font-weight: 500; margin-top: 1px; }
.seg button:hover, .pill-opt:hover { border-color: var(--text-2); }
.seg button.active { border-color: var(--lime); background: var(--lime-soft); color: var(--lime); box-shadow: 0 0 0 3px var(--lime-soft); }
.seg button.active small { color: var(--lime); }
.pills { display: flex; flex-wrap: wrap; gap: 8px; }
.pill-opt { height: 34px; padding: 0 14px; font-family: 'Geist'; font-weight: 500; font-size: 13px; border-radius: 99px; display: inline-flex; align-items: center; gap: 7px; }
.pill-opt.active { border-color: var(--lime); color: var(--lime); background: var(--lime-soft); }
.pill-opt .tick { width: 14px; height: 14px; display: none; }
.pill-opt.active .tick { display: block; }
.toggle-row { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 16px; border-radius: 11px; border: 1px solid var(--line); background: var(--bg); }
.switch { position: relative; width: 44px; height: 24px; flex-shrink: 0; }
.switch input { opacity: 0; width: 0; height: 0; }
.switch span { position: absolute; inset: 0; border-radius: 24px; background: var(--line-strong); cursor: pointer; transition: background .2s; }
.switch span::before { content: ''; position: absolute; left: 3px; top: 3px; width: 18px; height: 18px; border-radius: 50%; background: var(--text); transition: transform .25s cubic-bezier(.2,.8,.2,1); }
.switch input:checked + span { background: var(--sky); }
.switch input:checked + span::before { transform: translateX(20px); background: #fff; }
.switch input:focus-visible + span { box-shadow: 0 0 0 3px var(--sky-soft); }
.form-foot { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 18px 24px; border-top: 1px solid var(--line); flex-wrap: wrap; }
.summary { padding: 22px; display: flex; flex-direction: column; gap: 16px; position: sticky; top: 92px; }
.summary-row { display: flex; justify-content: space-between; gap: 12px; padding-bottom: 12px; border-bottom: 1px dashed var(--line); font-size: 13px; }
.summary-row:last-child { border-bottom: none; padding-bottom: 0; }
.summary-row span:last-child { text-align: right; font-weight: 500; overflow-wrap: anywhere; }
.gates { display: flex; flex-direction: column; gap: 10px; }
.gate { display: grid; grid-template-columns: 26px 1fr; gap: 10px; font-size: 12.5px; color: var(--text-2); line-height: 1.45; }
.gate i { width: 26px; height: 26px; border-radius: 7px; display: grid; place-items: center; font-family: 'JetBrains Mono'; font-style: normal; font-size: 11px; font-weight: 600; background: var(--elevated); border: 1px solid var(--line); color: var(--text); }
.gate b { color: var(--text); font-weight: 600; }
.geo-wrap { position: relative; }
.geo-menu { position: absolute; left: 0; right: 0; top: calc(100% + 6px); z-index: 30; background: var(--elevated); border: 1px solid var(--line-strong); border-radius: 11px; box-shadow: var(--shadow); max-height: 300px; overflow-y: auto; padding: 6px; }
.geo-item { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 9px 10px; border-radius: 8px; cursor: pointer; font-size: 13px; }
.geo-item:hover, .geo-item.focus { background: var(--hover); }
.geo-item small { color: var(--muted); font-size: 11.5px; }
.hint { font-size: 12px; color: var(--muted); line-height: 1.5; }
.geo-item mark { background: none; color: var(--lime); font-weight: 600; }
.geo-item .ri-sub { display: block; margin-top: 2px; }
.geo-item.use-own { border-top: 1px dashed var(--line); margin-top: 4px; padding-top: 11px; }
.dd { position: relative; max-width: 420px; }
.dd-opt { display: flex; align-items: center; gap: 10px; padding: 9px 10px; border-radius: 8px; cursor: pointer; font-size: 13.5px; }
.dd-opt:hover { background: var(--hover); }
.dd-opt input { width: 16px; height: 16px; accent-color: var(--lime); cursor: pointer; }
.dd-foot { display: flex; justify-content: flex-end; gap: 6px; padding: 8px 4px 2px; margin-top: 4px; border-top: 1px solid var(--line); }

/* Explorer */
.toolbar { display: flex; flex-direction: column; gap: 12px; padding: 16px 18px; border-bottom: 1px solid var(--line); }
.toolbar-row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
.toolbar-row .grow { flex: 1 1 240px; }
.filters { display: grid; grid-template-columns: repeat(7, minmax(0, 1fr)); gap: 10px; }
.filter { display: flex; flex-direction: column; gap: 5px; min-width: 0; }
.count-badge { font-family: 'JetBrains Mono'; font-size: 11.5px; color: var(--text-2); white-space: nowrap; }
.count-badge b { color: var(--text); }

/* Drawer */
.scrim { position: fixed; inset: 0; z-index: 50; background: rgba(0,0,0,.5); backdrop-filter: blur(3px); opacity: 0; pointer-events: none; transition: opacity .3s; }
.scrim.open { opacity: 1; pointer-events: auto; }
.drawer { position: fixed; top: 0; right: 0; bottom: 0; z-index: 51; width: min(560px, 100vw); background: var(--surface); border-left: 1px solid var(--line-strong); box-shadow: var(--shadow); transform: translateX(100%); transition: transform .38s cubic-bezier(.2,.8,.2,1); display: flex; flex-direction: column; }
.drawer.open { transform: none; }
.drawer-head { padding: 24px; border-bottom: 1px solid var(--line); display: flex; gap: 16px; align-items: flex-start; position: relative; overflow: hidden; }
.drawer-head::before { content: ''; position: absolute; inset: -80px -80px auto auto; width: 260px; height: 260px; background: radial-gradient(circle, var(--lime-soft), transparent 70%); pointer-events: none; }
.drawer-head .avatar { width: 56px; height: 56px; font-size: 19px; }
.drawer-name { font-family: 'Bricolage Grotesque'; font-size: 22px; font-weight: 600; letter-spacing: -0.02em; line-height: 1.2; }
.drawer-body { flex: 1; overflow-y: auto; padding: 22px 24px; display: flex; flex-direction: column; gap: 22px; }
.section h4 { margin-bottom: 12px; }
.kv { display: grid; grid-template-columns: 130px 1fr; gap: 10px 14px; font-size: 13px; }
.kv dt { color: var(--text-2); }
.kv dd { overflow-wrap: anywhere; }
.evidence { padding: 16px; border-radius: 11px; border: 1px solid var(--line); background: var(--bg); display: flex; flex-direction: column; gap: 10px; font-size: 13px; line-height: 1.5; }
.evidence.is-lime { border-color: color-mix(in srgb, var(--lime) 30%, transparent); background: linear-gradient(135deg, var(--lime-soft), transparent 70%); }
.evidence.is-ember { border-color: color-mix(in srgb, var(--ember) 30%, transparent); background: linear-gradient(135deg, var(--ember-soft), transparent 70%); }
.evidence.is-amber { border-color: color-mix(in srgb, var(--amber) 30%, transparent); background: linear-gradient(135deg, var(--amber-soft), transparent 70%); }
.skills { display: flex; flex-wrap: wrap; gap: 6px; }
.skill { font-size: 12px; padding: 4px 10px; border-radius: 99px; border: 1px solid var(--line-strong); color: var(--text-2); }
.copy-line { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 10px 12px; border-radius: 9px; border: 1px solid var(--line); background: var(--bg); }
.copy-line + .copy-line { margin-top: 8px; }
.drawer-foot { padding: 16px 24px; border-top: 1px solid var(--line); display: flex; gap: 8px; flex-wrap: wrap; }

/* Toasts */
.toasts { position: fixed; right: 20px; bottom: 20px; z-index: 80; display: flex; flex-direction: column; gap: 10px; }
.toast { display: flex; align-items: center; gap: 10px; padding: 12px 16px; border-radius: 11px; background: var(--elevated); border: 1px solid var(--line-strong); box-shadow: var(--shadow); font-size: 13px; max-width: 380px; animation: toastIn .35s cubic-bezier(.2,.8,.2,1); }
.toast .dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; background: var(--sky); }
.toast.success .dot { background: var(--lime); } .toast.error .dot { background: var(--ember); } .toast.warning .dot { background: var(--amber); }
.toast.out { animation: toastOut .3s forwards; }
@keyframes toastIn { from { opacity: 0; transform: translateY(12px) scale(.97); } to { opacity: 1; transform: none; } }
@keyframes toastOut { to { opacity: 0; transform: translateX(20px); } }

/* Responsive */
@media (max-width: 1200px) {
  .kpis { grid-template-columns: repeat(2, 1fr); }
  .split, .form-grid { grid-template-columns: 1fr; }
  .summary { position: static; }
  .filters { grid-template-columns: repeat(4, minmax(0, 1fr)); }
}
@media (max-width: 900px) {
  .app { grid-template-columns: 1fr; }
  .sidebar { position: fixed; left: 0; top: 0; z-index: 60; width: 260px; transform: translateX(-100%); transition: transform .3s; background: var(--surface); }
  .sidebar.open { transform: none; box-shadow: var(--shadow); }
  .btn.menu-btn { display: inline-flex; }
  .topbar { padding: 14px 16px; }
  .topbar h1 { font-size: 18px; }
  .content { padding: 18px 16px 40px; }
  .job { grid-template-columns: 1fr; }
  .radar { width: 150px; height: 150px; }
  .job-stats { grid-template-columns: repeat(2, 1fr); }
  .grid-2 { grid-template-columns: 1fr; }
  .seg { grid-template-columns: repeat(3, 1fr); }
  .filters { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .top-actions .btn-label { display: none; }
}
@media (max-width: 520px) {
  .kpis { grid-template-columns: 1fr 1fr; gap: 10px; }
  .kpi { padding: 14px; }
  .kpi-value { font-size: 30px; margin: 12px 0 6px; }
  .kv { grid-template-columns: 1fr; gap: 2px 0; }
  .kv dd { margin-bottom: 8px; }
}

/* ---------- New Search animations ---------- */
/* 1. Fields cascade in when the page opens */
/* 'backwards' (not 'both'): no transform is left behind after the entrance, so rows don't become
   permanent stacking layers that would cover the location dropdown */
#searchForm.enter .form-body > * { animation: fieldIn .5s cubic-bezier(.2,.8,.2,1) backwards; }
#searchForm.enter .form-body > *:nth-child(1) { animation-delay: .05s; }
#searchForm.enter .form-body > *:nth-child(2) { animation-delay: .12s; }
#searchForm.enter .form-body > *:nth-child(3) { animation-delay: .19s; }
#searchForm.enter .form-body > *:nth-child(4) { animation-delay: .26s; }
/* Keep the location dropdown above the rows below it */
.form-body > *:has(.geo-wrap) { position: relative; z-index: 5; }
#searchForm.enter .summary { animation: summaryIn .6s .15s cubic-bezier(.2,.8,.2,1) backwards; }
@keyframes fieldIn { from { opacity: 0; transform: translateY(14px); } to { opacity: 1; transform: none; } }
@keyframes summaryIn { from { opacity: 0; transform: translateX(18px); } to { opacity: 1; transform: none; } }

/* 2. Header scanning sweep: the tool is searching LinkedIn */
#view-search .card-head { position: relative; overflow: hidden; }
#view-search .card-head::after {
  content: ''; position: absolute; left: 0; right: 0; bottom: -1px; height: 2px;
  background: linear-gradient(90deg, transparent, var(--lime), transparent); background-size: 40% 100%; background-repeat: no-repeat;
  animation: scanLine 3.2s ease-in-out infinite;
}
@keyframes scanLine { 0% { background-position: -40% 0; } 100% { background-position: 140% 0; } }
.radar-mini { width: 22px; height: 22px; border-radius: 50%; position: relative; border: 1px solid color-mix(in srgb, var(--lime) 40%, transparent); flex-shrink: 0; }
.radar-mini::before { content: ''; position: absolute; inset: 2px; border-radius: 50%; background: conic-gradient(from 0deg, transparent 0 270deg, var(--lime-glow) 360deg); animation: sweep 2.4s linear infinite; }
.radar-mini::after { content: ''; position: absolute; left: 50%; top: 50%; width: 5px; height: 5px; margin: -2.5px; border-radius: 50%; background: var(--lime); box-shadow: 0 0 8px var(--lime); }

/* 3. Focus: label lights up with the field */
.field:focus-within > label { color: var(--lime); }
.field > label { transition: color .2s; }
.input, .select { transition: border-color .2s, box-shadow .2s, transform .2s; }
.input:focus, .select:focus { transform: translateY(-1px); }

/* 4. Lead count: selected number pops */
.seg button { position: relative; overflow: hidden; }
.seg button.active { animation: segPop .35s cubic-bezier(.3,1.6,.5,1); }
@keyframes segPop { 0% { transform: scale(.94); } 100% { transform: scale(1); } }
.seg button.active::after { content: ''; position: absolute; left: 50%; bottom: 6px; width: 18px; height: 3px; margin-left: -9px; border-radius: 3px; background: var(--lime); animation: barIn .3s ease-out; }
@keyframes barIn { from { width: 0; margin-left: 0; } }

/* 5. Summary values flash when your choice changes */
.summary-row span:last-child { transition: color .3s; }
.summary-row span.flash { animation: valFlash .7s ease-out; }
@keyframes valFlash { 0% { color: var(--lime); transform: translateX(-4px); } 100% { transform: none; } }

/* 6. The 5 checks light up in sequence, like a lead moving through the pipeline */
.gate { border-radius: 9px; padding: 4px; margin: -4px; transition: background .3s; }
.gate i { transition: background .3s, color .3s, box-shadow .3s; }
.gates .gate { animation: gateScan 9s infinite; }
.gates .gate i { animation: gateScanIcon 9s infinite; }
.gates .gate:nth-child(2), .gates .gate:nth-child(2) i { animation-delay: 1.5s; }
.gates .gate:nth-child(3), .gates .gate:nth-child(3) i { animation-delay: 3s; }
.gates .gate:nth-child(4), .gates .gate:nth-child(4) i { animation-delay: 4.5s; }
.gates .gate:nth-child(5), .gates .gate:nth-child(5) i { animation-delay: 6s; }
.gates .gate:nth-child(6), .gates .gate:nth-child(6) i { animation-delay: 7.5s; }
@keyframes gateScan { 0%, 15% { background: var(--lime-soft); } 20%, 100% { background: transparent; } }
@keyframes gateScanIcon { 0%, 15% { background: var(--lime); color: var(--lime-ink); box-shadow: 0 0 14px var(--lime-glow); } 20%, 100% { background: var(--elevated); color: var(--text); box-shadow: none; } }

/* 7. Launch button: periodic shine + launching state */
#btnLaunch { position: relative; overflow: hidden; }
#btnLaunch::after { content: ''; position: absolute; top: 0; bottom: 0; width: 40%; left: -60%; background: linear-gradient(100deg, transparent, rgba(255,255,255,.55), transparent); animation: btnShine 4s ease-in-out infinite; }
@keyframes btnShine { 0%, 70% { left: -60%; } 100% { left: 130%; } }
#btnLaunch svg { transition: transform .25s; }
#btnLaunch:hover:not(:disabled) svg { transform: translateX(3px); }
#btnLaunch.is-launching { opacity: 1 !important; }
#btnLaunch.is-launching svg { animation: spin .8s linear infinite; }
/* Location box: several places as chips */
.loc-box { height: auto; min-height: 42px; display: flex; flex-wrap: wrap; align-items: center; gap: 6px; padding: 5px 8px 5px 34px; cursor: text; position: relative; }
.loc-box:focus-within { border-color: var(--lime); box-shadow: 0 0 0 3px var(--lime-soft); }
.loc-box > svg { position: absolute; left: 12px; top: 13px; width: 15px; height: 15px; color: var(--muted); pointer-events: none; }
.loc-box:focus-within > svg { color: var(--lime); }
.loc-box input { flex: 1; min-width: 140px; height: 30px; border: 0; outline: none; background: transparent; color: inherit; font: inherit; font-size: 13.5px; padding: 0 4px; }
.loc-box input::placeholder { color: var(--muted); }
.loc-chips { display: contents; }
.loc-chip { display: inline-flex; align-items: center; gap: 4px; height: 28px; max-width: 100%; padding: 0 4px 0 10px; border-radius: 7px; background: var(--lime-soft); border: 1px solid color-mix(in srgb, var(--lime) 45%, transparent); font-size: 12.5px; font-weight: 500; animation: segPop .3s cubic-bezier(.3,1.6,.5,1); }
.loc-chip > span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.loc-chip.free { background: var(--hover); border-color: var(--line-strong); }
.loc-chip button { width: 20px; height: 20px; display: grid; place-items: center; border: 0; border-radius: 5px; background: transparent; color: var(--text-2); cursor: pointer; font-size: 15px; line-height: 1; }
.loc-chip button:hover { background: var(--line); color: var(--text); }

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { animation-duration: .001ms !important; animation-iteration-count: 1 !important; transition-duration: .001ms !important; }
}
</style>
</head>
<body>
<div class="app">
  <!-- ================= Sidebar ================= -->
  <aside class="sidebar" id="sidebar">
    <div>
      <div class="brand">
        <div class="brand-mark"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12h4l3-8 4 16 3-8h4"/></svg></div>
        <div><div class="brand-name">TalentPulse</div><div class="label brand-sub">Sourcing Console</div></div>
      </div>
      <nav class="nav">
        <button class="nav-btn active" data-view="overview" onclick="switchView('overview')">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="9" rx="1.5"/><rect x="14" y="3" width="7" height="5" rx="1.5"/><rect x="14" y="12" width="7" height="9" rx="1.5"/><rect x="3" y="16" width="7" height="5" rx="1.5"/></svg>
          Overview
        </button>
        <button class="nav-btn" data-view="search" onclick="switchView('search')">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
          New Search
        </button>
        <button class="nav-btn" data-view="explorer" onclick="switchView('explorer')">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg>
          Lead Explorer <span class="nav-count" id="navLeadCount">0</span>
        </button>
      </nav>
    </div>
    <div class="side-foot">
      <div>
        <div style="display:flex;justify-content:space-between;align-items:baseline">
          <span class="label" title="Rolling 24-hour limit to keep your LinkedIn account safe">Daily quota</span>
          <span class="mono" style="font-size:11px;color:var(--text-2)"><b id="capUsed" style="color:var(--text)">0</b> / <span id="capTotal">2,000</span></span>
        </div>
        <div class="usage-bar"><i id="capBar" style="width:0%"></i></div>
      </div>
      <div class="hint" id="quotaHint" style="margin-top:-4px">Resets on a rolling 24-hour window</div>
      <div class="conn" id="sseBadge"><span class="chip chip-amber pulse" style="height:auto;padding:0;background:none;border:none"><span class="dot"></span></span><span id="sseText">Connecting…</span></div>
      <div class="mono" style="font-size:10.5px;color:var(--muted);text-align:center">TalentPulse v2.2 · Zero-cost</div>
    </div>
  </aside>

  <!-- ================= Main ================= -->
  <div class="main">
    <header class="topbar">
      <div style="display:flex;align-items:center;gap:12px;min-width:0">
        <button class="btn btn-icon btn-ghost menu-btn" onclick="toggleSidebar()" aria-label="Menu"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 6h16M4 12h16M4 18h16"/></svg></button>
        <div style="min-width:0">
          <div class="label" id="pageEyebrow">Live Sales Navigator sourcing</div>
          <h1 id="pageTitle">Overview</h1>
        </div>
      </div>
      <div class="top-actions">
        <button class="btn btn-icon btn-ghost" onclick="toggleTheme()" title="Toggle theme" aria-label="Toggle theme">
          <svg id="themeIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>
        </button>
        <button class="btn btn-icon btn-ghost" onclick="refreshAll(true)" title="Refresh" aria-label="Refresh"><svg id="refreshIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12a9 9 0 1 1-2.64-6.36M21 3v6h-6"/></svg></button>
        <button class="btn btn-primary" onclick="switchView('search')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4"><path d="M12 5v14M5 12h14"/></svg><span class="btn-label">New Search</span></button>
      </div>
    </header>

    <main class="content">
      <!-- ============ OVERVIEW ============ -->
      <section class="view stack" id="view-overview">
        <div class="kpis">
          <div class="card kpi">
            <div class="kpi-top"><span class="label">Total candidates</span><span class="kpi-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg></span></div>
            <div class="kpi-value" data-count id="kpiTotal">0</div>
            <div class="kpi-foot">Deduplicated across all runs</div>
          </div>
          <div class="card kpi is-lime">
            <div class="kpi-top"><span class="label">Open to work</span><span class="kpi-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M2 12h4l3-7 4 14 3-7h6"/></svg></span></div>
            <div class="kpi-value" data-count id="kpiOtw">0</div>
            <div class="kpi-foot"><span id="kpiOtwPct">0%</span> of talent pool signalling</div>
            <div class="kpi-meter"><i id="kpiOtwBar" style="width:0;background:var(--lime)"></i></div>
          </div>
        </div>

        <div class="card job" id="jobCard">
          <div class="radar" id="radar"><div class="radar-sweep"></div><div class="radar-core"></div></div>
          <div class="job-body">
            <div class="job-head">
              <div style="min-width:0">
                <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
                  <span class="chip chip-muted" id="jobStatusChip"><span class="dot"></span>IDLE</span>
                  <span class="chip chip-muted" id="jobModeChip">—</span>
                  <span class="mono" style="font-size:11px;color:var(--muted)" id="jobIdText"></span>
                </div>
                <div class="job-desc" id="jobDesc">No search running</div>
              </div>
              <div style="text-align:right;flex-shrink:0">
                <div class="label">Elapsed</div>
                <div class="display" style="font-size:22px;font-weight:600;margin-top:4px;font-variant-numeric:tabular-nums" id="jobElapsed">00:00</div>
              </div>
            </div>
            <div class="job-stats">
              <div class="job-stat"><span class="label">Requested</span><b id="jobRequested">0</b></div>
              <div class="job-stat"><span class="label">Collected</span><b id="jobProcessed" style="color:var(--lime)">0</b></div>
            </div>
            <div>
              <div style="display:flex;justify-content:space-between;margin-bottom:8px"><span class="label">Progress</span><span class="mono" style="font-size:11px;color:var(--text-2)" id="jobPct">0%</span></div>
              <div class="progress"><i id="jobBar"></i></div>
            </div>
            <div class="hint" id="jobError" style="color:var(--ember)"></div>
            <div class="job-actions">
              <button class="btn btn-primary" id="btnJobNew" onclick="switchView('search')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4"><path d="M12 5v14M5 12h14"/></svg>Start a search</button>
              <button class="btn btn-danger" id="btnStop" onclick="stopJob()" disabled><svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12" rx="2"/></svg>Stop safely</button>
              <button class="btn" id="btnExportRun" onclick="exportCurrentRun()" disabled><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3v12m0 0 4-4m-4 4-4-4M4 17v2a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-2"/></svg>Export to Excel</button>
            </div>
          </div>
        </div>

        <div class="split">
          <div class="card">
            <div class="card-head">
              <div><div class="card-title">Live feed — this run</div><div class="card-sub"><span id="feedCount">0</span> candidates streamed from the active search</div></div>
              <span class="chip chip-muted" id="feedLiveChip"><span class="dot"></span>Waiting</span>
            </div>
            <div class="table-wrap">
              <table>
                <thead><tr><th class="label">Candidate</th><th class="label">Role</th><th class="label">Experience</th><th class="label">Location</th><th class="label">Contact</th><th class="label">Open to work</th><th></th></tr></thead>
                <tbody id="feedBody"></tbody>
              </table>
            </div>
          </div>
        </div>

        <div class="card">
          <div class="card-head"><div><div class="card-title">Recent runs</div><div class="card-sub">Last 10 jobs in this server session</div></div></div>
          <div class="table-wrap">
            <table>
              <thead><tr><th class="label">Job</th><th class="label">Search</th><th class="label">Status</th><th class="label">Duration</th><th class="label">Collected</th><th class="label">Started</th><th></th></tr></thead>
              <tbody id="runsBody"></tbody>
            </table>
          </div>
        </div>
      </section>

      <!-- ============ NEW SEARCH ============ -->
      <section class="view hidden" id="view-search">
        <form id="searchForm" class="form-grid" autocomplete="off">
          <div class="card">
            <div class="card-head">
              <div style="display:flex;gap:12px;align-items:flex-start"><span class="radar-mini" style="margin-top:2px"></span><div><div class="card-title">Find open-to-work candidates</div><div class="card-sub">Pick a role, a location and the seniority you need. Every search looks for people actively open to work.</div></div></div>
              <span class="chip chip-lime pulse"><span class="dot"></span>#OpenToWork</span>
            </div>
            <div class="form-body">
              <div class="grid-2">
                <div class="field">
                  <label for="inRole">Role / job title <span class="label" style="color:var(--ember)">Required</span></label>
                  <div class="geo-wrap">
                    <div class="input loc-box" id="roleBox" style="padding-left:8px">
                      <span id="roleChips" class="loc-chips"></span>
                      <input id="inRole" placeholder="Select role" autocomplete="off" role="combobox" aria-expanded="false" aria-controls="roleMenu">
                    </div>
                    <div class="geo-menu hidden" id="roleMenu" role="listbox"></div>
                  </div>
                  <div class="hint" id="roleHint">Add one or more roles. People with any of these titles are found.</div>
                </div>
                <div class="field">
                  <label for="inLocation">Location</label>
                  <div class="geo-wrap">
                    <div class="input loc-box" id="placeBox">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s-8-6.5-8-13a8 8 0 1 1 16 0c0 6.5-8 13-8 13z"/><circle cx="12" cy="9" r="3"/></svg>
                      <span id="placeChips" class="loc-chips"></span>
                      <input id="inLocation" placeholder="City, state or country" autocomplete="off">
                    </div>
                    <div class="geo-menu hidden" id="geoMenu"></div>
                  </div>
                  <div class="hint">Add one or more places. People in any of them are found.</div>
                </div>
              </div>
              <div class="field">
                <label for="inKeyword">Keywords <span class="cell-muted" style="font-weight:400">(optional)</span></label>
                <div style="display:flex;gap:10px;align-items:flex-start">
                  <div class="input loc-box" id="kwBox" style="padding-left:8px;flex:1">
                    <span id="kwChips" class="loc-chips"></span>
                    <input id="inKeyword" placeholder="e.g. Python, Kubernetes, AWS" autocomplete="off">
                  </div>
                  <select class="select" id="kwMode" style="width:auto;min-height:42px" title="How the keywords combine">
                    <option value="any">Match any</option><option value="all">Match all</option>
                  </select>
                </div>
                <div class="hint">Press Enter or comma after each keyword. Searched in the profile, like Sales Navigator's keyword box.</div>
              </div>


              <div class="field">
                <label>How many leads</label>
                <div class="seg" id="leadSeg">
                  <button type="button" data-n="25">25</button>
                  <button type="button" data-n="50" class="active">50</button>
                  <button type="button" data-n="100">100</button>
                  <button type="button" data-n="250">250</button>
                  <button type="button" data-n="500">500</button>
                </div>
              </div>

            </div>
            <div class="form-foot">
              <span class="hint">Opens Sales Navigator in a new tab. The TalentPulse extension sends the results here at a human pace; no extra LinkedIn requests.</span>
              <div style="display:flex;gap:8px">
                <button type="button" class="btn btn-ghost" onclick="switchView('overview')">Cancel</button>
                <button type="submit" class="btn btn-primary" id="btnLaunch"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M7 4v16l13-8z"/></svg><span id="btnLaunchText">Launch search</span></button>
              </div>
            </div>
          </div>

          <aside class="card summary">
            <div class="label">Search summary</div>
            <div>
              <div class="summary-row"><span class="cell-muted">Role</span><span id="sumRole">—</span></div>
              <div class="summary-row"><span class="cell-muted">Location</span><span id="sumLoc">—</span></div>
              <div class="summary-row"><span class="cell-muted">Keywords</span><span id="sumKw">—</span></div>
              
              
              <div class="summary-row"><span class="cell-muted">Leads</span><span id="sumLeads">50</span></div>
              <div class="summary-row"><span class="cell-muted">Est. time</span><span id="sumTime">~10s</span></div>
            </div>
          </aside>
        </form>
      </section>

      <!-- ============ EXPLORER ============ -->
      <section class="view hidden" id="view-explorer">
        <div class="card">
          <div class="toolbar">
            <div class="toolbar-row">
              <div class="input-icon grow">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
                <input class="input" id="exSearch" placeholder="Search name, title, company, skills…">
              </div>
              <div class="input-icon" style="flex:0 1 220px">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s-8-6.5-8-13a8 8 0 1 1 16 0c0 6.5-8 13-8 13z"/><circle cx="12" cy="9" r="3"/></svg>
                <input class="input" id="exLocation" placeholder="Location">
              </div>
              <button class="btn btn-ghost" onclick="clearFilters()">Clear</button>
              <button class="btn btn-primary" onclick="exportFiltered()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M12 3v12m0 0 4-4m-4 4-4-4M4 17v2a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-2"/></svg>Export filtered</button>
              <button class="btn" onclick="exportMaster()">Export all</button>
            </div>
            <div class="filters">
              <div class="filter"><span class="label">Role</span><select class="select select-sm" id="exRole"><option value="all">All roles</option></select></div>
              <div class="filter"><span class="label">Batch</span><select class="select select-sm" id="exBatch"><option value="all">All batches</option></select></div>
              <div class="filter"><span class="label">Open to work</span><select class="select select-sm" id="exOtw"><option value="all">All</option><option value="yes">Yes</option><option value="no">No</option></select></div>
              <div class="filter"><span class="label">Experience</span><select class="select select-sm" id="exExp" title="Years in current role, from LinkedIn"><option value="all">Any</option><option value="0-1">0–1 years</option><option value="1-3">1–3 years</option><option value="3-5">3–5 years</option><option value="5-10">5–10 years</option><option value="10+">10+ years</option></select></div>
              <div class="filter"><span class="label">Contact</span><select class="select select-sm" id="exContact"><option value="all">Any</option><option value="has_phone">Has phone</option><option value="has_email">Has email</option><option value="has_any">Phone or email</option></select></div>
              <div class="filter"><span class="label">Sort</span><select class="select select-sm" id="exSort"><option value="date_desc">Newest first</option><option value="date_asc">Oldest first</option><option value="name_asc">Name A–Z</option><option value="name_desc">Name Z–A</option><option value="location_asc">Location A–Z</option><option value="experience_desc">Most experience</option><option value="experience_asc">Least experience</option><option value="signal_date_desc">Signal date</option></select></div>
              <div class="filter" style="justify-content:flex-end;align-items:flex-end"><span class="count-badge"><b id="exCount">0</b> candidates</span></div>
            </div>
          </div>
          <div class="table-wrap">
            <table>
              <thead><tr><th class="label">Candidate</th><th class="label">Role</th><th class="label">Experience</th><th class="label">Location</th><th class="label">Contact</th><th class="label">Open to work</th><th></th></tr></thead>
              <tbody id="exBody"></tbody>
            </table>
          </div>
        </div>
      </section>
    </main>
  </div>
</div>

<!-- ================= Drawer ================= -->
<div class="scrim" id="scrim" onclick="closeDrawer()"></div>
<aside class="drawer" id="drawer" aria-label="Candidate details" aria-hidden="true">
  <div class="drawer-head">
    <div class="avatar" id="dwAvatar">—</div>
    <div style="flex:1;min-width:0;position:relative">
      <div class="drawer-name" id="dwName">—</div>
      <div class="cell-muted" style="margin-top:4px;font-size:13px" id="dwHeadline">—</div>
      <div style="display:flex;gap:6px;margin-top:12px;flex-wrap:wrap" id="dwChips"></div>
    </div>
    <button class="btn btn-icon btn-ghost" onclick="closeDrawer()" aria-label="Close" style="position:relative"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 6 6 18M6 6l12 12"/></svg></button>
  </div>
  <div class="drawer-body">
    <div class="section"><h4 class="label">Open-to-work evidence</h4><div class="evidence" id="dwEvidence"></div></div>
    <div class="section"><h4 class="label">Professional</h4><dl class="kv" id="dwPro"></dl></div>
    <div class="section"><h4 class="label">Skills</h4><div class="skills" id="dwSkills"></div></div>
    <div class="section"><h4 class="label">Contact</h4><div id="dwContact"></div><div class="hint" style="margin-top:10px" id="dwContactNote"></div></div>
    <div class="section"><h4 class="label">Record</h4><dl class="kv" id="dwMeta"></dl></div>
  </div>
  <div class="drawer-foot">
    <button class="btn btn-primary" id="dwVerify" onclick="drawerAction('verify')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M21 12a9 9 0 1 1-2.64-6.36M21 3v6h-6"/></svg>Re-check live status</button>
    <button class="btn" onclick="drawerAction('mark_open_to_work')">Mark open to work</button>
    <button class="btn btn-ghost" onclick="drawerAction('mark_not_looking')">Mark not looking</button>
    <span style="flex:1"></span>
    <a class="btn" id="dwLinkedIn" target="_blank" rel="noopener">Open LinkedIn profile ↗</a>
    <button class="btn btn-ghost" onclick="closeDrawer()">Close</button>
  </div>
</aside>

<div class="toasts" id="toasts"></div>

<script>
// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const state = {
  view: 'overview',
  job: null,
  sessionLeads: [],
  explorerLeads: [],
  drawerLead: null,
  targetLeads: 50,
  geoResults: [],
  geoFocus: -1,
  places: [],  // picked locations (chips); empty = user's choice
  // This browser's own search; other testers' searches on the same server are not shown here
  myJobId: (() => { try { return localStorage.getItem('tp.jobId') || ''; } catch (e) { return ''; } })(),
  roles: [],
  keywords: [],  // extra keywords, on top of 'Open to work'  // picked titles [{text, id}], OR'd like Sales Navigator's title filter
  roleResults: [],
  roleFocus: 0,
  stats: null,
};
let sse = null, statusTimer = null, elapsedTimer = null, exTimer = null, geoTimer = null, roleTimer = null, roleSeq = 0;
const seenLogKeys = new Set();

const VIEW_META = {
  overview: ['Live Sales Navigator sourcing', 'Overview'],
  search: ['Open-to-work sourcing', 'New Search'],
  explorer: ['Talent pool directory', 'Lead Explorer'],
};

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
const $ = (id) => document.getElementById(id);
function esc(v) {
  return String(v ?? '').replace(/[&<>"'`]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;','`':'&#96;'}[c]));
}
function safeUrl(u) { return /^https?:\/\//i.test(u || '') ? u : ''; }
function initials(name) {
  const p = String(name || '').trim().split(/\s+/).filter(Boolean);
  if (!p.length) return '?';
  return ((p[0][0] || '') + (p.length > 1 ? p[p.length - 1][0] : '')).toUpperCase();
}
function fmt(n) { return Number(n || 0).toLocaleString(); }
function isOtw(l) { return l && (l.otw_status === 'Yes' || l.is_open_to_work === true); }
function otwChip(l) {
  return isOtw(l)
    ? `<span class="chip chip-lime" title="${esc(l.signal_description)}"><span class="dot"></span>Yes</span>`
    : `<span class="chip chip-muted">No</span>`;
}
function avatar(l) { return `<div class="avatar${isOtw(l) ? ' otw' : ''}">${esc(initials(l.full_name))}</div>`; }
function personCell(l) {
  return `<div class="person">${avatar(l)}<div style="min-width:0"><div class="person-name">${esc(l.full_name)}</div><div class="person-meta">${esc(fmtUTC(l.discovered_at))}</div></div></div>`;
}
function roleCell(l) {
  const co = l.company_name && l.company_name !== 'Not available' ? `<small>${esc(l.company_name)}</small>` : '';
  return `<div class="cell-title">${esc(l.current_title)}${co}</div>`;
}
function toast(msg, level = 'info') {
  const t = document.createElement('div');
  t.className = `toast ${level}`;
  t.innerHTML = `<span class="dot"></span><span>${esc(msg)}</span>`;
  $('toasts').appendChild(t);
  setTimeout(() => { t.classList.add('out'); setTimeout(() => t.remove(), 300); }, 3800);
}
function countUp(el, to) {
  const from = Number(el.dataset.val || 0);
  el.dataset.val = to;
  if (from === to) { el.textContent = fmt(to); return; }
  const start = performance.now(), dur = 700;
  const step = (now) => {
    const p = Math.min(1, (now - start) / dur), e = 1 - Math.pow(1 - p, 3);
    el.textContent = fmt(Math.round(from + (to - from) * e));
    if (p < 1) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
}
function fmtUTC(ts) {
  if (!ts) return '';
  const d = new Date(String(ts).replace(' ', 'T') + 'Z');
  if (isNaN(d)) return String(ts);
  return d.toLocaleString('en-IN', { day: '2-digit', month: 'short', year: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true });
}
function parseLocal(ts) { if (!ts) return null; const d = new Date(String(ts).replace(' ', 'T')); return isNaN(d) ? null : d; }
function mmss(sec) { sec = Math.max(0, Math.floor(sec)); const h = Math.floor(sec / 3600), m = Math.floor(sec % 3600 / 60), s = sec % 60; return (h ? h + ':' : '') + String(m).padStart(2, '0') + ':' + String(s).padStart(2, '0'); }

// ---------------------------------------------------------------------------
// Theme & navigation
// ---------------------------------------------------------------------------
function applyTheme(t) {
  document.documentElement.dataset.theme = t;
  $('themeIcon').innerHTML = t === 'dark'
    ? '<path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/>'
    : '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/>';
}
function toggleTheme() {
  const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
  applyTheme(next);
  try { localStorage.setItem('tp-theme', next); } catch (e) {}
}
function toggleSidebar() { $('sidebar').classList.toggle('open'); }
// Phone menu covers the ☰ button, so close it on an outside tap or Escape
document.addEventListener('click', (e) => {
  if ($('sidebar').classList.contains('open') && !e.target.closest('#sidebar, .menu-btn')) $('sidebar').classList.remove('open');
});
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') $('sidebar').classList.remove('open'); });
function switchView(v) {
  state.view = v;
  ['overview', 'search', 'explorer'].forEach(x => $('view-' + x).classList.toggle('hidden', x !== v));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.toggle('active', b.dataset.view === v));
  $('pageEyebrow').textContent = VIEW_META[v][0];
  $('pageTitle').textContent = VIEW_META[v][1];
  $('sidebar').classList.remove('open');
  const el = $('view-' + v); el.style.animation = 'none'; el.offsetHeight; el.style.animation = '';
  if (v === 'explorer') { loadBatches(); loadExplorer(); }
  if (v === 'search') {
    updateSummary();
    const f = $('searchForm'); f.classList.remove('enter'); void f.offsetWidth; f.classList.add('enter');
  }
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

// ---------------------------------------------------------------------------
// Status / overview
// ---------------------------------------------------------------------------
async function fetchStatus() {
  try {
    const res = await fetch('/api/status?job_id=' + encodeURIComponent(state.myJobId));
    const data = await res.json();
    renderStats(data.stats || {});
    renderJob(data.job || {});
    renderRuns(data.recent_jobs || []);
    (data.activity_logs || []).forEach(appendLog);
    if (Array.isArray(data.session_leads)) {
      const known = new Set(state.sessionLeads.map(l => l.linkedin_url));
      const fresh = data.session_leads.filter(l => !known.has(l.linkedin_url));
      if (fresh.length || data.session_leads.length !== state.sessionLeads.length) {
        state.sessionLeads = data.session_leads.slice().reverse();
        renderFeed();
      }
    }
  } catch (e) { /* server unreachable; SSE badge reflects it */ }
}
function scheduleStatus() { clearTimeout(statusTimer); statusTimer = setTimeout(fetchStatus, 700); }

function renderStats(s) {
  state.stats = s;
  const total = s.db_total || 0, otw = s.db_otw || 0;
  countUp($('kpiTotal'), total); countUp($('kpiOtw'), otw);
  const pct = (n) => total ? Math.round(n / total * 100) : 0;
  $('kpiOtwPct').textContent = pct(otw) + '%';
  $('kpiOtwBar').style.width = pct(otw) + '%';
  $('navLeadCount').textContent = fmt(total);
  const cap = s.daily_cap || 2000, used = s.daily_used || 0;
  $('capTotal').textContent = fmt(cap);
  $('capUsed').textContent = fmt(used);
  $('capBar').style.width = Math.min(100, used / cap * 100) + '%';
  $('capBar').style.background = used >= cap ? 'var(--ember)' : used >= cap * 0.8 ? 'var(--amber)' : 'var(--lime)';
  $('quotaHint').textContent = used >= cap ? 'Quota reached. Live searches resume as the 24-hour window rolls.' : `${fmt(cap - used)} live leads left in the last 24 hours`;
}

function renderJob(job) {
  state.job = job;
  const status = job.status || 'idle';
  const running = status === 'running', stopping = status === 'stopping';
  const card = $('jobCard');
  card.classList.toggle('is-running', running);
  card.classList.toggle('is-stopping', stopping);
  card.classList.toggle('is-error', status === 'error');

  const chipMap = { idle: ['chip-muted', 'Idle'], waiting: ['chip-amber pulse', 'Waiting for Sales Navigator'], running: ['chip-sky pulse', 'Running'], stopping: ['chip-amber pulse', 'Stopping'], completed: ['chip-lime', 'Completed'], stopped: ['chip-amber', 'Stopped'], error: ['chip-ember', 'Error'] };
  const [cls, label] = chipMap[status] || chipMap.idle;
  $('jobStatusChip').className = 'chip ' + cls;
  $('jobStatusChip').innerHTML = `<span class="dot"></span>${label}`;
  const hasJob = !!job.job_id;
  $('jobModeChip').classList.toggle('hidden', !hasJob);
  $('jobModeChip').className = 'chip ' + (job.mode === 'dry_run' ? 'chip-sky' : 'chip-muted') + (hasJob ? '' : ' hidden');
  $('jobModeChip').textContent = job.mode === 'dry_run' ? 'Dry run' : 'Live LinkedIn';
  $('jobIdText').textContent = job.job_id || '';
  $('jobDesc').textContent = job.search_description || 'No search running';

  const processed = job.total_processed || 0, maxPages = job.max_pages || 0;
  $('jobRequested').textContent = hasJob ? fmt(job.target_leads || 0) : '0';
  $('jobProcessed').textContent = fmt(processed);
  let pct = 0;
  if (['completed', 'stopped'].includes(status)) pct = 100;
  else if (hasJob && maxPages) pct = Math.min(99, Math.round(processed / (maxPages * 25) * 100));
  $('jobPct').textContent = pct + '%';
  $('jobBar').style.width = pct + '%';
  $('jobBar').style.background = status === 'error' ? 'var(--ember)' : status === 'stopped' ? 'var(--amber)' : 'var(--lime)';
  $('jobError').textContent = (status === 'error' || status === 'completed') && job.error_message ? job.error_message : '';

  $('btnStop').disabled = !running;
  $('btnJobNew').disabled = running || stopping;
  $('btnExportRun').disabled = !hasJob || processed === 0;

  const live = $('feedLiveChip');
  live.className = 'chip ' + (running ? 'chip-lime pulse' : hasJob ? 'chip-muted' : 'chip-muted');
  live.innerHTML = `<span class="dot"></span>${running ? 'Streaming' : hasJob ? 'Finished' : 'Waiting'}`;

  clearInterval(elapsedTimer);
  const started = parseLocal(job.started_at);
  const tick = () => {
    if (!started) { $('jobElapsed').textContent = '00:00'; return; }
    const end = parseLocal(job.completed_at) || new Date();
    $('jobElapsed').textContent = mmss((end - started) / 1000);
  };
  tick();
  if (running || stopping) elapsedTimer = setInterval(tick, 1000);
}

function renderFeed(newUrl) {
  const body = $('feedBody');
  $('feedCount').textContent = fmt(state.sessionLeads.length);
  if (!state.sessionLeads.length) {
    body.innerHTML = `<tr><td colspan="7"><div class="empty"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1"/></svg><b>Radar is quiet</b>Start a search and candidates will stream in here as they're found.</div></td></tr>`;
    return;
  }
  body.innerHTML = state.sessionLeads.map((l, i) => leadRow(l, i, 'feed', l.linkedin_url === newUrl)).join('');
}
const PHONE_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 16.9v3a2 2 0 0 1-2.2 2 19.8 19.8 0 0 1-8.6-3.1 19.5 19.5 0 0 1-6-6A19.8 19.8 0 0 1 2.1 4.2 2 2 0 0 1 4.1 2h3a2 2 0 0 1 2 1.7c.1.9.4 1.8.7 2.7a2 2 0 0 1-.5 2.1L8 9.8a16 16 0 0 0 6 6l1.3-1.3a2 2 0 0 1 2.1-.4c.9.3 1.8.6 2.7.7a2 2 0 0 1 1.7 2z"/></svg>';
const MAIL_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="4" width="20" height="16" rx="2"/><path d="m22 6-10 7L2 6"/></svg>';
const REFRESH_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12a9 9 0 1 1-2.64-6.36M21 3v6h-6"/></svg>';
function fmtExp(v) { return (v === null || v === undefined || v === '') ? '' : `${Number(v).toFixed(1).replace(/\.0$/, '')} yrs`; }
function expCell(l) { const t = fmtExp(l.years_experience); return t ? `<span class="mono" style="font-weight:600">${t}</span>` : '<span class="cell-muted">—</span>'; }
// One row layout shared by the live feed and the Lead Explorer
function leadRow(l, i, src, isNew) {
  return `<tr class="clickable${isNew ? ' is-new' : ''}" data-i="${i}" data-src="${src}">
    <td>${personCell(l)}</td><td>${roleCell(l)}</td>
    <td>${expCell(l)}</td>
    <td class="cell-muted"><div class="trunc">${esc(l.location)}</div></td>
    <td><div class="contact-icons"><span class="ci${l.phone ? ' on' : ''}" title="${esc(l.phone || 'No phone')}">${PHONE_SVG}</span><span class="ci${l.email ? (['Verified', 'From profile'].includes(l.email_status) || !l.email_status ? ' on' : ' warn') : ''}" title="${esc(l.email ? l.email + (l.email_status ? ' · ' + l.email_status : '') : 'No email')}">${MAIL_SVG}</span></div></td>
    <td>${otwChip(l)}</td>
    <td><div class="row-actions">
      <button class="btn btn-sm btn-icon" title="Re-check live status" data-act="verify" data-i="${i}" data-src="${src}">${REFRESH_SVG}</button>
      ${detailsBtn()}${linkedinBtn(l)}
    </div></td>
  </tr>`;
}
const EYE_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>';
const EXT_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M15 3h6v6M10 14 21 3M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/></svg>';
function detailsBtn() { return `<button class="btn btn-sm btn-icon" title="View details">${EYE_SVG}</button>`; }
function linkedinBtn(l) {
  const li = safeUrl(l.linkedin_url);
  return li ? `<a class="btn btn-sm btn-icon" href="${esc(li)}" target="_blank" rel="noopener" title="Open on LinkedIn" data-stop="1">${EXT_SVG}</a>` : '';
}

function renderRuns(runs) {
  const body = $('runsBody');
  if (!runs.length) { body.innerHTML = `<tr><td colspan="8"><div class="empty" style="padding:32px">No runs yet in this server session.</div></td></tr>`; return; }
  const chip = { completed: 'chip-lime', stopped: 'chip-amber', error: 'chip-ember' };
  body.innerHTML = runs.map(r => `
    <tr>
      <td class="mono" style="font-size:12px">${esc(r.job_id)}</td>
      <td>${esc(r.search_description)}</td>
      <td><span class="chip ${chip[r.status] || 'chip-muted'}"><span class="dot"></span>${esc(r.status)}</span></td>
      <td class="mono cell-muted" style="font-size:12px">${esc(r.duration || '—')}</td>
      <td class="mono">${fmt(r.total_processed)}</td>
      <td class="mono cell-muted" style="font-size:12px">${esc(r.started_at)}</td>
      <td><button class="btn btn-sm" onclick="exportJob('${esc(r.job_id)}')">CSV</button></td>
    </tr>`).join('');
}

function appendLog(entry) {
  const key = entry.time + '|' + entry.message;
  if (seenLogKeys.has(key)) return;
  seenLogKeys.add(key);
  const box = $('logBox');
  if (!box) return;
  const empty = box.querySelector('.empty'); if (empty) empty.remove();
  const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
  box.insertAdjacentHTML('beforeend', `<div class="log-line ${esc(entry.level)}"><time>${esc(entry.time)}</time><i class="lv"></i><span>${esc(entry.message)}</span></div>`);
  while (box.children.length > 200) box.firstElementChild.remove();
  if (atBottom) box.scrollTop = box.scrollHeight;
}

function radarBlip() {
  const r = $('radar');
  const a = Math.random() * Math.PI * 2, d = 18 + Math.random() * 26;
  const b = document.createElement('div');
  b.className = 'radar-blip';
  b.style.left = (50 + Math.cos(a) * d) + '%';
  b.style.top = (50 + Math.sin(a) * d) + '%';
  r.appendChild(b);
  setTimeout(() => b.remove(), 2700);
}

// ---------------------------------------------------------------------------
// Server-Sent Events
// ---------------------------------------------------------------------------
function setConn(ok, text) {
  $('sseBadge').firstElementChild.className = 'chip ' + (ok ? 'chip-lime' : 'chip-amber pulse');
  $('sseText').textContent = text;
}
function initSSE() {
  if (sse) sse.close();
  sse = new EventSource('/api/events');
  sse.onopen = () => setConn(true, 'Live stream connected');
  sse.onerror = () => setConn(false, 'Reconnecting…');
  sse.onmessage = (e) => {
    let msg; try { msg = JSON.parse(e.data); } catch (err) { return; }
    const isMine = (j) => !j || !j.job_id || j.job_id === state.myJobId;
    if ((msg.type === 'init' || msg.type === 'job_status') && !isMine(msg.data)) { scheduleStatus(); return; }
    if (msg.type === 'lead' && msg.data && !isMine(msg.data.job)) {
      if (typeof msg.data.db_total === 'number') countUp($('kpiTotal'), msg.data.db_total);
      return;
    }
    if (msg.type === 'init' || msg.type === 'job_status') {
      const prev = state.job && state.job.status;
      renderJob(msg.data || {});
      if (msg.data && msg.data.status === 'running' && prev !== 'running' && msg.data.job_id && msg.data.total_processed === 0) {
        state.sessionLeads = []; renderFeed();
      }
      if (prev === 'running' && ['completed', 'stopped', 'error'].includes(msg.data.status)) {
        if (msg.data.status === 'completed' && msg.data.error_message) toast(msg.data.error_message, 'warning'); else toast(msg.data.status === 'error' ? 'Search failed: ' + (msg.data.error_message || 'unknown error') : `Search ${msg.data.status}: ${fmt(msg.data.total_processed)} candidates collected`, msg.data.status === 'error' ? 'error' : 'success');
        if (state.view === 'explorer') loadExplorer();
      }
      scheduleStatus();
    } else if (msg.type === 'lead_email') {
      // Emails are checked in the background and arrive a few seconds after the person
      const l = state.sessionLeads.find(x => x.linkedin_url === msg.data.linkedin_url);
      if (l && msg.data.email) { l.email = msg.data.email; l.email_status = msg.data.email_status; renderFeed(); }
    } else if (msg.type === 'lead') {
      const lead = msg.data.lead;
      if (msg.data.job) renderJob(msg.data.job);
      if (lead && !state.sessionLeads.some(l => l.linkedin_url === lead.linkedin_url)) {
        state.sessionLeads.unshift(lead);
        renderFeed(lead.linkedin_url);
        if (isOtw(lead)) radarBlip();
      }
      if (typeof msg.data.db_total === 'number') countUp($('kpiTotal'), msg.data.db_total);
      scheduleStatus();
    } else if (msg.type === 'activity_log') {
      appendLog(msg.data);
    }
  };
}

// ---------------------------------------------------------------------------
// Job control
// ---------------------------------------------------------------------------
async function stopJob() {
  $('btnStop').disabled = true;
  try { await fetch('/api/stop?job_id=' + encodeURIComponent(state.myJobId), { method: 'POST' }); toast('Stop requested. Finishing the current page, then stopping…', 'warning'); }
  catch (e) { toast('Could not reach the server to stop the job.', 'error'); $('btnStop').disabled = false; }
}
function exportJob(id) { if (id) window.location.href = '/api/export-csv?job_id=' + encodeURIComponent(id); }
function exportCurrentRun() { exportJob(state.job && state.job.job_id); }

// ---------------------------------------------------------------------------
// Search form
// ---------------------------------------------------------------------------
function renderSummary() {
  const n = state.targetLeads, pages = Math.max(1, Math.ceil(n / 25));
  $('sumRole').textContent = roleList().join(', ') || '—';
  $('sumLoc').textContent = placeList().join('; ') || 'Global';
  $('sumKw').textContent = state.keywords.join($('kwMode').value === 'all' ? ' + ' : ' / ') || '—';
  $('sumLeads').textContent = String(n);
  const secs = pages * 3 * 7;  // about 3 result pages per page of people, ~7 s each
  $('sumTime').textContent = secs < 60 ? `~${secs}s` : `~${Math.round(secs / 60)} min`;
}

$('leadSeg').addEventListener('click', (e) => {
  const b = e.target.closest('button'); if (!b) return;
  state.targetLeads = Number(b.dataset.n);
  $('leadSeg').querySelectorAll('button').forEach(x => x.classList.toggle('active', x === b));
  updateSummary();
});

$('searchForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  if (state.job && ['running', 'stopping'].includes(state.job.status)) { toast('A search is already running. Stop it or wait for it to finish.', 'warning'); return; }
  closeRoleMenu();
  if ($('inRole').value.trim()) pickRole($('inRole').value, '');  // typed but not yet added
  addKeywords($('inKeyword').value);
  if (!roleList().length) { toast('Enter the role or job title you are hiring for.', 'warning'); $('inRole').focus(); return; }
  const payload = {
    role: roleList().join(', '),
    role_titles: state.roles.map(r => ({ text: r.text, id: r.id || '' })),
    location: placeList().join('; ') || null,
    locations: state.places.map(p => ({ text: p.text, id: p.id || '' })),
    keywords: 'Open to work',
    extra_keywords: state.keywords,
    keyword_mode: $('kwMode').value,
    target_leads: state.targetLeads,
    dry_run: false,
    replace_job_id: state.myJobId || null,
  };
  const btn = $('btnLaunch'); btn.disabled = true;
  btn.classList.add('is-launching'); $('btnLaunchText').textContent = 'Launching…';
  try {
    const res = await fetch('/api/search/prepare', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    const data = await res.json();
    if (!res.ok || data.error) { toast(data.error || (data.detail ? JSON.stringify(data.detail) : 'Could not start the search.'), 'error'); return; }
    state.myJobId = data.job_id || '';
    try { localStorage.setItem('tp.jobId', state.myJobId); } catch (e) {}
    state.sessionLeads = []; renderFeed();
    // The search runs in your own Sales Navigator tab; the TalentPulse extension sends the results here
    // The extension opens Sales Navigator in a background tab, so you stay here and watch leads arrive
    if (document.documentElement.dataset.talentpulseExt) {
      window.postMessage({ source: 'talentpulse-dashboard', type: 'open', url: data.url, jobId: data.job_id }, location.origin);
      toast(`Searching Sales Navigator in the background: ${data.search_description}`, 'success');
    } else {
      window.open(data.url, '_blank', 'noopener');
      toast('TalentPulse extension not detected, so Sales Navigator opened in a new tab. Install or reload the extension to stay on this page.', 'warning');
    }
    switchView('overview');
  } catch (err) { toast('Server unreachable: ' + err.message, 'error'); }
  finally { btn.disabled = false; btn.classList.remove('is-launching'); $('btnLaunchText').textContent = 'Launch search'; }
});

// Summary values flash when they change
function updateSummary() {
  const els = [...document.querySelectorAll('.summary-row span:last-child')];
  const before = els.map(el => el.textContent);
  renderSummary();
  els.forEach((el, i) => {
    if (el.textContent !== before[i]) { el.classList.remove('flash'); void el.offsetWidth; el.classList.add('flash'); }
  });
}

// Role typeahead: ONE title, shown in the box itself (like Location). Suggestions come from
// LinkedIn's title list when logged in, otherwise the built-in catalogue.
// Keywords: free words as chips, combined with "any" (OR) or "all" (AND)
const MAX_KEYWORDS = 10;
function renderKeywords() {
  $('kwChips').innerHTML = state.keywords.map((k, i) =>
    `<span class="loc-chip"><span>${esc(k)}</span><button type="button" data-rm="${i}" aria-label="Remove ${esc(k)}">×</button></span>`).join('');
  $('inKeyword').placeholder = state.keywords.length ? 'Add another keyword…' : 'e.g. Python, Kubernetes, AWS';
  updateSummary();
}
function addKeywords(text) {
  String(text || '').split(',').map(k => k.replace(/["()]/g, ' ').replace(/\s+/g, ' ').trim()).filter(Boolean).forEach(k => {
    if (state.keywords.some(x => x.toLowerCase() === k.toLowerCase())) return;
    if (state.keywords.length >= MAX_KEYWORDS) { toast('You can add up to 10 keywords.', 'warning'); return; }
    state.keywords.push(k);
  });
  $('inKeyword').value = '';
  renderKeywords();
}
$('inKeyword').addEventListener('keydown', (e) => {
  if (e.key === 'Enter' || e.key === ',') { e.preventDefault(); addKeywords(e.target.value); }
  else if (e.key === 'Backspace' && !e.target.value && state.keywords.length) { state.keywords.pop(); renderKeywords(); }
});
$('inKeyword').addEventListener('blur', (e) => { if (e.target.value.trim()) addKeywords(e.target.value); });
$('kwChips').addEventListener('click', (e) => {
  const b = e.target.closest('button[data-rm]'); if (!b) return;
  state.keywords.splice(Number(b.dataset.rm), 1); renderKeywords(); $('inKeyword').focus();
});
$('kwBox').addEventListener('click', (e) => { if (!e.target.closest('button')) $('inKeyword').focus(); });
$('kwMode').addEventListener('change', updateSummary);

// Roles: several titles (OR'd), shown as chips like the places
const MAX_ROLES = 5;
function roleList() { return state.roles.map(r => r.text); }
function renderRoles() {
  $('roleChips').innerHTML = state.roles.map((r, i) =>
    `<span class="loc-chip"><span>${esc(r.text)}</span><button type="button" data-rm="${i}" aria-label="Remove ${esc(r.text)}">×</button></span>`).join('');
  const full = state.roles.length >= MAX_ROLES;
  $('inRole').disabled = full;
  $('inRole').placeholder = full ? 'Maximum 5 roles' : state.roles.length ? 'Add another role…' : 'Select role';
  updateSummary();
}
function pickRole(text, id) {
  text = String(text || '').replace(/\s+/g, ' ').trim();
  if (!text) return;
  if (!state.roles.some(r => r.text.toLowerCase() === text.toLowerCase())) {
    if (state.roles.length >= MAX_ROLES) { toast('You can add up to 5 roles.', 'warning'); return; }
    state.roles.push({ text, id: id || '' });
  }
  $('inRole').value = '';
  closeRoleMenu();
  renderRoles();
}
$('roleChips').addEventListener('click', (e) => {
  const b = e.target.closest('button[data-rm]'); if (!b) return;
  state.roles.splice(Number(b.dataset.rm), 1); renderRoles(); $('inRole').focus();
});
$('roleBox').addEventListener('click', (e) => { if (!e.target.closest('button')) $('inRole').focus(); });
function closeRoleMenu() { $('roleMenu').classList.add('hidden'); $('inRole').setAttribute('aria-expanded', 'false'); }
function highlight(name, q) {
  const words = q.toLowerCase().split(/[^a-z0-9+#.]+/).filter(w => w.length > 1);
  let out = esc(name);
  words.forEach(w => { out = out.replace(new RegExp('\\b(' + w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')', 'ig'), '<mark>$1</mark>'); });
  return out;
}
function roleItems(q) {
  const items = state.roleResults.map(r => ({ text: r.name, id: r.id || '', catalog: true, r }));
  const typed = q.replace(/\s+/g, ' ').trim();
  if (typed && !items.some(it => it.text.toLowerCase() === typed.toLowerCase())) items.push({ text: typed, id: '', own: true });
  return items;
}
function renderRoleMenu(q) {
  const items = roleItems(q), menu = $('roleMenu');
  state.roleFocus = Math.min(state.roleFocus, items.length - 1);
  menu.innerHTML = items.map((it, i) => {
    if (it.own) return `<div class="geo-item use-own${i === state.roleFocus ? ' focus' : ''}" data-i="${i}" role="option"><div>Use “${esc(it.text)}” as written<small class="ri-sub">Matched loosely: plurals, spacing and short forms are understood</small></div></div>`;
    const r = it.r;
    const sub = r.source === 'linkedin' ? 'LinkedIn standard title'
      : r.matched ? `Matches “${esc(r.matched)}”`
      : (r.aliases && r.aliases.length ? 'Also finds: ' + esc(r.aliases.slice(0, 3).join(', ')) : 'Standard title');
    return `<div class="geo-item${i === state.roleFocus ? ' focus' : ''}" data-i="${i}" role="option"><div>${highlight(it.text, q)}<small class="ri-sub">${sub}</small></div>${r.source === 'linkedin' ? '<span class="chip chip-lime" style="height:20px;font-size:10px">LinkedIn</span>' : ''}</div>`;
  }).join('');
  menu.classList.toggle('hidden', !items.length);
  $('inRole').setAttribute('aria-expanded', String(!!items.length));
  const f = menu.querySelector('.geo-item.focus'); if (f) f.scrollIntoView({ block: 'nearest' });
}
async function roleSearch(q) {
  const seq = ++roleSeq;
  try {
    const res = await fetch('/api/titles/search?q=' + encodeURIComponent(q));
    const data = await res.json();
    if (seq !== roleSeq || $('inRole').value.trim() !== q) return;  // a newer keystroke won
    state.roleResults = data.results || [];
    state.roleFocus = 0;
    renderRoleMenu(q);
  } catch (e) {}
}
$('inRole').addEventListener('input', (e) => {
  updateSummary();
  clearTimeout(roleTimer);
  const q = e.target.value.trim();
  if (!q) { closeRoleMenu(); return; }
  state.roleResults = []; state.roleFocus = 0; renderRoleMenu(q);  // "use as written" shows instantly
  roleTimer = setTimeout(() => roleSearch(q), 120);
});
$('inRole').addEventListener('keydown', (e) => {
  const q = $('inRole').value.trim(), items = roleItems(q), open = !$('roleMenu').classList.contains('hidden');
  if ((e.key === 'ArrowDown' || e.key === 'ArrowUp') && open && items.length) {
    e.preventDefault();
    state.roleFocus = (state.roleFocus + (e.key === 'ArrowDown' ? 1 : -1) + items.length) % items.length;
    renderRoleMenu(q);
  } else if (e.key === 'Enter' && open && items.length) {
    e.preventDefault();
    const it = items[state.roleFocus]; if (it) pickRole(it.text, it.id);
  } else if (e.key === 'Escape') closeRoleMenu();
  else if (e.key === 'Backspace' && !e.target.value && state.roles.length) { state.roles.pop(); renderRoles(); }
});
$('roleMenu').addEventListener('mousedown', (e) => {
  const el = e.target.closest('.geo-item[data-i]'); if (!el) return;
  e.preventDefault();
  const it = roleItems($('inRole').value.trim())[Number(el.dataset.i)];
  if (it) pickRole(it.text, it.id);
});
document.addEventListener('click', (e) => { if (!e.target.closest('#roleBox, #roleMenu')) closeRoleMenu(); });

// Location: several places (OR'd), like Sales Navigator's location filter.
// Picked places become chips; each keeps its LinkedIn geo ID when it has one.
const MAX_PLACES = 10;
function placeList() { return state.places.map(p => p.text); }
function renderPlaces() {
  $('placeChips').innerHTML = state.places.map((p, i) =>
    `<span class="loc-chip${p.id ? '' : ' free'}" title="${p.id ? 'LinkedIn location' : 'Searched by name, then filtered to this place'}"><span>${esc(p.text)}</span><button type="button" data-rm="${i}" aria-label="Remove ${esc(p.text)}">×</button></span>`).join('');
  const full = state.places.length >= MAX_PLACES;
  $('inLocation').disabled = full;
  $('inLocation').placeholder = full ? 'Maximum 10 places' : state.places.length ? 'Add another place…' : 'City, state or country';
  updateSummary();
}
function addPlace(text, id) {
  text = String(text || '').replace(/\s+/g, ' ').trim();
  if (!text) return;
  if (!state.places.some(p => p.text.toLowerCase() === text.toLowerCase())) {
    if (state.places.length >= MAX_PLACES) { toast('You can add up to 10 places.', 'warning'); return; }
    state.places.push({ text, id: id || '' });
  }
  $('inLocation').value = '';
  $('geoMenu').classList.add('hidden');
  renderPlaces();
}
$('inLocation').addEventListener('input', (e) => {
  clearTimeout(geoTimer);
  const q = e.target.value.trim();
  if (q.length < 2) { $('geoMenu').classList.add('hidden'); return; }
  geoTimer = setTimeout(() => geoSearch(q), 160);
});
$('inLocation').addEventListener('keydown', (e) => {
  const menu = $('geoMenu'), open = !menu.classList.contains('hidden');
  if ((e.key === 'ArrowDown' || e.key === 'ArrowUp') && open && state.geoResults.length) {
    e.preventDefault();
    state.geoFocus = (state.geoFocus + (e.key === 'ArrowDown' ? 1 : -1) + state.geoResults.length) % state.geoResults.length;
    menu.querySelectorAll('.geo-item').forEach((el, i) => el.classList.toggle('focus', i === state.geoFocus));
  } else if (e.key === 'Enter' && open && state.geoFocus >= 0) { e.preventDefault(); pickGeo(state.geoFocus); }
  else if (e.key === 'Enter' && e.target.value.trim()) { e.preventDefault(); }  // wait for suggestions
  else if (e.key === 'Backspace' && !e.target.value && state.places.length) { state.places.pop(); renderPlaces(); }
  else if (e.key === 'Escape') menu.classList.add('hidden');
});
document.addEventListener('click', (e) => { if (!e.target.closest('.geo-wrap')) $('geoMenu').classList.add('hidden'); });
$('placeBox').addEventListener('click', (e) => {
  const rm = e.target.closest('[data-rm]');
  if (rm) { state.places.splice(Number(rm.dataset.rm), 1); renderPlaces(); }
  if (!$('inLocation').disabled) $('inLocation').focus();
});
async function geoSearch(q) {
  try {
    const res = await fetch('/api/geo/search?q=' + encodeURIComponent(q));
    const data = await res.json();
    if ($('inLocation').value.trim() !== q) return;  // a newer keystroke won
    state.geoResults = data.results || [];
    const menu = $('geoMenu');
    state.geoFocus = state.geoResults.length ? 0 : -1;  // Enter picks the top match, like LinkedIn
    if (!state.geoResults.length) { menu.innerHTML = '<div class="geo-item" style="cursor:default;color:var(--muted)">No matching place. Check the spelling or try the city or country name.</div>'; }
    else menu.innerHTML = state.geoResults.map((r, i) => {
      const added = placeList().some(t => t.toLowerCase() === r.name.toLowerCase());
      const how = added ? 'Already added'
        : r.has_geo_id ? `${esc(r.type)} · ${esc(r.country)}`
        : r.search_as ? `${esc(r.type)} · searched within ${esc(r.search_as)}, then filtered to this place`
        : `${esc(r.type)} · ${esc(r.country)} · matched by name`;
      return `<div class="geo-item${i === state.geoFocus ? ' focus' : ''}" data-i="${i}"${added ? ' style="opacity:.55"' : ''}><div>${highlight(r.name, q)}<small class="ri-sub">${how}</small></div>${r.has_geo_id ? '<span class="chip chip-lime" style="height:20px;font-size:10px">LinkedIn</span>' : ''}</div>`;
    }).join('');
    menu.classList.remove('hidden');
  } catch (e) {}
}
$('geoMenu').addEventListener('mousedown', (e) => {
  const it = e.target.closest('.geo-item[data-i]'); if (!it) return;
  e.preventDefault(); pickGeo(Number(it.dataset.i)); $('inLocation').focus();
});
function pickGeo(i) {
  const r = state.geoResults[i]; if (!r) return;
  addPlace(r.name, r.has_geo_id ? (r.id || '') : '');
}
renderPlaces();

// ---------------------------------------------------------------------------
// Explorer
// ---------------------------------------------------------------------------
function explorerParams() {
  const p = new URLSearchParams();
  const s = $('exSearch').value.trim(), loc = $('exLocation').value.trim();
  if (s) p.set('search', s);
  if (loc) p.set('location', loc);
  p.set('search_role', $('exRole').value);
  p.set('job_id', $('exBatch').value);
  p.set('otw_status', $('exOtw').value);
  p.set('experience', $('exExp').value);
  p.set('contact_filter', $('exContact').value);
  return p;
}
async function loadExplorer() {
  const body = $('exBody');
  if (!state.explorerLeads.length) body.innerHTML = `<tr><td colspan="7"><div class="empty"><svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12a9 9 0 1 1-6.2-8.6"/></svg><b>Loading candidates…</b></div></td></tr>`;
  const p = explorerParams(); p.set('sort_by', $('exSort').value); p.set('limit', '500');
  try {
    const res = await fetch('/api/leads?' + p.toString());
    const data = await res.json();
    state.explorerLeads = data.leads || [];
    renderExplorer();
  } catch (e) { body.innerHTML = `<tr><td colspan="7"><div class="empty"><b>Could not load candidates</b>Check that the server is running.</div></td></tr>`; }
}
function renderExplorer() {
  const body = $('exBody');
  $('exCount').textContent = fmt(state.explorerLeads.length);
  if (!state.explorerLeads.length) {
    body.innerHTML = `<tr><td colspan="7"><div class="empty"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg><b>No candidates match</b>Try clearing a filter.</div></td></tr>`;
    return;
  }
  body.innerHTML = state.explorerLeads.map((l, i) => leadRow(l, i, 'explorer')).join('');
}
async function loadBatches() {
  try {
    const res = await fetch('/api/batches'); const data = await res.json();
    const role = $('exRole'), batch = $('exBatch'), rv = role.value, bv = batch.value;
    role.innerHTML = '<option value="all">All roles</option>' + (data.roles || []).map(r => `<option value="${esc(r)}">${esc(r)}</option>`).join('');
    batch.innerHTML = '<option value="all">All batches</option>' + (data.batches || []).map(b => `<option value="${esc(b.job_id)}">${esc(b.job_id)} · ${esc(b.search_role)} (${fmt(b.lead_count)})</option>`).join('');
    role.value = [...role.options].some(o => o.value === rv) ? rv : 'all';
    batch.value = [...batch.options].some(o => o.value === bv) ? bv : 'all';
  } catch (e) {}
}
['exRole', 'exBatch', 'exOtw', 'exExp', 'exContact', 'exSort'].forEach(id => $(id).addEventListener('change', loadExplorer));
['exSearch', 'exLocation'].forEach(id => $(id).addEventListener('input', () => { clearTimeout(exTimer); exTimer = setTimeout(loadExplorer, 280); }));
function clearFilters() {
  $('exSearch').value = ''; $('exLocation').value = '';
  ['exRole', 'exBatch', 'exOtw', 'exExp', 'exContact'].forEach(id => $(id).value = 'all');
  $('exSort').value = 'date_desc';
  loadExplorer();
}
function exportFiltered() { window.location.href = '/api/export-csv?' + explorerParams().toString(); }
function exportMaster() { window.location.href = '/api/export-csv?export_all=true'; }

// Row clicks (feed + explorer)
document.addEventListener('click', (e) => {
  if (e.target.closest('[data-stop]')) return;
  const act = e.target.closest('[data-act]');
  if (act) {
    e.stopPropagation();
    const src = act.dataset.src === 'feed' ? state.sessionLeads : state.explorerLeads;
    refreshLead(src[Number(act.dataset.i)], act.dataset.act, act);
    return;
  }
  const row = e.target.closest('tr.clickable');
  if (!row) return;
  const list = row.dataset.src === 'feed' ? state.sessionLeads : state.explorerLeads;
  const lead = list[Number(row.dataset.i)];
  if (lead) openDrawer(lead);
});

// ---------------------------------------------------------------------------
// Lead refresh
// ---------------------------------------------------------------------------
async function refreshLead(lead, action, btn) {
  if (!lead) return;
  if (btn) { btn.disabled = true; const s = btn.querySelector('svg'); if (s) s.classList.add('spin'); }
  try {
    const res = await fetch('/api/leads/refresh', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ linkedin_url: lead.linkedin_url, action }) });
    const data = await res.json();
    if (data.error) { toast(data.error, 'error'); return null; }
    if (data.verified_live === false) { toast(data.message, 'warning'); return null; }
    const updated = data.lead;
    if (updated) {
      [state.explorerLeads, state.sessionLeads].forEach(list => {
        const i = list.findIndex(l => l.linkedin_url === lead.linkedin_url);
        if (i >= 0) list[i] = { ...list[i], ...updated };
      });
      renderExplorer(); renderFeed();
      toast(`${updated.full_name}: open to work is ${updated.otw_status}`, 'success');
      scheduleStatus();
    }
    return updated;
  } catch (e) { toast('Refresh failed: ' + e.message, 'error'); return null; }
  finally { if (btn) { btn.disabled = false; const s = btn.querySelector('svg'); if (s) s.classList.remove('spin'); } }
}

// ---------------------------------------------------------------------------
// Drawer
// ---------------------------------------------------------------------------
function openDrawer(lead) {
  state.drawerLead = lead;
  renderDrawer(lead);
  $('drawer').classList.add('open'); $('scrim').classList.add('open');
  $('drawer').setAttribute('aria-hidden', 'false');
}
function closeDrawer() {
  $('drawer').classList.remove('open'); $('scrim').classList.remove('open');
  $('drawer').setAttribute('aria-hidden', 'true');
}
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeDrawer(); });
function kv(pairs) { return pairs.filter(p => p[1]).map(([k, v]) => `<dt>${esc(k)}</dt><dd>${v}</dd>`).join(''); }
function renderDrawer(l) {
  const otw = isOtw(l);
  $('dwAvatar').className = 'avatar' + (otw ? ' otw' : '');
  $('dwAvatar').textContent = initials(l.full_name);
  $('dwName').textContent = l.full_name || '—';
  $('dwHeadline').textContent = [l.current_title, l.company_name].filter(x => x && x !== 'Not available').join(' · ');
  $('dwChips').innerHTML = otwChip(l);

  $('dwEvidence').className = 'evidence' + (otw ? ' is-lime' : '');
  $('dwEvidence').innerHTML = `
    <div style="display:flex;justify-content:space-between;gap:10px;align-items:center"><b style="font-family:'Bricolage Grotesque';font-size:16px">${otw ? 'Open to work' : 'No active signal'}</b><span class="chip ${otw ? 'chip-lime' : 'chip-muted'}">${esc(l.signal_type || 'None')}</span></div>
    <div class="cell-muted">${esc(l.signal_description || 'No job-seeking signals detected.')}</div>
    <div class="mono" style="font-size:11px;color:var(--muted)">SIGNAL DATE · ${esc(l.signal_date || 'Not available')}${safeUrl(l.signal_source_url) ? ` · <a href="${esc(l.signal_source_url)}" target="_blank" rel="noopener">source ↗</a>` : ''}</div>`;

  const li = safeUrl(l.linkedin_url);
  $('dwLinkedIn').classList.toggle('hidden', !li);
  if (li) $('dwLinkedIn').href = li;
  $('dwPro').innerHTML = kv([
    ['Current title', esc(l.current_title)],
    ...(l.headline ? [['Headline', esc(l.headline)]] : []),
    ['Company', esc(l.company_name)],
    ['Experience', esc(fmtExp(l.years_experience) || 'Not available')],
    ['Location', esc(l.location)],
    ['LinkedIn', li ? `<a href="${esc(li)}" target="_blank" rel="noopener">${esc(li.replace(/^https?:\/\//, ''))} ↗</a>` : esc(l.linkedin_url)],
  ]);
  const skills = l.skills || [];
  $('dwSkills').innerHTML = skills.length ? skills.map(s => `<span class="skill">${esc(s)}</span>`).join('') : '<span class="cell-muted">No skills captured.</span>';

  const line = (label, val, type) => `<div class="copy-line"><div><div class="label">${label}</div><div style="margin-top:3px;${val ? '' : 'color:var(--muted)'}" class="${type === 'phone' ? 'mono' : ''}">${esc(val || 'Not available')}</div></div>${val ? `<button class="btn btn-sm" onclick="copyText(this, '${type}')">Copy</button>` : ''}</div>`;
  $('dwContact').innerHTML = line('Phone', l.phone, 'phone') + line('Email', l.email, 'email')
    + (l.email_status ? `<div style="display:flex;align-items:center;gap:8px;margin-top:10px;flex-wrap:wrap">${emailStatusChip(l)}</div>` : '');
  $('dwContactNote').textContent = EMAIL_STATUS_HELP[l.email_status]
    || 'Phone numbers come only from the candidate’s own headline or bio. Work emails are found from name + company and checked with the company mail server.';

  $('dwMeta').innerHTML = kv([
    ['Search role', esc(l.search_role)],
    ['Batch', `<span class="mono" style="font-size:12px">${esc(l.job_id)}</span>`],
    ['Data source', esc(l.data_source)],
    ['First found', esc(fmtUTC(l.first_found_at || l.discovered_at))],
    ['Last seen', esc(fmtUTC(l.discovered_at))],
    ['Last updated', esc(fmtUTC(l.updated_at))],
  ]);
}
function copyText(btn, type) {
  const l = state.drawerLead; if (!l) return;
  const v = type === 'phone' ? l.phone : l.email;
  navigator.clipboard.writeText(v || '').then(() => { btn.textContent = 'Copied'; setTimeout(() => btn.textContent = 'Copy', 1400); });
}
const EMAIL_STATUS_CHIP = {
  'Verified': 'chip-lime', 'From profile': 'chip-sky',
  'Risky (catch-all)': 'chip-amber', 'Risky (shared name)': 'chip-amber', 'Unverified': 'chip-amber', 'Not found': 'chip-muted',
};
const EMAIL_STATUS_HELP = {
  'Verified': 'The company mail server confirmed this address exists. No email was sent.',
  'From profile': 'The candidate published this address in their own headline or bio.',
  'Risky (catch-all)': 'This company accepts mail for any address, so the server cannot confirm it. Most likely format shown.',
  'Risky (shared name)': 'Another candidate with the same name at this company got the same address, so it may belong to them. Confirm before using.',
  'Unverified': 'The mail server could not be checked. Best-guess format shown; confirm before relying on it.',
  'Not found': 'No address could be confirmed for this name at the company domain.',
};
function emailStatusChip(l) {
  if (!l.email_status) return '';
  return `<span class="chip ${EMAIL_STATUS_CHIP[l.email_status] || 'chip-muted'}"><span class="dot"></span>${esc(l.email_status)}</span>`;
}
async function drawerAction(action) {
  const l = state.drawerLead; if (!l) return;
  const btn = action === 'verify' ? $('dwVerify') : null;
  const updated = await refreshLead(l, action, btn);
  if (updated) { state.drawerLead = { ...l, ...updated }; renderDrawer(state.drawerLead); }
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
async function refreshAll(spinIcon) {
  if (spinIcon) { $('refreshIcon').classList.add('spin'); setTimeout(() => $('refreshIcon').classList.remove('spin'), 700); }
  await fetchStatus();
  if (state.view === 'explorer') { loadBatches(); loadExplorer(); }
}
(function boot() {
  let theme = 'dark';
  try { theme = localStorage.getItem('tp-theme') || (window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark'); } catch (e) {}
  applyTheme(theme);
  renderFeed();
  updateSummary();
  fetchStatus();
  initSSE();
  setInterval(fetchStatus, 15000);
})();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    # Never let the browser reuse an old copy of the dashboard after an update
    return HTMLResponse(DASHBOARD_HTML, headers={"Cache-Control": "no-store, must-revalidate"})


if __name__ == "__main__":
    import uvicorn
    print("\n" + "=" * 72)
    print("🚀 TalentPulse AI — Production Dashboard Server Starting")
    print("=" * 72)
    print("  • Dashboard URL:   http://localhost:8000")
    print("  • API OpenAPI Doc: http://localhost:8000/docs")
    print("=" * 72 + "\n")
    # Bound to this computer only: the server holds LinkedIn session cookies and candidate data
    # Locally: 127.0.0.1:8000 as before. On a host that sets PORT, listen on all interfaces.
    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST") or ("0.0.0.0" if "PORT" in os.environ else "127.0.0.1")
    uvicorn.run("app:app", host=host, port=port, reload=False)
