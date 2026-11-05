import os
import sys
import csv
import sqlite3
import tempfile
from contextlib import closing
from sales_nav_extractor import (
    DataCleaner,
    LeadQualifier,
    LeadDatabase,
    LeadRecord,
    SalesNavURLParser,
    SearchFilterParams,
    SalesNavExtractor,
)


def test_clean_name_pronouns_degrees_and_emojis():
    raw = "Dr. Jane Doe (She/Her), Ph.D. 🚀 ✨"
    cleaned = DataCleaner.clean_name(raw)
    assert cleaned == "Jane Doe"

    first, last = DataCleaner.split_name(cleaned)
    assert first == "Jane"
    assert last == "Doe"


def test_clean_name_various_formats():
    assert DataCleaner.clean_name("John Smith [He/Him] MBA") == "John Smith"
    assert DataCleaner.clean_name("Michael Brown, B.Tech, PMP") == "Michael Brown"
    assert DataCleaner.clean_name("Prof. Robert Johnson CPA") == "Robert Johnson"
    assert DataCleaner.clean_name("Cher 🌟") == "Cher"
    assert DataCleaner.split_name("Cher") == ("Cher", "")


def test_clean_company_name_legal_suffixes():
    assert DataCleaner.clean_company_name("Acme Tech Solutions, Inc.") == "Acme Tech Solutions"
    assert DataCleaner.clean_company_name("DataCorp LLC 🚀") == "DataCorp"
    assert DataCleaner.clean_company_name("Tata Consultancy Services Pvt Ltd") == "Tata Consultancy Services"
    assert DataCleaner.clean_company_name("Global Innovators Limited") == "Global Innovators"
    assert DataCleaner.clean_company_name("Alpha Corp.") == "Alpha"


def test_clean_headline_decorative_symbols():
    raw = "Senior AI Engineer 🚀 | LLMs & PyTorch | ★ Open to Work ✨"
    cleaned = DataCleaner.clean_headline(raw)
    assert "🚀" not in cleaned
    assert "✨" not in cleaned
    assert "★" not in cleaned
    assert "Senior AI Engineer" in cleaned
    assert "Open to Work" in cleaned


def test_qualified_lead():
    is_qualified, reasons, is_otw = LeadQualifier.evaluate(
        current_title="Senior AI Engineer",
        company_name="Acme Tech",
        headline="Senior AI Engineer building GenAI models #OpenToWork",
        bio="Experienced AI researcher",
        current_positions=[{"title": "Senior AI Engineer", "company": "Acme Tech"}],
        past_positions=[{"title": "Software Engineer", "company": "OldCo"}],
        metadata_open_to_work=True,
        target_role="AI Engineer",
        target_keywords="Open to work",
    )
    assert is_qualified is True
    assert len(reasons) == 0
    assert is_otw is True


def test_gate_a_past_role_only():
    # Target role in past position, not current
    is_qualified, reasons, is_otw = LeadQualifier.evaluate(
        current_title="Senior Real Estate Broker",
        company_name="Premier Realty",
        headline="Real Estate Broker | Ex-AI Engineer",
        bio="Switched careers from tech to real estate",
        current_positions=[{"title": "Senior Real Estate Broker", "company": "Premier Realty"}],
        past_positions=[{"title": "Senior AI Engineer", "company": "DataCorp"}],
        metadata_open_to_work=False,
        target_role="AI Engineer",
    )
    assert is_qualified is False
    assert "wrong job title (past role only)" in reasons


def test_gate_b_company_disambiguation():
    # Keyword matches company name instead of job title
    is_qualified, reasons, is_otw = LeadQualifier.evaluate(
        current_title="Senior Account Executive",
        company_name="AI Engineer Staffing Agency LLC",
        headline="Account Executive at AI Engineer Staffing Agency",
        bio="Leading enterprise sales",
        current_positions=[{"title": "Senior Account Executive", "company": "AI Engineer Staffing Agency LLC"}],
        past_positions=[],
        metadata_open_to_work=False,
        target_role="AI Engineer",
    )
    assert is_qualified is False
    assert "company name match, not job title" in reasons


def test_gate_c_recruiter_filter():
    # Recruiter or hiring manager profile
    is_qualified, reasons, is_otw = LeadQualifier.evaluate(
        current_title="Head of Talent Acquisition",
        company_name="TechVentures",
        headline="We are hiring AI Engineers! Join our team 🚀",
        bio="Hiring top talent",
        current_positions=[{"title": "Head of Talent Acquisition", "company": "TechVentures"}],
        past_positions=[],
        metadata_open_to_work=False,
        target_role="AI Engineer",
    )
    assert is_qualified is False
    assert "recruiter or hiring manager profile" in reasons


def test_gate_d_open_to_work_filter():
    # Explicit search for Open to work when profile is not open to work
    is_qualified, reasons, is_otw = LeadQualifier.evaluate(
        current_title="AI Engineer",
        company_name="Acme Tech",
        headline="AI Engineer at Acme",
        bio="Building models",
        current_positions=[{"title": "AI Engineer", "company": "Acme Tech"}],
        past_positions=[],
        metadata_open_to_work=False,
        target_role="AI Engineer",
        target_keywords="Open to work",
    )
    assert is_qualified is False
    assert "not open to work" in reasons
    assert is_otw is False


def test_upsert_and_deduplication():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_file = os.path.join(tmp_dir, "test_leads.db")
        csv_file = os.path.join(tmp_dir, "test_export.csv")
        test_db = LeadDatabase(db_file)

        record1 = LeadRecord(
            linkedin_url="https://www.linkedin.com/in/jane-doe-ai",
            first_name="Jane",
            last_name="Doe",
            full_name="Jane Doe",
            current_title="Senior AI Engineer",
            company_name="Acme Tech",
            location="Bengaluru, India",
            is_open_to_work=True,
            is_qualified=True,
            no_match_reasons=[],
            discovered_at="2026-10-28 10:00:00",
        )
        test_db.upsert_lead(record1)
        assert test_db.get_total_count() == 1
        assert test_db.get_qualified_count() == 1

        # Insert duplicate URL with updated title
        record2 = LeadRecord(
            linkedin_url="https://www.linkedin.com/in/jane-doe-ai",
            first_name="Jane",
            last_name="Doe",
            full_name="Jane Doe",
            current_title="Lead AI Engineer",
            company_name="Acme Tech",
            location="Bengaluru, India",
            is_open_to_work=True,
            is_qualified=True,
            no_match_reasons=[],
            discovered_at="2026-10-28 12:00:00",
        )
        test_db.upsert_lead(record2)
        # Total count must still be 1 (zero duplicates)
        assert test_db.get_total_count() == 1

        # Check updated current_title
        with closing(sqlite3.connect(test_db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT current_title FROM leads WHERE linkedin_url = ?", (record1.linkedin_url,))
            title = cursor.fetchone()[0]
            assert title == "Lead AI Engineer"

        # Test CSV Export
        exported = test_db.export_to_csv(csv_file)
        assert exported == 1
        assert os.path.exists(csv_file)

        with open(csv_file, "r", encoding="utf-8-sig") as f:
            reader = list(csv.reader(f))
            headers = reader[0]
            assert headers == [
                "Candidate Name",
                "LinkedIn Profile URL",
                "Current Job Title",
                "Headline",
                "Current Company",
                "Years of Experience",
                "Location",
                "Open-to-Work Status",
                "Mobile Number",
                "Email Address",
                "Email Status",
                "Relevant Skills / Keywords",
                "First Found (IST)",
                "Last Seen (IST)",
            ]
            data_row = reader[1]
            assert data_row[0] == "Jane Doe"
            assert data_row[1] == "https://www.linkedin.com/in/jane-doe-ai"
            assert data_row[2] == "Lead AI Engineer"


def test_parse_sales_nav_encoded_url():
    url = (
        "https://www.linkedin.com/sales/search/people?query="
        "(recentSearchParam%3A(id%3A12345)%2Cfilters%3AList("
        "(type%3ACURRENT_TITLE%2Cvalues%3AList((text%3AAI%20Engineer%2CselectionType%3AINCLUDED)))%2C"
        "(type%3AREGION%2Cvalues%3AList((text%3AIndia%2CselectionType%3AINCLUDED)))%2C"
        "(type%3AKEYWORDS%2Cvalues%3AList((text%3AOpen%20to%20work%2CselectionType%3AINCLUDED)))))"
    )
    params = SalesNavURLParser.parse_url(url)
    assert params.role == "AI Engineer"
    assert params.location == "India"
    assert params.keywords == "Open to work"


def test_build_api_params():
    filters = SearchFilterParams(
        role="AI Engineer",
        location="India",
        seniority_levels=["Senior", "Director"],
        keywords="Open to work",
    )
    params = SalesNavURLParser.build_api_params(filters, start=25, count=25)
    assert params["start"] == 25
    assert params["count"] == 25
    assert params["q"] == "searchQuery"
    assert "type:CURRENT_TITLE" in params["query"]
    assert "AI Engineer" in params["query"]
    assert "India" in params["query"]
    assert "type:SENIORITY_LEVEL" in params["query"]
    # UI labels are translated to Sales Navigator numeric seniority IDs
    assert "(id:120,selectionType:INCLUDED)" in params["query"]
    assert "(id:220,selectionType:INCLUDED)" in params["query"]
    assert "id:Senior" not in params["query"]


def test_build_api_params_seniority_ids():
    filters = SearchFilterParams(role="AI Engineer", seniority_levels=["Entry-level", "Executive", "120", "Unknown"])
    query = SalesNavURLParser.build_api_params(filters)["query"]
    for s_id in ("110", "300", "310", "320", "120"):
        assert f"(id:{s_id},selectionType:INCLUDED)" in query
    assert "Unknown" not in query
    assert query.count("id:120,") == 1

    # Only unrecognised labels: the seniority filter is omitted entirely
    filters = SearchFilterParams(role="AI Engineer", seniority_levels=["Unknown"])
    assert "SENIORITY_LEVEL" not in SalesNavURLParser.build_api_params(filters)["query"]


def test_estimate_years_experience():
    from datetime import datetime, timezone
    now = datetime(2026, 1, 15, tzinfo=timezone.utc)

    # Current role since Jan 2021 (5y) + past role Jan 2016 - Dec 2020 (5y)
    lead = {
        "currentPositions": [{"title": "AI Engineer", "startedOn": {"year": 2021, "month": 1}}],
        "pastPositions": [{"title": "Dev", "startedOn": {"year": 2016, "month": 1}, "endedOn": {"year": 2020, "month": 12}}],
    }
    assert DataCleaner.estimate_years_experience(lead, now=now) == 10.1

    # Overlapping concurrent roles are not double-counted; Voyager dateRange is supported
    lead = {
        "currentPositions": [
            {"startedOn": {"year": 2024, "month": 1}},
            {"dateRange": {"start": {"year": 2024, "month": 7}}},
        ]
    }
    assert DataCleaner.estimate_years_experience(lead, now=now) == 2.1

    # No dates at all -> unknown, never a guess
    assert DataCleaner.estimate_years_experience({"currentPositions": [{"title": "X"}]}, now=now) is None
    assert DataCleaner.estimate_years_experience({}, now=now) is None


def test_years_experience_persisted_and_exported():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db = LeadDatabase(os.path.join(tmp_dir, "exp.db"))
        extractor = SalesNavExtractor(session=None, db=db, enable_jitter=False, dry_run=True)
        extractor.run(filters=SearchFilterParams(role="AI Engineer"), max_pages=1)
        lead = db.get_lead("https://linkedin.com/in/rajesh-kumar-ai")
        assert lead["years_experience"] and lead["years_experience"] > 10  # 2015 -> today

        csv_file = os.path.join(tmp_dir, "exp.csv")
        db.export_to_csv(csv_file)
        with open(csv_file, encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
        col = rows[0].index("Years of Experience")
        assert all(r[col] for r in rows[1:])


def test_build_api_params_years_of_experience():
    filters = SearchFilterParams(role="Java Developer", experience_ranges=["3-5", "6-10", "bogus"])
    query = SalesNavURLParser.build_api_params(filters)["query"]
    assert "type:YEARS_OF_EXPERIENCE" in query
    assert "(id:3,selectionType:INCLUDED)" in query
    assert "(id:4,selectionType:INCLUDED)" in query
    assert "bogus" not in query
    assert "YEARS_OF_EXPERIENCE" not in SalesNavURLParser.build_api_params(
        SearchFilterParams(role="Java Developer"))["query"]


def test_gate_f_years_of_experience():
    base = dict(
        current_title="Java Developer", company_name="Infosys", headline="Java Developer #OpenToWork", bio="",
        current_positions=[{"title": "Java Developer"}], past_positions=[], metadata_open_to_work=True,
        target_role="Java Developer", target_keywords="Open to work",
    )
    ok = LeadQualifier.evaluate(**base, years_experience=4.5, target_experience=["3-5"])
    assert ok.is_qualified is True

    out = LeadQualifier.evaluate(**base, years_experience=12.0, target_experience=["3-5", "6-10"])
    assert out.is_qualified is False
    assert "years of experience out of range" in out.no_match_reasons

    # Unknown experience is never rejected on this gate (LinkedIn's own filter already applied)
    unknown = LeadQualifier.evaluate(**base, years_experience=None, target_experience=["3-5"])
    assert unknown.is_qualified is True


def _offline_finder(mx_domains, mailbox, catch_all=False):
    """EmailFinder with DNS/SMTP replaced by in-memory fakes."""
    from sales_nav_extractor import EmailFinder
    f = EmailFinder()
    f.mx_host = lambda d: f"mx.{d}" if d in mx_domains else None
    def fake_smtp(mx, emails):
        return {e: (True if catch_all else (e in mailbox)) for e in emails}
    f._smtp_check = fake_smtp
    return f


def test_phone_extraction_from_headline():
    cases = [
        ("Java Dev | Mobile: +91 98765 43210", "+91 98765 43210"),
        ("Java Dev | Call me 9876543210", "9876543210"),
        ("Java Dev | 9876543210 | Open to work", "9876543210"),
        ("Java Dev | Ph: 080 2345 6789", "080 2345 6789"),
        ("Java Dev | Contact no. 98765 43210", "98765 43210"),
        ("Java Dev | reach me at 09876543210", "09876543210"),
        # Numbers that are not phones must never be picked up
        ("Java Dev | 5+ years experience | 2019-2024", None),
        ("Java Dev | 10+ yrs | ex-TCS 2015 - 2023", None),
        ("Java Dev | CTC 12 LPA | Notice 30 days", None),
        ("Java Dev | Employee ID 1234567", None),
    ]
    for headline, expected in cases:
        assert DataCleaner.extract_contact_info({"headline": headline})[0] == expected, headline


def test_email_finder_patterns():
    from sales_nav_extractor import EmailFinder
    emails = [e for _, e in EmailFinder.candidate_addresses("Rahul", "Sharma", "infosys.com")]
    assert emails[:3] == ["rahul.sharma@infosys.com", "rahulsharma@infosys.com", "rsharma@infosys.com"]
    # Accents are folded to ASCII
    assert EmailFinder.candidate_addresses("José", "Núñez", "x.com")[0][1] == "jose.nunez@x.com"


def test_email_finder_verified_and_pattern_reuse():
    from sales_nav_extractor import EMAIL_STATUS_VERIFIED
    f = _offline_finder({"infosys.com"}, {"rsharma@infosys.com", "pmehta@infosys.com"})
    assert f.find("Rahul", "Sharma", "Infosys") == ("rsharma@infosys.com", EMAIL_STATUS_VERIFIED)
    assert f._pattern_cache["infosys.com"] == "flast"
    assert f.find("Priya", "Mehta", "Infosys") == ("pmehta@infosys.com", EMAIL_STATUS_VERIFIED)


def test_email_finder_catch_all_and_missing_domain():
    from sales_nav_extractor import EMAIL_STATUS_RISKY, EMAIL_STATUS_NOT_FOUND
    f = _offline_finder({"acmetech.com"}, set(), catch_all=True)
    assert f.find("Jane", "Doe", "Acme Tech Solutions Pvt Ltd") == ("jane.doe@acmetech.com", EMAIL_STATUS_RISKY)
    f = _offline_finder(set(), set())
    assert f.find("Jane", "Doe", "Nowhere Corp") == (None, EMAIL_STATUS_NOT_FOUND)
    # Server rejects every guess -> nothing is invented
    f = _offline_finder({"wipro.com"}, set())
    assert f.find("Ravi", "Kumar", "Wipro") == (None, EMAIL_STATUS_NOT_FOUND)


def test_dry_run_never_calls_email_finder():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db = LeadDatabase(os.path.join(tmp_dir, "dry.db"))
        extractor = SalesNavExtractor(session=None, db=db, enable_jitter=False, dry_run=True)
        assert extractor.email_finder is None


def test_run_stops_after_current_page():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db = LeadDatabase(os.path.join(tmp_dir, "stop.db"))
        extractor = SalesNavExtractor(session=None, db=db, enable_jitter=False, dry_run=True)
        pages_fetched = []
        original_fetch = extractor.fetch_page

        def counting_fetch(filters, start):
            pages_fetched.append(start)
            payload = original_fetch(filters=filters, start=0)  # every page returns leads
            return payload

        extractor.fetch_page = counting_fetch
        stop = {"flag": False}

        def on_lead(record, progress):
            stop["flag"] = True  # stop requested while page 1 is in flight

        processed, _ = extractor.run(
            filters=SearchFilterParams(role="AI Engineer"),
            max_pages=5,
            lead_callback=on_lead,
            should_stop=lambda: stop["flag"],
        )
        assert pages_fetched == [0]   # no further pages fetched
        assert processed == 5         # the in-flight page was finished


def test_count_extracted_since_rolling_window():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db = LeadDatabase(os.path.join(tmp_dir, "quota.db"))
        extractor = SalesNavExtractor(session=None, db=db, enable_jitter=False, dry_run=True)
        extractor.run(filters=SearchFilterParams(role="AI Engineer"), max_pages=1)
        assert db.count_extracted_since(24) == 5

        with closing(sqlite3.connect(db.active_path)) as conn:
            conn.execute("UPDATE leads SET discovered_at = '2000-01-01 00:00:00' WHERE linkedin_url LIKE '%jane%';")
            conn.commit()
        assert db.count_extracted_since(24) == 4


def test_gate_e_seniority_qualification():
    # Candidate with Senior title
    is_qual, reasons, _ = LeadQualifier.evaluate(
        current_title="Senior AI Engineer",
        company_name="DeepMind",
        headline="Senior AI Engineer | Open to work",
        bio="Experienced deep learning practitioner",
        current_positions=[{"title": "Senior AI Engineer", "company": "DeepMind"}],
        past_positions=[],
        metadata_open_to_work=True,
        target_role="AI Engineer",
        target_seniorities=["Senior"],
    )
    assert is_qual is True
    assert len(reasons) == 0

    # Candidate with Junior title when Senior is requested
    is_qual, reasons, _ = LeadQualifier.evaluate(
        current_title="Junior AI Engineer",
        company_name="DeepMind",
        headline="Junior AI Engineer | Open to work",
        bio="Graduate engineer",
        current_positions=[{"title": "Junior AI Engineer", "company": "DeepMind"}],
        past_positions=[],
        metadata_open_to_work=True,
        target_role="AI Engineer",
        target_seniorities=["Senior"],
    )
    assert is_qual is False
    assert "seniority / experience level mismatch" in reasons


def test_end_to_end_dry_run():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_file = os.path.join(tmp_dir, "dry_run.db")
        csv_file = os.path.join(tmp_dir, "dry_run.csv")

        db = LeadDatabase(db_file)
        extractor = SalesNavExtractor(
            session=None,
            db=db,
            enable_jitter=False,
            dry_run=True,
        )

        filters = SearchFilterParams(
            role="AI Engineer",
            location="India",
            keywords="Open to work",
        )

        processed, qualified = extractor.run(filters=filters, max_pages=1)
        assert processed == 5
        assert qualified == 2  # Jane Doe and Rajesh Kumar pass all gates

        # Verify DB records
        assert db.get_total_count() == 5
        assert db.get_qualified_count() == 2

        # Verify CSV export
        exported_count = db.export_to_csv(csv_file)
        assert exported_count == 5
        assert os.path.exists(csv_file)


def test_csv_formula_injection_sanitization():
    with tempfile.TemporaryDirectory() as tmp_dir:
        csv_file = os.path.join(tmp_dir, "sanitized.csv")
        db_file = os.path.join(tmp_dir, "test.db")
        db = LeadDatabase(db_file)

        # Inject dangerous leading formula characters
        dangerous_record = LeadRecord(
            linkedin_url="https://www.linkedin.com/in/malicious-lead",
            first_name="=CMD|' /C calc'!A0",
            last_name="+12345",
            full_name="@SuperAdmin",
            current_title="-Developer",
            company_name="\tTabCompany",
            location="%NewlineCity",
            is_open_to_work=True,
            is_qualified=True,
            no_match_reasons=[],
            discovered_at="2026-10-28 10:00:00",
        )
        db.upsert_lead(dangerous_record)
        db.export_to_csv(csv_file)

        with open(csv_file, "r", encoding="utf-8-sig", newline="") as f:
            reader = list(csv.reader(f))
            row = reader[1]
            # Verify formula prefixes are safely escaped with a leading single quote (')
            assert row[0] == "'@SuperAdmin"      # Candidate Name
            assert row[2] == "'-Developer"       # Current Job Title
            assert row[4] == "'\tTabCompany"     # Current Company


def test_unicode_safe_name_cleaning():
    # Accented Latin names
    assert DataCleaner.clean_name("Renée Müller") == "Renée Müller"
    assert DataCleaner.clean_name("François Dubois, MBA") == "François Dubois"
    assert DataCleaner.clean_name("Dr. José María González (He/Him)") == "José María González"

    # Non-Latin scripts: Devanagari, CJK
    assert DataCleaner.clean_name("राजेश कुमार 🚀") == "राजेश कुमार"
    assert DataCleaner.clean_name("张伟 ✨") == "张伟"
    assert DataCleaner.clean_name("田中 太郎 [He/Him] Ph.D.") == "田中 太郎"

    # Emojis and decorative symbols stripped while preserving characters
    assert DataCleaner.clean_name("🌟 Michael O'Connor-Smith 🌟") == "Michael O'Connor-Smith"


def test_profile_url_normalization():
    # Query parameters, fragments, trailing slashes, www vs bare domain
    url1 = "https://www.linkedin.com/in/jane-doe/"
    url2 = "https://linkedin.com/in/jane-doe?trk=public_profile_browsemap&miniProfileUrn=123"
    url3 = "http://www.linkedin.com/in/jane-doe#experience"
    url4 = "https://www.linkedin.com/sales/lead/ACwAA12345,NAME_SEARCH/"

    assert DataCleaner.normalize_linkedin_url(url1) == "https://linkedin.com/in/jane-doe"
    assert DataCleaner.normalize_linkedin_url(url2) == "https://linkedin.com/in/jane-doe"
    assert DataCleaner.normalize_linkedin_url(url3) == "http://linkedin.com/in/jane-doe"
    assert DataCleaner.normalize_linkedin_url(url4) == "https://linkedin.com/sales/lead/ACwAA12345,NAME_SEARCH"

    # Distinct identifiers must NOT be falsely merged
    url_a = "https://www.linkedin.com/in/jane-doe-1"
    url_b = "https://www.linkedin.com/in/jane-doe-2"
    assert DataCleaner.normalize_linkedin_url(url_a) != DataCleaner.normalize_linkedin_url(url_b)


def test_dry_run_isolation_preserves_production_files():
    import subprocess
    import hashlib

    prod_db = "sales_nav_leads.db"
    prod_csv = "sales_nav_leads.csv"

    # Record baseline state of production files
    db_exists_before = os.path.exists(prod_db)
    csv_exists_before = os.path.exists(prod_csv)

    db_hash_before = None
    if db_exists_before:
        with open(prod_db, "rb") as f:
            db_hash_before = hashlib.sha256(f.read()).hexdigest()

    # Run CLI in dry-run mode
    res = subprocess.run(
        [sys.executable, "sales_nav_extractor.py", "--dry-run", "--no-jitter"],
        capture_output=True,
        text=True,
        cwd=os.getcwd(),
    )
    assert res.returncode == 0

    # Ensure production csv was not created or modified
    assert os.path.exists(prod_csv) == csv_exists_before

    # Ensure production database hash is identical (untouched)
    if db_exists_before:
        with open(prod_db, "rb") as f:
            db_hash_after = hashlib.sha256(f.read()).hexdigest()
        assert db_hash_after == db_hash_before

    # Clean up isolated dry-run output
    if os.path.exists("dry_run_leads.csv"):
        os.remove("dry_run_leads.csv")


def test_contact_extraction_and_safe_handling():
    # 1. International phone formatting with country code and leading zeros preserved
    raw_with_phone = {
        "phone": "+44 020 7946 0919",
        "email": "lead.engineer@uk-ai.co.uk",
        "phoneVerified": True,
        "emailVerified": True,
    }
    phone, email, src, verified = DataCleaner.extract_contact_info(raw_with_phone)
    assert phone == "+44 020 7946 0919"
    assert phone.startswith("+44 0")  # Leading zero preserved
    assert email == "lead.engineer@uk-ai.co.uk"
    assert verified is True
    assert src == "Authorized Data Source"

    # 2. Public text contact info extracted from headline/bio
    raw_public_text = {
        "headline": "Senior AI Architect | Reach out: dev@example.com or +1 (555) 019-2834",
        "bio": "Building scalable LLM pipelines.",
    }
    phone2, email2, src2, verified2 = DataCleaner.extract_contact_info(raw_public_text)
    assert email2 == "dev@example.com"
    assert phone2 == "+1 (555) 019-2834"
    assert verified2 is False
    assert src2 == "Profile Bio / Headline"

    # 3. Missing contact details return None (safe handling, never fabricated)
    raw_missing = {
        "headline": "Senior Engineer at Acme",
        "bio": "No contact information here.",
    }
    phone3, email3, src3, verified3 = DataCleaner.extract_contact_info(raw_missing)
    assert phone3 is None
    assert email3 is None
    assert verified3 is False
    assert src3 == "Not available"


def test_otw_status_separation_from_qualification():
    # Candidate matches target role but has NO job-seeking signals:
    # Must be Qualified=True, but otw_status='Unknown'
    res_passive = LeadQualifier.evaluate(
        current_title="AI Engineer",
        company_name="Acme Tech",
        headline="AI Engineer at Acme Tech",
        bio="Working on generative search",
        current_positions=[{"title": "AI Engineer", "company": "Acme Tech"}],
        past_positions=[],
        metadata_open_to_work=False,
        target_role="AI Engineer",
    )
    assert res_passive.is_qualified is True
    assert res_passive.qualification_status == "Qualified"
    assert res_passive.otw_status == "No"
    assert res_passive.signal_type == "None"

    # Candidate has explicit #OpenToWork but WRONG role (Gate A / B failure):
    # Must be Qualified=False, but otw_status='Yes' (binary Yes/No)
    res_wrong_role = LeadQualifier.evaluate(
        current_title="Senior Real Estate Agent",
        company_name="Realty Pro",
        headline="Senior Broker #OpenToWork",
        bio="Experienced broker looking for new ventures",
        current_positions=[{"title": "Senior Real Estate Agent", "company": "Realty Pro"}],
        past_positions=[],
        metadata_open_to_work=True,
        target_role="AI Engineer",
    )
    assert res_wrong_role.is_qualified is False
    assert res_wrong_role.qualification_status == "Rejected"
    assert res_wrong_role.otw_status == "Yes"
    assert res_wrong_role.signal_type == "Open to Work"

    # Candidate with possible job-seeking signal phrase
    res_possible = LeadQualifier.evaluate(
        current_title="AI Engineer",
        company_name="Startup Co",
        headline="AI Engineer | exploring new opportunities in GenAI",
        bio="Open to exciting challenges",
        current_positions=[{"title": "AI Engineer", "company": "Startup Co"}],
        past_positions=[],
        metadata_open_to_work=False,
        target_role="AI Engineer",
    )
    assert res_possible.is_qualified is True
    assert res_possible.otw_status == "Yes"
    assert res_possible.signal_type == "Job-Seeking Signal"


def test_recruiter_filtered_csv_export():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_file = os.path.join(tmp_dir, "filter_test.db")
        csv_file = os.path.join(tmp_dir, "filtered.csv")
        db = LeadDatabase(db_file)

        # Insert 3 diverse candidate records
        rec1 = LeadRecord(
            linkedin_url="https://linkedin.com/in/c1",
            first_name="Alice",
            last_name="Wang",
            full_name="Alice Wang",
            current_title="AI Engineer",
            company_name="DeepTech",
            location="Bengaluru, India",
            is_open_to_work=True,
            is_qualified=True,
            no_match_reasons=[],
            discovered_at="2026-10-29 01:00:00",
            phone="+91 99999 11111",
            email="alice@deeptech.com",
            otw_status="Explicitly Indicated",
            qualification_status="Qualified",
        )
        rec2 = LeadRecord(
            linkedin_url="https://linkedin.com/in/c2",
            first_name="Bob",
            last_name="Smith",
            full_name="Bob Smith",
            current_title="Recruiter",
            company_name="StaffingCo",
            location="London, UK",
            is_open_to_work=False,
            is_qualified=False,
            no_match_reasons=["recruiter profile"],
            discovered_at="2026-10-29 02:00:00",
            phone=None,
            email=None,
            otw_status="Unknown",
            qualification_status="Rejected",
        )
        rec3 = LeadRecord(
            linkedin_url="https://linkedin.com/in/c3",
            first_name="Charlie",
            last_name="Brown",
            full_name="Charlie Brown",
            current_title="ML Engineer",
            company_name="CloudAI",
            location="Bengaluru, India",
            is_open_to_work=False,
            is_qualified=True,
            no_match_reasons=[],
            discovered_at="2026-10-29 03:00:00",
            phone=None,
            email="charlie@cloudai.com",
            otw_status="Possible Signal",
            qualification_status="Qualified",
        )

        db.upsert_lead(rec1)
        db.upsert_lead(rec2)
        db.upsert_lead(rec3)

        # 1. Export filtered by Qualified
        count_qual = db.export_to_csv(csv_file, filters={"qualification_status": "qualified"})
        assert count_qual == 2

        with open(csv_file, "r", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
            assert len(rows) == 3  # Header + 2 rows
            names = [r[0] for r in rows[1:]]
            assert "Alice Wang" in names
            assert "Charlie Brown" in names
            assert "Bob Smith" not in names

        # 2. Export filtered by Phone presence
        count_phone = db.export_to_csv(csv_file, filters={"contact_filter": "has_phone"})
        assert count_phone == 1
        with open(csv_file, "r", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
            assert len(rows) == 2  # Header + 1 row
            assert rows[1][0] == "Alice Wang"
            assert rows[1][8] == "(+91) 99999 11111"  # Readable, and no leading '+' for Excel to run


def test_role_and_batch_isolation_and_master_export():
    """
    Verifies multi-role query isolation, batch filtering, role filtering,
    and complete master database export behavior in SQLite and CSV export.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_db = os.path.join(tmp_dir, "test_roles.db")
        csv_file = os.path.join(tmp_dir, "export.csv")
        db = LeadDatabase(db_path=test_db)

        # Batch 1: AI Engineer leads
        lead_ai = LeadRecord(
            linkedin_url="https://linkedin.com/in/ai-lead-1",
            first_name="Anita",
            last_name="Rao",
            full_name="Anita Rao",
            current_title="Senior AI Engineer",
            company_name="DeepTech",
            location="Bengaluru, India",
            is_open_to_work=True,
            is_qualified=True,
            no_match_reasons=[],
            discovered_at="2026-10-29 08:00:00",
            phone="+91 98888 11111",
            email="anita@deeptech.ai",
            otw_status="Explicitly Indicated",
            qualification_status="Qualified",
            target_role_match="Matched Target Role: AI Engineer",
            job_id="batch-ai-001",
            search_role="AI Engineer",
        )

        # Batch 2: Java Fullstack Developer leads
        lead_java1 = LeadRecord(
            linkedin_url="https://linkedin.com/in/java-lead-1",
            first_name="Vikram",
            last_name="Mehta",
            full_name="Vikram Mehta",
            current_title="Lead Java Developer",
            company_name="Enterprise Systems",
            location="Hyderabad, India",
            is_open_to_work=True,
            is_qualified=True,
            no_match_reasons=[],
            discovered_at="2026-10-29 09:00:00",
            phone="+91 97777 22222",
            email="vikram@enterprise.com",
            otw_status="Explicitly Indicated",
            qualification_status="Qualified",
            target_role_match="Matched Target Role: Java Fullstack Developer",
            job_id="batch-java-002",
            search_role="Java Fullstack Developer",
        )
        lead_java2 = LeadRecord(
            linkedin_url="https://linkedin.com/in/java-lead-2",
            first_name="Deepa",
            last_name="Nair",
            full_name="Deepa Nair",
            current_title="Java Fullstack Architect",
            company_name="FinServe Corp",
            location="Bengaluru, India",
            is_open_to_work=False,
            is_qualified=True,
            no_match_reasons=[],
            discovered_at="2026-10-29 09:05:00",
            phone=None,
            email=None,
            otw_status="Possible Signal",
            qualification_status="Qualified",
            target_role_match="Matched Target Role: Java Fullstack Developer",
            job_id="batch-java-002",
            search_role="Java Fullstack Developer",
        )

        db.upsert_lead(lead_ai)
        db.upsert_lead(lead_java1)
        db.upsert_lead(lead_java2)

        assert db.get_total_count() == 3

        # 1. Test Filter strictly by Role: Java Fullstack Developer
        count_java = db.export_to_csv(csv_file, filters={"search_role": "Java Fullstack Developer"})
        assert count_java == 2
        with open(csv_file, "r", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
            assert len(rows) == 3  # Header + 2 Java candidates
            names = [r[0] for r in rows[1:]]
            assert "Vikram Mehta" in names
            assert "Deepa Nair" in names
            assert "Anita Rao" not in names

        # 2. Test Filter strictly by Batch / Job ID: batch-ai-001
        count_batch = db.export_to_csv(csv_file, filters={"job_id": "batch-ai-001"})
        assert count_batch == 1
        with open(csv_file, "r", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
            assert len(rows) == 2  # Header + 1 AI candidate
            assert rows[1][0] == "Anita Rao"

        # 3. Test Master Export (filters=None): exports all 3 candidates
        count_master = db.export_to_csv(csv_file, filters=None)
        assert count_master == 3
        with open(csv_file, "r", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
            assert len(rows) == 4  # Header + 3 candidates
            names = [r[0] for r in rows[1:]]
            assert "Anita Rao" in names
            assert "Vikram Mehta" in names
            assert "Deepa Nair" in names



# ---------------------------------------------------------------------------
# Loose role matching (plurals, compound spellings, short forms)
# ---------------------------------------------------------------------------
from sales_nav_extractor import RoleMatcher


def test_role_matcher_accepts_loose_spellings():
    cases = [
        ("java full stack developers", "Java Full Stack Developer"),
        ("Java Fullstack Developer", "Java Full Stack Developer"),
        ("java full-stack developer", "Senior Java Fullstack Developer | Spring Boot"),
        ("Java FSD", "Java Full Stack Developer"),
        ("Sr Java Dev", "Senior Java Developer"),
        ("React.js developers", "ReactJS Developer"),
        ("Node JS Developer", "NodeJS developer"),
        (".NET developer", "Dot Net Developer"),
        ("Data Analysts", "Data Analyst"),
        ("DevOps engineers", "Dev-Ops Engineer"),
        ("SDE", "Software Development Engineer II"),
        ("frontend developer", "Front-End Developer"),
        ("AI Engineer", "Staff AI Engineer"),
    ]
    for role, title in cases:
        assert RoleMatcher.matches(role, title), (role, title)


def test_role_matcher_rejects_wrong_titles():
    assert not RoleMatcher.matches("java developer", "Senior JavaScript Developer")
    assert not RoleMatcher.matches("python developer", "Java Developer")
    assert not RoleMatcher.matches("AI Engineer", "Head of Talent Acquisition")
    assert RoleMatcher.matches("Sales Executive", "Sales Executive")  # 'sales' is not a plural


def test_plural_role_is_qualified_end_to_end():
    res = LeadQualifier.evaluate(
        current_title="Java Fullstack Developer", company_name="Acme", headline="#OpenToWork",
        bio="", current_positions=[], past_positions=[], metadata_open_to_work=True,
        target_role="java full stack developers",
    )
    assert res.is_qualified


def test_search_sends_spelling_variants_to_linkedin():
    variants = RoleMatcher.search_variants("java full stack developers")
    assert [v.lower() for v in variants] == [
        "java full stack developer", "java fullstack developer", "java full-stack developer"]
    params = SalesNavURLParser.build_api_params(SearchFilterParams(role="java full stack developers"))
    q = params["query"]
    assert q.count("selectionType:INCLUDED") >= 3 and "fullstack developer" in q


# ---------------------------------------------------------------------------
# Role typeahead (standard titles) + multi-title search
# ---------------------------------------------------------------------------
import job_titles


def _top(q):
    return [r["name"] for r in job_titles.search(q, 5)]


def test_title_typeahead_across_job_families():
    assert _top("java ful")[0] == "Java Full Stack Developer"          # partial last word
    assert _top("cust supp")[0].startswith("Customer Support")         # every word partial
    assert _top("sde")[0] == "Software Engineer"                       # short form -> standard title
    assert _top("softwre enginer")[0] == "Software Engineer"           # typos
    assert _top("python devlopr")[0] == "Python Developer"
    assert _top("java")[0] == "Java Developer"                         # common title ranks first
    assert "Recruiter" in _top("talent acq") or "Talent Acquisition Manager" in _top("talent acq")
    for q, want in [("nurse", "Nurse"), ("teacher", "Teacher"), ("accountant", "Accountant"),
                    ("bde", "Business Development Executive"), ("mern", "Full Stack Developer")]:
        assert want in _top(q), q


def test_standard_title_accepts_its_variants():
    # Picking "Software Engineer" also finds people who wrote "SDE" or "Software Developer"
    assert RoleMatcher.matches("Software Engineer", "SDE II at Flipkart")
    assert RoleMatcher.matches("Software Engineer", "Software Developer")
    assert not RoleMatcher.matches("Software Engineer", "Sales Executive")


def test_multiple_titles_match_any():
    role = "Java Developer | Python Developer"
    assert RoleMatcher.matches(role, "Senior Python Developer")
    assert RoleMatcher.matches(role, "Java Developer")
    assert not RoleMatcher.matches(role, "PHP Developer")


def test_linkedin_title_ids_are_sent():
    f = SearchFilterParams(role="Java Developer | AI Engineer",
                           role_titles=[{"text": "Java Developer", "id": "25201"}, {"text": "AI Engineer", "id": ""}])
    q = SalesNavURLParser.build_api_params(f)["query"]
    assert "(id:25201,text:Java Developer,selectionType:INCLUDED)" in q
    assert "text:AI Engineer,selectionType:INCLUDED" in q
    assert q.count("text:Java Developer,") == 1  # no duplicate of the id'd title


# ---------------------------------------------------------------------------
# Location: LinkedIn-style resolution to geo IDs + Gate G
# ---------------------------------------------------------------------------
import locations
from app import LINKEDIN_GEO_DIRECTORY as _GEO


def test_everyday_names_resolve_to_linkedin_ids():
    for typed, want_id in [("Bangalore", "105214831"), ("gurgaon", "104869687"), ("Bombay", "106155005"),
                           ("USA", "103644278"), ("uk", "101165590"), ("Hyderabad, India", "105556991"),
                           ("India", "102713980"), ("chennai", "106832467")]:
        r = locations.resolve(typed, None, _GEO)
        assert r["id"] == want_id and r["exact"] and not r["local_terms"], (typed, r)


def test_place_without_id_uses_parent_region_and_filters_back():
    r = locations.resolve("Banga, India", None, _GEO)
    assert r["id"] == "102713980" and not r["exact"] and r["local_terms"] == ["banga"]
    assert locations.lead_in_place("Banga, Punjab, India", r["local_terms"])
    assert not locations.lead_in_place("Mumbai, Maharashtra, India", r["local_terms"])
    assert locations.lead_in_place("", r["local_terms"])  # unknown location is not rejected


def test_picked_geo_id_is_kept():
    r = locations.resolve("Anything", "90009495", _GEO)
    assert r == {"name": "Anything", "id": "90009495", "exact": True, "local_terms": []}


def test_gate_g_rejects_leads_outside_requested_place():
    kw = dict(current_title="Java Developer", company_name="Acme", headline="#OpenToWork", bio="",
              current_positions=[], past_positions=[], metadata_open_to_work=True, target_role="Java Developer")
    out = LeadQualifier.evaluate(**kw, lead_location="Pune, Maharashtra, India", location_terms=["banga"])
    assert not out.is_qualified and "outside requested location" in out.no_match_reasons
    inside = LeadQualifier.evaluate(**kw, lead_location="Banga, Punjab, India", location_terms=["banga"])
    assert inside.is_qualified
    no_terms = LeadQualifier.evaluate(**kw, lead_location="Pune, Maharashtra, India")
    assert no_terms.is_qualified  # exact LinkedIn ID searches are not filtered again


def test_location_suggestions_like_linkedin():
    from app import search_geo
    names = lambda q: [r["name"] for r in search_geo(q)["results"]]
    assert names("bangalore")[0] == "Bengaluru, Karnataka, India"
    assert names("gurgaon")[0] == "Gurugram, Haryana, India"
    assert names("usa")[0] == "United States"
    hyd = names("hyderabad")
    assert hyd[0] == "Hyderabad, Telangana, India" and "Hyderabad, India" not in hyd  # no duplicate


def test_region_filter_escapes_commas_and_skips_idless_places():
    f = SearchFilterParams(role="Java Developer", location="Bengaluru, Karnataka, India", geo_id="105214831")
    q = SalesNavURLParser.build_api_params(f)["query"]
    assert "(id:105214831,text:Bengaluru%2C Karnataka%2C India,selectionType:INCLUDED)" in q
    # Resolved by the dashboard but no ID anywhere: no unreliable text-only REGION, Gate G filters
    f2 = SearchFilterParams(role="Java Developer", location="Dhaka, Bangladesh", location_terms=["dhaka"])
    assert "REGION" not in SalesNavURLParser.build_api_params(f2)["query"]
    # CLI path (not resolved): unchanged legacy behaviour
    f3 = SearchFilterParams(role="Java Developer", location="India")
    assert "(type:REGION,values:List((text:India,selectionType:INCLUDED)))" in SalesNavURLParser.build_api_params(f3)["query"]
    assert locations.resolve("Hyderabad, Pakistan", None, _GEO)["local_terms"] == ["hyderabad"]


def test_several_locations_are_ored_with_linkedin_ids():
    m = locations.resolve_many([{"text": "Bangalore", "id": ""}, {"text": "Chennai", "id": "106832467"}], _GEO)
    assert [r["id"] for r in m["regions"]] == ["105214831", "106832467"] and m["local_terms"] == []
    q = SalesNavURLParser.build_api_params(SearchFilterParams(role="Java Developer", regions=m["regions"]))["query"]
    assert "(type:REGION,values:List((id:105214831,text:Bengaluru%2C Karnataka%2C India,selectionType:INCLUDED)," \
           "(id:106832467,text:Chennai,selectionType:INCLUDED)))" in q


def test_mixed_locations_filter_back_to_each_place():
    # Bengaluru has its own ID, Banga only via India: Gate G must accept people in either place
    m = locations.resolve_many([{"text": "Bengaluru, Karnataka, India", "id": "105214831"}, {"text": "Banga, India", "id": ""}], _GEO)
    assert {r["id"] for r in m["regions"]} == {"105214831", "102713980"}
    assert locations.lead_in_place("Bengaluru, Karnataka, India", m["local_terms"])
    assert locations.lead_in_place("Banga, Punjab, India", m["local_terms"])
    assert not locations.lead_in_place("Pune, Maharashtra, India", m["local_terms"])


def test_place_without_any_id_drops_region_filter():
    # A REGION limited to Bengaluru would hide everyone in Dhaka, so none is sent; Gate G filters
    m = locations.resolve_many([{"text": "Bengaluru, Karnataka, India", "id": "105214831"}, {"text": "Dhaka, Bangladesh", "id": ""}], _GEO)
    assert m["regions"] == [] and "dhaka" in m["local_terms"] and "bengaluru" in m["local_terms"]


# ---------------------------------------------------------------------------
# Production CSV layout
# ---------------------------------------------------------------------------
def _export_rows(records):
    with tempfile.TemporaryDirectory() as tmp:
        db = LeadDatabase(os.path.join(tmp, "t.db"))
        for r in records:
            db.upsert_lead(r)
        path = os.path.join(tmp, "out.csv")
        db.export_to_csv(path)
        with open(path, encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        db = None
        return rows


def _rec(**kw):
    base = dict(linkedin_url="https://www.linkedin.com/in/a", first_name="Asha", last_name="Rao", full_name="Asha Rao",
                current_title="Java Developer", company_name="Acme", location="Pune, Maharashtra, India",
                is_open_to_work=True, is_qualified=True, no_match_reasons=[], discovered_at="2026-10-30 06:42:02")
    base.update(kw)
    return LeadRecord(**base)


def test_csv_headline_and_ist_times_and_first_found_kept():
    first = _rec(headline="Java Dev | Immediate joiner", discovered_at="2026-10-01 04:30:00")
    again = _rec(headline="Java Dev | Immediate joiner", discovered_at="2026-10-30 06:42:02")
    row = _export_rows([first, again])[0]
    assert row["Headline"] == "Java Dev | Immediate joiner"
    assert row["First Found (IST)"] == "01 Oct 2026, 10:00 AM"   # kept from the first search
    assert row["Last Seen (IST)"] == "30 Oct 2026, 12:12 PM"     # UTC 06:42 -> IST 12:12


def test_csv_phone_is_readable_and_safe():
    assert LeadDatabase._csv_phone("+91 98765 43210") == "(+91) 98765 43210"
    assert LeadDatabase._csv_phone("+919876543210") == "(+91) 98765 43210"
    assert LeadDatabase._csv_phone("+1-415-555-0100") == "(+1) 415-555-0100"
    assert LeadDatabase._csv_phone("98765 43210") == "98765 43210"


def test_csv_open_to_work_and_reason_are_specific():
    badge = _rec(otw_status="Yes", signal_type="Open to Work",
                 signal_description="Verified #OpenToWork indicator from Sales Navigator profile metadata.",
                 qualification_status="Qualified")
    rejected = _rec(linkedin_url="https://www.linkedin.com/in/b", is_open_to_work=False, is_qualified=False,
                    no_match_reasons=["company name match, not job title", "not open to work"],
                    qualification_status="Rejected")
    rows = {r["LinkedIn Profile URL"]: r for r in _export_rows([badge, rejected])}
    a, b = rows["https://www.linkedin.com/in/a"], rows["https://www.linkedin.com/in/b"]
    assert a["Open-to-Work Status"] == "Yes - LinkedIn #OpenToWork badge"
    assert "Qualification Reason" not in a and "Qualification Status" not in a
    assert b["Open-to-Work Status"] == "No"


def test_skills_found_for_non_tech_roles():
    skills = DataCleaner.extract_skills({}, "Inside Sales | Lead Generation | CRM | B2B Sales", "")
    assert {"Inside Sales", "Lead Generation", "CRM", "B2B Sales"} <= set(skills)
    hr = DataCleaner.extract_skills({}, "HR Executive - Recruitment, Payroll, Onboarding", "")
    assert {"Recruitment", "Payroll", "Onboarding"} <= set(hr)


def test_same_name_same_company_email_is_flagged_shared():
    from sales_nav_extractor import EMAIL_STATUS_SHARED
    with tempfile.TemporaryDirectory() as tmp:
        db = LeadDatabase(os.path.join(tmp, "t.db"))
        one = _rec(linkedin_url="https://www.linkedin.com/in/asha-rao-1", full_name="Asha Rao", company_name="Zoho",
                   email="asha.rao@zoho.com", email_status="Verified", contact_verified=True)
        two = _rec(linkedin_url="https://www.linkedin.com/in/asha-rao-2", full_name="Asha Rao", company_name="Zoho",
                   email="Asha.Rao@zoho.com", email_status="Verified", contact_verified=True)
        other = _rec(linkedin_url="https://www.linkedin.com/in/asha-rao-3", full_name="Asha Rao", company_name="Infosys",
                     email="asha.rao@infosys.com", email_status="Verified", contact_verified=True)
        db.upsert_lead(one)
        assert db.get_lead(one.linkedin_url)["email_status"] == "Verified"   # alone: still trusted
        db.upsert_lead(two)
        db.upsert_lead(other)
        assert db.get_lead(one.linkedin_url)["email_status"] == EMAIL_STATUS_SHARED
        assert db.get_lead(two.linkedin_url)["email_status"] == EMAIL_STATUS_SHARED
        assert db.get_lead(other.linkedin_url)["email_status"] == "Verified"  # different company: unaffected


def test_profile_email_is_never_flagged_shared():
    with tempfile.TemporaryDirectory() as tmp:
        db = LeadDatabase(os.path.join(tmp, "t.db"))
        mine = _rec(linkedin_url="https://www.linkedin.com/in/a1", email="asha@gmail.com", email_status="From profile")
        guess = _rec(linkedin_url="https://www.linkedin.com/in/a2", email="asha@gmail.com", email_status="Unverified")
        db.upsert_lead(mine)
        db.upsert_lead(guess)
        assert db.get_lead(mine.linkedin_url)["email_status"] == "From profile"
        assert db.get_lead(guess.linkedin_url)["email_status"] == "Risky (shared name)"


def test_api_refuses_search_without_role_and_stop_is_honest():
    from fastapi import BackgroundTasks
    import app as app_module
    for req in (app_module.StartJobRequest(role="", dry_run=True), app_module.StartJobRequest(dry_run=True)):
        assert app_module.start_extraction(req, BackgroundTasks())["error"]
    if app_module.current_job["status"] != "running":
        assert app_module.stop_extraction()["message"] == "No search is running."


def test_extension_mode_normalizes_salesnav_results_and_builds_link():
    import app as A
    el = {"firstName": "Asha", "lastName": "Rao", "fullName": "Asha Rao", "geoRegion": "Pune, Maharashtra, India",
          "entityUrn": "urn:li:fs_salesProfile:(ACwAAabc,NAME_SEARCH,x1)",
          "currentPositions": [{"title": "Java Developer", "companyName": "Acme", "startedOn": {"month": 1, "year": 2020}}],
          "spotlightBadges": [{"id": "OPEN_TO_WORK"}]}
    raw = A._normalize_salesnav_element(el)
    assert raw["title"] == "Java Developer" and raw["companyName"] == "Acme" and raw["openToWork"]
    assert raw["profileUrl"] == "https://www.linkedin.com/sales/lead/ACwAAabc,NAME_SEARCH,x1"
    assert A._normalize_salesnav_element({"foo": 1}) is None
    url = A._salesnav_search_url("Java Developer", [{"id": "105214831", "text": "Bengaluru, Karnataka, India"}], ["Senior"], "Open to work")
    assert url.startswith("https://www.linkedin.com/sales/search/people?query=(filters:List(")
    assert "type:REGION" in url and "id:102713980" in url and "type:SENIORITY_LEVEL" in url
    assert not A.SERVER_LINKEDIN_CALLS  # the server never calls LinkedIn by default


def test_email_names_drop_headline_text_and_titles():
    from sales_nav_extractor import EmailFinder
    clean = EmailFinder.clean_name
    assert clean("Murari", "Krishna-Java Full Stack Developer at Zensar") == ("Murari", "Krishna")
    assert clean("Er", "Surya Bhan") == ("Surya", "Bhan")
    assert clean("Dr. Asha", "Rao") == ("Asha", "Rao")
    assert clean("Ravi", "Kumar | Java Developer") == ("Ravi", "Kumar")
    assert clean("Priya", "Sharma (She/Her)") == ("Priya", "Sharma")
    # Ordinary names are unchanged
    assert clean("Tejesh", "Reddy KN") == ("Tejesh", "Reddy KN")
    assert clean("Jean-Luc", "Picard") == ("Jean-Luc", "Picard")
    assert clean("Anju", "Kumari") == ("Anju", "Kumari")


def test_no_work_email_without_a_real_employer():
    from sales_nav_extractor import EmailFinder, is_non_employer, EMAIL_STATUS_NOT_FOUND
    for name in ("Self", "Self-Employed", "Student", "(freelancing)", "Freelance", "Open to Work", "", "N/A"):
        assert is_non_employer(name), name
    for name in ("IBM", "Infosys", "Self Drive Rentals", "☁️ Intellinet ☁️", "Student Loan Finance Ltd"):
        assert not is_non_employer(name), name
    assert EmailFinder().find("Akshaya", "Dhaipulle", "Self") == (None, EMAIL_STATUS_NOT_FOUND)


def test_email_status_column_in_recruiter_words():
    from sales_nav_extractor import LeadDatabase
    status = LeadDatabase._csv_email_status
    assert status("a@tcs.com", "Verified") == "Confirmed"
    assert status("a@gmail.com", "From profile") == "From profile"
    assert status("a@gmail.com", "") == "From profile"  # older rows: emails read from the profile had no status
    assert status("a@ibm.com", "Risky (catch-all)") == "Likely (unconfirmed)"
    assert status("a@acme.com", "Unverified") == "Guess"
    assert status("a@zoho.com", "Risky (shared name)") == "Guess"
    assert status("", "Not found") == ""
