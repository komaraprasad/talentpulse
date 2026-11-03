#!/usr/bin/env python3
"""
TalentPulse AI — In-House Sales Navigator Extractor & Candidate Qualification Pipeline
======================================================================================
Production-grade internal pipeline to extract, clean, qualify, and export leads
directly from LinkedIn Sales Navigator sessions without third-party tools.
"""

import os
import sys
import re
import time
import random
import logging
import sqlite3
import csv
import shutil
import smtplib
import socket
import tempfile
import uuid
import threading
import unicodedata
import urllib.parse
from contextlib import closing
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple

# Attempt python-dotenv import with zero-dependency manual fallback
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(dotenv_path: Optional[str] = None) -> bool:
        path = dotenv_path or ".env"
        if not os.path.exists(path):
            return False
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip("'\"")
                if k and k not in os.environ:
                    os.environ[k] = v
        return True

import requests

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SalesNavExtractor")

# ---------------------------------------------------------------------------
# Constants & Safety Guardrails
# ---------------------------------------------------------------------------
LINKEDIN_SALES_SEARCH_API = "https://www.linkedin.com/sales-api/salesApiLeadSearch"
PAGE_SIZE = 25
SALES_NAV_MAX_CAP = 2500  # Hard Sales Navigator API query cap
DEFAULT_MAX_DAILY_LEADS = 2000  # Account safety daily guardrail
MIN_JITTER_SECS = 2.5
MAX_JITTER_SECS = 4.5
COOLDOWN_PAGES_INTERVAL = 10
COOLDOWN_PAUSE_SECS = 15.0
DAILY_QUOTA_WINDOW_HOURS = 24

# CSV export layout: identity -> role -> fit -> contact -> freshness
IST_OFFSET = timedelta(hours=5, minutes=30)
CSV_HEADERS = [
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
    "Relevant Skills / Keywords",
    "First Found (IST)",
    "Last Seen (IST)",
]

def experience_condition(exp):
    """Naukri-style experience range ("0-1", "1-3", "3-5", "5-10", "10+") as an SQL condition."""
    ranges = {"0-1": (0, 1), "1-3": (1, 3), "3-5": (3, 5), "5-10": (5, 10), "10+": (10, 99)}
    if exp not in ranges:
        return None, []
    lo, hi = ranges[exp]
    return "(years_experience >= ? AND years_experience < ?)", [lo, hi]


# Sales Navigator SENIORITY_LEVEL filter IDs, keyed by normalized UI label
SENIORITY_ID_MAP: Dict[str, List[str]] = {
    "intraining": ["100"],
    "entrylevel": ["110"],
    "entry": ["110"],
    "senior": ["120"],
    "strategic": ["130"],
    "manager": ["200", "210"],
    "director": ["220"],
    "vp": ["300"],
    "cxo": ["310"],
    "executive": ["300", "310", "320"],
    "owner": ["320"],
    "partner": ["320"],
    # Sales Navigator's own level names
    "entrylevelmanager": ["200"],
    "experiencedmanager": ["210"],
    "vicepresident": ["300"],
    "owner/partner": ["320"],
}

# Sales Navigator YEARS_OF_EXPERIENCE filter IDs and the (min, max) years each covers
EXPERIENCE_RANGES: Dict[str, Tuple[str, float, float]] = {
    "<1": ("1", 0.0, 1.0),
    "1-2": ("2", 1.0, 3.0),
    "3-5": ("3", 3.0, 6.0),
    "6-10": ("4", 6.0, 11.0),
    "10+": ("5", 11.0, 100.0),
}

CHROME_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Custom Exceptions for Status Handling
# ---------------------------------------------------------------------------
class ExtractorError(Exception):
    """Base exception for extraction errors."""
    pass


class ExpiredSessionError(ExtractorError):
    """Raised when LinkedIn returns HTTP 401 Unauthorized (expired session)."""
    pass


class RateLimitExceededError(ExtractorError):
    """Raised when LinkedIn returns HTTP 429 Too Many Requests."""
    pass


class CheckpointError(ExtractorError):
    """Raised when LinkedIn returns HTTP 999 or security challenge checkpoint."""
    pass


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------
@dataclass
class SearchFilterParams:
    role: Optional[str] = None  # one title, or several joined with " | "
    location: Optional[str] = None
    geo_id: Optional[str] = None
    # Titles picked from the typeahead: [{"text": ..., "id": LinkedIn title id or ""}]
    role_titles: List[Dict[str, str]] = field(default_factory=list)
    # Set when the place has no LinkedIn geo ID and the search used a wider region:
    # leads must mention one of these words in their location (Gate G)
    location_terms: List[str] = field(default_factory=list)
    # Several places (OR'd), each {"id": LinkedIn geo id, "text": name}; takes precedence over location/geo_id
    regions: List[Dict[str, str]] = field(default_factory=list)
    seniority_levels: List[str] = field(default_factory=list)
    experience_ranges: List[str] = field(default_factory=list)
    keywords: Optional[str] = None
    raw_query: Optional[str] = None


@dataclass
class LeadRecord:
    linkedin_url: str
    first_name: str
    last_name: str
    full_name: str
    current_title: str
    company_name: str
    location: str
    is_open_to_work: bool
    is_qualified: bool
    no_match_reasons: List[str] = field(default_factory=list)
    discovered_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    )
    # Recruiter-focused extended attributes (preserves 100% backward compatibility)
    phone: Optional[str] = None
    email: Optional[str] = None
    contact_source: str = "Not available"
    contact_verified: bool = False
    skills: List[str] = field(default_factory=list)
    otw_status: str = "No"  # Binary: "Yes" or "No"
    signal_type: str = "None"    # "Open to Work", "Job-Seeking Signal", "None"
    signal_description: str = ""
    signal_source_url: str = ""
    signal_date: str = ""
    qualification_status: str = "Needs Review"  # "Qualified", "Rejected", "Needs Review"
    qualification_reason: str = ""
    target_role_match: str = ""
    data_source: str = "LinkedIn Sales Navigator"
    updated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    )
    job_id: Optional[str] = None
    search_role: Optional[str] = None
    years_experience: Optional[float] = None
    email_status: str = ""
    headline: str = ""


# ---------------------------------------------------------------------------
# 1. Authentication & Session Management
# ---------------------------------------------------------------------------
def load_credentials(env_file: str = ".env") -> Tuple[str, str]:
    """
    Loads LI_AT session cookie and JSESSIONID CSRF token from .env.
    """
    load_dotenv(env_file)
    li_at = os.environ.get("LI_AT", "").strip()
    jsessionid = os.environ.get("JSESSIONID", "").strip()
    return li_at, jsessionid


def create_sales_nav_session(li_at: str, jsessionid: str, li_a: Optional[str] = None) -> requests.Session:
    """
    Configures a persistent requests.Session with verified headers and cookies.
    """
    if not li_at or not jsessionid:
        raise ValueError("Missing LI_AT or JSESSIONID credentials.")

    # Strip surrounding quotes and whitespace from JSESSIONID
    clean_csrf = jsessionid.strip("\"' \t\r\n")

    session = requests.Session()
    session.headers.update({
        "user-agent": CHROME_USER_AGENT,
        "accept": "application/vnd.linkedin.normalized+json+2.1",
        "x-restli-protocol-version": "2.0.0",
        "csrf-token": clean_csrf,
        "accept-language": "en-US,en;q=0.9",
        "x-li-lang": "en_US",
        "referer": "https://www.linkedin.com/sales/search/people",
    })

    # Set authentication cookies
    session.cookies.set("li_at", li_at, domain=".linkedin.com")
    session.cookies.set("JSESSIONID", clean_csrf, domain=".linkedin.com")

    # Sales Navigator seat token: without it the sales-api answers 403 SALES_SEAT_REQUIRED
    li_a = (li_a if li_a is not None else os.environ.get("LI_A", "")).strip().strip("\"'")
    if li_a:
        session.cookies.set("li_a", li_a, domain=".linkedin.com")

    return session


# ---------------------------------------------------------------------------
# 2. Ingestion & Search Protocol
# ---------------------------------------------------------------------------
class SalesNavURLParser:
    """
    Parses and transforms Sales Navigator search URLs or CLI filters into
    standardized request parameters.
    """

    @staticmethod
    def parse_url(url: str) -> SearchFilterParams:
        parsed = urllib.parse.urlparse(url)
        query_dict = urllib.parse.parse_qs(parsed.query)

        role = None
        location = None
        keywords = None
        raw_query = None

        if "query" in query_dict:
            raw_query = query_dict["query"][0]
            decoded_query = urllib.parse.unquote(raw_query)

            # Match CURRENT_TITLE filter: type:CURRENT_TITLE,values:List((text:AI Engineer,...))
            title_match = re.search(
                r'type:CURRENT_TITLE.*?text:([^\),]+)', decoded_query, re.IGNORECASE
            )
            if title_match:
                role = title_match.group(1).strip()

            # Match REGION / LOCATION filter (supports both id and text)
            geo_id_match = re.search(
                r'type:(?:REGION|GEO_REGION|LOCATION).*?id:([0-9]+)',
                decoded_query,
                re.IGNORECASE,
            )
            geo_id = geo_id_match.group(1).strip() if geo_id_match else None

            loc_match = re.search(
                r'type:(?:REGION|GEO_REGION|LOCATION).*?text:([^\),]+)',
                decoded_query,
                re.IGNORECASE,
            )
            if loc_match:
                location = loc_match.group(1).strip()

            # Match KEYWORDS filter
            kw_match = re.search(
                r'type:KEYWORDS.*?text:([^\),]+)', decoded_query, re.IGNORECASE
            )
            if kw_match:
                keywords = kw_match.group(1).strip()

            # Match SENIORITY_LEVEL filter
            seniority_levels: List[str] = []
            sen_matches = re.finditer(
                r'type:SENIORITY_LEVEL.*?values:List\(([^\)]+)\)', decoded_query, re.IGNORECASE
            )
            for m in sen_matches:
                inner = m.group(1)
                for id_match in re.finditer(r'id:([0-9]+|entry|senior|manager|director|executive|cxo|owner|partner)', inner, re.IGNORECASE):
                    seniority_levels.append(id_match.group(1))

        # Handle simple query params if present
        if not role and "title" in query_dict:
            role = query_dict["title"][0].strip()
        if not location and "geoIncluded" in query_dict:
            location = query_dict["geoIncluded"][0].strip()
        elif not location and "location" in query_dict:
            location = query_dict["location"][0].strip()
        if not keywords and "keywords" in query_dict:
            keywords = query_dict["keywords"][0].strip()

        return SearchFilterParams(
            role=role,
            location=location,
            geo_id=geo_id if 'geo_id' in locals() else None,
            seniority_levels=seniority_levels if 'seniority_levels' in locals() else [],
            keywords=keywords,
            raw_query=raw_query,
        )

    @staticmethod
    def build_api_params(
        filters: SearchFilterParams, start: int = 0, count: int = PAGE_SIZE
    ) -> Dict[str, Any]:
        """
        Builds LinkedIn internal salesApiLeadSearch request query parameters.
        Accurately structures REGION filters with exact Geo URNs when available.
        """
        params: Dict[str, Any] = {
            "start": start,
            "count": count,
            "q": "searchQuery",
        }
        if filters.raw_query:
            params["query"] = filters.raw_query
        else:
            # Construct standard restli query representation
            sub_filters = []
            title_values = []
            seen_titles = set()

            def _add_title(text: str, title_id: str = "") -> None:
                text = re.sub(r"[(),:]", " ", text or "").strip()
                if not text or text.lower() in seen_titles or len(title_values) >= 12:
                    return
                seen_titles.add(text.lower())
                id_part = f"id:{title_id}," if str(title_id).isdigit() else ""
                title_values.append(f"({id_part}text:{text},selectionType:INCLUDED)")

            # LinkedIn standard titles (with id) first: LinkedIn expands those to every variant itself
            for t in filters.role_titles:
                if t.get("id"):
                    _add_title(t.get("text", ""), t["id"])
            # Free text / catalogue titles: several spellings OR'd ('Full Stack' / 'Fullstack' / singular)
            for alt in RoleMatcher.split_roles(filters.role or ""):
                for v in RoleMatcher.search_variants(alt, limit=3):
                    _add_title(v)
            if title_values:
                sub_filters.append(f"(type:CURRENT_TITLE,values:List({','.join(title_values)}))")
            def _esc(text: str) -> str:
                # Commas/colons/parentheses are restli syntax, so escape them inside the text value
                # ("Bengaluru, Karnataka, India" would otherwise split into three values)
                return re.sub(r"[,:()]", lambda m: urllib.parse.quote(m.group(0)), text or "")

            if filters.regions:
                # Several places OR'd, like picking several locations in Sales Navigator
                region_values, seen_ids = [], set()
                for r in filters.regions:
                    rid = str(r.get("id") or "")
                    if rid.isdigit() and rid not in seen_ids:
                        seen_ids.add(rid)
                        region_values.append(f"(id:{rid},text:{_esc(r.get('text', ''))},selectionType:INCLUDED)")
                if region_values:
                    sub_filters.append(f"(type:REGION,values:List({','.join(region_values)}))")
            elif filters.location:
                region_text = _esc(filters.location)
                if filters.geo_id:
                    sub_filters.append(
                        f"(type:REGION,values:List((id:{filters.geo_id},text:{region_text},selectionType:INCLUDED)))"
                    )
                elif filters.location_terms:
                    # No LinkedIn geo ID anywhere up the hierarchy: a text-only REGION is not a
                    # reliable LinkedIn filter, so don't send one. Gate G keeps only leads in the place.
                    pass
                else:
                    sub_filters.append(
                        f"(type:REGION,values:List((text:{filters.location},selectionType:INCLUDED)))"
                    )
            if filters.seniority_levels:
                # Sales Navigator expects numeric seniority IDs; translate UI labels
                # and pass through anything already numeric.
                sen_ids: List[str] = []
                for level in filters.seniority_levels:
                    key = str(level).strip().lower().replace("-", "").replace(" ", "")
                    for s_id in SENIORITY_ID_MAP.get(key, [str(level).strip()] if str(level).strip().isdigit() else []):
                        if s_id not in sen_ids:
                            sen_ids.append(s_id)
                sen_items = [
                    f"(id:{s_id},selectionType:INCLUDED)" for s_id in sen_ids
                ]
                if sen_items:
                    sub_filters.append(
                        f"(type:SENIORITY_LEVEL,values:List({','.join(sen_items)}))"
                    )
            exp_ids = [
                EXPERIENCE_RANGES[r][0] for r in filters.experience_ranges if r in EXPERIENCE_RANGES
            ]
            if exp_ids:
                exp_items = [f"(id:{e_id},selectionType:INCLUDED)" for e_id in exp_ids]
                sub_filters.append(
                    f"(type:YEARS_OF_EXPERIENCE,values:List({','.join(exp_items)}))"
                )
            if filters.keywords:
                sub_filters.append(
                    f"(type:KEYWORDS,values:List((text:{filters.keywords},selectionType:INCLUDED)))"
                )

            filter_str = f"(filters:List({','.join(sub_filters)}))" if sub_filters else "()"
            params["query"] = filter_str

        return params


# ---------------------------------------------------------------------------
# 3. Proprietary Cleaning & Qualification Engine
# ---------------------------------------------------------------------------
class DataCleaner:
    """
    Normalizes names, headlines, and company names following TalentPulse AI standards.
    """

    PRONOUN_PATTERN = re.compile(
        r"[\(\[\{/]?\s*\b(?:he/him|she/her|they/them|he/they|she/they|xe/xem|any/all)\b\s*[\)\]\}/]?",
        re.IGNORECASE,
    )

    DEGREE_SUFFIX_PATTERNS = [
        re.compile(r"[,/]?\s*\bPh\.?D\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bM\.?B\.?A\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bM\.?Sc\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bM\.?S\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bB\.?Tech\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bB\.?E\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bB\.?S\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bCPA\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bM\.?D\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bPMP\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bEsq\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bJ\.?D\.?\b", re.IGNORECASE),
    ]

    PREFIX_PATTERNS = re.compile(
        r"^(?:Dr\.|Prof\.|Mr\.|Ms\.|Mrs\.)\s+", re.IGNORECASE
    )

    COMPANY_LEGAL_SUFFIXES = [
        re.compile(r"[,/]?\s*\bPvt\.?\s*Ltd\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bPrivate\s+Limited\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bLtd\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bLimited\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bInc\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bIncorporated\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bL\.?L\.?C\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bCorp\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bCorporation\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bCo\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bCompany\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bGmbH\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bS\.?A\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bS\.?R\.?L\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bB\.?V\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bL\.?L\.?P\.?\b", re.IGNORECASE),
        re.compile(r"[,/]?\s*\bP\.?C\.?\b", re.IGNORECASE),
    ]

    @classmethod
    def clean_name(cls, raw_name: str) -> str:
        """
        Strips emojis, decorative symbols, pronoun blocks, and degree suffixes/prefixes,
        while strictly preserving accented Latin, Devanagari, CJK, and other international letters.
        """
        if not raw_name:
            return ""

        # 1. Filter out symbols, emojis, and unassigned/surrogate characters
        chars = []
        for ch in raw_name:
            cat = unicodedata.category(ch)
            if cat.startswith(('S', 'C')):
                chars.append(" ")
            else:
                chars.append(ch)
        text = "".join(chars)

        # 2. Remove pronoun blocks
        text = cls.PRONOUN_PATTERN.sub(" ", text)

        # 3. Remove degree suffixes
        for pat in cls.DEGREE_SUFFIX_PATTERNS:
            text = pat.sub(" ", text)

        # 4. Remove prefixes (Dr., Prof.)
        text = cls.PREFIX_PATTERNS.sub(" ", text)

        # 5. Keep letters (L), combining marks (M), spaces, hyphens, and apostrophes
        filtered_chars = []
        for ch in text:
            cat = unicodedata.category(ch)
            if cat.startswith(('L', 'M')) or ch in (" ", "-", "'"):
                filtered_chars.append(ch)
            else:
                filtered_chars.append(" ")
        text = "".join(filtered_chars)

        # 6. Normalize whitespace and trim
        text = re.sub(r"\s+", " ", text).strip(" ,-./'")
        return text

    @classmethod
    def split_name(cls, cleaned_full_name: str) -> Tuple[str, str]:
        """
        Splits clean full name into (first_name, last_name).
        """
        if not cleaned_full_name:
            return "", ""
        parts = cleaned_full_name.split()
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], " ".join(parts[1:])

    @classmethod
    def clean_headline(cls, raw_headline: str) -> str:
        """
        Strips emojis, non-ASCII characters, and decorative symbols from headlines.
        """
        if not raw_headline:
            return ""
        return cls.clean_text(raw_headline)

    @classmethod
    def clean_text(cls, raw: str) -> str:
        """Readable text from LinkedIn: decodes HTML codes (&amp; &#39;), keeps apostrophes and accented
        letters, drops emojis and decorative symbols, and restores the space LinkedIn drops between sentences."""
        import html
        if not raw:
            return ""
        text = html.unescape(html.unescape(str(raw)))
        text = re.sub(r"(?<![&\w])(amp|quot|apos|lt|gt|nbsp);", lambda m: {"amp": "&", "quot": '"', "apos": "'",
                                                                            "lt": "<", "gt": ">", "nbsp": " "}[m.group(1)], text)
        text = text.translate({0x2018: "'", 0x2019: "'", 0x201C: '"', 0x201D: '"', 0x2013: "-", 0x2014: "-",
                               0x2026: "...", 0x00A0: " "})
        # Emojis and pictographs out; letters (including accented ones), digits and punctuation stay
        text = "".join(ch for ch in text if unicodedata.category(ch)[0] in "LNPZ" or ch in "+#&|/$%@")
        text = re.sub(r"[★✦✪➔•~]+", " ", text)
        text = re.sub(r"([a-z0-9)]|[A-Z]{2})([.!?])([A-Z][a-z])", r"\1\2 \3", text)  # "solutions.Currently" -> "solutions. Currently"
        text = re.sub(r"\s+", " ", text).strip(" |-,")
        return text

    @classmethod
    def clean_company_name(cls, raw_company: str) -> str:
        """
        Strips legal suffixes (Inc., LLC, Pvt Ltd, etc.) and decorative symbols.
        """
        if not raw_company:
            return ""
        text = re.sub(r"[^\x00-\x7F]+", " ", raw_company)
        for suffix_pat in cls.COMPANY_LEGAL_SUFFIXES:
            text = suffix_pat.sub(" ", text)
        # Remove lingering symbols while keeping alphanumeric, hyphens, and ampersands
        text = re.sub(r"[^a-zA-Z0-9\s\-\'&]", " ", text)
        text = re.sub(r"\s+", " ", text).strip(" ,-./&")
        return text

    @classmethod
    def normalize_linkedin_url(cls, raw_url: str) -> str:
        """
        Normalizes LinkedIn profile and Sales Navigator URLs conservatively:
        - Lowercases scheme and netloc (standardizing www.linkedin.com to linkedin.com)
        - Strips tracking/query parameters (e.g., trk, miniProfileUrn)
        - Strips URL fragments
        - Strips trailing slashes without altering path components
        """
        if not raw_url:
            return ""
        url = raw_url.strip()
        parsed = urllib.parse.urlparse(url)
        if not parsed.scheme and not parsed.netloc:
            return url

        scheme = parsed.scheme.lower() if parsed.scheme else "https"
        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]

        # Clean path: normalize slashes, strip trailing slash unless root path
        path = parsed.path.rstrip("/")

        return urllib.parse.urlunparse((scheme, netloc, path, "", "", ""))

    EMAIL_REGEX = re.compile(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    )

    # Phone numbers in public headline/bio text, tried in order:
    # 1. after a label ("Mobile:", "Ph", "Call me on", "Contact no", "WhatsApp" ...)
    # 2. international format starting with +
    # 3. bare Indian mobile: 10 digits starting 6-9, optional 91/0 prefix
    # Years and date ranges ("2019-2024", "5+ years") never match these shapes.
    PHONE_PATTERNS = [
        re.compile(
            r"\b(?:phone|ph|mobile|mob|cell|tel|whatsapp|wa|call|contact|reach)"
            r"(?:\s*(?:no|number|num|#))?\.?"
            r"(?:\s+(?:me|us))?(?:\s+(?:at|on|@))?"
            r"\s*[:\-]?\s*(\+?[\d\(][\d\(\)\s\.\-]{6,20}\d)",
            re.IGNORECASE,
        ),
        re.compile(r"(\+\d{1,4}[\s\(\)\.\-]*\d{2,4}[\s\(\)\.\-]*\d{3,4}[\s\(\)\.\-]*\d{3,5})"),
        re.compile(r"(?<![\d+])((?:\+?91[\s\-]?|0)?[6-9]\d{4}[\s\-]?\d{5})(?!\d)"),
    ]

    @classmethod
    def extract_contact_info(
        cls, raw_lead: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[str], str, bool]:
        """
        Extracts available mobile number and email address from authorized data structures,
        strictly preserving international phone formatting, country codes, and leading zeros.
        Never fabricates contact details; distinguishes verified from unverified data.
        Returns: (phone, email, contact_source, contact_verified)
        """
        phone: Optional[str] = None
        email: Optional[str] = None
        contact_source: str = "Not available"
        contact_verified: bool = False

        # 1. Direct fields in authorized raw_lead dictionary
        raw_phone = raw_lead.get("phone") or raw_lead.get("phoneNumber") or raw_lead.get("mobileNumber")
        if isinstance(raw_phone, list) and raw_phone:
            raw_phone = raw_phone[0]
        if raw_phone and str(raw_phone).strip():
            phone = str(raw_phone).strip()
            contact_source = "Authorized Data Source"
            contact_verified = bool(raw_lead.get("phoneVerified", False))

        raw_email = raw_lead.get("email") or raw_lead.get("emailAddress") or raw_lead.get("mail")
        if isinstance(raw_email, list) and raw_email:
            raw_email = raw_email[0]
        if raw_email and str(raw_email).strip():
            email = str(raw_email).strip()
            contact_source = "Authorized Data Source" if contact_source == "Not available" else contact_source
            contact_verified = contact_verified or bool(raw_lead.get("emailVerified", False))

        # 2. Check contactInfo object if present
        contact_obj = raw_lead.get("contactInfo", {})
        if isinstance(contact_obj, dict):
            if not phone and contact_obj.get("phoneNumbers"):
                p_list = contact_obj["phoneNumbers"]
                if isinstance(p_list, list) and p_list:
                    first_p = p_list[0]
                    phone = str(first_p.get("number") if isinstance(first_p, dict) else first_p).strip()
                    contact_source = "LinkedIn Contact Info"
                    contact_verified = bool(first_p.get("verified", False) if isinstance(first_p, dict) else False)
            if not email and contact_obj.get("emailAddresses"):
                e_list = contact_obj["emailAddresses"]
                if isinstance(e_list, list) and e_list:
                    first_e = e_list[0]
                    email = str(first_e.get("address") if isinstance(first_e, dict) else first_e).strip()
                    contact_source = "LinkedIn Contact Info"
                    contact_verified = contact_verified or bool(first_e.get("verified", False) if isinstance(first_e, dict) else False)

        # 3. Fallback: inspect public bio/headline for explicit user-published contact info
        headline = raw_lead.get("headline", "")
        bio = raw_lead.get("summary") or raw_lead.get("bio", "")
        combined_text = f"{headline} {bio}"

        if not email:
            email_match = cls.EMAIL_REGEX.search(combined_text)
            if email_match:
                email = email_match.group(0).strip()
                contact_source = "Profile Bio / Headline"
                contact_verified = False  # unverified public text

        if not phone:
            for pattern in cls.PHONE_PATTERNS:
                match = pattern.search(combined_text)
                if not match:
                    continue
                phone_candidate = match.group(1).strip(" ,.-")
                digits = sum(c.isdigit() for c in phone_candidate)
                if 7 <= digits <= 15:
                    phone = phone_candidate
                    contact_source = "Profile Bio / Headline"
                    contact_verified = False
                    break

        return phone, email, contact_source, contact_verified

    @staticmethod
    def _position_date(value: Any) -> Optional[Tuple[int, int]]:
        """Reads a LinkedIn {year, month} date object into (year, month)."""
        if not isinstance(value, dict) or not value.get("year"):
            return None
        try:
            return int(value["year"]), int(value.get("month") or 1)
        except (TypeError, ValueError):
            return None

    @classmethod
    def estimate_years_experience(
        cls, raw_lead: Dict[str, Any], now: Optional[datetime] = None
    ) -> Optional[float]:
        """
        Estimates total professional experience in years from position start/end
        dates (Sales Navigator `startedOn`/`endedOn` or Voyager `dateRange`).
        Overlapping roles are merged so concurrent jobs are not double-counted.
        Returns None when no position carries a start date.
        """
        now = now or datetime.now(timezone.utc)
        now_month = now.year * 12 + (now.month - 1)
        intervals: List[Tuple[int, int]] = []

        for key in ("currentPositions", "pastPositions", "positions"):
            positions = raw_lead.get(key) or []
            if not isinstance(positions, list):
                continue
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                date_range = pos.get("dateRange") if isinstance(pos.get("dateRange"), dict) else {}
                start = cls._position_date(pos.get("startedOn") or date_range.get("start"))
                if not start:
                    continue
                end = cls._position_date(pos.get("endedOn") or date_range.get("end"))
                start_m = start[0] * 12 + (start[1] - 1)
                end_m = (end[0] * 12 + (end[1] - 1)) if end else now_month
                end_m = min(end_m, now_month)
                if end_m >= start_m:
                    intervals.append((start_m, end_m + 1))

        if not intervals:
            return None

        intervals.sort()
        total_months = 0
        cur_start, cur_end = intervals[0]
        for s, e in intervals[1:]:
            if s <= cur_end:
                cur_end = max(cur_end, e)
            else:
                total_months += cur_end - cur_start
                cur_start, cur_end = s, e
        total_months += cur_end - cur_start
        return round(total_months / 12, 1)

    # Skills recognised in headline/bio when LinkedIn sends no skills list (all job families)
    SKILL_KEYWORDS = [
        # Software / data / AI
        "Python", "Java", "JavaScript", "TypeScript", "C++", "C#", ".NET", "Golang", "Rust", "PHP", "Ruby",
        "Kotlin", "Swift", "Spring Boot", "Django", "FastAPI", "Node.js", "React", "Angular", "Vue.js",
        "Next.js", "Flutter", "React Native", "Android", "iOS", "SQL", "PostgreSQL", "MySQL", "MongoDB",
        "GraphQL", "Microservices", "REST API", "Docker", "Kubernetes", "AWS", "Azure", "GCP", "Terraform",
        "Jenkins", "CI/CD", "DevOps", "Linux", "Git", "Selenium", "Manual Testing", "Automation Testing",
        "Machine Learning", "Deep Learning", "NLP", "Computer Vision", "LLMs", "GenAI", "PyTorch",
        "TensorFlow", "Data Analysis", "Power BI", "Tableau", "Excel", "Spark", "Hadoop", "Snowflake",
        "Cybersecurity", "SAP", "Salesforce", "ServiceNow",
        # Sales / business development / customer
        "B2B Sales", "B2C Sales", "Inside Sales", "Field Sales", "Lead Generation", "Cold Calling",
        "Negotiation", "Account Management", "Key Account Management", "Business Development", "CRM",
        "HubSpot", "Pre-Sales", "Customer Success", "Customer Service", "Client Relationship",
        # Marketing / content / design
        "Digital Marketing", "SEO", "SEM", "Google Ads", "Meta Ads", "Social Media Marketing",
        "Content Writing", "Copywriting", "Email Marketing", "Brand Management", "Market Research",
        "Figma", "Adobe Photoshop", "Illustrator", "UI/UX", "Video Editing",
        # HR / recruiting
        "Recruitment", "Talent Acquisition", "Sourcing", "Onboarding", "Payroll", "HRMS",
        "Employee Engagement", "Performance Management", "Compliance",
        # Finance / accounting
        "Accounting", "Tally", "GST", "Taxation", "Auditing", "Financial Analysis", "Financial Modeling",
        "Budgeting", "Accounts Payable", "Accounts Receivable", "Bookkeeping", "IFRS",
        # Operations / supply chain / engineering / other
        "Operations Management", "Supply Chain", "Logistics", "Procurement", "Inventory Management",
        "Project Management", "Agile", "Scrum", "Jira", "Six Sigma", "Lean", "AutoCAD", "SolidWorks",
        "PLC", "Quality Control", "Teaching", "Nursing", "Patient Care", "Medical Coding",
    ]

    @classmethod
    def extract_skills(
        cls, raw_lead: Dict[str, Any], headline: str = "", bio: str = ""
    ) -> List[str]:
        """
        Extracts candidate skills/keywords from structured skills list or profile text.
        """
        skills: List[str] = []
        if raw_lead.get("skills"):
            s_val = raw_lead["skills"]
            if isinstance(s_val, list):
                for item in s_val:
                    if isinstance(item, str) and item.strip():
                        skills.append(item.strip())
                    elif isinstance(item, dict) and item.get("name"):
                        skills.append(item["name"].strip())

        if not skills:
            common_keywords = cls.SKILL_KEYWORDS
            combined = f"{headline} {bio}".lower()
            for kw in common_keywords:
                if re.search(rf"\b{re.escape(kw.lower())}\b", combined):
                    skills.append(kw)

        return skills


# ---------------------------------------------------------------------------
# 3b. Work Email Finder (domain discovery + pattern guessing + SMTP check)
# ---------------------------------------------------------------------------
EMAIL_STATUS_VERIFIED = "Verified"
EMAIL_STATUS_RISKY = "Risky (catch-all)"
EMAIL_STATUS_UNVERIFIED = "Unverified"
EMAIL_STATUS_PROFILE = "From profile"
EMAIL_STATUS_NOT_FOUND = "Not found"
# Two candidates resolved to the same mailbox (e.g. two "Asha Rao" at Zoho): the server only
# proves the mailbox exists, not whose it is, so neither can be trusted
EMAIL_STATUS_SHARED = "Risky (shared name)"


class EmailFinder:
    """
    Finds a candidate's work email the way commercial finders do:
      1. Resolve the employer's email domain (company website, else name-based
         domain candidates that have MX records).
      2. Generate the common address patterns from first/last name.
      3. Ask the domain's mail server (SMTP RCPT TO, no message sent) which
         address exists, after detecting catch-all servers.
    Results are cached per domain so each company is probed only once.
    """

    DOH_URL = "https://cloudflare-dns.com/dns-query"
    TLDS = ("com", "in", "co.in", "io", "ai", "co", "net", "tech")
    SMTP_TIMEOUT = 10
    HELO_HOST = "talentpulse.local"
    MAIL_FROM = "verify@talentpulse.local"
    # Many cloud hosts block outgoing port 25. After this many failed connections in a row, checks pause
    # for SMTP_PAUSE seconds and addresses are saved as unverified guesses instead of waiting each time.
    SMTP_FAILS_BEFORE_PAUSE = 3
    SMTP_PAUSE = 30 * 60

    def __init__(self) -> None:
        # TALENTPULSE_SMTP_CHECK=0 turns mail-server checks off (domains and patterns are still found)
        self.smtp_enabled = os.environ.get("TALENTPULSE_SMTP_CHECK", "1") != "0"
        self._smtp_fails = 0
        self._smtp_paused_until = 0.0
        self._mx_cache: Dict[str, Optional[str]] = {}
        self._domain_cache: Dict[str, Optional[str]] = {}
        self._catch_all_cache: Dict[str, Optional[bool]] = {}
        self._pattern_cache: Dict[str, str] = {}  # domain -> pattern name that verified

    # ----------------------------- helpers ---------------------------------
    @staticmethod
    def _ascii_slug(text: str) -> str:
        text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode()
        return re.sub(r"[^a-z0-9]", "", text.lower())

    @classmethod
    def candidate_addresses(cls, first: str, last: str, domain: str) -> List[Tuple[str, str]]:
        """Common corporate address patterns, most frequent first: (pattern, email)."""
        f, l = cls._ascii_slug(first), cls._ascii_slug(last)
        if not f:
            return []
        patterns: List[Tuple[str, str]] = []
        if l:
            patterns += [
                ("first.last", f"{f}.{l}"), ("firstlast", f"{f}{l}"), ("flast", f"{f[0]}{l}"),
                ("first", f), ("f.last", f"{f[0]}.{l}"), ("first_last", f"{f}_{l}"),
                ("last.first", f"{l}.{f}"), ("firstl", f"{f}{l[0]}"), ("last", l),
            ]
        else:
            patterns.append(("first", f))
        seen, out = set(), []
        for name, local in patterns:
            if local not in seen:
                seen.add(local)
                out.append((name, f"{local}@{domain}"))
        return out

    def mx_host(self, domain: str) -> Optional[str]:
        """Highest-priority MX host for a domain via DNS-over-HTTPS (None if none)."""
        if domain in self._mx_cache:
            return self._mx_cache[domain]
        host = None
        try:
            resp = requests.get(
                self.DOH_URL, params={"name": domain, "type": "MX"},
                headers={"accept": "application/dns-json"}, timeout=8,
            )
            answers = [a for a in resp.json().get("Answer", []) if a.get("type") == 15]
            records = sorted(
                (int(a["data"].split()[0]), a["data"].split()[1].rstrip(".")) for a in answers
            )
            if records and records[0][1] not in ("", "."):
                host = records[0][1]
        except Exception as e:
            logger.debug("MX lookup failed for %s: %s", domain, e)
        self._mx_cache[domain] = host
        return host

    def find_domain(self, company_name: str, website: Optional[str] = None) -> Optional[str]:
        """Employer email domain: website host if known, else name-based guesses with MX."""
        if website:
            host = urllib.parse.urlparse(website if "//" in website else f"//{website}").netloc.lower()
            host = host.split(":")[0]
            if host.startswith("www."):
                host = host[4:]
            if host and self.mx_host(host):
                return host

        key = (company_name or "").strip().lower()
        if not key or key == "not available":
            return None
        if key in self._domain_cache:
            return self._domain_cache[key]

        words = [w for w in re.split(r"[^a-z0-9]+", self._ascii_slug_words(company_name)) if w]
        # Whole-name stem only: first-word ("Some Startup" -> some.com) and hyphenated
        # ("tata-consultancy.com") guesses hit unrelated or parked domains in testing,
        # and a wrong email is worse than none.
        stems: List[str] = []
        for stem in ("".join(words),):
            if stem and stem not in stems:
                stems.append(stem)

        domain = None
        for stem in stems:
            for tld in self.TLDS:
                cand = f"{stem}.{tld}"
                if self.mx_host(cand):
                    domain = cand
                    break
            if domain:
                break
        self._domain_cache[key] = domain
        return domain

    @staticmethod
    def _ascii_slug_words(text: str) -> str:
        text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode().lower()
        # Drop generic words that are rarely part of the email domain
        stop = {"the", "and", "of", "pvt", "ltd", "private", "limited", "inc", "llc", "corp",
                "corporation", "company", "co", "plc", "gmbh", "llp",
                "technologies", "technology", "solutions", "services", "software", "systems",
                "group", "global", "india", "labs", "consulting"}
        words = [w for w in re.split(r"[^a-z0-9]+", text) if w and w not in stop]
        return " ".join(words) if words else re.sub(r"[^a-z0-9 ]", " ", text)

    def _smtp_check(self, mx: str, emails: List[str]) -> Dict[str, Optional[bool]]:
        """RCPT TO each address on one SMTP session. True=exists, False=rejected, None=unknown."""
        results: Dict[str, Optional[bool]] = {e: None for e in emails}
        if not self.smtp_enabled or time.time() < self._smtp_paused_until:
            return results
        try:
            smtp_conn = smtplib.SMTP(mx, 25, timeout=self.SMTP_TIMEOUT)
        except (OSError, smtplib.SMTPException, socket.timeout) as e:
            self._smtp_fails += 1
            if self._smtp_fails >= self.SMTP_FAILS_BEFORE_PAUSE:
                self._smtp_paused_until = time.time() + self.SMTP_PAUSE
                self._smtp_fails = 0
                logger.warning("Can't reach mail servers on port 25 (%s); likely blocked by this host. Email "
                               "checks paused for %d min; emails are saved as unverified guesses.", e, self.SMTP_PAUSE // 60)
            else:
                logger.debug("SMTP connection to %s failed: %s", mx, e)
            return results
        self._smtp_fails = 0
        try:
            with smtp_conn as smtp:
                smtp.ehlo(self.HELO_HOST)
                code, _ = smtp.mail(self.MAIL_FROM)
                if code >= 400:
                    return results
                for email in emails:
                    code, _ = smtp.rcpt(email)
                    if 200 <= code < 300:
                        results[email] = True
                    elif code in (550, 551, 553):
                        results[email] = False
                    # 4xx (greylisting) / other codes stay unknown
        except (OSError, smtplib.SMTPException, socket.timeout) as e:
            logger.debug("SMTP check against %s failed: %s", mx, e)
        return results

    # ------------------------------ main -----------------------------------
    def find(
        self, first_name: str, last_name: str, company_name: str, website: Optional[str] = None
    ) -> Tuple[Optional[str], str]:
        """Returns (email, status) where status is one of the EMAIL_STATUS_* values."""
        domain = self.find_domain(company_name, website)
        if not domain:
            return None, EMAIL_STATUS_NOT_FOUND
        candidates = self.candidate_addresses(first_name, last_name, domain)
        if not candidates:
            return None, EMAIL_STATUS_NOT_FOUND

        # Try the pattern already proven at this company first
        known = self._pattern_cache.get(domain)
        if known:
            candidates.sort(key=lambda c: c[0] != known)
        best_guess = candidates[0][1]

        mx = self.mx_host(domain)
        if not mx:
            return best_guess, EMAIL_STATUS_UNVERIFIED

        # Catch-all detection: a random address must be rejected for checks to mean anything
        if domain not in self._catch_all_cache:
            probe = f"tp{uuid.uuid4().hex[:12]}@{domain}"
            self._catch_all_cache[domain] = self._smtp_check(mx, [probe])[probe]
        catch_all = self._catch_all_cache[domain]
        if catch_all is True:
            return best_guess, EMAIL_STATUS_RISKY
        if catch_all is None:
            return best_guess, EMAIL_STATUS_UNVERIFIED

        results = self._smtp_check(mx, [email for _, email in candidates])
        for pattern, email in candidates:
            if results.get(email) is True:
                self._pattern_cache[domain] = pattern
                return email, EMAIL_STATUS_VERIFIED
        if all(results.get(email) is False for _, email in candidates):
            return None, EMAIL_STATUS_NOT_FOUND
        return best_guess, EMAIL_STATUS_UNVERIFIED


class QualificationResult(tuple):
    """
    Subclasses tuple (is_qualified, no_match_reasons, is_open_to_work)
    to provide 100% backward-compatible 3-tuple unpacking while exposing
    rich recruiter-facing qualification and job-seeking signal attributes.
    """
    def __new__(
        cls,
        is_qualified: bool,
        no_match_reasons: List[str],
        is_open_to_work: bool,
        otw_status: str = "No",
        signal_type: str = "None",
        signal_description: str = "",
        qualification_status: str = "Needs Review",
        qualification_reason: str = "",
        target_role_match: str = "",
    ):
        obj = super().__new__(cls, (is_qualified, no_match_reasons, is_open_to_work))
        obj.is_qualified = is_qualified
        obj.no_match_reasons = no_match_reasons
        obj.is_open_to_work = is_open_to_work
        obj.otw_status = otw_status
        obj.signal_type = signal_type
        obj.signal_description = signal_description
        obj.qualification_status = qualification_status
        obj.qualification_reason = qualification_reason
        obj.target_role_match = target_role_match
        return obj


class RoleMatcher:
    """
    Loose job-title matching: 'java full stack developers' matches 'Java Fullstack Developer'.
    Normalises case, punctuation, plurals, compound spellings and common short forms,
    then compares whole words (so 'java' no longer matches 'javascript').
    """

    # Two-word spellings joined into one token (both sides are normalised the same way)
    COMPOUNDS = [
        ("full stack", "fullstack"), ("front end", "frontend"), ("back end", "backend"),
        ("dev ops", "devops"), ("dev sec ops", "devsecops"), ("data base", "database"),
        ("node js", "nodejs"), ("react js", "reactjs"), ("vue js", "vuejs"),
        ("next js", "nextjs"), ("angular js", "angularjs"), ("dot net", "dotnet"),
        ("web site", "website"), ("share point", "sharepoint"), ("sales force", "salesforce"),
    ]
    # Short forms -> full words
    ABBREVIATIONS = {
        "sr": "senior", "snr": "senior", "jr": "junior", "jnr": "junior",
        "dev": "developer", "devs": "developer", "developper": "developer",
        "eng": "engineer", "engr": "engineer", "engg": "engineer",
        "mgr": "manager", "mngr": "manager", "asst": "assistant", "assoc": "associate",
        "admin": "administrator", "arch": "architect", "exec": "executive",
        "mgmt": "management", "dir": "director", "vp": "vice president",
        "sde": "software development engineer", "swe": "software engineer",
        "fsd": "fullstack developer", "ml": "machine learning", "ai": "ai",
        "ui": "ui", "ux": "ux", "qa": "qa", "hr": "hr", "bi": "bi",
        "net": "dotnet", "golang": "go", "k8s": "kubernetes",
    }
    # Plural endings that are safe to strip ('sales', 'analytics', 'ops' stay as they are)
    _PLURAL_STEMS = ("er", "or", "ist", "yst", "ant", "ent", "ian", "ect", "ead", "ner", "ger", "ive", "ate")
    _STOP = {"and", "or", "in", "at", "for", "of", "the", "a", "an", "with", "to"}

    @classmethod
    def _singular(cls, word: str) -> str:
        if len(word) > 4 and word.endswith("ies"):
            return word[:-3] + "y"
        if len(word) > 3 and word.endswith("s") and word[:-1].endswith(cls._PLURAL_STEMS):
            return word[:-1]
        return word

    @classmethod
    def _base_text(cls, text: str) -> str:
        t = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode().lower()
        t = t.replace("c++", "cplusplus").replace("c#", "csharp").replace(".net", " dotnet")
        t = re.sub(r"(\w)\.js\b", r"\1js", t)            # react.js -> reactjs
        t = re.sub(r"[^a-z0-9+#]+", " ", t)                 # hyphens, slashes, commas -> spaces
        for spaced, joined in cls.COMPOUNDS:                 # before short forms: 'dev ops' != 'developer ops'
            t = re.sub(rf"\b{spaced}\b", joined, t)
        words = []
        for w in t.split():
            w = cls._singular(w)
            words.extend(cls.ABBREVIATIONS.get(w, w).split())
        t = " ".join(cls._singular(w) for w in words)
        for spaced, joined in cls.COMPOUNDS:
            t = re.sub(rf"\b{spaced}\b", joined, t)
        return t

    @classmethod
    def tokens(cls, text: str) -> List[str]:
        return cls._base_text(text).split()

    @classmethod
    def keywords(cls, role: str) -> List[str]:
        return [w for w in cls.tokens(role) if w not in cls._STOP]

    @classmethod
    def _matches_one(cls, role: str, have: set) -> bool:
        kws = cls.keywords(role)
        return bool(kws) and all(k in have for k in kws)

    @classmethod
    def matches(cls, role: str, text: str) -> bool:
        """
        True when every meaningful word of the role appears as a whole word in text.
        'A | B' means any of several selected titles; a standard title also accepts its
        known variants (Software Engineer -> SDE, Software Developer), like LinkedIn does.
        """
        import job_titles
        have = set(cls.tokens(text))
        for alt in cls.split_roles(role):
            if cls._matches_one(alt, have):
                return True
            if any(cls._matches_one(v, have) for v in job_titles.aliases(alt)):
                return True
        return False

    @staticmethod
    def split_roles(role: str) -> List[str]:
        return [r.strip() for r in (role or "").split("|") if r.strip()]

    @classmethod
    def search_variants(cls, role: str, limit: int = 4) -> List[str]:
        """
        Spellings sent to LinkedIn's title filter (OR'd), so the live search itself also
        returns 'Full Stack' and 'Fullstack' people when the user typed either.
        """
        clean = re.sub(r"[(),:]", " ", role or "")
        clean = re.sub(r"\s+", " ", clean).strip()
        if not clean:
            return []
        expanded = " ".join(cls.ABBREVIATIONS.get(w.lower(), w) if w.lower() in cls.ABBREVIATIONS
                            and w.lower() not in ("ai", "ui", "ux", "qa", "hr", "bi", "net", "js", "ts", "py")
                            else w for w in clean.split())
        singular = " ".join(cls._singular(w.lower()) if w.lower() != cls._singular(w.lower()) else w
                            for w in expanded.split())
        variants = [singular]
        low = singular.lower()
        for spaced, joined in cls.COMPOUNDS:
            if re.search(rf"\b{spaced}\b", low):
                variants.append(re.sub(rf"\b{spaced}\b", joined, singular, flags=re.IGNORECASE))
            elif re.search(rf"\b{joined}\b", low):
                variants.append(re.sub(rf"\b{joined}\b", spaced, singular, flags=re.IGNORECASE))
        if re.search(r"\bfull ?stack\b", low):
            variants.append(re.sub(r"\bfull ?stack\b", "Full-Stack", singular, flags=re.IGNORECASE))
        out: List[str] = []
        for v in variants:
            if v.lower() not in {o.lower() for o in out}:
                out.append(v)
        return out[:limit]


class LeadQualifier:
    """
    Implements TalentPulse AI's False-Positive Gatekeeper & Recruiter Signal Verifier:
    - Gate A: Current vs. Past Role verification
    - Gate B: Company Disambiguation (keyword matched company, not job title)
    - Gate C: Recruiter / Hiring-Manager Filter
    - Gate D: Open to Work Signal Verification
    - Gate E: Seniority / Experience Level Filter
    """

    RECRUITER_PATTERNS = [
        re.compile(r"\bwe\s+are\s+hiring\b", re.IGNORECASE),
        re.compile(r"\bi\s+am\s+hiring\b", re.IGNORECASE),
        re.compile(r"\bjoin\s+our\s+team\b", re.IGNORECASE),
        re.compile(r"\btechnical\s+recruiter\b", re.IGNORECASE),
        re.compile(r"\btalent\s+acquisition\b", re.IGNORECASE),
        re.compile(r"\bheadhunter\b", re.IGNORECASE),
        re.compile(r"\bsourcer\b", re.IGNORECASE),
        re.compile(r"\brecruiting\b", re.IGNORECASE),
        re.compile(r"\brecruiter\b", re.IGNORECASE),
    ]

    EXPLICIT_OTW_PATTERNS = [
        re.compile(r"#opentowork", re.IGNORECASE),
        re.compile(r"\bopen\s+to\s+work\b", re.IGNORECASE),
    ]

    POSSIBLE_OTW_PATTERNS = [
        re.compile(r"\bactively\s+looking\b", re.IGNORECASE),
        re.compile(r"\bseeking\s+new\s+opportunities\b", re.IGNORECASE),
        re.compile(r"\bseeking\s+opportunities\b", re.IGNORECASE),
        re.compile(r"\bexploring\s+(?:new\s+)?opportunities\b", re.IGNORECASE),
        re.compile(r"\bopen\s+for\s+opportunities\b", re.IGNORECASE),
        re.compile(r"\bopen\s+to\s+new\s+roles\b", re.IGNORECASE),
        re.compile(r"\blooking\s+for\s+new\s+roles\b", re.IGNORECASE),
        re.compile(r"\bavailable\s+for\s+hire\b", re.IGNORECASE),
        re.compile(r"\bready\s+for\s+new\s+challenges\b", re.IGNORECASE),
    ]

    OPEN_TO_WORK_PATTERNS = EXPLICIT_OTW_PATTERNS + POSSIBLE_OTW_PATTERNS

    SENIORITY_PATTERNS = {
        "entry": [
            re.compile(r"\bentry\b", re.IGNORECASE),
            re.compile(r"\bjunior\b", re.IGNORECASE),
            re.compile(r"\bassociate\b", re.IGNORECASE),
            re.compile(r"\btrainee\b", re.IGNORECASE),
            re.compile(r"\bintern\b", re.IGNORECASE),
            re.compile(r"\bgraduate\b", re.IGNORECASE),
        ],
        "senior": [
            re.compile(r"\bsenior\b", re.IGNORECASE),
            re.compile(r"\bsr\.?\b", re.IGNORECASE),
            re.compile(r"\blead\b", re.IGNORECASE),
            re.compile(r"\bprincipal\b", re.IGNORECASE),
            re.compile(r"\bstaff\b", re.IGNORECASE),
        ],
        "manager": [
            re.compile(r"\bmanager\b", re.IGNORECASE),
            re.compile(r"\bmgr\.?\b", re.IGNORECASE),
            re.compile(r"\blead\b", re.IGNORECASE),
            re.compile(r"\bsupervisor\b", re.IGNORECASE),
            re.compile(r"\bhead of\b", re.IGNORECASE),
        ],
        "director": [
            re.compile(r"\bdirector\b", re.IGNORECASE),
            re.compile(r"\bassistant director\b", re.IGNORECASE),
            re.compile(r"\bassociate director\b", re.IGNORECASE),
            re.compile(r"\bsr\.? director\b", re.IGNORECASE),
        ],
        "executive": [
            re.compile(r"\bvp\b", re.IGNORECASE),
            re.compile(r"\bvice president\b", re.IGNORECASE),
            re.compile(r"\bchief\b", re.IGNORECASE),
            re.compile(r"\bcxo\b", re.IGNORECASE),
            re.compile(r"\bceo\b", re.IGNORECASE),
            re.compile(r"\bcto\b", re.IGNORECASE),
            re.compile(r"\bcfo\b", re.IGNORECASE),
            re.compile(r"\bcmo\b", re.IGNORECASE),
            re.compile(r"\bcoo\b", re.IGNORECASE),
            re.compile(r"\bpresident\b", re.IGNORECASE),
            re.compile(r"\bfounder\b", re.IGNORECASE),
            re.compile(r"\bco-founder\b", re.IGNORECASE),
            re.compile(r"\bowner\b", re.IGNORECASE),
            re.compile(r"\bpartner\b", re.IGNORECASE),
        ],
    }

    @classmethod
    def check_open_to_work(
        cls, headline: str, bio: str, metadata_flag: bool = False
    ) -> bool:
        if metadata_flag:
            return True
        combined = f"{headline} {bio}"
        return any(pat.search(combined) for pat in cls.OPEN_TO_WORK_PATTERNS)

    @classmethod
    def evaluate(
        cls,
        current_title: str,
        company_name: str,
        headline: str,
        bio: str,
        current_positions: List[Dict[str, str]],
        past_positions: List[Dict[str, str]],
        metadata_open_to_work: bool,
        target_role: Optional[str] = None,
        target_keywords: Optional[str] = None,
        target_seniorities: Optional[List[str]] = None,
        years_experience: Optional[float] = None,
        target_experience: Optional[List[str]] = None,
        lead_location: str = "",
        location_terms: Optional[List[str]] = None,
    ) -> QualificationResult:
        """
        Evaluates a candidate profile against Gates A, B, C, D, and E.
        Returns a QualificationResult tuple subclass (is_qualified, no_match_reasons, is_open_to_work)
        with rich recruiter-facing properties.
        """
        no_match_reasons: List[str] = []
        combined_text = f"{headline} {bio}"

        # -------------------------------------------------------------------
        # 1. Job-Seeking / Open-to-Work Signal Evaluation
        # -------------------------------------------------------------------
        otw_status = "No"
        signal_type = "None"
        signal_description = "No job-seeking signals detected."

        if metadata_open_to_work:
            otw_status = "Yes"
            signal_type = "Open to Work"
            signal_description = "Verified #OpenToWork indicator from Sales Navigator profile metadata."
        else:
            for pat in cls.EXPLICIT_OTW_PATTERNS:
                m = pat.search(combined_text)
                if m:
                    otw_status = "Yes"
                    signal_type = "Open to Work"
                    signal_description = f"Explicit Open-to-Work hashtag/phrase '{m.group(0)}' found in profile."
                    break

            if otw_status == "No":
                for pat in cls.POSSIBLE_OTW_PATTERNS:
                    m = pat.search(combined_text)
                    if m:
                        otw_status = "Yes"
                        signal_type = "Job-Seeking Signal"
                        signal_description = f"Job-seeking phrasing '{m.group(0)}' detected in candidate headline/bio."
                        break

        is_open_to_work = (otw_status == "Yes")

        # -------------------------------------------------------------------
        # 2. Gate C: Recruiter / Hiring-Manager Filter
        # -------------------------------------------------------------------
        target_is_recruiter = (
            any(pat.search(target_role) for pat in cls.RECRUITER_PATTERNS)
            if target_role
            else False
        )

        if not target_is_recruiter:
            headline_recruiter = any(
                pat.search(headline) for pat in cls.RECRUITER_PATTERNS
            )
            title_recruiter = any(
                pat.search(current_title) for pat in cls.RECRUITER_PATTERNS
            )
            if headline_recruiter or title_recruiter:
                no_match_reasons.append("recruiter or hiring manager profile")

        # -------------------------------------------------------------------
        # 3. Gate A & B: Role Matching & Disambiguation
        # -------------------------------------------------------------------
        if target_role:
            # Aggregate current position titles
            current_titles = [current_title] + [
                pos.get("title", "") for pos in current_positions if pos.get("title")
            ]
            current_titles_text = " ".join(current_titles).lower()

            # Aggregate past position titles
            past_titles = [
                pos.get("title", "") for pos in past_positions if pos.get("title")
            ]
            past_titles_text = " ".join(past_titles).lower()

            # Aggregate company text
            companies = [company_name] + [
                pos.get("company", "") for pos in current_positions if pos.get("company")
            ]
            company_text = " ".join(companies).lower()

            # Loose matching: plurals, 'full stack'/'fullstack', 'Sr'/'Senior' etc. are equivalent
            matched_current = RoleMatcher.matches(target_role, current_titles_text)
            matched_past = RoleMatcher.matches(target_role, past_titles_text)
            matched_company = RoleMatcher.matches(target_role, company_text)

            # Gate B: Company Disambiguation
            if matched_company and not matched_current:
                no_match_reasons.append("company name match, not job title")
            # Gate A: Current vs. Past Role
            elif not matched_current:
                if matched_past:
                    no_match_reasons.append("wrong job title (past role only)")
                else:
                    no_match_reasons.append("wrong job title (not in current positions)")

        # -------------------------------------------------------------------
        # 4. Gate E: Experience / Seniority Level Verification
        # -------------------------------------------------------------------
        if target_seniorities:
            norm_sen = [s.lower().replace("-", "").strip() for s in target_seniorities]
            matched_sen = False
            title_headline = f"{current_title} {headline}".lower()
            for s in norm_sen:
                key = None
                if s in ("3", "entry", "entrylevel", "ic"):
                    key = "entry"
                elif s in ("4", "senior", "sr"):
                    key = "senior"
                elif s in ("5", "manager", "lead"):
                    key = "manager"
                elif s in ("6", "director"):
                    key = "director"
                elif s in ("7", "8", "9", "10", "executive", "vp", "cxo", "partner", "owner"):
                    key = "executive"

                if key and key in cls.SENIORITY_PATTERNS:
                    if any(p.search(title_headline) for p in cls.SENIORITY_PATTERNS[key]):
                        matched_sen = True
                        break

            if not matched_sen:
                no_match_reasons.append("seniority / experience level mismatch")

        # -------------------------------------------------------------------
        # 4b. Gate F: Years of Experience (only when the estimate is known)
        # -------------------------------------------------------------------
        ranges = [EXPERIENCE_RANGES[r] for r in (target_experience or []) if r in EXPERIENCE_RANGES]
        if ranges and years_experience is not None:
            if not any(lo <= years_experience < hi for _, lo, hi in ranges):
                no_match_reasons.append("years of experience out of range")

        # -------------------------------------------------------------------
        # 4c. Gate G: Location (only when the search had to use a wider region)
        # -------------------------------------------------------------------
        if location_terms:
            import locations
            if not locations.lead_in_place(lead_location, location_terms):
                no_match_reasons.append("outside requested location")

        # -------------------------------------------------------------------
        # 5. Gate D: Explicit Open to Work Query Filter
        # -------------------------------------------------------------------
        if target_keywords and "open to work" in target_keywords.lower():
            if not is_open_to_work:
                no_match_reasons.append("not open to work")

        # -------------------------------------------------------------------
        # 6. Overall Qualification Status Determination
        # -------------------------------------------------------------------
        if len(no_match_reasons) == 0:
            qualification_status = "Qualified"
            target_role_match = f"Matched Target Role: {target_role}" if target_role else "Role Criteria Satisfied"
            qualification_reason = "Passed all TalentPulse AI qualification gates (current role verified, non-recruiter, target criteria satisfied)"
            is_qualified = True
        else:
            is_qualified = False
            if len(no_match_reasons) == 1 and no_match_reasons[0] == "not open to work":
                qualification_status = "Needs Review"
                target_role_match = f"Role Matched: {target_role} (OTW Unconfirmed)" if target_role else "Needs Review"
                qualification_reason = "Candidate matches target role but does not show explicit Open to Work signals"
            else:
                qualification_status = "Rejected"
                target_role_match = f"Role Mismatch: {no_match_reasons[0]}"
                qualification_reason = "; ".join(no_match_reasons)

        return QualificationResult(
            is_qualified=is_qualified,
            no_match_reasons=no_match_reasons,
            is_open_to_work=is_open_to_work,
            otw_status=otw_status,
            signal_type=signal_type,
            signal_description=signal_description,
            qualification_status=qualification_status,
            qualification_reason=qualification_reason,
            target_role_match=target_role_match,
        )


# ---------------------------------------------------------------------------
# 4. Persistence & Deduplication (SQLite & CSV)
# ---------------------------------------------------------------------------
class LeadDatabase:
    """
    Handles SQLite database persistence, upsert deduplication, and CSV export.
    Includes automatic network/shared-filesystem shadow sync for seamless operation
    across cloud, VM (9p/NFS), and local environments.
    """

    # Seconds a connection waits for another writer (searches, email updates and dashboard reads run
    # at the same time) instead of failing straight away with "database is locked"
    BUSY_TIMEOUT = 30
    # Whether each database file's filesystem supports SQLite locking, tested once per file and process.
    # Testing on every open made busy moments look like a broken filesystem, and the shadow copy that
    # followed was written back over the real file, losing or corrupting rows.
    _lock_support: Dict[str, bool] = {}
    _lock_support_guard = threading.Lock()

    def __init__(self, db_path: str = "sales_nav_leads.db"):
        self.db_path = db_path
        self._use_shadow = False

        key = os.path.abspath(db_path)
        with LeadDatabase._lock_support_guard:
            if key not in LeadDatabase._lock_support:
                try:
                    # Test whether filesystem supports direct POSIX file locking
                    conn = sqlite3.connect(db_path, timeout=self.BUSY_TIMEOUT)
                    conn.execute("CREATE TABLE IF NOT EXISTS _fs_check (id int);")
                    conn.execute("DROP TABLE _fs_check;")
                    conn.close()
                    LeadDatabase._lock_support[key] = True
                except sqlite3.OperationalError as e:
                    # Busy only means another process is writing: locking works
                    LeadDatabase._lock_support[key] = "locked" in str(e).lower() or "busy" in str(e).lower()
                    if not LeadDatabase._lock_support[key]:
                        logger.warning("SQLite locking not supported for %s (%s); using a local shadow copy.", db_path, e)

        if LeadDatabase._lock_support[key]:
            self.active_path = db_path
        else:
            self._use_shadow = True
            sanitized_name = re.sub(r"[^\w\.-]", "_", os.path.basename(db_path))
            self.shadow_path = os.path.join(tempfile.gettempdir(), f"shadow_{sanitized_name}")
            if os.path.exists(db_path) and os.path.getsize(db_path) > 0:
                try:
                    shutil.copyfile(db_path, self.shadow_path)
                except Exception:
                    pass
            self.active_path = self.shadow_path

        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.active_path, timeout=self.BUSY_TIMEOUT)

    def sync_to_disk(self) -> None:
        if self._use_shadow and os.path.exists(self.active_path):
            try:
                shutil.copyfile(self.active_path, self.db_path)
            except Exception as e:
                logger.warning(
                    "Could not sync shadow database to target path '%s': %s",
                    self.db_path,
                    e,
                )

    def _init_db(self) -> None:
        with closing(self._get_connection()) as conn:
            cursor = conn.cursor()
            # Enable WAL mode for high concurrency, zero crash risk, and fast writes
            cursor.execute("PRAGMA journal_mode = WAL;")
            cursor.execute("PRAGMA synchronous = NORMAL;")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS leads (
                    linkedin_url TEXT PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    full_name TEXT,
                    current_title TEXT,
                    company_name TEXT,
                    location TEXT,
                    is_open_to_work INTEGER,
                    is_qualified INTEGER,
                    no_match_reasons TEXT,
                    discovered_at TEXT,
                    phone TEXT,
                    email TEXT,
                    contact_source TEXT,
                    contact_verified INTEGER DEFAULT 0,
                    skills TEXT,
                    otw_status TEXT,
                    signal_type TEXT,
                    signal_description TEXT,
                    signal_source_url TEXT,
                    signal_date TEXT,
                    qualification_status TEXT,
                    qualification_reason TEXT,
                    target_role_match TEXT,
                    data_source TEXT,
                    updated_at TEXT,
                    job_id TEXT,
                    search_role TEXT,
                    years_experience REAL,
                    email_status TEXT
                );
            """)
            # Performance indexes for lightning-fast lookups and filtering across millions of records
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_search_role ON leads(search_role);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_job_id ON leads(job_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_otw ON leads(otw_status);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_location ON leads(location);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_discovered ON leads(discovered_at);")
            conn.commit()
            self._migrate_schema(conn)
        self.sync_to_disk()

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        """
        Safely applies non-destructive schema migrations for extended recruiter attributes.
        Preserves all existing production data and ensures backwards compatibility.
        """
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(leads);")
        existing_cols = {row[1]: row[2] for row in cursor.fetchall()}

        new_columns = [
            ("phone", "TEXT"),
            ("email", "TEXT"),
            ("contact_source", "TEXT"),
            ("contact_verified", "INTEGER DEFAULT 0"),
            ("skills", "TEXT"),
            ("otw_status", "TEXT"),
            ("signal_type", "TEXT"),
            ("signal_description", "TEXT"),
            ("signal_source_url", "TEXT"),
            ("signal_date", "TEXT"),
            ("qualification_status", "TEXT"),
            ("qualification_reason", "TEXT"),
            ("target_role_match", "TEXT"),
            ("data_source", "TEXT"),
            ("updated_at", "TEXT"),
            ("job_id", "TEXT"),
            ("search_role", "TEXT"),
            ("years_experience", "REAL"),
            ("email_status", "TEXT"),
            ("headline", "TEXT"),
            ("first_found_at", "TEXT"),
        ]

        for col_name, col_type in new_columns:
            if col_name not in existing_cols:
                cursor.execute(f"ALTER TABLE leads ADD COLUMN {col_name} {col_type};")

        # First time we ever saw a candidate: older rows only have discovered_at
        cursor.execute("UPDATE leads SET first_found_at = discovered_at WHERE first_found_at IS NULL;")
        self._flag_shared_emails(cursor)

        # Backfill and normalize otw_status to strictly binary "Yes" or "No"
        cursor.execute("""
            UPDATE leads SET 
                otw_status = 'Yes'
            WHERE is_open_to_work = 1 OR otw_status IN ('Explicitly Indicated', 'Possible Signal', 'Yes');
        """)
        cursor.execute("""
            UPDATE leads SET 
                otw_status = 'No'
            WHERE is_open_to_work = 0 OR otw_status IN ('Unknown', 'No') OR otw_status IS NULL;
        """)
        cursor.execute("""
            UPDATE leads SET 
                qualification_status = CASE WHEN is_qualified = 1 THEN 'Qualified' ELSE 'Rejected' END
            WHERE qualification_status IS NULL;
        """)
        cursor.execute("""
            UPDATE leads SET 
                qualification_reason = CASE 
                    WHEN is_qualified = 1 THEN 'Passed all TalentPulse AI qualification gates'
                    ELSE COALESCE(no_match_reasons, 'Filtered out by qualification gates')
                END
            WHERE qualification_reason IS NULL;
        """)
        cursor.execute("""
            UPDATE leads SET 
                target_role_match = CASE WHEN is_qualified = 1 THEN 'Role Criteria Satisfied' ELSE 'Role Mismatch' END
            WHERE target_role_match IS NULL;
        """)
        cursor.execute("""
            UPDATE leads SET 
                signal_type = CASE WHEN is_open_to_work = 1 THEN 'Explicit Open-to-Work' ELSE 'None' END
            WHERE signal_type IS NULL;
        """)
        cursor.execute("""
            UPDATE leads SET 
                signal_description = CASE WHEN is_open_to_work = 1 THEN 'Explicit #OpenToWork indicator verified.' ELSE 'No job-seeking signals detected.' END
            WHERE signal_description IS NULL;
        """)
        cursor.execute("""
            UPDATE leads SET 
                data_source = 'LinkedIn Sales Navigator'
            WHERE data_source IS NULL;
        """)
        cursor.execute("""
            UPDATE leads SET 
                contact_source = 'Not available'
            WHERE contact_source IS NULL;
        """)
        cursor.execute("""
            UPDATE leads SET 
                updated_at = discovered_at
            WHERE updated_at IS NULL;
        """)
        conn.commit()

    def upsert_lead(self, record: LeadRecord) -> None:
        with closing(self._get_connection()) as conn:
            cursor = conn.cursor()
            reasons_str = "; ".join(record.no_match_reasons)
            skills_str = "; ".join(record.skills) if isinstance(record.skills, list) else str(record.skills or "")
            cursor.execute("""
                INSERT INTO leads (
                    linkedin_url, first_name, last_name, full_name, current_title,
                    company_name, location, is_open_to_work, is_qualified,
                    no_match_reasons, discovered_at,
                    phone, email, contact_source, contact_verified,
                    skills, otw_status, signal_type, signal_description,
                    signal_source_url, signal_date, qualification_status,
                    qualification_reason, target_role_match, data_source, updated_at,
                    job_id, search_role, years_experience, email_status,
                    headline, first_found_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(linkedin_url) DO UPDATE SET
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    full_name = excluded.full_name,
                    current_title = excluded.current_title,
                    company_name = excluded.company_name,
                    location = excluded.location,
                    is_open_to_work = excluded.is_open_to_work,
                    is_qualified = excluded.is_qualified,
                    no_match_reasons = excluded.no_match_reasons,
                    discovered_at = excluded.discovered_at,
                    phone = excluded.phone,
                    email = COALESCE(excluded.email, leads.email),
                    contact_source = CASE WHEN excluded.email IS NULL AND leads.email IS NOT NULL THEN leads.contact_source ELSE excluded.contact_source END,
                    contact_verified = CASE WHEN excluded.email IS NULL AND leads.email IS NOT NULL THEN leads.contact_verified ELSE excluded.contact_verified END,
                    skills = excluded.skills,
                    otw_status = excluded.otw_status,
                    signal_type = excluded.signal_type,
                    signal_description = excluded.signal_description,
                    signal_source_url = excluded.signal_source_url,
                    signal_date = excluded.signal_date,
                    qualification_status = excluded.qualification_status,
                    qualification_reason = excluded.qualification_reason,
                    target_role_match = excluded.target_role_match,
                    data_source = excluded.data_source,
                    updated_at = excluded.updated_at,
                    job_id = COALESCE(excluded.job_id, leads.job_id),
                    search_role = COALESCE(excluded.search_role, leads.search_role),
                    years_experience = COALESCE(excluded.years_experience, leads.years_experience),
                    email_status = CASE WHEN excluded.email IS NULL AND leads.email IS NOT NULL THEN leads.email_status ELSE excluded.email_status END,
                    headline = COALESCE(NULLIF(excluded.headline, ''), leads.headline),
                    first_found_at = COALESCE(leads.first_found_at, excluded.first_found_at);
            """, (
                record.linkedin_url,
                record.first_name,
                record.last_name,
                record.full_name,
                record.current_title,
                record.company_name,
                record.location,
                1 if record.is_open_to_work else 0,
                1 if record.is_qualified else 0,
                reasons_str,
                record.discovered_at,
                record.phone,
                record.email,
                record.contact_source,
                1 if record.contact_verified else 0,
                skills_str,
                record.otw_status,
                record.signal_type,
                record.signal_description,
                record.signal_source_url or record.linkedin_url,
                record.signal_date,
                record.qualification_status,
                record.qualification_reason,
                record.target_role_match,
                record.data_source,
                record.updated_at,
                record.job_id,
                record.search_role,
                record.years_experience,
                record.email_status,
                record.headline,
                record.discovered_at,
            ))
            self._flag_shared_emails(cursor)
            conn.commit()
        self.sync_to_disk()

    def get_total_count(self) -> int:
        with closing(self._get_connection()) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM leads;")
            row = cursor.fetchone()
            return row[0] if row else 0

    def count_extracted_since(self, hours: int = DAILY_QUOTA_WINDOW_HOURS) -> int:
        """
        Counts leads extracted (discovered_at, UTC) within the trailing window.
        Backs the rolling 24-hour account safety quota.
        """
        cutoff = datetime.fromtimestamp(time.time() - hours * 3600, tz=timezone.utc)
        with closing(self._get_connection()) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM leads WHERE discovered_at >= ?;",
                (cutoff.strftime("%Y-%m-%d %H:%M:%S"),),
            )
            row = cursor.fetchone()
            return row[0] if row else 0

    def get_qualified_count(self) -> int:
        with closing(self._get_connection()) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM leads WHERE is_qualified = 1;")
            row = cursor.fetchone()
            return row[0] if row else 0

    def get_lead(self, linkedin_url: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single candidate record by LinkedIn profile URL.
        """
        query = (
            "SELECT linkedin_url, first_name, last_name, full_name, current_title, "
            "company_name, location, is_open_to_work, is_qualified, no_match_reasons, discovered_at, "
            "phone, email, contact_source, contact_verified, "
            "skills, otw_status, signal_type, signal_description, "
            "signal_source_url, signal_date, qualification_status, "
            "qualification_reason, target_role_match, data_source, updated_at, "
            "job_id, search_role, years_experience, email_status, headline, first_found_at "
            "FROM leads WHERE linkedin_url = ?;"
        )
        with closing(self._get_connection()) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (linkedin_url,))
            r = cursor.fetchone()
            if not r:
                return None
            return {
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
                "skills": [s.strip() for s in r[15].split(";")] if r[15] else [],
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

    def update_lead_status(
        self,
        linkedin_url: str,
        is_open_to_work: bool,
        otw_status: str,
        signal_type: Optional[str] = None,
        signal_description: Optional[str] = None,
        current_title: Optional[str] = None,
        company_name: Optional[str] = None,
        updated_at: Optional[str] = None,
    ) -> bool:
        """
        Updates a candidate's live status, OTW indicators, and timestamp in SQLite.
        """
        now = updated_at or time.strftime("%Y-%m-%d %H:%M:%S")
        with closing(self._get_connection()) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE leads SET "
                "is_open_to_work = ?, "
                "otw_status = ?, "
                "signal_type = COALESCE(?, signal_type), "
                "signal_description = COALESCE(?, signal_description), "
                "current_title = COALESCE(?, current_title), "
                "company_name = COALESCE(?, company_name), "
                "updated_at = ? "
                "WHERE linkedin_url = ?;",
                (
                    1 if is_open_to_work else 0,
                    otw_status,
                    signal_type,
                    signal_description,
                    current_title,
                    company_name,
                    now,
                    linkedin_url,
                ),
            )
            conn.commit()
            updated = cursor.rowcount > 0
        self.sync_to_disk()
        return updated

    @staticmethod
    def _flag_shared_emails(cursor: sqlite3.Cursor) -> None:
        """Finder emails held by more than one candidate are downgraded; profile emails are kept."""
        cursor.execute(f"""
            UPDATE leads SET email_status = '{EMAIL_STATUS_SHARED}', contact_verified = 0
            WHERE email IS NOT NULL AND email != ''
              AND COALESCE(email_status, '') NOT IN ('{EMAIL_STATUS_PROFILE}', '{EMAIL_STATUS_SHARED}')
              AND lower(email) IN (
                  SELECT lower(email) FROM leads
                  WHERE email IS NOT NULL AND email != ''
                  GROUP BY lower(email) HAVING COUNT(DISTINCT linkedin_url) > 1
              );
        """)

    def update_lead_email(self, linkedin_url: str, email: Optional[str], status: str) -> bool:
        """Stores a finder result; keeps any existing email when nothing better was found."""
        with closing(self._get_connection()) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE leads SET email = COALESCE(?, email), email_status = CASE WHEN ? IS NULL AND email IS NOT NULL THEN email_status ELSE ? END, "
                "contact_source = CASE WHEN ? IS NULL THEN contact_source ELSE 'Email finder' END, "
                "contact_verified = CASE WHEN ? IS NULL THEN contact_verified ELSE ? END, "
                "updated_at = ? WHERE linkedin_url = ?;",
                (email, email, status, email, email, 1 if status == EMAIL_STATUS_VERIFIED else 0,
                 datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), linkedin_url),
            )
            self._flag_shared_emails(cursor)
            conn.commit()
            updated = cursor.rowcount > 0
        self.sync_to_disk()
        return updated

    DANGEROUS_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")

    @classmethod
    def sanitize_for_csv(cls, value: Any) -> Any:
        """
        Sanitizes untrusted fields against CSV / spreadsheet formula injection.
        Values beginning with '=', '+', '-', '@', tab, or carriage return are safely
        prefixed with a single quote (') while preserving the original string.
        """
        if value is None:
            return ""
        if isinstance(value, str):
            if value.startswith(cls.DANGEROUS_FORMULA_PREFIXES):
                return "'" + value
            return value
        return value

    @staticmethod
    def _csv_phone(phone: str) -> str:
        """'+91 98765 43210' -> '(+91) 98765 43210': keeps the country code, readable in Excel
        (a leading '+' would be read as a formula and need a visible apostrophe)."""
        p = (phone or "").strip()
        m = re.match(r"^\+(\d{1,3})[\s\-.]+(.+)$", p)
        if m:
            return f"(+{m.group(1)}) {m.group(2).strip()}"
        digits = re.sub(r"\D", "", p)
        if p.startswith("+") and len(digits) > 10:
            cc, rest = digits[:-10], digits[-10:]
            return f"(+{cc}) {rest[:5]} {rest[5:]}"
        return p

    @staticmethod
    def _csv_time(utc_text: str) -> str:
        """Stored UTC 'YYYY-MM-DD HH:MM:SS' -> India time '30 Oct 2026, 12:12 PM'."""
        try:
            dt = datetime.strptime(utc_text[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return (dt + IST_OFFSET).strftime("%d %b %Y, %I:%M %p")
        except (TypeError, ValueError):
            return utc_text or ""

    @staticmethod
    def _csv_otw(is_otw: int, signal_type: str, signal_description: str) -> str:
        if not is_otw:
            return "No"
        d = (signal_description or "").lower()
        if signal_type == "Recruiter Verified" or "recruiter confirmed" in d:
            return "Yes - confirmed by recruiter"
        if "metadata" in d or "badge" in d:
            return "Yes - LinkedIn #OpenToWork badge"
        if signal_type == "Job-Seeking Signal":
            return "Yes - job-seeking language in profile"
        if signal_type == "Open to Work":
            return "Yes - #OpenToWork stated in profile"
        return "Yes"

    @staticmethod
    def _csv_reason(status: str, reasons: str) -> str:
        if status == "Qualified":
            return "All checks passed"
        items = [r.strip() for r in (reasons or "").split(";") if r.strip()]
        if status == "Needs Review":
            return "Role matches; open-to-work not confirmed"
        nice = {
            "wrong job title (past role only)": "Role only in a past job",
            "wrong job title (not in current positions)": "Current title doesn't match",
            "company name match, not job title": "Matched company name, not job title",
            "recruiter or hiring manager profile": "Recruiter / hiring profile",
            "not open to work": "Not open to work",
            "years of experience out of range": "Experience out of range",
            "outside requested location": "Outside requested location",
        }
        return "; ".join(nice.get(r, r[:1].upper() + r[1:]) for r in items) or "Rejected"

    def _csv_row(self, r: Tuple[Any, ...]) -> List[Any]:
        (name, url, title, headline, company, years, location, is_otw, sig_type, sig_desc,
         status, reasons, phone, email, email_status, skills, first_found, last_seen) = r
        return [
            name, url, title, headline, company,
            f"{years:g}" if isinstance(years, (int, float)) else "",
            location,
            self._csv_otw(is_otw, sig_type, sig_desc),
            self._csv_phone(phone),
            email,
            (skills or "").replace(";", ",").replace(" ,", ","),
            self._csv_time(first_found), self._csv_time(last_seen),
        ]

    def export_to_csv(
        self,
        csv_path: str = "sales_nav_leads.csv",
        filters: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Exports candidate records in the recruiter-focused columns of CSV_HEADERS.
        Preserves Unicode, international phone numbers, and full formula-injection sanitization.
        Supports optional filtering matching active dashboard query criteria.
        """
        headers = CSV_HEADERS

        query = """
            SELECT
                COALESCE(full_name, ''),
                COALESCE(linkedin_url, ''),
                COALESCE(current_title, ''),
                COALESCE(headline, ''),
                COALESCE(company_name, ''),
                years_experience,
                COALESCE(location, ''),
                CASE WHEN is_open_to_work = 1 OR otw_status IN ('Yes', 'Explicitly Indicated') THEN 1 ELSE 0 END,
                COALESCE(signal_type, ''),
                COALESCE(signal_description, ''),
                COALESCE(qualification_status, CASE WHEN is_qualified = 1 THEN 'Qualified' ELSE 'Rejected' END),
                COALESCE(no_match_reasons, ''),
                COALESCE(phone, ''),
                COALESCE(email, ''),
                COALESCE(email_status, CASE WHEN email IS NOT NULL AND email != '' THEN 'From profile' ELSE '' END),
                COALESCE(skills, ''),
                COALESCE(first_found_at, discovered_at, ''),
                COALESCE(discovered_at, '')
            FROM leads
        """
        params: List[Any] = []
        conditions: List[str] = []

        if filters:
            qual_val = (filters.get("qualification_status") or "").lower()
            if filters.get("status_filter") == "qualified" or qual_val == "qualified":
                conditions.append("(is_qualified = 1 OR qualification_status = 'Qualified')")
            elif filters.get("status_filter") == "disqualified" or qual_val in ("rejected", "disqualified"):
                conditions.append("(is_qualified = 0 OR qualification_status = 'Rejected')")
            elif qual_val in ("needs_review", "needs review"):
                conditions.append("qualification_status = 'Needs Review'")

            otw_val = (filters.get("otw_status") or "").lower()
            if filters.get("status_filter") == "otw" or otw_val in ("yes", "explicit", "explicitly indicated"):
                conditions.append("(is_open_to_work = 1 OR otw_status IN ('Yes', 'Explicitly Indicated'))")
            elif otw_val in ("no", "unknown"):
                conditions.append("(is_open_to_work = 0 AND (otw_status IS NULL OR otw_status NOT IN ('Yes', 'Explicitly Indicated')))")
            elif otw_val in ("possible", "possible signal"):
                conditions.append("(is_open_to_work = 1 OR otw_status IN ('Yes', 'Possible Signal'))")

            if filters.get("search"):
                term = f"%{filters['search']}%"
                conditions.append("(full_name LIKE ? OR company_name LIKE ? OR current_title LIKE ? OR location LIKE ? OR skills LIKE ?)")
                params.extend([term, term, term, term, term])

            if filters.get("location"):
                conditions.append("location LIKE ?")
                params.append(f"%{filters['location']}%")

            exp_sql, exp_params = experience_condition(filters.get("experience"))
            if exp_sql:
                conditions.append(exp_sql)
                params.extend(exp_params)

            if filters.get("contact_filter") == "has_phone":
                conditions.append("phone IS NOT NULL AND phone != ''")
            elif filters.get("contact_filter") == "has_email":
                conditions.append("email IS NOT NULL AND email != ''")
            elif filters.get("contact_filter") == "has_any":
                conditions.append("((phone IS NOT NULL AND phone != '') OR (email IS NOT NULL AND email != ''))")

            if filters.get("job_id") and filters["job_id"] != "all":
                conditions.append("job_id = ?")
                params.append(filters["job_id"])

            if filters.get("search_role") and filters["search_role"] != "all":
                conditions.append("(search_role LIKE ? OR current_title LIKE ? OR target_role_match LIKE ?)")
                term_role = f"%{filters['search_role'].strip()}%"
                params.extend([term_role, term_role, term_role])

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY discovered_at DESC;"

        with closing(self._get_connection()) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()

        # utf-8-sig: Excel shows names like "Müller" / Indian scripts correctly
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                sanitized_row = [self.sanitize_for_csv(cell) for cell in self._csv_row(row)]
                writer.writerow(sanitized_row)

        return len(rows)


# ---------------------------------------------------------------------------
# 5. Extraction Pipeline & Anti-Detection Safety Engine
# ---------------------------------------------------------------------------
class SalesNavExtractor:
    """
    Core extraction orchestrator enforcing rate limits, status handling,
    data cleaning, qualification, and storage.
    """

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        db: Optional[LeadDatabase] = None,
        max_daily_leads: int = DEFAULT_MAX_DAILY_LEADS,
        enable_jitter: bool = True,
        dry_run: bool = False,
        find_emails: bool = True,
    ):
        self.session = session
        self.db = db or LeadDatabase()
        self.max_daily_leads = max_daily_leads
        self.enable_jitter = enable_jitter
        self.dry_run = dry_run
        self.leads_extracted = 0
        # Work-email lookup makes DNS/SMTP calls, so it never runs in dry-run mode
        self.email_finder: Optional[EmailFinder] = EmailFinder() if (find_emails and not dry_run) else None

    def _sleep_jitter(self) -> None:
        if self.enable_jitter and not self.dry_run:
            delay = random.uniform(MIN_JITTER_SECS, MAX_JITTER_SECS)
            logger.info("Applying anti-detection jitter: sleeping %.2fs", delay)
            time.sleep(delay)

    def _handle_cooldown(self, page_index: int) -> None:
        if (
            self.enable_jitter
            and not self.dry_run
            and page_index > 0
            and page_index % COOLDOWN_PAGES_INTERVAL == 0
        ):
            logger.info(
                "Reached %d pages. Applying cooldown pause of %.1fs...",
                page_index,
                COOLDOWN_PAUSE_SECS,
            )
            time.sleep(COOLDOWN_PAUSE_SECS)

    def fetch_page(
        self, filters: SearchFilterParams, start: int
    ) -> Dict[str, Any]:
        """
        Dispatches request to LinkedIn internal salesApiLeadSearch or mock generator.
        """
        if self.dry_run:
            return self._generate_mock_response(start=start, filters=filters)

        if not self.session:
            raise ExtractorError("Session required for live extraction.")

        params = SalesNavURLParser.build_api_params(filters, start=start, count=PAGE_SIZE)
        logger.info("Fetching Sales Navigator leads (start=%d, count=%d)...", start, PAGE_SIZE)

        try:
            resp = self.session.get(
                LINKEDIN_SALES_SEARCH_API,
                params=params,
                timeout=30,
            )
        except requests.RequestException as e:
            logger.error("Network request failed: %s", str(e))
            raise ExtractorError(f"Network error during lead search: {e}") from e

        # Handle specific status codes
        if resp.status_code == 401:
            raise ExpiredSessionError(
                "HTTP 401 Unauthorized: Sales Navigator session has expired. "
                "Please update LI_AT and JSESSIONID in your .env file."
            )
        elif resp.status_code == 429:
            raise RateLimitExceededError(
                "HTTP 429 Too Many Requests: LinkedIn rate limit exceeded. "
                "Aborting extraction safely to preserve account health."
            )
        elif resp.status_code == 999:
            raise CheckpointError(
                "HTTP 999 Checkpoint: LinkedIn anti-bot security checkpoint triggered. "
                "Stopping execution immediately to protect account."
            )
        elif not resp.ok:
            raise ExtractorError(
                f"HTTP {resp.status_code} Error from Sales Navigator API: {resp.text[:300]}"
            )

        return resp.json()

    def process_raw_lead(
        self, raw_lead: Dict[str, Any], filters: SearchFilterParams, job_id: Optional[str] = None
    ) -> LeadRecord:
        """
        Transforms raw LinkedIn profile JSON into a normalized, qualified LeadRecord.
        """
        raw_name = raw_lead.get("fullName") or (
            f"{raw_lead.get('firstName', '')} {raw_lead.get('lastName', '')}".strip()
        )
        cleaned_full_name = DataCleaner.clean_name(raw_name)
        first_name, last_name = DataCleaner.split_name(cleaned_full_name)

        raw_company = raw_lead.get("companyName") or raw_lead.get("company", "")
        cleaned_company = DataCleaner.clean_company_name(raw_company)

        raw_headline = raw_lead.get("headline", "")
        cleaned_headline = DataCleaner.clean_headline(raw_headline)

        current_title = DataCleaner.clean_text(raw_lead.get("title") or raw_lead.get("currentTitle", ""))
        location = raw_lead.get("location") or raw_lead.get("geoRegion", "")
        linkedin_url = raw_lead.get("profileUrl") or raw_lead.get("navigationUrl", "")

        # Fallback profile URL construction
        if not linkedin_url and raw_lead.get("entityUrn"):
            urn = raw_lead["entityUrn"]
            lead_id = urn.split(":")[-1]
            linkedin_url = f"https://www.linkedin.com/sales/lead/{lead_id}"

        linkedin_url = DataCleaner.normalize_linkedin_url(linkedin_url)

        bio = raw_lead.get("summary") or raw_lead.get("bio", "")
        current_positions = raw_lead.get("currentPositions", [])
        past_positions = raw_lead.get("pastPositions", [])
        raw_open_to_work = bool(raw_lead.get("openToWork", False))

        # Extract contact information (preserving international phone format and leading zeros)
        phone, email, contact_source, contact_verified = DataCleaner.extract_contact_info(raw_lead)

        # Extract relevant skills / keywords
        skills = DataCleaner.extract_skills(raw_lead, cleaned_headline, bio)

        # Estimate total experience from position start/end dates (before gating)
        years_experience = DataCleaner.estimate_years_experience(raw_lead)

        # Evaluate TalentPulse AI qualification gates
        qual_res = LeadQualifier.evaluate(
            current_title=current_title,
            company_name=cleaned_company,
            headline=cleaned_headline,
            bio=bio,
            current_positions=current_positions,
            past_positions=past_positions,
            metadata_open_to_work=raw_open_to_work,
            target_role=filters.role,
            target_keywords=filters.keywords,
            target_seniorities=filters.seniority_levels,
            years_experience=years_experience,
            target_experience=filters.experience_ranges,
            lead_location=location,
            location_terms=filters.location_terms,
        )

        # Work email: profile text first, then the finder for leads worth contacting
        email_status = EMAIL_STATUS_PROFILE if email else ""
        if not email and self.email_finder and qual_res.qualification_status in ("Qualified", "Needs Review"):
            website = raw_lead.get("companyWebsite") or raw_lead.get("website")
            found, email_status = self.email_finder.find(first_name, last_name, cleaned_company, website)
            if found:
                email = found
                contact_source = "Email finder"
                contact_verified = email_status == EMAIL_STATUS_VERIFIED

        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        signal_date = raw_lead.get("signalDate") or raw_lead.get("openToWorkDate", "")

        return LeadRecord(
            linkedin_url=linkedin_url,
            first_name=first_name,
            last_name=last_name,
            full_name=cleaned_full_name,
            current_title=current_title,
            company_name=cleaned_company,
            location=location,
            headline=cleaned_headline,
            is_open_to_work=qual_res.is_open_to_work,
            is_qualified=qual_res.is_qualified,
            no_match_reasons=qual_res.no_match_reasons,
            discovered_at=now_ts,
            phone=phone,
            email=email,
            contact_source=contact_source,
            contact_verified=contact_verified,
            skills=skills,
            otw_status=qual_res.otw_status,
            signal_type=qual_res.signal_type,
            signal_description=qual_res.signal_description,
            signal_source_url=linkedin_url,
            signal_date=signal_date,
            qualification_status=qual_res.qualification_status,
            qualification_reason=qual_res.qualification_reason,
            target_role_match=qual_res.target_role_match,
            data_source="LinkedIn Sales Navigator",
            updated_at=now_ts,
            job_id=job_id,
            search_role=filters.role or "Unspecified Role",
            years_experience=years_experience,
            email_status=email_status,
        )

    def run(
        self,
        filters: SearchFilterParams,
        max_pages: int = 1,
        lead_callback: Optional[Any] = None,
        job_id: Optional[str] = None,
        should_stop: Optional[Any] = None,
    ) -> Tuple[int, int]:
        """
        Executes extraction across requested pages, respecting safety limits.
        Optional `lead_callback(record, progress_info)` enables real-time UI streaming.
        Optional `should_stop()` is checked between pages: when it returns True the
        current page is finished and no further pages are fetched.
        Returns: (total_processed, qualified_count)
        """
        total_processed = 0
        qualified_count = 0

        logger.info(
            "Starting extraction run: role='%s', location='%s', keywords='%s', max_pages=%d (dry_run=%s)",
            filters.role,
            filters.location,
            filters.keywords,
            max_pages,
            self.dry_run,
        )

        for page_idx in range(max_pages):
            if should_stop and should_stop():
                logger.info("Stop requested. Halting after page %d.", page_idx)
                break

            start = page_idx * PAGE_SIZE
            if start >= SALES_NAV_MAX_CAP:
                logger.warning("Reached Sales Navigator 2,500 lead search cap. Stopping.")
                break

            if self.leads_extracted >= self.max_daily_leads:
                logger.warning(
                    "Hit hard account safety limit of %d leads. Stopping safely.",
                    self.max_daily_leads,
                )
                break

            self._handle_cooldown(page_idx)

            payload = self.fetch_page(filters=filters, start=start)
            elements = payload.get("elements", [])
            if not elements:
                logger.info("No more leads returned by Sales Navigator. Extraction complete.")
                break

            for raw in elements:
                if self.leads_extracted >= self.max_daily_leads:
                    break

                record = self.process_raw_lead(raw, filters, job_id=job_id)
                self.db.upsert_lead(record)
                self.leads_extracted += 1
                total_processed += 1
                if record.is_qualified:
                    qualified_count += 1

                if lead_callback:
                    try:
                        lead_callback(record, {
                            "processed": total_processed,
                            "qualified": qualified_count,
                            "page": page_idx + 1,
                            "max_pages": max_pages,
                            "db_total": self.db.get_total_count()
                        })
                    except Exception as cb_err:
                        logger.debug("Lead callback error: %s", cb_err)

            if should_stop and should_stop():
                continue  # skip the jitter wait; loop head exits cleanly
            self._sleep_jitter()

        logger.info(
            "Run finished. Processed this session: %d leads (%d qualified). Total DB records: %d",
            total_processed,
            qualified_count,
            self.db.get_total_count(),
        )
        return total_processed, qualified_count

    # -----------------------------------------------------------------------
    # Realistic Offline Mock Payload Generator
    # -----------------------------------------------------------------------
    def _generate_mock_response(
        self, start: int, filters: SearchFilterParams
    ) -> Dict[str, Any]:
        """
        Generates realistic Sales Navigator Normalized JSON responses to
        test the entire cleaning, gatekeeper, deduplication, and export pipeline.
        """
        if start > 0:
            return {"elements": [], "paging": {"total": 5, "count": PAGE_SIZE, "start": start}}

        mock_leads = [
            # 1. Qualified Lead with dirty name, emojis, pronouns, degree, open-to-work, and verified contact info
            {
                "entityUrn": "urn:li:fs_salesProfile:(ACwAAB12345,NAME_SEARCH)",
                "profileUrl": "https://www.linkedin.com/in/jane-doe-ai",
                "fullName": "Dr. Jane Doe (She/Her), Ph.D. 🚀",
                "title": "Senior AI Engineer",
                "companyName": "Acme Tech Solutions, Inc.",
                "headline": "Senior AI Engineer | LLMs & PyTorch | #OpenToWork ✨",
                "location": "Bengaluru, Karnataka, India",
                "openToWork": True,
                "phone": "+91 98765 43210",
                "email": "jane.doe@example.com",
                "phoneVerified": True,
                "emailVerified": True,
                "skills": ["LLMs", "PyTorch", "Python", "Deep Learning", "NLP"],
                "signalDate": "2026-10-25",
                "currentPositions": [
                    {"title": "Senior AI Engineer", "company": "Acme Tech Solutions, Inc.", "startedOn": {"year": 2021, "month": 3}}
                ],
                "pastPositions": [
                    {"title": "Software Developer", "company": "OldCorp LLC", "startedOn": {"year": 2016, "month": 7}, "endedOn": {"year": 2021, "month": 2}}
                ],
            },
            # 2. Gate A Failure: Target role in PAST position only
            {
                "entityUrn": "urn:li:fs_salesProfile:(ACwAAB23456,NAME_SEARCH)",
                "profileUrl": "https://www.linkedin.com/in/michael-smith-re",
                "fullName": "Michael Smith MBA",
                "title": "Senior Real Estate Agent",
                "companyName": "Premier Realty LLC",
                "headline": "Real Estate Broker | Ex-AI Engineer",
                "location": "Mumbai, Maharashtra, India",
                "openToWork": False,
                "skills": ["Real Estate", "Sales Negotiation", "Asset Management"],
                "currentPositions": [
                    {"title": "Senior Real Estate Agent", "company": "Premier Realty LLC", "startedOn": {"year": 2022, "month": 1}}
                ],
                "pastPositions": [
                    {"title": "AI Engineer", "company": "DataCorp", "startedOn": {"year": 2017, "month": 5}, "endedOn": {"year": 2021, "month": 12}}
                ],
            },
            # 3. Gate B Failure: Company name matches keyword, NOT the job title
            {
                "entityUrn": "urn:li:fs_salesProfile:(ACwAAB34567,NAME_SEARCH)",
                "profileUrl": "https://www.linkedin.com/in/david-miller-sales",
                "fullName": "David Miller",
                "title": "Senior Account Executive",
                "companyName": "AI Engineer Software Solutions Pvt Ltd",
                "headline": "Account Executive at AI Engineer Software Solutions",
                "location": "Hyderabad, Telangana, India",
                "openToWork": False,
                "skills": ["Enterprise Sales", "B2B Software", "Lead Generation"],
                "currentPositions": [
                    {"title": "Senior Account Executive", "company": "AI Engineer Software Solutions Pvt Ltd", "startedOn": {"year": 2019, "month": 8}}
                ],
                "pastPositions": [],
            },
            # 4. Gate C Failure: Recruiter / Hiring-Manager Profile (has email in bio)
            {
                "entityUrn": "urn:li:fs_salesProfile:(ACwAAB45678,NAME_SEARCH)",
                "profileUrl": "https://www.linkedin.com/in/sarah-jenkins-recruiter",
                "fullName": "Sarah Jenkins (She/Her)",
                "title": "Head of Talent Acquisition",
                "companyName": "InnovateTech LLC",
                "headline": "We are hiring AI Engineers! Reach me at talent@innovatetech.com 🚀 | Senior Tech Recruiter",
                "summary": "Building world-class AI engineering teams. Contact talent@innovatetech.com",
                "location": "Delhi, India",
                "openToWork": False,
                "skills": ["Technical Recruiting", "Talent Acquisition", "Executive Search"],
                "currentPositions": [
                    {"title": "Head of Talent Acquisition", "company": "InnovateTech LLC", "startedOn": {"year": 2018, "month": 2}}
                ],
                "pastPositions": [],
            },
            # 5. Qualified Lead: Clean profile open to work (no phone/email available from source)
            {
                "entityUrn": "urn:li:fs_salesProfile:(ACwAAB56789,NAME_SEARCH)",
                "profileUrl": "https://www.linkedin.com/in/rajesh-kumar-ai",
                "fullName": "Rajesh Kumar B.Tech",
                "title": "Staff AI Engineer",
                "companyName": "Cognitive Systems Ltd.",
                "headline": "Staff AI Engineer building autonomous agents #OpenToWork",
                "location": "Pune, Maharashtra, India",
                "openToWork": True,
                "skills": ["Autonomous Agents", "Python", "TensorFlow", "FastAPI"],
                "signalDate": "2026-10-28",
                "currentPositions": [
                    {"title": "Staff AI Engineer", "company": "Cognitive Systems Ltd.", "startedOn": {"year": 2020, "month": 6}}
                ],
                "pastPositions": [
                    {"title": "Machine Learning Engineer", "company": "DataWorks Inc.", "startedOn": {"year": 2015, "month": 1}, "endedOn": {"year": 2020, "month": 5}}
                ],
            },
        ]

        return {
            "elements": mock_leads,
            "paging": {
                "total": len(mock_leads),
                "count": PAGE_SIZE,
                "start": start,
            },
        }


# ---------------------------------------------------------------------------
# 6. CLI Execution Interface
# ---------------------------------------------------------------------------
def parse_arguments() -> Any:
    import argparse

    parser = argparse.ArgumentParser(
        description="In-House LinkedIn Sales Navigator Lead Extractor & TalentPulse AI Qualifier"
    )
    parser.add_argument(
        "--url",
        type=str,
        help="Direct Sales Navigator search URL",
    )
    parser.add_argument(
        "--role",
        type=str,
        default="AI Engineer",
        help="Target job title / role keyword (default: 'AI Engineer')",
    )
    parser.add_argument(
        "--location",
        type=str,
        default="India",
        help="Target geographic region (default: 'India')",
    )
    parser.add_argument(
        "--keywords",
        type=str,
        default="Open to work",
        help="Additional search keywords (default: 'Open to work')",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=1,
        help="Number of pages to extract (25 leads per page, default: 1)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Local SQLite database file path (default: sales_nav_leads.db in live mode, isolated in dry-run)",
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        default=None,
        help="CSV export file path (default: sales_nav_leads.csv in live mode, dry_run_leads.csv in dry-run)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute offline test run using realistic mock payloads (no cookies needed)",
    )
    parser.add_argument(
        "--no-jitter",
        action="store_true",
        help="Disable sleep jitter between requests (useful for fast testing)",
    )
    parser.add_argument(
        "--max-leads",
        type=int,
        default=DEFAULT_MAX_DAILY_LEADS,
        help="Maximum daily leads safety limit (default: 2000)",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    print("=" * 72)
    print("🚀 TalentPulse AI — In-House Extraction & Candidate Qualification Engine")
    print("=" * 72)

    # Ingestion & Parameter Preparation
    if args.url:
        print(f"[*] Ingesting Sales Navigator Search URL: {args.url}")
        filters = SalesNavURLParser.parse_url(args.url)
        # Allow CLI flags to override URL values if explicitly specified
        if args.role and not filters.role:
            filters.role = args.role
        if args.location and not filters.location:
            filters.location = args.location
        if args.keywords and not filters.keywords:
            filters.keywords = args.keywords
    else:
        filters = SearchFilterParams(
            role=args.role,
            location=args.location,
            keywords=args.keywords,
        )

    print("[*] Filter Criteria:")
    print(f"    - Target Role:    {filters.role or 'All Roles'}")
    print(f"    - Location:       {filters.location or 'Global'}")
    print(f"    - Keywords:       {filters.keywords or 'None'}")
    print(f"    - Target Pages:   {args.pages} ({args.pages * PAGE_SIZE} potential leads)")

    # Session Setup
    session = None
    if not args.dry_run:
        li_at, jsessionid = load_credentials()
        if not li_at or not jsessionid:
            print("\n❌ Error: Missing credentials!")
            print("Please create a .env file with your LinkedIn cookies:")
            print("  LI_AT=your_li_at_cookie_here")
            print("  JSESSIONID=\"ajax:your_jsessionid_here\"")
            print("\nTip: To test the pipeline offline without credentials, run with --dry-run")
            sys.exit(1)

        session = create_sales_nav_session(li_at, jsessionid)
        print("[*] Persistent authenticated session initialized.")
    else:
        print("[*] Mode: DRY RUN (Offline mock simulation, zero cookies required)")

    # Database & Pipeline Initialization
    # Enforce strict isolation for dry-run mode to prevent touching production files
    if args.dry_run:
        chosen_db_path = args.db_path or os.path.join(tempfile.gettempdir(), f"dry_run_{int(time.time())}.db")
        chosen_csv_path = args.csv_path or "dry_run_leads.csv"
    else:
        chosen_db_path = args.db_path or "sales_nav_leads.db"
        chosen_csv_path = args.csv_path or "sales_nav_leads.csv"

    db = LeadDatabase(db_path=chosen_db_path)
    extractor = SalesNavExtractor(
        session=session,
        db=db,
        max_daily_leads=args.max_leads,
        enable_jitter=not args.no_jitter,
        dry_run=args.dry_run,
    )

    # Execution
    try:
        processed, qualified = extractor.run(filters=filters, max_pages=args.pages)
    except ExpiredSessionError as e:
        print(f"\n❌ {e}")
        sys.exit(1)
    except RateLimitExceededError as e:
        print(f"\n⚠️ {e}")
        sys.exit(1)
    except CheckpointError as e:
        print(f"\n🛑 {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Extraction failed unexpectedly: {e}")
        logger.exception("Extraction error")
        sys.exit(1)

    # Export
    exported_count = db.export_to_csv(csv_path=chosen_csv_path)

    print("\n" + "=" * 72)
    print("📊 Extraction & Qualification Summary")
    print("=" * 72)
    print(f"  • Processed in this run:  {processed} leads")
    print(f"  • Qualified Leads:        {qualified} leads")
    print(f"  • Filtered Out:           {processed - qualified} leads")
    print(f"  • Total in SQLite DB:     {db.get_total_count()} leads (deduplicated)")
    print(f"  • SQLite Database:        {chosen_db_path}")
    print(f"  • Exported CSV File:      {chosen_csv_path} ({exported_count} rows)")
    print("=" * 72)
    print("✅ Pipeline execution completed successfully.\n")


if __name__ == "__main__":
    main()
