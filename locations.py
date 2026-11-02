"""
Location resolution, modelled on LinkedIn's location filter.

LinkedIn searches by location *IDs* (its geo list), not free text, and the IDs are hierarchical:
a country covers every city in it, a metro area covers a city and its suburbs. This module:

  * maps everyday names to the official ones  (Bangalore -> Bengaluru, USA -> United States)
  * resolves any typed/picked place to a LinkedIn geo ID, climbing to the nearest region that
    has one when the place itself has none (Banga, India -> India)
  * returns `local_terms` in that case, so the qualification gate can keep only candidates who
    are really in the place asked for (the search is never silently widened)
"""
import re
import unicodedata
from typing import Dict, List, Optional

# Everyday / old / short names -> the name used in LinkedIn's list
ALIASES: Dict[str, str] = {
    # India: renamed cities and common short forms
    "bangalore": "Bengaluru, Karnataka, India", "blr": "Bengaluru, Karnataka, India",
    "bengaluru": "Bengaluru, Karnataka, India",
    "gurgaon": "Gurugram, Haryana, India", "gurugram": "Gurugram, Haryana, India",
    "bombay": "Mumbai, Maharashtra, India", "navi mumbai": "Mumbai, Maharashtra, India",
    "thane": "Mumbai, Maharashtra, India", "mumbai": "Mumbai, Maharashtra, India",
    "madras": "Chennai, Tamil Nadu, India", "chennai": "Chennai, Tamil Nadu, India",
    "calcutta": "Kolkata, West Bengal, India", "kolkata": "Kolkata, West Bengal, India",
    "poona": "Pune, Maharashtra, India", "pune": "Pune, Maharashtra, India",
    "cochin": "Kochi, Kerala, India", "ernakulam": "Kochi, Kerala, India", "kochi": "Kochi, Kerala, India",
    "trivandrum": "Thiruvananthapuram, Kerala, India", "calicut": "Kozhikode, Kerala, India",
    "mysore": "Mysuru, Karnataka, India", "vizag": "Visakhapatnam, Andhra Pradesh, India",
    "trichy": "Tiruchirappalli, Tamil Nadu, India", "baroda": "Vadodara, Gujarat, India",
    "kovai": "Coimbatore, Tamil Nadu, India", "cbe": "Coimbatore, Tamil Nadu, India",
    "hyderabad": "Hyderabad, Telangana, India", "secunderabad": "Hyderabad, Telangana, India",
    "hyd": "Hyderabad, Telangana, India",
    "delhi": "Delhi, India", "new delhi": "Delhi, India", "ncr": "Delhi, India", "delhi ncr": "Delhi, India",
    "noida": "Noida, Uttar Pradesh, India", "greater noida": "Noida, Uttar Pradesh, India",
    "ahmedabad": "Ahmedabad, Gujarat, India", "jaipur": "Jaipur, Rajasthan, India",
    "chandigarh": "Chandigarh, India", "mohali": "Chandigarh, India",
    # Countries
    "usa": "United States", "us": "United States", "u.s.": "United States", "u.s.a.": "United States",
    "america": "United States", "united states of america": "United States",
    "uk": "United Kingdom", "u.k.": "United Kingdom", "england": "United Kingdom",
    "britain": "United Kingdom", "great britain": "United Kingdom",
    "uae": "United Arab Emirates", "emirates": "United Arab Emirates",
    "deutschland": "Germany", "bharat": "India",
    # Metro areas
    "nyc": "Greater New York City Area", "new york city": "Greater New York City Area",
    "sf": "San Francisco Bay Area", "bay area": "San Francisco Bay Area",
    "san francisco": "San Francisco Bay Area", "silicon valley": "San Francisco Bay Area",
    "la": "Greater Los Angeles Area", "los angeles": "Greater Los Angeles Area",
    "seattle": "Greater Seattle Area", "boston": "Greater Boston", "chicago": "Greater Chicago Area",
    "dallas": "Dallas-Fort Worth Metroplex", "austin": "Austin, Texas Metropolitan Area",
    "toronto": "Greater Toronto Area, Canada", "vancouver": "Greater Vancouver Area, Canada",
    "london": "London Area, United Kingdom", "berlin": "Berlin Metropolitan Area, Germany",
    "munich": "Munich Metropolitan Area, Germany", "amsterdam": "Amsterdam Area, Netherlands",
    "paris": "Paris Area, France", "dublin": "Greater Dublin Area, Ireland",
    "dubai": "Dubai, United Arab Emirates", "sydney": "Greater Sydney Area, Australia",
}


def _fold(text: str) -> str:
    t = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9.,\- ]", " ", t)).strip(" ,")


def canonical(text: str) -> str:
    """'bangalore' -> 'Bengaluru, Karnataka, India'; unknown text is returned unchanged."""
    return ALIASES.get(_fold(text), (text or "").strip())


def alias_suggestions(query: str, limit: int = 3) -> List[str]:
    """Official names whose everyday name starts with what the user typed."""
    q = _fold(query)
    if len(q) < 2:
        return []
    out: List[str] = []
    for alias, name in ALIASES.items():
        if alias.startswith(q) and name not in out and len(alias) >= len(q):
            out.append(name)
    out.sort(key=lambda n: (not _fold(n).startswith(q), len(n)))
    return out[:limit]


def _local_terms(place: str) -> List[str]:
    """Words a lead's location must contain to count as 'in' this place (city + its old names)."""
    parts = [p.strip() for p in place.split(",") if p.strip()]
    first = _fold(parts[0]) if parts else ""
    if not first:
        return []
    terms = {first}
    country = _fold(parts[-1]) if len(parts) > 1 else ""
    target = ALIASES.get(first, "")
    for alias, name in ALIASES.items():
        if country and _fold(name.split(",")[-1]) != country:
            continue  # Hyderabad, Pakistan must not borrow India's 'Secunderabad'
        if name == target or _fold(name.split(",")[0]) == first:
            if len(alias) > 3 and "." not in alias:
                terms.add(alias)
            terms.add(_fold(name.split(",")[0]))
    return sorted(terms)


def resolve(text: str, geo_id: Optional[str], directory: List[Dict[str, str]]) -> Dict[str, object]:
    """
    Turn the Location box into what LinkedIn needs.
    Returns {name, id, exact, local_terms}:
      exact=True  -> the place itself has a LinkedIn ID, no extra filtering
      exact=False -> searched within the nearest region with an ID; local_terms narrow it back
    """
    text = (text or "").strip()
    if not text:
        return {"name": "", "id": "", "exact": True, "local_terms": []}
    if geo_id and str(geo_id).isdigit():
        return {"name": text, "id": str(geo_id), "exact": True, "local_terms": []}

    by_name = {_fold(d["name"]): d for d in directory}
    by_first = {}
    for d in directory:
        by_first.setdefault(_fold(d["name"].split(",")[0]), d)

    name = canonical(text)
    hit = by_name.get(_fold(name))
    if hit:
        return {"name": hit["name"], "id": hit["id"], "exact": True, "local_terms": []}
    first = _fold(name.split(",")[0])
    hit = by_first.get(first)
    if hit and (len(name.split(",")) == 1 or _fold(name.split(",")[-1]) == _fold(hit.get("country", ""))):
        return {"name": hit["name"], "id": hit["id"], "exact": True, "local_terms": []}

    # Climb: "City, State, Country" -> State -> Country (first one LinkedIn knows)
    parts = [p.strip() for p in name.split(",") if p.strip()]
    for part in parts[1:]:
        parent = by_name.get(_fold(canonical(part))) or by_first.get(_fold(canonical(part)))
        if parent:
            return {"name": parent["name"], "id": parent["id"], "exact": False, "local_terms": _local_terms(name)}

    # Nothing with an ID: send the text as LinkedIn would see it, and filter by it locally too
    return {"name": name, "id": "", "exact": False, "local_terms": _local_terms(name)}


def resolve_many(places: List[Dict[str, str]], directory: List[Dict[str, str]]) -> Dict[str, object]:
    """
    Several places, OR'd (like Sales Navigator's multi-location filter).
    Returns {regions: [{id, text}], local_terms, names}:
      * all places have an ID (own or parent) -> every ID goes to LinkedIn
      * any place has no ID at all            -> no REGION filter (it would exclude that place);
                                                 Gate G keeps leads in any of the places
      * Gate G runs whenever a place was widened; it then accepts any of the places, exact ones too
    """
    resolved = [resolve(p.get("text", ""), p.get("id"), directory) for p in places if (p.get("text") or "").strip()]
    names = [r["name"] for r in resolved]
    if not resolved:
        return {"regions": [], "local_terms": [], "names": []}
    any_widened = any(not r["exact"] for r in resolved)
    any_unlinked = any(not r["id"] for r in resolved)
    regions = [] if any_unlinked else [{"id": r["id"], "text": r["name"]} for r in resolved]
    terms: List[str] = []
    if any_widened:
        for p, r in zip(places, resolved):
            for t in (r["local_terms"] or _local_terms(r["name"]) or _local_terms(p.get("text", ""))):
                if t not in terms:
                    terms.append(t)
    return {"regions": regions, "local_terms": terms, "names": names}


def lead_in_place(lead_location: str, local_terms: List[str]) -> bool:
    """True when the lead's location mentions the requested place (or is unknown)."""
    if not local_terms or not (lead_location or "").strip():
        return True
    loc = f" {_fold(lead_location).replace(',', ' ')} "
    return any(f" {t} " in loc for t in local_terms)
