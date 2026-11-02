"""
Standard job-title catalogue + typeahead, modelled on LinkedIn's title box.

LinkedIn keeps a taxonomy of standardized titles and rolls the free-text titles members type
("SWE", "Software Developer", "Software Engg") into them. This module does the same offline:

  * CATALOG  - standard titles across all job families, each with the variants people write
  * search() - typeahead: prefix, word-order-free, abbreviation and typo tolerant
  * aliases()- the variants of a standard title (used by the qualification gate)

When Sales Navigator cookies are configured, app.py asks LinkedIn's own title typeahead first
and falls back to this catalogue.
"""
import difflib
from functools import lru_cache
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# Hand-curated standard titles: "Standard Title": [variants people write]
# ---------------------------------------------------------------------------
_CURATED: Dict[str, List[str]] = {
    # Software engineering
    "Software Engineer": ["Software Developer", "SDE", "SWE", "Software Development Engineer", "Programmer", "Application Developer", "Coder"],
    "Senior Software Engineer": ["Senior Software Developer", "Sr Software Engineer", "SDE 2", "SDE II", "SDE 3", "SDE III"],
    "Staff Software Engineer": ["Staff Engineer"],
    "Principal Software Engineer": ["Principal Engineer"],
    "Lead Software Engineer": ["Tech Lead", "Technical Lead", "Team Lead Software", "Engineering Lead"],
    "Associate Software Engineer": ["Junior Software Engineer", "Junior Developer", "Trainee Software Engineer", "Graduate Engineer Trainee", "SDE 1", "SDE I"],
    "Software Engineer Intern": ["Software Intern", "Developer Intern", "SDE Intern"],
    "Full Stack Developer": ["Full Stack Engineer", "Fullstack Developer", "Full-Stack Developer", "FSD", "MERN Stack Developer", "MEAN Stack Developer"],
    "Frontend Developer": ["Front End Developer", "Front-End Engineer", "Frontend Engineer", "UI Developer", "Web UI Developer"],
    "Backend Developer": ["Back End Developer", "Backend Engineer", "Back-End Engineer", "Server Side Developer"],
    "Web Developer": ["Website Developer", "Web Programmer", "Web Designer and Developer"],
    "Mobile Application Developer": ["Mobile Developer", "App Developer", "Mobile Engineer"],
    "Embedded Software Engineer": ["Embedded Engineer", "Firmware Engineer", "Embedded Developer", "Embedded Systems Engineer"],
    "Game Developer": ["Game Programmer", "Unity Developer", "Unreal Developer", "Game Engineer"],
    "Blockchain Developer": ["Web3 Developer", "Smart Contract Developer", "Solidity Developer"],
    "Software Architect": ["Solutions Architect", "Solution Architect", "Technical Architect", "Application Architect", "Enterprise Architect"],
    "Cloud Architect": ["AWS Architect", "Azure Architect", "GCP Architect", "Cloud Solutions Architect"],
    "Engineering Manager": ["Software Engineering Manager", "Development Manager", "Manager Software Engineering", "EM"],
    "Director of Engineering": ["Engineering Director", "Head of Engineering", "VP Engineering", "Vice President Engineering"],
    "Chief Technology Officer": ["CTO", "Chief Technical Officer"],
    # Infra / DevOps / security
    "DevOps Engineer": ["DevOps", "Dev Ops Engineer", "Build and Release Engineer", "Release Engineer", "CI CD Engineer"],
    "Site Reliability Engineer": ["SRE", "Reliability Engineer", "Production Engineer"],
    "Cloud Engineer": ["AWS Engineer", "Azure Engineer", "GCP Engineer", "Cloud Infrastructure Engineer", "Cloud DevOps Engineer"],
    "Platform Engineer": ["Infrastructure Engineer", "Platform Developer"],
    "System Administrator": ["Sysadmin", "Systems Administrator", "Linux Administrator", "Windows Administrator", "Server Administrator"],
    "Network Engineer": ["Network Administrator", "Networking Engineer", "CCNA Engineer", "Network Support Engineer"],
    "Database Administrator": ["DBA", "Oracle DBA", "SQL Server DBA", "Database Engineer", "MySQL DBA"],
    "Cybersecurity Engineer": ["Security Engineer", "Information Security Engineer", "Cyber Security Engineer", "InfoSec Engineer", "Application Security Engineer"],
    "Security Analyst": ["Cybersecurity Analyst", "SOC Analyst", "Information Security Analyst", "Cyber Security Analyst"],
    "Penetration Tester": ["Ethical Hacker", "Pentester", "Pen Tester", "Offensive Security Engineer", "VAPT Engineer"],
    "Chief Information Security Officer": ["CISO", "Head of Security", "Head of Information Security"],
    "IT Support Engineer": ["IT Support", "Desktop Support Engineer", "Help Desk Technician", "Technical Support Engineer", "IT Helpdesk", "Service Desk Analyst"],
    "IT Manager": ["IT Head", "Head of IT", "Information Technology Manager"],
    "Chief Information Officer": ["CIO"],
    # QA
    "QA Engineer": ["Quality Assurance Engineer", "Software Tester", "Test Engineer", "Manual Tester", "QA Analyst", "Software Test Engineer"],
    "Automation Test Engineer": ["QA Automation Engineer", "Test Automation Engineer", "SDET", "Selenium Tester", "Automation Tester", "Software Development Engineer in Test"],
    "Performance Test Engineer": ["Performance Tester", "JMeter Engineer", "Load Test Engineer"],
    "QA Lead": ["Test Lead", "QA Manager", "Test Manager"],
    # Data & AI
    "Data Scientist": ["Data Science Engineer", "Senior Data Scientist", "Applied Scientist"],
    "Data Analyst": ["Data Analytics Specialist", "Analytics Analyst", "Reporting Analyst", "MIS Analyst", "MIS Executive"],
    "Data Engineer": ["Big Data Engineer", "ETL Developer", "Data Pipeline Engineer", "Spark Developer", "Hadoop Developer", "Analytics Engineer"],
    "Business Intelligence Analyst": ["BI Analyst", "BI Developer", "Power BI Developer", "Tableau Developer", "Business Intelligence Developer"],
    "Machine Learning Engineer": ["ML Engineer", "MLE", "Machine Learning Developer", "MLOps Engineer"],
    "AI Engineer": ["Artificial Intelligence Engineer", "AI Developer", "Generative AI Engineer", "GenAI Engineer", "LLM Engineer", "AI ML Engineer", "Applied AI Engineer"],
    "Deep Learning Engineer": ["Computer Vision Engineer", "NLP Engineer", "Natural Language Processing Engineer"],
    "AI Researcher": ["Research Scientist", "Machine Learning Researcher", "AI Research Scientist"],
    "Prompt Engineer": ["AI Prompt Engineer"],
    "Data Architect": ["Big Data Architect", "Data Platform Architect"],
    "Head of Data": ["Director of Data Science", "Data Science Manager", "Analytics Manager", "Chief Data Officer", "CDO"],
    "Statistician": ["Biostatistician", "Statistical Analyst"],
    # Product / project / design
    "Product Manager": ["PM", "Product Owner", "Associate Product Manager", "APM", "Technical Product Manager"],
    "Senior Product Manager": ["Sr Product Manager", "Lead Product Manager", "Group Product Manager"],
    "Head of Product": ["Director of Product", "VP Product", "Chief Product Officer", "CPO"],
    "Project Manager": ["IT Project Manager", "Technical Project Manager", "Delivery Manager", "PMP"],
    "Program Manager": ["Technical Program Manager", "TPM", "Programme Manager"],
    "Scrum Master": ["Agile Coach", "Agile Project Manager", "Scrum Master Agile"],
    "Business Analyst": ["BA", "IT Business Analyst", "Functional Analyst", "Requirements Analyst", "Business Systems Analyst"],
    "UX Designer": ["User Experience Designer", "UX Researcher", "Interaction Designer", "UX UI Designer"],
    "UI Designer": ["User Interface Designer", "UI UX Designer", "Visual Designer", "Web Designer"],
    "Product Designer": ["Digital Product Designer", "Senior Product Designer"],
    "Graphic Designer": ["Visual Designer", "Creative Designer", "Illustrator", "Motion Graphics Designer", "Video Editor"],
    "Design Lead": ["Head of Design", "Design Manager", "Creative Director", "Art Director"],
    "Technical Writer": ["Documentation Specialist", "Content Developer Technical", "Documentation Engineer"],
    # Sales & business development
    "Sales Executive": ["Sales Associate", "Sales Representative", "Sales Officer", "Field Sales Executive", "Sales Rep"],
    "Business Development Executive": ["BDE", "Business Development Associate", "Business Development Representative", "BDR"],
    "Business Development Manager": ["BDM", "Business Development Lead", "Head of Business Development"],
    "Sales Development Representative": ["SDR", "Inside Sales Representative", "Inside Sales Executive", "Lead Generation Executive"],
    "Account Executive": ["AE", "Enterprise Account Executive", "Senior Account Executive"],
    "Account Manager": ["Key Account Manager", "KAM", "Client Relationship Manager", "Relationship Manager"],
    "Sales Manager": ["Area Sales Manager", "ASM", "Regional Sales Manager", "RSM", "Territory Sales Manager", "Zonal Sales Manager"],
    "Head of Sales": ["Sales Director", "Director of Sales", "VP Sales", "Vice President Sales", "Chief Sales Officer", "National Sales Manager"],
    "Pre Sales Consultant": ["Presales Engineer", "Solutions Consultant", "Sales Engineer", "Pre-Sales Consultant"],
    "Customer Success Manager": ["CSM", "Client Success Manager", "Customer Success Executive", "Customer Success Lead"],
    "Telecaller": ["Telesales Executive", "Tele Sales Executive", "Telemarketing Executive", "Tele Caller"],
    "Retail Sales Associate": ["Store Associate", "Retail Executive", "Store Manager", "Showroom Sales Executive"],
    # Marketing
    "Digital Marketing Executive": ["Digital Marketing Specialist", "Digital Marketer", "Online Marketing Executive", "Performance Marketing Executive"],
    "Digital Marketing Manager": ["Performance Marketing Manager", "Growth Marketing Manager", "Online Marketing Manager"],
    "Marketing Manager": ["Brand Manager", "Product Marketing Manager", "PMM", "Marketing Lead"],
    "Head of Marketing": ["Marketing Director", "Director of Marketing", "VP Marketing", "Chief Marketing Officer", "CMO"],
    "SEO Specialist": ["SEO Executive", "SEO Analyst", "Search Engine Optimization Specialist", "SEO Manager"],
    "Social Media Manager": ["Social Media Executive", "Social Media Marketing Specialist", "Community Manager", "Social Media Strategist"],
    "Content Writer": ["Copywriter", "Content Creator", "Content Strategist", "Content Marketing Specialist", "Blog Writer", "Content Editor"],
    "Marketing Executive": ["Marketing Associate", "Marketing Coordinator", "Marketing Specialist", "Marketing Officer"],
    "Public Relations Manager": ["PR Manager", "Communications Manager", "Corporate Communications Manager", "PR Executive"],
    "Growth Manager": ["Growth Hacker", "Head of Growth", "Growth Lead"],
    "Event Manager": ["Event Coordinator", "Event Planner", "Events Executive"],
    # HR & recruiting
    "Recruiter": ["Talent Acquisition Specialist", "Technical Recruiter", "IT Recruiter", "HR Recruiter", "Talent Acquisition Executive", "Sourcer", "Talent Sourcer"],
    "Talent Acquisition Manager": ["TA Manager", "Head of Talent Acquisition", "Recruitment Manager", "Recruiting Manager", "Talent Acquisition Lead"],
    "HR Executive": ["Human Resources Executive", "HR Associate", "HR Coordinator", "HR Assistant", "HR Officer"],
    "HR Generalist": ["Human Resources Generalist", "HR Specialist"],
    "HR Business Partner": ["HRBP", "Human Resources Business Partner"],
    "HR Manager": ["Human Resources Manager", "People Manager", "People Operations Manager", "HR Lead"],
    "Head of HR": ["HR Director", "Director of Human Resources", "Chief Human Resources Officer", "CHRO", "Chief People Officer", "VP HR"],
    "Payroll Specialist": ["Payroll Executive", "Payroll Administrator", "Payroll Manager"],
    "Learning and Development Manager": ["L&D Manager", "Training Manager", "Corporate Trainer", "Learning Specialist"],
    # Finance & accounting
    "Accountant": ["Accounts Executive", "Accounts Assistant", "Junior Accountant", "Staff Accountant", "Senior Accountant", "Bookkeeper"],
    "Chartered Accountant": ["CA", "Certified Public Accountant", "CPA"],
    "Financial Analyst": ["Finance Analyst", "FP&A Analyst", "Investment Analyst", "Equity Research Analyst", "Credit Analyst"],
    "Finance Manager": ["Accounts Manager", "Finance Controller", "Financial Controller", "FP&A Manager"],
    "Chief Financial Officer": ["CFO", "Finance Director", "VP Finance", "Head of Finance"],
    "Auditor": ["Internal Auditor", "Audit Associate", "Audit Manager", "External Auditor"],
    "Tax Consultant": ["Tax Analyst", "Tax Associate", "Tax Manager", "GST Consultant"],
    "Company Secretary": ["CS", "Corporate Secretary"],
    "Investment Banker": ["Investment Banking Analyst", "Investment Banking Associate", "M&A Analyst"],
    "Risk Analyst": ["Risk Manager", "Credit Risk Analyst", "Compliance Analyst", "Compliance Officer"],
    "Bank Officer": ["Banker", "Branch Manager Bank", "Probationary Officer", "Relationship Officer", "Loan Officer"],
    "Insurance Advisor": ["Insurance Agent", "Insurance Sales Manager", "Underwriter", "Claims Executive"],
    # Operations, supply chain, admin
    "Operations Executive": ["Operations Associate", "Operations Coordinator", "Operations Analyst", "Back Office Executive"],
    "Operations Manager": ["Head of Operations", "Director of Operations", "COO", "Chief Operating Officer", "General Manager Operations"],
    "Supply Chain Manager": ["Supply Chain Analyst", "SCM Executive", "Logistics Manager", "Logistics Coordinator", "Logistics Executive"],
    "Procurement Manager": ["Purchase Manager", "Purchase Executive", "Buyer", "Sourcing Manager", "Procurement Specialist", "Vendor Manager"],
    "Warehouse Manager": ["Warehouse Supervisor", "Inventory Manager", "Store Keeper", "Inventory Controller"],
    "Administrative Assistant": ["Admin Executive", "Office Assistant", "Office Administrator", "Admin Officer", "Front Office Executive", "Receptionist"],
    "Executive Assistant": ["EA", "Personal Assistant", "PA", "Executive Secretary", "Secretary"],
    "Office Manager": ["Admin Manager", "Facility Manager", "Facilities Manager"],
    "Data Entry Operator": ["Data Entry Executive", "Data Entry Clerk", "Computer Operator"],
    # Customer service
    "Customer Support Executive": ["Customer Service Representative", "Customer Care Executive", "Customer Support Associate", "CSR", "Call Center Executive", "BPO Executive", "Voice Process Executive", "Chat Support Executive"],
    "Customer Support Manager": ["Customer Service Manager", "Support Manager", "Team Leader Customer Support", "Head of Customer Support"],
    "Technical Support Executive": ["Technical Support Associate", "Tech Support", "Product Support Engineer", "Application Support Engineer", "L1 Support Engineer", "L2 Support Engineer"],
    # Consulting & leadership
    "Management Consultant": ["Consultant", "Strategy Consultant", "Business Consultant", "Associate Consultant", "Senior Consultant"],
    "SAP Consultant": ["SAP FICO Consultant", "SAP MM Consultant", "SAP SD Consultant", "SAP ABAP Developer", "SAP Basis Consultant", "SAP HANA Consultant"],
    "Salesforce Developer": ["Salesforce Consultant", "Salesforce Administrator", "Salesforce Admin", "SFDC Developer"],
    "ServiceNow Developer": ["ServiceNow Administrator", "ServiceNow Consultant"],
    "Chief Executive Officer": ["CEO", "Managing Director", "MD", "Founder", "Co-Founder", "President", "Owner"],
    "General Manager": ["GM", "Business Head", "Country Manager", "Branch Manager"],
    "Strategy Manager": ["Corporate Strategy Manager", "Business Strategy Manager", "Chief of Staff"],
    # Engineering (non-software)
    "Mechanical Engineer": ["Design Engineer Mechanical", "Mechanical Design Engineer", "CAD Engineer", "Production Engineer Mechanical"],
    "Electrical Engineer": ["Electrical Design Engineer", "Electrical Maintenance Engineer", "Power Engineer"],
    "Electronics Engineer": ["ECE Engineer", "Hardware Engineer", "PCB Design Engineer", "VLSI Engineer", "Design Verification Engineer", "RTL Design Engineer"],
    "Civil Engineer": ["Site Engineer", "Structural Engineer", "Construction Engineer", "Project Engineer Civil", "Quantity Surveyor"],
    "Chemical Engineer": ["Process Engineer", "Process Chemist"],
    "Production Engineer": ["Manufacturing Engineer", "Plant Engineer", "Production Supervisor", "Shift Engineer"],
    "Quality Engineer": ["Quality Control Engineer", "QC Engineer", "Quality Assurance Engineer Manufacturing", "Quality Inspector", "QA QC Engineer"],
    "Maintenance Engineer": ["Maintenance Technician", "Service Engineer", "Field Service Engineer", "Field Engineer"],
    "Automotive Engineer": ["Automobile Engineer", "Vehicle Engineer", "EV Engineer"],
    "Aerospace Engineer": ["Aeronautical Engineer", "Avionics Engineer"],
    "Robotics Engineer": ["Automation Engineer", "PLC Engineer", "SCADA Engineer", "Controls Engineer", "Instrumentation Engineer"],
    "Architect": ["Building Architect", "Interior Designer", "Interior Architect", "Landscape Architect", "Urban Planner"],
    "Plant Manager": ["Factory Manager", "Production Manager", "Manufacturing Manager", "Works Manager"],
    "Safety Officer": ["EHS Officer", "HSE Officer", "Safety Engineer", "Fire and Safety Officer", "EHS Manager"],
    # Healthcare & life sciences
    "Doctor": ["Physician", "Medical Officer", "MBBS Doctor", "General Physician", "Consultant Physician", "Resident Doctor"],
    "Nurse": ["Staff Nurse", "Registered Nurse", "RN", "Nursing Officer", "ICU Nurse", "Head Nurse"],
    "Pharmacist": ["Clinical Pharmacist", "Hospital Pharmacist", "Retail Pharmacist"],
    "Medical Representative": ["MR", "Medical Sales Representative", "Pharma Sales Executive", "Area Business Manager", "ABM"],
    "Lab Technician": ["Laboratory Technician", "Medical Lab Technician", "MLT", "Lab Assistant"],
    "Clinical Research Associate": ["CRA", "Clinical Research Coordinator", "Clinical Data Manager", "Pharmacovigilance Associate", "Drug Safety Associate"],
    "Medical Coder": ["Medical Coding Specialist", "Certified Medical Coder", "CPC", "Medical Billing Executive"],
    "Physiotherapist": ["Physical Therapist", "Physio"],
    "Dentist": ["Dental Surgeon", "BDS", "Orthodontist"],
    "Psychologist": ["Counselor", "Counsellor", "Therapist", "Clinical Psychologist"],
    "Dietitian": ["Nutritionist", "Clinical Dietitian"],
    "Hospital Administrator": ["Healthcare Administrator", "Hospital Manager", "Medical Superintendent"],
    "Research Scientist Life Sciences": ["Research Associate", "Scientist", "Biotechnologist", "Microbiologist", "Molecular Biologist"],
    # Education
    "Teacher": ["School Teacher", "PGT Teacher", "TGT Teacher", "PRT Teacher", "Primary Teacher", "Educator", "Faculty"],
    "Professor": ["Assistant Professor", "Associate Professor", "Lecturer", "Lecturer Professor", "Academic"],
    "Tutor": ["Online Tutor", "Home Tutor", "Subject Matter Expert", "SME"],
    "Principal": ["School Principal", "Headmaster", "Head Teacher", "Vice Principal"],
    "Academic Counselor": ["Education Counselor", "Admission Counselor", "Career Counselor", "Student Counselor"],
    "Instructional Designer": ["E-Learning Developer", "eLearning Designer", "Curriculum Developer", "Curriculum Designer"],
    # Legal
    "Lawyer": ["Advocate", "Attorney", "Legal Counsel", "Associate Lawyer", "Solicitor", "Barrister"],
    "Legal Manager": ["Head of Legal", "General Counsel", "Legal Head", "Corporate Counsel"],
    "Legal Associate": ["Legal Executive", "Paralegal", "Legal Assistant", "Legal Analyst"],
    # Hospitality, retail, logistics, field
    "Chef": ["Cook", "Sous Chef", "Commis Chef", "Executive Chef", "Head Chef", "Chef de Partie"],
    "Hotel Manager": ["Front Office Manager", "Guest Relations Executive", "F&B Manager", "Restaurant Manager", "Housekeeping Supervisor"],
    "Travel Consultant": ["Travel Agent", "Ticketing Executive", "Tour Manager", "Holiday Consultant"],
    "Delivery Executive": ["Delivery Boy", "Delivery Associate", "Courier", "Rider", "Delivery Partner"],
    "Driver": ["Car Driver", "Truck Driver", "Chauffeur", "Heavy Vehicle Driver"],
    "Electrician": ["Electrical Technician", "Wireman"],
    "Technician": ["Service Technician", "AC Technician", "Mechanic", "Fitter", "Welder", "Machine Operator", "CNC Operator"],
    "Security Guard": ["Security Officer", "Security Supervisor"],
    "Real Estate Agent": ["Real Estate Consultant", "Property Consultant", "Real Estate Sales Executive", "Realtor"],
    "Fashion Designer": ["Apparel Designer", "Textile Designer", "Merchandiser", "Fashion Merchandiser"],
    "Photographer": ["Videographer", "Cinematographer", "Photo Editor"],
    "Journalist": ["Reporter", "Correspondent", "News Editor", "Editor", "Sub Editor"],
    "Translator": ["Interpreter", "Language Specialist", "Localization Specialist"],
    "Fitness Trainer": ["Gym Trainer", "Personal Trainer", "Yoga Instructor", "Fitness Coach"],
    "Social Worker": ["NGO Worker", "Community Worker", "Program Officer NGO", "Field Officer"],
    "Pilot": ["Commercial Pilot", "First Officer", "Captain Pilot"],
    "Cabin Crew": ["Flight Attendant", "Air Hostess", "Steward"],
}

# ---------------------------------------------------------------------------
# Generated titles: every common technology x developer/engineer role
# ---------------------------------------------------------------------------
_TECH: Dict[str, List[str]] = {
    "Java": ["J2EE", "Spring Boot", "Core Java"], "Python": ["Django", "Flask", "FastAPI"],
    ".NET": ["Dot Net", "C#", "ASP.NET"], "React": ["ReactJS", "React.js"], "Angular": ["AngularJS"],
    "Node.js": ["NodeJS", "Node"], "JavaScript": ["JS"], "TypeScript": [], "Vue.js": ["VueJS"],
    "PHP": ["Laravel", "CodeIgniter", "WordPress"], "Golang": ["Go"], "Ruby on Rails": ["Ruby", "RoR"],
    "C++": [], "Rust": [], "Scala": [], "Kotlin": [], "Swift": [], "iOS": [], "Android": [],
    "Flutter": [], "React Native": [], "Magento": [], "Shopify": [], "Drupal": [],
    "SQL": ["PL SQL", "PL/SQL", "Oracle"], "Mainframe": ["COBOL"], "Power Apps": ["Power Platform"],
    "Unity": [], "Snowflake": [], "Databricks": [], "Kafka": [], "AWS": [], "Azure": [], "GCP": [],
}
_TECH_ROLES = ["Developer", "Full Stack Developer", "Backend Developer", "Lead", "Architect"]
_MOBILE_OR_FRONT = {"React", "Angular", "Vue.js", "JavaScript", "TypeScript", "iOS", "Android",
                    "Flutter", "React Native", "Swift", "Kotlin", "Unity"}
_DATA_CLOUD = {"Snowflake", "Databricks", "Kafka", "AWS", "Azure", "GCP", "SQL"}


def _build() -> Dict[str, List[str]]:
    cat = {k: list(v) for k, v in _CURATED.items()}
    for tech, alts in _TECH.items():
        names = [tech] + alts
        if tech in _DATA_CLOUD:
            roles = ["Developer", "Engineer", "Architect"]
        elif tech in _MOBILE_OR_FRONT:
            roles = ["Developer", "Lead"]  # "React Engineer" etc. are variants of "React Developer"
        else:
            roles = _TECH_ROLES
        for role in roles:
            title = f"{tech} {role}"
            variants = [f"{n} {role}" for n in alts]
            if role == "Developer":
                variants += [f"{n} Engineer" for n in names] + [f"{n} Programmer" for n in names]
                variants += [f"Senior {n} Developer" for n in names]
            if role == "Full Stack Developer":
                variants += [f"{n} Fullstack Developer" for n in names] + [f"{n} Full Stack Engineer" for n in names]
            if role == "Backend Developer":
                variants += [f"{n} Back End Developer" for n in names] + [f"{n} Backend Engineer" for n in names]
            if role == "Lead":
                variants += [f"{n} Tech Lead" for n in names] + [f"{n} Team Lead" for n in names]
            cat.setdefault(title, []).extend(v for v in variants if v != title)
    # Seniority prefixes on the most searched titles (LinkedIn lists these as separate titles)
    for base in ["Software Engineer", "Data Scientist", "Data Engineer", "Data Analyst", "Business Analyst",
                 "Product Manager", "QA Engineer", "DevOps Engineer", "Full Stack Developer", "Java Developer",
                 "Python Developer", "Frontend Developer", "Backend Developer", "Machine Learning Engineer",
                 "AI Engineer", "UX Designer", "Accountant", "Recruiter", "Sales Executive", "HR Executive"]:
        for pre, short in (("Senior", "Sr"), ("Junior", "Jr"), ("Lead", None), ("Principal", None)):
            t = f"{pre} {base}"
            cat.setdefault(t, [])
            if short:
                cat[t].append(f"{short} {base}")
    return cat


CATALOG: Dict[str, List[str]] = _build()


# ---------------------------------------------------------------------------
# Typeahead
# ---------------------------------------------------------------------------
def _norm_tokens(text: str) -> List[str]:
    from sales_nav_extractor import RoleMatcher  # local import: avoids an import cycle
    return RoleMatcher.tokens(text)


@lru_cache(maxsize=1)
def _index() -> Tuple[List[Tuple[str, Tuple[str, ...], List[Tuple[str, Tuple[str, ...]]]]], List[str]]:
    rows = []
    vocab = set()
    for title, variants in CATALOG.items():
        t_tok = tuple(_norm_tokens(title))
        v_tok = [(v, tuple(_norm_tokens(v))) for v in variants]
        rows.append((title, t_tok, v_tok))
        vocab.update(t_tok)
        for _, vt in v_tok:
            vocab.update(vt)
    return rows, sorted(vocab)


def _fix_typos(tokens: List[str], vocab: List[str]) -> List[str]:
    out = []
    for i, tok in enumerate(tokens):
        last = i == len(tokens) - 1
        if tok in vocab or (last and any(v.startswith(tok) for v in vocab)) or len(tok) < 4:
            out.append(tok)
        else:
            close = difflib.get_close_matches(tok, vocab, n=1, cutoff=0.8)
            out.append(close[0] if close else tok)
    return out


_LESS_COMMON = {"lead", "architect", "junior", "principal", "staff", "intern", "head", "chief",
                "director", "backend", "vice", "president"}


def _score(q: List[str], cand: Tuple[str, ...]) -> int:
    """
    Every query word must be found, in any order. Any word may be a prefix ('cust supp');
    whole-word hits score higher. Plain titles rank above Lead/Architect/Junior variants.
    """
    if not q or not cand:
        return 0
    used = set()
    partial = 0
    for w in q:
        hit = next((j for j, c in enumerate(cand) if j not in used and c == w), None)
        if hit is None:
            hit = next((j for j, c in enumerate(cand) if j not in used and len(w) >= 2 and c.startswith(w)), None)
            partial += 1
        if hit is None:
            return 0
        used.add(hit)
    if tuple(q) == cand:
        return 100
    base = 85 if cand[:len(q)] == tuple(q) or cand[0].startswith(q[0]) else 70
    extra = [c for j, c in enumerate(cand) if j not in used]
    return base - 3 * partial - 2 * len(extra) - 4 * sum(c in _LESS_COMMON for c in extra)


def search(query: str, limit: int = 10) -> List[Dict[str, object]]:
    """Typeahead over the catalogue. Returns standard titles, best first."""
    rows, vocab = _index()
    q = _norm_tokens(query)
    if not q:
        return []
    variants_of_q = [q, _fix_typos(q, vocab)]
    best: Dict[str, Tuple[int, str]] = {}
    for title, t_tok, v_tok in rows:
        for qi, qt in enumerate(variants_of_q):
            if qi == 1 and qt == q:
                continue
            penalty = 15 if qi == 1 else 0
            s = _score(qt, t_tok)
            if s:
                s -= penalty
                if s > best.get(title, (0, ""))[0]:
                    best[title] = (s, "")
            for v, vt in v_tok:
                sv = _score(qt, vt)
                if sv:
                    sv = sv - 12 - penalty
                    if sv > best.get(title, (0, ""))[0]:
                        best[title] = (sv, v)
    ranked = sorted(best.items(), key=lambda kv: (-kv[1][0], len(kv[0]), kv[0]))
    return [
        {"name": title, "matched": via, "aliases": CATALOG[title][:4], "source": "catalog"}
        for title, (_, via) in ranked[:limit]
    ]


def aliases(title: str) -> List[str]:
    """Variants of a standard title (case-insensitive lookup); [] for free text."""
    key = _lower_map().get((title or "").strip().lower())
    return list(CATALOG[key]) if key else []


@lru_cache(maxsize=1)
def _lower_map() -> Dict[str, str]:
    return {t.lower(): t for t in CATALOG}
