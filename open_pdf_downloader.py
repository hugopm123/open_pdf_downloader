#!/usr/bin/env python3
"""
open_pdf_downloader – Multi-source Open Access PDF retrieval for Literature Reviews and AI research agents.

Author: Pedro Santana, https://www.pedrosantana.mx/
Copyright (c) 2026 Pedro Santana

Notes
-----
- This script does NOT bypass paywalls.
- It only attempts open / legally accessible sources.
- Some publisher pages may still block automated downloading.

Sources attempted (in configurable priority order):
  1. PubMed Central (PMC)     – via NCBI OA API
  2. Europe PMC               – via EBI REST API
  3. Unpaywall                – requires --email
  4. OpenAlex                 – disabled by default; enable with --openalex --openalex-api-key
  5. Semantic Scholar         – disabled by default; enable with --semantic-scholar
  6. Crossref                 – link objects in metadata (max 3 concurrent workers recommended)
  7. DOI landing-page scan    – meta-tag / anchor heuristics

License: MIT. See LICENSE file.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple
from urllib.parse import quote, urljoin, urlparse
import xml.etree.ElementTree as ET

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: requests\n"
        "Install it with: pip install requests"
    ) from exc

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
logger = logging.getLogger("open_pdf_downloader")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TOOL_VERSION = "1.1"
DEFAULT_TIMEOUT = 30
DEFAULT_DELAY = 0.5
DEFAULT_WORKERS = 1

USER_AGENT = (
    f"open-pdf-downloader/{TOOL_VERSION} "
    "(+https://github.com/pecesama/open_pdf_downloader)"
)

DOI_REGEX = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)
PDF_LIKE_RE = re.compile(r"(?:\.pdf(?:$|[?#]))|(?:/pdf(?:$|[/?#]))", re.IGNORECASE)

META_PDF_NAMES = {
    "citation_pdf_url",
    "eprints.document_url",
    "wkhealth_pdf_url",
    "pdf_url",
}
META_PDF_PROPERTIES = {
    "og:pdf",
    "og:pdf:url",
}

REPORT_FIELDNAMES = [
    "row_index",
    "title",
    "authors",
    "pmid",
    "doi",
    "pmcid_original",
    "pmcid_resolved",
    "source",
    "status",
    "pdf_url",
    "filename",
    "note",
]

# Names of all boolean source flags in DiscoveryConfig (used for **kwargs passthrough)
_SOURCE_FLAG_NAMES = (
    "use_pmc",
    "use_europepmc",
    "use_unpaywall",
    "use_openalex",
    "use_semantic_scholar",
    "use_crossref",
    "use_landing_page",
)

# Crossref polite pool documented concurrency limit.
# https://api.crossref.org/swagger-ui/index.html (rate limiting section)
CROSSREF_MAX_WORKERS = 3

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

class DiscoveryConfig(NamedTuple):
    email: str = ""
    s2_api_key: str = ""
    openalex_api_key: str = ""
    timeout: int = DEFAULT_TIMEOUT
    use_pmc: bool = True
    use_europepmc: bool = True
    use_unpaywall: bool = True
    # OpenAlex: disabled by default. The polite-pool email parameter is treated
    # as legacy by their current docs; authenticated access uses --openalex-api-key.
    # Enable with --openalex (opt-in). Free key at https://openalex.org/
    use_openalex: bool = False
    use_semantic_scholar: bool = False   # opt-in: enable with --semantic-scholar
    use_crossref: bool = True
    use_landing_page: bool = True


@dataclass
class DownloadResult:
    """
    Returned by retrieve_pdf(). Designed for clean consumption by agents,
    pipelines, and notebooks — no side effects, no sys.exit.
    """
    doi: str
    pmid: str
    pmcid_original: str
    pmcid_resolved: str
    title: str
    authors: str         # populated when authors column is present in input CSV
    status: str          # "downloaded" | "not_found" | "failed" | "skipped" | "error"
    source: str          # "PMC" | "EuropePMC" | "OpenAlex" | ...
    pdf_path: Optional[Path]
    pdf_url: str
    note: str

    def as_dict(self) -> Dict[str, str]:
        """Serialize to a plain dict (e.g. for JSON / CSV reporting)."""
        d = asdict(self)
        d["pdf_path"] = str(self.pdf_path) if self.pdf_path else ""
        return d

    @property
    def ok(self) -> bool:
        return self.status == "downloaded"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download open-access PDFs from a CSV using PMC, Europe PMC, "
            "Unpaywall, OpenAlex, Semantic Scholar, Crossref, "
            "and landing-page heuristics."
        )
    )
    # I/O
    parser.add_argument("--input", required=True, help="Path to CSV input.")
    parser.add_argument("--outdir", default="pdfs", help="Directory where PDFs are saved (default: pdfs).")
    parser.add_argument("--report", default="download_report.csv", help="Output CSV report path.")

    # Column overrides
    parser.add_argument("--doi-column", default="", help="Override DOI column name.")
    parser.add_argument("--pmcid-column", default="", help="Override PMCID column name.")
    parser.add_argument("--pmid-column", default="", help="Override PMID column name.")
    parser.add_argument("--citation-column", default="Citation", help="Citation column used as DOI fallback (default: Citation).")
    parser.add_argument("--title-column", default="Title", help="Title column for filenames/logs (default: Title).")
    parser.add_argument("--authors-column", default="Authors", help="Authors column (default: Authors).")

    # Credentials
    parser.add_argument("--email", default=os.environ.get("UNPAYWALL_EMAIL", ""),
                        help="Email for polite API usage (Unpaywall, Crossref).")
    parser.add_argument("--openalex-api-key", default=os.environ.get("OPENALEX_API_KEY", ""),
                        help="OpenAlex API key (free at https://openalex.org/). Required when --openalex is enabled.")
    parser.add_argument("--semantic-scholar-api-key", default=os.environ.get("S2_API_KEY", ""),
                        help="Semantic Scholar API key (optional; raises rate limits).")

    # Behaviour
    parser.add_argument("--max", type=int, default=0, help="Process only first N rows (0 = all).")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                        help=f"Delay between records in seconds (default: {DEFAULT_DELAY}).")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help=f"HTTP timeout in seconds (default: {DEFAULT_TIMEOUT}).")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel download workers (default: {DEFAULT_WORKERS}). "
                             f"Max {CROSSREF_MAX_WORKERS} recommended when Crossref is enabled (their polite pool limit).")

    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip when target PDF already exists.")

    # Source toggles
    parser.add_argument("--no-pmc", action="store_true", help="Disable PMC.")
    parser.add_argument("--no-europepmc", action="store_true", help="Disable Europe PMC.")
    parser.add_argument("--no-unpaywall", action="store_true", help="Disable Unpaywall.")
    parser.add_argument("--openalex", action="store_true",
                        help="Enable OpenAlex (disabled by default). Requires --openalex-api-key.")
    parser.add_argument("--semantic-scholar", action="store_true",
                        help="Enable Semantic Scholar (disabled by default). Useful for CS/engineering literature.")
    parser.add_argument("--no-crossref", action="store_true", help="Disable Crossref.")
    parser.add_argument("--no-landing-page", action="store_true", help="Disable landing-page scan.")

    parser.add_argument("--verbose", action="store_true", help="Print per-record progress to stderr.")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def build_session(timeout: int = DEFAULT_TIMEOUT) -> requests.Session:
    """Return a requests.Session with retry logic and shared headers."""
    retry_strategy = Retry(
        total=3,
        connect=0,        # Don't retry DNS failures or SSL errors — they won't resolve
        read=1,           # Retry once on read timeouts (transient)
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/pdf, application/json, text/xml, text/html, */*",
    })
    return session


def http_get(
    session: requests.Session,
    url: str,
    timeout: int,
    **kwargs,
) -> requests.Response:
    response = session.get(url, timeout=timeout, **kwargs)
    response.raise_for_status()
    return response


# ---------------------------------------------------------------------------
# HTML parser for landing-page PDF discovery
# ---------------------------------------------------------------------------
class PDFLinkHTMLParser(HTMLParser):
    MAX_CANDIDATES = 30

    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.candidates: List[Tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: Sequence[Tuple[str, Optional[str]]]) -> None:
        if len(self.candidates) >= self.MAX_CANDIDATES:
            return
        attr = {k.lower(): (v or "") for k, v in attrs}

        if tag.lower() == "meta":
            name = attr.get("name", "").strip().lower()
            prop = attr.get("property", "").strip().lower()
            content = attr.get("content", "").strip()
            if content:
                if name in META_PDF_NAMES:
                    self.candidates.append((urljoin(self.base_url, content), f"landing_meta:{name}"))
                elif prop in META_PDF_PROPERTIES:
                    self.candidates.append((urljoin(self.base_url, content), f"landing_meta:{prop}"))
                elif PDF_LIKE_RE.search(content):
                    self.candidates.append((urljoin(self.base_url, content), "landing_meta:pdf_like_content"))

        elif tag.lower() == "link":
            rel = attr.get("rel", "").lower()
            href = attr.get("href", "").strip()
            type_ = attr.get("type", "").lower()
            if href:
                absolute = urljoin(self.base_url, href)
                if "alternate" in rel and "application/pdf" in type_:
                    self.candidates.append((absolute, "landing_link:alternate_pdf"))
                elif "application/pdf" in type_ or PDF_LIKE_RE.search(absolute):
                    self.candidates.append((absolute, "landing_link:pdf_like"))

        elif tag.lower() == "a":
            href = attr.get("href", "").strip()
            if href:
                absolute = urljoin(self.base_url, href)
                if PDF_LIKE_RE.search(absolute):
                    self.candidates.append((absolute, "landing_anchor:pdf_like"))


# ---------------------------------------------------------------------------
# Text / identifier utilities
# ---------------------------------------------------------------------------
def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none"} else text


def safe_filename(text: str, max_len: int = 150) -> str:
    text = re.sub(r"[\\/:*?\"<>|]+", "_", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = text[:max_len].rstrip(" .")
    return text or "article"


def normalize_pmcid(value: str) -> str:
    value = clean_text(value).upper()
    if not value:
        return ""
    value = value.replace("PMCID:", "").replace("PMC", "")
    value = re.sub(r"\D", "", value)
    return f"PMC{value}" if value else ""


def normalize_pmid(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    return re.sub(r"\D", "", value)


def extract_doi(text: str) -> str:
    text = clean_text(text)
    if not text:
        return ""
    for prefix in (
        "https://doi.org/", "http://doi.org/",
        "https://dx.doi.org/", "http://dx.doi.org/",
        "doi:",
    ):
        text = text.replace(prefix, "")
    text = text.strip()
    match = DOI_REGEX.search(text)
    # Return "" when no valid DOI pattern found — avoids sending raw citation
    # text as a fake DOI to every upstream API.
    return match.group(0).rstrip(".);,") if match else ""


def detect_column(fieldnames: List[str], preferred: str, candidates: List[str]) -> str:
    if preferred and preferred in fieldnames:
        return preferred
    lower_map = {name.lower(): name for name in fieldnames}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return ""


def iter_rows(csv_path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return (reader.fieldnames or [], rows)


def unique_preserve_order(items: Iterable[Tuple[str, str]]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    seen: set = set()
    for url, note in items:
        if not url or url in seen:
            continue
        seen.add(url)
        out.append((url, note))
    return out


def choose_output_name(doi: str, pmcid: str, pmid: str, title: str, index: int) -> str:
    if doi:
        stem = safe_filename(doi.replace("/", "_"))
    elif pmcid:
        stem = safe_filename(pmcid)
    elif pmid:
        stem = safe_filename(f"PMID_{pmid}")
    elif title:
        stem = safe_filename(title)
    else:
        stem = f"article_{index:04d}"
    return f"{stem}.pdf"


# ---------------------------------------------------------------------------
# LAYER 1 — CORE LIBRARY: source-specific finders
# ---------------------------------------------------------------------------

def find_pmc_pdf(session: requests.Session, pmcid: str, timeout: int) -> Tuple[str, str]:
    if not pmcid:
        return "", "no_pmcid"
    oa_url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={quote(pmcid)}"
    try:
        resp = http_get(session, oa_url, timeout)
        root = ET.fromstring(resp.text)
    except Exception as exc:
        return "", f"pmc_lookup_error:{exc}"

    if root.find("error") is not None:
        return "", f"pmc_error:{clean_text(root.find('error').text)}"  # type: ignore[union-attr]

    for record in root.findall(".//record"):
        for link in record.findall("link"):
            if clean_text(link.attrib.get("format")).lower() == "pdf":
                href = clean_text(link.attrib.get("href"))
                if href.startswith("ftp://ftp.ncbi.nlm.nih.gov"):
                    href = href.replace("ftp://ftp.ncbi.nlm.nih.gov", "https://ftp.ncbi.nlm.nih.gov", 1)
                return href, "pmc_pdf_found"
    return "", "pmc_pdf_not_found"


def convert_to_pmcid(
    session: requests.Session,
    *,
    pmid: str = "",
    doi: str = "",
    timeout: int,
) -> Tuple[str, str]:
    """Try PMID first, fall back to DOI for PMCID conversion via NCBI ID Converter."""
    identifier = pmid or doi
    if not identifier:
        return "", "idconv_no_identifier"
    api_url = (
        "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
        f"?ids={quote(identifier, safe='')}&format=json"
    )
    try:
        resp = http_get(session, api_url, timeout)
        data = resp.json()
    except Exception as exc:
        return "", f"idconv_error:{exc}"

    for rec in (data.get("records", []) if isinstance(data, dict) else []):
        pmcid = normalize_pmcid(rec.get("pmcid", ""))
        if pmcid:
            return pmcid, "idconv_pmcid_found"
    return "", "idconv_pmcid_not_found"


def find_europepmc_pdf(
    session: requests.Session,
    doi: str,
    pmid: str,
    timeout: int,
) -> Tuple[str, str]:
    """Query the Europe PMC REST API for an open-access PDF link."""
    if doi:
        identifier = f"DOI:{doi}"
    elif pmid:
        identifier = f"EXT_ID:{pmid} AND SRC:MED"
    else:
        return "", "europepmc_no_identifier"

    api_url = (
        "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        f"?query={quote(identifier)}&resulttype=core&format=json&pageSize=1"
    )
    try:
        resp = http_get(session, api_url, timeout)
        data = resp.json()
    except Exception as exc:
        return "", f"europepmc_lookup_error:{exc}"

    results = (data.get("resultList") or {}).get("result") or []
    if not results:
        return "", "europepmc_no_results"

    article = results[0]
    for entry in (article.get("fullTextUrlList", {}).get("fullTextUrl") or []):
        if isinstance(entry, dict):
            doc_style = (entry.get("documentStyle") or "").lower()
            availability = (entry.get("availability") or "").lower()
            url = clean_text(entry.get("url"))
            if doc_style == "pdf" and url and availability in {"open", ""}:
                return url, "europepmc_pdf_found"

    # Fallback: derive PMCID from response and try PMC directly
    pmcid_val = normalize_pmcid(clean_text(article.get("pmcid")))
    if pmcid_val:
        return find_pmc_pdf(session, pmcid_val, timeout)

    return "", "europepmc_no_pdf_url"


def pick_unpaywall_pdf_url(data: Dict[str, object]) -> List[Tuple[str, str]]:
    """Return all deduped OA PDF candidates from an Unpaywall response."""
    candidates: List[Tuple[str, str]] = []
    best = data.get("best_oa_location") or {}
    if isinstance(best, dict):
        pdf = clean_text(best.get("url_for_pdf"))
        url = clean_text(best.get("url"))
        if pdf:
            candidates.append((pdf, "unpaywall_best_oa_pdf"))
        if url and PDF_LIKE_RE.search(url):
            candidates.append((url, "unpaywall_best_oa_url_pdf"))

    for loc in (data.get("oa_locations") or []):
        if not isinstance(loc, dict):
            continue
        pdf = clean_text(loc.get("url_for_pdf"))
        url = clean_text(loc.get("url"))
        if pdf:
            candidates.append((pdf, "unpaywall_oa_location_pdf"))
        if url and PDF_LIKE_RE.search(url):
            candidates.append((url, "unpaywall_oa_location_url_pdf"))

    return unique_preserve_order(candidates)


def find_unpaywall_pdf(
    session: requests.Session,
    doi: str,
    email: str,
    timeout: int,
) -> List[Tuple[str, str]]:
    """Return all OA PDF candidates from Unpaywall for a given DOI."""
    if not doi:
        return []
    if not email:
        return []
    api_url = f"https://api.unpaywall.org/v2/{quote(doi, safe='')}?email={quote(email)}"
    try:
        resp = http_get(session, api_url, timeout)
        data = resp.json()
    except Exception as exc:
        logger.debug("Unpaywall lookup failed for %s: %s", doi, exc)
        return []
    return pick_unpaywall_pdf_url(data)


def find_openalex_pdf(
    session: requests.Session,
    doi: str,
    pmid: str,
    api_key: str,
    timeout: int,
) -> Tuple[str, str]:
    """
    Query OpenAlex for an open-access PDF URL.

    Docs: https://docs.openalex.org/how-to-use-the-api/authentication
    Note: OpenAlex moved from a polite-pool email scheme to API key
    authentication. An api_key is required for reliable access.
    Free keys available at https://openalex.org/
    """
    if not api_key:
        return "", "openalex_no_api_key"
    if doi:
        work_id = f"https://doi.org/{doi}"
    elif pmid:
        work_id = f"pmid:{pmid}"
    else:
        return "", "openalex_no_identifier"

    params: Dict[str, str] = {
        "select": "id,open_access,best_oa_location,primary_location",
        "api_key": api_key,
    }

    api_url = f"https://api.openalex.org/works/{quote(work_id, safe=':/.')}"
    try:
        resp = http_get(session, api_url, timeout, params=params)
        data = resp.json()
    except Exception as exc:
        return "", f"openalex_lookup_error:{exc}"

    for loc_key in ("best_oa_location", "primary_location"):
        loc = data.get(loc_key) or {}
        if isinstance(loc, dict):
            pdf = clean_text(loc.get("pdf_url"))
            if pdf:
                return pdf, f"openalex_{loc_key}_pdf"

    return "", "openalex_no_pdf_url"


def find_semantic_scholar_pdf(
    session: requests.Session,
    doi: str,
    pmid: str,
    api_key: str,
    timeout: int,
) -> Tuple[str, str]:
    """Query Semantic Scholar for an open-access PDF URL. Docs: https://api.semanticscholar.org"""
    if doi:
        paper_id = f"DOI:{doi}"
    elif pmid:
        paper_id = f"PMID:{pmid}"
    else:
        return "", "s2_no_identifier"

    headers: Dict[str, str] = {}
    if api_key:
        headers["x-api-key"] = api_key

    api_url = f"https://api.semanticscholar.org/graph/v1/paper/{quote(paper_id, safe=':')}"
    try:
        resp = http_get(session, api_url, timeout,
                        params={"fields": "openAccessPdf"}, headers=headers)
        data = resp.json()
    except Exception as exc:
        return "", f"s2_lookup_error:{exc}"

    oa = data.get("openAccessPdf") or {}
    if isinstance(oa, dict):
        url = clean_text(oa.get("url"))
        if url:
            return url, "s2_open_access_pdf"

    return "", "s2_no_pdf_url"


def find_crossref_pdf(
    session: requests.Session,
    doi: str,
    email: str,
    timeout: int,
) -> List[Tuple[str, str]]:
    """Return all PDF link candidates from Crossref metadata for a given DOI."""
    if not doi:
        return []
    api_url = f"https://api.crossref.org/works/{quote(doi, safe='')}"
    params = {"mailto": email} if email else None
    try:
        resp = http_get(session, api_url, timeout, params=params)
        data = resp.json()
    except Exception as exc:
        logger.debug("Crossref lookup failed for %s: %s", doi, exc)
        return []

    message = (data.get("message") or {}) if isinstance(data, dict) else {}
    links = (message.get("link") or []) if isinstance(message, dict) else []
    candidates: List[Tuple[str, str]] = []

    for link in (links if isinstance(links, list) else []):
        if not isinstance(link, dict):
            continue
        url = clean_text(link.get("URL"))
        content_type = clean_text(link.get("content-type")).lower()
        app = clean_text(link.get("intended-application")).lower()
        if not url:
            continue
        if content_type == "application/pdf":
            candidates.append((url, f"crossref_link_pdf:{app or 'unknown'}"))
        elif PDF_LIKE_RE.search(url):
            candidates.append((url, f"crossref_link_pdf_like:{app or 'unknown'}"))

    return unique_preserve_order(candidates)



def get_landing_page_url(
    session: requests.Session,
    doi: str,
    timeout: int,
) -> Tuple[str, str]:
    """Resolve a DOI to its landing page URL, following redirects."""
    if not doi:
        return "", "no_doi"
    url = f"https://doi.org/{quote(doi, safe='/')}"
    try:
        resp = http_get(
            session, url, timeout,
            allow_redirects=True,
            headers={"Accept": "text/html,application/xhtml+xml"},
            stream=False,
        )
    except Exception as exc:
        return "", f"landing_resolve_error:{exc}"
    return str(resp.url), "landing_resolved"


def find_pdf_from_landing_page(
    session: requests.Session,
    landing_url: str,
    timeout: int,
) -> List[Tuple[str, str]]:
    """Return all PDF link candidates found in a DOI landing page."""
    if not landing_url:
        return []
    try:
        resp = http_get(
            session, landing_url, timeout,
            headers={"Accept": "text/html,application/xhtml+xml"},
        )
    except Exception as exc:
        logger.debug("Landing page fetch failed for %s: %s", landing_url, exc)
        return []

    content_type = clean_text(resp.headers.get("Content-Type")).lower()
    if resp.content[:5] == b"%PDF-":
        return [(str(resp.url), "landing_is_pdf")]

    parser = PDFLinkHTMLParser(str(resp.url))
    try:
        parser.feed(resp.text or "")
    except Exception:
        pass

    return unique_preserve_order(parser.candidates)


def _collect_pdf_candidates(
    session: requests.Session,
    *,
    doi: str,
    pmcid: str,
    pmid: str,
    title: str,
    cfg: DiscoveryConfig,
) -> Tuple[str, List[Tuple[str, str, str]]]:
    """
    Query all configured sources and return every candidate PDF URL found.

    Returns (resolved_pmcid, [(url, source, note), ...]) in priority order.
    The caller is responsible for trying each candidate download and stopping
    at the first success — this is the true multi-source cascade.

    The direct DOI fallback is intentionally excluded: doi.org resolves to a
    landing page, not a PDF, so including it generates spurious 'failed'
    results instead of honest 'not_found' ones.
    """
    candidates: List[Tuple[str, str, str]] = []
    resolved_pmcid = pmcid

    # 1. PMC
    if cfg.use_pmc:
        if not resolved_pmcid and (pmid or doi):
            converted, _ = convert_to_pmcid(session, pmid=pmid, doi=doi, timeout=cfg.timeout)
            if converted:
                resolved_pmcid = converted
        if resolved_pmcid:
            url, note = find_pmc_pdf(session, resolved_pmcid, cfg.timeout)
            if url:
                candidates.append((url, "PMC", note))

    # Resolve landing page once — reused by LandingPage source
    landing_url = ""
    if doi and cfg.use_landing_page:
        landing_url, _ = get_landing_page_url(session, doi, cfg.timeout)

    # 2. Europe PMC
    if cfg.use_europepmc and (doi or pmid):
        url, note = find_europepmc_pdf(session, doi, pmid, cfg.timeout)
        if url:
            candidates.append((url, "EuropePMC", note))

    # 3. Unpaywall — may return multiple OA locations
    if cfg.use_unpaywall and doi:
        for url, note in find_unpaywall_pdf(session, doi, cfg.email, cfg.timeout):
            candidates.append((url, "Unpaywall", note))

    # 4. OpenAlex
    if cfg.use_openalex and (doi or pmid):
        url, note = find_openalex_pdf(session, doi, pmid, cfg.openalex_api_key, cfg.timeout)
        if url:
            candidates.append((url, "OpenAlex", note))

    # 5. Semantic Scholar
    if cfg.use_semantic_scholar and (doi or pmid):
        url, note = find_semantic_scholar_pdf(session, doi, pmid, cfg.s2_api_key, cfg.timeout)
        if url:
            candidates.append((url, "SemanticScholar", note))

    # 6. Crossref — may return multiple PDF links in metadata
    if cfg.use_crossref and doi:
        for url, note in find_crossref_pdf(session, doi, cfg.email, cfg.timeout):
            candidates.append((url, "Crossref", note))

    # 7. Landing-page HTML scan — may find multiple PDF anchors
    if cfg.use_landing_page and landing_url:
        for url, note in find_pdf_from_landing_page(session, landing_url, cfg.timeout):
            candidates.append((url, "LandingPage", note))

    return resolved_pmcid, candidates


def discover_pdf(
    session: requests.Session,
    *,
    doi: str,
    pmcid: str,
    pmid: str,
    title: str,
    cfg: DiscoveryConfig,
) -> Tuple[str, str, str, str]:
    """
    Thin wrapper around _collect_pdf_candidates for backward compatibility.
    Returns (pdf_url, source, note, resolved_pmcid) for the first candidate only.
    Use retrieve_pdf() for full cascade with download retry across all candidates.
    """
    resolved_pmcid, candidates = _collect_pdf_candidates(
        session, doi=doi, pmcid=pmcid, pmid=pmid, title=title, cfg=cfg
    )
    if candidates:
        url, source, note = candidates[0]
        return url, source, note, resolved_pmcid
    return "", "", "no_pdf_url_found", resolved_pmcid


def download_file(
    session: requests.Session,
    url: str,
    output_path: Path,
    timeout: int,
    skip_existing: bool,
) -> Tuple[bool, str]:
    if skip_existing and output_path.exists() and output_path.stat().st_size > 0:
        return True, "already_exists"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Use a unique temp name so concurrent downloads of different rows that
    # resolve to the same output filename don't clobber each other's .part file.
    tmp_path = output_path.with_name(
        f"{output_path.stem}.{threading.get_ident()}.part"
    )

    try:
        with session.get(url, timeout=timeout, stream=True,
                         allow_redirects=True) as resp:
            resp.raise_for_status()
            first_chunk = next(resp.iter_content(chunk_size=8192), b"")
            content_type = clean_text(resp.headers.get("Content-Type")).lower()

            # Require %PDF- magic bytes regardless of Content-Type.
            # Some servers (e.g. JMIR) return HTML with HTTP 200 and an
            # ambiguous or missing Content-Type — trusting the header alone
            # produces corrupt "downloaded" files that are actually HTML.
            if not first_chunk.startswith(b"%PDF-"):
                return False, f"not_pdf_magic_bytes:{content_type or 'unknown'}"

            with tmp_path.open("wb") as f:
                if first_chunk:
                    f.write(first_chunk)
                for chunk in resp.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        f.write(chunk)

        if tmp_path.exists() and tmp_path.stat().st_size > 0:
            size = tmp_path.stat().st_size
            # Reject PDF stubs — valid articles are never smaller than 20 KB.
            # Some servers (e.g. JMIR) return a file with a valid %PDF- header
            # but near-empty content (~8 KB) that Adobe cannot open.
            if size < 20_000:
                tmp_path.unlink(missing_ok=True)
                return False, f"pdf_too_small:{size}_bytes"
            tmp_path.rename(output_path)
            return True, "downloaded"
        return False, "download_failed_empty"

    except requests.HTTPError as exc:
        return False, f"http_error:{exc}"
    except requests.RequestException as exc:
        return False, f"request_error:{exc}"
    except OSError as exc:
        return False, f"file_error:{exc}"
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


# ---------------------------------------------------------------------------
# LAYER 2 — AGENT INTERFACE: retrieve_pdf()
# ---------------------------------------------------------------------------

def retrieve_pdf(
    *,
    doi: str = "",
    pmid: str = "",
    pmcid: str = "",
    title: str = "",
    authors: str = "",
    outdir: Path = Path("pdfs"),
    email: str = "",
    s2_api_key: str = "",
    openalex_api_key: str = "",
    skip_existing: bool = True,
    timeout: int = DEFAULT_TIMEOUT,
    use_pmc: bool = True,
    use_europepmc: bool = True,
    use_unpaywall: bool = True,
    use_openalex: bool = False,
    use_semantic_scholar: bool = False,
    use_crossref: bool = True,
    use_landing_page: bool = True,
    session: Optional[requests.Session] = None,
) -> DownloadResult:
    """
    Programmatic entry point for agents, pipelines, and notebooks.

    Does NOT call sys.exit(). Does NOT print to stdout.
    Propagates only standard Python exceptions.

    Implements a true multi-source cascade: all sources are queried first to
    collect candidate URLs, then each is attempted for download in priority
    order until one succeeds. A source returning a URL that cannot be
    downloaded does not end the process — the next candidate is tried.

    Parameters
    ----------
    doi, pmid, pmcid : str
        At least one identifier should be provided. All are optional.
    title : str
        Used as the output filename stem when no identifier is available.
    outdir : Path
        Directory where the PDF will be saved.
    email : str
        Required by Unpaywall; also used for Crossref polite pool.
    openalex_api_key : str
        Required when use_openalex=True. Free key at https://openalex.org/
    s2_api_key : str
        Optional Semantic Scholar key (only relevant when use_semantic_scholar=True).
    skip_existing : bool
        If True, return status="skipped" when the output file already exists.
    timeout : int
        HTTP timeout in seconds per request.
    use_* : bool
        Toggle individual sources on/off.
    session : requests.Session, optional
        Provide a pre-built session to reuse connections across multiple calls
        (recommended for batch usage inside an agent loop).

    Returns
    -------
    DownloadResult
        result.ok        → True if PDF was saved successfully.
        result.status    → "downloaded" | "not_found" | "failed" | "skipped"
        result.pdf_path  → Path to saved file, or None.
        result.source    → Which source produced the successful download.
        result.note      → Diagnostic note for logging/debugging.

    Example
    -------
    >>> from open_pdf_downloader import retrieve_pdf
    >>> result = retrieve_pdf(doi="10.1186/s12889-019-6761-x", email="you@uni.edu")
    >>> if result.ok:
    ...     print(result.pdf_path)
    """
    doi = extract_doi(doi)
    pmcid_norm = normalize_pmcid(pmcid)
    pmid_norm = normalize_pmid(pmid)

    cfg = DiscoveryConfig(
        email=email,
        s2_api_key=s2_api_key,
        openalex_api_key=openalex_api_key,
        timeout=timeout,
        use_pmc=use_pmc,
        use_europepmc=use_europepmc,
        use_unpaywall=use_unpaywall,
        use_openalex=use_openalex,
        use_semantic_scholar=use_semantic_scholar,
        use_crossref=use_crossref,
        use_landing_page=use_landing_page,
    )

    _session = session or build_session(timeout)

    resolved_pmcid, candidates = _collect_pdf_candidates(
        _session,
        doi=doi,
        pmcid=pmcid_norm,
        pmid=pmid_norm,
        title=title,
        cfg=cfg,
    )

    if not candidates:
        return DownloadResult(
            doi=doi, pmid=pmid_norm, pmcid_original=pmcid_norm,
            pmcid_resolved=resolved_pmcid, title=title, authors=authors,
            status="not_found", source="",
            pdf_path=None, pdf_url="", note="no_pdf_url_found",
        )

    filename = choose_output_name(doi, resolved_pmcid, pmid_norm, title, 0)
    output_path = Path(outdir) / filename

    # True cascade: try each candidate URL until one downloads successfully.
    last_note = ""
    for pdf_url, source, discovery_note in candidates:
        ok, download_note = download_file(_session, pdf_url, output_path, timeout, skip_existing)

        if ok and download_note == "already_exists":
            return DownloadResult(
                doi=doi, pmid=pmid_norm, pmcid_original=pmcid_norm,
                pmcid_resolved=resolved_pmcid, title=title, authors=authors,
                status="skipped", source=source,
                pdf_path=output_path, pdf_url=pdf_url, note=download_note,
            )
        if ok:
            return DownloadResult(
                doi=doi, pmid=pmid_norm, pmcid_original=pmcid_norm,
                pmcid_resolved=resolved_pmcid, title=title, authors=authors,
                status="downloaded", source=source,
                pdf_path=output_path, pdf_url=pdf_url, note=download_note,
            )

        last_note = f"{source}:{download_note}"
        logger.debug("Candidate failed (%s), trying next source.", last_note)

    # All candidates tried and failed
    return DownloadResult(
        doi=doi, pmid=pmid_norm, pmcid_original=pmcid_norm,
        pmcid_resolved=resolved_pmcid, title=title, authors=authors,
        status="failed", source="",
        pdf_path=None, pdf_url="",
        note=f"all_candidates_failed:{last_note}",
    )


# ---------------------------------------------------------------------------
# LAYER 3 — CLI ENTRY POINT
# ---------------------------------------------------------------------------

def _process_row_cli(
    idx: int,
    row: Dict[str, str],
    *,
    doi_col: str,
    pmcid_col: str,
    pmid_col: str,
    citation_col: str,
    title_col: str,
    authors_col: str,
    outdir: Path,
    session: requests.Session,
    cfg: DiscoveryConfig,
    skip_existing: bool,
    total: int,
) -> Dict[str, str]:
    """Extract identifiers from a CSV row and call retrieve_pdf()."""
    title = clean_text(row.get(title_col, "")) if title_col else ""
    authors = clean_text(row.get(authors_col, "")) if authors_col else ""
    doi_raw = row.get(doi_col, "") if doi_col else ""
    if not doi_raw and citation_col:
        doi_raw = row.get(citation_col, "")

    result = retrieve_pdf(
        doi=doi_raw,
        pmid=row.get(pmid_col, "") if pmid_col else "",
        pmcid=row.get(pmcid_col, "") if pmcid_col else "",
        title=title,
        authors=authors,
        outdir=outdir,
        email=cfg.email,
        s2_api_key=cfg.s2_api_key,
        openalex_api_key=cfg.openalex_api_key,
        skip_existing=skip_existing,
        timeout=cfg.timeout,
        use_pmc=cfg.use_pmc,
        use_europepmc=cfg.use_europepmc,
        use_unpaywall=cfg.use_unpaywall,
        use_openalex=cfg.use_openalex,
        use_semantic_scholar=cfg.use_semantic_scholar,
        use_crossref=cfg.use_crossref,
        use_landing_page=cfg.use_landing_page,
        session=session,
    )

    label = result.doi or result.pmcid_resolved or result.pmid or f"row {idx}"
    logger.debug("[%d/%d] %s -> %s via %s (%s)", idx, total, label, result.status, result.source or "none", result.note)

    return {
        "row_index": str(idx),
        "title": result.title,
        "authors": result.authors,
        "pmid": result.pmid,
        "doi": result.doi,
        "pmcid_original": result.pmcid_original,
        "pmcid_resolved": result.pmcid_resolved,
        "source": result.source,
        "status": result.status,
        "pdf_url": result.pdf_url,
        "filename": result.pdf_path.name if result.pdf_path else "",
        "note": result.note,
    }


def main() -> int:
    args = parse_args()

    # Logs → stderr; JSON summary → stdout (agent-friendly separation)
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    csv_path = Path(args.input)
    outdir = Path(args.outdir)
    report_path = Path(args.report)

    if not csv_path.exists():
        logger.error("Input file not found: %s", csv_path)
        return 2

    fieldnames, rows = iter_rows(csv_path)
    if not rows:
        logger.error("CSV is empty or has no data rows.")
        return 2

    doi_col = detect_column(fieldnames, args.doi_column, ["DOI", "doi"])
    pmcid_col = detect_column(fieldnames, args.pmcid_column, ["PMCID", "pmcid", "pmc id", "PMC ID"])
    pmid_col = detect_column(fieldnames, args.pmid_column, ["PMID", "pmid", "pubmed id", "PubMed ID"])
    citation_col = args.citation_column if args.citation_column in fieldnames else ""
    title_col = args.title_column if args.title_column in fieldnames else ""
    authors_col = args.authors_column if args.authors_column in fieldnames else ""

    # Fix: citation_col is a valid identifier source — don't abort if it's present.
    if not doi_col and not pmcid_col and not pmid_col and not citation_col:
        logger.error(
            "Could not detect DOI, PMCID, PMID, or Citation columns. "
            "Use --doi-column / --pmcid-column / --pmid-column / --citation-column to specify them."
        )
        return 2

    if args.max and args.max > 0:
        rows = rows[: args.max]
        logger.info("Limiting to first %d rows.", args.max)

    workers = max(1, args.workers)

    cfg = DiscoveryConfig(
        email=args.email,
        s2_api_key=args.semantic_scholar_api_key,
        openalex_api_key=args.openalex_api_key,
        timeout=args.timeout,
        use_pmc=not args.no_pmc,
        use_europepmc=not args.no_europepmc,
        use_unpaywall=not args.no_unpaywall,
        use_openalex=args.openalex,
        use_semantic_scholar=args.semantic_scholar,
        use_crossref=not args.no_crossref,
        use_landing_page=not args.no_landing_page,
    )

    # Fail-fast: --openalex is a no-op without an API key — make it explicit.
    if cfg.use_openalex and not cfg.openalex_api_key:
        logger.error(
            "--openalex requires --openalex-api-key. "
            "Get a free key at https://openalex.org/"
        )
        return 2

    # Crossref polite pool is documented to allow max 3 concurrent connections.
    # Warn if the user exceeds this; we don't hard-cap to preserve user autonomy.
    if cfg.use_crossref and workers > CROSSREF_MAX_WORKERS:
        logger.warning(
            "Crossref polite pool recommends max %d concurrent connections. "
            "You requested --workers %d. Consider --no-crossref or reducing workers "
            "to avoid HTTP 429 errors from Crossref.",
            CROSSREF_MAX_WORKERS, workers,
        )

    total = len(rows)
    logger.info("Starting: %d records, %d worker(s).", total, workers)

    report_rows: List[Dict[str, str]] = [{}] * total
    success_count = failed_count = not_found_count = skipped_count = error_count = 0

    # Fix: use thread-local sessions so each worker thread has its own connection
    # pool. requests.Session is not guaranteed thread-safe when shared across threads.
    _thread_local = threading.local()

    def get_thread_session() -> requests.Session:
        if not hasattr(_thread_local, "session"):
            _thread_local.session = build_session(cfg.timeout)
        return _thread_local.session

    def process_row_with_local_session(idx: int, row: Dict[str, str]) -> Dict[str, str]:
        return _process_row_cli(
            idx, row,
            doi_col=doi_col, pmcid_col=pmcid_col, pmid_col=pmid_col,
            citation_col=citation_col, title_col=title_col, authors_col=authors_col,
            outdir=outdir, session=get_thread_session(), cfg=cfg,
            skip_existing=args.skip_existing, total=total,
        )

    if workers == 1:
        session = build_session(args.timeout)
        for idx, row in enumerate(rows, start=1):
            report_rows[idx - 1] = _process_row_cli(
                idx, row,
                doi_col=doi_col, pmcid_col=pmcid_col, pmid_col=pmid_col,
                citation_col=citation_col, title_col=title_col, authors_col=authors_col,
                outdir=outdir, session=session, cfg=cfg,
                skip_existing=args.skip_existing, total=total,
            )
            if args.delay > 0 and idx < total:
                time.sleep(args.delay)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit jobs one at a time with delay between submissions so
            # --delay actually throttles outgoing traffic, not result collection.
            futures: Dict = {}
            for idx, row in enumerate(rows, start=1):
                future = executor.submit(process_row_with_local_session, idx, row)
                futures[future] = idx
                if args.delay > 0 and idx < total:
                    time.sleep(args.delay)

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    report_rows[idx - 1] = future.result()
                except Exception as exc:
                    logger.error("Row %d raised an unexpected error: %s", idx, exc)
                    report_rows[idx - 1] = {
                        "row_index": str(idx), "status": "error", "note": str(exc),
                        **{k: "" for k in REPORT_FIELDNAMES if k not in ("row_index", "status", "note")},
                    }

    # Fix: count "error" separately — don't fold it into not_found.
    for r in report_rows:
        s = r.get("status", "")
        if s == "downloaded":
            success_count += 1
        elif s == "failed":
            failed_count += 1
        elif s == "skipped":
            skipped_count += 1
        elif s == "error":
            error_count += 1
        else:
            not_found_count += 1

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(report_rows)

    logger.info(
        "Done. downloaded=%d skipped=%d failed=%d not_found=%d errors=%d",
        success_count, skipped_count, failed_count, not_found_count, error_count,
    )

    # stdout carries only the machine-readable JSON summary
    summary = {
        "tool_version": TOOL_VERSION,
        "input": str(csv_path),
        "rows_processed": total,
        "downloads_ok": success_count,
        "downloads_skipped": skipped_count,
        "downloads_failed": failed_count,
        "not_found": not_found_count,
        "errors": error_count,
        "outdir": str(outdir),
        "report": str(report_path),
        "columns": {
            k: v for k, v in {
                "doi": doi_col,
                "pmid": pmid_col,
                "pmcid": pmcid_col,
                "doi_fallback_column": citation_col,
            }.items() if v
        },
        "sources_enabled": {
            "pmc": cfg.use_pmc,
            "europepmc": cfg.use_europepmc,
            "unpaywall": cfg.use_unpaywall,
            "openalex": cfg.use_openalex,
            "semantic_scholar": cfg.use_semantic_scholar,
            "crossref": cfg.use_crossref,
            "landing_page": cfg.use_landing_page,
        },
        "workers": workers,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())