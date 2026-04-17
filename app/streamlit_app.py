"""
open_pdf_downloader — Streamlit UI
Provides a web interface for non-technical users.

Run from the repo root:
    streamlit run app/streamlit_app.py
"""

import io
import sys
import zipfile
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------------------------
# Path setup — allow running from /app subfolder or repo root
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from open_pdf_downloader import DownloadResult, build_session, retrieve_pdf, TOOL_VERSION
except ImportError:
    st.error(
        "Could not import `open_pdf_downloader`. "
        "Make sure `open_pdf_downloader.py` is in the repo root and you are "
        "running this app from the repo root:\n\n"
        "```\nstreamlit run app/streamlit_app.py\n```"
    )
    st.stop()

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="open_pdf_downloader",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');

    :root {
        --navy:   #0f1f38;
        --slate:  #1e3456;
        --blue:   #2563eb;
        --amber:  #f59e0b;
        --green:  #10b981;
        --red:    #ef4444;
        --muted:  #64748b;
        --border: #e2e8f0;
        --bg:     #f8fafc;
        --white:  #ffffff;
    }

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
        background-color: var(--bg);
        color: var(--navy);
    }

    /* Header */
    .app-header {
        background: linear-gradient(135deg, var(--navy) 0%, var(--slate) 100%);
        color: var(--white);
        padding: 2.5rem 2rem 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        position: relative;
        overflow: hidden;
    }
    .app-header::before {
        content: '';
        position: absolute;
        top: -40px; right: -40px;
        width: 200px; height: 200px;
        background: radial-gradient(circle, rgba(245,158,11,0.15) 0%, transparent 70%);
        border-radius: 50%;
    }
    .app-header h1 {
        font-family: 'DM Serif Display', serif;
        font-size: 2.2rem;
        margin: 0 0 0.4rem;
        letter-spacing: -0.02em;
    }
    .app-header p {
        font-size: 0.95rem;
        opacity: 0.75;
        margin: 0;
        font-weight: 300;
    }
    .version-badge {
        display: inline-block;
        background: rgba(245,158,11,0.25);
        color: var(--amber);
        font-family: 'DM Mono', monospace;
        font-size: 0.7rem;
        padding: 2px 8px;
        border-radius: 20px;
        margin-left: 10px;
        vertical-align: middle;
    }

    /* Section headings */
    .section-label {
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--muted);
        margin-bottom: 0.5rem;
    }

    /* Metric cards */
    .metric-row {
        display: flex;
        gap: 1rem;
        margin: 1.5rem 0;
    }
    .metric-card {
        flex: 1;
        background: var(--white);
        border-radius: 10px;
        padding: 1.2rem 1.4rem;
        border: 1px solid var(--border);
        text-align: center;
    }
    .metric-card .metric-value {
        font-family: 'DM Serif Display', serif;
        font-size: 2.4rem;
        line-height: 1;
        margin-bottom: 0.3rem;
    }
    .metric-card .metric-label {
        font-size: 0.75rem;
        font-weight: 500;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        color: var(--muted);
    }
    .metric-ok    .metric-value { color: var(--green); }
    .metric-fail  .metric-value { color: var(--red);   }
    .metric-miss  .metric-value { color: var(--muted); }
    .metric-total .metric-value { color: var(--navy);  }

    /* Policy box */
    .policy-box {
        background: #fffbeb;
        border: 1px solid #fde68a;
        border-radius: 8px;
        padding: 0.9rem 1.1rem;
        font-size: 0.82rem;
        color: #92400e;
        margin-bottom: 1rem;
    }
    .policy-box strong { color: #78350f; }

    /* Download buttons row */
    .dl-row { display: flex; gap: 0.8rem; margin-top: 1rem; }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: var(--white);
        border-right: 1px solid var(--border);
    }
    section[data-testid="stSidebar"] .sidebar-title {
        font-family: 'DM Serif Display', serif;
        font-size: 1.1rem;
        color: var(--navy);
        margin-bottom: 1rem;
    }

    /* Tab styling */
    div[data-baseweb="tab-list"] {
        gap: 4px;
        background: var(--bg) !important;
        border-bottom: 2px solid var(--border);
        padding-bottom: 0;
    }
    div[data-baseweb="tab"] {
        font-weight: 500;
        font-size: 0.88rem;
        color: var(--muted) !important;
    }

    /* Progress table */
    .stDataFrame { border-radius: 8px; overflow: hidden; }

    /* Hide Streamlit footer */
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TEMPLATE_CSV = "Title,Authors,DOI,PMID,PMCID\n"

SOURCE_LABELS = {
    "pmc":              "PubMed Central (PMC)",
    "europepmc":        "Europe PMC",
    "unpaywall":        "Unpaywall",
    "openalex":         "OpenAlex",
    "semantic_scholar": "Semantic Scholar",
    "crossref":         "Crossref",
    "landing_page":     "Landing Page scan",
}

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def parse_doi_text(text: str) -> list[str]:
    """Extract one DOI per non-empty line from a text area."""
    return [ln.strip() for ln in text.strip().splitlines() if ln.strip()]


def results_to_csv_bytes(results: list[DownloadResult]) -> bytes:
    import csv, io
    fieldnames = ["title", "authors", "doi", "pmid", "pmcid_resolved",
                  "source", "status", "pdf_url", "filename", "note"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    for r in results:
        w.writerow({
            "title":         r.title,
            "authors":       r.authors,
            "doi":           r.doi,
            "pmid":          r.pmid,
            "pmcid_resolved": r.pmcid_resolved,
            "source":        r.source,
            "status":        r.status,
            "pdf_url":       r.pdf_url,
            "filename":      r.pdf_path.name if r.pdf_path else "",
            "note":          r.note,
        })
    return buf.getvalue().encode("utf-8")


def results_to_zip_bytes(results: list[DownloadResult]) -> bytes:
    buf = io.BytesIO()
    written = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r in results:
            if r.pdf_path and r.pdf_path.exists():
                zf.write(r.pdf_path, r.pdf_path.name)
                written += 1
    buf.seek(0)
    return buf.read() if written > 0 else b""


def status_icon(status: str) -> str:
    return {"downloaded": "✅", "skipped": "⏭️", "failed": "❌",
            "not_found": "🔍", "error": "⚠️"}.get(status, "❓")


# ---------------------------------------------------------------------------
# Sidebar — configuration
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown('<p class="sidebar-title">⚙️ Configuration</p>', unsafe_allow_html=True)

    st.markdown('<p class="section-label">Credentials</p>', unsafe_allow_html=True)
    email = st.text_input(
        "Email address",
        placeholder="you@institution.edu",
        help="Required by Unpaywall. Also improves Crossref rate limits.",
    )
    openalex_key = st.text_input(
        "OpenAlex API key",
        type="password",
        placeholder="Optional — free at openalex.org",
        help="Required to enable OpenAlex as a source.",
    )
    s2_key = st.text_input(
        "Semantic Scholar API key",
        type="password",
        placeholder="Optional — raises rate limits",
    )

    st.divider()
    st.markdown('<p class="section-label">Sources</p>', unsafe_allow_html=True)

    source_defaults = {
        "pmc": True, "europepmc": True, "unpaywall": True,
        "crossref": True, "landing_page": True,
        "openalex": False, "semantic_scholar": False,
    }
    source_enabled = {}
    for key, label in SOURCE_LABELS.items():
        default = source_defaults[key]
        disabled = False
        hint = ""
        if key == "unpaywall" and not email:
            hint = " (needs email)"
        if key == "openalex" and not openalex_key:
            hint = " (needs API key)"
            disabled = True
        source_enabled[key] = st.checkbox(
            f"{label}{hint}", value=default and not disabled, disabled=disabled
        )

    st.divider()
    st.markdown('<p class="section-label">Options</p>', unsafe_allow_html=True)
    skip_existing = st.checkbox("Skip already-downloaded PDFs", value=True)
    timeout = st.slider("Request timeout (s)", 10, 60, 30)
    delay = st.slider("Delay between requests (s)", 0.0, 3.0, 0.5, 0.1)


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown(f"""
<div class="app-header">
    <h1>📄 open_pdf_downloader <span class="version-badge">v{TOOL_VERSION}</span></h1>
    <p>Retrieve open-access PDFs from multiple legal sources — no paywall bypassing.</p>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Responsible use notice
# ---------------------------------------------------------------------------
st.markdown("""
<div class="policy-box">
    <strong>Responsible use:</strong> This tool only retrieves legally open-access versions
    (preprints, institutional repositories, OA journals). It does <strong>not</strong> bypass
    paywalls. Please provide your institutional email to respect polite-pool API policies.
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Input — two tabs
# ---------------------------------------------------------------------------
tab_csv, tab_doi = st.tabs(["📂  Upload CSV", "✏️  Paste DOIs"])

articles: list[dict] = []   # list of {title, authors, doi, pmid, pmcid}

with tab_csv:
    col_upload, col_template = st.columns([3, 1])
    with col_upload:
        uploaded = st.file_uploader(
            "Upload your literature CSV",
            type=["csv"],
            help="Columns: Title, Authors, DOI (required) · PMID, PMCID (optional)",
        )
    with col_template:
        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            "⬇️ Download template",
            data=TEMPLATE_CSV,
            file_name="literature_template.csv",
            mime="text/csv",
            use_container_width=True,
        )

    if uploaded:
        import csv, io as _io
        try:
            text = uploaded.read().decode("utf-8-sig", errors="replace")
            reader = csv.DictReader(_io.StringIO(text))
            fieldnames_lower = {f.strip().lower(): f.strip() for f in (reader.fieldnames or [])}
            for row in reader:
                def col(candidates):
                    for c in candidates:
                        if c.lower() in fieldnames_lower:
                            return (row.get(fieldnames_lower[c.lower()]) or "").strip()
                    return ""
                articles.append({
                    "title":   col(["Title", "title"]),
                    "authors": col(["Authors", "authors"]),
                    "doi":     col(["DOI", "doi"]),
                    "pmid":    col(["PMID", "pmid"]),
                    "pmcid":   col(["PMCID", "pmcid"]),
                })
            st.success(f"Loaded **{len(articles)}** records from CSV.")
        except Exception as exc:
            st.error(f"Could not parse CSV: {exc}")

with tab_doi:
    doi_text = st.text_area(
        "Paste DOIs — one per line",
        placeholder="10.1186/s12889-019-6761-x\n10.2196/64372\n10.1371/journal.pone.0210569",
        height=180,
    )
    if doi_text.strip():
        dois = parse_doi_text(doi_text)
        for d in dois:
            articles.append({"title": "", "authors": "", "doi": d, "pmid": "", "pmcid": ""})
        st.caption(f"{len(dois)} DOI(s) ready.")

# ---------------------------------------------------------------------------
# Run button
# ---------------------------------------------------------------------------
st.markdown("<br>", unsafe_allow_html=True)

run_disabled = len(articles) == 0
if run_disabled:
    st.info("Upload a CSV or paste DOIs to get started.", icon="👆")

run_clicked = st.button(
    f"🚀  Download PDFs  ({len(articles)} articles)" if articles else "🚀  Download PDFs",
    type="primary",
    disabled=run_disabled,
    use_container_width=True,
)

# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
if run_clicked and articles:

    outdir = Path("pdfs")
    outdir.mkdir(exist_ok=True)

    session = build_session(timeout=timeout)
    results: list[DownloadResult] = []

    # Live progress area
    progress_bar   = st.progress(0, text="Starting…")
    status_area    = st.empty()
    live_table_area = st.empty()

    live_rows = []

    for i, art in enumerate(articles, start=1):
        label = art["doi"] or art["pmid"] or art["title"] or f"row {i}"
        progress_bar.progress(i / len(articles), text=f"Processing {i}/{len(articles)}: {label[:60]}")

        result = retrieve_pdf(
            doi=art["doi"],
            pmid=art["pmid"],
            pmcid=art["pmcid"],
            title=art["title"],
            authors=art["authors"],
            outdir=outdir,
            email=email,
            openalex_api_key=openalex_key,
            s2_api_key=s2_key,
            skip_existing=skip_existing,
            timeout=timeout,
            use_pmc=source_enabled["pmc"],
            use_europepmc=source_enabled["europepmc"],
            use_unpaywall=source_enabled["unpaywall"] and bool(email),
            use_openalex=source_enabled["openalex"] and bool(openalex_key),
            use_semantic_scholar=source_enabled["semantic_scholar"],
            use_crossref=source_enabled["crossref"],
            use_landing_page=source_enabled["landing_page"],
            session=session,
        )
        results.append(result)

        live_rows.append({
            "":        status_icon(result.status),
            "Title":   (result.title or label)[:60],
            "Source":  result.source or "—",
            "Status":  result.status,
            "Note":    result.note[:60] if result.note else "",
        })
        live_table_area.dataframe(live_rows, use_container_width=True, height=min(400, 42 + 35 * len(live_rows)))

        import time as _time
        if delay > 0 and i < len(articles):
            _time.sleep(delay)

    progress_bar.empty()
    status_area.empty()

    # -------------------------------------------------------------------------
    # Summary metrics
    # -------------------------------------------------------------------------
    n_ok      = sum(1 for r in results if r.status == "downloaded")
    n_skip    = sum(1 for r in results if r.status == "skipped")
    n_fail    = sum(1 for r in results if r.status == "failed")
    n_miss    = sum(1 for r in results if r.status == "not_found")
    n_total   = len(results)

    st.markdown(f"""
    <div class="metric-row">
        <div class="metric-card metric-total">
            <div class="metric-value">{n_total}</div>
            <div class="metric-label">Total</div>
        </div>
        <div class="metric-card metric-ok">
            <div class="metric-value">{n_ok + n_skip}</div>
            <div class="metric-label">Retrieved</div>
        </div>
        <div class="metric-card metric-fail">
            <div class="metric-value">{n_fail}</div>
            <div class="metric-label">Failed</div>
        </div>
        <div class="metric-card metric-miss">
            <div class="metric-value">{n_miss}</div>
            <div class="metric-label">Not Found</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # -------------------------------------------------------------------------
    # Download buttons
    # -------------------------------------------------------------------------
    report_bytes = results_to_csv_bytes(results)
    zip_bytes    = results_to_zip_bytes(results)

    dl_col1, dl_col2, dl_col3 = st.columns(3)
    with dl_col1:
        st.download_button(
            "📋  Download report CSV",
            data=report_bytes,
            file_name="download_report.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with dl_col2:
        if zip_bytes:
            st.download_button(
                f"📦  Download PDFs as ZIP ({n_ok + n_skip} files)",
                data=zip_bytes,
                file_name="pdfs.zip",
                mime="application/zip",
                use_container_width=True,
            )
        else:
            st.button("📦  No PDFs to download", disabled=True, use_container_width=True)
    with dl_col3:
        st.link_button(
            "⭐  View on GitHub",
            url="https://github.com/pecesama/open_pdf_downloader",
            use_container_width=True,
        )

    # -------------------------------------------------------------------------
    # Full results table
    # -------------------------------------------------------------------------
    with st.expander("📊 Full results table", expanded=False):
        import pandas as pd
        df = pd.DataFrame([{
            "Status":   status_icon(r.status) + " " + r.status,
            "Title":    r.title or r.doi or "—",
            "Authors":  r.authors or "—",
            "DOI":      r.doi or "—",
            "Source":   r.source or "—",
            "PDF URL":  r.pdf_url or "—",
            "Note":     r.note or "—",
        } for r in results])
        st.dataframe(df, use_container_width=True)
