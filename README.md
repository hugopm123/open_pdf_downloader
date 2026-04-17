# open_pdf_downloader
**Multi-source Open Access PDF retrieval for Literature Reviews and AI research agents**

Facilitates the mass retrieval of open-access (OA) PDF documents to support rigorous systematic literature review methodologies such as PRISMA. Designed to minimize technical friction: works out-of-the-box with no API keys required, supports concurrent processing, and respects publisher and repository policies.

Can be used as a **CLI** or imported directly as a **tool** inside agents, notebooks, and automated pipelines.

---

## 🚀 Key Features

- **Zero-configuration:** Ready to run with free sources — no registration or API keys required.
- **Dual interface:** CLI for batch CSV workflows; `retrieve_pdf()` function for programmatic and agentic use.
- **True multi-source cascade:** Collects candidate URLs from all sources, then tries each candidate URL in priority order — including all OA locations from Unpaywall, all PDF links from Crossref, and all anchors from landing pages.
- **Concurrent processing:** Parallel downloads via `--workers` with a thread-local session per worker.
- **Resilience:** HTTP adapter with automatic retries and exponential backoff on failures and rate limits.
- **Safe writes:** Downloads to a `.part` temporary file and renames on success — no corrupt files left on disk.
- **Agent-friendly output:** JSON summary to `stdout`; diagnostic logs to `stderr` (clean separation for pipelines).
- **Web UI:** Streamlit interface for non-technical users — upload CSV, paste DOIs, monitor progress, download PDFs as ZIP.

## ⚡ Quick Start

```bash
python open_pdf_downloader.py --input examples/literature.csv
```

That's it. PDFs are saved to `pdfs/` and a report to `download_report.csv`.

Add your email to unlock Unpaywall and improve Crossref rate limits:

```bash
python open_pdf_downloader.py --input examples/literature.csv --email you@university.edu
```

---

## 📚 Sources

The tool attempts sources in the following priority order:

| # | Source | Requires | Notes |
| :--- | :--- | :--- | :--- |
| 1 | **PubMed Central (PMC)** | Nothing | NCBI OA API; auto-converts PMID/DOI to PMCID. |
| 2 | **Europe PMC** | Nothing | EBI REST API; strong coverage of biomedical literature. |
| 3 | **Unpaywall** | `--email` (required) | Per their Terms of Service. |
| 4 | **OpenAlex** | `--openalex-api-key` (required) | **Disabled by default.** Enable with `--openalex`. Free key at [openalex.org](https://openalex.org/). |
| 5 | **Semantic Scholar** | Nothing | **Disabled by default.** Enable with `--semantic-scholar`. Useful for CS/engineering literature. |
| 6 | **Crossref** | `--email` (optional) | Searches for PDF links within publisher metadata. Max 3 concurrent workers recommended. |
| 7 | **Landing Page** | Nothing | Scans DOI landing pages for meta-tags and `<link rel="alternate">` anchors. |

---

## 🛠 Installation

Clone the repository and install the dependencies. A virtual environment is recommended.

```bash
git clone https://github.com/pecesama/open_pdf_downloader.git
cd open_pdf_downloader
pip install -r requirements.txt
```

**requirements.txt**
```
requests>=2.28
urllib3>=1.26.0
```

### Web UI (Streamlit)

For non-technical users, a browser-based interface is available in the `app/` folder.

```bash
pip install -r app/requirements_app.txt
streamlit run app/streamlit_app.py
```

Open http://localhost:8501 in your browser. No command line knowledge required.

---

## 💡 CLI Usage

The `/examples/` folder contains a sample `literature.csv` to get started quickly.

**Note:** The `literature.csv` file uses a minimal, source-agnostic format compatible with exports from any academic database (PubMed, Scopus, Web of Science, Zotero, Mendeley, etc.):

| Column | Required | Notes |
| :--- | :--- | :--- |
| `Title` | Yes | Article title; used as filename fallback. |
| `Authors` | Yes | Author list. |
| `DOI` | Yes* | Primary identifier for most sources. |
| `PMID` | No | Enables PMC and Europe PMC lookup. |
| `PMCID` | No | Skips ID conversion when already known. |

\* At least one of `DOI`, `PMID`, or `PMCID` must be present per row.

The `/examples/` folder also includes a `sample_download_report.csv` showing the output format for representative cases: a successful PMC download, an Unpaywall result, an EuropePMC result, and a `not_found` article. Useful for understanding the report schema before running the tool on your own dataset.

**1. Basic use — no configuration needed**
```bash
python open_pdf_downloader.py --input examples/literature.csv
```

**2. Recommended use — unlock Unpaywall and Crossref polite pool**
```bash
python open_pdf_downloader.py --input examples/literature.csv --email you@university.edu
```

**3. Concurrent downloads**
Use multiple workers to speed up large batches. Keep `--workers` at 3 or below when Crossref is enabled to respect their polite pool limit:
```bash
python open_pdf_downloader.py --input examples/literature.csv --email you@university.edu --workers 3 --outdir prisma_downloads
```

**4. Custom column mapping**
For CSV exports from reference managers (Zotero, Mendeley, EndNote) with non-standard column names:
```bash
python open_pdf_downloader.py --input export.csv --doi-column "Digital Object Identifier" --pmid-column "PubMed ID"
```

**5. Enable opt-in sources**
Semantic Scholar is useful for CS, engineering, or interdisciplinary corpora. OpenAlex requires a free API key:
```bash
python open_pdf_downloader.py --input examples/literature.csv --email you@university.edu \
  --semantic-scholar \
  --openalex --openalex-api-key "YOUR_KEY"
```

**6. Resume an interrupted run**
Skip articles whose PDF was already downloaded:
```bash
python open_pdf_downloader.py --input examples/literature.csv --email you@university.edu --skip-existing
```

---

## 🤖 Programmatic / Agent Usage

Import `retrieve_pdf()` directly for use in agents, LLM pipelines, or notebooks.
It returns a typed `DownloadResult` — no `sys.exit`, no prints to `stdout`.

```python
from open_pdf_downloader import retrieve_pdf, build_session
from pathlib import Path

# Reuse a single session across calls for efficiency
session = build_session(timeout=30)

result = retrieve_pdf(
    doi="10.3390/sym17122083",
    email="you@university.edu",
    outdir=Path("corpus"),
    session=session,
)

if result.ok:
    print(f"Saved to: {result.pdf_path}")
elif result.status == "not_found":
    print(f"No OA version found — {result.note}")
else:
    print(f"Download failed ({result.source}): {result.note}")
```

**`DownloadResult` fields:**

| Field | Type | Description |
| :--- | :--- | :--- |
| `ok` | `bool` | `True` if the PDF was saved successfully. |
| `status` | `str` | `"downloaded"` · `"not_found"` · `"failed"` · `"skipped"` |
| `source` | `str` | Source that produced the successful download (e.g. `"PMC"`, `"Unpaywall"`, `"EuropePMC"`). |
| `pdf_path` | `Path \| None` | Path to the saved file, or `None`. |
| `pdf_url` | `str` | URL used for the download attempt. |
| `note` | `str` | Diagnostic string for logging and debugging. |

**Agent loop example:**
```python
papers = [
    {"doi": "10.3390/virtualworlds4040056", "title": "Extended Reality in Computer Science Education: A Narrative Review of Pedagogical Benefits, Challenges, and Future Directions"},
    {"doi": "10.3390/app15158679", "title": "Machine Learning and Generative AI in Learning Analytics for Higher Education: A Systematic Review of Models, Trends, and Challenges"},
]

for paper in papers:
    result = retrieve_pdf(**paper, email="you@university.edu", session=session)
    if result.ok:
        ingest_to_vector_store(result.pdf_path)   # next step in your pipeline
    elif result.status == "not_found":
        flag_for_manual_review(paper)
```

---

## 🖥️ Web UI

A Streamlit interface is included for researchers who prefer not to use the command line.

```bash
streamlit run app/streamlit_app.py
```

**Features:**
- Upload a CSV or paste DOIs directly
- Download the CSV template
- Toggle sources on/off and configure credentials from the sidebar
- Live progress table updates as each article is processed
- Download the full results report as CSV
- Download all retrieved PDFs as a single ZIP file

---

## ⚙️ All CLI Options

```
--input                    Path to CSV input (required)
--outdir                   Output directory for PDFs (default: pdfs)
--report                   Path to CSV report file (default: download_report.csv)
--email                    Email for Unpaywall (required) and Crossref polite pool (optional)
--openalex-api-key         API key for OpenAlex (free at openalex.org; required with --openalex)
--semantic-scholar-api-key Optional API key to increase Semantic Scholar rate limits
--doi-column               Override auto-detected DOI column name
--pmcid-column             Override auto-detected PMCID column name
--pmid-column              Override auto-detected PMID column name
--citation-column          Column used as DOI fallback (default: Citation)
--title-column             Column used for filenames and logs (default: Title)
--max                      Process only the first N rows
--delay                    Seconds between record submissions (default: 0.5; applies per-submission in parallel mode)
--timeout                  HTTP timeout in seconds (default: 30)
--workers                  Parallel download threads (default: 1; max 3 recommended with Crossref)
--skip-existing            Skip articles whose PDF already exists
--openalex                 Enable OpenAlex source (disabled by default). Running with --openalex without --openalex-api-key exits immediately with an error.
--semantic-scholar         Enable Semantic Scholar source (disabled by default)
--no-pmc                   Disable PubMed Central
--no-europepmc             Disable Europe PMC
--no-unpaywall             Disable Unpaywall
--no-crossref              Disable Crossref
--no-landing-page          Disable landing-page scan
--verbose                  Print per-record progress to stderr
```

---

## 📄 Output

**PDF files** are saved to `--outdir` (default: `pdfs/`), named by DOI, PMCID, PMID, or title.

**CSV report** (`download_report.csv`) contains one row per article:

| Column | Description |
| :--- | :--- |
| `row_index` | Position in the input CSV |
| `title` | Article title |
| `doi` / `pmid` / `pmcid_original` / `pmcid_resolved` | Identifiers |
| `status` | `downloaded` · `failed` · `not_found` · `skipped` |
| `source` | Which source succeeded |
| `pdf_url` | URL attempted |
| `filename` | Saved filename |
| `note` | Diagnostic detail. Common values: `downloaded`, `already_exists`, `pdf_too_small:{N}_bytes` (stub PDF rejected), `not_pdf_magic_bytes:{content-type}` (HTML served as PDF), `http_error:403` (paywall), `all_candidates_failed:{source}:{reason}` |

**JSON summary** is printed to `stdout` on completion:
```json
{
  "tool_version": "1.0",
  "rows_processed": 65,
  "downloads_ok": 52,
  "downloads_skipped": 0,
  "downloads_failed": 3,
  "not_found": 9,
  "errors": 1,
  "sources_enabled": {
    "pmc": true, "europepmc": true,
    "unpaywall": true, "openalex": false, "semantic_scholar": false,
    "crossref": true, "landing_page": true
  }
}
```

---

## ⚖️ Responsible Use Policy

- **No paywall bypassing:** This tool does **not** circumvent paywalls or use unauthorized methods to obtain documents. It locates only legal open-access versions (preprints, institutional repositories, OA journals).
- **Polite traffic:** A delay between requests is applied by default. If you increase `--workers`, keep it at 3 or below when Crossref is enabled to avoid HTTP 429 responses.
- **Email usage:** Providing `--email` is strongly encouraged for Unpaywall (required) and Crossref (improves rate limits).

---

## 👨‍💻 Author

Developed by **Pedro C. Santana-Mancilla** — [pedrosantana.mx](https://www.pedrosantana.mx/)  
As part of his own automation and research tools efforts at the [IHCLab Research Group](https://ihclab.ucol.mx/)
School of Telematics, Universidad de Colima.