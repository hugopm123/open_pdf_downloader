# Changelog

All notable changes to this project will be documented in this file.

---

## [1.0.1] – Fixes

### Fixed
- `UnicodeDecodeError` on CSV files with non-UTF-8 encoding (Latin-1, Windows-1252); `errors="replace"` now applied on read.
- PDF stub detection: files smaller than 20 KB are rejected even when `%PDF-` magic bytes are present — prevents corrupt downloads from servers that return near-empty PDFs with valid headers.
- Strict `%PDF-` magic byte validation in `download_file` and `find_pdf_from_landing_page`; `Content-Type` header alone is no longer trusted.
- Retry adapter no longer retries DNS failures or SSL certificate errors (`connect=0`); only transient HTTP status codes and read timeouts are retried.
- `_process_row_cli` signature restored after refactor (`outdir`, `session`, `cfg`, `skip_existing` params were inadvertently dropped).

### Added
- `authors` field in `DownloadResult` and CSV report — populated from the `Authors` column in the input CSV.
- `--authors-column` CLI flag (default: `Authors`).
- `app/streamlit_app.py` — browser-based UI for non-technical users: CSV upload, DOI paste, live progress table, ZIP download of PDFs, report CSV download.
- `app/requirements_app.txt` — Streamlit dependencies.
- JSON summary `columns` field now omits empty keys and renames citation fallback to `doi_fallback_column`.

---

## [1.0.0] – 2026-04-16 — Initial Release

### Added

**Sources**
- PubMed Central (PMC) via NCBI OA API, with automatic PMID/DOI → PMCID conversion.
- Europe PMC via EBI REST API.
- Unpaywall (requires `--email` per their Terms of Service).
- OpenAlex (disabled by default; enable with `--openalex --openalex-api-key`).
- Semantic Scholar (disabled by default; enable with `--semantic-scholar`).
- Crossref link metadata.
- DOI landing-page HTML scan (meta-tags and `<link rel="alternate">` anchors).

**Architecture**
- Three-layer design: core library → `retrieve_pdf()` agent interface → CLI entry point.
- `retrieve_pdf()` returns a typed `DownloadResult` dataclass — no `sys.exit`, no stdout output, suitable for direct import in agents, pipelines, and notebooks.
- True multi-source cascade: all sources are queried first to collect every candidate URL; each is attempted for download in priority order until one succeeds. A URL that fails to download does not end the process.
- Unpaywall, Crossref, and landing-page scanner return all their candidates, not just the first.

**Reliability**
- HTTP adapter with automatic retry and exponential backoff (3 retries; backs off on 429 and 5xx).
- Thread-local `requests.Session` per worker — no shared-session race conditions.
- Unique `.{thread_id}.part` temp file per download — prevents concurrent writes to the same output path for duplicate DOIs.
- `--delay` applied at job *submission* time in parallel mode, so it genuinely throttles outgoing traffic rather than result collection.
- Fail-fast error when `--openalex` is used without `--openalex-api-key`.
- Crossref polite pool warning at runtime when `--workers` exceeds 3.

**CLI**
- Full flag set for enabling/disabling individual sources.
- `--skip-existing` to resume interrupted runs.
- `--workers` for parallel downloads.
- Logs to `stderr`; JSON summary to `stdout` (clean separation for pipeline integration).
- `--citation-column` accepted as a valid identifier source during column validation.

**Input**
- Source-agnostic CSV format: `Title`, `Authors`, `DOI` (primary); `PMID`, `PMCID` (optional).
- Compatible with exports from PubMed, Scopus, Web of Science, Zotero, Mendeley, and any tool that produces a CSV with these columns.
- Auto-detection of column names with case-insensitive matching.

**Output**
- PDF files named by DOI, PMCID, PMID, or title — in that priority order.
- CSV report with per-article `status`, `source`, `pdf_url`, `filename`, and diagnostic `note`.
- JSON summary includes separate counters for `downloads_ok`, `downloads_failed`, `not_found`, `errors`, and `downloads_skipped`.
