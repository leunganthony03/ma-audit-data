# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repo Does

Two independent civic audit pipelines that generate self-contained HTML reports from public Massachusetts government data. No web server — everything runs locally from SQLite databases.

**Pipeline 1 — Massachusetts State (FY2025)**
- `spending.db` + `commbuys.db` → `build_audit_html.py` → `audit.html`

**Pipeline 2 — City of Cambridge**
- `cambridge.db` (built by `import_cambridge.py`) → `build_cambridge_audit.py` → `cambridge_audit.html`
- `cambridge.db` → `build_cambridge_2025.py` → `cambridge_2025.html`

## Commands

```bash
# Regenerate the MA state report (reads spending.db + commbuys.db)
python3 build_audit_html.py

# Cambridge: first-time DB import from live Socrata APIs (~5 min)
python3 import_cambridge.py

# Cambridge: multi-year overview report
python3 build_cambridge_audit.py

# Cambridge: FY2025 deep-dive (ACFR actuals, bonds, enriched settlements)
python3 build_cambridge_2025.py
```

No test suite, no build step, no linting. Verify by running the script and opening the output HTML.

## Architecture

### spending.db (MA state — ~2 GB)
Core tables: `spending` (2.86M rows, FY2010-2026 CTHRU transactions), `payroll` (2.69M rows, open payroll CY2010-2025), `revenue`. `commbuys.db` is ATTACHed at query time.

Performance: `build_audit_html.py` materialises two temp tables at startup — `_fy25_vendor` and `_vendor_best` — with indexes, avoiding repeated `CAST(REPLACE(Amount,',','') AS REAL)` expressions. All N+1 loops were replaced with batched `IN(...)` queries. A `settlement_case_context` table caches CourtListener API lookups so they run only once per unique payee.

### cambridge.db (~14 MB)
Tables: `op_expenditures` (40k rows, FY2011-2026 adopted budget), `op_revenues`, `capital` (7-year CIP), `salary`, `contracts`, `bids`, `property` (30k parcels), `acfr_actuals` (parsed from FY2025 ACFR PDF), `bonds` + bond support tables (`bond_categories`, `bond_debt_service`, `bond_history`, `bond_lto`, `bond_summary`).

### Report generation pattern (all three build scripts)
1. `load_data()` — SQL queries return a single `data` dict
2. Optional enrichment: `validate_data(data)`, `fetch_gov_annotations(data)`
3. `HTML_TEMPLATE` — large raw string with `__DATA_JSON__` placeholder
4. `main()` — substitutes `json.dumps(data)`, writes self-contained HTML

All rendering is client-side JavaScript. No external CDN or server needed.

### Key Python helpers in build_audit_html.py
- `KNOWN_CASES` dict + `enrich_settlement_cases()` + `_courtlistener_lookup()` — settlement enrichment; CourtListener results cached in `spending.db`
- `validate_data()` — 14 automated checks against Socrata Open Payroll API and published budget references
- `build_fund_sources()` — categorises CTHRU appropriation-type codes into gross-vs-net budget reconciliation
- `fetch_gov_annotations()` — live Socrata fetch of Settlements & Judgments dataset (gpqz-7ppn); results annotate department tables with `⚖ settle` badges

### Key JavaScript patterns in HTML templates
- `buildPaginatorWidget(items, pageSize, headers, renderRow)` — generic paginator reused across vendor transactions, OT employees, and bond issuances
- `buildPaginatedVendorTable(vendors, txnLookup, parentCols, pageSize)` — wraps `buildVendorTxnDrilldown` with top/bottom pagers
- `deptAnnoBadges(deptName)` — returns settlement badge HTML for flagged departments
- `fmt$` / `fp` / `fmtV` / `fN` — currency/number formatters used consistently across all reports

### import_cambridge.py notes
- `socrata_fetch()` paginates with 60s timeout; use `page=500` for large datasets (property has 30k rows)
- Property dataset times out at default page size — always use `page=500`
- Re-running wipes and recreates `cambridge.db` from scratch

## Data Sources

| Dataset | Source | How to refresh |
|---|---|---|
| MA CTHRU spending | `cthru.data.socrata.com` dataset `pegc-naaa` | Manual bulk download → rebuild spending.db |
| MA Open Payroll | `cthru.data.socrata.com` dataset `9ttk-7vz6` | Manual bulk download → rebuild spending.db |
| COMMBUYS contracts | Scraped from `commbuys.com` | Manual scrape → rebuild commbuys.db |
| Cambridge budget/revenue/capital | `data.cambridgema.gov` (`5bn4-5wey`, `ixyv-mje6`, `9chi-2ed3`) | `python3 import_cambridge.py` |
| Cambridge property | `data.cambridgema.gov` dataset `waa7-ibdu` | `python3 import_cambridge.py` |
| Cambridge ACFR actuals | PDF at `cambridgema.gov` parsed with `pdfplumber` | Re-parse if new ACFR published |
| Settlement case context | CourtListener API + static `KNOWN_CASES` dict | DB-cached in `spending.db` after first run |

## Git Workflow

- Active branch: `feat/spending-audit-report`
- Remotes: `origin` → `leunganthony03/playground`, `ma-audit` → `leunganthony03/ma-audit-data`
- Push to both: `git push origin feat/spending-audit-report && git push ma-audit feat/spending-audit-report:main`
- Commit format: `type: short description` (e.g. `feat:`, `fix:`)
