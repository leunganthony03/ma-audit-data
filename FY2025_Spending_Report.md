# Commonwealth of Massachusetts — FY2025 Spending Visibility Report

**Prepared:** 2026-04-04
**Source data:** Comptroller of the Commonwealth Spending file (`spending.db`) — 47.7M rows covering FY2010–FY2026; this report filters to `Budget_Fiscal_Year = 2025`.
**Public cross-reference sources:** mass.gov, malegislature.gov, macomptroller.org, COMMBUYS, DESE BESE filings.

---

## 1. Executive Summary

| Metric | Value |
|---|---|
| **Total FY2025 recorded spending** | **$98.33 billion** |
| Total transactions | 2,864,979 |
| Departments | 160 |
| Distinct vendors (inc. rollups) | 31,865 |
| Enacted FY2025 operating budget (public) | ~$57.78 billion ([Mass.gov](https://www.mass.gov/news/governor-healey-and-lieutenant-governor-driscoll-sign-5778-billion-fiscal-year-2025-budget)) |

### Why does the database show $98B when the enacted budget was $57.8B?

The Comptroller's spending file is **broader than the General Appropriations Act**. The $98B figure includes:

- **General Fund operating budget** (~$57.8B)
- **Federal trust funds** (Medicaid federal match, ARPA, ESSER, SNAP, TANF, IDEA — billions)
- **Non-budgeted trusts** (Group Insurance Commission, DUA assets, Pension funds, Capital trust)
- **Debt service** ($3.09B — principal and interest on Commonwealth bonds)
- **Non-cash accounting entries** (`3TX TRUSTS - NON-CASH`, `3TN TRUSTS` — bookkeeping for trust fund positions)
- **MBTA, MSBA, and quasi-independent authority transfers** (~$2.9B observed)
- **Pass-through state aid to municipalities** ($19.1B "PP" object class)

A meaningful "cash-out" figure after removing non-cash trust entries would likely land closer to $80–85B. The Comptroller publishes this as a single file without a flag separating cash from accounting entries.

---

## 2. Spending by Object Class

Object Classes group every transaction into 19 functional buckets:

| # | Object Class | FY2025 Total | % of Total |
|---|---|---:|---:|
| 1 | (RR) Benefit Programs | $35.97B | 36.6% |
| 2 | (PP) State Aid / Political Subdivisions | $19.12B | 19.4% |
| 3 | (DD) Pension & Insurance | $11.17B | 11.4% |
| 4 | (AA) Regular Employee Compensation | $10.24B | 10.4% |
| 5 | (MM) Purchased Client/Program Services | $6.96B | 7.1% |
| 6 | (NN) Infrastructure | $3.80B | 3.9% |
| 7 | (SS) Debt Payment | $3.09B | 3.1% |
| 8 | (UU) IT Non-Payroll Expenses | $1.56B | 1.6% |
| 9 | (TT) Loans & Special Payments | $1.43B | 1.5% |
| 10 | **(HH) Consultant Services** | **$1.16B** | **1.2%** |
| 11 | (EE) Administrative Expenses | $0.80B | 0.8% |
| 12 | (CC) Special Employees | $0.80B | 0.8% |
| 13 | (GG) Energy & Space Rental | $0.68B | 0.7% |
| 14 | (FF) Facility Operational | $0.52B | 0.5% |
| 15 | (JJ) Operational Services | $0.47B | 0.5% |
| 16 | (KK) Equipment Purchase | $0.34B | 0.3% |
| 17 | (LL) Equipment Lease/Maintain | $0.13B | 0.1% |
| 18 | (BB) Employee-Related (Travel etc.) | $0.10B | 0.1% |
| 19 | (99) Payroll Rejects | −$0.0005B | 0.0% |

**Key observation:** 58% of all recorded spending is two categories — **benefit programs (mostly MassHealth/Medicaid)** and **local aid**. Discretionary operating categories (consulting, travel, admin, equipment) together are ~3% of the total.

---

## 3. Spending by Cabinet / Secretariat

| Cabinet | FY2025 Total |
|---|---:|
| Executive Office of Health & Human Services (EHS) | $36.14B |
| Executive Office for Administration & Finance (ANF) | $18.61B |
| Executive Office of Education | $10.04B |
| Treasurer & Receiver General (debt, pensions, lottery) | $9.68B |
| Executive Office of Labor & Workforce Development | $9.64B |
| MassDOT | $5.21B |
| Public Safety & Homeland Security | $2.07B |
| Judiciary | $1.36B |
| Environmental Affairs | $1.31B |
| Housing & Livable Communities | $1.19B |
| Sheriff Departments | $0.81B |
| Economic Development | $0.68B |
| EO of Technology Services & Security (EOTSS) | $0.30B |
| District Attorneys | $0.17B |

**EHS alone = 37% of all state spending**, reflecting MassHealth's dominance.

---

## 4. Top 20 Vendors (excluding summary/aggregate rollups)

| Rank | Vendor | FY2025 Total | Category |
|---:|---|---:|---|
| 1 | Boston Medical Center Health P (WellSense) | $4.63B | MassHealth ACO/MCO |
| 2 | Retirement Allowance - Teachers | $3.58B | Pension payments |
| 3 | Retirement Allowance - State Employees | $3.04B | Pension payments |
| 4 | MBTA | $1.63B | Transit authority transfer |
| 5 | Medicaid (federal transfer vehicle) | $1.60B | Federal match |
| 6 | Tufts Health Public Plans | $1.56B | MassHealth ACO |
| 7 | Bond Redemption (Principal) | $1.52B | Debt service |
| 8 | Mass General Brigham Health Plan | $1.44B | MassHealth ACO |
| 9 | Commonwealth Care Alliance | $1.33B | MassHealth SCO/One Care |
| 10 | MA School Building Authority | $1.28B | School construction |
| 11 | Fallon Community Health Plan | $1.24B | MassHealth ACO |
| 12 | Bond Redemption (Interest) | $1.04B | Debt service |
| 13 | Tempus Unlimited Inc | $1.03B | PCA fiscal intermediary |
| 14 | Wellpoint Life & Health Ins (Anthem) | $1.03B | GIC employee health |
| 15 | Harvard Pilgrim Health Care | $0.89B | GIC employee health |
| 16 | Boston Medical Center Corp | $0.73B | Hospital (direct services) |
| 17 | MA State Lottery Commission | $0.70B | Lottery prize/operations |
| 18 | MBTA (secondary code) | $0.64B | Transit |
| 19 | City of Springfield | $0.64B | Local aid |
| 20 | City of Boston | $0.63B | Local aid |

**Interpretation:** The top 20 vendors reveal that the Commonwealth's largest payees are almost entirely **institutional**: Medicaid managed-care organizations, pension systems, bond trustees, transit authorities, and municipalities. This is structurally where taxpayer money goes — not to consultants, travel, or equipment.

---

## 5. Deep Dive: Consulting Services — $1.16 Billion

Consulting (object class HH) is often the first target of waste audits. Sub-category breakdown:

| Code | Description | Total |
|---|---|---:|
| H19 | Management Consultants | **$464.3M** |
| H87 | Cash with Campus (higher-ed pass-through) | $309.9M |
| H23 | Program Coordinators | $162.7M |
| HH3 | Media/Editorial/Communications | $50.0M |
| HH4 | Health & Safety Services | $47.9M |
| HH2 | Engineering/Research/Scientific | $39.3M |
| HH1 | Financial Services | $30.6M |
| H09 | Attorneys/Legal Services | $19.2M |
| Other | Planners, exam developers, recruiters, speakers | $36.5M |

### Top consulting vendors with verified purpose

| Vendor | FY2025 | Purpose (from DB appropriation + public sources) |
|---|---:|---|
| Massachusetts League of Community Health Centers | $145.1M | Behavioral health workforce, addiction services, opioid recovery, MassHealth delivery reform |
| University of Massachusetts | $69.9M | Research contracts, intergovernmental services, 115 appropriations across 36 departments |
| Automated Health Systems | $44.1M | 100% from MassHealth **Indemnity/Third Party Liability Plan** (appr 40000700) |
| **Cognia Inc** | **$38.4M** | **MCAS statewide student assessment contract** — 5-year $179.6M contract signed April 2024 ([DESE BESE](https://www.doe.mass.edu/bese/docs/fy2024/2024-05/item4c.docx)) |
| Optum Government Solutions | $29.2M | MassHealth Third Party Liability administration; Optum also serves as MassHealth LTSS Third Party Administrator ([Mass.gov](https://www.mass.gov/info-details/masshealth-and-private-health-insurance-also-known-as-third-party-liability-tpl)) |
| Deloitte Consulting | $18.9M | MassHealth delivery reform, population health trust, CDC epidemiology, ARPA HCBS |
| Maximus US Services | $18.2M | 100% MassHealth TPL |
| Mercer Health & Benefits | $17.5M | MassHealth TPL actuarial + delivery reform |
| Argus Communications | $17.1M | DPH Public Health Trust Fund — suicide prevention, vocational rehab, behavioral health awareness campaigns |
| Public Consulting Group | $12.9M | Cross-agency (38 appropriations, 11 departments) |
| JSI Research & Training | $12.9M | Public health program support |
| TNTP (The New Teacher Project) | $9.4M | DESE ESSER COVID relief, early literacy, Title I |
| Accenture LLP | $8.1M | MassHealth TPL, ARPA fiscal recovery |

### Consulting insight

Of the $1.16B coded as "consulting," approximately:

- **~$135M+ flows to MassHealth TPL contractors** (Automated Health, Optum, Maximus, Mercer, Deloitte, Accenture) — federally required cost-avoidance work under 42 CFR 433
- **~$145M to Massachusetts League of Community Health Centers** — nonprofit service delivery
- **~$38M to Cognia** — mandated MCAS testing
- **~$300M in "Cash with Campus"** (H87) — pass-through to state universities, not traditional consulting
- **~$40M genuine advisory** (Deloitte/Accenture/PCG management consulting outside TPL)

**True discretionary "strategy consulting" is ~$40M — less than 0.05% of the state's total spending.** The "$1.16B consulting" headline is misleading; most is federally mandated or service-delivery work.

---

## 6. Deep Dive: IT Spending — $1.56 Billion

| Sub-category | Total |
|---|---:|
| (U11) IT Contract Services | $538.3M |
| (U05) IT Staff Augmentation | $219.6M |
| (U87) Cash with Campus (higher-ed IT) | $188.3M |
| (U12) Cloud Services | $141.9M |
| (U10) IT Equipment Maintenance | $140.1M |
| (U03) Software & IT Licenses | $121.2M |
| (U07) IT Equipment | $101.7M |
| (U02) Voice Telecom | $68.4M |
| (U01) Data Telecom | $27.5M |
| (U08) IT Equipment Lease | $4.0M |

**Top IT vendors (FY2025):**

| Vendor | Total |
|---|---:|
| Smartronix, LLC | $63.5M |
| McInnis Consulting Services | $55.0M |
| OptumInsight | $54.5M |
| Dell Marketing | $53.2M |
| Carahsoft Technology | $52.2M |
| SHI International | $45.5M |
| Gainwell Technologies | $31.9M |
| CGI Technologies & Solutions | $29.5M |
| Deloitte Consulting | $29.2M |
| FAST LP | $27.7M |
| XFACT Inc | $24.8M |
| Securus Technologies | $22.2M |
| Conduent State Healthcare | $22.0M |
| Accenture | $21.5M |
| HP Inc | $21.1M |

**Observation:** $219.6M in "IT staff augmentation" (U05) is worth scrutiny. Staff augmentation = temporary contractors backfilling permanent positions — often a warning sign for either skills gaps, hiring freezes, or vendor-dependency. McInnis Consulting Services has 10,322 transactions averaging ~$5,300 each, consistent with hourly-rate staff augmentation.

---

## 7. Deep Dive: Travel — $69.7 Million

| Category | Total |
|---|---:|
| (B02) In-state travel | $33.4M |
| (B01) Out-of-state travel (airfare, hotels) | $29.7M |
| (C98) Special contract employee travel | $4.4M |
| (E42) Admin in-state travel | $1.1M |
| (E41) Admin out-of-state travel | $1.0M |
| (N98) Infrastructure project travel | $0.04M |

**Out-of-state travel concentration:**

- **University of Mass System: $20.4M (69%)** of all out-of-state state travel
- State universities + community colleges combined: ~$27M (91%)
- Non-higher-ed agencies total <$3M — State Police ($0.42M), DOR, DPH, Trial Court each under $500K

**Traceability gap:** 100% of UMass and state-university travel rows are aggregated as `[CAMPUS] SUMMARY TRUST PAYMENT` — individual travelers, destinations, and trip purposes are NOT in this dataset. State Police travel is bundled into payroll reimbursements. To audit actual trips would require each campus's or department's internal travel records.

---

## 8. Red Flags — Areas Warranting Further Review

These are **anomaly patterns**, not proven waste. Each requires investigation to determine legitimacy.

### 🚩 8.1 Exact-duplicate high-value payments
**106 groups** of 3+ rows with identical vendor, date, amount, and appropriation (>$100K each) in FY2025.

- If all are true duplicates → ~**$77.8M** in potential overpayments
- Most are likely **legitimate splits** across funding sources (state + federal + trust) or multiple invoices on one day
- Worth auditing:
  - **DSCI LLC**: 5× $450,000 on 06/27/2025 from appropriation 05210000 ($2.25M)
  - **Tufts Medical Center**: 3× $3,416,066.90 on 10/01/2024 ($10.25M)
  - **Conduent State Healthcare**: 4× $1,464,677.00 on 06/04/2025 ($5.86M)

### 🚩 8.2 IT Staff Augmentation ($219.6M)
Five times larger than IT equipment spend. Persistent reliance on contractor staff for core IT operations is a classic "shadow headcount" pattern. Worth understanding whether these are project-based (legitimate) or ongoing operational backfill (a budgeting workaround).

### 🚩 8.3 Traceability gaps
Very large portions of the dataset cannot be audited at the vendor level from this file alone:

- **$24.6B** in rows with `Vendor LIKE '%SUMMARY%'` — aggregate trust payments, payroll, campus rollups
- **$77.7B** in rows where `Vendor = 'UNASSIGNED'` or `Vendor_Id = 'UNASSIGNED'` (significant overlap with summary rows, plus benefit-program beneficiaries whose PHI is redacted)
- **$36.2B** in rows where `Zip_Code = 'UNASSIGNED'`

This is partly by design (Medicaid beneficiary PHI, pension recipients), but the lack of any flag distinguishing "redacted for privacy" from "missing data" reduces usefulness for public oversight.

### 🚩 8.4 Management Consulting — Deloitte / Accenture / PCG
Roughly **$40M** in genuine strategy consulting across multiple appropriations. Not large in absolute terms, but:
- Deloitte receives funds from 9 different appropriations in EHS alone
- Accenture appears in both MassHealth TPL and ARPA fiscal recovery work
- PCG operates across 38 appropriations and 11 departments
Multi-appropriation vendors often indicate master contracts where scope creep is possible.

### 🚩 8.5 Non-cash trust entries inflate apparent spending
The largest single "expenditure" in FY2025 is a **$1.33B non-cash trust bookkeeping entry** (`3TX TRUSTS - NON-CASH` — DUA Assets Held in Trust). The #2 non-unemployment item is a similar **$935M GIC Assets Held in Trust non-cash entry**. These are asset accounting, not outflows. The database does not flag these clearly, and naive summaries would double-count them against actual cash payments to carriers (~$2.96B to the top 8 GIC health insurers).

### 🚩 8.6 Payroll Rejects (Object Class 99)
Each department has near-zero net totals (reverse-and-rebook payroll corrections), but **one line remains un-reversed: CME −$543,374.47**. This is the only material net negative. Could be a normal year-end reconciliation or an outstanding correction — worth a one-line check.

---

## 9. What This Database Does Well vs. Poorly

### ✅ Strengths
- Complete transaction-level detail for non-benefit payments
- Appropriation and object code hierarchy provides good program-level traceability
- Vendor names (where present) align with public contract records on COMMBUYS
- 17 years of history enables trend analysis

### ❌ Limitations for waste analysis
1. **No contract ID field** — cannot link transactions to the Scope of Work on file in COMMBUYS
2. **Summary rollups for higher-ed** hide ~$20M of travel and ~$300M of "cash with campus" behind single lines
3. **No flag for non-cash entries** (3TX, 3TN) vs. actual cash outflows
4. **No employee count** or service-unit denominators — cannot compute per-capita metrics
5. **Vendor_Id often UNASSIGNED** — prevents reliable vendor deduplication
6. **Benefit recipients** are properly anonymized but indistinguishable from true data gaps
7. **No competition flag** — cannot identify sole-source vs. competitively bid awards

---

## 10. Recommended Next Steps for Deeper Visibility

| # | Action | Expected Payoff |
|---|---|---|
| 1 | Join this dataset to **COMMBUYS** contract master to attach contract IDs and SOWs | Enables true sole-source / competition analysis |
| 2 | Request UMass and state university **travel detail records** | Unmasks $27M of higher-ed travel |
| 3 | Audit the **106 exact-duplicate payment groups** (~$77.8M) | Confirms whether splits are legitimate |
| 4 | Drill into **IT Staff Augmentation ($219.6M)** by duration of engagement | Identifies shadow-headcount pattern |
| 5 | Separate **non-cash trust entries** (`3TX`, `3TN`) from cash outflows | Produces accurate cash-basis spending total |
| 6 | Cross-reference **Deloitte/Accenture/PCG** multi-appropriation work with COMMBUYS master contracts | Detects scope creep |
| 7 | Pull **GIC enrollment report** to compute per-member health benefit costs | Makes the $2.96B carrier spend comparable to private-sector benchmarks |
| 8 | Build a **YoY trend module** comparing FY2023 → FY2024 → FY2025 for every top-20 vendor | Flags abnormal growth rates |

---

## 11. Sources

**Primary data:**
- Comptroller of the Commonwealth Spending file (`Comptroller_of_the_Commonwealth_Spending.csv.gz`) — 47.7M rows, imported to SQLite

**Public references used in this report:**
- [Governor Healey Signs $57.78B FY2025 Budget — Mass.gov](https://www.mass.gov/news/governor-healey-and-lieutenant-governor-driscoll-sign-5778-billion-fiscal-year-2025-budget)
- [FY2025 Final Budget — malegislature.gov](https://malegislature.gov/Budget/FY2025/FinalBudget)
- [Enacted FY25 Statewide Summary — budget.digital.mass.gov](https://budget.digital.mass.gov/summary/fy25/enacted/)
- [Office of the Comptroller — macomptroller.org Contracts](https://www.macomptroller.org/contracts/)
- [MassHealth Third Party Liability — Mass.gov](https://www.mass.gov/info-details/masshealth-and-private-health-insurance-also-known-as-third-party-liability-tpl)
- [Update on MCAS Contract (Cognia $179.6M, April 2024) — DESE BESE](https://www.doe.mass.edu/bese/docs/fy2024/2024-05/item4c.docx)
- [Boston ACO / BMC HealthNet Plan — Mass.gov](https://www.mass.gov/info-details/boston-accountable-care-organization-in-partnership-with-bmc-healthnet-plan)
- [Statewide IT Strategy 2024–2025 — Mass.gov](https://www.mass.gov/info-details/statewide-it-strategy-for-2024-2025)
- [COMMBUYS — Commonwealth Contract System](https://www.commbuys.com/)

---

## 12. Closing Assessment

The Massachusetts FY2025 spending database is a **valuable but blunt instrument** for public oversight. It reveals *where* money is directed at the program and vendor level, but it does not answer *why* (no scope-of-work linkage) or *whether the rate is fair* (no unit-cost metrics).

**The honest finding:** genuine discretionary overhead — management consulting, travel, administrative expenses, non-IT equipment — together totals roughly **$2.5 billion**, or **~2.5% of recorded spending**. The remaining 97.5% is structurally locked into benefit programs, payroll, pensions, local aid, debt service, and federally required contractors. Waste reduction efforts focused only on the 2.5% can produce visible wins, but the larger opportunity — if efficiency is the goal — lies in **program design** (MassHealth cost growth, GIC benefit structure, pension liability management) rather than line-item cuts to the visible "consulting and travel" bucket.

Targeted audit opportunities surfaced by this analysis:

1. ~$78M in exact-duplicate high-value payments (likely legitimate splits, but worth verifying)
2. $219.6M in IT staff augmentation (possible shadow headcount)
3. ~$40M in cross-appropriation management consulting (scope creep risk)
4. $27M in opaque higher-education travel rollups (traceability)

These are realistic starting points for a more rigorous waste-review engagement.
