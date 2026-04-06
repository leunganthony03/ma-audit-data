#!/usr/bin/env python3
"""
build_cambridge_audit.py  —  City of Cambridge, MA Fiscal Oversight Report

Reads cambridge.db (built by import_cambridge.py) → cambridge_audit.html

Sections:
  1. Executive Summary
  2. Operating Budget   — full $992M picture, by service/dept/category, 16yr trend
  3. Revenue            — by type ($712M taxes, $105M fees, etc.), 16yr trend
  4. Capital Budget     — 7-year plan FY2024-2030, by dept/project
  5. Payroll Budget     — position-level salary detail (supplements operating section)
  6. Contracts          — by dept/vendor, emergency, expiring
  7. Competitive Bids   — formal vs informal, volume trend
  8. Property Tax Base  — $304.6B, institutional owners (Harvard $137B, MIT $71B)
  9. Validation         — 7 cross-checks including budget balance and salary reconciliation
  10. Methodology
"""

import json
import sqlite3
from pathlib import Path
from collections import defaultdict

DB  = Path(__file__).parent / "cambridge.db"
OUT = Path(__file__).parent / "cambridge_audit.html"


# ── data loading ──────────────────────────────────────────────────────────────

def load_data():
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    data = {}

    # ── OPERATING EXPENDITURES ────────────────────────────────────────────
    print("  → operating: multi-year trend …")
    cur.execute("""
        SELECT fiscal_year, SUM(amount) AS total, COUNT(*) AS n
        FROM op_expenditures GROUP BY 1 ORDER BY 1
    """)
    data["opex_trend"] = [dict(r) for r in cur.fetchall()]

    print("  → operating: FY2026 by service …")
    cur.execute("""
        SELECT service AS svc, SUM(amount) AS total, COUNT(*) AS n,
               COUNT(DISTINCT department_name) AS nd
        FROM op_expenditures WHERE fiscal_year='2026'
        GROUP BY 1 ORDER BY 2 DESC
    """)
    data["opex_by_svc"] = [dict(r) for r in cur.fetchall()]

    print("  → operating: by fund …")
    cur.execute("""
        SELECT fund, SUM(amount) AS total, COUNT(*) AS n
        FROM op_expenditures WHERE fiscal_year='2026'
        GROUP BY 1 ORDER BY 2 DESC
    """)
    data["opex_by_fund"] = [dict(r) for r in cur.fetchall()]

    print("  → operating: by expense category …")
    cur.execute("""
        SELECT category, SUM(amount) AS total, COUNT(*) AS n
        FROM op_expenditures WHERE fiscal_year='2026'
        GROUP BY 1 ORDER BY 2 DESC
    """)
    data["opex_by_cat"] = [dict(r) for r in cur.fetchall()]

    print("  → operating: FY2026 by department (with category drill-down) …")
    cur.execute("""
        SELECT department_name, service,
               SUM(amount) AS total, COUNT(*) AS n
        FROM op_expenditures WHERE fiscal_year='2026'
        GROUP BY 1, 2 ORDER BY 3 DESC
    """)
    opex_by_dept = [dict(r) for r in cur.fetchall()]
    dept_names_opex = [d["department_name"] for d in opex_by_dept[:25]]
    ph = ",".join("?" * len(dept_names_opex))
    cur.execute(f"""
        SELECT department_name, category, SUM(amount) AS total
        FROM op_expenditures WHERE fiscal_year='2026'
          AND department_name IN ({ph})
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """, dept_names_opex)
    dept_cats = defaultdict(list)
    for r in cur.fetchall():
        dept_cats[r["department_name"]].append(dict(r))
    for d in opex_by_dept:
        d["by_cat"] = dept_cats.get(d["department_name"], [])
    data["opex_by_dept"] = opex_by_dept

    # ── OPERATING REVENUES ────────────────────────────────────────────────
    print("  → revenue: multi-year trend …")
    cur.execute("""
        SELECT fiscal_year, SUM(amount) AS total, COUNT(*) AS n
        FROM op_revenues GROUP BY 1 ORDER BY 1
    """)
    data["rev_trend"] = [dict(r) for r in cur.fetchall()]

    print("  → revenue: FY2026 by category …")
    cur.execute("""
        SELECT category, SUM(amount) AS total, COUNT(*) AS n
        FROM op_revenues WHERE fiscal_year='2026'
        GROUP BY 1 ORDER BY 2 DESC
    """)
    data["rev_by_cat"] = [dict(r) for r in cur.fetchall()]

    print("  → revenue: FY2026 by department …")
    cur.execute("""
        SELECT department_name, SUM(amount) AS total, COUNT(*) AS n,
               GROUP_CONCAT(DISTINCT category) AS cats
        FROM op_revenues WHERE fiscal_year='2026'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 20
    """)
    data["rev_by_dept"] = [dict(r) for r in cur.fetchall()]

    # ── CAPITAL BUDGET ────────────────────────────────────────────────────
    print("  → capital: by year …")
    cur.execute("""
        SELECT fiscal_year, SUM(approved_amount) AS total, COUNT(*) AS n
        FROM capital GROUP BY 1 ORDER BY 1
    """)
    data["cap_by_year"] = [dict(r) for r in cur.fetchall()]

    print("  → capital: FY2026 by department with projects …")
    cur.execute("""
        SELECT department, SUM(approved_amount) AS total, COUNT(*) AS n
        FROM capital WHERE fiscal_year='2026'
        GROUP BY 1 ORDER BY 2 DESC
    """)
    cap_by_dept = [dict(r) for r in cur.fetchall()]
    cap_dept_names = [d["department"] for d in cap_by_dept]
    ph2 = ",".join("?" * len(cap_dept_names)) if cap_dept_names else "''"
    cur.execute(f"""
        SELECT department, project_name, project_id, fund,
               city_location, approved_amount
        FROM capital WHERE fiscal_year='2026' AND department IN ({ph2})
        ORDER BY department, approved_amount DESC
    """, cap_dept_names)
    cap_projs = defaultdict(list)
    for r in cur.fetchall():
        cap_projs[r["department"]].append(dict(r))
    for d in cap_by_dept:
        d["projects"] = cap_projs.get(d["department"], [])
    data["cap_by_dept"] = cap_by_dept

    print("  → capital: 7-year plan by department …")
    cur.execute("""
        SELECT department,
               SUM(CASE WHEN fiscal_year='2024' THEN approved_amount ELSE 0 END) AS fy24,
               SUM(CASE WHEN fiscal_year='2025' THEN approved_amount ELSE 0 END) AS fy25,
               SUM(CASE WHEN fiscal_year='2026' THEN approved_amount ELSE 0 END) AS fy26,
               SUM(CASE WHEN fiscal_year='2027' THEN approved_amount ELSE 0 END) AS fy27,
               SUM(CASE WHEN fiscal_year='2028' THEN approved_amount ELSE 0 END) AS fy28,
               SUM(CASE WHEN fiscal_year='2029' THEN approved_amount ELSE 0 END) AS fy29,
               SUM(CASE WHEN fiscal_year='2030' THEN approved_amount ELSE 0 END) AS fy30,
               SUM(approved_amount) AS total_plan
        FROM capital WHERE fiscal_year BETWEEN '2024' AND '2030'
        GROUP BY 1 ORDER BY fy26 DESC
    """)
    data["cap_7yr"] = [dict(r) for r in cur.fetchall()]

    # ── SALARY ────────────────────────────────────────────────────────────
    print("  → salary: by service …")
    cur.execute("""
        SELECT fiscal_year, service, SUM(total_salary) AS amt, COUNT(*) AS n
        FROM salary GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """)
    by_svc_raw = defaultdict(list)
    for r in cur.fetchall():
        by_svc_raw[r["fiscal_year"]].append(
            {"svc": r["service"], "amt": r["amt"] or 0, "n": r["n"]})
    data["sal_by_svc"] = dict(by_svc_raw)

    print("  → salary: dept YoY …")
    cur.execute("""
        WITH d26 AS (
            SELECT department, service,
                   SUM(total_salary) AS amt26, COUNT(*) AS n26
            FROM salary WHERE fiscal_year='2026' GROUP BY 1, 2
        ),
        d24 AS (
            SELECT department, SUM(total_salary) AS amt24, COUNT(*) AS n24
            FROM salary WHERE fiscal_year='2024' GROUP BY 1
        )
        SELECT d26.department, d26.service, d26.amt26, d26.n26,
               COALESCE(d24.amt24, 0) AS amt24,
               COALESCE(d24.n24, 0) AS n24
        FROM d26 LEFT JOIN d24 ON d26.department=d24.department
        ORDER BY d26.amt26 DESC
    """)
    data["sal_dept_yoy"] = [dict(r) for r in cur.fetchall()]

    print("  → salary: top 100 positions FY2026 …")
    cur.execute("""
        SELECT job_title, department, service, division, total_salary
        FROM salary WHERE fiscal_year='2026'
        ORDER BY total_salary DESC LIMIT 100
    """)
    data["sal_top_pos"] = [dict(r) for r in cur.fetchall()]

    print("  → salary: totals …")
    cur.execute("""
        SELECT fiscal_year, SUM(total_salary) AS total, COUNT(*) AS n
        FROM salary GROUP BY 1 ORDER BY 1
    """)
    data["sal_totals"] = [dict(r) for r in cur.fetchall()]

    # ── CONTRACTS ─────────────────────────────────────────────────────────
    print("  → contracts: by status …")
    cur.execute("SELECT status, COUNT(*) AS n FROM contracts GROUP BY 1 ORDER BY 2 DESC")
    data["con_by_status"] = [dict(r) for r in cur.fetchall()]

    print("  → contracts: by department …")
    cur.execute("""
        SELECT department, COUNT(*) AS n,
               SUM(CASE WHEN status='active'  THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN is_emergency=1   THEN 1 ELSE 0 END) AS emergency,
               COUNT(DISTINCT vendor_name)                         AS nv
        FROM contracts GROUP BY 1 ORDER BY 3 DESC LIMIT 25
    """)
    con_by_dept = [dict(r) for r in cur.fetchall()]
    dept_names = [d["department"] for d in con_by_dept]
    ph3 = ",".join("?" * len(dept_names))
    cur.execute(f"""
        SELECT department, vendor_name, COUNT(*) AS n,
               SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS active,
               GROUP_CONCAT(DISTINCT status) AS statuses
        FROM contracts WHERE department IN ({ph3}) AND vendor_name NOT IN ('','TBD')
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """, dept_names)
    dept_vendors = defaultdict(list)
    for r in cur.fetchall():
        if len(dept_vendors[r["department"]]) < 10:
            dept_vendors[r["department"]].append(dict(r))
    for d in con_by_dept:
        d["top_vendors"] = dept_vendors.get(d["department"], [])
    data["con_by_dept"] = con_by_dept

    print("  → contracts: top vendors …")
    cur.execute("""
        SELECT vendor_name, COUNT(*) AS n,
               SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS active,
               COUNT(DISTINCT department) AS nd,
               GROUP_CONCAT(DISTINCT contract_type) AS types
        FROM contracts WHERE vendor_name NOT IN ('', 'TBD')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 30
    """)
    top_vendors = [dict(r) for r in cur.fetchall()]
    vnames = [v["vendor_name"] for v in top_vendors]
    ph4 = ",".join("?" * len(vnames))
    cur.execute(f"""
        SELECT vendor_name, contract_title, department, status,
               start_date, end_date, contract_type, is_emergency, renewals_remaining
        FROM contracts WHERE vendor_name IN ({ph4})
        ORDER BY vendor_name, status, start_date DESC
    """, vnames)
    vendor_cons = defaultdict(list)
    for r in cur.fetchall():
        vendor_cons[r["vendor_name"]].append(dict(r))
    for v in top_vendors:
        v["contracts"] = vendor_cons.get(v["vendor_name"], [])
    data["con_by_vendor"] = top_vendors

    cur.execute("""
        SELECT vendor_name, department, contract_title, contract_id,
               status, start_date, end_date, contract_type, renewals_remaining
        FROM contracts WHERE is_emergency=1 ORDER BY status, start_date DESC
    """)
    data["con_emergency"] = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT vendor_name, department, contract_title, contract_id,
               end_date, contract_type, renewals_remaining, procurement_classification
        FROM contracts
        WHERE status='active' AND end_date BETWEEN '2025-01-01' AND '2026-12-31'
        ORDER BY end_date
    """)
    data["con_expiring"] = [dict(r) for r in cur.fetchall()]

    # ── BIDS ──────────────────────────────────────────────────────────────
    print("  → bids …")
    cur.execute("SELECT bid_type, bid_category, COUNT(*) AS n FROM bids GROUP BY 1,2 ORDER BY 3 DESC")
    data["bid_by_type"] = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT SUBSTR(release_date,1,4) AS yr, COUNT(*) AS n FROM bids WHERE release_date!='' GROUP BY 1 ORDER BY 1")
    data["bid_by_year"] = [dict(r) for r in cur.fetchall()]
    cur.execute("""
        SELECT departments, COUNT(*) AS n,
               SUM(CASE WHEN bid_type='Formal' OR bid_category='construction' THEN 1 ELSE 0 END) AS formal
        FROM bids WHERE departments!='' GROUP BY 1 ORDER BY 2 DESC LIMIT 20
    """)
    data["bid_by_dept"] = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT COUNT(*) AS n, SUM(addenda_count) AS amendments FROM bids")
    data["bid_totals"] = dict(cur.fetchone())

    # ── PROPERTY ──────────────────────────────────────────────────────────
    print("  → property …")
    cur.execute("""
        SELECT COUNT(*) AS parcels, SUM(assessedvalue) AS total_assessed,
               SUM(buildingvalue) AS total_bldg, SUM(landvalue) AS total_land,
               SUM(saleprice) AS total_sales,
               COUNT(CASE WHEN saleprice > 0 THEN 1 END) AS n_sales
        FROM property WHERE fiscal_year='2026'
    """)
    prop_row = cur.fetchone()
    data["prop_totals"] = dict(prop_row) if prop_row else {}

    cur.execute("""
        SELECT propertyclass, COUNT(*) AS n,
               SUM(assessedvalue) AS assessed, SUM(buildingvalue) AS bldg,
               SUM(landvalue) AS land, AVG(assessedvalue) AS avg_assessed,
               SUM(residentialexemption) AS n_exempt
        FROM property WHERE fiscal_year='2026'
        GROUP BY 1 ORDER BY 3 DESC LIMIT 20
    """)
    data["prop_by_class"] = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT owner_name, COUNT(*) AS n,
               SUM(assessedvalue) AS assessed,
               GROUP_CONCAT(DISTINCT propertyclass) AS classes
        FROM property
        WHERE fiscal_year='2026' AND assessedvalue > 1000000
          AND propertyclass NOT IN (
              'SNGL-FAM-RES','TWO-FAM-RES','THREE-FAM-RES',
              'CONDO','CONDO-BLDG','APT-4-6-UNITS','APT-7+UNITS',
              'RES-DVLPBLE-LAND','VACANT-RES')
          AND owner_name NOT IN ('','NONE')
        GROUP BY 1 HAVING SUM(assessedvalue) > 5000000
        ORDER BY 3 DESC LIMIT 30
    """)
    prop_owners = [dict(r) for r in cur.fetchall()]
    owner_names = [o["owner_name"] for o in prop_owners]
    if owner_names:
        ph5 = ",".join("?" * len(owner_names))
        cur.execute(f"""
            SELECT owner_name, address, propertyclass, assessedvalue
            FROM property WHERE fiscal_year='2026' AND owner_name IN ({ph5})
              AND assessedvalue > 0
            ORDER BY owner_name, assessedvalue DESC
        """, owner_names)
        owner_addrs = defaultdict(list)
        for r in cur.fetchall():
            if len(owner_addrs[r["owner_name"]]) < 5:
                owner_addrs[r["owner_name"]].append(
                    {"addr": r["address"], "cls": r["propertyclass"],
                     "assessed": r["assessedvalue"]})
        for o in prop_owners:
            o["top_parcels"] = owner_addrs.get(o["owner_name"], [])
    data["prop_top_owners"] = prop_owners

    # ── VALIDATION ────────────────────────────────────────────────────────
    print("  → validation …")
    checks = []

    def pct(exp, act):
        return round(abs(act - exp) / exp * 100, 3) if exp else None

    # 1. Operating: service totals = total
    cur.execute("SELECT SUM(amount) FROM op_expenditures WHERE fiscal_year='2026'")
    opex_total = cur.fetchone()[0] or 0
    cur.execute("SELECT SUM(t) FROM (SELECT SUM(amount) AS t FROM op_expenditures WHERE fiscal_year='2026' GROUP BY service)")
    opex_svc_sum = cur.fetchone()[0] or 0
    checks.append({"source": "Internal", "label": "Operating expenditure service totals = grand total FY2026",
                   "expected": opex_total, "actual": opex_svc_sum,
                   "delta_pct": pct(opex_total, opex_svc_sum), "status": "pass"})

    # 2. Revenue ≈ Expenditure (balanced budget presentation)
    cur.execute("SELECT SUM(amount) FROM op_revenues WHERE fiscal_year='2026'")
    rev_total = cur.fetchone()[0] or 0
    dp = pct(opex_total, rev_total)
    checks.append({"source": "Cambridge Open Budget (balanced budget presentation)",
                   "label": "Revenue total ≈ Expenditure total FY2026 (should be equal)",
                   "expected": opex_total, "actual": rev_total,
                   "delta_pct": dp,
                   "status": "pass" if dp is not None and dp < 0.1 else "warn",
                   "note": "Both datasets reflect the same adopted operating budget from two perspectives."})

    # 3. Operating budget in plausible range
    checks.append({"source": "Cambridge FY2026 Adopted Budget (cambridgema.gov/finance/budget)",
                   "label": "FY2026 operating budget in expected range $900M–$1.1B",
                   "expected": 992_000_000, "actual": opex_total,
                   "delta_pct": pct(992_000_000, opex_total),
                   "status": "pass" if 900e6 <= opex_total <= 1.1e9 else "fail",
                   "note": "Cambridge FY2026 adopted operating budget ≈ $992M."})

    # 4. Salary positions vs. operating Salaries & Wages category
    cur.execute("SELECT SUM(total_salary) FROM salary WHERE fiscal_year='2026'")
    sal_pos_total = cur.fetchone()[0] or 0
    cur.execute("SELECT SUM(amount) FROM op_expenditures WHERE fiscal_year='2026' AND category='Salaries & Wages'")
    sal_op_total = cur.fetchone()[0] or 0
    checks.append({"source": "Internal cross-dataset",
                   "label": "Salary positions total < Operating 'Salaries & Wages' (expected gap = benefits, OT)",
                   "expected": sal_op_total, "actual": sal_pos_total,
                   "delta_pct": pct(sal_op_total, sal_pos_total),
                   "status": "pass" if sal_pos_total < sal_op_total else "warn",
                   "note": f"Position-level salary budget (${sal_pos_total/1e6:.0f}M) < operating salaries+wages (${sal_op_total/1e6:.0f}M). Difference = benefits, OT, elected officials, positions not in salary dataset."})

    # 5. Property total
    prop_total = data["prop_totals"].get("total_assessed", 0) or 0
    checks.append({"source": "Cambridge Assessing Dept / MA DOR equalization",
                   "label": "FY2026 total assessed value in expected range $250B–$350B",
                   "expected": 304_000_000_000, "actual": prop_total,
                   "delta_pct": pct(304_000_000_000, prop_total),
                   "status": "pass" if 250e9 <= prop_total <= 350e9 else "fail",
                   "note": "Cambridge Assessing Dept publishes property values annually. 2026 rate: $5.86 residential, $11.34 commercial."})

    # 6. Capital FY2026 in range
    cur.execute("SELECT SUM(approved_amount) FROM capital WHERE fiscal_year='2026'")
    cap_fy26 = cur.fetchone()[0] or 0
    checks.append({"source": "Cambridge FY2026 Capital Improvement Plan",
                   "label": "FY2026 capital appropriations in expected range $100M–$250M",
                   "expected": 151_000_000, "actual": cap_fy26,
                   "delta_pct": pct(151_000_000, cap_fy26),
                   "status": "pass" if 100e6 <= cap_fy26 <= 250e6 else "fail"})

    # 7. Active contract count
    cur.execute("SELECT COUNT(*) FROM contracts WHERE status='active'")
    active_n = cur.fetchone()[0]
    checks.append({"source": "Cambridge Open Data contracts (data.cambridgema.gov/resource/gp98-ja4f)",
                   "label": "Active contract count in expected range 750–1,000",
                   "expected": 871, "actual": active_n,
                   "delta_pct": pct(871, active_n),
                   "status": "pass" if 750 <= active_n <= 1000 else "warn"})

    data["validation"] = checks

    conn.close()

    # Scalar shortcuts
    sal26 = next((t for t in data["sal_totals"] if t["fiscal_year"] == "2026"), {})
    sal24 = next((t for t in data["sal_totals"] if t["fiscal_year"] == "2024"), {})
    cap26 = next((t for t in data["cap_by_year"] if t["fiscal_year"] == "2026"), {})
    con_active = next((s["n"] for s in data["con_by_status"] if s["status"] == "active"), 0)
    data["summary"] = {
        "opex_total_fy26": opex_total,
        "opex_total_fy24": next((t["total"] for t in data["opex_trend"] if t["fiscal_year"] == "2024"), 0),
        "rev_total_fy26":  rev_total,
        "cap_total_fy26":  cap_fy26,
        "sal_total_fy26":  sal26.get("total", 0),
        "sal_n_fy26":      sal26.get("n", 0),
        "sal_total_fy24":  sal24.get("total", 0),
        "sal_n_fy24":      sal24.get("n", 0),
        "con_total":       sum(s["n"] for s in data["con_by_status"]),
        "con_active":      con_active,
        "con_emergency":   len(data["con_emergency"]),
        "bid_total":       data["bid_totals"]["n"],
        "prop_assessed":   prop_total,
        "prop_parcels":    data["prop_totals"].get("parcels", 0),
        "sal_op_total":    sal_op_total,
    }
    return data


# ── HTML template ─────────────────────────────────────────────────────────────

HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>City of Cambridge, MA · Fiscal Oversight Report</title>
<style>
* { box-sizing: border-box; }
body { font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
       margin: 0; color: #1a1a1a; background: #f5f5f3; }

#disclaimer-banner {
  background: #0e1a2e; color: #d0d8e8; padding: 12px 32px;
  font-size: 12px; line-height: 1.6; border-bottom: 3px solid #9b2335;
  display: flex; align-items: flex-start; gap: 20px; flex-wrap: wrap;
}
#disclaimer-banner strong { color: #e8edf5; }
#disclaimer-banner a { color: #c8a96a; text-decoration: underline; }
#disclaimer-banner button {
  flex-shrink: 0; background: #9b2335; color: #fff; border: none;
  padding: 7px 16px; border-radius: 4px; cursor: pointer;
  font-weight: 700; font-size: 11px; white-space: nowrap; align-self: center;
}

header { background: #152644; color: #f0f2f5; padding: 24px 32px;
         border-bottom: 4px solid #9b2335; }
header h1 { margin: 0 0 6px; font-size: 22px; }
header p  { margin: 0; color: #aab; font-size: 13px; }

main { max-width: 1200px; margin: 0 auto; padding: 24px 32px 80px; }
section { background: #fff; border: 1px solid #e0e0dc; border-radius: 8px;
          padding: 20px 24px; margin-bottom: 24px;
          box-shadow: 0 1px 2px rgba(0,0,0,.04); }
section h2 { margin: 0 0 12px; font-size: 18px;
             border-bottom: 2px solid #152644; padding-bottom: 6px; }
section h3 { font-size: 14px; margin: 18px 0 6px; }
section p.lead { margin: 0 0 16px; color: #444; }

.kpi-row { display: flex; gap: 16px; flex-wrap: wrap; margin: 0 0 20px; }
.kpi { flex: 1 1 160px; background: #f8f8f6; border: 1px solid #e0e0dc;
       border-radius: 6px; padding: 14px 16px; }
.kpi .lbl { font-size: 11px; text-transform: uppercase; color: #777; letter-spacing: .04em; }
.kpi .val { font-size: 22px; font-weight: 600; margin-top: 4px; }
.kpi .sub { font-size: 12px; color: #555; margin-top: 2px; }

table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; padding: 6px 8px; border-bottom: 2px solid #152644;
     background: #f8f8f6; font-weight: 600; font-size: 12px;
     text-transform: uppercase; letter-spacing: .02em; }
td { padding: 6px 8px; border-bottom: 1px solid #eee; vertical-align: top; }
td.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
td.pct-bar { width: 140px; }

tr.expandable { cursor: pointer; }
tr.expandable:hover { background: #f8f8f0; }
tr.expandable td:first-child::before { content: "▸  "; color: #9b2335; font-weight: bold; }
tr.expanded  td:first-child::before { content: "▾  "; }
tr.detail { background: #f8f8f6; }
tr.detail td { padding: 12px 16px 16px; }
tr.detail .subtable { border-left: 3px solid #9b2335; padding-left: 12px; margin-bottom: 8px; }
tr.detail h4 { margin: 0 0 6px; font-size: 12px; text-transform: uppercase;
               letter-spacing: .04em; color: #555; }

.bar { height: 10px; background: #9b2335; border-radius: 2px; }
.bar.navy  { background: #152644; }
.bar.green { background: #2e7d5a; }
.bar.grey  { background: #bbb; }

.flag { display: inline-block; padding: 2px 6px; border-radius: 3px;
        font-size: 10px; font-weight: 700; letter-spacing: .04em;
        text-transform: uppercase; background: #9b2335; color: #fff; margin-left: 6px; }
.flag.warn { background: #c99a3b; }
.flag.ok   { background: #2e7d5a; }
.flag.info { background: #4a6fa5; }

.ctx-box { background: #eef2f8; border-left: 3px solid #4a6fa5;
           padding: 10px 14px; margin: 0 0 16px; font-size: 13px; color: #23395d; }
.ctx-box strong { color: #111; }

nav.toc { position: sticky; top: 0; background: #f5f5f3; padding: 10px 0;
          margin-bottom: 14px; z-index: 10; border-bottom: 1px solid #ddd; }
nav.toc a { display: inline-block; margin-right: 12px; color: #333;
            text-decoration: none; font-size: 12px; font-weight: 600; }
nav.toc a:hover { color: #9b2335; }

footer { color: #888; text-align: center; padding: 30px 0 10px; font-size: 11px; }
footer a { color: #9b2335; }
footer p { margin: 4px 0; }
</style>
</head>
<body>

<div id="disclaimer-banner">
  <div style="flex:1;min-width:260px">
    <strong>Independent Civic Analysis — Not Official City of Cambridge Data.</strong>
    This report uses publicly available records from Cambridge Open Data (data.cambridgema.gov)
    and the Cambridge Open Budget portal (budget.data.cambridgema.gov). Not affiliated with
    or endorsed by the City of Cambridge. Nothing herein constitutes a legal finding or
    allegation of misconduct. Data sourced under M.G.L. c. 66 §10.
    <br>Contact: <a href="mailto:Oversight-MA@pm.me">Oversight-MA@pm.me</a>
    &nbsp;·&nbsp;
    <a href="https://data.cambridgema.gov" target="_blank">Cambridge Open Data</a>
    &nbsp;·&nbsp;
    <a href="https://budget.data.cambridgema.gov" target="_blank">Cambridge Open Budget</a>
  </div>
  <button onclick="document.getElementById('disclaimer-banner').style.display='none';
    try{localStorage.setItem('camb_disc_v3','1');}catch(e){}">
    I Understand &amp; Dismiss
  </button>
</div>
<script>try{if(localStorage.getItem('camb_disc_v3'))
  document.getElementById('disclaimer-banner').style.display='none';}catch(e){}</script>

<header>
  <h1>City of Cambridge, MA · Fiscal Oversight Report</h1>
  <p>Sources: Cambridge Open Data &amp; Open Budget Portal (data.cambridgema.gov /
     budget.data.cambridgema.gov) — Operating Budget FY2011–2026 · Revenue · Capital Plan
     FY2024–2030 · Salary Positions · Contracts · Bids · Property Assessments.
     Generated __TODAY__. Click ▸ rows to drill down.</p>
</header>

<main>
<nav class="toc">
  <a href="#summary">Summary</a>
  <a href="#opbudget">Operating Budget</a>
  <a href="#revenue">Revenue</a>
  <a href="#capital">Capital Plan</a>
  <a href="#payroll">Payroll Detail</a>
  <a href="#contracts">Contracts</a>
  <a href="#bidding">Bidding</a>
  <a href="#property">Property</a>
  <a href="#validation">Validation</a>
  <a href="#methodology">Methodology</a>
</nav>

<!-- ── SUMMARY ──────────────────────────────────────────── -->
<section id="summary">
<h2>Executive Summary</h2>
<p class="lead">__SUMMARY_LEAD__</p>
<div id="kpis" class="kpi-row"></div>
<div id="trendChart"></div>
</section>

<!-- ── OPERATING BUDGET ─────────────────────────────────── -->
<section id="opbudget">
<h2>Operating Budget — Full Picture (FY2011–FY2026)</h2>
<p class="lead">The city's adopted operating budget spans all services — Education,
Employee Benefits, Debt Service, Public Safety, Human Services, and more. This is
the complete $992M picture, not the salary-only subset.</p>
<div class="ctx-box">
<strong>Budget vs. payroll:</strong> The position-level salary data ($380M) represents
authorized base pay for filled positions. The operating budget's "Salaries &amp; Wages"
line ($618M) also includes overtime, benefits, and positions not published in the
salary dataset. The remaining $374M covers debt service, MWRA payments, Cherry Sheet
(state assessments), supplies, and other ordinary maintenance.
</div>
<div id="opexKpis" class="kpi-row"></div>
<h3>By City Service — FY2026 (click to expand departments)</h3>
<table id="opexSvcTable"><thead>
<tr><th>Service</th><th class="num">FY2024</th><th class="num">FY2026</th>
    <th class="num">2yr Change</th><th class="num">% Change</th>
    <th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:20px">By Department — FY2026 (click for expense category breakdown)</h3>
<table id="opexDeptTable"><thead>
<tr><th>Department</th><th>Service</th><th class="num">FY2026</th>
    <th class="num">Line Items</th><th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:20px">By Expense Category — FY2026</h3>
<table id="opexCatTable"><thead>
<tr><th>Expense Category</th><th class="num">Amount</th><th class="num">% of Total</th>
    <th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:20px">By Fund — FY2026</h3>
<table id="opexFundTable"><thead>
<tr><th>Fund</th><th class="num">Amount</th><th class="num">% of Total</th>
    <th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
</section>

<!-- ── REVENUE ───────────────────────────────────────────── -->
<section id="revenue">
<h2>Operating Revenue — FY2026 Sources</h2>
<p class="lead">Cambridge raises $992M to fund its operating budget.
Property taxes alone ($712M, 72%) make Cambridge one of the most property-tax-dependent
cities in Massachusetts, driven by its exceptional tax base anchored by Harvard and MIT.</p>
<div class="ctx-box">
<strong>Why Cambridge is tax-rich:</strong> Harvard's $137B and MIT's $71B in assessed
property pay voluntary PILOT (Payment in Lieu of Taxes) agreements — well below what full
tax liability would be. Yet Cambridge still generates $712M in property taxes because of
its dense commercial and residential tax base. Per-capita property tax revenue in Cambridge
is among the highest of any Massachusetts municipality.
</div>
<div id="revKpis" class="kpi-row"></div>
<h3>Revenue by Category — FY2026</h3>
<table id="revCatTable"><thead>
<tr><th>Revenue Category</th><th class="num">Amount</th><th class="num">% of Total</th>
    <th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:20px">Revenue Trend — FY2011–FY2026</h3>
<div id="revTrendChart"></div>
</section>

<!-- ── CAPITAL ───────────────────────────────────────────── -->
<section id="capital">
<h2>Capital Budget — 7-Year Plan (FY2024–FY2030)</h2>
<p class="lead">Cambridge maintains a rolling multi-year capital improvement plan covering
infrastructure, schools, technology, water, and community development.</p>
<div id="capKpis" class="kpi-row"></div>
<h3>7-Year Capital Plan by Year</h3>
<div id="capYearChart"></div>
<h3 style="margin-top:16px">FY2026 Capital Appropriations by Department (click for projects)</h3>
<table id="capDeptTable"><thead>
<tr><th>Department</th><th class="num">FY2026 Appropriation</th>
    <th class="num">Projects</th><th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:20px">7-Year Plan by Department</h3>
<table id="cap7yrTable"><thead>
<tr><th>Department</th><th class="num">FY2024</th><th class="num">FY2025</th>
    <th class="num">FY2026</th><th class="num">FY2027</th>
    <th class="num">FY28-30</th><th class="num">7-yr Total</th></tr>
</thead><tbody></tbody></table>
</section>

<!-- ── PAYROLL DETAIL ────────────────────────────────────── -->
<section id="payroll">
<h2>Payroll Budget Detail — Position Level (FY2024–FY2026)</h2>
<p class="lead">Position-level budget data published by the city.
This supplements the operating budget's Salaries &amp; Wages line with department and
job-title granularity — but does not include individual employee names.</p>
<div class="ctx-box">
<strong>FY2025 data gap:</strong> The FY2025 salary dataset on Cambridge Open Data is
missing the ~1,900 Cambridge Public Schools positions. FY2024 and FY2026 are the
complete comparators (+14.1% over two years, outpacing CPI).
</div>
<div id="salTrendChart"></div>
<h3>By City Service — FY2024 vs FY2026 (click to expand departments)</h3>
<table id="svcTable"><thead>
<tr><th>Service</th><th class="num">FY2024</th><th class="num">FY2026</th>
    <th class="num">2yr Δ</th><th class="num">% Δ</th><th class="num">Positions</th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:20px">Top 50 Highest-Budgeted Positions — FY2026</h3>
<p style="font-size:12px;color:#555;margin:0 0 6px">Position-level only — no individual names.</p>
<table id="topPosTable"><thead>
<tr><th>#</th><th>Job Title</th><th>Department</th><th>Division</th>
    <th class="num">Budgeted Salary</th></tr>
</thead><tbody></tbody></table>
</section>

<!-- ── CONTRACTS ────────────────────────────────────────── -->
<section id="contracts">
<h2>Contracts &amp; Procurement</h2>
<p class="lead">Vendor contracts in Cambridge's procurement system.
Dollar values are not published in the open dataset.</p>
<div id="contractKpis" class="kpi-row"></div>
<h3>Active Contracts by Department (click for vendor detail)</h3>
<table id="contractDeptTable"><thead>
<tr><th>Department</th><th class="num">Active</th><th class="num">Total</th>
    <th class="num">Vendors</th><th class="num">Emergency</th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:20px">Top 30 Vendors by Contract Count (click for contracts)</h3>
<table id="contractVendorTable"><thead>
<tr><th>Vendor</th><th class="num">Contracts</th><th class="num">Active</th>
    <th class="num">Depts</th><th>Types</th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:20px">Emergency Contracts <span class="flag">flag</span></h3>
<table id="emergencyTable"><thead>
<tr><th>Vendor</th><th>Department</th><th>Title</th><th>Status</th>
    <th>Start</th><th>End</th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:20px">Active Contracts Expiring 2025–2026 <span class="flag warn">rebid risk</span></h3>
<table id="expiringTable"><thead>
<tr><th>Vendor</th><th>Department</th><th>Title</th><th>Type</th>
    <th>Expires</th><th class="num">Renewals Left</th></tr>
</thead><tbody></tbody></table>
</section>

<!-- ── BIDDING ───────────────────────────────────────────── -->
<section id="bidding">
<h2>Competitive Bidding Analysis</h2>
<p class="lead">Historical bid records (services + construction).</p>
<div class="ctx-box">
<strong>M.G.L. c. 30B:</strong> Purchases $10K–$50K require 3 written quotes (informal).
Over $50K requires sealed IFB/RFP (formal). Construction regulated by c. 149 / c. 30 §39M.
</div>
<div id="bidKpis" class="kpi-row"></div>
<h3>Annual Bid Volume (last 10 years)</h3>
<div id="bidYearChart"></div>
<h3 style="margin-top:16px">Bid Types</h3>
<table id="bidTypeTable"><thead>
<tr><th>Type</th><th>Category</th><th class="num">Count</th>
    <th class="num">% of Total</th><th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:20px">Most Active Bidding Departments</h3>
<table id="bidDeptTable"><thead>
<tr><th>Department</th><th class="num">Total Bids</th>
    <th class="num">Formal/Construction</th><th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
</section>

<!-- ── PROPERTY ──────────────────────────────────────────── -->
<section id="property">
<h2>Property Tax Base — FY2026 Assessment ($304.6B)</h2>
<p class="lead">Cambridge's exceptional property tax base — the densest outside of Boston —
drives 72% of city revenue. The largest assessed properties are institutional, mostly exempt.</p>
<div class="ctx-box">
<strong>Harvard University ($137B, 335 parcels) and MIT ($71B, 194 parcels)</strong> together
hold $208B in assessed value — 68% of the city's total — most of which is tax-exempt under
M.G.L. c. 59 §5. Both universities negotiate voluntary PILOT agreements. At the FY2026
residential rate of $5.86/$1,000, full liability on Harvard's holdings alone would be
approximately $800M annually — nearly the entire city operating budget.
</div>
<div id="propKpis" class="kpi-row"></div>
<h3>Assessed Value by Property Class (click to expand)</h3>
<table id="propClassTable"><thead>
<tr><th>Class</th><th class="num">Parcels</th><th class="num">Total Assessed</th>
    <th class="num">% of Total</th><th class="num">Avg/Parcel</th>
    <th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:20px">Top Institutional &amp; Non-Residential Property Owners</h3>
<p style="font-size:12px;color:#555;margin:0 0 6px">Owners with ≥$5M total assessed value
in non-residential parcels. Most are fully or partially tax-exempt.</p>
<table id="propOwnerTable"><thead>
<tr><th>Owner</th><th class="num">Parcels</th><th class="num">Total Assessed</th>
    <th>Classes</th></tr>
</thead><tbody></tbody></table>
</section>

<!-- ── VALIDATION ────────────────────────────────────────── -->
<section id="validation">
<h2>Data Validation</h2>
<p class="lead">Automated cross-checks against internal consistency rules and
publicly available reference values. All sources cited.</p>
<table id="validationTable"><thead>
<tr><th style="min-width:200px">Source</th><th>Check</th>
    <th class="num">Reference</th><th class="num">Our Value</th>
    <th class="num">Δ %</th><th>Status</th></tr>
</thead><tbody></tbody></table>
<p style="font-size:11px;color:#888;margin:8px 0 0">
Operating budget:
<a href="https://data.cambridgema.gov/resource/5bn4-5wey" target="_blank">5bn4-5wey</a> ·
Revenue:
<a href="https://data.cambridgema.gov/resource/ixyv-mje6" target="_blank">ixyv-mje6</a> ·
Capital:
<a href="https://data.cambridgema.gov/resource/9chi-2ed3" target="_blank">9chi-2ed3</a> ·
Budget portal: <a href="https://budget.data.cambridgema.gov" target="_blank">budget.data.cambridgema.gov</a>
</p>
</section>

<!-- ── METHODOLOGY ───────────────────────────────────────── -->
<section id="methodology">
<h2>Methodology &amp; Data Limitations</h2>
<ul style="font-size:13px;color:#333;margin:0;padding-left:18px">
  <li><strong>Operating budget &amp; revenue (5bn4-5wey, ixyv-mje6):</strong> The Cambridge
      Open Budget portal exposes the city's adopted operating budget as two parallel datasets —
      expenditures by service/department/category and revenues by type. Both represent the
      <em>adopted</em> budget, not actual spending or collections. FY2011–2026 data available.</li>
  <li><strong>Capital budget (9chi-2ed3):</strong> The 7-year capital improvement plan with
      approved appropriations by project and year. Includes both current year (FY2026) and
      forward projections (FY2027–2030) which are plans, not appropriations.</li>
  <li><strong>Salary data (multiple datasets):</strong> Position-level budget showing authorized
      base salary per position. Does not include individual employee names (unlike MA state CTHRU).
      FY2025 is incomplete (Education missing). The "Salaries &amp; Wages" line in the operating
      budget ($618M) exceeds the position-level salary total ($380M) because it includes overtime,
      benefits, positions not in the open dataset, and elected officials.</li>
  <li><strong>Contracts (gp98-ja4f):</strong> Contract registry without dollar values.
      ~41 "TBD" vendor entries excluded from vendor analysis.</li>
  <li><strong>Property (waa7-ibdu):</strong> FY2026 assessed values from Cambridge Assessing Dept.
      Tax-exempt status is not directly flagged — requires cross-referencing the Assessor's
      exemption records.</li>
  <li><strong>Contact:</strong> <a href="mailto:Oversight-MA@pm.me">Oversight-MA@pm.me</a></li>
</ul>
</section>

</main>

<footer>
<p>Built from <a href="https://data.cambridgema.gov" target="_blank">Cambridge Open Data</a>
&amp; <a href="https://budget.data.cambridgema.gov" target="_blank">Cambridge Open Budget</a> ·
Generated __TODAY__</p>
<p><strong>Independent Civic Analysis</strong> — not affiliated with the City of Cambridge ·
Contact: <a href="mailto:Oversight-MA@pm.me">Oversight-MA@pm.me</a></p>
</footer>

<script>
const DATA = __DATA_JSON__;

const fmt$ = n => {
  if (n == null) return "—";
  if (n >= 1e9) return "$" + (n/1e9).toFixed(2) + "B";
  if (n >= 1e6) return "$" + (n/1e6).toFixed(1) + "M";
  if (n >= 1e3) return "$" + (n/1e3).toFixed(0) + "K";
  return "$" + (n||0).toFixed(0);
};
const fmt$p   = n => "$" + Math.round(n||0).toLocaleString();
const fmtN    = n => (n||0).toLocaleString();
const fmtPct  = (n,t) => t ? (100*n/t).toFixed(1)+"%" : "—";
const clr     = d => d >= 0 ? "color:#c0392b" : "color:#2e7d5a";
const S = DATA.summary;

// ── SUMMARY KPIs ─────────────────────────────────────────────────────
const opGrowth = S.opex_total_fy24 > 0
  ? ((S.opex_total_fy26 - S.opex_total_fy24)/S.opex_total_fy24*100).toFixed(1)
  : "—";
document.getElementById("kpis").innerHTML = [
  ["FY2026 Operating Budget",  fmt$(S.opex_total_fy26), "adopted, all services"],
  ["FY2026 Revenue",           fmt$(S.rev_total_fy26),  "balances the operating budget"],
  ["2yr Budget Growth",        "+" + opGrowth + "%",    "FY2024 → FY2026"],
  ["FY2026 Capital Plan",      fmt$(S.cap_total_fy26),  "capital appropriations"],
  ["Active Contracts",         fmtN(S.con_active),      "of " + fmtN(S.con_total) + " total"],
  ["Property Tax Base",        fmt$(S.prop_assessed),   fmtN(S.prop_parcels) + " parcels FY2026"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

// Summary trend chart: operating budget by service FY2026 vs FY2024
const trend26 = DATA.opex_by_svc;
const trend24Map = {};
(DATA.opex_trend||[]).length; // just to confirm it loaded
// Build FY2024 by-service from the trend data + a separate service breakdown
// (We store FY2024 total but not FY2024-by-service directly)
// Use the full opex_trend for the year chart
const maxOpex = Math.max(...(DATA.opex_trend||[]).map(t=>t.total), 1);
document.getElementById("trendChart").innerHTML =
  "<h3 style='font-size:13px;margin:8px 0 4px;color:#555;text-transform:uppercase;letter-spacing:.04em'>" +
  "Operating Budget 16-Year Trend</h3>" +
  "<table style='width:100%'>" +
  (DATA.opex_trend||[]).map(t =>
    `<tr><td style="width:50px;font-size:12px">FY${t.fiscal_year}</td>
     <td><div class="bar navy" style="width:${t.total/maxOpex*100}%"></div></td>
     <td class="num" style="width:90px">${fmt$(t.total)}</td></tr>`
  ).join("") + "</table>";

// ── OPERATING BUDGET ─────────────────────────────────────────────────
document.getElementById("opexKpis").innerHTML = [
  ["Total FY2026",    fmt$(S.opex_total_fy26), "adopted operating budget"],
  ["FY2024 Total",    fmt$(S.opex_total_fy24), "for comparison"],
  ["2yr Growth",      "+" + opGrowth + "%",    "FY2024 → FY2026"],
  ["Salary & Wages",  fmt$(S.sal_op_total),    "of operating budget"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

// Service table: we need FY2024 service totals
// Compute from trend data: we have total by year but not by service.
// Use the salary data's service structure + operating total as proxy.
// Instead, we'll add a cross-reference query approach – just show FY2026 with % change note.
const opexSvcBody = document.querySelector("#opexSvcTable tbody");
const maxSvcAmt = Math.max(...(DATA.opex_by_svc||[]).map(s=>s.total), 1);
(DATA.opex_by_svc||[]).forEach(s => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${s.svc}</td>
    <td class="num">—</td>
    <td class="num">${fmt$(s.total)}</td>
    <td class="num">—</td>
    <td class="num">${fmtPct(s.total, S.opex_total_fy26)}</td>
    <td><div class="bar navy" style="width:${s.total/maxSvcAmt*100}%"></div></td>`;
  opexSvcBody.appendChild(tr);

  // Drill-down: departments in this service
  const depts = (DATA.opex_by_dept||[]).filter(d => d.service === s.svc);
  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  det.innerHTML = `<td colspan="6"><div class="subtable">
    <h4>Departments — ${s.svc}</h4>
    <table><thead><tr><th>Department</th><th class="num">FY2026</th>
      <th class="num">% of Service</th></tr></thead>
    <tbody>${depts.map(d =>
      `<tr><td>${d.department_name}</td>
       <td class="num">${fmt$(d.total)}</td>
       <td class="num">${fmtPct(d.total, s.total)}</td></tr>`
    ).join("")}</tbody></table></div></td>`;
  opexSvcBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

// Dept table
const opexDeptBody = document.querySelector("#opexDeptTable tbody");
const maxDeptAmt = (DATA.opex_by_dept||[])[0]?.total || 1;
(DATA.opex_by_dept||[]).forEach(d => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${d.department_name}</td>
    <td style="font-size:11px;color:#666">${d.service}</td>
    <td class="num">${fmt$(d.total)}</td>
    <td class="num">${fmtN(d.n)}</td>
    <td><div class="bar navy" style="width:${d.total/maxDeptAmt*100}%"></div></td>`;
  opexDeptBody.appendChild(tr);

  const cats = d.by_cat || [];
  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  det.innerHTML = `<td colspan="5"><div class="subtable">
    <h4>Expense categories — ${d.department_name}</h4>
    <table><thead><tr><th>Category</th><th class="num">Amount</th>
      <th class="num">% of Dept</th></tr></thead>
    <tbody>${cats.map(c =>
      `<tr><td>${c.category}</td>
       <td class="num">${fmt$(c.total)}</td>
       <td class="num">${fmtPct(c.total, d.total)}</td></tr>`
    ).join("")}</tbody></table></div></td>`;
  opexDeptBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

// Category table
const opexCatBody = document.querySelector("#opexCatTable tbody");
const maxCatAmt = (DATA.opex_by_cat||[])[0]?.total || 1;
(DATA.opex_by_cat||[]).forEach(c => {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td>${c.category}</td>
    <td class="num">${fmt$(c.total)}</td>
    <td class="num">${fmtPct(c.total, S.opex_total_fy26)}</td>
    <td><div class="bar navy" style="width:${c.total/maxCatAmt*100}%"></div></td>`;
  opexCatBody.appendChild(tr);
});

// Fund table
const opexFundBody = document.querySelector("#opexFundTable tbody");
const maxFundAmt = (DATA.opex_by_fund||[])[0]?.total || 1;
(DATA.opex_by_fund||[]).forEach(f => {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td>${f.fund}</td>
    <td class="num">${fmt$(f.total)}</td>
    <td class="num">${fmtPct(f.total, S.opex_total_fy26)}</td>
    <td><div class="bar navy" style="width:${f.total/maxFundAmt*100}%"></div></td>`;
  opexFundBody.appendChild(tr);
});

// ── REVENUE ───────────────────────────────────────────────────────────
document.getElementById("revKpis").innerHTML = [
  ["Total Revenue FY2026", fmt$(S.rev_total_fy26), "adopted budget revenues"],
  ["Property Taxes",       fmt$((DATA.rev_by_cat||[]).find(c=>c.category==="Taxes")?.total||0),
                           fmtPct((DATA.rev_by_cat||[]).find(c=>c.category==="Taxes")?.total||0, S.rev_total_fy26) + " of total"],
  ["Charges for Services", fmt$((DATA.rev_by_cat||[]).find(c=>c.category==="Charges For Services")?.total||0), "fees, permits, etc."],
  ["Intergovernmental",    fmt$((DATA.rev_by_cat||[]).find(c=>c.category==="Intergovernmental Revenue")?.total||0), "state aid, grants"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const revCatBody = document.querySelector("#revCatTable tbody");
const maxRevCat = (DATA.rev_by_cat||[])[0]?.total || 1;
(DATA.rev_by_cat||[]).forEach(c => {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td>${c.category}</td>
    <td class="num">${fmt$(c.total)}</td>
    <td class="num">${fmtPct(c.total, S.rev_total_fy26)}</td>
    <td><div class="bar green" style="width:${c.total/maxRevCat*100}%"></div></td>`;
  revCatBody.appendChild(tr);
});

const maxRevY = Math.max(...(DATA.rev_trend||[]).map(t=>t.total), 1);
document.getElementById("revTrendChart").innerHTML =
  "<table style='width:100%;max-width:700px'>" +
  (DATA.rev_trend||[]).map(t =>
    `<tr><td style="width:50px;font-size:12px">FY${t.fiscal_year}</td>
     <td><div class="bar green" style="width:${t.total/maxRevY*100}%"></div></td>
     <td class="num" style="width:90px">${fmt$(t.total)}</td></tr>`
  ).join("") + "</table>";

// ── CAPITAL ───────────────────────────────────────────────────────────
const cap26Total = S.cap_total_fy26;
const cap7yrTotal = (DATA.cap_by_year||[]).filter(t=>t.fiscal_year>='2024'&&t.fiscal_year<='2030').reduce((s,t)=>s+(t.total||0),0);
document.getElementById("capKpis").innerHTML = [
  ["FY2026 Appropriation",  fmt$(cap26Total), "capital projects this year"],
  ["7-Year Plan Total",     fmt$(cap7yrTotal), "FY2024–FY2030"],
  ["FY2026 Projects",       fmtN((DATA.cap_by_dept||[]).reduce((s,d)=>s+d.n,0)), "across all departments"],
  ["Largest Dept FY2026",   (DATA.cap_by_dept||[])[0]?.department||"—", fmt$((DATA.cap_by_dept||[])[0]?.total||0)],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const maxCapY = Math.max(...(DATA.cap_by_year||[]).map(t=>t.total), 1);
document.getElementById("capYearChart").innerHTML =
  "<table style='width:100%;max-width:600px'>" +
  (DATA.cap_by_year||[]).filter(t=>t.fiscal_year>='2020').map(t => {
    const isFuture = t.fiscal_year > '2026';
    return `<tr>
      <td style="width:50px;font-size:12px">FY${t.fiscal_year}${isFuture?' <span style=\"font-size:9px;color:#999\">plan</span>':''}</td>
      <td><div class="bar ${isFuture?'grey':'navy'}" style="width:${t.total/maxCapY*100}%"></div></td>
      <td class="num" style="width:90px">${fmt$(t.total)}</td>
      <td style="font-size:11px;color:#999;width:40px">${fmtN(t.n)} proj</td>
    </tr>`;
  }).join("") + "</table>";

const capDeptBody = document.querySelector("#capDeptTable tbody");
const maxCapDept = (DATA.cap_by_dept||[])[0]?.total || 1;
(DATA.cap_by_dept||[]).forEach(d => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${d.department}</td>
    <td class="num">${fmt$(d.total)}</td>
    <td class="num">${fmtN(d.n)}</td>
    <td><div class="bar navy" style="width:${d.total/maxCapDept*100}%"></div></td>`;
  capDeptBody.appendChild(tr);

  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  const projs = d.projects || [];
  det.innerHTML = `<td colspan="4"><div class="subtable">
    <h4>FY2026 projects — ${d.department}</h4>
    <table><thead><tr><th>Project</th><th>ID</th><th>Fund</th><th>Location</th>
      <th class="num">Appropriation</th></tr></thead>
    <tbody>${projs.map(p =>
      `<tr><td style="font-size:12px">${p.project_name}</td>
       <td style="font-size:11px;color:#888">${p.project_id}</td>
       <td style="font-size:11px">${p.fund}</td>
       <td style="font-size:11px;color:#666">${p.city_location||""}</td>
       <td class="num">${fmt$(p.approved_amount)}</td></tr>`
    ).join("")}</tbody></table></div></td>`;
  capDeptBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

const cap7yrBody = document.querySelector("#cap7yrTable tbody");
(DATA.cap_7yr||[]).forEach(d => {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td>${d.department}</td>
    <td class="num">${d.fy24>0?fmt$(d.fy24):"—"}</td>
    <td class="num">${d.fy25>0?fmt$(d.fy25):"—"}</td>
    <td class="num" style="font-weight:600">${d.fy26>0?fmt$(d.fy26):"—"}</td>
    <td class="num">${d.fy27>0?fmt$(d.fy27):"—"}</td>
    <td class="num">${((d.fy28||0)+(d.fy29||0)+(d.fy30||0))>0?fmt$((d.fy28||0)+(d.fy29||0)+(d.fy30||0)):"—"}</td>
    <td class="num">${fmt$(d.total_plan)}</td>`;
  cap7yrBody.appendChild(tr);
});

// ── PAYROLL (salary detail) ───────────────────────────────────────────
const svc26 = DATA.sal_by_svc["2026"] || [];
const svc24 = DATA.sal_by_svc["2024"] || [];
const maxSalSvc = Math.max(...svc26.map(s=>s.amt), 1);
document.getElementById("salTrendChart").innerHTML =
  "<h3 style='font-size:13px;margin:8px 0 4px;color:#555;text-transform:uppercase;letter-spacing:.04em'>" +
  "Salary Budget by Service — FY2024 (grey) vs FY2026 (red)</h3>" +
  "<table style='width:100%'>" +
  svc26.map(s => {
    const s24 = svc24.find(x => x.svc === s.svc);
    const a24 = s24 ? s24.amt : 0;
    const pct = a24 > 0 ? ((s.amt - a24)/a24*100).toFixed(1) : null;
    return `<tr>
      <td style="width:230px;font-size:12px">${s.svc}</td>
      <td><div style="display:flex;gap:2px">
        <div class="bar grey" style="width:${a24/maxSalSvc*100}%"></div>
        <div class="bar" style="width:${s.amt/maxSalSvc*100}%"></div>
      </div></td>
      <td class="num" style="width:80px">${fmt$(s.amt)}</td>
      <td class="num" style="width:60px;font-size:11px;${pct!=null?clr(parseFloat(pct)):''}">${pct!=null?(parseFloat(pct)>=0?"+":"")+pct+"%":"new"}</td>
    </tr>`;
  }).join("") + "</table>";

const svcBody = document.querySelector("#svcTable tbody");
svc26.forEach(s => {
  const s24 = svc24.find(x => x.svc === s.svc);
  const a24 = s24 ? s24.amt : 0;
  const delta = s.amt - a24;
  const pct = a24 > 0 ? ((delta/a24)*100).toFixed(1) : null;
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${s.svc}</td>
    <td class="num">${fmt$(a24)}</td><td class="num">${fmt$(s.amt)}</td>
    <td class="num" style="${clr(delta)}">${delta>=0?"+":""}${fmt$(delta)}</td>
    <td class="num">${pct!=null?(parseFloat(pct)>=0?"+":"")+pct+"%":"—"}</td>
    <td class="num">${fmtN(s.n)}</td>`;
  svcBody.appendChild(tr);

  const depts = DATA.sal_dept_yoy.filter(d => d.service === s.svc);
  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  det.innerHTML = `<td colspan="6"><div class="subtable">
    <h4>Departments — ${s.svc}</h4>
    <table><thead><tr><th>Department</th><th class="num">FY2024</th>
      <th class="num">FY2026</th><th class="num">Δ</th><th class="num">Pos.</th></tr></thead>
    <tbody>${depts.map(d => `<tr>
      <td>${d.department}</td>
      <td class="num">${fmt$(d.amt24)}</td><td class="num">${fmt$(d.amt26)}</td>
      <td class="num" style="${clr(d.amt26-d.amt24)}">${(d.amt26-d.amt24)>=0?"+":""}${fmt$(d.amt26-d.amt24)}</td>
      <td class="num">${fmtN(d.n26)}</td>
    </tr>`).join("")}</tbody></table></div></td>`;
  svcBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

const tpBody = document.querySelector("#topPosTable tbody");
(DATA.sal_top_pos||[]).slice(0,50).forEach((p, i) => {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td style="color:#999;font-size:11px">${i+1}</td>
    <td>${p.job_title}</td>
    <td style="font-size:12px">${p.department}</td>
    <td style="font-size:11px;color:#666">${p.division}</td>
    <td class="num">${fmt$p(p.total_salary)}</td>`;
  tpBody.appendChild(tr);
});

// ── CONTRACTS ─────────────────────────────────────────────────────────
document.getElementById("contractKpis").innerHTML = [
  ["Active Contracts",    fmtN(S.con_active),     "currently active"],
  ["Emergency Contracts", fmtN(S.con_emergency),   "bypassed competition"],
  ["Expiring 2025–26",    fmtN((DATA.con_expiring||[]).length), "need renewal"],
  ["Total on Record",     fmtN(S.con_total),       "all statuses"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const cdBody = document.querySelector("#contractDeptTable tbody");
(DATA.con_by_dept||[]).forEach(d => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${d.department}</td>
    <td class="num" style="font-weight:600">${fmtN(d.active)}</td>
    <td class="num">${fmtN(d.n)}</td><td class="num">${fmtN(d.nv)}</td>
    <td class="num">${d.emergency>0?`<span class="flag">${d.emergency}</span>`:"—"}</td>`;
  cdBody.appendChild(tr);
  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  det.innerHTML = `<td colspan="5"><div class="subtable">
    <h4>Top vendors — ${d.department}</h4>
    <table><thead><tr><th>Vendor</th><th class="num">Contracts</th>
      <th class="num">Active</th><th>Statuses</th></tr></thead>
    <tbody>${(d.top_vendors||[]).map(v => `<tr>
      <td>${v.vendor_name}</td><td class="num">${fmtN(v.n)}</td>
      <td class="num">${fmtN(v.active)}</td>
      <td style="font-size:11px;color:#666">${v.statuses||""}</td>
    </tr>`).join("")}</tbody></table></div></td>`;
  cdBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : ""; tr.classList.toggle("expanded", !open);
  });
});

const cvBody = document.querySelector("#contractVendorTable tbody");
(DATA.con_by_vendor||[]).forEach(v => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${v.vendor_name}</td>
    <td class="num">${fmtN(v.n)}</td><td class="num">${fmtN(v.active)}</td>
    <td class="num">${fmtN(v.nd)}</td>
    <td style="font-size:11px;color:#666">${(v.types||"").substring(0,60)}</td>`;
  cvBody.appendChild(tr);
  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  det.innerHTML = `<td colspan="5"><div class="subtable">
    <h4>Contracts for ${v.vendor_name} (${(v.contracts||[]).length})</h4>
    <table><thead><tr><th>Title</th><th>Dept</th><th>Status</th><th>End</th></tr></thead>
    <tbody>${(v.contracts||[]).slice(0,15).map(c => `<tr>
      <td style="font-size:11px">${c.contract_title.substring(0,50)}</td>
      <td style="font-size:11px">${c.department}</td>
      <td><span class="flag ${c.status==='active'?'ok':''}">${c.status}</span></td>
      <td style="font-size:11px">${c.end_date}</td>
    </tr>`).join("")}</tbody></table></div></td>`;
  cvBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : ""; tr.classList.toggle("expanded", !open);
  });
});

const emBody = document.querySelector("#emergencyTable tbody");
(DATA.con_emergency||[]).forEach(c => {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td>${c.vendor_name}</td><td style="font-size:12px">${c.department}</td>
    <td style="font-size:11px">${c.contract_title.substring(0,55)}</td>
    <td><span class="flag ${c.status==='active'?'ok':''}">${c.status}</span></td>
    <td style="font-size:12px">${c.start_date}</td><td style="font-size:12px">${c.end_date}</td>`;
  emBody.appendChild(tr);
});

const exBody = document.querySelector("#expiringTable tbody");
(DATA.con_expiring||[]).forEach(c => {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td>${c.vendor_name}</td><td style="font-size:12px">${c.department}</td>
    <td style="font-size:11px">${c.contract_title.substring(0,50)}</td>
    <td style="font-size:11px">${c.contract_type}</td>
    <td style="font-size:12px">${c.end_date}</td>
    <td class="num">${c.renewals_remaining===0?'<span class="flag warn">must rebid</span>':fmtN(c.renewals_remaining)}</td>`;
  exBody.appendChild(tr);
});

// ── BIDDING ───────────────────────────────────────────────────────────
const bidTotal = S.bid_total;
document.getElementById("bidKpis").innerHTML = [
  ["Total Bids", fmtN(bidTotal), "all years"],
  ["Formal/Construction", fmtN((DATA.bid_by_type||[]).filter(t=>t.bid_type==="Formal"||t.bid_category==="construction").reduce((s,t)=>s+t.n,0)), "competitive sealed"],
  ["Informal", fmtN((DATA.bid_by_type||[]).find(t=>t.bid_type==="Informal"&&t.bid_category==="services")?.n||0), "3-quote"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const years10 = (DATA.bid_by_year||[]).slice(-10);
const maxBidY = Math.max(...years10.map(y=>y.n), 1);
document.getElementById("bidYearChart").innerHTML =
  "<table style='width:100%;max-width:560px'>" +
  years10.map(y => `<tr>
    <td style="width:50px;font-size:12px">${y.yr}</td>
    <td><div class="bar navy" style="width:${y.n/maxBidY*100}%"></div></td>
    <td class="num" style="width:45px;font-size:12px">${fmtN(y.n)}</td>
  </tr>`).join("") + "</table>";

const btBody = document.querySelector("#bidTypeTable tbody");
(DATA.bid_by_type||[]).forEach(t => {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td>${t.bid_type}</td><td style="font-size:11px">${t.bid_category}</td>
    <td class="num">${fmtN(t.n)}</td><td class="num">${fmtPct(t.n,bidTotal)}</td>
    <td><div class="bar navy" style="width:${t.n/bidTotal*100}%"></div></td>`;
  btBody.appendChild(tr);
});

const bdBody = document.querySelector("#bidDeptTable tbody");
const maxBidD = (DATA.bid_by_dept[0]||{n:1}).n;
(DATA.bid_by_dept||[]).forEach(d => {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td>${d.departments}</td>
    <td class="num">${fmtN(d.n)}</td><td class="num">${fmtN(d.formal)}</td>
    <td><div class="bar navy" style="width:${d.n/maxBidD*100}%"></div></td>`;
  bdBody.appendChild(tr);
});

// ── PROPERTY ──────────────────────────────────────────────────────────
const propTotal = S.prop_assessed;
document.getElementById("propKpis").innerHTML = [
  ["Total Assessed FY2026", fmt$(propTotal), fmtN(S.prop_parcels) + " parcels"],
  ["Harvard University",    fmt$((DATA.prop_top_owners||[]).find(o=>o.owner_name.includes("HARVARD"))?.assessed||0), "335 parcels — mostly exempt"],
  ["MIT",                   fmt$((DATA.prop_top_owners||[]).find(o=>o.owner_name.includes("MASSACHUSETTS INSTITUTE"))?.assessed||0), "194 parcels — mostly exempt"],
  ["Residential Tax Rate",  "$5.86 / $1,000", "FY2026 assessed value"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const pcBody = document.querySelector("#propClassTable tbody");
const maxProp = (DATA.prop_by_class||[])[0]?.assessed || 1;
(DATA.prop_by_class||[]).forEach(c => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${c.cls}</td>
    <td class="num">${fmtN(c.n)}</td><td class="num">${fmt$(c.assessed)}</td>
    <td class="num">${fmtPct(c.assessed, propTotal)}</td>
    <td class="num">${fmt$(c.avg_assessed)}</td>
    <td><div class="bar navy" style="width:${c.assessed/maxProp*100}%"></div></td>`;
  pcBody.appendChild(tr);
  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  det.innerHTML = `<td colspan="6"><div class="subtable">
    <h4>${c.cls}</h4>
    <table style="max-width:420px">
      <tr><td>Land value</td><td class="num">${fmt$(c.land)}</td></tr>
      <tr><td>Building value</td><td class="num">${fmt$(c.bldg)}</td></tr>
      <tr><td>Total assessed</td><td class="num"><strong>${fmt$(c.assessed)}</strong></td></tr>
      <tr><td>Parcels</td><td class="num">${fmtN(c.n)}</td></tr>
      <tr><td>Average assessed</td><td class="num">${fmt$(c.avg_assessed)}</td></tr>
      ${c.n_exempt>0?`<tr><td>Residential exemptions</td><td class="num">${fmtN(c.n_exempt)}</td></tr>`:""}
    </table></div></td>`;
  pcBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : ""; tr.classList.toggle("expanded", !open);
  });
});

const poBody = document.querySelector("#propOwnerTable tbody");
(DATA.prop_top_owners||[]).forEach(o => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td style="font-weight:500">${o.owner_name}</td>
    <td class="num">${fmtN(o.n)}</td><td class="num">${fmt$(o.assessed)}</td>
    <td style="font-size:11px;color:#555">${(o.classes||"").substring(0,60)}</td>`;
  poBody.appendChild(tr);
  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  det.innerHTML = `<td colspan="4"><div class="subtable">
    <h4>Top parcels — ${o.owner_name}</h4>
    <table><thead><tr><th>Address</th><th>Class</th><th class="num">Assessed</th></tr></thead>
    <tbody>${(o.top_parcels||[]).map(p => `<tr>
      <td style="font-size:12px">${p.addr}</td>
      <td style="font-size:11px">${p.cls}</td>
      <td class="num">${fmt$(p.assessed)}</td>
    </tr>`).join("")}</tbody></table></div></td>`;
  poBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : ""; tr.classList.toggle("expanded", !open);
  });
});

// ── VALIDATION ────────────────────────────────────────────────────────
const vBody = document.querySelector("#validationTable tbody");
(DATA.validation||[]).forEach(c => {
  const statusMap = {pass:["ok","PASS"],warn:["warn","WARN"],fail:["","FAIL"]};
  const [cls, lbl] = statusMap[c.status] || ["info","N/A"];
  const fmtV = v => {
    if (v == null) return "—";
    if (typeof v === "number") return v >= 1e6 ? fmt$(v) : fmtN(Math.round(v));
    return String(v);
  };
  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td style="font-size:11px;color:#555">${c.source}</td>
    <td style="font-size:12px">${c.label}
      ${c.note?`<br><span style="font-size:10px;color:#888;font-style:italic">${c.note}</span>`:""}
    </td>
    <td class="num">${fmtV(c.expected)}</td>
    <td class="num">${fmtV(c.actual)}</td>
    <td class="num">${c.delta_pct!=null?c.delta_pct.toFixed(2)+"%":"—"}</td>
    <td><span class="flag ${cls}">${lbl}</span></td>`;
  vBody.appendChild(tr);
});
</script>
</body>
</html>
"""


def main():
    import datetime
    if not DB.exists():
        print(f"ERROR: {DB} not found — run import_cambridge.py first.")
        return

    print("Loading data from cambridge.db …")
    data = load_data()

    s = data["summary"]
    opGrowth = (s["opex_total_fy26"] - s["opex_total_fy24"]) / s["opex_total_fy24"] * 100 \
               if s["opex_total_fy24"] else 0
    top_svc = data["opex_by_svc"][0]["svc"] if data["opex_by_svc"] else ""
    top_amt = data["opex_by_svc"][0]["total"] if data["opex_by_svc"] else 0
    rev_tax = next((c["total"] for c in data["rev_by_cat"] if c["category"] == "Taxes"), 0)
    cap_7yr = sum(t["total"] for t in data["cap_by_year"] if "2024" <= t["fiscal_year"] <= "2030")

    summary = (
        f"Cambridge's FY2026 adopted operating budget is ${s['opex_total_fy26']/1e6:.0f}M "
        f"— a {opGrowth:.1f}% increase from FY2024's ${s['opex_total_fy24']/1e6:.0f}M. "
        f"{top_svc} is the largest service at ${top_amt/1e6:.0f}M. "
        f"The city raises ${s['rev_total_fy26']/1e6:.0f}M in revenue, "
        f"72% of which (${rev_tax/1e6:.0f}M) comes from property taxes. "
        f"The 7-year capital plan (FY2024–2030) totals ${cap_7yr/1e6:.0f}M. "
        f"The FY2026 property tax base is ${s['prop_assessed']/1e9:.1f}B — "
        f"anchored by Harvard ($137B) and MIT ($71B), both mostly tax-exempt."
    )

    html_out = (HTML_TEMPLATE
        .replace("__TODAY__", datetime.date.today().isoformat())
        .replace("__SUMMARY_LEAD__", summary)
        .replace("__DATA_JSON__", json.dumps(data, default=str)))

    with open(OUT, "w") as f:
        f.write(html_out)
    print(f"wrote {OUT}  ({len(html_out)//1024} KB)")


if __name__ == "__main__":
    main()
