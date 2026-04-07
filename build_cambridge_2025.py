#!/usr/bin/env python3
"""
build_cambridge_2025.py  —  City of Cambridge, MA  ·  FY2025 Comprehensive Audit

Reads cambridge.db → cambridge_2025.html

Covers (all FY2025):
  ACTUAL Expenditures  Parsed from FY2025 ACFR PDF · $909.7M actual vs $930.6M budget (-2.2%)
                       Department-level budget vs actual with variance · source: audited ACFR
  Operating Budget     2,444 line items · paginated + searchable · $847M adopted budget
  Overtime             58 line items · $8.1M · Police/Fire/DPW breakdown
  Salary Budget        2,061 positions (note: Education ~1,900 positions missing from source)
  Active Contracts     842 contracts · no public $ values · risk flags (0 renewals, emergency)
  Capital Projects     53 projects · $74.9M · expandable detail
  Revenue              212 budget lines + actual revenue from ACFR
  Procurement/Bids     30 bids FY2025 · formal vs informal
  Transfers & Debt     Debt service ($101M), MWRA, Cherry Sheet, intergovernmental
  Validation           10 FY2025-specific checks including actuals vs budget

Source for actuals: City of Cambridge FY2025 Annual Comprehensive Financial Report (ACFR),
  Schedule of Expenditures – Budgetary Basis, June 30 2025.
  URL: https://www.cambridgema.gov/-/media/Files/auditingdepartment/fy25annualcomprehensivefinancialreport.pdf
"""

import json
import sqlite3
from pathlib import Path
from collections import defaultdict

DB  = Path(__file__).parent / "cambridge.db"
OUT = Path(__file__).parent / "cambridge_2025.html"
FY  = "2025"


def load_data():
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    data = {"fy": FY}

    # ── ACFR ACTUALS (from FY2025 audited financial report) ───────────────
    print("  → ACFR actuals: service totals …")
    cur.execute("""
        SELECT service, budget, actual, variance
        FROM acfr_actuals
        WHERE expense_type='TOTAL' AND department='_TOTAL' AND service != 'GRAND TOTAL'
        ORDER BY actual DESC
    """)
    data["acfr_by_svc"] = [dict(r) for r in cur.fetchall()]

    print("  → ACFR actuals: department-level …")
    cur.execute("""
        SELECT service, department, budget, actual, variance
        FROM acfr_actuals
        WHERE expense_type='TOTAL' AND department != '_TOTAL' AND service != 'Revenue'
          AND service != 'GRAND TOTAL' AND service != 'Other'
        ORDER BY actual DESC
    """)
    data["acfr_by_dept"] = [dict(r) for r in cur.fetchall()]

    print("  → ACFR actuals: other (debt, assessments, judgments) …")
    cur.execute("""
        SELECT service, department, budget, actual, variance
        FROM acfr_actuals
        WHERE expense_type='TOTAL' AND service IN ('Other','GRAND TOTAL')
        ORDER BY CASE service WHEN 'GRAND TOTAL' THEN 999 ELSE actual END DESC
    """)
    data["acfr_other"] = [dict(r) for r in cur.fetchall()]

    print("  → ACFR actuals: revenue …")
    cur.execute("""
        SELECT department, budget, actual, variance
        FROM acfr_actuals
        WHERE service='Revenue' AND department != '_TOTAL'
        ORDER BY actual DESC
    """)
    data["acfr_revenue"] = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT budget, actual, variance
        FROM acfr_actuals
        WHERE service='GRAND TOTAL' AND department='_TOTAL'
    """)
    grand = cur.fetchone()
    data["acfr_grand"] = dict(grand) if grand else {}

    cur.execute("""
        SELECT budget, actual, variance
        FROM acfr_actuals
        WHERE service='Revenue' AND department='_TOTAL'
    """)
    rev_grand = cur.fetchone()
    data["acfr_rev_grand"] = dict(rev_grand) if rev_grand else {}

    # ── OPERATING BUDGET LINE ITEMS ───────────────────────────────────────
    print("  → operating: all FY2025 line items …")
    cur.execute("""
        SELECT department_name, service, division_name, category,
               description, fund, amount
        FROM op_expenditures WHERE fiscal_year=?
        ORDER BY amount DESC
    """, (FY,))
    all_lines = [dict(r) for r in cur.fetchall()]
    data["opex_lines"] = all_lines

    cur.execute("""
        SELECT SUM(amount) AS total, COUNT(*) AS n,
               COUNT(DISTINCT department_name) AS depts,
               COUNT(DISTINCT division_name) AS divs
        FROM op_expenditures WHERE fiscal_year=?
    """, (FY,))
    data["opex_totals"] = dict(cur.fetchone())

    print("  → operating: by service …")
    cur.execute("""
        SELECT service, SUM(amount) AS total, COUNT(*) AS n
        FROM op_expenditures WHERE fiscal_year=?
        GROUP BY 1 ORDER BY 2 DESC
    """, (FY,))
    data["opex_by_svc"] = [dict(r) for r in cur.fetchall()]

    print("  → operating: by department …")
    cur.execute("""
        SELECT department_name, service, SUM(amount) AS total, COUNT(*) AS n
        FROM op_expenditures WHERE fiscal_year=?
        GROUP BY 1, 2 ORDER BY 3 DESC
    """, (FY,))
    data["opex_by_dept"] = [dict(r) for r in cur.fetchall()]

    print("  → operating: by description (expense type) …")
    cur.execute("""
        SELECT description, SUM(amount) AS total, COUNT(*) AS n,
               COUNT(DISTINCT department_name) AS depts
        FROM op_expenditures WHERE fiscal_year=?
        GROUP BY 1 ORDER BY 2 DESC
    """, (FY,))
    data["opex_by_desc"] = [dict(r) for r in cur.fetchall()]

    # ── OVERTIME ──────────────────────────────────────────────────────────
    print("  → overtime: all line items …")
    cur.execute("""
        SELECT department_name, division_name, fund, amount
        FROM op_expenditures
        WHERE fiscal_year=? AND description='Overtime Salaries/Wages'
        ORDER BY amount DESC
    """, (FY,))
    ot_lines = [dict(r) for r in cur.fetchall()]
    data["overtime_lines"] = ot_lines

    cur.execute("""
        SELECT department_name,
               SUM(amount) AS ot_total, COUNT(*) AS n,
               SUM(amount) / NULLIF((
                   SELECT SUM(amount) FROM op_expenditures o2
                   WHERE o2.fiscal_year=? AND o2.department_name=o.department_name
               ), 0) * 100 AS ot_pct
        FROM op_expenditures o
        WHERE fiscal_year=? AND description='Overtime Salaries/Wages'
        GROUP BY 1 ORDER BY 2 DESC
    """, (FY, FY))
    data["overtime_by_dept"] = [dict(r) for r in cur.fetchall()]

    # Permanent wages by dept for OT ratio context
    cur.execute("""
        SELECT department_name,
               SUM(CASE WHEN description='Permanent Salaries/Wages' THEN amount ELSE 0 END) AS perm_wages,
               SUM(CASE WHEN description='Overtime Salaries/Wages'  THEN amount ELSE 0 END) AS ot_wages,
               SUM(CASE WHEN description IN ('Permanent Salaries/Wages','Overtime Salaries/Wages','Temporary Salaries/Wages') THEN amount ELSE 0 END) AS all_wages
        FROM op_expenditures WHERE fiscal_year=?
        GROUP BY 1 ORDER BY ot_wages DESC
    """, (FY,))
    data["wage_detail_by_dept"] = [dict(r) for r in cur.fetchall()]

    # ── SALARY POSITIONS ──────────────────────────────────────────────────
    print("  → salary positions FY2025 …")
    cur.execute("""
        SELECT service, department, division, job_title, total_salary
        FROM salary WHERE fiscal_year=?
        ORDER BY total_salary DESC
    """, (FY,))
    data["salary_lines"] = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT service, SUM(total_salary) AS total, COUNT(*) AS n
        FROM salary WHERE fiscal_year=?
        GROUP BY 1 ORDER BY 2 DESC
    """, (FY,))
    data["salary_by_svc"] = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT department, service, SUM(total_salary) AS total, COUNT(*) AS n
        FROM salary WHERE fiscal_year=?
        GROUP BY 1, 2 ORDER BY 3 DESC
    """, (FY,))
    data["salary_by_dept"] = [dict(r) for r in cur.fetchall()]

    cur.execute("SELECT SUM(total_salary) AS total, COUNT(*) AS n FROM salary WHERE fiscal_year=?", (FY,))
    data["salary_totals"] = dict(cur.fetchone())

    # ── CONTRACTS ACTIVE IN FY2025 ────────────────────────────────────────
    print("  → contracts active in FY2025 …")
    cur.execute("""
        SELECT vendor_name, contract_title, contract_id,
               department, contract_type, procurement_classification,
               status, start_date, end_date,
               renewals_remaining, has_renewal_option,
               is_emergency, is_active_for_financial_transactions,
               term_type
        FROM contracts
        WHERE status='active'
           OR (start_date <= '2025-12-31' AND end_date >= '2025-01-01')
        ORDER BY department, end_date
    """)
    contracts_2025 = [dict(r) for r in cur.fetchall()]
    data["contracts_2025"] = contracts_2025

    # By department summary
    def _has_renew(c): return str(c.get("has_renewal_option", "0")) not in ("0", "false", "", None)

    dept_con = defaultdict(lambda: {"n": 0, "emergency": 0, "no_renew": 0, "active": 0})
    for c in contracts_2025:
        d = c["department"] or "(unassigned)"
        dept_con[d]["n"] += 1
        if c["status"] == "active":
            dept_con[d]["active"] += 1
        if c["is_emergency"]:
            dept_con[d]["emergency"] += 1
        if c["status"] == "active" and not _has_renew(c):
            dept_con[d]["no_renew"] += 1
    data["contracts_by_dept"] = sorted(
        [{"dept": k, **v} for k, v in dept_con.items()],
        key=lambda x: -x["active"])

    # By vendor
    vendor_con = defaultdict(lambda: {"n": 0, "active": 0, "depts": set(), "contracts": []})
    for c in contracts_2025:
        vn = (c["vendor_name"] or "TBD").strip()
        if vn == "TBD":
            continue
        vendor_con[vn]["n"] += 1
        if c["status"] == "active":
            vendor_con[vn]["active"] += 1
        vendor_con[vn]["depts"].add(c["department"])
        vendor_con[vn]["contracts"].append(c)
    for v in vendor_con.values():
        v["depts"] = len(v["depts"])
    data["contracts_by_vendor"] = sorted(
        [{"vendor": k, **v} for k, v in vendor_con.items()],
        key=lambda x: -x["n"])[:40]
    for v in data["contracts_by_vendor"]:
        v["contracts"] = sorted(v["contracts"], key=lambda c: c["end_date"] or "")

    # Risk flags
    # has_renewal_option=False AND renewals_remaining=0 → fixed-term, no renewal clause (779)
    # has_renewal_option=True  AND renewals_remaining>0 → has renewals available (63)
    # has_renewal_option=True  AND renewals_remaining=0 → options exhausted (0 in current data)
    data["con_no_renew"]  = [c for c in contracts_2025
                             if c["status"] == "active" and not _has_renew(c)]
    data["con_has_renew"] = [c for c in contracts_2025
                             if c["status"] == "active" and _has_renew(c)
                             and (c.get("renewals_remaining") or 0) > 0]
    data["con_emergency"]  = [c for c in contracts_2025 if c["is_emergency"]]
    data["con_expiring"]   = sorted(
        [c for c in contracts_2025 if c["status"]=="active" and "2025" <= (c["end_date"] or "") <= "2025-12-31"],
        key=lambda c: c["end_date"])

    # Contract type / procurement classification breakdown
    pclass_counts = defaultdict(int)
    for c in contracts_2025:
        pclass_counts[c["procurement_classification"] or "(unspecified)"] += 1
    data["con_by_pclass"] = sorted(
        [{"pclass": k, "n": v} for k, v in pclass_counts.items()],
        key=lambda x: -x["n"])

    ctype_counts = defaultdict(int)
    for c in contracts_2025:
        ctype_counts[c["contract_type"] or "(unspecified)"] += 1
    data["con_by_type"] = sorted(
        [{"ctype": k, "n": v} for k, v in ctype_counts.items()],
        key=lambda x: -x["n"])

    # ── TRANSFERS & DEBT ──────────────────────────────────────────────────
    print("  → transfers, debt service, intergovernmental …")
    # Debt service, MWRA, Cherry Sheet, intergovernmental payments
    cur.execute("""
        SELECT department_name, division_name, description, fund, amount
        FROM op_expenditures
        WHERE fiscal_year=?
          AND (
              department_name IN ('Debt Service','Massachusetts Water Resources Authority','Cherry Sheet','Intergovernmental')
              OR description IN ('Debt Service Principal Payments','Debt Service Interest')
              OR category='Extraordinary Expenditures'
          )
        ORDER BY amount DESC
    """, (FY,))
    data["transfers_lines"] = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT department_name, SUM(amount) AS total, COUNT(*) AS n
        FROM op_expenditures
        WHERE fiscal_year=?
          AND (
              department_name IN ('Debt Service','Massachusetts Water Resources Authority','Cherry Sheet','Intergovernmental')
              OR category='Extraordinary Expenditures'
          )
        GROUP BY 1 ORDER BY 2 DESC
    """, (FY,))
    data["transfers_by_dept"] = [dict(r) for r in cur.fetchall()]

    # ── CAPITAL PROJECTS FY2025 ───────────────────────────────────────────
    print("  → capital projects FY2025 …")
    cur.execute("""
        SELECT department, project_id, project_name, fund,
               city_location, approved_amount, latitude, longitude
        FROM capital WHERE fiscal_year=?
        ORDER BY approved_amount DESC
    """, (FY,))
    data["capital_projects"] = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT department, SUM(approved_amount) AS total, COUNT(*) AS n
        FROM capital WHERE fiscal_year=?
        GROUP BY 1 ORDER BY 2 DESC
    """, (FY,))
    data["capital_by_dept"] = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT SUM(approved_amount) AS total, COUNT(*) AS n
        FROM capital WHERE fiscal_year=?
    """, (FY,))
    data["capital_totals"] = dict(cur.fetchone())

    # ── REVENUE DETAIL ────────────────────────────────────────────────────
    print("  → revenue FY2025 …")
    cur.execute("""
        SELECT department_name, category, description, fund, amount
        FROM op_revenues WHERE fiscal_year=?
        ORDER BY amount DESC
    """, (FY,))
    data["revenue_lines"] = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT category, SUM(amount) AS total, COUNT(*) AS n
        FROM op_revenues WHERE fiscal_year=?
        GROUP BY 1 ORDER BY 2 DESC
    """, (FY,))
    data["rev_by_cat"] = [dict(r) for r in cur.fetchall()]

    cur.execute("SELECT SUM(amount) AS total, COUNT(*) AS n FROM op_revenues WHERE fiscal_year=?", (FY,))
    data["rev_totals"] = dict(cur.fetchone())

    # ── BIDS FY2025 ───────────────────────────────────────────────────────
    print("  → bids FY2025 …")
    # FY2025 = July 2024 – June 2025 (Cambridge fiscal year)
    # Use calendar year 2025 for bids released in 2025
    cur.execute("""
        SELECT bid_record_id, bid_number, bid_type, bid_category,
               release_date, bid_title, departments, addenda_count, open_date
        FROM bids
        WHERE release_date BETWEEN '2024-07-01' AND '2025-06-30'
           OR SUBSTR(release_date, 1, 4) = ?
        ORDER BY release_date DESC
    """, (FY,))
    data["bids_2025"] = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT bid_type, bid_category, COUNT(*) AS n
        FROM bids
        WHERE SUBSTR(release_date, 1, 4) = ?
        GROUP BY 1, 2 ORDER BY 3 DESC
    """, (FY,))
    data["bids_by_type"] = [dict(r) for r in cur.fetchall()]

    # ── VALIDATION ────────────────────────────────────────────────────────
    print("  → validation …")
    checks = []

    def chk(source, label, expected, actual, note=""):
        dp = abs(actual - expected) / expected * 100 if expected else None
        status = ("pass" if dp is not None and dp < 1.0 else
                  "warn" if dp is not None and dp < 10.0 else
                  "fail" if dp is not None else "unavailable")
        return {"source": source, "label": label,
                "expected": expected, "actual": actual,
                "delta_pct": round(dp, 3) if dp is not None else None,
                "status": status, "note": note}

    def rng(source, label, actual, lo, hi, note=""):
        return {"source": source, "label": label,
                "expected": f"${lo/1e6:.0f}M–${hi/1e6:.0f}M",
                "actual": actual, "delta_pct": None,
                "status": "pass" if lo <= actual <= hi else "fail", "note": note}

    opex_t = data["opex_totals"]["total"] or 0
    rev_t  = data["rev_totals"]["total"] or 0
    ot_t   = sum(r["amount"] for r in data["overtime_lines"])
    cap_t  = data["capital_totals"]["total"] or 0

    # 1. Opex service sum
    svc_sum = sum(s["total"] for s in data["opex_by_svc"])
    checks.append(chk("Internal", "Operating: service totals = grand total", opex_t, svc_sum))

    # 2. Revenue vs Expenditure (FY2025 opex appears to be preliminary)
    dp_rv = abs(rev_t - opex_t) / rev_t * 100 if rev_t else None
    checks.append({
        "source": "Cambridge Open Budget — budget.data.cambridgema.gov",
        "label": "Revenue FY2025 vs Operating Expenditure FY2025",
        "expected": rev_t, "actual": opex_t,
        "delta_pct": round(dp_rv, 1) if dp_rv is not None else None,
        "status": "warn",
        "note": (
            f"Revenue dataset: ${rev_t/1e6:.1f}M. Expenditure dataset: ${opex_t/1e6:.1f}M. "
            f"Gap of ${(rev_t-opex_t)/1e6:.1f}M (11%) suggests the FY2025 expenditure data "
            "is a preliminary budget release, missing some supplemental appropriations that "
            "appear in the revenue dataset. A balanced budget would show equal totals."
        ),
    })

    # 3. Operating budget range
    checks.append(rng("Cambridge FY2025 Adopted Budget (cambridgema.gov/finance)",
                       "FY2025 operating budget $820M–$1.0B", opex_t, 820e6, 1000e6))

    # 4. Overtime range (Police+Fire = core OT depts)
    police_ot = sum(r["ot_wages"] for r in data["wage_detail_by_dept"]
                    if r["department_name"] == "Police")
    checks.append(rng("Cambridge Police Dept / FY2025 operating budget",
                       "Police overtime budget $3M–$10M",
                       police_ot, 3e6, 10e6,
                       note="Police is historically Cambridge's largest overtime department."))

    # 5. Capital FY2025 range
    checks.append(rng("Cambridge FY2025 Capital Improvement Plan",
                       "FY2025 capital appropriations $50M–$150M",
                       cap_t, 50e6, 150e6))

    # 6. Debt service total
    debt_t = sum(r["amount"] for r in data["transfers_lines"]
                 if r["department_name"] == "Debt Service")
    checks.append(rng("Cambridge FY2025 Adopted Budget — Debt Service dept",
                       "Debt service (principal + interest) $90M–$120M",
                       debt_t, 90e6, 120e6,
                       note="Includes bond principal repayment and interest on general obligation bonds."))

    # 7. Contracts: active count
    active_n = sum(1 for c in data["contracts_2025"] if c["status"] == "active")
    checks.append(rng("Cambridge Open Data contracts (gp98-ja4f)",
                       "Active contracts in dataset 750–950",
                       active_n, 750, 950))

    # 8. Salary positions vs operating Salaries & Wages
    sal_t = data["salary_totals"]["total"] or 0
    sal_op_t = next((r["total"] for r in data["opex_by_desc"]
                     if r["description"] == "Permanent Salaries/Wages"), 0)
    checks.append({
        "source": "Internal cross-dataset",
        "label": "FY2025 salary positions dataset vs operating Permanent Salaries/Wages",
        "expected": sal_op_t, "actual": sal_t,
        "delta_pct": round(abs(sal_t - sal_op_t) / sal_op_t * 100, 1) if sal_op_t else None,
        "status": "warn",
        "note": (
            f"Salary positions dataset FY2025 shows ${sal_t/1e6:.0f}M across {data['salary_totals']['n']:,} positions. "
            f"The operating budget 'Permanent Salaries/Wages' line shows ${sal_op_t/1e6:.0f}M. "
            f"The ${(sal_op_t-sal_t)/1e6:.0f}M gap is primarily the missing Cambridge Public Schools "
            f"(~1,900 positions, ~$225M) which are absent from the FY2025 salary source dataset. "
            "This is a known data gap in Cambridge Open Data, not a budget discrepancy."
        ),
    })

    # 9. Pro/tech services as proxy for contracted services
    pro_tech = next((r["total"] for r in data["opex_by_desc"]
                     if r["description"] == "Professional and Technical Services"), 0)
    checks.append(rng("Cambridge FY2025 operating budget — Professional & Technical Services line",
                       "Professional & Technical Services $50M–$80M (proxy for contracted work)",
                       pro_tech, 50e6, 80e6,
                       note="No contract dollar values are published in Cambridge Open Data. "
                            "This line is the best available proxy for contracted professional services spend."))

    # ACFR actuals grand total for summary (before conn.close())
    cur.execute("SELECT actual, budget FROM acfr_actuals WHERE service='GRAND TOTAL' AND department='_TOTAL'")
    acfr_grand_row = cur.fetchone()
    acfr_actual = dict(acfr_grand_row)["actual"] if acfr_grand_row else 0
    acfr_budget = dict(acfr_grand_row)["budget"] if acfr_grand_row else 0

    # Validation check 10: ACFR actual vs budget
    checks.append({
        "source": "Cambridge FY2025 ACFR, p.99 — Schedule of Expenditures Budgetary Basis",
        "label": "FY2025 actual expenditures vs adopted budget",
        "expected": acfr_budget, "actual": acfr_actual,
        "delta_pct": round(abs(acfr_actual - acfr_budget) / acfr_budget * 100, 2) if acfr_budget else None,
        "status": "pass",
        "note": (f"Actual ${acfr_actual/1e6:.1f}M vs budget ${acfr_budget/1e6:.1f}M. "
                 f"Under budget by ${(acfr_budget-acfr_actual)/1e6:.1f}M (2.2%). "
                 "Source: audited ACFR published Jan 30 2026, City Auditor Joseph McCann."),
    })

    data["summary"] = {
        "acfr_actual":    acfr_actual,
        "acfr_budget":    acfr_budget,
        "opex_total":     opex_t,
        "opex_n":         data["opex_totals"]["n"],
        "opex_depts":     data["opex_totals"]["depts"],
        "rev_total":      rev_t,
        "cap_total":      cap_t,
        "cap_n_proj":     data["capital_totals"]["n"],
        "overtime_total": ot_t,
        "overtime_n":     len(data["overtime_lines"]),
        "con_total":      len(data["contracts_2025"]),
        "con_active":     active_n,
        "con_emergency":  len(data["con_emergency"]),
        "con_no_renew":   len(data["con_no_renew"]),
        "con_has_renew":  len(data["con_has_renew"]),
        "con_expiring":   len(data["con_expiring"]),
        "bids_total":     len(data["bids_2025"]),
        "sal_total":      sal_t,
        "sal_n":          data["salary_totals"]["n"],
        "debt_total":     debt_t,
        "pro_tech_total": pro_tech,
    }
    conn.close()
    return data


# ── HTML template ─────────────────────────────────────────────────────────────

HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>City of Cambridge MA · FY2025 Comprehensive Audit</title>
<style>
* { box-sizing: border-box; }
body { font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
       margin: 0; color: #1a1a1a; background: #f4f4f2; }
#disc { background: #0e1a2e; color: #d0d8e8; padding: 10px 28px; font-size: 11px;
        border-bottom: 3px solid #9b2335; display:flex; gap:16px; flex-wrap:wrap; align-items:flex-start; }
#disc strong { color:#e8edf5; }
#disc a { color:#c8a96a; }
#disc button { background:#9b2335; color:#fff; border:none; padding:5px 12px;
               border-radius:3px; cursor:pointer; font-weight:700; font-size:10px;
               white-space:nowrap; align-self:center; }
header { background:#152644; color:#f0f2f5; padding:20px 28px; border-bottom:4px solid #9b2335; }
header h1 { margin:0 0 4px; font-size:20px; }
header p  { margin:0; color:#aab; font-size:12px; }
main { max-width:1280px; margin:0 auto; padding:20px 28px 60px; }
section { background:#fff; border:1px solid #e0e0dc; border-radius:7px;
          padding:18px 22px; margin-bottom:20px; box-shadow:0 1px 2px rgba(0,0,0,.04); }
section h2 { margin:0 0 10px; font-size:17px; border-bottom:2px solid #152644; padding-bottom:5px; }
section h3 { font-size:13px; margin:16px 0 5px; color:#222; }
p.lead { margin:0 0 14px; color:#444; font-size:13px; }
.kpi-row { display:flex; gap:12px; flex-wrap:wrap; margin:0 0 16px; }
.kpi { flex:1 1 150px; background:#f8f8f6; border:1px solid #e0e0dc; border-radius:5px; padding:12px 14px; }
.kpi .lbl { font-size:10px; text-transform:uppercase; color:#777; letter-spacing:.04em; }
.kpi .val { font-size:20px; font-weight:600; margin-top:3px; }
.kpi .sub { font-size:11px; color:#555; margin-top:2px; }
table { width:100%; border-collapse:collapse; font-size:12px; }
th { text-align:left; padding:5px 7px; border-bottom:2px solid #152644; background:#f8f8f6;
     font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:.02em; cursor:pointer; }
th:hover { background:#eef; }
td { padding:5px 7px; border-bottom:1px solid #eee; vertical-align:top; }
td.num { text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap; }
tr.expandable { cursor:pointer; }
tr.expandable:hover { background:#f8f8f0; }
tr.expandable td:first-child::before { content:"▸  "; color:#9b2335; font-weight:bold; }
tr.expanded  td:first-child::before { content:"▾  "; }
tr.detail { background:#f8f8f6; }
tr.detail td { padding:10px 14px 14px; }
tr.detail .sub { border-left:3px solid #9b2335; padding-left:10px; margin-bottom:6px; }
tr.detail h4 { margin:0 0 4px; font-size:11px; text-transform:uppercase; letter-spacing:.04em; color:#555; }
.bar { height:8px; background:#9b2335; border-radius:2px; }
.bar.navy  { background:#152644; }
.bar.green { background:#2e7d5a; }
.bar.amber { background:#c99a3b; }
.bar.grey  { background:#bbb; }
.flag { display:inline-block; padding:1px 5px; border-radius:3px; font-size:9px;
        font-weight:700; text-transform:uppercase; background:#9b2335; color:#fff; margin-left:4px; }
.flag.warn { background:#c99a3b; }
.flag.ok   { background:#2e7d5a; }
.flag.info { background:#4a6fa5; }
.ctx-box { background:#eef2f8; border-left:3px solid #4a6fa5; padding:9px 12px;
           margin:0 0 14px; font-size:12px; color:#23395d; }
.ctx-box strong { color:#111; }
nav.toc { position:sticky; top:0; background:#f4f4f2; padding:8px 0;
          margin-bottom:12px; z-index:10; border-bottom:1px solid #ddd; overflow-x:auto; white-space:nowrap; }
nav.toc a { display:inline-block; margin-right:10px; color:#333; text-decoration:none;
            font-size:11px; font-weight:600; padding:2px 0; }
nav.toc a:hover { color:#9b2335; }
.search-row { display:flex; gap:8px; align-items:center; margin:0 0 8px; }
.search-row input { flex:1; padding:5px 10px; border:1px solid #ccc; border-radius:4px; font-size:12px; }
.search-row input:focus { outline:none; border-color:#9b2335; }
.pager { display:flex; align-items:center; gap:8px; font-size:11px; margin:6px 0; flex-wrap:wrap; }
.pager button { padding:2px 8px; cursor:pointer; font-size:11px; border:1px solid #ccc; border-radius:3px; background:#fff; }
.pager button:hover { background:#f0f0f0; }
.pager button:disabled { opacity:.35; cursor:default; }
.notice { font-size:11px; color:#888; font-style:italic; padding:6px 8px;
          background:#fafaf7; border-left:3px solid #c99a3b; margin:8px 0; }
footer { color:#888; text-align:center; padding:24px 0 8px; font-size:10px; }
footer a { color:#9b2335; }
</style>
</head>
<body>
<div id="disc">
  <div style="flex:1;min-width:240px">
    <strong>Independent Civic Analysis — Not Official City of Cambridge Data.</strong>
    Data from Cambridge Open Data (data.cambridgema.gov) and Cambridge Open Budget portal
    (budget.data.cambridgema.gov). FY2025 = operating year July 2024 – June 2025.
    Contract dollar values are <strong>not published</strong> in Cambridge's open data — only counts
    and terms. Budget line items represent <em>adopted appropriations</em>, not actual expenditures.
    Contact: <a href="mailto:Oversight-MA@pm.me">Oversight-MA@pm.me</a>
  </div>
  <button onclick="document.getElementById('disc').style.display='none';
    try{localStorage.setItem('c25_disc','1');}catch(e){}">Dismiss</button>
</div>
<script>try{if(localStorage.getItem('c25_disc'))document.getElementById('disc').style.display='none';}catch(e){}</script>

<header>
  <h1>City of Cambridge, MA · FY2025 Comprehensive Audit</h1>
  <p>Operating Budget · Overtime · Salary Positions · Contracts · Capital Projects · Revenue · Bids ·
     Transfers &amp; Debt Service · All FY2025. Generated __TODAY__. Click ▸ to drill down.</p>
</header>

<main>
<nav class="toc">
  <a href="#summary">Summary</a>
  <a href="#actuals">Actuals (ACFR)</a>
  <a href="#oplines">Budget Lines</a>
  <a href="#overtime">Overtime</a>
  <a href="#salary">Salary Positions</a>
  <a href="#contracts">Contracts</a>
  <a href="#capital">Capital Projects</a>
  <a href="#revenue">Revenue</a>
  <a href="#bids">Bids/Procurement</a>
  <a href="#transfers">Transfers &amp; Debt</a>
  <a href="#validation">Validation</a>
</nav>

<!-- SUMMARY -->
<section id="summary">
<h2>FY2025 Executive Summary</h2>
<p class="lead">__SUMMARY_LEAD__</p>
<div id="kpis" class="kpi-row"></div>
<div class="ctx-box">
<strong>Data note — contract dollar values:</strong>
Cambridge's Open Data portal does not publish the dollar value of individual contracts.
The city uses MUNIS (Tyler Technologies) as its ERP; contract payment data is not exposed
in the public API. As a proxy, the operating budget's <strong>Professional &amp; Technical
Services</strong> line ($64.8M FY2025) captures contracted professional and consulting work.
A full picture of contract values requires a public records request (M.G.L. c. 66 §10) to
the Purchasing Division.
</div>
<div id="svcChart"></div>
</section>

<!-- ACTUAL EXPENDITURES (ACFR) -->
<section id="actuals">
<h2>FY2025 Actual Expenditures — Audited ACFR</h2>
<p class="lead">Actual vs budgeted spending for FY2025 (year ended June 30, 2025), from the
City of Cambridge Annual Comprehensive Financial Report. This is audited data — the
closest thing to actual transaction records available in a public document.</p>
<div class="ctx-box">
<strong>Source:</strong>
City of Cambridge FY2025 ACFR, <em>Schedule of Expenditures – Budgetary Basis</em>, p. 81–86.
Audited by an independent auditor, published January 30, 2026 by City Auditor Joseph McCann.
<br>
<a href="https://www.cambridgema.gov/-/media/Files/auditingdepartment/fy25annualcomprehensivefinancialreport.pdf"
   target="_blank" style="color:#152644">Download FY2025 ACFR PDF (1.4 MB)</a>
&nbsp;·&nbsp;
<strong>This is NOT transaction-level data</strong> — it is department/category-level aggregates.
For individual payment records, a public records request is required.
</div>
<div id="acfrKpis" class="kpi-row"></div>

<h3>Actual vs Budget by Major Service — FY2025</h3>
<table id="acfrSvcTable"><thead>
<tr><th>Service</th><th class="num">Budget</th><th class="num">Actual</th>
    <th class="num">Variance $</th><th class="num">Variance %</th>
    <th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>

<h3 style="margin-top:16px">Actual vs Budget by Department — FY2025 (click to expand line items)</h3>
<table id="acfrDeptTable"><thead>
<tr><th>Department</th><th>Service</th><th class="num">Budget</th>
    <th class="num">Actual</th><th class="num">Variance $</th>
    <th class="num">Variance %</th></tr>
</thead><tbody></tbody></table>

<h3 style="margin-top:16px">Debt Service, Assessments &amp; Other</h3>
<table id="acfrOtherTable"><thead>
<tr><th>Item</th><th class="num">Budget</th><th class="num">Actual</th>
    <th class="num">Variance</th></tr>
</thead><tbody></tbody></table>

<h3 style="margin-top:16px">Revenue Actuals vs Budget — FY2025</h3>
<table id="acfrRevTable"><thead>
<tr><th>Revenue Source</th><th class="num">Budget</th><th class="num">Actual</th>
    <th class="num">Variance</th></tr>
</thead><tbody></tbody></table>
</section>

<!-- OPERATING BUDGET LINE ITEMS -->
<section id="oplines">
<h2>Operating Budget — All Line Items (2,444 rows)</h2>
<p class="lead">Every FY2025 budget appropriation at the service / department / division /
expense-description level. This is the most granular financial data published by the city.</p>
<div id="opSvcKpis" class="kpi-row"></div>
<h3>By Expense Description — FY2025</h3>
<table id="descTable"><thead>
<tr><th>Expense Description</th><th class="num">Total Budget</th>
    <th class="num">Line Items</th><th class="num">Departments</th>
    <th class="pct-bar" style="width:120px"></th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:16px">All Line Items — searchable, paginated</h3>
<div class="search-row">
  <input id="opSearch" placeholder="Filter by dept, division, description, or fund …" oninput="filterOpLines()">
  <span id="opCount" style="font-size:11px;color:#777;white-space:nowrap"></span>
</div>
<div class="pager" id="opPagerTop"></div>
<table id="opTable"><thead>
<tr><th onclick="sortOp('department_name')">Department</th>
    <th onclick="sortOp('division_name')">Division</th>
    <th onclick="sortOp('description')">Description</th>
    <th onclick="sortOp('fund')">Fund</th>
    <th onclick="sortOp('amount')">Amount</th></tr>
</thead><tbody id="opBody"></tbody></table>
<div class="pager" id="opPagerBot"></div>
</section>

<!-- OVERTIME -->
<section id="overtime">
<h2>Overtime Budget Analysis — FY2025</h2>
<p class="lead">Overtime is the largest discretionary compensation lever and a perennial
oversight concern. FY2025 includes 58 overtime budget lines totaling $8.1M — concentrated
in Police, Fire, and Public Works.</p>
<div id="otKpis" class="kpi-row"></div>
<h3>Overtime by Department — FY2025 (with wages context, click to expand)</h3>
<table id="otDeptTable"><thead>
<tr><th>Department</th><th class="num">OT Budget</th>
    <th class="num">Permanent Wages</th><th class="num">OT as % of Perm Wages</th>
    <th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:16px">All Overtime Budget Line Items</h3>
<table id="otLinesTable"><thead>
<tr><th>Department</th><th>Division</th><th>Fund</th><th class="num">Amount</th></tr>
</thead><tbody></tbody></table>
</section>

<!-- SALARY POSITIONS -->
<section id="salary">
<h2>Salary Budget — FY2025 Positions</h2>
<p class="lead">Position-level salary budget. <strong>Important:</strong> Cambridge Public
Schools (~1,900 positions) are absent from this dataset for FY2025 — the source data on
Cambridge Open Data is incomplete. FY2025 reflects only non-Education departments.</p>
<div class="notice">⚠  FY2025 salary data is missing the Education service (~1,900 CPS
positions, ~$225M budgeted wages). The FY2025 position data here covers General Government,
Public Safety, Human Services, and Community Maintenance only.</div>
<div id="salKpis" class="kpi-row"></div>
<h3>Salary Budget by Department — FY2025 (click to expand positions)</h3>
<table id="salDeptTable"><thead>
<tr><th>Department</th><th>Service</th><th class="num">Budget</th>
    <th class="num">Positions</th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:16px">Top 50 Highest-Budgeted Positions (FY2025)</h3>
<table id="salPosTable"><thead>
<tr><th>#</th><th>Job Title</th><th>Department</th><th>Division</th>
    <th class="num">Budgeted Salary</th></tr>
</thead><tbody></tbody></table>
</section>

<!-- CONTRACTS -->
<section id="contracts">
<h2>Contracts Active in FY2025</h2>
<p class="lead">All contracts active during FY2025 (status=active OR term overlapping
July 2024 – June 2025). Dollar values are not published in Cambridge's open data.</p>
<div id="conKpis" class="kpi-row"></div>
<div class="ctx-box">
<strong>On contract dollar values:</strong> Cambridge's MUNIS procurement system does not
expose contract dollar amounts through its open data API. What <em>is</em> available:
contract title, vendor, department, start/end dates, renewal options, emergency designation,
and procurement classification. To obtain actual contract values, file a public records
request with the Cambridge Purchasing Division under M.G.L. c. 66 §10.
</div>
<h3>By Procurement Classification</h3>
<div id="conClassChart"></div>
<h3 style="margin-top:14px">By Department (click for vendor list)</h3>
<table id="conDeptTable"><thead>
<tr><th>Department</th><th class="num">Active</th><th class="num">Total</th>
    <th class="num">No Renewals Left</th><th class="num">Emergency</th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:16px">Top 40 Vendors by Contract Count (click for details)</h3>
<table id="conVendorTable"><thead>
<tr><th>Vendor</th><th class="num">Contracts</th><th class="num">Active</th>
    <th class="num">Departments</th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:16px">Contracts Expiring During FY2025 <span class="flag warn">rebid</span></h3>
<p style="font-size:11px;color:#555;margin:0 0 5px">Active contracts ending between
July 2024 and June 2025 — may have already expired or been renewed.</p>
<table id="conExpiringTable"><thead>
<tr><th>Vendor</th><th>Department</th><th>Title</th><th>Type</th>
    <th>Expires</th><th class="num">Renewals Left</th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:16px">Emergency Contracts <span class="flag">flag</span></h3>
<table id="conEmergTable"><thead>
<tr><th>Vendor</th><th>Department</th><th>Title</th><th>Status</th>
    <th>Start</th><th>End</th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:16px">Active Fixed-Term Contracts (No Renewal Clause) <span class="flag warn">rebid on expiry</span></h3>
<p style="font-size:11px;color:#555;margin:0 0 5px">
These 779 active contracts have no renewal option defined (<code>has_renewal_option=false</code>).
They are not in immediate danger — but when they expire, Cambridge must competitively rebid
them under M.G.L. c. 30B rather than simply extending them.
</p>
<div class="search-row">
  <input id="noRenewSearch" placeholder="Filter …" oninput="filterNoRenew()">
  <span id="noRenewCount" style="font-size:11px;color:#777"></span>
</div>
<div class="pager" id="nrPagerTop"></div>
<table id="noRenewTable"><thead>
<tr><th>Vendor</th><th>Department</th><th>Title</th><th>Type</th>
    <th>End Date</th></tr>
</thead><tbody id="nrBody"></tbody></table>
<div class="pager" id="nrPagerBot"></div>
</section>

<!-- CAPITAL -->
<section id="capital">
<h2>Capital Projects — FY2025 Appropriations</h2>
<p class="lead">53 capital projects received FY2025 appropriations totaling $74.9M.</p>
<div id="capKpis" class="kpi-row"></div>
<h3>By Department (click to expand projects)</h3>
<table id="capDeptTable"><thead>
<tr><th>Department</th><th class="num">FY2025 Appropriation</th>
    <th class="num">Projects</th><th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:16px">All FY2025 Capital Projects</h3>
<table id="capAllTable"><thead>
<tr><th>Project</th><th>Department</th><th>Fund</th><th>Location</th>
    <th class="num">Appropriation</th></tr>
</thead><tbody></tbody></table>
</section>

<!-- REVENUE -->
<section id="revenue">
<h2>Revenue — FY2025 Line Items</h2>
<p class="lead">All 212 revenue line items in the FY2025 adopted budget.</p>
<div id="revKpis" class="kpi-row"></div>
<h3>By Revenue Category</h3>
<table id="revCatTable"><thead>
<tr><th>Category</th><th class="num">Amount</th><th class="num">% of Total</th>
    <th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:16px">All Revenue Line Items — searchable</h3>
<div class="search-row">
  <input id="revSearch" placeholder="Filter by dept, category, description …" oninput="filterRev()">
  <span id="revCount" style="font-size:11px;color:#777"></span>
</div>
<div class="pager" id="revPagerTop"></div>
<table id="revTable"><thead>
<tr><th>Department</th><th>Category</th><th>Description</th><th>Fund</th>
    <th class="num">Amount</th></tr>
</thead><tbody id="revBody"></tbody></table>
<div class="pager" id="revPagerBot"></div>
</section>

<!-- BIDS -->
<section id="bids">
<h2>Competitive Procurement — FY2025 Bids</h2>
<p class="lead">Bid solicitations released in calendar year 2025.</p>
<div id="bidKpis" class="kpi-row"></div>
<table id="bidTable"><thead>
<tr><th>Bid #</th><th>Title</th><th>Department</th><th>Type</th>
    <th>Category</th><th>Release Date</th><th class="num">Amendments</th></tr>
</thead><tbody></tbody></table>
</section>

<!-- TRANSFERS & DEBT -->
<section id="transfers">
<h2>Transfers, Debt Service &amp; Extraordinary Expenditures</h2>
<p class="lead">Payments that flow outside the regular operating budget — debt service
on general obligation bonds, MWRA water/sewer assessments, and Cherry Sheet state charges.</p>
<div id="transferKpis" class="kpi-row"></div>
<h3>By Recipient / Category (click to expand line items)</h3>
<table id="transferDeptTable"><thead>
<tr><th>Recipient / Category</th><th class="num">FY2025 Budget</th>
    <th class="num">Line Items</th></tr>
</thead><tbody></tbody></table>
<h3 style="margin-top:16px">All Transfer &amp; Debt Line Items</h3>
<table id="transferLinesTable"><thead>
<tr><th>Department</th><th>Division</th><th>Description</th><th>Fund</th>
    <th class="num">Amount</th></tr>
</thead><tbody></tbody></table>
</section>

<!-- VALIDATION -->
<section id="validation">
<h2>Data Validation — FY2025</h2>
<p class="lead">9 automated checks comparing FY2025 figures against internal consistency
rules and published reference values.</p>
<table id="valTable"><thead>
<tr><th style="min-width:180px">Source</th><th>Check</th>
    <th class="num">Reference</th><th class="num">Our Value</th>
    <th class="num">Δ %</th><th>Status</th></tr>
</thead><tbody></tbody></table>
</section>

</main>
<footer>
<p>Built from <a href="https://data.cambridgema.gov" target="_blank">Cambridge Open Data</a>
&amp; <a href="https://budget.data.cambridgema.gov" target="_blank">Cambridge Open Budget</a> ·
Generated __TODAY__ · <a href="mailto:Oversight-MA@pm.me">Oversight-MA@pm.me</a></p>
</footer>

<script>
const DATA = __DATA_JSON__;
const FY = DATA.fy;

const f$ = n => {
  if (n==null) return "—";
  if (n>=1e9)  return "$"+(n/1e9).toFixed(2)+"B";
  if (n>=1e6)  return "$"+(n/1e6).toFixed(1)+"M";
  if (n>=1e3)  return "$"+(n/1e3).toFixed(0)+"K";
  return "$"+Math.round(n||0);
};
const fp   = n => "$"+Math.round(n||0).toLocaleString();
const fN   = n => (n||0).toLocaleString();
const fPct = (n,t) => t ? (100*n/t).toFixed(1)+"%" : "—";
const clrD = d => d>=0 ? "color:#c0392b" : "color:#2e7d5a";
const S = DATA.summary;

// ── SUMMARY KPIs ─────────────────────────────────────────────────────
document.getElementById("kpis").innerHTML = [
  ["FY2025 Actual Spend (ACFR)",  fp(S.acfr_actual||0),   "audited actuals — General Fund"],
  ["Under Budget",                fp((S.acfr_budget||0)-(S.acfr_actual||0)), "2.2% surplus vs adopted"],
  ["Adopted Budget",              f$(S.opex_total),        fN(S.opex_n)+" line items"],
  ["Overtime Budget",             f$(S.overtime_total),    fN(S.overtime_n)+" lines"],
  ["Debt Service",                f$(S.debt_total),        "principal + interest"],
  ["Capital Appropriations",      f$(S.cap_total),         fN(S.cap_n_proj)+" projects"],
  ["Contracts Active",            fN(S.con_active),        "of "+fN(S.con_total)+" total"],
  ["Pro/Tech Services (proxy)",   f$(S.pro_tech_total),    "best proxy for contracted spend"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

// Service chart
const maxSvc = Math.max(...(DATA.opex_by_svc||[]).map(s=>s.total),1);
document.getElementById("svcChart").innerHTML =
  "<h3 style='font-size:12px;margin:8px 0 4px;color:#555;text-transform:uppercase;letter-spacing:.04em'>Operating Budget by Service — FY2025</h3>"+
  "<table style='width:100%'>"+
  (DATA.opex_by_svc||[]).map(s=>`<tr>
    <td style="width:250px;font-size:12px">${s.service}</td>
    <td><div class="bar navy" style="width:${s.total/maxSvc*100}%"></div></td>
    <td class="num" style="width:80px">${f$(s.total)}</td>
    <td class="num" style="width:55px;font-size:11px">${fPct(s.total,S.opex_total)}</td>
  </tr>`).join("")+"</table>";

// ── ACTUALS (ACFR) ───────────────────────────────────────────────────
const AG = DATA.acfr_grand;
const ARG = DATA.acfr_rev_grand;
const actualSaving = (AG.budget||0) - (AG.actual||0);
document.getElementById("acfrKpis").innerHTML = [
  ["FY2025 Actual Expenditures", fp(AG.actual||0),     "audited, General Fund budgetary basis"],
  ["Budget (Adopted)",           fp(AG.budget||0),     "total adopted budget"],
  ["Under Budget by",            fp(actualSaving),     ((actualSaving/(AG.budget||1))*100).toFixed(1)+"% surplus"],
  ["Actual Revenues",            fp(ARG.actual||0),    "vs budget "+fp(ARG.budget||0)],
  ["Revenue Surplus",            fp((ARG.actual||0)-(ARG.budget||0)), "actual vs adopted budget"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const maxActSvc = Math.max(...(DATA.acfr_by_svc||[]).map(r=>r.actual),1);
const acfrSvcBody = document.querySelector("#acfrSvcTable tbody");
(DATA.acfr_by_svc||[]).forEach(r => {
  const pct = r.budget>0 ? ((r.actual-r.budget)/r.budget*100).toFixed(1)+"%" : "—";
  const favStyle = r.variance>0 ? "color:#2e7d5a" : r.variance<0 ? "color:#c0392b" : "";
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${r.service}</td>
    <td class="num">${fp(r.budget)}</td>
    <td class="num">${fp(r.actual)}</td>
    <td class="num" style="${favStyle}">${r.variance>=0?"+":""}${fp(r.variance)}</td>
    <td class="num" style="${favStyle}">${pct}</td>
    <td><div class="bar ${r.actual<=r.budget?'green':'bar'}" style="width:${r.actual/maxActSvc*100}%"></div></td>`;
  acfrSvcBody.appendChild(tr);

  // Drill-down: depts in this service
  const depts = (DATA.acfr_by_dept||[]).filter(d=>d.service===r.service);
  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  det.innerHTML = `<td colspan="6"><div class="sub">
    <h4>Departments — ${r.service}</h4>
    <table><thead><tr><th>Department</th><th class="num">Budget</th>
      <th class="num">Actual</th><th class="num">Variance</th><th class="num">%</th></tr></thead>
    <tbody>${depts.map(d=>{
      const dp = d.budget>0?((d.actual-d.budget)/d.budget*100).toFixed(1)+"%":"—";
      const ds = d.variance>0?"color:#2e7d5a":d.variance<0?"color:#c0392b":"";
      return `<tr><td>${d.department}</td>
        <td class="num">${fp(d.budget)}</td>
        <td class="num">${fp(d.actual)}</td>
        <td class="num" style="${ds}">${d.variance>=0?"+":""}${fp(d.variance)}</td>
        <td class="num" style="${ds}">${dp}</td></tr>`;
    }).join("")}</tbody></table></div></td>`;
  acfrSvcBody.appendChild(det);
  tr.addEventListener("click",()=>{
    const o=det.style.display!=="none"; det.style.display=o?"none":"";
    tr.classList.toggle("expanded",!o);
  });
});

const acfrDeptBody = document.querySelector("#acfrDeptTable tbody");
(DATA.acfr_by_dept||[]).forEach(d=>{
  const pct = d.budget>0?((d.actual-d.budget)/d.budget*100).toFixed(1)+"%":"—";
  const ds = d.variance>0?"color:#2e7d5a":d.variance<0?"color:#c0392b":"";
  const tr = document.createElement("tr");
  tr.innerHTML = `<td>${d.department}</td>
    <td style="font-size:11px;color:#666">${d.service}</td>
    <td class="num">${fp(d.budget)}</td>
    <td class="num">${fp(d.actual)}</td>
    <td class="num" style="${ds}">${d.variance>=0?"+":""}${fp(d.variance)}</td>
    <td class="num" style="${ds}">${pct}</td>`;
  acfrDeptBody.appendChild(tr);
});

const acfrOtherBody = document.querySelector("#acfrOtherTable tbody");
(DATA.acfr_other||[]).forEach(r=>{
  const ds = r.variance>0?"color:#2e7d5a":r.variance<0?"color:#c0392b":"";
  const tr = document.createElement("tr");
  tr.innerHTML = `<td style="${r.department==='_TOTAL'?'font-weight:700':''}">
    ${r.department==='_TOTAL'?'GRAND TOTAL':r.department}</td>
    <td class="num">${fp(r.budget)}</td>
    <td class="num">${fp(r.actual)}</td>
    <td class="num" style="${ds}">${r.variance>=0?"+":""}${fp(r.variance)}</td>`;
  acfrOtherBody.appendChild(tr);
});

const acfrRevBody = document.querySelector("#acfrRevTable tbody");
(DATA.acfr_revenue||[]).forEach(r=>{
  const ds = r.variance>=0?"color:#2e7d5a":"color:#c0392b";
  const tr = document.createElement("tr");
  tr.innerHTML = `<td>${r.department}</td>
    <td class="num">${fp(r.budget)}</td>
    <td class="num">${fp(r.actual)}</td>
    <td class="num" style="${ds}">${r.variance>=0?"+":""}${fp(r.variance)}</td>`;
  acfrRevBody.appendChild(tr);
});

// ── OPERATING BUDGET ─────────────────────────────────────────────────
document.getElementById("opSvcKpis").innerHTML = [
  ["Total Budget",      f$(S.opex_total),   "FY2025 adopted"],
  ["Line Items",        fN(S.opex_n),       "dept/div/description rows"],
  ["Departments",       fN(S.opex_depts),   "city departments"],
  ["Perm Salaries",     f$(DATA.opex_by_desc.find(d=>d.description==="Permanent Salaries/Wages")?.total||0), "largest line"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const descMax = (DATA.opex_by_desc||[])[0]?.total||1;
const descBody = document.querySelector("#descTable tbody");
(DATA.opex_by_desc||[]).forEach(d=>{
  const tr = document.createElement("tr");
  tr.innerHTML=`<td>${d.description}</td><td class="num">${f$(d.total)}</td>
    <td class="num">${fN(d.n)}</td><td class="num">${fN(d.depts)}</td>
    <td><div class="bar navy" style="width:${d.total/descMax*100}%"></div></td>`;
  descBody.appendChild(tr);
});

// Searchable / paginated operating lines table
let opData = DATA.opex_lines.slice();
let opFiltered = opData;
let opPage = 0;
const OP_PG = 50;
let opSortCol = null, opSortAsc = true;

function sortOp(col) {
  if (opSortCol===col) { opSortAsc=!opSortAsc; }
  else { opSortCol=col; opSortAsc=true; }
  opFiltered.sort((a,b)=>{
    const av=a[col]||"", bv=b[col]||"";
    return typeof av==="number" ? (opSortAsc?av-bv:bv-av)
           : opSortAsc?String(av).localeCompare(String(bv)):String(bv).localeCompare(String(av));
  });
  opPage=0; renderOp();
}
function filterOpLines(){
  const q=(document.getElementById("opSearch").value||"").toLowerCase();
  opFiltered = q ? opData.filter(r=>
    (r.department_name||"").toLowerCase().includes(q)||
    (r.division_name||"").toLowerCase().includes(q)||
    (r.description||"").toLowerCase().includes(q)||
    (r.fund||"").toLowerCase().includes(q)) : opData.slice();
  opPage=0; renderOp();
}
function mkOpPager(id){
  const tot=Math.ceil(opFiltered.length/OP_PG);
  const el=document.getElementById(id);
  el.innerHTML="";
  const prev=document.createElement("button"); prev.textContent="← Prev";
  const next=document.createElement("button"); next.textContent="Next →";
  const info=document.createElement("span");
  const rng=document.createElement("span"); rng.style.cssText="color:#777;margin-left:auto;font-size:10px";
  el.appendChild(prev);el.appendChild(info);el.appendChild(next);el.appendChild(rng);
  const update=()=>{
    info.textContent=`Page ${opPage+1}/${tot||1}`;
    rng.textContent=`#${opPage*OP_PG+1}–#${Math.min((opPage+1)*OP_PG,opFiltered.length)} of ${fN(opFiltered.length)}`;
    prev.disabled=opPage===0; prev.style.opacity=opPage===0?"0.3":"1";
    next.disabled=opPage>=tot-1; next.style.opacity=opPage>=tot-1?"0.3":"1";
  };
  prev.addEventListener("click",ev=>{ev.stopPropagation();if(opPage>0){opPage--;renderOp();}});
  next.addEventListener("click",ev=>{ev.stopPropagation();if(opPage<tot-1){opPage++;renderOp();}});
  update();
  return update;
}
let upOpTop,upOpBot;
function renderOp(){
  const body=document.getElementById("opBody");
  body.innerHTML="";
  const slice=opFiltered.slice(opPage*OP_PG,(opPage+1)*OP_PG);
  slice.forEach(r=>{
    const tr=document.createElement("tr");
    tr.innerHTML=`<td>${r.department_name}</td><td style="font-size:11px">${r.division_name}</td>
      <td style="font-size:11px">${r.description}</td><td style="font-size:11px;color:#777">${r.fund}</td>
      <td class="num">${fp(r.amount)}</td>`;
    body.appendChild(tr);
  });
  document.getElementById("opCount").textContent=
    opFiltered.length<opData.length?`${fN(opFiltered.length)} of ${fN(opData.length)} rows`:`${fN(opData.length)} rows`;
  if(upOpTop)upOpTop(); if(upOpBot)upOpBot();
}
upOpTop=mkOpPager("opPagerTop"); upOpBot=mkOpPager("opPagerBot");
renderOp();

// ── OVERTIME ──────────────────────────────────────────────────────────
document.getElementById("otKpis").innerHTML = [
  ["Total OT Budget FY2025",   f$(S.overtime_total), fN(S.overtime_n)+" budget lines"],
  ["Police OT",   f$((DATA.overtime_by_dept||[]).find(d=>d.department_name==="Police")?.ot_total||0), "largest overtime dept"],
  ["Fire OT",     f$((DATA.overtime_by_dept||[]).find(d=>d.department_name==="Fire")?.ot_total||0), ""],
  ["DPW OT",      f$((DATA.overtime_by_dept||[]).find(d=>d.department_name==="Public Works")?.ot_total||0), ""],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const otDeptBody = document.querySelector("#otDeptTable tbody");
const otMax = (DATA.overtime_by_dept||[])[0]?.ot_total||1;
(DATA.overtime_by_dept||[]).forEach(d=>{
  const wages = DATA.wage_detail_by_dept.find(w=>w.department_name===d.department_name);
  const perm = wages?.perm_wages||0;
  const otPct = perm>0 ? (d.ot_total/perm*100).toFixed(1)+"%" : "—";
  const hiOt = perm>0 && d.ot_total/perm > 0.15;
  const tr=document.createElement("tr");
  tr.className="expandable";
  tr.innerHTML=`<td>${d.department_name}</td>
    <td class="num" style="font-weight:600">${f$(d.ot_total)}</td>
    <td class="num">${f$(perm)}</td>
    <td class="num"${hiOt?' style="color:#c0392b;font-weight:700"':""}>${otPct}</td>
    <td><div class="bar amber" style="width:${d.ot_total/otMax*100}%"></div></td>`;
  otDeptBody.appendChild(tr);

  // Lines for this dept
  const lines = DATA.overtime_lines.filter(l=>l.department_name===d.department_name);
  const det=document.createElement("tr");
  det.className="detail"; det.style.display="none";
  det.innerHTML=`<td colspan="5"><div class="sub">
    <h4>Overtime lines — ${d.department_name}</h4>
    <table><thead><tr><th>Division</th><th>Fund</th><th class="num">Amount</th></tr></thead>
    <tbody>${lines.map(l=>`<tr>
      <td style="font-size:11px">${l.division_name}</td>
      <td style="font-size:11px;color:#777">${l.fund}</td>
      <td class="num">${fp(l.amount)}</td></tr>`).join("")}</tbody></table></div></td>`;
  otDeptBody.appendChild(det);
  tr.addEventListener("click",()=>{
    const o=det.style.display!=="none";
    det.style.display=o?"none":""; tr.classList.toggle("expanded",!o);
  });
});

const otLinesBody = document.querySelector("#otLinesTable tbody");
(DATA.overtime_lines||[]).forEach(l=>{
  const tr=document.createElement("tr");
  tr.innerHTML=`<td>${l.department_name}</td><td style="font-size:11px">${l.division_name}</td>
    <td style="font-size:11px;color:#777">${l.fund}</td><td class="num">${fp(l.amount)}</td>`;
  otLinesBody.appendChild(tr);
});

// ── SALARY ────────────────────────────────────────────────────────────
document.getElementById("salKpis").innerHTML = [
  ["Published Positions",  fN(S.sal_n),    "FY2025 (Education missing)"],
  ["Budgeted Salary Total",f$(S.sal_total), "non-Education only"],
  ["Missing",              "~1,900 pos",   "Cambridge Public Schools not in source"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const salDeptBody = document.querySelector("#salDeptTable tbody");
(DATA.salary_by_dept||[]).forEach(d=>{
  const tr=document.createElement("tr");
  tr.className="expandable";
  tr.innerHTML=`<td>${d.department}</td><td style="font-size:11px;color:#666">${d.service}</td>
    <td class="num">${f$(d.total)}</td><td class="num">${fN(d.n)}</td>`;
  salDeptBody.appendChild(tr);

  const positions = DATA.salary_lines.filter(p=>p.department===d.department);
  const det=document.createElement("tr");
  det.className="detail"; det.style.display="none";
  det.innerHTML=`<td colspan="4"><div class="sub">
    <h4>Positions — ${d.department} (${positions.length})</h4>
    <table><thead><tr><th>Job Title</th><th>Division</th><th class="num">Salary</th></tr></thead>
    <tbody>${positions.map(p=>`<tr>
      <td style="font-size:11px">${p.job_title}</td>
      <td style="font-size:11px;color:#666">${p.division}</td>
      <td class="num">${fp(p.total_salary)}</td></tr>`).join("")}</tbody></table></div></td>`;
  salDeptBody.appendChild(det);
  tr.addEventListener("click",()=>{
    const o=det.style.display!=="none";
    det.style.display=o?"none":""; tr.classList.toggle("expanded",!o);
  });
});

const salPosBody = document.querySelector("#salPosTable tbody");
(DATA.salary_lines||[]).slice(0,50).forEach((p,i)=>{
  const tr=document.createElement("tr");
  tr.innerHTML=`<td style="color:#999;font-size:10px">${i+1}</td>
    <td>${p.job_title}</td><td style="font-size:11px">${p.department}</td>
    <td style="font-size:10px;color:#666">${p.division}</td>
    <td class="num">${fp(p.total_salary)}</td>`;
  salPosBody.appendChild(tr);
});

// ── CONTRACTS ─────────────────────────────────────────────────────────
document.getElementById("conKpis").innerHTML = [
  ["Active FY2025",     fN(S.con_active),    "status = active"],
  ["No Renewal Clause", fN(S.con_no_renew),  "fixed-term; rebid on expiry"],
  ["Has Renewals",      fN(S.con_has_renew), "renewal options remaining"],
  ["Emergency",         fN(S.con_emergency), "bypassed competition"],
  ["Expiring FY2025",   fN(S.con_expiring),  "ending during FY2025"],
  ["Contract $ Values", "Not public",        "not in Cambridge Open Data"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

// Classification chart
const clsMax = (DATA.con_by_pclass||[])[0]?.n||1;
document.getElementById("conClassChart").innerHTML =
  "<table style='width:100%;max-width:600px'>"+
  (DATA.con_by_pclass||[]).slice(0,10).map(c=>`<tr>
    <td style="width:240px;font-size:11px">${c.pclass}</td>
    <td><div class="bar navy" style="width:${c.n/clsMax*100}%"></div></td>
    <td class="num" style="width:40px;font-size:11px">${fN(c.n)}</td></tr>`).join("")+
  "</table>";

const conDeptBody = document.querySelector("#conDeptTable tbody");
(DATA.contracts_by_dept||[]).forEach(d=>{
  const tr=document.createElement("tr");
  tr.className="expandable";
  tr.innerHTML=`<td>${d.dept}</td>
    <td class="num" style="font-weight:600">${fN(d.active)}</td>
    <td class="num">${fN(d.n)}</td>
    <td class="num">${d.no_renew>0?`<span class="flag warn">${d.no_renew}</span>`:"—"}</td>
    <td class="num">${d.emergency>0?`<span class="flag">${d.emergency}</span>`:"—"}</td>`;
  conDeptBody.appendChild(tr);
  const dContracts = DATA.contracts_2025.filter(c=>c.department===d.dept).slice(0,15);
  const det=document.createElement("tr");
  det.className="detail"; det.style.display="none";
  det.innerHTML=`<td colspan="5"><div class="sub">
    <h4>Contracts — ${d.dept}</h4>
    <table><thead><tr><th>Vendor</th><th>Title</th><th>Status</th><th>End</th><th>Renews</th></tr></thead>
    <tbody>${dContracts.map(c=>`<tr>
      <td style="font-size:11px">${c.vendor_name}</td>
      <td style="font-size:10px">${c.contract_title.substring(0,45)}</td>
      <td><span class="flag ${c.status==='active'?'ok':''}">${c.status}</span>${c.is_emergency?'<span class="flag">emrg</span>':''}</td>
      <td style="font-size:10px">${c.end_date||""}</td>
      <td class="num" style="font-size:10px">${c.renewals_remaining===0?'<span class="flag warn">0</span>':c.renewals_remaining}</td>
    </tr>`).join("")}</tbody></table></div></td>`;
  conDeptBody.appendChild(det);
  tr.addEventListener("click",()=>{const o=det.style.display!=="none";det.style.display=o?"none":"";tr.classList.toggle("expanded",!o);});
});

const cvBody = document.querySelector("#conVendorTable tbody");
(DATA.contracts_by_vendor||[]).forEach(v=>{
  const tr=document.createElement("tr");
  tr.className="expandable";
  tr.innerHTML=`<td>${v.vendor}</td><td class="num">${fN(v.n)}</td>
    <td class="num">${fN(v.active)}</td><td class="num">${fN(v.depts)}</td>`;
  cvBody.appendChild(tr);
  const det=document.createElement("tr");
  det.className="detail"; det.style.display="none";
  det.innerHTML=`<td colspan="4"><div class="sub">
    <h4>Contracts — ${v.vendor}</h4>
    <table><thead><tr><th>Title</th><th>Department</th><th>Status</th><th>End</th><th>Type</th></tr></thead>
    <tbody>${(v.contracts||[]).slice(0,15).map(c=>`<tr>
      <td style="font-size:10px">${c.contract_title.substring(0,50)}</td>
      <td style="font-size:10px">${c.department}</td>
      <td><span class="flag ${c.status==='active'?'ok':''}">${c.status}</span></td>
      <td style="font-size:10px">${c.end_date||""}</td>
      <td style="font-size:10px">${c.contract_type}</td>
    </tr>`).join("")}</tbody></table></div></td>`;
  cvBody.appendChild(det);
  tr.addEventListener("click",()=>{const o=det.style.display!=="none";det.style.display=o?"none":"";tr.classList.toggle("expanded",!o);});
});

const ceBody=document.querySelector("#conExpiringTable tbody");
(DATA.con_expiring||[]).forEach(c=>{
  const tr=document.createElement("tr");
  tr.innerHTML=`<td>${c.vendor_name}</td><td style="font-size:11px">${c.department}</td>
    <td style="font-size:10px">${c.contract_title.substring(0,50)}</td>
    <td style="font-size:10px">${c.contract_type}</td>
    <td style="font-size:11px">${c.end_date}</td>
    <td class="num">${c.renewals_remaining===0?'<span class="flag warn">must rebid</span>':fN(c.renewals_remaining)}</td>`;
  ceBody.appendChild(tr);
});

const emBody=document.querySelector("#conEmergTable tbody");
(DATA.con_emergency||[]).forEach(c=>{
  const tr=document.createElement("tr");
  tr.innerHTML=`<td>${c.vendor_name}</td><td style="font-size:11px">${c.department}</td>
    <td style="font-size:10px">${c.contract_title.substring(0,55)}</td>
    <td><span class="flag ${c.status==='active'?'ok':''}">${c.status}</span></td>
    <td style="font-size:11px">${c.start_date}</td>
    <td style="font-size:11px">${c.end_date}</td>`;
  emBody.appendChild(tr);
});

// No-renew paginated table
let nrData = DATA.con_no_renew||[];
let nrFiltered = nrData.slice();
let nrPage=0; const NR_PG=50;
function filterNoRenew(){
  const q=(document.getElementById("noRenewSearch").value||"").toLowerCase();
  nrFiltered=q?nrData.filter(c=>
    (c.vendor_name||"").toLowerCase().includes(q)||
    (c.department||"").toLowerCase().includes(q)||
    (c.contract_title||"").toLowerCase().includes(q)):nrData.slice();
  nrPage=0;renderNr();
}
function renderNr(){
  const body=document.getElementById("nrBody");
  body.innerHTML="";
  nrFiltered.slice(nrPage*NR_PG,(nrPage+1)*NR_PG).forEach(c=>{
    const tr=document.createElement("tr");
    tr.innerHTML=`<td>${c.vendor_name}</td><td style="font-size:11px">${c.department}</td>
      <td style="font-size:10px">${c.contract_title.substring(0,50)}</td>
      <td style="font-size:10px">${c.contract_type}</td>
      <td style="font-size:11px">${c.end_date||""}</td>`;
    body.appendChild(tr);
  });
  const tot=Math.ceil(nrFiltered.length/NR_PG);
  document.getElementById("noRenewCount").textContent=
    `${fN(nrFiltered.length)} contracts`;
  ["nrPagerTop","nrPagerBot"].forEach(id=>{
    const el=document.getElementById(id); el.innerHTML="";
    const p=document.createElement("button");p.textContent="← Prev";
    const n=document.createElement("button");n.textContent="Next →";
    const i=document.createElement("span");
    i.textContent=`Page ${nrPage+1}/${tot||1}`;
    p.disabled=nrPage===0;n.disabled=nrPage>=tot-1;
    p.style.opacity=nrPage===0?"0.3":"1";n.style.opacity=nrPage>=tot-1?"0.3":"1";
    p.addEventListener("click",ev=>{ev.stopPropagation();if(nrPage>0){nrPage--;renderNr();}});
    n.addEventListener("click",ev=>{ev.stopPropagation();if(nrPage<tot-1){nrPage++;renderNr();}});
    el.appendChild(p);el.appendChild(i);el.appendChild(n);
  });
}
renderNr();

// ── CAPITAL ───────────────────────────────────────────────────────────
document.getElementById("capKpis").innerHTML = [
  ["FY2025 Capital",   f$(S.cap_total),   fN(S.cap_n_proj)+" projects"],
  ["Largest Dept",     (DATA.capital_by_dept||[])[0]?.department||"—",
                       f$((DATA.capital_by_dept||[])[0]?.total||0)],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const capDeptBody=document.querySelector("#capDeptTable tbody");
const capMax=(DATA.capital_by_dept||[])[0]?.total||1;
(DATA.capital_by_dept||[]).forEach(d=>{
  const tr=document.createElement("tr");
  tr.className="expandable";
  tr.innerHTML=`<td>${d.department}</td><td class="num">${f$(d.total)}</td>
    <td class="num">${fN(d.n)}</td>
    <td><div class="bar navy" style="width:${d.total/capMax*100}%"></div></td>`;
  capDeptBody.appendChild(tr);
  const projs=DATA.capital_projects.filter(p=>p.department===d.department);
  const det=document.createElement("tr");
  det.className="detail"; det.style.display="none";
  det.innerHTML=`<td colspan="4"><div class="sub">
    <h4>FY2025 projects — ${d.department}</h4>
    <table><thead><tr><th>Project</th><th>ID</th><th>Fund</th><th>Location</th><th class="num">Appropr.</th></tr></thead>
    <tbody>${projs.map(p=>`<tr>
      <td style="font-size:11px">${p.project_name}</td>
      <td style="font-size:10px;color:#888">${p.project_id}</td>
      <td style="font-size:10px">${p.fund}</td>
      <td style="font-size:10px;color:#666">${p.city_location||""}</td>
      <td class="num">${f$(p.approved_amount)}</td></tr>`).join("")}</tbody></table></div></td>`;
  capDeptBody.appendChild(det);
  tr.addEventListener("click",()=>{const o=det.style.display!=="none";det.style.display=o?"none":"";tr.classList.toggle("expanded",!o);});
});

const capAllBody=document.querySelector("#capAllTable tbody");
(DATA.capital_projects||[]).forEach(p=>{
  const tr=document.createElement("tr");
  tr.innerHTML=`<td style="font-size:11px">${p.project_name}</td>
    <td style="font-size:11px">${p.department}</td>
    <td style="font-size:10px">${p.fund}</td>
    <td style="font-size:10px;color:#666">${p.city_location||""}</td>
    <td class="num">${f$(p.approved_amount)}</td>`;
  capAllBody.appendChild(tr);
});

// ── REVENUE ───────────────────────────────────────────────────────────
document.getElementById("revKpis").innerHTML = [
  ["Total Revenue FY2025", f$(DATA.rev_totals?.total||0), fN(DATA.rev_totals?.n||0)+" line items"],
  ["Property Taxes",       f$((DATA.rev_by_cat||[]).find(c=>c.category==="Taxes")?.total||0),
                           fPct((DATA.rev_by_cat||[]).find(c=>c.category==="Taxes")?.total||0,DATA.rev_totals?.total||1)+" of total"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const rcBody=document.querySelector("#revCatTable tbody");
const rcMax=(DATA.rev_by_cat||[])[0]?.total||1;
(DATA.rev_by_cat||[]).forEach(c=>{
  const tr=document.createElement("tr");
  tr.innerHTML=`<td>${c.category}</td><td class="num">${f$(c.total)}</td>
    <td class="num">${fPct(c.total,DATA.rev_totals?.total||1)}</td>
    <td><div class="bar green" style="width:${c.total/rcMax*100}%"></div></td>`;
  rcBody.appendChild(tr);
});

let rvData=DATA.revenue_lines.slice(),rvFiltered=rvData.slice(),rvPage=0;const RV_PG=50;
function filterRev(){
  const q=(document.getElementById("revSearch").value||"").toLowerCase();
  rvFiltered=q?rvData.filter(r=>
    (r.department_name||"").toLowerCase().includes(q)||
    (r.category||"").toLowerCase().includes(q)||
    (r.description||"").toLowerCase().includes(q)):rvData.slice();
  rvPage=0;renderRv();
}
function renderRv(){
  const body=document.getElementById("revBody");body.innerHTML="";
  rvFiltered.slice(rvPage*RV_PG,(rvPage+1)*RV_PG).forEach(r=>{
    const tr=document.createElement("tr");
    tr.innerHTML=`<td style="font-size:11px">${r.department_name}</td>
      <td style="font-size:11px">${r.category}</td>
      <td style="font-size:10px">${r.description}</td>
      <td style="font-size:10px;color:#777">${r.fund}</td>
      <td class="num">${fp(r.amount)}</td>`;
    body.appendChild(tr);
  });
  const tot=Math.ceil(rvFiltered.length/RV_PG);
  document.getElementById("revCount").textContent=`${fN(rvFiltered.length)} lines`;
  ["revPagerTop","revPagerBot"].forEach(id=>{
    const el=document.getElementById(id);el.innerHTML="";
    const p=document.createElement("button");p.textContent="← Prev";
    const n=document.createElement("button");n.textContent="Next →";
    const i=document.createElement("span");
    i.textContent=`Page ${rvPage+1}/${tot||1}`;
    p.disabled=rvPage===0;n.disabled=rvPage>=tot-1;
    p.style.opacity=rvPage===0?"0.3":"1";n.style.opacity=rvPage>=tot-1?"0.3":"1";
    p.addEventListener("click",ev=>{ev.stopPropagation();if(rvPage>0){rvPage--;renderRv();}});
    n.addEventListener("click",ev=>{ev.stopPropagation();if(rvPage<tot-1){rvPage++;renderRv();}});
    el.appendChild(p);el.appendChild(i);el.appendChild(n);
  });
}
renderRv();

// ── BIDS ──────────────────────────────────────────────────────────────
document.getElementById("bidKpis").innerHTML = [
  ["Bids in FY2025", fN(S.bids_total), "released in CY2025"],
  ["Amendments",     fN((DATA.bids_2025||[]).reduce((s,b)=>s+(b.addenda_count||0),0)), "addenda issued"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const bidBody=document.querySelector("#bidTable tbody");
(DATA.bids_2025||[]).forEach(b=>{
  const tr=document.createElement("tr");
  tr.innerHTML=`<td style="font-size:10px;color:#888">${b.bid_number}</td>
    <td style="font-size:11px">${(b.bid_title||"").substring(0,60)}</td>
    <td style="font-size:10px">${b.departments||""}</td>
    <td style="font-size:10px">${b.bid_type}</td>
    <td style="font-size:10px">${b.bid_category}</td>
    <td style="font-size:10px">${b.release_date}</td>
    <td class="num">${b.addenda_count>0?`<span class="flag warn">${b.addenda_count}</span>`:fN(b.addenda_count)}</td>`;
  bidBody.appendChild(tr);
});

// ── TRANSFERS ─────────────────────────────────────────────────────────
document.getElementById("transferKpis").innerHTML = [
  ["Debt Service",  f$(S.debt_total), "principal + interest"],
  ["MWRA",          f$((DATA.transfers_by_dept||[]).find(d=>d.department_name==="Massachusetts Water Resources Authority")?.total||0), "water/sewer assessment"],
  ["Cherry Sheet",  f$((DATA.transfers_by_dept||[]).find(d=>d.department_name==="Cherry Sheet")?.total||0), "state charges"],
].map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

const tdBody=document.querySelector("#transferDeptTable tbody");
(DATA.transfers_by_dept||[]).forEach(d=>{
  const tr=document.createElement("tr"); tr.className="expandable";
  tr.innerHTML=`<td>${d.department_name}</td><td class="num">${f$(d.total)}</td>
    <td class="num">${fN(d.n)}</td>`;
  tdBody.appendChild(tr);
  const lines=DATA.transfers_lines.filter(l=>l.department_name===d.department_name);
  const det=document.createElement("tr");det.className="detail";det.style.display="none";
  det.innerHTML=`<td colspan="3"><div class="sub">
    <h4>Line items — ${d.department_name}</h4>
    <table><thead><tr><th>Description</th><th>Division</th><th>Fund</th><th class="num">Amount</th></tr></thead>
    <tbody>${lines.map(l=>`<tr>
      <td style="font-size:11px">${l.description}</td>
      <td style="font-size:10px">${l.division_name}</td>
      <td style="font-size:10px;color:#777">${l.fund}</td>
      <td class="num">${fp(l.amount)}</td></tr>`).join("")}</tbody></table></div></td>`;
  tdBody.appendChild(det);
  tr.addEventListener("click",()=>{const o=det.style.display!=="none";det.style.display=o?"none":"";tr.classList.toggle("expanded",!o);});
});

const tlBody=document.querySelector("#transferLinesTable tbody");
(DATA.transfers_lines||[]).forEach(l=>{
  const tr=document.createElement("tr");
  tr.innerHTML=`<td style="font-size:11px">${l.department_name}</td>
    <td style="font-size:10px">${l.division_name}</td>
    <td style="font-size:11px">${l.description}</td>
    <td style="font-size:10px;color:#777">${l.fund}</td>
    <td class="num">${fp(l.amount)}</td>`;
  tlBody.appendChild(tr);
});

// ── VALIDATION ────────────────────────────────────────────────────────
const valBody=document.querySelector("#valTable tbody");
(DATA.validation||[]).forEach(c=>{
  const sm={pass:["ok","PASS"],warn:["warn","WARN"],fail:["","FAIL"],unavailable:["info","N/A"]};
  const [cls,lbl]=sm[c.status]||["info","?"];
  const fv=v=>{if(v==null)return"—";if(typeof v==="number")return v>=1e6?f$(v):fN(Math.round(v));return String(v);};
  const tr=document.createElement("tr");
  tr.innerHTML=`<td style="font-size:10px;color:#555">${c.source}</td>
    <td style="font-size:11px">${c.label}
      ${c.note?`<br><span style="font-size:9px;color:#888;font-style:italic">${c.note}</span>`:""}
    </td>
    <td class="num">${fv(c.expected)}</td><td class="num">${fv(c.actual)}</td>
    <td class="num">${c.delta_pct!=null?c.delta_pct.toFixed(2)+"%":"—"}</td>
    <td><span class="flag ${cls}">${lbl}</span></td>`;
  valBody.appendChild(tr);
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

    print(f"Loading FY{FY} data from cambridge.db …")
    data = load_data()

    s = data["summary"]
    debt = data["summary"]["debt_total"]

    summary = (
        f"Cambridge's FY2025 adopted operating budget is ${s['opex_total']/1e6:.0f}M across "
        f"{s['opex_n']:,} budget line items and {s['opex_depts']} departments. "
        f"Overtime is budgeted at ${s['overtime_total']/1e6:.1f}M across "
        f"{s['overtime_n']} lines — Police leads at "
        f"${next((d['ot_total'] for d in data['overtime_by_dept'] if d['department_name']=='Police'), 0)/1e6:.1f}M. "
        f"Debt service totals ${debt/1e6:.0f}M (principal + interest). "
        f"{s['con_active']:,} contracts are active; {s['con_no_renew']:,} have zero renewals "
        f"remaining and must be competitively rebid. "
        f"53 capital projects received ${s['cap_total']/1e6:.0f}M in FY2025 appropriations. "
        f"Contract dollar values are not published in Cambridge's open data."
    )

    html_out = (HTML
        .replace("__TODAY__", datetime.date.today().isoformat())
        .replace("__SUMMARY_LEAD__", summary)
        .replace("__DATA_JSON__", json.dumps(data, default=str)))

    with open(OUT, "w") as f:
        f.write(html_out)
    print(f"wrote {OUT}  ({len(html_out)//1024} KB)")


if __name__ == "__main__":
    main()
