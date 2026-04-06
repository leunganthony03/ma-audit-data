"""Generate a self-contained HTML audit report of Massachusetts state spending.

Sources:
  - spending.db (Comptroller CTHRU, FY2025, 2.86M transactions)
  - commbuys.db (COMMBUYS scraped contracts, 17,275 contracts)

Output: audit.html (drill-down explorer + curated waste/attention flags)
"""
import json
import sqlite3
import html

SPENDING_DB = "/Users/anthonyleung/playground2/spending.db"
COMMBUYS_DB = "/Users/anthonyleung/playground2/commbuys.db"
OUT = "/Users/anthonyleung/playground2/audit.html"
FY = "2025"


def amt(val):
    """Convert Amount string to float."""
    if val is None:
        return 0.0
    try:
        return float(str(val).replace(",", ""))
    except ValueError:
        return 0.0


# Context notes keyed by object-class prefix (2-letter code).
# These explain what the category means and how much is realistically procurable.
OBJECT_NOTES = {
    "AA": ("State employee payroll. Paid via HR/CMS, not procurement. "
           "Not biddable — but size and growth are oversight signals."),
    "BB": ("Employee-related expenses (travel, training reimbursement). Small but frequently audited for misuse."),
    "CC": ("Contract employees / temporary staff. Often placed through IT/consulting staffing firms — "
           "a recurring OIG-flagged area for rate-card abuse."),
    "DD": ("Pension obligations (State Employees' Retirement System, Teachers' Retirement, GIC health insurance). "
           "Actuarially determined — not discretionary procurement."),
    "EE": ("Administrative expenses (office supplies, postage, printing). Small items — aggregated "
           "under statewide contracts. Watch for off-contract spend."),
    "FF": ("Facility operations (cleaning, security, utilities). Should be on competed statewide contracts."),
    "GG": ("Energy and space rental. Leases reviewed by DCAMM; energy by DOER. Look for non-competed renewals."),
    "HH": ("Consultant services paid to state departments. Major red-flag area — includes management "
           "consulting, legal services, advisory. Historically high sole-source rates."),
    "JJ": ("Operational services (laundry, food service, security guards). Mostly on statewide contracts."),
    "KK": ("Equipment purchases (vehicles, machinery, furniture). Should flow through COMMBUYS."),
    "LL": ("Equipment leases and maintenance. Watch auto-renewals that dodge rebidding."),
    "MM": ("Purchased client/program services — community-based providers, hospitals, human services. "
           "Rate-set by EOHHS/CHIA, not openly bid. Largest 'contract' bucket that isn't actually competed."),
    "NN": ("Capital infrastructure — roads, bridges, buildings. Bid through MassDOT and DCAMM "
           "(Designer Selection Board), not COMMBUYS. Separate oversight regime."),
    "PP": ("State aid to cities/towns/school districts (Chapter 70, Chapter 90). "
           "Formula-driven grants, not procurement. Not biddable."),
    "RR": ("Benefit programs — MassHealth (Medicaid) to MCOs, unemployment, SNAP, childcare vouchers. "
           "Statutory entitlements. MassHealth MCO capitation rates are the dominant line."),
    "SS": ("Debt service — bond principal & interest to bondholders. Market-priced, not procured."),
    "TT": ("Loans, grants and special payments (MassWorks, MassGrant scholarships, etc.)."),
    "UU": ("IT non-payroll: software, hosting, licensing, SaaS. Frequently flagged for sole-source "
           "renewals, shelfware, and duplicate contracts."),
    "VV": ("Capital equipment leases."),
}


def load_data():
    conn = sqlite3.connect(SPENDING_DB)
    conn.execute(f"ATTACH '{COMMBUYS_DB}' AS cb")
    cur = conn.cursor()

    data = {"fy": FY}

    print("  → totals by fiscal year …")
    cur.execute("""
        SELECT Budget_Fiscal_Year,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(*) AS n
        FROM spending
        WHERE Budget_Fiscal_Year BETWEEN '2020' AND '2025'
        GROUP BY 1 ORDER BY 1
    """)
    data["yearly"] = [{"year": y, "amt": a or 0, "n": n} for y, a, n in cur.fetchall()]

    print("  → by object class …")
    cur.execute(f"""
        SELECT Object_Class,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(*) AS n,
               COUNT(DISTINCT Vendor) AS nv,
               COUNT(DISTINCT Department) AS nd
        FROM spending WHERE Budget_Fiscal_Year='{FY}'
        GROUP BY 1 ORDER BY 2 DESC
    """)
    oc = []
    for row in cur.fetchall():
        code = (row[0] or "").strip()
        key = code[1:3] if code.startswith("(") and len(code) > 3 else ""
        oc.append({
            "code": code, "key": key,
            "amt": row[1] or 0, "n": row[2],
            "nv": row[3], "nd": row[4],
            "note": OBJECT_NOTES.get(key, ""),
        })
    data["object_classes"] = oc

    print("  → by cabinet/secretariat …")
    cur.execute(f"""
        SELECT Cabinet_Secretariat,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(*) AS n,
               COUNT(DISTINCT Vendor) AS nv,
               COUNT(DISTINCT Department) AS nd
        FROM spending WHERE Budget_Fiscal_Year='{FY}'
        GROUP BY 1 ORDER BY 2 DESC
    """)
    data["cabinets"] = [
        {"name": r[0] or "(unspecified)", "amt": r[1] or 0, "n": r[2], "nv": r[3], "nd": r[4]}
        for r in cur.fetchall()
    ]

    print("  → departments with vendor detail …")
    cur.execute(f"""
        SELECT Department, Cabinet_Secretariat,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(*) AS n,
               COUNT(DISTINCT Vendor) AS nv
        FROM spending WHERE Budget_Fiscal_Year='{FY}'
        GROUP BY 1, 2 ORDER BY 3 DESC
    """)
    data["departments"] = [
        {"dept": r[0], "cab": r[1], "amt": r[2] or 0, "n": r[3], "nv": r[4]}
        for r in cur.fetchall()
    ]

    print("  → top vendors per department (for cabinet drill-down) …")
    dept_vendors = {}
    for d in data["departments"][:60]:  # top 60 depts by spend
        cur.execute(f"""
            SELECT Vendor,
                   SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
                   COUNT(*) AS n,
                   COUNT(DISTINCT Object_Class) AS noc
            FROM spending
            WHERE Budget_Fiscal_Year='{FY}' AND Department=? AND Vendor<>''
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """, (d["dept"],))
        dept_vendors[d["dept"]] = [
            {"vendor": r[0], "amt": r[1] or 0, "n": r[2], "noc": r[3]}
            for r in cur.fetchall()
        ]
    data["dept_vendors"] = dept_vendors

    print("  → vendor×dept detail (for cabinet 4th tier) …")
    # For top vendors in top departments, get object class breakdown
    vendor_dept_detail = {}
    for dept_name, vendors in dept_vendors.items():
        for v in vendors[:10]:  # top 10 vendors per dept
            key = f"{dept_name}|||{v['vendor']}"
            cur.execute(f"""
                SELECT Object_Class,
                       SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
                       COUNT(*) AS n
                FROM spending
                WHERE Budget_Fiscal_Year='{FY}' AND Department=? AND Vendor=?
                GROUP BY 1 ORDER BY 2 DESC LIMIT 10
            """, (dept_name, v["vendor"]))
            vendor_dept_detail[key] = [
                {"oc": r[0], "amt": r[1] or 0, "n": r[2]} for r in cur.fetchall()
            ]
    data["vendor_dept_detail"] = vendor_dept_detail

    print("  → top vendors overall …")
    cur.execute(f"""
        SELECT Vendor,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(*) AS n,
               COUNT(DISTINCT Department) AS nd,
               COUNT(DISTINCT Object_Class) AS noc
        FROM spending WHERE Budget_Fiscal_Year='{FY}' AND Vendor<>''
        GROUP BY 1 ORDER BY 2 DESC LIMIT 50
    """)
    data["top_vendors"] = [
        {"vendor": r[0], "amt": r[1] or 0, "n": r[2], "nd": r[3], "noc": r[4]}
        for r in cur.fetchall()
    ]

    print("  → vendor detail for top 50 …")
    vendor_detail = {}
    for v in data["top_vendors"]:
        vn = v["vendor"]
        cur.execute(f"""
            SELECT Department,
                   SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
                   COUNT(*) AS n
            FROM spending
            WHERE Budget_Fiscal_Year='{FY}' AND Vendor=?
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """, (vn,))
        by_dept = [{"dept": r[0], "amt": r[1] or 0, "n": r[2]} for r in cur.fetchall()]

        cur.execute(f"""
            SELECT Object_Class,
                   SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
                   COUNT(*) AS n
            FROM spending
            WHERE Budget_Fiscal_Year='{FY}' AND Vendor=?
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """, (vn,))
        by_oc = [{"oc": r[0], "amt": r[1] or 0, "n": r[2]} for r in cur.fetchall()]

        cur.execute(f"""
            SELECT Department, Object_Class, Appropriation_Name, Date,
                   CAST(REPLACE(Amount,',','') AS REAL) AS amt
            FROM spending
            WHERE Budget_Fiscal_Year='{FY}' AND Vendor=?
            ORDER BY amt DESC LIMIT 10
        """, (vn,))
        top_txns = [{"dept": r[0], "oc": r[1], "approp": r[2], "date": r[3], "amt": r[4] or 0}
                    for r in cur.fetchall()]

        vendor_detail[vn] = {"by_dept": by_dept, "by_oc": by_oc, "top_txns": top_txns}
    data["vendor_detail"] = vendor_detail

    print("  → top vendors per object class …")
    oc_vendors = {}
    for o in oc[:20]:
        cur.execute(f"""
            SELECT Vendor,
                   SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
                   COUNT(*) AS n
            FROM spending
            WHERE Budget_Fiscal_Year='{FY}' AND Object_Class=? AND Vendor<>''
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """, (o["code"],))
        oc_vendors[o["code"]] = [
            {"vendor": r[0], "amt": r[1] or 0, "n": r[2]}
            for r in cur.fetchall()
        ]
    data["oc_vendors"] = oc_vendors

    print("  → departments per object class …")
    oc_depts = {}
    for o in oc[:20]:
        cur.execute(f"""
            SELECT Department,
                   SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt
            FROM spending
            WHERE Budget_Fiscal_Year='{FY}' AND Object_Class=?
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """, (o["code"],))
        oc_depts[o["code"]] = [
            {"dept": r[0], "amt": r[1] or 0} for r in cur.fetchall()
        ]
    data["oc_depts"] = oc_depts

    print("  → appropriation types …")
    cur.execute(f"""
        SELECT Appropriation_Type,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(*) AS n
        FROM spending WHERE Budget_Fiscal_Year='{FY}'
        GROUP BY 1 ORDER BY 2 DESC
    """)
    data["appropriation_types"] = [
        {"type": r[0], "amt": r[1] or 0, "n": r[2]} for r in cur.fetchall()
    ]

    print("  → trust spending (3TN) …")
    cur.execute(f"""
        SELECT Vendor,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(*) AS n
        FROM spending
        WHERE Budget_Fiscal_Year='{FY}' AND Appropriation_Type='(3TN) TRUSTS'
          AND Vendor<>''
        GROUP BY 1 ORDER BY 2 DESC LIMIT 20
    """)
    data["trust_vendors"] = [
        {"vendor": r[0], "amt": r[1] or 0, "n": r[2]} for r in cur.fetchall()
    ]

    print("  → COMMBUYS competition mix …")
    cur.execute("""
        SELECT competition, COUNT(*) n,
               SUM(COALESCE(total_dollars_spent,0)) spent,
               COUNT(DISTINCT vendor) nv
        FROM cb.contract_classified
        GROUP BY 1 ORDER BY 2 DESC
    """)
    data["competition_mix"] = [
        {"cat": r[0] or "(unknown)", "n": r[1], "spent": r[2] or 0, "nv": r[3]}
        for r in cur.fetchall()
    ]

    print("  → top NO_BID contract vendors (by reported $) …")
    cur.execute("""
        SELECT vendor,
               SUM(total_dollars_spent) AS spent,
               COUNT(*) n
        FROM cb.contract_classified
        WHERE competition IN ('NO_BID','BID_UNKNOWN','CLOSED','LIMITED','EMERGENCY')
          AND total_dollars_spent > 0
        GROUP BY vendor
        ORDER BY spent DESC LIMIT 25
    """)
    data["no_bid_vendors"] = [
        {"vendor": r[0], "amt": r[1] or 0, "n": r[2]} for r in cur.fetchall()
    ]

    print("  → largest FY2025 vendors with a contract but NO open bid …")
    cur.execute("""
        WITH v AS (
            SELECT spending_vendor,
                   MAX(CASE WHEN competition='OPEN_COMPETITIVE' THEN 1 ELSE 0 END) AS has_open,
                   GROUP_CONCAT(DISTINCT competition) AS cats
            FROM cb.vendor_match
            GROUP BY spending_vendor
        )
        SELECT s.Vendor,
               SUM(CAST(REPLACE(s.Amount,',','') AS REAL)) AS amt,
               v.cats
        FROM spending s
        JOIN v ON s.Vendor=v.spending_vendor
        WHERE s.Budget_Fiscal_Year='2025' AND v.has_open=0
        GROUP BY s.Vendor ORDER BY amt DESC LIMIT 25
    """)
    data["no_open_bid_fy25"] = [
        {"vendor": r[0], "amt": r[1] or 0, "cats": r[2]} for r in cur.fetchall()
    ]

    print("  → FY2025 spend by COMMBUYS-match category …")
    cur.execute("""
        WITH vendor_best AS (
            SELECT spending_vendor,
                   CASE
                       WHEN SUM(CASE WHEN competition='OPEN_COMPETITIVE' THEN 1 ELSE 0 END) > 0 THEN 'HAS_OPEN_BID'
                       WHEN SUM(CASE WHEN competition='CLOSED' THEN 1 ELSE 0 END) > 0 THEN 'CLOSED_BID_ONLY'
                       WHEN SUM(CASE WHEN competition='NO_BID' THEN 1 ELSE 0 END) > 0 THEN 'NO_BID_REF'
                       ELSE 'OTHER'
                   END AS category
            FROM cb.vendor_match
            GROUP BY spending_vendor
        )
        SELECT COALESCE(vb.category,'NO_COMMBUYS_CONTRACT') AS cat,
               COUNT(DISTINCT s.Vendor) AS nv,
               SUM(CAST(REPLACE(s.Amount,',','') AS REAL)) AS amt
        FROM spending s
        LEFT JOIN vendor_best vb ON s.Vendor=vb.spending_vendor
        WHERE s.Budget_Fiscal_Year='2025' AND s.Vendor<>''
        GROUP BY cat ORDER BY amt DESC
    """)
    data["fy25_by_match"] = [
        {"cat": r[0], "nv": r[1], "amt": r[2] or 0} for r in cur.fetchall()
    ]

    print("  → materializing vendor_best table …")
    cur.executescript("""
        DROP TABLE IF EXISTS _vendor_best;
        CREATE TABLE _vendor_best AS
            SELECT spending_vendor,
                   CASE
                       WHEN SUM(CASE WHEN competition='OPEN_COMPETITIVE' THEN 1 ELSE 0 END) > 0 THEN 'HAS_OPEN_BID'
                       WHEN SUM(CASE WHEN competition='CLOSED' THEN 1 ELSE 0 END) > 0 THEN 'CLOSED_BID_ONLY'
                       WHEN SUM(CASE WHEN competition='NO_BID' THEN 1 ELSE 0 END) > 0 THEN 'NO_BID_REF'
                       ELSE 'OTHER'
                   END AS category
            FROM cb.vendor_match GROUP BY spending_vendor;
        CREATE INDEX idx_vb_sv ON _vendor_best(spending_vendor);
        CREATE INDEX idx_vb_cat ON _vendor_best(category);
    """)

    print("  → materializing FY25 vendor totals …")
    cur.executescript(f"""
        DROP TABLE IF EXISTS _fy25_vendor;
        CREATE TABLE _fy25_vendor AS
            SELECT Vendor, Department, Object_Class, Appropriation_Name, Date,
                   CAST(REPLACE(Amount,',','') AS REAL) AS amt
            FROM spending WHERE Budget_Fiscal_Year='{FY}' AND Vendor<>'';
        CREATE INDEX idx_fv_vendor ON _fy25_vendor(Vendor);
    """)

    print("  → match category → vendor detail …")
    match_cat_vendors = {}
    for m in data["fy25_by_match"]:
        cat = m["cat"]
        if cat == "NO_COMMBUYS_CONTRACT":
            cur.execute("""
                SELECT s.Vendor, SUM(s.amt) AS a, COUNT(*) n, COUNT(DISTINCT s.Department) nd
                FROM _fy25_vendor s
                LEFT JOIN _vendor_best vb ON s.Vendor=vb.spending_vendor
                WHERE vb.spending_vendor IS NULL
                GROUP BY 1 ORDER BY 2 DESC LIMIT 20
            """)
        else:
            cur.execute("""
                SELECT s.Vendor, SUM(s.amt) AS a, COUNT(*) n, COUNT(DISTINCT s.Department) nd
                FROM _fy25_vendor s
                JOIN _vendor_best vb ON s.Vendor=vb.spending_vendor
                WHERE vb.category=?
                GROUP BY 1 ORDER BY 2 DESC LIMIT 20
            """, (cat,))
        match_cat_vendors[cat] = [
            {"vendor": r[0], "amt": r[1] or 0, "n": r[2], "nd": r[3]}
            for r in cur.fetchall()
        ]
    data["match_cat_vendors"] = match_cat_vendors

    print("  → match vendor → transactions …")
    match_vendor_txns = {}
    for cat, vendors in match_cat_vendors.items():
        for v in vendors[:10]:
            cur.execute("""
                SELECT Department, Object_Class, Appropriation_Name, Date, amt
                FROM _fy25_vendor WHERE Vendor=? ORDER BY amt DESC LIMIT 10
            """, (v["vendor"],))
            match_vendor_txns[v["vendor"]] = [
                {"dept": r[0], "oc": r[1], "approp": r[2], "date": r[3], "amt": r[4] or 0}
                for r in cur.fetchall()
            ]
    data["match_vendor_txns"] = match_vendor_txns

    print("  → competition mix → top contracts …")
    comp_mix_contracts = {}
    for m in data["competition_mix"]:
        cur.execute("""
            SELECT vendor, blanket_id, description, raw_bid_type,
                   total_dollars_spent, total_dollar_limit, organization
            FROM cb.contract_classified
            WHERE competition=?
            ORDER BY COALESCE(total_dollars_spent,0) DESC LIMIT 15
        """, (m["cat"],))
        comp_mix_contracts[m["cat"]] = [
            {"vendor": r[0], "blanket": r[1], "desc": r[2], "bid_type": r[3],
             "spent": r[4] or 0, "limit": r[5] or 0, "org": r[6] or ""}
            for r in cur.fetchall()
        ]
    data["comp_mix_contracts"] = comp_mix_contracts

    print("  → no-open-bid vendor detail …")
    no_open_detail = {}
    for v in data["no_open_bid_fy25"][:25]:
        cur.execute("""
            SELECT Department, SUM(amt) a, COUNT(*) n
            FROM _fy25_vendor WHERE Vendor=? GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """, (v["vendor"],))
        by_dept = [{"dept": r[0], "amt": r[1] or 0, "n": r[2]} for r in cur.fetchall()]
        cur.execute("""
            SELECT Department, Object_Class, Appropriation_Name, Date, amt
            FROM _fy25_vendor WHERE Vendor=? ORDER BY amt DESC LIMIT 10
        """, (v["vendor"],))
        txns = [{"dept": r[0], "oc": r[1], "approp": r[2], "date": r[3], "amt": r[4] or 0}
                for r in cur.fetchall()]
        no_open_detail[v["vendor"]] = {"by_dept": by_dept, "txns": txns}
    data["no_open_detail"] = no_open_detail

    print("  → YoY growth vendor detail …")
    yoy_detail = {}
    for v in data["yoy_growth"][:20]:
        cur.execute("""
            SELECT Department, SUM(amt) a, COUNT(*) n
            FROM _fy25_vendor WHERE Vendor=? GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """, (v["vendor"],))
        by_dept = [{"dept": r[0], "amt": r[1] or 0, "n": r[2]} for r in cur.fetchall()]
        cur.execute("""
            SELECT Department, Object_Class, Appropriation_Name, Date, amt
            FROM _fy25_vendor WHERE Vendor=? ORDER BY amt DESC LIMIT 10
        """, (v["vendor"],))
        txns = [{"dept": r[0], "oc": r[1], "approp": r[2], "date": r[3], "amt": r[4] or 0}
                for r in cur.fetchall()]
        yoy_detail[v["vendor"]] = {"by_dept": by_dept, "txns": txns}
    data["yoy_detail"] = yoy_detail

    print("  → IT/consulting vendor detail …")
    it_detail = {}
    for v in data["it_consulting"][:25]:
        cur.execute("""
            SELECT Department, SUM(amt) a, COUNT(*) n
            FROM _fy25_vendor WHERE Vendor=?
              AND Object_Class IN ('(UU) IT NON-PAYROLL EXPENSES','(HH) CONSULTANT SVCS (TO DEPTS)')
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """, (v["vendor"],))
        by_dept = [{"dept": r[0], "amt": r[1] or 0, "n": r[2]} for r in cur.fetchall()]
        cur.execute("""
            SELECT Department, Object_Class, Appropriation_Name, Date, amt
            FROM _fy25_vendor WHERE Vendor=?
              AND Object_Class IN ('(UU) IT NON-PAYROLL EXPENSES','(HH) CONSULTANT SVCS (TO DEPTS)')
            ORDER BY amt DESC LIMIT 10
        """, (v["vendor"],))
        txns = [{"dept": r[0], "oc": r[1], "approp": r[2], "date": r[3], "amt": r[4] or 0}
                for r in cur.fetchall()]
        it_detail[v["vendor"]] = {"by_dept": by_dept, "txns": txns}
    data["it_detail"] = it_detail

    print("  → round-dollar vendor detail …")
    rd_vendors = list(set(r["vendor"] for r in data["round_dollar"] if r["vendor"]))
    rd_detail = {}
    for vn in rd_vendors[:30]:
        cur.execute("""
            SELECT Department, Object_Class, Appropriation_Name, Date, amt
            FROM _fy25_vendor WHERE Vendor=? ORDER BY amt DESC LIMIT 15
        """, (vn,))
        rd_detail[vn] = [
            {"dept": r[0], "oc": r[1], "approp": r[2], "date": r[3], "amt": r[4] or 0}
            for r in cur.fetchall()
        ]
    data["rd_detail"] = rd_detail

    print("  → round-dollar flags (potential budget-padding) …")
    cur.execute(f"""
        SELECT Vendor, Department,
               CAST(REPLACE(Amount,',','') AS REAL) AS a,
               Object_Class, Date, Appropriation_Name
        FROM spending
        WHERE Budget_Fiscal_Year='{FY}'
          AND CAST(REPLACE(Amount,',','') AS REAL) >= 1000000
          AND CAST(REPLACE(Amount,',','') AS REAL) = CAST(CAST(REPLACE(Amount,',','') AS REAL) AS INTEGER)
          AND CAST(REPLACE(Amount,',','') AS REAL) % 100000 = 0
        ORDER BY a DESC LIMIT 30
    """)
    data["round_dollar"] = [
        {"vendor": r[0], "dept": r[1], "amt": r[2], "oc": r[3], "date": r[4], "approp": r[5]}
        for r in cur.fetchall()
    ]

    print("  → IT / consulting concentration (UU + HH) …")
    cur.execute(f"""
        SELECT Vendor,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(DISTINCT Department) nd,
               COUNT(*) n
        FROM spending
        WHERE Budget_Fiscal_Year='{FY}'
          AND Object_Class IN ('(UU) IT NON-PAYROLL EXPENSES','(HH) CONSULTANT SVCS (TO DEPTS)')
          AND Vendor<>''
        GROUP BY 1 ORDER BY 2 DESC LIMIT 25
    """)
    data["it_consulting"] = [
        {"vendor": r[0], "amt": r[1] or 0, "nd": r[2], "n": r[3]} for r in cur.fetchall()
    ]

    print("  → YoY growth signals …")
    cur.execute("""
        WITH a AS (SELECT Vendor, SUM(CAST(REPLACE(Amount,',','') AS REAL)) s FROM spending
                   WHERE Budget_Fiscal_Year='2024' AND Vendor<>'' GROUP BY 1),
             b AS (SELECT Vendor, SUM(CAST(REPLACE(Amount,',','') AS REAL)) s FROM spending
                   WHERE Budget_Fiscal_Year='2025' AND Vendor<>'' GROUP BY 1)
        SELECT b.Vendor, a.s AS fy24, b.s AS fy25, (b.s - COALESCE(a.s,0)) AS delta
        FROM b LEFT JOIN a ON a.Vendor=b.Vendor
        WHERE b.s >= 5000000
        ORDER BY delta DESC LIMIT 20
    """)
    data["yoy_growth"] = [
        {"vendor": r[0], "fy24": r[1] or 0, "fy25": r[2] or 0, "delta": r[3] or 0}
        for r in cur.fetchall()
    ]

    # ---- PAYROLL DATA ----
    print("  → payroll yearly totals …")
    cur.execute("""
        SELECT year,
               COUNT(*) n,
               SUM(pay_total) total,
               SUM(pay_base) base,
               SUM(pay_overtime) ot,
               SUM(pay_buyout) buyout,
               SUM(pay_other) other
        FROM payroll WHERE year BETWEEN 2020 AND 2025
        GROUP BY 1 ORDER BY 1
    """)
    data["payroll_yearly"] = [
        {"year": r[0], "n": r[1], "total": r[2] or 0, "base": r[3] or 0,
         "ot": r[4] or 0, "buyout": r[5] or 0, "other": r[6] or 0}
        for r in cur.fetchall()
    ]

    print("  → overtime by department …")
    cur.execute("""
        SELECT department_code, department,
               SUM(pay_overtime) ot,
               SUM(pay_total) total,
               COUNT(*) n,
               SUM(CASE WHEN year=2023 THEN pay_overtime ELSE 0 END) ot23,
               SUM(CASE WHEN year=2024 THEN pay_overtime ELSE 0 END) ot24,
               SUM(CASE WHEN year=2025 THEN pay_overtime ELSE 0 END) ot25
        FROM payroll WHERE year BETWEEN 2023 AND 2025 AND pay_overtime>0
        GROUP BY department_code
        HAVING SUM(CASE WHEN year=2025 THEN pay_overtime ELSE 0 END) > 0
        ORDER BY ot25 DESC LIMIT 20
    """)
    data["ot_by_dept"] = [
        {"code": r[0], "dept": r[1], "ot": r[2] or 0, "total": r[3] or 0,
         "n": r[4], "ot23": r[5] or 0, "ot24": r[6] or 0, "ot25": r[7] or 0}
        for r in cur.fetchall()
    ]

    print("  → top earners FY2025 …")
    cur.execute("""
        SELECT name_first||' '||name_last, department, position_title, position_type,
               pay_total, pay_base, pay_overtime, pay_buyout, pay_other, annual_rate
        FROM payroll WHERE year=2025
        ORDER BY pay_total DESC LIMIT 30
    """)
    data["top_earners"] = [
        {"name": r[0], "dept": r[1], "title": r[2], "type": r[3],
         "total": r[4] or 0, "base": r[5] or 0, "ot": r[6] or 0,
         "buyout": r[7] or 0, "other": r[8] or 0, "rate": r[9] or 0}
        for r in cur.fetchall()
    ]

    print("  → employees earning 2x+ annual rate …")
    cur.execute("""
        SELECT name_first||' '||name_last, department_code, department, position_title,
               annual_rate, pay_total, pay_overtime, pay_buyout,
               ROUND(pay_total/NULLIF(annual_rate,0), 1) ratio
        FROM payroll
        WHERE year=2025 AND annual_rate>0 AND pay_total > 2*annual_rate
        ORDER BY pay_total DESC LIMIT 1000
    """)
    data["pay_ratio_flags"] = [
        {"name": r[0], "code": r[1], "dept": r[2], "title": r[3],
         "rate": r[4] or 0, "total": r[5] or 0, "ot": r[6] or 0,
         "buyout": r[7] or 0, "ratio": r[8] or 0}
        for r in cur.fetchall()
    ]

    print("  → department staffing & avg pay …")
    cur.execute("""
        SELECT department_code, department,
               COUNT(*) n,
               SUM(pay_total) total,
               AVG(pay_total) avg_pay,
               AVG(annual_rate) avg_rate,
               SUM(pay_overtime) ot,
               SUM(pay_buyout) buyout,
               SUM(CASE WHEN pay_buyout > 0 THEN 1 ELSE 0 END) buyout_count,
               CASE WHEN SUM(CASE WHEN pay_buyout > 0 THEN 1 ELSE 0 END) > 0
                    THEN SUM(pay_buyout) * 1.0 / SUM(CASE WHEN pay_buyout > 0 THEN 1 ELSE 0 END)
                    ELSE 0 END avg_buyout
        FROM payroll WHERE year=2025
        GROUP BY department_code
        ORDER BY total DESC LIMIT 25
    """)
    data["dept_staffing"] = [
        {"code": r[0], "dept": r[1], "n": r[2], "total": r[3] or 0,
         "avg_pay": r[4] or 0, "avg_rate": r[5] or 0,
         "ot": r[6] or 0, "buyout": r[7] or 0,
         "buyout_count": r[8] or 0, "avg_buyout": r[9] or 0}
        for r in cur.fetchall()
    ]

    print("  → top 100 employees per department …")
    dept_top_employees = {}
    for d in data["dept_staffing"]:
        cur.execute("""
            SELECT name_first||' '||name_last, position_title, position_type,
                   pay_total, pay_base, pay_overtime, pay_buyout, annual_rate
            FROM payroll WHERE year=2025 AND department_code=?
            ORDER BY pay_total DESC LIMIT 100
        """, (d["code"],))
        dept_top_employees[d["code"]] = [
            {"name": r[0], "title": r[1], "type": r[2],
             "total": r[3] or 0, "base": r[4] or 0, "ot": r[5] or 0,
             "buyout": r[6] or 0, "rate": r[7] or 0}
            for r in cur.fetchall()
        ]
    data["dept_top_employees"] = dept_top_employees

    print("  → bargaining group summary …")
    cur.execute("""
        SELECT bargaining_group_title,
               COUNT(*) n,
               SUM(pay_total) total,
               AVG(pay_total) avg_pay,
               SUM(pay_overtime) ot
        FROM payroll WHERE year=2025 AND bargaining_group_title<>''
        GROUP BY 1 ORDER BY total DESC LIMIT 20
    """)
    data["bargaining_groups"] = [
        {"group": r[0], "n": r[1], "total": r[2] or 0,
         "avg_pay": r[3] or 0, "ot": r[4] or 0}
        for r in cur.fetchall()
    ]

    print("  → position type breakdown …")
    cur.execute("""
        SELECT position_type,
               COUNT(*) n,
               SUM(pay_total) total,
               AVG(pay_total) avg_pay,
               SUM(pay_overtime) ot
        FROM payroll WHERE year=2025
        GROUP BY 1 ORDER BY total DESC
    """)
    data["position_types"] = [
        {"type": r[0] or "(unknown)", "n": r[1], "total": r[2] or 0,
         "avg_pay": r[3] or 0, "ot": r[4] or 0}
        for r in cur.fetchall()
    ]

    conn.close()
    data["total_fy25"] = sum(o["amt"] for o in data["object_classes"])
    return data


# -------------------- HTML builder --------------------

HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Mass FY2025 Spending Audit</title>
<style>
* { box-sizing: border-box; }
body {
  font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  margin: 0; color: #1a1a1a; background: #f7f7f5;
}
header {
  background: #111; color: #f5f5f2; padding: 24px 32px;
  border-bottom: 4px solid #c99a3b;
}
header h1 { margin: 0 0 6px; font-size: 24px; }
header p  { margin: 0; color: #bbb; font-size: 13px; }
main { max-width: 1200px; margin: 0 auto; padding: 24px 32px 80px; }
section { background: #fff; border: 1px solid #e3e3df; border-radius: 8px;
  padding: 20px 24px; margin-bottom: 24px; box-shadow: 0 1px 2px rgba(0,0,0,.03); }
section h2 { margin: 0 0 12px; font-size: 18px; border-bottom: 2px solid #111; padding-bottom: 6px; }
section p.lead { margin: 0 0 16px; color: #444; }

.kpi-row { display: flex; gap: 16px; flex-wrap: wrap; margin: 0 0 20px; }
.kpi { flex: 1 1 180px; background: #fafaf7; border: 1px solid #e3e3df; border-radius: 6px; padding: 14px 16px; }
.kpi .lbl { font-size: 11px; text-transform: uppercase; color: #777; letter-spacing: .04em; }
.kpi .val { font-size: 22px; font-weight: 600; margin-top: 4px; }
.kpi .sub { font-size: 12px; color: #555; margin-top: 2px; }

table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; padding: 6px 8px; border-bottom: 2px solid #111;
     background: #fafaf7; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: .02em; }
td { padding: 6px 8px; border-bottom: 1px solid #eee; vertical-align: top; }
td.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
td.pct-bar { width: 160px; }
tr.expandable { cursor: pointer; }
tr.expandable:hover { background: #fafaf0; }
tr.expandable td:first-child::before { content: "▸  "; color: #c99a3b; font-weight: bold; }
tr.expanded td:first-child::before { content: "▾  "; }
tr.detail { background: #fafaf7; }
tr.detail td { padding: 12px 16px 16px; }
tr.detail .subtable { border-left: 3px solid #c99a3b; padding-left: 12px; margin-bottom: 8px; }
tr.detail h4 { margin: 0 0 6px; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; color: #555; }
tr.detail .note { font-size: 12px; color: #555; font-style: italic;
                  background: #fff8e6; border-left: 3px solid #c99a3b; padding: 8px 12px; margin: 0 0 10px; }

.bar { height: 10px; background: #c99a3b; border-radius: 2px; }
.bar.red { background: #c0392b; }
.bar.grey { background: #999; }

.flag {
  display: inline-block; padding: 2px 6px; border-radius: 3px;
  font-size: 10px; font-weight: 700; letter-spacing: .04em; text-transform: uppercase;
  background: #c0392b; color: #fff; margin-left: 6px;
}
.flag.warn { background: #c99a3b; }
.flag.ok { background: #2e7d5a; }
.flag.info { background: #4a6fa5; }

nav.toc { position: sticky; top: 0; background: #f7f7f5; padding: 10px 0; margin-bottom: 14px; z-index: 10; border-bottom: 1px solid #ddd; }
nav.toc a { display: inline-block; margin-right: 14px; color: #333; text-decoration: none; font-size: 12px; font-weight: 600; }
nav.toc a:hover { color: #c99a3b; }

footer { color: #888; text-align: center; padding: 30px 0 10px; font-size: 11px; }
footer code { background: #eee; padding: 1px 4px; border-radius: 2px; }

.ctx-box { background: #eef2f8; border-left: 3px solid #4a6fa5; padding: 10px 14px; margin: 0 0 16px; font-size: 13px; color: #23395d; }
.ctx-box strong { color: #111; }
</style>
</head>
<body>
<header>
  <h1>Massachusetts State Spending Audit · Fiscal Year __FY__</h1>
  <p>Source: Comptroller CTHRU (__N_TX__ transactions, __N_PAYROLL__ payroll records) × COMMBUYS (__N_CONTRACTS__ contracts).
     Generated __TODAY__. Click any row marked ▸ to expand.</p>
</header>
<main>

<nav class="toc">
  <a href="#summary">Summary</a>
  <a href="#objectclass">By Object Class</a>
  <a href="#cabinets">By Cabinet</a>
  <a href="#vendors">Top Vendors</a>
  <a href="#payroll">Payroll</a>
  <a href="#overtime">Overtime</a>
  <a href="#competition">Competition Analysis</a>
  <a href="#waste">Attention & Waste</a>
  <a href="#trusts">Trusts</a>
  <a href="#methodology">Methodology</a>
</nav>

<section id="summary">
<h2>Executive Summary</h2>
<p class="lead">__SUMMARY_LEAD__</p>
<div id="kpis" class="kpi-row"></div>
<div id="yearlyChart"></div>
</section>

<section id="objectclass">
<h2>Spend by Object Class (click to drill down)</h2>
<p class="lead">Every line of state spending is tagged with an object class. Click any class for its top
vendors, top departments, and an explanation of what that category actually contains.</p>
<div class="ctx-box">
<strong>How to read this:</strong> The top three classes (RR Benefits, PP State Aid, DD Pensions)
are statutory / formula obligations — they are <em>not procured</em>. The procurable universe
(MM, NN, HH, UU, FF, GG, JJ, KK, LL, EE) is roughly __PROCURABLE_PCT__% of total spend.
</div>
<table id="ocTable"><thead>
<tr><th>Object Class</th><th class="num">Amount</th><th class="num">Share</th>
    <th class="pct-bar"></th><th class="num">Txns</th><th class="num">Vendors</th></tr>
</thead><tbody></tbody></table>
</section>

<section id="cabinets">
<h2>Spend by Cabinet / Secretariat</h2>
<p class="lead">Organizational view — where control and accountability live.
Click a cabinet to see its departments, then click a department to see its top vendors.</p>
<table id="cabTable"><thead>
<tr><th>Cabinet</th><th class="num">Amount</th><th class="num">Share</th>
    <th class="pct-bar"></th><th class="num">Depts</th><th class="num">Vendors</th></tr>
</thead><tbody></tbody></table>
</section>

<section id="vendors">
<h2>Top 50 Vendors (all categories)</h2>
<p class="lead">Largest counterparties by FY__FY__ payout.
Hover for number of departments/object-classes touched.</p>
<table id="vendorTable"><thead>
<tr><th>Vendor</th><th class="num">Amount</th><th class="num">Txns</th>
    <th class="num">Depts</th><th class="num">Obj Classes</th></tr>
</thead><tbody></tbody></table>
</section>

<section id="payroll">
<h2>Payroll Analysis (CTHRU Open Payroll)</h2>
<p class="lead">Individual-level payroll data for all state employees, sourced from the Comptroller's
CTHRU open payroll dataset (2.69M records, CY2010-2025). FY2025: 165,851 employees, $10.89B total compensation.</p>

<div class="ctx-box">
<strong>Public context:</strong> Massachusetts publishes individual state employee compensation through
CTHRU per M.G.L. Chapter 7 §4A. This includes executive branch, judiciary, legislature, UMass system,
and constitutional offices. Quasi-public agencies (MassDOT/MBTA, MassPort) are included since CY2018.
<br><br>
<strong>Key trend:</strong> Total overtime statewide grew from <strong>$553M (2023) → $617M (2024) → $651M (2025)</strong>,
an 18% increase over two years, significantly outpacing headcount and base pay growth.
</div>

<div id="payrollKpis" class="kpi-row"></div>
<div id="payrollYearlyChart"></div>

<h3 style="font-size:14px;margin:18px 0 6px">Department Staffing &amp; Compensation (click to drill down)</h3>
<table id="deptStaffTable"><thead>
<tr><th>Department</th><th class="num">Employees</th><th class="num">Total Pay</th>
    <th class="num">Avg Pay</th><th class="num">Overtime</th><th class="num">OT %</th>
    <th class="num">Buyouts</th><th class="num">Avg Buyout</th></tr>
</thead><tbody></tbody></table>

<h3 style="font-size:14px;margin:18px 0 6px">By Position Type</h3>
<table id="posTypeTable"><thead>
<tr><th>Position Type</th><th class="num">Count</th><th class="num">Total Pay</th>
    <th class="num">Avg Pay</th><th class="num">Overtime</th></tr>
</thead><tbody></tbody></table>

<h3 style="font-size:14px;margin:18px 0 6px">Top Bargaining Groups by Total Pay</h3>
<table id="bargainTable"><thead>
<tr><th>Bargaining Group</th><th class="num">Members</th><th class="num">Total Pay</th>
    <th class="num">Avg Pay</th><th class="num">Overtime</th></tr>
</thead><tbody></tbody></table>

<h3 style="font-size:14px;margin:18px 0 6px">Highest-Paid Employees FY2025 (click to drill down)</h3>
<table id="topEarnersTable"><thead>
<tr><th>Name</th><th>Department</th><th>Title</th><th class="num">Total Pay</th>
    <th class="num">Base</th><th class="num">Overtime</th></tr>
</thead><tbody></tbody></table>
</section>

<section id="overtime">
<h2>Overtime Deep Dive <span class="flag">$651M</span></h2>
<p class="lead">Overtime is the single largest discretionary payroll lever and a perennial audit concern.
$651M in FY2025 represents 6.0% of total pay — concentrated in corrections, state police, MBTA, and mental health facilities.</p>

<div class="ctx-box">
<strong>Why this matters:</strong> Massachusetts OIG has repeatedly flagged overtime abuse in corrections
and state police. The Suffolk County Sheriff's office (SDS) has an OT-to-pay ratio of <strong>32.4%</strong> —
nearly 1 in 3 payroll dollars is overtime. MBTA overtime ($133.6M) has been the subject of
multiple FMCB/DPU audits since 2019. State Police OT was at the center of the
<strong>Troop E overtime scandal</strong> (2017-2018 federal prosecutions).
<br><br>
Individual employees earning <strong>2-3x their annual rate</strong> are statistically anomalous and
warrant review — these are flagged in the table below.
</div>

<h3 style="font-size:14px;margin:18px 0 6px">Overtime by Department (3-year trend, click to expand) <span class="flag warn">review</span></h3>
<table id="otDeptTable"><thead>
<tr><th>Department</th><th class="num">FY23 OT</th><th class="num">FY24 OT</th>
    <th class="num">FY25 OT</th><th class="num">2yr Growth</th><th class="num">OT/Total</th>
    <th class="pct-bar"></th></tr>
</thead><tbody></tbody></table>

<h3 style="font-size:14px;margin:18px 0 6px">Employees Earning 2x+ Their Annual Rate <span class="flag">anomaly</span></h3>
<p style="font-size:12px;color:#555;margin:0 0 6px;">These individuals' total compensation exceeds
double their annual salary rate — the excess comes from overtime, buyouts, or other pay.
Common in 24/7 facilities (corrections, hospitals) but the extreme cases warrant review.</p>
<div id="payRatioPager" style="display:flex;align-items:center;gap:12px;margin-bottom:8px;font-size:13px;">
  <button id="prPrev" style="padding:4px 12px;cursor:pointer;">← Prev</button>
  <span id="prInfo"></span>
  <button id="prNext" style="padding:4px 12px;cursor:pointer;">Next →</button>
  <span style="color:#777;margin-left:auto" id="prTotal"></span>
</div>
<table id="payRatioTable"><thead>
<tr><th>Name</th><th>Dept</th><th>Title</th><th class="num">Annual Rate</th>
    <th class="num">Actual Pay</th><th class="num">Overtime</th><th class="num">Ratio</th></tr>
</thead><tbody></tbody></table>
</section>

<section id="competition">
<h2>Procurement Competition Analysis</h2>
<p class="lead">Cross-reference with scraped COMMBUYS contract database.
Shows how much FY__FY__ spend actually flowed through an openly-competed bid.</p>
<div class="ctx-box">
<strong>Key finding:</strong> Only about 11% of total FY__FY__ spend touches a COMMBUYS contract
with an OPEN competitive bid on file. The rest is either statutory (benefits, aid, pensions, debt,
payroll — not procurable) or is purchased under closed/expired/no-bid contracts.
</div>
<h3 style="font-size:14px;margin:18px 0 6px">FY__FY__ spend by vendor-contract category</h3>
<table id="matchTable"><thead>
<tr><th>Category</th><th class="num">Amount</th><th class="num">% of FY__FY__</th>
    <th class="pct-bar"></th><th class="num">Vendors</th></tr>
</thead><tbody></tbody></table>

<h3 style="font-size:14px;margin:18px 0 6px">COMMBUYS contract count by bid type</h3>
<table id="mixTable"><thead>
<tr><th>Competition Tag</th><th class="num">Contracts</th><th class="num">Vendors</th>
    <th class="num">Reported $</th></tr>
</thead><tbody></tbody></table>
</section>

<section id="waste">
<h2>Attention &amp; Potential Waste</h2>
<p class="lead">Curated red flags drawn from the data. None of these are proof of waste
— they are statistically unusual patterns that justify a closer look.</p>

<div class="ctx-box">
<strong>Methodology note:</strong> These flags use well-understood audit heuristics
(single-source contracts, round-dollar amounts, rapid YoY growth, concentration in HH/UU classes).
Public context where applicable:
<ul style="margin:6px 0 0 18px;padding:0;">
  <li><strong>Boston Medical Center</strong> receives MassHealth DSH and safety-net supplemental payments
      outside of open bidding — rate-set by EOHHS, reviewed by CHIA.</li>
  <li><strong>Chapter 70 / Chapter 90</strong> school and road aid is formula-based (G.L. c.70, c.90).</li>
  <li><strong>MassHealth MCOs</strong> (BMC HealthNet, Tufts TogetherHealth, WellSense) are capitated at
      actuarially-set PMPM rates; procurement is via periodic 5-year MCO re-procurement.</li>
  <li><strong>IT firms</strong> (Accenture, Deloitte, IBM, Tyler Technologies, Salesforce resellers) are
      the most frequent targets of OIG and Inspector General reviews for rate-card abuse
      and sole-source renewals.</li>
</ul>
</div>

<h3 style="font-size:14px;margin:18px 0 6px">Top 25 vendors with a COMMBUYS contract but no OPEN bid on file <span class="flag warn">review</span></h3>
<table id="noOpenTable"><thead>
<tr><th>Vendor</th><th class="num">FY__FY__ $</th><th>Contract categories seen</th></tr>
</thead><tbody></tbody></table>

<h3 style="font-size:14px;margin:18px 0 6px">Largest YoY growth (FY24→FY25, vendors ≥ $5M) <span class="flag info">trend</span></h3>
<table id="yoyTable"><thead>
<tr><th>Vendor</th><th class="num">FY24 $</th><th class="num">FY25 $</th><th class="num">Δ</th><th class="num">% Δ</th></tr>
</thead><tbody></tbody></table>

<h3 style="font-size:14px;margin:18px 0 6px">IT &amp; Consulting Concentration (Object Classes HH + UU) <span class="flag">focus</span></h3>
<table id="itTable"><thead>
<tr><th>Vendor</th><th class="num">Amount</th><th class="num">Depts</th><th class="num">Txns</th></tr>
</thead><tbody></tbody></table>

<h3 style="font-size:14px;margin:18px 0 6px">Round-dollar transactions ≥ $1M (multiples of $100k) <span class="flag warn">flag</span></h3>
<p style="font-size:12px;color:#555;margin:0 0 6px;">Transactions that are exact multiples of $100,000 above
$1M are statistically rare in invoice-driven spending. They often correspond to grants, transfers, or
lump-sum advances — legitimate in most cases, but worth spot-checking.</p>
<table id="roundTable"><thead>
<tr><th>Vendor</th><th>Department</th><th>Object Class</th><th>Date</th><th class="num">Amount</th></tr>
</thead><tbody></tbody></table>
</section>

<section id="trusts">
<h2>Trust-Fund Spending (3TN)</h2>
<p class="lead">Trust funds operate partially outside normal appropriation controls.
They are a recurring topic in the State Auditor's reports.</p>
<table id="trustTable"><thead>
<tr><th>Vendor</th><th class="num">Amount</th><th class="num">Txns</th></tr>
</thead><tbody></tbody></table>
</section>

<section id="methodology">
<h2>Methodology &amp; Limitations</h2>
<ul style="font-size:13px;color:#333;margin:0;padding-left:18px;">
  <li><strong>Spending data:</strong> Comptroller CTHRU open data, 47.6M transactions 2010-2026.
      FY2025 = 2.86M transactions, 31,865 distinct vendors, 160 departments.</li>
  <li><strong>Payroll data:</strong> CTHRU Open Payroll (Socrata dataset 9ttk-7vz6), 2.69M records
      covering CY2010-2025. Includes individual name, department, position, base pay,
      overtime, buyout, annual rate, bargaining group. Updated bi-weekly.</li>
  <li><strong>Contract data:</strong> 17,275 active + historical contract blankets scraped from
      COMMBUYS (www.commbuys.com), with 17,270 Purchase Order detail pages and 1,693 Bid
      detail pages fetched and parsed into structured fields.</li>
  <li><strong>Vendor matching</strong> normalizes company names (strip LLC/INC/CORP,
      punctuation, case) and joins spending ↔ contracts on the normalized key. This is
      imperfect — DBA names, mergers, and typos cause false negatives.</li>
  <li><strong>"Open bid"</strong> = a COMMBUYS <code>bid_type</code> containing "OPEN".
      "Closed" bids are historical. "No bid" = contract has no bid reference
      (typically legacy conversions, rate-set providers, or inter-agency agreements).</li>
  <li><strong>Limitations:</strong> MassDOT and DCAMM capital projects have separate procurement
      systems; they are visible in CTHRU (NN class) but not in COMMBUYS.
      MassHealth MCO payments do not go through COMMBUYS at all.</li>
  <li><strong>Nothing here alleges misconduct.</strong> Flags are patterns, not findings.</li>
</ul>
</section>

</main>

<footer>
Generated locally from <code>spending.db</code> + <code>commbuys.db</code>.
No data leaves your machine.
</footer>

<script>
const DATA = __DATA_JSON__;
const fmt$ = n => {
  if (n >= 1e9) return "$" + (n/1e9).toFixed(2) + "B";
  if (n >= 1e6) return "$" + (n/1e6).toFixed(1) + "M";
  if (n >= 1e3) return "$" + (n/1e3).toFixed(0) + "K";
  return "$" + (n||0).toFixed(0);
};
const fmt$precise = n => "$" + Math.round(n).toLocaleString();
const fmtN = n => (n||0).toLocaleString();
const pct = (n, t) => (100 * n / t).toFixed(1) + "%";

// --- KPIs ---
const total = DATA.total_fy25;
const kpiEl = document.getElementById("kpis");
const kpis = [
  ["Total FY" + DATA.fy + " Spend", fmt$(total), DATA.yearly.length ? "vs FY" + DATA.yearly[DATA.yearly.length-2].year + " " + (((total - DATA.yearly[DATA.yearly.length-2].amt)/DATA.yearly[DATA.yearly.length-2].amt*100).toFixed(1)) + "%" : ""],
  ["Transactions", fmtN(DATA.yearly[DATA.yearly.length-1].n), "~" + Math.round(DATA.yearly[DATA.yearly.length-1].n / 365) + "/day"],
  ["Distinct Vendors", fmtN(DATA.top_vendors.length >= 50 ? 31865 : DATA.top_vendors.length), "31,865 total"],
  ["Departments", "160", "across 39 cabinets"],
  ["COMMBUYS Contracts", "17,275", "17,270 PO details · 1,693 bids"],
  ["State Employees (FY25)", fmtN(DATA.payroll_yearly && DATA.payroll_yearly.length ? DATA.payroll_yearly[DATA.payroll_yearly.length-1].n : 0), "Open Payroll dataset"],
  ["Total Compensation", DATA.payroll_yearly && DATA.payroll_yearly.length ? fmt$(DATA.payroll_yearly[DATA.payroll_yearly.length-1].total) : "—", "incl. $651M overtime"],
];
kpiEl.innerHTML = kpis.map(([l,v,s]) =>
  `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
).join("");

// --- Yearly chart ---
const yc = document.getElementById("yearlyChart");
const maxY = Math.max(...DATA.yearly.map(y=>y.amt));
yc.innerHTML = "<h3 style='font-size:13px;margin:8px 0 4px;color:#555;text-transform:uppercase;letter-spacing:.04em'>6-Year Trend</h3>"
  + "<table style='width:100%'>"
  + DATA.yearly.map(y => {
      const w = y.amt/maxY*100;
      return `<tr><td style="width:60px">FY${y.year}</td>
        <td><div class="bar" style="width:${w}%"></div></td>
        <td class="num">${fmt$(y.amt)}</td></tr>`;
    }).join("")
  + "</table>";

// --- Object class table with drill-down ---
const ocBody = document.querySelector("#ocTable tbody");
const ocMax = DATA.object_classes[0].amt;
DATA.object_classes.forEach((o, i) => {
  const share = o.amt/total*100;
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${o.code}</td>
    <td class="num">${fmt$(o.amt)}</td>
    <td class="num">${share.toFixed(1)}%</td>
    <td><div class="bar" style="width:${o.amt/ocMax*100}%"></div></td>
    <td class="num">${fmtN(o.n)}</td>
    <td class="num">${fmtN(o.nv)}</td>`;
  ocBody.appendChild(tr);

  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  const vendors = DATA.oc_vendors[o.code] || [];
  const depts = DATA.oc_depts[o.code] || [];
  det.innerHTML = `<td colspan="6">
    ${o.note ? `<p class="note">${o.note}</p>` : ""}
    <div style="display:flex;gap:24px;flex-wrap:wrap">
      <div class="subtable" style="flex:1 1 320px">
        <h4>Top vendors</h4>
        <table>${vendors.slice(0,15).map(v =>
          `<tr><td>${v.vendor}</td><td class="num">${fmt$(v.amt)}</td><td class="num">${fmtN(v.n)}</td></tr>`
        ).join("")}</table>
      </div>
      <div class="subtable" style="flex:1 1 280px">
        <h4>Top departments</h4>
        <table>${depts.slice(0,10).map(d =>
          `<tr><td>${d.dept}</td><td class="num">${fmt$(d.amt)}</td></tr>`
        ).join("")}</table>
      </div>
    </div>
  </td>`;
  ocBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

// --- Cabinet table (two-level drill-down: cabinet → dept → vendors) ---
const cabBody = document.querySelector("#cabTable tbody");
const cabMax = DATA.cabinets[0].amt;
DATA.cabinets.forEach(c => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  const share = c.amt/total*100;
  tr.innerHTML = `<td>${c.name}</td>
    <td class="num">${fmt$(c.amt)}</td>
    <td class="num">${share.toFixed(1)}%</td>
    <td><div class="bar" style="width:${c.amt/cabMax*100}%"></div></td>
    <td class="num">${fmtN(c.nd)}</td>
    <td class="num">${fmtN(c.nv)}</td>`;
  cabBody.appendChild(tr);

  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  const detTd = document.createElement("td");
  detTd.colSpan = 6;

  const depts = DATA.departments.filter(d => d.cab === c.name);
  const deptMax = depts.length ? depts[0].amt : 1;

  const wrapper = document.createElement("div");
  wrapper.className = "subtable";
  wrapper.innerHTML = `<h4>Departments under ${c.name} (click department for vendor detail)</h4>`;

  const deptTbl = document.createElement("table");
  deptTbl.innerHTML = `<thead><tr>
    <th>Department</th><th class="num">Amount</th><th class="num">Txns</th>
    <th class="num">Vendors</th><th class="pct-bar" style="width:120px"></th></tr></thead>`;
  const deptBody = document.createElement("tbody");
  deptTbl.appendChild(deptBody);

  depts.forEach(d => {
    const dtr = document.createElement("tr");
    dtr.className = "expandable";
    dtr.style.cursor = "pointer";
    dtr.innerHTML = `<td>${d.dept}</td>
      <td class="num">${fmt$(d.amt)}</td>
      <td class="num">${fmtN(d.n || 0)}</td>
      <td class="num">${fmtN(d.nv || 0)}</td>
      <td><div class="bar" style="width:${d.amt/deptMax*100}%"></div></td>`;
    deptBody.appendChild(dtr);

    const vdet = document.createElement("tr");
    vdet.className = "detail"; vdet.style.display = "none";
    const vendors = (DATA.dept_vendors && DATA.dept_vendors[d.dept]) || [];
    const vtd = document.createElement("td");
    vtd.colSpan = 5;
    vtd.style.paddingLeft = "24px";
    if (vendors.length) {
      const vWrapper = document.createElement("div");
      vWrapper.className = "subtable";
      vWrapper.style.borderLeftColor = "#999";
      vWrapper.innerHTML = `<h4 style="color:#777">Top vendors in ${d.dept} (click vendor for object-class breakdown)</h4>`;
      const vTbl = document.createElement("table");
      vTbl.innerHTML = `<thead><tr><th>Vendor</th><th class="num">Amount</th><th class="num">Txns</th><th class="num">Obj Classes</th></tr></thead>`;
      const vTbody = document.createElement("tbody");
      vTbl.appendChild(vTbody);

      vendors.forEach(v => {
        const vtr = document.createElement("tr");
        vtr.className = "expandable";
        vtr.style.cursor = "pointer";
        vtr.innerHTML = `<td>${v.vendor}</td><td class="num">${fmt$(v.amt)}</td>
          <td class="num">${fmtN(v.n)}</td><td class="num">${fmtN(v.noc)}</td>`;
        vTbody.appendChild(vtr);

        const vddet = document.createElement("tr");
        vddet.className = "detail"; vddet.style.display = "none";
        const vddTd = document.createElement("td");
        vddTd.colSpan = 4; vddTd.style.paddingLeft = "32px";
        const detKey = d.dept + "|||" + v.vendor;
        const ocRows = (DATA.vendor_dept_detail && DATA.vendor_dept_detail[detKey]) || [];
        if (ocRows.length) {
          vddTd.innerHTML = `<div class="subtable" style="border-left-color:#ccc">
            <h4 style="color:#999">Object class breakdown for ${v.vendor}</h4>
            <table><thead><tr><th>Object Class</th><th class="num">Amount</th><th class="num">Txns</th></tr></thead>
            <tbody>${ocRows.map(o =>
              `<tr><td style="font-size:11px">${o.oc}</td><td class="num">${fmt$(o.amt)}</td><td class="num">${fmtN(o.n)}</td></tr>`
            ).join("")}</tbody></table></div>`;
        } else {
          vddTd.innerHTML = `<p style="font-size:11px;color:#bbb;margin:4px 0">No further detail available.</p>`;
        }
        vddet.appendChild(vddTd);
        vTbody.appendChild(vddet);

        vtr.addEventListener("click", (ev) => {
          ev.stopPropagation();
          const open = vddet.style.display !== "none";
          vddet.style.display = open ? "none" : "";
          vtr.classList.toggle("expanded", !open);
        });
      });

      vWrapper.appendChild(vTbl);
      vtd.appendChild(vWrapper);
    } else {
      vtd.innerHTML = `<p style="font-size:12px;color:#999;margin:4px 0">Vendor detail not available for this department.</p>`;
    }
    vdet.appendChild(vtd);
    deptBody.appendChild(vdet);

    dtr.addEventListener("click", (ev) => {
      ev.stopPropagation();
      const open = vdet.style.display !== "none";
      vdet.style.display = open ? "none" : "";
      dtr.classList.toggle("expanded", !open);
    });
  });

  wrapper.appendChild(deptTbl);
  detTd.appendChild(wrapper);
  det.appendChild(detTd);
  cabBody.appendChild(det);

  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

// --- Top vendor table (with drill-down) ---
const vBody = document.querySelector("#vendorTable tbody");
DATA.top_vendors.forEach(v => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${v.vendor}</td>
    <td class="num">${fmt$(v.amt)}</td>
    <td class="num">${fmtN(v.n)}</td>
    <td class="num">${fmtN(v.nd)}</td>
    <td class="num">${fmtN(v.noc)}</td>`;
  vBody.appendChild(tr);

  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  const detTd = document.createElement("td");
  detTd.colSpan = 5;

  const vd = (DATA.vendor_detail && DATA.vendor_detail[v.vendor]) || {};
  const byDept = vd.by_dept || [];
  const byOc = vd.by_oc || [];
  const topTxns = vd.top_txns || [];

  detTd.innerHTML = `
    <div style="display:flex;gap:24px;flex-wrap:wrap">
      <div class="subtable" style="flex:1 1 320px">
        <h4>By Department</h4>
        <table>
          <thead><tr><th>Department</th><th class="num">Amount</th><th class="num">Txns</th></tr></thead>
          <tbody>${byDept.map(d =>
            `<tr><td>${d.dept}</td><td class="num">${fmt$(d.amt)}</td><td class="num">${fmtN(d.n)}</td></tr>`
          ).join("")}</tbody>
        </table>
      </div>
      <div class="subtable" style="flex:1 1 320px">
        <h4>By Object Class</h4>
        <table>
          <thead><tr><th>Object Class</th><th class="num">Amount</th><th class="num">Txns</th></tr></thead>
          <tbody>${byOc.map(o =>
            `<tr><td style="font-size:12px">${o.oc}</td><td class="num">${fmt$(o.amt)}</td><td class="num">${fmtN(o.n)}</td></tr>`
          ).join("")}</tbody>
        </table>
      </div>
    </div>
    ${topTxns.length ? `
    <div class="subtable" style="margin-top:8px">
      <h4>Top 10 Largest Transactions</h4>
      <table>
        <thead><tr><th>Department</th><th>Object Class</th><th>Appropriation</th><th>Date</th><th class="num">Amount</th></tr></thead>
        <tbody>${topTxns.map(t =>
          `<tr><td style="font-size:12px">${t.dept}</td>
               <td style="font-size:11px">${t.oc}</td>
               <td style="font-size:11px">${t.approp||""}</td>
               <td style="font-size:12px">${t.date||""}</td>
               <td class="num">${fmt$(t.amt)}</td></tr>`
        ).join("")}</tbody>
      </table>
    </div>` : ""}`;

  det.appendChild(detTd);
  vBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

// --- helper: render a vendor+txn drill-down ---
function buildVendorTxnDrilldown(vendors, txnLookup, parentCols) {
  const frag = document.createDocumentFragment();
  vendors.forEach(v => {
    const vtr = document.createElement("tr");
    vtr.className = "expandable";
    vtr.innerHTML = `<td>${v.vendor}</td>
      <td class="num">${fmt$(v.amt)}</td>
      <td class="num">${fmtN(v.n)}</td>
      <td class="num">${fmtN(v.nd || 0)}</td>`;
    frag.appendChild(vtr);

    const vdet = document.createElement("tr");
    vdet.className = "detail"; vdet.style.display = "none";
    const vtd = document.createElement("td");
    vtd.colSpan = parentCols;
    const txns = (txnLookup && txnLookup[v.vendor]) || [];
    vtd.innerHTML = txns.length ? `<div class="subtable">
      <h4>Top transactions for ${v.vendor}</h4>
      <table><thead><tr><th>Department</th><th>Object Class</th><th>Appropriation</th><th>Date</th><th class="num">Amount</th></tr></thead>
      <tbody>${txns.map(t =>
        `<tr><td style="font-size:12px">${t.dept}</td><td style="font-size:11px">${t.oc}</td>
         <td style="font-size:11px">${t.approp||""}</td><td style="font-size:12px">${t.date||""}</td>
         <td class="num">${fmt$(t.amt)}</td></tr>`
      ).join("")}</tbody></table></div>`
      : `<p style="font-size:12px;color:#999">No transaction detail available.</p>`;
    vdet.appendChild(vtd);
    frag.appendChild(vdet);

    vtr.addEventListener("click", (ev) => {
      ev.stopPropagation();
      const open = vdet.style.display !== "none";
      vdet.style.display = open ? "none" : "";
      vtr.classList.toggle("expanded", !open);
    });
  });
  return frag;
}

// --- Match table (3-tier: category → vendors → transactions) ---
const mBody = document.querySelector("#matchTable tbody");
const totalMatch = DATA.fy25_by_match.reduce((s,x)=>s+x.amt,0);
const maxMatch = Math.max(...DATA.fy25_by_match.map(x=>x.amt));
const matchLabel = (cat) => ({
  "HAS_OPEN_BID": ["Open competitive bid on file", "ok"],
  "CLOSED_BID_ONLY": ["Closed / expired bid only", "warn"],
  "NO_BID_REF": ["Contract with NO bid reference", "warn"],
  "NO_COMMBUYS_CONTRACT": ["No COMMBUYS contract (statutory / capital / payroll)", "info"],
  "OTHER": ["Other", "info"],
}[cat] || [cat, "info"]);
DATA.fy25_by_match.forEach(m => {
  const [lbl, tag] = matchLabel(m.cat);
  const tr = document.createElement("tr");
  tr.className = "expandable";
  const cls = tag === "ok" ? "" : tag === "warn" ? "red" : "grey";
  tr.innerHTML = `<td>${lbl} <span class="flag ${tag}">${m.cat}</span></td>
    <td class="num">${fmt$(m.amt)}</td>
    <td class="num">${pct(m.amt, totalMatch)}</td>
    <td><div class="bar ${cls}" style="width:${m.amt/maxMatch*100}%"></div></td>
    <td class="num">${fmtN(m.nv)}</td>`;
  mBody.appendChild(tr);

  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  const detTd = document.createElement("td");
  detTd.colSpan = 5;
  const vendors = (DATA.match_cat_vendors && DATA.match_cat_vendors[m.cat]) || [];
  if (vendors.length) {
    const wrapper = document.createElement("div");
    wrapper.className = "subtable";
    wrapper.innerHTML = `<h4>Top vendors in "${m.cat}" (click for transactions)</h4>`;
    const vtbl = document.createElement("table");
    vtbl.innerHTML = `<thead><tr><th>Vendor</th><th class="num">Amount</th><th class="num">Txns</th><th class="num">Depts</th></tr></thead>`;
    const vtbody = document.createElement("tbody");
    vtbody.appendChild(buildVendorTxnDrilldown(vendors, DATA.match_vendor_txns, 4));
    vtbl.appendChild(vtbody);
    wrapper.appendChild(vtbl);
    detTd.appendChild(wrapper);
  }
  det.appendChild(detTd);
  mBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

// --- Competition mix (2-tier: category → top contracts) ---
const mixBody = document.querySelector("#mixTable tbody");
DATA.competition_mix.forEach(m => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${m.cat}</td>
    <td class="num">${fmtN(m.n)}</td>
    <td class="num">${fmtN(m.nv)}</td>
    <td class="num">${fmt$(m.spent)}</td>`;
  mixBody.appendChild(tr);

  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  const contracts = (DATA.comp_mix_contracts && DATA.comp_mix_contracts[m.cat]) || [];
  det.innerHTML = `<td colspan="4">
    ${contracts.length ? `<div class="subtable"><h4>Top contracts (${m.cat})</h4>
    <table><thead><tr><th>Vendor</th><th>Blanket ID</th><th>Description</th><th>Org</th>
      <th class="num">Spent</th><th class="num">Limit</th></tr></thead>
    <tbody>${contracts.map(c =>
      `<tr><td>${c.vendor}</td><td style="font-size:11px">${c.blanket}</td>
       <td style="font-size:11px">${(c.desc||"").substring(0,60)}</td>
       <td style="font-size:11px">${c.org||""}</td>
       <td class="num">${fmt$(c.spent)}</td><td class="num">${fmt$(c.limit)}</td></tr>`
    ).join("")}</tbody></table></div>` : '<p style="font-size:12px;color:#999">No contracts.</p>'}
  </td>`;
  mixBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

// --- No-open-bid table (3-tier: vendor → depts → transactions) ---
const noBody = document.querySelector("#noOpenTable tbody");
DATA.no_open_bid_fy25.forEach(v => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${v.vendor}</td>
    <td class="num">${fmt$(v.amt)}</td>
    <td style="font-size:11px;color:#666">${v.cats || ""}</td>`;
  noBody.appendChild(tr);

  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  const detTd = document.createElement("td");
  detTd.colSpan = 3;
  const vd = (DATA.no_open_detail && DATA.no_open_detail[v.vendor]) || {};
  const byDept = vd.by_dept || [];
  const txns = vd.txns || [];
  detTd.innerHTML = `<div style="display:flex;gap:20px;flex-wrap:wrap">
    <div class="subtable" style="flex:1 1 300px"><h4>By Department</h4>
      <table><thead><tr><th>Department</th><th class="num">Amount</th><th class="num">Txns</th></tr></thead>
      <tbody>${byDept.map(d => `<tr><td>${d.dept}</td><td class="num">${fmt$(d.amt)}</td><td class="num">${fmtN(d.n)}</td></tr>`).join("")}</tbody></table>
    </div>
    ${txns.length ? `<div class="subtable" style="flex:1 1 400px"><h4>Top 10 Transactions</h4>
      <table><thead><tr><th>Department</th><th>Object Class</th><th>Appropriation</th><th>Date</th><th class="num">Amount</th></tr></thead>
      <tbody>${txns.map(t => `<tr><td style="font-size:12px">${t.dept}</td><td style="font-size:11px">${t.oc}</td>
        <td style="font-size:11px">${t.approp||""}</td><td style="font-size:12px">${t.date||""}</td>
        <td class="num">${fmt$(t.amt)}</td></tr>`).join("")}</tbody></table>
    </div>` : ""}
  </div>`;
  det.appendChild(detTd);
  noBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

// --- YoY (3-tier: vendor → depts → transactions) ---
const yBody = document.querySelector("#yoyTable tbody");
DATA.yoy_growth.forEach(v => {
  const pctchg = v.fy24 > 0 ? ((v.delta/v.fy24)*100).toFixed(0)+"%" : "new";
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${v.vendor}</td>
    <td class="num">${fmt$(v.fy24)}</td>
    <td class="num">${fmt$(v.fy25)}</td>
    <td class="num">${v.delta>0?"+":""}${fmt$(v.delta)}</td>
    <td class="num">${pctchg}</td>`;
  yBody.appendChild(tr);

  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  const detTd = document.createElement("td");
  detTd.colSpan = 5;
  const vd = (DATA.yoy_detail && DATA.yoy_detail[v.vendor]) || {};
  const byDept = vd.by_dept || [];
  const txns = vd.txns || [];
  detTd.innerHTML = `<div style="display:flex;gap:20px;flex-wrap:wrap">
    <div class="subtable" style="flex:1 1 300px"><h4>FY25 by Department</h4>
      <table><thead><tr><th>Department</th><th class="num">Amount</th><th class="num">Txns</th></tr></thead>
      <tbody>${byDept.map(d => `<tr><td>${d.dept}</td><td class="num">${fmt$(d.amt)}</td><td class="num">${fmtN(d.n)}</td></tr>`).join("")}</tbody></table>
    </div>
    ${txns.length ? `<div class="subtable" style="flex:1 1 400px"><h4>Top 10 Transactions (FY25)</h4>
      <table><thead><tr><th>Department</th><th>Object Class</th><th>Appropriation</th><th>Date</th><th class="num">Amount</th></tr></thead>
      <tbody>${txns.map(t => `<tr><td style="font-size:12px">${t.dept}</td><td style="font-size:11px">${t.oc}</td>
        <td style="font-size:11px">${t.approp||""}</td><td style="font-size:12px">${t.date||""}</td>
        <td class="num">${fmt$(t.amt)}</td></tr>`).join("")}</tbody></table>
    </div>` : ""}
  </div>`;
  det.appendChild(detTd);
  yBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

// --- IT/consulting (3-tier: vendor → depts → transactions) ---
const itBody = document.querySelector("#itTable tbody");
DATA.it_consulting.forEach(v => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${v.vendor}</td>
    <td class="num">${fmt$(v.amt)}</td>
    <td class="num">${fmtN(v.nd)}</td>
    <td class="num">${fmtN(v.n)}</td>`;
  itBody.appendChild(tr);

  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  const detTd = document.createElement("td");
  detTd.colSpan = 4;
  const vd = (DATA.it_detail && DATA.it_detail[v.vendor]) || {};
  const byDept = vd.by_dept || [];
  const txns = vd.txns || [];
  detTd.innerHTML = `<div style="display:flex;gap:20px;flex-wrap:wrap">
    <div class="subtable" style="flex:1 1 300px"><h4>By Department (HH+UU only)</h4>
      <table><thead><tr><th>Department</th><th class="num">Amount</th><th class="num">Txns</th></tr></thead>
      <tbody>${byDept.map(d => `<tr><td>${d.dept}</td><td class="num">${fmt$(d.amt)}</td><td class="num">${fmtN(d.n)}</td></tr>`).join("")}</tbody></table>
    </div>
    ${txns.length ? `<div class="subtable" style="flex:1 1 400px"><h4>Top 10 IT/Consulting Transactions</h4>
      <table><thead><tr><th>Department</th><th>Object Class</th><th>Appropriation</th><th>Date</th><th class="num">Amount</th></tr></thead>
      <tbody>${txns.map(t => `<tr><td style="font-size:12px">${t.dept}</td><td style="font-size:11px">${t.oc}</td>
        <td style="font-size:11px">${t.approp||""}</td><td style="font-size:12px">${t.date||""}</td>
        <td class="num">${fmt$(t.amt)}</td></tr>`).join("")}</tbody></table>
    </div>` : ""}
  </div>`;
  det.appendChild(detTd);
  itBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

// --- Round dollar (2-tier: transaction → all vendor transactions) ---
const rBody = document.querySelector("#roundTable tbody");
DATA.round_dollar.forEach(v => {
  const tr = document.createElement("tr");
  tr.className = "expandable";
  tr.innerHTML = `<td>${v.vendor}</td>
    <td>${v.dept}</td>
    <td style="font-size:11px">${v.oc}</td>
    <td>${v.date||""}</td>
    <td class="num">${fmt$precise(v.amt)}</td>`;
  rBody.appendChild(tr);

  const det = document.createElement("tr");
  det.className = "detail"; det.style.display = "none";
  const txns = (DATA.rd_detail && DATA.rd_detail[v.vendor]) || [];
  det.innerHTML = `<td colspan="5">
    ${txns.length ? `<div class="subtable"><h4>All top transactions for ${v.vendor}</h4>
      <table><thead><tr><th>Department</th><th>Object Class</th><th>Appropriation</th><th>Date</th><th class="num">Amount</th></tr></thead>
      <tbody>${txns.map(t => `<tr><td style="font-size:12px">${t.dept}</td><td style="font-size:11px">${t.oc}</td>
        <td style="font-size:11px">${t.approp||""}</td><td style="font-size:12px">${t.date||""}</td>
        <td class="num">${fmt$(t.amt)}</td></tr>`).join("")}</tbody></table></div>`
      : '<p style="font-size:12px;color:#999">No detail.</p>'}
  </td>`;
  rBody.appendChild(det);
  tr.addEventListener("click", () => {
    const open = det.style.display !== "none";
    det.style.display = open ? "none" : "";
    tr.classList.toggle("expanded", !open);
  });
});

// --- Trusts ---
const trBody = document.querySelector("#trustTable tbody");
DATA.trust_vendors.forEach(v => {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td>${v.vendor}</td>
    <td class="num">${fmt$(v.amt)}</td>
    <td class="num">${fmtN(v.n)}</td>`;
  trBody.appendChild(tr);
});

// ===================== PAYROLL SECTIONS =====================

// --- Payroll KPIs ---
if (DATA.payroll_yearly && DATA.payroll_yearly.length) {
  const py25 = DATA.payroll_yearly[DATA.payroll_yearly.length - 1];
  const py24 = DATA.payroll_yearly.length > 1 ? DATA.payroll_yearly[DATA.payroll_yearly.length - 2] : null;
  const payGrowth = py24 ? ((py25.total - py24.total) / py24.total * 100).toFixed(1) : "";
  const otGrowth23to25 = DATA.payroll_yearly.length >= 3 ?
    ((py25.ot - DATA.payroll_yearly[DATA.payroll_yearly.length - 3].ot) / DATA.payroll_yearly[DATA.payroll_yearly.length - 3].ot * 100).toFixed(0) : "";
  const pkEl = document.getElementById("payrollKpis");
  pkEl.innerHTML = [
    ["Employees (FY25)", fmtN(py25.n), py24 ? (py25.n - py24.n > 0 ? "+" : "") + fmtN(py25.n - py24.n) + " vs FY24" : ""],
    ["Total Compensation", fmt$(py25.total), payGrowth ? payGrowth + "% YoY" : ""],
    ["Overtime", fmt$(py25.ot), otGrowth23to25 ? "+" + otGrowth23to25 + "% since FY23" : ""],
    ["Buyouts", fmt$(py25.buyout), "Separation/termination payouts"],
    ["OT as % of Pay", (py25.ot/py25.total*100).toFixed(1)+"%", ""],
  ].map(([l,v,s]) =>
    `<div class="kpi"><div class="lbl">${l}</div><div class="val">${v}</div><div class="sub">${s}</div></div>`
  ).join("");

  // Payroll yearly chart
  const pyc = document.getElementById("payrollYearlyChart");
  const pyMax = Math.max(...DATA.payroll_yearly.map(y=>y.total));
  pyc.innerHTML = "<h3 style='font-size:13px;margin:8px 0 4px;color:#555;text-transform:uppercase;letter-spacing:.04em'>Payroll Trend (total pay = base + OT + buyout + other)</h3>"
    + "<table style='width:100%'>"
    + DATA.payroll_yearly.map(y => {
        const baseW = y.base/pyMax*100;
        const otW = y.ot/pyMax*100;
        const otherW = (y.buyout+y.other)/pyMax*100;
        return `<tr><td style="width:60px">FY${y.year}</td>
          <td><div style="display:flex;gap:1px">
            <div class="bar" style="width:${baseW}%" title="Base: ${fmt$(y.base)}"></div>
            <div class="bar red" style="width:${otW}%" title="OT: ${fmt$(y.ot)}"></div>
            <div class="bar grey" style="width:${otherW}%" title="Buyout+Other: ${fmt$(y.buyout+y.other)}"></div>
          </div></td>
          <td class="num">${fmt$(y.total)}</td>
          <td class="num" style="font-size:11px;color:#c0392b">OT ${fmt$(y.ot)}</td></tr>`;
      }).join("")
    + "</table>"
    + "<div style='font-size:11px;margin:4px 0 0;color:#777'>"
    + "<span style='display:inline-block;width:10px;height:10px;background:#c99a3b;border-radius:2px;margin-right:3px'></span>Base "
    + "<span style='display:inline-block;width:10px;height:10px;background:#c0392b;border-radius:2px;margin:0 3px 0 10px'></span>Overtime "
    + "<span style='display:inline-block;width:10px;height:10px;background:#999;border-radius:2px;margin:0 3px 0 10px'></span>Buyout + Other</div>";
}

// --- Dept staffing table ---
const dsBody = document.querySelector("#deptStaffTable tbody");
if (DATA.dept_staffing) {
  DATA.dept_staffing.forEach(d => {
    const otPct = d.total > 0 ? (d.ot/d.total*100).toFixed(1)+"%" : "0%";
    const tr = document.createElement("tr");
    tr.className = "expandable";
    tr.innerHTML = `<td>${d.dept || d.code}</td>
      <td class="num">${fmtN(d.n)}</td>
      <td class="num">${fmt$(d.total)}</td>
      <td class="num">${fmt$(d.avg_pay)}</td>
      <td class="num">${fmt$(d.ot)}</td>
      <td class="num">${otPct}</td>
      <td class="num">${d.buyout_count > 0 ? fmtN(d.buyout_count) : "—"}</td>
      <td class="num">${d.buyout_count > 0 ? fmt$(d.avg_buyout) : "—"}</td>`;
    dsBody.appendChild(tr);

    // Build drill-down with summary stats + paginated top-100 employees
    const det = document.createElement("tr");
    det.className = "detail"; det.style.display = "none";
    const emps = (DATA.dept_top_employees && DATA.dept_top_employees[d.code]) || [];
    const EMP_PAGE = 20;
    const detTd = document.createElement("td");
    detTd.colSpan = 8;

    // Summary row
    const summaryDiv = document.createElement("div");
    summaryDiv.style.cssText = "display:flex;gap:20px;flex-wrap:wrap;margin-bottom:12px";
    summaryDiv.innerHTML = `
      <div class="subtable" style="flex:1"><h4>Avg annual rate</h4><p>${fmt$(d.avg_rate)}</p></div>
      <div class="subtable" style="flex:1"><h4>Buyout total</h4><p>${fmt$(d.buyout)}</p></div>
      <div class="subtable" style="flex:1"><h4># Buyouts</h4><p>${fmtN(d.buyout_count)}</p></div>
      <div class="subtable" style="flex:1"><h4>Avg buyout cost</h4><p>${d.buyout_count > 0 ? fmt$(d.avg_buyout) : "—"}</p></div>
      <div class="subtable" style="flex:1"><h4>OT as % of total</h4><p style="font-size:20px;font-weight:700;${d.total>0&&d.ot/d.total>0.15?'color:#c0392b':''}">${otPct}</p></div>`;
    detTd.appendChild(summaryDiv);

    // Top employees sub-section
    if (emps.length) {
      const empSection = document.createElement("div");
      empSection.className = "subtable";
      empSection.innerHTML = `<h4>Top ${emps.length} highest-paid employees</h4>`;

      const pagerDiv = document.createElement("div");
      pagerDiv.style.cssText = "display:flex;align-items:center;gap:8px;margin-bottom:6px;font-size:12px";
      const prevBtn = document.createElement("button");
      prevBtn.textContent = "← Prev"; prevBtn.style.cssText = "padding:2px 8px;cursor:pointer;font-size:11px";
      const nextBtn = document.createElement("button");
      nextBtn.textContent = "Next →"; nextBtn.style.cssText = "padding:2px 8px;cursor:pointer;font-size:11px";
      const infoSpan = document.createElement("span");
      pagerDiv.appendChild(prevBtn);
      pagerDiv.appendChild(infoSpan);
      pagerDiv.appendChild(nextBtn);
      empSection.appendChild(pagerDiv);

      const empTbl = document.createElement("table");
      empTbl.innerHTML = `<thead><tr>
        <th>#</th><th>Name</th><th>Title</th><th>Type</th>
        <th class="num">Total</th><th class="num">Base</th>
        <th class="num">OT</th><th class="num">Buyout</th><th class="num">Rate</th>
      </tr></thead>`;
      const empBody = document.createElement("tbody");
      empTbl.appendChild(empBody);
      empSection.appendChild(empTbl);
      detTd.appendChild(empSection);

      let ePage = 0;
      const ePages = Math.ceil(emps.length / EMP_PAGE);
      function renderEmpPage() {
        empBody.innerHTML = "";
        const start = ePage * EMP_PAGE;
        emps.slice(start, start + EMP_PAGE).forEach((e, i) => {
          const r = document.createElement("tr");
          const ratioFlag = e.rate > 0 && e.total/e.rate > 1.5 ? ' style="color:#c0392b"' : '';
          r.innerHTML = `<td style="color:#999;font-size:11px">${start+i+1}</td>
            <td>${e.name}</td>
            <td style="font-size:11px">${e.title}</td>
            <td style="font-size:11px">${e.type}</td>
            <td class="num"${ratioFlag}>${fmt$(e.total)}</td>
            <td class="num">${fmt$(e.base)}</td>
            <td class="num">${e.ot > 0 ? fmt$(e.ot) : "—"}</td>
            <td class="num">${e.buyout > 0 ? fmt$(e.buyout) : "—"}</td>
            <td class="num">${fmt$(e.rate)}</td>`;
          empBody.appendChild(r);
        });
        infoSpan.textContent = `Page ${ePage+1}/${ePages}`;
        prevBtn.disabled = ePage === 0; prevBtn.style.opacity = ePage === 0 ? 0.4 : 1;
        nextBtn.disabled = ePage >= ePages-1; nextBtn.style.opacity = ePage >= ePages-1 ? 0.4 : 1;
      }
      prevBtn.addEventListener("click", (ev) => { ev.stopPropagation(); if (ePage > 0) { ePage--; renderEmpPage(); } });
      nextBtn.addEventListener("click", (ev) => { ev.stopPropagation(); if (ePage < ePages-1) { ePage++; renderEmpPage(); } });
      renderEmpPage();
    }

    det.appendChild(detTd);
    dsBody.appendChild(det);
    tr.addEventListener("click", () => {
      const open = det.style.display !== "none";
      det.style.display = open ? "none" : "";
      tr.classList.toggle("expanded", !open);
    });
  });
}

// --- Position type table ---
const ptBody = document.querySelector("#posTypeTable tbody");
if (DATA.position_types) {
  DATA.position_types.forEach(p => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${p.type}</td>
      <td class="num">${fmtN(p.n)}</td>
      <td class="num">${fmt$(p.total)}</td>
      <td class="num">${fmt$(p.avg_pay)}</td>
      <td class="num">${fmt$(p.ot)}</td>`;
    ptBody.appendChild(tr);
  });
}

// --- Bargaining groups ---
const bgBody = document.querySelector("#bargainTable tbody");
if (DATA.bargaining_groups) {
  DATA.bargaining_groups.forEach(g => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${g.group}</td>
      <td class="num">${fmtN(g.n)}</td>
      <td class="num">${fmt$(g.total)}</td>
      <td class="num">${fmt$(g.avg_pay)}</td>
      <td class="num">${fmt$(g.ot)}</td>`;
    bgBody.appendChild(tr);
  });
}

// --- Top earners ---
const teBody = document.querySelector("#topEarnersTable tbody");
if (DATA.top_earners) {
  DATA.top_earners.forEach(e => {
    const tr = document.createElement("tr");
    tr.className = "expandable";
    tr.innerHTML = `<td>${e.name}</td>
      <td style="font-size:12px">${e.dept}</td>
      <td style="font-size:12px">${e.title}</td>
      <td class="num">${fmt$(e.total)}</td>
      <td class="num">${fmt$(e.base)}</td>
      <td class="num">${e.ot > 0 ? fmt$(e.ot) : "—"}</td>`;
    teBody.appendChild(tr);
    const det = document.createElement("tr");
    det.className = "detail"; det.style.display = "none";
    det.innerHTML = `<td colspan="6">
      <div style="display:flex;gap:20px;flex-wrap:wrap">
        <div class="subtable"><h4>Annual Rate</h4><p>${fmt$(e.rate)}</p></div>
        <div class="subtable"><h4>Buyout</h4><p>${fmt$(e.buyout)}</p></div>
        <div class="subtable"><h4>Other Pay</h4><p>${fmt$(e.other)}</p></div>
        <div class="subtable"><h4>Type</h4><p>${e.type}</p></div>
        ${e.rate > 0 && e.total/e.rate > 1.5 ? '<div class="subtable"><h4>Pay/Rate Ratio</h4><p style="color:#c0392b;font-weight:700">'+(e.total/e.rate).toFixed(1)+'x</p></div>' : ''}
      </div></td>`;
    teBody.appendChild(det);
    tr.addEventListener("click", () => {
      const open = det.style.display !== "none";
      det.style.display = open ? "none" : "";
      tr.classList.toggle("expanded", !open);
    });
  });
}

// --- OT by department (3-year trend) ---
const otdBody = document.querySelector("#otDeptTable tbody");
if (DATA.ot_by_dept) {
  const otMax = Math.max(...DATA.ot_by_dept.map(d=>d.ot25));
  DATA.ot_by_dept.forEach(d => {
    const growth = d.ot23 > 0 ? ((d.ot25 - d.ot23)/d.ot23*100).toFixed(0) + "%" : "n/a";
    const otPct = d.total > 0 ? (d.ot/d.total*100).toFixed(1)+"%" : "";
    const tr = document.createElement("tr");
    tr.className = "expandable";
    const growthColor = d.ot23 > 0 && (d.ot25-d.ot23)/d.ot23 > 0.2 ? "color:#c0392b;font-weight:700" : "";
    tr.innerHTML = `<td>${d.dept || d.code}</td>
      <td class="num">${fmt$(d.ot23)}</td>
      <td class="num">${fmt$(d.ot24)}</td>
      <td class="num">${fmt$(d.ot25)}</td>
      <td class="num" style="${growthColor}">${growth}</td>
      <td class="num">${otPct}</td>
      <td><div class="bar red" style="width:${d.ot25/otMax*100}%"></div></td>`;
    otdBody.appendChild(tr);
  });
}

// --- Pay ratio flags (paginated) ---
if (DATA.pay_ratio_flags && DATA.pay_ratio_flags.length) {
  const PR_PAGE = 25;
  let prPage = 0;
  const prData = DATA.pay_ratio_flags;
  const prBody = document.querySelector("#payRatioTable tbody");
  const prInfo = document.getElementById("prInfo");
  const prTotal = document.getElementById("prTotal");
  const prPrev = document.getElementById("prPrev");
  const prNext = document.getElementById("prNext");
  const prPages = Math.ceil(prData.length / PR_PAGE);

  function renderPrPage() {
    prBody.innerHTML = "";
    const start = prPage * PR_PAGE;
    const slice = prData.slice(start, start + PR_PAGE);
    slice.forEach((p, i) => {
      const tr = document.createElement("tr");
      const ratioStyle = p.ratio >= 3 ? "color:#c0392b;font-weight:700" : "font-weight:600";
      tr.innerHTML = `<td><span style="color:#999;font-size:11px;margin-right:4px">${start+i+1}.</span>${p.name}</td>
        <td style="font-size:12px">${p.code}</td>
        <td style="font-size:12px">${p.title}</td>
        <td class="num">${fmt$(p.rate)}</td>
        <td class="num">${fmt$(p.total)}</td>
        <td class="num">${fmt$(p.ot)}</td>
        <td class="num" style="${ratioStyle}">${p.ratio}x</td>`;
      prBody.appendChild(tr);
    });
    prInfo.textContent = `Page ${prPage+1} of ${prPages}`;
    prTotal.textContent = `${prData.length} employees total`;
    prPrev.disabled = prPage === 0;
    prNext.disabled = prPage >= prPages - 1;
    prPrev.style.opacity = prPage === 0 ? 0.4 : 1;
    prNext.style.opacity = prPage >= prPages - 1 ? 0.4 : 1;
  }

  prPrev.addEventListener("click", () => { if (prPage > 0) { prPage--; renderPrPage(); } });
  prNext.addEventListener("click", () => { if (prPage < prPages - 1) { prPage++; renderPrPage(); } });
  renderPrPage();
}
</script>
</body>
</html>
"""


def main():
    import datetime
    print("Loading data …")
    data = load_data()

    procurable_keys = {"MM","NN","HH","UU","FF","GG","JJ","KK","LL","EE","BB"}
    procurable = sum(o["amt"] for o in data["object_classes"] if o["key"] in procurable_keys)
    procurable_pct = f"{procurable/data['total_fy25']*100:.0f}"

    summary = (
        f"Total FY{FY} outlays were "
        f"${data['total_fy25']/1e9:.1f}B across {data['yearly'][-1]['n']:,} transactions, "
        f"31,865 vendors, 160 departments and 39 cabinets. "
        f"About {procurable_pct}% falls in classes that are, in principle, biddable. "
        f"The remainder is statutory — benefits, local aid, pensions, payroll, and debt service."
    )

    n_payroll = data["payroll_yearly"][-1]["n"] if data.get("payroll_yearly") else 0

    html_out = (HTML_TEMPLATE
        .replace("__FY__", FY)
        .replace("__N_TX__", f"{data['yearly'][-1]['n']:,}")
        .replace("__N_PAYROLL__", f"{n_payroll:,}")
        .replace("__N_CONTRACTS__", "17,275")
        .replace("__TODAY__", datetime.date.today().isoformat())
        .replace("__SUMMARY_LEAD__", summary)
        .replace("__PROCURABLE_PCT__", procurable_pct)
        .replace("__DATA_JSON__", json.dumps(data)))

    with open(OUT, "w") as f:
        f.write(html_out)
    print(f"wrote {OUT} ({len(html_out)/1024:.0f} KB)")


if __name__ == "__main__":
    main()
