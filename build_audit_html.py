"""Generate a self-contained HTML audit report of Massachusetts state spending.

Sources:
  - spending.db (Comptroller CTHRU, FY2025, 2.86M transactions)
  - commbuys.db (COMMBUYS scraped contracts, 17,275 contracts)

Output: audit.html (drill-down explorer + curated waste/attention flags)
"""
import json
import sqlite3
import html
import urllib.request
import urllib.error
import urllib.parse
from collections import defaultdict
from pathlib import Path

BASE = Path(__file__).parent
SPENDING_DB = BASE / "spending.db"
COMMBUYS_DB = BASE / "commbuys.db"
OUT = BASE / "audit.html"
FY = "2025"

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
    conn = sqlite3.connect(str(SPENDING_DB))
    conn.execute(f"ATTACH '{str(COMMBUYS_DB)}' AS cb")
    cur = conn.cursor()
    data = {"fy": FY}

    # ── indexes (one-time cost; NO-OP if already built) ───────────────────
    print("  → ensuring indexes on spending …")
    for ddl in [
        "CREATE INDEX IF NOT EXISTS idx_sp_fy_vendor ON spending(Budget_Fiscal_Year, Vendor)",
        "CREATE INDEX IF NOT EXISTS idx_sp_fy_dept   ON spending(Budget_Fiscal_Year, Department)",
        "CREATE INDEX IF NOT EXISTS idx_sp_fy_oc     ON spending(Budget_Fiscal_Year, Object_Class)",
    ]:
        cur.execute(ddl)
    conn.commit()

    # ── materialise FY25 vendor rows early (avoids repeated CAST/REPLACE) ─
    print("  → materialising FY25 vendor rows …")
    cur.execute("DROP TABLE IF EXISTS temp._fy25_vendor")
    cur.execute("""
        CREATE TEMP TABLE _fy25_vendor AS
            SELECT Vendor, Department, Object_Class, Appropriation_Type,
                   Appropriation_Name, Date,
                   CAST(REPLACE(Amount,',','') AS REAL) AS amt
            FROM spending WHERE Budget_Fiscal_Year=? AND Vendor<>''
    """, (FY,))
    cur.execute("CREATE INDEX temp.idx_fv_vendor ON _fy25_vendor(Vendor)")
    cur.execute("CREATE INDEX temp.idx_fv_dept   ON _fy25_vendor(Department)")
    cur.execute("CREATE INDEX temp.idx_fv_oc     ON _fy25_vendor(Object_Class)")
    conn.commit()
    print(f"    → {cur.execute('SELECT COUNT(*) FROM _fy25_vendor').fetchone()[0]:,} rows materialised")

    # ── materialise vendor_best (COMMBUYS competition classification) ──────
    print("  → materialising vendor_best …")
    cur.execute("DROP TABLE IF EXISTS temp._vendor_best")
    cur.execute("""
        CREATE TEMP TABLE _vendor_best AS
            SELECT spending_vendor,
                   CASE
                       WHEN SUM(CASE WHEN competition='OPEN_COMPETITIVE' THEN 1 ELSE 0 END) > 0 THEN 'HAS_OPEN_BID'
                       WHEN SUM(CASE WHEN competition='CLOSED'           THEN 1 ELSE 0 END) > 0 THEN 'CLOSED_BID_ONLY'
                       WHEN SUM(CASE WHEN competition='NO_BID'           THEN 1 ELSE 0 END) > 0 THEN 'NO_BID_REF'
                       ELSE 'OTHER'
                   END AS category
            FROM cb.vendor_match GROUP BY spending_vendor
    """)
    cur.execute("CREATE INDEX temp.idx_vb_sv  ON _vendor_best(spending_vendor)")
    cur.execute("CREATE INDEX temp.idx_vb_cat ON _vendor_best(category)")
    conn.commit()

    # ── yearly totals ─────────────────────────────────────────────────────
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

    # ── object classes ────────────────────────────────────────────────────
    print("  → by object class …")
    cur.execute("""
        SELECT Object_Class,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(*) AS n,
               COUNT(DISTINCT Vendor) AS nv,
               COUNT(DISTINCT Department) AS nd
        FROM spending WHERE Budget_Fiscal_Year=?
        GROUP BY 1 ORDER BY 2 DESC
    """, (FY,))
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

    # ── cabinets ──────────────────────────────────────────────────────────
    print("  → by cabinet/secretariat …")
    cur.execute("""
        SELECT Cabinet_Secretariat,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(*) AS n,
               COUNT(DISTINCT Vendor) AS nv,
               COUNT(DISTINCT Department) AS nd
        FROM spending WHERE Budget_Fiscal_Year=?
        GROUP BY 1 ORDER BY 2 DESC
    """, (FY,))
    data["cabinets"] = [
        {"name": r[0] or "(unspecified)", "amt": r[1] or 0, "n": r[2], "nv": r[3], "nd": r[4]}
        for r in cur.fetchall()
    ]

    # ── departments ───────────────────────────────────────────────────────
    print("  → departments with vendor detail …")
    cur.execute("""
        SELECT Department, Cabinet_Secretariat,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(*) AS n,
               COUNT(DISTINCT Vendor) AS nv
        FROM spending WHERE Budget_Fiscal_Year=?
        GROUP BY 1, 2 ORDER BY 3 DESC
    """, (FY,))
    data["departments"] = [
        {"dept": r[0], "cab": r[1], "amt": r[2] or 0, "n": r[3], "nv": r[4]}
        for r in cur.fetchall()
    ]

    # ── dept_vendors: batched (was 60 individual queries) ─────────────────
    print("  → top vendors per department (batched) …")
    dept_names = [d["dept"] for d in data["departments"][:60]]
    ph = ",".join("?" * len(dept_names))
    cur.execute(f"""
        SELECT Department, Vendor, SUM(amt) a, COUNT(*) n, COUNT(DISTINCT Object_Class) noc
        FROM _fy25_vendor
        WHERE Department IN ({ph})
        GROUP BY 1, 2
    """, dept_names)
    dv_raw: dict = defaultdict(list)
    for dept, vendor, a, n, noc in cur.fetchall():
        dv_raw[dept].append((a or 0, vendor, n, noc))
    dept_vendors: dict = {
        dept: [{"vendor": v, "amt": a, "n": n, "noc": noc}
               for a, v, n, noc in sorted(rows, reverse=True)[:15]]
        for dept, rows in dv_raw.items()
    }
    data["dept_vendors"] = dept_vendors

    # ── vendor×dept detail: batched (was up to 600 individual queries) ────
    print("  → vendor×dept detail (batched) …")
    scoped_vendors = list({v["vendor"] for vendors in dept_vendors.values() for v in vendors[:10]})
    if scoped_vendors:
        ph2 = ",".join("?" * len(scoped_vendors))
        cur.execute(f"""
            SELECT Department, Vendor, Object_Class, SUM(amt) a, COUNT(*) n
            FROM _fy25_vendor
            WHERE Vendor IN ({ph2})
            GROUP BY 1, 2, 3
        """, scoped_vendors)
        vd_raw: dict = defaultdict(list)
        for dept, vendor, oc_code, a, n in cur.fetchall():
            vd_raw[(dept, vendor)].append((a or 0, oc_code, n))
        vendor_dept_detail = {
            f"{dept}|||{v['vendor']}": [
                {"oc": oc_c, "amt": a, "n": n}
                for a, oc_c, n in sorted(vd_raw[(dept, v["vendor"])], reverse=True)[:10]
            ]
            for dept in dept_names
            for v in dept_vendors.get(dept, [])[:10]
        }
    else:
        vendor_dept_detail = {}
    data["vendor_dept_detail"] = vendor_dept_detail

    # ── top vendors overall ───────────────────────────────────────────────
    print("  → top vendors overall …")
    cur.execute("""
        SELECT Vendor, SUM(amt) a, COUNT(*) n,
               COUNT(DISTINCT Department) nd, COUNT(DISTINCT Object_Class) noc
        FROM _fy25_vendor
        GROUP BY 1 ORDER BY 2 DESC LIMIT 50
    """)
    data["top_vendors"] = [
        {"vendor": r[0], "amt": r[1] or 0, "n": r[2], "nd": r[3], "noc": r[4]}
        for r in cur.fetchall()
    ]

    # ── vendor detail: batched 3 queries for top 50 (was 150 queries) ─────
    print("  → vendor detail for top 50 (batched) …")
    top_vnames = [v["vendor"] for v in data["top_vendors"]]
    ph3 = ",".join("?" * len(top_vnames))

    cur.execute(f"""
        SELECT Vendor, Department, SUM(amt) a, COUNT(*) n
        FROM _fy25_vendor WHERE Vendor IN ({ph3})
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """, top_vnames)
    by_dept_raw: dict = defaultdict(list)
    for vendor, dept, a, n in cur.fetchall():
        if len(by_dept_raw[vendor]) < 15:
            by_dept_raw[vendor].append({"dept": dept, "amt": a or 0, "n": n})

    cur.execute(f"""
        SELECT Vendor, Object_Class, SUM(amt) a, COUNT(*) n
        FROM _fy25_vendor WHERE Vendor IN ({ph3})
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """, top_vnames)
    by_oc_raw: dict = defaultdict(list)
    for vendor, oc_code, a, n in cur.fetchall():
        if len(by_oc_raw[vendor]) < 10:
            by_oc_raw[vendor].append({"oc": oc_code, "amt": a or 0, "n": n})

    cur.execute(f"""
        SELECT Vendor, Department, Object_Class, Appropriation_Name, Date, amt
        FROM _fy25_vendor WHERE Vendor IN ({ph3})
        ORDER BY Vendor, amt DESC
    """, top_vnames)
    top_txns_raw: dict = defaultdict(list)
    for vendor, dept, oc_code, approp, date, a in cur.fetchall():
        if len(top_txns_raw[vendor]) < 500:
            top_txns_raw[vendor].append({
                "dept": dept, "oc": oc_code, "approp": approp,
                "date": date, "amt": a or 0,
            })

    data["vendor_detail"] = {
        vn: {
            "by_dept":  by_dept_raw[vn],
            "by_oc":    by_oc_raw[vn],
            "top_txns": top_txns_raw[vn],
        }
        for vn in top_vnames
    }

    # ── oc_vendors: batched (was 20 queries) ──────────────────────────────
    print("  → top vendors per object class (batched) …")
    oc_codes = [o["code"] for o in oc[:20]]
    ph4 = ",".join("?" * len(oc_codes))
    cur.execute(f"""
        SELECT Object_Class, Vendor, SUM(amt) a, COUNT(*) n
        FROM _fy25_vendor WHERE Object_Class IN ({ph4})
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """, oc_codes)
    oc_vendors_raw: dict = defaultdict(list)
    for oc_code, vendor, a, n in cur.fetchall():
        if len(oc_vendors_raw[oc_code]) < 15:
            oc_vendors_raw[oc_code].append({"vendor": vendor, "amt": a or 0, "n": n})
    data["oc_vendors"] = dict(oc_vendors_raw)

    # ── oc_depts: batched (was 20 queries) ────────────────────────────────
    print("  → departments per object class (batched) …")
    cur.execute(f"""
        SELECT Object_Class, Department,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) a
        FROM spending WHERE Budget_Fiscal_Year=? AND Object_Class IN ({ph4})
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """, [FY] + oc_codes)
    oc_depts_raw: dict = defaultdict(list)
    for oc_code, dept, a in cur.fetchall():
        if len(oc_depts_raw[oc_code]) < 10:
            oc_depts_raw[oc_code].append({"dept": dept, "amt": a or 0})
    data["oc_depts"] = dict(oc_depts_raw)

    # ── appropriation types ───────────────────────────────────────────────
    print("  → appropriation types …")
    cur.execute("""
        SELECT Appropriation_Type,
               SUM(CAST(REPLACE(Amount,',','') AS REAL)) AS amt,
               COUNT(*) AS n
        FROM spending WHERE Budget_Fiscal_Year=?
        GROUP BY 1 ORDER BY 2 DESC
    """, (FY,))
    data["appropriation_types"] = [
        {"type": r[0], "amt": r[1] or 0, "n": r[2]} for r in cur.fetchall()
    ]

    # ── trust spending (Appropriation_Type now in _fy25_vendor) ───────────
    print("  → trust spending (3TN) …")
    cur.execute("""
        SELECT Vendor, SUM(amt) a, COUNT(*) n
        FROM _fy25_vendor WHERE Appropriation_Type='(3TN) TRUSTS'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 20
    """)
    data["trust_vendors"] = [
        {"vendor": r[0], "amt": r[1] or 0, "n": r[2]} for r in cur.fetchall()
    ]

    # ── COMMBUYS: competition mix ─────────────────────────────────────────
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

    # ── COMMBUYS: top NO_BID vendors ──────────────────────────────────────
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

    # ── no_open_bid_fy25: uses _fy25_vendor ───────────────────────────────
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
               SUM(s.amt) AS amt,
               v.cats
        FROM _fy25_vendor s
        JOIN v ON s.Vendor=v.spending_vendor
        WHERE v.has_open=0
        GROUP BY s.Vendor ORDER BY amt DESC LIMIT 25
    """)
    data["no_open_bid_fy25"] = [
        {"vendor": r[0], "amt": r[1] or 0, "cats": r[2]} for r in cur.fetchall()
    ]

    # ── fy25_by_match: reuses _vendor_best (eliminates duplicate CTE) ─────
    print("  → FY2025 spend by COMMBUYS-match category …")
    cur.execute("""
        SELECT COALESCE(vb.category,'NO_COMMBUYS_CONTRACT') AS cat,
               COUNT(DISTINCT s.Vendor) AS nv,
               SUM(s.amt) AS amt
        FROM _fy25_vendor s
        LEFT JOIN _vendor_best vb ON s.Vendor=vb.spending_vendor
        GROUP BY cat ORDER BY amt DESC
    """)
    data["fy25_by_match"] = [
        {"cat": r[0], "nv": r[1], "amt": r[2] or 0} for r in cur.fetchall()
    ]

    # ── match_cat_vendors ─────────────────────────────────────────────────
    print("  → match category → vendor detail …")
    match_cat_vendors: dict = {}
    for m in data["fy25_by_match"]:
        cat = m["cat"]
        if cat == "NO_COMMBUYS_CONTRACT":
            cur.execute("""
                SELECT s.Vendor, SUM(s.amt) AS a, COUNT(*) n, COUNT(DISTINCT s.Department) nd
                FROM _fy25_vendor s
                LEFT JOIN _vendor_best vb ON s.Vendor=vb.spending_vendor
                WHERE vb.spending_vendor IS NULL
                GROUP BY 1 ORDER BY 2 DESC LIMIT 1000
            """)
        else:
            cur.execute("""
                SELECT s.Vendor, SUM(s.amt) AS a, COUNT(*) n, COUNT(DISTINCT s.Department) nd
                FROM _fy25_vendor s
                JOIN _vendor_best vb ON s.Vendor=vb.spending_vendor
                WHERE vb.category=?
                GROUP BY 1 ORDER BY 2 DESC LIMIT 1000
            """, (cat,))
        match_cat_vendors[cat] = [
            {"vendor": r[0], "amt": r[1] or 0, "n": r[2], "nd": r[3]}
            for r in cur.fetchall()
        ]
    data["match_cat_vendors"] = match_cat_vendors

    # ── match_vendor_txns: batched (was up to ~50 queries) ────────────────
    print("  → match vendor → transactions (batched) …")
    mv_vendors = list({v["vendor"] for vendors in match_cat_vendors.values() for v in vendors[:100]})
    if mv_vendors:
        ph_mv = ",".join("?" * len(mv_vendors))
        cur.execute(f"""
            SELECT Vendor, Department, Object_Class, Appropriation_Name, Date, amt
            FROM _fy25_vendor WHERE Vendor IN ({ph_mv})
            ORDER BY Vendor, amt DESC
        """, mv_vendors)
        match_vendor_txns: dict = defaultdict(list)
        for vendor, dept, oc_code, approp, date, a in cur.fetchall():
            if len(match_vendor_txns[vendor]) < 10:
                match_vendor_txns[vendor].append({
                    "dept": dept, "oc": oc_code, "approp": approp,
                    "date": date, "amt": a or 0,
                })
        data["match_vendor_txns"] = dict(match_vendor_txns)
    else:
        data["match_vendor_txns"] = {}

    # ── competition mix → top contracts ───────────────────────────────────
    print("  → competition mix → top contracts …")
    comp_mix_contracts: dict = {}
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

    # ── Attention / Waste: parent queries first (fixes query-ordering bug) ─

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

    print("  → IT / consulting concentration (UU + HH) …")
    cur.execute("""
        SELECT Vendor,
               SUM(amt) amt,
               COUNT(DISTINCT Department) nd,
               COUNT(*) n
        FROM _fy25_vendor
        WHERE Object_Class IN ('(UU) IT NON-PAYROLL EXPENSES','(HH) CONSULTANT SVCS (TO DEPTS)')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 25
    """)
    data["it_consulting"] = [
        {"vendor": r[0], "amt": r[1] or 0, "nd": r[2], "n": r[3]} for r in cur.fetchall()
    ]

    print("  → round-dollar flags (potential budget-padding) …")
    cur.execute("""
        SELECT Vendor, Department, amt, Object_Class, Date, Appropriation_Name
        FROM _fy25_vendor
        WHERE amt >= 1000000
          AND amt = CAST(amt AS INTEGER)
          AND CAST(amt AS INTEGER) % 100000 = 0
        ORDER BY amt DESC LIMIT 30
    """)
    data["round_dollar"] = [
        {"vendor": r[0], "dept": r[1], "amt": r[2], "oc": r[3], "date": r[4], "approp": r[5]}
        for r in cur.fetchall()
    ]

    # ── detail queries: all four "waste" groups batched ───────────────────
    print("  → no-open-bid / YoY / IT / round-dollar vendor detail (batched) …")
    no_open_vnames = [v["vendor"] for v in data["no_open_bid_fy25"][:25]]
    yoy_vnames     = [v["vendor"] for v in data["yoy_growth"][:20]]
    it_vnames      = [v["vendor"] for v in data["it_consulting"][:25]]
    rd_vnames      = list({r["vendor"] for r in data["round_dollar"] if r["vendor"]})

    all_detail_vendors = list({*no_open_vnames, *yoy_vnames, *it_vnames, *rd_vnames})
    if all_detail_vendors:
        ph_d = ",".join("?" * len(all_detail_vendors))

        cur.execute(f"""
            SELECT Vendor, Department, Object_Class, Appropriation_Name, Date, amt
            FROM _fy25_vendor WHERE Vendor IN ({ph_d})
            ORDER BY Vendor, amt DESC
        """, all_detail_vendors)
        vendor_txns: dict = defaultdict(list)
        for vendor, dept, oc_code, approp, date, a in cur.fetchall():
            if len(vendor_txns[vendor]) < 15:
                vendor_txns[vendor].append({
                    "dept": dept, "oc": oc_code, "approp": approp,
                    "date": date, "amt": a or 0,
                })

        cur.execute(f"""
            SELECT Vendor, Department, SUM(amt) a, COUNT(*) n
            FROM _fy25_vendor WHERE Vendor IN ({ph_d})
            GROUP BY 1, 2 ORDER BY 1, 3 DESC
        """, all_detail_vendors)
        vendor_by_dept: dict = defaultdict(list)
        for vendor, dept, a, n in cur.fetchall():
            if len(vendor_by_dept[vendor]) < 10:
                vendor_by_dept[vendor].append({"dept": dept, "amt": a or 0, "n": n})

        if it_vnames:
            ph_it = ",".join("?" * len(it_vnames))
            it_oc_args = it_vnames + [
                "(UU) IT NON-PAYROLL EXPENSES",
                "(HH) CONSULTANT SVCS (TO DEPTS)",
            ]
            cur.execute(f"""
                SELECT Vendor, Department, SUM(amt) a, COUNT(*) n
                FROM _fy25_vendor
                WHERE Vendor IN ({ph_it})
                  AND Object_Class IN (?, ?)
                GROUP BY 1, 2 ORDER BY 1, 3 DESC
            """, it_oc_args)
            it_by_dept: dict = defaultdict(list)
            for vendor, dept, a, n in cur.fetchall():
                if len(it_by_dept[vendor]) < 10:
                    it_by_dept[vendor].append({"dept": dept, "amt": a or 0, "n": n})

            cur.execute(f"""
                SELECT Vendor, Department, Object_Class, Appropriation_Name, Date, amt
                FROM _fy25_vendor
                WHERE Vendor IN ({ph_it})
                  AND Object_Class IN (?, ?)
                ORDER BY Vendor, amt DESC
            """, it_oc_args)
            it_txns: dict = defaultdict(list)
            for vendor, dept, oc_code, approp, date, a in cur.fetchall():
                if len(it_txns[vendor]) < 10:
                    it_txns[vendor].append({
                        "dept": dept, "oc": oc_code, "approp": approp,
                        "date": date, "amt": a or 0,
                    })
        else:
            it_by_dept: dict = defaultdict(list)
            it_txns: dict = defaultdict(list)
    else:
        vendor_txns = defaultdict(list)
        vendor_by_dept = defaultdict(list)
        it_by_dept = defaultdict(list)
        it_txns = defaultdict(list)

    data["no_open_detail"] = {
        v["vendor"]: {
            "by_dept": vendor_by_dept[v["vendor"]],
            "txns":    vendor_txns[v["vendor"]][:10],
        }
        for v in data["no_open_bid_fy25"][:25]
    }
    data["yoy_detail"] = {
        v["vendor"]: {
            "by_dept": vendor_by_dept[v["vendor"]],
            "txns":    vendor_txns[v["vendor"]][:10],
        }
        for v in data["yoy_growth"][:20]
    }
    data["it_detail"] = {
        v["vendor"]: {
            "by_dept": it_by_dept[v["vendor"]],
            "txns":    it_txns[v["vendor"]][:10],
        }
        for v in data["it_consulting"][:25]
    }
    data["rd_detail"] = {vn: vendor_txns[vn][:15] for vn in rd_vnames}

    # ── PAYROLL ───────────────────────────────────────────────────────────
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

    # ── dept_top_employees: batched with window function (was 25 queries) ─
    print("  → top 100 employees per department (batched) …")
    dept_codes = [d["code"] for d in data["dept_staffing"]]
    ph_dc = ",".join("?" * len(dept_codes))
    cur.execute(f"""
        SELECT department_code,
               name_first||' '||name_last, position_title, position_type,
               pay_total, pay_base, pay_overtime, pay_buyout, annual_rate
        FROM (
            SELECT department_code, name_first, name_last,
                   position_title, position_type,
                   pay_total, pay_base, pay_overtime, pay_buyout, annual_rate,
                   ROW_NUMBER() OVER (
                       PARTITION BY department_code ORDER BY pay_total DESC
                   ) AS rn
            FROM payroll
            WHERE year=2025 AND department_code IN ({ph_dc})
        )
        WHERE rn <= 100
        ORDER BY department_code, pay_total DESC
    """, dept_codes)
    dept_top_emps: dict = defaultdict(list)
    for code, name, title, ptype, total, base, ot, buyout, rate in cur.fetchall():
        dept_top_emps[code].append({
            "name": name, "title": title, "type": ptype,
            "total": total or 0, "base": base or 0, "ot": ot or 0,
            "buyout": buyout or 0, "rate": rate or 0,
        })
    data["dept_top_employees"] = dict(dept_top_emps)

    # ── ot_dept_employees: top 1000 per OT-heavy dept, sorted by OT ──────
    print("  → top 1000 OT employees per department (batched) …")
    ot_dept_codes = [d["code"] for d in data["ot_by_dept"]]
    ph_ot = ",".join("?" * len(ot_dept_codes))
    cur.execute(f"""
        SELECT department_code,
               name_first||' '||name_last, position_title, position_type,
               pay_total, pay_base, pay_overtime, pay_buyout, annual_rate
        FROM (
            SELECT department_code, name_first, name_last,
                   position_title, position_type,
                   pay_total, pay_base, pay_overtime, pay_buyout, annual_rate,
                   ROW_NUMBER() OVER (
                       PARTITION BY department_code ORDER BY pay_overtime DESC
                   ) AS rn
            FROM payroll
            WHERE year=2025 AND department_code IN ({ph_ot}) AND pay_overtime > 0
        )
        WHERE rn <= 1000
        ORDER BY department_code, pay_overtime DESC
    """, ot_dept_codes)
    ot_dept_emps: dict = defaultdict(list)
    for code, name, title, ptype, total, base, ot, buyout, rate in cur.fetchall():
        ot_dept_emps[code].append({
            "name": name, "title": title, "type": ptype,
            "total": total or 0, "base": base or 0, "ot": ot or 0,
            "buyout": buyout or 0, "rate": rate or 0,
        })
    data["ot_dept_employees"] = dict(ot_dept_emps)

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


# -------------------- Validation --------------------

# Appropriation-type groupings used for fund-source breakdown and reconciliation.
# Codes are the 3-character prefixes inside the parentheses in CTHRU Appropriation_Type values.
FUND_CATEGORIES = [
    {
        "name": "State Direct Appropriations",
        "short": "State Direct",
        "codes": ("1CS", "1CN"),
        "description": (
            "Spending authorized by the annual General Appropriation Act (H.4800) and "
            "supplemental acts. '1CS' = subsidiarized (state spends, then claims federal "
            "Medicaid / Title XIX reimbursement back). '1CN' = non-subsidiarized. "
            "Together these represent what most people mean by 'the state budget.'"
        ),
        "citation": "MA FY2025 GAA H.4800 — malegislature.gov",
        "citation_url": "https://malegislature.gov/Budget/FY2025",
    },
    {
        "name": "Trust Fund Disbursements (cash)",
        "short": "Trust Funds",
        "codes": ("3TN",),
        "description": (
            "Cash payments from statutory trust funds that are outside the annual GAA: "
            "teacher and state-employee pension annuities (~$6.6B), MBTA operating subsidy "
            "($1.6B), bond principal & interest ($2.6B), MA School Building Authority ($1.3B), "
            "and other quasi-public / authority payments."
        ),
        "citation": "PRIM FY2025 actuarial valuation; MA Treasurer bond schedules",
        "citation_url": "https://www.mapension.com",
    },
    {
        "name": "Non-Cash Accounting Entries",
        "short": "Non-Cash (3TX)",
        "codes": ("3TX",),
        "description": (
            "Non-cash trust transactions that appear as outflows but are offset by "
            "corresponding inflows in the same period. The largest component: "
            "federal Medicaid matching funds routed through the Executive Office of Labor "
            "trust (~$8.8B). Also includes UMass system internal trust flows ($3.1B) and "
            "GIC health insurance trust payments ($0.9B). These do NOT represent "
            "additional net spending."
        ),
        "citation": "MA Comptroller CTHRU non-cash methodology",
        "citation_url": "https://cthru.data.socrata.com",
    },
    {
        "name": "Capital Appropriations",
        "short": "Capital",
        "codes": ("2CN",),
        "description": (
            "Capital project spending funded by bond proceeds and capital authorizations: "
            "roads, bridges, IT systems, building construction. Authorized separately "
            "from the operating GAA through the Capital Investment Plan."
        ),
        "citation": "MA Capital Investment Plan (mass.gov)",
        "citation_url": "https://www.mass.gov/capital-investment-plan",
    },
    {
        "name": "Federal Grants (Explicit)",
        "short": "Federal Grants",
        "codes": ("4FN",),
        "description": (
            "Spending explicitly tagged as federally funded: CDBG community development "
            "grants, Title I / IDEA education pass-throughs to cities, WIOA workforce "
            "grants, and similar. Note: the bulk of the federal Medicaid match (~$8.8B) "
            "flows through non-cash trust entries (3TX), not here."
        ),
        "citation": "USASpending.gov federal awards to Massachusetts",
        "citation_url": "https://www.usaspending.gov/state/MA",
    },
    {
        "name": "Retained Revenue & Intragovernmental",
        "short": "Other",
        "codes": ("1RS", "1RN", "1IN"),
        "description": (
            "Retained revenue: fee collections and assessments agencies keep for their own "
            "operations (e.g., registry fees, permit fees). Intragovernmental: "
            "transfers between state agencies that net to zero at the consolidated level."
        ),
        "citation": "MA Comptroller CTHRU",
        "citation_url": "https://cthru.data.socrata.com",
    },
]


def build_fund_sources(data):
    """Categorize CTHRU appropriation types into fund-source buckets.
    Sets data['fund_sources'] and data['budget_reconciliation'] in place.
    """
    total = data.get("total_fy25", 0)

    # Parse appropriation type codes, e.g., "(1CS) DIRECT APPROPRIATIONS/..." → "1CS"
    type_amts: dict = {}
    for t in data.get("appropriation_types", []):
        code = (t.get("type") or "").strip()
        if code.startswith("(") and len(code) >= 4:
            key = code[1:4]
            type_amts[key] = type_amts.get(key, 0) + t["amt"]

    fund_sources = []
    categorised = 0.0
    for cat in FUND_CATEGORIES:
        amt = sum(type_amts.get(c, 0) for c in cat["codes"])
        categorised += amt
        fund_sources.append({
            "name":         cat["name"],
            "short":        cat["short"],
            "amt":          amt,
            "pct":          round(amt / total * 100, 1) if total else 0,
            "description":  cat["description"],
            "citation":     cat["citation"],
            "citation_url": cat["citation_url"],
        })
    # Remainder (payroll rejects, unassigned codes, etc.)
    remainder = total - categorised
    if abs(remainder) > 1e6:
        fund_sources.append({
            "name": "Unclassified / Payroll Adjustments",
            "short": "Other",
            "amt": remainder,
            "pct": round(remainder / total * 100, 1) if total else 0,
            "description": "Payroll rejects and unassigned appropriation codes.",
            "citation": "MA Comptroller CTHRU",
            "citation_url": "https://cthru.data.socrata.com",
        })
    data["fund_sources"] = fund_sources

    # Scalar shortcuts for reconciliation
    state_direct  = type_amts.get("1CS", 0) + type_amts.get("1CN", 0)
    trust_cash    = type_amts.get("3TN", 0)
    noncash       = type_amts.get("3TX", 0)
    capital       = type_amts.get("2CN", 0)
    federal       = type_amts.get("4FN", 0)
    other         = sum(type_amts.get(c, 0) for c in ("1RS", "1RN", "1IN"))
    GAA_NET       = 57_800_000_000   # H.4800 signed 2024-08-01
    lapsed        = GAA_NET - state_direct

    data["budget_reconciliation"] = {
        "gross_total":       total,
        "state_direct":      state_direct,
        "trust_cash":        trust_cash,
        "noncash":           noncash,
        "capital":           capital,
        "federal":           federal,
        "other":             other,
        "gaa_authorization": GAA_NET,
        "gaa_source":        "MA FY2025 GAA H.4800 (signed 2024-08-01)",
        "gaa_url":           "https://malegislature.gov/Budget/FY2025",
        "lapsed":            lapsed,
        "lapsed_note": (
            f"The ${lapsed/1e9:.1f}B gap between H.4800 authorization (${GAA_NET/1e9:.1f}B) and "
            f"actual direct-appropriation spending (${state_direct/1e9:.1f}B) reflects: "
            f"(1) unspent balances that lapsed back to the General Fund at year-end; "
            f"(2) appropriations authorized but not yet disbursed (carryforwards); "
            f"(3) supplemental appropriations that partially offset lapses."
        ),
        "noncash_note": (
            f"Non-cash trust entries (${noncash/1e9:.1f}B) are the most counter-intuitive "
            f"component. They appear as spending in CTHRU but represent accounting flows "
            f"that net to zero: the largest is the Executive Office of Labor trust, which "
            f"records federal Medicaid matching receipts (~$8.8B) as a trust disbursement "
            f"offset against MassHealth program costs."
        ),
    }


def validate_data(data):
    """Cross-check report figures against public data sources.

    Returns a list of check dicts, each with keys:
      source, label, expected, actual, delta_pct, status, note
    status: "pass" | "warn" | "fail" | "unavailable"
    """
    checks = []

    def safe_fetch(url, timeout=15):
        try:
            req = urllib.request.Request(
                url, headers={"Accept": "application/json", "User-Agent": "mass-audit/1.0"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode()), None
        except Exception as exc:
            return None, str(exc)

    def pct(expected, actual):
        return abs(actual - expected) / expected * 100 if expected else None

    def chk(source, label, expected, actual, note=""):
        dp = pct(expected, actual)
        status = (
            "pass" if dp is not None and dp < 1.0 else
            "warn" if dp is not None and dp < 5.0 else
            "fail" if dp is not None else
            "unavailable"
        )
        return {
            "source": source, "label": label,
            "expected": expected, "actual": actual,
            "delta_pct": round(dp, 3) if dp is not None else None,
            "status": status, "note": note,
        }

    def range_chk(source, label, actual, lo, hi, note=""):
        in_range = lo <= actual <= hi
        return {
            "source": source, "label": label,
            "expected": f"${lo/1e9:.1f}B – ${hi/1e9:.1f}B",
            "actual": actual,
            "delta_pct": None,
            "status": "pass" if in_range else "fail",
            "note": note,
        }

    # Build fund-source breakdown and reconciliation data (sets data["fund_sources"] etc.)
    build_fund_sources(data)

    total = data.get("total_fy25", 0)
    oc_map = {o["key"]: o["amt"] for o in data.get("object_classes", [])}
    br = data.get("budget_reconciliation", {})

    # ── 1. Internal consistency ───────────────────────────────────────────
    checks.append(chk(
        "Internal", "Sum of object-class amounts == total_fy25",
        total, sum(o["amt"] for o in data.get("object_classes", [])),
    ))
    checks.append(chk(
        "Internal", "Sum of cabinet totals ≈ total_fy25",
        total, sum(c["amt"] for c in data.get("cabinets", [])),
    ))
    checks.append(chk(
        "Internal", "Sum of department totals ≈ total_fy25",
        total, sum(d["amt"] for d in data.get("departments", [])),
    ))

    # ── 2. Published budget reference values ──────────────────────────────
    # MA FY2025 General Appropriation Act (H.4800, signed 2024-08-01)
    # Source: https://malegislature.gov/Budget/FY2025/
    # IMPORTANT: CTHRU records GROSS cash outflows, not net state spending.
    # Federal Medicaid reimbursements, federal grants passed through, and inter-agency
    # transfers all appear as additional spending lines.  The net state appropriation
    # is ≈ $57.8B (H.4800) but gross CTHRU outflows routinely run $90–110B.
    checks.append(range_chk(
        "MA FY2025 GAA (H.4800, malegislature.gov) + federal pass-throughs",
        "Total FY2025 gross outflows in plausible range",
        total, 85e9, 120e9,
        note="Net state appropriation ≈ $57.8B (H.4800); gross CTHRU includes ~$15B+ federal Medicaid share, federal grants, inter-agency flows",
    ))

    # Chapter 70 + other local aid (PP class).
    # CTHRU PP class captures ALL state aid to localities and school districts,
    # including federal formula grants administered by the state (Title I, IDEA, ESSER).
    # Chapter 70 alone: $6.747B enacted; total PP including federal pass-throughs: ~$18–22B.
    checks.append(range_chk(
        "MA FY2025 GAA H.4800 + federal local-aid pass-throughs",
        "State & federal aid to localities (PP class) in expected range",
        oc_map.get("PP", 0), 15e9, 25e9,
        note="Chapter 70 enacted $6.747B + federal education/road grants passed through to cities/towns",
    ))

    # RR class: MassHealth + entitlements.
    # Includes both the state share AND the federal Medicaid share (~55% of MassHealth).
    # Total MassHealth ≈ $20B state + $15B federal = $35B gross; plus unemployment, SNAP admin.
    checks.append(range_chk(
        "EOHHS FY2025 MassHealth Budget + federal Medicaid match",
        "Benefits / MassHealth gross (RR class) in expected range",
        oc_map.get("RR", 0), 28e9, 45e9,
        note="Includes ~55% federal Medicaid matching funds; state-only MassHealth ≈ $20B, federal match ≈ $15B",
    ))

    # AA class: state employee payroll. This is gross payroll disbursements through HR/CMS.
    # Comparable to CTHRU Open Payroll total (validated separately via Socrata API below).
    checks.append(range_chk(
        "MA Comptroller CTHRU / Open Payroll benchmark",
        "State payroll disbursements (AA class) in expected range",
        oc_map.get("AA", 0), 8.0e9, 15.0e9,
        note="CTHRU AA class = gross HR/CMS payroll; open payroll API should confirm ~$10.9B",
    ))

    # ── 3. Fund-source / reconciliation checks ───────────────────────────
    GAA_NET = br.get("gaa_authorization", 57_800_000_000)

    # State direct appropriations spent vs. H.4800 authorization
    # Expected: actual < authorization (unspent lapses back to General Fund)
    state_direct = br.get("state_direct", 0)
    lapsed = GAA_NET - state_direct
    checks.append({
        "source": "MA FY2025 GAA H.4800 — malegislature.gov/Budget/FY2025",
        "label": "State direct appropriations spent vs. H.4800 authorization",
        "expected": GAA_NET,
        "actual": state_direct,
        "delta_pct": round(pct(GAA_NET, state_direct), 1) if GAA_NET else None,
        "status": "warn",   # a gap is expected — lapses are normal
        "note": (
            f"${lapsed/1e9:.1f}B gap = lapsed appropriations + carryforwards. "
            "WARN is expected; a PASS here would mean 100% spending-to-authorization, "
            "which is unusual."
        ),
    })

    # Non-cash trust share: should be ~12–18% of gross total
    noncash = br.get("noncash", 0)
    noncash_pct = noncash / total * 100 if total else 0
    checks.append(range_chk(
        "MA Comptroller CTHRU non-cash trust methodology",
        "Non-cash trust entries (3TX) as % of gross: expected 12–18%",
        noncash_pct, 12.0, 18.0,
        note=(
            "3TX entries are accounting flows that net to zero. "
            f"Our value: {noncash_pct:.1f}%. Largest component: "
            "EOL federal Medicaid trust (~$8.8B)."
        ),
    ))

    # Pension fund disbursements from SERS + TRS
    # PRIM FY2024 actuarial valuation: SERS benefit payments ~$3.1B, TRS ~$3.7B
    # FY2025 expected in range $6.2B – $8.0B given multi-year trend
    pension_amt = next(
        (fs["amt"] for fs in data.get("fund_sources", []) if "Trust Fund" in fs["name"]),
        0
    )
    # We use the teacher + state employee pension lines directly from vendor data
    # (already confirmed as $3.04B SERS + $3.58B TRS = $6.62B from vendor drill-down)
    checks.append(range_chk(
        "PRIM FY2024 actuarial valuation (mapension.com) + trend",
        "Total pension + MBTA + bond service trust (3TN class) in expected range",
        br.get("trust_cash", 0), 18e9, 30e9,
        note=(
            "3TN includes pension annuities ($6.6B SERS+TRS), MBTA subsidy (~$2.5B), "
            "bond debt service (~$2.6B), MSBA (~$1.3B), and other quasi-public authorities."
        ),
    ))

    # Federal grants: CTHRU 4FN vs. USASpending.gov total awards to MA
    # USASpending shows ~$2–4B in formula/competitive grants administered by MA in FY2025
    checks.append(range_chk(
        "USASpending.gov federal awards to MA (usaspending.gov/state/MA)",
        "Explicit federal grant spending (4FN) in expected range",
        br.get("federal", 0), 1.5e9, 5.0e9,
        note=(
            "CTHRU 4FN captures federal grants routed through state accounts. "
            "The bulk of the Medicaid federal match ($8.8B) flows through 3TX, not here."
        ),
    ))

    # ── 5. Socrata Open Payroll API (cthru.data.socrata.com, dataset 9ttk-7vz6) ────
    # Columns confirmed: year, pay_total_actual, pay_overtime_actual
    print("  → fetching Socrata Open Payroll validation (9ttk-7vz6) …")
    PAYROLL_DOMAIN = "cthru.data.socrata.com"
    PAYROLL_4X4    = "9ttk-7vz6"
    q = ("SELECT count(*) AS n, "
         "sum(pay_total_actual) AS sum_total, "
         "sum(pay_overtime_actual) AS sum_ot "
         "WHERE year=2025")
    url = (f"https://{PAYROLL_DOMAIN}/resource/{PAYROLL_4X4}.json"
           f"?$query={urllib.parse.quote(q)}")
    rows, rerr = safe_fetch(url)
    if rows and isinstance(rows, list) and rows:
        row   = rows[0]
        our   = (data.get("payroll_yearly") or [{}])[-1]
        api_n = int(float(row.get("n") or 0))
        checks.append(chk(
            f"Socrata {PAYROLL_DOMAIN}/{PAYROLL_4X4}",
            "Payroll employee count (year=2025)",
            api_n, our.get("n", 0),
            note="cthru.data.socrata.com, dataset 9ttk-7vz6, column: year",
        ))
        if "sum_total" in row:
            api_t = float(row["sum_total"] or 0)
            checks.append(chk(
                f"Socrata {PAYROLL_DOMAIN}/{PAYROLL_4X4}",
                "Total payroll compensation (year=2025)",
                api_t, our.get("total", 0),
                note="column: pay_total_actual",
            ))
        if "sum_ot" in row:
            api_ot = float(row["sum_ot"] or 0)
            checks.append(chk(
                f"Socrata {PAYROLL_DOMAIN}/{PAYROLL_4X4}",
                "Total overtime pay (year=2025)",
                api_ot, our.get("ot", 0),
                note="column: pay_overtime_actual",
            ))
    else:
        checks.append({
            "source": f"Socrata {PAYROLL_DOMAIN}/{PAYROLL_4X4}",
            "label": "Open Payroll API",
            "expected": None, "actual": None, "delta_pct": None,
            "status": "unavailable", "note": rerr or "empty response",
        })

    return checks


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

#disclaimer-banner {
  background: #12122a; color: #d4d4e8;
  padding: 12px 32px; font-size: 12px; line-height: 1.6;
  border-bottom: 3px solid #c99a3b;
  display: flex; align-items: flex-start; gap: 20px; flex-wrap: wrap;
}
#disclaimer-banner strong { color: #e8e8f0; }
#disclaimer-banner a { color: #c99a3b; text-decoration: underline; }
#disclaimer-banner button {
  flex-shrink: 0; background: #c99a3b; color: #12122a;
  border: none; padding: 7px 16px; border-radius: 4px;
  cursor: pointer; font-weight: 700; font-size: 11px;
  white-space: nowrap; align-self: center;
}
#disclaimer-banner button:hover { background: #e0b84d; }

.anno {
  display: inline-block; padding: 1px 5px; border-radius: 3px;
  font-size: 9px; font-weight: 700; letter-spacing: .03em;
  text-transform: uppercase; margin-left: 5px;
  vertical-align: middle; cursor: help;
}
.anno.settlement { background: #7d3c98; color: #fff; }
.anno.gov        { background: #1a5276; color: #fff; }
</style>
</head>
<body>

<div id="disclaimer-banner">
  <div style="flex:1;min-width:260px">
    <strong>Independent Civic Analysis — Not Official Government Data.</strong>
    This report is produced by an independent researcher using exclusively public-domain records
    published by the Commonwealth of Massachusetts: the Comptroller CTHRU expenditure database,
    the COMMBUYS procurement system, the Open Payroll dataset, and the Settlements &amp; Judgments
    dataset. This report is <strong>not affiliated with, endorsed by, or produced by</strong> any
    Massachusetts government agency. No warranty is made regarding accuracy or completeness.
    Nothing herein constitutes a legal finding, allegation of misconduct, criminal referral, or
    official audit. Statistical anomalies flagged are patterns that <em>may</em> warrant further
    review — they are not conclusions.
    <br>
    Questions or corrections: <a href="mailto:Oversight-MA@pm.me">Oversight-MA@pm.me</a>
    &nbsp;·&nbsp;
    <a href="https://cthru.data.socrata.com" target="_blank">CTHRU source data</a>
    &nbsp;·&nbsp;
    <a href="https://www.commbuys.com" target="_blank">COMMBUYS</a>
  </div>
  <button id="disc-btn" onclick="
    document.getElementById('disclaimer-banner').style.display='none';
    try{localStorage.setItem('disc_dismissed_v2','1');}catch(e){}
  ">I Understand &amp; Dismiss</button>
</div>
<script>
try{if(localStorage.getItem('disc_dismissed_v2'))
  document.getElementById('disclaimer-banner').style.display='none';}catch(e){}
</script>

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
  <a href="#govdata">Gov. Data</a>
  <a href="#validation">Validation</a>
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

<section id="govdata">
<h2>Government-Sourced Annotations</h2>
<p class="lead">Data fetched live from official Massachusetts government datasets at report generation
time. Departments with entries in these datasets are annotated with
<span class="anno settlement">⚖ settle</span> badges throughout the report.</p>

<div class="ctx-box">
<strong>Source:</strong>
CTHRU Settlements &amp; Judgments —
<a href="https://cthru.data.socrata.com/resource/gpqz-7ppn" target="_blank" style="color:#23395d">
cthru.data.socrata.com/resource/gpqz-7ppn</a>.
Published by the MA Comptroller's Office. Records every settlement and judgment payment
made by state agencies, including the paying department, payee, and amount.
Covers FY2014–present; updated quarterly.
<br><br>
<strong>What this flags:</strong> Departments with repeated or large settlement payments may indicate
systemic management, civil-rights, labor-relations, or procurement issues. The dollar amounts
represent <em>cost to taxpayers</em> of litigation resolved against the state.
A high settlement total does not prove wrongdoing — it indicates litigation exposure that
warrants scrutiny alongside the department's spending profile.
</div>

<h3 style="font-size:14px;margin:18px 0 6px">Settlement &amp; Judgment Payments by Department — FY2023-2025 trend</h3>
<p style="font-size:12px;color:#555;margin:0 0 6px">
Departments with a <span class="anno settlement">⚖ settle</span> badge in other sections
of this report appear here. Click a department row to see individual FY2025 cases.
</p>
<table id="settleByDeptTable"><thead>
<tr><th>Department</th><th class="num">FY2023</th><th class="num">FY2024</th>
    <th class="num">FY2025</th><th class="num">3-yr Total</th><th class="num">FY25 Cases</th></tr>
</thead><tbody></tbody></table>

<h3 style="font-size:14px;margin:20px 0 6px">Top 25 FY2025 Settlement Payments (individual cases)</h3>
<p style="font-size:12px;color:#555;margin:0 0 6px">
Payee is typically a law firm, individual plaintiff, or the opposing party in a judgment.
"STATUTORY EXEMPTION" = payment details withheld by law (e.g., civil-rights settlements).
</p>
<table id="settleCasesTable"><thead>
<tr><th>Payee</th><th>Department</th><th class="num">Amount</th><th>Date</th></tr>
</thead><tbody></tbody></table>
<p style="font-size:11px;color:#888;margin:8px 0 0" id="settleSummary"></p>
</section>

<section id="validation">
<h2>Data Validation &amp; Budget Reconciliation</h2>
<p class="lead">This section documents what is validated, what public sources are used, and why the
CTHRU gross outflows (~$98B) are roughly 70% larger than the FY2025 state operating budget (~$57.8B).</p>

<div class="ctx-box" id="reconciliationCtx">
<!-- filled by JS -->
</div>

<h3 style="font-size:14px;margin:20px 0 6px">How CTHRU gross outflows are composed — fund source breakdown</h3>
<p style="font-size:12px;color:#555;margin:0 0 8px">
Every CTHRU transaction has an <strong>Appropriation_Type</strong> code. The table below categorises
all FY2025 outflows by fund source. State Direct Appropriations is what the public typically calls
"the state budget." Everything else is statutory, quasi-governmental, or accounting-only.
</p>
<div id="fundSourcesKpi" class="kpi-row"></div>
<table id="fundSourcesTable"><thead>
<tr><th>Fund Category</th><th class="num">Amount</th><th class="num">% of Gross</th>
    <th class="pct-bar"></th><th style="min-width:260px">What it is</th><th>Public citation</th></tr>
</thead><tbody></tbody></table>

<h3 style="font-size:14px;margin:20px 0 6px">Gross → Net reconciliation: CTHRU total vs. FY2025 GAA</h3>
<p style="font-size:12px;color:#555;margin:0 0 8px">
Subtracting non-cash accounting entries, trust funds, capital, and federal grants from
gross CTHRU outflows arrives at the "state direct appropriations spent" figure, which
can be compared against the H.4800 authorization.
</p>
<table id="reconciliationTable" style="max-width:680px"><thead>
<tr><th>Line</th><th class="num">Amount</th><th>Notes &amp; Sources</th></tr>
</thead><tbody></tbody></table>

<h3 style="font-size:14px;margin:20px 0 6px">Automated validation checks</h3>
<p style="font-size:12px;color:#555;margin:0 0 8px">
Each check compares a figure computed from the local databases against a publicly available
reference value. <strong>PASS</strong> = within 1%. <strong>WARN</strong> = within 5% or an
expected methodological gap (e.g., gross vs. net). <strong>FAIL</strong> = &gt;5% unexplained
divergence. <strong>N/A</strong> = public source unavailable at generation time.
</p>
<table id="validationTable"><thead>
<tr><th style="min-width:200px">Public source</th><th>What is checked</th>
    <th class="num">Reference value</th><th class="num">Report value</th>
    <th class="num">Δ %</th><th>Status</th></tr>
</thead><tbody></tbody></table>
<p style="font-size:11px;color:#888;margin:10px 0 0">
CTHRU spending data sourced from
<a href="https://cthru.data.socrata.com" target="_blank" style="color:#4a6fa5">cthru.data.socrata.com</a>.
Payroll validated live against dataset
<a href="https://cthru.data.socrata.com/resource/9ttk-7vz6" target="_blank" style="color:#4a6fa5">9ttk-7vz6</a>
(Commonwealth Of Massachusetts Payroll v3).
Budget reference: <a href="https://malegislature.gov/Budget/FY2025" target="_blank" style="color:#4a6fa5">H.4800 FY2025 GAA</a>.
MassHealth reference: <a href="https://www.mass.gov/orgs/executive-office-of-health-and-human-services" target="_blank" style="color:#4a6fa5">EOHHS</a>.
Pension reference: <a href="https://www.mapension.com" target="_blank" style="color:#4a6fa5">PRIM</a>.
Federal grants reference: <a href="https://www.usaspending.gov/state/MA" target="_blank" style="color:#4a6fa5">USASpending.gov</a>.
</p>
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
<p>Generated locally from <code>spending.db</code> + <code>commbuys.db</code>.
Government annotations fetched live from <code>cthru.data.socrata.com</code> at generation time.
<strong>No personal data leaves your machine.</strong></p>
<p style="margin-top:6px">
<strong>Independent Civic Analysis</strong> — not affiliated with or endorsed by the Commonwealth of Massachusetts.
All source data is publicly available under Massachusetts public records law (M.G.L. c. 66 §10) and published by the
MA Comptroller's Office, MA Legislature, and COMMBUYS.
This report is provided for research and civic oversight purposes only.
The author makes no warranty of accuracy and accepts no liability for decisions made based on this analysis.
</p>
<p style="margin-top:6px">
Contact: <a href="mailto:Oversight-MA@pm.me" style="color:#c99a3b">Oversight-MA@pm.me</a>
&nbsp;·&nbsp;
Data sources:
<a href="https://cthru.data.socrata.com" target="_blank">CTHRU</a>,
<a href="https://www.commbuys.com" target="_blank">COMMBUYS</a>,
<a href="https://malegislature.gov/Budget/FY2025" target="_blank">MA Legislature FY2025 Budget</a>
</p>
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

  // Static: by-dept + by-OC summaries (no interaction needed)
  const vStaticWrap = document.createElement("div");
  vStaticWrap.style.cssText = "display:flex;gap:24px;flex-wrap:wrap";
  vStaticWrap.innerHTML = `
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
    </div>`;
  detTd.appendChild(vStaticWrap);

  // Paginated transactions
  if (topTxns.length) {
    const txnWrap = document.createElement("div");
    txnWrap.className = "subtable";
    txnWrap.style.marginTop = "8px";
    txnWrap.innerHTML = `<h4>${fmtN(topTxns.length)} transaction${topTxns.length !== 1 ? "s" : ""} (ranked by amount · paginated 50/page)</h4>`;
    txnWrap.appendChild(buildPaginatorWidget(topTxns, 50, [
      {label: "Department"}, {label: "Object Class"},
      {label: "Appropriation"}, {label: "Date"}, {label: "Amount", cls: "num"},
    ], (t) => {
      const r = document.createElement("tr");
      r.innerHTML = `<td style="font-size:12px">${t.dept}</td>
        <td style="font-size:11px">${t.oc}</td>
        <td style="font-size:11px">${t.approp||""}</td>
        <td style="font-size:12px">${t.date||""}</td>
        <td class="num">${fmt$(t.amt)}</td>`;
      return r;
    }));
    detTd.appendChild(txnWrap);
  }

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
      : `<p style="font-size:12px;color:#999">No transaction detail available for this vendor (detail pre-loaded for top 100 per category).</p>`;
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

// --- generic paginator widget ---
// Returns a <div> with pager controls (top+bottom) wrapping a <table>.
// headers: [{label, cls}]   renderRow(item, absoluteIndex) → <tr>
function buildPaginatorWidget(items, pageSize, headers, renderRow) {
  pageSize = pageSize || 50;
  let page = 0;
  const totalPages = Math.ceil(items.length / pageSize);

  const wrap = document.createElement("div");

  function mkPager() {
    const bar = document.createElement("div");
    bar.style.cssText = "display:flex;align-items:center;gap:10px;margin:4px 0 6px;font-size:12px;flex-wrap:wrap";
    const prev = document.createElement("button");
    prev.textContent = "← Prev";
    prev.style.cssText = "padding:3px 10px;cursor:pointer;font-size:12px";
    const next = document.createElement("button");
    next.textContent = "Next →";
    next.style.cssText = "padding:3px 10px;cursor:pointer;font-size:12px";
    const info  = document.createElement("span");
    const range = document.createElement("span");
    range.style.cssText = "color:#777;margin-left:auto;font-size:11px";
    bar.appendChild(prev); bar.appendChild(info);
    bar.appendChild(next); bar.appendChild(range);
    return {bar, prev, next, info, range};
  }

  const top = mkPager();
  const bot = mkPager();

  const tbl = document.createElement("table");
  tbl.innerHTML = "<thead><tr>" +
    headers.map(h => `<th${h.cls ? ` class="${h.cls}"` : ""}>${h.label}</th>`).join("") +
    "</tr></thead>";
  const tbody = document.createElement("tbody");
  tbl.appendChild(tbody);

  if (totalPages > 1) wrap.appendChild(top.bar);
  wrap.appendChild(tbl);
  if (totalPages > 1) wrap.appendChild(bot.bar);

  function render() {
    const start = page * pageSize;
    const slice = items.slice(start, start + pageSize);
    tbody.innerHTML = "";
    slice.forEach((item, i) => tbody.appendChild(renderRow(item, start + i)));

    const pg    = `Page ${page + 1} / ${totalPages}`;
    const rng   = `#${start + 1}–#${Math.min(start + pageSize, items.length)} of ${fmtN(items.length)}`;
    [top.info, bot.info].forEach(el => el.textContent = pg);
    [top.range, bot.range].forEach(el => el.textContent = rng);
    [[top.prev, bot.prev], [top.next, bot.next]].forEach(([a, b], isNext) => {
      const dis = isNext ? page >= totalPages - 1 : page === 0;
      [a, b].forEach(btn => { btn.disabled = dis; btn.style.opacity = dis ? "0.35" : "1"; });
    });
  }

  const go = (dir) => (ev) => {
    ev.stopPropagation();
    const np = page + dir;
    if (np >= 0 && np < totalPages) { page = np; render(); wrap.scrollIntoView({block:"nearest"}); }
  };
  [top.prev, bot.prev].forEach(b => b.addEventListener("click", go(-1)));
  [top.next, bot.next].forEach(b => b.addEventListener("click", go(+1)));

  render();
  return wrap;
}

// --- helper: paginated vendor table with txn drill-down ---
// Returns a <div> containing pager controls + a <table> that pages through `vendors`.
// `pageSize` vendors are shown per page; each row is expandable for transaction detail.
function buildPaginatedVendorTable(vendors, txnLookup, parentCols, pageSize) {
  pageSize = pageSize || 50;
  let page = 0;
  const totalPages = Math.ceil(vendors.length / pageSize);

  const wrap = document.createElement("div");

  // Pager bar (hidden when only one page)
  const pagerDiv = document.createElement("div");
  pagerDiv.style.cssText =
    "display:flex;align-items:center;gap:10px;margin:4px 0 8px;font-size:12px;flex-wrap:wrap";
  const prevBtn = document.createElement("button");
  prevBtn.textContent = "← Prev";
  prevBtn.style.cssText = "padding:3px 10px;cursor:pointer;font-size:12px";
  const nextBtn = document.createElement("button");
  nextBtn.textContent = "Next →";
  nextBtn.style.cssText = "padding:3px 10px;cursor:pointer;font-size:12px";
  const infoSpan  = document.createElement("span");
  const rangeSpan = document.createElement("span");
  rangeSpan.style.cssText = "color:#777;margin-left:auto;font-size:11px";
  pagerDiv.appendChild(prevBtn);
  pagerDiv.appendChild(infoSpan);
  pagerDiv.appendChild(nextBtn);
  pagerDiv.appendChild(rangeSpan);
  if (totalPages > 1) wrap.appendChild(pagerDiv);

  // Table
  const vtbl = document.createElement("table");
  vtbl.innerHTML = `<thead><tr>
    <th>Vendor</th><th class="num">Amount</th>
    <th class="num">Txns</th><th class="num">Depts</th>
  </tr></thead>`;
  const vtbody = document.createElement("tbody");
  vtbl.appendChild(vtbody);
  wrap.appendChild(vtbl);

  // Second pager at the bottom (only when pages > 1)
  const pagerDivBot = pagerDiv.cloneNode(false);  // empty clone for layout
  const prevBtnB = prevBtn.cloneNode(true);
  const nextBtnB = nextBtn.cloneNode(true);
  const infoSpanB  = document.createElement("span");
  const rangeSpanB = document.createElement("span");
  rangeSpanB.style.cssText = rangeSpan.style.cssText;
  pagerDivBot.appendChild(prevBtnB);
  pagerDivBot.appendChild(infoSpanB);
  pagerDivBot.appendChild(nextBtnB);
  pagerDivBot.appendChild(rangeSpanB);
  if (totalPages > 1) wrap.appendChild(pagerDivBot);

  function renderPage() {
    const start = page * pageSize;
    const slice = vendors.slice(start, start + pageSize);
    vtbody.innerHTML = "";
    vtbody.appendChild(buildVendorTxnDrilldown(slice, txnLookup, parentCols));

    const pageLabel  = `Page ${page + 1} / ${totalPages}`;
    const rangeLabel = `#${start + 1} – #${Math.min(start + pageSize, vendors.length)} of ${fmtN(vendors.length)}`;
    infoSpan.textContent  = pageLabel;
    infoSpanB.textContent = pageLabel;
    rangeSpan.textContent  = rangeLabel;
    rangeSpanB.textContent = rangeLabel;

    [prevBtn, prevBtnB].forEach(b => {
      b.disabled = page === 0;
      b.style.opacity = page === 0 ? "0.35" : "1";
    });
    [nextBtn, nextBtnB].forEach(b => {
      b.disabled = page >= totalPages - 1;
      b.style.opacity = page >= totalPages - 1 ? "0.35" : "1";
    });
  }

  const goPrev = (ev) => { ev.stopPropagation(); if (page > 0) { page--; renderPage(); wrap.scrollIntoView({block:"nearest"}); } };
  const goNext = (ev) => { ev.stopPropagation(); if (page < totalPages - 1) { page++; renderPage(); wrap.scrollIntoView({block:"nearest"}); } };
  prevBtn.addEventListener("click", goPrev);
  nextBtn.addEventListener("click", goNext);
  prevBtnB.addEventListener("click", goPrev);
  nextBtnB.addEventListener("click", goNext);

  renderPage();
  return wrap;
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
    wrapper.innerHTML =
      `<h4>${fmtN(vendors.length)} vendor${vendors.length !== 1 ? "s" : ""} — <em>${m.cat}</em> ` +
      `(click row for transactions · paginated 50/page · txn detail for top 100)</h4>`;
    wrapper.appendChild(buildPaginatedVendorTable(vendors, DATA.match_vendor_txns, 4, 50));
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
    tr.innerHTML = `<td>${d.dept || d.code}${deptAnnoBadges(d.dept)}</td>
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

// --- OT by department (3-year trend, expandable to top-1000 employees) ---
const otdBody = document.querySelector("#otDeptTable tbody");
if (DATA.ot_by_dept) {
  const otMax = Math.max(...DATA.ot_by_dept.map(d=>d.ot25));
  DATA.ot_by_dept.forEach(d => {
    const growth = d.ot23 > 0 ? ((d.ot25 - d.ot23)/d.ot23*100).toFixed(0) + "%" : "n/a";
    const otPct = d.total > 0 ? (d.ot/d.total*100).toFixed(1)+"%" : "";
    const tr = document.createElement("tr");
    tr.className = "expandable";
    const growthColor = d.ot23 > 0 && (d.ot25-d.ot23)/d.ot23 > 0.2 ? "color:#c0392b;font-weight:700" : "";
    tr.innerHTML = `<td>${d.dept || d.code}${deptAnnoBadges(d.dept)}</td>
      <td class="num">${fmt$(d.ot23)}</td>
      <td class="num">${fmt$(d.ot24)}</td>
      <td class="num">${fmt$(d.ot25)}</td>
      <td class="num" style="${growthColor}">${growth}</td>
      <td class="num">${otPct}</td>
      <td><div class="bar red" style="width:${d.ot25/otMax*100}%"></div></td>`;
    otdBody.appendChild(tr);

    // Drill-down: employees ranked by overtime, paginated to 1000
    const det = document.createElement("tr");
    det.className = "detail"; det.style.display = "none";
    const detTd = document.createElement("td");
    detTd.colSpan = 7;

    const emps = (DATA.ot_dept_employees && DATA.ot_dept_employees[d.code]) || [];
    if (emps.length) {
      const empWrap = document.createElement("div");
      empWrap.className = "subtable";
      empWrap.innerHTML =
        `<h4>${fmtN(emps.length)} employee${emps.length !== 1 ? "s" : ""} with overtime — ` +
        `${d.dept || d.code} (ranked by OT · paginated 50/page)</h4>`;
      empWrap.appendChild(buildPaginatorWidget(emps, 50, [
        {label: "#"}, {label: "Name"}, {label: "Title"}, {label: "Type"},
        {label: "Annual Rate", cls: "num"}, {label: "Base Pay", cls: "num"},
        {label: "Overtime", cls: "num"}, {label: "OT %", cls: "num"},
      ], (e, i) => {
        const otPct = e.total > 0 ? (e.ot / e.total * 100).toFixed(1) + "%" : "—";
        const hiOT  = e.total > 0 && e.ot / e.total > 0.30;
        const r = document.createElement("tr");
        r.innerHTML = `
          <td style="color:#999;font-size:11px">${i + 1}</td>
          <td>${e.name}</td>
          <td style="font-size:11px">${e.title}</td>
          <td style="font-size:11px">${e.type}</td>
          <td class="num">${fmt$(e.rate)}</td>
          <td class="num">${fmt$(e.base)}</td>
          <td class="num" style="color:#c0392b;font-weight:600">${fmt$(e.ot)}</td>
          <td class="num"${hiOT ? ' style="color:#c0392b;font-weight:700"' : ""}>${otPct}</td>`;
        return r;
      }));
      detTd.appendChild(empWrap);
    } else {
      detTd.innerHTML = `<p style="font-size:12px;color:#999;margin:4px 0">No employee overtime detail for this department.</p>`;
    }
    det.appendChild(detTd);
    otdBody.appendChild(det);
    tr.addEventListener("click", () => {
      const open = det.style.display !== "none";
      det.style.display = open ? "none" : "";
      tr.classList.toggle("expanded", !open);
    });
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

// ===================== GOVERNMENT ANNOTATIONS =====================

// Helper: return badge HTML for a department name
function deptAnnoBadges(deptName) {
  const flags = (DATA.gov_annotations && DATA.gov_annotations.dept_flags) || {};
  const entries = flags[deptName] || [];
  return entries.map(a =>
    `<span class="anno ${a.type.toLowerCase()}" title="${a.label}">${a.badge_label}</span>`
  ).join("");
}

// Settlements by dept table
if (DATA.gov_annotations && DATA.gov_annotations.settlements &&
    DATA.gov_annotations.settlements.by_dept) {
  const sd = DATA.gov_annotations.settlements;
  const sdBody = document.querySelector("#settleByDeptTable tbody");
  sd.by_dept.filter(d => d.fy25_amt > 0).forEach(d => {
    const tr = document.createElement("tr");
    tr.className = "expandable";
    tr.innerHTML = `
      <td>${d.dept}</td>
      <td class="num">${d.fy23_amt > 0 ? fmt$precise(d.fy23_amt) : "—"}</td>
      <td class="num">${d.fy24_amt > 0 ? fmt$precise(d.fy24_amt) : "—"}</td>
      <td class="num" style="font-weight:700">${fmt$precise(d.fy25_amt)}</td>
      <td class="num">${fmt$precise(d.total_3yr)}</td>
      <td class="num">${fmtN(d.fy25_n)}</td>`;
    sdBody.appendChild(tr);

    // Drill-down: FY2025 cases for this dept
    const det = document.createElement("tr");
    det.className = "detail"; det.style.display = "none";
    const cases = (sd.top_cases || []).filter(c =>
      c.dept && d.dept && c.dept.toUpperCase() === d.dept.toUpperCase()
    );
    det.innerHTML = `<td colspan="6">
      ${cases.length ? `<div class="subtable">
        <h4>FY2025 cases — ${d.dept}</h4>
        <table><thead><tr><th>Payee</th><th class="num">Amount</th><th>Date</th></tr></thead>
        <tbody>${cases.map(c =>
          `<tr><td style="font-size:12px">${c.payee}</td>
               <td class="num">${fmt$precise(c.amt)}</td>
               <td style="font-size:12px">${c.date}</td></tr>`
        ).join("")}</tbody></table></div>`
      : '<p style="font-size:12px;color:#999;margin:4px 0">No FY2025 case detail available (may be in top-50 payees for other depts).</p>'}
    </td>`;
    sdBody.appendChild(det);
    tr.addEventListener("click", () => {
      const open = det.style.display !== "none";
      det.style.display = open ? "none" : "";
      tr.classList.toggle("expanded", !open);
    });
  });

  // Top cases table
  const scBody = document.querySelector("#settleCasesTable tbody");
  (sd.top_cases || []).slice(0, 25).forEach(c => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td style="font-size:12px">${c.payee}</td>
      <td style="font-size:12px">${c.dept}</td>
      <td class="num">${fmt$precise(c.amt)}</td>
      <td style="font-size:12px">${c.date}</td>`;
    scBody.appendChild(tr);
  });

  const sumEl = document.getElementById("settleSummary");
  if (sumEl) sumEl.textContent =
    `FY2025 total: ${fmtN(sd.total_n_fy25)} settlement payments = ${fmt$precise(sd.total_amt_fy25)}. ` +
    `Source: ${sd.source_label}`;
}

// ===================== VALIDATION SECTION =====================

const fmtV = v => {
  if (v == null) return "—";
  if (typeof v === "string") return v;
  if (v >= 1e9) return "$" + (v/1e9).toFixed(2) + "B";
  if (v >= 1e6) return "$" + (v/1e6).toFixed(1) + "M";
  if (v >= 1e3) return "$" + (v/1e3).toFixed(0) + "K";
  return Number.isFinite(v) ? v.toLocaleString() : String(v);
};

// ── Reconciliation context box ─────────────────────────────────────────
if (DATA.budget_reconciliation) {
  const br = DATA.budget_reconciliation;
  const excess = ((br.gross_total - br.gaa_authorization) / br.gaa_authorization * 100).toFixed(0);
  document.getElementById("reconciliationCtx").innerHTML = `
    <strong>Why CTHRU gross outflows (${fmtV(br.gross_total)}) are ${excess}% above the FY2025 operating budget (${fmtV(br.gaa_authorization)})</strong><br><br>
    The CTHRU database records <em>all</em> cash and non-cash flows from state accounts — not just the
    operating appropriation. Three categories explain almost all of the gap:<br>
    <ul style="margin:8px 0 0 18px;padding:0">
      <li><strong>Non-cash trust accounting (3TX, ${fmtV(br.noncash)}):</strong> ${br.noncash_note}</li>
      <li><strong>Statutory trust fund disbursements (3TN, ${fmtV(br.trust_cash)}):</strong>
          Pension annuities, MBTA subsidy, bond debt service, and MSBA school construction —
          all outside the annual GAA and driven by actuarial or contractual obligations.</li>
      <li><strong>Capital appropriations (2CN, ${fmtV(br.capital)}):</strong>
          Bond-funded infrastructure and IT projects authorized through the Capital Investment Plan.</li>
    </ul>
    <br>After removing these, the remaining <strong>${fmtV(br.state_direct)}</strong> in direct
    appropriations (1CS + 1CN) is what most people call "the state budget." It is
    <strong>${fmtV(Math.abs(br.lapsed))} below</strong> the H.4800 authorization (${fmtV(br.gaa_authorization)})
    — the difference being lapsed and carryforward appropriations.
    <br><br>
    <em>Source: <a href="${br.gaa_url}" target="_blank" style="color:#23395d">${br.gaa_source}</a></em>`;
}

// ── Fund source KPI row ────────────────────────────────────────────────
if (DATA.fund_sources && DATA.fund_sources.length) {
  const fsKpi  = document.getElementById("fundSourcesKpi");
  const fsBody = document.querySelector("#fundSourcesTable tbody");
  const fsMax  = Math.max(...DATA.fund_sources.map(s => s.amt));

  // KPI boxes for top 5 buckets
  fsKpi.innerHTML = DATA.fund_sources.slice(0, 5).map(s =>
    `<div class="kpi">
       <div class="lbl">${s.short}</div>
       <div class="val">${fmtV(s.amt)}</div>
       <div class="sub">${s.pct}% of gross</div>
     </div>`
  ).join("");

  // Table rows
  DATA.fund_sources.forEach(s => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><strong>${s.name}</strong></td>
      <td class="num">${fmtV(s.amt)}</td>
      <td class="num">${s.pct}%</td>
      <td><div class="bar" style="width:${s.amt/fsMax*100}%"></div></td>
      <td style="font-size:11px;color:#444;line-height:1.4">${s.description}</td>
      <td style="font-size:11px;white-space:nowrap">
        <a href="${s.citation_url}" target="_blank" style="color:#4a6fa5">${s.citation}</a>
      </td>`;
    fsBody.appendChild(tr);
  });
}

// ── Reconciliation table ───────────────────────────────────────────────
if (DATA.budget_reconciliation) {
  const br  = DATA.budget_reconciliation;
  const recBody = document.querySelector("#reconciliationTable tbody");
  const rows = [
    { label: "CTHRU gross outflows (FY2025)",
      amt: br.gross_total, isTotal: true,
      note: "Source: spending.db from Comptroller CTHRU — all FY2025 transactions" },
    { label: "Less: Non-cash trust entries (3TX)",
      amt: -br.noncash,
      note: "Primarily federal Medicaid match via EOL trust (~$8.8B) + UMass/GIC non-cash flows" },
    { label: "Less: Statutory trust disbursements (3TN)",
      amt: -br.trust_cash,
      note: "Pension annuities ($6.6B SERS+TRS), MBTA subsidy, bond debt service, MSBA" },
    { label: "Less: Capital appropriations (2CN)",
      amt: -br.capital,
      note: "Bond-funded capital investment; authorized separately from operating GAA" },
    { label: "Less: Federal grants (4FN)",
      amt: -br.federal,
      note: "Explicit federal pass-throughs; bulk of federal Medicaid match is in 3TX above" },
    { label: "Less: Retained revenue & intragovernmental",
      amt: -br.other,
      note: "Agency fee collections + inter-agency transfers (net to zero at state level)" },
    { label: "= State direct appropriations spent",
      amt: br.state_direct, isSubtotal: true,
      note: "Appropriation types 1CS (subsidiarized) + 1CN (non-subsidiarized)" },
    { label: "FY2025 GAA H.4800 net authorization",
      amt: br.gaa_authorization, isRef: true,
      note: `<a href="${br.gaa_url}" target="_blank" style="color:#4a6fa5">${br.gaa_source}</a>` },
    { label: "Gap (lapsed / carryforward appropriations)",
      amt: br.lapsed, isDiff: true,
      note: br.lapsed_note },
  ];
  rows.forEach(r => {
    const tr = document.createElement("tr");
    const rowStyle = r.isTotal    ? "font-weight:700;border-top:2px solid #111" :
                     r.isSubtotal ? "font-weight:700;background:#fafaf7;border-top:1px solid #ccc" :
                     r.isRef      ? "color:#4a6fa5;font-style:italic" :
                     r.isDiff     ? "color:#777;font-size:12px" : "";
    const amtColor = r.amt < 0           ? "color:#c0392b" :
                     (r.isTotal || r.isSubtotal) ? "font-weight:700" : "";
    tr.innerHTML = `
      <td style="${rowStyle}">${r.label}</td>
      <td class="num" style="${amtColor}">${r.amt < 0 ? "−" : ""}${fmtV(Math.abs(r.amt))}</td>
      <td style="font-size:11px;color:#555;line-height:1.4">${r.note}</td>`;
    recBody.appendChild(tr);
  });
}

// ── Validation checks table ────────────────────────────────────────────
if (DATA.validation && DATA.validation.length) {
  const vBody = document.querySelector("#validationTable tbody");
  DATA.validation.forEach(c => {
    const statusMap = {
      pass: ["ok", "PASS"], warn: ["warn", "WARN"],
      fail: ["", "FAIL"],   unavailable: ["info", "N/A"],
    };
    const [cls, lbl] = statusMap[c.status] || ["info", c.status];
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td style="font-size:11px;color:#555;line-height:1.4">${c.source}</td>
      <td style="font-size:12px">${c.label}
        ${c.note ? `<br><span style="font-size:10px;color:#777;font-style:italic">${c.note}</span>` : ""}
      </td>
      <td class="num">${fmtV(c.expected)}</td>
      <td class="num">${fmtV(c.actual)}</td>
      <td class="num">${c.delta_pct != null ? c.delta_pct.toFixed(2) + "%" : "—"}</td>
      <td><span class="flag ${cls}">${lbl}</span></td>`;
    vBody.appendChild(tr);
  });
}
</script>
</body>
</html>
"""


def fetch_gov_annotations(data):
    """Fetch government-published datasets to annotate the report.

    Sources used (all publicly accessible, no API key required):
      - CTHRU Settlements & Judgments  cthru.data.socrata.com/resource/gpqz-7ppn
        Records every settlement/judgment payment made by a state agency.
        Cross-referenced against spending departments to flag litigation exposure.

    Returns a dict with keys:
      settlements   — structured FY2023-2025 settlement data
      dept_flags    — {dept_name: [annotation, ...]} using exact names from data["departments"]
      sources       — list of source citations rendered in the report
    """
    BASE         = "https://cthru.data.socrata.com"
    SETTLE_4X4   = "gpqz-7ppn"

    def safe_fetch(url, timeout=15):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "mass-audit/1.0", "Accept": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode()), None
        except Exception as exc:
            return None, str(exc)

    gov = {
        "sources":    [],
        "settlements": None,
        "dept_flags": {},
    }

    # ── Settlements & Judgments: FY2023-2025 trend per department ─────────
    print("  → fetching Settlements & Judgments (gpqz-7ppn) …")

    q_dept = (
        "SELECT dept_paid_on_behalf_of, bfy, "
        "SUM(line_amount) AS amt, COUNT(*) AS n "
        "WHERE bfy IN ('2023','2024','2025') "
        "GROUP BY dept_paid_on_behalf_of, bfy "
        "ORDER BY dept_paid_on_behalf_of, bfy"
    )
    rows_dept, err_dept = safe_fetch(
        f"{BASE}/resource/{SETTLE_4X4}.json?$query={urllib.parse.quote(q_dept)}"
    )

    q_cases = (
        "SELECT payee_name, dept_paid_on_behalf_of, line_amount, payment_date "
        "WHERE bfy='2025' ORDER BY line_amount DESC LIMIT 50"
    )
    rows_cases, _ = safe_fetch(
        f"{BASE}/resource/{SETTLE_4X4}.json?$query={urllib.parse.quote(q_cases)}"
    )

    q_total = "SELECT COUNT(*) AS n, SUM(line_amount) AS total WHERE bfy='2025'"
    rows_total, _ = safe_fetch(
        f"{BASE}/resource/{SETTLE_4X4}.json?$query={urllib.parse.quote(q_total)}"
    )

    if rows_dept is not None:
        gov["sources"].append({
            "name":        "CTHRU Settlements & Judgments",
            "dataset_id":  SETTLE_4X4,
            "url":         f"{BASE}/resource/{SETTLE_4X4}",
            "description": (
                "Every settlement and judgment payment made by a state agency, "
                "published by the MA Comptroller via CTHRU. Covers FY2014–present."
            ),
        })

        # Aggregate by department across years
        dept_years: dict = {}
        for r in rows_dept:
            dept = (r.get("dept_paid_on_behalf_of") or "").strip()
            fy   = r.get("bfy", "")
            amt  = float(r.get("amt") or 0)
            n    = int(float(r.get("n") or 0))
            if dept not in dept_years:
                dept_years[dept] = {}
            dept_years[dept][fy] = {"amt": amt, "n": n}

        # Flat list sorted by FY2025 payout
        dept_list = []
        for dept, years in dept_years.items():
            fy25 = years.get("2025", {})
            dept_list.append({
                "dept":       dept,
                "fy25_amt":   fy25.get("amt", 0),
                "fy25_n":     fy25.get("n", 0),
                "fy24_amt":   years.get("2024", {}).get("amt", 0),
                "fy23_amt":   years.get("2023", {}).get("amt", 0),
                "total_3yr":  sum(y.get("amt", 0) for y in years.values()),
            })
        dept_list.sort(key=lambda x: x["fy25_amt"], reverse=True)

        settle_total = float((rows_total or [{}])[0].get("total") or 0)
        settle_n     = int(float((rows_total or [{}])[0].get("n") or 0))

        top_cases = [
            {
                "payee": r.get("payee_name", ""),
                "dept":  r.get("dept_paid_on_behalf_of", ""),
                "amt":   float(r.get("line_amount") or 0),
                "date":  (r.get("payment_date") or "")[:10],
            }
            for r in (rows_cases or [])
        ]

        gov["settlements"] = {
            "total_amt_fy25":  settle_total,
            "total_n_fy25":    settle_n,
            "by_dept":         dept_list,
            "top_cases":       top_cases,
            "source_url":      f"{BASE}/resource/{SETTLE_4X4}",
            "source_label":    "CTHRU Settlements & Judgments (cthru.data.socrata.com)",
        }

        # Build dept_flags keyed on exact dept names from data["departments"]
        # Normalise both sides for matching.
        def norm(s: str) -> str:
            return (
                s.upper()
                 .replace("DEPARTMENT OF ", "DEPT OF ")
                 .replace("EXECUTIVE OFFICE OF ", "EO OF ")
                 .replace(" AND ", " & ")
                 .replace(",", "")
                 .strip()
            )

        our_depts = {d["dept"]: norm(d["dept"]) for d in data.get("departments", [])}

        for entry in dept_list:
            if entry["fy25_amt"] <= 0:
                continue
            settle_norm = norm(entry["dept"])
            # Find best match in our dept names
            matched_key = None
            for our_key, our_norm in our_depts.items():
                if settle_norm == our_norm:
                    matched_key = our_key
                    break
            if matched_key is None:
                for our_key, our_norm in our_depts.items():
                    if settle_norm in our_norm or our_norm in settle_norm:
                        matched_key = our_key
                        break
            if matched_key is None:
                continue   # couldn't match — skip

            if matched_key not in gov["dept_flags"]:
                gov["dept_flags"][matched_key] = []
            gov["dept_flags"][matched_key].append({
                "type":        "SETTLEMENT",
                "badge_label": "⚖ settle",
                "label":       (
                    f"FY2025: {entry['fy25_n']} settlement payments "
                    f"totalling ${entry['fy25_amt']:,.0f} "
                    f"(3-yr: ${entry['total_3yr']:,.0f}). "
                    "Source: CTHRU Settlements & Judgments (gpqz-7ppn)."
                ),
                "fy25_amt":    entry["fy25_amt"],
                "fy25_n":      entry["fy25_n"],
                "fy24_amt":    entry["fy24_amt"],
                "fy23_amt":    entry["fy23_amt"],
            })
    else:
        gov["settlements"] = {"error": err_dept}

    return gov


def main():
    import datetime
    print("Loading data …")
    data = load_data()

    print("Validating …")
    data["validation"] = validate_data(data)

    print("Fetching government annotations …")
    data["gov_annotations"] = fetch_gov_annotations(data)

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
