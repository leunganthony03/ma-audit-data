"""Sole-source / competition analysis using scraped COMMBUYS detail data.

Joins contracts + po_details + bid_details + spending DB to answer:
  1. What fraction of COMMBUYS contracts were competed vs sole-source?
  2. Breakdown by dollar volume (not just count).
  3. Top vendors by sole-source / no-bid dollars.
  4. FY2025 spending flowing through sole-source contracts.
  5. Vendors receiving large FY2025 spend with no competed bid.
"""
import re
import sqlite3

SPENDING_DB = "/Users/anthonyleung/playground2/spending.db"
COMMBUYS_DB = "/Users/anthonyleung/playground2/commbuys.db"


def normalize(name):
    if not name:
        return ""
    s = name.upper()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    drop = {"INCORPORATED","INC","LLC","LLP","LP","LTD","CORPORATION","CORP",
            "COMPANY","CO","PC","PLLC","NA","THE","SOMWBA","MBE","WBE","GROUP","HOLDINGS"}
    toks = [t for t in s.split() if t not in drop]
    return " ".join(toks)


def main():
    conn = sqlite3.connect(COMMBUYS_DB)
    conn.create_function("norm", 1, normalize)
    conn.execute(f"ATTACH '{SPENDING_DB}' AS sp")

    # Classify each contract: has_bid (competed or not), bid_type category
    conn.executescript("""
        DROP VIEW IF EXISTS contract_classified;
        CREATE VIEW contract_classified AS
        SELECT
            c.blanket_id,
            c.bid_id,
            c.vendor,
            c.description,
            c.type_code,
            c.dollars_spent,
            c.organization,
            c.begin_date,
            c.end_date,
            CASE
                WHEN c.bid_id IS NULL OR c.bid_id = '' THEN 'NO_BID'
                WHEN b.bid_type IS NULL THEN 'BID_UNKNOWN'
                WHEN UPPER(b.bid_type) LIKE '%OPEN%' THEN 'OPEN_COMPETITIVE'
                WHEN UPPER(b.bid_type) LIKE '%SOLE%' OR UPPER(b.bid_type) LIKE 'SS%' THEN 'SOLE_SOURCE'
                WHEN UPPER(b.bid_type) LIKE '%EMERGENCY%' THEN 'EMERGENCY'
                WHEN UPPER(b.bid_type) LIKE '%LIMITED%' THEN 'LIMITED'
                WHEN UPPER(b.bid_type) LIKE '%INVITE%' THEN 'INVITED'
                ELSE UPPER(COALESCE(b.bid_type, ''))
            END AS competition,
            b.bid_type AS raw_bid_type,
            b.informal_bid_flag,
            b.purchase_method,
            p.total_dollar_limit,
            p.total_dollars_spent,
            p.vendor_name AS po_vendor,
            p.vendor_id,
            p.status AS po_status
        FROM contracts c
        LEFT JOIN bid_details b ON c.bid_id = b.bid_id
        LEFT JOIN po_details p ON c.blanket_id = p.blanket_id;
    """)

    # ----- 1. Competition distribution -----
    print("=" * 72)
    print("1. COMPETITION MIX (COMMBUYS contracts)")
    print("=" * 72)
    print(f"{'competition':<22} {'contracts':>10} {'vendors':>10} {'$ reported':>20}")
    rows = conn.execute("""
        SELECT competition,
               COUNT(*) AS n,
               COUNT(DISTINCT vendor) AS nv,
               SUM(COALESCE(total_dollars_spent, 0)) AS spent
        FROM contract_classified
        GROUP BY competition
        ORDER BY n DESC
    """).fetchall()
    for c, n, nv, sp in rows:
        print(f"{c:<22} {n:>10,} {nv:>10,} ${sp:>18,.0f}")

    # ----- 2. Raw bid_type values -----
    print("\n" + "=" * 72)
    print("2. RAW bid_type VALUES (top)")
    print("=" * 72)
    rows = conn.execute("""
        SELECT COALESCE(bid_type,'(null)') AS bt, COUNT(*) n
        FROM bid_details GROUP BY bt ORDER BY n DESC LIMIT 15
    """).fetchall()
    for bt, n in rows:
        print(f"  {bt:<50} {n:>8,}")

    # ----- 3. Purchase methods & informal bids -----
    print("\n" + "=" * 72)
    print("3. PURCHASE METHOD & INFORMAL BID FLAG")
    print("=" * 72)
    rows = conn.execute("""
        SELECT COALESCE(purchase_method,'(null)'), COUNT(*) FROM bid_details
        GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """).fetchall()
    print("Purchase methods:")
    for pm, n in rows:
        print(f"  {pm:<50} {n:>8,}")
    rows = conn.execute("""
        SELECT COALESCE(informal_bid_flag,'(null)'), COUNT(*) FROM bid_details
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    print("Informal bid flag:")
    for f, n in rows:
        print(f"  {f:<10} {n:>8,}")

    # ----- 4. Top sole-source / no-bid vendors by reported spend -----
    print("\n" + "=" * 72)
    print("4. TOP VENDORS BY NO-BID / UNKNOWN CONTRACT VOLUME (reported $)")
    print("=" * 72)
    rows = conn.execute("""
        SELECT vendor,
               SUM(total_dollars_spent) AS spent,
               COUNT(*) n,
               GROUP_CONCAT(DISTINCT competition)
        FROM contract_classified
        WHERE competition IN ('NO_BID','SOLE_SOURCE','EMERGENCY','LIMITED','BID_UNKNOWN')
          AND total_dollars_spent > 0
        GROUP BY vendor
        ORDER BY spent DESC
        LIMIT 25
    """).fetchall()
    for v, sp, n, comps in rows:
        print(f"  ${sp:>15,.0f}  [{n:>3}x]  {comps:<30}  {v}")

    # ----- 5. FY2025 spending through each competition category -----
    print("\n" + "=" * 72)
    print("5. FY2025 SPENDING DOLLARS BY CONTRACT COMPETITION CATEGORY")
    print("=" * 72)
    # Build vendor match via pre-normalized dim tables (avoid norm() on 47M rows).
    print("  building vendor dim tables...", flush=True)
    conn.executescript("""
        DROP TABLE IF EXISTS vdim_spending;
        CREATE TABLE vdim_spending AS
            SELECT DISTINCT Vendor AS vendor, norm(Vendor) AS vkey
            FROM sp.spending WHERE Budget_Fiscal_Year='2025' AND Vendor <> '';
        CREATE INDEX idx_vdim_sp_vkey ON vdim_spending(vkey);

        DROP TABLE IF EXISTS vdim_commbuys;
        CREATE TABLE vdim_commbuys AS
            SELECT DISTINCT vendor AS vendor, norm(vendor) AS vkey
            FROM contract_classified WHERE vendor <> '';
        CREATE INDEX idx_vdim_cb_vkey ON vdim_commbuys(vkey);

        DROP TABLE IF EXISTS vendor_match;
        CREATE TABLE vendor_match AS
        SELECT DISTINCT
            vs.vendor AS spending_vendor,
            cc.blanket_id, cc.vendor AS commbuys_vendor,
            cc.competition, cc.raw_bid_type
        FROM vdim_spending vs
        JOIN vdim_commbuys vc ON vs.vkey = vc.vkey
        JOIN contract_classified cc ON cc.vendor = vc.vendor;
        CREATE INDEX idx_vm_sv ON vendor_match(spending_vendor);
    """)
    print("  vendor_match rows:",
          conn.execute("SELECT COUNT(*) FROM vendor_match").fetchone()[0], flush=True)

    rows = conn.execute("""
        WITH vendor_best AS (
            -- a vendor is considered 'competed' if any of their contracts is OPEN_COMPETITIVE
            SELECT spending_vendor,
                   CASE
                       WHEN SUM(CASE WHEN competition='OPEN_COMPETITIVE' THEN 1 ELSE 0 END) > 0 THEN 'HAS_OPEN_BID'
                       WHEN SUM(CASE WHEN competition='SOLE_SOURCE' THEN 1 ELSE 0 END) > 0 THEN 'SOLE_SOURCE_ONLY'
                       WHEN SUM(CASE WHEN competition IN ('EMERGENCY','LIMITED','INVITED') THEN 1 ELSE 0 END) > 0 THEN 'LIMITED_COMP'
                       WHEN SUM(CASE WHEN competition='NO_BID' THEN 1 ELSE 0 END) > 0 THEN 'NO_BID_REF'
                       ELSE 'UNKNOWN'
                   END AS category
            FROM vendor_match
            GROUP BY spending_vendor
        )
        SELECT COALESCE(vb.category,'NO_COMMBUYS_CONTRACT') AS cat,
               COUNT(DISTINCT s.Vendor) AS nv,
               SUM(CAST(REPLACE(s.Amount,',','') AS REAL)) AS amt
        FROM sp.spending s
        LEFT JOIN vendor_best vb ON s.Vendor = vb.spending_vendor
        WHERE s.Budget_Fiscal_Year='2025' AND s.Vendor <> ''
        GROUP BY cat
        ORDER BY amt DESC
    """).fetchall()
    total = sum(r[2] or 0 for r in rows)
    for cat, nv, amt in rows:
        amt = amt or 0
        print(f"  {cat:<25} vendors={nv:>7,}  ${amt:>17,.0f}  {amt/total*100:5.1f}%")
    print(f"  {'TOTAL':<25} {'':<15}  ${total:>17,.0f}")

    # ----- 6. Largest FY2025 vendors with NO open bid on file -----
    print("\n" + "=" * 72)
    print("6. LARGEST FY2025 VENDORS WITH A COMMBUYS CONTRACT BUT NO OPEN BID")
    print("=" * 72)
    rows = conn.execute("""
        WITH v AS (
            SELECT spending_vendor,
                   MAX(CASE WHEN competition='OPEN_COMPETITIVE' THEN 1 ELSE 0 END) AS has_open,
                   GROUP_CONCAT(DISTINCT competition) AS cats,
                   GROUP_CONCAT(DISTINCT raw_bid_type) AS raw
            FROM vendor_match
            GROUP BY spending_vendor
        )
        SELECT s.Vendor,
               SUM(CAST(REPLACE(s.Amount,',','') AS REAL)) AS amt,
               v.cats,
               v.raw
        FROM sp.spending s
        JOIN v ON s.Vendor = v.spending_vendor
        WHERE s.Budget_Fiscal_Year='2025' AND v.has_open=0
        GROUP BY s.Vendor
        ORDER BY amt DESC
        LIMIT 25
    """).fetchall()
    for v, a, cats, raw in rows:
        print(f"  ${a:>15,.0f}  cats={cats:<35}  {v}")

    conn.close()


if __name__ == "__main__":
    main()
