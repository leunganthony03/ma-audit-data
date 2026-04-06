"""Join COMMBUYS contracts to spending DB on normalized vendor name.

Produces a new table `spending.vendor_contract_match` and prints coverage stats:
  - How many distinct spending vendors match a COMMBUYS vendor
  - What % of FY2025 spending dollars are covered by a COMMBUYS contract
  - Vendors with large spend but NO COMMBUYS contract (potential sole-source gaps)
"""
import re
import sqlite3

SPENDING_DB = "/Users/anthonyleung/playground2/spending.db"
COMMBUYS_DB = "/Users/anthonyleung/playground2/commbuys.db"


def normalize(name: str) -> str:
    if not name:
        return ""
    s = name.upper()
    s = re.sub(r"[^\w\s]", " ", s)          # strip punctuation
    s = re.sub(r"\s+", " ", s).strip()
    # strip common legal-form suffixes
    suffixes = [
        "INCORPORATED", "INC", "LLC", "LLP", "LP", "LTD", "CORPORATION", "CORP",
        "COMPANY", "CO", "PC", "PLLC", "NA", "THE", "SOMWBA", "M WBE", "MBE", "WBE",
        "GROUP", "HOLDINGS",
    ]
    tokens = s.split()
    # remove trailing suffix words repeatedly
    while tokens and tokens[-1] in suffixes:
        tokens.pop()
    # also drop leading "THE"
    while tokens and tokens[0] == "THE":
        tokens.pop(0)
    return " ".join(tokens)


def main():
    # Attach commbuys to spending
    conn = sqlite3.connect(SPENDING_DB)
    conn.create_function("norm", 1, normalize)
    conn.execute(f"ATTACH DATABASE '{COMMBUYS_DB}' AS cb")

    # Build normalized vendor tables
    conn.executescript("""
        DROP TABLE IF EXISTS vendor_norm_spending;
        CREATE TABLE vendor_norm_spending AS
            SELECT DISTINCT Vendor AS vendor, norm(Vendor) AS vkey
            FROM spending WHERE Vendor IS NOT NULL AND Vendor <> '';
        CREATE INDEX idx_vns_vkey ON vendor_norm_spending(vkey);

        DROP TABLE IF EXISTS vendor_norm_commbuys;
        CREATE TABLE vendor_norm_commbuys AS
            SELECT DISTINCT vendor AS vendor, norm(vendor) AS vkey
            FROM cb.contracts WHERE vendor IS NOT NULL AND vendor <> '';
        CREATE INDEX idx_vnc_vkey ON vendor_norm_commbuys(vkey);
    """)

    # Build match table linking spending vendor → commbuys contract(s)
    conn.executescript("""
        DROP TABLE IF EXISTS vendor_contract_match;
        CREATE TABLE vendor_contract_match AS
            SELECT
                s.vendor AS spending_vendor,
                c.blanket_id,
                c.bid_id,
                c.vendor AS commbuys_vendor,
                c.description,
                c.type_code,
                c.dollars_spent,
                c.organization,
                c.begin_date,
                c.end_date,
                s.vkey
            FROM vendor_norm_spending s
            JOIN cb.contracts c ON norm(c.vendor) = s.vkey;
        CREATE INDEX idx_vcm_vendor ON vendor_contract_match(spending_vendor);
    """)
    conn.commit()

    # Coverage stats
    print("=" * 70)
    print("MATCH COVERAGE")
    print("=" * 70)

    (n_cb,) = conn.execute("SELECT COUNT(*) FROM cb.contracts").fetchone()
    (n_cb_vendors,) = conn.execute("SELECT COUNT(DISTINCT vendor) FROM cb.contracts").fetchone()
    (n_sp_vendors,) = conn.execute("SELECT COUNT(DISTINCT Vendor) FROM spending").fetchone()
    print(f"COMMBUYS: {n_cb:,} contracts across {n_cb_vendors:,} vendors")
    print(f"Spending DB: {n_sp_vendors:,} distinct vendors")

    (matched_vendors,) = conn.execute("""
        SELECT COUNT(DISTINCT s.vendor)
        FROM vendor_norm_spending s
        JOIN vendor_norm_commbuys c USING (vkey)
    """).fetchone()
    (cb_matched,) = conn.execute("""
        SELECT COUNT(DISTINCT c.vendor)
        FROM vendor_norm_commbuys c
        JOIN vendor_norm_spending s USING (vkey)
    """).fetchone()
    print(f"\nSpending vendors with a COMMBUYS match: {matched_vendors:,} "
          f"({matched_vendors/n_sp_vendors*100:.1f}%)")
    print(f"COMMBUYS vendors with a spending match:  {cb_matched:,} "
          f"({cb_matched/n_cb_vendors*100:.1f}%)")

    # FY2025 spend covered
    row = conn.execute("""
        SELECT
            SUM(CAST(REPLACE(s.Amount,',','') AS REAL)) AS total,
            SUM(CASE WHEN m.spending_vendor IS NOT NULL
                     THEN CAST(REPLACE(s.Amount,',','') AS REAL)
                     ELSE 0 END) AS covered
        FROM spending s
        LEFT JOIN (SELECT DISTINCT spending_vendor FROM vendor_contract_match) m
               ON s.Vendor = m.spending_vendor
        WHERE s.Budget_Fiscal_Year = '2025'
    """).fetchone()
    total, covered = row
    print(f"\nFY2025 total: ${total:,.0f}")
    print(f"FY2025 covered by COMMBUYS contract: ${covered:,.0f} "
          f"({covered/total*100:.1f}%)")

    # Top 20 FY2025 vendors WITHOUT any COMMBUYS contract (potential gaps)
    print("\n" + "=" * 70)
    print("TOP 20 FY2025 VENDORS WITHOUT A COMMBUYS CONTRACT MATCH")
    print("=" * 70)
    rows = conn.execute("""
        SELECT s.Vendor, SUM(CAST(REPLACE(s.Amount,',','') AS REAL)) AS amt
        FROM spending s
        LEFT JOIN (SELECT DISTINCT spending_vendor FROM vendor_contract_match) m
               ON s.Vendor = m.spending_vendor
        WHERE s.Budget_Fiscal_Year = '2025'
          AND m.spending_vendor IS NULL
          AND s.Vendor IS NOT NULL AND s.Vendor <> ''
        GROUP BY s.Vendor
        ORDER BY amt DESC
        LIMIT 20
    """).fetchall()
    for v, a in rows:
        print(f"  ${a:>15,.0f}  {v}")

    # Top 20 FY2025 vendors WITH COMMBUYS contracts
    print("\n" + "=" * 70)
    print("TOP 20 FY2025 VENDORS WITH COMMBUYS CONTRACTS (sample contract ID)")
    print("=" * 70)
    rows = conn.execute("""
        SELECT s.Vendor,
               SUM(CAST(REPLACE(s.Amount,',','') AS REAL)) AS amt,
               (SELECT blanket_id FROM vendor_contract_match m
                 WHERE m.spending_vendor = s.Vendor LIMIT 1) AS sample_contract,
               (SELECT COUNT(*) FROM vendor_contract_match m
                 WHERE m.spending_vendor = s.Vendor) AS n_contracts
        FROM spending s
        WHERE s.Budget_Fiscal_Year = '2025'
          AND s.Vendor IN (SELECT DISTINCT spending_vendor FROM vendor_contract_match)
        GROUP BY s.Vendor
        ORDER BY amt DESC
        LIMIT 20
    """).fetchall()
    for v, a, c, n in rows:
        print(f"  ${a:>15,.0f}  [{n:>3}x]  {c:<45}  {v}")

    conn.close()


if __name__ == "__main__":
    main()
