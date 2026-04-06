"""Download CTHRU Payroll + Revenue from Socrata and load into spending.db."""
import csv
import io
import sqlite3
import time
import requests

DB = "/Users/anthonyleung/playground2/spending.db"
PAYROLL_ID = "9ttk-7vz6"
REVENUE_CSV = "/Users/anthonyleung/playground2/cthru_revenue.csv"
BASE = "https://cthru.data.socrata.com/resource"
BATCH = 50000


def download_payroll():
    """Page through Socrata API and yield rows."""
    offset = 0
    sess = requests.Session()
    while True:
        url = f"{BASE}/{PAYROLL_ID}.json?$limit={BATCH}&$offset={offset}&$order=:id"
        r = sess.get(url, timeout=120)
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        yield from rows
        offset += len(rows)
        print(f"  payroll: fetched {offset:,} rows …", flush=True)
        time.sleep(0.5)


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # --- Revenue (already downloaded as CSV) ---
    print("=== Revenue ===")
    cur.execute("DROP TABLE IF EXISTS revenue")
    cur.execute("""
        CREATE TABLE revenue (
            agency_code TEXT,
            fund_type TEXT,
            calendar_year INTEGER,
            amount REAL,
            description TEXT
        )
    """)
    with open(REVENUE_CSV, newline="") as f:
        rdr = csv.DictReader(f)
        rows = []
        for r in rdr:
            rows.append((
                r.get("QSIAGENCYCODE", ""),
                r.get("REVENUEFUNDTYPE", ""),
                int(r.get("CALENDARYEAR", 0) or 0),
                float(r.get("REVENUEAMOUNT", 0) or 0),
                r.get("REVENUETYPEDESCR", ""),
            ))
        cur.executemany("INSERT INTO revenue VALUES (?,?,?,?,?)", rows)
    conn.commit()
    print(f"  revenue: {len(rows)} rows loaded")

    # --- Payroll ---
    print("=== Payroll ===")
    cur.execute("DROP TABLE IF EXISTS payroll")
    cur.execute("""
        CREATE TABLE payroll (
            year INTEGER,
            trans_no TEXT,
            name_last TEXT,
            name_first TEXT,
            department TEXT,
            position_title TEXT,
            position_type TEXT,
            service_end_date TEXT,
            pay_total REAL,
            pay_base REAL,
            pay_buyout REAL,
            pay_overtime REAL,
            pay_other REAL,
            annual_rate REAL,
            pay_ytd REAL,
            location_zip TEXT,
            contract TEXT,
            bargaining_group_no TEXT,
            bargaining_group_title TEXT,
            department_code TEXT
        )
    """)

    batch = []
    total = 0
    for r in download_payroll():
        batch.append((
            int(r.get("year", 0) or 0),
            r.get("trans_no", ""),
            r.get("name_last", ""),
            r.get("name_first", ""),
            r.get("department_division", ""),
            r.get("position_title", ""),
            r.get("position_type", ""),
            r.get("service_end_date", ""),
            float(r.get("pay_total_actual", 0) or 0),
            float(r.get("pay_base_actual", 0) or 0),
            float(r.get("pay_buyout_actual", 0) or 0),
            float(r.get("pay_overtime_actual", 0) or 0),
            float(r.get("pay_other_actual", 0) or 0),
            float(r.get("annual_rate", 0) or 0),
            float(r.get("pay_year_to_date", 0) or 0),
            r.get("department_location_zip_code", ""),
            r.get("contract", ""),
            r.get("bargaining_group_no", ""),
            r.get("bargaining_group_title", ""),
            r.get("department_code", ""),
        ))
        if len(batch) >= 10000:
            cur.executemany(
                "INSERT INTO payroll VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                batch
            )
            conn.commit()
            total += len(batch)
            batch = []

    if batch:
        cur.executemany(
            "INSERT INTO payroll VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch
        )
        total += len(batch)
    conn.commit()

    print(f"  payroll: {total:,} rows loaded")

    # Indexes
    print("  creating indexes …")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payroll_year ON payroll(year)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payroll_dept ON payroll(department_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payroll_name ON payroll(name_last, name_first)")
    conn.commit()

    # Quick stats
    for yr in [2023, 2024, 2025]:
        row = cur.execute(
            "SELECT COUNT(*), SUM(pay_total), SUM(pay_overtime) FROM payroll WHERE year=?",
            (yr,)
        ).fetchone()
        print(f"  {yr}: {row[0]:,} rows, total_pay=${(row[1] or 0)/1e9:.2f}B, overtime=${(row[2] or 0)/1e6:.0f}M")

    conn.close()
    print("done.")


if __name__ == "__main__":
    main()
