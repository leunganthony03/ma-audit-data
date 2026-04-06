#!/usr/bin/env python3
"""
import_cambridge.py  —  Fetch Cambridge Open Data → cambridge.db (SQLite)

Datasets imported:
  op_expenditures  ~40k rows  Budget - Operating Expenditures FY2011–2026   (5bn4-5wey)
  op_revenues      ~1.7k rows Budget - Operating Revenues FY2011–2026       (ixyv-mje6)
  capital          555 rows   Capital Budget FY2020–2030 (7-year plan)       (9chi-2ed3)
  salary           ~10k rows  Budget Salaries FY2024/2025/2026               (multiple)
  contracts        1,239      Contracts Bid List                             (gp98-ja4f)
  bids             8,330      Bid List (services + construction)             (iud6-avxc, pmii-ykdf)
  property         30,156     Property Assessments FY2026                    (waa7-ibdu)

Run once (or re-run to refresh):
    python3 import_cambridge.py
"""

import sqlite3
import urllib.request
import urllib.parse
import json
import sys
from pathlib import Path

DB   = Path(__file__).parent / "cambridge.db"
BASE = "https://data.cambridgema.gov"

HEADERS = {"User-Agent": "cambridge-audit/1.0", "Accept": "application/json"}


def socrata_fetch(ds_id, select="*", where=None, order=None, page=2000, timeout=60):
    """Paginate through all rows of a Socrata dataset."""
    rows, offset = [], 0
    while True:
        parts = [f"$select={urllib.parse.quote(select)}",
                 f"$limit={page}", f"$offset={offset}"]
        if where:
            parts.append(f"$where={urllib.parse.quote(where)}")
        if order:
            parts.append(f"$order={urllib.parse.quote(order)}")
        url = f"{BASE}/resource/{ds_id}.json?{'&'.join(parts)}"
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                batch = json.loads(r.read().decode())
        except Exception as e:
            print(f"  WARN {ds_id} offset={offset}: {e}", file=sys.stderr)
            break
        if not batch:
            break
        rows.extend(batch)
        print(f"    {len(rows):,} rows …", end="\r")
        if len(batch) < page:
            break
        offset += page
    print(f"    {len(rows):,} rows fetched.   ")
    return rows


def create_schema(cur):
    cur.executescript("""
    DROP TABLE IF EXISTS op_expenditures;
    CREATE TABLE op_expenditures (
        fiscal_year     TEXT,
        service         TEXT,
        department_name TEXT,
        division_name   TEXT,
        category        TEXT,
        description     TEXT,
        amount          REAL,
        fund            TEXT
    );

    DROP TABLE IF EXISTS op_revenues;
    CREATE TABLE op_revenues (
        fiscal_year     TEXT,
        service         TEXT,
        department_name TEXT,
        category        TEXT,
        description     TEXT,
        amount          REAL,
        fund            TEXT
    );

    DROP TABLE IF EXISTS capital;
    CREATE TABLE capital (
        fiscal_year     TEXT,
        department      TEXT,
        project_id      TEXT,
        project_name    TEXT,
        fund            TEXT,
        city_location   TEXT,
        latitude        REAL,
        longitude       REAL,
        approved_amount REAL
    );

    DROP TABLE IF EXISTS salary;
    CREATE TABLE salary (
        fiscal_year       TEXT,
        service           TEXT,
        department        TEXT,
        division          TEXT,
        position_number   TEXT,
        job_title         TEXT,
        total_salary      REAL
    );

    DROP TABLE IF EXISTS contracts;
    CREATE TABLE contracts (
        vendor_name                   TEXT,
        vendor_number                 TEXT,
        contract_title                TEXT,
        contract_id                   TEXT PRIMARY KEY,
        term_type                     TEXT,
        status                        TEXT,
        start_date                    TEXT,
        end_date                      TEXT,
        initial_term_length           TEXT,
        initial_term_unit             TEXT,
        has_renewal_option            TEXT,
        renewals_remaining            INTEGER,
        department                    TEXT,
        contract_type                 TEXT,
        procurement_classification    TEXT,
        is_emergency                  INTEGER,
        is_active_for_financial_transactions INTEGER,
        created_at                    TEXT,
        updated_at                    TEXT
    );

    DROP TABLE IF EXISTS bids;
    CREATE TABLE bids (
        bid_record_id       TEXT PRIMARY KEY,
        bid_number          TEXT,
        bid_type            TEXT,
        release_date        TEXT,
        bid_title           TEXT,
        departments         TEXT,
        addenda_count       INTEGER,
        open_date           TEXT,
        bid_category        TEXT   -- 'services' or 'construction'
    );

    DROP TABLE IF EXISTS property;
    CREATE TABLE property (
        pid                   TEXT,
        fiscal_year           TEXT,
        address               TEXT,
        unit                  TEXT,
        stateclasscode        TEXT,
        propertyclass         TEXT,
        zoning                TEXT,
        map_lot               TEXT,
        landarea              REAL,
        taxdistrict           TEXT,
        residentialexemption  INTEGER,
        buildingvalue         REAL,
        landvalue             REAL,
        assessedvalue         REAL,
        saleprice             REAL,
        saledate              TEXT,
        previousassessedvalue REAL,
        owner_name            TEXT,
        owner_coownername     TEXT,
        owner_address         TEXT,
        owner_city            TEXT,
        owner_state           TEXT,
        owner_zip             TEXT,
        latitude              REAL,
        longitude             REAL,
        condition_yearbuilt   INTEGER,
        interior_livingarea   REAL,
        interior_numunits     INTEGER
    );
    """)


def load_operating(cur, conn):
    print("  → operating expenditures (5bn4-5wey) …")
    rows = socrata_fetch("5bn4-5wey")
    cur.executemany("""
        INSERT INTO op_expenditures VALUES (?,?,?,?,?,?,?,?)
    """, [
        (
            r.get("fiscal_year") or "",
            r.get("service") or "",
            r.get("department_name") or "",
            r.get("division_name") or "",
            r.get("category") or "",
            r.get("description") or "",
            float(r.get("amount") or 0),
            r.get("fund") or "",
        )
        for r in rows
    ])
    conn.commit()
    print(f"    inserted {len(rows):,} operating expenditure rows")

    print("  → operating revenues (ixyv-mje6) …")
    rows2 = socrata_fetch("ixyv-mje6")
    cur.executemany("""
        INSERT INTO op_revenues VALUES (?,?,?,?,?,?,?)
    """, [
        (
            r.get("fiscal_year") or "",
            r.get("service") or "",
            r.get("department_name") or "",
            r.get("category") or "",
            r.get("description") or "",
            float(r.get("amount") or 0),
            r.get("fund") or "",
        )
        for r in rows2
    ])
    conn.commit()
    print(f"    inserted {len(rows2):,} operating revenue rows")


def load_capital(cur, conn):
    print("  → capital budget (9chi-2ed3) …")
    rows = socrata_fetch("9chi-2ed3")
    cur.executemany("""
        INSERT INTO capital VALUES (?,?,?,?,?,?,?,?,?)
    """, [
        (
            r.get("fiscal_year") or "",
            r.get("department") or "",
            r.get("project_id") or "",
            r.get("project_name") or "",
            r.get("fund") or "",
            r.get("city_location") or "",
            float(r.get("latitude") or 0),
            float(r.get("longitude") or 0),
            float(r.get("approved_amount") or 0),
        )
        for r in rows
    ])
    conn.commit()
    print(f"    inserted {len(rows):,} capital budget rows")


def load_salary(cur, conn):
    # FY2024 has no fiscal_year column in the source — inject '2024'
    specs = [
        ("9deu-zhmw", "2024"),
        ("fe9r-dqee", "2025"),
        ("fdgz-2wqe", "2026"),
    ]
    for ds_id, fy in specs:
        print(f"  → salary FY{fy} ({ds_id}) …")
        rows = socrata_fetch(ds_id)
        cur.executemany("""
            INSERT INTO salary VALUES (?,?,?,?,?,?,?)
        """, [
            (
                fy,
                r.get("service") or "",
                r.get("department") or "",
                r.get("division") or "",
                r.get("position_number") or "",
                r.get("job_title") or "",
                float(r.get("total_salary") or 0),
            )
            for r in rows
        ])
        conn.commit()
        print(f"    inserted {len(rows):,} rows into salary (FY{fy})")


def load_contracts(cur, conn):
    print("  → contracts (gp98-ja4f) …")
    rows = socrata_fetch("gp98-ja4f")
    inserted = 0
    for r in rows:
        try:
            cur.execute("""
                INSERT OR REPLACE INTO contracts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                r.get("vendor_name") or "",
                r.get("vendor_number") or "",
                r.get("contract_title") or "",
                r.get("contract_id") or "",
                r.get("term_type") or "",
                r.get("status") or "",
                (r.get("start_date") or "")[:10],
                (r.get("end_date") or "")[:10],
                r.get("initial_term_length") or "",
                r.get("initial_term_unit") or "",
                r.get("has_renewal_option") or "",
                int(r.get("renewals_remaining") or 0),
                r.get("department") or "",
                r.get("contract_type") or "",
                r.get("procurement_classification") or "",
                1 if r.get("is_emergency") == "true" else 0,
                1 if r.get("is_active_for_financial_transactions") == "true" else 0,
                (r.get("created_at") or "")[:10],
                (r.get("updated_at") or "")[:10],
            ))
            inserted += 1
        except Exception as e:
            print(f"  WARN contract {r.get('contract_id')}: {e}", file=sys.stderr)
    conn.commit()
    print(f"    inserted {inserted:,} contracts")


def load_bids(cur, conn):
    # General bids
    print("  → bids (iud6-avxc) …")
    rows = socrata_fetch("iud6-avxc")
    cur.executemany("""
        INSERT OR REPLACE INTO bids VALUES (?,?,?,?,?,?,?,?,?)
    """, [
        (
            r.get("bid_record_id") or "",
            r.get("bid_number") or "",
            r.get("bid_type_description") or "",
            (r.get("release_date") or "")[:10],
            r.get("bid_title") or "",
            r.get("departments") or "",
            int(r.get("addenda_count") or 0),
            (r.get("open_date") or "")[:10],
            "services",
        )
        for r in rows
    ])
    conn.commit()
    print(f"    inserted {len(rows):,} service bids")

    # Construction bids
    print("  → construction bids (pmii-ykdf) …")
    rows2 = socrata_fetch("pmii-ykdf")
    cur.executemany("""
        INSERT OR REPLACE INTO bids VALUES (?,?,?,?,?,?,?,?,?)
    """, [
        (
            r.get("constbid_record_id") or "",
            r.get("bid_number") or "",
            "Construction",
            (r.get("release_date") or "")[:10],
            r.get("bid_description") or "",
            "",
            int(r.get("addenda_count") or 0),
            (r.get("open_date") or "")[:10],
            "construction",
        )
        for r in rows2
    ])
    conn.commit()
    print(f"    inserted {len(rows2):,} construction bids")


def load_property(cur, conn):
    # Select only the columns we need to keep fetches lean
    PROP_SELECT = (
        "pid,yearofassessment,address,unit,stateclasscode,propertyclass,"
        "zoning,map_lot,landarea,taxdistrict,residentialexemption,"
        "buildingvalue,landvalue,assessedvalue,saleprice,saledate,"
        "previousassessedvalue,owner_name,owner_coownername,"
        "owner_address,owner_city,owner_state,owner_zip,"
        "latitude,longitude,condition_yearbuilt,"
        "interior_livingarea,interior_numunits"
    )
    for ds_id, fy in [("waa7-ibdu", "2026"), ("wb6g-ebmw", "2025")]:
        print(f"  → property FY{fy} ({ds_id}) …")
        rows = socrata_fetch(ds_id, select=PROP_SELECT, page=500)
        cur.executemany("""
            INSERT INTO property VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [
            (
                r.get("pid") or "",
                fy,
                r.get("address") or "",
                r.get("unit") or "",
                r.get("stateclasscode") or "",
                r.get("propertyclass") or "",
                r.get("zoning") or "",
                r.get("map_lot") or "",
                float(r.get("landarea") or 0),
                r.get("taxdistrict") or "",
                1 if r.get("residentialexemption") == "True" else 0,
                float(r.get("buildingvalue") or 0),
                float(r.get("landvalue") or 0),
                float(r.get("assessedvalue") or 0),
                float(r.get("saleprice") or 0),
                (r.get("saledate") or "")[:10],
                float(r.get("previousassessedvalue") or 0),
                r.get("owner_name") or "",
                r.get("owner_coownername") or "",
                r.get("owner_address") or "",
                r.get("owner_city") or "",
                r.get("owner_state") or "",
                r.get("owner_zip") or "",
                float(r.get("latitude") or 0),
                float(r.get("longitude") or 0),
                int(r.get("condition_yearbuilt") or 0),
                float(r.get("interior_livingarea") or 0),
                int(r.get("interior_numunits") or 0),
            )
            for r in rows
        ])
        conn.commit()
        print(f"    inserted {len(rows):,} property rows (FY{fy})")


def create_indexes(cur, conn):
    print("  → creating indexes …")
    cur.executescript("""
        CREATE INDEX IF NOT EXISTS idx_sal_fy    ON salary(fiscal_year);
        CREATE INDEX IF NOT EXISTS idx_sal_dept  ON salary(department);
        CREATE INDEX IF NOT EXISTS idx_sal_svc   ON salary(service);

        CREATE INDEX IF NOT EXISTS idx_con_dept   ON contracts(department);
        CREATE INDEX IF NOT EXISTS idx_con_vendor ON contracts(vendor_name);
        CREATE INDEX IF NOT EXISTS idx_con_status ON contracts(status);

        CREATE INDEX IF NOT EXISTS idx_bid_type ON bids(bid_type);
        CREATE INDEX IF NOT EXISTS idx_bid_date ON bids(release_date);
        CREATE INDEX IF NOT EXISTS idx_bid_dept ON bids(departments);

        CREATE INDEX IF NOT EXISTS idx_prop_fy    ON property(fiscal_year);
        CREATE INDEX IF NOT EXISTS idx_prop_cls   ON property(propertyclass);
        CREATE INDEX IF NOT EXISTS idx_prop_owner ON property(owner_name);
        CREATE INDEX IF NOT EXISTS idx_prop_dist  ON property(taxdistrict);

        CREATE INDEX IF NOT EXISTS idx_opex_fy   ON op_expenditures(fiscal_year);
        CREATE INDEX IF NOT EXISTS idx_opex_dept ON op_expenditures(department_name);
        CREATE INDEX IF NOT EXISTS idx_opex_svc  ON op_expenditures(service);
        CREATE INDEX IF NOT EXISTS idx_opex_cat  ON op_expenditures(category);
        CREATE INDEX IF NOT EXISTS idx_opex_fund ON op_expenditures(fund);

        CREATE INDEX IF NOT EXISTS idx_oprev_fy   ON op_revenues(fiscal_year);
        CREATE INDEX IF NOT EXISTS idx_oprev_cat  ON op_revenues(category);

        CREATE INDEX IF NOT EXISTS idx_cap_fy   ON capital(fiscal_year);
        CREATE INDEX IF NOT EXISTS idx_cap_dept ON capital(department);
    """)
    conn.commit()
    print("    indexes created.")


def main():
    print(f"Creating {DB} …")
    DB.unlink(missing_ok=True)
    conn = sqlite3.connect(str(DB))
    cur  = conn.cursor()

    print("Creating schema …")
    create_schema(cur)
    conn.commit()

    print("Importing operating budget data …")
    load_operating(cur, conn)

    print("Importing capital budget …")
    load_capital(cur, conn)

    print("Importing salary data …")
    load_salary(cur, conn)

    print("Importing contracts …")
    load_contracts(cur, conn)

    print("Importing bids …")
    load_bids(cur, conn)

    print("Importing property assessments …")
    load_property(cur, conn)

    print("Building indexes …")
    create_indexes(cur, conn)

    # Summary
    for table in ("op_expenditures","op_revenues","capital","salary","contracts","bids","property"):
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        n = cur.fetchone()[0]
        print(f"  {table:<20} {n:>7,} rows")

    conn.close()
    size_kb = DB.stat().st_size // 1024
    print(f"\nDone. {DB} ({size_kb:,} KB)")


if __name__ == "__main__":
    main()
