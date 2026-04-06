#!/usr/bin/env python3
"""
import_cambridge.py  —  Fetch Cambridge Open Data → cambridge.db (SQLite)

Datasets imported:
  salary       4 rows x ~4k positions (FY2024, FY2025, FY2026)
  contracts    1,239 contracts
  bids         7,082 + 1,248 construction bids
  property     ~30k parcels each for FY2025 and FY2026

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


def socrata_fetch(ds_id, select="*", where=None, order=None, page=2000):
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
            with urllib.request.urlopen(req, timeout=30) as r:
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
        rows = socrata_fetch(ds_id, select=PROP_SELECT)
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
    cur.execute("SELECT COUNT(*) FROM salary")
    ns = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM contracts")
    nc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM bids")
    nb = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM property")
    np = cur.fetchone()[0]

    conn.close()
    size_kb = DB.stat().st_size // 1024
    print(f"\nDone. {DB} ({size_kb:,} KB)")
    print(f"  salary: {ns:,} rows  |  contracts: {nc:,}  |  bids: {nb:,}  |  property: {np:,}")


if __name__ == "__main__":
    main()
