"""
Import commbuys.db and spending.db (SQLite) into a single DuckDB file.

Each SQLite database maps to a DuckDB schema:
  - commbuys.db  → schema: commbuys
  - spending.db  → schema: spending

Tables are materialized as native DuckDB tables.
SQLite views are re-created as DuckDB views.
"""

import duckdb

DUCKDB_FILE = "audit.duckdb"

COMMBUYS_TABLES = [
    "contracts",
    "po_details",
    "bid_details",
    "vdim_spending",
    "vdim_commbuys",
    "vendor_match",
]

COMMBUYS_VIEWS = {
    "contract_classified": """
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
        FROM commbuys.contracts c
        LEFT JOIN commbuys.bid_details b ON c.bid_id = b.bid_id
        LEFT JOIN commbuys.po_details p ON c.blanket_id = p.blanket_id
    """,
}

SPENDING_TABLES = [
    "spending",
    "vendor_norm_spending",
    "vendor_norm_commbuys",
    "vendor_contract_match",
    "revenue",
    "payroll",
]


def import_sqlite(con: duckdb.DuckDBPyConnection, sqlite_path: str, schema: str, tables: list[str]) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"ATTACH '{sqlite_path}' AS src (TYPE sqlite, READ_ONLY)")
    try:
        for table in tables:
            print(f"  Importing {schema}.{table} ...", end=" ", flush=True)
            con.execute(f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * FROM src.{table}")
            count = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
            print(f"{count:,} rows")
    finally:
        con.execute("DETACH src")


def create_views(con: duckdb.DuckDBPyConnection, schema: str, views: dict[str, str]) -> None:
    for name, sql in views.items():
        print(f"  Creating view {schema}.{name} ...")
        con.execute(f"CREATE OR REPLACE VIEW {schema}.{name} AS {sql}")


def main() -> None:
    print(f"Opening DuckDB: {DUCKDB_FILE}")
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("INSTALL sqlite; LOAD sqlite")

    print("\n[commbuys.db]")
    import_sqlite(con, "commbuys.db", "commbuys", COMMBUYS_TABLES)
    create_views(con, "commbuys", COMMBUYS_VIEWS)

    print("\n[spending.db]")
    import_sqlite(con, "spending.db", "spending", SPENDING_TABLES)

    print("\nDone. Schemas available:")
    schemas = con.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name").fetchall()
    for (s,) in schemas:
        if s not in ("information_schema", "main", "pg_catalog"):
            tables = con.execute(
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name",
                [s],
            ).fetchall()
            print(f"  {s}:")
            for name, ttype in tables:
                print(f"    {name} ({ttype.lower()})")

    con.close()


if __name__ == "__main__":
    main()
