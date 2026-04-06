"""Scrape PO (poSummary) and Bid (bidDetail) pages in parallel.

Reads blanket_id/bid_id from commbuys.db, fetches each detail page via
simple GET (no JSF state), parses key fields with regex on flat text,
writes into new tables:
  - po_details(blanket_id PK, ...)
  - bid_details(bid_id PK, ...)

Uses a thread pool (configurable). Safe to rerun: uses INSERT OR IGNORE.
"""
import argparse
import queue
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

DB = "/Users/anthonyleung/playground2/commbuys.db"
BASE = "https://www.commbuys.com"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"

# Thread-local session (one per worker)
_tls = threading.local()


def get_session():
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        s.headers["User-Agent"] = UA
        s.get(f"{BASE}/bso/", timeout=30)
        _tls.s = s
    return s


# ---------- DB schema ----------

def init_db():
    conn = sqlite3.connect(DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS po_details (
            blanket_id TEXT PRIMARY KEY,
            release_number TEXT,
            short_description TEXT,
            status TEXT,
            purchaser TEXT,
            fiscal_year TEXT,
            po_type TEXT,
            organization TEXT,
            department TEXT,
            entered_date TEXT,
            vendor_raw TEXT,
            vendor_id TEXT,
            vendor_name TEXT,
            vendor_email TEXT,
            vendor_phone TEXT,
            begin_date TEXT,
            end_date TEXT,
            cooperative_purchasing TEXT,
            total_dollar_limit REAL,
            total_dollars_spent REAL,
            bid_ref TEXT,
            item_count INTEGER,
            unspsc_codes TEXT,
            agency_attachments TEXT,
            vendor_attachments TEXT,
            fetch_status INTEGER,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bid_details (
            bid_id TEXT PRIMARY KEY,
            description TEXT,
            bid_opening_date TEXT,
            purchaser TEXT,
            organization TEXT,
            department TEXT,
            fiscal_year TEXT,
            type_code TEXT,
            allow_electronic_quote TEXT,
            available_date TEXT,
            bid_type TEXT,
            informal_bid_flag TEXT,
            purchase_method TEXT,
            begin_date TEXT,
            end_date TEXT,
            sbpp_eligible TEXT,
            amendment_count INTEGER,
            file_attachments TEXT,
            fetch_status INTEGER,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


# ---------- Parsers ----------

# Labels we care about on the PO page. Order matters (earlier wins if dup).
PO_LABELS = [
    "Purchase Order Number", "Release Number", "Short Description", "Status",
    "Purchaser", "Receipt Method", "Fiscal Year", "PO Type", "Minor Status",
    "Organization", "Department", "Location", "Type Code", "Alternate ID",
    "Entered Date", "Days ARO", "Retainage %", "Discount %", "Release Type",
    "Contact Instructions", "Actual Cost", "Print Format", "Special Instructions",
    "Agency Attachments", "Vendor Attachments",
    "Primary Vendor Information & PO Terms", "Vendor",
    "Payment Terms", "Shipping Method", "Shipping Terms", "Freight Terms",
    "PO Acknowledgements", "Master Blanket Vendor Distributor List",
    "Master Blanket Controls", "Master Blanket Begin Date", "Master Blanket End Date",
    "Cooperative Purchasing Allowed", "Item Information",
    "Email", "Phone", "FAX",
]

BID_LABELS = [
    "Bid Number", "Description", "Bid Opening Date", "Purchaser", "Organization",
    "Department", "Location", "Fiscal Year", "Type Code", "Allow Electronic Quote",
    "Alternate Id", "Required Date", "Available Date", "Info Contact",
    "Bid Type", "Informal Bid Flag", "Purchase Method", "Begin Date", "End Date",
    "Pre Bid Conference", "Bulletin Desc", "Ship-to Address", "Bill-to Address",
    "Print Format", "File Attachments", "Form Attachments", "Amendments",
    "Item Information",
]


def flatten_text(html):
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    return soup.get_text(" ", strip=True)


def kv_extract(text, labels):
    """For each label, find 'Label:' then capture until the next known label (or EOS)."""
    # Build alternation of label patterns (escaped), longest first so 'Master Blanket Begin Date'
    # wins over 'Begin Date'.
    sorted_labels = sorted(labels, key=len, reverse=True)
    alt = "|".join(re.escape(l) for l in sorted_labels)
    pattern = re.compile(rf"({alt})\s*:\s*(.*?)(?=\s+(?:{alt})\s*:|$)")
    out = {}
    for m in pattern.finditer(text):
        k, v = m.group(1), m.group(2).strip()
        # Keep first hit per label
        if k not in out:
            out[k] = v
    return out


def parse_money(s):
    if not s:
        return 0.0
    s = s.replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_po(blanket_id, html):
    text = flatten_text(html)
    kv = kv_extract(text, PO_LABELS)

    # Vendor info: "Vendor: 300155 - Franklin Caterers Michael Franklin 1 Nicholas Rd ..."
    vendor_raw = kv.get("Vendor", "")
    vid, vname = "", ""
    m = re.match(r"(\d+)\s*-\s*(.+)", vendor_raw)
    if m:
        vid = m.group(1)
        vname = m.group(2).strip()
        # vname may include trailing address; keep only up to first run of 2+ spaces or digits
        vname = re.split(r"\s{2,}|\d{1,5}\s", vname, maxsplit=1)[0].strip()

    # Email/phone from the vendor block specifically
    vendor_block = ""
    mb = re.search(r"Primary Vendor Information[^:]*:?\s*(.*?)(?:Master Blanket|PO Acknowledgements|$)", text, re.S)
    if mb:
        vendor_block = mb.group(1)
    email_m = re.search(r"Email:\s*([^\s]+@[^\s]+)", vendor_block) or re.search(r"Email:\s*([^\s]+@[^\s]+)", text)
    phone_m = re.search(r"Phone:\s*(\([^)]+\)\s*[\d\-]+|\d{3}[\s\-]\d{3}[\s\-]\d{4})", vendor_block) or re.search(r"Phone:\s*(\([^)]+\)\s*[\d\-]+|\d{3}[\s\-]\d{3}[\s\-]\d{4})", text)

    # Dollar limit table: each row = org | dept | dollar_limit | spent | min
    # Find the Dollar Limit table header, then sum following rows.
    total_limit = 0.0
    total_spent = 0.0
    m2 = re.search(r"Organization\s+Department\s+Dollar Limit\s+Dollars Spent to Date\s+Minimum Order Amount(.*?)Item Information",
                   text, re.S)
    if m2:
        body = m2.group(1)
        # rows of 5 cells: match sequences of "... num num num"
        for row in re.finditer(r"(\S.*?)\s+([\d,\.]+)\s+([\d,\.]+)\s+([\d,\.]+)(?=\s|$)", body):
            total_limit += parse_money(row.group(2))
            total_spent += parse_money(row.group(3))

    # Bid reference inside Item Information
    bid_m = re.search(r"Bid #\s*/\s*Bid Item #:\s*(BD-[\w\-]+)", text)

    # UNSPSC codes
    unspsc = re.findall(r"\b\d{2}-\d{2}-\d{2}-\d{2}-\d{4}\b", text)

    # Item count: number of "Print Sequence #" occurrences
    item_count = len(re.findall(r"Print Sequence #", text))

    return {
        "blanket_id": blanket_id,
        "release_number": kv.get("Release Number", ""),
        "short_description": kv.get("Short Description", ""),
        "status": kv.get("Status", ""),
        "purchaser": kv.get("Purchaser", ""),
        "fiscal_year": kv.get("Fiscal Year", ""),
        "po_type": kv.get("PO Type", ""),
        "organization": kv.get("Organization", ""),
        "department": kv.get("Department", ""),
        "entered_date": kv.get("Entered Date", ""),
        "vendor_raw": vendor_raw[:500],
        "vendor_id": vid,
        "vendor_name": vname,
        "vendor_email": email_m.group(1) if email_m else "",
        "vendor_phone": phone_m.group(1) if phone_m else "",
        "begin_date": kv.get("Master Blanket Begin Date", ""),
        "end_date": kv.get("Master Blanket End Date", ""),
        "cooperative_purchasing": kv.get("Cooperative Purchasing Allowed", ""),
        "total_dollar_limit": total_limit,
        "total_dollars_spent": total_spent,
        "bid_ref": bid_m.group(1) if bid_m else "",
        "item_count": item_count,
        "unspsc_codes": ",".join(sorted(set(unspsc)))[:2000],
        "agency_attachments": kv.get("Agency Attachments", "")[:2000],
        "vendor_attachments": kv.get("Vendor Attachments", "")[:2000],
        "fetch_status": 200,
    }


def parse_bid(bid_id, html):
    text = flatten_text(html)
    kv = kv_extract(text, BID_LABELS)

    # SBPP eligibility
    sbpp_m = re.search(r"SBPP.*?Eligible\?\s*:\s*(\w+)", text)

    # Amendment count
    amends = re.findall(r"Amendment #\s*(\d+)|^\s*(\d+)\s+\d{2}/\d{2}/\d{4}", text, re.M)
    # fallback: find the amendments table header then count numbered rows
    am_count = 0
    m = re.search(r"Amendment #\s+Amendment Date.*?(?=Item Information|File Attachments:|$)", text, re.S)
    if m:
        am_count = len(re.findall(r"\b\d+\s+\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}", m.group(0)))

    file_att = ""
    m = re.search(r"File Attachments:\s*(.*?)(?:Form Attachments:|Required Quote Attachments|Amendments:|Item Information)", text, re.S)
    if m:
        file_att = m.group(1).strip()[:2000]

    return {
        "bid_id": bid_id,
        "description": kv.get("Description", ""),
        "bid_opening_date": kv.get("Bid Opening Date", ""),
        "purchaser": kv.get("Purchaser", ""),
        "organization": kv.get("Organization", ""),
        "department": kv.get("Department", ""),
        "fiscal_year": kv.get("Fiscal Year", ""),
        "type_code": kv.get("Type Code", ""),
        "allow_electronic_quote": kv.get("Allow Electronic Quote", ""),
        "available_date": kv.get("Available Date", ""),
        "bid_type": kv.get("Bid Type", ""),
        "informal_bid_flag": kv.get("Informal Bid Flag", ""),
        "purchase_method": kv.get("Purchase Method", ""),
        "begin_date": kv.get("Begin Date", ""),
        "end_date": kv.get("End Date", ""),
        "sbpp_eligible": sbpp_m.group(1) if sbpp_m else "",
        "amendment_count": am_count,
        "file_attachments": file_att,
        "fetch_status": 200,
    }


# ---------- Fetchers ----------

def fetch_po(blanket_id):
    url = f"{BASE}/bso/external/purchaseorder/poSummary.sda?docId={blanket_id}&releaseNbr=0&external=true"
    try:
        r = get_session().get(url, timeout=30)
        if r.status_code != 200 or len(r.text) < 2000:
            return {"blanket_id": blanket_id, "fetch_status": r.status_code,
                    **{k: "" for k in PO_EMPTY}, "total_dollar_limit": 0, "total_dollars_spent": 0, "item_count": 0}
        return parse_po(blanket_id, r.text)
    except Exception as e:
        return {"blanket_id": blanket_id, "fetch_status": -1,
                **{k: "" for k in PO_EMPTY}, "total_dollar_limit": 0, "total_dollars_spent": 0, "item_count": 0}


def fetch_bid(bid_id):
    url = f"{BASE}/bso/external/bidDetail.sda?docId={bid_id}&external=true&parentUrl=close"
    try:
        r = get_session().get(url, timeout=30)
        if r.status_code != 200 or len(r.text) < 2000:
            return {"bid_id": bid_id, "fetch_status": r.status_code,
                    **{k: "" for k in BID_EMPTY}, "amendment_count": 0}
        return parse_bid(bid_id, r.text)
    except Exception:
        return {"bid_id": bid_id, "fetch_status": -1,
                **{k: "" for k in BID_EMPTY}, "amendment_count": 0}


PO_EMPTY = ["release_number","short_description","status","purchaser","fiscal_year","po_type",
            "organization","department","entered_date","vendor_raw","vendor_id","vendor_name",
            "vendor_email","vendor_phone","begin_date","end_date","cooperative_purchasing",
            "bid_ref","unspsc_codes","agency_attachments","vendor_attachments"]
BID_EMPTY = ["description","bid_opening_date","purchaser","organization","department","fiscal_year",
             "type_code","allow_electronic_quote","available_date","bid_type","informal_bid_flag",
             "purchase_method","begin_date","end_date","sbpp_eligible","file_attachments"]


# ---------- Runners ----------

def run_po(workers, limit):
    conn = init_db()
    done = set(r[0] for r in conn.execute("SELECT blanket_id FROM po_details"))
    todo = [r[0] for r in conn.execute("SELECT blanket_id FROM contracts WHERE blanket_id NOT IN (SELECT blanket_id FROM po_details)").fetchall()]
    if limit:
        todo = todo[:limit]
    print(f"[po] {len(todo):,} to fetch with {workers} workers (already done: {len(done):,})", flush=True)

    n = 0
    ok = 0
    batch = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for row in ex.map(fetch_po, todo):
            n += 1
            if row.get("fetch_status") == 200:
                ok += 1
            batch.append(row)
            if len(batch) >= 50:
                write_po(conn, batch); batch = []
                rate = n / (time.time() - t0)
                eta = (len(todo) - n) / rate if rate else 0
                print(f"[po] {n}/{len(todo)} ok={ok} rate={rate:.1f}/s eta={eta/60:.1f}m", flush=True)
    if batch:
        write_po(conn, batch)
    print(f"[po] done: {n} fetched, {ok} ok", flush=True)
    conn.close()


def write_po(conn, rows):
    cols = ("blanket_id,release_number,short_description,status,purchaser,fiscal_year,po_type,"
            "organization,department,entered_date,vendor_raw,vendor_id,vendor_name,vendor_email,"
            "vendor_phone,begin_date,end_date,cooperative_purchasing,total_dollar_limit,"
            "total_dollars_spent,bid_ref,item_count,unspsc_codes,agency_attachments,"
            "vendor_attachments,fetch_status")
    placeholders = ",".join(":" + c for c in cols.split(","))
    conn.executemany(f"INSERT OR REPLACE INTO po_details ({cols}) VALUES ({placeholders})", rows)
    conn.commit()


def run_bid(workers, limit):
    conn = init_db()
    done = set(r[0] for r in conn.execute("SELECT bid_id FROM bid_details"))
    todo = [r[0] for r in conn.execute("""
        SELECT DISTINCT bid_id FROM contracts
         WHERE bid_id IS NOT NULL AND bid_id <> ''
           AND bid_id NOT IN (SELECT bid_id FROM bid_details)
    """).fetchall()]
    if limit:
        todo = todo[:limit]
    print(f"[bid] {len(todo):,} distinct bids to fetch with {workers} workers (already done: {len(done):,})", flush=True)

    n = 0; ok = 0; batch = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for row in ex.map(fetch_bid, todo):
            n += 1
            if row.get("fetch_status") == 200:
                ok += 1
            batch.append(row)
            if len(batch) >= 50:
                write_bid(conn, batch); batch = []
                rate = n / (time.time() - t0)
                eta = (len(todo) - n) / rate if rate else 0
                print(f"[bid] {n}/{len(todo)} ok={ok} rate={rate:.1f}/s eta={eta/60:.1f}m", flush=True)
    if batch:
        write_bid(conn, batch)
    print(f"[bid] done: {n} fetched, {ok} ok", flush=True)
    conn.close()


def write_bid(conn, rows):
    cols = ("bid_id,description,bid_opening_date,purchaser,organization,department,fiscal_year,"
            "type_code,allow_electronic_quote,available_date,bid_type,informal_bid_flag,"
            "purchase_method,begin_date,end_date,sbpp_eligible,amendment_count,file_attachments,"
            "fetch_status")
    placeholders = ",".join(":" + c for c in cols.split(","))
    conn.executemany(f"INSERT OR REPLACE INTO bid_details ({cols}) VALUES ({placeholders})", rows)
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("target", choices=["po", "bid", "both"])
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    if args.target in ("po", "both"):
        run_po(args.workers, args.limit)
    if args.target in ("bid", "both"):
        run_bid(args.workers, args.limit)


if __name__ == "__main__":
    main()
