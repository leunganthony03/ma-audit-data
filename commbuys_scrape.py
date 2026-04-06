"""Comprehensive COMMBUYS contract blanket scraper.

Flow:
  1. Bootstrap session, GET search page, extract ViewState/CSRF/source-id.
  2. POST empty-vendor search → first page of results + totalRecords.
  3. Loop DataTable pagination (event=page, _first=N*25) for all pages.
  4. Parse each <tr data-ri="N"> row → structured dict.
  5. Upsert into commbuys.db table `contracts`.
"""
import re
import sqlite3
import sys
import time
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
BASE = "https://www.commbuys.com"
URL = f"{BASE}/bso/view/search/external/advancedSearchContractBlanket.xhtml"
DB = "/Users/anthonyleung/playground2/commbuys.db"
PAGE_SIZE = 25
SLEEP = 1.2  # polite rate limit


# ---------- HTTP helpers ----------

def new_session():
    s = requests.Session()
    s.headers["User-Agent"] = UA
    s.get(f"{BASE}/bso/")
    return s


def extract_tokens(html):
    vs = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', html).group(1)
    csrf = re.search(r'name="_csrf"[^>]*value="([^"]+)"', html).group(1)
    m = re.search(r'searchNew = function.*?s:"([^"]+)",f:"([^"]+)",u:"([^"]+)"', html)
    return vs, csrf, m.group(1), m.group(2), m.group(3)


def update_viewstate(xml, current):
    m = re.search(r'<update id="j_id1:javax\.faces\.ViewState:0"><!\[CDATA\[([^\]]+)\]\]>', xml)
    return m.group(1) if m else current


AJAX_HEADERS = {
    "Faces-Request": "partial/ajax",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
    "Referer": URL,
    "Origin": BASE,
    "Accept": "application/xml, text/xml, */*; q=0.01",
}


def initial_search(s, vs, csrf, source_id, form_id, update_ids):
    """Submit empty-vendor search, return response XML."""
    payload = {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": source_id,
        "javax.faces.partial.execute": form_id,
        "javax.faces.partial.render": update_ids,
        source_id: source_id,
        form_id: form_id,
        f"{form_id}:poNbr": "",
        f"{form_id}:alternateId": "",
        f"{form_id}:desc": "",
        f"{form_id}:vendorName": "",
        f"{form_id}:organization_focus": "",
        f"{form_id}:organization_input": "",
        f"{form_id}:departmentPrefix_focus": "",
        f"{form_id}:departmentPrefix_input": "",
        f"{form_id}:buyer_focus": "",
        f"{form_id}:buyer_input": "",
        f"{form_id}:bidNbr": "",
        f"{form_id}:expireFromDate_input": "",
        f"{form_id}:expireToDate_input": "",
        f"{form_id}:typeCode_focus": "",
        f"{form_id}:typeCode_input": "",
        f"{form_id}:itemDesc": "",
        f"{form_id}:categoryId_focus": "",
        f"{form_id}:categoryId_input": "",
        f"{form_id}:classId_focus": "",
        f"{form_id}:classId_input": "",
        f"{form_id}:classItemId_focus": "",
        f"{form_id}:classItemId_input": "",
        f"{form_id}:includeExpired_input": "on",
        "_csrf": csrf,
        "javax.faces.ViewState": vs,
    }
    r = s.post(URL, data=payload, headers=AJAX_HEADERS)
    r.raise_for_status()
    return r.text


def page_request(s, vs, csrf, first):
    """Request a specific offset via DataTable pagination."""
    dt = "contractBlanketSearchResultsForm:contractResultId"
    payload = {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": dt,
        "javax.faces.partial.execute": dt,
        "javax.faces.partial.render": dt,
        "javax.faces.behavior.event": "page",
        "javax.faces.partial.event": "page",
        f"{dt}_pagination": "true",
        f"{dt}_first": str(first),
        f"{dt}_rows": str(PAGE_SIZE),
        f"{dt}_encodeFeature": "true",
        f"{dt}_skipChildren": "true",
        "contractBlanketSearchResultsForm": "contractBlanketSearchResultsForm",
        "_csrf": csrf,
        "javax.faces.ViewState": vs,
    }
    r = s.post(URL, data=payload, headers=AJAX_HEADERS)
    r.raise_for_status()
    return r.text


# ---------- Parsing ----------

def extract_results_html(xml):
    """Pull the CDATA body out of the DataTable update."""
    # Full-page update after initial search
    m = re.search(r'<update id="advSearchResults"><!\[CDATA\[(.*?)\]\]></update>', xml, re.S)
    if m:
        return m.group(1)
    # DataTable pagination update
    m = re.search(r'<update id="contractBlanketSearchResultsForm:contractResultId"><!\[CDATA\[(.*?)\]\]></update>', xml, re.S)
    return m.group(1) if m else ""


def extract_total(xml):
    m = re.search(r'"totalRecords":(\d+)', xml)
    if m:
        return int(m.group(1))
    m = re.search(r'\d[\d,]*\s*-\s*\d[\d,]*\s*of\s*(\d[\d,]*)', xml)
    return int(m.group(1).replace(",", "")) if m else None


def parse_rows(html):
    """Parse <tr data-ri="N"> rows → list of dicts."""
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for tr in soup.find_all("tr", attrs={"data-ri": True}):
        cells = tr.find_all("td")
        if len(cells) < 12:
            continue

        def ctext(i):
            return cells[i].get_text(" ", strip=True) if i < len(cells) else ""

        # Columns: 0=blanket_select, 1=blanket#(link), 2=bid#(link), 3=bid_solicitation,
        # 4=description, 5=vendor, 6=type_code, 7=dollars, 8=org, 9=status, 10=begin, 11=end
        blanket_link = cells[1].find("a")
        bid_link = cells[2].find("a")
        blanket_id = blanket_link.get_text(strip=True) if blanket_link else ctext(1)
        blanket_href = blanket_link.get("href", "") if blanket_link else ""
        bid_id = bid_link.get_text(strip=True) if bid_link else ctext(2)
        bid_href = bid_link.get("href", "") if bid_link else ""

        rows.append({
            "data_ri": int(tr["data-ri"]),
            "blanket_id": blanket_id,
            "blanket_url": blanket_href,
            "bid_id": bid_id,
            "bid_url": bid_href,
            "description": ctext(4),
            "vendor": ctext(5),
            "type_code": ctext(6),
            "dollars_spent": ctext(7),
            "organization": ctext(8),
            "status": ctext(9),
            "begin_date": ctext(10),
            "end_date": ctext(11),
        })
    return rows


# ---------- DB ----------

def init_db():
    conn = sqlite3.connect(DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS contracts (
            blanket_id TEXT PRIMARY KEY,
            blanket_url TEXT,
            bid_id TEXT,
            bid_url TEXT,
            description TEXT,
            vendor TEXT,
            type_code TEXT,
            dollars_spent TEXT,
            organization TEXT,
            status TEXT,
            begin_date TEXT,
            end_date TEXT,
            scraped_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vendor ON contracts(vendor)")
    conn.commit()
    return conn


def upsert(conn, rows):
    conn.executemany("""
        INSERT INTO contracts
            (blanket_id, blanket_url, bid_id, bid_url, description, vendor,
             type_code, dollars_spent, organization, status, begin_date, end_date)
        VALUES (:blanket_id,:blanket_url,:bid_id,:bid_url,:description,:vendor,
                :type_code,:dollars_spent,:organization,:status,:begin_date,:end_date)
        ON CONFLICT(blanket_id) DO UPDATE SET
            bid_id=excluded.bid_id,
            description=excluded.description,
            vendor=excluded.vendor,
            type_code=excluded.type_code,
            dollars_spent=excluded.dollars_spent,
            organization=excluded.organization,
            status=excluded.status,
            begin_date=excluded.begin_date,
            end_date=excluded.end_date,
            scraped_at=CURRENT_TIMESTAMP
    """, rows)
    conn.commit()


# ---------- Main ----------

def main():
    limit_pages = int(sys.argv[1]) if len(sys.argv) > 1 else None  # optional cap for testing

    conn = init_db()
    s = new_session()
    r = s.get(URL)
    vs, csrf, source_id, form_id, update_ids = extract_tokens(r.text)
    print(f"[bootstrap] form={form_id} source={source_id}", flush=True)

    xml = initial_search(s, vs, csrf, source_id, form_id, update_ids)
    vs = update_viewstate(xml, vs)
    total = extract_total(xml) or 0
    page1_html = extract_results_html(xml)
    rows = parse_rows(page1_html)
    print(f"[page 1] total={total} parsed={len(rows)}", flush=True)
    if rows:
        upsert(conn, rows)

    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    if limit_pages:
        total_pages = min(total_pages, limit_pages)

    def refresh_session():
        """Re-bootstrap and re-submit initial search so the ViewState covers pagination."""
        s2 = new_session()
        r2 = s2.get(URL)
        vs2, csrf2, src2, form2, upd2 = extract_tokens(r2.text)
        xml2 = initial_search(s2, vs2, csrf2, src2, form2, upd2)
        vs2 = update_viewstate(xml2, vs2)
        return s2, vs2, csrf2

    for p in range(2, total_pages + 1):
        first = (p - 1) * PAGE_SIZE
        time.sleep(SLEEP)
        try:
            xml = page_request(s, vs, csrf, first)
        except requests.HTTPError as e:
            print(f"[page {p}] HTTP error: {e}; resetting session", flush=True)
            s, vs, csrf = refresh_session()
            xml = page_request(s, vs, csrf, first)
        vs = update_viewstate(xml, vs)
        body = extract_results_html(xml)
        rows = parse_rows(body)
        if not rows:
            # Likely session timeout — refresh and retry ONCE.
            print(f"[page {p}] empty body (len={len(body)}) — refreshing session", flush=True)
            s, vs, csrf = refresh_session()
            xml = page_request(s, vs, csrf, first)
            vs = update_viewstate(xml, vs)
            body = extract_results_html(xml)
            rows = parse_rows(body)
            if not rows:
                print(f"[page {p}] still empty after refresh — stopping", flush=True)
                break
        upsert(conn, rows)
        print(f"[page {p}/{total_pages}] +{len(rows)} (offset={first})", flush=True)

    (count,) = conn.execute("SELECT COUNT(*) FROM contracts").fetchone()
    print(f"\n[done] contracts in db: {count}")
    conn.close()


if __name__ == "__main__":
    main()
