"""Probe 2: submit an empty contract blanket search to see total results + pagination."""
import re
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
BASE = "https://www.commbuys.com"
SEARCH_URL = f"{BASE}/bso/view/search/external/advancedSearchContractBlanket.xhtml"


def get_viewstate(html):
    m = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', html)
    return m.group(1) if m else None


def form_hidden_fields(soup, form_id):
    form = soup.find("form", id=form_id)
    if not form:
        return {}
    out = {}
    for inp in form.find_all("input", type="hidden"):
        n = inp.get("name")
        if n:
            out[n] = inp.get("value", "")
    return out


def main():
    s = requests.Session()
    s.headers["User-Agent"] = UA
    s.get(f"{BASE}/bso/")
    r = s.get(SEARCH_URL)
    print("GET", r.status_code, len(r.text))
    vs = get_viewstate(r.text)
    print("ViewState:", vs[:40] if vs else None)

    soup = BeautifulSoup(r.text, "lxml")
    hidden = form_hidden_fields(soup, "contractBlanketSearchForm")
    print("hidden fields:", list(hidden.keys()))

    # Build payload for a broad search (empty vendor name, include expired = unchecked = active only)
    payload = {
        "contractBlanketSearchForm": "contractBlanketSearchForm",
        "contractBlanketSearchForm:poNbr": "",
        "contractBlanketSearchForm:alternateId": "",
        "contractBlanketSearchForm:desc": "",
        "contractBlanketSearchForm:vendorName": "",
        "contractBlanketSearchForm:organization_focus": "",
        "contractBlanketSearchForm:organization_input": "",
        "contractBlanketSearchForm:departmentPrefix_focus": "",
        "contractBlanketSearchForm:departmentPrefix_input": "",
        "contractBlanketSearchForm:buyer_focus": "",
        "contractBlanketSearchForm:buyer_input": "",
        "contractBlanketSearchForm:bidNbr": "",
        "contractBlanketSearchForm:expireFromDate_input": "",
        "contractBlanketSearchForm:expireToDate_input": "",
        "contractBlanketSearchForm:typeCode_focus": "",
        "contractBlanketSearchForm:typeCode_input": "",
        "contractBlanketSearchForm:itemDesc": "",
        "contractBlanketSearchForm:categoryId_focus": "",
        "contractBlanketSearchForm:categoryId_input": "",
        "contractBlanketSearchForm:classId_focus": "",
        "contractBlanketSearchForm:classId_input": "",
        "contractBlanketSearchForm:classItemId_focus": "",
        "contractBlanketSearchForm:classItemId_input": "",
        "contractBlanketSearchForm:btnPoSearch": "",
        "javax.faces.ViewState": vs,
    }
    payload.update(hidden)

    r2 = s.post(SEARCH_URL, data=payload, headers={"Referer": SEARCH_URL})
    print("POST", r2.status_code, len(r2.text))
    with open("/tmp/results.html", "w") as f:
        f.write(r2.text)

    soup2 = BeautifulSoup(r2.text, "lxml")
    # Look for results count + any data table
    body_text = soup2.get_text(" ", strip=True)
    m = re.search(r"(\d[\d,]*)\s+(?:records|results|contract)", body_text, re.I)
    if m:
        print("RESULTS FOUND:", m.group(0))
    # Find tables
    tables = soup2.find_all("table")
    print("tables on results page:", len(tables))
    # Any table with contract-like headers
    for t in tables:
        hdrs = [th.get_text(strip=True) for th in t.find_all("th")]
        if hdrs and any("contract" in h.lower() or "vendor" in h.lower() or "description" in h.lower() for h in hdrs):
            print("RESULT TABLE HEADERS:", hdrs)
            print("  rows:", len(t.find_all("tr")))
    # Print any "X of Y" paginator text
    for el in soup2.find_all(string=re.compile(r"\b\d+\s*of\s*\d+", re.I)):
        print("paginator:", el.strip())


if __name__ == "__main__":
    main()
