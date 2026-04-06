"""Probe 3: proper PrimeFaces AJAX POST to btnPoSearch."""
import re
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
BASE = "https://www.commbuys.com"
URL = f"{BASE}/bso/view/search/external/advancedSearchContractBlanket.xhtml"


def get_vs(html):
    m = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', html)
    return m.group(1) if m else None


def main():
    s = requests.Session()
    s.headers["User-Agent"] = UA
    s.get(f"{BASE}/bso/")
    r = s.get(URL)
    vs = get_vs(r.text)
    csrf_m = re.search(r'name="_csrf"[^>]*value="([^"]+)"', r.text)
    csrf = csrf_m.group(1) if csrf_m else None
    print("GET", r.status_code, "vs=", vs[:30], "csrf=", csrf[:30] if csrf else None)

    # Proper PrimeFaces AJAX payload
    payload = {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": "contractBlanketSearchForm:btnPoSearch",
        "javax.faces.partial.execute": "contractBlanketSearchForm",
        "javax.faces.partial.render": "contractBlanketSearchForm",
        "contractBlanketSearchForm:btnPoSearch": "contractBlanketSearchForm:btnPoSearch",
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
        "_csrf": csrf or "",
        "javax.faces.ViewState": vs,
    }
    headers = {
        "Faces-Request": "partial/ajax",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "Referer": URL,
        "Origin": BASE,
        "Accept": "application/xml, text/xml, */*; q=0.01",
    }
    r2 = s.post(URL, data=payload, headers=headers)
    print("POST", r2.status_code, len(r2.text))
    with open("/tmp/ajax_search.xml", "w") as f:
        f.write(r2.text)
    print(r2.text[:2000])


if __name__ == "__main__":
    main()
