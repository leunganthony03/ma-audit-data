"""Probe 5: empty search to count total contracts + test pagination."""
import re
import requests

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
BASE = "https://www.commbuys.com"
URL = f"{BASE}/bso/view/search/external/advancedSearchContractBlanket.xhtml"


def do_search(s, vendor_name=""):
    r = s.get(URL)
    html = r.text
    vs = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', html).group(1)
    csrf = re.search(r'name="_csrf"[^>]*value="([^"]+)"', html).group(1)
    m = re.search(r'searchNew = function.*?s:"([^"]+)",f:"([^"]+)",u:"([^"]+)"', html)
    source_id, form_id, update_ids = m.group(1), m.group(2), m.group(3)

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
        f"{form_id}:vendorName": vendor_name,
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
        "_csrf": csrf,
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
    return r2.text


def parse_total(xml):
    m = re.search(r'(\d[\d,]*)\s*-\s*(\d[\d,]*)\s*of\s*(\d[\d,]*)', xml)
    return m.group(0) if m else None


def main():
    s = requests.Session()
    s.headers["User-Agent"] = UA
    s.get(f"{BASE}/bso/")

    for vn in ["", "A", "DELOITTE", "SMITH"]:
        xml = do_search(s, vn)
        tot = parse_total(xml)
        # count rows
        m = re.search(r'<update id="advSearchResults"><!\[CDATA\[(.*?)\]\]></update>', xml, re.S)
        rows = len(re.findall(r'<tr[^>]*role="row"', m.group(1))) if m else 0
        # Also check for error
        err = "error" in xml.lower() or "messages-error" in xml
        print(f"vendor={vn!r}: paginator={tot} row_role_count={rows} bytes={len(xml)} err={err}")


if __name__ == "__main__":
    main()
