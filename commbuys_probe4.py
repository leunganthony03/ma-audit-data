"""Probe 4: use the real searchNew() source ID extracted from the page."""
import re
import requests

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
BASE = "https://www.commbuys.com"
URL = f"{BASE}/bso/view/search/external/advancedSearchContractBlanket.xhtml"


def main():
    s = requests.Session()
    s.headers["User-Agent"] = UA
    s.get(f"{BASE}/bso/")
    r = s.get(URL)
    html = r.text
    vs = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', html).group(1)
    csrf = re.search(r'name="_csrf"[^>]*value="([^"]+)"', html).group(1)
    # Dynamic source id from searchNew function
    m = re.search(r'searchNew = function.*?s:"([^"]+)",f:"([^"]+)",u:"([^"]+)"', html)
    source_id, form_id, update_ids = m.group(1), m.group(2), m.group(3)
    print(f"source_id={source_id}\nform_id={form_id}\nupdate_ids={update_ids}")

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
        f"{form_id}:vendorName": "DELOITTE",
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
    print(f"POST status={r2.status_code} bytes={len(r2.text)}")
    with open("/tmp/search_deloitte.xml", "w") as f:
        f.write(r2.text)
    # Count visible table rows in the response
    update_ids_list = update_ids.split()
    for uid in update_ids_list:
        mm = re.search(r'<update id="' + re.escape(uid) + r'"><!\[CDATA\[(.*?)\]\]></update>', r2.text, re.S)
        if mm:
            body = mm.group(1)
            trs = re.findall(r'<tr[^>]*>', body)
            print(f"  update[{uid}]: {len(body)} bytes, {len(trs)} <tr> tags")


if __name__ == "__main__":
    main()
