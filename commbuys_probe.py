"""
Probe COMMBUYS advanced search to capture the PrimeFaces AJAX handshake
needed to submit a CONTRACT_BLANKETS search.

Phase 1: reverse-engineering. We need to discover what fields appear
after the documentType dropdown change event fires.
"""
import re
import sys
import requests
from bs4 import BeautifulSoup

BASE = "https://www.commbuys.com"
ADV_URL = f"{BASE}/bso/view/search/external/advancedSearch.xhtml"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def get_viewstate(html: str) -> str:
    m = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', html)
    return m.group(1) if m else None


def extract_form_fields(html: str, form_prefix: str = "advancedSearchForm"):
    """Return all input/select/textarea fields under a JSF form by name."""
    soup = BeautifulSoup(html, "lxml")
    out = {}
    for tag in soup.find_all(["input", "select", "textarea"]):
        name = tag.get("name", "")
        if not name or not name.startswith(form_prefix + ":"):
            continue
        if tag.name == "select":
            # grab the selected option or first option
            selected = tag.find("option", selected=True)
            if selected:
                out[name] = selected.get("value", "")
            else:
                opts = tag.find_all("option")
                out[name] = opts[0].get("value", "") if opts else ""
        else:
            out[name] = tag.get("value", "")
    return out


def main():
    s = new_session()

    # Step 1: GET advanced search page
    print("[1] GET advancedSearch.xhtml")
    r = s.get(ADV_URL)
    print(f"    status={r.status_code} bytes={len(r.text)}")
    vs = get_viewstate(r.text)
    print(f"    ViewState: {vs[:50] if vs else None}...")

    # Find all primefaces widgets on the page - look for documentTypeSelect widget id
    m = re.search(r'PrimeFaces\.cw\("SelectOneMenu","([^"]+)".*?id:"(advancedSearchForm:documentTypeSelect)"', r.text, re.S)
    print(f"    select widget: {m.group(1) if m else 'NOT FOUND'}")

    # Step 2: simulate the onchange AJAX event for documentTypeSelect = CONTRACT_BLANKETS
    # JSF/PrimeFaces AJAX POST uses these standard fields:
    #   javax.faces.partial.ajax = true
    #   javax.faces.source = advancedSearchForm:documentTypeSelect
    #   javax.faces.partial.execute = advancedSearchForm:documentTypeSelect
    #   javax.faces.partial.render = ... (determined by page)
    #   javax.faces.behavior.event = valueChange  (or change)
    #   javax.faces.partial.event = change
    # Plus the form's hidden fields.
    print("\n[2] POST AJAX change documentTypeSelect=CONTRACT_BLANKETS")
    payload = {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": "advancedSearchForm:documentTypeSelect",
        "javax.faces.partial.execute": "advancedSearchForm:documentTypeSelect",
        "javax.faces.partial.render": "advancedSearchForm",
        "javax.faces.behavior.event": "change",
        "javax.faces.partial.event": "change",
        "advancedSearchForm": "advancedSearchForm",
        "advancedSearchForm:documentTypeSelect_focus": "",
        "advancedSearchForm:documentTypeSelect_input": "CONTRACT_BLANKETS",
        "javax.faces.ViewState": vs,
    }
    headers = {
        "Faces-Request": "partial/ajax",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "Origin": BASE,
        "Referer": ADV_URL,
    }
    r2 = s.post(ADV_URL, data=payload, headers=headers)
    print(f"    status={r2.status_code} bytes={len(r2.text)}")
    # print first 600 chars
    print("    --- response head ---")
    print(r2.text[:1200])
    print("    --- end ---")

    # Save for inspection
    with open("/tmp/commbuys_ajax_response.xml", "w") as f:
        f.write(r2.text)
    print("\nSaved full AJAX response to /tmp/commbuys_ajax_response.xml")


if __name__ == "__main__":
    main()
