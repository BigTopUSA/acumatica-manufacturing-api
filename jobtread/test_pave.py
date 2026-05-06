"""
Pave API discovery script. Run this BEFORE editing the ENTITIES list in
connector.py — it confirms which collections actually exist on your
organization, what scalar fields each one exposes, and which pagination
shape Pave uses.

Usage:
    1. Fill in configuration.json with your grantKey + organization_id
    2. python test_pave.py
    3. Use the output to update ENTITIES in connector.py
"""
import json
import requests

PAVE_URL = "https://api.jobtread.com/pave"

with open("configuration.json") as f:
    cfg = json.load(f)
GRANT_KEY = cfg["grant_key"]
ORG_ID = cfg["organization_id"]


HEADERS = {"Authorization": f"Bearer {GRANT_KEY}"}


def pave(query: dict) -> dict:
    r = requests.post(PAVE_URL, json=query, headers=HEADERS, timeout=30)
    try:
        body = r.json() if r.text else {}
    except Exception:
        body = {"_raw_text": r.text}
    return {"status": r.status_code, "body": body}


def hr(s: str) -> None:
    print(f"\n{'='*8} {s} {'='*8}")


# ---------------------------------------------------------------------------
# 1. Confirm grantKey + org auth resolves
# ---------------------------------------------------------------------------
hr("auth check — fetch organization id+name only")
print(pave({
    "query": {"organization": {"$": {"id": ORG_ID},
                                "id": {}, "name": {}}}
}))


# ---------------------------------------------------------------------------
# 2. Probe collection field names by asking for invalid ones — Pave's error
#    messages typically suggest the closest valid field.
# ---------------------------------------------------------------------------
hr("collection field probes (look at error suggestions)")
for guess in ["jobs", "customers", "vendors", "tasks", "documents",
              "users", "templates", "schedules", "comments",
              "lineItems", "costItems", "dailyLogs", "files"]:
    r = pave({
        "query": {"organization": {
            "$": {"id": ORG_ID},
            guess: {"$": {"size": 1, "page": 1}, "nodes": {"id": {}}},
        }}
    })
    msg = r["body"].get("errors") or r["body"]
    print(f"  {guess}: {str(msg)[:200]}")


# ---------------------------------------------------------------------------
# 3. Try a single-record introspection on a known-good collection
#    (replace 'jobs' below with whichever name worked above)
# ---------------------------------------------------------------------------
hr("scalar field discovery on 'jobs' (replace if jobs isn't valid)")
# Ask for invalid field — the error suggests valid sibling field names
r = pave({
    "query": {"organization": {
        "$": {"id": ORG_ID},
        "jobs": {"$": {"size": 1, "page": 1},
                 "nodes": {"___invalid___": {}}},
    }}
})
print(r["body"])


# ---------------------------------------------------------------------------
# 4. Pagination probe — try alternate arg names
# ---------------------------------------------------------------------------
hr("pagination shape probe")
for args in [
    {"size": 1, "page": 1},
    {"first": 1},
    {"limit": 1, "offset": 0},
    {"first": 1, "after": ""},
]:
    r = pave({
        "query": {"organization": {
            "$": {"id": ORG_ID},
            "jobs": {"$": args, "nodes": {"id": {}}},
        }}
    })
    err = r["body"].get("errors")
    status = "OK" if not err else f"REJECTED: {str(err)[:120]}"
    print(f"  args={args}: {status}")
