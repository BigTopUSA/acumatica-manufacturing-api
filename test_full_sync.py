"""Probe every manufacturing entity with a live access token to confirm
permissions, endpoint paths, and record shape before wiring up real auth."""
import json
import requests

from connector import ENTITIES, normalise_record, extract_last_modified

ACCESS_TOKEN = "4OSbtgj0CXi5ywwz-cYnAfqRW4pcypR5ADLFcKoJZ4Y"

with open("configuration.json") as f:
    cfg = json.load(f)

base = cfg["acumatica_url"].rstrip("/")
api_version = cfg.get("api_version", "24.200.001")
base_url = f"{base}/entity/MANUFACTURING/{api_version}"

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json",
})

print(f"Base: {base_url}\n")

results = []
for entity in ENTITIES:
    if "_parent" in entity:  # detail entities come from parent $expand, skip top-level probe
        continue
    name = entity["name"]
    endpoint = entity["endpoint"]
    expand = entity.get("expand")

    params = {"$top": 3}
    if expand:
        params["$expand"] = expand

    url = f"{base_url}/{endpoint}"
    try:
        r = session.get(url, params=params, timeout=60)
    except Exception as e:
        results.append((name, "ERR", str(e)[:120], 0, 0))
        continue

    if r.status_code != 200:
        results.append((name, r.status_code, r.text[:140], 0, 0))
        continue

    data = r.json()
    records = data if isinstance(data, list) else data.get("value", [])
    child_count = 0
    if expand and records:
        child_count = sum(len(rec.get(expand, []) or []) for rec in records)

    # Sanity: flatten one record so we know normalise_record doesn't blow up
    sample_cols = 0
    sample_lm = None
    if records:
        flat = normalise_record(records[0])
        sample_cols = len(flat)
        sample_lm = extract_last_modified(records[0])

    results.append((name, r.status_code, "", len(records), child_count, sample_cols, sample_lm))

print(f"{'entity':<28} {'status':<8} {'rows':<5} {'children':<9} {'cols':<5} lastmod")
print("-" * 100)
for row in results:
    if len(row) == 5:
        name, status, err, rows, children = row
        print(f"{name:<28} {status!s:<8} {rows:<5} {children:<9} —     {err}")
    else:
        name, status, _, rows, children, cols, lm = row
        print(f"{name:<28} {status!s:<8} {rows:<5} {children:<9} {cols:<5} {lm or '—'}")
