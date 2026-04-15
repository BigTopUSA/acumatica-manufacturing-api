"""Auth test against the tenant-scoped token endpoint."""
import json
import requests

with open("configuration.json") as f:
    cfg = json.load(f)

# Tenant-scoped endpoint (per the Client Application Access page)
token_url = f"{cfg['acumatica_url'].rstrip('/')}/t/production/identity/connect/token"
print(f"POST {token_url}")

resp = requests.post(
    token_url,
    data={
        "grant_type": "client_credentials",
        "client_id": cfg["client_id"],
        "client_secret": cfg["client_secret"],
        "scope": "api",
    },
    timeout=30,
)
print(f"HTTP {resp.status_code}: {resp.text[:400]}")

if resp.status_code == 200:
    token = resp.json().get("access_token")
    print(f"\nSUCCESS — token length {len(token)}")

    # Sanity ping against the Manufacturing endpoint
    api_version = cfg.get("api_version", "24.200.001")
    base = f"{cfg['acumatica_url'].rstrip('/')}/entity/Manufacturing/{api_version}"
    print(f"\nGET {base}/BillOfMaterial?$top=1")
    r2 = requests.get(
        f"{base}/BillOfMaterial",
        params={"$top": 1},
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=30,
    )
    print(f"  HTTP {r2.status_code}")
    print(f"  Body (first 400 chars): {r2.text[:400]}")
