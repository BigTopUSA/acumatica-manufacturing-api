"""Probe every OAuth grant type to see which one this client is actually allowed to use."""
import json
import requests

with open("configuration.json") as f:
    cfg = json.load(f)

base = cfg["acumatica_url"].rstrip("/")
token_url = f"{base}/t/production/identity/connect/token"
cid = cfg["client_id"]
csec = cfg["client_secret"]


def attempt(label, data):
    print(f"\n--- {label} ---")
    r = requests.post(token_url, data=data, timeout=30)
    print(f"  HTTP {r.status_code}: {r.text[:300]}")


# Client credentials (what connector.py uses today)
attempt("client_credentials", {
    "grant_type": "client_credentials",
    "client_id": cid, "client_secret": csec,
    "scope": "api",
})

# Password grant — deliberately blank user/pass so the server tells us if the grant
# itself is allowed (diff error = grant allowed; same error = grant not allowed)
attempt("password (blank creds, probing grant permission)", {
    "grant_type": "password",
    "client_id": cid, "client_secret": csec,
    "username": "", "password": "",
    "scope": "api",
})

# Authorization code — missing code, same probe intent
attempt("authorization_code (missing code, probing grant permission)", {
    "grant_type": "authorization_code",
    "client_id": cid, "client_secret": csec,
    "code": "x", "redirect_uri": "http://localhost",
})

# Try client_id without @Production suffix
if "@" in cid:
    plain_cid = cid.split("@")[0]
    attempt(f"client_credentials with stripped client_id ({plain_cid})", {
        "grant_type": "client_credentials",
        "client_id": plain_cid, "client_secret": csec,
        "scope": "api",
    })
