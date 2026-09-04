"""Re-seed the Acumatica refresh token after the chain breaks (invalid_grant).

The refresh tokens are single-use; when both the Fivetran-state token and the
configuration.json bootstrap token are dead, someone has to re-authorize
interactively. This script makes that a two-step paste instead of the full
Postman dance:

  1. `python reseed.py`
     Prints the authorize URL. Open it in a browser, log in to Acumatica,
     and copy the full URL you land on (it contains ?code=...).

  2. `python reseed.py "<redirected-url-or-code>"`
     Exchanges the code and writes the new refresh_token into
     configuration.json. Never prints the token itself.

Then redeploy:  fivetran deploy ... --force  (see deploy.log for the shape).

Works for either connector — run it from the repo root for manufacturing, or
pass --config default-extras/configuration.json for the extras connector.
"""
import argparse
import json
import sys
from urllib.parse import quote, urlparse, parse_qs

import requests

DEFAULT_REDIRECT = "https://oauth.pstmn.io/v1/callback"


def load_cfg(path):
    with open(path) as f:
        return json.load(f)


def authorize_url(cfg, redirect_uri):
    base = cfg["acumatica_url"].rstrip("/")
    return (
        f"{base}/identity/connect/authorize"
        f"?response_type=code"
        f"&client_id={quote(cfg['client_id'])}"
        f"&redirect_uri={quote(redirect_uri, safe='')}"
        f"&scope={quote('api offline_access')}"
    )


def extract_code(arg):
    if arg.startswith("http"):
        qs = parse_qs(urlparse(arg).query)
        code = qs.get("code", [None])[0]
        if not code:
            sys.exit("No ?code= found in that URL — paste the full redirected URL.")
        return code
    return arg  # assume it's the bare code


def exchange(cfg_path, cfg, code, redirect_uri):
    base = cfg["acumatica_url"].rstrip("/")
    resp = requests.post(
        f"{base}/identity/connect/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
        },
        timeout=30,
    )
    if not resp.ok:
        # Error bodies here are safe to show (no tokens on failure)
        sys.exit(f"Token exchange failed: HTTP {resp.status_code}: {resp.text[:300]}")
    data = resp.json()
    refresh = data.get("refresh_token")
    if not refresh:
        sys.exit(
            "No refresh_token in response — was the 'offline_access' scope "
            "granted? Check the Acumatica connected app allows it."
        )
    cfg["refresh_token"] = refresh
    with open(cfg_path, "w") as f:
        json.dump(cfg, f, indent=2)
        f.write("\n")
    print(f"OK — refresh token (…{refresh[-6:]}) written to {cfg_path}.")
    print("Now redeploy so the connector picks it up (config token supersedes state).")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("code", nargs="?", help="redirected URL (with ?code=...) or bare code")
    ap.add_argument("--config", default="configuration.json")
    ap.add_argument("--redirect-uri", default=DEFAULT_REDIRECT)
    args = ap.parse_args()

    cfg = load_cfg(args.config)
    if not args.code:
        print("Open this in a browser, log in, then re-run with the URL you land on:\n")
        print(authorize_url(cfg, args.redirect_uri))
        return
    exchange(args.config, cfg, extract_code(args.code), args.redirect_uri)


if __name__ == "__main__":
    main()
