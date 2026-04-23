"""
Fivetran Connector SDK — Acumatica Manufacturing Endpoint
Syncs all manufacturing entities via full refresh.

Note: this endpoint does NOT expose LastModifiedDateTime, so incremental
sync by cursor is not possible. Every sync is a full refresh.
"""

import requests
import json
from typing import Generator

from fivetran_connector_sdk import Connector, Operations as op, Logging as log

# ---------------------------------------------------------------------------
# Entity definitions
# ---------------------------------------------------------------------------
# Each parent entry may declare a child collection that comes back via $expand.
# Acumatica's globally-unique `id` is used as the primary key everywhere — it's
# reliable, present on every record, and avoids PK typos against business keys.
# ---------------------------------------------------------------------------
ENTITIES = [
    {
        "name": "bill_of_material",
        "endpoint": "BillOfMaterial",
        "expand": "Operations",
        "child_table": "bill_of_material_operation",
        "child_key": "Operations",
    },
    {
        "name": "production_order",
        "endpoint": "ProductionOrder",
        "expand": None,
    },
    {
        "name": "production_order_detail",
        "endpoint": "ProductionOrderDetail",
        "expand": None,
    },
    {
        "name": "labor_entry",
        "endpoint": "LaborEntry",
        "expand": "Details",
        "child_table": "labor_entry_detail",
        "child_key": "Details",
    },
    {
        "name": "material_entry",
        "endpoint": "MaterialEntry",
        "expand": "Details",
        "child_table": "material_entry_detail",
        "child_key": "Details",
    },
    {
        "name": "work_center",
        "endpoint": "WorkCenter",
        "expand": None,
    },
    {
        "name": "machine",
        "endpoint": "Machine",
        "expand": None,
    },
    {
        "name": "shift",
        "endpoint": "Shift",
        "expand": None,
    },
]

# All tables keyed by Acumatica's globally-unique `id` guid
PRIMARY_KEY = ["id"]

PAGE_SIZE = 100


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_token(cfg: dict) -> str:
    """
    Return an OAuth access token.

    Priority:
      1. Static `access_token` in config (for local debug with a Postman-issued token).
      2. client_credentials grant against the tenant's IdentityServer.
    """
    static = cfg.get("access_token")
    if static:
        log.info("Using static access_token from configuration")
        return static

    token_url = f"{cfg['acumatica_url'].rstrip('/')}/identity/connect/token"
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
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise ValueError(f"No access_token in response: {resp.text[:200]}")
    log.info("OAuth token acquired successfully")
    return token


def build_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_page(session, base_url, endpoint, skip, expand):
    params = {"$top": PAGE_SIZE, "$skip": skip}
    if expand:
        params["$expand"] = expand
    url = f"{base_url}/{endpoint}"
    resp = session.get(url, params=params, timeout=60)
    if resp.status_code == 404:
        log.warning(f"Endpoint not found (404): {endpoint} — skipping")
        return []
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "value" in data:
        return data["value"]
    return []


def fetch_all_pages(session, base_url, endpoint, expand) -> Generator[dict, None, None]:
    skip = 0
    while True:
        page = fetch_page(session, base_url, endpoint, skip, expand)
        if not page:
            break
        for record in page:
            yield record
        if len(page) < PAGE_SIZE:
            break
        skip += PAGE_SIZE


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

def flatten_value(v):
    """Acumatica wraps most field values as {"value": X}. Unwrap to scalar."""
    if isinstance(v, dict) and "value" in v and len(v) == 1:
        return v["value"]
    if isinstance(v, dict):
        return json.dumps(v) if v else None
    return v


def normalise_record(raw: dict) -> dict:
    """Flatten an Acumatica record. Drops child collections and _links metadata."""
    out = {}
    for k, v in raw.items():
        if k == "_links":
            continue
        if isinstance(v, list):
            continue  # children handled separately
        out[k] = flatten_value(v)
    return out


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

def sync_entity(session, base_url, entity, state) -> Generator:
    name = entity["name"]
    endpoint = entity["endpoint"]
    expand = entity.get("expand")
    child_key = entity.get("child_key")
    child_table = entity.get("child_table")

    log.info(f"Syncing {name} (full refresh)")
    parent_count = 0
    child_count = 0

    for raw in fetch_all_pages(session, base_url, endpoint, expand):
        yield op.upsert(name, normalise_record(raw))
        parent_count += 1

        if child_key and child_table:
            parent_id = raw.get("id")
            for child in raw.get(child_key, []) or []:
                child_row = normalise_record(child)
                # Tie children back to the parent even if the business keys don't
                child_row.setdefault("parent_id", parent_id)
                yield op.upsert(child_table, child_row)
                child_count += 1

    log.info(f"  → {parent_count} {name} rows" + (f", {child_count} {child_table} rows" if child_table else ""))
    yield op.checkpoint(state)


# ---------------------------------------------------------------------------
# Fivetran entrypoints
# ---------------------------------------------------------------------------

def schema(configuration: dict):
    tables = []
    for entity in ENTITIES:
        tables.append({"table": entity["name"], "primary_key": PRIMARY_KEY})
        if entity.get("child_table"):
            tables.append({"table": entity["child_table"], "primary_key": PRIMARY_KEY})
    return tables


def update(configuration: dict, state: dict):
    acumatica_url = configuration["acumatica_url"].rstrip("/")
    api_version = configuration.get("api_version", "24.200.001")
    base_url = f"{acumatica_url}/entity/MANUFACTURING/{api_version}"

    log.info(f"Connecting to Acumatica Manufacturing API: {base_url}")

    token = get_token(configuration)
    session = requests.Session()
    session.headers.update(build_headers(token))

    for entity in ENTITIES:
        try:
            yield from sync_entity(session, base_url, entity, state)
        except requests.exceptions.HTTPError as e:
            log.severe(f"HTTP error syncing {entity['name']}: {e}")
            raise
        except Exception as e:
            log.severe(f"Unexpected error syncing {entity['name']}: {e}")
            raise

    log.info("Manufacturing sync complete.")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json") as f:
        _cfg = json.load(f)
    connector.debug(configuration=_cfg)
