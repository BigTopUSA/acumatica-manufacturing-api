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
        # Materials (AMBomMatl) are nested one level under each operation, so we
        # expand both the operations and their material lines in one call.
        "expand": "Operations,Operations/Material",
        "child_table": "bill_of_material_operation",
        "child_key": "Operations",
        # Grandchild: each operation's Material[] → bill_of_material_material.
        "grandchild_key": "Material",
        "grandchild_table": "bill_of_material_material",
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

def get_token(cfg: dict, state: dict | None = None) -> str:
    """
    Return an OAuth access token.

    Priority:
      1. refresh_token grant — production headless path. Refresh token is read
         from state first (most recent), then config (initial bootstrap).
      2. Static `access_token` in config — one-shot debug only.
      3. client_credentials grant — requires that grant enabled on the client.

    If the refresh_token rotates, the new value is written back to `state` so
    Fivetran persists it via the next op.checkpoint().
    """
    token_url = f"{cfg['acumatica_url'].rstrip('/')}/identity/connect/token"

    refresh = (state or {}).get("refresh_token") or cfg.get("refresh_token")
    if refresh:
        log.info("Using refresh_token grant")
        resp = requests.post(
            token_url,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh,
                "client_id": cfg["client_id"],
                "client_secret": cfg["client_secret"],
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        new_refresh = data.get("refresh_token")
        if new_refresh and new_refresh != refresh and state is not None:
            state["refresh_token"] = new_refresh
            # Full value is logged here ONLY because refresh tokens are single-use and,
            # if the connector crashes before the state checkpoint lands, manually
            # recovering the rotated token is the only way to avoid a full re-auth.
            log.info(f"Refresh token rotated; new value: {new_refresh}")
        return data["access_token"]

    static = cfg.get("access_token")
    if static:
        log.info("Using static access_token from configuration")
        return static

    log.info("Falling back to client_credentials grant")
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

def normalise_record(raw: dict, prefix: str = "") -> dict:
    """
    Flatten an Acumatica record into a flat dict of scalar columns.

    - {"value": X} wrappers are unwrapped to the scalar.
    - Nested complex objects (e.g. MainContact, Address) are recursively
      flattened with underscore-joined keys: MainContact_Address_City.
    - List values are skipped — child collections are handled separately.
    - _links metadata is dropped.
    """
    out = {}
    for k, v in raw.items():
        if k == "_links":
            continue
        if isinstance(v, list):
            continue
        col = f"{prefix}{k}" if prefix else k

        if isinstance(v, dict) and len(v) == 1 and "value" in v:
            out[col] = v["value"]
        elif isinstance(v, dict) and v:
            out.update(normalise_record(v, prefix=f"{col}_"))
        elif isinstance(v, dict):
            out[col] = None
        else:
            out[col] = v
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
    grandchild_key = entity.get("grandchild_key")
    grandchild_table = entity.get("grandchild_table")

    log.info(f"Syncing {name} (full refresh)")
    parent_count = 0
    child_count = 0
    grandchild_count = 0

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

                # Grandchildren (e.g. BOM operation → material lines). Tag with
                # both the top-level parent id and the immediate child's id.
                if grandchild_key and grandchild_table:
                    child_id = child.get("id")
                    for grandchild in child.get(grandchild_key, []) or []:
                        gc_row = normalise_record(grandchild)
                        gc_row.setdefault("parent_id", parent_id)
                        gc_row.setdefault("operation_id", child_id)
                        yield op.upsert(grandchild_table, gc_row)
                        grandchild_count += 1

    log.info(
        f"  → {parent_count} {name} rows"
        + (f", {child_count} {child_table} rows" if child_table else "")
        + (f", {grandchild_count} {grandchild_table} rows" if grandchild_table else "")
    )
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
        if entity.get("grandchild_table"):
            tables.append({"table": entity["grandchild_table"], "primary_key": PRIMARY_KEY})
    return tables


def update(configuration: dict, state: dict):
    acumatica_url = configuration["acumatica_url"].rstrip("/")
    api_version = configuration.get("api_version", "24.200.001")
    base_url = f"{acumatica_url}/entity/MANUFACTURING/{api_version}"

    log.info(f"Connecting to Acumatica Manufacturing API: {base_url}")

    token = get_token(configuration, state)
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
