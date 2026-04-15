"""
Fivetran Connector SDK — Acumatica Manufacturing Endpoint
Syncs all manufacturing entities incrementally via LastModifiedDateTime cursors.
"""

import requests
import json
from datetime import datetime, timezone
from typing import Generator

from fivetran_connector_sdk import Connector, Operations as op, Logging as log

# ---------------------------------------------------------------------------
# Entity definitions
# ---------------------------------------------------------------------------
# Each entry:
#   name          : Acumatica endpoint segment & Fivetran table name (snake_case)
#   endpoint      : Acumatica REST path segment
#   primary_key   : list of field(s) that uniquely identify a record
#   incremental   : True if the entity exposes LastModifiedDateTime for filtering
#   expand        : optional $expand parameter (comma-separated child collections)
# ---------------------------------------------------------------------------
ENTITIES = [
    {
        "name": "bill_of_material",
        "endpoint": "BillOfMaterial",
        "primary_key": ["InventoryID", "Revision"],
        "incremental": True,
        "expand": None,
    },
    {
        "name": "bill_of_material_detail",
        "endpoint": "BillOfMaterial",          # fetched via expand from BOM
        "primary_key": ["InventoryID", "Revision", "LineNbr"],
        "incremental": True,
        "expand": "Details",
        "_detail_key": "Details",              # child collection key in response
        "_parent": "bill_of_material",
    },
    {
        "name": "production_order",
        "endpoint": "ProductionOrder",
        "primary_key": ["OrderType", "ProductionNbr"],
        "incremental": True,
        "expand": None,
    },
    {
        "name": "production_order_detail",
        "endpoint": "ProductionOrder",
        "primary_key": ["OrderType", "ProductionNbr", "LineNbr"],
        "incremental": True,
        "expand": "Details",
        "_detail_key": "Details",
        "_parent": "production_order",
    },
    {
        "name": "labor_entry",
        "endpoint": "LaborEntry",
        "primary_key": ["BatchNbr", "LineNbr"],
        "incremental": True,
        "expand": None,
    },
    {
        "name": "material_entry",
        "endpoint": "MaterialEntry",
        "primary_key": ["BatchNbr", "LineNbr"],
        "incremental": True,
        "expand": None,
    },
    {
        "name": "estimate_item",
        "endpoint": "EstimateItem",
        "primary_key": ["EstimateID", "RevisionID"],
        "incremental": True,
        "expand": None,
    },
    # Reference / slowly-changing tables — full refresh on every sync
    {
        "name": "work_center",
        "endpoint": "WorkCenter",
        "primary_key": ["WorkCenterID"],
        "incremental": False,
        "expand": None,
    },
    {
        "name": "machine",
        "endpoint": "Machine",
        "primary_key": ["MachineID"],
        "incremental": False,
        "expand": None,
    },
    {
        "name": "shift",
        "endpoint": "Shift",
        "primary_key": ["ShiftCD"],
        "incremental": False,
        "expand": None,
    },
]

# Page size for all paginated requests
PAGE_SIZE = 100

# Epoch used as the "beginning of time" cursor for first sync
EPOCH = "1900-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def get_token(cfg: dict) -> str:
    """Fetch an OAuth 2.0 access token using client_credentials grant."""
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
# API fetch helpers
# ---------------------------------------------------------------------------

def build_filter(last_modified: str | None) -> str | None:
    """Build an OData $filter string for incremental sync."""
    if not last_modified or last_modified == EPOCH:
        return None
    # Acumatica accepts datetimeoffset literals
    return f"LastModifiedDateTime gt datetimeoffset'{last_modified}'"


def fetch_page(
    session: requests.Session,
    base_url: str,
    endpoint: str,
    skip: int,
    odata_filter: str | None,
    expand: str | None,
) -> list[dict]:
    """Fetch a single page of records from the Acumatica REST API."""
    params = {
        "$top": PAGE_SIZE,
        "$skip": skip,
    }
    if odata_filter:
        params["$filter"] = odata_filter
    if expand:
        params["$expand"] = expand

    url = f"{base_url}/{endpoint}"
    resp = session.get(url, params=params, timeout=60)

    if resp.status_code == 404:
        log.warning(f"Endpoint not found (404): {endpoint} — skipping")
        return []

    resp.raise_for_status()

    data = resp.json()
    # Acumatica returns a list directly, or sometimes wraps in {"value": [...]}
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "value" in data:
        return data["value"]
    return []


def fetch_all_pages(
    session: requests.Session,
    base_url: str,
    endpoint: str,
    odata_filter: str | None,
    expand: str | None,
) -> Generator[dict, None, None]:
    """Paginate through all records for an endpoint, yielding each record."""
    skip = 0
    while True:
        page = fetch_page(session, base_url, endpoint, skip, odata_filter, expand)
        if not page:
            break
        for record in page:
            yield record
        if len(page) < PAGE_SIZE:
            break  # Last page
        skip += PAGE_SIZE


# ---------------------------------------------------------------------------
# Record normalisation
# ---------------------------------------------------------------------------

def flatten_value(v) -> any:
    """
    Acumatica wraps most field values as {"value": X}.
    Unwrap them so we get clean scalar values into Fivetran.
    """
    if isinstance(v, dict) and "value" in v and len(v) == 1:
        return v["value"]
    if isinstance(v, dict):
        return json.dumps(v)   # Nested objects → JSON string (safe fallback)
    return v


def normalise_record(raw: dict, prefix: str = "") -> dict:
    """Flatten an Acumatica response record into a plain dict."""
    out = {}
    for k, v in raw.items():
        if k.startswith("_"):           # Skip Acumatica metadata fields
            continue
        col = f"{prefix}{k}" if prefix else k
        if isinstance(v, list):
            # Inline child arrays are handled separately; skip here
            continue
        out[col] = flatten_value(v)
    return out


def extract_last_modified(record: dict) -> str | None:
    """Pull LastModifiedDateTime from a raw Acumatica record."""
    raw = record.get("LastModifiedDateTime")
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw.get("value")
    return str(raw)


# ---------------------------------------------------------------------------
# Per-entity sync
# ---------------------------------------------------------------------------

def sync_entity(
    session: requests.Session,
    base_url: str,
    entity: dict,
    state: dict,
) -> Generator:
    """
    Sync a single entity. Yields Fivetran op.upsert calls.
    Updates state in-place with the latest cursor seen.
    """
    name = entity["name"]
    endpoint = entity["endpoint"]
    incremental = entity["incremental"]
    expand = entity["expand"]
    detail_key = entity.get("_detail_key")
    is_detail_entity = "_parent" in entity

    # Detail entities are pulled as expanded children of the parent —
    # we don't make separate top-level API calls for them.
    if is_detail_entity:
        return

    cursor_key = f"cursor_{name}"
    last_modified = state.get(cursor_key, EPOCH) if incremental else None
    odata_filter = build_filter(last_modified) if incremental else None

    log.info(f"Syncing {name} | incremental={incremental} | cursor={last_modified or 'N/A'}")

    latest_ts = last_modified or EPOCH
    record_count = 0

    for raw in fetch_all_pages(session, base_url, endpoint, odata_filter, expand):
        # --- Parent record ---
        row = normalise_record(raw)
        yield op.upsert(name, row)
        record_count += 1

        # --- Child detail records (if expand was requested) ---
        if detail_key and detail_key in raw:
            detail_entity_name = f"{name}_detail"
            for child in raw.get(detail_key, []):
                # Inherit parent keys so each child row is self-contained
                child_row = normalise_record(child)
                # Copy parent PK fields into child row
                for pk in entity["primary_key"]:
                    if pk in row:
                        child_row.setdefault(pk, row[pk])
                yield op.upsert(detail_entity_name, child_row)

        # Track the most recent LastModifiedDateTime seen
        if incremental:
            ts = extract_last_modified(raw)
            if ts and ts > latest_ts:
                latest_ts = ts

    # Persist the cursor after syncing this entity
    if incremental:
        state[cursor_key] = latest_ts

    log.info(f"  → {record_count} records synced for {name}")
    yield op.checkpoint(state)


# ---------------------------------------------------------------------------
# Fivetran entrypoints
# ---------------------------------------------------------------------------

def schema(configuration: dict):
    """
    Declare table schemas for Fivetran.
    Primary keys are required; all other columns are inferred at sync time.
    """
    tables = []
    for entity in ENTITIES:
        tables.append({
            "table": entity["name"],
            "primary_key": entity["primary_key"],
        })
    return tables


def update(configuration: dict, state: dict):
    """Main sync function called by Fivetran on every sync run."""
    # Build base API URL
    acumatica_url = configuration["acumatica_url"].rstrip("/")
    api_version = configuration.get("api_version", "24.200.001")
    base_url = f"{acumatica_url}/entity/Manufacturing/{api_version}"

    log.info(f"Connecting to Acumatica Manufacturing API: {base_url}")

    # Acquire OAuth token
    token = get_token(configuration)

    # Build a persistent session for connection reuse
    session = requests.Session()
    session.headers.update(build_headers(token))

    # Work through each entity
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


# ---------------------------------------------------------------------------
# Connector wiring
# ---------------------------------------------------------------------------

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Local debug run: reads configuration.json from the same directory
    connector.debug()
