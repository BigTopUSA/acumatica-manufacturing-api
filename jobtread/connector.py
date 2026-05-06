"""
Fivetran Connector SDK — JobTread (Pave API)

JobTread is a construction project-management platform. Their API uses a
JSON-based query language called "Pave" (similar in spirit to GraphQL but
not interchangeable). Single endpoint: POST https://api.jobtread.com/pave

Auth (verified live against api.jobtread.com):
  - Send `Authorization: Bearer <grantKey>` HTTP header.
  - Top-level fields like `organization` accept an `id` arg in `$`.
  - Grant keys are long-lived; no rotation.

Pagination (verified):
  - `size` arg sets page size; max appears to be ~100.
  - First page: `{"$": {"size": 100}, "nodes": {...}, "nextPage": {}}`
  - Subsequent pages: `{"$": {"size": 100, "page": <cursor>}, ...}`
  - Loop until `nextPage` is null.

Entity field definitions below were discovered via test_pave.py — not
guesses. See the script for how to refresh if JobTread adds new fields.
"""

import json
import requests
from typing import Generator

from fivetran_connector_sdk import Connector, Operations as op, Logging as log


PAVE_URL = "https://api.jobtread.com/pave"
PAGE_SIZE = 100


# ---------------------------------------------------------------------------
# Entity definitions — scalar fields verified against the real org
# ---------------------------------------------------------------------------
# Each entry:
#   name        : Snowflake table name (snake_case)
#   collection  : Pave field name on `organization` (camelCase)
#   scalars     : list of scalar field names to request (id, name, etc.)
#   relations   : list of {key, subselect} — nested objects (e.g. account)
#                 are pulled as flat columns via the recursive flattener,
#                 producing `account_id`, `account_name`, etc.
# ---------------------------------------------------------------------------
ENTITIES = [
    {
        "name": "account",
        "collection": "accounts",
        "scalars": ["id", "name", "type", "createdAt", "archivedAt"],
        "relations": [{"key": "organization", "subselect": ["id", "name"]}],
    },
    {
        "name": "job",
        "collection": "jobs",
        "scalars": ["id", "name", "number", "status", "description", "createdAt", "closedOn"],
        "relations": [
            {"key": "organization", "subselect": ["id", "name"]},
            {"key": "location", "subselect": ["id", "name"]},
        ],
    },
    {
        "name": "task",
        "collection": "tasks",
        "scalars": ["id", "name", "description", "createdAt", "startDate", "endDate", "progress"],
        "relations": [
            {"key": "account", "subselect": ["id", "name"]},
            {"key": "organization", "subselect": ["id", "name"]},
            {"key": "location", "subselect": ["id", "name"]},
            {"key": "job", "subselect": ["id", "name", "number"]},
        ],
    },
    {
        "name": "contact",
        "collection": "contacts",
        "scalars": ["id", "name", "title", "createdAt"],
        "relations": [{"key": "account", "subselect": ["id", "name"]}],
    },
    {
        "name": "event",
        "collection": "events",
        "scalars": ["id", "type", "createdAt"],
        "relations": [
            {"key": "account", "subselect": ["id", "name"]},
            {"key": "organization", "subselect": ["id", "name"]},
            {"key": "location", "subselect": ["id", "name"]},
            {"key": "job", "subselect": ["id", "name", "number"]},
        ],
    },
    {
        "name": "location",
        "collection": "locations",
        "scalars": ["id", "name", "createdAt"],
        "relations": [
            {"key": "account", "subselect": ["id", "name"]},
            {"key": "address", "subselect": ["id"]},
        ],
    },
    # `invoices` collection returned HTTP 400 "field 'id' is not expected" on
    # this org — the invoicing module/schema isn't accessible via Pave.
    # Removed from sync until JobTread documents the right query shape.
    {
        "name": "payment",
        "collection": "payments",
        "scalars": ["id", "type", "description", "createdAt", "amount", "externalId"],
        "relations": [
            {"key": "account", "subselect": ["id", "name"]},
            {"key": "organization", "subselect": ["id", "name"]},
        ],
    },
    {
        "name": "cost_item",
        "collection": "costItems",
        "scalars": ["id", "name", "description", "createdAt", "price", "cost", "quantity"],
        "relations": [
            {"key": "organization", "subselect": ["id", "name"]},
            {"key": "job", "subselect": ["id", "name", "number"]},
        ],
    },
    {
        "name": "daily_log",
        "collection": "dailyLogs",
        "scalars": ["id", "notes", "createdAt"],
        "relations": [
            {"key": "organization", "subselect": ["id", "name"]},
            {"key": "job", "subselect": ["id", "name", "number"]},
        ],
    },
    {
        "name": "comment",
        "collection": "comments",
        "scalars": ["id", "name", "createdAt"],
        "relations": [
            {"key": "account", "subselect": ["id", "name"]},
            {"key": "organization", "subselect": ["id", "name"]},
            {"key": "job", "subselect": ["id", "name", "number"]},
        ],
    },
    {
        "name": "document",
        "collection": "documents",
        "scalars": ["id", "name", "number", "status", "type", "description",
                    "createdAt", "closedAt", "dueDate", "tax", "price", "cost", "externalId"],
        "relations": [
            {"key": "account", "subselect": ["id", "name"]},
            {"key": "organization", "subselect": ["id", "name"]},
            {"key": "job", "subselect": ["id", "name", "number"]},
        ],
    },
    {
        "name": "file",
        "collection": "files",
        "scalars": ["id", "name", "type", "description", "createdAt"],
        "relations": [
            {"key": "account", "subselect": ["id", "name"]},
            {"key": "organization", "subselect": ["id", "name"]},
            {"key": "location", "subselect": ["id", "name"]},
            {"key": "job", "subselect": ["id", "name", "number"]},
        ],
    },
]

PRIMARY_KEY = ["id"]


# ---------------------------------------------------------------------------
# Pave helpers
# ---------------------------------------------------------------------------

def field_subselect(scalars, relations):
    """Build the Pave node subselect: {field: {}} for scalars, nested obj for relations."""
    out = {f: {} for f in scalars}
    for r in (relations or []):
        out[r["key"]] = {sf: {} for sf in r["subselect"]}
    return out


def build_page_query(org_id, collection, scalars, relations, page_token):
    args = {"size": PAGE_SIZE}
    if page_token:
        args["page"] = page_token
    return {
        "query": {
            "organization": {
                "$": {"id": org_id},
                collection: {
                    "$": args,
                    "nodes": field_subselect(scalars, relations),
                    "nextPage": {},
                },
            }
        }
    }


def post_pave(query, grant_key):
    """POST a Pave query with Bearer auth. Pave returns text/plain on errors."""
    headers = {"Authorization": f"Bearer {grant_key}"}
    resp = requests.post(PAVE_URL, json=query, headers=headers, timeout=120)
    if resp.status_code >= 400:
        raise RuntimeError(f"Pave HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_all_pages(grant_key, org_id, entity) -> Generator[dict, None, None]:
    page_token = None
    while True:
        q = build_page_query(
            org_id,
            entity["collection"],
            entity["scalars"],
            entity.get("relations"),
            page_token,
        )
        body = post_pave(q, grant_key)
        coll = (body.get("organization") or {}).get(entity["collection"]) or {}
        nodes = coll.get("nodes") or []
        for rec in nodes:
            yield rec
        page_token = coll.get("nextPage")
        if not page_token or not nodes:
            break


# ---------------------------------------------------------------------------
# Normalisation — recursive flattener with prefix joining
# ---------------------------------------------------------------------------

def normalise_record(raw, prefix=""):
    """
    Pave returns plain JSON. Nested objects (e.g. account: {id, name})
    flatten to account_id / account_name columns via underscore-joining.
    """
    out = {}
    for k, v in raw.items():
        if isinstance(v, list):
            continue  # children handled separately if added later
        col = f"{prefix}{k}" if prefix else k
        if isinstance(v, dict) and v:
            out.update(normalise_record(v, prefix=f"{col}_"))
        elif isinstance(v, dict):
            out[col] = None
        else:
            out[col] = v
    return out


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

def sync_entity(grant_key, org_id, entity, state) -> Generator:
    name = entity["name"]
    log.info(f"Syncing {name} (full refresh)")
    count = 0
    for raw in fetch_all_pages(grant_key, org_id, entity):
        yield op.upsert(name, normalise_record(raw))
        count += 1
    log.info(f"  → {count} {name} rows")
    yield op.checkpoint(state)


# ---------------------------------------------------------------------------
# Fivetran entrypoints
# ---------------------------------------------------------------------------

def schema(configuration: dict):
    return [{"table": e["name"], "primary_key": PRIMARY_KEY} for e in ENTITIES]


def update(configuration: dict, state: dict):
    grant_key = configuration["grant_key"]
    org_id = configuration["organization_id"]

    log.info(f"Connecting to JobTread Pave API for organization {org_id}")

    for entity in ENTITIES:
        try:
            yield from sync_entity(grant_key, org_id, entity, state)
        except RuntimeError as e:
            log.severe(f"Pave error syncing {entity['name']}: {e}")
            raise
        except Exception as e:
            log.severe(f"Unexpected error syncing {entity['name']}: {e}")
            raise

    log.info("JobTread sync complete.")


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    with open("configuration.json") as f:
        _cfg = json.load(f)
    connector.debug(configuration=_cfg)
