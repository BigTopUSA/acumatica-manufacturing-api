"""
Fivetran Connector SDK — JobTread (Pave API)

JobTread is a construction project-management platform. Their public API uses
a JSON-based query language called "Pave" (similar in spirit to GraphQL but
not interchangeable). The endpoint is a single POST to /pave.

Auth model (confirmed by probing the unauthenticated endpoint):
  - The API expects a `grantKey` issued by JobTread per organization.
  - Top-level fields (`organization`, `job`, `account`) require an `id` arg
    and (almost certainly) the `grantKey` arg in the same `$` block.
  - No OAuth, no rotation — it's a long-lived API key. Store in config.

Query shape:
  POST https://api.jobtread.com/pave
  body: {"query": {<root>: {"$": {<args>}, <field>: {}, ...}}}

Pagination — UNCONFIRMED until we have a grantKey to test with. Pave most
likely follows a connection-style pattern (`nodes` + `nextPage`/`size` args).
This scaffold assumes that; sync_entity() will need adjustment if the actual
shape differs (see TODO comments below).

This file is INTENTIONALLY a scaffold. Once a grantKey is available:
  1. Run `python test_pave.py` to discover actual collection field names
     under `organization` (jobs? customers? vendors? taskTemplates?) and
     verify the pagination shape.
  2. Update the ENTITIES list with confirmed field names + child structure.
  3. Run `python connector.py` for a local debug sync.
  4. Deploy to Fivetran.
"""

import json
import requests
from typing import Generator

from fivetran_connector_sdk import Connector, Operations as op, Logging as log


PAVE_URL = "https://api.jobtread.com/pave"
PAGE_SIZE = 100  # adjust once Pave's max-per-page is confirmed


# ---------------------------------------------------------------------------
# Entity definitions
# ---------------------------------------------------------------------------
# Each entity describes a collection nested under `organization`. The Pave
# query for a sync looks like:
#   { "query": { "organization": { "$": {id, grantKey},
#                                   "<field>": { "$": {size, page},
#                                                "nodes": { <field>: {}, ... } } } } }
#
# All field names here are PLACEHOLDERS based on common JobTread terminology.
# Real names must be confirmed via test_pave.py before deploy. Fields likely
# to exist (per JobTread's product surface):
#   - jobs, customers, vendors, tasks, taskTemplates, dailyLogs, documents,
#     comments, lineItems, costItems, schedules, users
# ---------------------------------------------------------------------------
ENTITIES = [
    # Each entry:
    #   name          — Fivetran/Snowflake table name (snake_case)
    #   collection    — Pave field name on `organization` (camelCase)
    #   fields        — list of scalar field names to request
    #   child_fields  — optional nested-object fields to flatten inline
    #                   (recursive flattener handles them)
    #
    # TODO: replace placeholders once test_pave.py confirms real names.
    {
        "name": "job",
        "collection": "jobs",
        "fields": [
            "id", "name", "number", "createdAt", "updatedAt",
            "status", "type", "estimatedRevenue", "actualRevenue",
            # location, customer, etc. likely nested objects — flatten inline:
        ],
        "child_fields": ["location", "customer"],
    },
    {
        "name": "customer",
        "collection": "customers",
        "fields": [
            "id", "name", "email", "phone", "createdAt", "updatedAt",
        ],
        "child_fields": ["address", "primaryContact"],
    },
    {
        "name": "vendor",
        "collection": "vendors",
        "fields": [
            "id", "name", "email", "phone", "createdAt", "updatedAt",
        ],
        "child_fields": ["address"],
    },
    {
        "name": "task",
        "collection": "tasks",
        "fields": [
            "id", "name", "status", "startDate", "endDate",
            "createdAt", "updatedAt",
        ],
        "child_fields": ["assignee"],
    },
    # Add more entities here as they are discovered via test_pave.py
]

PRIMARY_KEY = ["id"]


# ---------------------------------------------------------------------------
# Pave query helpers
# ---------------------------------------------------------------------------

def field_subselect(fields: list[str], child_fields: list[str] | None = None) -> dict:
    """
    Build a Pave subselect block. Pave wants every field as a key with an
    empty dict value, e.g. {"id": {}, "name": {}}. Nested objects are also
    {<name>: {}} which returns the whole sub-object inline (flattener handles).
    """
    out = {f: {} for f in fields}
    for c in (child_fields or []):
        out[c] = {}
    return out


def build_org_query(grant_key: str, org_id: str, collection: str,
                    fields: list[str], child_fields: list[str] | None,
                    size: int, page: int) -> dict:
    """
    Build a single page query against organization.<collection>.

    NOTE: pagination args (`size`, `page`) are best guesses. Pave conventions
    seen in similar APIs use these names; alternatives are `first`/`after`
    (cursor-style) or `limit`/`offset`. test_pave.py probes which works.
    """
    return {
        "query": {
            "organization": {
                "$": {"id": org_id, "grantKey": grant_key},
                "id": {},
                collection: {
                    "$": {"size": size, "page": page},
                    "nextPage": {},
                    "nodes": field_subselect(fields, child_fields),
                },
            }
        }
    }


def post_pave(query: dict) -> dict:
    """Send a Pave query and return the response JSON's data envelope."""
    resp = requests.post(PAVE_URL, json=query, timeout=60)
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"Pave error: {body['errors']}")
    return body


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_all_pages(
    grant_key: str, org_id: str, entity: dict
) -> Generator[dict, None, None]:
    """Page through one collection, yielding flat records."""
    page = 1
    while True:
        q = build_org_query(
            grant_key, org_id,
            entity["collection"],
            entity["fields"],
            entity.get("child_fields"),
            PAGE_SIZE, page,
        )
        body = post_pave(q)

        org = body.get("organization") or body.get("data", {}).get("organization") or {}
        coll = org.get(entity["collection"], {}) or {}
        nodes = coll.get("nodes") or []
        next_page = coll.get("nextPage")

        for rec in nodes:
            yield rec

        if not next_page or len(nodes) < PAGE_SIZE:
            break
        page += 1


# ---------------------------------------------------------------------------
# Normalisation (recursive flattener — same logic as Acumatica connectors)
# ---------------------------------------------------------------------------

def normalise_record(raw: dict, prefix: str = "") -> dict:
    """
    Flatten a Pave record. Pave returns plain JSON (no {"value": X} wrappers),
    so the logic is simpler than the Acumatica version. We just recurse into
    nested dicts with prefix joining. Lists are skipped (children handled
    separately if needed).
    """
    out = {}
    for k, v in raw.items():
        if isinstance(v, list):
            continue  # skip arrays — handle as children if/when added
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

def sync_entity(grant_key: str, org_id: str, entity: dict, state: dict) -> Generator:
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
        except requests.exceptions.HTTPError as e:
            log.severe(f"HTTP error syncing {entity['name']}: {e}")
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
