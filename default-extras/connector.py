"""
Fivetran Connector SDK — Acumatica Default Endpoint (Extras)

Mirrors the Default endpoint into its own Snowflake schema so downstream
dbt models can pick up fields that the existing managed ACUMATICA_BTM
connector doesn't expose — most notably SalesInvoice.Details.OrderNbr,
which links an invoice line back to its originating sales order.

This sync is intentionally comprehensive: every available entity and child
collection. Overlap with ACUMATICA_BTM is expected and handled downstream.

Sync is incremental where the Default endpoint supports it: each entity carries
a modified-timestamp cursor and only rows changed since the last high-water mark
are pulled. A few entities expose no usable timestamp and stay full-refresh; see
the incremental cursor policy below.
"""

import re
import requests
import json
from datetime import datetime
from typing import Generator

from fivetran_connector_sdk import Connector, Operations as op, Logging as log


# ---------------------------------------------------------------------------
# Entity definitions
# ---------------------------------------------------------------------------
# Each parent entry may declare one or more child collections that come back
# via $expand. `children` is a list of {"key": <Acumatica collection name>,
# "table": <Fivetran table name>} tuples.
# ---------------------------------------------------------------------------
ENTITIES = [
    # --- Finance ---
    {"name": "account",                 "endpoint": "Account"},
    {"name": "bill",                    "endpoint": "Bill",
        "children": [{"key": "Details", "table": "bill_detail"}]},
    {"name": "currency",                "endpoint": "Currency"},
    {"name": "financial_period",        "endpoint": "FinancialPeriod",
        "children": [{"key": "Details", "table": "financial_period_detail"}]},
    {"name": "invoice",                 "endpoint": "Invoice",
        "expand_inline": ["BillToContact", "ShipToContact"],
        "children": [{"key": "Details", "table": "invoice_detail"}]},
    {"name": "journal_transaction",     "endpoint": "JournalTransaction",
        "children": [{"key": "Details", "table": "journal_transaction_detail"}]},
    {"name": "ledger",                  "endpoint": "Ledger"},
    {"name": "payment",                 "endpoint": "Payment"},
    {"name": "payment_method",          "endpoint": "PaymentMethod"},
    {"name": "sub_account",             "endpoint": "SubAccount"},
    {"name": "tax_category",            "endpoint": "TaxCategory",
        "children": [{"key": "Details", "table": "tax_category_detail"}]},
    {"name": "tax_zone",                "endpoint": "TaxZone"},

    # --- CRM ---
    {"name": "contact",                 "endpoint": "Contact",
        "expand_inline": ["Address"],
        "children": [{"key": "Attributes", "table": "contact_attribute"}]},
    {"name": "employee",                "endpoint": "Employee",
        "expand_inline": ["ContactInfo"],
        "children": [{"key": "Attributes", "table": "employee_attribute"}]},
    {"name": "sales_person",            "endpoint": "SalesPerson"},

    # --- Sales ---
    {"name": "customer",                "endpoint": "Customer",
        "expand_inline": [
            "MainContact/Address",
            "BillingContact/Address",
            "ShippingContact/Address",
        ],
        "custom_fields": ["BAccount.AttributeCRMREFNO"],
        "children": [
            {"key": "Contacts",   "table": "customer_contact"},
            {"key": "Attributes", "table": "customer_attribute"},
        ]},
    {"name": "customer_class",          "endpoint": "CustomerClass"},
    {"name": "customer_location",       "endpoint": "CustomerLocation"},
    {"name": "sales_invoice",           "endpoint": "SalesInvoice",
        "expand_inline": ["BillToAddress", "ShipToAddress", "ShipToContact"],
        "children": [{"key": "Details", "table": "sales_invoice_detail"}]},
    {"name": "sales_order",             "endpoint": "SalesOrder",
        "expand_inline": [
            "BillToAddress", "ShipToAddress",
            "BillToContact", "ShipToContact",
        ],
        "custom_fields": [
            # User-Defined Fields tab on the Sales Orders screen — surfaced
            # as Acumatica Attributes on the Document section. Field IDs are
            # the internal codes (UI labels in the comment).
            # 2026-08-18: ACCPERCENT, ELEPERCT, FINALCHECK and WARRANTY were
            # unassigned from the Sales Order screen in Acumatica (the attribute
            # definitions still exist tenant-wide). Requesting them now 500s with
            # "column ... not found in the data set", which took the whole sync
            # down. Removed here; re-add if they're ever reassigned to the order
            # attribute class. fetch_page() also self-heals this failure mode.
            "Document.AttributeACCESSORIE",   # Accessories
            "Document.AttributeCONEORDNBR",   # C1 Ord Nbr
            "Document.AttributeELECTRICAL",   # Electrical
            "Document.AttributeENDUSER",      # End Market User
            "Document.AttributeHOTDIPPER",    # Hot Dipper
            "Document.AttributeINSTALLER1",   # Installer 1
            "Document.AttributeINSTALLER2",   # Installer 2
            "Document.AttributeINSTALLER3",   # Installer 3
            "Document.AttributeINSTALLER4",   # Installer 4
            "Document.AttributeINSTALLTEC",   # Install Tech
            "Document.AttributeLIGHTNING",    # Lightning Protection
            "Document.AttributePMTNBR",       # Payment Request #
            "Document.AttributeSALESFORDR",   # Salesforce Opp #
            "Document.AttributeSHLTRSIZE",    # Shelter Square Footage
            "Document.AttributeSHLTRTYPE",    # Shelter Style
            "Document.AttributeSKU",          # SKU
            "Document.AttributeSOURCE",       # Source
            "Document.AttributeTURNKEYINS",   # Turn-key Install
            "Document.AttributeUNITS",        # Units
            "Document.AttributeUSECASE",      # Shelter Use
            "Document.AttributeVERTICALS",    # Sales Verticals
        ],
        "children": [
            {"key": "Details",    "table": "sales_order_detail"},
            {"key": "Shipments",  "table": "sales_order_shipment"},
            {"key": "TaxDetails", "table": "sales_order_tax_detail"},
        ]},
    {"name": "shipment",                "endpoint": "Shipment",
        "children": [
            {"key": "Details",  "table": "shipment_detail"},
            {"key": "Packages", "table": "shipment_package"},
        ]},

    # --- Purchasing ---
    {"name": "purchase_order",          "endpoint": "PurchaseOrder",
        "custom_fields": ["Document.AttributeSOREF"],
        "children": [
            {"key": "Details",    "table": "purchase_order_detail"},
            {"key": "TaxDetails", "table": "purchase_order_tax_detail"},
        ]},
    {"name": "purchase_receipt",        "endpoint": "PurchaseReceipt",
        "children": [{"key": "Details", "table": "purchase_receipt_detail"}]},
    {"name": "vendor",                  "endpoint": "Vendor",
        "expand_inline": [
            "MainContact/Address",
            "ShippingContact/Address",
        ],
        "children": [
            {"key": "Contacts",   "table": "vendor_contact"},
            {"key": "Attributes", "table": "vendor_attribute"},
        ]},
    {"name": "vendor_class",            "endpoint": "VendorClass"},

    # --- Inventory ---
    {"name": "inventory_issue",         "endpoint": "InventoryIssue",
        "children": [{"key": "Details", "table": "inventory_issue_detail"}]},
    {"name": "inventory_receipt",       "endpoint": "InventoryReceipt",
        "children": [{"key": "Details", "table": "inventory_receipt_detail"}]},
    {"name": "non_stock_item",          "endpoint": "NonStockItem",
        "children": [
            {"key": "Attributes",      "table": "non_stock_item_attribute"},
            {"key": "CrossReferences", "table": "non_stock_item_cross_ref"},
        ]},
    {"name": "physical_inventory_review","endpoint": "PhysicalInventoryReview",
        "children": [{"key": "Details", "table": "physical_inventory_review_detail"}]},
    {"name": "stock_item",              "endpoint": "StockItem",
        "children": [
            {"key": "Attributes",               "table": "stock_item_attribute"},
            {"key": "WarehouseDetails",         "table": "stock_item_warehouse_detail"},
            {"key": "CrossReferences",          "table": "stock_item_cross_ref"},
            {"key": "ReplenishmentParameters",  "table": "stock_item_replenishment"},
        ]},
    {"name": "warehouse",               "endpoint": "Warehouse",
        "children": [{"key": "Locations", "table": "warehouse_location"}]},
    {"name": "item_class",              "endpoint": "ItemClass"},
    {"name": "units_of_measure",        "endpoint": "UnitsOfMeasure"},
]

PRIMARY_KEY = ["id"]
PAGE_SIZE = 100


# ---------------------------------------------------------------------------
# Incremental cursor policy
# ---------------------------------------------------------------------------
# Probed against the Default endpoint on 2026-07-31: every entity exposes a
# filterable modified-timestamp EXCEPT the three in FULL_REFRESH_ONLY, which
# carry no usable timestamp ($filter on it 500s). Those are tiny reference /
# rarely-changing tables, so a full refresh each run costs almost nothing.
# SalesOrder is the lone entity whose field is named `LastModified` rather than
# `LastModifiedDateTime`.
# stock_item DOES expose a usable timestamp, but Qty On Hand (WarehouseDetails)
# is computed from INSiteStatus at read time — inventory transactions change the
# quantity without touching the item's LastModifiedDateTime, so an incremental
# pull silently freezes quantities. Full refresh is the only way to keep them live.
DEFAULT_CURSOR_FIELD = "LastModifiedDateTime"
CURSOR_FIELD_OVERRIDES = {"sales_order": "LastModified"}
FULL_REFRESH_ONLY = {"sub_account", "physical_inventory_review", "units_of_measure", "stock_item"}


def cursor_field_for(entity: dict) -> str | None:
    """Timestamp field to use as this entity's incremental cursor, or None to
    always full-refresh it."""
    name = entity["name"]
    if name in FULL_REFRESH_ONLY:
        return None
    return CURSOR_FIELD_OVERRIDES.get(name, DEFAULT_CURSOR_FIELD)


def _cursor_raw_value(raw: dict, field: str):
    """Pull the scalar cursor value out of a raw record ({"value": ...} wrapper)."""
    v = raw.get(field)
    return v.get("value") if isinstance(v, dict) else v


def _parse_dt(s: str) -> datetime:
    """Parse an Acumatica timestamp, e.g. '2026-05-19T15:33:51.253+00:00' (or 'Z')."""
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _post_token(token_url: str, payload: dict) -> requests.Response:
    """POST to the token endpoint, surfacing the OAuth error body on failure.

    raise_for_status() hides the body, which is exactly what tells us *why* a 400
    happened (invalid_grant vs invalid_client). Log it so failures are diagnosable.
    """
    resp = requests.post(token_url, data=payload, timeout=30)
    if not resp.ok:
        log.warning(
            f"Token endpoint returned {resp.status_code} for "
            f"grant_type={payload.get('grant_type')}: {resp.text[:300]}"
        )
    return resp


def get_token(cfg: dict, state: dict | None = None) -> str:
    """
    Return an OAuth access token via refresh_token grant.

    Candidate tokens are tried in order: the (freshest) value in state, then the
    config bootstrap value. A new token dropped into config supersedes a stale
    stored token, so "update config + redeploy" recovers a broken token chain.
    Rotated values are written back to state for the next op.checkpoint().
    """
    token_url = f"{cfg['acumatica_url'].rstrip('/')}/identity/connect/token"
    if state is None:
        state = {}

    cfg_refresh = cfg.get("refresh_token")

    # A new refresh_token in config supersedes whatever single-use token is parked
    # in Fivetran state — the state token is almost certainly the stale/consumed one
    # that broke the chain. Without this, state always wins and a fresh deploy never
    # actually takes effect.
    if cfg_refresh and state.get("config_refresh_seen") != cfg_refresh:
        log.info("New refresh_token detected in configuration; resetting stored token")
        state["refresh_token"] = cfg_refresh
        state["config_refresh_seen"] = cfg_refresh

    # Freshest token first (state), then fall back to the config token if state has
    # gone stale. De-dup so the same dead token isn't replayed twice.
    candidates = []
    for tok in (state.get("refresh_token"), cfg_refresh):
        if tok and tok not in candidates:
            candidates.append(tok)

    for i, refresh in enumerate(candidates):
        log.info(f"Using refresh_token grant (candidate {i + 1}/{len(candidates)})")
        resp = _post_token(
            token_url,
            {
                "grant_type": "refresh_token",
                "refresh_token": refresh,
                "client_id": cfg["client_id"],
                "client_secret": cfg["client_secret"],
            },
        )
        if not resp.ok:
            continue  # stale token — try the next candidate
        data = resp.json()
        new_refresh = data.get("refresh_token")
        if new_refresh:
            state["refresh_token"] = new_refresh
            if new_refresh != refresh:
                # Never log the full token — Fivetran logs are inspectable and a
                # refresh token is a live credential. A short suffix confirms rotation.
                log.info(f"Refresh token rotated (…{new_refresh[-6:]})")
        return data["access_token"]

    static = cfg.get("access_token")
    if static:
        log.info("Refresh grant failed/absent; using static access_token from configuration")
        return static

    raise ValueError("All refresh_token candidates failed and no static access_token provided")


def build_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _reauth(session, configuration, state) -> None:
    """Mint a fresh access token mid-sync and swap it into the shared session.

    Acumatica access tokens live only ~1 hour, but a comprehensive full-refresh
    of every entity routinely runs longer than that. Rather than pre-computing
    expiry, we re-auth reactively when a request 401s. get_token() uses (and
    rotates) the refresh_token in `state`, so the credential chain keeps moving.
    """
    token = get_token(configuration, state)
    session.headers.update(build_headers(token))


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

# Acumatica's error when a $custom field references an attribute that is no
# longer assigned to the entity's screen (e.g. an admin unassigns a UDF from
# the Sales Orders attribute class). The column name comes back with the '.'
# flattened to '_': Document.AttributeFOO → 'Document_AttributeFOO'.
_MISSING_COLUMN_RE = re.compile(r"The column '([^']+)' is not found in the data set")


def fetch_page(session, base_url, endpoint, skip, expand, custom_parts, filt, configuration, state):
    """Fetch one page. `custom_parts` is a MUTABLE list shared across the whole
    entity sync: if Acumatica 500s because one of the $custom fields has been
    unassigned from the screen, that field is dropped from the list (so every
    subsequent page skips it too) and the request is retried — a missing
    optional column shouldn't take the whole connector down."""
    url = f"{base_url}/{endpoint}"
    while True:
        params = {"$top": PAGE_SIZE, "$skip": skip}
        if expand:
            params["$expand"] = expand
        if custom_parts:
            params["$custom"] = ",".join(custom_parts)
        if filt:
            params["$filter"] = filt
        resp = session.get(url, params=params, timeout=120)
        if resp.status_code == 401:
            # Access token almost certainly expired mid-sync. Re-auth once and retry;
            # if it 401s again the raise_for_status() below surfaces a real auth error.
            log.info(f"401 on {endpoint} @ skip={skip}; refreshing access token and retrying")
            _reauth(session, configuration, state)
            resp = session.get(url, params=params, timeout=120)
        if resp.status_code == 404:
            log.warning(f"Endpoint not found (404): {endpoint} — skipping")
            return []
        if resp.status_code == 500 and custom_parts:
            m = _MISSING_COLUMN_RE.search(resp.text)
            dead = None
            if m:
                col = m.group(1)
                dead = next((f for f in custom_parts if f.replace(".", "_") == col), None)
            if dead:
                log.warning(
                    f"{endpoint}: custom field {dead} is no longer assigned to this "
                    f"screen in Acumatica — dropping it for the rest of this sync. "
                    f"Remove it from ENTITIES (or reassign the attribute) to clear this warning."
                )
                custom_parts.remove(dead)
                continue
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "value" in data:
            return data["value"]
        return []


def fetch_all_pages(session, base_url, endpoint, expand, custom_parts, filt, configuration, state) -> Generator[dict, None, None]:
    skip = 0
    while True:
        page = fetch_page(session, base_url, endpoint, skip, expand, custom_parts, filt, configuration, state)
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

def sync_entity(session, base_url, entity, configuration, state) -> Generator:
    name = entity["name"]
    endpoint = entity["endpoint"]
    children_spec = entity.get("children", [])
    inline_expansions = entity.get("expand_inline", [])
    custom_fields = entity.get("custom_fields", [])

    # Combined $expand: inline-singular (folded into parent record by the
    # recursive flattener) + children (synced as their own tables).
    expand_parts = list(inline_expansions) + [c["key"] for c in children_spec]
    expand = ",".join(expand_parts) if expand_parts else None

    # $custom pulls user-defined / Attribute fields that aren't returned by
    # default. Each entry is "Section.FieldName" — recursive flattener will
    # produce columns like custom_document_attribute_shltrtype. Copied to a
    # fresh mutable list so fetch_page() can drop fields Acumatica no longer
    # exposes without mutating the module-level ENTITIES spec.
    custom_parts = list(custom_fields)

    # Incremental cursor: pull only rows changed since the stored high-water mark.
    # `ge` (not `gt`) re-pulls the boundary row(s) each run — harmless because every
    # upsert is keyed on the immutable `id`, so a re-pull is idempotent, and it avoids
    # dropping a row saved at the exact cursor timestamp just after the prior read.
    cfield = cursor_field_for(entity)
    cursors = state.setdefault("cursors", {})
    saved = cursors.get(name) if cfield else None
    filt = f"{cfield} ge datetimeoffset'{saved}'" if (cfield and saved) else None

    mode = "incremental" if filt else ("full refresh — first run" if cfield else "full refresh")
    log.info(f"Syncing {name} ({mode})")
    parent_count = 0
    child_counts = {c["table"]: 0 for c in children_spec}
    max_cursor = None      # newest cursor value seen this run (ISO string)
    max_cursor_dt = None   # its parsed form, so we don't re-parse the max each row

    for raw in fetch_all_pages(session, base_url, endpoint, expand, custom_parts, filt, configuration, state):
        yield op.upsert(name, normalise_record(raw))
        parent_count += 1
        parent_id = raw.get("id")

        if cfield:
            cval = _cursor_raw_value(raw, cfield)
            if cval:
                try:
                    dt = _parse_dt(cval)
                except (ValueError, TypeError):
                    dt = None
                if dt and (max_cursor_dt is None or dt > max_cursor_dt):
                    max_cursor, max_cursor_dt = cval, dt

        for c in children_spec:
            for child in raw.get(c["key"], []) or []:
                child_row = normalise_record(child)
                child_row.setdefault("parent_id", parent_id)
                yield op.upsert(c["table"], child_row)
                child_counts[c["table"]] += 1

    # Advance the high-water mark only after the entity synced cleanly — a mid-entity
    # failure then re-pulls the full delta next run rather than skipping rows. When no
    # rows came back (nothing changed) max_cursor stays None and the mark is preserved.
    if cfield and max_cursor:
        cursors[name] = max_cursor

    summary = f"{parent_count} {name}"
    if child_counts:
        summary += ", " + ", ".join(f"{n} {t}" for t, n in child_counts.items())
    log.info(f"  → {summary}")
    yield op.checkpoint(state)


# ---------------------------------------------------------------------------
# Fivetran entrypoints
# ---------------------------------------------------------------------------

def schema(configuration: dict):
    tables = []
    for entity in ENTITIES:
        tables.append({"table": entity["name"], "primary_key": PRIMARY_KEY})
        for c in entity.get("children", []):
            tables.append({"table": c["table"], "primary_key": PRIMARY_KEY})
    return tables


def update(configuration: dict, state: dict):
    acumatica_url = configuration["acumatica_url"].rstrip("/")
    api_version = configuration.get("api_version", "24.200.001")
    base_url = f"{acumatica_url}/entity/Default/{api_version}"

    log.info(f"Connecting to Acumatica Default API: {base_url}")

    token = get_token(configuration, state)
    session = requests.Session()
    session.headers.update(build_headers(token))

    for entity in ENTITIES:
        try:
            yield from sync_entity(session, base_url, entity, configuration, state)
        except requests.exceptions.HTTPError as e:
            log.severe(f"HTTP error syncing {entity['name']}: {e}")
            raise
        except Exception as e:
            log.severe(f"Unexpected error syncing {entity['name']}: {e}")
            raise

    log.info("Default sync complete.")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json") as f:
        _cfg = json.load(f)
    connector.debug(configuration=_cfg)
