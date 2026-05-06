"""
Fivetran Connector SDK — Acumatica Default Endpoint (Extras)

Mirrors the Default endpoint into its own Snowflake schema so downstream
dbt models can pick up fields that the existing managed ACUMATICA_BTM
connector doesn't expose — most notably SalesInvoice.Details.OrderNbr,
which links an invoice line back to its originating sales order.

This sync is intentionally comprehensive: every available entity and child
collection. Overlap with ACUMATICA_BTM is expected and handled downstream.
"""

import requests
import json
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
            "Document.AttributeACCESSORIE",   # Accessories
            "Document.AttributeACCPERCENT",   # Accessories Percentage
            "Document.AttributeCONEORDNBR",   # C1 Ord Nbr
            "Document.AttributeELECTRICAL",   # Electrical
            "Document.AttributeELEPERCT",     # Electrical Percentage
            "Document.AttributeENDUSER",      # End Market User
            "Document.AttributeFINALCHECK",   # Final Check
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
            "Document.AttributeWARRANTY",     # Warranty
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
# Auth
# ---------------------------------------------------------------------------

def get_token(cfg: dict, state: dict | None = None) -> str:
    """
    Return an OAuth access token via refresh_token grant. Refresh token is read
    from state first (most recent), then config (initial bootstrap). Rotated
    values are written back to state for the next op.checkpoint() to persist.
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
            log.info(f"Refresh token rotated; new value: {new_refresh}")
        return data["access_token"]

    static = cfg.get("access_token")
    if static:
        log.info("Using static access_token from configuration")
        return static

    raise ValueError("No refresh_token or access_token in configuration")


def build_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_page(session, base_url, endpoint, skip, expand, custom):
    params = {"$top": PAGE_SIZE, "$skip": skip}
    if expand:
        params["$expand"] = expand
    if custom:
        params["$custom"] = custom
    url = f"{base_url}/{endpoint}"
    resp = session.get(url, params=params, timeout=120)
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


def fetch_all_pages(session, base_url, endpoint, expand, custom) -> Generator[dict, None, None]:
    skip = 0
    while True:
        page = fetch_page(session, base_url, endpoint, skip, expand, custom)
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
    children_spec = entity.get("children", [])
    inline_expansions = entity.get("expand_inline", [])
    custom_fields = entity.get("custom_fields", [])

    # Combined $expand: inline-singular (folded into parent record by the
    # recursive flattener) + children (synced as their own tables).
    expand_parts = list(inline_expansions) + [c["key"] for c in children_spec]
    expand = ",".join(expand_parts) if expand_parts else None

    # $custom pulls user-defined / Attribute fields that aren't returned by
    # default. Each entry is "Section.FieldName" — recursive flattener will
    # produce columns like custom_document_attribute_shltrtype.
    custom = ",".join(custom_fields) if custom_fields else None

    log.info(f"Syncing {name} (full refresh)")
    parent_count = 0
    child_counts = {c["table"]: 0 for c in children_spec}

    for raw in fetch_all_pages(session, base_url, endpoint, expand, custom):
        yield op.upsert(name, normalise_record(raw))
        parent_count += 1
        parent_id = raw.get("id")

        for c in children_spec:
            for child in raw.get(c["key"], []) or []:
                child_row = normalise_record(child)
                child_row.setdefault("parent_id", parent_id)
                yield op.upsert(c["table"], child_row)
                child_counts[c["table"]] += 1

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
            yield from sync_entity(session, base_url, entity, state)
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
