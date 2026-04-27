# Acumatica → Fivetran Connectors

This repo contains two custom Fivetran Connector SDK connectors that sync data
from Acumatica into BigTop's Snowflake warehouse.

| Connector | Folder | Acumatica Endpoint | Snowflake Schema |
|---|---|---|---|
| Manufacturing | `./` (repo root) | `/entity/MANUFACTURING/{version}/` | `BT_RAW.ACUMATICA_MANUFACTURING` |
| Default Extras | `./default-extras/` | `/entity/Default/{version}/` | `BT_RAW.ACUMATICA_DEFAULT_EXTRAS` |

`acumatica_default_extras` exists alongside the existing managed Fivetran
Acumatica connector (`ACUMATICA_BTM`) — it covers what the managed one doesn't,
most importantly `sales_invoice_detail.order_nbr`, which links each invoice
line back to its originating sales order.

---

## Sync Mode

**Both connectors are full-refresh.** Every scheduled run pulls every record.

This is not a choice — the Manufacturing endpoint doesn't expose
`LastModifiedDateTime` at all (filtering on it returns HTTP 500), so true
cursor-based incremental sync is impossible. Default-endpoint entities *do*
expose `LastModifiedDateTime` and could be made incremental in the future, but
today the second connector is also full-refresh for code consistency.

**Cost implication:** Fivetran bills on Monthly Active Rows (rows that *change*),
not records read. Full refresh re-reads every row but only counts changed rows
toward billing. The first sync was the expensive one; ongoing cost is low.

---

## Tables Synced — Manufacturing

11 tables in `ACUMATICA_MANUFACTURING`:

| Fivetran Table | Acumatica Endpoint |
|---|---|
| `bill_of_material` | `BillOfMaterial` |
| `bill_of_material_operation` | `BillOfMaterial` → `$expand=Operations` |
| `production_order` | `ProductionOrder` |
| `production_order_detail` | `ProductionOrderDetail` (separate top-level endpoint) |
| `labor_entry` | `LaborEntry` |
| `labor_entry_detail` | `LaborEntry` → `$expand=Details` |
| `material_entry` | `MaterialEntry` |
| `material_entry_detail` | `MaterialEntry` → `$expand=Details` |
| `work_center` | `WorkCenter` |
| `machine` | `Machine` |
| `shift` | `Shift` |

## Tables Synced — Default Extras

33 parents + 30 children = 63 tables in `ACUMATICA_DEFAULT_EXTRAS`. Full list
in [`default-extras/connector.py`](default-extras/connector.py) under
`ENTITIES`. Highlights: `sales_invoice` + `sales_invoice_detail`,
`sales_order` (with `Details`, `Shipments`, `TaxDetails`),
`shipment` (with `Details`, `Packages`), and full Default-endpoint coverage of
finance, inventory, purchasing, and CRM modules.

---

## Authentication — How It Actually Works

Acumatica's available OAuth grants in the Connected Applications screen are:
**Authorization Code, Implicit, Hybrid, ROPC.** No `client_credentials`
option, and ROPC requires a local password (which SSO-backed users don't have).

So the connector uses the **Refresh Token** grant:

1. Someone authorizes interactively **once** in Postman to get an
   `access_token` + `refresh_token`. Required scopes: `api offline_access`.
   Sliding expiration must be enabled on the Acumatica client.
2. The `refresh_token` is stored in `configuration.json`.
3. On every sync, the connector exchanges the refresh_token for a fresh
   access_token (no human interaction).
4. Acumatica rotates the refresh_token on each use (single-use tokens). The
   connector writes the new value back to Fivetran's `state` so the next sync
   picks it up.

### Re-authentication

Refresh tokens eventually expire (Acumatica defaults to 60 days but
sliding expiration extends this on every use). If a sync fails with
`invalid_grant`, the refresh token chain is broken and someone needs to:

1. Run the auth flow in Postman again
2. Copy the new `refresh_token`
3. Update `configuration.json` (or the value in the Fivetran connector config)
4. Redeploy or save the config

---

## Configuration

| Field | Description |
|---|---|
| `acumatica_url` | Base URL of your Acumatica instance, e.g. `https://bigtopshelters.acumatica.com` |
| `client_id` | OAuth 2.0 client ID, including `@TenantName` suffix |
| `client_secret` | OAuth 2.0 client secret |
| `api_version` | Endpoint version, e.g. `24.200.001` |
| `refresh_token` | Long-lived refresh token captured from Postman |
| `access_token` | *(optional)* Static access token for one-shot debugging only |

`configuration.json` is gitignored — never commit secrets.

---

## Local Debug Run

```bash
# From the repo root for the manufacturing connector:
python connector.py

# From default-extras/ for the default extras connector:
cd default-extras && python connector.py
```

This calls `connector.debug()`, reads `configuration.json`, runs one full sync,
and writes output to `files/warehouse.db` (DuckDB) so you can inspect what
Fivetran would receive without a real Fivetran connection.

**Heads up:** running `connector.debug()` rotates the refresh token. The new
value is written to `files/state.json`, but if Fivetran has also synced more
recently, the token in your `configuration.json` is now stale. Plan for one
re-auth in Postman after every local debug run.

---

## Deploying to Fivetran

```bash
# Manufacturing connector
fivetran deploy \
  --api-key <base64-encoded-key:secret> \
  --destination Snowflake_East_US2 \
  --connection acumatica_manufacturing \
  --configuration configuration.json

# Default extras connector
cd default-extras && fivetran deploy \
  --api-key <base64-encoded-key:secret> \
  --destination Snowflake_East_US2 \
  --connection acumatica_default_extras \
  --configuration configuration.json
```

Connections are paused by default after deploy — start the first sync from the
Fivetran dashboard.

---

## Handling Endpoint Changes

Acumatica's REST endpoints evolve over time. Here's how to handle each kind of
change:

### 1. New fields appear on an existing entity
**No action needed.** The connector calls `normalise_record()` which iterates
every field Acumatica returns — new ones land automatically. Fivetran's schema
drift handling adds the new column to Snowflake on the next sync.

Example: Acumatica adds `EstimatedCompletionDate` to `ProductionOrder`. After
the next sync, you'll see `ESTIMATED_COMPLETION_DATE` (snake_case'd) in the
Snowflake table. No code change.

### 2. Fields are removed or renamed
**No code change for the connector**, but downstream models break. Fivetran
keeps the column with NULLs going forward (it doesn't drop columns). If a
field was renamed, both old and new will appear: old populated until the
change date, new populated after. Update dbt models to handle the transition,
or coalesce both.

### 3. New entity needs to be synced
Add an entry to the `ENTITIES` list in the relevant `connector.py`:

```python
{
    "name":     "my_new_table",      # snake_case — becomes Snowflake table name
    "endpoint": "MyNewEntity",       # Exact Acumatica endpoint segment
    # Optional: child collections retrieved via $expand
    "children": [
        {"key": "Lines", "table": "my_new_table_line"},
    ],
},
```

Then redeploy:
```bash
fivetran deploy --api-key ... --destination ... --connection acumatica_manufacturing --configuration configuration.json
```

The schema function picks up the new entity automatically. First sync after
deploy will populate the new table.

### 4. New child collection on an existing entity
Add it to that entity's `children` list. The connector concatenates all
`children[].key` values into a single comma-separated `$expand` parameter, so
a single API call returns parent + every child collection.

```python
{
    "name": "sales_order",
    "endpoint": "SalesOrder",
    "children": [
        {"key": "Details",       "table": "sales_order_detail"},
        {"key": "Shipments",     "table": "sales_order_shipment"},
        {"key": "TaxDetails",    "table": "sales_order_tax_detail"},
        {"key": "NewCollection", "table": "sales_order_new_thing"},  # ← added
    ],
},
```

### 5. Existing child collection renamed (e.g. `Details` → `Lines`)
Update the `key` in `children` to the new name. Acumatica will return HTTP 500
on `$expand` with the old name — the sync will fail loudly until corrected.
Fivetran will retain the old child table with stale data until you drop or
ignore it downstream.

### 6. Acumatica API version upgrade (e.g. `24.200.001` → `25.100.001`)
1. Update `api_version` in `configuration.json`.
2. Run a local debug sync — most entities should still work since the URL
   structure is the same.
3. Watch for 404s or 500s on specific entities; some collection names change
   between versions.
4. Probe new versions for available `$expand` collections — see the
   `default-extras` enumeration approach (probe entities with common
   `$expand` candidates and record what returns 200).
5. Deploy.

### 7. Acumatica entity has new custom fields you want
Acumatica wraps custom fields in a `custom` field on each record. By default
the connector flattens this as a JSON string. If you want individual columns,
add `$custom=Entity.FieldName,Entity.OtherField` to the request — but this
requires the field's exact internal name from Acumatica (visible in the
Customization Project metadata).

### 8. OAuth client gets rotated / replaced
1. Update `client_id` and `client_secret` in `configuration.json`.
2. Re-do the Postman auth dance to get a fresh `refresh_token`.
3. Redeploy.

The connector falls back to using `configuration.json`'s `refresh_token` if
the value in Fivetran state fails — so a fresh deploy with new credentials
recovers automatically.

### 9. Acumatica endpoint name changes (e.g. `/Manufacturing/` → `/MANUFACTURING/`)
Edit the `base_url` construction in `update()`:

```python
base_url = f"{acumatica_url}/entity/MANUFACTURING/{api_version}"
```

We hit this exact case at deploy time — Acumatica's discovery endpoint
returned `MANUFACTURING` (uppercase) as the canonical name even though the
docs listed it as `Manufacturing`.

---

## Verifying a Sync

```sql
-- All synced tables and freshness:
SELECT table_schema, table_name, row_count, last_altered
FROM bt_raw.information_schema.tables
WHERE table_schema IN ('ACUMATICA_MANUFACTURING','ACUMATICA_DEFAULT_EXTRAS')
ORDER BY table_schema, table_name;

-- Last sync time per table:
SELECT MAX(_fivetran_synced) FROM bt_raw.acumatica_manufacturing.bill_of_material;
```

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `invalid_grant` on token endpoint | Refresh token was rotated by another sync (local *or* Fivetran). Re-auth in Postman, update config. |
| `invalid_client` on token endpoint | Wrong client_id / secret, or the OAuth client was deleted/disabled in Acumatica. |
| `unauthorized_client` | The OAuth grant type isn't enabled on this client. Need to enable it in Acumatica's Connected Applications screen (Authorization Code + Refresh Token). |
| `401` mid-sync, then recovers | Access token expired during a long sync. The connector re-mints; not a real issue. |
| `401` mid-sync, no recovery | Refresh token rotation got out of sync. Re-auth and redeploy. |
| `404` on an endpoint | Entity not available in this `api_version`. Try a different version, or remove the entity from `ENTITIES`. |
| `500` on `$expand=X` | The named collection doesn't exist on this entity in this version. Probe for the correct name. |
| Sync runs but a specific table is empty | Either the source data really is empty, OR the connector hit a non-fatal exception per entity. Check Fivetran's connector logs. |

---

## Files in this repo

```
acumatica-manufacturing-api/
├── connector.py              # Manufacturing connector
├── configuration.json        # Manufacturing config (gitignored)
├── requirements.txt
├── default-extras/
│   ├── connector.py          # Default endpoint connector
│   ├── configuration.json    # Default config (gitignored)
│   └── requirements.txt
├── README.md                 # this file
└── .gitignore
```
