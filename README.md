# Acumatica Manufacturing → Fivetran Connector

Custom Fivetran Connector SDK connector that syncs all entities from the
Acumatica **Manufacturing** endpoint (`/entity/Manufacturing/{version}/`) into
your Snowflake warehouse incrementally.

---

## Tables Synced

| Fivetran Table | Acumatica Endpoint | Sync Mode |
|---|---|---|
| `bill_of_material` | `BillOfMaterial` | Incremental (LastModifiedDateTime) |
| `bill_of_material_detail` | `BillOfMaterial` → `Details` | Incremental (via parent) |
| `production_order` | `ProductionOrder` | Incremental |
| `production_order_detail` | `ProductionOrder` → `Details` | Incremental (via parent) |
| `labor_entry` | `LaborEntry` | Incremental |
| `material_entry` | `MaterialEntry` | Incremental |
| `estimate_item` | `EstimateItem` | Incremental |
| `work_center` | `WorkCenter` | Full refresh |
| `machine` | `Machine` | Full refresh |
| `shift` | `Shift` | Full refresh |

Detail tables (`bill_of_material_detail`, `production_order_detail`) are fetched
in the same API call as the parent using `$expand`, so they don't cost extra
API requests.

---

## Prerequisites

- Python 3.11+
- Fivetran account with **Connector SDK** (Custom Connectors) enabled
- Acumatica OAuth 2.0 client credentials with access to the Manufacturing endpoint
- Acumatica instance with Manufacturing module licensed and enabled

---

## Local Setup

```bash
# 1. Clone / copy this directory
cd acumatica-manufacturing-api

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Fill in your credentials (never commit this file)
cp configuration.json configuration.json
# Edit configuration.json with your real values
```

### configuration.json fields

| Field | Description |
|---|---|
| `acumatica_url` | Base URL of your Acumatica instance, e.g. `https://myco.acumatica.com` |
| `client_id` | OAuth 2.0 client ID |
| `client_secret` | OAuth 2.0 client secret |
| `api_version` | Manufacturing endpoint version, e.g. `24.200.001` |

---

## Local Debug Run

```bash
python connector.py
```

This calls `connector.debug()` which reads `configuration.json`, runs one full
sync, and writes output to `files/` locally so you can inspect what Fivetran
would receive — no real Fivetran connection needed.

---

## Finding Your API Version

In Acumatica: **System** → **Web Service Endpoints** → filter by `Manufacturing`.
The version column shows what's available (e.g. `24.200.001`).

---

## Deploying to Fivetran

1. In the Fivetran dashboard, go to **Connectors → Add Connector → Build Your Own**
2. Select **Connector SDK (Python)**
3. Upload `connector.py` and `requirements.txt`
4. Enter the configuration values from `configuration.json` in the Fivetran UI
5. Set your sync frequency and destination schema
6. Run a manual sync to verify

---

## Incremental Sync Logic

Each incrementally-synced entity uses a per-table cursor stored in Fivetran
state:

```
state = {
  "cursor_bill_of_material": "2025-03-01T12:00:00Z",
  "cursor_production_order": "2025-03-01T11:45:00Z",
  ...
}
```

On each run, the connector filters with:
```
$filter=LastModifiedDateTime gt datetimeoffset'<cursor>'
```

After all records are processed, the cursor is updated to the newest
`LastModifiedDateTime` seen. A checkpoint is written after each entity
so a mid-sync failure doesn't restart from scratch.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `401 Unauthorized` | Wrong client_id / client_secret, or client not granted API access in Acumatica |
| `404 Not Found` on an endpoint | Entity not available in your api_version — try a different version string |
| Empty results | Manufacturing module may not be licensed, or OAuth scope doesn't include Manufacturing data |
| Field `value` appearing as JSON strings | New nested field added by Acumatica — open an issue to add explicit flattening |

---

## Adding New Entities

Add an entry to the `ENTITIES` list in `connector.py`:

```python
{
    "name": "my_new_table",         # snake_case — becomes the Snowflake table name
    "endpoint": "MyNewEndpoint",    # Acumatica endpoint segment
    "primary_key": ["ID"],          # Unique key field(s)
    "incremental": True,            # Set False for reference tables
    "expand": None,                 # "$expand" param if needed
},
```

No other changes needed — the schema and sync logic handle it automatically.
