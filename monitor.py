"""Daily health check for the two Acumatica Fivetran connectors.

Run by a Windows Scheduled Task (see README). Checks both connectors against
the health rules below; on any failure it pops a desktop alert and exits 1.
Appends one line per run to monitor.log next to this script.

The Fivetran API key is read from the FIVETRAN_API_KEY env var (base64
"key:secret" form) or from a `.fivetran_key` file next to this script.
That file is gitignored — never commit it.

Health rules per connector (schedule is every 6 hours):
  1. setup_state == "connected"
  2. not paused
  3. succeeded_at within the last 12 hours (two missed windows = stale)
  4. failed_at not more recent than succeeded_at (latest sync failed)
  5. sync_frequency still 360 (flag silent schedule changes)

Known triage paths when this fires:
  - 400/invalid_grant in Fivetran logs -> refresh-token chain is dead:
    python reseed.py (see README, Authentication section), then redeploy.
  - succeeded_at frozen, no new failed_at -> Fivetran scheduler stalled:
    PATCH paused:true then paused:false + re-save sync_frequency.
  - This script errors with AuthFailed -> the Fivetran API key rotated;
    update .fivetran_key.
"""
import base64
import ctypes
import json
import os
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

CONNECTORS = {
    "manufacturing": "rescuer_headstone",
    "default-extras": "monoclonal_innervation",
}
STALE_AFTER_HOURS = 12
EXPECTED_FREQUENCY = 360

HERE = Path(__file__).resolve().parent
LOG_PATH = HERE / "monitor.log"


def api_key() -> str:
    key = os.environ.get("FIVETRAN_API_KEY", "").strip()
    if not key:
        f = HERE / ".fivetran_key"
        if f.exists():
            key = f.read_text().strip()
    if not key:
        raise SystemExit("No Fivetran API key: set FIVETRAN_API_KEY or create .fivetran_key")
    # Accept either the base64 form or raw key:secret
    if ":" in key:
        key = base64.b64encode(key.encode()).decode()
    return key


def fetch(connector_id: str, key: str) -> dict:
    req = urllib.request.Request(
        f"https://api.fivetran.com/v1/connectors/{connector_id}",
        headers={"Authorization": f"Basic {key}"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)["data"]


def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def check(name: str, d: dict) -> list[str]:
    problems = []
    status = d.get("status", {})
    if status.get("setup_state") != "connected":
        problems.append(f"{name}: setup_state={status.get('setup_state')} (credentials/config broken)")
    if d.get("paused"):
        problems.append(f"{name}: connector is paused")
    succeeded = parse_ts(d.get("succeeded_at"))
    failed = parse_ts(d.get("failed_at"))
    now = datetime.now(timezone.utc)
    if not succeeded:
        problems.append(f"{name}: no successful sync recorded")
    elif now - succeeded > timedelta(hours=STALE_AFTER_HOURS):
        problems.append(
            f"{name}: last success {succeeded.isoformat()} is >{STALE_AFTER_HOURS}h old "
            f"(scheduler stall or repeated failures)"
        )
    if succeeded and failed and failed > succeeded:
        problems.append(f"{name}: latest sync FAILED at {failed.isoformat()} (last success {succeeded.isoformat()})")
    if d.get("sync_frequency") != EXPECTED_FREQUENCY:
        problems.append(f"{name}: sync_frequency changed to {d.get('sync_frequency')} (expected {EXPECTED_FREQUENCY})")
    return problems


def alert(text: str) -> None:
    # 0x1040 = MB_ICONWARNING | MB_SYSTEMMODAL so it surfaces over other windows
    ctypes.windll.user32.MessageBoxW(0, text, "BigTop Acumatica connector alert", 0x1040)


def main() -> int:
    key = api_key()
    problems, summaries = [], []
    for name, cid in CONNECTORS.items():
        try:
            d = fetch(cid, key)
        except Exception as e:  # noqa: BLE001 - any fetch failure is itself an alert
            problems.append(f"{name}: Fivetran API check failed: {e}")
            continue
        problems.extend(check(name, d))
        summaries.append(f"{name} ok (succeeded {d.get('succeeded_at')})")

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    if problems:
        line = f"{stamp} UNHEALTHY: " + " | ".join(problems)
    else:
        line = f"{stamp} healthy: " + "; ".join(summaries)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)

    if problems:
        alert("Acumatica pipeline needs attention:\n\n" + "\n".join(problems)
              + "\n\nTriage notes are at the top of monitor.py.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
