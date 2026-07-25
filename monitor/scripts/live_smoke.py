"""Run ONE real check cycle against the production gateway and all providers.

Not part of CI (it needs the network and takes ~1-2 minutes). Run it before
deploying and whenever a pinned query is changed:

    uv run python scripts/live_smoke.py
"""

import asyncio
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from cycle import run_cycle  # noqa: E402
from derive import derive_status  # noqa: E402
from endpoints_config import ENDPOINTS  # noqa: E402
from storage import Store  # noqa: E402

DEFAULT_GATEWAY = "https://sdmx-mcp-gateway-production.up.railway.app/mcp"


async def main() -> int:
    gateway_url = os.getenv("GATEWAY_URL", DEFAULT_GATEWAY)
    store = Store(Path(tempfile.mkdtemp()) / "smoke.db")
    print("Running one live cycle against " + gateway_url + " ...")
    await run_cycle(store, ENDPOINTS, gateway_url)
    latest = store.latest_cycle()
    assert latest is not None
    print(
        "gateway_up=" + str(latest["gateway_up"])
        + " latency_ms=" + str(latest["gateway_latency_ms"])
    )
    if latest["drift"]:
        print("drift: " + latest["drift"])
    by_key: dict[str, list[dict]] = {}
    for row in latest["results"]:
        by_key.setdefault(row["endpoint_key"], []).append(row)
    failures = 0
    for key in sorted(by_key):
        status, reason = derive_status(by_key[key], latest["gateway_up"])
        if status != "healthy":
            failures += 1
        print(f"{key:9s} {status:14s} {reason}")
        for row in by_key[key]:
            if not row["ok"] and not row["skipped"]:
                print("          " + row["path"] + "/" + row["kind"]
                      + ": " + str(row["error"]))
    print(str(len(by_key)) + " endpoints, " + str(failures) + " not healthy")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
