# /// script
# requires-python = ">=3.11"
# dependencies = ["websockets>=12"]
# ///
"""24-hour AIS coverage test for the Pilbara ports. Saves every message as raw JSON lines."""

import asyncio
import json
import os
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import websockets

API_KEY = os.environ["AISSTREAM_API_KEY"]
URL = "wss://stream.aisstream.io/v0/stream"
BBOX = [[[-19.6, 114.6], [-22.2, 119.4]]]  # [lat, lon] corners: Onslow/Ashburton to Port Hedland
HOURS = 24
OUT_DIR = Path("data/raw/ais_coverage")


async def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"ais_{datetime.now(UTC):%Y%m%dT%H%M%SZ}.jsonl"
    end = datetime.now(UTC) + timedelta(hours=HOURS)
    counts, ships, reconnects = Counter(), set(), 0
    last_report = time.monotonic()
    sub = {
        "APIKey": API_KEY,
        "BoundingBoxes": BBOX,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    with out_path.open("a") as out:
        while datetime.now(UTC) < end:
            try:
                async with websockets.connect(URL, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(sub))
                    print(f"Connected; writing to {out_path}", flush=True)
                    async for raw in ws:
                        msg = json.loads(raw)
                        msg["_received_utc"] = datetime.now(UTC).isoformat()
                        out.write(json.dumps(msg) + "\n")
                        counts[msg.get("MessageType", "unknown")] += 1
                        ships.add(msg.get("MetaData", {}).get("MMSI"))
                        if sum(counts.values()) == 1:
                            print("First message received", flush=True)
                        if time.monotonic() - last_report > 600:
                            out.flush()
                            print(
                                f"{datetime.now():%H:%M} {dict(counts)} "
                                f"ships={len(ships)} reconnects={reconnects}",
                                flush=True,
                            )
                            last_report = time.monotonic()
                        if datetime.now(UTC) >= end:
                            break
            except Exception as e:
                reconnects += 1
                print(f"Connection dropped ({e!r}); reconnecting in 10 s", flush=True)
                await asyncio.sleep(10)

    print(f"Done. {dict(counts)} ships={len(ships)} reconnects={reconnects} file={out_path}")


asyncio.run(main())
