#!/usr/bin/env python3
"""
Standalone debug script for the Binance USDTF trade WebSocket.

Usage examples
--------------
# single symbol, default aggTrade:
  python debug_futures_trade_ws.py --symbol BTCUSDT

# single symbol, @trade mode:
  python debug_futures_trade_ws.py --symbol BTCUSDT --mode trade

# multiple symbols, 90-second window:
  python debug_futures_trade_ws.py --symbols BTCUSDT,ETHUSDT,BNBUSDT --timeout 90

# spot exchange (stream.binance.com):
  python debug_futures_trade_ws.py --symbol BTCUSDT --venue BINANCE_SPOT

Outputs
-------
- WebSocket URL
- Connection status (connected / error)
- First 5 frame types received (TEXT / PING / CLOSE / ERROR / …)
- First TEXT payload (pretty-printed)
- Any exception / timeout reason
- Total elapsed seconds
"""

import argparse
import asyncio
import json
import sys
import time
from typing import List

import aiohttp

# ── Binance WS URL helpers ────────────────────────────────────────────────────

_FUTURES_BASE = "wss://fstream.binance.com/stream?streams="
_SPOT_BASE = "wss://stream.binance.com:9443/stream?streams="

_PING_INTERVAL = 20  # seconds


def _build_url(venue: str, symbols: List[str], mode: str) -> str:
    """Return the combined stream URL for the given venue, symbols, and mode."""
    if venue == "BINANCE_SPOT":
        stream_names = [f"{s.lower()}@trade" for s in symbols]
        return _SPOT_BASE + "/".join(stream_names)
    else:  # BINANCE_USDTF
        stream_names = [f"{s.lower()}@{mode}" for s in symbols]
        return _FUTURES_BASE + "/".join(stream_names)


# ── Core probe ───────────────────────────────────────────────────────────────

async def probe(
    url: str,
    timeout_sec: float = 30.0,
    max_frames: int = 5,
) -> None:
    """Connect to *url*, collect up to *max_frames* frames, then exit."""
    print(f"\nURL   : {url}")
    print(f"Params: timeout={timeout_sec}s  max_text_frames={max_frames}\n")

    t0 = time.monotonic()
    frame_types: List[str] = []
    first_text_payload = None
    error_message: str | None = None

    try:
        connector = aiohttp.TCPConnector(ssl=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            try:
                ws = await asyncio.wait_for(
                    session.ws_connect(url, heartbeat=_PING_INTERVAL),
                    timeout=10.0,
                )
            except asyncio.TimeoutError:
                elapsed = time.monotonic() - t0
                print(f"[FAIL]  TCP/WS handshake timed out after {elapsed:.1f}s")
                return
            except Exception as exc:
                elapsed = time.monotonic() - t0
                print(f"[FAIL]  Could not connect: {exc!r}  (elapsed={elapsed:.1f}s)")
                return

            print(f"[OK]    Connected in {time.monotonic() - t0:.2f}s")

            text_count = 0
            deadline = t0 + timeout_sec
            try:
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        error_message = f"timeout after {timeout_sec:.0f}s"
                        break
                    try:
                        msg = await asyncio.wait_for(ws.receive(), timeout=min(remaining, 1.0))
                    except asyncio.TimeoutError:
                        continue

                    type_name = msg.type.name  # e.g. TEXT, PING, CLOSE, ERROR
                    frame_types.append(type_name)

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        text_count += 1
                        if first_text_payload is None:
                            first_text_payload = msg.data
                        if text_count >= max_frames:
                            break

                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        error_message = f"WS error: {ws.exception()}"
                        break

                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.CLOSING,
                    ):
                        error_message = f"server closed ({msg.data!r})"
                        break
            finally:
                await ws.close()
    except Exception as exc:
        elapsed = time.monotonic() - t0
        print(f"[FAIL]  Unexpected error: {exc!r}  (elapsed={elapsed:.1f}s)")
        return

    elapsed = time.monotonic() - t0

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\nFrame types received : {frame_types if frame_types else '(none)'}")
    text_received = any(t == "TEXT" for t in frame_types)

    if first_text_payload is not None:
        try:
            parsed = json.loads(first_text_payload)
            pretty = json.dumps(parsed, indent=2)
        except Exception:
            pretty = first_text_payload[:500]
        print(f"\nFirst TEXT payload:\n{pretty}")
    else:
        print("\nFirst TEXT payload  : (none received)")

    if error_message:
        print(f"\nStop reason         : {error_message}")

    print(f"Elapsed             : {elapsed:.2f}s")

    if text_received:
        print("\nRESULT: OK — exchange is sending trade messages on this URL/mode.")
    else:
        print(
            "\nRESULT: SILENT — no TEXT frames within timeout.\n"
            "  Possibilities:\n"
            "  1. Wrong stream mode  (try --mode trade  or  --mode aggTrade)\n"
            "  2. Binance endpoint routing change for this IP\n"
            "  3. Firewall / NAT blocking WebSocket data frames after handshake\n"
            "  4. Symbol is not tradable (low volume / suspended)\n"
        )
        sys.exit(1)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Debug Binance futures/spot trade WebSocket connectivity."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--symbol",
        metavar="SYM",
        default="BTCUSDT",
        help="Single symbol to subscribe to (default: BTCUSDT)",
    )
    group.add_argument(
        "--symbols",
        metavar="A,B,C",
        help="Comma-separated list of symbols (overrides --symbol)",
    )
    parser.add_argument(
        "--mode",
        choices=["aggTrade", "trade"],
        default="aggTrade",
        help="Stream mode for futures venue (default: aggTrade)",
    )
    parser.add_argument(
        "--venue",
        choices=["BINANCE_USDTF", "BINANCE_SPOT"],
        default="BINANCE_USDTF",
        help="Exchange venue (default: BINANCE_USDTF)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        metavar="SEC",
        help="Seconds to wait for the first text frame (default: 30)",
    )
    parser.add_argument(
        "--frames",
        type=int,
        default=5,
        metavar="N",
        help="Stop after receiving N TEXT frames (default: 5)",
    )

    args = parser.parse_args()
    symbols: List[str]
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = [args.symbol.upper()]

    url = _build_url(args.venue, symbols, args.mode)
    asyncio.run(probe(url, timeout_sec=args.timeout, max_frames=args.frames))


if __name__ == "__main__":
    main()
