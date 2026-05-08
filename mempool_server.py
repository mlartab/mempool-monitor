"""
Mempool Monitor — Backend Server
Streams live Ethereum mempool data to the dashboard via WebSocket.

Install: pip install fastapi uvicorn web3 websockets python-dotenv
Run:     python mempool_server.py
"""

import asyncio
import json
import os
import time
from collections import deque
from datetime import datetime
from typing import Set

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from web3 import AsyncWeb3, AsyncHTTPProvider
import websockets

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────
RPC_WS = os.getenv(
    "RPC_WS",
    "wss://eth-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_KEY"
)

# Known drainer / suspicious function signatures
DRAINER_SIGS = {
    "23b872dd": "transferFrom",
    "095ea7b3": "approve",
    "a9059cbb": "transfer",
    "e1fffcc4": "deposit",
    "2e1a7d4d": "withdraw",
    "f242432a": "safeTransferFrom(NFT)",
    "a22cb465": "setApprovalForAll",
}

HIGH_VALUE_ETH = 10   # flag transfers > 10 ETH
HIGH_GAS_GWEI  = 200  # flag gas price > 200 Gwei

# ── App State ───────────────────────────────────────────────────────────
app = FastAPI(title="Mempool Monitor API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

clients:      Set[WebSocket]  = set()
recent_txs:   deque           = deque(maxlen=500)
stats = {
    "total_seen":    0,
    "high_value":    0,
    "drainer_flags": 0,
    "high_gas":      0,
    "eth_volume":    0.0,
    "start_time":    time.time(),
}

# ── Helpers ─────────────────────────────────────────────────────────────
def classify_tx(tx: dict, w3: AsyncWeb3) -> dict:
    """Decode and classify a raw transaction."""
    value_eth  = float(w3.from_wei(tx.get("value", 0), "ether"))
    gas_price  = float(w3.from_wei(tx.get("gasPrice", 0), "gwei"))
    input_data = tx.get("input", b"")

    if isinstance(input_data, bytes):
        hex_data = input_data.hex()
    else:
        hex_data = str(input_data).replace("0x", "")

    func_sig  = hex_data[:8] if len(hex_data) >= 8 else ""
    func_name = DRAINER_SIGS.get(func_sig, "")

    flags = []
    risk  = "low"

    if value_eth > HIGH_VALUE_ETH:
        flags.append("HIGH_VALUE")
        risk = "high"

    if func_name in ("approve", "setApprovalForAll", "transferFrom"):
        flags.append("TOKEN_DRAIN_SIG")
        risk = "critical" if risk != "high" else "critical"

    if gas_price > HIGH_GAS_GWEI:
        flags.append("HIGH_GAS")
        if risk == "low":
            risk = "medium"

    if value_eth == 0 and func_name == "":
        flags.append("UNKNOWN_CALL")

    return {
        "hash":       tx["hash"].hex() if hasattr(tx["hash"], "hex") else tx["hash"],
        "from":       tx.get("from", ""),
        "to":         tx.get("to", "") or "CONTRACT_CREATE",
        "value_eth":  round(value_eth, 6),
        "gas_gwei":   round(gas_price, 2),
        "func_sig":   func_sig,
        "func_name":  func_name or "unknown",
        "flags":      flags,
        "risk":       risk,
        "timestamp":  datetime.utcnow().isoformat(),
    }


async def broadcast(message: dict):
    """Send a message to all connected dashboard clients."""
    if not clients:
        return
    data = json.dumps(message)
    dead = set()
    for ws in clients:
        try:
            await ws.send_text(data)
        except Exception:
            dead.add(ws)
    clients.difference_update(dead)


# ── Mempool Listener ────────────────────────────────────────────────────
async def mempool_listener():
    http_url = RPC_WS.replace("wss://", "https://").replace("ws://", "http://")
    w3 = AsyncWeb3(AsyncHTTPProvider(http_url))
    print("[*] Connecting to mempool: " + RPC_WS[:40] + "...")
    while True:
        try:
            async with websockets.connect(RPC_WS, ping_interval=20) as ws:
                await ws.send('{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newPendingTransactions"]}')
                resp = json.loads(await ws.recv())
                sub_id = resp.get("result", "?")
                print("[OK] Connected. Streaming mempool... sub=" + str(sub_id))
                await broadcast({"type": "status", "status": "connected"})
                async for raw in ws:
                    msg = json.loads(raw)
                    tx_hash = msg.get("params", {}).get("result")
                    if not tx_hash:
                        continue
                    try:
                        tx = await w3.eth.get_transaction(tx_hash)
                        if not tx:
                            continue
                        classified = classify_tx(dict(tx), w3)
                        stats["total_seen"]    += 1
                        stats["eth_volume"]    += classified["value_eth"]
                        if "HIGH_VALUE"        in classified["flags"]: stats["high_value"]    += 1
                        if "TOKEN_DRAIN_SIG"   in classified["flags"]: stats["drainer_flags"] += 1
                        if "HIGH_GAS"          in classified["flags"]: stats["high_gas"]      += 1
                        recent_txs.appendleft(classified)
                        await broadcast({
                            "type": "tx",
                            "data": classified,
                            "stats": {**stats, "eth_volume": round(stats["eth_volume"], 4)},
                        })
                    except Exception:
                        pass
        except Exception as e:
            print("[!] Connection lost: " + str(e) + ". Reconnecting in 5s...")
            await broadcast({"type": "status", "status": "reconnecting"})
            await asyncio.sleep(5)


# ── API Routes ──────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    asyncio.create_task(mempool_listener())


@app.get("/api/stats")
async def get_stats():
    return {**stats, "eth_volume": round(stats["eth_volume"], 4)}


@app.get("/api/recent")
async def get_recent(limit: int = 50):
    return list(recent_txs)[:limit]


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    print(f"[+] Dashboard client connected. Total: {len(clients)}")

    # Send snapshot on connect
    await ws.send_text(json.dumps({
        "type":   "snapshot",
        "recent": list(recent_txs)[:50],
        "stats":  {**stats, "eth_volume": round(stats["eth_volume"], 4)},
    }))

    try:
        while True:
            await ws.receive_text()   # keep alive
    except WebSocketDisconnect:
        clients.discard(ws)
        print(f"[-] Client disconnected. Total: {len(clients)}")


# ── Entry Point ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════╗
║   MEMPOOL MONITOR — Backend Server       ║
║   Dashboard → http://localhost:8000      ║
║   WebSocket → ws://localhost:8000/ws     ║
╚══════════════════════════════════════════╝

  1. Get free Alchemy key: https://alchemy.com
  2. Set RPC_WS in .env file or env var
  3. Open dashboard.html in your browser
""")
    uvicorn.run("mempool_server:app", host="0.0.0.0", port=8000, reload=False)
