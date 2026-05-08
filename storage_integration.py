"""
HOW TO PLUG mempool_storage.py INTO mempool_server.py
Add these 3 lines and your data is permanently saved.
──────────────────────────────────────────────────────
"""

# ── 1. At top of mempool_server.py, add this import ───────────────────
from mempool_storage import db, exporter, AutoExporter

# ── 2. In the classify_tx function, after building the `classified` dict,
#       add one line:
#       db.save_tx(classified)
#       — full example below ─────────────────────────────────────────

async def handle_tx_with_storage(tx_hash, w3):
    """Drop-in replacement for the handle_tx loop body in mempool_server.py"""
    try:
        tx = await w3.eth.get_transaction(tx_hash)
        if not tx:
            return

        classified = classify_tx(dict(tx), w3)   # existing function

        # ✅ SAVE TO DATABASE — one line added
        db.save_tx(classified)

        # ✅ SAVE ALERT if high risk — one line added
        if classified["risk"] in ("critical", "high"):
            db.save_alert(classified)

        # existing stats + broadcast code stays the same ...
        stats["total_seen"] += 1
        recent_txs.appendleft(classified)
        await broadcast({"type": "tx", "data": classified, "stats": stats})

    except Exception:
        pass


# ── 3. In the startup event, add the auto-exporter task ──────────────
# @app.on_event("startup")
# async def startup():
#     asyncio.create_task(mempool_listener())
#     asyncio.create_task(AutoExporter(exporter, interval_minutes=15).run())  # ← add this


# ── OPTIONAL: Add these API endpoints to mempool_server.py ────────────
from fastapi import FastAPI
app = FastAPI()

@app.get("/api/export")
async def trigger_export(hours: int = 24):
    """Manually trigger an export from the dashboard or curl."""
    paths = exporter.export_all(hours=hours)
    return {"status": "exported", "files": paths}

@app.get("/api/db/stats")
async def db_stats():
    """Total counts from the persistent database."""
    return db.get_stats()

@app.get("/api/db/search")
async def db_search(wallet: str = "", func: str = "", risk: str = "", limit: int = 100):
    """Search stored transactions by wallet, function, or risk level."""
    return db.search(wallet=wallet, func=func, risk=risk, limit=limit)

@app.get("/api/db/alerts")
async def db_alerts(hours: int = 24):
    """Fetch all high/critical alerts from the last N hours."""
    return db.get_alerts(hours=hours)
