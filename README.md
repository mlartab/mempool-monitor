# MEMPOOL.WATCH
> Real-time blockchain mempool monitor with threat detection.
> Built for security researchers, wallet teams, and DeFi protocols.

## The Problem
Every day, millions of dollars are lost to wallet drainers, 
sandwich attacks, and suspicious high-value transfers — 
often visible in the mempool *before* they confirm.
MEMPOOL.WATCH surfaces these threats in real time.

## Live Features (Ethereum — Solana expansion in progress)
- Live pending transaction stream via Alchemy RPC
- HIGH VALUE alerts (> 10 ETH transfers)
- Token drainer signature detection
- MEV / sandwich attack pattern flagging  ← add this
- Gas price history chart
- Risk tiers: Critical / High / Medium / Low
- Filters: ALL / CRITICAL / HIGH / DRAINER

## Roadmap
- [ ] Solana mempool integration (Alchemy RPC)
- [ ] Webhook alerts (Telegram / Discord bot)
- [ ] Public API for wallet apps to query risk scores
- [ ] MEV bundle detection

## Stack
- Python + FastAPI + WebSockets
- Web3.py / Solana-py
- SQLite (persistent storage)
- Vanilla JS — zero framework overhead

## Setup
```bash
git clone https://github.com/YOURNAME/mempool-monitor
cd mempool-monitor
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env — add your key from https://alchemy.com
```

## Run
```bash
python mempool_server.py
# Then open mempool_dashboard.html in browser
```

## Stack
- Python + FastAPI + WebSockets
- Web3.py — Ethereum RPC
- SQLite — persistent storage
- Vanilla JS dashboard — no framework needed
