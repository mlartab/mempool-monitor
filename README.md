# MEMPOOL.WATCH
> Live Ethereum mempool monitor with real-time threat detection dashboard.

![Dashboard](assets/dashboard-all.png)
![High Risk Filter](assets/dashboard-high.png)

## Features
- Live pending transaction stream
- HIGH VALUE alerts (> 10 ETH transfers)
- Token drainer signature detection
- Gas price history chart
- Risk breakdown (Critical / High / Medium / Low)
- Filters: ALL / CRITICAL / HIGH / DRAINER

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
