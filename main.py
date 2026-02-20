from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from collections import deque
from typing import Optional, Dict, Any
import os

# =========================
# APP
# =========================
app = FastAPI(title="Signal Agent API")

# =========================
# CONFIG
# =========================
FALLBACK_SECRET = "claus-2026-xau-01!"
SECRET = os.getenv("SECRET_KEY") or FALLBACK_SECRET
QUEUE_MAX = int(os.getenv("QUEUE_MAX", "50"))

print("=== SIGNAL AGENT START ===")
print("SECRET_KEY source:", "ENV" if os.getenv("SECRET_KEY") else "FALLBACK")
print("QUEUE_MAX:", QUEUE_MAX)
print("==========================")

# =========================
# MODELS
# =========================
class TVSignal(BaseModel):
    key: str
    symbol: str
    action: str  # BUY / SELL
    ts: Optional[str] = None
    id: Optional[str] = None

# =========================
# STATE (in-memory)
# =========================
STATE: Dict[str, Dict[str, Any]] = {}

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def norm_symbol(s: Optional[str]) -> str:
    return (s or "").strip().upper()

def norm_action(a: Optional[str]) -> str:
    return (a or "").strip().upper()

def ensure_symbol(sym: str) -> None:
    if sym not in STATE:
        STATE[sym] = {
            "latest": None,
            "updated_utc": None,
            "queue": deque(maxlen=QUEUE_MAX),
            "ack": None,
        }

# =========================
# ROOT
# =========================
@app.get("/")
def root():
    return {
        "status": "Signal Agent API is running",
        "secret_source": "ENV" if os.getenv("SECRET_KEY") else "FALLBACK",
    }

@app.head("/")
def head_root():
    return

# =========================
# TRADINGVIEW WEBHOOK
# =========================
@app.post("/tv")
def tv_webhook(sig: TVSignal):
    if sig.key != SECRET:
        raise HTTPException(status_code=401, detail="bad key")

    sym = norm_symbol(sig.symbol)
    act = norm_action(sig.action)

    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")
    if act not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="bad action")

    ensure_symbol(sym)

    payload = {
        "symbol": sym,
        "action": act,
        "ts": sig.ts,
        "id": sig.id,
    }

    t = now_utc()
    STATE[sym]["latest"] = payload
    STATE[sym]["updated_utc"] = t
    STATE[sym]["queue"].append({"signal": payload, "updated_utc": t})

    return {"ok": True, "symbol": sym, "updated_utc": t}

# =========================
# LATEST SIGNAL
# =========================
@app.get("/latest")
def latest(symbol: str):
    sym = norm_symbol(symbol)
    if sym not in STATE or STATE[sym]["latest"] is None:
        return {"symbol": sym, "signal": None, "updated_utc": None}

    return {
        "symbol": sym,
        "signal": STATE[sym]["latest"],
        "updated_utc": STATE[sym]["updated_utc"],
    }

# =========================
# SIGNAL QUEUE
# =========================
@app.get("/queue")
def queue(symbol: str):
    sym = norm_symbol(symbol)
    if sym not in STATE:
        return {"symbol": sym, "items": []}
    return {"symbol": sym, "items": list(STATE[sym]["queue"])}

# =========================
# ACK (Signal verbraucht)
# =========================
@app.post("/ack")
def ack(symbol: str, updated_utc: str):
    sym = norm_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")
    if not updated_utc:
        raise HTTPException(status_code=400, detail="bad updated_utc")
    if sym not in STATE:
        raise HTTPException(status_code=404, detail="unknown symbol")

    STATE[sym]["ack"] = updated_utc

    if STATE[sym]["updated_utc"] == updated_utc:
        STATE[sym]["latest"] = None
        STATE[sym]["updated_utc"] = None

    return {
        "ok": True,
        "symbol": sym,
        "ack": updated_utc,
        "cleared_latest": STATE[sym]["latest"] is None,
    }

# =========================
# DEBUG (optional, hilfreich)
# =========================
@app.get("/debug/state")
def debug_state():
    return {
        sym: {
            "latest": v["latest"],
            "updated_utc": v["updated_utc"],
            "ack": v["ack"],
            "queue_len": len(v["queue"]),
        }
        for sym, v in STATE.items()
    }
