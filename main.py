from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from collections import deque
from typing import Optional, Dict, Any

app = FastAPI(title="Signal Agent API")

SECRET = "claus-2026-xau-01!"   # dein Key
QUEUE_MAX = 50                  # pro Symbol die letzten N Signale

class TVSignal(BaseModel):
    key: str
    symbol: str
    action: str  # BUY / SELL
    ts: Optional[str] = None
    id: Optional[str] = None

# symbol -> {"latest": {...}, "updated_utc": "...", "queue": deque([...]), "ack": "..."}
STATE: Dict[str, Dict[str, Any]] = {}

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def norm_symbol(s: str) -> str:
    return (s or "").strip().upper()

def norm_action(a: str) -> str:
    return (a or "").strip().upper()

def ensure_sym(sym: str) -> None:
    if sym not in STATE:
        STATE[sym] = {
            "latest": None,
            "updated_utc": None,
            "queue": deque(maxlen=QUEUE_MAX),
            "ack": None
        }

@app.get("/")
def root():
    return {"status": "Signal Agent API is running"}

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

    ensure_sym(sym)

    payload = {"symbol": sym, "action": act, "ts": sig.ts, "id": sig.id}
    t = now_utc()

    STATE[sym]["latest"] = payload
    STATE[sym]["updated_utc"] = t
    STATE[sym]["queue"].append({"signal": payload, "updated_utc": t})

    return {"ok": True, "symbol": sym, "updated_utc": t}

@app.get("/latest")
def latest(symbol: str):
    sym = norm_symbol(symbol)
    if sym not in STATE or STATE[sym]["latest"] is None:
        return {"symbol": sym, "signal": None, "updated_utc": None}

    return {
        "symbol": sym,
        "signal": STATE[sym]["latest"],
        "updated_utc": STATE[sym]["updated_utc"]
    }

@app.get("/queue")
def queue(symbol: str):
    sym = norm_symbol(symbol)
    if sym not in STATE:
        return {"symbol": sym, "items": []}
    return {"symbol": sym, "items": list(STATE[sym]["queue"])}

# ---------------- NEW: ACK endpoint ----------------
# EA ruft auf:  POST /ack?symbol=BTCUSD&updated_utc=...
# Wirkung:
# - speichert ack
# - wenn ack == aktuellstes updated_utc -> latest wird gelöscht (damit /latest sofort None liefert)
@app.post("/ack")
def ack(symbol: str, updated_utc: str):
    sym = norm_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")
    if not updated_utc or updated_utc.strip() == "":
        raise HTTPException(status_code=400, detail="bad updated_utc")

    if sym not in STATE:
        raise HTTPException(status_code=404, detail="unknown symbol")

    updated_utc = updated_utc.strip()
    STATE[sym]["ack"] = updated_utc

    # Wenn das ACK genau das "aktuelle latest" betrifft: latest leeren
    if STATE[sym].get("updated_utc") == updated_utc:
        STATE[sym]["latest"] = None
        STATE[sym]["updated_utc"] = None

    return {"ok": True, "symbol": sym, "ack": updated_utc, "cleared_latest": (STATE[sym]["latest"] is None)}

# Optional: Ack-Status ansehen (Debug)
@app.get("/ack_status")
def ack_status(symbol: str):
    sym = norm_symbol(symbol)
    if sym not in STATE:
        return {"symbol": sym, "ack": None}
    return {"symbol": sym, "ack": STATE[sym].get("ack")}
