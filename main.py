from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from collections import deque
from typing import Optional, Dict, Any, List
import os

app = FastAPI(title="Signal Agent API")

FALLBACK_SECRET = "claus-2026-xau-01!"
SECRET = os.getenv("SECRET_KEY") or FALLBACK_SECRET
QUEUE_MAX = int(os.getenv("QUEUE_MAX", "50"))

# Optional: Comma-separated list in Render env, e.g.:
# CONSUMERS_XAUUSD="EA61001,EA61002"
# CONSUMERS_BTCUSD="EA62001,EA62002"
def expected_consumers_for(sym: str) -> List[str]:
    key = f"CONSUMERS_{sym}"
    raw = (os.getenv(key, "") or "").strip()
    if not raw:
        return []  # empty => "no strict list" (see behavior below)
    return [p.strip() for p in raw.split(",") if p.strip()]

class TVSignal(BaseModel):
    key: str
    symbol: str
    action: str  # BUY / SELL
    ts: Optional[str] = None
    id: Optional[str] = None

STATE: Dict[str, Dict[str, Any]] = {}

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def norm_symbol(s: Optional[str]) -> str:
    return (s or "").strip().upper()

def norm_action(a: Optional[str]) -> str:
    return (a or "").strip().upper()

def norm_consumer(c: Optional[str]) -> str:
    return (c or "").strip()

def ensure_sym(sym: str) -> None:
    if sym not in STATE:
        STATE[sym] = {
            "latest": None,
            "updated_utc": None,
            "queue": deque(maxlen=QUEUE_MAX),
            # NEW: per-consumer ack map for CURRENT latest
            # consumer -> updated_utc they acked
            "acks": {},  # Dict[str,str]
        }

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

    # NEW: reset per-consumer acks for the new latest
    STATE[sym]["acks"] = {}

    return {"ok": True, "symbol": sym, "updated_utc": t}

@app.get("/latest")
def latest(symbol: str, consumer: Optional[str] = None):
    sym = norm_symbol(symbol)
    c = norm_consumer(consumer)

    if sym not in STATE or STATE[sym]["latest"] is None:
        return {"symbol": sym, "signal": None, "updated_utc": None}

    u = STATE[sym]["updated_utc"]
    if not u:
        return {"symbol": sym, "signal": None, "updated_utc": None}

    # NEW: if consumer provided and has already acked this updated_utc -> hide it
    if c:
        acks: Dict[str, str] = STATE[sym].get("acks", {})
        if acks.get(c) == u:
            return {"symbol": sym, "signal": None, "updated_utc": None}

    return {"symbol": sym, "signal": STATE[sym]["latest"], "updated_utc": u}

@app.post("/ack")
def ack(symbol: str, updated_utc: str, consumer: str):
    sym = norm_symbol(symbol)
    u = (updated_utc or "").strip()
    c = norm_consumer(consumer)

    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")
    if not u:
        raise HTTPException(status_code=400, detail="bad updated_utc")
    if not c:
        raise HTTPException(status_code=400, detail="bad consumer")
    if sym not in STATE:
        raise HTTPException(status_code=404, detail="unknown symbol")

    # record per-consumer ack
    acks: Dict[str, str] = STATE[sym].setdefault("acks", {})
    acks[c] = u

    cleared = False
    # Clear latest ONLY if this ack matches the current latest updated_utc AND all expected consumers acked
    cur_u = STATE[sym].get("updated_utc")

    if cur_u == u:
        exp = expected_consumers_for(sym)

        if exp:
            # strict mode: require all expected consumers to ack current updated_utc
            all_ok = all(acks.get(x) == cur_u for x in exp)
            if all_ok:
                STATE[sym]["latest"] = None
                STATE[sym]["updated_utc"] = None
                cleared = True
        else:
            # non-strict mode: do NOT auto-clear (broadcast stays available)
            # You can add a TTL cleaner later if desired.
            cleared = False

    return {
        "ok": True,
        "symbol": sym,
        "consumer": c,
        "ack": u,
        "cleared_latest": cleared,
        "expected_consumers": expected_consumers_for(sym),
        "acks_count": len(STATE[sym].get("acks", {})),
    }

@app.get("/debug/state")
def debug_state():
    out = {}
    for sym, v in STATE.items():
        out[sym] = {
            "updated_utc": v.get("updated_utc"),
            "latest": v.get("latest"),
            "acks": v.get("acks", {}),
            "expected_consumers": expected_consumers_for(sym),
            "queue_len": len(v.get("queue", [])),
        }
    return out
