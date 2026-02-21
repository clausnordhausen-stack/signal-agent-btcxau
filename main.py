from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from collections import deque
import sqlite3
import os
from threading import Lock

app = FastAPI(title="Signal Agent API")

# =====================================================
# CONFIG
# =====================================================
SECRET = os.getenv("SECRET_KEY", "claus-2026-xau-01!")
QUEUE_MAX = int(os.getenv("QUEUE_MAX", "50"))
DB_PATH = os.getenv("DB_PATH", "data.db")
_db_lock = Lock()

# =====================================================
# MODELS
# =====================================================
class TVSignal(BaseModel):
    key: str
    symbol: str
    action: str
    ts: Optional[str] = None
    id: Optional[str] = None

class DealEvent(BaseModel):
    account: Optional[str] = None
    symbol: str
    magic: Optional[int] = None
    ticket: Optional[int] = None
    deal_id: Optional[int] = None
    type: str
    lots: Optional[float] = None

    open_time: Optional[str] = None
    close_time: Optional[str] = None
    open_price: Optional[float] = None
    close_price: Optional[float] = None

    sl: Optional[float] = None
    tp: Optional[float] = None

    profit: Optional[float] = None
    commission: Optional[float] = None
    swap: Optional[float] = None
    comment: Optional[str] = None

    # 🔥 NEW
    risk_usd: Optional[float] = None
    r_multiple: Optional[float] = None

# =====================================================
# STATE (Signals)
# =====================================================
STATE: Dict[str, Dict[str, Any]] = {}

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def norm_symbol(s: Optional[str]) -> str:
    return (s or "").strip().upper()

def norm_action(a: Optional[str]) -> str:
    return (a or "").strip().upper()

def ensure_sym(sym: str):
    if sym not in STATE:
        STATE[sym] = {
            "latest": None,
            "updated_utc": None,
            "queue": deque(maxlen=QUEUE_MAX),
            "ack": None
        }

# =====================================================
# DATABASE
# =====================================================
def db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def db_init():
    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_utc TEXT NOT NULL,
            account TEXT,
            symbol TEXT NOT NULL,
            magic INTEGER,
            ticket INTEGER,
            deal_id INTEGER,
            type TEXT NOT NULL,
            lots REAL,
            open_time TEXT,
            close_time TEXT,
            open_price REAL,
            close_price REAL,
            sl REAL,
            tp REAL,
            profit REAL,
            commission REAL,
            swap REAL,
            comment TEXT,

            risk_usd REAL,
            r_multiple REAL
        )
        """)

        # Auto-migration (safe if column exists)
        for col in ["risk_usd", "r_multiple"]:
            try:
                cur.execute(f"ALTER TABLE deals ADD COLUMN {col} REAL")
            except Exception:
                pass

        conn.commit()
        conn.close()

@app.on_event("startup")
def startup():
    db_init()

# =====================================================
# ROOT
# =====================================================
@app.get("/")
def root():
    return {"status": "Signal Agent API is running", "version": "main.py"}

# =====================================================
# TRADINGVIEW → SIGNAL
# =====================================================
@app.post("/tv")
def tv_webhook(sig: TVSignal):
    if sig.key != SECRET:
        raise HTTPException(status_code=401, detail="bad key")

    sym = norm_symbol(sig.symbol)
    act = norm_action(sig.action)

    if act not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="bad action")

    ensure_sym(sym)
    t = now_utc()

    payload = {"symbol": sym, "action": act, "ts": sig.ts, "id": sig.id}
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

@app.post("/ack")
def ack(symbol: str, updated_utc: str):
    sym = norm_symbol(symbol)
    if sym not in STATE:
        raise HTTPException(status_code=404, detail="unknown symbol")

    STATE[sym]["ack"] = updated_utc
    if STATE[sym]["updated_utc"] == updated_utc:
        STATE[sym]["latest"] = None
        STATE[sym]["updated_utc"] = None

    return {"ok": True}

# =====================================================
# MT5 → DEAL INGEST
# =====================================================
@app.post("/deal")
def ingest_deal(d: DealEvent):
    sym = norm_symbol(d.symbol)
    typ = norm_action(d.type)

    if typ not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="bad type")

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO deals(
            received_utc, account, symbol, magic, ticket, deal_id, type, lots,
            open_time, close_time, open_price, close_price, sl, tp,
            profit, commission, swap, comment,
            risk_usd, r_multiple
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_utc(), d.account, sym, d.magic, d.ticket, d.deal_id, typ, d.lots,
            d.open_time, d.close_time, d.open_price, d.close_price, d.sl, d.tp,
            d.profit, d.commission, d.swap, d.comment,
            d.risk_usd, d.r_multiple
        ))

        conn.commit()
        conn.close()

    return {"ok": True}

# =====================================================
# DEALS LIST
# =====================================================
@app.get("/deals")
def list_deals(limit: int = 20):
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            "SELECT * FROM deals ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
        conn.close()

    return {"items": [dict(r) for r in rows], "count": len(rows)}

# =====================================================
# SUMMARY
# =====================================================
@app.get("/summary")
def summary():
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            "SELECT * FROM deals ORDER BY id DESC LIMIT 10"
        ).fetchall()
        conn.close()

    return {
        "last_deals": [dict(r) for r in rows],
        "signals": STATE
    }
