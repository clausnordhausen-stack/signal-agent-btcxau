from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from collections import deque
from typing import Optional, Dict, Any, List
import os
import sqlite3
from threading import Lock

app = FastAPI(title="Signal Agent API")

# =========================
# CONFIG
# =========================
# Better: set SECRET_KEY in Render env vars. Fallback works for local tests.
SECRET = os.getenv("SECRET_KEY", "claus-2026-xau-01!")
QUEUE_MAX = int(os.getenv("QUEUE_MAX", "50"))

# SQLite: for real persistence on Render, mount a Disk and set DB_PATH e.g. /var/data/data.db
DB_PATH = os.getenv("DB_PATH", "data.db")
_db_lock = Lock()

# =========================
# MODELS
# =========================
class TVSignal(BaseModel):
    key: str
    symbol: str
    action: str  # BUY / SELL
    ts: Optional[str] = None
    id: Optional[str] = None

class DealEvent(BaseModel):
    account: Optional[str] = None
    symbol: str
    magic: Optional[int] = None
    ticket: Optional[int] = None
    deal_id: Optional[int] = None
    type: str  # BUY / SELL
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

# =========================
# STATE (signals)
# =========================
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

# =========================
# DB
# =========================
def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def db_init() -> None:
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
            type TEXT NOT NULL,              -- BUY/SELL
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
            comment TEXT
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_symbol_time ON deals(symbol, close_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_magic ON deals(magic)")
        conn.commit()
        conn.close()

@app.on_event("startup")
def _startup():
    db_init()

# =========================
# ROOT / HEALTH
# =========================
@app.get("/")
def root():
    return {"status": "Signal Agent API is running"}

# =========================
# SIGNAL INGEST (TradingView)
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

# ACK: EA ruft auf: POST /ack?symbol=BTCUSD&updated_utc=...
# Wenn ack == aktuellstes updated_utc -> latest wird gelöscht
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

    if STATE[sym].get("updated_utc") == updated_utc:
        STATE[sym]["latest"] = None
        STATE[sym]["updated_utc"] = None

    return {"ok": True, "symbol": sym, "ack": updated_utc, "cleared_latest": (STATE[sym]["latest"] is None)}

@app.get("/ack_status")
def ack_status(symbol: str):
    sym = norm_symbol(symbol)
    if sym not in STATE:
        return {"symbol": sym, "ack": None}
    return {"symbol": sym, "ack": STATE[sym].get("ack")}

# =========================
# DEAL INGEST (MT5 -> API)
# =========================
@app.post("/deal")
def ingest_deal(d: DealEvent):
    sym = norm_symbol(d.symbol)
    typ = norm_action(d.type)

    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")
    if typ not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="bad type (BUY/SELL)")

    received = now_utc()

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO deals(
                received_utc, account, symbol, magic, ticket, deal_id, type, lots,
                open_time, close_time, open_price, close_price, sl, tp,
                profit, commission, swap, comment
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            received, d.account, sym, d.magic, d.ticket, d.deal_id, typ, d.lots,
            d.open_time, d.close_time, d.open_price, d.close_price, d.sl, d.tp,
            d.profit, d.commission, d.swap, d.comment
        ))
        conn.commit()
        row_id = cur.lastrowid
        conn.close()

    return {"ok": True, "id": row_id, "received_utc": received, "symbol": sym}

@app.get("/deals")
def list_deals(
    symbol: Optional[str] = None,
    magic: Optional[int] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 200
):
    limit = max(1, min(limit, 2000))
    clauses: List[str] = []
    args: List[Any] = []

    if symbol:
        clauses.append("symbol = ?")
        args.append(norm_symbol(symbol))
    if magic is not None:
        clauses.append("magic = ?")
        args.append(magic)
    if date_from:
        clauses.append("close_time >= ?")
        args.append(date_from)
    if date_to:
        clauses.append("close_time <= ?")
        args.append(date_to)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT * FROM deals
        {where}
        ORDER BY COALESCE(close_time, received_utc) DESC
        LIMIT ?
    """
    args.append(limit)

    with _db_lock:
        conn = db_conn()
        rows = conn.execute(sql, args).fetchall()
        conn.close()

    return {"items": [dict(r) for r in rows], "count": len(rows)}

def _compute_max_dd(pnls: List[float]) -> float:
    peak = 0.0
    max_dd = 0.0
    eq = 0.0
    for p in pnls:
        eq += (p or 0.0)
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
    return max_dd

@app.get("/kpis")
def kpis(
    symbol: Optional[str] = None,
    magic: Optional[int] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    clauses: List[str] = []
    args: List[Any] = []

    if symbol:
        clauses.append("symbol = ?")
        args.append(norm_symbol(symbol))
    if magic is not None:
        clauses.append("magic = ?")
        args.append(magic)
    if date_from:
        clauses.append("close_time >= ?")
        args.append(date_from)
    if date_to:
        clauses.append("close_time <= ?")
        args.append(date_to)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT profit, commission, swap, close_time, received_utc
        FROM deals
        {where}
        ORDER BY COALESCE(close_time, received_utc) ASC
    """

    with _db_lock:
        conn = db_conn()
        rows = conn.execute(sql, args).fetchall()
        conn.close()

    pnls: List[float] = []
    for r in rows:
        p = (r["profit"] or 0.0) + (r["commission"] or 0.0) + (r["swap"] or 0.0)
        pnls.append(float(p))

    trades = len(pnls)
    wins = sum(1 for x in pnls if x > 0)
    losses = sum(1 for x in pnls if x < 0)
    breakeven = trades - wins - losses

    gross_profit = sum(x for x in pnls if x > 0)
    gross_loss_abs = abs(sum(x for x in pnls if x < 0))
    net_profit = sum(pnls)
    avg_trade = (net_profit / trades) if trades else 0.0
    winrate = (wins / trades * 100.0) if trades else 0.0
    profit_factor = (gross_profit / gross_loss_abs) if gross_loss_abs > 0 else None
    max_dd = _compute_max_dd(pnls) if trades else 0.0

    return {
        "filters": {
            "symbol": norm_symbol(symbol) if symbol else None,
            "magic": magic,
            "from": date_from,
            "to": date_to
        },
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "winrate_pct": round(winrate, 2),
        "net_profit": round(net_profit, 2),
        "gross_profit": round(gross_profit, 2),
        "gross_loss_abs": round(gross_loss_abs, 2),
        "profit_factor": (round(profit_factor, 3) if profit_factor is not None else None),
        "avg_trade": round(avg_trade, 2),
        "max_drawdown": round(max_dd, 2)
    }
