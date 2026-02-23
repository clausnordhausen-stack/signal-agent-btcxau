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

# Persist on Render by mounting a Disk and setting: DB_PATH=/var/data/data.db
DB_PATH = os.getenv("DB_PATH", "data.db")
_db_lock = Lock()

# =====================================================
# MODELS
# =====================================================
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

    # IMPORTANT: join key to risks
    position_id: Optional[str] = None

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

    risk_usd: Optional[float] = None
    r_multiple: Optional[float] = None


class RiskEvent(BaseModel):
    account: Optional[str] = None
    symbol: str
    position_id: str
    magic: int

    open_time: Optional[str] = None
    entry_price: Optional[float] = None
    sl: Optional[float] = None
    lots: Optional[float] = None
    risk_usd: Optional[float] = None
    source: Optional[str] = None


# =====================================================
# STATE (Signals in-memory)
# =====================================================
STATE: Dict[str, Dict[str, Any]] = {}

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def norm_symbol(s: Optional[str]) -> str:
    return (s or "").strip().upper()

def norm_action(a: Optional[str]) -> str:
    return (a or "").strip().upper()

def ensure_sym(sym: str) -> None:
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

            position_id TEXT,

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

        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_symbol_time ON deals(symbol, close_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_magic ON deals(magic)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_deal_id ON deals(deal_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_posid ON deals(position_id)")

        # Safe migrations
        for col, ddl in [
            ("risk_usd", "REAL"),
            ("r_multiple", "REAL"),
            ("position_id", "TEXT"),
        ]:
            try:
                cur.execute(f"ALTER TABLE deals ADD COLUMN {col} {ddl}")
            except Exception:
                pass

        conn.commit()
        conn.close()

def db_init_risks() -> None:
    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS risks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_utc TEXT NOT NULL,
            account TEXT,
            symbol TEXT NOT NULL,
            position_id TEXT NOT NULL,
            magic INTEGER NOT NULL,

            open_time TEXT,
            entry_price REAL,
            sl REAL,
            lots REAL,
            risk_usd REAL,
            source TEXT
        )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_risks_symbol_time ON risks(symbol, open_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risks_posid ON risks(position_id)")

        conn.commit()
        conn.close()

@app.on_event("startup")
def _startup():
    db_init()
    db_init_risks()

# =====================================================
# ROOT
# =====================================================
@app.get("/")
def root():
    return {"status": "Signal Agent API is running", "version": "main.py"}

@app.head("/")
def head_root():
    return

# =====================================================
# TRADINGVIEW → SIGNAL INGEST
# =====================================================
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
    return {"symbol": sym, "signal": STATE[sym]["latest"], "updated_utc": STATE[sym]["updated_utc"]}

@app.get("/queue")
def queue(symbol: str):
    sym = norm_symbol(symbol)
    if sym not in STATE:
        return {"symbol": sym, "items": []}
    return {"symbol": sym, "items": list(STATE[sym]["queue"])}

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

# =====================================================
# MT5 → RISK INGEST
# =====================================================
@app.post("/risk")
def ingest_risk(r: RiskEvent):
    sym = norm_symbol(r.symbol)
    if not sym or sym == "0":
        raise HTTPException(status_code=400, detail="bad symbol")

    posid = (r.position_id or "").strip()
    if not posid or posid == "0":
        raise HTTPException(status_code=400, detail="bad position_id")

    if int(r.magic) == 0:
        raise HTTPException(status_code=400, detail="bad magic")

    received = now_utc()

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        row = cur.execute("SELECT id FROM risks WHERE position_id = ? LIMIT 1", (posid,)).fetchone()
        if row is not None:
            conn.close()
            return {"ok": True, "dedup": True, "id": int(row["id"]), "received_utc": received, "symbol": sym, "position_id": posid}

        cur.execute("""
            INSERT INTO risks(
                received_utc, account, symbol, position_id, magic,
                open_time, entry_price, sl, lots, risk_usd, source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            received, r.account, sym, posid, int(r.magic),
            r.open_time, r.entry_price, r.sl, r.lots, r.risk_usd, r.source
        ))

        conn.commit()
        row_id = cur.lastrowid
        conn.close()

    return {"ok": True, "id": row_id, "received_utc": received, "symbol": sym, "position_id": posid}

@app.get("/risks")
def list_risks(symbol: Optional[str] = None, limit: int = 200):
    limit = max(1, min(int(limit), 2000))
    sym = norm_symbol(symbol) if symbol else None

    with _db_lock:
        conn = db_conn()
        if sym:
            rows = conn.execute(
                "SELECT * FROM risks WHERE symbol = ? ORDER BY id DESC LIMIT ?",
                (sym, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM risks ORDER BY id DESC LIMIT ?",
                (limit,)
            ).fetchall()
        conn.close()

    return {"items": [dict(r) for r in rows], "count": len(rows), "filters": {"symbol": sym, "limit": limit}}

# =====================================================
# MT5 → DEAL INGEST + server-side R compute
# =====================================================
def _net_profit_from_deal(d: DealEvent) -> float:
    return float((d.profit or 0.0) + (d.commission or 0.0) + (d.swap or 0.0))

def _fetch_risk_usd_by_position_id(conn: sqlite3.Connection, posid: str) -> Optional[float]:
    row = conn.execute(
        "SELECT risk_usd FROM risks WHERE position_id = ? ORDER BY id DESC LIMIT 1",
        (posid,)
    ).fetchone()
    if row is None:
        return None
    v = row["risk_usd"]
    if v is None:
        return None
    try:
        v = float(v)
    except Exception:
        return None
    return v if v > 0 else None

@app.post("/deal")
def ingest_deal(d: DealEvent):
    sym = norm_symbol(d.symbol)
    typ = norm_action(d.type)

    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")
    if typ not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="bad type (BUY/SELL)")

    received = now_utc()
    posid = (d.position_id or "").strip() or None

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        if d.deal_id is not None:
            row = cur.execute("SELECT id FROM deals WHERE deal_id = ? LIMIT 1", (int(d.deal_id),)).fetchone()
            if row is not None:
                conn.close()
                return {"ok": True, "dedup": True, "id": int(row["id"]), "received_utc": received, "symbol": sym}

        net = _net_profit_from_deal(d)

        risk = d.risk_usd
        rmult = d.r_multiple

        if (risk is None or float(risk) <= 0.0) and posid:
            risk = _fetch_risk_usd_by_position_id(conn, posid)

        if rmult is None and (risk is not None) and float(risk) > 0.0:
            rmult = float(net) / float(risk)

        cur.execute("""
            INSERT INTO deals(
                received_utc, account, symbol, magic, ticket, deal_id, position_id, type, lots,
                open_time, close_time, open_price, close_price, sl, tp,
                profit, commission, swap, comment,
                risk_usd, r_multiple
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            received, d.account, sym, d.magic, d.ticket, d.deal_id, posid, typ, d.lots,
            d.open_time, d.close_time, d.open_price, d.close_price, d.sl, d.tp,
            d.profit, d.commission, d.swap, d.comment,
            risk, rmult
        ))

        conn.commit()
        row_id = cur.lastrowid
        conn.close()

    return {"ok": True, "id": row_id, "received_utc": received, "symbol": sym, "position_id": posid, "risk_usd": risk, "r_multiple": rmult}

@app.get("/deals")
def list_deals(symbol: Optional[str] = None, limit: int = 200):
    limit = max(1, min(int(limit), 2000))
    sym = norm_symbol(symbol) if symbol else None

    with _db_lock:
        conn = db_conn()
        if sym:
            rows = conn.execute(
                "SELECT * FROM deals WHERE symbol = ? ORDER BY id DESC LIMIT ?",
                (sym, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM deals ORDER BY id DESC LIMIT ?",
                (limit,)
            ).fetchall()
        conn.close()

    return {"items": [dict(r) for r in rows], "count": len(rows), "filters": {"symbol": sym, "limit": limit}}

# =====================================================
# RE-CALC ROUTE (GET + POST)
# =====================================================
def _recalc_r_core(symbol: Optional[str], limit: int) -> Dict[str, Any]:
    sym = norm_symbol(symbol) if symbol else None
    limit = max(1, min(int(limit), 50000))

    updated = 0
    skipped = 0
    missing_pos = 0
    missing_risk = 0

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        if sym:
            deals = cur.execute(
                """
                SELECT id, position_id, profit, commission, swap, risk_usd, r_multiple
                FROM deals
                WHERE symbol = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (sym, limit)
            ).fetchall()
        else:
            deals = cur.execute(
                """
                SELECT id, position_id, profit, commission, swap, risk_usd, r_multiple
                FROM deals
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,)
            ).fetchall()

        for d in deals:
            deal_row_id = int(d["id"])
            posid = (d["position_id"] or "").strip()

            if not posid:
                missing_pos += 1
                continue

            net = float((d["profit"] or 0.0) + (d["commission"] or 0.0) + (d["swap"] or 0.0))

            has_rmult = d["r_multiple"] is not None
            has_risk = d["risk_usd"] is not None and float(d["risk_usd"] or
