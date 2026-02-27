# app.py
# FastAPI Signal Agent API (Copy/Paste)
# - Per-Account ACK (so 5 MT5 terminals can all execute the same signal)
# - /latest supports ?account= &magic= to return "unacked for this account"
# - SQLite persistence (Render-safe if you mount a disk)
#
# Endpoints:
#   GET  /                       -> health
#   POST /tv                     -> ingest TradingView signal (key-protected)
#   GET  /latest?symbol=...&account=...&magic=...
#   POST /ack?symbol=...&updated_utc=...&account=...&magic=...
#   GET  /status/gate_combo?symbol=...
#   POST /status/gate_combo      -> update gate levels (key-protected)
#   POST /risk                   -> store initial risk snapshot (optional)
#
# Notes:
# - For true multi-account mirroring: update your EA PollLatest() URL to include &account=ACCOUNT_LOGIN&magic=InpMagic
# - Your EA already sends account/magic in /ack in my RR1to1 version. This backend supports it.

from __future__ import annotations

import os
import json
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Literal, Any, Dict

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

# --------------------------- CONFIG ---------------------------

APP_NAME = "Signal Agent API (Per-Account ACK)"

# Use env SECRET if set; fallback for local dev
SECRET = os.getenv("SECRET", "claus-2026-xau-01!")

# IMPORTANT on Render:
# - If you mount a persistent disk, set DB_PATH to that mount path, e.g. /var/data/agent.db
DB_PATH = os.getenv("DB_PATH", "agent.db")

# --------------------------- UTILS ---------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def norm_symbol(s: str) -> str:
    return (s or "").strip().upper()

def norm_action(a: str) -> str:
    return (a or "").strip().upper()

def require_key(key: str) -> None:
    if not key or key != SECRET:
        raise HTTPException(status_code=401, detail="Invalid key")

def db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def db_init() -> None:
    con = db_connect()
    cur = con.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            symbol TEXT PRIMARY KEY,
            updated_utc TEXT NOT NULL,
            action TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            received_utc TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS acks (
            symbol TEXT NOT NULL,
            account TEXT NOT NULL,
            magic TEXT NOT NULL,
            updated_utc TEXT NOT NULL,
            ack_utc TEXT NOT NULL,
            PRIMARY KEY(symbol, account, magic)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gates (
            symbol TEXT PRIMARY KEY,
            combo_level TEXT NOT NULL,
            usd_level TEXT NOT NULL,
            r_level TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS risks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account TEXT NOT NULL,
            symbol TEXT NOT NULL,
            position_id TEXT NOT NULL,
            magic INTEGER NOT NULL,
            open_time TEXT NOT NULL,
            entry_price REAL NOT NULL,
            sl REAL NOT NULL,
            lots REAL NOT NULL,
            risk_usd REAL NOT NULL,
            source TEXT NOT NULL,
            received_utc TEXT NOT NULL
        )
        """
    )

    con.commit()
    con.close()

# --------------------------- MODELS ---------------------------

class TVSignal(BaseModel):
    key: str
    symbol: str
    action: Literal["BUY", "SELL"]
    # Optional timestamp from TV; if missing we generate updated_utc ourselves
    ts: Optional[str] = None
    # Optional id from TV
    id: Optional[str] = None

class GateUpdate(BaseModel):
    key: str
    symbol: str
    combo_level: Literal["GREEN", "YELLOW", "RED"] = "GREEN"
    usd_level: str = "UNKNOWN"
    r_level: str = "UNKNOWN"

class RiskSnapshot(BaseModel):
    account: str
    symbol: str
    position_id: str
    magic: int
    open_time: str
    entry_price: float
    sl: float
    lots: float
    risk_usd: float
    source: str = "init_sl"

# --------------------------- APP ---------------------------

app = FastAPI(title=APP_NAME)

@app.on_event("startup")
def _startup() -> None:
    db_init()

@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": APP_NAME,
        "utc": now_utc_iso(),
        "db_path": DB_PATH,
    }

# --------------------------- SIGNAL INGEST ---------------------------

@app.post("/tv")
def tv(signal: TVSignal) -> Dict[str, Any]:
    require_key(signal.key)

    sym = norm_symbol(signal.symbol)
    act = norm_action(signal.action)
    if act not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="action must be BUY or SELL")

    updated_utc = (signal.ts or "").strip()
    if not updated_utc:
        updated_utc = now_utc_iso()

    payload = {
        "symbol": sym,
        "action": act,
        "updated_utc": updated_utc,
        "id": (signal.id or ""),
    }

    con = db_connect()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO signals(symbol, updated_utc, action, payload_json, received_utc)
        VALUES(?,?,?,?,?)
        ON CONFLICT(symbol) DO UPDATE SET
            updated_utc=excluded.updated_utc,
            action=excluded.action,
            payload_json=excluded.payload_json,
            received_utc=excluded.received_utc
        """,
        (sym, updated_utc, act, json.dumps(payload, ensure_ascii=False), now_utc_iso()),
    )
    con.commit()
    con.close()

    return {"ok": True, "symbol": sym, "updated_utc": updated_utc, "action": act}

# --------------------------- LATEST (Per-Account) ---------------------------

@app.get("/latest")
def latest(
    symbol: str = Query(..., description="e.g. BTCUSD, XAUUSD"),
    account: Optional[str] = Query(None, description="MT5 login; enables per-account ACK logic"),
    magic: Optional[str] = Query(None, description="EA magic; used for ACK keying (recommended)"),
) -> Dict[str, Any]:
    sym = norm_symbol(symbol)
    con = db_connect()
    cur = con.cursor()

    row = cur.execute("SELECT * FROM signals WHERE symbol=?", (sym,)).fetchone()
    if not row:
        con.close()
        return {"symbol": sym, "updated_utc": "", "signal": None}

    payload = json.loads(row["payload_json"])
    upd = str(row["updated_utc"])

    # Backward compatible:
    # - If account not given, always return latest (old behavior).
    if not account:
        con.close()
        return {"symbol": sym, "updated_utc": upd, "signal": payload}

    acc = str(account).strip()
    mag = str(magic or "").strip()
    if not mag:
        mag = "0"

    ack = cur.execute(
        "SELECT updated_utc FROM acks WHERE symbol=? AND account=? AND magic=?",
        (sym, acc, mag),
    ).fetchone()

    # If this account already acked THIS updated_utc -> return no signal
    if ack and str(ack["updated_utc"]) == upd:
        con.close()
        return {"symbol": sym, "updated_utc": upd, "signal": None, "acked": True}

    con.close()
    return {"symbol": sym, "updated_utc": upd, "signal": payload, "acked": False}

# --------------------------- ACK (Per-Account) ---------------------------

@app.post("/ack")
def ack(
    symbol: str = Query(...),
    updated_utc: str = Query(...),
    account: str = Query(...),
    magic: str = Query("0"),
) -> Dict[str, Any]:
    sym = norm_symbol(symbol)
    acc = str(account).strip()
    mag = str(magic).strip() if magic is not None else "0"
    upd = (updated_utc or "").strip()
    if not upd:
        raise HTTPException(status_code=400, detail="updated_utc required")
    if not acc:
        raise HTTPException(status_code=400, detail="account required")

    con = db_connect()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO acks(symbol, account, magic, updated_utc, ack_utc)
        VALUES(?,?,?,?,?)
        ON CONFLICT(symbol, account, magic) DO UPDATE SET
            updated_utc=excluded.updated_utc,
            ack_utc=excluded.ack_utc
        """,
        (sym, acc, mag, upd, now_utc_iso()),
    )
    con.commit()
    con.close()

    return {"ok": True, "symbol": sym, "account": acc, "magic": mag, "updated_utc": upd}

# --------------------------- GATE COMBO ---------------------------

@app.get("/status/gate_combo")
def gate_combo(symbol: str = Query(...)) -> Dict[str, Any]:
    sym = norm_symbol(symbol)
    con = db_connect()
    cur = con.cursor()

    row = cur.execute("SELECT * FROM gates WHERE symbol=?", (sym,)).fetchone()
    con.close()

    if not row:
        # default if never set
        return {
            "symbol": sym,
            "combo_level": "GREEN",
            "usd_level": "UNKNOWN",
            "r_level": "UNKNOWN",
            "updated_utc": "",
        }

    return {
        "symbol": sym,
        "combo_level": row["combo_level"],
        "usd_level": row["usd_level"],
        "r_level": row["r_level"],
        "updated_utc": row["updated_utc"],
    }

@app.post("/status/gate_combo")
def gate_combo_update(g: GateUpdate) -> Dict[str, Any]:
    require_key(g.key)
    sym = norm_symbol(g.symbol)

    con = db_connect()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO gates(symbol, combo_level, usd_level, r_level, updated_utc)
        VALUES(?,?,?,?,?)
        ON CONFLICT(symbol) DO UPDATE SET
            combo_level=excluded.combo_level,
            usd_level=excluded.usd_level,
            r_level=excluded.r_level,
            updated_utc=excluded.updated_utc
        """,
        (sym, g.combo_level, g.usd_level, g.r_level, now_utc_iso()),
    )
    con.commit()
    con.close()

    return {"ok": True, "symbol": sym, "combo_level": g.combo_level, "utc": now_utc_iso()}

# --------------------------- RISK SNAPSHOT ---------------------------

@app.post("/risk")
def risk(snapshot: RiskSnapshot) -> Dict[str, Any]:
    # optional: if you want to protect, add require_key and include key in model
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO risks(account, symbol, position_id, magic, open_time, entry_price, sl, lots, risk_usd, source, received_utc)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            str(snapshot.account),
            norm_symbol(snapshot.symbol),
            str(snapshot.position_id),
            int(snapshot.magic),
            str(snapshot.open_time),
            float(snapshot.entry_price),
            float(snapshot.sl),
            float(snapshot.lots),
            float(snapshot.risk_usd),
            str(snapshot.source),
            now_utc_iso(),
        ),
    )
    con.commit()
    con.close()
    return {"ok": True}
