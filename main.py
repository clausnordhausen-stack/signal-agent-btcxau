# app.py
# FastAPI Signal Agent API (Copy/Paste) - FIXED (Idempotent /tv)
# - Per-Account ACK (so 5 MT5 terminals can all execute the same signal)
# - /latest supports ?account= &magic= to return "unacked for this account"
# - SQLite persistence (Render-safe if you mount a disk)
#
# Endpoints:
#   GET  /                       -> health
#   POST /tv                     -> ingest TradingView signal (key-protected, tolerant + de-dup)
#   GET  /latest?symbol=...&account=...&magic=...
#   POST /ack?symbol=...&updated_utc=...&account=...&magic=...
#   GET  /status/gate_combo?symbol=...
#   POST /status/gate_combo      -> update gate levels (key-protected)
#   POST /risk                   -> store initial risk snapshot (optional)

from __future__ import annotations

import os
import json
import sqlite3
import hashlib
from datetime import datetime, timezone
from typing import Optional, Literal, Any, Dict

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

# --------------------------- CONFIG ---------------------------

APP_NAME = "Signal Agent API (Per-Account ACK)"

SECRET = os.getenv("SECRET", "claus-2026-xau-01!")
DB_PATH = os.getenv("DB_PATH", "agent.db")

# --------------------------- UTILS ---------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def norm_symbol(s: str) -> str:
    return (s or "").strip().upper()

def norm_action(a: str) -> str:
    return (a or "").strip().upper()

def coerce_action(a: str) -> str:
    x = norm_action(a)
    if x in ("BUY", "LONG"):
        return "BUY"
    if x in ("SELL", "SHORT"):
        return "SELL"
    return ""

def require_key(key: str) -> None:
    if not key or key != SECRET:
        raise HTTPException(status_code=401, detail="Invalid key")

def db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def sha1_hex(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()

# --------------------------- DB INIT ---------------------------

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

    # NEW: idempotency table (prevents repeated trades on webhook retries/duplicates)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tv_events (
            event_key TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            created_utc TEXT NOT NULL
        )
        """
    )

    con.commit()
    con.close()

# --------------------------- MODELS ---------------------------

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

# --------------------------- SIGNAL INGEST (TOLERANT + IDEMPOTENT) ---------------------------

@app.post("/tv")
async def tv(req: Request) -> Dict[str, Any]:
    """
    Tolerant TradingView ingest:
    - key in body OR ?key=
    - symbol OR ticker
    - action OR side (buy/sell/long/short, any case)
    - ts optional, id optional

    IDEMPOTENCY:
    - We derive event_key = id OR ts OR sha1(raw_body)
    - If the same event_key arrives again, we do NOT update signals(updated_utc),
      so EAs won't see a "new" signal and won't open repeated trades.
    """
    raw = await req.body()
    if not raw:
        raise HTTPException(status_code=400, detail="Body must not be empty")

    try:
        body = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Body must be valid JSON")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="JSON must be an object")

    key = str(body.get("key") or req.query_params.get("key") or "").strip()
    require_key(key)

    sym = norm_symbol(str(body.get("symbol") or body.get("ticker") or ""))
    if not sym:
        raise HTTPException(status_code=400, detail="Missing symbol (use 'symbol' or 'ticker')")

    act = coerce_action(str(body.get("action") or body.get("side") or ""))
    if act not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="Invalid action (use BUY/SELL or buy/sell/long/short)")

    ts = str(body.get("ts") or "").strip()
    tv_id = str(body.get("id") or "").strip()

    # Stable event key: prefer explicit id, then ts, else body-hash (TV retries are usually identical body)
    event_key = f"{sym}|{tv_id}" if tv_id else (f"{sym}|{ts}" if ts else f"{sym}|sha1:{sha1_hex(raw)}")

    con = db_connect()
    cur = con.cursor()

    # Try to insert event_key; if it already exists -> duplicate -> do nothing
    try:
        cur.execute(
            "INSERT INTO tv_events(event_key, symbol, created_utc) VALUES(?,?,?)",
            (event_key, sym, now_utc_iso()),
        )
        con.commit()
    except sqlite3.IntegrityError:
        con.close()
        return {"ok": True, "duplicate": True, "symbol": sym, "action": act, "event_key": event_key}

    # IMPORTANT: updated_utc must be stable per event (use ts if present, else generate once here)
    updated_utc = ts or now_utc_iso()

    payload = {
        "symbol": sym,
        "action": act,
        "updated_utc": updated_utc,
        "id": tv_id,
        "event_key": event_key,
    }

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

    return {"ok": True, "symbol": sym, "updated_utc": updated_utc, "action": act, "event_key": event_key}

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

    if not account:
        con.close()
        return {"symbol": sym, "updated_utc": upd, "signal": payload}

    acc = str(account).strip()
    mag = str(magic or "").strip() or "0"

    ack = cur.execute(
        "SELECT updated_utc FROM acks WHERE symbol=? AND account=? AND magic=?",
        (sym, acc, mag),
    ).fetchone()

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
