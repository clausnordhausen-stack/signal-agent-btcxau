# app.py
# FastAPI Signal Agent API (Copy/Paste) - v2 HARD SAFE MODE
# Goals:
# - 1 trade per account per signal (per-account ACK)
# - Ignore TradingView ts/id completely
# - updated_utc ONLY changes when action changes (BUY<->SELL)
# - Extra protection: debounce action changes (anti flip-spam)
#
# Endpoints:
#   GET  /                       -> health
#   POST /tv                     -> ingest TradingView signal (key-protected, tolerant, HARD SAFE)
#   GET  /latest?symbol=...&account=...&magic=...
#   POST /ack?symbol=...&updated_utc=...&account=...&magic=...
#   GET  /status/gate_combo?symbol=...
#   POST /status/gate_combo      -> update gate levels (key-protected)
#   POST /risk                   -> store initial risk snapshot (optional)
#   GET  /debug/signal?symbol=... -> debug last stored signal (safe)

from __future__ import annotations

import os
import json
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Literal, Any, Dict

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel


# --------------------------- CONFIG ---------------------------

APP_NAME = "Signal Agent API v2 (HardSafe, Per-Account ACK)"
SECRET = os.getenv("SECRET", "claus-2026-xau-01!")
DB_PATH = os.getenv("DB_PATH", "agent.db")

# Anti-spam: minimum seconds between ACCEPTED direction changes for the same symbol
# (prevents rapid BUY->SELL->BUY flapping from indicator noise or duplicated alerts)
DEBOUNCE_SEC = int(os.getenv("DEBOUNCE_SEC", "60"))

# Optional: allow first-ever signal without debounce (recommended True)
ALLOW_FIRST_SIGNAL_ALWAYS = os.getenv("ALLOW_FIRST_SIGNAL_ALWAYS", "1").strip() not in ("0", "false", "False")


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

def _iso_to_dt(iso: str) -> Optional[datetime]:
    try:
        # Python can parse ISO with timezone; if no timezone, assume UTC
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def _seconds_since(iso: str) -> Optional[int]:
    dt = _iso_to_dt(iso)
    if not dt:
        return None
    return int((datetime.now(timezone.utc) - dt).total_seconds())


# --------------------------- DB INIT ---------------------------

def db_init() -> None:
    con = db_connect()
    cur = con.cursor()

    # Keep schema stable to avoid migrations breaking on Render
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
        "debounce_sec": DEBOUNCE_SEC,
    }


# --------------------------- SIGNAL INGEST (HARD SAFE) ---------------------------

@app.post("/tv")
async def tv(req: Request) -> Dict[str, Any]:
    """
    HARD SAFE MODE:
    - Ignore TradingView ts/id completely (even if present).
    - A "new" signal is created ONLY when action changes vs last stored for this symbol.
    - Additional anti-spam: action change is ignored if it happens within DEBOUNCE_SEC
      from the previous accepted change (per symbol). This prevents rapid flip storms.
    """
    try:
        body = await req.json()
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

    con = db_connect()
    cur = con.cursor()

    row = cur.execute("SELECT updated_utc, action FROM signals WHERE symbol=?", (sym,)).fetchone()
    prev_upd = str(row["updated_utc"]) if row else ""
    prev_act = str(row["action"]) if row else ""

    # Same direction => ALWAYS duplicate (no refresh). This kills "many trades per same signal".
    if prev_act == act and prev_upd:
        con.close()
        return {
            "ok": True,
            "duplicate": True,
            "reason": "same_action_no_refresh",
            "symbol": sym,
            "updated_utc": prev_upd,
            "action": act,
        }

    # Direction change => apply debounce protection
    if row and prev_upd:
        age_sec = _seconds_since(prev_upd)
        if age_sec is not None and age_sec < DEBOUNCE_SEC:
            # ignore flip spam
            con.close()
            return {
                "ok": True,
                "duplicate": True,
                "reason": f"debounced_change<{DEBOUNCE_SEC}s",
                "symbol": sym,
                "updated_utc": prev_upd,
                "action": prev_act,
                "incoming_action": act,
                "age_sec": age_sec,
            }

    # First ever signal: allow (option), otherwise also debounce not relevant
    if not row and not ALLOW_FIRST_SIGNAL_ALWAYS:
        # if user disables first-signal-allow, still accept (no prior), so this branch is mostly future-proof
        pass

    updated_utc = now_utc_iso()
    payload = {
        "symbol": sym,
        "action": act,
        "updated_utc": updated_utc,
        # IMPORTANT: intentionally not storing ts/id
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

    return {"ok": True, "symbol": sym, "updated_utc": updated_utc, "action": act, "duplicate": False}


# --------------------------- LATEST (Per-Account) ---------------------------

@app.get("/latest")
def latest(
    symbol: str = Query(...),
    account: Optional[str] = Query(None),
    magic: Optional[str] = Query(None),
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
        return {"symbol": sym, "combo_level": "GREEN", "usd_level": "UNKNOWN", "r_level": "UNKNOWN", "updated_utc": ""}

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


# --------------------------- DEBUG ---------------------------

@app.get("/debug/signal")
def debug_signal(symbol: str = Query(...)) -> Dict[str, Any]:
    sym = norm_symbol(symbol)
    con = db_connect()
    cur = con.cursor()
    row = cur.execute("SELECT * FROM signals WHERE symbol=?", (sym,)).fetchone()
    con.close()
    if not row:
        return {"symbol": sym, "exists": False}
    return {
        "symbol": sym,
        "exists": True,
        "action": row["action"],
        "updated_utc": row["updated_utc"],
        "received_utc": row["received_utc"],
        "payload": json.loads(row["payload_json"]),
    }
