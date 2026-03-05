# app.py
# Signal Agent API v3.1 (Ultra-Stable + Trade-Memory + Cooldown, Render-safe, SQLite)
#
# CORE:
# - HARD SAFE MODE: ignores ts/id completely
# - Trade-Memory (Direction-Lock): same-direction signals are ALWAYS ignored until direction changes
# - Cooldown: direction changes are blocked for N seconds after last accepted direction change (per symbol)
# - Per-account ACK: 1 trade per account per accepted signal (symbol + account + magic)
# - SQLite persistence (Render-safe)
# - Stub routes: /hb and /controls/effective to stop 404 spam
#
# Endpoints:
#   GET  /                         -> health
#   POST /tv                       -> ingest TradingView signal (key-protected)
#   GET  /latest?symbol=...&account=...&magic=...
#   POST /ack?symbol=...&updated_utc=...&account=...&magic=...
#   GET  /status/gate_combo?symbol=...
#   POST /status/gate_combo        -> update gate (key-protected)
#   POST /risk                     -> store initial risk snapshot (optional)
#   POST /reset                    -> reset one symbol (key-protected) (optional safety)
#   GET/POST /hb                   -> stub
#   GET /controls/effective        -> stub (returns cooldown map)
#
from __future__ import annotations

import os
import json
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Literal, Any, Dict

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

# --------------------------- CONFIG ---------------------------

APP_NAME = "Signal Agent API v3.1 (Memory+Cooldown)"
SECRET = os.getenv("SECRET", "claus-2026-xau-01!")
DB_PATH = os.getenv("DB_PATH", "agent.db")

# If ON, EAs must add &key=SECRET to /latest and /ack
PROTECT_LATEST_ACK = os.getenv("PROTECT_LATEST_ACK", "0").strip() in ("1", "true", "TRUE", "yes", "YES")

# Cooldowns (seconds):
# Option A (recommended): COOLDOWN_MAP_JSON='{"BTCUSD":600,"XAUUSD":300}'
# Option B: set COOLDOWN_DEFAULT_SEC and per-symbol overrides COOLDOWN_BTCUSD, COOLDOWN_XAUUSD
COOLDOWN_DEFAULT_SEC = int(os.getenv("COOLDOWN_DEFAULT_SEC", "0") or "0")
COOLDOWN_MAP_JSON = os.getenv("COOLDOWN_MAP_JSON", "").strip()

# --------------------------- UTILS ---------------------------

def now_utc_dt() -> datetime:
    return datetime.now(timezone.utc)

def now_utc_iso() -> str:
    return now_utc_dt().isoformat()

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

def maybe_require_key(key: str) -> None:
    if not PROTECT_LATEST_ACK:
        return
    require_key(key)

def db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def parse_iso_dt(s: str) -> Optional[datetime]:
    try:
        if not s:
            return None
        return datetime.fromisoformat(s)
    except Exception:
        return None

def get_cooldown_sec(sym: str) -> int:
    s = norm_symbol(sym)

    # JSON map takes priority
    if COOLDOWN_MAP_JSON:
        try:
            m = json.loads(COOLDOWN_MAP_JSON)
            if isinstance(m, dict):
                v = m.get(s)
                if v is None:
                    # allow common alt keys e.g. "BTC" etc. but keep simple
                    return max(0, int(COOLDOWN_DEFAULT_SEC))
                return max(0, int(v))
        except Exception:
            # if JSON is broken, fall back to env overrides/default
            pass

    # per-symbol override env: COOLDOWN_BTCUSD=600 etc.
    env_key = f"COOLDOWN_{s}"
    if env_key in os.environ:
        try:
            return max(0, int(os.environ.get(env_key, "0") or "0"))
        except Exception:
            return max(0, int(COOLDOWN_DEFAULT_SEC))

    return max(0, int(COOLDOWN_DEFAULT_SEC))

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

class ResetReq(BaseModel):
    key: str
    symbol: str
    clear_acks: bool = True

# --------------------------- APP ---------------------------

app = FastAPI(title=APP_NAME)

@app.on_event("startup")
def _startup() -> None:
    db_init()

@app.get("/")
def root() -> Dict[str, Any]:
    # show effective cooldown hints without leaking secret
    return {
        "status": "ok",
        "service": APP_NAME,
        "utc": now_utc_iso(),
        "db_path": DB_PATH,
        "protect_latest_ack": PROTECT_LATEST_ACK,
        "cooldown_default_sec": COOLDOWN_DEFAULT_SEC,
        "cooldown_map_json_set": bool(COOLDOWN_MAP_JSON),
    }

# --------------------------- STUB ROUTES (stop 404 spam) ---------------------------

@app.get("/hb")
@app.post("/hb")
def hb() -> Dict[str, Any]:
    return {"ok": True, "utc": now_utc_iso()}

@app.get("/controls/effective")
def controls_effective(symbol: str = "", since_version: int = 0) -> Dict[str, Any]:
    sym = norm_symbol(symbol) if symbol else ""
    cd = get_cooldown_sec(sym) if sym else COOLDOWN_DEFAULT_SEC
    return {
        "ok": True,
        "symbol": sym,
        "since_version": since_version,
        "controls": {
            "cooldown_sec": cd,
            "protect_latest_ack": PROTECT_LATEST_ACK,
        },
    }

# --------------------------- RESET (optional safety) ---------------------------

@app.post("/reset")
def reset(req: ResetReq) -> Dict[str, Any]:
    require_key(req.key)
    sym = norm_symbol(req.symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol required")

    con = db_connect()
    cur = con.cursor()

    cur.execute("DELETE FROM signals WHERE symbol=?", (sym,))
    if req.clear_acks:
        cur.execute("DELETE FROM acks WHERE symbol=?", (sym,))

    con.commit()
    con.close()
    return {"ok": True, "symbol": sym, "cleared_acks": bool(req.clear_acks)}

# --------------------------- SIGNAL INGEST (Memory + Cooldown) ---------------------------

@app.post("/tv")
async def tv(req: Request) -> Dict[str, Any]:
    """
    HARD SAFE MODE + Trade-Memory + Cooldown:
    - Ignore sender ts/id completely.
    - SAME-DIRECTION signals are ALWAYS duplicates => no updated_utc refresh => no re-trade.
    - Opposite-direction signals are accepted ONLY if cooldown has passed since last accepted change.
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

    # ---- Trade-Memory: same direction is ALWAYS duplicate
    if prev_act == act and prev_upd:
        con.close()
        return {"ok": True, "duplicate": True, "symbol": sym, "updated_utc": prev_upd, "action": act}

    # ---- Cooldown only applies when direction is changing (or first ever)
    cooldown_sec = get_cooldown_sec(sym)
    if prev_upd and cooldown_sec > 0:
        dt_prev = parse_iso_dt(prev_upd)
        if dt_prev is not None:
            age = (now_utc_dt() - dt_prev).total_seconds()
            if age < float(cooldown_sec):
                # Block direction change
                con.close()
                return {
                    "ok": True,
                    "blocked_cooldown": True,
                    "symbol": sym,
                    "requested_action": act,
                    "current_action": prev_act,
                    "current_updated_utc": prev_upd,
                    "cooldown_sec": cooldown_sec,
                    "seconds_remaining": int(max(0.0, float(cooldown_sec) - age)),
                }

    updated_utc = now_utc_iso()
    payload = {"symbol": sym, "action": act, "updated_utc": updated_utc}

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

    return {"ok": True, "symbol": sym, "updated_utc": updated_utc, "action": act, "cooldown_sec": cooldown_sec}

# --------------------------- LATEST (Per-Account) ---------------------------

@app.get("/latest")
def latest(
    symbol: str = Query(...),
    account: Optional[str] = Query(None),
    magic: Optional[str] = Query(None),
    key: Optional[str] = Query(None),
) -> Dict[str, Any]:
    maybe_require_key((key or "").strip())

    sym = norm_symbol(symbol)
    con = db_connect()
    cur = con.cursor()

    row = cur.execute("SELECT * FROM signals WHERE symbol=?", (sym,)).fetchone()
    if not row:
        con.close()
        return {"symbol": sym, "updated_utc": "", "signal": None}

    payload = json.loads(row["payload_json"])
    upd = str(row["updated_utc"])

    # If no account passed -> raw signal
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
    key: Optional[str] = Query(None),
) -> Dict[str, Any]:
    maybe_require_key((key or "").strip())

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
