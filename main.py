# app.py
# FastAPI Signal Agent API
# HARD RULES:
# - Exactly ONE executable signal per symbol at a time
# - Exclusive claim: only one account/magic can receive a pending signal
# - ACK starts a hard 30-minute cooldown per symbol
# - During cooldown, ALL new TV signals for that symbol are ignored
# - Repeated / re-sent / new-id signals are ignored while pending or cooling down
#
# Compatible endpoints:
#   GET  /
#   POST /tv
#   GET  /latest?symbol=...&account=...&magic=...
#   POST /ack?symbol=...&updated_utc=...&account=...&magic=...
#   GET  /status/gate_combo?symbol=...
#   POST /risk
#   GET  /debug/state?symbol=...
#
# Start:
#   uvicorn app:app --host 0.0.0.0 --port 10000

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime, timezone, timedelta
from typing import Optional, Any
import hashlib
import json
import os
import sqlite3
import threading

app = FastAPI(title="Signal Agent API", version="2.0.0")

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
SECRET_KEY = os.getenv("SECRET_KEY", "claus-2026-xau-01!")
DB_PATH = os.getenv("DB_PATH", "signal_agent.db")

# HARD RULES
SYMBOL_COOLDOWN_MIN = int(os.getenv("SYMBOL_COOLDOWN_MIN", "30"))
CLAIM_TTL_SEC = int(os.getenv("CLAIM_TTL_SEC", "20"))

# optional gate defaults
DEFAULT_GATE_LEVEL = os.getenv("DEFAULT_GATE_LEVEL", "GREEN").upper()

DB_LOCK = threading.Lock()

# -------------------------------------------------------------------
# MODELS
# -------------------------------------------------------------------
class TVSignal(BaseModel):
    key: str
    symbol: str
    action: str          # BUY / SELL
    ts: Optional[str] = None
    id: Optional[str] = None

class RiskEvent(BaseModel):
    account: Optional[str] = None
    symbol: str
    position_id: Optional[str] = None
    magic: Optional[int] = None
    open_time: Optional[str] = None
    entry_price: Optional[float] = None
    sl: Optional[float] = None
    lots: Optional[float] = None
    risk_usd: Optional[float] = None
    source: Optional[str] = None

# -------------------------------------------------------------------
# TIME / NORMALIZATION
# -------------------------------------------------------------------
def now_utc_dt() -> datetime:
    return datetime.now(timezone.utc)

def now_utc_iso() -> str:
    return now_utc_dt().isoformat()

def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def norm_symbol(s: str) -> str:
    return (s or "").strip().upper()

def norm_action(a: str) -> str:
    return (a or "").strip().upper()

def payload_hash(symbol: str, action: str, tv_id: str, tv_ts: str) -> str:
    raw = f"{symbol}|{action}|{tv_id}|{tv_ts}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

# -------------------------------------------------------------------
# DB
# -------------------------------------------------------------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS symbol_state (
            symbol TEXT PRIMARY KEY,

            pending_action TEXT,
            pending_updated_utc TEXT,
            pending_tv_id TEXT,
            pending_tv_ts TEXT,
            pending_payload_hash TEXT,
            pending_created_utc TEXT,

            claimed_by_account TEXT,
            claimed_by_magic TEXT,
            claim_until_utc TEXT,

            globally_acked INTEGER NOT NULL DEFAULT 0,

            cooldown_until_utc TEXT,

            last_executed_updated_utc TEXT,
            last_executed_action TEXT,
            last_executed_utc TEXT,

            last_seen_tv_id TEXT,
            last_seen_tv_ts TEXT,
            last_seen_action TEXT,
            last_seen_payload_hash TEXT,

            updated_utc TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_utc TEXT NOT NULL,
            symbol TEXT NOT NULL,
            action TEXT,
            tv_id TEXT,
            tv_ts TEXT,
            updated_utc TEXT,
            status TEXT NOT NULL,
            note TEXT,
            payload_hash TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS risk_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_utc TEXT NOT NULL,
            account TEXT,
            symbol TEXT NOT NULL,
            position_id TEXT,
            magic INTEGER,
            open_time TEXT,
            entry_price REAL,
            sl REAL,
            lots REAL,
            risk_usd REAL,
            source TEXT
        )
        """)

        conn.commit()
        conn.close()

@app.on_event("startup")
def startup() -> None:
    init_db()

def get_state(conn: sqlite3.Connection, symbol: str) -> Optional[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM symbol_state WHERE symbol = ?", (symbol,))
    return cur.fetchone()

def upsert_empty_state(conn: sqlite3.Connection, symbol: str) -> sqlite3.Row:
    row = get_state(conn, symbol)
    if row is not None:
        return row

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO symbol_state (
            symbol,
            globally_acked,
            updated_utc
        ) VALUES (?, 0, ?)
    """, (symbol, now_utc_iso()))
    conn.commit()
    return get_state(conn, symbol)

def log_signal(conn: sqlite3.Connection,
               symbol: str,
               action: Optional[str],
               tv_id: Optional[str],
               tv_ts: Optional[str],
               updated_utc: Optional[str],
               status: str,
               note: str,
               phash: Optional[str]) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO signal_log (
            created_utc, symbol, action, tv_id, tv_ts, updated_utc,
            status, note, payload_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        now_utc_iso(), symbol, action, tv_id, tv_ts, updated_utc,
        status, note, phash
    ))
    conn.commit()

# -------------------------------------------------------------------
# ROOT
# -------------------------------------------------------------------
@app.get("/")
def root() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "Signal Agent API",
        "cooldown_min": SYMBOL_COOLDOWN_MIN,
        "claim_ttl_sec": CLAIM_TTL_SEC
    }

# -------------------------------------------------------------------
# TV INGEST
# -------------------------------------------------------------------
@app.post("/tv")
def tv(signal: TVSignal) -> dict[str, Any]:
    if signal.key != SECRET_KEY:
        raise HTTPException(status_code=401, detail="invalid key")

    symbol = norm_symbol(signal.symbol)
    action = norm_action(signal.action)
    tv_id = (signal.id or "").strip()
    tv_ts = (signal.ts or "").strip()
    phash = payload_hash(symbol, action, tv_id, tv_ts)

    if not symbol:
        raise HTTPException(status_code=400, detail="symbol required")
    if action not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="action must be BUY or SELL")

    with DB_LOCK:
        conn = get_conn()
        row = upsert_empty_state(conn, symbol)

        now = now_utc_dt()
        cooldown_until = parse_iso(row["cooldown_until_utc"])
        claim_until = parse_iso(row["claim_until_utc"])

        # HARD RULE 1:
        # while cooldown is active, ignore all new signals for this symbol
        if cooldown_until and now < cooldown_until:
            left = int((cooldown_until - now).total_seconds())
            log_signal(
                conn, symbol, action, tv_id, tv_ts, None,
                "ignored_cooldown",
                f"cooldown active, {left}s left",
                phash
            )
            conn.close()
            return {
                "status": "ignored_cooldown",
                "symbol": symbol,
                "action": action,
                "cooldown_left_sec": left
            }

        # HARD RULE 2:
        # if one signal is still pending and not globally acked, ignore everything else
        if row["pending_updated_utc"] and int(row["globally_acked"] or 0) == 0:
            log_signal(
                conn, symbol, action, tv_id, tv_ts, row["pending_updated_utc"],
                "ignored_pending_exists",
                "another signal is still pending",
                phash
            )
            conn.close()
            return {
                "status": "ignored_pending_exists",
                "symbol": symbol,
                "pending_updated_utc": row["pending_updated_utc"]
            }

        # HARD RULE 3:
        # duplicate TV event guard (same exact payload)
        if row["last_seen_payload_hash"] == phash:
            log_signal(
                conn, symbol, action, tv_id, tv_ts, None,
                "ignored_duplicate_payload",
                "same payload hash already seen",
                phash
            )
            conn.close()
            return {
                "status": "ignored_duplicate_payload",
                "symbol": symbol
            }

        # accept exactly one new pending signal
        updated_utc = now_utc_iso()

        cur = conn.cursor()
        cur.execute("""
            UPDATE symbol_state
            SET
                pending_action = ?,
                pending_updated_utc = ?,
                pending_tv_id = ?,
                pending_tv_ts = ?,
                pending_payload_hash = ?,
                pending_created_utc = ?,

                claimed_by_account = NULL,
                claimed_by_magic = NULL,
                claim_until_utc = NULL,

                globally_acked = 0,

                last_seen_tv_id = ?,
                last_seen_tv_ts = ?,
                last_seen_action = ?,
                last_seen_payload_hash = ?,

                updated_utc = ?
            WHERE symbol = ?
        """, (
            action,
            updated_utc,
            tv_id or None,
            tv_ts or None,
            phash,
            updated_utc,

            tv_id or None,
            tv_ts or None,
            action,
            phash,

            updated_utc,
            symbol
        ))
        conn.commit()

        log_signal(
            conn, symbol, action, tv_id, tv_ts, updated_utc,
            "accepted",
            "signal accepted as exclusive pending signal",
            phash
        )

        conn.close()
        return {
            "status": "accepted",
            "symbol": symbol,
            "action": action,
            "updated_utc": updated_utc
        }

# -------------------------------------------------------------------
# LATEST (exclusive claim)
# -------------------------------------------------------------------
@app.get("/latest")
def latest(symbol: str,
           account: Optional[str] = Query(default=None),
           magic: Optional[str] = Query(default=None)) -> dict[str, Any]:
    sym = norm_symbol(symbol)
    acc = (account or "").strip()
    mag = (magic or "").strip()

    if not sym:
        raise HTTPException(status_code=400, detail="symbol required")

    with DB_LOCK:
        conn = get_conn()
        row = upsert_empty_state(conn, sym)

        now = now_utc_dt()
        cooldown_until = parse_iso(row["cooldown_until_utc"])
        claim_until = parse_iso(row["claim_until_utc"])

        # no pending signal
        if not row["pending_updated_utc"] or int(row["globally_acked"] or 0) == 1:
            left = 0
            if cooldown_until and now < cooldown_until:
                left = int((cooldown_until - now).total_seconds())
            conn.close()
            return {
                "symbol": sym,
                "signal": None,
                "updated_utc": "",
                "cooldown_left_sec": left
            }

        # if someone else already holds a live claim -> return null
        if row["claimed_by_account"] and row["claimed_by_magic"] and claim_until and now < claim_until:
            if not (row["claimed_by_account"] == acc and row["claimed_by_magic"] == mag):
                conn.close()
                return {
                    "symbol": sym,
                    "signal": None,
                    "updated_utc": "",
                    "cooldown_left_sec": 0
                }

        # create / refresh claim for this requester
        claim_until_utc = (now + timedelta(seconds=CLAIM_TTL_SEC)).isoformat()
        cur = conn.cursor()
        cur.execute("""
            UPDATE symbol_state
            SET
                claimed_by_account = ?,
                claimed_by_magic = ?,
                claim_until_utc = ?,
                updated_utc = ?
            WHERE symbol = ?
        """, (
            acc or None,
            mag or None,
            claim_until_utc,
            now.isoformat(),
            sym
        ))
        conn.commit()

        action = row["pending_action"]
        updated_utc = row["pending_updated_utc"]

        conn.close()
        return {
            "symbol": sym,
            "updated_utc": updated_utc,
            "signal": {
                "symbol": sym,
                "action": action
            },
            "claimed_by_account": acc,
            "claimed_by_magic": mag,
            "claim_ttl_sec": CLAIM_TTL_SEC,
            "cooldown_left_sec": 0
        }

# -------------------------------------------------------------------
# ACK (global execute lock + cooldown start)
# -------------------------------------------------------------------
@app.post("/ack")
def ack(symbol: str,
        updated_utc: str,
        account: Optional[str] = Query(default=None),
        magic: Optional[str] = Query(default=None)) -> dict[str, Any]:
    sym = norm_symbol(symbol)
    acc = (account or "").strip()
    mag = (magic or "").strip()
    upd = (updated_utc or "").strip()

    if not sym or not upd:
        raise HTTPException(status_code=400, detail="symbol and updated_utc required")

    with DB_LOCK:
        conn = get_conn()
        row = upsert_empty_state(conn, sym)
        now = now_utc_dt()

        # already executed / duplicate ACK
        if row["last_executed_updated_utc"] == upd:
            cooldown_until = parse_iso(row["cooldown_until_utc"])
            left = 0
            if cooldown_until and now < cooldown_until:
                left = int((cooldown_until - now).total_seconds())
            conn.close()
            return {
                "status": "already_acked",
                "symbol": sym,
                "updated_utc": upd,
                "cooldown_left_sec": left
            }

        # must match current pending signal
        if row["pending_updated_utc"] != upd:
            conn.close()
            return {
                "status": "ignored_unknown_updated_utc",
                "symbol": sym,
                "updated_utc": upd
            }

        # enforce exclusive claimer
        if row["claimed_by_account"] and row["claimed_by_magic"]:
            if row["claimed_by_account"] != acc or row["claimed_by_magic"] != mag:
                conn.close()
                return {
                    "status": "ignored_not_claimer",
                    "symbol": sym,
                    "updated_utc": upd
                }

        cooldown_until_utc = (now + timedelta(minutes=SYMBOL_COOLDOWN_MIN)).isoformat()

        cur = conn.cursor()
        cur.execute("""
            UPDATE symbol_state
            SET
                globally_acked = 1,

                last_executed_updated_utc = pending_updated_utc,
                last_executed_action = pending_action,
                last_executed_utc = ?,

                cooldown_until_utc = ?,

                pending_action = NULL,
                pending_updated_utc = NULL,
                pending_tv_id = NULL,
                pending_tv_ts = NULL,
                pending_payload_hash = NULL,
                pending_created_utc = NULL,

                claimed_by_account = NULL,
                claimed_by_magic = NULL,
                claim_until_utc = NULL,

                updated_utc = ?
            WHERE symbol = ?
        """, (
            now.isoformat(),
            cooldown_until_utc,
            now.isoformat(),
            sym
        ))
        conn.commit()

        log_signal(
            conn,
            sym,
            row["pending_action"],
            row["pending_tv_id"],
            row["pending_tv_ts"],
            upd,
            "acked_executed",
            f"executed by account={acc} magic={mag}; cooldown started",
            row["pending_payload_hash"]
        )

        conn.close()
        return {
            "status": "acked",
            "symbol": sym,
            "updated_utc": upd,
            "cooldown_until_utc": cooldown_until_utc,
            "cooldown_min": SYMBOL_COOLDOWN_MIN
        }

# -------------------------------------------------------------------
# GATE STUB / COMPAT
# -------------------------------------------------------------------
@app.get("/status/gate_combo")
def gate_combo(symbol: str) -> dict[str, Any]:
    sym = norm_symbol(symbol)
    lvl = DEFAULT_GATE_LEVEL if DEFAULT_GATE_LEVEL in {"GREEN", "YELLOW", "RED"} else "GREEN"
    return {
        "symbol": sym,
        "combo_level": lvl,
        "usd_level": lvl,
        "r_level": lvl
    }

# -------------------------------------------------------------------
# RISK CAPTURE / COMPAT
# -------------------------------------------------------------------
@app.post("/risk")
def risk(event: RiskEvent) -> dict[str, Any]:
    sym = norm_symbol(event.symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol required")

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO risk_events (
                created_utc, account, symbol, position_id, magic, open_time,
                entry_price, sl, lots, risk_usd, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now_utc_iso(),
            event.account,
            sym,
            event.position_id,
            event.magic,
            event.open_time,
            event.entry_price,
            event.sl,
            event.lots,
            event.risk_usd,
            event.source
        ))
        conn.commit()
        conn.close()

    return {"status": "ok"}

# -------------------------------------------------------------------
# DEBUG
# -------------------------------------------------------------------
@app.get("/debug/state")
def debug_state(symbol: str) -> dict[str, Any]:
    sym = norm_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol required")

    with DB_LOCK:
        conn = get_conn()
        row = upsert_empty_state(conn, sym)
        data = dict(row)
        conn.close()

    return {
        "symbol": sym,
        "state": data,
        "server_time_utc": now_utc_iso()
    }
