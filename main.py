# app.py
# FastAPI Signal Agent API - MULTI-ACCOUNT BROADCAST
#
# RULES:
# - One TradingView signal per symbol becomes one active broadcast signal
# - Every account+magic can receive that signal exactly once
# - Every account+magic can ACK that signal exactly once
# - No account can receive the same signal twice
# - Global 30-minute cooldown per symbol after signal acceptance
# - During cooldown, all new TV signals for that symbol are ignored
# - Active signal expires automatically after TTL
#
# ENDPOINTS:
#   GET  /
#   POST /tv
#   GET  /latest?symbol=...&account=...&magic=...
#   POST /ack?symbol=...&updated_utc=...&account=...&magic=...
#   GET  /status/gate_combo?symbol=...
#   POST /risk
#   GET  /debug/state?symbol=...
#
# START:
#   uvicorn app:app --host 0.0.0.0 --port 10000

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime, timezone, timedelta
from typing import Optional, Any
import hashlib
import os
import sqlite3
import threading

app = FastAPI(title="Signal Agent API", version="3.0.0")

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
SECRET_KEY = os.getenv("SECRET_KEY", "claus-2026-xau-01!")
DB_PATH = os.getenv("DB_PATH", "signal_agent.db")

SYMBOL_COOLDOWN_MIN = int(os.getenv("SYMBOL_COOLDOWN_MIN", "30"))
SIGNAL_TTL_SEC = int(os.getenv("SIGNAL_TTL_SEC", "1800"))   # active signal lifetime
DEFAULT_GATE_LEVEL = os.getenv("DEFAULT_GATE_LEVEL", "GREEN").upper()

DB_LOCK = threading.Lock()

# -------------------------------------------------------------------
# MODELS
# -------------------------------------------------------------------
class TVSignal(BaseModel):
    key: str
    symbol: str
    action: str
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
# HELPERS
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

def secs_left(future_dt: Optional[datetime]) -> int:
    if future_dt is None:
        return 0
    return max(0, int((future_dt - now_utc_dt()).total_seconds()))

def norm_symbol(raw: str) -> str:
    s = (raw or "").strip().upper()

    if s in {"GOLD", "XAU", "XAUUSD", "OANDA:XAUUSD", "FOREXCOM:XAUUSD"}:
        return "XAUUSD"

    if s in {"BTC", "BTCUSD", "BITCOIN", "COINBASE:BTCUSD", "BINANCE:BTCUSDT"}:
        return "BTCUSD"

    return s

def norm_action(a: str) -> str:
    return (a or "").strip().upper()

def acct_key(account: Optional[str], magic: Optional[str]) -> str:
    return f"{(account or '').strip()}|{(magic or '').strip()}"

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

            active_updated_utc TEXT,
            active_action TEXT,
            active_tv_id TEXT,
            active_tv_ts TEXT,
            active_payload_hash TEXT,
            active_created_utc TEXT,

            cooldown_until_utc TEXT,

            last_seen_tv_id TEXT,
            last_seen_tv_ts TEXT,
            last_seen_action TEXT,
            last_seen_payload_hash TEXT,

            updated_utc TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_delivery (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            updated_utc TEXT NOT NULL,
            account TEXT,
            magic TEXT,
            delivered INTEGER NOT NULL DEFAULT 0,
            delivered_utc TEXT,
            acked INTEGER NOT NULL DEFAULT 0,
            acked_utc TEXT,
            UNIQUE(symbol, updated_utc, account, magic)
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
        INSERT INTO symbol_state (symbol, updated_utc)
        VALUES (?, ?)
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

def clear_active_signal(conn: sqlite3.Connection, symbol: str, reason: str) -> None:
    row = get_state(conn, symbol)
    if row is None or not row["active_updated_utc"]:
        return

    log_signal(
        conn,
        symbol,
        row["active_action"],
        row["active_tv_id"],
        row["active_tv_ts"],
        row["active_updated_utc"],
        "signal_cleared",
        reason,
        row["active_payload_hash"]
    )

    cur = conn.cursor()
    cur.execute("""
        UPDATE symbol_state
        SET
            active_updated_utc = NULL,
            active_action = NULL,
            active_tv_id = NULL,
            active_tv_ts = NULL,
            active_payload_hash = NULL,
            active_created_utc = NULL,
            updated_utc = ?
        WHERE symbol = ?
    """, (now_utc_iso(), symbol))
    conn.commit()

def expire_active_signal_if_needed(conn: sqlite3.Connection, symbol: str) -> sqlite3.Row:
    row = upsert_empty_state(conn, symbol)

    if not row["active_updated_utc"]:
        return row

    created = parse_iso(row["active_created_utc"])
    if created is None:
        clear_active_signal(conn, symbol, "active signal had invalid timestamp")
        return get_state(conn, symbol)

    age = int((now_utc_dt() - created).total_seconds())
    if age >= SIGNAL_TTL_SEC:
        clear_active_signal(conn, symbol, f"active signal expired after {age}s")
        return get_state(conn, symbol)

    return row

def ensure_delivery_row(conn: sqlite3.Connection, symbol: str, updated_utc: str, account: str, magic: str) -> sqlite3.Row:
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO signal_delivery (
            symbol, updated_utc, account, magic, delivered, acked
        ) VALUES (?, ?, ?, ?, 0, 0)
    """, (symbol, updated_utc, account, magic))
    conn.commit()

    cur.execute("""
        SELECT * FROM signal_delivery
        WHERE symbol = ? AND updated_utc = ? AND account = ? AND magic = ?
    """, (symbol, updated_utc, account, magic))
    return cur.fetchone()

# -------------------------------------------------------------------
# ROOT
# -------------------------------------------------------------------
@app.get("/")
def root() -> dict[str, Any]:
    return {
        "status": "Signal Agent API running",
        "mode": "multi_account_broadcast",
        "cooldown_min": SYMBOL_COOLDOWN_MIN,
        "signal_ttl_sec": SIGNAL_TTL_SEC
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
        row = expire_active_signal_if_needed(conn, symbol)
        now = now_utc_dt()

        cooldown_until = parse_iso(row["cooldown_until_utc"])
        if cooldown_until and now < cooldown_until:
            left = secs_left(cooldown_until)
            log_signal(conn, symbol, action, tv_id, tv_ts, None,
                       "ignored_cooldown", f"cooldown active, {left}s left", phash)
            conn.close()
            return {
                "status": "ignored_cooldown",
                "symbol": symbol,
                "cooldown_left_sec": left
            }

        # if one active signal already exists, ignore all new ones until it expires
        if row["active_updated_utc"]:
            created = parse_iso(row["active_created_utc"])
            ttl_left = max(0, SIGNAL_TTL_SEC - int((now - created).total_seconds())) if created else 0
            log_signal(conn, symbol, action, tv_id, tv_ts, row["active_updated_utc"],
                       "ignored_active_exists", f"signal already active, ttl_left={ttl_left}s", phash)
            conn.close()
            return {
                "status": "ignored_active_exists",
                "symbol": symbol,
                "active_updated_utc": row["active_updated_utc"],
                "signal_ttl_left_sec": ttl_left
            }

        # exact duplicate payload guard
        if row["last_seen_payload_hash"] == phash:
            log_signal(conn, symbol, action, tv_id, tv_ts, None,
                       "ignored_duplicate_payload", "same payload hash already seen", phash)
            conn.close()
            return {"status": "ignored_duplicate_payload", "symbol": symbol}

        updated_utc = now_utc_iso()

        cur = conn.cursor()
        cur.execute("""
            UPDATE symbol_state
            SET
                active_updated_utc = ?,
                active_action = ?,
                active_tv_id = ?,
                active_tv_ts = ?,
                active_payload_hash = ?,
                active_created_utc = ?,

                cooldown_until_utc = ?,

                last_seen_tv_id = ?,
                last_seen_tv_ts = ?,
                last_seen_action = ?,
                last_seen_payload_hash = ?,

                updated_utc = ?
            WHERE symbol = ?
        """, (
            updated_utc,
            action,
            tv_id or None,
            tv_ts or None,
            phash,
            updated_utc,
            (now + timedelta(minutes=SYMBOL_COOLDOWN_MIN)).isoformat(),
            tv_id or None,
            tv_ts or None,
            action,
            phash,
            updated_utc,
            symbol
        ))
        conn.commit()

        log_signal(conn, symbol, action, tv_id, tv_ts, updated_utc,
                   "accepted_broadcast", "broadcast signal accepted", phash)

        conn.close()
        return {
            "status": "accepted",
            "symbol": symbol,
            "action": action,
            "updated_utc": updated_utc,
            "cooldown_min": SYMBOL_COOLDOWN_MIN
        }

# -------------------------------------------------------------------
# LATEST - every account gets the signal once
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
    if not acc:
        raise HTTPException(status_code=400, detail="account required")
    if not mag:
        raise HTTPException(status_code=400, detail="magic required")

    with DB_LOCK:
        conn = get_conn()
        row = expire_active_signal_if_needed(conn, sym)

        if not row["active_updated_utc"]:
            cooldown_until = parse_iso(row["cooldown_until_utc"])
            conn.close()
            return {
                "symbol": sym,
                "signal": None,
                "updated_utc": "",
                "cooldown_left_sec": secs_left(cooldown_until)
            }

        updated_utc = row["active_updated_utc"]
        delivery = ensure_delivery_row(conn, sym, updated_utc, acc, mag)

        # already acked -> never deliver again
        if int(delivery["acked"]) == 1:
            conn.close()
            return {
                "symbol": sym,
                "signal": None,
                "updated_utc": "",
                "cooldown_left_sec": 0
            }

        # already delivered but not acked -> return same signal again
        # this helps if EA fetched but order/ack got delayed
        if int(delivery["delivered"]) == 0:
            cur = conn.cursor()
            cur.execute("""
                UPDATE signal_delivery
                SET delivered = 1, delivered_utc = ?
                WHERE id = ?
            """, (now_utc_iso(), delivery["id"]))
            conn.commit()

        conn.close()
        return {
            "symbol": sym,
            "updated_utc": updated_utc,
            "signal": {
                "symbol": sym,
                "action": row["active_action"]
            },
            "account": acc,
            "magic": mag,
            "cooldown_left_sec": 0
        }

# -------------------------------------------------------------------
# ACK - each account ACKs once
# -------------------------------------------------------------------
@app.post("/ack")
def ack(symbol: str,
        updated_utc: str,
        account: Optional[str] = Query(default=None),
        magic: Optional[str] = Query(default=None)) -> dict[str, Any]:
    sym = norm_symbol(symbol)
    upd = (updated_utc or "").strip()
    acc = (account or "").strip()
    mag = (magic or "").strip()

    if not sym:
        raise HTTPException(status_code=400, detail="symbol required")
    if not upd:
        raise HTTPException(status_code=400, detail="updated_utc required")
    if not acc:
        raise HTTPException(status_code=400, detail="account required")
    if not mag:
        raise HTTPException(status_code=400, detail="magic required")

    with DB_LOCK:
        conn = get_conn()
        row = expire_active_signal_if_needed(conn, sym)

        # if signal already expired/cleared
        if not row["active_updated_utc"]:
            conn.close()
            return {"status": "no_active_signal", "symbol": sym, "updated_utc": upd}

        if row["active_updated_utc"] != upd:
            conn.close()
            return {"status": "ignored_unknown_updated_utc", "symbol": sym, "updated_utc": upd}

        delivery = ensure_delivery_row(conn, sym, upd, acc, mag)

        if int(delivery["acked"]) == 1:
            conn.close()
            return {"status": "already_acked", "symbol": sym, "updated_utc": upd}

        cur = conn.cursor()
        cur.execute("""
            UPDATE signal_delivery
            SET
                delivered = 1,
                delivered_utc = COALESCE(delivered_utc, ?),
                acked = 1,
                acked_utc = ?
            WHERE id = ?
        """, (now_utc_iso(), now_utc_iso(), delivery["id"]))
        conn.commit()

        conn.close()
        return {
            "status": "acked",
            "symbol": sym,
            "updated_utc": upd,
            "account": acc,
            "magic": mag
        }

# -------------------------------------------------------------------
# GATE COMPAT
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
# RISK COMPAT
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
        row = expire_active_signal_if_needed(conn, sym)

        deliveries = []
        if row["active_updated_utc"]:
            cur = conn.cursor()
            cur.execute("""
                SELECT account, magic, delivered, delivered_utc, acked, acked_utc
                FROM signal_delivery
                WHERE symbol = ? AND updated_utc = ?
                ORDER BY account, magic
            """, (sym, row["active_updated_utc"]))
            deliveries = [dict(r) for r in cur.fetchall()]

        data = dict(row)
        conn.close()

    return {
        "symbol": sym,
        "state": data,
        "deliveries": deliveries,
        "server_time_utc": now_utc_iso()
    }
