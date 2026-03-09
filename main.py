from __future__ import annotations

import os
import json
import hashlib
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional, Literal, Any, Dict

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

# =========================================================
# CONFIG
# =========================================================

APP_NAME = "Signal Agent API v5 - Prop Firm Safe Execution Engine"

SECRET = os.getenv("SECRET_KEY", "claus-2026-xau-01!")
DB_PATH = os.getenv("DB_PATH", "agent.db")

# Claim TTL: wie lange ein Konto exklusiv ein Signal reservieren darf
CLAIM_TTL_SEC = int(os.getenv("CLAIM_TTL_SEC", "20"))

# Cooldown pro Symbol
DEFAULT_COOLDOWN_MIN = int(os.getenv("COOLDOWN_MIN", "30"))
BTC_COOLDOWN_MIN = int(os.getenv("BTC_COOLDOWN_MIN", str(DEFAULT_COOLDOWN_MIN)))
XAU_COOLDOWN_MIN = int(os.getenv("XAU_COOLDOWN_MIN", str(DEFAULT_COOLDOWN_MIN)))

# Optional: ACK schützen
PROTECT_LATEST_ACK = os.getenv("PROTECT_LATEST_ACK", "0").strip() in ("1", "true", "TRUE", "yes", "YES")


# =========================================================
# HELPERS
# =========================================================

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
    if PROTECT_LATEST_ACK:
        require_key(key)

def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def get_cooldown_min(symbol: str) -> int:
    s = norm_symbol(symbol)
    if s == "BTCUSD":
        return BTC_COOLDOWN_MIN
    if s == "XAUUSD":
        return XAU_COOLDOWN_MIN
    return DEFAULT_COOLDOWN_MIN


# =========================================================
# DATABASE
# =========================================================

def db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def db_init() -> None:
    con = db_connect()
    cur = con.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_state (
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

            globally_acked INTEGER DEFAULT 0,
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
            PRIMARY KEY(symbol, account, magic, updated_utc)
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


# =========================================================
# MODELS
# =========================================================

class TVSignal(BaseModel):
    key: str
    symbol: str
    action: str
    ts: Optional[str] = None
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

class ManualSignal(BaseModel):
    key: str
    symbol: str
    action: str

class ResetReq(BaseModel):
    key: str
    symbol: str
    clear_acks: bool = True
    clear_cooldown: bool = False


# =========================================================
# APP
# =========================================================

app = FastAPI(title=APP_NAME)

@app.on_event("startup")
def _startup() -> None:
    db_init()


# =========================================================
# ROOT / HEALTH
# =========================================================

@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": APP_NAME,
        "cooldown_min": DEFAULT_COOLDOWN_MIN,
        "claim_ttl_sec": CLAIM_TTL_SEC,
        "utc": now_utc_iso(),
    }


# =========================================================
# STUB ROUTES
# =========================================================

@app.get("/hb")
@app.post("/hb")
def hb() -> Dict[str, Any]:
    return {"ok": True, "utc": now_utc_iso()}

@app.get("/controls/effective")
def controls_effective(symbol: str = "", since_version: int = 0) -> Dict[str, Any]:
    sym = norm_symbol(symbol)
    return {
        "ok": True,
        "symbol": sym,
        "since_version": since_version,
        "controls": {
            "cooldown_min": get_cooldown_min(sym),
            "claim_ttl_sec": CLAIM_TTL_SEC,
        },
    }


# =========================================================
# INTERNAL STATE HELPERS
# =========================================================

def get_state_row(cur: sqlite3.Cursor, symbol: str):
    row = cur.execute("SELECT * FROM signal_state WHERE symbol=?", (symbol,)).fetchone()
    if row:
        return row

    cur.execute(
        """
        INSERT INTO signal_state(symbol, updated_utc)
        VALUES(?, ?)
        """,
        (symbol, now_utc_iso()),
    )
    return cur.execute("SELECT * FROM signal_state WHERE symbol=?", (symbol,)).fetchone()

def update_state(cur: sqlite3.Cursor, symbol: str, fields: dict):
    if not fields:
        return
    fields = dict(fields)
    fields["updated_utc"] = now_utc_iso()

    sets = ", ".join([f"{k}=?" for k in fields.keys()])
    vals = list(fields.values()) + [symbol]

    cur.execute(f"UPDATE signal_state SET {sets} WHERE symbol=?", vals)

def is_claim_active(row) -> bool:
    claim_until = parse_iso(row["claim_until_utc"])
    if not claim_until:
        return False
    return claim_until > now_utc_dt()

def is_cooldown_active(row) -> bool:
    cd = parse_iso(row["cooldown_until_utc"])
    if not cd:
        return False
    return cd > now_utc_dt()

def clear_expired_claim_if_needed(cur: sqlite3.Cursor, row):
    if row["claim_until_utc"]:
        dt = parse_iso(row["claim_until_utc"])
        if dt and dt <= now_utc_dt():
            update_state(cur, row["symbol"], {
                "claimed_by_account": None,
                "claimed_by_magic": None,
                "claim_until_utc": None,
            })


# =========================================================
# SIGNAL INGEST
# =========================================================

@app.post("/tv")
async def tv(req: Request) -> Dict[str, Any]:
    """
    HARD SAFE:
    - gleiches Signal gleicher Richtung -> duplicate
    - Signal in Cooldown -> blockiert
    - neues Signal setzt pending_* neu
    - Claim/ACK werden zurückgesetzt
    """
    try:
        body = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Body must be valid JSON")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="JSON must be an object")

    key = str(body.get("key") or req.query_params.get("key") or "").strip()
    require_key(key)

    symbol = norm_symbol(str(body.get("symbol") or body.get("ticker") or ""))
    if not symbol:
        raise HTTPException(status_code=400, detail="Missing symbol")

    action = coerce_action(str(body.get("action") or body.get("side") or ""))
    if action not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="Invalid action")

    # bewusst nur für Debug gespeichert, nicht für Logik verwendet
    tv_id = str(body.get("id") or "").strip() or None
    tv_ts = str(body.get("ts") or "").strip() or None

    payload_min = {
        "symbol": symbol,
        "action": action,
    }
    payload_hash = sha256_text(json.dumps(payload_min, sort_keys=True))

    con = db_connect()
    cur = con.cursor()

    row = get_state_row(cur, symbol)
    clear_expired_claim_if_needed(cur, row)
    row = get_state_row(cur, symbol)

    # Dedupe: gleiche Richtung wie last_seen_action + gleicher Payload-Hash => ignorieren
    if row["last_seen_action"] == action and row["last_seen_payload_hash"] == payload_hash:
        con.commit()
        con.close()
        return {
            "status": "duplicate",
            "symbol": symbol,
            "action": action,
            "updated_utc": row["updated_utc"],
        }

    # Cooldown blockiert neue Signale
    if is_cooldown_active(row):
        con.commit()
        con.close()
        return {
            "status": "blocked_cooldown",
            "symbol": symbol,
            "action": action,
            "cooldown_until_utc": row["cooldown_until_utc"],
        }

    pending_updated_utc = now_utc_iso()

    update_state(cur, symbol, {
        "pending_action": action,
        "pending_updated_utc": pending_updated_utc,
        "pending_tv_id": tv_id,
        "pending_tv_ts": tv_ts,
        "pending_payload_hash": payload_hash,
        "pending_created_utc": pending_updated_utc,

        "claimed_by_account": None,
        "claimed_by_magic": None,
        "claim_until_utc": None,

        "globally_acked": 0,

        "last_seen_tv_id": tv_id,
        "last_seen_tv_ts": tv_ts,
        "last_seen_action": action,
        "last_seen_payload_hash": payload_hash,
    })

    con.commit()
    con.close()

    return {
        "status": "accepted",
        "symbol": symbol,
        "action": action,
        "updated_utc": pending_updated_utc,
    }


# =========================================================
# MANUAL TEST ROUTE
# =========================================================

@app.get("/manual_signal")
def manual_signal(
    symbol: str = Query(...),
    action: str = Query(...),
    key: str = Query(...),
) -> Dict[str, Any]:
    require_key(key)

    symbol_n = norm_symbol(symbol)
    action_n = coerce_action(action)
    if action_n not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="Invalid action")

    con = db_connect()
    cur = con.cursor()

    row = get_state_row(cur, symbol_n)
    clear_expired_claim_if_needed(cur, row)
    row = get_state_row(cur, symbol_n)

    payload_hash = sha256_text(json.dumps({"symbol": symbol_n, "action": action_n}, sort_keys=True))

    if row["last_seen_action"] == action_n and row["last_seen_payload_hash"] == payload_hash:
        con.commit()
        con.close()
        return {
            "status": "duplicate",
            "symbol": symbol_n,
            "action": action_n,
            "updated_utc": row["updated_utc"],
        }

    if is_cooldown_active(row):
        con.commit()
        con.close()
        return {
            "status": "blocked_cooldown",
            "symbol": symbol_n,
            "action": action_n,
            "cooldown_until_utc": row["cooldown_until_utc"],
        }

    pending_updated_utc = now_utc_iso()

    update_state(cur, symbol_n, {
        "pending_action": action_n,
        "pending_updated_utc": pending_updated_utc,
        "pending_tv_id": None,
        "pending_tv_ts": None,
        "pending_payload_hash": payload_hash,
        "pending_created_utc": pending_updated_utc,

        "claimed_by_account": None,
        "claimed_by_magic": None,
        "claim_until_utc": None,

        "globally_acked": 0,

        "last_seen_tv_id": None,
        "last_seen_tv_ts": None,
        "last_seen_action": action_n,
        "last_seen_payload_hash": payload_hash,
    })

    con.commit()
    con.close()

    return {
        "status": "accepted",
        "symbol": symbol_n,
        "action": action_n,
        "updated_utc": pending_updated_utc,
    }


# =========================================================
# LATEST FOR EA
# =========================================================

@app.get("/latest")
def latest(
    symbol: str = Query(...),
    account: str = Query(...),
    magic: str = Query(...),
    key: Optional[str] = Query(None),
) -> Dict[str, Any]:
    maybe_require_key((key or "").strip())

    symbol_n = norm_symbol(symbol)
    account_n = str(account).strip()
    magic_n = str(magic).strip()

    con = db_connect()
    cur = con.cursor()

    row = get_state_row(cur, symbol_n)
    clear_expired_claim_if_needed(cur, row)
    row = get_state_row(cur, symbol_n)

    # nichts pending
    if not row["pending_action"] or not row["pending_updated_utc"]:
        con.commit()
        con.close()
        return {
            "symbol": symbol_n,
            "updated_utc": "",
            "signal": None,
        }

    # schon global abgeschlossen
    if int(row["globally_acked"] or 0) == 1:
        con.commit()
        con.close()
        return {
            "symbol": symbol_n,
            "updated_utc": row["pending_updated_utc"],
            "signal": None,
            "acked": True,
        }

    # hat dieses Konto dieses konkrete Signal schon bestätigt?
    ack = cur.execute(
        """
        SELECT 1 FROM acks
        WHERE symbol=? AND account=? AND magic=? AND updated_utc=?
        LIMIT 1
        """,
        (symbol_n, account_n, magic_n, row["pending_updated_utc"]),
    ).fetchone()

    if ack:
        con.commit()
        con.close()
        return {
            "symbol": symbol_n,
            "updated_utc": row["pending_updated_utc"],
            "signal": None,
            "acked": True,
        }

    # exklusiver Claim: entweder frei oder schon von genau diesem Konto/Magic
    claim_active = is_claim_active(row)
    same_claimer = (row["claimed_by_account"] == account_n and row["claimed_by_magic"] == magic_n)

    if claim_active and not same_claimer:
        con.commit()
        con.close()
        return {
            "symbol": symbol_n,
            "updated_utc": row["pending_updated_utc"],
            "signal": None,
            "claimed": True,
            "claimed_by_account": row["claimed_by_account"],
            "claimed_by_magic": row["claimed_by_magic"],
        }

    # Claim setzen/verlängern
    claim_until = (now_utc_dt() + timedelta(seconds=CLAIM_TTL_SEC)).isoformat()
    update_state(cur, symbol_n, {
        "claimed_by_account": account_n,
        "claimed_by_magic": magic_n,
        "claim_until_utc": claim_until,
    })

    row = get_state_row(cur, symbol_n)

    con.commit()
    con.close()

    return {
        "symbol": symbol_n,
        "updated_utc": row["pending_updated_utc"],
        "signal": {
            "symbol": symbol_n,
            "action": row["pending_action"],
        },
        "claimed": True,
        "claim_until_utc": claim_until,
    }


# =========================================================
# ACK
# =========================================================

@app.post("/ack")
def ack(
    symbol: str = Query(...),
    updated_utc: str = Query(...),
    account: str = Query(...),
    magic: str = Query(...),
    key: Optional[str] = Query(None),
) -> Dict[str, Any]:
    maybe_require_key((key or "").strip())

    symbol_n = norm_symbol(symbol)
    account_n = str(account).strip()
    magic_n = str(magic).strip()
    updated_n = str(updated_utc).strip()

    if not updated_n:
        raise HTTPException(status_code=400, detail="updated_utc required")

    con = db_connect()
    cur = con.cursor()

    row = get_state_row(cur, symbol_n)

    if row["pending_updated_utc"] != updated_n:
        con.commit()
        con.close()
        return {
            "status": "stale",
            "symbol": symbol_n,
            "expected_updated_utc": row["pending_updated_utc"],
            "got_updated_utc": updated_n,
        }

    # ACK speichern
    cur.execute(
        """
        INSERT OR REPLACE INTO acks(symbol, account, magic, updated_utc, ack_utc)
        VALUES(?,?,?,?,?)
        """,
        (symbol_n, account_n, magic_n, updated_n, now_utc_iso()),
    )

    # Pending als global ausgeführt markieren
    cooldown_until = (now_utc_dt() + timedelta(minutes=get_cooldown_min(symbol_n))).isoformat()

    update_state(cur, symbol_n, {
        "globally_acked": 1,
        "cooldown_until_utc": cooldown_until,

        "last_executed_updated_utc": row["pending_updated_utc"],
        "last_executed_action": row["pending_action"],
        "last_executed_utc": now_utc_iso(),

        "pending_action": None,
        "pending_updated_utc": None,
        "pending_tv_id": None,
        "pending_tv_ts": None,
        "pending_payload_hash": None,
        "pending_created_utc": None,

        "claimed_by_account": None,
        "claimed_by_magic": None,
        "claim_until_utc": None,
    })

    con.commit()
    con.close()

    return {
        "status": "ack_ok",
        "symbol": symbol_n,
        "account": account_n,
        "magic": magic_n,
        "cooldown_until_utc": cooldown_until,
    }


# =========================================================
# GATE
# =========================================================

@app.get("/status/gate_combo")
def gate_combo(symbol: str = Query(...)) -> Dict[str, Any]:
    symbol_n = norm_symbol(symbol)
    con = db_connect()
    cur = con.cursor()

    row = cur.execute("SELECT * FROM gates WHERE symbol=?", (symbol_n,)).fetchone()
    con.close()

    if not row:
        return {
            "symbol": symbol_n,
            "combo_level": "GREEN",
            "usd_level": "UNKNOWN",
            "r_level": "UNKNOWN",
            "updated_utc": "",
        }

    return {
        "symbol": symbol_n,
        "combo_level": row["combo_level"],
        "usd_level": row["usd_level"],
        "r_level": row["r_level"],
        "updated_utc": row["updated_utc"],
    }

@app.post("/status/gate_combo")
def gate_combo_update(g: GateUpdate) -> Dict[str, Any]:
    require_key(g.key)
    symbol_n = norm_symbol(g.symbol)

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
        (symbol_n, g.combo_level, g.usd_level, g.r_level, now_utc_iso()),
    )

    con.commit()
    con.close()

    return {
        "ok": True,
        "symbol": symbol_n,
        "combo_level": g.combo_level,
        "utc": now_utc_iso(),
    }


# =========================================================
# RISK SNAPSHOT
# =========================================================

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


# =========================================================
# DEBUG
# =========================================================

@app.get("/debug/state")
def debug_state(symbol: str = Query(...)) -> Dict[str, Any]:
    symbol_n = norm_symbol(symbol)
    con = db_connect()
    cur = con.cursor()
    row = get_state_row(cur, symbol_n)
    con.commit()
    con.close()

    return {
        "symbol": symbol_n,
        "state": dict(row),
        "server_time_utc": now_utc_iso(),
    }


# =========================================================
# RESET
# =========================================================

@app.post("/reset")
def reset(req: ResetReq) -> Dict[str, Any]:
    require_key(req.key)
    symbol_n = norm_symbol(req.symbol)

    con = db_connect()
    cur = con.cursor()
    row = get_state_row(cur, symbol_n)

    fields = {
        "pending_action": None,
        "pending_updated_utc": None,
        "pending_tv_id": None,
        "pending_tv_ts": None,
        "pending_payload_hash": None,
        "pending_created_utc": None,
        "claimed_by_account": None,
        "claimed_by_magic": None,
        "claim_until_utc": None,
        "globally_acked": 0,
    }

    if req.clear_cooldown:
        fields["cooldown_until_utc"] = None

    update_state(cur, symbol_n, fields)

    if req.clear_acks:
        cur.execute("DELETE FROM acks WHERE symbol=?", (symbol_n,))

    con.commit()
    con.close()

    return {
        "ok": True,
        "symbol": symbol_n,
        "clear_acks": req.clear_acks,
        "clear_cooldown": req.clear_cooldown,
    }
