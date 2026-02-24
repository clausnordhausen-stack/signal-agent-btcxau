# main.py
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Literal
from collections import deque
import sqlite3
import os
import json
import hashlib
from threading import Lock
from uuid import uuid4

app = FastAPI(title="Signal Agent API", version="v2-controls")

# =====================================================
# CONFIG
# =====================================================
SECRET = os.getenv("SECRET_KEY", "claus-2026-xau-01!")          # TradingView webhook key (unchanged)
QUEUE_MAX = int(os.getenv("QUEUE_MAX", "50"))

# Persist on Render by mounting a Disk and setting: DB_PATH=/var/data/data.db
DB_PATH = os.getenv("DB_PATH", "data.db")
_db_lock = Lock()

# --- v1 Auth (simple, but future-proof) ---
# EA_KEYS: comma-separated list of EA bearer tokens (e.g. "ea123,ea456")
EA_KEYS_ENV = os.getenv("EA_KEYS", "").strip()
EA_KEYS = [k.strip() for k in EA_KEYS_ENV.split(",") if k.strip()]

# APP_TOKEN: single bearer token for your phone app (e.g. "app123")
APP_TOKEN = os.getenv("APP_TOKEN", "").strip()

# Controls: defaults
DEFAULT_MODE = "ON"
DEFAULT_RISK_FACTOR = 1.0
DEFAULT_ALLOW_NEW_ENTRIES = True


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
    position_id: Optional[str] = None  # IMPORTANT join-key
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

    # Optional direct (EA can send, but we also recalc via risks)
    risk_usd: Optional[float] = None
    r_multiple: Optional[float] = None

class RiskEvent(BaseModel):
    account: Optional[str] = None
    symbol: str
    position_id: str

    magic: Optional[int] = None
    open_time: Optional[str] = None

    entry_price: Optional[float] = None
    sl: Optional[float] = None
    lots: Optional[float] = None

    risk_usd: float
    source: Optional[str] = "mt5-risk-snapshot"

# --- Controls v1 models ---
ScopeType = Literal["GLOBAL", "SYMBOL"]
ModeType = Literal["ON", "REDUCED", "PAUSED"]

class HeartbeatEvent(BaseModel):
    device_id: Optional[str] = None
    label: Optional[str] = None
    symbol: str
    magic: Optional[int] = None
    ea_version: Optional[str] = None
    ts: Optional[str] = None  # optional client ts

class ControlsPatch(BaseModel):
    mode: Optional[ModeType] = None
    risk_factor: Optional[float] = None
    allow_new_entries: Optional[bool] = None

class ControlsSetRequest(BaseModel):
    scope_type: ScopeType
    scope_id: Optional[str] = ""         # "" for GLOBAL, "BTCUSD" for SYMBOL
    patch: ControlsPatch
    reason: Optional[str] = None
    updated_by: Optional[str] = "app"    # optional label


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
# AUTH HELPERS
# =====================================================
def _bearer_token(authorization: Optional[str]) -> str:
    if not authorization:
        return ""
    parts = authorization.split(" ", 1)
    if len(parts) != 2:
        return ""
    scheme, token = parts[0].strip(), parts[1].strip()
    if scheme.lower() != "bearer":
        return ""
    return token

def require_app(authorization: Optional[str]) -> None:
    tok = _bearer_token(authorization)
    if not APP_TOKEN:
        raise HTTPException(status_code=500, detail="APP_TOKEN not set on server")
    if tok != APP_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized (app)")

def require_ea(authorization: Optional[str]) -> None:
    tok = _bearer_token(authorization)
    if not EA_KEYS:
        raise HTTPException(status_code=500, detail="EA_KEYS not set on server")
    if tok not in EA_KEYS:
        raise HTTPException(status_code=401, detail="unauthorized (ea)")

def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


# =====================================================
# DATABASE
# =====================================================
def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def _table_cols(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r["name"] for r in rows]

def db_init() -> None:
    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        # Deals table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_utc TEXT NOT NULL,
            account TEXT,
            symbol TEXT NOT NULL,
            position_id TEXT,
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

        # Risks table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS risks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_utc TEXT NOT NULL,
            account TEXT,
            symbol TEXT NOT NULL,
            position_id TEXT NOT NULL,
            magic INTEGER,
            open_time TEXT,
            entry_price REAL,
            sl REAL,
            lots REAL,
            risk_usd REAL NOT NULL,
            source TEXT
        )
        """)

        # --- Devices (heartbeat) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,
            label TEXT,
            symbol TEXT NOT NULL,
            magic INTEGER,
            ea_version TEXT,
            ea_key_hash TEXT,
            last_seen_utc TEXT,
            status TEXT
        )
        """)

        # --- Controls (GLOBAL + SYMBOL) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS controls (
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            mode TEXT NOT NULL,
            risk_factor REAL NOT NULL,
            allow_new_entries INTEGER NOT NULL,
            updated_utc TEXT NOT NULL,
            version INTEGER NOT NULL,
            updated_by TEXT,
            reason TEXT,
            PRIMARY KEY (scope_type, scope_id)
        )
        """)

        # --- Control events (audit) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS control_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            old_json TEXT,
            new_json TEXT,
            updated_utc TEXT NOT NULL,
            updated_by TEXT,
            reason TEXT
        )
        """)

        # Indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_symbol_id ON deals(symbol, id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_deal_id ON deals(deal_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_position_id ON deals(position_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risks_symbol_id ON risks(symbol, id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risks_position_id ON risks(position_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_devices_symbol_seen ON devices(symbol, last_seen_utc)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_controls_version ON controls(version)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_control_events_scope ON control_events(scope_type, scope_id, id)")

        # Safe migrations (legacy safety)
        dcols = _table_cols(conn, "deals")
        for col, coldef in [("position_id", "TEXT"), ("risk_usd", "REAL"), ("r_multiple", "REAL")]:
            if col not in dcols:
                try:
                    cur.execute(f"ALTER TABLE deals ADD COLUMN {col} {coldef}")
                except Exception:
                    pass

        rcols = _table_cols(conn, "risks")
        for col, coldef in [
            ("account", "TEXT"), ("magic", "INTEGER"), ("open_time", "TEXT"),
            ("entry_price", "REAL"), ("sl", "REAL"), ("lots", "REAL"), ("source", "TEXT")
        ]:
            if col not in rcols:
                try:
                    cur.execute(f"ALTER TABLE risks ADD COLUMN {col} {coldef}")
                except Exception:
                    pass

        # Initialize default controls if missing
        def _ensure_control(scope_type: str, scope_id: str):
            row = cur.execute(
                "SELECT scope_type, scope_id FROM controls WHERE scope_type=? AND scope_id=? LIMIT 1",
                (scope_type, scope_id)
            ).fetchone()
            if row is None:
                cur.execute(
                    "INSERT INTO controls(scope_type, scope_id, mode, risk_factor, allow_new_entries, updated_utc, version, updated_by, reason) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        scope_type, scope_id,
                        DEFAULT_MODE, float(DEFAULT_RISK_FACTOR), int(DEFAULT_ALLOW_NEW_ENTRIES),
                        now_utc(), 1, "system", "init default"
                    )
                )

        _ensure_control("GLOBAL", "")
        # Note: SYMBOL rows created on-demand when app sets them.

        conn.commit()
        conn.close()

@app.on_event("startup")
def _startup():
    db_init()

# =====================================================
# ROOT / HEALTH
# =====================================================
@app.get("/")
def root():
    return {"status": "Signal Agent API is running", "version": "main.py", "controls": "v1-global+symbol"}

@app.get("/health")
def health():
    with _db_lock:
        conn = db_conn()
        conn.execute("SELECT 1").fetchone()
        conn.close()
    return {
        "ok": True,
        "utc": now_utc(),
        "db_path": DB_PATH,
        "queue_max": QUEUE_MAX,
        "auth": {
            "ea_keys_configured": bool(EA_KEYS),
            "app_token_configured": bool(APP_TOKEN),
        }
    }

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
# DEVICES / HEARTBEAT (EA)
# =====================================================
@app.post("/hb")
def heartbeat(hb: HeartbeatEvent, authorization: Optional[str] = Header(default=None)):
    require_ea(authorization)

    sym = norm_symbol(hb.symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")

    device_id = (hb.device_id or "").strip()
    if not device_id:
        device_id = str(uuid4())

    label = (hb.label or "").strip()
    ea_version = (hb.ea_version or "").strip()
    received = now_utc()
    key_hash = _sha256(_bearer_token(authorization))

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        row = cur.execute("SELECT device_id FROM devices WHERE device_id=? LIMIT 1", (device_id,)).fetchone()
        if row is None:
            cur.execute(
                """
                INSERT INTO devices(device_id, label, symbol, magic, ea_version, ea_key_hash, last_seen_utc, status)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (device_id, label, sym, hb.magic, ea_version, key_hash, received, "ONLINE")
            )
        else:
            cur.execute(
                """
                UPDATE devices
                SET label=?, symbol=?, magic=?, ea_version=?, ea_key_hash=?, last_seen_utc=?, status=?
                WHERE device_id=?
                """,
                (label, sym, hb.magic, ea_version, key_hash, received, "ONLINE", device_id)
            )

        conn.commit()
        conn.close()

    return {"ok": True, "server_utc": received, "device_id": device_id, "status": "ONLINE"}

@app.get("/devices")
def list_devices(limit: int = 200, authorization: Optional[str] = Header(default=None)):
    require_app(authorization)
    limit = max(1, min(int(limit), 2000))
    with _db_lock:
        conn = db_conn()
        rows = conn.execute("SELECT * FROM devices ORDER BY last_seen_utc DESC LIMIT ?", (limit,)).fetchall()
        conn.close()
    return {"items": [dict(r) for r in rows], "count": len(rows)}

# =====================================================
# CONTROLS (GLOBAL + SYMBOL)
# =====================================================
def _get_control_row(cur: sqlite3.Cursor, scope_type: str, scope_id: str) -> Optional[sqlite3.Row]:
    return cur.execute(
        "SELECT * FROM controls WHERE scope_type=? AND scope_id=? LIMIT 1",
        (scope_type, scope_id)
    ).fetchone()

def _next_version(cur: sqlite3.Cursor) -> int:
    row = cur.execute("SELECT MAX(version) AS mv FROM controls").fetchone()
    mv = int(row["mv"] or 0)
    return mv + 1

def _normalize_scope(scope_type: str, scope_id: str) -> (str, str):
    st = (scope_type or "").strip().upper()
    sid = (scope_id or "").strip()
    if st not in ("GLOBAL", "SYMBOL"):
        raise HTTPException(status_code=400, detail="bad scope_type")
    if st == "GLOBAL":
        return "GLOBAL", ""
    # SYMBOL
    sym = norm_symbol(sid)
    if not sym:
        raise HTTPException(status_code=400, detail="bad scope_id for SYMBOL")
    return "SYMBOL", sym

def _clamp_risk_factor(x: float) -> float:
    try:
        v = float(x)
    except Exception:
        return DEFAULT_RISK_FACTOR
    # keep it sane:
    if v < 0.05:
        v = 0.05
    if v > 1.0:
        v = 1.0
    return v

def _effective_controls(symbol: str, cur: sqlite3.Cursor) -> Dict[str, Any]:
    sym = norm_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")

    # Ensure GLOBAL exists
    g = _get_control_row(cur, "GLOBAL", "")
    if g is None:
        # should not happen if db_init ran, but safe:
        cur.execute(
            "INSERT INTO controls(scope_type, scope_id, mode, risk_factor, allow_new_entries, updated_utc, version, updated_by, reason) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            ("GLOBAL", "", DEFAULT_MODE, float(DEFAULT_RISK_FACTOR), int(DEFAULT_ALLOW_NEW_ENTRIES), now_utc(), 1, "system", "init default")
        )
        g = _get_control_row(cur, "GLOBAL", "")

    s = _get_control_row(cur, "SYMBOL", sym)  # may be None

    # base from GLOBAL
    mode = (g["mode"] or DEFAULT_MODE).upper()
    rf = float(g["risk_factor"] or DEFAULT_RISK_FACTOR)
    ane = bool(int(g["allow_new_entries"] or 0))

    sources = {
        "global": {"version": int(g["version"]), "updated_utc": g["updated_utc"]},
        "symbol": None
    }

    effective_version = int(g["version"])

    if s is not None:
        sources["symbol"] = {"version": int(s["version"]), "updated_utc": s["updated_utc"]}
        effective_version = max(effective_version, int(s["version"]))

        s_mode = (s["mode"] or mode).upper()
        s_rf = float(s["risk_factor"] or rf)
        s_ane = bool(int(s["allow_new_entries"] or 0))

        # Merge rules:
        # - PAUSED beats all
        # - allow_new_entries false beats all
        # - risk_factor = min
        if s_mode == "PAUSED" or mode == "PAUSED":
            mode = "PAUSED"
        else:
            # if symbol is set to PAUSED it already handled; otherwise symbol can override ON/REDUCED
            if s_mode in ("ON", "REDUCED"):
                mode = s_mode

        ane = (ane and s_ane)  # any false => false
        rf = min(rf, s_rf)

    # normalize rf and ane
    rf = _clamp_risk_factor(rf)

    return {
        "symbol": sym,
        "effective": {
            "mode": mode if mode in ("ON", "REDUCED", "PAUSED") else DEFAULT_MODE,
            "risk_factor": float(rf),
            "allow_new_entries": bool(ane),
        },
        "sources": sources,
        "effective_version": int(effective_version),
    }

@app.get("/controls/effective")
def controls_effective(
    symbol: str,
    since_version: int = 0,
    authorization: Optional[str] = Header(default=None)
):
    # EA reads effective controls
    require_ea(authorization)

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()
        eff = _effective_controls(symbol, cur)
        conn.close()

    ev = int(eff["effective_version"])
    if int(since_version) >= ev:
        return {"unchanged": True, "effective_version": ev}

    return eff

@app.get("/controls/get")
def controls_get(
    scope_type: ScopeType,
    scope_id: str = "",
    authorization: Optional[str] = Header(default=None)
):
    # App reads raw control row
    require_app(authorization)
    st, sid = _normalize_scope(scope_type, scope_id)

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()
        row = _get_control_row(cur, st, sid)
        conn.close()

    if row is None:
        # if SYMBOL missing, return implied defaults (do not auto-create)
        if st == "SYMBOL":
            return {
                "scope_type": "SYMBOL",
                "scope_id": norm_symbol(sid),
                "exists": False,
                "implied": {
                    "mode": None,
                    "risk_factor": None,
                    "allow_new_entries": None
                }
            }
        raise HTTPException(status_code=404, detail="control not found")

    return {"exists": True, "item": dict(row)}

@app.post("/controls/set")
def controls_set(req: ControlsSetRequest, authorization: Optional[str] = Header(default=None)):
    # App sets controls (GLOBAL or SYMBOL)
    require_app(authorization)
    st, sid = _normalize_scope(req.scope_type, req.scope_id or "")

    patch = req.patch
    if patch.mode is None and patch.risk_factor is None and patch.allow_new_entries is None:
        raise HTTPException(status_code=400, detail="empty patch")

    reason = (req.reason or "").strip()[:250]
    updated_by = (req.updated_by or "app").strip()[:80]
    updated_utc = now_utc()

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        old = _get_control_row(cur, st, sid)
        old_json = json.dumps(dict(old), ensure_ascii=False) if old is not None else None

        # base from old or defaults
        mode = (old["mode"] if old is not None else DEFAULT_MODE)
        rf = float(old["risk_factor"] if old is not None else DEFAULT_RISK_FACTOR)
        ane = bool(int(old["allow_new_entries"] if old is not None else int(DEFAULT_ALLOW_NEW_ENTRIES)))

        if patch.mode is not None:
            mode = patch.mode
        if patch.risk_factor is not None:
            rf = _clamp_risk_factor(float(patch.risk_factor))
        if patch.allow_new_entries is not None:
            ane = bool(patch.allow_new_entries)

        # version bump (monotonic across table)
        new_ver = _next_version(cur)

        if old is None:
            cur.execute(
                """
                INSERT INTO controls(scope_type, scope_id, mode, risk_factor, allow_new_entries, updated_utc, version, updated_by, reason)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (st, sid, mode, float(rf), int(ane), updated_utc, new_ver, updated_by, reason)
            )
        else:
            cur.execute(
                """
                UPDATE controls
                SET mode=?, risk_factor=?, allow_new_entries=?, updated_utc=?, version=?, updated_by=?, reason=?
                WHERE scope_type=? AND scope_id=?
                """,
                (mode, float(rf), int(ane), updated_utc, new_ver, updated_by, reason, st, sid)
            )

        new_row = _get_control_row(cur, st, sid)
        new_json = json.dumps(dict(new_row), ensure_ascii=False) if new_row is not None else None

        cur.execute(
            """
            INSERT INTO control_events(scope_type, scope_id, old_json, new_json, updated_utc, updated_by, reason)
            VALUES(?,?,?,?,?,?,?)
            """,
            (st, sid, old_json, new_json, updated_utc, updated_by, reason)
        )

        conn.commit()
        conn.close()

    return {"ok": True, "scope_type": st, "scope_id": sid, "new_version": int(new_ver), "updated_utc": updated_utc}

@app.get("/control_events")
def control_events(scope_type: ScopeType, scope_id: str = "", limit: int = 100, authorization: Optional[str] = Header(default=None)):
    require_app(authorization)
    st, sid = _normalize_scope(scope_type, scope_id or "")
    limit = max(1, min(int(limit), 2000))

    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            "SELECT * FROM control_events WHERE scope_type=? AND scope_id=? ORDER BY id DESC LIMIT ?",
            (st, sid, limit)
        ).fetchall()
        conn.close()

    return {"items": [dict(r) for r in rows], "count": len(rows)}

# =====================================================
# DEALS
# =====================================================
def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _net_profit_row(r: sqlite3.Row) -> float:
    return _safe_float(r["profit"]) + _safe_float(r["commission"]) + _safe_float(r["swap"])

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

        if d.deal_id is not None:
            row = cur.execute("SELECT id FROM deals WHERE deal_id = ? LIMIT 1", (int(d.deal_id),)).fetchone()
            if row is not None:
                conn.close()
                return {"ok": True, "dedup": True, "id": int(row["id"]), "received_utc": received, "symbol": sym}

        cur.execute("""
            INSERT INTO deals(
                received_utc, account, symbol, position_id, magic, ticket, deal_id, type, lots,
                open_time, close_time, open_price, close_price, sl, tp,
                profit, commission, swap, comment,
                risk_usd, r_multiple
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            received, d.account, sym, d.position_id, d.magic, d.ticket, d.deal_id, typ, d.lots,
            d.open_time, d.close_time, d.open_price, d.close_price, d.sl, d.tp,
            d.profit, d.commission, d.swap, d.comment,
            d.risk_usd, d.r_multiple
        ))

        conn.commit()
        row_id = cur.lastrowid
        conn.close()

    return {"ok": True, "id": row_id, "received_utc": received, "symbol": sym}

@app.get("/deals")
def list_deals(symbol: Optional[str] = None, limit: int = 200):
    limit = max(1, min(int(limit), 2000))
    sym = norm_symbol(symbol) if symbol else None

    with _db_lock:
        conn = db_conn()
        if sym:
            rows = conn.execute("SELECT * FROM deals WHERE symbol = ? ORDER BY id DESC LIMIT ?", (sym, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM deals ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        conn.close()

    return {"items": [dict(r) for r in rows], "count": len(rows)}

# =====================================================
# RISKS
# =====================================================
@app.post("/risk")
def ingest_risk(r: RiskEvent):
    sym = norm_symbol(r.symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")
    if not r.position_id or str(r.position_id).strip() == "":
        raise HTTPException(status_code=400, detail="bad position_id")
    if r.risk_usd is None or float(r.risk_usd) <= 0:
        raise HTTPException(status_code=400, detail="bad risk_usd")

    received = now_utc()
    posid = str(r.position_id).strip()
    src = (r.source or "mt5-risk-snapshot").strip()

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        row = cur.execute(
            "SELECT id FROM risks WHERE position_id = ? AND (source = ? OR source IS NULL) ORDER BY id DESC LIMIT 1",
            (posid, src)
        ).fetchone()
        if row is not None:
            conn.close()
            return {"ok": True, "dedup": True, "id": int(row["id"]), "received_utc": received, "symbol": sym, "position_id": posid}

        cur.execute("""
            INSERT INTO risks(
                received_utc, account, symbol, position_id, magic, open_time, entry_price, sl, lots, risk_usd, source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            received, r.account, sym, posid, r.magic, r.open_time, r.entry_price, r.sl, r.lots, float(r.risk_usd), src
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
            rows = conn.execute("SELECT * FROM risks WHERE symbol = ? ORDER BY id DESC LIMIT ?", (sym, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM risks ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        conn.close()

    return {"items": [dict(r) for r in rows], "count": len(rows)}

# =====================================================
# JOIN CHECK (Debug)
# =====================================================
@app.get("/join_check")
def join_check(symbol: str, limit: int = 50):
    sym = norm_symbol(symbol)
    limit = max(1, min(int(limit), 2000))

    with _db_lock:
        conn = db_conn()
        risk_rows = conn.execute("SELECT position_id FROM risks WHERE symbol=? ORDER BY id DESC LIMIT ?", (sym, limit)).fetchall()
        deal_rows = conn.execute("SELECT position_id FROM deals WHERE symbol=? ORDER BY id DESC LIMIT ?", (sym, limit)).fetchall()
        conn.close()

    risks = [str(r["position_id"]) for r in risk_rows if r["position_id"]]
    deals = [str(r["position_id"]) for r in deal_rows if r["position_id"]]
    set_r, set_d = set(risks), set(deals)

    matching = sorted(list(set_r.intersection(set_d)))
    only_r = sorted(list(set_r - set_d))
    only_d = sorted(list(set_d - set_r))

    return {
        "symbol": sym,
        "limit": limit,
        "counts": {
            "risks": len(risks),
            "deals": len(deals),
            "risk_posids": len(set_r),
            "deal_posids": len(set_d),
            "matching_posids": len(matching),
            "only_in_risks": len(only_r),
            "only_in_deals": len(only_d)
        },
        "matching_posids_sample": matching[:10],
        "only_in_risks_sample": only_r[:10],
        "only_in_deals_sample": only_d[:10],
        "tips": [
            "matching_posids should grow after EA entries + deal closes.",
            "If deals have position_id but risks are empty -> /risk not sent yet.",
            "If both exist but do not match -> identifiers mismatch (POSITION_IDENTIFIER vs DEAL_POSITION_ID)."
        ]
    }

# =====================================================
# RE-CALC R (Deal←Risk join)
# =====================================================
@app.get("/recalc_r")
def recalc_r(symbol: str, limit: int = 5000):
    sym = norm_symbol(symbol)
    limit = max(1, min(int(limit), 50000))

    updated = 0
    scanned = 0

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        deals = cur.execute(
            """
            SELECT id, position_id, profit, commission, swap, risk_usd, r_multiple
            FROM deals
            WHERE symbol = ? AND position_id IS NOT NULL AND position_id != ''
            ORDER BY id DESC
            LIMIT ?
            """,
            (sym, limit)
        ).fetchall()

        for d in deals:
            scanned += 1
            deal_row_id = int(d["id"])
            posid = str(d["position_id"])

            has_r = d["r_multiple"] is not None
            has_risk = d["risk_usd"] is not None
            if has_r and has_risk:
                continue

            rrow = cur.execute(
                "SELECT risk_usd FROM risks WHERE position_id=? ORDER BY id DESC LIMIT 1",
                (posid,)
            ).fetchone()
            if rrow is None:
                continue

            risk_usd = float(rrow["risk_usd"] or 0.0)
            if risk_usd <= 0:
                continue

            net = float(_safe_float(d["profit"]) + _safe_float(d["commission"]) + _safe_float(d["swap"]))
            r_mult = net / risk_usd

            cur.execute("UPDATE deals SET risk_usd=?, r_multiple=? WHERE id=?", (risk_usd, r_mult, deal_row_id))
            updated += 1

        conn.commit()

        preview = cur.execute(
            """
            SELECT d.id AS deal_row_id, d.deal_id, d.position_id AS deal_position_id,
                   d.close_time, (COALESCE(d.profit,0)+COALESCE(d.commission,0)+COALESCE(d.swap,0)) AS net_profit,
                   d.risk_usd AS deal_risk_usd, d.r_multiple AS deal_r_multiple,
                   r.id AS risk_row_id, r.risk_usd AS risk_risk_usd, r.open_time AS risk_open_time, r.source AS risk_source
            FROM deals d
            LEFT JOIN risks r ON r.position_id = d.position_id
            WHERE d.symbol = ?
            ORDER BY d.id DESC
            LIMIT 10
            """,
            (sym,)
        ).fetchall()

        conn.close()

    return {"ok": True, "symbol": sym, "scanned": scanned, "updated": updated, "join_preview": [dict(r) for r in preview]}

# =====================================================
# KPI HELPERS
# =====================================================
def _profit_factor(values: List[float]) -> Optional[float]:
    gp = sum(x for x in values if x > 0)
    gl = abs(sum(x for x in values if x < 0))
    if gl <= 0:
        return None
    return gp / gl

def _loss_streak(values: List[float]) -> int:
    s = 0
    for x in reversed(values):
        if x < 0:
            s += 1
        else:
            break
    return s

def _max_drawdown(values: List[float]) -> float:
    peak = float("-inf")
    eq = 0.0
    max_dd = 0.0
    for p in values:
        eq += p
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
    return max_dd

def _today_prefix_mt5() -> str:
    return datetime.now().strftime("%Y.%m.%d")

def _today_pnl(symbol: str) -> float:
    sym = norm_symbol(symbol)
    pref = _today_prefix_mt5()
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            "SELECT profit, commission, swap FROM deals WHERE symbol=? AND close_time LIKE ?",
            (sym, f"{pref}%")
        ).fetchall()
        conn.close()
    return float(sum(_net_profit_row(r) for r in rows))

def _fetch_netpnls_last_n(symbol: str, n: int) -> List[float]:
    sym = norm_symbol(symbol)
    n = max(1, min(int(n), 5000))
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            "SELECT profit, commission, swap FROM deals WHERE symbol=? ORDER BY id DESC LIMIT ?",
            (sym, n)
        ).fetchall()
        conn.close()
    vals = [float(_net_profit_row(r)) for r in rows]
    vals.reverse()
    return vals

def _fetch_netpnls_all(symbol: str) -> List[float]:
    sym = norm_symbol(symbol)
    with _db_lock:
        conn = db_conn()
        rows = conn.execute("SELECT profit, commission, swap FROM deals WHERE symbol=? ORDER BY id ASC", (sym,)).fetchall()
        conn.close()
    return [float(_net_profit_row(r)) for r in rows]

def _fetch_rmultiples_last_n(symbol: str, n: int) -> List[float]:
    sym = norm_symbol(symbol)
    n = max(1, min(int(n), 5000))
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            "SELECT r_multiple FROM deals WHERE symbol=? AND r_multiple IS NOT NULL ORDER BY id DESC LIMIT ?",
            (sym, n)
        ).fetchall()
        conn.close()
    vals = [float(r["r_multiple"]) for r in rows if r["r_multiple"] is not None]
    vals.reverse()
    return vals

def _fetch_rmultiples_all(symbol: str) -> List[float]:
    sym = norm_symbol(symbol)
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            "SELECT r_multiple FROM deals WHERE symbol=? AND r_multiple IS NOT NULL ORDER BY id ASC",
            (sym,)
        ).fetchall()
        conn.close()
    return [float(r["r_multiple"]) for r in rows if r["r_multiple"] is not None]

# =====================================================
# USD KPIs + USD GATE
# =====================================================
@app.get("/status/propfirm")
def status_propfirma(
    symbol: str,
    n: int = 20,
    min_trades: int = 3,
    pf_min: float = 1.30,
    net_last_n_min: float = -200.0,
    loss_streak_max: int = 3,
    daily_dd_limit: float = 350.0,
    total_dd_limit: float = 600.0
):
    sym = norm_symbol(symbol)
    pnls_last = _fetch_netpnls_last_n(sym, n)
    trades = len(pnls_last)

    if trades < int(min_trades):
        return {
            "symbol": sym, "level": "YELLOW",
            "reasons": [f"NotEnoughDeals<{min_trades}"],
            "rolling": {"n": int(n), "trades": trades}
        }

    pf = _profit_factor(pnls_last)
    net_last_n = float(sum(pnls_last))
    loss_streak = _loss_streak(pnls_last)
    today_net = _today_pnl(sym)
    total_max_dd = _max_drawdown(_fetch_netpnls_all(sym))

    reasons: List[str] = []
    if pf is not None and float(pf) < float(pf_min): reasons.append(f"PF<{pf_min}")
    if net_last_n < float(net_last_n_min): reasons.append(f"NetLast{int(n)}<{net_last_n_min}")
    if int(loss_streak) > int(loss_streak_max): reasons.append(f"LossStreak>{int(loss_streak_max)}")
    if today_net <= -abs(float(daily_dd_limit)): reasons.append(f"DailyPnL<={-abs(float(daily_dd_limit))}")
    if total_max_dd > abs(float(total_dd_limit)): reasons.append(f"TotalMaxDD>{abs(float(total_dd_limit))}")

    level = "RED" if reasons else "GREEN"
    return {"symbol": sym, "level": level, "reasons": reasons}

# =====================================================
# R KPIs + R GATE
# =====================================================
@app.get("/status/propfirm_r")
def status_propfirma_r(
    symbol: str,
    n: int = 20,
    min_trades_r: int = 3,
    pf_r_min: float = 1.20,
    net_r_last_n_min: float = -3.0,
    loss_streak_r_max: int = 3,
    max_dd_r_limit: float = 6.0,
    rolling_dd_r_limit: float = 4.0
):
    sym = norm_symbol(symbol)
    rs_last = _fetch_rmultiples_last_n(sym, n)
    trades = len(rs_last)

    if trades < int(min_trades_r):
        return {
            "symbol": sym, "level": "YELLOW",
            "reasons": [f"NotEnoughR<{min_trades_r}"],
            "rolling": {"n": int(n), "trades": trades}
        }

    pf_r = _profit_factor(rs_last)
    net_r_last_n = float(sum(rs_last))
    streak_r = _loss_streak(rs_last)
    rolling_dd_r = _max_drawdown(rs_last)

    rs_all = _fetch_rmultiples_all(sym)
    max_dd_r_all = _max_drawdown(rs_all) if rs_all else 0.0

    reasons: List[str] = []
    if pf_r is not None and float(pf_r) < float(pf_r_min): reasons.append(f"PF_R<{pf_r_min}")
    if net_r_last_n < float(net_r_last_n_min): reasons.append(f"NetRLast{int(n)}<{net_r_last_n_min}")
    if int(streak_r) > int(loss_streak_r_max): reasons.append(f"LossStreakR>{int(loss_streak_r_max)}")
    if float(rolling_dd_r) > float(rolling_dd_r_limit): reasons.append(f"RollingDD_R>{rolling_dd_r_limit}")
    if float(max_dd_r_all) > float(max_dd_r_limit): reasons.append(f"MaxDD_R>{max_dd_r_limit}")

    level = "RED" if reasons else "GREEN"
    return {
        "symbol": sym, "level": level, "reasons": reasons,
        "rolling": {
            "n": int(n), "trades": trades,
            "pf_r": (round(pf_r, 4) if pf_r is not None else None),
            "net_r_last_n": round(net_r_last_n, 4),
            "loss_streak_r": int(streak_r),
            "rolling_dd_r": round(rolling_dd_r, 4),
        },
        "total": {"max_dd_r": round(max_dd_r_all, 4)}
    }

# =====================================================
# COMBO GATE (USD+R) per SYMBOL
# =====================================================
@app.get("/status/gate_combo")
def status_gate_combo(symbol: str, n: int = 20):
    usd = status_propfirma(symbol=symbol, n=n)
    r = status_propfirma_r(symbol=symbol, n=n)

    usd_level = (usd.get("level") or "YELLOW").upper()
    r_level = (r.get("level") or "YELLOW").upper()

    if usd_level == "RED" or r_level == "RED":
        combo = "RED"
    elif usd_level == "GREEN" and r_level == "GREEN":
        combo = "GREEN"
    else:
        combo = "YELLOW"

    return {
        "symbol": norm_symbol(symbol),
        "combo_level": combo,
        "usd_level": usd_level,
        "r_level": r_level,
        "usd": usd,
        "r": r,
        "note": "Combo: RED if any RED, GREEN if both GREEN, else YELLOW."
    }

# =====================================================
# DASHBOARD (read-only monitoring)
# =====================================================
def _last_rows(table: str, symbol: Optional[str], limit: int) -> List[dict]:
    limit = max(1, min(int(limit), 200))
    sym = norm_symbol(symbol) if symbol else None
    with _db_lock:
        conn = db_conn()
        if sym:
            rows = conn.execute(f"SELECT * FROM {table} WHERE symbol=? ORDER BY id DESC LIMIT ?", (sym, limit)).fetchall()
        else:
            rows = conn.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        conn.close()
    return [dict(r) for r in rows]

def _parse_syms_csv(s: str) -> List[str]:
    parts = [norm_symbol(x) for x in (s or "").split(",")]
    return [p for p in parts if p]

@app.get("/dashboard")
def dashboard(
    symbols: str = "BTCUSD,XAUUSD",
    limit_deals: int = 10,
    limit_risks: int = 10,
    n_gate: int = 20,
    authorization: Optional[str] = Header(default=None)
):
    # For v1 you can leave dashboard public or protect it.
    # If you want to protect it now, uncomment next line:
    # require_app(authorization)

    syms = _parse_syms_csv(symbols)

    # signals snapshot
    sigs: Dict[str, Any] = {}
    for s in syms:
        if s in STATE:
            sigs[s] = {
                "latest": STATE[s].get("latest"),
                "updated_utc": STATE[s].get("updated_utc"),
                "ack": STATE[s].get("ack")
            }
        else:
            sigs[s] = {"latest": None, "updated_utc": None, "ack": None}

    # per-symbol gates + last rows + effective controls
    per: Dict[str, Any] = {}
    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()
        for s in syms:
            per[s] = {
                "gate_combo": status_gate_combo(symbol=s, n=n_gate),
                "gate_r": status_propfirma_r(symbol=s, n=n_gate),
                "last_deals": _last_rows("deals", s, limit_deals),
                "last_risks": _last_rows("risks", s, limit_risks),
                "join_check": join_check(symbol=s, limit=50),
                "controls_effective": _effective_controls(s, cur),
            }
        conn.close()

    return {
        "utc": now_utc(),
        "symbols": syms,
        "signals": sigs,
        "per_symbol": per,
        "note": "Read-only dashboard."
    }

# =====================================================
# SUMMARY (legacy)
# =====================================================
@app.get("/summary")
def summary(symbol: Optional[str] = None, limit: int = 20):
    limit = max(1, min(int(limit), 200))
    sym = norm_symbol(symbol) if symbol else None

    with _db_lock:
        conn = db_conn()
        if sym:
            rows = conn.execute("SELECT * FROM deals WHERE symbol=? ORDER BY id DESC LIMIT ?", (sym, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM deals ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        conn.close()

    sigs = {}
    if sym:
        if sym in STATE:
            sigs[sym] = {"latest": STATE[sym].get("latest"), "updated_utc": STATE[sym].get("updated_utc"), "ack": STATE[sym].get("ack")}
    else:
        for s in list(STATE.keys())[:50]:
            sigs[s] = {"latest": STATE[s].get("latest"), "updated_utc": STATE[s].get("updated_utc"), "ack": STATE[s].get("ack")}

    return {"filters": {"symbol": sym, "limit": limit}, "last_deals": [dict(r) for r in rows], "signals": sigs}
