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
DB_PATH = os.getenv("DB_PATH", "data.db")
_db_lock = Lock()

# Optional Signal TTL (server-side safety; EAs haben zusätzlich InpMaxSignalAgeSec)
SIGNAL_TTL_SEC = int(os.getenv("SIGNAL_TTL_SEC", "600"))  # 10 min default

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
    position_id: Optional[str] = None
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
    source: Optional[str] = "init_sl"

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
            "updated_ts": None,          # server timestamp (epoch sec)
            "queue": deque(maxlen=QUEUE_MAX),
            "acks": {}                  # consumer -> updated_utc
        }

def _epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def _signal_expired(sym: str) -> bool:
    if sym not in STATE: return True
    ts = STATE[sym].get("updated_ts")
    if ts is None: return True
    return (_epoch() - int(ts)) > SIGNAL_TTL_SEC

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

        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_deal_id ON deals(deal_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_position_id ON deals(position_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_symbol_id ON deals(symbol, id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risks_position_id ON risks(position_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risks_symbol_id ON risks(symbol, id)")

        # Safe migrations
        dcols = _table_cols(conn, "deals")
        for col, coldef in [("position_id", "TEXT"), ("risk_usd", "REAL"), ("r_multiple", "REAL")]:
            if col not in dcols:
                try: cur.execute(f"ALTER TABLE deals ADD COLUMN {col} {coldef}")
                except Exception: pass

        rcols = _table_cols(conn, "risks")
        for col, coldef in [
            ("account", "TEXT"), ("magic", "INTEGER"), ("open_time", "TEXT"),
            ("entry_price", "REAL"), ("sl", "REAL"), ("lots", "REAL"), ("source", "TEXT")
        ]:
            if col not in rcols:
                try: cur.execute(f"ALTER TABLE risks ADD COLUMN {col} {coldef}")
                except Exception: pass

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
    return {"status": "Signal Agent API is running", "utc": now_utc()}

@app.get("/health")
def health():
    with _db_lock:
        conn = db_conn()
        conn.execute("SELECT 1").fetchone()
        conn.close()
    return {"ok": True, "utc": now_utc(), "db_path": DB_PATH, "queue_max": QUEUE_MAX, "signal_ttl_sec": SIGNAL_TTL_SEC}

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
    STATE[sym]["updated_ts"] = _epoch()

    # Reset acks on new signal (important!)
    STATE[sym]["acks"] = {}

    STATE[sym]["queue"].append({"signal": payload, "updated_utc": t})
    return {"ok": True, "symbol": sym, "updated_utc": t}

@app.get("/latest")
def latest(symbol: str):
    sym = norm_symbol(symbol)
    if sym not in STATE or STATE[sym]["latest"] is None:
        return {"symbol": sym, "signal": None, "updated_utc": None}

    # Server-side TTL: if expired, clear it
    if _signal_expired(sym):
        STATE[sym]["latest"] = None
        STATE[sym]["updated_utc"] = None
        STATE[sym]["updated_ts"] = None
        STATE[sym]["acks"] = {}
        return {"symbol": sym, "signal": None, "updated_utc": None}

    return {"symbol": sym, "signal": STATE[sym]["latest"], "updated_utc": STATE[sym]["updated_utc"]}

@app.get("/queue")
def queue(symbol: str):
    sym = norm_symbol(symbol)
    if sym not in STATE:
        return {"symbol": sym, "items": []}
    return {"symbol": sym, "items": list(STATE[sym]["queue"])}

# =====================================================
# MULTI-CONSUMER ACK
# =====================================================
@app.post("/ack")
def ack(symbol: str, updated_utc: str, consumer: str):
    sym = norm_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")
    if not updated_utc or updated_utc.strip() == "":
        raise HTTPException(status_code=400, detail="bad updated_utc")
    if not consumer or consumer.strip() == "":
        raise HTTPException(status_code=400, detail="bad consumer")
    if sym not in STATE:
        raise HTTPException(status_code=404, detail="unknown symbol")

    # If current signal is gone, still accept ack (idempotent)
    cur_upd = STATE[sym].get("updated_utc")
    STATE[sym]["acks"][consumer.strip()] = updated_utc.strip()

    # We do NOT clear latest here (multi-consumer)
    return {
        "ok": True,
        "symbol": sym,
        "consumer": consumer.strip(),
        "ack": updated_utc.strip(),
        "current_updated_utc": cur_upd,
        "note": "Multi-consumer ack: latest is NOT cleared on first ack."
    }

@app.get("/ack_status")
def ack_status(symbol: str):
    sym = norm_symbol(symbol)
    if sym not in STATE:
        return {"symbol": sym, "updated_utc": None, "acks": {}}
    return {
        "symbol": sym,
        "updated_utc": STATE[sym].get("updated_utc"),
        "acks": STATE[sym].get("acks") or {}
    }

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
            row = cur.execute("SELECT id FROM deals WHERE deal_id=? LIMIT 1", (int(d.deal_id),)).fetchone()
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
            rows = conn.execute("SELECT * FROM deals WHERE symbol=? ORDER BY id DESC LIMIT ?", (sym, limit)).fetchall()
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
    posid = str(r.position_id or "").strip()
    if not posid:
        raise HTTPException(status_code=400, detail="bad position_id")
    if r.risk_usd is None or float(r.risk_usd) <= 0:
        raise HTTPException(status_code=400, detail="bad risk_usd")

    received = now_utc()
    src = (r.source or "init_sl").strip()

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        # Dedup: position_id + source
        row = cur.execute(
            "SELECT id FROM risks WHERE position_id=? AND source=? ORDER BY id DESC LIMIT 1",
            (posid, src)
        ).fetchone()
        if row is not None:
            conn.close()
            return {"ok": True, "dedup": True, "id": int(row["id"]), "received_utc": received, "symbol": sym, "position_id": posid}

        cur.execute("""
            INSERT INTO risks(
                received_utc, account, symbol, position_id, magic, open_time,
                entry_price, sl, lots, risk_usd, source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            received, r.account, sym, posid, r.magic, r.open_time,
            r.entry_price, r.sl, r.lots, float(r.risk_usd), src
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
            rows = conn.execute("SELECT * FROM risks WHERE symbol=? ORDER BY id DESC LIMIT ?", (sym, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM risks ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        conn.close()
    return {"items": [dict(r) for r in rows], "count": len(rows)}

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
            WHERE symbol=? AND position_id IS NOT NULL AND position_id!=''
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

            rrow = cur.execute("SELECT risk_usd FROM risks WHERE position_id=? ORDER BY id DESC LIMIT 1", (posid,)).fetchone()
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
                   d.close_time,
                   (COALESCE(d.profit,0)+COALESCE(d.commission,0)+COALESCE(d.swap,0)) AS net_profit,
                   d.risk_usd AS deal_risk_usd, d.r_multiple AS deal_r_multiple,
                   r.id AS risk_row_id, r.risk_usd AS risk_risk_usd, r.open_time AS risk_open_time, r.source AS risk_source
            FROM deals d
            LEFT JOIN risks r ON r.position_id = d.position_id
            WHERE d.symbol=?
            ORDER BY d.id DESC
            LIMIT 10
            """,
            (sym,)
        ).fetchall()

        conn.close()

    return {"ok": True, "symbol": sym, "scanned": scanned, "updated": updated, "join_preview": [dict(r) for r in preview]}

# =====================================================
# R-GATE
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

@app.get("/status/gate_combo")
def status_gate_combo(symbol: str, n: int = 20):
    # For now: combo == R-gate only (safe). You can re-add USD gate later.
    r = status_propfirma_r(symbol=symbol, n=n)
    lvl = (r.get("level") or "YELLOW").upper()
    return {
        "symbol": norm_symbol(symbol),
        "combo_level": lvl,
        "r_level": lvl,
        "r": r,
        "note": "Combo currently equals R-gate (safe default)."
    }
