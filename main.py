from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Union
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
    # Accept extra fields from MT5 sender (prevents 422 extra_forbidden)
    class Config:
        extra = "allow"
        allow_population_by_field_name = True

    account: Optional[str] = None
    symbol: str
    magic: Optional[int] = None
    ticket: Optional[int] = None
    deal_id: Optional[str] = None
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

    # Risk / R (optional; API will auto-fill if missing)
    risk_usd: Optional[float] = None
    r_multiple: Optional[float] = None

    # MT5 sender alias fields
    account_login: Optional[str] = Field(default=None, alias="account_login")
    server: Optional[str] = Field(default=None, alias="server")
    entry: Optional[str] = Field(default=None, alias="entry")
    deal_time: Optional[str] = Field(default=None, alias="deal_time")
    price: Optional[float] = Field(default=None, alias="price")
    position_id: Optional[str] = Field(default=None, alias="position_id")

class RiskSnapshot(BaseModel):
    class Config:
        extra = "allow"

    symbol: str
    position_id: str
    magic: Optional[int] = None
    open_time: Optional[str] = None
    entry_price: Optional[float] = None
    sl: Optional[float] = None
    lots: Optional[float] = None
    risk_usd: float

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

def _col_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)

def db_init() -> None:
    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        # --- deals table (extended) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_utc TEXT NOT NULL,
            account TEXT,
            symbol TEXT NOT NULL,
            magic INTEGER,
            ticket INTEGER,
            deal_id TEXT,
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

        # indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_symbol_time ON deals(symbol, close_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_magic ON deals(magic)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_deal_id ON deals(deal_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_pos_id ON deals(position_id)")

        # safe migrations (only if missing)
        for col, typ in [
            ("deal_id", "TEXT"),
            ("position_id", "TEXT"),
            ("risk_usd", "REAL"),
            ("r_multiple", "REAL"),
        ]:
            try:
                if not _col_exists(cur, "deals", col):
                    cur.execute(f"ALTER TABLE deals ADD COLUMN {col} {typ}")
            except Exception:
                pass

        # --- risks table (position_id -> initial risk) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS risks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_utc TEXT NOT NULL,
            symbol TEXT NOT NULL,
            position_id TEXT NOT NULL,
            magic INTEGER,
            open_time TEXT,
            entry_price REAL,
            sl REAL,
            lots REAL,
            risk_usd REAL NOT NULL
        )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_risks_pos ON risks(position_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risks_symbol ON risks(symbol)")

        conn.commit()
        conn.close()

@app.on_event("startup")
def _startup():
    db_init()

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
# HEALTH / MONITORING
# =====================================================
@app.get("/health")
def health():
    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        deals_count = int(cur.execute("SELECT COUNT(1) AS c FROM deals").fetchone()["c"])
        risks_count = int(cur.execute("SELECT COUNT(1) AS c FROM risks").fetchone()["c"])

        last_deal = cur.execute(
            "SELECT received_utc, symbol FROM deals ORDER BY id DESC LIMIT 1"
        ).fetchone()
        last_risk = cur.execute(
            "SELECT received_utc, symbol FROM risks ORDER BY id DESC LIMIT 1"
        ).fetchone()

        conn.close()

    return {
        "ok": True,
        "db_path": DB_PATH,
        "counts": {
            "deals": deals_count,
            "risks": risks_count
        },
        "last": {
            "deal_received_utc": (last_deal["received_utc"] if last_deal else None),
            "deal_symbol": (last_deal["symbol"] if last_deal else None),
            "risk_received_utc": (last_risk["received_utc"] if last_risk else None),
            "risk_symbol": (last_risk["symbol"] if last_risk else None)
        },
        "time_utc": now_utc()
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
    return {
        "symbol": sym,
        "signal": STATE[sym]["latest"],
        "updated_utc": STATE[sym]["updated_utc"]
    }

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

    # If ACK matches current latest -> clear latest
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
# MT5 → RISK SNAPSHOT (ENTRY)
# =====================================================
@app.post("/risk")
def ingest_risk(r: RiskSnapshot):
    sym = norm_symbol(r.symbol)
    pos_id = (r.position_id or "").strip()

    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")
    if not pos_id:
        raise HTTPException(status_code=400, detail="bad position_id")
    if r.risk_usd is None or float(r.risk_usd) <= 0:
        raise HTTPException(status_code=400, detail="risk_usd must be > 0")

    received = now_utc()

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        # Upsert by UNIQUE(position_id)
        try:
            cur.execute(
                """
                INSERT INTO risks(received_utc,symbol,position_id,magic,open_time,entry_price,sl,lots,risk_usd)
                VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(position_id) DO UPDATE SET
                  received_utc=excluded.received_utc,
                  symbol=excluded.symbol,
                  magic=excluded.magic,
                  open_time=excluded.open_time,
                  entry_price=excluded.entry_price,
                  sl=excluded.sl,
                  lots=excluded.lots,
                  risk_usd=excluded.risk_usd
                """,
                (received, sym, pos_id, r.magic, r.open_time, r.entry_price, r.sl, r.lots, float(r.risk_usd))
            )
        except Exception:
            # fallback (older sqlite): replace
            cur.execute(
                """
                INSERT OR REPLACE INTO risks(received_utc,symbol,position_id,magic,open_time,entry_price,sl,lots,risk_usd)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (received, sym, pos_id, r.magic, r.open_time, r.entry_price, r.sl, r.lots, float(r.risk_usd))
            )

        conn.commit()
        conn.close()

    return {"ok": True, "received_utc": received, "symbol": sym, "position_id": pos_id, "risk_usd": float(r.risk_usd)}

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
# MT5 → DEAL INGEST (CLOSE) + AUTO-R
# =====================================================
@app.post("/deal")
def ingest_deal(payload: Union[DealEvent, List[DealEvent]]):
    # Accept either a single object {...} or a list [{...}]
    d: DealEvent
    if isinstance(payload, list):
        if len(payload) < 1:
            raise HTTPException(status_code=400, detail="empty deal list")
        d = payload[0]
    else:
        d = payload

    sym = norm_symbol(d.symbol)
    typ = norm_action(d.type)

    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")
    if typ not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="bad type (BUY/SELL)")

    received = now_utc()

    # Map possible aliases
    account = d.account or d.account_login
    close_time = d.close_time or d.deal_time
    close_price = d.close_price if d.close_price is not None else d.price

    deal_id_norm: Optional[str] = None
    if d.deal_id is not None:
        deal_id_norm = str(d.deal_id).strip() or None

    pos_id_norm: Optional[str] = None
    if getattr(d, "position_id", None):
        pos_id_norm = str(d.position_id).strip() or None

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        # Dedup if deal_id exists
        if deal_id_norm is not None:
            row = cur.execute(
                "SELECT id FROM deals WHERE deal_id = ? LIMIT 1",
                (deal_id_norm,)
            ).fetchone()
            if row is not None:
                conn.close()
                return {"ok": True, "dedup": True, "id": int(row["id"]), "received_utc": received, "symbol": sym}

        # Auto attach risk + compute R
        risk_usd = d.risk_usd
        r_mult = d.r_multiple

        if (risk_usd is None or r_mult is None) and pos_id_norm:
            rr = cur.execute("SELECT risk_usd FROM risks WHERE position_id=? LIMIT 1", (pos_id_norm,)).fetchone()
            if rr is not None:
                risk_usd = float(rr["risk_usd"])

        net_pnl = float((d.profit or 0.0) + (d.commission or 0.0) + (d.swap or 0.0))
        if r_mult is None and risk_usd is not None and float(risk_usd) > 0:
            r_mult = net_pnl / float(risk_usd)

        cur.execute("""
            INSERT INTO deals(
                received_utc, account, symbol, magic, ticket, deal_id, position_id, type, lots,
                open_time, close_time, open_price, close_price, sl, tp,
                profit, commission, swap, comment,
                risk_usd, r_multiple
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            received, account, sym, d.magic, d.ticket, deal_id_norm, pos_id_norm, typ, d.lots,
            d.open_time, close_time, d.open_price, close_price, d.sl, d.tp,
            d.profit, d.commission, d.swap, d.comment,
            risk_usd, r_mult
        ))

        conn.commit()
        row_id = cur.lastrowid
        conn.close()

    return {"ok": True, "id": row_id, "received_utc": received, "symbol": sym, "r_multiple": r_mult}

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

    return {"items": [dict(r) for r in rows], "count": len(rows)}

# =====================================================
# KPI HELPERS (€)
# =====================================================
def _pnl_row(r: sqlite3.Row) -> float:
    return float((r["profit"] or 0.0) + (r["commission"] or 0.0) + (r["swap"] or 0.0))

def _fetch_pnls(symbol: str) -> List[float]:
    sym = norm_symbol(symbol)
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            "SELECT profit, commission, swap FROM deals WHERE symbol = ? ORDER BY id ASC",
            (sym,)
        ).fetchall()
        conn.close()
    return [_pnl_row(r) for r in rows]

def _fetch_pnls_last_n(symbol: str, n: int) -> List[float]:
    sym = norm_symbol(symbol)
    n = max(1, min(int(n), 5000))
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            "SELECT profit, commission, swap FROM deals WHERE symbol = ? ORDER BY id DESC LIMIT ?",
            (sym, n)
        ).fetchall()
        conn.close()
    pnls = [_pnl_row(r) for r in rows]
    pnls.reverse()
    return pnls

def _profit_factor(pnls: List[float]) -> Optional[float]:
    gp = sum(x for x in pnls if x > 0)
    gl = abs(sum(x for x in pnls if x < 0))
    if gl <= 0:
        return None
    return gp / gl

def _loss_streak(pnls: List[float]) -> int:
    s = 0
    for x in reversed(pnls):
        if x < 0:
            s += 1
        else:
            break
    return s

def _max_drawdown(pnls: List[float]) -> float:
    peak = float("-inf")
    eq = 0.0
    max_dd = 0.0
    for p in pnls:
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
            "SELECT profit, commission, swap FROM deals WHERE symbol = ? AND close_time LIKE ?",
            (sym, f"{pref}%")
        ).fetchall()
        conn.close()
    return float(sum(_pnl_row(r) for r in rows))

# =====================================================
# KPIs + € Gate
# =====================================================
@app.get("/kpis/rolling")
def kpis_rolling(symbol: str, n: int = 20):
    sym = norm_symbol(symbol)
    pnls = _fetch_pnls_last_n(sym, n)

    trades = len(pnls)
    wins = sum(1 for x in pnls if x > 0)
    losses = sum(1 for x in pnls if x < 0)
    net = float(sum(pnls))
    pf = _profit_factor(pnls)
    streak = _loss_streak(pnls)
    max_dd = _max_drawdown(pnls)

    return {
        "symbol": sym,
        "n": int(n),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "net_last_n": round(net, 2),
        "profit_factor": (round(pf, 3) if pf is not None else None),
        "loss_streak": int(streak),
        "max_drawdown_last_n": round(max_dd, 2),
    }

@app.get("/status/propfirm")
def status_propfirma(
    symbol: str,
    n: int = 20,
    pf_min: float = 1.30,
    net_last_n_min: float = -200.0,
    loss_streak_max: int = 3,
    daily_dd_limit: float = 350.0,
    total_dd_limit: float = 600.0
):
    sym = norm_symbol(symbol)

    pnls_last = _fetch_pnls_last_n(sym, n)
    pf = _profit_factor(pnls_last)
    net_last_n = float(sum(pnls_last))
    loss_streak = _loss_streak(pnls_last)

    today_net = _today_pnl(sym)

    pnls_all = _fetch_pnls(sym)
    total_max_dd = _max_drawdown(pnls_all)

    reasons: List[str] = []

    if pf is not None and float(pf) < float(pf_min):
        reasons.append(f"PF<{pf_min}")
    if net_last_n < float(net_last_n_min):
        reasons.append(f"NetLast{int(n)}<{net_last_n_min}")
    if int(loss_streak) > int(loss_streak_max):
        reasons.append(f"LossStreak>{int(loss_streak_max)}")
    if today_net <= -abs(float(daily_dd_limit)):
        reasons.append(f"DailyPnL<={-abs(float(daily_dd_limit))}")
    if total_max_dd > abs(float(total_dd_limit)):
        reasons.append(f"TotalMaxDD>{abs(float(total_dd_limit))}")

    level = "RED" if reasons else "GREEN"

    return {
        "symbol": sym,
        "level": level,
        "reasons": reasons,
        "rolling": {
            "n": int(n),
            "profit_factor": (round(pf, 3) if pf is not None else None),
            "net_last_n": round(net_last_n, 2),
            "loss_streak": int(loss_streak),
        },
        "today": {"day": _today_prefix_mt5(), "net_profit": round(today_net, 2)},
        "total": {"max_drawdown": round(total_max_dd, 2)},
        "thresholds": {
            "pf_min": float(pf_min),
            "net_last_n_min": float(net_last_n_min),
            "loss_streak_max": int(loss_streak_max),
            "daily_dd_limit": -abs(float(daily_dd_limit)),
            "total_dd_limit": abs(float(total_dd_limit)),
        }
    }

# =====================================================
# R helpers + R KPIs + R Gate
# =====================================================
def _fetch_rs_last_n(symbol: str, n: int) -> List[float]:
    sym = norm_symbol(symbol)
    n = max(1, min(int(n), 5000))
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            """
            SELECT r_multiple
            FROM deals
            WHERE symbol = ?
              AND r_multiple IS NOT NULL
            ORDER BY id DESC
            LIMIT ?
            """,
            (sym, n)
        ).fetchall()
        conn.close()
    rs = [float(r["r_multiple"]) for r in rows if r["r_multiple"] is not None]
    rs.reverse()
    return rs

def _max_drawdown_series(xs: List[float]) -> float:
    peak = float("-inf")
    eq = 0.0
    max_dd = 0.0
    for x in xs:
        eq += x
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
    return max_dd

def _loss_streak_count(xs: List[float]) -> int:
    s = 0
    for x in reversed(xs):
        if x < 0:
            s += 1
        else:
            break
    return s

@app.get("/kpis/r_rolling")
def kpis_r_rolling(symbol: str, n: int = 20):
    sym = norm_symbol(symbol)
    rs = _fetch_rs_last_n(sym, n)

    trades = len(rs)
    wins = sum(1 for x in rs if x > 0)
    losses = sum(1 for x in rs if x < 0)
    net_r = float(sum(rs)) if rs else 0.0
    dd_r = float(_max_drawdown_series(rs)) if rs else 0.0
    streak = _loss_streak_count(rs) if rs else 0

    return {
        "symbol": sym,
        "n": int(n),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "net_r_last_n": round(net_r, 3),
        "max_drawdown_r_last_n": round(dd_r, 3),
        "loss_streak": int(streak)
    }

@app.get("/status/propfirm_r")
def status_propfirma_r(
    symbol: str,
    n: int = 20,
    net_r_min: float = -3.0,
    dd_r_max: float = 4.0,
    loss_streak_max: int = 3
):
    sym = norm_symbol(symbol)
    rs = _fetch_rs_last_n(sym, n)

    if len(rs) == 0:
        return {"symbol": sym, "level": "YELLOW", "reasons": ["No R data yet (need /risk snapshots)"]}

    reasons: List[str] = []

    net_r = float(sum(rs))
    dd_r = float(_max_drawdown_series(rs))
    streak = int(_loss_streak_count(rs))

    if net_r < float(net_r_min): reasons.append(f"NetRLast{int(n)}<{net_r_min}")
    if dd_r > float(dd_r_max): reasons.append(f"DD_R>{dd_r_max}")
    if streak > int(loss_streak_max): reasons.append(f"LossStreak>{int(loss_streak_max)}")

    level = "RED" if reasons else "GREEN"
    return {
        "symbol": sym,
        "level": level,
        "reasons": reasons,
        "rolling": {
            "n": int(n),
            "net_r_last_n": round(net_r, 3),
            "max_drawdown_r_last_n": round(dd_r, 3),
            "loss_streak": streak
        },
        "thresholds": {
            "net_r_min": float(net_r_min),
            "dd_r_max": float(dd_r_max),
            "loss_streak_max": int(loss_streak_max)
        }
    }

# =====================================================
# SUMMARY
# =====================================================
@app.get("/summary")
def summary(symbol: Optional[str] = None, limit: int = 20):
    limit = max(1, min(int(limit), 200))
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

    sigs = {}
    if sym:
        if sym in STATE:
            sigs[sym] = {"latest": STATE[sym].get("latest"), "updated_utc": STATE[sym].get("updated_utc"), "ack": STATE[sym].get("ack")}
    else:
        for s in list(STATE.keys())[:50]:
            sigs[s] = {"latest": STATE[s].get("latest"), "updated_utc": STATE[s].get("updated_utc"), "ack": STATE[s].get("ack")}

    return {
        "filters": {"symbol": sym, "limit": limit},
        "last_deals": [dict(r) for r in rows],
        "signals": sigs,
        "note": "R: POST /risk at entry (position_id); /deal at close auto computes r_multiple via position_id."
    }
