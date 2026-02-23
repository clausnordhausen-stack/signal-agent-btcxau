from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from collections import deque
import sqlite3
import os
from threading import Lock
import math

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
    account: Optional[str] = None
    symbol: str

    magic: Optional[int] = None
    ticket: Optional[int] = None
    deal_id: Optional[int] = None

    # NEW: Join key to /risk
    position_id: Optional[str] = None

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

    # Optional: can be provided by EA, but we also compute server-side if possible
    risk_usd: Optional[float] = None
    r_multiple: Optional[float] = None


class RiskEvent(BaseModel):
    account: Optional[str] = None
    symbol: str
    position_id: str
    magic: int

    open_time: Optional[str] = None
    entry_price: Optional[float] = None
    sl: Optional[float] = None
    lots: Optional[float] = None
    risk_usd: Optional[float] = None
    source: Optional[str] = None


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

def db_init() -> None:
    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        # Deals
        cur.execute("""
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_utc TEXT NOT NULL,
            account TEXT,
            symbol TEXT NOT NULL,

            magic INTEGER,
            ticket INTEGER,
            deal_id INTEGER,

            -- NEW: join to risks
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_posid ON deals(position_id)")

        # auto-migration
        for col, ddl in [
            ("risk_usd", "REAL"),
            ("r_multiple", "REAL"),
            ("position_id", "TEXT"),
        ]:
            try:
                cur.execute(f"ALTER TABLE deals ADD COLUMN {col} {ddl}")
            except Exception:
                pass

        conn.commit()
        conn.close()

def db_init_risks() -> None:
    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS risks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_utc TEXT NOT NULL,
            account TEXT,
            symbol TEXT NOT NULL,
            position_id TEXT NOT NULL,
            magic INTEGER NOT NULL,

            open_time TEXT,
            entry_price REAL,
            sl REAL,
            lots REAL,
            risk_usd REAL,
            source TEXT
        )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_risks_symbol_time ON risks(symbol, open_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risks_posid ON risks(position_id)")

        conn.commit()
        conn.close()

@app.on_event("startup")
def _startup():
    db_init()
    db_init_risks()

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

    # If ACK matches current latest -> clear latest (single-consumer semantics)
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
# MT5 → RISK INGEST (Step 7.3 foundation)
# =====================================================
@app.post("/risk")
def ingest_risk(r: RiskEvent):
    sym = norm_symbol(r.symbol)
    if not sym or sym == "0":
        raise HTTPException(status_code=400, detail="bad symbol")
    posid = (r.position_id or "").strip()
    if not posid or posid == "0":
        raise HTTPException(status_code=400, detail="bad position_id")
    if int(r.magic) == 0:
        raise HTTPException(status_code=400, detail="bad magic")

    received = now_utc()

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        # dedup: 1 snapshot per position_id (EA retry safe)
        row = cur.execute("SELECT id FROM risks WHERE position_id = ? LIMIT 1", (posid,)).fetchone()
        if row is not None:
            conn.close()
            return {"ok": True, "dedup": True, "id": int(row["id"]), "received_utc": received, "symbol": sym, "position_id": posid}

        cur.execute("""
            INSERT INTO risks(
                received_utc, account, symbol, position_id, magic,
                open_time, entry_price, sl, lots, risk_usd, source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            received, r.account, sym, posid, int(r.magic),
            r.open_time, r.entry_price, r.sl, r.lots, r.risk_usd, r.source
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
# MT5 → DEAL INGEST (Step 7)
# =====================================================
def _net_profit(d: DealEvent) -> float:
    return float((d.profit or 0.0) + (d.commission or 0.0) + (d.swap or 0.0))

def _fetch_risk_usd_by_position_id(conn: sqlite3.Connection, posid: str) -> Optional[float]:
    row = conn.execute(
        "SELECT risk_usd FROM risks WHERE position_id = ? ORDER BY id DESC LIMIT 1",
        (posid,)
    ).fetchone()
    if row is None:
        return None
    v = row["risk_usd"]
    if v is None:
        return None
    try:
        v = float(v)
    except Exception:
        return None
    return v if v > 0 else None

@app.post("/deal")
def ingest_deal(d: DealEvent):
    sym = norm_symbol(d.symbol)
    typ = norm_action(d.type)

    if not sym:
        raise HTTPException(status_code=400, detail="bad symbol")
    if typ not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="bad type (BUY/SELL)")

    received = now_utc()
    posid = (d.position_id or "").strip() or None

    with _db_lock:
        conn = db_conn()
        cur = conn.cursor()

        # Optional: dedup if deal_id exists (avoid duplicates if EA retries)
        if d.deal_id is not None:
            row = cur.execute("SELECT id FROM deals WHERE deal_id = ? LIMIT 1", (int(d.deal_id),)).fetchone()
            if row is not None:
                conn.close()
                return {"ok": True, "dedup": True, "id": int(row["id"]), "received_utc": received, "symbol": sym}

        # compute risk_usd/r_multiple if missing and we have position_id
        net = _net_profit(d)
        risk = d.risk_usd
        rmult = d.r_multiple

        if (risk is None or float(risk) <= 0.0) and posid:
            risk = _fetch_risk_usd_by_position_id(conn, posid)

        if rmult is None and (risk is not None) and float(risk) > 0.0:
            rmult = float(net) / float(risk)

        cur.execute("""
            INSERT INTO deals(
                received_utc, account, symbol, magic, ticket, deal_id, position_id, type, lots,
                open_time, close_time, open_price, close_price, sl, tp,
                profit, commission, swap, comment,
                risk_usd, r_multiple
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            received, d.account, sym, d.magic, d.ticket, d.deal_id, posid, typ, d.lots,
            d.open_time, d.close_time, d.open_price, d.close_price, d.sl, d.tp,
            d.profit, d.commission, d.swap, d.comment,
            risk, rmult
        ))

        conn.commit()
        row_id = cur.lastrowid
        conn.close()

    return {"ok": True, "id": row_id, "received_utc": received, "symbol": sym, "position_id": posid, "risk_usd": risk, "r_multiple": rmult}

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
# KPI HELPERS (USD PnL, old gates)
# =====================================================
def _pnl_row(r: sqlite3.Row) -> float:
    return float((r["profit"] or 0.0) + (r["commission"] or 0.0) + (r["swap"] or 0.0))

def _fetch_pnls(symbol: str) -> List[float]:
    sym = norm_symbol(symbol)
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            """
            SELECT profit, commission, swap
            FROM deals
            WHERE symbol = ?
            ORDER BY id ASC
            """,
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
            """
            SELECT profit, commission, swap
            FROM deals
            WHERE symbol = ?
            ORDER BY id DESC
            LIMIT ?
            """,
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
    # close_time stored by EA as "YYYY.MM.DD HH:MM:SS"
    return datetime.now().strftime("%Y.%m.%d")

def _today_pnl(symbol: str) -> float:
    sym = norm_symbol(symbol)
    pref = _today_prefix_mt5()
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            """
            SELECT profit, commission, swap
            FROM deals
            WHERE symbol = ?
              AND close_time LIKE ?
            """,
            (sym, f"{pref}%")
        ).fetchall()
        conn.close()
    return float(sum(_pnl_row(r) for r in rows))

# =====================================================
# KPIs (USD / legacy)
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
# R-BASED KPIs + R-GATE (Step 8)
# =====================================================
def _fetch_rs(symbol: str) -> List[float]:
    sym = norm_symbol(symbol)
    with _db_lock:
        conn = db_conn()
        rows = conn.execute(
            """
            SELECT r_multiple
            FROM deals
            WHERE symbol = ?
              AND r_multiple IS NOT NULL
            ORDER BY id ASC
            """,
            (sym,)
        ).fetchall()
        conn.close()

    out: List[float] = []
    for r in rows:
        v = r["r_multiple"]
        if v is None:
            continue
        try:
            out.append(float(v))
        except Exception:
            pass
    return out

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

    rs: List[float] = []
    for r in rows:
        v = r["r_multiple"]
        if v is None:
            continue
        try:
            rs.append(float(v))
        except Exception:
            pass
    rs.reverse()
    return rs

def _profit_factor_r(rs: List[float]) -> Optional[float]:
    gp = sum(x for x in rs if x > 0)
    gl = abs(sum(x for x in rs if x < 0))
    if gl <= 0:
        return None
    return gp / gl

def _loss_streak_r(rs: List[float]) -> int:
    s = 0
    for x in reversed(rs):
        if x < 0:
            s += 1
        else:
            break
    return s

def _max_drawdown_r(rs: List[float]) -> float:
    peak = float("-inf")
    eq = 0.0
    max_dd = 0.0
    for r in rs:
        eq += r
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
    return float(max_dd)

@app.get("/kpis_r/rolling")
def kpis_r_rolling(symbol: str, n: int = 20):
    sym = norm_symbol(symbol)
    rs = _fetch_rs_last_n(sym, n)

    trades = len(rs)
    wins = sum(1 for x in rs if x > 0)
    losses = sum(1 for x in rs if x < 0)

    net_r = float(sum(rs))
    pf_r = _profit_factor_r(rs)
    streak_r = _loss_streak_r(rs)
    max_dd_r = _max_drawdown_r(rs)

    return {
        "symbol": sym,
        "n": int(n),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "net_r_last_n": round(net_r, 3),
        "profit_factor_r": (round(pf_r, 3) if pf_r is not None else None),
        "loss_streak_r": int(streak_r),
        "max_drawdown_r_last_n": round(max_dd_r, 3),
    }

@app.get("/status/propfirm_r")
def status_propfirma_r(
    symbol: str,
    n: int = 20,

    pf_r_min: float = 1.20,
    net_r_last_n_min: float = -3.0,
    loss_streak_r_max: int = 3,

    max_dd_r_limit: float = 6.0,
    rolling_dd_r_limit: float = 4.0
):
    sym = norm_symbol(symbol)

    rs_last = _fetch_rs_last_n(sym, n)
    rs_all = _fetch_rs(sym)

    reasons: List[str] = []

    if len(rs_last) < max(5, int(n) // 2):
        reasons.append("Not enough R data yet")

    pf_r = _profit_factor_r(rs_last)
    net_r_last = float(sum(rs_last))
    streak_r = _loss_streak_r(rs_last)
    rolling_dd_r = _max_drawdown_r(rs_last)
    total_dd_r = _max_drawdown_r(rs_all) if len(rs_all) else 0.0

    if pf_r is not None and float(pf_r) < float(pf_r_min):
        reasons.append(f"PF_R<{pf_r_min}")
    if net_r_last < float(net_r_last_n_min):
        reasons.append(f"NetR_Last{int(n)}<{net_r_last_n_min}")
    if int(streak_r) > int(loss_streak_r_max):
        reasons.append(f"LossStreakR>{int(loss_streak_r_max)}")
    if float(rolling_dd_r) > float(rolling_dd_r_limit):
        reasons.append(f"RollingDD_R>{rolling_dd_r_limit}")
    if float(total_dd_r) > float(max_dd_r_limit):
        reasons.append(f"TotalMaxDD_R>{max_dd_r_limit}")

    if reasons:
        if len(reasons) == 1 and reasons[0] == "Not enough R data yet":
            level = "YELLOW"
        else:
            level = "RED"
    else:
        level = "GREEN"

    return {
        "symbol": sym,
        "level": level,
        "reasons": reasons,
        "rolling": {
            "n": int(n),
            "trades_with_r": int(len(rs_last)),
            "profit_factor_r": (round(pf_r, 3) if pf_r is not None else None),
            "net_r_last_n": round(net_r_last, 3),
            "loss_streak_r": int(streak_r),
            "rolling_dd_r": round(rolling_dd_r, 3),
        },
        "total": {
            "trades_with_r": int(len(rs_all)),
            "total_max_dd_r": round(total_dd_r, 3),
        },
        "thresholds": {
            "pf_r_min": float(pf_r_min),
            "net_r_last_n_min": float(net_r_last_n_min),
            "loss_streak_r_max": int(loss_streak_r_max),
            "rolling_dd_r_limit": float(rolling_dd_r_limit),
            "max_dd_r_limit": float(max_dd_r_limit),
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
        "note": "Gate is per symbol. /status/propfirm_r?symbol=BTCUSD or XAUUSD"
    }
