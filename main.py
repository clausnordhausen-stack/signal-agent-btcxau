from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from collections import deque
from typing import Optional, Dict, Any, List, Tuple
import os
import sqlite3
from threading import Lock

app = FastAPI(title="Signal Agent API")

# =========================
# CONFIG
# =========================
SECRET = os.getenv("SECRET_KEY", "claus-2026-xau-01!")
QUEUE_MAX = int(os.getenv("QUEUE_MAX", "50"))

# Persist DB on Render by mounting a Disk and setting: DB_PATH=/var/data/data.db
DB_PATH = os.getenv("DB_PATH", "data.db")
_db_lock = Lock()

# Your production magics (still useful for filtering / dashboards)
MAGICS_PROD = {61001, 61002}

# =========================
# MODELS
# =========================
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

# =========================
# SIGNAL STATE
# =========================
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

# =========================
# DB
# =========================
def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

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
            magic INTEGER,
            ticket INTEGER,
            deal_id INTEGER,
            type TEXT NOT NULL,              -- BUY/SELL
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
            comment TEXT
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_symbol_time ON deals(symbol, close_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_magic ON deals(magic)")
        conn.commit()
        conn.close()

@app.on_event("startup")
def _startup():
    db_init()

# =========================
# HELPERS: FILTER + KPI
# =========================
def _pnl_row(r: sqlite3.Row) -> float:
    return float((r["profit"] or 0.0) + (r["commission"] or 0.0) + (r["swap"] or 0.0))

def _compute_max_dd_from_equity(equity: List[float]) -> float:
    peak = float("-inf")
    max_dd = 0.0
    for eq in equity:
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
    return max_dd

def _compute_kpis_from_pnls(pnls: List[float]) -> Dict[str, Any]:
    trades = len(pnls)
    wins = sum(1 for x in pnls if x > 0)
    losses = sum(1 for x in pnls if x < 0)
    breakeven = trades - wins - losses

    gross_profit = sum(x for x in pnls if x > 0)
    gross_loss_abs = abs(sum(x for x in pnls if x < 0))
    net_profit = sum(pnls)
    avg_trade = (net_profit / trades) if trades else 0.0
    winrate = (wins / trades * 100.0) if trades else 0.0
    profit_factor = (gross_profit / gross_loss_abs) if gross_loss_abs > 0 else None

    equity = []
    eq = 0.0
    for p in pnls:
        eq += (p or 0.0)
        equity.append(eq)
    max_dd = _compute_max_dd_from_equity(equity) if trades else 0.0

    return {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "winrate_pct": round(winrate, 2),
        "net_profit": round(net_profit, 2),
        "gross_profit": round(gross_profit, 2),
        "gross_loss_abs": round(gross_loss_abs, 2),
        "profit_factor": (round(profit_factor, 3) if profit_factor is not None else None),
        "avg_trade": round(avg_trade, 2),
        "max_drawdown": round(max_dd, 2),
    }

def _rolling_tail(values: List[float], n: int) -> List[float]:
    if n <= 0:
        return values
    if len(values) <= n:
        return values
    return values[-n:]

def _loss_streak(pnls: List[float]) -> int:
    s = 0
    for x in reversed(pnls):
        if x < 0:
            s += 1
        else:
            break
    return s

def _win_streak(pnls: List[float]) -> int:
    s = 0
    for x in reversed(pnls):
        if x > 0:
            s += 1
        else:
            break
    return s

def _magic_filter_list(magics_csv: Optional[str]) -> Optional[List[int]]:
    if not magics_csv:
        return None
    parts = [p.strip() for p in magics_csv.split(",") if p.strip() != ""]
    out: List[int] = []
    for p in parts:
        if p.isdigit():
            out.append(int(p))
    return out if out else None

def _today_mt5_day_str() -> str:
    # We match MT5 close_time day format "YYYY.MM.DD"
    return datetime.now().strftime("%Y.%m.%d")

def _build_filters_where(symbol: Optional[str], magic: Optional[int], magics_list: Optional[List[int]],
                         date_from: Optional[str], date_to: Optional[str]) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    args: List[Any] = []

    if symbol:
        clauses.append("symbol = ?")
        args.append(norm_symbol(symbol))
    if magic is not None:
        clauses.append("magic = ?")
        args.append(magic)
    if magics_list:
        placeholders = ",".join(["?"] * len(magics_list))
        clauses.append(f"magic IN ({placeholders})")
        args.extend(magics_list)
    if date_from:
        clauses.append("close_time >= ?")
        args.append(date_from)
    if date_to:
        clauses.append("close_time <= ?")
        args.append(date_to)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, args

# =========================
# ROOT
# =========================
@app.get("/")
def root():
    return {"status": "Signal Agent API is running"}

# Optional: avoid Render HEAD / 405 noise
@app.head("/")
def head_root():
    return

# =========================
# SIGNAL INGEST (TradingView)
# =========================
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

# =========================
# DEAL INGEST (MT5 -> API)  (MANUAL TRADES ALLOWED)
# =========================
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
        cur.execute("""
            INSERT INTO deals(
                received_utc, account, symbol, magic, ticket, deal_id, type, lots,
                open_time, close_time, open_price, close_price, sl, tp,
                profit, commission, swap, comment
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            received, d.account, sym, d.magic, d.ticket, d.deal_id, typ, d.lots,
            d.open_time, d.close_time, d.open_price, d.close_price, d.sl, d.tp,
            d.profit, d.commission, d.swap, d.comment
        ))
        conn.commit()
        row_id = cur.lastrowid
        conn.close()

    return {"ok": True, "id": row_id, "received_utc": received, "symbol": sym}

# =========================
# DEALS LIST
# =========================
@app.get("/deals")
def list_deals(
    symbol: Optional[str] = None,
    magic: Optional[int] = None,
    magics: Optional[str] = None,          # "61001,61002"
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 200
):
    limit = max(1, min(limit, 2000))
    magics_list = _magic_filter_list(magics)
    where, args = _build_filters_where(symbol, magic, magics_list, date_from, date_to)

    sql = f"""
        SELECT * FROM deals
        {where}
        ORDER BY COALESCE(close_time, received_utc) DESC
        LIMIT ?
    """
    args2 = args + [limit]

    with _db_lock:
        conn = db_conn()
        rows = conn.execute(sql, args2).fetchall()
        conn.close()

    return {"items": [dict(r) for r in rows], "count": len(rows)}

# =========================
# KPI: OVERALL
# =========================
@app.get("/kpis")
def kpis(
    symbol: Optional[str] = None,
    magic: Optional[int] = None,
    magics: Optional[str] = None,          # "61001,61002"
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    magics_list = _magic_filter_list(magics)
    where, args = _build_filters_where(symbol, magic, magics_list, date_from, date_to)

    sql = f"""
        SELECT profit, commission, swap, close_time, received_utc
        FROM deals
        {where}
        ORDER BY COALESCE(close_time, received_utc) ASC
    """

    with _db_lock:
        conn = db_conn()
        rows = conn.execute(sql, args).fetchall()
        conn.close()

    pnls = [_pnl_row(r) for r in rows]
    out = _compute_kpis_from_pnls(pnls)
    out["filters"] = {
        "symbol": norm_symbol(symbol) if symbol else None,
        "magic": magic,
        "magics": magics_list,
        "from": date_from,
        "to": date_to
    }
    return out

# =========================
# KPI: DAILY
# =========================
@app.get("/kpis/daily")
def kpis_daily(
    symbol: Optional[str] = None,
    magic: Optional[int] = None,
    magics: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
):
    magics_list = _magic_filter_list(magics)
    where, args = _build_filters_where(symbol, magic, magics_list, date_from, date_to)

    sql = f"""
        SELECT
          SUBSTR(close_time,1,10) AS day,
          profit, commission, swap, close_time, received_utc
        FROM deals
        {where}
        ORDER BY COALESCE(close_time, received_utc) ASC
    """

    with _db_lock:
        conn = db_conn()
        rows = conn.execute(sql, args).fetchall()
        conn.close()

    daily: Dict[str, List[float]] = {}
    for r in rows:
        day = r["day"] or "unknown"
        daily.setdefault(day, []).append(_pnl_row(r))

    items = []
    for day in sorted(daily.keys()):
        k = _compute_kpis_from_pnls(daily[day])
        k["day"] = day
        items.append(k)

    return {
        "filters": {"symbol": norm_symbol(symbol) if symbol else None, "magic": magic, "magics": magics_list, "from": date_from, "to": date_to},
        "items": items,
        "days": len(items),
        "prod_magics": sorted(MAGICS_PROD),
    }

# =========================
# EQUITY CURVE
# =========================
@app.get("/equity")
def equity_curve(
    symbol: Optional[str] = None,
    magic: Optional[int] = None,
    magics: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 5000
):
    limit = max(1, min(limit, 5000))
    magics_list = _magic_filter_list(magics)
    where, args = _build_filters_where(symbol, magic, magics_list, date_from, date_to)

    sql = f"""
        SELECT profit, commission, swap, close_time, received_utc
        FROM deals
        {where}
        ORDER BY COALESCE(close_time, received_utc) ASC
        LIMIT ?
    """
    args2 = args + [limit]

    with _db_lock:
        conn = db_conn()
        rows = conn.execute(sql, args2).fetchall()
        conn.close()

    eq = 0.0
    points = []
    for r in rows:
        pnl = _pnl_row(r)
        eq += pnl
        t = r["close_time"] or r["received_utc"]
        points.append({"t": t, "pnl": round(pnl, 2), "equity": round(eq, 2)})

    return {
        "filters": {"symbol": norm_symbol(symbol) if symbol else None, "magic": magic, "magics": magics_list, "from": date_from, "to": date_to},
        "points": points,
        "count": len(points),
        "equity_end": round(eq, 2)
    }

# =========================
# KPI: ROLLING (last N trades)
# =========================
@app.get("/kpis/rolling")
def kpis_rolling(
    symbol: Optional[str] = None,
    magic: Optional[int] = None,
    magics: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    n: int = 50
):
    n = max(1, min(n, 5000))
    magics_list = _magic_filter_list(magics)
    where, args = _build_filters_where(symbol, magic, magics_list, date_from, date_to)

    sql = f"""
        SELECT profit, commission, swap, close_time, received_utc
        FROM deals
        {where}
        ORDER BY COALESCE(close_time, received_utc) ASC
    """

    with _db_lock:
        conn = db_conn()
        rows = conn.execute(sql, args).fetchall()
        conn.close()

    pnls_all = [_pnl_row(r) for r in rows]
    pnls = _rolling_tail(pnls_all, n)

    out = _compute_kpis_from_pnls(pnls)
    out["filters"] = {
        "symbol": norm_symbol(symbol) if symbol else None,
        "magic": magic,
        "magics": magics_list,
        "from": date_from,
        "to": date_to,
        "n": n
    }
    out["loss_streak"] = _loss_streak(pnls)
    out["win_streak"] = _win_streak(pnls)
    out["net_last_n"] = round(sum(pnls), 2)
    return out

# =========================
# STATUS: PROP-FIRM (your defaults)
# =========================
@app.get("/status/propfirm")
def status_propfirma(
    symbol: Optional[str] = None,

    # Rolling window defaults (your spec)
    n: int = 20,

    # Rolling thresholds (your spec)
    pf_min: float = 1.30,
    net_last_n_min: float = -200.0,
    loss_streak_max: int = 3,

    # Trading Pit limits (your spec)
    daily_dd_limit: float = 350.0,   # means day pnl must be > -350
    total_dd_limit: float = 600.0    # means max_dd must be <= 600
):
    # 1) Rolling KPIs (last N trades, ALL trades – manual + any magic)
    roll = kpis_rolling(symbol=symbol, magic=None, magics=None, n=n)

    pf = roll.get("profit_factor", None)
    net_last_n = float(roll.get("net_last_n", 0.0) or 0.0)
    loss_streak = int(roll.get("loss_streak", 0) or 0)

    # 2) Daily PnL (today) from daily KPIs
    today = _today_mt5_day_str()
    daily = kpis_daily(symbol=symbol, magic=None, magics=None, date_from=None, date_to=None)

    today_item = None
    for it in daily.get("items", []):
        if it.get("day") == today:
            today_item = it
            break
    today_net = float((today_item or {}).get("net_profit", 0.0) or 0.0)

    # 3) Total DD from ALL equity points (increase limit to capture history)
    eq = equity_curve(symbol=symbol, magic=None, magics=None, date_from=None, date_to=None, limit=5000)
    points = eq.get("points", [])
    equities = [float(p.get("equity", 0.0) or 0.0) for p in points] if points else []

    peak = float("-inf")
    max_dd = 0.0
    for e in equities:
        if e > peak:
            peak = e
        dd = peak - e
        if dd > max_dd:
            max_dd = dd

    reasons: List[str] = []

    # Rolling checks
    if pf is not None and float(pf) < pf_min:
        reasons.append(f"PF<{pf_min}")
    if net_last_n < net_last_n_min:
        reasons.append(f"NetLast{n}<{net_last_n_min}")
    if loss_streak > loss_streak_max:
        reasons.append(f"LossStreak>{loss_streak_max}")

    # Daily DD: today net must be > -daily_dd_limit
    if today_net <= -abs(daily_dd_limit):
        reasons.append(f"DailyPnL<={-abs(daily_dd_limit)}")

    # Total DD: max_dd must be <= total_dd_limit
    if max_dd > abs(total_dd_limit):
        reasons.append(f"TotalMaxDD>{abs(total_dd_limit)}")

    level = "RED" if reasons else "GREEN"

    return {
        "level": level,
        "reasons": reasons,
        "symbol": norm_symbol(symbol) if symbol else None,
        "rolling": roll,
        "today": {"day": today, "net_profit": round(today_net, 2)},
        "total": {"max_drawdown": round(max_dd, 2)},
        "thresholds": {
            "n": n,
            "pf_min": pf_min,
            "net_last_n_min": net_last_n_min,
            "loss_streak_max": loss_streak_max,
            "daily_dd_limit": -abs(daily_dd_limit),
            "total_dd_limit": abs(total_dd_limit),
        }
    }

# =========================
# SUMMARY
# =========================
@app.get("/summary")
def summary(symbol: Optional[str] = None, magic: Optional[int] = None, magics: Optional[str] = None):
    magics_list = _magic_filter_list(magics)
    where, args = _build_filters_where(symbol, magic, magics_list, None, None)

    sql = f"""
        SELECT * FROM deals
        {where}
        ORDER BY COALESCE(close_time, received_utc) DESC
        LIMIT 20
    """

    with _db_lock:
        conn = db_conn()
        last = conn.execute(sql, args).fetchall()
        conn.close()

    overall = kpis(symbol=symbol, magic=magic, magics=magics, date_from=None, date_to=None)

    sigs = {}
    if symbol:
        s = norm_symbol(symbol)
        if s in STATE:
            sigs[s] = {"latest": STATE[s].get("latest"), "updated_utc": STATE[s].get("updated_utc"), "ack": STATE[s].get("ack")}
    else:
        for s in list(STATE.keys())[:50]:
            sigs[s] = {"latest": STATE[s].get("latest"), "updated_utc": STATE[s].get("updated_utc"), "ack": STATE[s].get("ack")}

    return {
        "filters": {"symbol": norm_symbol(symbol) if symbol else None, "magic": magic, "magics": magics_list},
        "kpis": overall,
        "last_deals": [dict(r) for r in last],
        "signals": sigs,
        "prod_magics": sorted(MAGICS_PROD),
        "note": "Manual trades allowed: /deal does not enforce magic."
    }
