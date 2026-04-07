from fastapi import FastAPI, HTTPException, Query, Depends, status, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, Dict, List
from jose import jwt, JWTError
import os
import sqlite3
import threading
import json

app = FastAPI(title="Signal Agent API", version="6.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
SECRET_KEY = os.getenv("SECRET_KEY", "supersecret123")
DB_PATH = os.getenv("DB_PATH", "signal_agent.db")
DEFAULT_GATE_LEVEL = os.getenv("DEFAULT_GATE_LEVEL", "GREEN").upper()
HEARTBEAT_TIMEOUT_SEC = int(os.getenv("HEARTBEAT_TIMEOUT_SEC", "90"))

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
APP_USERNAME = os.getenv("APP_USERNAME", "admin")
APP_PASSWORD = os.getenv("APP_PASSWORD", "123456")

# Optional app token protection for dashboard routes
APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
APP_TOKEN_HEADER = os.getenv("APP_TOKEN_HEADER", "X-APP-TOKEN").strip() or "X-APP-TOKEN"

# TV/API key
TV_API_KEY = os.getenv("TV_API_KEY", SECRET_KEY)

# Controls / manual overrides
DEFAULT_ALLOW_NEW_ENTRIES = os.getenv("DEFAULT_ALLOW_NEW_ENTRIES", "true").lower() == "true"
DEFAULT_RISK_MULTIPLIER = float(os.getenv("DEFAULT_RISK_MULTIPLIER", "1.0"))
DEFAULT_PAUSED = os.getenv("DEFAULT_PAUSED", "false").lower() == "true"

# KPI CONFIG (Phase 6)
DEFAULT_KPI_LOOKBACK_DAYS = int(os.getenv("DEFAULT_KPI_LOOKBACK_DAYS", "7"))
DEFAULT_KPI_LIMIT_TRADES = int(os.getenv("DEFAULT_KPI_LIMIT_TRADES", "50"))

AUTO_GATE_ENABLED = os.getenv("AUTO_GATE_ENABLED", "true").lower() == "true"

YELLOW_DD_PCT = float(os.getenv("YELLOW_DD_PCT", "3.0"))
RED_DD_PCT = float(os.getenv("RED_DD_PCT", "6.0"))

YELLOW_LOSS_STREAK = int(os.getenv("YELLOW_LOSS_STREAK", "3"))
RED_LOSS_STREAK = int(os.getenv("RED_LOSS_STREAK", "5"))

YELLOW_R_SUM = float(os.getenv("YELLOW_R_SUM", "-2.0"))
RED_R_SUM = float(os.getenv("RED_R_SUM", "-4.0"))

YELLOW_WINRATE_MIN = float(os.getenv("YELLOW_WINRATE_MIN", "35.0"))
RED_WINRATE_MIN = float(os.getenv("RED_WINRATE_MIN", "25.0"))

DB_LOCK = threading.Lock()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = now_utc()
    return dt.astimezone(timezone.utc).isoformat()


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = dict_factory
    return conn


def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x, default=0):
    try:
        if x is None or x == "":
            return default
        return int(x)
    except Exception:
        return default


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = now_utc() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )


def get_current_user(token: str = Depends(oauth2_scheme)):
    payload = decode_token(token)
    username = payload.get("sub")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload"
        )
    return {"username": username}


def app_token_guard(x_app_token: Optional[str] = Header(default=None, alias=APP_TOKEN_HEADER)):
    if APP_TOKEN:
        if x_app_token != APP_TOKEN:
            raise HTTPException(status_code=401, detail="Invalid app token")
    return True


def get_runtime_controls(symbol: Optional[str] = None) -> Dict[str, Any]:
    return {
        "paused": DEFAULT_PAUSED,
        "allow_new_entries": DEFAULT_ALLOW_NEW_ENTRIES,
        "risk_multiplier": DEFAULT_RISK_MULTIPLIER,
        "symbol": symbol.upper() if symbol else None,
        "source": "default_env"
    }


# -------------------------------------------------------------------
# DB INIT
# -------------------------------------------------------------------
def init_db():
    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT,
            payload_json TEXT,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_acks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            account TEXT NOT NULL,
            magic TEXT,
            ack_utc TEXT NOT NULL,
            UNIQUE(signal_id, account, magic)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS heartbeats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account TEXT,
            magic TEXT,
            symbol TEXT,
            ea_name TEXT,
            version TEXT,
            last_seen_utc TEXT NOT NULL,
            status TEXT,
            comment TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account TEXT,
            magic TEXT,
            symbol TEXT,
            side TEXT,
            ticket TEXT,
            volume REAL,
            entry_price REAL,
            exit_price REAL,
            sl REAL,
            tp REAL,
            pnl REAL,
            pnl_currency TEXT,
            commission REAL,
            swap REAL,
            risk_amount REAL,
            r_multiple REAL,
            strategy TEXT,
            deal_time_utc TEXT NOT NULL,
            created_utc TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS risks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account TEXT,
            magic TEXT,
            symbol TEXT,
            event_type TEXT,
            level TEXT,
            message TEXT,
            value REAL,
            created_utc TEXT NOT NULL
        )
        """)

        conn.commit()
        conn.close()


init_db()


# -------------------------------------------------------------------
# MODELS
# -------------------------------------------------------------------
class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str


class UserResponse(BaseModel):
    username: str


class TVSignalIn(BaseModel):
    key: Optional[str] = None
    symbol: str
    side: Optional[str] = None
    action: Optional[str] = None
    id: Optional[str] = None
    ts: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None


class AckIn(BaseModel):
    symbol: str
    updated_utc: str
    account: str
    magic: Optional[str] = None


class HeartbeatPing(BaseModel):
    key: Optional[str] = None
    symbol: str
    account: str
    magic: Optional[str] = None
    ea_name: Optional[str] = None
    version: Optional[str] = None
    status: Optional[str] = "alive"
    comment: Optional[str] = None


class DealIn(BaseModel):
    account: Optional[str] = None
    magic: Optional[str] = None
    symbol: str
    side: Optional[str] = None
    ticket: Optional[str] = None
    volume: Optional[float] = None
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    pnl: Optional[float] = None
    pnl_currency: Optional[str] = "USD"
    commission: Optional[float] = 0.0
    swap: Optional[float] = 0.0
    risk_amount: Optional[float] = None
    r_multiple: Optional[float] = None
    strategy: Optional[str] = None
    deal_time_utc: Optional[str] = None


class RiskIn(BaseModel):
    account: Optional[str] = None
    magic: Optional[str] = None
    symbol: str
    event_type: str
    level: Optional[str] = None
    message: Optional[str] = None
    value: Optional[float] = None


# -------------------------------------------------------------------
# BASIC ROUTES
# -------------------------------------------------------------------
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "Signal Agent API",
        "version": "6.1.0",
        "server_time_utc": utc_iso()
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "signal-agent-api",
        "time": utc_iso()
    }


# -------------------------------------------------------------------
# AUTH ROUTES
# -------------------------------------------------------------------
@app.post("/login", response_model=TokenResponse)
def login(data: LoginRequest):
    username = data.username.strip()
    password = data.password.strip()

    if username != APP_USERNAME or password != APP_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    access_token = create_access_token({"sub": username})
    return {
        "access_token": access_token,
        "token_type": "bearer"
    }


@app.get("/me", response_model=UserResponse)
def me(current_user: dict = Depends(get_current_user)):
    return {"username": current_user["username"]}


# -------------------------------------------------------------------
# SIGNAL ROUTES
# -------------------------------------------------------------------
@app.post("/tv")
def tv_signal(data: TVSignalIn, x_api_key: Optional[str] = Header(default=None)):
    req_key = (x_api_key or data.key or "").strip()
    if req_key != TV_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    symbol = data.symbol.strip().upper()

    side_raw = (data.side or data.action or "").strip().upper()
    if side_raw in ["BUY", "LONG"]:
        side = "BUY"
    elif side_raw in ["SELL", "SHORT"]:
        side = "SELL"
    else:
        side = ""

    payload = data.payload or {}
    payload["tv_id"] = data.id
    payload["tv_ts"] = data.ts
    payload["raw_action"] = data.action
    payload_json = json.dumps(payload, ensure_ascii=False)

    now = utc_iso()

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO signals(symbol, side, payload_json, created_utc, updated_utc, status)
        VALUES (?, ?, ?, ?, ?, 'pending')
        """, (symbol, side, payload_json, now, now))

        signal_id = cur.lastrowid
        conn.commit()
        conn.close()

    return {
        "ok": True,
        "signal_id": signal_id,
        "symbol": symbol,
        "side": side,
        "created_utc": now
    }


@app.get("/latest")
def latest_signal(
    symbol: str = Query(...),
    account: str = Query(...),
    magic: Optional[str] = Query(default=None),
):
    symbol = symbol.strip().upper()
    account = account.strip()

    controls = get_runtime_controls(symbol)
    gate_payload = gate_combo(symbol=symbol, account=account, magic=magic)

    if controls["paused"] or not controls["allow_new_entries"] or not gate_payload["allow_new_entries"]:
        return {
            "ok": True,
            "has_signal": False,
            "symbol": symbol,
            "blocked": True,
            "controls": controls,
            "gate": gate_payload
        }

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        SELECT s.*
        FROM signals s
        WHERE s.symbol = ?
          AND s.status = 'pending'
          AND s.side IN ('BUY', 'SELL')
          AND NOT EXISTS (
              SELECT 1 FROM signal_acks a
              WHERE a.signal_id = s.id
                AND a.account = ?
                AND COALESCE(a.magic, '') = COALESCE(?, '')
          )
        ORDER BY s.id DESC
        LIMIT 1
        """, (symbol, account, magic))

        row = cur.fetchone()
        conn.close()

    if not row:
        return {
            "ok": True,
            "has_signal": False,
            "symbol": symbol,
            "blocked": False,
            "controls": controls,
            "gate": gate_payload
        }

    try:
        payload = json.loads(row.get("payload_json") or "{}")
    except Exception:
        payload = {}

    return {
        "ok": True,
        "has_signal": True,
        "blocked": False,
        "symbol": symbol,
        "side": row.get("side"),
        "updated_utc": row.get("updated_utc"),
        "signal_id": row.get("id"),
        "payload": payload,
        "controls": controls,
        "gate": gate_payload,
        "signal": row
    }


@app.post("/ack")
def ack_signal(data: AckIn):
    symbol = data.symbol.strip().upper()
    account = data.account.strip()
    magic = data.magic

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        SELECT *
        FROM signals
        WHERE symbol = ?
          AND updated_utc = ?
          AND status = 'pending'
        ORDER BY id DESC
        LIMIT 1
        """, (symbol, data.updated_utc))

        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="Signal not found")

        signal_id = row["id"]

        cur.execute("""
        INSERT OR IGNORE INTO signal_acks(signal_id, symbol, account, magic, ack_utc)
        VALUES (?, ?, ?, ?, ?)
        """, (signal_id, symbol, account, magic, utc_iso()))

        conn.commit()
        conn.close()

    return {
        "ok": True,
        "signal_id": signal_id,
        "symbol": symbol,
        "account": account,
        "magic": magic
    }


# -------------------------------------------------------------------
# HEARTBEAT
# -------------------------------------------------------------------
@app.post("/hb")
def heartbeat(data: HeartbeatPing):
    if data.key and data.key != TV_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid key")

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO heartbeats(account, magic, symbol, ea_name, version, last_seen_utc, status, comment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.account,
            data.magic,
            data.symbol.upper(),
            data.ea_name,
            data.version,
            utc_iso(),
            data.status,
            data.comment
        ))

        conn.commit()
        conn.close()

    return {"ok": True, "server_time_utc": utc_iso()}


@app.get("/status/heartbeat")
def heartbeat_status(symbol: Optional[str] = Query(default=None)):
    symbol_norm = symbol.strip().upper() if symbol else None
    cutoff = now_utc() - timedelta(seconds=HEARTBEAT_TIMEOUT_SEC)

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        if symbol_norm:
            cur.execute("""
            SELECT * FROM heartbeats
            WHERE symbol = ?
            ORDER BY id DESC
            LIMIT 200
            """, (symbol_norm,))
        else:
            cur.execute("""
            SELECT * FROM heartbeats
            ORDER BY id DESC
            LIMIT 500
            """)

        rows = cur.fetchall()
        conn.close()

    latest_map = {}
    for r in rows:
        key = f"{r.get('account','')}|{r.get('magic','')}|{r.get('symbol','')}"
        if key not in latest_map:
            latest_map[key] = r

    result = []
    connected_count = 0

    for _, r in latest_map.items():
        last_seen = parse_dt(r["last_seen_utc"])
        connected = bool(last_seen and last_seen >= cutoff)
        if connected:
            connected_count += 1

        result.append({
            "account": r.get("account"),
            "magic": r.get("magic"),
            "symbol": r.get("symbol"),
            "ea_name": r.get("ea_name"),
            "version": r.get("version"),
            "last_seen_utc": r.get("last_seen_utc"),
            "connected": connected,
            "status": r.get("status"),
            "comment": r.get("comment"),
        })

    return {
        "ok": True,
        "timeout_sec": HEARTBEAT_TIMEOUT_SEC,
        "connected_count": connected_count,
        "items": result
    }


# -------------------------------------------------------------------
# DEALS + RISKS
# -------------------------------------------------------------------
@app.post("/deal")
def post_deal(data: DealIn):
    deal_time = data.deal_time_utc or utc_iso()

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO deals(
            account, magic, symbol, side, ticket, volume,
            entry_price, exit_price, sl, tp,
            pnl, pnl_currency, commission, swap,
            risk_amount, r_multiple, strategy,
            deal_time_utc, created_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.account,
            data.magic,
            data.symbol.upper(),
            data.side,
            data.ticket,
            data.volume,
            data.entry_price,
            data.exit_price,
            data.sl,
            data.tp,
            data.pnl,
            data.pnl_currency,
            data.commission,
            data.swap,
            data.risk_amount,
            data.r_multiple,
            data.strategy,
            deal_time,
            utc_iso()
        ))

        conn.commit()
        conn.close()

    return {"ok": True}


@app.post("/risk")
def post_risk(data: RiskIn):
    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO risks(account, magic, symbol, event_type, level, message, value, created_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.account,
            data.magic,
            data.symbol.upper(),
            data.event_type,
            data.level,
            data.message,
            data.value,
            utc_iso()
        ))

        conn.commit()
        conn.close()

    return {"ok": True}


# -------------------------------------------------------------------
# KPI CORE
# -------------------------------------------------------------------
def get_deals_filtered(
    symbol: Optional[str] = None,
    account: Optional[str] = None,
    magic: Optional[str] = None,
    lookback_days: int = DEFAULT_KPI_LOOKBACK_DAYS,
    limit_trades: int = DEFAULT_KPI_LIMIT_TRADES,
):
    dt_from = now_utc() - timedelta(days=lookback_days)

    query = """
    SELECT *
    FROM deals
    WHERE deal_time_utc >= ?
    """
    params = [utc_iso(dt_from)]

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())

    if account:
        query += " AND account = ?"
        params.append(account)

    if magic:
        query += " AND magic = ?"
        params.append(magic)

    query += " ORDER BY deal_time_utc DESC LIMIT ?"
    params.append(limit_trades)

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()

    rows = list(reversed(rows))
    return rows


def calc_equity_curve_from_pnl(rows: List[Dict[str, Any]]) -> List[float]:
    curve = [0.0]
    running = 0.0
    for r in rows:
        pnl = safe_float(r.get("pnl"), 0.0)
        running += pnl
        curve.append(running)
    return curve


def calc_max_drawdown_abs(curve: List[float]) -> float:
    peak = -10**18
    max_dd = 0.0
    for x in curve:
        if x > peak:
            peak = x
        dd = peak - x
        if dd > max_dd:
            max_dd = dd
    return max_dd


def calc_max_drawdown_pct(curve: List[float]) -> float:
    peak = None
    max_dd_pct = 0.0
    for x in curve:
        if peak is None or x > peak:
            peak = x
        if peak and peak > 0:
            dd_pct = ((peak - x) / peak) * 100.0
            if dd_pct > max_dd_pct:
                max_dd_pct = dd_pct
    return max_dd_pct


def calc_loss_streak(rows: List[Dict[str, Any]]) -> int:
    streak = 0
    max_streak = 0
    for r in rows:
        pnl = safe_float(r.get("pnl"), 0.0)
        if pnl < 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak


def calc_current_loss_streak(rows: List[Dict[str, Any]]) -> int:
    streak = 0
    for r in reversed(rows):
        pnl = safe_float(r.get("pnl"), 0.0)
        if pnl < 0:
            streak += 1
        else:
            break
    return streak


def summarize_kpis(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_trades = len(rows)
    wins = 0
    losses = 0
    breakeven = 0

    gross_profit = 0.0
    gross_loss = 0.0
    net_pnl = 0.0
    total_r = 0.0

    for r in rows:
        pnl = safe_float(r.get("pnl"), 0.0)
        r_mult = safe_float(r.get("r_multiple"), 0.0)

        net_pnl += pnl
        total_r += r_mult

        if pnl > 0:
            wins += 1
            gross_profit += pnl
        elif pnl < 0:
            losses += 1
            gross_loss += abs(pnl)
        else:
            breakeven += 1

    winrate = (wins / total_trades * 100.0) if total_trades > 0 else 0.0
    avg_pnl = (net_pnl / total_trades) if total_trades > 0 else 0.0
    avg_r = (total_r / total_trades) if total_trades > 0 else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

    curve = calc_equity_curve_from_pnl(rows)
    max_dd_abs = calc_max_drawdown_abs(curve)
    max_dd_pct = calc_max_drawdown_pct(curve)
    max_loss_streak = calc_loss_streak(rows)
    current_loss_streak = calc_current_loss_streak(rows)

    last_trade_time = rows[-1]["deal_time_utc"] if total_trades > 0 else None

    return {
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "winrate_pct": round(winrate, 2),
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(gross_loss, 2),
        "net_pnl": round(net_pnl, 2),
        "avg_pnl": round(avg_pnl, 2),
        "sum_r": round(total_r, 2),
        "avg_r": round(avg_r, 2),
        "profit_factor": round(profit_factor, 2),
        "max_drawdown_abs": round(max_dd_abs, 2),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "max_loss_streak": max_loss_streak,
        "current_loss_streak": current_loss_streak,
        "last_trade_time_utc": last_trade_time,
    }


def auto_gate_from_kpis(kpi: Dict[str, Any]) -> Dict[str, Any]:
    if not AUTO_GATE_ENABLED:
        return {
            "gate_level": DEFAULT_GATE_LEVEL,
            "allow_new_entries": DEFAULT_GATE_LEVEL != "RED",
            "risk_multiplier": 1.0 if DEFAULT_GATE_LEVEL == "GREEN" else 0.5 if DEFAULT_GATE_LEVEL == "YELLOW" else 0.0,
            "reasons": ["AUTO_GATE_DISABLED"]
        }

    reasons_red = []
    reasons_yellow = []

    dd_pct = safe_float(kpi.get("max_drawdown_pct"))
    cur_loss_streak = safe_int(kpi.get("current_loss_streak"))
    sum_r = safe_float(kpi.get("sum_r"))
    winrate = safe_float(kpi.get("winrate_pct"))
    total_trades = safe_int(kpi.get("total_trades"))

    if dd_pct >= RED_DD_PCT:
        reasons_red.append(f"MAX_DD_PCT>={RED_DD_PCT}")

    if cur_loss_streak >= RED_LOSS_STREAK:
        reasons_red.append(f"LOSS_STREAK>={RED_LOSS_STREAK}")

    if sum_r <= RED_R_SUM:
        reasons_red.append(f"SUM_R<={RED_R_SUM}")

    if total_trades >= 5 and winrate < RED_WINRATE_MIN:
        reasons_red.append(f"WINRATE<{RED_WINRATE_MIN}")

    if reasons_red:
        return {
            "gate_level": "RED",
            "allow_new_entries": False,
            "risk_multiplier": 0.0,
            "reasons": reasons_red
        }

    if dd_pct >= YELLOW_DD_PCT:
        reasons_yellow.append(f"MAX_DD_PCT>={YELLOW_DD_PCT}")

    if cur_loss_streak >= YELLOW_LOSS_STREAK:
        reasons_yellow.append(f"LOSS_STREAK>={YELLOW_LOSS_STREAK}")

    if sum_r <= YELLOW_R_SUM:
        reasons_yellow.append(f"SUM_R<={YELLOW_R_SUM}")

    if total_trades >= 5 and winrate < YELLOW_WINRATE_MIN:
        reasons_yellow.append(f"WINRATE<{YELLOW_WINRATE_MIN}")

    if reasons_yellow:
        return {
            "gate_level": "YELLOW",
            "allow_new_entries": True,
            "risk_multiplier": 0.5,
            "reasons": reasons_yellow
        }

    return {
        "gate_level": "GREEN",
        "allow_new_entries": True,
        "risk_multiplier": 1.0,
        "reasons": ["NORMAL"]
    }


# -------------------------------------------------------------------
# CONTROLS + GATES
# -------------------------------------------------------------------
@app.get("/controls/effective")
def controls_effective(
    symbol: Optional[str] = Query(default=None),
    _: bool = Depends(app_token_guard)
):
    return {
        "ok": True,
        "controls": get_runtime_controls(symbol)
    }


@app.get("/status/gate_auto")
def gate_auto(
    symbol: Optional[str] = Query(default=None),
    account: Optional[str] = Query(default=None),
    magic: Optional[str] = Query(default=None),
    lookback_days: int = Query(default=DEFAULT_KPI_LOOKBACK_DAYS),
    limit_trades: int = Query(default=DEFAULT_KPI_LIMIT_TRADES),
):
    rows = get_deals_filtered(
        symbol=symbol,
        account=account,
        magic=magic,
        lookback_days=lookback_days,
        limit_trades=limit_trades
    )

    kpis = summarize_kpis(rows)
    gate = auto_gate_from_kpis(kpis)

    return {
        "ok": True,
        "filters": {
            "symbol": symbol.upper() if symbol else None,
            "account": account,
            "magic": magic,
            "lookback_days": lookback_days,
            "limit_trades": limit_trades,
        },
        "kpis": kpis,
        "gate": gate
    }


@app.get("/status/gate_combo")
def gate_combo(
    symbol: Optional[str] = Query(default=None),
    account: Optional[str] = Query(default=None),
    magic: Optional[str] = Query(default=None),
):
    controls = get_runtime_controls(symbol)
    auto_payload = gate_auto(symbol=symbol, account=account, magic=magic)
    auto_gate = auto_payload["gate"]

    paused = bool(controls["paused"])
    controls_allow = bool(controls["allow_new_entries"])
    auto_allow = bool(auto_gate["allow_new_entries"])

    allow_new_entries = (not paused) and controls_allow and auto_allow
    final_risk_multiplier = safe_float(controls["risk_multiplier"], 1.0) * safe_float(auto_gate["risk_multiplier"], 1.0)

    gate_level = auto_gate["gate_level"]
    if paused:
        gate_level = "RED"

    reasons = []
    if paused:
        reasons.append("PAUSED")
    if not controls_allow:
        reasons.append("CONTROL_BLOCK")
    reasons.extend(auto_gate.get("reasons", []))

    return {
        "ok": True,
        "symbol": symbol.upper() if symbol else None,
        "gate_level": gate_level,
        "allow_new_entries": allow_new_entries,
        "risk_multiplier": round(final_risk_multiplier, 4),
        "paused": paused,
        "controls": controls,
        "auto_gate": auto_gate,
        "reasons": reasons
    }


# -------------------------------------------------------------------
# KPI ENDPOINTS
# -------------------------------------------------------------------
@app.get("/kpis/rolling")
def rolling_kpis(
    symbol: Optional[str] = Query(default=None),
    account: Optional[str] = Query(default=None),
    magic: Optional[str] = Query(default=None),
    lookback_days: int = Query(default=DEFAULT_KPI_LOOKBACK_DAYS),
    limit_trades: int = Query(default=DEFAULT_KPI_LIMIT_TRADES),
):
    rows = get_deals_filtered(
        symbol=symbol,
        account=account,
        magic=magic,
        lookback_days=lookback_days,
        limit_trades=limit_trades
    )

    kpis = summarize_kpis(rows)

    return {
        "ok": True,
        "filters": {
            "symbol": symbol.upper() if symbol else None,
            "account": account,
            "magic": magic,
            "lookback_days": lookback_days,
            "limit_trades": limit_trades,
        },
        "kpis": kpis
    }


@app.get("/status/system_overview")
def system_overview(
    symbol: Optional[str] = Query(default=None),
    account: Optional[str] = Query(default=None),
    magic: Optional[str] = Query(default=None),
    lookback_days: int = Query(default=DEFAULT_KPI_LOOKBACK_DAYS),
    limit_trades: int = Query(default=DEFAULT_KPI_LIMIT_TRADES),
):
    rows = get_deals_filtered(
        symbol=symbol,
        account=account,
        magic=magic,
        lookback_days=lookback_days,
        limit_trades=limit_trades
    )

    kpis = summarize_kpis(rows)
    gate = gate_combo(symbol=symbol, account=account, magic=magic)

    heartbeat_payload = {"ok": False, "connected_count": 0, "items": []}
    try:
        if symbol:
            heartbeat_payload = heartbeat_status(symbol=symbol)
        else:
            heartbeat_payload = heartbeat_status()
    except Exception:
        pass

    return {
        "ok": True,
        "server_time_utc": utc_iso(),
        "filters": {
            "symbol": symbol.upper() if symbol else None,
            "account": account,
            "magic": magic,
            "lookback_days": lookback_days,
            "limit_trades": limit_trades,
        },
        "heartbeat": heartbeat_payload,
        "controls": get_runtime_controls(symbol),
        "kpis": kpis,
        "gate": gate
    }
