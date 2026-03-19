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

app = FastAPI(title="Signal Agent API", version="4.1.0")

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

# App dashboard protection
APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
APP_TOKEN_HEADER = os.getenv("APP_TOKEN_HEADER", "X-APP-TOKEN").strip() or "X-APP-TOKEN"

DB_LOCK = threading.Lock()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

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

class HeartbeatPing(BaseModel):
    key: str
    symbol: str
    account: Optional[str] = None
    magic: Optional[str] = None
    ea_name: Optional[str] = None
    version: Optional[str] = None
    status: Optional[str] = None

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

class DealEvent(BaseModel):
    account: Optional[str] = None
    symbol: str
    position_id: Optional[str] = None
    magic: Optional[int] = None
    ticket: Optional[int] = None
    deal_id: Optional[int] = None
    type: str
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

class ControlActionRequest(BaseModel):
    symbol: Optional[str] = None
    note: Optional[str] = None

class ControlRiskFactorRequest(BaseModel):
    symbol: Optional[str] = None
    risk_factor: float
    note: Optional[str] = None

class TVSignal(BaseModel):
    key: str
    symbol: str
    action: str
    id: Optional[str] = None
    ts: Optional[str] = None

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

def norm_symbol(raw: str) -> str:
    s = (raw or "").strip().upper()

    if s in {
        "GOLD",
        "XAU",
        "XAUUSD",
        "OANDA:XAUUSD",
        "FOREXCOM:XAUUSD",
        "CAPITALCOM:GOLD",
    }:
        return "XAUUSD"

    if s in {
        "BTC",
        "BTCUSD",
        "BITCOIN",
        "COINBASE:BTCUSD",
        "BINANCE:BTCUSDT",
        "INDEX:BTCUSD",
    }:
        return "BTCUSD"

    return s

def norm_action(raw: str) -> str:
    a = (raw or "").strip().upper()

    if a in {"BUY", "LONG"}:
        return "BUY"

    if a in {"SELL", "SHORT"}:
        return "SELL"

    return ""

def heartbeat_age_sec(last_seen_utc: Optional[str]) -> Optional[int]:
    dt = parse_iso(last_seen_utc)
    if dt is None:
        return None
    return max(0, int((now_utc_dt() - dt).total_seconds()))

def is_heartbeat_connected(last_seen_utc: Optional[str]) -> bool:
    age = heartbeat_age_sec(last_seen_utc)
    if age is None:
        return False
    return age <= HEARTBEAT_TIMEOUT_SEC

def control_scope_key(symbol: Optional[str]) -> str:
    sym = norm_symbol(symbol or "")
    return "GLOBAL" if not sym else sym

def delivery_key(account: Optional[str], magic: Optional[str]) -> tuple[str, str]:
    return ((account or "").strip(), (magic or "").strip())

def safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def require_app_token(x_app_token: Optional[str]) -> None:
    # If APP_TOKEN is not set, dashboard remains open (dev mode).
    if not APP_TOKEN:
        return
    if (x_app_token or "").strip() != APP_TOKEN:
        raise HTTPException(status_code=401, detail="bad app token")

def date_matches_today(raw: Optional[str]) -> bool:
    if not raw:
        return False

    s = raw.strip()
    if not s:
        return False

    # ISO
    dt = parse_iso(s)
    if dt is not None:
        return dt.astimezone(timezone.utc).date() == now_utc_dt().date()

    # MT5 style: YYYY.MM.DD HH:MM:SS
    today_prefix_mt5 = now_utc_dt().strftime("%Y.%m.%d")
    if s.startswith(today_prefix_mt5):
        return True

    # ISO date only fallback
    today_prefix_iso = now_utc_dt().strftime("%Y-%m-%d")
    if s.startswith(today_prefix_iso):
        return True

    return False

def profit_factor(values: List[float]) -> Optional[float]:
    gp = sum(x for x in values if x > 0)
    gl = abs(sum(x for x in values if x < 0))
    if gl <= 0:
        return None
    return gp / gl

def loss_streak(values: List[float]) -> int:
    s = 0
    for x in reversed(values):
        if x < 0:
            s += 1
        else:
            break
    return s

def max_drawdown(values: List[float]) -> float:
    peak = 0.0
    equity = 0.0
    max_dd = 0.0

    for p in values:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    return max_dd

def winrate(values: List[float]) -> Optional[float]:
    if not values:
        return None
    wins = sum(1 for x in values if x > 0)
    return wins / len(values)

def row_net_profit(row: sqlite3.Row) -> float:
    return (
        safe_float(row["profit"])
        + safe_float(row["commission"])
        + safe_float(row["swap"])
    )

# -------------------------------------------------------------------
# LOGIN / JWT
# -------------------------------------------------------------------
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = now_utc_dt() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def authenticate_user(username: str, password: str) -> bool:
    return username == APP_USERNAME and password == APP_PASSWORD

def get_current_user(token: str = Depends(oauth2_scheme)) -> UserResponse:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Ungültiger oder abgelaufener Token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise credentials_exception
        return UserResponse(username=username)
    except JWTError:
        raise credentials_exception

# -------------------------------------------------------------------
# DB
# -------------------------------------------------------------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def table_cols(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r["name"] for r in rows]

def ensure_control_row(conn: sqlite3.Connection, scope_key: str) -> sqlite3.Row:
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO control_state (
            scope_key,
            pause_new_trades,
            risk_factor,
            updated_utc,
            updated_by,
            note
        ) VALUES (?, 0, 1.0, ?, ?, ?)
    """, (scope_key, now_utc_iso(), "system", "auto-init"))
    conn.commit()
    cur.execute("SELECT * FROM control_state WHERE scope_key = ?", (scope_key,))
    return cur.fetchone()

def init_db() -> None:
    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS ea_heartbeat (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            account TEXT,
            magic TEXT,
            ea_name TEXT,
            version TEXT,
            status TEXT,
            first_seen_utc TEXT NOT NULL,
            last_seen_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL,
            UNIQUE(symbol, account, magic)
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

        # New institutional deals table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_utc TEXT NOT NULL,
            account TEXT,
            symbol TEXT NOT NULL,
            position_id TEXT,
            magic INTEGER,
            ticket INTEGER,
            deal_id INTEGER,
            type TEXT,
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
        CREATE INDEX IF NOT EXISTS idx_deals_symbol_id
        ON deals(symbol, id)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_deals_position_id
        ON deals(position_id)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_risk_events_symbol_id
        ON risk_events(symbol, id)
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_risk_events_position_id
        ON risk_events(position_id)
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS control_state (
            scope_key TEXT PRIMARY KEY,
            pause_new_trades INTEGER NOT NULL DEFAULT 0,
            risk_factor REAL NOT NULL DEFAULT 1.0,
            updated_utc TEXT NOT NULL,
            updated_by TEXT,
            note TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS debug_state (
            symbol TEXT PRIMARY KEY,
            active_action TEXT,
            updated_utc TEXT,
            note TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_state (
            symbol TEXT PRIMARY KEY,
            signal_id TEXT,
            action TEXT,
            source_ts TEXT,
            updated_utc TEXT NOT NULL,
            raw_symbol TEXT,
            raw_action TEXT,
            note TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_deliveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            updated_utc TEXT NOT NULL,
            account TEXT NOT NULL,
            magic TEXT NOT NULL,
            acked_utc TEXT NOT NULL,
            UNIQUE(symbol, updated_utc, account, magic)
        )
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_signal_deliveries_lookup
        ON signal_deliveries(symbol, updated_utc, account, magic)
        """)

        # safe migrations
        dcols = table_cols(conn, "deals")
        for col, coldef in [
            ("position_id", "TEXT"),
            ("risk_usd", "REAL"),
            ("r_multiple", "REAL"),
            ("deal_id", "INTEGER"),
            ("ticket", "INTEGER"),
        ]:
            if col not in dcols:
                try:
                    cur.execute(f"ALTER TABLE deals ADD COLUMN {col} {coldef}")
                except Exception:
                    pass

        conn.commit()

        ensure_control_row(conn, "GLOBAL")
        ensure_control_row(conn, "XAUUSD")
        ensure_control_row(conn, "BTCUSD")

        conn.close()

@app.on_event("startup")
def startup() -> None:
    init_db()

# -------------------------------------------------------------------
# CONTROL STATE HELPERS
# -------------------------------------------------------------------
def get_control_state_payload(conn: sqlite3.Connection, symbol: Optional[str]) -> dict[str, Any]:
    global_row = ensure_control_row(conn, "GLOBAL")

    sym = norm_symbol(symbol or "")
    symbol_row = ensure_control_row(conn, sym) if sym else None

    global_pause = int(global_row["pause_new_trades"]) == 1
    global_rf = float(global_row["risk_factor"])

    symbol_pause = False
    symbol_rf = 1.0

    if symbol_row is not None:
        symbol_pause = int(symbol_row["pause_new_trades"]) == 1
        symbol_rf = float(symbol_row["risk_factor"])

    effective_pause = global_pause or symbol_pause
    effective_rf = min(global_rf, symbol_rf) if symbol_row is not None else global_rf

    return {
        "symbol": sym or None,
        "global": {
            "scope_key": global_row["scope_key"],
            "pause_new_trades": global_pause,
            "risk_factor": global_rf,
            "updated_utc": global_row["updated_utc"],
            "updated_by": global_row["updated_by"],
            "note": global_row["note"],
        },
        "symbol_control": None if symbol_row is None else {
            "scope_key": symbol_row["scope_key"],
            "pause_new_trades": symbol_pause,
            "risk_factor": symbol_rf,
            "updated_utc": symbol_row["updated_utc"],
            "updated_by": symbol_row["updated_by"],
            "note": symbol_row["note"],
        },
        "effective": {
            "pause_new_trades": effective_pause,
            "risk_factor": effective_rf,
        },
        "effective_pause_new_trades": effective_pause,
        "effective_risk_factor": effective_rf,
    }

# -------------------------------------------------------------------
# ROOT
# -------------------------------------------------------------------
@app.get("/")
def root() -> dict[str, Any]:
    return {
        "status": "Signal Agent API running",
        "mode": "dashboard_backend_plus_signal_agent",
        "login_enabled": True,
        "heartbeat_timeout_sec": HEARTBEAT_TIMEOUT_SEC,
        "app_token_protected": bool(APP_TOKEN),
    }

@app.get("/health")
def health() -> dict[str, Any]:
    with DB_LOCK:
        conn = get_conn()
        conn.execute("SELECT 1").fetchone()
        conn.close()

    return {
        "ok": True,
        "utc": now_utc_iso(),
        "db_path": DB_PATH,
        "app_token_configured": bool(APP_TOKEN),
    }

# -------------------------------------------------------------------
# LOGIN
# -------------------------------------------------------------------
@app.post("/login", response_model=TokenResponse)
def login(data: LoginRequest) -> dict[str, str]:
    if not authenticate_user(data.username, data.password):
        raise HTTPException(status_code=401, detail="Benutzername oder Passwort falsch")

    access_token = create_access_token(
        data={"sub": data.username},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )

    return {
        "access_token": access_token,
        "token_type": "bearer"
    }

@app.get("/me", response_model=UserResponse)
def me(current_user: UserResponse = Depends(get_current_user)) -> UserResponse:
    return current_user

# -------------------------------------------------------------------
# SIGNAL AGENT: TV
# -------------------------------------------------------------------
@app.post("/tv")
def tv(signal: TVSignal) -> dict[str, Any]:
    if signal.key != SECRET_KEY:
        raise HTTPException(status_code=401, detail="invalid key")

    sym = norm_symbol(signal.symbol)
    action = norm_action(signal.action)

    if not sym:
        raise HTTPException(status_code=400, detail="invalid symbol")

    if not action:
        raise HTTPException(status_code=400, detail="invalid action")

    updated_utc = now_utc_iso()
    signal_id = (signal.id or "").strip()
    if not signal_id:
        signal_id = f"{sym}{action}{updated_utc}"

    source_ts = (signal.ts or "").strip()
    note = f"tv id={signal_id} ts={source_ts} raw_symbol={signal.symbol} raw_action={signal.action}"

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO signal_state (
                symbol, signal_id, action, source_ts, updated_utc, raw_symbol, raw_action, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                signal_id   = excluded.signal_id,
                action      = excluded.action,
                source_ts   = excluded.source_ts,
                updated_utc = excluded.updated_utc,
                raw_symbol  = excluded.raw_symbol,
                raw_action  = excluded.raw_action,
                note        = excluded.note
        """, (
            sym,
            signal_id,
            action,
            source_ts,
            updated_utc,
            signal.symbol,
            signal.action,
            note,
        ))

        cur.execute("""
            INSERT INTO debug_state (symbol, active_action, updated_utc, note)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                active_action = excluded.active_action,
                updated_utc   = excluded.updated_utc,
                note          = excluded.note
        """, (sym, action, updated_utc, note))

        conn.commit()
        conn.close()

    return {
        "status": "ok",
        "symbol": sym,
        "action": action,
        "id": signal_id,
        "updated_utc": updated_utc,
    }

@app.post("/webhook")
def webhook_alias(signal: TVSignal) -> dict[str, Any]:
    return tv(signal)

# -------------------------------------------------------------------
# SIGNAL AGENT: LATEST
# -------------------------------------------------------------------
@app.get("/latest")
def latest(
    symbol: str,
    account: Optional[str] = Query(default=None),
    magic: Optional[str] = Query(default=None),
) -> dict[str, Any]:
    sym = norm_symbol(symbol)
    acc, mag = delivery_key(account, magic)

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT symbol, signal_id, action, source_ts, updated_utc, raw_symbol, raw_action, note
            FROM signal_state
            WHERE symbol = ?
        """, (sym,))
        row = cur.fetchone()

        if row is None or not row["action"] or not row["updated_utc"]:
            conn.close()
            return {"signal": None}

        if acc and mag:
            cur.execute("""
                SELECT 1
                FROM signal_deliveries
                WHERE symbol = ? AND updated_utc = ? AND account = ? AND magic = ?
                LIMIT 1
            """, (sym, row["updated_utc"], acc, mag))
            ack_row = cur.fetchone()
            if ack_row is not None:
                conn.close()
                return {"signal": None}

        conn.close()

    return {
        "signal": {
            "symbol": row["symbol"],
            "action": row["action"],
            "id": row["signal_id"],
            "ts": row["source_ts"],
        },
        "updated_utc": row["updated_utc"],
        "note": row["note"],
        "account": account,
        "magic": magic,
    }

# -------------------------------------------------------------------
# SIGNAL AGENT: ACK
# -------------------------------------------------------------------
@app.post("/ack")
def ack(
    symbol: str,
    updated_utc: str,
    account: Optional[str] = Query(default=None),
    magic: Optional[str] = Query(default=None),
) -> dict[str, Any]:
    sym = norm_symbol(symbol)
    acc, mag = delivery_key(account, magic)

    if not acc:
        raise HTTPException(status_code=400, detail="account required")
    if not mag:
        raise HTTPException(status_code=400, detail="magic required")
    if not updated_utc:
        raise HTTPException(status_code=400, detail="updated_utc required")

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT symbol, updated_utc
            FROM signal_state
            WHERE symbol = ?
        """, (sym,))
        row = cur.fetchone()

        if row is None:
            conn.close()
            return {
                "status": "ok",
                "acknowledged": False,
                "reason": "no signal for symbol",
                "symbol": sym,
            }

        if row["updated_utc"] != updated_utc:
            conn.close()
            return {
                "status": "ok",
                "acknowledged": False,
                "reason": "updated_utc mismatch",
                "symbol": sym,
                "server_updated_utc": row["updated_utc"],
            }

        cur.execute("""
            INSERT OR IGNORE INTO signal_deliveries (
                symbol, updated_utc, account, magic, acked_utc
            ) VALUES (?, ?, ?, ?, ?)
        """, (sym, updated_utc, acc, mag, now_utc_iso()))

        cur.execute("""
            UPDATE debug_state
            SET note = ?
            WHERE symbol = ?
        """, (f"last_ack account={acc} magic={mag} updated_utc={updated_utc}", sym))

        conn.commit()
        conn.close()

    return {
        "status": "ok",
        "acknowledged": True,
        "symbol": sym,
        "updated_utc": updated_utc,
        "account": acc,
        "magic": mag,
    }

# -------------------------------------------------------------------
# GATE COMPAT
# -------------------------------------------------------------------
# Intentionally kept compatible for existing EAs.
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
# DEBUG STATE
# -------------------------------------------------------------------
@app.get("/debug/state")
def debug_state(symbol: Optional[str] = Query(default=None)) -> dict[str, Any]:
    sym = norm_symbol(symbol or "")

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        if sym:
            cur.execute("""
                SELECT symbol, signal_id, action, source_ts, updated_utc, raw_symbol, raw_action, note
                FROM signal_state
                WHERE symbol = ?
            """, (sym,))
            state_rows = cur.fetchall()

            cur.execute("""
                SELECT symbol, updated_utc, account, magic, acked_utc
                FROM signal_deliveries
                WHERE symbol = ?
                ORDER BY acked_utc DESC
            """, (sym,))
            delivery_rows = cur.fetchall()
        else:
            cur.execute("""
                SELECT symbol, signal_id, action, source_ts, updated_utc, raw_symbol, raw_action, note
                FROM signal_state
                ORDER BY symbol
            """)
            state_rows = cur.fetchall()

            cur.execute("""
                SELECT symbol, updated_utc, account, magic, acked_utc
                FROM signal_deliveries
                ORDER BY symbol, acked_utc DESC
            """)
            delivery_rows = cur.fetchall()

        conn.close()

    states = []
    for r in state_rows:
        states.append({
            "symbol": r["symbol"],
            "signal_id": r["signal_id"],
            "action": r["action"],
            "source_ts": r["source_ts"],
            "updated_utc": r["updated_utc"],
            "raw_symbol": r["raw_symbol"],
            "raw_action": r["raw_action"],
            "note": r["note"],
        })

    deliveries = []
    for r in delivery_rows:
        deliveries.append({
            "symbol": r["symbol"],
            "updated_utc": r["updated_utc"],
            "account": r["account"],
            "magic": r["magic"],
            "acked_utc": r["acked_utc"],
        })

    return {
        "server_time_utc": now_utc_iso(),
        "states": states,
        "deliveries": deliveries,
    }

# -------------------------------------------------------------------
# RISK
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
# DEALS (NEW, safe addition)
# -------------------------------------------------------------------
@app.post("/deal")
def deal(event: DealEvent) -> dict[str, Any]:
    sym = norm_symbol(event.symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol required")

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        if event.deal_id is not None:
            row = cur.execute(
                "SELECT id FROM deals WHERE deal_id = ? LIMIT 1",
                (int(event.deal_id),),
            ).fetchone()
            if row is not None:
                conn.close()
                return {"status": "ok", "dedup": True, "id": int(row["id"])}

        cur.execute("""
            INSERT INTO deals (
                created_utc, account, symbol, position_id, magic, ticket, deal_id, type,
                lots, open_time, close_time, open_price, close_price, sl, tp,
                profit, commission, swap, comment, risk_usd, r_multiple
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now_utc_iso(),
            event.account,
            sym,
            event.position_id,
            event.magic,
            event.ticket,
            event.deal_id,
            event.type,
            event.lots,
            event.open_time,
            event.close_time,
            event.open_price,
            event.close_price,
            event.sl,
            event.tp,
            event.profit,
            event.commission,
            event.swap,
            event.comment,
            event.risk_usd,
            event.r_multiple,
        ))

        conn.commit()
        row_id = cur.lastrowid
        conn.close()

    return {"status": "ok", "id": row_id}

@app.get("/deals")
def deals(symbol: Optional[str] = Query(default=None), limit: int = 50) -> dict[str, Any]:
    limit = max(1, min(int(limit), 500))
    sym = norm_symbol(symbol or "")

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        if sym:
            cur.execute("""
                SELECT *
                FROM deals
                WHERE symbol = ?
                ORDER BY id DESC
                LIMIT ?
            """, (sym, limit))
        else:
            cur.execute("""
                SELECT *
                FROM deals
                ORDER BY id DESC
                LIMIT ?
            """, (limit,))

        rows = cur.fetchall()
        conn.close()

    return {"items": [dict(r) for r in rows], "count": len(rows)}

# -------------------------------------------------------------------
# HEARTBEAT
# -------------------------------------------------------------------
@app.post("/heartbeat")
def heartbeat(ping: HeartbeatPing) -> dict[str, Any]:
    if ping.key != SECRET_KEY:
        raise HTTPException(status_code=401, detail="invalid key")

    sym = norm_symbol(ping.symbol)
    acc = (ping.account or "").strip()
    mag = (ping.magic or "").strip()
    ea_name = (ping.ea_name or "").strip() or None
    version = (ping.version or "").strip() or None
    hb_status = (ping.status or "").strip() or "running"

    if not sym:
        raise HTTPException(status_code=400, detail="symbol required")
    if not acc:
        raise HTTPException(status_code=400, detail="account required")
    if not mag:
        raise HTTPException(status_code=400, detail="magic required")

    now_iso = now_utc_iso()

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            INSERT OR IGNORE INTO ea_heartbeat (
                symbol, account, magic, ea_name, version, status,
                first_seen_utc, last_seen_utc, updated_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sym, acc, mag, ea_name, version, hb_status,
            now_iso, now_iso, now_iso
        ))

        conn.commit()

        cur.execute("""
            UPDATE ea_heartbeat
            SET
                ea_name = COALESCE(?, ea_name),
                version = COALESCE(?, version),
                status = ?,
                last_seen_utc = ?,
                updated_utc = ?
            WHERE symbol = ? AND account = ? AND magic = ?
        """, (
            ea_name,
            version,
            hb_status,
            now_iso,
            now_iso,
            sym,
            acc,
            mag
        ))

        conn.commit()
        conn.close()

    return {
        "status": "ok",
        "symbol": sym,
        "account": acc,
        "magic": mag,
        "last_seen_utc": now_iso
    }

@app.get("/heartbeat/status")
def heartbeat_status(symbol: str) -> dict[str, Any]:
    sym = norm_symbol(symbol)

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, account, magic, ea_name, version, status, last_seen_utc
            FROM ea_heartbeat
            WHERE symbol = ?
            ORDER BY last_seen_utc DESC
            LIMIT 1
        """, (sym,))
        row = cur.fetchone()
        conn.close()

    if row is None:
        return {
            "symbol": sym,
            "connected": False,
            "last_seen_utc": None,
            "age_sec": None,
            "account": None,
            "magic": None,
            "ea_name": None,
            "version": None,
            "status": "offline",
            "timeout_sec": HEARTBEAT_TIMEOUT_SEC
        }

    last_seen = row["last_seen_utc"]
    connected = is_heartbeat_connected(last_seen)

    return {
        "symbol": sym,
        "connected": connected,
        "last_seen_utc": last_seen,
        "age_sec": heartbeat_age_sec(last_seen),
        "account": row["account"],
        "magic": row["magic"],
        "ea_name": row["ea_name"],
        "version": row["version"],
        "status": row["status"] if connected else "offline",
        "timeout_sec": HEARTBEAT_TIMEOUT_SEC
    }

@app.get("/heartbeat/overview")
def heartbeat_overview() -> dict[str, Any]:
    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, account, magic, ea_name, version, status, last_seen_utc
            FROM ea_heartbeat
            ORDER BY symbol, last_seen_utc DESC
        """)
        rows = cur.fetchall()
        conn.close()

    items = []
    for r in rows:
        last_seen = r["last_seen_utc"]
        items.append({
            "symbol": r["symbol"],
            "account": r["account"],
            "magic": r["magic"],
            "ea_name": r["ea_name"],
            "version": r["version"],
            "last_seen_utc": last_seen,
            "age_sec": heartbeat_age_sec(last_seen),
            "connected": is_heartbeat_connected(last_seen),
            "status": r["status"] if is_heartbeat_connected(last_seen) else "offline",
        })

    return {
        "timeout_sec": HEARTBEAT_TIMEOUT_SEC,
        "items": items
    }

# -------------------------------------------------------------------
# CONTROL STATE
# -------------------------------------------------------------------
@app.get("/control/state")
def control_state(symbol: Optional[str] = Query(default=None)) -> dict[str, Any]:
    with DB_LOCK:
        conn = get_conn()
        payload = get_control_state_payload(conn, symbol)
        conn.close()
    return payload

@app.post("/control/pause")
def control_pause(
    data: ControlActionRequest,
    current_user: UserResponse = Depends(get_current_user),
) -> dict[str, Any]:
    scope_key = control_scope_key(data.symbol)

    with DB_LOCK:
        conn = get_conn()
        ensure_control_row(conn, scope_key)
        cur = conn.cursor()
        cur.execute("""
            UPDATE control_state
            SET
                pause_new_trades = 1,
                updated_utc = ?,
                updated_by = ?,
                note = ?
            WHERE scope_key = ?
        """, (
            now_utc_iso(),
            current_user.username,
            data.note or "paused via app",
            scope_key
        ))
        conn.commit()
        payload = get_control_state_payload(conn, None if scope_key == "GLOBAL" else scope_key)
        conn.close()

    return {
        "status": "ok",
        "action": "pause",
        "scope_key": scope_key,
        "control": payload
    }

@app.post("/control/resume")
def control_resume(
    data: ControlActionRequest,
    current_user: UserResponse = Depends(get_current_user),
) -> dict[str, Any]:
    scope_key = control_scope_key(data.symbol)

    with DB_LOCK:
        conn = get_conn()
        ensure_control_row(conn, scope_key)
        cur = conn.cursor()
        cur.execute("""
            UPDATE control_state
            SET
                pause_new_trades = 0,
                updated_utc = ?,
                updated_by = ?,
                note = ?
            WHERE scope_key = ?
        """, (
            now_utc_iso(),
            current_user.username,
            data.note or "resumed via app",
            scope_key
        ))
        conn.commit()
        payload = get_control_state_payload(conn, None if scope_key == "GLOBAL" else scope_key)
        conn.close()

    return {
        "status": "ok",
        "action": "resume",
        "scope_key": scope_key,
        "control": payload
    }

@app.post("/control/risk_factor")
def control_risk_factor(
    data: ControlRiskFactorRequest,
    current_user: UserResponse = Depends(get_current_user),
) -> dict[str, Any]:
    if data.risk_factor <= 0 or data.risk_factor > 1.0:
        raise HTTPException(status_code=400, detail="risk_factor must be > 0 and <= 1.0")

    scope_key = control_scope_key(data.symbol)

    with DB_LOCK:
        conn = get_conn()
        ensure_control_row(conn, scope_key)
        cur = conn.cursor()
        cur.execute("""
            UPDATE control_state
            SET
                risk_factor = ?,
                updated_utc = ?,
                updated_by = ?,
                note = ?
            WHERE scope_key = ?
        """, (
            float(data.risk_factor),
            now_utc_iso(),
            current_user.username,
            data.note or "risk factor updated via app",
            scope_key
        ))
        conn.commit()
        payload = get_control_state_payload(conn, None if scope_key == "GLOBAL" else scope_key)
        conn.close()

    return {
        "status": "ok",
        "action": "risk_factor",
        "scope_key": scope_key,
        "control": payload
    }

# -------------------------------------------------------------------
# HISTORY / COMPAT
# -------------------------------------------------------------------
@app.get("/signals/history")
def signals_history(limit: int = 20) -> dict[str, Any]:
    limit = max(1, min(int(limit), 200))

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, signal_id, action, source_ts, updated_utc, note
            FROM signal_state
            ORDER BY updated_utc DESC
            LIMIT ?
        """, (limit,))
        rows = cur.fetchall()
        conn.close()

    signals = []
    for r in rows:
        signals.append({
            "symbol": r["symbol"],
            "action": r["action"],
            "status": "active",
            "time": r["updated_utc"],
            "tv_id": r["signal_id"],
            "tv_ts": r["source_ts"],
            "updated_utc": r["updated_utc"],
            "note": r["note"],
        })

    return {"signals": signals}

@app.get("/risk/history")
def risk_history(limit: int = 20) -> dict[str, Any]:
    limit = max(1, min(int(limit), 200))

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT created_utc, account, symbol, risk_usd, lots, position_id, magic, entry_price, sl, open_time
            FROM risk_events
            ORDER BY id DESC
            LIMIT ?
        """, (limit,))
        rows = cur.fetchall()
        conn.close()

    risk = []
    for r in rows:
        risk.append({
            "time": r["created_utc"],
            "account": r["account"],
            "symbol": r["symbol"],
            "risk_usd": r["risk_usd"],
            "lots": r["lots"],
            "position_id": r["position_id"],
            "magic": r["magic"],
            "entry_price": r["entry_price"],
            "sl": r["sl"],
            "open_time": r["open_time"],
        })

    return {"risk": risk}

@app.get("/accounts/overview")
def accounts_overview() -> dict[str, Any]:
    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                account,
                COUNT(*) AS risk_event_count,
                MAX(created_utc) AS last_event_utc,
                ROUND(COALESCE(SUM(risk_usd), 0), 2) AS total_risk_usd
            FROM risk_events
            WHERE account IS NOT NULL
              AND TRIM(account) <> ''
            GROUP BY account
            ORDER BY last_event_utc DESC
        """)
        rows = cur.fetchall()
        conn.close()

    return {
        "accounts": [
            {
                "account": r["account"],
                "risk_event_count": r["risk_event_count"],
                "last_event_utc": r["last_event_utc"],
                "total_risk_usd": r["total_risk_usd"],
            }
            for r in rows
        ]
    }

# -------------------------------------------------------------------
# KPI HELPERS
# -------------------------------------------------------------------
def fetch_deal_rows(symbol: str) -> List[sqlite3.Row]:
    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT *
            FROM deals
            WHERE symbol = ?
            ORDER BY id ASC
        """, (symbol,))
        rows = cur.fetchall()
        conn.close()
    return rows

def fetch_deal_rows_last_n(symbol: str, n: int) -> List[sqlite3.Row]:
    n = max(1, min(int(n), 500))
    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT *
            FROM deals
            WHERE symbol = ?
            ORDER BY id DESC
            LIMIT ?
        """, (symbol, n))
        rows = cur.fetchall()
        conn.close()
    rows.reverse()
    return rows

def fetch_risk_rows(symbol: str) -> List[sqlite3.Row]:
    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT *
            FROM risk_events
            WHERE symbol = ?
            ORDER BY id ASC
        """, (symbol,))
        rows = cur.fetchall()
        conn.close()
    return rows

def daily_pnl(symbol: str) -> float:
    rows = fetch_deal_rows(symbol)
    return round(sum(row_net_profit(r) for r in rows if date_matches_today(r["close_time"])), 2)

def daily_r(symbol: str) -> float:
    rows = fetch_deal_rows(symbol)
    values = [
        safe_float(r["r_multiple"])
        for r in rows
        if r["r_multiple"] is not None and date_matches_today(r["close_time"])
    ]
    return round(sum(values), 4)

def trade_count_today(symbol: str) -> int:
    rows = fetch_deal_rows(symbol)
    return sum(1 for r in rows if date_matches_today(r["close_time"]))

def rolling_pnl_last_n(symbol: str, n: int) -> float:
    rows = fetch_deal_rows_last_n(symbol, n)
    return round(sum(row_net_profit(r) for r in rows), 2)

def rolling_r_last_n(symbol: str, n: int) -> float:
    rows = fetch_deal_rows_last_n(symbol, n)
    values = [safe_float(r["r_multiple"]) for r in rows if r["r_multiple"] is not None]
    return round(sum(values), 4)

def profit_factor_last_n(symbol: str, n: int) -> Optional[float]:
    rows = fetch_deal_rows_last_n(symbol, n)
    values = [row_net_profit(r) for r in rows]
    pf = profit_factor(values)
    return None if pf is None else round(pf, 4)

def winrate_last_n(symbol: str, n: int) -> Optional[float]:
    rows = fetch_deal_rows_last_n(symbol, n)
    values = [row_net_profit(r) for r in rows]
    wr = winrate(values)
    return None if wr is None else round(wr, 4)

def loss_streak_last_n(symbol: str, n: int) -> int:
    rows = fetch_deal_rows_last_n(symbol, n)
    values = [row_net_profit(r) for r in rows]
    return loss_streak(values)

def max_drawdown_usd(symbol: str) -> float:
    rows = fetch_deal_rows(symbol)
    values = [row_net_profit(r) for r in rows]
    return round(max_drawdown(values), 2)

def max_drawdown_r(symbol: str) -> float:
    rows = fetch_deal_rows(symbol)
    values = [safe_float(r["r_multiple"]) for r in rows if r["r_multiple"] is not None]
    return round(max_drawdown(values), 4)

def open_risk_usd(symbol: str) -> float:
    rows = fetch_risk_rows(symbol)

    # latest row per position_id only
    latest_by_position: Dict[str, sqlite3.Row] = {}
    for r in rows:
        pos_id = (r["position_id"] or "").strip()
        if not pos_id:
            continue
        latest_by_position[pos_id] = r

    total = 0.0
    for r in latest_by_position.values():
        total += safe_float(r["risk_usd"])

    return round(total, 2)

def institutional_kpis(symbol: str, n: int = 20) -> dict[str, Any]:
    sym = norm_symbol(symbol)
    rows_last_n = fetch_deal_rows_last_n(sym, n)

    net_values_last_n = [row_net_profit(r) for r in rows_last_n]
    r_values_last_n = [safe_float(r["r_multiple"]) for r in rows_last_n if r["r_multiple"] is not None]

    pf_r = profit_factor(r_values_last_n)
    sample_size = len(rows_last_n)

    return {
        "daily_pnl": daily_pnl(sym),
        "daily_r": daily_r(sym),
        "trade_count_today": trade_count_today(sym),
        "rolling_pnl_last_n": rolling_pnl_last_n(sym, n),
        "rolling_r_last_n": rolling_r_last_n(sym, n),
        "profit_factor_last_n": profit_factor_last_n(sym, n),
        "profit_factor_r_last_n": None if pf_r is None else round(pf_r, 4),
        "winrate_last_n": winrate_last_n(sym, n),
        "loss_streak_last_n": loss_streak_last_n(sym, n),
        "max_drawdown_usd": max_drawdown_usd(sym),
        "max_drawdown_r": max_drawdown_r(sym),
        "open_risk_usd": open_risk_usd(sym),
        "sample_size_last_n": sample_size,
    }

def portfolio_institutional_kpis(symbols: List[str], n: int = 20) -> dict[str, Any]:
    daily_pnl_sum = 0.0
    daily_r_sum = 0.0
    trade_count_sum = 0
    rolling_pnl_sum = 0.0
    rolling_r_sum = 0.0
    max_dd_usd = 0.0
    max_dd_r = 0.0
    open_risk_sum = 0.0

    merged_net_values: List[float] = []

    for sym in symbols:
        k = institutional_kpis(sym, n=n)
        daily_pnl_sum += safe_float(k["daily_pnl"])
        daily_r_sum += safe_float(k["daily_r"])
        trade_count_sum += int(k["trade_count_today"])
        rolling_pnl_sum += safe_float(k["rolling_pnl_last_n"])
        rolling_r_sum += safe_float(k["rolling_r_last_n"])
        max_dd_usd = max(max_dd_usd, safe_float(k["max_drawdown_usd"]))
        max_dd_r = max(max_dd_r, safe_float(k["max_drawdown_r"]))
        open_risk_sum += safe_float(k["open_risk_usd"])

        rows_last_n = fetch_deal_rows_last_n(sym, n)
        merged_net_values.extend([row_net_profit(r) for r in rows_last_n])

    pf = profit_factor(merged_net_values)
    wr = winrate(merged_net_values)

    return {
        "daily_pnl": round(daily_pnl_sum, 2),
        "daily_r": round(daily_r_sum, 4),
        "trade_count_today": trade_count_sum,
        "rolling_pnl_last_n": round(rolling_pnl_sum, 2),
        "rolling_r_last_n": round(rolling_r_sum, 4),
        "profit_factor_last_n": None if pf is None else round(pf, 4),
        "winrate_last_n": None if wr is None else round(wr, 4),
        "max_drawdown_usd": round(max_dd_usd, 2),
        "max_drawdown_r": round(max_dd_r, 4),
        "open_risk_usd": round(open_risk_sum, 2),
        "sample_size_last_n": len(merged_net_values),
    }

# -------------------------------------------------------------------
# DASHBOARD
# -------------------------------------------------------------------
def last_rows(table: str, symbol: Optional[str], limit: int) -> List[dict]:
    limit = max(1, min(int(limit), 200))
    sym = norm_symbol(symbol or "")

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        if table == "deals":
            if sym:
                cur.execute("""
                    SELECT *
                    FROM deals
                    WHERE symbol = ?
                    ORDER BY id DESC
                    LIMIT ?
                """, (sym, limit))
            else:
                cur.execute("""
                    SELECT *
                    FROM deals
                    ORDER BY id DESC
                    LIMIT ?
                """, (limit,))
        elif table == "risk_events":
            if sym:
                cur.execute("""
                    SELECT *
                    FROM risk_events
                    WHERE symbol = ?
                    ORDER BY id DESC
                    LIMIT ?
                """, (sym, limit))
            else:
                cur.execute("""
                    SELECT *
                    FROM risk_events
                    ORDER BY id DESC
                    LIMIT ?
                """, (limit,))
        else:
            conn.close()
            return []

        rows = cur.fetchall()
        conn.close()

    return [dict(r) for r in rows]

@app.get("/dashboard")
def dashboard(
    symbols: str = "BTCUSD,XAUUSD",
    limit_deals: int = 10,
    limit_risks: int = 10,
    n_gate: int = 20,
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> dict[str, Any]:
    require_app_token(x_app_token)

    syms = [norm_symbol(s) for s in symbols.split(",") if norm_symbol(s)]
    if not syms:
        syms = ["BTCUSD", "XAUUSD"]

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        # signals snapshot
        sigs: Dict[str, Any] = {}
        for s in syms:
            cur.execute("""
                SELECT symbol, signal_id, action, source_ts, updated_utc, note
                FROM signal_state
                WHERE symbol = ?
            """, (s,))
            row = cur.fetchone()

            if row is None:
                sigs[s] = {"latest": None, "updated_utc": None, "ack": None}
                continue

            sigs[s] = {
                "latest": {
                    "symbol": row["symbol"],
                    "action": row["action"],
                    "id": row["signal_id"],
                    "ts": row["source_ts"],
                },
                "updated_utc": row["updated_utc"],
                "ack": None,
            }

        conn.close()

    per_symbol = {}
    for s in syms:
        per_symbol[s] = {
            "gate_combo": gate_combo(s),
            "gate_r": {
                "symbol": s,
                "level": gate_combo(s)["r_level"],
                "reasons": [],
            },
            "last_deals": last_rows("deals", s, limit_deals),
            "last_risks": last_rows("risk_events", s, limit_risks),
            "join_check": {
                "symbol": s,
                "status": "not_implemented_in_dashboard_v4_1",
            },
            "kpis": institutional_kpis(s, n=n_gate),
        }

    portfolio_combo_level = "GREEN"
    levels = [gate_combo(s)["combo_level"] for s in syms]
    if any(lvl == "RED" for lvl in levels):
        portfolio_combo_level = "RED"
    elif any(lvl == "YELLOW" for lvl in levels):
        portfolio_combo_level = "YELLOW"

    portfolio = {
        "r": {
            "portfolio_level": portfolio_combo_level,
            "symbols": syms,
            "levels": {s: gate_combo(s)["r_level"] for s in syms},
            "reasons": [],
        },
        "combo": {
            "portfolio_level": portfolio_combo_level,
            "symbols": syms,
            "levels": {s: gate_combo(s)["combo_level"] for s in syms},
            "reasons": [],
        },
        "kpis": portfolio_institutional_kpis(syms, n=n_gate),
    }

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM ea_heartbeat")
        device_row = cur.fetchone()
        conn.close()

    return {
        "utc": now_utc_iso(),
        "symbols": syms,
        "signals": sigs,
        "devices": {
            "count": int(device_row["cnt"] or 0),
        },
        "portfolio": portfolio,
        "per_symbol": per_symbol,
        "note": "Institutional dashboard payload while preserving existing EA compatibility.",
    }
