from fastapi import FastAPI, HTTPException, Query, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from datetime import datetime, timezone, timedelta
from typing import Optional, Any
from jose import jwt, JWTError
import os
import sqlite3
import threading

app = FastAPI(title="Signal Agent API", version="3.6.0")

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

class ControlActionRequest(BaseModel):
    symbol: Optional[str] = None
    note: Optional[str] = None

class ControlRiskFactorRequest(BaseModel):
    symbol: Optional[str] = None
    risk_factor: float
    note: Optional[str] = None

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
    if s in {"GOLD", "XAU", "XAUUSD", "OANDA:XAUUSD", "FOREXCOM:XAUUSD"}:
        return "XAUUSD"
    if s in {"BTC", "BTCUSD", "BITCOIN", "COINBASE:BTCUSD", "BINANCE:BTCUSDT"}:
        return "BTCUSD"
    return s

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
        "mode": "dashboard_backend",
        "login_enabled": True,
        "heartbeat_timeout_sec": HEARTBEAT_TIMEOUT_SEC
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
# DEBUG STATE
# -------------------------------------------------------------------
@app.get("/debug/state")
def debug_state(symbol: str) -> dict[str, Any]:
    sym = norm_symbol(symbol)

    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM debug_state WHERE symbol = ?", (sym,))
        row = cur.fetchone()
        conn.close()

    if row is None:
        return {
            "symbol": sym,
            "state": {
                "symbol": sym,
                "active_action": None,
                "updated_utc": None,
                "note": "no signal yet"
            },
            "deliveries": [],
            "server_time_utc": now_utc_iso()
        }

    return {
        "symbol": sym,
        "state": {
            "symbol": sym,
            "active_action": row["active_action"],
            "updated_utc": row["updated_utc"],
            "note": row["note"],
        },
        "deliveries": [],
        "server_time_utc": now_utc_iso()
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
