# app.py
# FastAPI Signal Agent API
#
# HARD RULES:
# - Exactly ONE executable signal per symbol at a time
# - Exclusive claim: only one account/magic can receive a pending signal
# - ACK starts a hard 30-minute cooldown per symbol
# - During cooldown, ALL new TV signals for that symbol are ignored
# - Repeated / re-sent / new-id signals are ignored while pending or cooling down
# - Pending signals auto-expire if not ACKed in time (prevents deadlock)
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
# LOGIN ENDPOINTS:
#   POST /login
#   GET  /me
#
# START:
#   uvicorn app:app --host 0.0.0.0 --port 10000

from fastapi import FastAPI, HTTPException, Query, Depends, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from datetime import datetime, timezone, timedelta
from typing import Optional, Any
from jose import jwt, JWTError
import hashlib
import os
import sqlite3
import threading

app = FastAPI(title="Signal Agent API", version="2.2.0")

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
SECRET_KEY = os.getenv("SECRET_KEY", "claus-2026-xau-01!")
DB_PATH = os.getenv("DB_PATH", "signal_agent.db")

# Login / JWT
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
APP_USERNAME = os.getenv("APP_USERNAME", "admin")
APP_PASSWORD = os.getenv("APP_PASSWORD", "123456")

# hard rules
SYMBOL_COOLDOWN_MIN = int(os.getenv("SYMBOL_COOLDOWN_MIN", "30"))
CLAIM_TTL_SEC = int(os.getenv("CLAIM_TTL_SEC", "20"))
PENDING_TTL_SEC = int(os.getenv("PENDING_TTL_SEC", "120"))  # prevents deadlock
DEFAULT_GATE_LEVEL = os.getenv("DEFAULT_GATE_LEVEL", "GREEN").upper()

DB_LOCK = threading.Lock()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# -------------------------------------------------------------------
# MODELS
# -------------------------------------------------------------------
class TVSignal(BaseModel):
    key: str
    symbol: str
    action: str          # BUY / SELL
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

class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str

class UserResponse(BaseModel):
    username: str

# -------------------------------------------------------------------
# TIME / HELPERS
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
    left = int((future_dt - now_utc_dt()).total_seconds())
    return max(0, left)

def norm_symbol(raw: str) -> str:
    s = (raw or "").strip().upper()

    # gold aliases -> canonical XAUUSD
    if s in {"GOLD", "XAU", "XAUUSD", "OANDA:XAUUSD", "FOREXCOM:XAUUSD"}:
        return "XAUUSD"

    # btc aliases -> canonical BTCUSD
    if s in {"BTC", "BTCUSD", "BITCOIN", "COINBASE:BTCUSD", "BINANCE:BTCUSDT"}:
        return "BTCUSD"

    return s

def norm_action(a: str) -> str:
    return (a or "").strip().upper()

def payload_hash(symbol: str, action: str, tv_id: str, tv_ts: str) -> str:
    raw = f"{symbol}|{action}|{tv_id}|{tv_ts}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

# -------------------------------------------------------------------
# LOGIN / JWT HELPERS
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

def init_db() -> None:
    with DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS symbol_state (
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

            globally_acked INTEGER NOT NULL DEFAULT 0,

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
        INSERT INTO symbol_state (
            symbol,
            globally_acked,
            updated_utc
        ) VALUES (?, 0, ?)
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

def clear_pending(conn: sqlite3.Connection, symbol: str, reason: str) -> None:
    row = get_state(conn, symbol)
    if row is None:
        return

    if not row["pending_updated_utc"]:
        return

    log_signal(
        conn=conn,
        symbol=symbol,
        action=row["pending_action"],
        tv_id=row["pending_tv_id"],
        tv_ts=row["pending_tv_ts"],
        updated_utc=row["pending_updated_utc"],
        status="expired_pending",
        note=reason,
        phash=row["pending_payload_hash"],
    )

    cur = conn.cursor()
    cur.execute("""
        UPDATE symbol_state
        SET
            pending_action = NULL,
            pending_updated_utc = NULL,
            pending_tv_id = NULL,
            pending_tv_ts = NULL,
            pending_payload_hash = NULL,
            pending_created_utc = NULL,

            claimed_by_account = NULL,
            claimed_by_magic = NULL,
            claim_until_utc = NULL,

            globally_acked = 0,
            updated_utc = ?
        WHERE symbol = ?
    """, (now_utc_iso(), symbol))
    conn.commit()

def expire_pending_if_needed(conn: sqlite3.Connection, symbol: str) -> sqlite3.Row:
    row = upsert_empty_state(conn, symbol)

    if not row["pending_updated_utc"]:
        return row

    if int(row["globally_acked"] or 0) == 1:
        return row

    pending_created = parse_iso(row["pending_created_utc"])
    if pending_created is None:
        clear_pending(conn, symbol, "pending had no valid created timestamp")
        return get_state(conn, symbol)

    age_sec = int((now_utc_dt() - pending_created).total_seconds())
    if age_sec >= PENDING_TTL_SEC:
        clear_pending(conn, symbol, f"pending expired after {age_sec}s without ACK")
        return get_state(conn, symbol)

    return row

# -------------------------------------------------------------------
# ROOT
# -------------------------------------------------------------------
@app.get("/")
def root() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "Signal Agent API",
        "cooldown_min": SYMBOL_COOLDOWN_MIN,
        "claim_ttl_sec": CLAIM_TTL_SEC,
        "pending_ttl_sec": PENDING_TTL_SEC,
        "login_enabled": True
    }

# -------------------------------------------------------------------
# LOGIN
# -------------------------------------------------------------------
@app.post("/login", response_model=TokenResponse)
def login(data: LoginRequest) -> dict[str, str]:
    if not authenticate_user(data.username, data.password):
        raise HTTPException(
            status_code=401,
            detail="Benutzername oder Passwort falsch"
        )

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
        row = expire_pending_if_needed(conn, symbol)

        now = now_utc_dt()
        cooldown_until = parse_iso(row["cooldown_until_utc"])

        # hard cooldown: ignore absolutely everything during cooldown
        if cooldown_until and now < cooldown_until:
            left = secs_left(cooldown_until)
            log_signal(
                conn, symbol, action, tv_id, tv_ts, None,
                "ignored_cooldown",
                f"cooldown active, {left}s left",
                phash
            )
            conn.close()
            return {
                "status": "ignored_cooldown",
                "symbol": symbol,
                "action": action,
                "cooldown_left_sec": left
            }

        # if pending exists and is still valid, ignore all further TV events
        if row["pending_updated_utc"] and int(row["globally_acked"] or 0) == 0:
            age_sec = int((now - parse_iso(row["pending_created_utc"])).total_seconds()) if row["pending_created_utc"] else 0
            left = max(0, PENDING_TTL_SEC - age_sec)

            log_signal(
                conn, symbol, action, tv_id, tv_ts, row["pending_updated_utc"],
                "ignored_pending_exists",
                f"another signal is still pending, ttl_left={left}s",
                phash
            )
            conn.close()
            return {
                "status": "ignored_pending_exists",
                "symbol": symbol,
                "pending_updated_utc": row["pending_updated_utc"],
                "pending_ttl_left_sec": left
            }

        # duplicate exact payload guard
        if row["last_seen_payload_hash"] == phash:
            log_signal(
                conn, symbol, action, tv_id, tv_ts, None,
                "ignored_duplicate_payload",
                "same payload hash already seen",
                phash
            )
            conn.close()
            return {
                "status": "ignored_duplicate_payload",
                "symbol": symbol
            }

        updated_utc = now_utc_iso()

        cur = conn.cursor()
        cur.execute("""
            UPDATE symbol_state
            SET
                pending_action = ?,
                pending_updated_utc = ?,
                pending_tv_id = ?,
                pending_tv_ts = ?,
                pending_payload_hash = ?,
                pending_created_utc = ?,

                claimed_by_account = NULL,
                claimed_by_magic = NULL,
                claim_until_utc = NULL,

                globally_acked = 0,

                last_seen_tv_id = ?,
                last_seen_tv_ts = ?,
                last_seen_action = ?,
                last_seen_payload_hash = ?,

                updated_utc = ?
            WHERE symbol = ?
        """, (
            action,
            updated_utc,
            tv_id or None,
            tv_ts or None,
            phash,
            updated_utc,

            tv_id or None,
            tv_ts or None,
            action,
            phash,

            updated_utc,
            symbol
        ))
        conn.commit()

        log_signal(
            conn, symbol, action, tv_id, tv_ts, updated_utc,
            "accepted",
            "signal accepted as exclusive pending signal",
            phash
        )

        conn.close()
        return {
            "status": "accepted",
            "symbol": symbol,
            "action": action,
            "updated_utc": updated_utc
        }

# -------------------------------------------------------------------
# LATEST (exclusive claim)
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

    with DB_LOCK:
        conn = get_conn()
        row = expire_pending_if_needed(conn, sym)

        now = now_utc_dt()
        cooldown_until = parse_iso(row["cooldown_until_utc"])
        claim_until = parse_iso(row["claim_until_utc"])

        # if cooldown active: return null
        if cooldown_until and now < cooldown_until:
            left = secs_left(cooldown_until)
            conn.close()
            return {
                "symbol": sym,
                "signal": None,
                "updated_utc": "",
                "cooldown_left_sec": left
            }

        # if no pending signal: return null
        if not row["pending_updated_utc"] or int(row["globally_acked"] or 0) == 1:
            conn.close()
            return {
                "symbol": sym,
                "signal": None,
                "updated_utc": "",
                "cooldown_left_sec": 0
            }

        # another account/magic already holds a live claim
        if row["claimed_by_account"] and row["claimed_by_magic"] and claim_until and now < claim_until:
            if not (row["claimed_by_account"] == acc and row["claimed_by_magic"] == mag):
                conn.close()
                return {
                    "symbol": sym,
                    "signal": None,
                    "updated_utc": "",
                    "cooldown_left_sec": 0
                }

        # create / refresh claim for this requester
        claim_until_utc = (now + timedelta(seconds=CLAIM_TTL_SEC)).isoformat()
        cur = conn.cursor()
        cur.execute("""
            UPDATE symbol_state
            SET
                claimed_by_account = ?,
                claimed_by_magic = ?,
                claim_until_utc = ?,
                updated_utc = ?
            WHERE symbol = ?
        """, (
            acc or None,
            mag or None,
            claim_until_utc,
            now.isoformat(),
            sym
        ))
        conn.commit()

        action = row["pending_action"]
        updated_utc = row["pending_updated_utc"]

        conn.close()
        return {
            "symbol": sym,
            "updated_utc": updated_utc,
            "signal": {
                "symbol": sym,
                "action": action
            },
            "claimed_by_account": acc,
            "claimed_by_magic": mag,
            "claim_ttl_sec": CLAIM_TTL_SEC,
            "cooldown_left_sec": 0
        }

# -------------------------------------------------------------------
# ACK (global execute lock + cooldown start)
# -------------------------------------------------------------------
@app.post("/ack")
def ack(symbol: str,
        updated_utc: str,
        account: Optional[str] = Query(default=None),
        magic: Optional[str] = Query(default=None)) -> dict[str, Any]:
    sym = norm_symbol(symbol)
    acc = (account or "").strip()
    mag = (magic or "").strip()
    upd = (updated_utc or "").strip()

    if not sym or not upd:
        raise HTTPException(status_code=400, detail="symbol and updated_utc required")

    with DB_LOCK:
        conn = get_conn()
        row = expire_pending_if_needed(conn, sym)
        now = now_utc_dt()

        # duplicate ACK to already executed signal
        if row["last_executed_updated_utc"] == upd:
            cooldown_until = parse_iso(row["cooldown_until_utc"])
            left = secs_left(cooldown_until)
            conn.close()
            return {
                "status": "already_acked",
                "symbol": sym,
                "updated_utc": upd,
                "cooldown_left_sec": left
            }

        # must match current pending signal
        if row["pending_updated_utc"] != upd:
            conn.close()
            return {
                "status": "ignored_unknown_updated_utc",
                "symbol": sym,
                "updated_utc": upd
            }

        # only current claimer may ACK
        if row["claimed_by_account"] and row["claimed_by_magic"]:
            if row["claimed_by_account"] != acc or row["claimed_by_magic"] != mag:
                conn.close()
                return {
                    "status": "ignored_not_claimer",
                    "symbol": sym,
                    "updated_utc": upd
                }

        pending_action = row["pending_action"]
        pending_tv_id = row["pending_tv_id"]
        pending_tv_ts = row["pending_tv_ts"]
        pending_hash = row["pending_payload_hash"]

        cooldown_until_utc = (now + timedelta(minutes=SYMBOL_COOLDOWN_MIN)).isoformat()

        cur = conn.cursor()
        cur.execute("""
            UPDATE symbol_state
            SET
                globally_acked = 1,

                last_executed_updated_utc = ?,
                last_executed_action = ?,
                last_executed_utc = ?,

                cooldown_until_utc = ?,

                pending_action = NULL,
                pending_updated_utc = NULL,
                pending_tv_id = NULL,
                pending_tv_ts = NULL,
                pending_payload_hash = NULL,
                pending_created_utc = NULL,

                claimed_by_account = NULL,
                claimed_by_magic = NULL,
                claim_until_utc = NULL,

                updated_utc = ?
            WHERE symbol = ?
        """, (
            upd,
            pending_action,
            now.isoformat(),
            cooldown_until_utc,
            now.isoformat(),
            sym
        ))
        conn.commit()

        log_signal(
            conn,
            sym,
            pending_action,
            pending_tv_id,
            pending_tv_ts,
            upd,
            "acked_executed",
            f"executed by account={acc} magic={mag}; cooldown started",
            pending_hash
        )

        conn.close()
        return {
            "status": "acked",
            "symbol": sym,
            "updated_utc": upd,
            "cooldown_until_utc": cooldown_until_utc,
            "cooldown_min": SYMBOL_COOLDOWN_MIN
        }

# -------------------------------------------------------------------
# GATE STUB / COMPAT
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
# RISK CAPTURE / COMPAT
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
        row = expire_pending_if_needed(conn, sym)
        data = dict(row)
        conn.close()

    return {
        "symbol": sym,
        "state": data,
        "server_time_utc": now_utc_iso()
    }
