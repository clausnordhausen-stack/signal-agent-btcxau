from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
from typing import Optional

app = FastAPI()

# -----------------------------
# CORS (für Flutter Web)
# -----------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Models
# -----------------------------

class LoginRequest(BaseModel):
    email: str
    password: str


class Signal(BaseModel):
    symbol: str
    side: str
    price: float
    timeframe: Optional[str] = None
    timestamp: Optional[str] = None


# -----------------------------
# Root Endpoint
# -----------------------------

@app.get("/")
def root():
    return {
        "service": "signal-agent-api",
        "status": "running",
        "time": datetime.utcnow().isoformat()
    }


# -----------------------------
# Health Check
# -----------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat()
    }


# -----------------------------
# Login Endpoint (Flutter)
# -----------------------------

@app.post("/login")
async def login(data: LoginRequest):

    email = data.email.strip()
    password = data.password.strip()

    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password required")

    # Demo Login
    if email == "test@test.com" and password == "123456":

        return {
            "success": True,
            "token": "demo_token_123",
            "user": {
                "id": "1",
                "email": email
            }
        }

    raise HTTPException(status_code=401, detail="Invalid credentials")


# -----------------------------
# Trading Signal Endpoint
# -----------------------------

@app.post("/signal")
async def receive_signal(signal: Signal):

    print("----- SIGNAL RECEIVED -----")
    print("Symbol:", signal.symbol)
    print("Side:", signal.side)
    print("Price:", signal.price)
    print("Timeframe:", signal.timeframe)
    print("Timestamp:", signal.timestamp)

    return {
        "status": "received",
        "symbol": signal.symbol,
        "side": signal.side,
        "price": signal.price
    }


# -----------------------------
# TradingView Webhook
# -----------------------------

@app.post("/webhook")
@app.post("/webhook/")
async def webhook(request: Request):

    data = await request.json()

    print("----- WEBHOOK RECEIVED -----")
    print(data)

    return {
        "status": "ok",
        "received": data,
        "timestamp": datetime.utcnow().isoformat()
    }
