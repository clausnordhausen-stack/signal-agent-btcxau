from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime

app = FastAPI()

# Allow frontend access
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

class Signal(BaseModel):
    symbol: str
    side: str
    price: float
    timestamp: str | None = None


# -----------------------------
# Root endpoint
# -----------------------------

@app.get("/")
def root():
    return {
        "status": "running",
        "service": "signal-agent-api",
        "timestamp": datetime.utcnow()
    }


# -----------------------------
# Health check
# -----------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "time": datetime.utcnow()
    }


# -----------------------------
# Receive trading signals
# -----------------------------

@app.post("/signal")
async def receive_signal(signal: Signal):

    print("----- SIGNAL RECEIVED -----")
    print(f"Symbol: {signal.symbol}")
    print(f"Side: {signal.side}")
    print(f"Price: {signal.price}")
    print(f"Time: {signal.timestamp}")

    return {
        "status": "received",
        "symbol": signal.symbol,
        "side": signal.side,
        "price": signal.price
    }


# -----------------------------
# Generic webhook endpoint
# -----------------------------

@app.post("/webhook")
async def webhook(request: Request):

    data = await request.json()

    print("----- WEBHOOK RECEIVED -----")
    print(data)

    return {
        "status": "ok",
        "data": data
    }
