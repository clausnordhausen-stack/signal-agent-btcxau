from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone

app = FastAPI()

SECRET = "claus-2026-xau-01!"  #

class TVSignal(BaseModel):
    key: str
    symbol: str
    action: str   # BUY / SELL
    ts: str | None = None
    id: str | None = None

LAST = {"signal": None, "updated_utc": None}

@app.get("/")
def root():
    return {"status": "Signal Agent API is running"}

@app.post("/tv")
def tv_webhook(sig: TVSignal):
    if sig.key != SECRET:
        raise HTTPException(status_code=401, detail="bad key")

    act = sig.action.upper().strip()
    if act not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="bad action")

    LAST["signal"] = {
        "symbol": sig.symbol.strip(),
        "action": act,
        "ts": sig.ts,
        "id": sig.id
    }
    LAST["updated_utc"] = datetime.now(timezone.utc).isoformat()
    return {"ok": True}

@app.get("/latest")
def latest():
    return {"signal": LAST["signal"], "updated_utc": LAST["updated_utc"]}
