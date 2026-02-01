from fastapi import FastAPI, Query
from datetime import datetime, timezone

app = FastAPI(title="Signal Agent API", version="1.0")

@app.get("/")
def root():
    return {"message": "Signal Agent API is running"}

@app.get("/v1/signal")
def get_signal(symbol: str = Query(..., description="BTCUSD or XAUUSD")):
    symbol = symbol.upper().strip()
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    if symbol not in ("BTCUSD", "XAUUSD"):
        return {
            "schema": "signal_v1_2",
            "trade_allowed": False,
            "error": "unsupported_symbol",
            "supported": ["BTCUSD", "XAUUSD"]
        }

    if symbol == "BTCUSD":
        return {
            "schema": "signal_v1_2",
            "signal_id": "demo-btcusd-001",
            "symbol": "BTCUSD",
            "generated_at_utc": now,
            "valid_until_utc": now,
            "trade_allowed": True,
            "selected_module": "ORB_BREAKOUT",
            "oco_group": "DEMO_ORB_BTCUSD",
            "orders": [
                {
                    "type": "BUYSTOP",
                    "price": 0.0,
                    "sl_price": 0.0,
                    "tp1_price": 0.0,
                    "tp2_price": 0.0,
                    "expiration_utc": now
                },
                {
                    "type": "SELLSTOP",
                    "price": 0.0,
                    "sl_price": 0.0,
                    "tp1_price": 0.0,
                    "tp2_price": 0.0,
                    "expiration_utc": now
                }
            ],
            "constraints": {
                "max_spread_points": 0,
                "max_slippage_points": 0,
                "place_policy": "PLACE_ON_NEWBAR"
            },
            "risk": {
                "max_risk_usd": 10.0,
                "max_trades_today": 2
            },
            "trail": {
                "enabled": True,
                "mode": "atr",
                "atr_mult": 2.0
            },
            "reason": "Demo ORB BTCUSD (Platzhalter, Preise = 0)"
        }

    # XAUUSD
    return {
        "schema": "signal_v1_2",
        "signal_id": "demo-xauusd-001",
        "symbol": "XAUUSD",
        "generated_at_utc": now,
        "valid_until_utc": now,
        "trade_allowed": True,
        "selected_module": "TREND_PULLBACK",
        "orders": [
            {
                "type": "BUYLIMIT",
                "price": 0.0,
                "sl_price": 0.0,
                "tp1_price": 0.0,
                "tp2_price": 0.0,
                "expiration_utc": now
            }
        ],
        "constraints": {
            "max_spread_points": 0,
            "max_slippage_points": 0,
            "place_policy": "PLACE_ON_NEWBAR"
        },
        "risk": {
            "max_risk_usd": 10.0,
            "max_trades_today": 2
        },
        "trail": {
            "enabled": True,
            "mode": "atr",
            "atr_mult": 2.0
        },
        "reason": "Demo Trend Pullback XAUUSD (Platzhalter, Preise = 0)"
    }
