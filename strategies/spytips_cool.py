"""
SPY‑TIPS‑Strategie – kompakte Ausgabe
"""

from __future__ import annotations
from datetime import datetime
import yahooquery as yq

SPY, TIPS = "SPY", "TIP"
COOLDOWN_DAYS = 0

def _momentum(ticker: str) -> float:
    hist = yq.Ticker(ticker).history(period="1y")["close"]
    return (hist.iloc[-1] / hist.iloc[0] - 1) * 100

def spy_tips_cool():
    spy_mom  = _momentum(SPY)
    tips_mom = _momentum(TIPS)
    diff     = spy_mom - tips_mom
    signal   = "BUY" if diff > 0 else "SELL"

    subj = f"GO {signal} NOW (cooldown activated for {COOLDOWN_DAYS} days)"
    body = [
        f"Currently in market ({COOLDOWN_DAYS} cooldown days remaining)",
        f"The SIGNAL is {signal}",
        f"The SPY signal is {signal} with a difference of {abs(diff):0.2f}%",
        f"The TIPS signal is {'BUY' if tips_mom>0 else 'SELL'} "
        f"with a difference of {abs(tips_mom):0.2f}%",
        "",
        f"Last update: {datetime.utcnow():%Y-%m-%d %H:%M UTC}",
    ]
    return subj, "", "\n".join(body)
