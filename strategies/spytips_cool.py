"""
SPY-TIPS-Strategie – kompakte Ausgabe

liefert (subject, subject2, body)
subject  : Headline inkl. Action und Cool-Down
subject2 : bleibt leer (Reserviert)
body     : maximal 4-5 Zeilen Kerndaten
"""

from __future__ import annotations
from datetime import datetime
import pandas as pd
import yahooquery as yq

# ---------------------------------------------------------------------------#
# Parameter, Datenquellen
SPY  = "SPY"
TIPS = "TIP"
COOLDOWN_DAYS = 0          # hier ggf. Logik für Cool-Down einbauen
# ---------------------------------------------------------------------------#
def _momentum(ticker: str) -> float:
    """einfaches 200-Tage-Momentum als Beispiel"""
    hist = yq.Ticker(ticker).history(period="1y")["close"]
    return (hist.iloc[-1] / hist.iloc[0] - 1) * 100

# ---------------------------------------------------------------------------#
def spy_tips_cool():
    """liefert Kurzmeldung oder (None,None,None)"""
    # Signallogik (Beispiel)
    spy_mom  = _momentum(SPY)
    tips_mom = _momentum(TIPS)
    diff     = spy_mom - tips_mom
    signal   = "BUY" if diff > 0 else "SELL"

    subject = f"GO {signal} NOW (cooldown activated for {COOLDOWN_DAYS} days)"

    body = [
        f"Currently in market ({COOLDOWN_DAYS} cooldown days remaining)",
        f"The SIGNAL is {signal}",
        f"The SPY signal is {signal} with a difference of {diff:0.2f}%",
        f"The TIPS signal is {'BUY' if tips_mom>0 else 'SELL'} "
        f"with a difference of {abs(tips_mom):0.2f}%",
        "",
        f"Last update: {datetime.utcnow():%Y-%m-%d %H:%M UTC}",
    ]

    return subject, "", "\n".join(body)
