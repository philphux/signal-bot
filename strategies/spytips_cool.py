"""
SPY‑/TIPS‑Spread‑Strategie  –  tägliche Meldung, History bei Signalwechsel

• Signal  : BUY  wenn Momentum SPY > TIPS, sonst SELL
• History : Eine Zeile pro Tag *falls* Signal für den Tag neu ist
• Discord  :
    ‑ Änderung → Subjekt "GO BUY/SELL NOW …"
    ‑ Unverändert → Subjekt "Signal BUY/SELL (unverändert)"
"""

from __future__ import annotations
from datetime import datetime, timezone
import os
import yahooquery as yq

SPY, TIPS = "SPY", "TIP"
HIST_FILE = "spy_tips_history.csv"
COOLDOWN_DAYS = 0

# ---------------------------------------------------------------------- #
def _momentum(ticker: str) -> float:
    hist = yq.Ticker(ticker).history(period="1y")["close"]
    return (hist.iloc[-1] / hist.iloc[0] - 1) * 100   # Prozent

# ---------------------------------------------------------------------- #
def spy_tips_cool():
    spy_mom  = _momentum(SPY)
    tips_mom = _momentum(TIPS)
    diff_pct = spy_mom - tips_mom
    signal   = "BUY" if diff_pct > 0 else "SELL"

    today = datetime.now(timezone.utc).date()
    last_sig = None

    # --- History lesen ----------------------------------------------------
    history_exists = os.path.exists(HIST_FILE) and os.path.getsize(HIST_FILE)
    if history_exists:
        last_line  = open(HIST_FILE).read().strip().splitlines()[-1]
        last_date, last_sig, *_ = last_line.split(",")

    # --- History schreiben, wenn für heute noch nicht vorhanden ----------
    if (not history_exists) or last_date != today.isoformat():
        header_needed = not history_exists
        with open(HIST_FILE, "a") as f:
            if header_needed:
                f.write("date,signal,diff_pct,spy_mom,tips_mom\n")
            f.write(f"{today},{signal},{diff_pct:.2f},{spy_mom:.2f},{tips_mom:.2f}\n")

    # --- Subjekt ----------------------------------------------------------
    if last_sig is None or last_sig != signal:
        subj = f"GO {signal} NOW (cooldown activated for {COOLDOWN_DAYS} days)"
    else:
        subj = f"Signal {signal} (unverändert)"

    # --- Nachrichtentext --------------------------------------------------
    body = [
        f"Currently in market ({COOLDOWN_DAYS} cooldown days remaining)",
        f"The SIGNAL is {signal}",
        f"The SPY signal is {'BUY' if spy_mom>0 else 'SELL'} "
        f"with a difference of {abs(spy_mom):.2f}%",
        f"The TIPS signal is {'BUY' if tips_mom>0 else 'SELL'} "
        f"with a difference of {abs(tips_mom):.2f}%",
        "",
        f"Spread (SPY‑TIPS): {diff_pct:+.2f} %",
        "",
        f"Last update: {datetime.utcnow():%Y-%m-%d %H:%M UTC}",
    ]

    return subj, "", "\n".join(body)
