"""
SPY‑/TIPS‑Spread‑Strategie
Sendet NICHTS, wenn sich gegenüber dem Vortag weder Signal noch Spread geändert haben
(z. B. Börsen‑Wochenende mit identischen Schlusskursen).

• History  : Eine Zeile pro Kalendertag (Signal + Spread), wenn noch nicht vorhanden.
• Discord  : Nachricht nur, wenn
               – Signal BUY ↔ SELL wechselt  ODER
               – Spread (auf 2 Nachkommastellen) sich geändert hat.
"""

from __future__ import annotations
from datetime import datetime, timezone
import os
import yahooquery as yq
from math import isclose

SPY, TIPS = "SPY", "TIP"
HIST_FILE = "spy_tips_history.csv"
COOLDOWN_DAYS = 0
TOL = 0.005          # ±0.005 %, um Rundungsrauschen abzufangen

# ─────────────────────────────────────────────────────────────── #
def _momentum(ticker: str) -> float:
    hist = yq.Ticker(ticker).history(period="1y")["close"]
    return (hist.iloc[-1] / hist.iloc[0] - 1) * 100   # Prozent

# ─────────────────────────────────────────────────────────────── #
def spy_tips_cool():
    spy_mom  = _momentum(SPY)
    tips_mom = _momentum(TIPS)
    diff_pct = spy_mom - tips_mom
    signal   = "BUY" if diff_pct > 0 else "SELL"

    today = datetime.now(timezone.utc).date()

    # -------- History lesen ------------------------------------
    last_sig = last_diff = None
    if os.path.exists(HIST_FILE) and os.path.getsize(HIST_FILE):
        last_line  = open(HIST_FILE).read().strip().splitlines()[-1]
        last_date, last_sig, last_diff, *_ = last_line.split(",")
        last_diff = float(last_diff)

    # -------- History schreiben (einmal pro Tag) ----------------
    if (last_sig is None) or (last_date != today.isoformat()):
        first_line = not os.path.exists(HIST_FILE)
        with open(HIST_FILE, "a") as f:
            if first_line:
                f.write("date,signal,diff_pct,spy_mom,tips_mom\n")
            f.write(f"{today},{signal},{diff_pct:.2f},{spy_mom:.2f},{tips_mom:.2f}\n")

    # -------- Entscheiden, ob wir senden ------------------------
    must_send = False
    if last_sig is None or last_sig != signal:
        must_send = True                         # Signalwechsel
    elif not isclose(diff_pct, last_diff or 0, abs_tol=TOL):
        must_send = True                         # Spread bewegt

    if not must_send:
        return None, None, None                 # nichts an Discord schicken

    # -------- Nachricht zusammenstellen -------------------------
    subj = (
        f"GO {signal} NOW (cooldown activated for {COOLDOWN_DAYS} days)"
        if last_sig != signal
        else f"Signal {signal}"
    )

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
