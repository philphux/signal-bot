"""
SPY‑/TIPS‑Spread‑Strategie  –  tägliche Meldung + schlanke History

Signal : BUY  wenn SPY‑Momentum > TIPS‑Momentum, sonst SELL
History: 1 Zeile pro Tag (nur wenn neues oder geändertes Signal)

Ausgabe (Discord):
  • GO BUY/SELL NOW …
  • Cooldown‑Info (hier Platzhalter = 0 Tage)
  • Momentum‑Differenzen
  • Zeitstempel (UTC)
"""

from __future__ import annotations
from datetime import datetime, timezone
import os
import yahooquery as yq

SPY, TIPS = "SPY", "TIP"
HIST_FILE = "spy_tips_history.csv"
COOLDOWN_DAYS = 0

# ───────────────────────────────────────────────────────────────────────── #
def _momentum(ticker: str) -> float:
    """
    Einfache 12‑Monats‑Momentum‑Kennzahl in %.
    Änderung hier möglich, ohne restlichen Code anzupassen.
    """
    hist = yq.Ticker(ticker).history(period="1y")["close"]
    return (hist.iloc[-1] / hist.iloc[0] - 1) * 100


# ───────────────────────────────────────────────────────────────────────── #
def spy_tips_cool():
    spy_mom  = _momentum(SPY)
    tips_mom = _momentum(TIPS)
    diff_pct = spy_mom - tips_mom
    signal   = "BUY" if diff_pct > 0 else "SELL"

    today = datetime.now(timezone.utc).date()
    write_history = True

    # History lesen
    if os.path.exists(HIST_FILE) and os.path.getsize(HIST_FILE):
        last_line  = open(HIST_FILE).read().strip().splitlines()[-1]
        last_date, last_sig, *_ = last_line.split(",")
        if last_date == today.isoformat() and last_sig == signal:
            write_history = False   # gleicher Tag & Signal → keine neue Zeile

    # History ggf. anlegen / erweitern
    if write_history:
        header_needed = not os.path.exists(HIST_FILE)
        with open(HIST_FILE, "a") as f:
            if header_needed:
                f.write("date,signal,diff_pct,spy_mom,tips_mom\n")
            f.write(f"{today},{signal},{diff_pct:.2f},{spy_mom:.2f},{tips_mom:.2f}\n")

    # ------------------------------------------------------------------ #
    # Discord‑Nachricht (immer)
    subj = f"GO {signal} NOW (cooldown activated for {COOLDOWN_DAYS} days)"
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
