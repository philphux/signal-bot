"""
Global-Asset-Allocation-Strategie (Momentum) nach Meb Faber

• Universe : NQ=F, BTC=F, GC=F, CL=F, EEM, FEZ, IEF
• Momentum : Rendite 1 M + 3 M + 6 M + 9 M
• Filter   : Investition nur, wenn Schlusskurs > SMA150
• Rebalance: nach jedem Monatsende (TOP_N Positionen werden gehalten)
"""

from __future__ import annotations

import os
import time
from typing import List, Tuple

import pandas as pd
import yahooquery as yq

# ---------------------------------------------------------------------------#
# Parameter                                                                   #
# ---------------------------------------------------------------------------#
TICKERS      = ["NQ=F", "BTC=F", "GC=F", "CL=F", "EEM", "FEZ", "IEF"]
TOP_N        = 3               # max. gehaltene Assets
SMA_WINDOW   = 150             # SMA-Länge
HISTORY_FILE = "gaa_history.csv"
RETRY        = 3               # Yahoo-Retry-Versuche

# ---------------------------------------------------------------------------#
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    """
    Liefert höchstens *eine* Meldung pro Kalendermonat:
    (subject, subject2, text) oder (None, None, None)
    """
    today = pd.Timestamp.utcnow().normalize()

    # 1) Kursdaten ----------------------------------------------------------
    for attempt in range(1, RETRY + 1):
        try:
            raw = (
                yq.Ticker(" ".join(TICKERS))
                .history(period="2y", interval="1d")["close"]
                .unstack(level=0)
                .sort_index()
            )
            if not raw.empty:
                break
        except Exception as exc:
            print(f"[{attempt}/{RETRY}] Yahoo-Query error: {exc}")
            time.sleep(2)
    else:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    # Index sauber auf Timestamps umstellen
    raw.index = pd.to_datetime(raw.index)
    hist = raw

    # 2) Letzte *vollendete* Monatskerze ------------------------------------
    month_ends = hist.index.to_period("M").unique().to_timestamp("M")
    month_end = month_ends[-1]
    if (today.year, today.month) == (month_end.year, month_end.month):
        # aktuell laufender Monat – nimm den vorigen
        if len(month_ends) < 2:
            return None, None, None
        month_end = month_ends[-2]

    # 3) Schon verbucht? ----------------------------------------------------
    last_positions: List[str] = []
    if os.path.exists(HISTORY_FILE):
        last_line = open(HISTORY_FILE).read().strip().split(";")
        if last_line and pd.to_datetime(last_line[0]) == month_end:
            return None, None, None
        if len(last_line) > 1:
            last_positions = [t for t in last_line[1].split(",") if t]

    # 4) Momentum-Score -----------------------------------------------------
    monthly = hist.resample("M").last()
    r1, r3, r6, r9 = (monthly.pct_change(n).iloc[-1] for n in (1, 3, 6, 9))
    score   = (r1 + r3 + r6 + r9).dropna()

    sma150  = hist.rolling(SMA_WINDOW).mean().loc[month_end]
    eligible = score.index[hist.loc[month_end, score.index] > sma150[score.index]]
    score    = score.loc[eligible]

    top = score.sort_values(ascending=False).head(TOP_N)
    new_positions = list(top.index)

    # 5) History sichern ----------------------------------------------------
    with open(HISTORY_FILE, "a") as f:
        f.write(f"{month_end:%Y-%m-%d};{','.join(new_positions)}\n")

    # 6) Meldung bauen ------------------------------------------------------
    if set(new_positions) == set(last_positions):
        return (f"GAA – keine Änderung ({month_end:%b %Y})",
                "",
                f"Portfolio unverändert: {', '.join(new_positions) or 'Cash'}")

    lines: List[str] = []
    if (added := set(new_positions) - set(last_positions)):
        lines.append("Neu kaufen: " + ", ".join(sorted(added)))
    if (sold := set(last_positions) - set(new_positions)):
        lines.append("Verkaufen: " + ", ".join(sorted(sold)))
    lines.append("Aktuelles Portfolio: " +
                 (", ".join(new_positions) if new_positions else "Cash"))
    lines.append("")
    lines.append("Momentum-Scores:")
    for ticker, sc in top.items():
        lines.append(f"{ticker}: {sc:+.2%}")

    return f"GAA Rebalance ({month_end:%b %Y})", "", "\n".join(lines)
