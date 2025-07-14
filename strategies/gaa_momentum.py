"""
Global-Asset-Allocation-Strategie (Momentum) nach Meb Faber
(Universum: NQ=F, BTC=F, GC=F, CL=F, EEM, FEZ, IEF)
"""

from __future__ import annotations
import os, time
from typing import List, Tuple
import pandas as pd
import yahooquery as yq

TICKERS      = ["NQ=F", "BTC=F", "GC=F", "CL=F", "EEM", "FEZ", "IEF"]
TOP_N        = 3
SMA_WINDOW   = 150
HISTORY_FILE = "gaa_history.csv"
RETRY        = 3

# ---------------------------------------------------------------------------
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    """Erzeugt maximal *ein* Rebalance-Signal pro Kalendermonat."""
    today = pd.Timestamp.utcnow().normalize()

    # 1) Kursdaten ----------------------------------------------------------
    for i in range(RETRY):
        try:
            hist = (
                yq.Ticker(" ".join(TICKERS))
                .history(period="2y", interval="1d")["close"]
                .unstack(level=0)
                .sort_index()
            )
            if not hist.empty:
                break
        except Exception as e:
            print(f"[{i+1}/{RETRY}] Yahoo-Query error: {e}")
            time.sleep(2)
    else:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    # **Fix**: Index auf echte Timestamps umstellen
    hist.index = pd.to_datetime(hist.index)

    # 2) Zuletzt *vollendeten* Monat bestimmen -----------------------------
    month_ends = hist.index.to_period("M").unique().to_timestamp("M")
    month_end = month_ends[-1]
    if (today.year, today.month) == (month_end.year, month_end.month):
        if len(month_ends) < 2:
            return None, None, None
        month_end = month_ends[-2]

    # Bereits verbucht?
    last_pos: List[str] = []
    if os.path.exists(HISTORY_FILE):
        last_line = open(HISTORY_FILE).read().strip().split(";")
        if last_line and pd.to_datetime(last_line[0]) == month_end:
            return None, None, None
        if len(last_line) > 1:
            last_pos = last_line[1].split(",")

    # 3) Momentum-Score -----------------------------------------------------
    monthly = hist.resample("M").last()
    r1, r3, r6, r9 = (monthly.pct_change(n).iloc[-1] for n in (1, 3, 6, 9))
    score = (r1 + r3 + r6 + r9).dropna()

    sma150 = hist.rolling(SMA_WINDOW).mean().loc[month_end]
    elig   = score.index[hist.loc[month_end, score.index] > sma150[score.index]]
    score  = score.loc[elig]

    top = score.sort_values(ascending=False).head(TOP_N)
    new_pos = list(top.index)

    # 4) Verlauf sichern ----------------------------------------------------
    with open(HISTORY_FILE, "a") as f:
        f.write(f"{month_end:%Y-%m-%d};{','.join(new_pos)}\n")

    # 5) Meldung bauen ------------------------------------------------------
    if set(new_pos) == set(last_pos):
        return (f"GAA – keine Änderung ({month_end:%b %Y})",
                "",
                f"Portfolio unverändert: {', '.join(new_pos) or 'Cash'}")

    lines: List[str] = []
    if (added := set(new_pos) - set(last_pos)):
        lines.append("Neu kaufen: " + ", ".join(sorted(added)))
    if (sold := set(last_pos) - set(new_pos)):
        lines.append("Verkaufen: " + ", ".join(sorted(sold)))
    lines.append("Aktuelles Portfolio: " +
                 (", ".join(new_pos) if new_pos else "Cash"))
    lines.append("")
    lines.append("Momentum-Scores:")
    for t, s in top.items():
        lines.append(f"{t}: {s:+.2%}")

    return f"GAA Rebalance ({month_end:%b %Y})", "", "\n".join(lines)
