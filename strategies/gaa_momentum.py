"""
Global-Asset-Allocation-Strategie (Momentum) nach Meb Faber.

- Universe: NQ=F, BTC=F, GC=F, CL=F, EEM, FEZ, IEF
- Momentum-Score = Rendite 1 M + 3 M + 6 M + 9 M (jeweils in %)
- Investiert wird nur, wenn Schlusskurs > SMA150
- An jedem Monatsende werden die 3 besten (TOP_N) Werte gleichgewichtet gekauft
  bzw. gehalten.
- Rebalancing monatlich, Haltedauer mindestens 1 Monat.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import yahooquery as yq

# ---------------------------------------------------------------------------#
# Parameter                                                                   #
# ---------------------------------------------------------------------------#
TICKERS: List[str] = [
    "NQ=F",   # Nasdaq-100 Futures (continuous)
    "BTC=F",  # Bitcoin Futures (CME)
    "GC=F",   # Gold Futures
    "CL=F",   # Crude-Oil Futures (WTI)
    "EEM",    # iShares MSCI Emerging Markets
    "FEZ",    # SPDR Euro Stoxx 50
    "IEF",    # iShares 7-10 y Treasury
]

TOP_N: int = 3          # Anzahl der zu kaufenden Assets
SMA_WINDOW: int = 150   # SMA-Länge
HISTORY_FILE: str = "gaa_history.csv"
RETRY: int = 3          # Yahoo-Query-Versuche

# ---------------------------------------------------------------------------#
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    """Wird täglich aufgerufen, gibt aber nur *einmal* pro Monat eine Meldung
    zurück (nach Abschluss des Kalendermonats)."""
    today_utc = pd.Timestamp.utcnow().normalize()

    # Kursdaten laden -------------------------------------------------------
    for attempt in range(1, RETRY + 1):
        try:
            tq = yq.Ticker(" ".join(TICKERS))
            raw = tq.history(period="2y", interval="1d")
            if not raw.empty:
                break
        except Exception as exc:
            print(f"[{attempt}/{RETRY}] Yahoo-Fehler: {exc}")
            time.sleep(2)
    else:
        return "GAA-Fehler", None, "Kursdaten konnten nicht geladen werden."

    close = raw["close"].unstack(level=0).dropna(how="all").sort_index()

    # Letzter Tag des *abgeschlossenen* Monats -----------------------------
    month_ends = close.index.to_period("M").unique().to_timestamp("M")
    month_end = month_ends[-1]

    # Wenn wir noch im selben Monat sind → nichts tun
    if (today_utc.year, today_utc.month) == (month_end.year, month_end.month):
        return None, None, None

    # Schon verarbeitet? ----------------------------------------------------
    if os.path.exists(HISTORY_FILE):
        last_line = open(HISTORY_FILE, "r", encoding="utf-8").read().strip().split(";")
        if last_line and pd.to_datetime(last_line[0]) == month_end:
            return None, None, None
        last_positions = last_line[1].split(",") if len(last_line) > 1 else []
    else:
        last_positions: List[str] = []

    # SMA150 & Preise am Monatsende ----------------------------------------
    sma150 = close.rolling(SMA_WINDOW).mean().loc[month_end]
    prices = close.loc[month_end]

    # Momentum-Score --------------------------------------------------------
    monthly = close.resample("M").last()
    r1 = monthly.pct_change(1).iloc[-1]
    r3 = monthly.pct_change(3).iloc[-1]
    r6 = monthly.pct_change(6).iloc[-1]
    r9 = monthly.pct_change(9).iloc[-1]
    momentum = (r1 + r3 + r6 + r9).dropna()

    # Nur Werte über SMA150 zulassen
    eligible = momentum.index[prices[momentum.index] > sma150[momentum.index]]
    momentum = momentum.loc[eligible]

    top = momentum.sort_values(ascending=False).head(TOP_N)
    new_positions = list(top.index)

    # History schreiben -----------------------------------------------------
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(f"{month_end:%Y-%m-%d};{','.join(new_positions)}\n")

    # Nachricht bauen -------------------------------------------------------
    if set(new_positions) == set(last_positions):
        subject = f"GAA – keine Änderung ({month_end:%b %Y})"
        text = f"Portfolio unverändert: {', '.join(new_positions) or 'Cash'}"
        return subject, "", text

    moved_in = set(new_positions) - set(last_positions)
    moved_out = set(last_positions) - set(new_positions)

    subject = f"GAA Rebalance ({month_end:%b %Y})"
    lines: List[str] = []
    if moved_in:
        lines.append("Neu kaufen: " + ", ".join(sorted(moved_in)))
    if moved_out:
        lines.append("Verkaufen: " + ", ".join(sorted(moved_out)))
    lines.append("Aktuelles Portfolio: " + (", ".join(new_positions) if new_positions else "→ Cash"))
    lines.append("")
    lines.append("Momentum-Scores (Stand Monatsschluss):")
    for ticker, score in top.items():
        lines.append(f"{ticker}: {score:+.2%}")

    return subject, "", "\n".join(lines)
