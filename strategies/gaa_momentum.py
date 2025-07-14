# strategies/gaa_momentum.py
"""Global Asset Allocation‑Strategie nach Meb Faber

‑ Universe: NQ=F, BTC=F, GC=F, CL=F, EEM, FEZ, IEF
‑ Momentum‑Score = 1‑Monats‑Rendite + 3M + 6M + 9M (jeweils in Prozent)
‑ Es wird nur investiert, wenn der Schlusskurs > SMA150 liegt.
‑ Am Monatsende werden die drei stärksten Assets (nach Score) gleichgewichtet
  gekauft bzw. beibehalten.
‑ Mindestens ein Monat Haltedauer – Re‑Balancing an jedem Monatswechsel.
‑ Läuft als einzelne Strategie‑Funktion für den signal‑bot und liefert wie
  gewohnt (subject, subject2, text) oder (None, None, None).
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import yahooquery as yq

# ---------------------------------------------------------------------------
# Parameter
# ---------------------------------------------------------------------------
TICKERS: List[str] = [
    "NQ=F",  # Nasdaq‑100 Futures (Continous)
    "BTC=F", # Bitcoin‑Futures (CME)
    "GC=F",  # Gold Futures
    "CL=F",  # Crude‑Oil Futures (WTI)
    "EEM",   # iShares MSCI Emerging Markets ETF
    "FEZ",   # SPDR Euro Stoxx 50 ETF
    "IEF",   # iShares 7‑10y Treasury ETF
]

TOP_N: int = 3          # wie viele Assets maximal gekauft werden
SMA_WINDOW: int = 150   # SMA‑Filter (handelt nur > SMA150)
HISTORY_FILE: str = "gaa_history.csv"  # Verlauf    (YYYY‑MM‑DD;T1,T2,T3)
RETRY: int = 3          # Download‑Versuche, falls Yahoo zickt

# ---------------------------------------------------------------------------
# Kernfunktion
# ---------------------------------------------------------------------------

def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    """Wird vom Bot täglich aufgerufen, liefert aber nur *einmal* pro Monat
    eine Nachricht (jeweils nach Abschluss des Kalendermonats)."""

    # --- 0) Datum / letzte Monatskerze bestimmen ---------------------------
    today_utc = pd.Timestamp.utcnow().normalize()

    # Die Daten reichen 2 Jahre zurück (>= 9 Monate + SMA150)
    for attempt in range(1, RETRY + 1):
        try:
            tq = yq.Ticker(" ".join(TICKERS))
            raw = tq.history(period="2y", interval="1d")
            if not raw.empty:
                break
        except Exception as exc:  # pragma: no cover – reines Logging
            print(f"[{attempt}/{RETRY}] Yahoo‑Fehler: {exc}")
            time.sleep(2)
    else:
        return "GAA‑Fehler", None, "Kursdaten konnten nicht geladen werden."

    close = raw["close"].unstack(level=0).dropna(how="all").sort_index()

    # Letzter Börsentag des *abgeschlossenen* Kalendermonats
    month_ends = close.index.to_period("M").unique().to_timestamp("M")
    month_end = month_ends[-1]

    # Wenn heute noch *im* selben Monat wie month_end, haben wir den Monat
    # noch nicht abgeschlossen → nichts zu tun.
    if (today_utc.year, today_utc.month) == (month_end.year, month_end.month):
        return None, None, None

    # --- 1) Prüfen, ob wir diesen Monat schon verarbeitet haben ------------
    last_processed = None
    last_positions: List[str] = []
    if os.path.exists(HISTORY_FILE):
        try:
            last_line = open(HISTORY_FILE, "r", encoding="utf‑8").read().strip().split(";")
            last_processed = pd.to_datetime(last_line[0])
            last_positions = [t for t in last_line[1].split(",") if t]
        except Exception:
            pass  # defektes File ignorieren

    if last_processed is not None and last_processed == month_end:
        return None, None, None  # Monat bereits verbucht

    # --- 2) SMA150 auf Daily‑Basis an month_end ---------------------------
    sma150 = close.rolling(SMA_WINDOW).mean().loc[month_end]
    prices = close.loc[month_end]

    # --- 3) Momentum‑Score (1M+3M+6M+9M) ----------------------------------
    monthly = close.resample("M").last()
    r1 = monthly.pct_change(1).iloc[-1]
    r3 = monthly.pct_change(3).iloc[-1]
    r6 = monthly.pct_change(6).iloc[-1]
    r9 = monthly.pct_change(9).iloc[-1]
    momentum = (r1 + r3 + r6 + r9).dropna()

    # Nur Assets > SMA150 berücksichtigen
    eligible = momentum.index[prices[momentum.index] > sma150[momentum.index]]
    momentum = momentum.loc[eligible]

    top = momentum.sort_values(ascending=False).head(TOP_N)
    new_positions: List[str] = list(top.index)

    # --- 4) History updaten -----------------------------------------------
    with open(HISTORY_FILE, "a", encoding="utf‑8") as f:
        f.write(f"{month_end:%Y‑%m‑%d};{','.join(new_positions)}\n")

    # --- 5) Message bauen --------------------------------------------------
    if set(new_positions) == set(last_positions):
        subject = f"GAA – keine Änderung ({month_end:%b %Y})"
        text = f"Portfolio unverändert: {', '.join(new_positions) or 'Cash'}"
        return subject, "", text

    moved_in = set(new_positions) - set(last_positions)
    moved_out = set(last_positions) - set(new_positions)

    subject = f"GAA Rebalance ({month_end:%b %Y})"
    lines: List[str] = []
    if moved_in:
        lines.append("Neu kaufen: " + ", ".join(sorted(moved_in)))
    if moved_out:
        lines.append("Verkaufen: " + ", ".join(sorted(moved_out)))
    lines.append("Aktuelles Portfolio: " + (", ".join(new_positions) if new_positions else "→ Cash"))
    lines.append("")
    lines.append("Momentum‑Scores (Stand Monatsschluss):")
    for ticker, score in top.items():
        lines.append(f"{ticker}: {score:+.2%}")

    return subject, "", "\n".join(lines)
