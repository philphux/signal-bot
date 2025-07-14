"""
Global-Asset-Allocation-Strategie (Momentum) nach Meb Faber

• Universum : NQ=F, BTC=F, GC=F, CL=F, EEM, FEZ, IEF
• Momentum  : Rendite 1 M + 3 M + 6 M + 9 M
• Filter    : Kurs > SMA150 (berechnet auf dem letzten Tag mit *vollständigen*
               Preisen für alle Assets)
• Rebalance : Einmal pro Kalendermonat (TOP_N beste Assets)
"""

from __future__ import annotations
import os, time, warnings
from typing import Dict, List, Tuple

import pandas as pd
import yahooquery as yq
import yfinance as yf

# ---------------------------------------------------------------------------#
ETF_TICKERS      = ["EEM", "FEZ", "IEF"]
FUTURE_TICKERS: Dict[str, str] = {          # <interner Name>: <yfinance-Symbol>
    "NQ=F":  "NQ=F",
    "BTC=F": "BTC-USD",   # Spot als Ersatz
    "GC=F":  "GC=F",
    "CL=F":  "CL=F",
}
TOP_N        = 3
SMA_WINDOW   = 150
HISTORY_FILE = "gaa_history.csv"
RETRY        = 3
# ---------------------------------------------------------------------------#
def _naive_utc(idx: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """tz-aware → UTC → tz-naiv (verhindert Mix-Fehler)."""
    return pd.to_datetime(idx, utc=True).tz_convert(None)

# ETFs ----------------------------------------------------------------------
def fetch_etfs() -> pd.DataFrame:
    tq = yq.Ticker(" ".join(ETF_TICKERS))
    df = tq.history(period="2y", interval="1d")["close"].unstack(level=0)
    df.index = _naive_utc(df.index)
    return df

# Futures / Bitcoin ---------------------------------------------------------
def fetch_futures() -> Tuple[pd.DataFrame, List[str]]:
    frames, ignored = [], []
    for name, sym in FUTURE_TICKERS.items():
        try:
            ser = yf.download(sym, period="2y", interval="1d",
                              progress=False)["Close"]
            if ser.empty:
                raise ValueError("leerer Datensatz")
            ser.index = _naive_utc(ser.index)
            ser.name  = name          # Spaltenname = internes Kürzel
            frames.append(ser)
        except Exception as e:
            warnings.warn(f"{name}: {e}")
            ignored.append(name)
    fut = pd.concat(frames, axis=1) if frames else pd.DataFrame()
    return fut, ignored
# ---------------------------------------------------------------------------#
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    today = pd.Timestamp.utcnow().normalize()

    # 1) Daten holen --------------------------------------------------------
    etf_df        = fetch_etfs()
    fut_df, skip  = fetch_futures()
    hist          = pd.concat([etf_df, fut_df], axis=1).sort_index()

    # letzter Tag mit *vollständigen* Daten
    hist_full = hist.dropna()
    if hist_full.empty:
        return "GAA-Fehler", None, "Kein Handelstag mit vollständigen Kursen."

    last_date = hist_full.index[-1]           # z. B. letzter Freitag
    hist      = hist.loc[:last_date]          # auf diesen Tag beschneiden

    # 2) Letzte abgeschlossene Monatskerze ----------------------------------
    month_ends = hist.index.to_period("M").unique().to_timestamp("M")
    month_end  = month_ends[-1]
    if (today.year, today.month) == (month_end.year, month_end.month):
        # aktueller Monat läuft noch – nimm den vorherigen
        if len(month_ends) < 2:
            return None, None, None
        month_end = month_ends[-2]

    # 3) Doppel-Verbuchen verhindern ---------------------------------------
    last_pos: List[str] = []
    if os.path.exists(HISTORY_FILE):
        last = open(HISTORY_FILE).read().strip().split(";")
        if last and pd.to_datetime(last[0]) == month_end:
            return None, None, None
        if len(last) > 1:
            last_pos = [t for t in last[1].split(",") if t]

    # 4) Momentum-Score -----------------------------------------------------
    monthly  = hist.resample("M").last()
    r1, r3, r6, r9 = (monthly.pct_change(n).iloc[-1] for n in (1, 3, 6, 9))
    score    = (r1 + r3 + r6 + r9).dropna()

    # 5) SMA150-Filter auf dem letzten vollständigen Tag --------------------
    sma150   = hist.rolling(SMA_WINDOW).mean().loc[last_date]
    price    = hist.loc[last_date]
    elig     = score.index[price[score.index] > sma150[score.index]]
    score    = score.loc[elig]

    if score.empty:
        txt = "→ Cash (kein Asset > SMA150)"
        if skip:
            txt += f"\nIgnoriert (keine Daten): {', '.join(skip)}"
        return "GAA – kein Kauf", "", txt

    top           = score.sort_values(ascending=False).head(TOP_N)
    new_positions = list(top.index)

    # 6) Verlauf sichern ----------------------------------------------------
    with open(HISTORY_FILE, "a") as f:
        f.write(f"{month_end:%Y-%m-%d};{','.join(new_positions)}\n")

    # 7) Meldung ------------------------------------------------------------
    if set(new_positions) == set(last_pos):
        header = f"GAA – keine Änderung ({month_end:%b %Y})"
        lines  = [f"Portfolio unverändert: {', '.join(new_positions)}"]
    else:
        header = f"GAA Rebalance ({month_end:%b %Y})"
        lines  = []
        if (add := set(new_positions) - set(last_pos)):
            lines.append("Neu kaufen: " + ", ".join(sorted(add)))
        if (sell := set(last_pos) - set(new_positions)):
            lines.append("Verkaufen: " + ", ".join(sorted(sell)))
        lines.append("Aktuelles Portfolio: " +
                     (", ".join(new_positions) if new_positions else "Cash"))

    # Momentum-Details
    lines.append("\nMomentum-Scores:")
    for t, sc in top.items():
        lines.append(f"{t}: {sc:+.2%}")

    if skip:
        lines.append("\nIgnoriert (keine Daten): " + ", ".join(skip))

    return header, "", "\n".join(lines)
