"""
Global-Asset-Allocation-Strategie (Momentum) nach Meb Faber

• Universe : NQ=F, BTC=F, GC=F, CL=F, EEM, FEZ, IEF
• Momentum : Rendite 1 M + 3 M + 6 M + 9 M
• Filter   : Investition nur, wenn Schlusskurs > SMA150
• Rebalance: nach jedem Monatsende (TOP_N beste Werte werden gehalten)
"""

from __future__ import annotations

import os
import time
import warnings
from typing import List, Tuple

import pandas as pd
import yahooquery as yq
import yfinance as yf

# ---------------------------------------------------------------------------#
# Parameter                                                                   #
# ---------------------------------------------------------------------------#
ETF_TICKERS     = ["EEM", "FEZ", "IEF"]
FUTURE_TICKERS  = {
    "NQ=F": "NQ=F",
    "BTC=F": "BTC-USD",
    "GC=F": "GC=F",
    "CL=F": "CL=F",
}
TOP_N           = 3
SMA_WINDOW      = 150
HISTORY_FILE    = "gaa_history.csv"
RETRY           = 3

# ---------------------------------------------------------------------------#
def _naive_utc_index(idx: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Index → UTC → tz-naiv."""
    idx = pd.to_datetime(idx, utc=True)
    return idx.tz_convert(None)

# ---------------------------------------------------------------------------#
def fetch_etfs() -> pd.DataFrame:
    tq  = yq.Ticker(" ".join(ETF_TICKERS))
    df  = tq.history(period="2y", interval="1d")["close"].unstack(level=0)
    df.index = _naive_utc_index(df.index)
    return df

def fetch_futures() -> pd.DataFrame:
    frames = []
    for ym, yf_sym in FUTURE_TICKERS.items():
        try:
            data = yf.download(
                yf_sym, period="2y", interval="1d", progress=False
            )["Close"]
            data.name = ym
            data.index = _naive_utc_index(data.index)
            frames.append(data)
        except Exception as e:
            warnings.warn(f"{ym}: {e}")
    return pd.concat(frames, axis=1) if frames else pd.DataFrame()

# ---------------------------------------------------------------------------#
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    """Einmal pro Monat ein Rebalance-Signal, sonst (None, None, None)."""
    today = pd.Timestamp.utcnow().normalize()

    # 1) Kursdaten ----------------------------------------------------------
    etf_df = fetch_etfs()
    fut_df = fetch_futures()
    hist   = pd.concat([etf_df, fut_df], axis=1).sort_index().dropna(how="all")

    if hist.empty:
        return "GAA-Fehler", None, "Kein einziger Ticker liefert Kursdaten."

    # 2) Letzte vollendete Monatskerze --------------------------------------
    month_ends = hist.index.to_period("M").unique().to_timestamp("M")
    month_end  = month_ends[-1]
    if (today.year, today.month) == (month_end.year, month_end.month):
        if len(month_ends) < 2:
            return None, None, None
        month_end = month_ends[-2]

    # Schon verbucht?
    last_positions: List[str] = []
    if os.path.exists(HISTORY_FILE):
        last_line = open(HISTORY_FILE).read().strip().split(";")
        if last_line and pd.to_datetime(last_line[0]) == month_end:
            return None, None, None
        if len(last_line) > 1:
            last_positions = [t for t in last_line[1].split(",") if t]

    # 3) Momentum-Score -----------------------------------------------------
    monthly = hist.resample("M").last()
    r1, r3, r6, r9 = (monthly.pct_change(n).iloc[-1] for n in (1, 3, 6, 9))
    score   = (r1 + r3 + r6 + r9).dropna()

    sma150  = hist.rolling(SMA_WINDOW).mean().loc[month_end]
    elig    = score.index[hist.loc[month_end, score.index] > sma150[score.index]]
    score   = score.loc[elig]

    if score.empty:
        return "GAA – kein Asset über SMA150", "", "→ Cash"

    top           = score.sort_values(ascending=False).head(TOP_N)
    new_positions = list(top.index)

    # 4) Verlauf sichern ----------------------------------------------------
    with open(HISTORY_FILE, "a") as f:
        f.write(f"{month_end:%Y-%m-%d};{','.join(new_positions)}\n")

    # 5) Meldung ------------------------------------------------------------
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
