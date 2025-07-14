"""
Global Asset Allocation – Momentum (kompakte Fassung)

Universum : NQ=F, BTC=F, GC=F, CL=F, EEM, FEZ, IEF
Momentum  : Rendite 1 M + 3 M + 6 M + 9 M
Filter    : Kurs > SMA150
Rebalance : Nur am Monatsende; es werden die 3 Top-Assets gehalten
"""

from __future__ import annotations
import os, warnings
from typing import Dict, List, Tuple

import pandas as pd
import yahooquery as yq
import yfinance as yf

ETF          = ["EEM", "FEZ", "IEF"]
FUT: Dict[str, str] = {             # interner Name → yfinance-Symbol
    "NQ=F":  "NQ=F",
    "BTC=F": "BTC-USD",
    "GC=F":  "GC=F",
    "CL=F":  "CL=F",
}

TOP_N     = 3
SMA_LEN   = 150
HIST_FILE = "gaa_history.csv"

# ---------------------------------------------------------------------------#
def _utc_naive(idx: pd.Index) -> pd.DatetimeIndex:
    return pd.to_datetime(idx, utc=True).tz_convert(None)

def _fetch_etf() -> pd.DataFrame:
    df = (
        yq.Ticker(" ".join(ETF))
        .history(period="2y", interval="1d")["close"]
        .unstack()
    )
    df.index = _utc_naive(df.index)
    return df

def _fetch_fut() -> pd.DataFrame:
    frames: List[pd.Series] = []
    for name, sym in FUT.items():
        try:
            ser = yf.download(sym, period="2y", interval="1d", progress=False)["Close"]
            if ser.empty:
                raise ValueError("leerer Datensatz")
            ser = ser.rename(name).tz_localize(None)
            frames.append(ser)
        except Exception as exc:
            warnings.warn(f"{name}: {exc}")
    return pd.concat(frames, axis=1) if frames else pd.DataFrame()

# ---------------------------------------------------------------------------#
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    today = pd.Timestamp.utcnow().normalize()

    hist = pd.concat([_fetch_etf(), _fetch_fut()], axis=1).sort_index()
    if hist.dropna().empty:
        return "GAA-Fehler", None, "Keine vollständigen Kursreihen verfügbar."

    last_day = hist.dropna().index[-1]   # letzter Tag mit allen Preisen
    hist = hist.loc[:last_day]

    # letztes abgeschlossenes Monatsende
    m_ends = hist.index.to_period("M").unique().to_timestamp("M")
    month_end = m_ends[-1]
    if (today.year, today.month) == (month_end.year, month_end.month):
        if len(m_ends) < 2:
            return None, None, None          # zu wenig Historie
        month_end = m_ends[-2]

    # schon verbucht?
    prev_pos: List[str] = []
    if os.path.exists(HIST_FILE):
        last_line = open(HIST_FILE).read().strip().split(";")
        if last_line and pd.to_datetime(last_line[0]) == month_end:
            return None, None, None
        if len(last_line) > 1:
            prev_pos = [x for x in last_line[1].split(",") if x]

    # Momentum-Score berechnen
    monthly = hist.resample("M").last()
    momentum = (
        monthly.pct_change(1).iloc[-1]
        + monthly.pct_change(3).iloc[-1]
        + monthly.pct_change(6).iloc[-1]
        + monthly.pct_change(9).iloc[-1]
    ).dropna()

    price = hist.loc[last_day]
    sma   = hist.rolling(SMA_LEN).mean().loc[last_day]

    eligible = momentum.index[price[momentum.index] > sma[momentum.index]]
    top = momentum.loc[eligible].sort_values(ascending=False).head(TOP_N)

    new_pos = list(top.index)

    # Verlauf speichern
    with open(HIST_FILE, "a") as f:
        f.write(f"{month_end:%F};{','.join(new_pos)}\n")

    # Meldung
    subject = f"GAA Rebalance ({month_end:%b %Y})"
    body = [
        f"Neu kaufen: {', '.join(sorted(set(new_pos) - set(prev_pos)))}",
        f"Aktuelles Portfolio: {', '.join(new_pos)}",
        "",
        "Momentum-Scores:",
    ]
    body += [f"{t}: {sc:+.2%}" for t, sc in top.items()]

    return subject, "", "\n".join(body)
