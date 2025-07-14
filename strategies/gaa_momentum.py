"""
Global Asset Allocation – Momentum (kompakt)

Universum : NQ=F, BTC=F, GC=F, CL=F, EEM, FEZ, IEF
Momentum  : Rendite 1 M + 3 M + 6 M + 9 M
Filter    : Kurs > SMA150
Rebalance : nur am Monatsende; es werden die 3 Top-Assets gehalten
"""

from __future__ import annotations
import os, warnings
from typing import Dict, List, Tuple

import pandas as pd
import yahooquery as yq
import yfinance as yf

ETF       = ["EEM", "FEZ", "IEF"]
FUT: Dict[str, str] = {          # interner Name → yfinance-Symbol
    "NQ=F":  "NQ=F",
    "BTC=F": "BTC-USD",
    "GC=F":  "GC=F",
    "CL=F":  "CL=F",
}
TOP_N        = 3
SMA_LEN      = 150
HIST_FILE    = "gaa_history.csv"

# ---------------------------------------------------------------------------#
def _utc_naive(idx):
    return pd.to_datetime(idx, utc=True).tz_convert(None)

def _fetch_etf() -> pd.DataFrame:
    df = yq.Ticker(" ".join(ETF)).history(period="2y", interval="1d")["close"].unstack()
    df.index = _utc_naive(df.index)
    return df

def _fetch_fut() -> pd.DataFrame:
    frames = []
    for name, sym in FUT.items():
        try:
            s = yf.download(sym, period="2y", interval="1d", progress=False")["Close"]
            if s.empty:
                raise ValueError("leerer Datensatz")
            frames.append(s.rename(name).tz_localize(None))
        except Exception as e:
            warnings.warn(f"{name}: {e}")
    return pd.concat(frames, axis=1)

# ---------------------------------------------------------------------------#
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    today = pd.Timestamp.utcnow().normalize()

    hist = pd.concat([_fetch_etf(), _fetch_fut()], axis=1).sort_index()
    hist = hist.loc[:hist.dropna().index[-1]]        # letzter kompletter Tag

    # Letzte abgeschlossene Monatskerze
    month_ends = hist.index.to_period("M").unique().to_timestamp("M")
    month_end  = month_ends[-1]
    if (today.month, today.year) == (month_end.month, month_end.year):
        if len(month_ends) < 2:
            return None, None, None
        month_end = month_ends[-2]

    # Bereits verbucht?
    if os.path.exists(HIST_FILE):
        last_d, *_ = open(HIST_FILE).read().strip().partition(";")
        if last_d == f"{month_end:%F}":
            return None, None, None
        prev_pos = _.strip().split(",") if _ else []
    else:
        prev_pos = []

    # Momentum-Score
    mon = hist.resample("M").last()
    score = (mon.pct_change(1).iloc[-1] +
             mon.pct_change(3).iloc[-1] +
             mon.pct_change(6).iloc[-1] +
             mon.pct_change(9).iloc[-1]).dropna()

    # SMA-Filter
    last = hist.index[-1]
    sma  = hist.rolling(SMA_LEN).mean().loc[last]
    price = hist.loc[last]
    elig   = score.index[price[score.index] > sma[score.index]]
    top    = score.loc[elig].sort_values(ascending=False).head(TOP_N)
    hold   = list(top.index)

    # Verlauf sichern
    with open(HIST_FILE, "a") as f:
        f.write(f"{month_end:%F};{','.join(hold)}\n")

    # Meldung aufbauen
    subj = f"GAA Rebalance ({month_end:%b %Y})"
    body = [f"Neu kaufen: {', '.join(sorted(set(hold)-set(prev_pos)))}",
            f"Aktuelles Portfolio: {', '.join(hold)}",
            "",
            "Momentum-Scores:"]
    body += [f"{t}: {sc:+.2%}" for t, sc in top.items()]

    return subj, "", "\n".join(body)
