"""
Global Asset Allocation – Momentum (robust, MultiIndex‑Fix)
"""

from __future__ import annotations
import os, warnings
from typing import Dict, List, Tuple
import pandas as pd, yahooquery as yq

ETF = ["EEM", "FEZ", "IEF"]
FUT: Dict[str, str] = {          # internes Kürzel → YF‑Symbol
    "NQ=F":  "NQ=F",
    "BTC=F": "BTC-USD",
    "GC=F":  "GC=F",
    "CL=F":  "CL=F",
}
REN_MAP = {v: k for k, v in FUT.items()}   # Rück‑Mapping
TICKERS = ETF + list(FUT.values())

TOP_N, SMA_LEN = 3, 150
HIST_FILE = "gaa_history.csv"

# ------------------------------------------------------------------------- #
def _utc(idx):
    if isinstance(idx, pd.DatetimeIndex):
        return idx.tz_localize(None) if idx.tz else idx
    return pd.to_datetime(idx, utc=True).tz_convert(None)

# ------------------------------------------------------------------------- #
def _fetch_all() -> pd.DataFrame:
    """holt alle Ticker in einem Call, formatiert zu Date×Ticker‐Matrix"""
    h = yq.Ticker(" ".join(TICKERS)).history(period="2y", interval="1d")["close"]
    if isinstance(h.index, pd.MultiIndex):
        h = (
            h.reset_index()                       # → Spalten: symbol, date, close
              .pivot(index="date", columns="symbol", values="close")
        )
    else:                                         # Fallback: schon korrekt
        h = h.to_frame().rename(columns={"close": h.name})
    h = h.rename(columns=REN_MAP)                 # Futures auf internes Kürzel
    h.index = _utc(h.index)
    return h.sort_index()

# ------------------------------------------------------------------------- #
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    hist = _fetch_all()

    if hist.dropna(how="all").empty:
        return "GAA‑Fehler", None, "Keine Kursdaten verfügbar."

    print("DEBUG hist shape:", hist.shape,
          "vollständige Tage:", hist.dropna().shape[0])

    last = hist.dropna().index[-1]
    hist = hist.loc[:last]

    today  = pd.Timestamp.utcnow().normalize()
    m_ends = hist.index.to_period("M").unique().to_timestamp("M")
    m_end  = m_ends[-2] if (today.month, today.year) == (m_ends[-1].month, m_ends[-1].year) else m_ends[-1]

    prev: List[str] = []
    if os.path.exists(HIST_FILE):
        date, *_ = open(HIST_FILE).read().strip().split(";")
        if date == f"{m_end:%F}":
            return None, None, None
        prev = _[0].split(",") if _ else []

    # Momentum‑Score
    mon = hist.resample("M").last()
    mom = (
        mon.pct_change(1).iloc[-1]
        + mon.pct_change(3).iloc[-1]
        + mon.pct_change(6).iloc[-1]
        + mon.pct_change(9).iloc[-1]
    ).dropna()

    price = hist.loc[last]
    sma   = hist.rolling(SMA_LEN).mean().loc[last]

    eligible = mom.index[(price > sma) & sma.notna()]
    print("DEBUG eligible tickers:", list(eligible))

    if eligible.empty:
        top  = pd.Series(dtype=float)
        hold = []          # Cash
    else:
        top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
        hold = list(top.index)

    print("DEBUG top tickers:", hold)

    # Verlauf schreiben (auch Cash)
    with open(HIST_FILE, "a") as f:
        f.write(f"{m_end:%F};{','.join(hold)}\n")

    subject = f"GAA Rebalance ({m_end:%b %Y})"
    body = [
        f"Neu kaufen: {', '.join(sorted(set(hold) - set(prev)))}" if hold else "Neu kaufen: –",
        f"Aktuelles Portfolio: {', '.join(hold) if hold else 'Cash'}",
        "",
        "Momentum‑Scores:" if not top.empty else "Keine eligible Assets"
    ]
    if not top.empty:
        body += [f"{t}: {sc:+.2%}" for t, sc in top.items()]

    return subject, "", "\n".join(body)
