"""
Global Asset Allocation – Momentum (yahooquery‑only, robust)
"""

from __future__ import annotations
import os
from typing import Dict, List, Tuple
import pandas as pd, yahooquery as yq

# ---------------------------------------------------------------------------#
ETF = ["EEM", "FEZ", "IEF"]
FUT: Dict[str, str] = {
    "NQ=F":  "NQ=F",
    "BTC=F": "BTC-USD",
    "GC=F":  "GC=F",
    "CL=F":  "CL=F",
}
TICKERS = ETF + list(FUT.values())           # gesamte Watchlist
REN_MAP = {v: k for k, v in FUT.items()}     # FUT‑Symbol → internes Kürzel

TOP_N, SMA_LEN = 3, 150
HIST_FILE = "gaa_history.csv"

# ---------------------------------------------------------------------------#
def _utc(idx):
    if isinstance(idx, pd.DatetimeIndex):
        return idx.tz_localize(None) if idx.tz else idx
    return pd.to_datetime(idx, utc=True).tz_convert(None)


def _fetch_all() -> pd.DataFrame:
    """Lädt alle Ticker mit yahooquery in einem Call (period 2 Jahre)."""
    h = yq.Ticker(" ".join(TICKERS)).history(period="2y", interval="1d")["close"].unstack()
    h.index = _utc(h.index)
    # Spalten ggf. auf unsere FUT‑Keys umbenennen
    h = h.rename(columns=REN_MAP).sort_index()
    return h


# ---------------------------------------------------------------------------#
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    hist = _fetch_all()

    if hist.dropna(how="all").empty:
        return "GAA‑Fehler", None, "Keine Kursdaten verfügbar."

    print("DEBUG hist shape:", hist.shape, "vollständige Tage:",
          hist.dropna().shape[0])

    last = hist.dropna().index[-1]
    hist = hist.loc[:last]

    today = pd.Timestamp.utcnow().normalize()
    m_ends = hist.index.to_period("M").unique().to_timestamp("M")
    m_end = m_ends[-2] if (today.month, today.year) == (m_ends[-1].month, m_ends[-1].year) else m_ends[-1]

    prev: List[str] = []
    if os.path.exists(HIST_FILE):
        date, *_ = open(HIST_FILE).read().strip().split(";")
        if date == f"{m_end:%F}":
            return None, None, None          # bereits verbucht
        prev = _[0].split(",") if _ else []

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
        top = pd.Series(dtype=float)
        hold: List[str] = []
    else:
        top = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
        hold = list(top.index)

    print("DEBUG top tickers:", hold)

    # -- History schreiben ---------------------------------------------------
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
