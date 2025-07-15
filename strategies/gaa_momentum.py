"""
Global Asset Allocation – Momentum (robuste ETF‑Abfrage)
"""

from __future__ import annotations
import os, warnings
from typing import Dict, List, Tuple
import pandas as pd, yfinance as yf

ETF = ["EEM", "FEZ", "IEF"]
FUT: Dict[str, str] = {
    "NQ=F":  "NQ=F",
    "BTC=F": "BTC-USD",
    "GC=F":  "GC=F",
    "CL=F":  "CL=F",
}
TOP_N, SMA_LEN = 3, 150
HIST_FILE = "gaa_history.csv"

# ---------------------------------------------------------------------------#
def _utc(idx):
    """gibt einen tz‑naiven DatetimeIndex zurück"""
    if isinstance(idx, pd.DatetimeIndex):
        return idx.tz_localize(None) if idx.tz else idx
    return pd.to_datetime(idx, utc=True).tz_convert(None)


def _fetch_series(symbols: List[str]) -> pd.DataFrame:
    frames = []
    for sym in symbols:
        try:
            ser = yf.download(sym, period="2y", interval="1d",
                              progress=False)["Close"]
            if ser.empty:
                raise ValueError("leerer Datensatz")
            frames.append(ser.rename(sym).tz_localize(None))
        except Exception as e:
            warnings.warn(f"{sym}: {e}")
    return pd.concat(frames, axis=1) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------#
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    today = pd.Timestamp.utcnow().normalize()
    hist = pd.concat([
        _fetch_series(ETF),
        _fetch_series(list(FUT.values())).rename(columns={v: k for k, v in FUT.items()}),
    ], axis=1).sort_index()

    if hist.dropna(how="all").empty:
        return "GAA‑Fehler", None, "Keine Kursdaten verfügbar."

    print("DEBUG hist shape:", hist.shape, "vollständige Tage:", hist.dropna().shape[0])

    last = hist.dropna().index[-1]
    hist = hist.loc[:last]

    m_ends = hist.index.to_period("M").unique().to_timestamp("M")
    m_end = m_ends[-2] if (today.month, today.year) == (m_ends[-1].month, m_ends[-1].year) else m_ends[-1]

    prev: List[str] = []
    if os.path.exists(HIST_FILE):
        date, *_ = open(HIST_FILE).read().strip().split(";")
        if date == f"{m_end:%F}":
            return None, None, None
        prev = _[0].split(",") if _ else []

    mon = hist.resample("M").last()
    mom = (
        mon.pct_change(1).iloc[-1]
        + mon.pct_change(3).iloc[-1]
        + mon.pct_change(6).iloc[-1]
        + mon.pct_change(9).iloc[-1]
    ).dropna()

    price = hist.loc[last]
    sma = hist.rolling(SMA_LEN).mean().loc[last]

    valid = sma.notna()
    filt = (price > sma) & valid
    eligible = mom.index[filt[mom.index]]

    print("DEBUG eligible tickers:", list(eligible))

    if eligible.empty:
        top = pd.Series(dtype=float)
        hold: List[str] = []
    else:
        top = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
        hold = list(top.index)

    print("DEBUG top tickers:", hold)

    with open(HIST_FILE, "a") as f:
        f.write(f"{m_end:%F};{','.join(hold)}\n")

    subject = f"GAA Rebalance ({m_end:%b %Y})"
    body = [
        f"Neu kaufen: {', '.join(sorted(set(hold) - set(prev)))}" if hold else "Neu kaufen: –",
        f"Aktuelles Portfolio: {', '.join(hold) if hold else 'Cash'}",
        "",
        "Momentum‑Scores:" if not top.empty else "Keine eligible Assets",
    ]
    if not top.empty:
        body += [f"{t}: {sc:+.2%}" for t, sc in top.items()]

    return subject, "", "\n".join(body)
