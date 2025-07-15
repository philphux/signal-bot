"""
Global Asset Allocation – Momentum (kompakt + SMA‑Fix)
"""

from __future__ import annotations
import os, warnings
from typing import Dict, List, Tuple
import pandas as pd, yahooquery as yq, yfinance as yf

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
def _utc(idx):  # tz‑aware → UTC → tz‑naiv
    return pd.to_datetime(idx, utc=True).tz_convert(None)


def _fetch_etf() -> pd.DataFrame:
    df = yq.Ticker(" ".join(ETF)).history(period="2y", interval="1d")["close"].unstack()
    df.index = _utc(df.index)
    return df


def _fetch_fut() -> pd.DataFrame:
    frames = []
    for name, sym in FUT.items():
        try:
            ser = yf.download(sym, period="2y", interval="1d", progress=False)["Close"]
            if ser.empty:
                raise ValueError("leerer Datensatz")
            frames.append(ser.rename(name).tz_localize(None))
        except Exception as e:
            warnings.warn(f"{name}: {e}")
    fut = pd.concat(frames, axis=1) if frames else pd.DataFrame()
    # Falls yfinance Spaltennamen ändert: hart auf unser Mapping setzen
    fut.columns = [c if c in FUT else next(k for k, v in FUT.items() if v == c) for c in fut.columns]
    return fut


# ---------------------------------------------------------------------------#
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    today = pd.Timestamp.utcnow().normalize()
    hist = pd.concat([_fetch_etf(), _fetch_fut()], axis=1).sort_index()

    if hist.dropna().empty:
        return "GAA‑Fehler", None, "Keine vollständigen Kursreihen."

    last = hist.dropna().index[-1]
    hist = hist.loc[:last]

    m_ends = hist.index.to_period("M").unique().to_timestamp("M")
    m_end = m_ends[-2] if (today.month, today.year) == (m_ends[-1].month, m_ends[-1].year) else m_ends[-1]

    # schon verbucht?
    if os.path.exists(HIST_FILE):
        date, *_ = open(HIST_FILE).read().strip().split(";")
        if date == f"{m_end:%F}":
            return None, None, None
        prev = _[0].split(",") if _ else []
    else:
        prev = []

    # Momentum
    mon = hist.resample("M").last()
    mom = sum(mon.pct_change(n).iloc[-1] for n in (1, 3, 6, 9)).dropna()

    price = hist.loc[last]
    sma = hist.rolling(SMA_LEN).mean().loc[last]

    # -------- Fix: Nur Zeilen mit gültigem SMA berücksichtigen -----------
    valid = sma.notna()
    filt = (price > sma) & valid
    eligible = mom.index[filt[mom.index]]

    top = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold = list(top.index)

    # Mini‑Debug‑Zeile (im Log, nicht in Discord)
    print("GAA filter — eligible:", ", ".join(eligible))

    # History schreiben
    with open(HIST_FILE, "a") as f:
        f.write(f"{m_end:%F};{','.join(hold)}\n")

    subj = f"GAA Rebalance ({m_end:%b %Y})"
    body = [
        f"Neu kaufen: {', '.join(sorted(set(hold) - set(prev)))}",
        f"Aktuelles Portfolio: {', '.join(hold) if hold else 'Cash'}",
        "",
        "Momentum‑Scores:",
    ] + [f"{t}: {sc:+.2%}" for t, sc in top.items()]

    return subj, "", "\n".join(body)
