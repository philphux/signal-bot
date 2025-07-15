"""
Global Asset Allocation – Momentum (kompakt, robust, Debug)

Universum : NQ=F, BTC=F, GC=F, CL=F, EEM, FEZ, IEF
Momentum  : Rendite 1 M + 3 M + 6 M + 9 M
Filter    : Kurs > SMA150 (je Ticker letzter Schlusskurs)
Rebalance : Monatsende; Top‑3 gleichgewichtet
"""

from __future__ import annotations
import os, warnings
from typing import Dict, List, Tuple
import pandas as pd, yahooquery as yq

# -------------------------------------------------------------------------- #
ETF = ["EEM", "FEZ", "IEF"]
FUT: Dict[str, str] = {
    "NQ=F":  "NQ=F",
    "BTC=F": "BTC-USD",
    "GC=F":  "GC=F",
    "CL=F":  "CL=F",
}
REN_MAP = {v: k for k, v in FUT.items()}
TICKERS = ETF + list(FUT.values())

TOP_N, SMA_LEN = 3, 150
HIST_FILE = "gaa_history.csv"

# -------------------------------------------------------------------------- #
def _utc(idx):
    """tz‑aware → UTC → tz‑naiv"""
    if isinstance(idx, pd.DatetimeIndex):
        return idx.tz_localize(None) if idx.tz else idx
    return pd.to_datetime(idx, utc=True).tz_convert(None)

def _fetch_all() -> pd.DataFrame:
    """
    holt alle Ticker in einem Call,
    liefert Date × Ticker‑Matrix (Close‑Preise)
    """
    h = yq.Ticker(" ".join(TICKERS)).history(period="2y", interval="1d")["close"]

    # MultiIndex (symbol, date) → Pivot
    if isinstance(h.index, pd.MultiIndex):
        h = (
            h.reset_index()                       # columns: symbol, date, close
              .pivot(index="date", columns="symbol", values="close")
        )
    else:  # fallback
        h = h.to_frame().rename(columns={"close": h.name})

    h = h.rename(columns=REN_MAP)          # Futures auf internes Kürzel
    h.index = _utc(h.index)
    return h.sort_index()

# -------------------------------------------------------------------------- #
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    hist = _fetch_all()

    if hist.dropna(how="all").empty:
        return "GAA‑Fehler", None, "Keine Kursdaten verfügbar."

    print("DEBUG hist shape:", hist.shape,
          "vollständige Tage:", hist.dropna().shape[0])

    # Daten bis zum letzten Tag mit irgendeinem Kurs
    today = pd.Timestamp.utcnow().normalize()
    last  = hist.dropna(how="all").index[-1]
    hist  = hist.loc[:last]

    # finales Monatsende
    m_ends = hist.index.to_period("M").unique().to_timestamp("M")
    m_end  = m_ends[-2] if (today.month, today.year) == (m_ends[-1].month,
                                                         m_ends[-1].year) else m_ends[-1]

    # verbucht?
    prev: List[str] = []
    if os.path.exists(HIST_FILE):
        date, *_ = open(HIST_FILE).read().strip().split(";")
        if date == f"{m_end:%F}":
            return None, None, None
        prev = _[0].split(",") if _ else []

    # Momentum‑Score
    mon = hist.resample("M").last()
    mom = (
        mon.pct_change(1).iloc[-1] +
        mon.pct_change(3).iloc[-1] +
        mon.pct_change(6).iloc[-1] +
        mon.pct_change(9).iloc[-1]
    ).dropna()

    # letzter Kurs & SMA150 je Ticker
    latest = hist.ffill().iloc[-1]
    sma    = hist.rolling(SMA_LEN).mean().ffill().iloc[-1]
    eligible = mom.index[(latest > sma) & sma.notna()]

    # ----- Debug: Kurs vs. SMA150 ------------------------------------------
    debug_tbl = pd.DataFrame({
        "price": latest,
        "SMA150": sma,
        "diff%": (latest / sma - 1) * 100
    }).round(2)
    print("\nDEBUG Price vs SMA150")
    print(debug_tbl.to_string())
    print("Eligible:", list(eligible), "\n")
    # -----------------------------------------------------------------------

    if eligible.empty:
        top = pd.Series(dtype=float)
        hold: List[str] = []
    else:
        top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
        hold = list(top.index)

    print("DEBUG top tickers:", hold)

    # Verlauf schreiben (auch Cash‑Monate)
    with open(HIST_FILE, "a") as f:
        f.write(f"{m_end:%F};{','.join(hold)}\n")

    # Meldung
    subj = f"GAA Rebalance ({m_end:%b %Y})"
    body = [
        f"Neu kaufen: {', '.join(sorted(set(hold) - set(prev)))}" if hold else "Neu kaufen: –",
        f"Aktuelles Portfolio: {', '.join(hold) if hold else 'Cash'}",
        "",
        "Momentum‑Scores:" if not top.empty else "Keine eligible Assets"
    ]
    if not top.empty:
        body += [f"{t}: {sc:+.2%}" for t, sc in top.items()]

    return subj, "", "\n".join(body)
