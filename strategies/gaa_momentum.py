"""
Global Asset Allocation – Momentum (robust + Debug)

Universum : NQ=F, BTC=F, GC=F, CL=F, EEM, FEZ, IEF
Momentum  : Summe Renditen 1 M + 3 M + 6 M + 9 M
Filter    : Letzter Schlusskurs > SMA150  (je Ticker)
Rebalance : Nur Monatsende – Top‑3 Assets gleichgewichtet
"""

from __future__ import annotations
import os, warnings
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
REN_MAP = {v: k for k, v in FUT.items()}   # Futures zurück auf internes Kürzel
TICKERS = ETF + list(FUT.values())

TOP_N, SMA_LEN = 3, 150
HIST_FILE = "gaa_history.csv"

# ---------------------------------------------------------------------------#
def _utc(idx):
    """tz‑aware → UTC → tz‑naiv"""
    if isinstance(idx, pd.DatetimeIndex):
        return idx.tz_localize(None) if idx.tz else idx
    return pd.to_datetime(idx, utc=True).tz_convert(None)

def _fetch_all() -> pd.DataFrame:
    """lädt alle Ticker in einem Call, wandelt in Date×Ticker‑Matrix"""
    h = yq.Ticker(" ".join(TICKERS)).history(period="2y", interval="1d")["close"]

    # MultiIndex (symbol, date) → Pivot
    if isinstance(h.index, pd.MultiIndex):
        h = (
            h.reset_index()                         # Spalten: symbol, date, close
              .pivot(index="date", columns="symbol", values="close")
        )
    else:                                          # Fallback: bereits richtig
        h = h.to_frame().rename(columns={"close": h.name})

    h = h.rename(columns=REN_MAP)                  # Futures umbenennen
    h.index = _utc(h.index)
    return h.sort_index()

# ---------------------------------------------------------------------------#
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    hist = _fetch_all()

    if hist.dropna(how="all").empty:
        return "GAA‑Fehler", None, "Keine Kursdaten verfügbar."

    # Debug 1: Datensatz‑Übersicht
    print("DEBUG hist shape:", hist.shape,
          "vollständige Tage:", hist.dropna().shape[0])

    today = pd.Timestamp.utcnow().normalize()
    last  = hist.dropna(how="all").index[-1]        # letzter beliebiger Handelstag
    hist  = hist.loc[:last]

    # Monatsende bestimmen
    m_ends = hist.index.to_period("M").unique().to_timestamp("M")
    m_end  = m_ends[-2] if (today.month, today.year) == (m_ends[-1].month,
                                                         m_ends[-1].year) else m_ends[-1]

    # bereits verbucht?
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

    # Letzter Kurs & SMA150 je Ticker
    latest = hist.ffill().iloc[-1]
    sma    = hist.rolling(SMA_LEN).mean().ffill().iloc[-1]

    eligible = mom.index[(latest > sma) & sma.notna()]

    # Debug 2: Price vs. SMA 150‑Tabelle
    debug_tbl = pd.DataFrame({
        "price": latest,
        "SMA150": sma,
        "diff%": (latest / sma - 1) * 100
    }).round(2)
    print("\nDEBUG Price vs SMA150")
    print(debug_tbl.to_string())
    print("Eligible:", list(eligible), "\n")

    # Top‑Auswahl
    if eligible.empty:
        top  = pd.Series(dtype=float)
        hold: List[str] = []          # Cash
    else:
        top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
        hold = list(top.index)

    print("DEBUG top tickers:", hold)

    # History schreiben (auch Cash‑Monate)
    with open(HIST_FILE, "a") as f:
        f.write(f"{m_end:%F};{','.join(hold)}\n")

    # Nachricht erstellen
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
