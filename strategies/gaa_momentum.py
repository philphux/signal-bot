"""
Global Asset Allocation – Momentum (per‑Ticker Last Close)

Universum : NQ=F, BTC=F, GC=F, CL=F, EEM, FEZ, IEF
Momentum  : Summe Renditen 1 M + 3 M + 6 M + 9 M
Filter    : Letzter *valider* Schlusskurs > SMA150  (je Ticker)
Rebalance : Monatsende; Top‑3 Assets
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
REN_MAP = {v: k for k, v in FUT.items()}
TICKERS = ETF + list(FUT.values())

TOP_N, SMA_LEN = 3, 150
HIST_FILE = "gaa_history.csv"

# ---------------------------------------------------------------------------#
def _utc(idx):
    return pd.to_datetime(idx, utc=True).tz_convert(None)

def _fetch_all() -> pd.DataFrame:
    h = yq.Ticker(" ".join(TICKERS)).history(period="2y", interval="1d")["close"]
    if isinstance(h.index, pd.MultiIndex):
        h = (h.reset_index()
               .pivot(index="date", columns="symbol", values="close"))
    else:
        h = h.to_frame().rename(columns={"close": h.name})
    h = h.rename(columns=REN_MAP)
    h.index = _utc(h.index)
    return h.sort_index()

# -------- per‑Ticker letzter Preis & SMA -----------------------------------#
def _last_price_and_sma(hist: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    prices, smas = {}, {}
    for col, ser in hist.items():
        ser = ser.dropna()
        if len(ser) < SMA_LEN:
            continue
        last_date = ser.index[-1]
        prices[col] = ser.iloc[-1]
        sma = ser.rolling(SMA_LEN).mean().loc[last_date]
        smas[col] = sma
    return pd.Series(prices), pd.Series(smas)

# ---------------------------------------------------------------------------#
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    hist = _fetch_all()
    if hist.dropna(how="all").empty:
        return "GAA‑Fehler", None, "Keine Kursdaten verfügbar."

    # Momentum‑Score
    mon = hist.resample("M").last()
    mom = (
        mon.pct_change(1).iloc[-1] +
        mon.pct_change(3).iloc[-1] +
        mon.pct_change(6).iloc[-1] +
        mon.pct_change(9).iloc[-1]
    ).dropna()

    # per‑Ticker Kurs & SMA
    price, sma = _last_price_and_sma(hist)
    eligible = mom.index[(price > sma) & sma.notna()]

    # Debug‑Tabelle
    debug_tbl = pd.DataFrame({
        "price": price,
        "SMA150": sma,
        "diff%": (price / sma - 1) * 100
    }).round(2).sort_index()
    print("\nDEBUG Price vs SMA150\n", debug_tbl)
    print("Eligible:", list(eligible), "\n")

    # Top‑Auswahl
    if eligible.empty:
        top = pd.Series(dtype=float)
        hold: List[str] = []
    else:
        top = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
        hold = list(top.index)

    # ----- Verlauf & Meldung -------------------------------------------------
    today = pd.Timestamp.utcnow()
    m_end = today.to_period("M").to_timestamp("M")

    if os.path.exists(HIST_FILE):
        last_entry = open(HIST_FILE).read().strip().split(";")[0]
        if last_entry == f"{m_end:%F}":
            return None, None, None

    with open(HIST_FILE, "a") as f:
        f.write(f"{m_end:%F};{','.join(hold)}\n")

    subj = f"GAA Rebalance ({m_end:%b %Y})"
    body = [
        f"Neu kaufen: {', '.join(hold) if hold else '–'}",
        f"Aktuelles Portfolio: {', '.join(hold) if hold else 'Cash'}",
        "",
        "Momentum‑Scores:" if not top.empty else "Keine eligible Assets"
    ]
    if not top.empty:
        body += [f"{t}: {sc:+.2%}" for t, sc in top.items()]

    return subj, "", "\n".join(body)
