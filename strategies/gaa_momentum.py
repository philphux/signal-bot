"""
Global Asset Allocation – Momentum
Sendet IMMER eine Meldung:

• Wenn Portfolio wechselt  →  Kaufen / Verkaufen / Halten
• Wenn Portfolio gleich bleibt →  Halten: …

Universum   : Bitcoin, Nasdaq‑100, Gold, WTI Crude Oil,
              Emerging Markets, Euro Stoxx 50, Treasury Bonds
Momentum    : 1 M + 3 M + 6 M + 9 M
Filter      : Letzter Schlusskurs > SMA 150
Rebalance   : Monatsende; Top‑3 Assets
History     : gaa_history.csv (nur bei Wechsel)
"""

from __future__ import annotations
import os, warnings
from typing import Dict, List, Tuple
import pandas as pd, yahooquery as yq

# ─────────────────────────────────────────────  Parameter  ─────────────── #
ETF = ["EEM", "FEZ", "IEF"]
FUT: Dict[str, str] = {
    "NQ=F":  "NQ=F",
    "BTC=F": "BTC-USD",
    "GC=F":  "GC=F",
    "CL=F":  "CL=F",
}
REN_MAP = {v: k for k, v in FUT.items()}
TICKERS = ETF + list(FUT.values())

NAMES = {
    "BTC=F": "Bitcoin",
    "NQ=F":  "Nasdaq‑100",
    "GC=F":  "Gold",
    "CL=F":  "WTI Crude Oil",
    "EEM":   "Emerging Markets",
    "FEZ":   "Euro Stoxx 50",
    "IEF":   "Treasury Bonds",
}

TOP_N, SMA_LEN = 3, 150
HIST_FILE = "gaa_history.csv"

# ─────────────────────────────────────────  Hilfsfunktionen  ───────────── #
def _utc(idx):
    return pd.to_datetime(idx, utc=True).tz_convert(None)

def _fetch_all() -> pd.DataFrame:
    h = yq.Ticker(" ".join(TICKERS)).history(period="2y", interval="1d")["close"]
    if isinstance(h.index, pd.MultiIndex):
        h = h.reset_index().pivot(index="date", columns="symbol", values="close")
    else:
        h = h.to_frame().rename(columns={"close": h.name})
    h = h.rename(columns=REN_MAP)
    h.index = _utc(h.index)
    return h.sort_index()

def _last_price_and_sma(hist: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    prices, smas = {}, {}
    for col, s in hist.items():
        s = s.dropna()
        if len(s) < SMA_LEN:
            continue
        prices[col] = s.iloc[-1]
        smas[col]   = s.rolling(SMA_LEN).mean().iloc[-1]
    return pd.Series(prices), pd.Series(smas)

def _name(lst):            # Klartext‑Helfer
    return ", ".join(NAMES.get(x, x) for x in lst) if lst else "–"

# ─────────────────────────────────────────  Hauptfunktion  ─────────────── #
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    hist = _fetch_all()
    if hist.dropna(how="all").empty:
        return "GAA‑Fehler", None, "Keine Kursdaten verfügbar."

    # Momentum‑Score
    mon = hist.resample("M").last()
    mom = (mon.pct_change(1).iloc[-1] +
           mon.pct_change(3).iloc[-1] +
           mon.pct_change(6).iloc[-1] +
           mon.pct_change(9).iloc[-1]).dropna()

    price, sma = _last_price_and_sma(hist)
    eligible   = mom.index[(price > sma) & sma.notna()]

    if eligible.empty:
        top  = pd.Series(dtype=float)
        hold: List[str] = []
    else:
        top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
        hold = list(top.index)

    # Vorheriges Portfolio aus History
    prev: List[str] = []
    if os.path.exists(HIST_FILE) and os.path.getsize(HIST_FILE):
        prev_str = open(HIST_FILE).read().strip().splitlines()[-1].split(";")[1]
        prev = prev_str.split(",") if prev_str else []

    # Klassifizieren
    buys   = sorted(set(hold)  - set(prev))
    sells  = sorted(set(prev)  - set(hold))
    holds  = sorted(set(prev) & set(hold))

    # History nur bei Wechsel updaten
    if buys or sells:
        today = pd.Timestamp.utcnow()
        m_end = today.to_period("M").to_timestamp("M")
        with open(HIST_FILE, "a") as f:
            f.write(f"{m_end:%F};{','.join(hold)}\n")

    # ── Discord‑Nachricht ────────────────────────────────────────────────
    m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
    subj  = f"GAA Rebalance ({m_end:%b %Y})"
    body  = []

    if buys:
        body.append(f"Kaufen: {_name(buys)}")
    if sells:
        body.append(f"Verkaufen: {_name(sells)}")
    if holds:
        body.append(f"Halten: {_name(holds)}")
    if not hold:                                # Cash‑Portfolio
        body.append("Cash halten")

    body.append(f"Aktuelles Portfolio: {_name(hold)}")
    body.append("")
    body.append("Momentum‑Scores:")
    if top.empty:
        body.append("Keine eligible Assets")
    else:
        body += [f"{NAMES.get(t,t)}: {sc:+.2%}" for t, sc in top.items()]

    return subj, "", "\n".join(body)
