"""
Global Asset Allocation – Momentum (ETF-Version, Monats-Stichtag)

• Universum
    BTC-USD  Bitcoin
    QQQ      Invesco Nasdaq-100
    GLD      SPDR Gold Shares
    USO      US Oil Fund (WTI)
    EEM      iShares MSCI Emerging Markets
    FEZ      SPDR Euro Stoxx 50
    IEF      iShares 7-10 y Treasury Bonds

• Momentum-Score = 1 M + 3 M + 6 M + 9 M Rendite
    – Grundlage sind **Monats-Schlusskurse** (identisch zu Excel)  
    – Zeitzone vor Resampling zu „US/Eastern“, damit der Monats-EoD  
      bei 24 × 7-Assets (BTC-USD) nicht in den Folgetag rutscht

• Filter   = Schlusskurs > SMA 150  
• Rebal    = Monatsende; Top-3, fehlende Slots mit „CASH“ gefüllt  
• History  = gaa_history.csv (wird nur angehängt, wenn Portfolio wechselt)

Nachricht listet Kaufen / Verkaufen / Halten plus Momentum-Scores.
"""

from __future__ import annotations
import os
from typing import List, Tuple
import pandas as pd, yahooquery as yq

# ───────── Universum & Namen ───────── #
TICKERS = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]

NAMES = {
    "BTC-USD": "Bitcoin",
    "QQQ":     "Nasdaq-100",
    "GLD":     "Gold",
    "USO":     "WTI Crude Oil",
    "EEM":     "Emerging Markets",
    "FEZ":     "Euro Stoxx 50",
    "IEF":     "Treasury Bonds",
    "CASH":    "Cash",
}

TOP_N, SMA_LEN = 3, 150
HIST_FILE      = "gaa_history.csv"

# ───────── Hilfsfunktionen ─────────── #
def _utc(idx):               # tz-aware → UTC → tz-naiv
    return pd.to_datetime(idx, utc=True).tz_convert(None)

def _fetch_all() -> pd.DataFrame:
    """Lädt alle Ticker (Adj Close) → Date×Ticker-Matrix"""
    h = yq.Ticker(" ".join(TICKERS)).history(period="2y", interval="1d")["close"]
    if isinstance(h.index, pd.MultiIndex):
        h = h.reset_index().pivot(index="date", columns="symbol", values="close")
    else:
        h = h.to_frame().rename(columns={"close": h.name})
    h.index = _utc(h.index)
    return h.sort_index()

def _last_price_and_sma(hist: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    p, s = {}, {}
    for tkr, col in hist.items():
        col = col.dropna()
        if len(col) < SMA_LEN:
            continue
        p[tkr] = col.iloc[-1]
        s[tkr] = col.rolling(SMA_LEN).mean().iloc[-1]
    return pd.Series(p), pd.Series(s)

def _name(seq: List[str]) -> str:
    return ", ".join(NAMES.get(x, x) for x in seq) if seq else "–"

# ───────── Hauptfunktion ──────────── #
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    hist = _fetch_all()
    if hist.dropna(how="all").empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    # Monats-Schluss (NY-Zeit zur Sicherheit)
    hist.index = hist.index.tz_localize("UTC").tz_convert("US/Eastern")
    mon = hist.resample("M").last()
    hist.index = hist.index.tz_convert(None)        # zurück tz-naiv

    mom = (
        mon.pct_change(1).iloc[-1] +
        mon.pct_change(3).iloc[-1] +
        mon.pct_change(6).iloc[-1] +
        mon.pct_change(9).iloc[-1]
    ).dropna()

    price, sma = _last_price_and_sma(hist)
    eligible   = mom.index[(price > sma) & sma.notna()]

    top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold = list(top.index)
    if len(hold) < TOP_N:
        hold.extend(["CASH"] * (TOP_N - len(hold)))

    # Vorheriges Portfolio aus History
    prev: List[str] = []
    if os.path.exists(HIST_FILE) and os.path.getsize(HIST_FILE):
        prev_line = open(HIST_FILE).read().strip().splitlines()[-1]
        prev = prev_line.split(";")[1].split(",") if ";" in prev_line else []

    # Differenzen
    buys  = sorted([x for x in hold if hold.count(x) > prev.count(x)])
    sells = sorted([x for x in prev if prev.count(x) > hold.count(x)])
    holds = sorted(set(hold) & set(prev))

    # History nur bei Wechsel
    if buys or sells or not prev:
        today = pd.Timestamp.utcnow()
        m_end = today.to_period("M").to_timestamp("M")
        header_needed = not os.path.exists(HIST_FILE)
        with open(HIST_FILE, "a") as f:
            if header_needed:
                f.write("date;portfolio\n")
            f.write(f"{m_end:%F};{','.join(hold)}\n")

    # ─── Discord-Nachricht ────────────────────────────────────────────
    m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
    subj  = f"GAA Rebalance ({m_end:%b %Y})"
    body  = []
    if buys:
        body.append(f"Kaufen: {_name(buys)}")
    if sells:
        body.append(f"Verkaufen: {_name(sells)}")
    if holds:
        body.append(f"Halten: {_name(holds)}")
    if not buys and not sells and not holds:
        body.append("Cash halten")

    body.append(f"Aktuelles Portfolio: {_name(hold)}")
    body.append("")
    body.append("Momentum-Scores:")
    if top.empty:
        body.append("Keine eligible Assets")
    else:
        body += [f"{NAMES.get(t,t)}: {sc:+.2%}" for t, sc in top.items()]

    return subj, "", "\n".join(body)
