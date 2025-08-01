"""
Global Asset Allocation – Momentum
(ETF-basierte Version, Cash-Padding auf 3 Slots)

Universum (7 Tickers)
  BTC-USD : Bitcoin-Spot (Coinbase feed)
  QQQ     : Invesco Nasdaq-100 ETF
  GLD     : SPDR Gold Shares
  USO     : United States Oil Fund (WTI Crude Oil)
  EEM     : iShares MSCI Emerging Markets
  FEZ     : SPDR Euro Stoxx 50
  IEF     : iShares 7-10y Treasury Bonds

Momentum-Score = 1 M + 3 M + 6 M + 9 M Rendite  
Filter         = Schlusskurs > SMA 150  
Rebalance      = Monatsende; Top-3, fehlende Slots = Cash  
History        = gaa_history.csv (nur, wenn Portfolio wechselt)

Jede Meldung listet Kaufen / Verkaufen / Halten
sowie Momentum-Scores der Top-Assets.
"""

from __future__ import annotations
import os
from typing import List, Tuple
import pandas as pd, yahooquery as yq

# ─────────── Universum & Klartextnamen ─────────── #
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

# ───────────────────────── Hilfsfunktionen ─────────────────────────────── #
def _utc(idx):
    """tz-aware Index → UTC → tz-naiv"""
    return pd.to_datetime(idx, utc=True).tz_convert(None)

def _fetch_all() -> pd.DataFrame:
    """Lädt alle Ticker in einem Call, gibt Date×Ticker-Matrix zurück"""
    h = yq.Ticker(" ".join(TICKERS)).history(period="2y", interval="1d")["close"]
    if isinstance(h.index, pd.MultiIndex):
        h = (
            h.reset_index()
              .pivot(index="date", columns="symbol", values="close")
        )
    else:  # single-column fallback
        h = h.to_frame().rename(columns={"close": h.name})
    h.index = _utc(h.index)
    return h.sort_index()

def _last_price_and_sma(hist: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """Letzter Schlusskurs & SMA150 je Ticker"""
    p, s = {}, {}
    for t, col in hist.items():
        col = col.dropna()
        if len(col) < SMA_LEN:
            continue
        p[t] = col.iloc[-1]
        s[t] = col.rolling(SMA_LEN).mean().iloc[-1]
    return pd.Series(p), pd.Series(s)

def _name(lst: List[str]) -> str:
    return ", ".join(NAMES.get(x, x) for x in lst) if lst else "–"

# ───────────────────────────── Hauptfunktion ───────────────────────────── #
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    hist = _fetch_all()
    if hist.dropna(how="all").empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    # Momentum-Score (1+3+6+9 Monate)
    mon = hist.resample("M").last()
    mom = (
        mon.pct_change(1).iloc[-1] +
        mon.pct_change(3).iloc[-1] +
        mon.pct_change(6).iloc[-1] +
        mon.pct_change(9).iloc[-1]
    ).dropna()

    price, sma = _last_price_and_sma(hist)
    eligible   = mom.index[(price > sma) & sma.notna()]

    # Top-Liste + Cash-Auffüllung
    top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold = list(top.index)
    if len(hold) < TOP_N:
        hold.extend(["CASH"] * (TOP_N - len(hold)))

    # ───────── Verlauf einlesen ──────────────────────────────────────────
    prev: List[str] = []
    if os.path.exists(HIST_FILE) and os.path.getsize(HIST_FILE):
        prev_line = open(HIST_FILE).read().strip().splitlines()[-1]
        prev = prev_line.split(";")[1].split(",") if ";" in prev_line else []

    # Portfoliodifferenz
    buys  = sorted([x for x in hold if hold.count(x) > prev.count(x)])
    sells = sorted([x for x in prev if prev.count(x) > hold.count(x)])
    holds = sorted(set(hold) & set(prev))

    # Nur History-Update, wenn Portfolio wechselt
    if buys or sells or not prev:
        today = pd.Timestamp.utcnow()
        m_end = today.to_period("M").to_timestamp("M")
        header_needed = not os.path.exists(HIST_FILE)
        with open(HIST_FILE, "a") as f:
            if header_needed:
                f.write("date;portfolio\n")
            f.write(f"{m_end:%F};{','.join(hold)}\n")

    # ───────── Discord-Meldung (immer) ───────────────────────────────────
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
