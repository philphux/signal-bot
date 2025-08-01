"""
Global-Asset-Allocation – Momentum
==================================
Universum      : BTC-USD, QQQ, GLD, USO, EEM, FEZ, IEF  
MomentumScore  : Σ %-Rendite (1 M + 3 M + 6 M + 9 M)  
Filter         : Schlusskurs > SMA150 (Adj Close)  
Rebalance      : Monatsultimo • Top-3 • Lücken = Cash  
History-File   : gaa_history.csv   (Schreibt nur, wenn sich das
                 Portfolio ändert oder beim 1. Lauf)
"""

from __future__ import annotations
import os, warnings
from typing import List, Tuple

import pandas as pd
import yahooquery as yq           # primäre Datenquelle
import yfinance as yf             # Fallback

# ────────── Parameter ────────────────────────────────────────────────
TICKERS: list[str] = [
    "BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"
]
NAMES = {
    "BTC-USD": "Bitcoin",        "QQQ": "Nasdaq-100",
    "GLD": "Gold",               "USO": "WTI Crude Oil",
    "EEM": "Emerging Markets",   "FEZ": "Euro Stoxx 50",
    "IEF": "Treasury Bonds",     "CASH": "Cash"
}
TOP_N, SMA_LEN = 3, 150
HIST_FILE = "gaa_history.csv"

nice = lambda xs: ", ".join(NAMES.get(x, x) for x in xs) if xs else "–"

# ────────── Data-Helper ──────────────────────────────────────────────
def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
    """UTC-DatetimeIndex → tz-naiv, sortiert."""
    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    df  = df[idx.notna()].copy()
    df.index = idx[idx.notna()].tz_convert(None)
    return df.sort_index()

def _fetch_daily() -> pd.DataFrame:
    """
    Liefert 2 Jahre Tages-Adj-Close (tz-naiv).  
    1️⃣ yahooquery – schneller, präziser  
    2️⃣ yfinance  – Fallback bei Problemen
    """
    # ---------- 1) yahooquery ----------
    try:
        tq  = yq.Ticker(" ".join(TICKERS))
        raw = tq.history(period="2y", interval="1d", adj_ohlc=True)
        if isinstance(raw.index, pd.MultiIndex):
            df = (raw.reset_index()
                      .pivot(index="date", columns="symbol",
                             values="adjclose"))
        else:
            df = raw["adjclose"]
        if not df.isna().all().all():          # Daten vorhanden
            return _to_dt(df)
    except Exception:
        pass                                   # gehe zu Fallback

    # ---------- 2) yfinance ----------
    raw = yf.download(
        tickers=" ".join(TICKERS),
        period="2y",
        interval="1d",
        auto_adjust=False,      # wir wählen selbst Spalte
        progress=False,
        threads=False,
    )

    if isinstance(raw.columns, pd.MultiIndex):
        df = raw.xs("Adj Close", level=1, axis=1)
        if df.isna().all().all():              # kein Adj Close → Close
            df = raw.xs("Close", level=1, axis=1)
    else:
        df = raw["Adj Close"] if "Adj Close" in raw else raw["Close"]

    return _to_dt(df)

def _monthly_close(daily: pd.DataFrame) -> pd.DataFrame:
    """Letzter Handelstag je Monat (Excel/TradingView-kompatibel)."""
    return daily.resample("M").last()

# ────────── Strategie ────────────────────────────────────────────────
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    # -------- Tagesdaten + SMA --------------------------------------
    daily = _fetch_daily()
    if daily.empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    daily = daily.ffill()               # fehlende Handelstage füllen
    mon   = _monthly_close(daily)

    # Laufenden Monat ergänzen, falls noch kein Monats-Close existiert
    if mon.index[-1].month != pd.Timestamp.utcnow().month:
        mon.loc[daily.index[-1]] = daily.iloc[-1]

    # -------- Momentum-Score ----------------------------------------
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "The default fill_method")
        mom = (
            (mon / mon.shift(1) - 1) +
            (mon / mon.shift(3) - 1) +
            (mon / mon.shift(6) - 1) +
            (mon / mon.shift(9) - 1)
        ).iloc[-1].dropna()

    price = daily.iloc[-1]
    sma   = daily.rolling(SMA_LEN).mean().iloc[-1]

    eligible = mom.index[(price > sma) & (price.notna()) & (sma.notna())]

    top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold = list(top.index) + ["CASH"] * (TOP_N - len(top))

    # -------- History-Handling --------------------------------------
    prev: List[str] = []
    if os.path.isfile(HIST_FILE) and os.path.getsize(HIST_FILE):
        prev = (
            open(HIST_FILE)
            .read().splitlines()[-1]
            .split(";")[1].split(",")
        )

    buys  = sorted([x for x in hold if hold.count(x) > prev.count(x)])
    sells = sorted([x for x in prev if prev.count(x) > hold.count(x)])
    holds = sorted(set(hold) & set(prev))

    if buys or sells or not prev:
        m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
        with open(HIST_FILE, "a") as f:
            if os.path.getsize(HIST_FILE) == 0:
                f.write("date;portfolio\n")
            f.write(f"{m_end:%F};{','.join(hold)}\n")

    # -------- Discord-Nachricht -------------------------------------
    m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
    parts: List[str] = []
    if buys:  parts.append(f"Kaufen: {nice(buys)}")
    if sells: parts.append(f"Verkaufen: {nice(sells)}")
    if holds: parts.append(f"Halten: {nice(holds)}")
    if not (buys or sells or holds):
        parts.append("Cash halten")

    parts.append(f"Aktuelles Portfolio: {nice(hold)}\n")
    parts.append("Momentum-Scores:")
    for t, sc in top.items():
        parts.append(f"{NAMES.get(t,t)}: {sc:+.2%}")

    subject = f"GAA Rebalance ({m_end:%b %Y})"
    return subject, "", "\n".join(parts)
