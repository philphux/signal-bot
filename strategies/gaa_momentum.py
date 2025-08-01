"""
Global-Asset-Allocation – Momentum (kompakt)
===========================================

Universum ⋯ BTC-USD, QQQ, GLD, USO, EEM, FEZ, IEF
Momentum  ⋯ 1 M + 3 M + 6 M + 9 M (%-Rendite, Monats-Close)
Filter    ⋯ Preis > SMA150  |  ≤ TOP_N sonst Cash
Rebalance ⋯ Monatsultimo
History   ⋯ gaa_history.csv  (wird nur ergänzt, wenn sich das Portfolio
            ändert oder beim ersten Lauf)
"""

from __future__ import annotations
import os, warnings
from typing import List, Tuple

import pandas as pd
import yahooquery as yq
import yfinance as yf

# ───────────── Parameter ────────────────────────────────────────────
TICKERS = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]
NAMES   = {
    "BTC-USD": "Bitcoin",        "QQQ": "Nasdaq-100",
    "GLD":     "Gold",           "USO": "WTI Crude Oil",
    "EEM":     "Emerging Markets","FEZ": "Euro Stoxx 50",
    "IEF":     "Treasury Bonds", "CASH": "Cash",
}
TOP_N, SMA_LEN = 3, 150
HIST_FILE      = "gaa_history.csv"

nice = lambda xs: ", ".join(NAMES.get(x, x) for x in xs) if xs else "–"

# ───────────── Helper ───────────────────────────────────────────────
def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    df  = df.loc[~idx.isna()].copy()
    df.index = idx[~idx.isna()].tz_convert(None)        # tz-naiv
    return df.sort_index()

def _fetch_daily() -> pd.DataFrame:
    """2 Jahre Tagesdaten (Adj Close) – yahooquery → yfinance Fallback."""
    try:                                    # yahooquery-Weg
        tq = yq.Ticker(" ".join(TICKERS))
        df = tq.history(period="2y", interval="1d", adj_ohlc=True)
        # Multi-Index → Pivot
        if isinstance(df.index, pd.MultiIndex):
            df = (
                df.reset_index()
                  .pivot(index="date", columns="symbol", values="adjclose")
            )
        # Manche Symbole haben keine AdjClose → Close
        if df.isna().all().all():           # komplett fehlgeschlagen
            raise ValueError("adjclose leer")
        if "adjclose" in df.columns:        # falls pivot nicht nötig war
            df = df["adjclose"]
    except Exception:
        # ---------- yfinance Fallback ----------
        df = yf.download(
            tickers=" ".join(TICKERS),
            period="2y",
            interval="1d",
            auto_adjust=True,
            progress=False
        )["Adj Close"]
    # einheitliches Datetime-Index
    return _to_dt(df)

def _monthly_close(daily: pd.DataFrame) -> pd.DataFrame:
    """Letzter Handelstag pro Monat (Excel-/TradingView-kompatibel)."""
    return daily.resample("M").last()

# ───────────── Strategie ────────────────────────────────────────────
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    daily = _fetch_daily()
    if daily.empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    daily = daily.ffill()                  # Lücken auffüllen
    mon   = _monthly_close(daily)

    # Laufender Monat ergänzen, falls noch kein Monats-Close
    if mon.index[-1].month != pd.Timestamp.utcnow().month:
        mon.loc[daily.index[-1]] = daily.iloc[-1]

    # ── Momentum 1 + 3 + 6 + 9 M ────────────────────────────────────
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

    eligible = mom.index[(price > sma) & ~mom.isna()]
    top      = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold     = list(top.index) + ["CASH"] * (TOP_N - len(top))

    # ── History laden ───────────────────────────────────────────────
    prev: List[str] = []
    if os.path.isfile(HIST_FILE) and os.path.getsize(HIST_FILE):
        prev = (
            open(HIST_FILE).read().splitlines()[-1]
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

    # ── Discord-Nachricht ───────────────────────────────────────────
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
