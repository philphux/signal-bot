"""
Global-Asset-Allocation – Momentum
----------------------------------
• Universum        BTC-USD, QQQ, GLD, USO, EEM, FEZ, IEF
• Momentum-Score   Σ Rendite(1 + 3 + 6 + 9 Monate)
  – Monats-Schluss (letzter Handelstag des Monats)
• Filter           Schlusskurs > SMA150 (täglicher Adj-Close)
• Rebalance        Monatsultimo, Top-3; fehlende Positionen = Cash
• History          gaa_history.csv  (wird nur ergänzt, wenn Portfolio
  sich ändert)
"""

from __future__ import annotations
import os, warnings
from typing import List, Tuple

import pandas as pd
import yahooquery as yq

# ───────── Parameter ────────────────────────────────────────────────
TICKERS = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]
NAMES   = {
    "BTC-USD": "Bitcoin", "QQQ": "Nasdaq-100", "GLD": "Gold",
    "USO": "WTI Crude Oil", "EEM": "Emerging Markets",
    "FEZ": "Euro Stoxx 50", "IEF": "Treasury Bonds",
    "CASH": "Cash",
}
TOP_N, SMA_LEN = 3, 150
HIST_FILE      = "gaa_history.csv"

# ───────── Helper ───────────────────────────────────────────────────
def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    df  = df.loc[~idx.isna()].copy()
    df.index = idx[~idx.isna()].tz_convert(None)        # tz-naiv
    return df.sort_index()

def _fetch_daily() -> pd.DataFrame:
    tq = yq.Ticker(" ".join(TICKERS))
    df = tq.history(period="2y", interval="1d", adj_ohlc=True)["adjclose"]
    if isinstance(df.index, pd.MultiIndex):
        df = (df.reset_index()
                .pivot(index="date", columns="symbol", values="adjclose"))
    return _to_dt(df)

def _monthly_close(daily: pd.DataFrame) -> pd.DataFrame:
    """Letzter Handelstag pro Monat (wie Excel/TradingView)."""
    return daily.resample("M").last()

nice = lambda xs: ", ".join(NAMES.get(x, x) for x in xs) if xs else "–"

# ───────── Strategie ────────────────────────────────────────────────
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    # ---- Tagesdaten für SMA ----------------------------------------
    daily = _fetch_daily().ffill()
    daily = daily.dropna(how="all")          # WE ohne Kurse löschen
    if daily.empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    # ---- Monats-Schlusskurse ---------------------------------------
    mon = _monthly_close(daily)
    if mon.index[-1].month != pd.Timestamp.utcnow().month:
        # Monatsbalken für laufenden Monat ergänzen
        mon.loc[daily.index[-1]] = daily.iloc[-1]

    # ---- Momentum = Σ Prozent-Renditen -----------------------------
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

    # ---- Debug -----------------------------------------------------
    dbg = pd.DataFrame({
        "momentum%": (mom * 100).round(2),
        "price": price,
        "SMA150": sma,
        "diff%": ((price / sma - 1) * 100).round(2),
        "≥150d": daily.notna().sum() >= SMA_LEN,
        "SMA_OK": price > sma,
    })
    print("\n=== DEBUG – Price vs SMA150 ===")
    print(dbg.to_string(float_format="%.2f"))
    print("================================\n")

    # ---- Eligible & Top-N -----------------------------------------
    eligible = dbg.index[(dbg["≥150d"]) & (dbg["SMA_OK"])]
    top      = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold     = list(top.index) + ["CASH"] * (TOP_N - len(top))

    # ---- History ---------------------------------------------------
    prev: List[str] = []
    if os.path.isfile(HIST_FILE) and os.path.getsize(HIST_FILE):
        prev = open(HIST_FILE).read().splitlines()[-1].split(";")[1].split(",")

    buys  = sorted([x for x in hold if hold.count(x) > prev.count(x)])
    sells = sorted([x for x in prev if prev.count(x) > hold.count(x)])
    holds = sorted(set(hold) & set(prev))

    if buys or sells or not prev:
        m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
        with open(HIST_FILE, "a") as f:
            if os.path.getsize(HIST_FILE) == 0:
                f.write("date;portfolio\n")
            f.write(f"{m_end:%F};{','.join(hold)}\n")

    # ---- Discord-Nachricht ----------------------------------------
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
