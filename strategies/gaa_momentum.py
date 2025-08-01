# strategies/gaa_momentum.py
"""
Global-Asset-Allocation – Momentum (Top-3 + Cash-Fallback)
=========================================================

Universum   : BTC-USD, QQQ, GLD, USO, EEM, FEZ, IEF  
Momentum    : Σ %-Rendite (1 M + 3 M + 6 M + 9 M)  
Filter      : Schlusskurs > SMA150  
Rebalance   : Monatsultimo, Lücken = Cash  
History-CSV : gaa_history.csv
"""

from __future__ import annotations
import os, warnings
from typing import List, Tuple

import pandas as pd
import yahooquery as yq
import yfinance   as yf


# ────────── Parameter ───────────────────────────────────────────────
TICKERS = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]
NAMES   = {
    "BTC-USD": "Bitcoin",       "QQQ": "Nasdaq-100",
    "GLD": "Gold",              "USO": "WTI Crude Oil",
    "EEM": "Emerging Markets",  "FEZ": "Euro Stoxx 50",
    "IEF": "Treasury Bonds",    "CASH": "Cash",
}
TOP_N, SMA_LEN  = 3, 150
SHOW_TOP_MOM    = 5                      # wie viele Scores anzeigen
HIST_FILE       = "gaa_history.csv"
nice = lambda xs: ", ".join(NAMES.get(x, x) for x in xs) if xs else "–"


# ────────── Helper ──────────────────────────────────────────────────
def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.to_datetime(df.index, utc=True, errors="coerce")
    df  = df[idx.notna()].copy()
    df.index = idx[idx.notna()].tz_convert(None)
    return df.sort_index()


def _pivot_raw(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Variante 2: Adj Close **und** Close laden  
    → NaNs in Adj Close werden mit entsprechendem Close-Wert gefüllt.
    Danach sind alle Reihen adjustiert UND lückenlos.
    """
    # Einzelticker-Download → einfache Spalte
    if not isinstance(raw.columns, pd.MultiIndex):
        col = "Adj Close" if "Adj Close" in raw else "Close"
        return raw[[col]].rename(columns={col: TICKERS[0]})

    # Multi-Index angleichen: Ebene-0 = Felder („Adj Close“, „Close“ …)
    if raw.columns.get_level_values(0)[0] in TICKERS:
        raw = raw.swaplevel(0, 1, axis=1)

    adj = raw.xs("Adj Close", level=0, axis=1)
    cls = raw.xs("Close",     level=0, axis=1)

    # Fehlende Adj-Werte mit Close füllen
    filled = adj.where(~adj.isna(), cls)
    filled.columns.name = None
    return filled


def _fetch_daily() -> pd.DataFrame:
    """2 Jahre Tagesdaten – yahooquery (adj_ohlc) → yfinance Fallback"""
    try:
        tq  = yq.Ticker(" ".join(TICKERS))
        raw = tq.history(period="2y", interval="1d", adj_ohlc=True)
        if isinstance(raw.index, pd.MultiIndex):
            df = (raw.reset_index()
                    .pivot(index="date", columns="symbol",
                           values="adjclose"))
        else:
            df = raw["adjclose"]
        if not df.isna().all().all():
            return _to_dt(df)
    except Exception:
        pass

    raw = yf.download(" ".join(TICKERS), period="2y", interval="1d",
                      auto_adjust=False, progress=False, threads=False)
    return _to_dt(_pivot_raw(raw))


def _monthly_close(daily: pd.DataFrame) -> pd.DataFrame:
    """Monats-Schluss = letzter vorhandener Kurs innerhalb des Monats."""
    return daily.resample("M").apply(lambda x: x.iloc[-1])


# ────────── Strategie ───────────────────────────────────────────────
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    daily = _fetch_daily().ffill()
    if daily.empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    mon   = _monthly_close(daily)

    # Momentum-Score = Σ (1 M, 3 M, 6 M, 9 M)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "The default fill_method")
        mom = (
            (mon / mon.shift(1) - 1) +
            (mon / mon.shift(3) - 1) +
            (mon / mon.shift(6) - 1) +
            (mon / mon.shift(9) - 1)
        ).iloc[-1].dropna()

    price     = daily.iloc[-1]
    sma       = daily.rolling(SMA_LEN).mean().iloc[-1]
    eligible  = mom.index[(price > sma) & price.notna() & sma.notna()]

    top       = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold      = list(top.index) + ["CASH"] * (TOP_N - len(top))

    # ── History ────────────────────────────────────────────────────
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

    # ── Discord-Text ──────────────────────────────────────────────
    m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
    txt: List[str] = []
    if buys:  txt.append(f"Kaufen: {nice(buys)}")
    if sells: txt.append(f"Verkaufen: {nice(sells)}")
    if holds: txt.append(f"Halten: {nice(holds)}")
    if not (buys or sells or holds):
        txt.append("Cash halten")

    txt.append(f"Aktuelles Portfolio: {nice(hold)}\n")
    txt.append("Momentum-Scores (Top 5, ungefiltert):")
    for t, sc in mom.sort_values(ascending=False).head(SHOW_TOP_MOM).items():
        mark = "  ⬅︎ SMA ok" if t in eligible else ""
        txt.append(f"{NAMES.get(t,t)}: {sc:+.2%}{mark}")

    subject = f"GAA Rebalance ({m_end:%b %Y})"
    return subject, "", "\n".join(txt)
