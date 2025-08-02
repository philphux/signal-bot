# strategies/gaa_momentum.py
"""
Global-Asset-Allocation – Monthly Momentum (Top-3 + Cash, Debug)
================================================================
• Momentum-Berechnung   : CLOSE-Kurse
• SMA-150-Filter        : ADJ CLOSE-Kurse
————————————————————————————————————————————————————————————————————
Universum  : BTC-USD, QQQ, GLD, USO, EEM, FEZ, IEF
Momentum   : Σ %-Rendite (1 M + 3 M + 6 M + 9 M)
Filter     : Schlusskurs > SMA150
Rebalance  : Monatsultimo (letzter realer Handelstag)
History    : gaa_history.csv
"""

from __future__ import annotations
import os, sys, warnings
from typing import List, Tuple
import pandas as pd
import yahooquery as yq
import yfinance   as yf


# ───────────────────────── Parameter ───────────────────────────────
TICKERS        = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]
NAMES          = {
    "BTC-USD": "Bitcoin",       "QQQ": "Nasdaq-100",
    "GLD": "Gold",              "USO": "WTI Crude Oil",
    "EEM": "Emerging Markets",  "FEZ": "Euro Stoxx 50",
    "IEF": "Treasury Bonds",    "CASH": "Cash",
}
TOP_N, SMA_LEN = 3, 150
SHOW_TOP_MOM   = 5
HIST_FILE      = "gaa_history.csv"

nice = lambda xs: ", ".join(NAMES.get(x, x) for x in xs) if xs else "–"
dbg  = lambda *a, **k: print(*a, **k, file=sys.stdout, flush=True)


# ───────────────────────── Helper ──────────────────────────────────
def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.to_datetime(df.index, utc=True, errors="coerce")
    df  = df[idx.notna()].copy()
    df.index = idx[idx.notna()].tz_convert(None)   # tz-naiv
    return df.sort_index()


# ---------- Download-Funktionen -----------------------------------
def _fetch_close() -> pd.DataFrame:
    """2 J Close-Kurse – yahooquery → yfinance (Close)."""
    try:
        tq  = yq.Ticker(" ".join(TICKERS))
        raw = tq.history(period="2y", interval="1d")
        if isinstance(raw.index, pd.MultiIndex):
            df = (raw.reset_index()
                    .pivot(index="date", columns="symbol", values="close"))
        else:
            df = raw["close"]
        if not df.isna().all().all():
            dbg("== yahooquery CLOSE ==")
            return _to_dt(df)
    except Exception as e:
        dbg("yahooquery-Close-Fehler:", e)

    dbg("== yfinance CLOSE fallback ==")
    raw = yf.download(" ".join(TICKERS), period="2y", interval="1d",
                      auto_adjust=False, progress=False, threads=False)
    if isinstance(raw.columns, pd.MultiIndex):
        if raw.columns.get_level_values(0)[0] in TICKERS:
            raw = raw.swaplevel(0, 1, axis=1)
        df = raw.xs("Close", level=0, axis=1)
    else:
        df = raw[["Close"]].rename(columns={"Close": TICKERS[0]})
    return _to_dt(df)


def _fetch_adj() -> pd.DataFrame:
    """2 J Adj Close-Kurse – yahooquery → yfinance."""
    try:
        tq  = yq.Ticker(" ".join(TICKERS))
        raw = tq.history(period="2y", interval="1d", adj_ohlc=True)
        if isinstance(raw.index, pd.MultiIndex):
            df = (raw.reset_index()
                    .pivot(index="date", columns="symbol", values="adjclose"))
        else:
            df = raw["adjclose"]
        if not df.isna().all().all():
            dbg("== yahooquery ADJ CLOSE ==")
            return _to_dt(df)
    except Exception as e:
        dbg("yahooquery-Adj-Fehler:", e)

    dbg("== yfinance ADJ CLOSE fallback ==")
    raw = yf.download(" ".join(TICKERS), period="2y", interval="1d",
                      auto_adjust=False, progress=False, threads=False)
    if isinstance(raw.columns, pd.MultiIndex):
        if raw.columns.get_level_values(0)[0] in TICKERS:
            raw = raw.swaplevel(0, 1, axis=1)
        df = raw.xs("Adj Close", level=0, axis=1)
    else:
        df = raw[["Adj Close"]].rename(columns={"Adj Close": TICKERS[0]})
    return _to_dt(df)


def _monthly_close(d: pd.DataFrame) -> pd.DataFrame:
    """Monats-Close = letzter realer Handelstag (max 3 Tage ffill)."""
    filled = d.ffill(limit=3)
    mon    = filled.resample("ME").last()
    today  = pd.Timestamp.utcnow().tz_convert(None).normalize()
    return mon[mon.index <= today]


# ───────────────────── Strategie-Funktion ──────────────────────────
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    # ---------- Daten ------------------------------------------------
    close_daily = _fetch_close().ffill()      # Momentum-Reihe
    adj_daily   = _fetch_adj().ffill()        # SMA-Reihe

    if close_daily.empty or adj_daily.empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    mon_close = _monthly_close(close_daily)

    # ---------- Momentum --------------------------------------------
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "The default fill_method")
        mom = ((mon_close / mon_close.shift(1) - 1) +
               (mon_close / mon_close.shift(3) - 1) +
               (mon_close / mon_close.shift(6) - 1) +
               (mon_close / mon_close.shift(9) - 1)).iloc[-1]

    price = close_daily.iloc[-1]                         # letzter Close
    sma   = adj_daily.rolling(SMA_LEN, min_periods=1).mean().iloc[-1]  # Adj-SMA

    dbg("\n=== Debug ===")
    dbg(pd.DataFrame({"price": price, "SMA150": sma,
                      "mom%": mom*100, "SMA_ok": price > sma}))

    eligible = mom.index[(price > sma) & mom.notna()]
    top      = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold     = list(top.index) + ["CASH"] * (TOP_N - len(top))

    # ---------- History ---------------------------------------------
    prev: List[str] = []
    if os.path.isfile(HIST_FILE) and os.path.getsize(HIST_FILE):
        prev = open(HIST_FILE).read().splitlines()[-1].split(";")[1].split(",")

    buys  = sorted([x for x in hold if hold.count(x) > prev.count(x)])
    sells = sorted([x for x in prev if prev.count(x) > hold.count(x)])
    holds = sorted(set(hold) & set(prev))

    if buys or sells or not prev:
        stamp = pd.Timestamp.utcnow().tz_convert(None).to_period("M").to_timestamp("M")
        with open(HIST_FILE, "a") as f:
            if os.path.getsize(HIST_FILE) == 0:
                f.write("date;portfolio\n")
            f.write(f"{stamp:%F};{','.join(hold)}\n")

    # ---------- Discord-Text ----------------------------------------
    stamp = pd.Timestamp.utcnow().tz_convert(None).to_period("M").to_timestamp("M")
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
        txt.append(f"{NAMES.get(t, t)}: {sc:+.2%}{mark}")

    missed = [t for t in mom.sort_values(ascending=False).head(10).index
              if t not in eligible]
    if missed:
        txt.append("\nNicht berücksichtigt (SMA oder NaN): " + nice(missed[:5]))

    return f"GAA Rebalance ({stamp:%b %Y})", "", "\n".join(txt)
