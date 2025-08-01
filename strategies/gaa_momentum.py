"""
Global-Asset-Allocation – Momentum  (Top-3, Cash-Fallback)
"""
from __future__ import annotations
import os, warnings
from typing import List, Tuple
import pandas as pd, yahooquery as yq, yfinance as yf

# ───────── Parameter ────────────────────────────────────────────────
TICKERS = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]
NAMES   = {
    "BTC-USD": "Bitcoin", "QQQ": "Nasdaq-100", "GLD": "Gold",
    "USO": "WTI Crude Oil", "EEM": "Emerging Markets", "FEZ": "Euro Stoxx 50",
    "IEF": "Treasury Bonds", "CASH": "Cash"
}
TOP_N, SMA_LEN = 3, 150
HIST_FILE      = "gaa_history.csv"
nice = lambda xs: ", ".join(NAMES.get(x, x) for x in xs) if xs else "–"

# ───────── Helper ───────────────────────────────────────────────────
def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    df  = df[idx.notna()].copy()
    df.index = idx[idx.notna()].tz_convert(None)
    return df.sort_index()

def _pivot_raw(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Wandelt das von yfinance zurückgelieferte Multi-Index-DataFrame
    (egal ob Variante A oder B) in ein einfaches Adj-Close-DataFrame
    mit Spalten = Ticker.
    """
    if not isinstance(raw.columns, pd.MultiIndex):
        # Single-Ticker-Download   -> einfacher Index
        col = "Adj Close" if "Adj Close" in raw else "Close"
        return raw[[col]].rename(columns={col: TICKERS[0]})

    # Multi-Ticker: erst prüfen, ob Ebene-0 oder Ebene-1 das OHLCV-Label enthält
    lvl0 = raw.columns.get_level_values(0)
    lvl1 = raw.columns.get_level_values(1)

    if "Adj Close" in lvl0:
        df = raw.xs("Adj Close", level=0, axis=1)
    elif "Adj Close" in lvl1:
        df = raw.xs("Adj Close", level=1, axis=1)
    elif "Close" in lvl0:
        df = raw.xs("Close", level=0, axis=1)
    else:                                   # Fallback
        df = raw.xs("Close", level=1, axis=1)

    return df

def _fetch_daily() -> pd.DataFrame:
    """2 Jahre Tages-Adj-Close (yahooquery ➜ yfinance Fallback)."""
    # ---------- 1) yahooquery ----------
    try:
        tq  = yq.Ticker(" ".join(TICKERS))
        raw = tq.history(period="2y", interval="1d", adj_ohlc=True)
        if isinstance(raw.index, pd.MultiIndex):
            df = (raw.reset_index()
                    .pivot(index="date", columns="symbol", values="adjclose"))
        else:
            df = raw["adjclose"]
        if not df.isna().all().all():
            return _to_dt(df)
    except Exception:
        pass

    # ---------- 2) yfinance ----------
    raw = yf.download(
        tickers=" ".join(TICKERS),
        period="2y",
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    return _to_dt(_pivot_raw(raw))

def _monthly_close(daily: pd.DataFrame) -> pd.DataFrame:
    """Letzter Handelstag pro Monat (Excel-/TradingView-kompatibel)."""
    return daily.resample("M").last()

# ───────── Strategie ────────────────────────────────────────────────
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    daily = _fetch_daily().ffill()
    if daily.empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    mon = _monthly_close(daily)
    if mon.index[-1].month != pd.Timestamp.utcnow().month:
        mon.loc[daily.index[-1]] = daily.iloc[-1]

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

    eligible = mom.index[(price > sma) & price.notna() & sma.notna()]
    top      = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold     = list(top.index) + ["CASH"] * (TOP_N - len(top))

    # ---------- History --------------------------------------------
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

    # ---------- Discord-Text ---------------------------------------
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
