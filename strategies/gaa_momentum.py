"""
Global Asset Allocation – Momentum (Adj Close, End-of-Month, Cash-Filler)

• Momentum = Σ Renditen 1 / 3 / 6 / 9 Monate (Monats-Schlusskurse)
• Filter   = AdjClose > SMA150 (Tages-AdjClose)
• Rebal    = Monatsende, Top-3, Rest Cash
"""

from __future__ import annotations
import os
from typing import List, Tuple
import pandas as pd, yahooquery as yq

# ---------------- Parameter ------------------------------------------
TICKERS = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]
NAMES   = {
    "BTC-USD": "Bitcoin", "QQQ": "Nasdaq-100", "GLD": "Gold",
    "USO": "WTI Crude Oil", "EEM": "Emerging Markets",
    "FEZ": "Euro Stoxx 50", "IEF": "Treasury Bonds", "CASH": "Cash",
}
TOP_N, SMA_LEN = 3, 150
HIST_FILE      = "gaa_history.csv"

# ---------------- Helper ---------------------------------------------
def _to_dt_index(df: pd.DataFrame) -> pd.DataFrame:
    """Einheitlicher, tz-naiver DatetimeIndex (UTC entfernt)."""
    # alles in UTC-aware konvertieren …
    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    df  = df.copy()
    df.index = idx
    df = df[~df.index.isna()]           # Zeilen ohne Datum verwerfen
    # … dann in tz-naiv umwandeln
    return df.tz_convert(None).sort_index()

def _fetch(interval: str) -> pd.DataFrame:
    tq = yq.Ticker(" ".join(TICKERS))
    df = tq.history(period="2y", interval=interval, adj_ohlc=True)
    df = df["adjclose"] if "adjclose" in df.columns else df["close"]
    if isinstance(df.index, pd.MultiIndex):
        df = (
            df.reset_index()
              .pivot(index="date", columns="symbol", values=df.name)
        )
    return _to_dt_index(df)

_fetch_daily   = lambda: _fetch("1d")
_fetch_monthly = lambda: _fetch("1mo")

def _last_price_and_sma(hist: pd.DataFrame):
    price = hist.iloc[-1]
    sma   = hist.rolling(SMA_LEN).mean().iloc[-1]
    return price.dropna(), sma.dropna()

def _name(lst: List[str]) -> str:
    return ", ".join(NAMES.get(x, x) for x in lst) if lst else "–"

# ---------------- Main ------------------------------------------------
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    daily = _fetch_daily()
    if daily.empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    mon   = _fetch_monthly()
    if mon.index[-1].month != pd.Timestamp.utcnow().month:
        mon.loc[daily.index[-1]] = daily.iloc[-1]

    mom = (
        mon.pct_change(1).iloc[-1] +
        mon.pct_change(3).iloc[-1] +
        mon.pct_change(6).iloc[-1] +
        mon.pct_change(9).iloc[-1]
    ).dropna()

    price, sma = _last_price_and_sma(daily)
    eligible   = mom.index[(price > sma)]

    # --- Debug ins Log -------------------------------------------------
    print("\nDEBUG Price vs. SMA150 & Momentum")
    dbg = pd.DataFrame({
        "mom%":  (mom*100).round(2),
        "price": price.round(2),
        "SMA150": sma.round(2),
        "diff%": ((price/sma - 1)*100).round(2)
    })
    print(dbg.to_string())

    # --- Top-N Auswahl -------------------------------------------------
    top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold = list(top.index) + ["CASH"]*(TOP_N - len(top))

    # --- Verlauf -------------------------------------------------------
    prev: List[str] = []
    if os.path.exists(HIST_FILE) and os.path.getsize(HIST_FILE):
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

    # --- Discord Message ----------------------------------------------
    m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
    lines: List[str] = []
    if buys:   lines.append(f"Kaufen: {_name(buys)}")
    if sells:  lines.append(f"Verkaufen: {_name(sells)}")
    if holds:  lines.append(f"Halten: {_name(holds)}")
    if not (buys or sells or holds):
        lines.append("Cash halten")

    lines.append(f"Aktuelles Portfolio: {_name(hold)}\n")
    lines.append("Momentum-Scores:")
    for t, sc in top.items():
        lines.append(f"{NAMES.get(t,t)}: {sc:+.2%}")

    subject = f"GAA Rebalance ({m_end:%b %Y})"
    return subject, "", "\n".join(lines)
