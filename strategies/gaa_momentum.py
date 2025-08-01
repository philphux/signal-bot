"""
Global Asset Allocation – Momentum
----------------------------------
• Momentum-Score  = Σ Rendite (1 / 3 / 6 / 9 Monate)  
  – Basis: Adj Close, Monats-Schluss  
• Filter          = Schlusskurs > SMA 150 (Tages-Adj Close)  
• Rebalance       = Monatsende, Top-3; fehlende Positionen = Cash  
• Verlauf         = gaa_history.csv (wird nur ergänzt, wenn sich das
  Portfolio ändert)
"""

from __future__ import annotations
import os
from typing import List, Tuple
import pandas as pd, yahooquery as yq

# ───────── Parameter ────────────────────────────────────────────────
TICKERS = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]
NAMES   = {
    "BTC-USD": "Bitcoin",        "QQQ": "Nasdaq-100",   "GLD": "Gold",
    "USO": "WTI Crude Oil",      "EEM": "Emerging Markets",
    "FEZ": "Euro Stoxx 50",      "IEF": "Treasury Bonds",
    "CASH": "Cash",
}
TOP_N, SMA_LEN = 3, 150
HIST_FILE      = "gaa_history.csv"

# ───────── Helper: Daten laden & indizieren ─────────────────────────
def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    df  = df.loc[~idx.isna()].copy()
    df.index = idx[~idx.isna()].tz_convert(None)   # tz-naiv
    return df.sort_index()

def _fetch(interval: str) -> pd.DataFrame:
    tq = yq.Ticker(" ".join(TICKERS))
    df = tq.history(period="2y", interval=interval, adj_ohlc=True)
    col = "adjclose" if "adjclose" in df.columns else "close"
    df  = df[col]
    if isinstance(df.index, pd.MultiIndex):
        df = (df.reset_index()
                .pivot(index="date", columns="symbol", values=col))
    return _to_dt(df)

_fetch_daily   = lambda: _fetch("1d")
_fetch_monthly = lambda: _fetch("1mo")
nice           = lambda xs: ", ".join(NAMES.get(x, x) for x in xs) if xs else "–"

# ───────── Strategy ────────────────────────────────────────────────
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    # ---- Tagesdaten (für SMA) --------------------------------------
    daily = _fetch_daily().ffill()        # fehlende Börsentage auffüllen
    daily = daily.dropna(how="all")       # WE-Zeilen ohne Kurse entfernen
    if daily.empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    # ---- Monats-Bars (für Momentum) --------------------------------
    mon = _fetch_monthly()
    if mon.index[-1].month != pd.Timestamp.utcnow().month:
        mon.loc[daily.index[-1]] = daily.iloc[-1]     # aktuellen Monat nachziehen

    mom = (
        mon.pct_change(1).iloc[-1] +
        mon.pct_change(3).iloc[-1] +
        mon.pct_change(6).iloc[-1] +
        mon.pct_change(9).iloc[-1]
    ).dropna()

    price = daily.iloc[-1]
    sma   = daily.rolling(SMA_LEN).mean().iloc[-1]

    # ---- Debug-Ausgabe --------------------------------------------
    dbg = pd.DataFrame({
        "momentum%": (mom*100).round(2),
        "price": price,
        "SMA150": sma,
        "diff%": ((price/sma - 1)*100).round(2)
    })
    dbg["SMA_OK"] = price > sma
    dbg["≥150d"]  = daily.notna().sum() >= SMA_LEN
    print("\n=== DEBUG Price vs SMA150 ===")
    print(dbg.to_string(float_format="%.2f"))
    print("==============================================\n")

    # ---- Eligibility-Filter ---------------------------------------
    eligible = [
        t for t in mom.index
        if dbg.at[t, "≥150d"] and dbg.at[t, "SMA_OK"]
    ]

    top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold = list(top.index) + ["CASH"] * (TOP_N - len(top))

    # ---- History-File ---------------------------------------------
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

    # ---- Discord-Text ---------------------------------------------
    m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
    text: List[str] = []
    if buys:  text.append(f"Kaufen: {nice(buys)}")
    if sells: text.append(f"Verkaufen: {nice(sells)}")
    if holds: text.append(f"Halten: {nice(holds)}")
    if not (buys or sells or holds):
        text.append("Cash halten")

    text.append(f"Aktuelles Portfolio: {nice(hold)}\n")
    text.append("Momentum-Scores:")
    for t, sc in top.items():
        text.append(f"{NAMES.get(t,t)}: {sc:+.2%}")

    subject = f"GAA Rebalance ({m_end:%b %Y})"
    return subject, "", "\n".join(text)
