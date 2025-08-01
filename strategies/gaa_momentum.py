"""
Global Asset Allocation – Momentum
ETF-Variante • Adj Close • End-of-Month • Cash-Filler
"""

from __future__ import annotations
import os
from typing import List, Tuple
import pandas as pd, yahooquery as yq

# ---- Universum --------------------------------------------------------- #
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

# ---- Hilfsfunktionen --------------------------------------------------- #
def _utc(idx):  # tz-aware → UTC tz-naiv
    return pd.to_datetime(idx, utc=True).tz_convert(None)

def _fetch_all() -> pd.DataFrame:
    """liefert Adj Close falls verfügbar, sonst Close"""
    tq = yq.Ticker(" ".join(TICKERS))
    df = tq.history(period="2y", interval="1d", adj_ohlc=True)

    # 'adjclose' bevorzugen
    if 'adjclose' in df.columns:
        df = df['adjclose']
    else:
        df = df['close']

    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index().pivot(index="date", columns="symbol", values=df.name)
    else:
        df = df.to_frame().rename(columns={df.name: df.name})

    df.index = _utc(df.index)
    return df.sort_index()

def _last_price_and_sma(hist: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    price, sma = {}, {}
    for tkr, s in hist.items():
        s = s.dropna()
        if len(s) < SMA_LEN:
            continue
        price[tkr] = s.iloc[-1]
        sma[tkr]   = s.rolling(SMA_LEN).mean().iloc[-1]
    return pd.Series(price), pd.Series(sma)

def _name(seq: List[str]) -> str:
    return ", ".join(NAMES.get(x, x) for x in seq) if seq else "–"

# ---- Hauptfunktion ----------------------------------------------------- #
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    hist = _fetch_all()
    if hist.dropna(how="all").empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    # Monats-Schlusskurse (NY-Zeit)
    hist = hist.tz_localize("UTC").tz_convert("US/Eastern")
    mon  = hist.resample("M").last()
    hist = hist.tz_convert(None)            # zurück naiv

    # Momentum (Summe 1/3/6/9 M)
    mom = (
        mon.pct_change(1).iloc[-1] +
        mon.pct_change(3).iloc[-1] +
        mon.pct_change(6).iloc[-1] +
        mon.pct_change(9).iloc[-1]
    ).dropna()

    price, sma = _last_price_and_sma(hist)
    eligible   = mom.index[(price > sma) & sma.notna()]

    # --- DEBUG ----------------------------------------------------------
    dbg = pd.DataFrame({
        "price": price.round(2),
        "SMA150": sma.round(2),
        "diff%": ((price / sma - 1) * 100).round(2)
    })
    print("\nDEBUG Price vs SMA150\n", dbg.to_string())

    # --- Top-Assets -----------------------------------------------------
    top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold = list(top.index)
    if len(hold) < TOP_N:
        hold.extend(["CASH"] * (TOP_N - len(hold)))

    # --- Verlauf --------------------------------------------------------
    prev: List[str] = []
    if os.path.exists(HIST_FILE) and os.path.getsize(HIST_FILE):
        prev = open(HIST_FILE).read().strip().splitlines()[-1].split(";")[1].split(",")

    buys  = sorted([x for x in hold if hold.count(x) > prev.count(x)])
    sells = sorted([x for x in prev if prev.count(x) > hold.count(x)])
    holds = sorted(set(hold) & set(prev))

    if buys or sells or not prev:
        m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
        with open(HIST_FILE, "a") as f:
            if not os.path.exists(HIST_FILE) or os.path.getsize(HIST_FILE) == 0:
                f.write("date;portfolio\n")
            f.write(f"{m_end:%F};{','.join(hold)}\n")

    # --- Discord-Message -----------------------------------------------
    m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
    lines = []
    if buys:  lines.append(f"Kaufen: {_name(buys)}")
    if sells: lines.append(f"Verkaufen: {_name(sells)}")
    if holds: lines.append(f"Halten: {_name(holds)}")
    if not (buys or sells or holds):
        lines.append("Cash halten")

    lines.append(f"Aktuelles Portfolio: {_name(hold)}")
    lines.append("")
    lines.append("Momentum-Scores:")
    if top.empty:
        lines.append("Keine eligible Assets")
    else:
        for t, sc in top.items():
            lines.append(f"{NAMES.get(t,t)}: {sc:+.2%}")

    subj = f"GAA Rebalance ({m_end:%b %Y})"
    return subj, "", "\n".join(lines)
