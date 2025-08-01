"""
Global Asset Allocation – Momentum (Excel-identisch, 1mo-Abfrage)

• Momentum = Σ Renditen 1 / 3 / 6 / 9 Monate (Adj Close, Monats-Schluss)
• Filter   = Preis > SMA150 (auf Tages-AdjClose)
• Rebal    = Monatsende, Top-3, Rest Cash
• Verlauf  = gaa_history.csv (nur bei Portfolio-Wechsel)
"""

from __future__ import annotations
import os
from typing import List, Tuple
import pandas as pd, yahooquery as yq

TICKERS = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]
NAMES   = {
    "BTC-USD": "Bitcoin",        "QQQ": "Nasdaq-100",   "GLD": "Gold",
    "USO": "WTI Crude Oil",      "EEM": "Emerging Markets",
    "FEZ": "Euro Stoxx 50",      "IEF": "Treasury Bonds",
    "CASH": "Cash",
}
TOP_N      = 3
SMA_LEN    = 150
HIST_FILE  = "gaa_history.csv"

# ───────── Kursabfrage ──────────────────────────────────────────────── #
def _fetch_daily() -> pd.DataFrame:
    """AdjClose Tagesauflösung (2 Jahre) – für SMA-Berechnung"""
    df = yq.Ticker(" ".join(TICKERS)).history(period="2y", interval="1d", adj_ohlc=True)
    df = df['adjclose'] if 'adjclose' in df.columns else df['close']
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index().pivot(index="date", columns="symbol", values=df.name)
    return df.tz_localize("UTC").tz_convert(None).sort_index()

def _fetch_monthly() -> pd.DataFrame:
    """AdjClose Monats-Schluss (2 Jahre, 1mo-Intervall)"""
    df = yq.Ticker(" ".join(TICKERS)).history(period="2y", interval="1mo", adj_ohlc=True)
    df = df['adjclose'] if 'adjclose' in df.columns else df['close']
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index().pivot(index="date", columns="symbol", values=df.name)
    return df.tz_localize("UTC").tz_convert(None).sort_index()

# ───────── Helper ───────────────────────────────────────────────────── #
def _last_price_and_sma(hist: pd.DataFrame):
    price = hist.iloc[-1]
    sma   = hist.rolling(SMA_LEN).mean().iloc[-1]
    return price.dropna(), sma.dropna()

def _name(seq: List[str]) -> str:
    return ", ".join(NAMES.get(x, x) for x in seq) if seq else "–"

# ───────── Hauptfunktion ────────────────────────────────────────────── #
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    daily = _fetch_daily()
    if daily.empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    # Monatsdaten für Momentum
    mon = _fetch_monthly()
    # falls BTC-USD den aktuellen Monat noch nicht trägt → letzten Tages-Close ergänzen
    if mon.index[-1].month != pd.Timestamp.utcnow().month:
        mon.loc[daily.index[-1]] = daily.loc[daily.index[-1]]

    # Momentum-Score (1 / 3 / 6 / 9 Monate)
    mom = (
        mon.pct_change(1).iloc[-1] +
        mon.pct_change(3).iloc[-1] +
        mon.pct_change(6).iloc[-1] +
        mon.pct_change(9).iloc[-1]
    ).dropna()

    price, sma = _last_price_and_sma(daily)
    eligible   = mom.index[(price > sma)]

    # DEBUG – im Job-Log ersichtlich
    print("\nDEBUG Momentum & SMA check")
    print(pd.DataFrame({
        "mom%": mom.round(2) * 100,
        "price": price.round(2),
        "SMA150": sma.round(2),
        "diff%": ((price/sma - 1)*100).round(2)
    }).loc[mom.index].sort_values("mom%", ascending=False).to_string())

    top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold = list(top.index) + ["CASH"] * (TOP_N - len(top))

    # Verlauf laden
    prev: List[str] = []
    if os.path.exists(HIST_FILE) and os.path.getsize(HIST_FILE):
        prev = open(HIST_FILE).read().splitlines()[-1].split(";")[1].split(",")

    buys  = sorted([x for x in hold if hold.count(x) > prev.count(x)])
    sells = sorted([x for x in prev if prev.count(x) > hold.count(x)])
    holds = sorted(set(hold) & set(prev))

    if buys or sells or not prev:
        m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
        with open(HIST_FILE, "a") as f:
            if not os.path.exists(HIST_FILE) or os.path.getsize(HIST_FILE) == 0:
                f.write("date;portfolio\n")
            f.write(f"{m_end:%F};{','.join(hold)}\n")

    # ── Discord-Nachricht ──────────────────────────────────────────────
    m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
    lines = []
    if buys:  lines.append(f"Kaufen: {_name(buys)}")
    if sells: lines.append(f"Verkaufen: {_name(sells)}")
    if holds: lines.append(f"Halten: {_name(holds)}")
    if not (buys or sells or holds):
        lines.append("Cash halten")

    lines.append(f"Aktuelles Portfolio: {_name(hold)}\n")
    lines.append("Momentum-Scores:")
    for t, sc in top.items():
        lines.append(f"{NAMES.get(t,t)}: {sc:+.2%}")

    subj = f"GAA Rebalance ({m_end:%b %Y})"
    return subj, "", "\n".join(lines)
