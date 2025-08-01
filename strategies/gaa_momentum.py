"""
Global Asset Allocation – Momentum (Top-3, Cash fallback)
"""

from __future__ import annotations
import os, pandas as pd, yahooquery as yq
from typing import List, Tuple

TICKERS = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]
LABEL   = {
    "BTC-USD":"Bitcoin","QQQ":"Nasdaq-100","GLD":"Gold","USO":"WTI Crude Oil",
    "EEM":"Emerging Markets","FEZ":"Euro Stoxx 50","IEF":"Treasury Bonds",
    "CASH":"Cash",
}
TOP_N, SMA_LEN = 3, 150
HIST_FILE = "gaa_history.csv"

# ───────── helpers ────────────────────────────────────────────────────
def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    df  = df[idx.notna()].copy()
    df.index = idx[idx.notna()].tz_convert(None)
    return df.sort_index()

def _fetch(ivl: str) -> pd.DataFrame:
    tq = yq.Ticker(" ".join(TICKERS))
    df = tq.history(period="2y", interval=ivl, adj_ohlc=True)
    col = "adjclose" if "adjclose" in df.columns else "close"
    df  = df[col]
    if isinstance(df.index, pd.MultiIndex):
        df = (df.reset_index()
                .pivot(index="date", columns="symbol", values=col))
    return _to_dt(df)

_fetch_daily   = lambda: _fetch("1d")
_fetch_monthly = lambda: _fetch("1mo")

nice = lambda xs: ", ".join(LABEL.get(x,x) for x in xs) if xs else "–"

# ───────── strategy ──────────────────────────────────────────────────
def gaa_monthly_momentum() -> Tuple[str|None,str|None,str|None]:

    daily = _fetch_daily()
    if daily.empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    # ---------------- Momentum (monatliche Kurse) --------------------
    mon = _fetch_monthly()
    if mon.index[-1].month != pd.Timestamp.utcnow().month:
        mon.loc[daily.index[-1]] = daily.iloc[-1]

    mom = (
        mon.pct_change(1).iloc[-1].fillna(0) +
        mon.pct_change(3).iloc[-1].fillna(0) +
        mon.pct_change(6).iloc[-1].fillna(0) +
        mon.pct_change(9).iloc[-1].fillna(0)
    )

    # ---------------- Preis & SMA150 (Lücken füllen!) ---------------
    daily_f = daily.ffill()
    price   = daily.iloc[-1]
    sma     = daily_f.rolling(SMA_LEN).mean().iloc[-1]
    enough  = daily_f.notna().sum() >= SMA_LEN    # mind. 150 Werte?

    # ---------------- Debug -----------------------------------------
    dbg = pd.DataFrame({
        "momentum%": (mom*100).round(2),
        "price": price,
        "SMA150": sma,
        "≥150d": enough,
        "SMA_OK": price > sma,
    })
    print("\n=== DEBUG ===================================================")
    print(dbg.to_string(float_format="%.2f"))
    print("=============================================================\n")

    # ---------------- Auswahl ---------------------------------------
    eligible = [t for t in mom.index if enough[t] and price[t] > sma[t]]
    top      = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold     = list(top.index) + ["CASH"]*(TOP_N - len(top))

    # ---------------- History-Datei ---------------------------------
    prev: List[str] = []
    if os.path.isfile(HIST_FILE) and os.path.getsize(HIST_FILE):
        prev = open(HIST_FILE).read().splitlines()[-1].split(";")[1].split(",")

    buys  = sorted([x for x in hold if hold.count(x) > prev.count(x)])
    sells = sorted([x for x in prev if prev.count(x) > hold.count(x)])
    holds = sorted(set(hold) & set(prev))

    if buys or sells or not prev:
        stamp = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
        with open(HIST_FILE, "a") as f:
            if os.path.getsize(HIST_FILE)==0: f.write("date;portfolio\n")
            f.write(f"{stamp:%F};{','.join(hold)}\n")

    # ---------------- Discord-Nachricht -----------------------------
    stamp = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
    msg: List[str] = []
    if buys:  msg.append(f"Kaufen: {nice(buys)}")
    if sells: msg.append(f"Verkaufen: {nice(sells)}")
    if holds: msg.append(f"Halten: {nice(holds)}")
    if not (buys or sells or holds):
        msg.append("Cash halten")

    msg.append(f"Aktuelles Portfolio: {nice(hold)}\n")
    msg.append("Momentum-Scores:")
    for t, sc in top.items():
        msg.append(f"{LABEL.get(t,t)}: {sc:+.2%}")

    return f"GAA Rebalance ({stamp:%b %Y})", "", "\n".join(msg)
