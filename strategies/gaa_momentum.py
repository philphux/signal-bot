"""
GAA-Momentum nach Meb Faber – korrigierte Fassung
"""

from __future__ import annotations
import os, warnings
from typing import Dict, List, Tuple
import pandas as pd, yahooquery as yq, yfinance as yf

ETF  = ["EEM", "FEZ", "IEF"]
FUT: Dict[str, str] = {      # interne Bezeichnung : yfinance-Symbol
    "NQ=F":  "NQ=F",
    "BTC=F": "BTC-USD",
    "GC=F":  "GC=F",
    "CL=F":  "CL=F",
}

TOP_N, SMA_N = 3, 150
HIST_FILE    = "gaa_history.csv"

# ---------------------------------------------------------------------------
def _utc_naive(idx): return pd.to_datetime(idx, utc=True).tz_convert(None)

def _etf() -> pd.DataFrame:
    df = yq.Ticker(" ".join(ETF)).history(period="2y", interval="1d")["close"].unstack()
    df.index = _utc_naive(df.index)
    return df

def _fut() -> Tuple[pd.DataFrame, List[str]]:
    frames, bad = [], []
    for name, sym in FUT.items():
        try:
            s = yf.download(sym, period="2y", interval="1d", progress=False)["Close"]
            if s.empty: raise ValueError("leere Serie")
            frames.append(s.rename(name).tz_localize(None))
        except Exception as e:
            warnings.warn(f"{name}: {e}"), bad.append(name)
    return (pd.concat(frames, axis=1) if frames else pd.DataFrame(), bad)

# ---------------------------------------------------------------------------
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    today   = pd.Timestamp.utcnow().normalize()
    etf, fut, bad = _etf(), *_fut()

    hist = pd.concat([etf, fut], axis=1).sort_index()
    last = hist.dropna().index[-1]  # letzter Tag mit vollständigen Daten
    hist = hist.loc[:last]

    # Monatsende bestimmen
    m_ends = hist.index.to_period("M").unique().to_timestamp("M")
    m_end  = m_ends[-1] if (today.month != m_ends[-1].month) else m_ends[-2]

    # bereits verbucht?
    if os.path.exists(HIST_FILE):
        last_line = open(HIST_FILE).read().strip().split(";")
        if last_line and pd.to_datetime(last_line[0]) == m_end:
            return None, None, None
        prev = last_line[1].split(",") if len(last_line) > 1 else []
    else:
        prev = []

    # Momentum-Score
    mon = hist.resample("M").last()
    r1, r3, r6, r9 = (mon.pct_change(n).iloc[-1] for n in (1,3,6,9))
    score = (r1 + r3 + r6 + r9).dropna()

    price = hist.loc[last]
    sma   = hist.rolling(SMA_N).mean().loc[last]
    elig  = score.index[price[score.index] > sma[score.index]]
    score = score.loc[elig]

    if score.empty:
        msg = "→ Cash (kein Asset > SMA150)"
        if bad: msg += f"\nIgnoriert: {', '.join(bad)}"
        return "GAA – kein Kauf", "", msg

    top   = score.sort_values(ascending=False).head(TOP_N)
    hold  = list(top.index)

    # Verlauf speichern
    with open(HIST_FILE, "a") as f:
        f.write(f"{m_end:%F};{','.join(hold)}\n")

    # Meldung
    subj = f"GAA Rebalance ({m_end:%b %Y})" if set(hold)!=set(prev)\
           else f"GAA – keine Änderung ({m_end:%b %Y})"

    lines = []
    if add:=set(hold)-set(prev):  lines.append("Neu kaufen: "+", ".join(add))
    if sell:=set(prev)-set(hold): lines.append("Verkaufen: "+", ".join(sell))
    lines.append("Aktuelles Portfolio: "+(", ".join(hold) or "Cash"))

    # Debug-Tabelle
    lines.append("\nDetails (Schlusskurs "+last.strftime('%Y-%m-%d')+"):")
    for t in hist.columns:
        tag = "✔" if t in hold else " "
        lines.append(f"{t:<6}  P {price[t]:>8.2f}  SMA {sma[t]:>8.2f}  "
                     f"Δ {price[t]-sma[t]:>7.2f}  Mom {score.get(t,pd.NA):>7}")

    if bad: lines.append("\nIgnoriert (keine Daten): "+", ".join(bad))

    return subj, "", "\n".join(lines)
