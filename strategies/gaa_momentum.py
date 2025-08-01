"""
Global Asset Allocation – Momentum  (Excel-kompatibel)

  • Momentum-Score = Σ [%-Rendite nach 1 M, 3 M, 6 M, 9 M]
      – Berechnet auf **Monats-Schlusskursen** (EoM)
      – Zeitzone vorher auf US/Eastern gestellt,
        damit BTC-USD auch am 31. Kalendertag schließt
  • Kursfilter      = letztes Close > SMA 150
  • Rebalance       = Monatsende; Top-3, sonst Cash-Auffüllung
  • Verlauf         = gaa_history.csv (nur bei Portfolio-Wechsel)

Universum: BTC-USD, QQQ, GLD, USO, EEM, FEZ, IEF
"""

from __future__ import annotations
import os
from typing import List, Tuple
import pandas as pd, yahooquery as yq

# ---------------- Universum & Namen ------------------------------------ #
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
HIST_FILE = "gaa_history.csv"

# ---------------- Hilfsfunktionen -------------------------------------- #
def _utc(idx):       # tz-aware Index → UTC → tz-naiv
    return pd.to_datetime(idx, utc=True).tz_convert(None)

def _fetch_all() -> pd.DataFrame:
    h = yq.Ticker(" ".join(TICKERS)).history(period="2y", interval="1d")["close"]
    if isinstance(h.index, pd.MultiIndex):
        h = h.reset_index().pivot(index="date", columns="symbol", values="close")
    else:
        h = h.to_frame().rename(columns={"close": h.name})
    h.index = _utc(h.index)
    return h.sort_index()

def _last_price_and_sma(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    price, sma = {}, {}
    for t, s in df.items():
        s = s.dropna()
        if len(s) < SMA_LEN:
            continue
        price[t] = s.iloc[-1]
        sma[t]   = s.rolling(SMA_LEN).mean().iloc[-1]
    return pd.Series(price), pd.Series(sma)

def _name(seq: List[str]) -> str:
    return ", ".join(NAMES.get(x, x) for x in seq) if seq else "–"

# ---------------- Hauptfunktion ---------------------------------------- #
def gaa_monthly_momentum() -> Tuple[str | None, str | None, str | None]:
    hist = _fetch_all()
    if hist.dropna(how="all").empty:
        return "GAA-Fehler", None, "Keine Kursdaten verfügbar."

    # --- Monats-Schlusskurse (NY-Zeit) ----------------------------------
    hist = hist.tz_localize("UTC").tz_convert("US/Eastern")
    mon  = hist.resample("M").last()
    hist = hist.tz_convert(None)                 # zurück naiv

    # DEBUG: letzte Reihe anzeigen
    print("\nDEBUG Monats-Schluss (EoM)")
    print(mon.tail(1).T)

    mom = (
        mon.pct_change(1).iloc[-1] +
        mon.pct_change(3).iloc[-1] +
        mon.pct_change(6).iloc[-1] +
        mon.pct_change(9).iloc[-1]
    ).dropna()

    price, sma = _last_price_and_sma(hist)
    eligible   = mom.index[(price > sma) & sma.notna()]

    top  = mom.loc[eligible].sort_values(ascending=False).head(TOP_N)
    hold = list(top.index)
    if len(hold) < TOP_N:
        hold.extend(["CASH"] * (TOP_N - len(hold)))

    # ----- Verlauf ------------------------------------------------------
    prev: List[str] = []
    if os.path.exists(HIST_FILE) and os.path.getsize(HIST_FILE):
        prev = open(HIST_FILE).read().strip().splitlines()[-1].split(";")[1].split(",")

    buys  = sorted([x for x in hold if hold.count(x) > prev.count(x)])
    sells = sorted([x for x in prev if prev.count(x) > hold.count(x)])
    holds = sorted(set(hold) & set(prev))

    if buys or sells or not prev:
        m_end = pd.Timestamp.utcnow().to_period("M").to_timestamp("M")
        with open(HIST_FILE, "a") as f:
            if os.path.getsize(HIST_FILE) == 0:
                f.write("date;portfolio\n")
            f.write(f"{m_end:%F};{','.join(hold)}\n")

    # ----- Discord-Text -------------------------------------------------
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
        lines += [f"{NAMES.get(t,t)}: {sc:+.2%}" for t, sc in top.items()]

    subj = f"GAA Rebalance ({m_end:%b %Y})"
    return subj, "", "\n".join(lines)
