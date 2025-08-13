# strategies/ult_gtaa_sma_only.py
# ULT-GTAA — SMA150, Equal Weight Top-3, Monthly
# Leverage-Gate: 3x if (price > 10M-SMA) & (20d vol < 30%)
# Startdatum bewusst kurz (2024-01-01) für schnellen Download

from __future__ import annotations

import os
from datetime import datetime
import pandas as pd
import numpy as np
import yfinance as yf

TICKERS         = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]
START           = "2024-01-01"
TOP_N           = 3
SMA_DAYS        = 150
VOL_THR         = 0.30                      # 30% annualisiert
TENM_SMA_DAYS   = 210                       # ~10 Monate à 21 Handelstage
CSV_PATH        = "ult_gtaa_history.csv"    # CSV im Repo-Root

# ---------- Helpers ----------
def _tz_naive(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df

def download_close(tickers, start) -> pd.DataFrame:
    """
    Lädt Schlusskurse (Close). Nutzt group_by='ticker' für robuste Spalten.
    """
    raw = yf.download(
        tickers, start=start, auto_adjust=True,
        progress=False, group_by="ticker", threads=True
    )
    if isinstance(raw.columns, pd.MultiIndex):
        if "Close" in raw.columns.get_level_values(1):
            close = raw.xs("Close", axis=1, level=1)
        else:
            raise KeyError("Download enthält kein 'Close'.")
    else:
        if "Close" not in raw.columns:
            raise KeyError("Download enthält kein 'Close'.")
        close = raw[["Close"]]
        close.columns = tickers[:1]
    close = _tz_naive(close.sort_index()).ffill()
    while len(close) and close.iloc[0].isna().all():
        close = close.iloc[1:]
    return close.reindex(columns=tickers)

def monthly_last(df: pd.DataFrame) -> pd.DataFrame:
    """Letzter beobachteter Handelstag pro Kalendermonat (Label = Monatsende)."""
    return df.resample("ME").last()

def rolling_sma(df: pd.DataFrame, window_days: int) -> pd.DataFrame:
    return df.rolling(window_days, min_periods=window_days).mean()

def ten_month_sma(df: pd.DataFrame) -> pd.DataFrame:
    return rolling_sma(df, TENM_SMA_DAYS)

def realized_vol_ann(returns: pd.DataFrame, window=20) -> pd.DataFrame:
    return returns.rolling(window).std() * np.sqrt(252)

def momentum_sum(prices_m: pd.DataFrame) -> pd.DataFrame:
    """Summe der Total-Returns über 1/3/6/9 Monate (Monatsdaten)."""
    out = None
    for m in [1, 3, 6, 9]:
        part = prices_m.pct_change(m)
        out = part if out is None else out.add(part, fill_value=0)
    return out

def select_equal_weights(prices_m: pd.DataFrame,
                         sma150_m: pd.DataFrame,
                         rank_sum: pd.DataFrame,
                         ref_me: pd.Timestamp,
                         top_n: int):
    """Top-N nach Momentum, dann SMA150-Filter; Equal Weight auf selektierte."""
    row = rank_sum.loc[ref_me].dropna()
    top = row.sort_values(ascending=False).head(top_n).index.tolist()
    sma_ok = (prices_m.loc[ref_me] > sma150_m.loc[ref_me])
    selected = [t for t in top if bool(sma_ok.get(t, False))]
    weights = {}
    if selected:
        w = round(100.0 / len(selected), 1)
        for t in selected:
            weights[t] = f"{w:.1f}%"
    return selected, weights

def leverage_gate(prices_d: pd.DataFrame,
                  prices_m: pd.DataFrame,
                  ref_me: pd.Timestamp,
                  selected: list[str],
                  vol_thr: float) -> str:
    """3x nur wenn (alle selektierten über 10M-SMA) UND (alle unter Vol-Schwelle)."""
    if not selected:
        return "1x"
    vol20_m = realized_vol_ann(prices_d.pct_change(), 20).resample("ME").last()
    sma10_m = ten_month_sma(prices_d).resample("ME").last()
    gate_price = (prices_m.loc[ref_me, selected] > sma10_m.loc[ref_me, selected]).all()
    gate_vol   = (vol20_m.loc[ref_me, selected] < vol_thr).all()
    return "3x" if (gate_price and gate_vol) else "1x"

def delta_official_vs_preview(sel_official: list[str], sel_preview: list[str]):
    off = set(sel_official); prv = set(sel_preview)
    added   = sorted(list(prv - off))
    removed = sorted(list(off - prv))
    kept    = sorted(list(prv & off))
    return added, removed, kept

# ---------- Public API ----------
def generate_message(save_csv: bool = True) -> str:
    """
    Erzeugt den kompakten Text-Output für Discord und speichert (falls Ultimo)
    die offiziellen Signale als CSV im Repo-Root: ./ult_gtaa_history.csv
    """
    prices_d = download_close(TICKERS, START)
    if prices_d.empty:
        return "Fehler: Keine Preisdaten geladen."

    prices_m = monthly_last(prices_d)

    # Letzter Handelstag & Monatslabels
    last_trade = prices_d.index.max()
    curr_label_me = last_trade.to_period("M").to_timestamp("M")
    last_label_in_m = prices_m.index.max()

    # Offiziell = letzter abgeschlossener Monat (nicht der noch laufende)
    if last_label_in_m == curr_label_me and last_trade < last_label_in_m:
        ref_off = prices_m.index[-2] if len(prices_m.index) >= 2 else prices_m.index[-1]
    else:
        ref_off = prices_m.index[-1]

    next_rebal = ref_off + pd.offsets.MonthEnd(1)   # Kalender-Monatsultimo
    ref_prev   = prices_m.index[-1]                 # Preview nutzt laufenden Monatslabel

    # Indikatoren & Auswahl
    sma150_m = rolling_sma(prices_d, SMA_DAYS).resample("ME").last()
    rank_sum = momentum_sum(prices_m)

    sel_off, w_off = select_equal_weights(prices_m, sma150_m, rank_sum, ref_off, TOP_N)
    lev_off = leverage_gate(prices_d, prices_m, ref_off, sel_off, VOL_THR)

    sel_prev, w_prev = select_equal_weights(prices_m, sma150_m, rank_sum, ref_prev, TOP_N)
    lev_prev = leverage_gate(prices_d, prices_m, ref_prev, sel_prev, VOL_THR)

    added, removed, kept = delta_official_vs_preview(sel_off, sel_prev)

    # ---------- CSV-Speicherung NUR am offiziellen Rebalancing-Tag ----------
    # Definition: letzter HANDELSTAG des aktuellen Monats (robust gegen Wochenende/Feiertage),
    # und dieser Handelstag entspricht dem ME-Label der Preisdaten.
    if save_csv:
        today = pd.Timestamp.utcnow().normalize()

        # letzter beobachteter Handelstag innerhalb des aktuellen Monats
        try:
            last_trading_day_this_month = prices_d.loc[
                prices_d.index.to_period("M") == today.to_period("M")
            ].index.max()
        except ValueError:
            last_trading_day_this_month = None

        # Monatsend-Label laut Kalender und laut beobachteten Daten
        this_me_label = today.to_period("M").to_timestamp("M")
        month_ends_obs = prices_d.resample("ME").last().index

        is_last_trading_day = (
            last_trading_day_this_month is not None
            and last_trading_day_this_month.normalize() == today
            and (this_me_label in month_ends_obs)
            and (last_trading_day_this_month.normalize() == this_me_label)
        )

        if is_last_trading_day:
            # Zeile vorbereiten (immer drei Slots; ggf. mit Leerwerten auffüllen)
            tickers = list(w_off.keys())
            n = len(tickers)
            weight_num = round(1.0 / n, 4) if n > 0 else 0.0  # in Dezimal (z. B. 0.3333)
            row = {
                "date": ref_off.strftime("%Y-%m-%d"),
                "ticker1": tickers[0] if n > 0 else "",
                "weight1": weight_num if n > 0 else 0.0,
                "ticker2": tickers[1] if n > 1 else "",
                "weight2": weight_num if n > 1 else 0.0,
                "ticker3": tickers[2] if n > 2 else "",
                "weight3": weight_num if n > 2 else 0.0,
                "leverage": lev_off,
            }
            df_entry = pd.DataFrame([row])

            if os.path.exists(CSV_PATH):
                df_entry.to_csv(CSV_PATH, mode="a", header=False, index=False)
            else:
                df_entry.to_csv(CSV_PATH, index=False)

    # ---------- Kompakter Text-Output ----------
    lines = []
    lines.append("=== Letzter abgeschlossener Monat ===")
    lines.append(f"As-of:     {ref_off.strftime('%Y-%m-%d')}\n")
    lines.append("Top-3:")
    if w_off:
        for k in sorted(w_off.keys()):
            lines.append(f"  {k:<8} {w_off[k]}")
    else:
        lines.append("  -- CASH --")
    lines.append(f"\nLeverage-Empfehlung:  {lev_off}\n")

    lines.append("=== Stand heute ===")
    lines.append(f"Rebalancing:  {next_rebal.strftime('%Y-%m-%d')}\n")
    lines.append("Top-3:")
    if w_prev:
        for k in sorted(w_prev.keys()):
            lines.append(f"  {k:<8} {w_prev[k]}")
    else:
        lines.append("  -- CASH --")
    lines.append(f"\nLeverage-Empfehlung:  {lev_prev}\n")

    lines.append("Veränderungen:")
    lines.append(f"  Neu   : {', '.join(added) if added else '—'}")
    lines.append(f"  Raus  : {', '.join(removed) if removed else '—'}")
    lines.append(f"  Gleich: {', '.join(kept) if kept else '—'}")

    return "\n".join(lines)
