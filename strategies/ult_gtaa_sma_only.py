# strategies/ult_gtaa_sma_only.py
# ULT-GTAA — SMA150, Monthly; Ausgabe NUR "Stand heute":
#   - Liste aller Assets, die über ihrem SMA150 liegen
#   - Sortiert nach Summe der Momenta (1M+3M+6M+9M), Summe wird angezeigt
#   - Leverage-Empfehlung (3x wenn: alle gelisteten Assets über 10M-SMA UND 20d-Vol < 30%)
# Zusätzlich: CSV-Append NUR am offiziellen Rebalancing-Tag (letzter Handelstag des Monats) in ./ult_gtaa_history.csv

from __future__ import annotations

import os
import pandas as pd
import numpy as np
import yfinance as yf

TICKERS         = ["BTC-USD", "QQQ", "GLD", "USO", "EEM", "FEZ", "IEF"]
START           = "2024-01-01"              # kurzer Zeitraum genügt
SMA_DAYS        = 150
TOP_N           = 3                          # nur für CSV-Log am Monatsultimo (offizielle Top-3)
VOL_THR         = 0.30                       # 30% annualisiert
TENM_SMA_DAYS   = 210                        # ~10 Monate à 21 Handelstage
CSV_PATH        = "ult_gtaa_history.csv"     # CSV im Repo-Root (nur am Monatsultimo)

# ---------- Helpers ----------
def _tz_naive(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df

def download_close(tickers, start) -> pd.DataFrame:
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
    return df.resample("ME").last()  # Monatsultimo-Label

def rolling_sma(df: pd.DataFrame, window_days: int) -> pd.DataFrame:
    return df.rolling(window_days, min_periods=window_days).mean()

def ten_month_sma(df: pd.DataFrame) -> pd.DataFrame:
    return rolling_sma(df, TENM_SMA_DAYS)

def realized_vol_ann(returns: pd.DataFrame, window=20) -> pd.DataFrame:
    return returns.rolling(window).std() * np.sqrt(252)

def momentum_sum(prices_m: pd.DataFrame) -> pd.DataFrame:
    # Summe der Total-Returns (1/3/6/9 Monate) auf Monatsbasis
    out = None
    for m in [1, 3, 6, 9]:
        part = prices_m.pct_change(m)
        out = part if out is None else out.add(part, fill_value=0)
    return out

def official_last_completed_month(prices_d: pd.DataFrame, prices_m: pd.DataFrame) -> pd.Timestamp:
    last_trade = prices_d.index.max()
    curr_me_label = last_trade.to_period("M").to_timestamp("M")
    last_label_in_m = prices_m.index.max()
    if last_label_in_m == curr_me_label and last_trade < last_label_in_m:
        return prices_m.index[-2] if len(prices_m.index) >= 2 else prices_m.index[-1]
    return prices_m.index[-1]

# ---------- Public API ----------
def generate_message(save_csv: bool = True) -> str:
    prices_d = download_close(TICKERS, START)
    if prices_d.empty:
        return "Fehler: Keine Preisdaten geladen."

    prices_m = monthly_last(prices_d)

    # Preview-Referenz: laufender Monatslabel (Daten bis heute)
    ref_prev = prices_m.index[-1]

    # SMA150 (daily -> ME), 10M-SMA & Volatilität (für Gate)
    sma150_m = rolling_sma(prices_d, SMA_DAYS).resample("ME").last()
    sma10_m  = ten_month_sma(prices_d).resample("ME").last()
    vol20_m  = realized_vol_ann(prices_d.pct_change(), 20).resample("ME").last()

    # Momentum-Summe
    rank_sum = momentum_sum(prices_m)

    # "Stand heute": Alle Assets, die über SMA150 notieren — sortiert nach Momentum-Summe desc
    sma_ok_series = (prices_m.loc[ref_prev] > sma150_m.loc[ref_prev]).dropna()
    investables = sma_ok_series.index[sma_ok_series].tolist()

    # Tabelle mit Momenta je Asset für ref_prev
    mom_table = pd.DataFrame(index=prices_m.columns)
    for m, days in [("1M", 1), ("3M", 3), ("6M", 6), ("9M", 9)]:
        mom_table[m] = prices_m.pct_change(days).loc[ref_prev]
    mom_table["SUM"] = mom_table[["1M", "3M", "6M", "9M"]].sum(axis=1)

    # Filter auf investables (über SMA150) und sortieren nach SUM absteigend
    today_list = (
        mom_table.loc[investables]
        .sort_values("SUM", ascending=False)
    )

    # Leverage-Empfehlung basierend auf "investables":
    # 3x nur wenn ALLE gelisteten Assets (über SMA150) gleichzeitig über 10M-SMA sind und Vol < Schwelle
    leverage = "1x"
    if len(investables) > 0:
        gate_price = (prices_m.loc[ref_prev, investables] > sma10_m.loc[ref_prev, investables]).all()
        gate_vol   = (vol20_m.loc[ref_prev, investables] < VOL_THR).all()
        leverage = "3x" if (gate_price and gate_vol) else "1x"

    # ---------- CSV (nur offizieller Rebalancing-Tag / letzter Handelstag des Monats) ----------
    if save_csv:
        today = pd.Timestamp.utcnow().normalize()
        this_month = today.to_period("M")
        try:
            last_trading_day_this_month = prices_d.loc[
                prices_d.index.to_period("M") == this_month
            ].index.max()
        except ValueError:
            last_trading_day_this_month = None
        this_me_label = this_month.to_timestamp("M")
        month_ends_obs = prices_d.resample("ME").last().index

        is_last_trading_day = (
            last_trading_day_this_month is not None
            and last_trading_day_this_month.normalize() == today
            and (this_me_label in month_ends_obs)
            and (last_trading_day_this_month.normalize() == this_me_label)
        )

        if is_last_trading_day:
            # Für die Historie speichern wir weiterhin die OFFIZIELLE Top-3-Aufteilung
            ref_off = official_last_completed_month(prices_d, prices_m)
            # Offizielle Top-3 (Momentum + SMA150 auf ref_off)
            sma_ok_off = (prices_m.loc[ref_off] > sma150_m.loc[ref_off]).dropna()
            ok_assets  = sma_ok_off.index[sma_ok_off].tolist()
            # Momentum Sum auf ref_off
            rank_sum_off = momentum_sum(prices_m)
            row_off = rank_sum_off.loc[ref_off].dropna().sort_values(ascending=False)
            top_off = [a for a in row_off.index if a in ok_assets][:TOP_N]
            n = len(top_off)
            w = round(1.0 / n, 4) if n > 0 else 0.0
            lev_off = "1x"
            if n > 0:
                gate_price_off = (prices_m.loc[ref_off, top_off] > sma10_m.loc[ref_off, top_off]).all()
                gate_vol_off   = (vol20_m.loc[ref_off, top_off] < VOL_THR).all()
                lev_off = "3x" if (gate_price_off and gate_vol_off) else "1x"
            row = {
                "date": ref_off.strftime("%Y-%m-%d"),
                "ticker1": top_off[0] if n > 0 else "",
                "weight1": w if n > 0 else 0.0,
                "ticker2": top_off[1] if n > 1 else "",
                "weight2": w if n > 1 else 0.0,
                "ticker3": top_off[2] if n > 2 else "",
                "weight3": w if n > 2 else 0.0,
                "leverage": lev_off,
            }
            df_entry = pd.DataFrame([row])
            if os.path.exists(CSV_PATH):
                df_entry.to_csv(CSV_PATH, mode="a", header=False, index=False)
            else:
                df_entry.to_csv(CSV_PATH, index=False)

    # ---------- Textausgabe (nur "Stand heute" laut Wunsch) ----------
    lines = []
    lines.append("=== Stand heute ===\n")
    if today_list.empty:
        lines.append("Über SMA150: —")
    else:
        lines.append("Über SMA150 (sortiert nach ΣMomentum = 1M+3M+6M+9M):")
        for ticker, row in today_list.iterrows():
            sum_pct = row["SUM"] * 100.0
            lines.append(f"  {ticker:<8}  ΣMom: {sum_pct:>6.2f}%")

    lines.append(f"\nLeverage-Empfehlung:  {leverage}")

    return "\n".join(lines)
