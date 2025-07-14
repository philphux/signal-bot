"""
Global Asset Allocation – Momentum (kompakt)
"""

from __future__ import annotations
import os, warnings
from typing import Dict, List, Tuple
import pandas as pd, yahooquery as yq, yfinance as yf

ETF = ["EEM","FEZ","IEF"]
FUT: Dict[str,str] = {"NQ=F":"NQ=F","BTC=F":"BTC-USD","GC=F":"GC=F","CL=F":"CL=F"}
TOP_N, SMA_LEN = 3, 150
HIST_FILE = "gaa_history.csv"

def _utc(idx): return pd.to_datetime(idx,utc=True).tz_convert(None)

def _etf():
    df=yq.Ticker(" ".join(ETF)).history(period="2y",interval="1d")["close"].unstack()
    df.index=_utc(df.index); return df

def _fut():
    out=[]
    for name,sym in FUT.items():
        try:
            s=yf.download(sym,period="2y",interval="1d",progress=False)["Close"]
            if s.empty: raise ValueError
            out.append(s.rename(name).tz_localize(None))
        except Exception as e:
            warnings.warn(f"{name}: {e}")
    return pd.concat(out,axis=1) if out else pd.DataFrame()

def gaa_monthly_momentum() -> Tuple[str|None,str|None,str|None]:
    today=pd.Timestamp.utcnow().normalize()
    hist=pd.concat([_etf(),_fut()],axis=1).sort_index()
    if hist.dropna().empty:
        return "GAA-Fehler",None,"Keine vollständigen Kurse."
    last=hist.dropna().index[-1]; hist=hist.loc[:last]

    m_ends=hist.index.to_period("M").unique().to_timestamp("M")
    m_end=m_ends[-2] if (today.month,today.year)==(m_ends[-1].month,m_ends[-1].year) else m_ends[-1]

    prev=[]
    if os.path.exists(HIST_FILE):
        d,*rest=open(HIST_FILE).read().strip().split(";")
        if d==f"{m_end:%F}": return None,None,None
        prev=rest[0].split(",") if rest else []

    mon=hist.resample("M").last()
    mom=(mon.pct_change(1).iloc[-1]+mon.pct_change(3).iloc[-1]+
         mon.pct_change(6).iloc[-1]+mon.pct_change(9).iloc[-1]).dropna()
    price,sma=hist.loc[last],hist.rolling(SMA_LEN).mean().loc[last]
    top=mom[price> sma].sort_values(ascending=False).head(TOP_N)
    hold=list(top.index)

    with open(HIST_FILE,"a") as f:
        f.write(f"{m_end:%F};{','.join(hold)}\n")

    subj=f"GAA Rebalance ({m_end:%b %Y})"
    body=[f"Neu kaufen: {', '.join(sorted(set(hold)-set(prev)))}",
          f"Aktuelles Portfolio: {', '.join(hold)}",
          "","Momentum-Scores:"]
    body+=[f"{t}: {sc:+.2%}" for t,sc in top.items()]
    return subj,"","\n".join(body)
