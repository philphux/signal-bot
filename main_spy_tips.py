"""
Entry‑Point NUR für die SPY‑/TIPS‑Strategie
"""

from __future__ import annotations
import traceback, os
from strategies.spytips_cool import spy_tips_cool as strat

def save(subj, subj2=None, txt=None):
    if not (subj or subj2): return
    with open("message.txt", "a", encoding="utf-8") as f:
        for part in (subj, subj2, txt):
            if part: f.write(part + "\n\n")

def main():
    open("message.txt", "w").close()
    try:
        s, s2, t = strat()
        if any(x is not None for x in (s, s2, t)):
            save(s, s2, t)
    except Exception as exc:
        save("Error in spy_tips_cool", None, "".join(traceback.format_exception(exc)))

if __name__ == "__main__":
    main()
