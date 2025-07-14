"""
main.py – zentraler Einstieg für alle Strategien

- leert bei jedem Lauf die Datei message.txt
- ruft nacheinander alle Strategien in STRATEGIES auf
- jede Strategie liefert (subject, subject2, text) oder (None, None, None)
- nicht-leere Ergebnisse werden an message.txt angehängt
"""

from __future__ import annotations
import os
import traceback

# ---- Strategien -----------------------------------------------------------
from strategies.gaa_momentum import gaa_monthly_momentum
from strategies.spytips_cool import spy_tips_cool

STRATEGIES = [
    gaa_monthly_momentum,   # GAA-Momentum (monatlich)
    spy_tips_cool,          # bestehende SPY/TIPS-Strategie
]

# ---- Helper ---------------------------------------------------------------
def save(subject: str | None,
         subject2: str | None = None,
         text: str | None = None) -> None:
    """Hängt die Inhalte in message.txt an (wenn mindestens ein Teil vorhanden)."""
    if not (subject or subject2):
        return
    with open("message.txt", "a", encoding="utf-8") as f:
        for part in (subject, subject2, text):
            if part:
                f.write(part + "\n\n")

# ---- Hauptprogramm --------------------------------------------------------
def main() -> None:
    # Datei zum Start leeren
    open("message.txt", "w").close()

    for strat in STRATEGIES:
        try:
            subj, subj2, body = strat()
            if any(x is not None for x in (subj, subj2, body)):
                save(subj, subj2, body)
            else:
                print(f"{strat.__name__}: skipped")
        except Exception as exc:
            err = "".join(traceback.format_exception(exc))
            save(f"Error in {strat.__name__}", None, err)

if __name__ == "__main__":
    main()
