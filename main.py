import traceback
from strategies.spytips_cool import spy_tips_cool
from strategies.gaa_momentum import gaa_monthly_momentum

# ---------------------------------------------------------------
# Hilfsfunktion: schreibt (subject, subject2, text) in message.txt
# ---------------------------------------------------------------
def save_text(subject, subject2=None, text=None):
    if not (subject or subject2):
        return
    with open("message.txt", "a", encoding="utf-8") as f:  # ➊  APPEND
        if subject:
            f.write(subject + "\n\n")
        if subject2:
            f.write(subject2 + "\n\n")
        if text:
            f.write(text + "\n\n")

# ---------------------------------------------------------------
# Liste aller Strategien
# ---------------------------------------------------------------
STRATEGIES = [
    spy_tips_cool,
    gaa_monthly_momentum,
]

# ---------------------------------------------------------------
# Hauptprogramm
# ---------------------------------------------------------------
def main():
    # ➋  message.txt zu Beginn leeren
    open("message.txt", "w").close()

    anything_written = False

    for strat in STRATEGIES:
        try:
            s, s2, t = strat()                # jeweils (subject, subject2, text)
            if any(x is not None for x in (s, s2, t)):
                save_text(s, s2, t)
                anything_written = True
            else:
                print(f"{strat.__name__}: Skipped")
        except Exception as e:
            err_msg = "".join(traceback.format_exception(e))
            save_text(f"Error in {strat.__name__}", None, err_msg)
            anything_written = True

    if not anything_written:
        print("Keine Strategie hat heute etwas zu melden.")

# ---------------------------------------------------------------
if __name__ == "__main__":
    main()
