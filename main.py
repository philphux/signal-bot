import traceback

from strategies.spytips_cool import spy_tips_cool
from strategies.gaa_momentum import gaa_monthly_momentum

STRATEGIES = [
    spy_tips_cool,
    gaa_monthly_momentum,
]

def save_text(subject, subject2=None, text=None):
    """Hängt eine Meldung an message.txt an."""
    if not (subject or subject2):
        return
    with open("message.txt", "a", encoding="utf-8") as f:
        if subject:
            f.write(subject + "\n\n")
        if subject2:
            f.write(subject2 + "\n\n")
        if text:
            f.write(text + "\n\n")

def main():
    # frische Datei für jeden Lauf
    open("message.txt", "w").close()

    for strat in STRATEGIES:
        try:
            s, s2, t = strat()
            if any(x is not None for x in (s, s2, t)):
                save_text(s, s2, t)
        except Exception as e:
            save_text(f"Error in {strat.__name__}", None,
                      "".join(traceback.format_exception(e)))

if __name__ == "__main__":
    main()
