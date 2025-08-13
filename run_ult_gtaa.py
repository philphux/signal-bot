import os
import requests

from strategies.ult_gtaa_sma_only import generate_message

def send_discord_message(message: str) -> None:
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not url:
        raise RuntimeError("Missing DISCORD_WEBHOOK_URL secret")
    # Als Codeblock posten, damit das Layout sauber monospaced bleibt
    payload = {"content": f"```\n{message}\n```"}
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()

def main():
    msg = generate_message(save_csv=True)  # CSV wird nur am Monatsultimo erzeugt
    print(msg)                             # Log in den Actions-Runs
    send_discord_message(msg)

if __name__ == "__main__":
    main()
