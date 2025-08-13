from strategies.ult_gtaa_sma_only import generate_message
from bot.discord_sender import send_discord_message

def main():
    msg = generate_message(save_csv=True)
    print(msg)
    send_discord_message(msg)

if __name__ == "__main__":
    main()
