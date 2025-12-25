import os
import requests
from dotenv import load_dotenv


def send_telegram_message(message: str) -> None:
    """Отправляет сообщение в Telegram"""
    load_dotenv()

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print("⚠️  Telegram credentials not found")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message,
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        print(f"✅ Telegram sent: {message}")
    except Exception as e:
        print(f"❌ Telegram failed: {e}")
