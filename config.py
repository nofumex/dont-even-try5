from dotenv import load_dotenv
import os

load_dotenv()  # Загружает .env

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
CRYPTOBOT_API_TOKEN = os.getenv("CRYPTOBOT_API_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID")) if os.getenv("CHANNEL_ID") else 0
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
REQUIRE_SUBSCRIPTION = os.getenv("REQUIRE_SUBSCRIPTION", "true").lower() in ["true", "1", "yes"]

# Для отладки
print("BOT_TOKEN:", BOT_TOKEN)
print("ADMIN_IDS:", ADMIN_IDS)
print("CRYPTOBOT_API_TOKEN:", CRYPTOBOT_API_TOKEN)
print("CHANNEL_ID:", CHANNEL_ID)
print("CHANNEL_USERNAME:", CHANNEL_USERNAME)
print("REQUIRE_SUBSCRIPTION:", REQUIRE_SUBSCRIPTION)