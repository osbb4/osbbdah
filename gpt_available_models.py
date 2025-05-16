import os
from openai import OpenAI
from dotenv import load_dotenv

# Завантажуємо ключі з .env
load_dotenv()

# Ініціалізуємо клієнт
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

# Отримати список доступних моделей
models = client.models.list()

print("📦 Доступні моделі у твоєму обліковому записі:")
for model in models.data:
    print("–", model.id)
