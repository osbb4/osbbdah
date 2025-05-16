import sqlite3
import os
from openai import OpenAI
from dotenv import load_dotenv

# Завантажуємо ключі з .env
load_dotenv()

# Створюємо клієнта
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# === Підключення до бази ===
conn = sqlite3.connect("db/OSBBDAH.sqlite")
cursor = conn.cursor()

# === Отримання повідомлень, які ще не аналізувались ===
cursor.execute("""
SELECT message_id, text
FROM messages
WHERE text IS NOT NULL AND LENGTH(text) > 20
AND message_id NOT IN (SELECT message_id FROM gpt_analysis_logs)
LIMIT 10
""")
messages = cursor.fetchall()

if not messages:
    print("⚠️ Немає повідомлень для аналізу.")
    conn.close()
    exit()

# === Пріоритет моделей ===
preferred_models = ["gpt-4o", "gpt-4.1", "gpt-3.5-turbo"]

available_models = [m.id for m in client.models.list().data]

selected_model = None
for model in preferred_models:
    if model in available_models:
        selected_model = model
        break

if not selected_model:
    print("❌ Немає доступної моделі GPT серед бажаних.")
    conn.close()
    exit()

print(f"✅ Використовується модель: {selected_model}")

# === Аналіз повідомлень GPT ===
for message_id, text in messages:
    prompt = f"""
{text}

Проаналізуй це повідомлення з групи ОСББ:

1. Коротко підсумуй зміст (Summary).
2. Визнач проблему, яка піднімається (Problem).
3. Запропонуй можливе рішення (Solution).

Формат відповіді:
Summary: ...
Problem: ...
Solution: ...
""".strip()

    try:
        response = client.chat.completions.create(
            model=selected_model,
            messages=[
                {"role": "system", "content": "Ти експерт з управління ОСББ в Україні."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        reply = response.choices[0].message.content.strip()

        # Парсинг відповіді
        summary = problem = solution = ""
        for line in reply.splitlines():
            if line.lower().startswith("summary:"):
                summary = line.split(":", 1)[1].strip()
            elif line.lower().startswith("problem:"):
                problem = line.split(":", 1)[1].strip()
            elif line.lower().startswith("solution:"):
                solution = line.split(":", 1)[1].strip()

        if not summary:
            summary = reply  # fallback

        cursor.execute("""
            INSERT INTO gpt_analysis_logs (message_id, analysis_date, summary, problem_description, suggested_solution)
            VALUES (?, datetime('now'), ?, ?, ?)
        """, (message_id, summary, problem, solution))
        conn.commit()

        print(f"🧠 Проаналізовано повідомлення ID {message_id}")

    except Exception as e:
        print(f"❌ Помилка для message_id {message_id}: {e}")
        continue

conn.close()
print("✅ Аналіз завершено.")
