import sqlite3
import openai
from datetime import datetime, timezone
from config import OPENAI_API_KEY

openai.api_key = OPENAI_API_KEY
DB_PATH = "db/OSBBDAH.sqlite"

def analyze_page(title, text, source_url, published_at):
    prompt = f"""
Нижче наведено текст зі сторінки публічного вебсайту, присвяченого тематиці ОСББ:

Назва сторінки: {title}
URL: {source_url}
Дата публікації: {published_at if published_at else "невідома"}

{text}

Проаналізуй цей матеріал та сформулюй:
1. Стислий зміст матеріалу (Summary)
2. Основну проблему, на яку він спрямований (Problem)
3. Пропоновані рішення, рекомендації чи дії (Solution)

Використай структуру:
Summary: ...
Problem: ...
Solution: ...
"""

    response = openai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "Ти аналітик, що спеціалізується на українському житловому управлінні та ОСББ."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3
    )
    return response.choices[0].message.content.strip()

# Підключення до бази
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Додати колонку raw_gpt_response, якщо її немає
cursor.execute("PRAGMA table_info(semantic_themes)")
columns = [row[1] for row in cursor.fetchall()]
if "raw_gpt_response" not in columns:
    cursor.execute("ALTER TABLE semantic_themes ADD COLUMN raw_gpt_response TEXT")
    conn.commit()

# Отримати всі неаналізовані або оновлені сторінки
cursor.execute("""
SELECT url, site_id, title, raw_text, published_at
FROM indexed_pages
WHERE theme_id IS NULL OR last_analyzed_at IS NULL
ORDER BY published_at DESC NULLS LAST
""")
rows = cursor.fetchall()

if not rows:
    print("✅ Усі сторінки проаналізовані.")
    conn.close()
    exit()

for url, site_id, title, raw_text, published_at in rows:
    print(f"🔍 Аналіз: {url}")
    try:
        result = analyze_page(title, raw_text[:8000], url, published_at)
        raw = result.strip()
        summary = problem = solution = ""

        for line in raw.splitlines():
            if line.lower().startswith("summary:"):
                summary = line.split(":", 1)[1].strip()
            elif line.lower().startswith("problem:"):
                problem = line.split(":", 1)[1].strip()
            elif line.lower().startswith("solution:"):
                solution = line.split(":", 1)[1].strip()

        if not summary or not problem or not solution:
            print(f"⚠️ Невалідна GPT-відповідь, збережено лише для аудиту.")
            continue

        now_utc = datetime.now(timezone.utc).isoformat()
        cursor.execute("""
            INSERT INTO semantic_themes (topic_id, title, summary, problem, solution, last_analyzed_at, source_type, source_url, raw_gpt_response)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (-1, title, summary, problem, solution, now_utc, "site", url, raw))

        cursor.execute("""
            UPDATE indexed_pages
            SET theme_id = ?, last_analyzed_at = ?
            WHERE url = ?
        """, (cursor.lastrowid, now_utc, url))

        conn.commit()
        print(f"✅ OK: {url}")
    except Exception as e:
        print(f"❌ Помилка для {url}: {e}")

conn.close()
print("🏁 Аналіз усіх сторінок завершено.")
