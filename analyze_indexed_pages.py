import sqlite3
import openai
from datetime import datetime
from config import OPENAI_API_KEY

openai.api_key = OPENAI_API_KEY
DB_PATH = "db/OSBBDAH.sqlite"

def analyze_page(theme_id, title, text, source_url, published_at):
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

# Отримати всі сторінки без аналізу або оновлені
cursor.execute("""
SELECT url, site_id, title, raw_text, published_at
FROM indexed_pages
WHERE (theme_id IS NULL OR last_analyzed_at IS NULL)
ORDER BY published_at DESC NULLS LAST
LIMIT 1
""")
row = cursor.fetchone()

if not row:
    print("✅ Немає сторінок для аналізу.")
    conn.close()
    exit()

url, site_id, title, raw_text, published_at = row

print(f"🔍 Аналіз сторінки: {url}")

try:
    result = analyze_page(None, title, raw_text[:8000], url, published_at)
    summary = problem = solution = ""

    for line in result.splitlines():
        if line.lower().startswith("summary:"):
            summary = line.split(":", 1)[1].strip()
        elif line.lower().startswith("problem:"):
            problem = line.split(":", 1)[1].strip()
        elif line.lower().startswith("solution:"):
            solution = line.split(":", 1)[1].strip()

    # Додати у semantic_themes
    cursor.execute("""
        INSERT INTO semantic_themes (topic_id, title, summary, problem, solution, last_analyzed_at, source_type)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (-1, title, summary, problem, solution, datetime.utcnow().isoformat(), "site"))
    theme_id = cursor.lastrowid

    # Оновити indexed_pages
    cursor.execute("""
        UPDATE indexed_pages
        SET theme_id = ?, last_analyzed_at = ?
        WHERE url = ?
    """, (theme_id, datetime.utcnow().isoformat(), url))

    conn.commit()
    print(f"✅ GPT-аналіз завершено і збережено для: {url}")
except Exception as e:
    print(f"❌ Помилка GPT-аналізу: {e}")

conn.close()
