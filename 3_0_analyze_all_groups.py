import sqlite3
import openai
from config import OPENAI_API_KEY

openai.api_key = OPENAI_API_KEY
DB_PATH = "db/OSBBDAH.sqlite"

# Підключення до бази
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Додати колонку last_analyzed_at, якщо вона відсутня
cursor.execute("PRAGMA table_info(semantic_themes)")
columns = [row[1] for row in cursor.fetchall()]
if "last_analyzed_at" not in columns:
    cursor.execute("ALTER TABLE semantic_themes ADD COLUMN last_analyzed_at TEXT")
    # привʼязати GPT-тему до semantic_thread_groups
    cursor.execute("UPDATE semantic_thread_groups SET theme_id = ? WHERE group_id = ?", (theme_id, group_id))
    # також оновлюємо theme_id в таблиці messages
    cursor.execute("""
            UPDATE messages SET theme_id = ? WHERE message_id IN (
            SELECT message_id FROM message_to_group WHERE group_id = ?
            )
    """, (theme_id, group_id))
    conn.commit()
    print("✅ Додано колонку last_analyzed_at у semantic_themes.")

# Отримати всі групи, які потрібно проаналізувати або переаналізувати
cursor.execute("""
SELECT g.group_id, g.topic_id
FROM semantic_thread_groups g
LEFT JOIN semantic_themes st ON g.group_id = st.theme_id
WHERE st.last_analyzed_at IS NULL
   OR EXISTS (
       SELECT 1 FROM messages m
       JOIN message_to_group mg ON m.message_id = mg.message_id
       WHERE mg.group_id = g.group_id
         AND datetime(m.date) > IFNULL(datetime(st.last_analyzed_at), '1970-01-01')
   )
""")
groups = cursor.fetchall()

if not groups:
    print("✅ Усі смислові теми актуальні, нічого оновлювати.")
    conn.close()
    exit()

print(f"🔍 Груп для аналізу: {len(groups)}")

for group_id, topic_id in groups:
    print(f"🧠 Аналіз групи {group_id} в темі {topic_id}")

    # Отримати повідомлення цієї групи
    cursor.execute("""
    SELECT m.message_id, m.text
    FROM messages m
    JOIN message_to_group mg ON m.message_id = mg.message_id
    WHERE mg.group_id = ?
    ORDER BY m.date ASC
    """, (group_id,))
    messages = cursor.fetchall()

    if not messages:
        print(f"⚠️ Група {group_id} порожня.")
        continue

    # Формування промпта
    conversation = "\n".join([f"{i+1}. {text}" for i, (mid, text) in enumerate(messages)])
    prompt = f"""
Нижче наведено фрагмент обговорення учасників ОСББ:

{conversation}

Проаналізуй це обговорення та:
1. Коротко сформулюй суть обговорення (summary)
2. Визнач головну проблему або питання (problem)
3. Запропонуй рішення або висновки (solution)

Відповідь має містити блоки:
Summary: ...
Problem: ...
Solution: ...
"""

    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Ти експерт з управління ОСББ в Україні."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        reply = response.choices[0].message.content.strip()

        summary = problem = solution = ""
        for line in reply.splitlines():
            if line.lower().startswith("summary:"):
                summary = line.split(":", 1)[1].strip()
            elif line.lower().startswith("problem:"):
                problem = line.split(":", 1)[1].strip()
            elif line.lower().startswith("solution:"):
                solution = line.split(":", 1)[1].strip()

        # Вставити або оновити аналіз
        cursor.execute("SELECT 1 FROM semantic_themes WHERE theme_id = ?", (group_id,))
        exists = cursor.fetchone()

        if exists:
            cursor.execute("""
                UPDATE semantic_themes
                SET summary = ?, problem = ?, solution = ?, last_analyzed_at = datetime('now')
                WHERE theme_id = ?
            """, (summary, problem, solution, group_id))
        else:
            cursor.execute("""
                INSERT INTO semantic_themes (theme_id, topic_id, title, summary, problem, solution, last_analyzed_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            """, (group_id, topic_id, f"Theme from group {group_id}", summary, problem, solution))
                # привʼязати GPT-тему до semantic_thread_groups
        cursor.execute("UPDATE semantic_thread_groups SET theme_id = ? WHERE group_id = ?", (theme_id, group_id))
        conn.commit()
        print(f"✅ Група {group_id} проаналізована.")
    except Exception as e:
        print(f"❌ Помилка аналізу групи {group_id}: {e}")
        continue

conn.close()
print("🏁 Аналіз усіх груп завершено.")
