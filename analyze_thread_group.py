import sqlite3
import openai
from config import OPENAI_API_KEY

openai.api_key = OPENAI_API_KEY
DB_PATH = "db/OSBBDAH.sqlite"

# Підключення до бази
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Отримати всі групи, які ще не аналізувались
cursor.execute("""
SELECT g.group_id, g.topic_id
FROM semantic_thread_groups g
LEFT JOIN semantic_themes st ON g.group_id = st.theme_id
WHERE st.theme_id IS NULL
LIMIT 1
""")
group = cursor.fetchone()

if not group:
    print("✅ Усі смислові теми вже проаналізовані.")
    exit()

group_id, topic_id = group
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
    print("⚠️ Немає повідомлень у групі.")
    exit()

# Сформувати текст промпта
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

    cursor.execute("""
        INSERT INTO semantic_themes (theme_id, topic_id, title, summary, problem, solution)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (group_id, topic_id, f"Theme from group {group_id}", summary, problem, solution))
    conn.commit()
    print("✅ GPT-аналіз збережено.")
except Exception as e:
    print(f"❌ Помилка аналізу: {e}")

conn.close()
