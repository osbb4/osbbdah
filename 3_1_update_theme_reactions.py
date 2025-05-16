import sqlite3
import json

DB_PATH = "db/OSBBDAH.sqlite"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Перевіримо, чи існує колонка reactions_summary
cursor.execute("PRAGMA table_info(semantic_themes)")
columns = [row[1] for row in cursor.fetchall()]
if "reactions_summary" not in columns:
    cursor.execute("ALTER TABLE semantic_themes ADD COLUMN reactions_summary TEXT")
    conn.commit()

# Знайдемо всі theme_id, пов'язані з повідомленнями
cursor.execute("""
SELECT st.theme_id, mr.emoji, SUM(mr.count)
FROM semantic_themes st
JOIN messages m ON st.theme_id = m.theme_id
JOIN message_reactions mr ON m.message_id = mr.message_id
WHERE st.source_type = 'telegram'
GROUP BY st.theme_id, mr.emoji
""")

reaction_map = {}

for theme_id, emoji, count in cursor.fetchall():
    if theme_id not in reaction_map:
        reaction_map[theme_id] = {}
    reaction_map[theme_id][emoji] = count

# Запишемо в колонку reactions_summary у форматі JSON
for theme_id, emoji_dict in reaction_map.items():
    reactions_json = json.dumps(emoji_dict, ensure_ascii=False)
    cursor.execute("""
        UPDATE semantic_themes
        SET reactions_summary = ?
        WHERE theme_id = ?
    """, (reactions_json, theme_id))

conn.commit()
conn.close()
print("✅ Зведення реакцій оновлено в semantic_themes.")
