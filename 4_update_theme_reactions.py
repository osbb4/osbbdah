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

# Отримаємо theme_id для груп з Telegram
cursor.execute("""
SELECT stg.group_id, stg.theme_id
FROM semantic_thread_groups stg
JOIN semantic_themes t ON stg.theme_id = t.theme_id
WHERE t.source_type = 'telegram'
""")
theme_links = cursor.fetchall()

updated = 0

for group_id, theme_id in theme_links:
    cursor.execute("""
    SELECT mr.emoji, SUM(mr.count)
    FROM message_to_group mg
    JOIN message_reactions mr ON mg.message_id = mr.message_id
    WHERE mg.group_id = ?
    GROUP BY mr.emoji
    """, (group_id,))
    
    emoji_counts = cursor.fetchall()
    if emoji_counts:
        reactions = {emoji: count for emoji, count in emoji_counts}
        reactions_json = json.dumps(reactions, ensure_ascii=False)
        cursor.execute("""
            UPDATE semantic_themes
            SET reactions_summary = ?
            WHERE theme_id = ?
        """, (reactions_json, theme_id))
        updated += 1

conn.commit()
conn.close()

print(f"✅ Оновлено реакції для {updated} тем.")
