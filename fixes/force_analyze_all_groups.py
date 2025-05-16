import sqlite3

DB_PATH = "db/OSBBDAH.sqlite"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Отримати всі теми з telegram, які вже були проаналізовані
cursor.execute("""
SELECT t.theme_id, g.group_id
FROM semantic_themes t
JOIN semantic_thread_groups g ON t.theme_id = g.group_id
WHERE t.source_type = 'telegram'
""")
pairs = cursor.fetchall()

groups_updated = 0
messages_updated = 0

for theme_id, group_id in pairs:
    # оновлюємо theme_id в групі
    cursor.execute("""
        UPDATE semantic_thread_groups
        SET theme_id = ?
        WHERE group_id = ?
    """, (theme_id, group_id))
    groups_updated += cursor.rowcount

    # оновлюємо theme_id в повідомленнях цієї групи
    cursor.execute("""
        UPDATE messages
        SET theme_id = ?
        WHERE message_id IN (
            SELECT message_id FROM message_to_group WHERE group_id = ?
        )
    """, (theme_id, group_id))
    messages_updated += cursor.rowcount

conn.commit()
conn.close()

print(f"✅ Синхронізовано {groups_updated} груп та {messages_updated} повідомлень із GPT-темами.")
