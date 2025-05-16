import sqlite3

DB_PATH = "db/OSBBDAH.sqlite"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Отримати всі group_id
cursor.execute("SELECT group_id FROM semantic_thread_groups")
group_ids = [row[0] for row in cursor.fetchall()]

updated = 0
skipped = 0

for group_id in group_ids:
    # Отримати всі theme_id повідомлень у групі
    cursor.execute("""
    SELECT DISTINCT m.theme_id
    FROM messages m
    JOIN message_to_group mg ON m.message_id = mg.message_id
    WHERE mg.group_id = ? AND m.theme_id IS NOT NULL
    """, (group_id,))
    themes = cursor.fetchall()

    if len(themes) == 1:
        theme_id = themes[0][0]
        cursor.execute("""
            UPDATE semantic_thread_groups
            SET theme_id = ?
            WHERE group_id = ?
        """, (theme_id, group_id))
        updated += 1
    else:
        skipped += 1

conn.commit()
conn.close()

print(f"✅ Оновлено {updated} semantic_thread_groups з theme_id.")
print(f"ℹ️ Пропущено {skipped} груп (0 або >1 різні theme_id).")
