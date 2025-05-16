import sqlite3

DB_PATH = "db/OSBBDAH.sqlite"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Отримати всі group_id з message_to_group
cursor.execute("SELECT DISTINCT group_id FROM message_to_group")
group_ids = [row[0] for row in cursor.fetchall()]

updated_groups = 0
updated_messages = 0

for group_id in group_ids:
    # Вибираємо перше повідомлення з групи
    cursor.execute("""
        SELECT m.message_id, m.theme_id
        FROM message_to_group mg
        JOIN messages m ON mg.message_id = m.message_id
        WHERE mg.group_id = ? AND m.theme_id IS NOT NULL
        LIMIT 1
    """, (group_id,))
    result = cursor.fetchone()

    if result:
        theme_id = result[1]
        # оновлюємо semantic_thread_groups
        cursor.execute("""
            UPDATE semantic_thread_groups
            SET theme_id = ?
            WHERE group_id = ?
        """, (theme_id, group_id))
        updated_groups += cursor.rowcount

        # оновлюємо всі повідомлення в цій групі
        cursor.execute("""
            UPDATE messages
            SET theme_id = ?
            WHERE message_id IN (
                SELECT message_id FROM message_to_group WHERE group_id = ?
            )
        """, (theme_id, group_id))
        updated_messages += cursor.rowcount

conn.commit()
conn.close()

print(f"✅ Оновлено theme_id у {updated_groups} групах та {updated_messages} повідомленнях.")
