import sqlite3

# Параметри
DB_PATH = "db/OSBBDAH.sqlite"

# Підключення до бази
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Створити таблицю тимчасових кластерів смислових тем (якщо ще немає)
cursor.execute("""
CREATE TABLE IF NOT EXISTS semantic_thread_groups (
    group_id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic_id INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS message_to_group (
    message_id INTEGER PRIMARY KEY,
    group_id INTEGER,
    FOREIGN KEY(message_id) REFERENCES messages(message_id),
    FOREIGN KEY(group_id) REFERENCES semantic_thread_groups(group_id)
)
""")

conn.commit()

# Обробити всі topic_id з повідомленнями
cursor.execute("SELECT DISTINCT topic_id FROM messages WHERE topic_id IS NOT NULL")
topic_ids = [row[0] for row in cursor.fetchall()]

group_count = 0

for topic_id in topic_ids:
    print(f"🔎 Обробка topic_id = {topic_id}")

    # Отримати всі повідомлення по темі, впорядковані за часом
    cursor.execute("""
        SELECT message_id, sender_id, date
        FROM messages
        WHERE topic_id = ?
        ORDER BY date ASC
    """, (topic_id,))
    messages = cursor.fetchall()

    if not messages:
        continue

    # Кластеризація: кожне повідомлення без reply — нова тема
    current_group_id = None
    for i, (message_id, sender_id, date) in enumerate(messages):
        # якщо це перше повідомлення — новий кластер
        if i == 0:
            cursor.execute("INSERT INTO semantic_thread_groups (topic_id) VALUES (?)", (topic_id,))
            current_group_id = cursor.lastrowid
            cursor.execute("INSERT INTO message_to_group (message_id, group_id) VALUES (?, ?)", (message_id, current_group_id))
            continue

        # Отримати чи це reply (можна додати в базу або логіку окремо)
        cursor.execute("SELECT reply_to_msg_id FROM messages WHERE message_id = ?", (message_id,))
        reply = cursor.fetchone()
        reply_to_id = reply[0] if reply else None

        if reply_to_id:
            # спробувати знайти існуючу групу для reply_to_id
            cursor.execute("SELECT group_id FROM message_to_group WHERE message_id = ?", (reply_to_id,))
            reply_group = cursor.fetchone()
            if reply_group:
                current_group_id = reply_group[0]
            else:
                cursor.execute("INSERT INTO semantic_thread_groups (topic_id) VALUES (?)", (topic_id,))
                current_group_id = cursor.lastrowid
        else:
            # нова гілка — новий group_id
            cursor.execute("INSERT INTO semantic_thread_groups (topic_id) VALUES (?)", (topic_id,))
            current_group_id = cursor.lastrowid

        cursor.execute("INSERT INTO message_to_group (message_id, group_id) VALUES (?, ?)", (message_id, current_group_id))

    conn.commit()
    group_count += 1

print(f"✅ Оброблено {group_count} topics і створено semantic thread groups.")
conn.close()
