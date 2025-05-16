import sqlite3

# Підключення до бази
conn = sqlite3.connect('db/OSBBDAH.sqlite')
cursor = conn.cursor()

# Всього повідомлень
cursor.execute("SELECT COUNT(*) FROM messages")
print("Всього повідомлень:", cursor.fetchone()[0])

# Повідомлень з текстом > 20 символів
cursor.execute("SELECT COUNT(*) FROM messages WHERE text IS NOT NULL AND length(text) > 20")
print("Повідомлень з текстом > 20 символів:", cursor.fetchone()[0])

# Показати приклади
cursor.execute("SELECT message_id, text FROM messages WHERE text IS NOT NULL AND length(text) > 20 LIMIT 3")
rows = cursor.fetchall()

for i, (msg_id, text) in enumerate(rows, 1):
    print(f"\n📨 Повідомлення {i}:")
    print(f"ID: {msg_id}")
    print(text[:300], "..." if len(text) > 300 else "")

conn.close()
