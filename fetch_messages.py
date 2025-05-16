from telethon.sync import TelegramClient
from telethon.tl.functions.channels import GetForumTopicsRequest
import sqlite3
from config import API_ID, API_HASH, GROUP_NAME

client = TelegramClient("osbb_session", API_ID, API_HASH)
client.start()

conn = sqlite3.connect("db/OSBBDAH.sqlite")
cursor = conn.cursor()

# Отримати список тем
topics = client(GetForumTopicsRequest(
    channel=GROUP_NAME,
    offset_date=0,
    offset_id=0,
    offset_topic=0,
    limit=100
))

# Зберегти теми в БД
for topic in topics.topics:
    cursor.execute("""
        INSERT OR IGNORE INTO telegram_topics (topic_id, name, group_id, last_updated)
        VALUES (?, ?, ?, datetime('now'))
    """, (topic.id, topic.title, None))
conn.commit()

# Збір повідомлень по кожній темі
for topic in topics.topics:
    topic_id = topic.id
    print(f"📥 Topic: {topic.title} (ID: {topic_id})")

    for message in client.iter_messages(GROUP_NAME, reply_to=topic_id, limit=100):
        if message.text:
            cursor.execute("""
                INSERT OR IGNORE INTO messages (message_id, topic_id, sender_id, date, text)
                VALUES (?, ?, ?, ?, ?)
            """, (
                message.id,
                topic_id,
                message.sender_id,
                message.date.strftime("%Y-%m-%d %H:%M:%S"),
                message.text.strip()
            ))
conn.commit()
conn.close()
