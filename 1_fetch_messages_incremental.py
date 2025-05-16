from telethon.sync import TelegramClient
from telethon.tl.types import Message, PeerChannel
import sqlite3
import json
import time
from config import API_ID, API_HASH, GROUP_NAME

DB_PATH = "db/OSBBDAH.sqlite"

def save_reactions(cursor, message_id, reactions):
    for reaction in reactions:
        emoji = reaction.reaction.emoticon
        count = reaction.count
        cursor.execute("""
            INSERT INTO message_reactions (message_id, emoji, count)
            VALUES (?, ?, ?)
            ON CONFLICT(message_id, emoji) DO UPDATE SET count = excluded.count
        """, (message_id, emoji, count))

def main():
    client = TelegramClient("tg_session", API_ID, API_HASH)
    client.start()

    with client:
        entity = client.get_entity(GROUP_NAME)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT MAX(message_id) FROM messages")
        last_id = cursor.fetchone()[0] or 0

        print(f"🔍 Завантаження нових повідомлень після ID {last_id}")

        for message in client.iter_messages(entity, min_id=last_id, reverse=True):
            if not isinstance(message, Message):
                continue

            cursor.execute("""
                INSERT OR IGNORE INTO messages (message_id, topic_id, sender_id, date, text, reply_to_msg_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                message.id,
                message.topic_id if hasattr(message, "topic_id") else None,
                message.sender_id,
                message.date.isoformat(),
                message.text or "",
                message.reply_to_msg_id
            ))

            if message.reactions:
                save_reactions(cursor, message.id, message.reactions.results)

            conn.commit()
            time.sleep(0.05)

        conn.close()
        print("✅ Завантаження завершено.")

if __name__ == "__main__":
    main()
