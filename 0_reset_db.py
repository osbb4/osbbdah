import sqlite3

def confirm():
    print("⚠️  Увага! Ви збираєтесь повністю очистити наступні таблиці:")
    print(" - gpt_analysis_logs")
    print(" - semantic_themes")
    print(" - messages")
    print(" - telegram_topics")
    choice = input("Ви впевнені? Введіть 'yes' для підтвердження: ")
    return choice.strip().lower() == 'yes'

def reset_db():
    conn = sqlite3.connect("db/OSBBDAH.sqlite")
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM gpt_analysis_logs;")
        cursor.execute("DELETE FROM semantic_themes;")
        cursor.execute("DELETE FROM messages;")
        cursor.execute("DELETE FROM telegram_topics;")
        conn.commit()
        print("✅ Таблиці успішно очищені.")
    except Exception as e:
        print("❌ Помилка при очищенні:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    if confirm():
        reset_db()
    else:
        print("❎ Очищення скасовано користувачем.")
