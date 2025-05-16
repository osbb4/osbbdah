import sqlite3
import json
import sys

DB_PATH = "db/OSBBDAH.sqlite"

def add_site(name, base_url, include_patterns):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Перевірити, чи вже існує такий сайт
    cursor.execute("SELECT * FROM monitored_sites WHERE base_url = ?", (base_url,))
    if cursor.fetchone():
        print(f"⚠️ Сайт з base_url = {base_url} вже існує.")
        conn.close()
        return

    patterns_json = json.dumps(include_patterns)
    cursor.execute("""
        INSERT INTO monitored_sites (name, base_url, include_patterns, enabled, last_indexed_at)
        VALUES (?, ?, ?, 1, NULL)
    """, (name, base_url, patterns_json))

    conn.commit()
    conn.close()
    print(f"✅ Сайт '{name}' додано до моніторингу.")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("⚠️ Використання: python add_site.py 'Назва' 'https://site.com' '/articles/*,/resources/*'")
    else:
        _, name, base_url, patterns_str = sys.argv
        patterns = [p.strip() for p in patterns_str.split(',')]
        add_site(name, base_url, patterns)
