import sqlite3
import json
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = "db/OSBBDAH.sqlite"
DAYS_BACK = 7
TG_BASE_URL = "https://t.me/osbbdah"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Дата створення дайджесту
today_str = datetime.now().strftime("%Y-%m-%d")
output_filename = f"weekly_digest_{today_str}.txt"

# Отримуємо теми з Telegram за останній тиждень
cutoff = (datetime.now() - timedelta(days=DAYS_BACK)).isoformat()

cursor.execute("""
SELECT t.theme_id, t.title, t.summary, t.problem, t.solution, t.reactions_summary,
       t.last_analyzed_at, stg.group_id, t.source_url, tp.name
FROM semantic_themes t
JOIN semantic_thread_groups stg ON t.theme_id = stg.theme_id
LEFT JOIN telegram_topics tp ON stg.topic_id = tp.topic_id
WHERE t.source_type = 'telegram' AND t.last_analyzed_at >= ?
ORDER BY tp.name, t.last_analyzed_at DESC
""", (cutoff,))

themes = cursor.fetchall()
conn.close()

def format_reactions(rjson):
    if not rjson:
        return ""
    try:
        obj = json.loads(rjson)
        sorted_items = sorted(obj.items(), key=lambda x: -x[1])
        return " | ".join(f"{emoji} × {count}" for emoji, count in sorted_items)
    except:
        return ""

def is_valid_text(text, min_len=20):
    return text and len(text.strip()) >= min_len

def is_valid_solution(text):
    if not is_valid_text(text):
        return False
    lower = text.lower().strip()
    return not (lower.startswith("для вирішення цієї ситуації необхідно") and len(lower) <= 60)

# Групування тем по name
grouped = defaultdict(list)
for row in themes:
    theme_id, title, summary, problem, solution, reactions, analyzed_at, group_id, source_url, name = row
    if not (is_valid_text(summary) and is_valid_text(problem) and is_valid_solution(solution)):
        continue
    key = name if name else "🔸 Без категорії"
    grouped[key].append((title, summary, problem, solution, reactions, analyzed_at, group_id, source_url))

# Формуємо текст дайджесту
digest = f"🗓️ Дайджест обговорень ОСББ за останні {DAYS_BACK} днів\n\n"
for topic, items in grouped.items():
    digest += f"📂 Topic: {topic}\n"
    for idx, (title, summary, problem, solution, reactions, analyzed_at, group_id, source_url) in enumerate(items, start=1):
        digest += f"🔹 Тема {idx}: {title.strip()}\n"
        digest += f"📅 GPT-аналіз: {analyzed_at[:10]}\n"
        if reactions:
            digest += f"📊 Реакції: {format_reactions(reactions)}\n"
        digest += f"📌 Summary: {summary.strip()}\n"
        digest += f"❗ Problem: {problem.strip()}\n"
        digest += f"✅ Solution: {solution.strip()}\n"
        if source_url:
            digest += f"🔗 Джерело: {source_url.strip()}\n"
        else:
            digest += f"🔗 Джерело: {TG_BASE_URL}/{group_id}\n"
        digest += f"{'-'*40}\n"
    digest += "\n"

# Запис у файл
with open(output_filename, "w", encoding="utf-8") as f:
    f.write(digest.replace('\\n', '\n'))

print(f"✅ Дайджест створено: {output_filename}")
