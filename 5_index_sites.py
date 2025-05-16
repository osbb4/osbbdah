import sqlite3
import json
import hashlib
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from trafilatura import extract
from datetime import datetime, timezone
import re

DB_PATH = "db/OSBBDAH.sqlite"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def normalize_url(base, link):
    return urljoin(base, link.split("#")[0])

def is_url_allowed(url, base_url, patterns):
    path = urlparse(url).path
    for pattern in patterns:
        if pattern.endswith("*") and path.startswith(pattern[:-1]):
            return True
        elif pattern == path:
            return True
    return False

def hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def extract_publication_date(soup, text):
    # Спосіб 1: meta-теги
    meta_tags = [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"name": "pubdate"}),
        ("meta", {"name": "date"}),
        ("meta", {"name": "DC.date.issued"}),
        ("meta", {"itemprop": "datePublished"})
    ]
    for tag_name, attrs in meta_tags:
        tag = soup.find(tag_name, attrs=attrs)
        if tag and tag.get("content"):
            return tag["content"]

    # Спосіб 2: <time datetime=...>
    time_tag = soup.find("time")
    if time_tag and time_tag.has_attr("datetime"):
        return time_tag["datetime"]

    # Спосіб 3: regex у тексті
    date_patterns = [
        r"(\d{4}-\d{2}-\d{2})",
        r"(\d{2}/\d{2}/\d{4})",
        r"(\d{2}\.\d{2}\.\d{4})"
    ]
    for pattern in date_patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)

    return None

def crawl_site(site_id, base_url, patterns):
    visited = set()
    to_visit = {base_url}
    new_pages = 0

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    while to_visit:
        url = to_visit.pop()
        if url in visited:
            continue
        visited.add(url)

        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            if response.status_code != 200 or "text/html" not in response.headers.get("Content-Type", ""):
                continue
        except:
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        canonical_url = normalize_url(base_url, soup.find("link", rel="canonical")["href"]) if soup.find("link", rel="canonical") else url
        title = soup.title.string.strip() if soup.title else url

        raw_text = extract(response.text, include_comments=False, include_tables=False, favor_recall=True, url=url)
        if not raw_text or len(raw_text.strip()) < 100:
            continue

        content_hash = hash_text(raw_text)
        published_at = extract_publication_date(soup, raw_text)

        cursor.execute("SELECT content_hash FROM indexed_pages WHERE url = ?", (canonical_url,))
        row = cursor.fetchone()
        if row and row[0] == content_hash:
            continue

        cursor.execute("""
            INSERT INTO indexed_pages (url, site_id, title, raw_text, html, content_hash, last_scraped_at, published_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                raw_text=excluded.raw_text,
                html=excluded.html,
                content_hash=excluded.content_hash,
                last_scraped_at=excluded.last_scraped_at,
                published_at=excluded.published_at
        """, (
            canonical_url, site_id, title, raw_text.strip(), response.text,
            content_hash, datetime.now(timezone.utc).isoformat(), published_at
        ))
        new_pages += 1

        for a in soup.find_all("a", href=True):
            link = normalize_url(url, a["href"])
            if link.startswith(base_url) and is_url_allowed(link, base_url, patterns):
                to_visit.add(link)

    conn.commit()
    conn.close()
    print(f"✅ Сайт {base_url} — додано/оновлено сторінок: {new_pages}")

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT site_id, base_url, include_patterns FROM monitored_sites WHERE enabled = 1")
    sites = cursor.fetchall()

    if not sites:
        print("⚠️ Немає активних сайтів для індексації.")
        return

    for site_id, base_url, pattern_json in sites:
        try:
            patterns = json.loads(pattern_json)
            crawl_site(site_id, base_url, patterns)
            cursor.execute("UPDATE monitored_sites SET last_indexed_at = ? WHERE site_id = ?", (datetime.now(timezone.utc).isoformat(), site_id))
            conn.commit()
        except Exception as e:
            print(f"❌ Помилка при індексації сайту {base_url}: {e}")

    conn.close()
    print("🏁 Індексація завершена.")

if __name__ == "__main__":
    main()
