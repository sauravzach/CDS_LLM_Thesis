import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
import sqlite3
import logging

# Setting up logging
logging.basicConfig(filename='crawler.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Initialize database connection
conn = sqlite3.connect('crawled_data.db')
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS pages (url TEXT, content TEXT)''')

def is_valid_url(url):
    """Check if the url is valid and not just an internal link or fragment."""
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def store_data(url, content):
    """Store the URL and content in the SQLite database."""
    c.execute("INSERT INTO pages (url, content) VALUES (?, ?)", (url, content))
    conn.commit()

def get_page_content(url):
    """Fetch page content without using proxies."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        print(f"Failed to fetch {url}: {e}")
        return None

def crawl_web(base_url, max_depth=2):
    visited = set()
    queue = deque([(base_url, 0)])
    try:
        while queue:
            current_url, depth = queue.popleft()
            if depth > max_depth:
                print(f"Stopped processing at depth {depth} for URL {current_url}")
                break

            if current_url not in visited:
                print(f"Processing URL {current_url} at depth {depth}")
                content = get_page_content(current_url)
                if content:
                    soup = BeautifulSoup(content, 'html.parser')
                    store_data(current_url, content)
                    visited.add(current_url)

                    for link in soup.find_all('a'):
                        href = link.get('href')
                        if href:
                            full_url = urljoin(current_url, href)
                            if is_valid_url(full_url) and full_url not in visited:
                                queue.append((full_url, depth + 1))
                                print(f"Queued {full_url}")
                                logging.info(f"Queued {full_url}")
    finally:
        conn.close()  # Ensure the database connection is closed
        logging.info("Crawling completed.")
        print("Crawling completed.")

# Example usage
if __name__ == "__main__":
    base_url = 'https://example.com/'  # Replace with the actual URL
    crawl_web(base_url, max_depth=2)
