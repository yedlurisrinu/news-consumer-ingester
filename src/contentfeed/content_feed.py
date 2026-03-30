import httpx
from bs4 import BeautifulSoup
import logging
logger = logging.getLogger(__name__)
IGNORE_DOMAINS = [
    "youtube.com", "twitter.com",
    "facebook.com", "instagram.com"
]

def fetch_full_article(url: str) -> str:
    try:
        # Skip social/video links
        if any(domain in url for domain in IGNORE_DOMAINS):
            return ""

        headers = {"User-Agent": "Mozilla/5.0"}
        response = httpx.get(url, timeout = 10, headers = headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Remove noise
        for tag in soup(["nav","footer","aside","script",
                         "style","iframe","form","ads"]):
            tag.decompose()

        # Extract main content
        main = (
            soup.find("article") or
            soup.find("main") or
            soup.find("div", class_=lambda c: c and
                      "article" in c.lower()) or
            soup.find("body")
        )

        text = main.get_text(separator=" ", strip=True)
        return text[:5000]  # Cap at 5000 chars

    except Exception as e:
        logger.error(f"[Article Fetcher] Failed to fetch {url}: {e}")
        return ""