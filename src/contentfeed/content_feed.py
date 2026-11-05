import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from config.application_config import get_config

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _http_client() -> httpx.Client:
    """Shared HTTP client (connection pooling) for all article fetches"""
    feed_config = get_config("CONTENT_FEED")
    return httpx.Client(
        timeout=feed_config["timeout"],
        follow_redirects=True,
        headers={"User-Agent": feed_config["user_agent"]},
    )


def _is_ignored_host(host: str, ignore_domains: list[str]) -> bool:
    return any(
        host == domain or host.endswith("." + domain) for domain in ignore_domains
    )


def fetch_full_article(url: str) -> str:
    try:
        feed_config = get_config("CONTENT_FEED")
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            logger.debug(f"[Article Fetcher] Skipping unsupported URL scheme for {url}")
            return ""

        # Skip social/video links
        if _is_ignored_host(
            (parsed.hostname or "").lower(), feed_config["ignore_domains"]
        ):
            return ""

        response = _http_client().get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Remove noise
        for tag in soup(
            ["nav", "footer", "aside", "script", "style", "iframe", "form", "ads"]
        ):
            tag.decompose()

        # Extract main content
        main = (
            soup.find("article")
            or soup.find("main")
            or soup.find("div", class_=lambda c: c and "article" in c.lower())
            or soup.find("body")
        )

        if main is None:
            logger.warning(f"[Article Fetcher] No content element found for {url}")
            return ""

        text = main.get_text(separator=" ", strip=True)
        return text[: feed_config["max_content_chars"]]

    except Exception as e:  # noqa: BLE001 - one failed article must not stop the batch
        logger.error(f"[Article Fetcher] Failed to fetch {url}: {e}")
        return ""


def fetch_article_contents(articles: list[dict[str, Any]]) -> list[str]:
    """
    Fetch full content for all articles concurrently within an overall time budget.
    Returns one content string per article (same order); falls back to the
    article summary when the fetch is empty or did not finish within budget.
    """
    feed_config = get_config("CONTENT_FEED")
    contents = [article["summary"] for article in articles]
    if not articles:
        return contents

    _http_client()  # create the shared client before worker threads use it
    executor = ThreadPoolExecutor(max_workers=int(feed_config.get("max_workers", 8)))
    futures = {
        executor.submit(fetch_full_article, article["link"]): index
        for index, article in enumerate(articles)
    }
    completed = 0
    try:
        for future in as_completed(
            futures, timeout=float(feed_config.get("fetch_budget_seconds", 120))
        ):
            index = futures[future]
            contents[index] = future.result() or articles[index]["summary"]
            completed += 1
    except TimeoutError:
        logger.warning(
            f"[Article Fetcher] Fetch budget exceeded, "
            f"{len(articles) - completed} articles fall back to summary"
        )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return contents
