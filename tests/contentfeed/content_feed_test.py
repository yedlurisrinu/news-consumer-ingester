"""
Unit tests for src/contentfeed/content_feed.py
"""
import pytest
from unittest.mock import patch, MagicMock
import httpx

from contentfeed.content_feed import fetch_full_article, IGNORE_DOMAINS


class TestFetchFullArticle:
    # ── Ignored domains ────────────────────────────────────────────────────

    @pytest.mark.parametrize("url", [
        "https://www.youtube.com/watch?v=abc123",
        "https://twitter.com/user/status/123",
        "https://www.facebook.com/story/456",
        "https://www.instagram.com/p/xyz",
    ])
    def test_returns_empty_string_for_ignored_domains(self, url):
        result = fetch_full_article(url)
        assert result == ""

    # ── Successful article fetch ────────────────────────────────────────────

    def test_extracts_article_text(self):
        html = """
        <html><body>
          <article>
            <p>This is the full article content about AI news.</p>
          </article>
        </body></html>
        """
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()

        with patch("contentfeed.content_feed.httpx.get", return_value=mock_resp):
            result = fetch_full_article("https://example.com/news/ai")

        assert "full article content about AI news" in result

    def test_falls_back_to_main_tag(self):
        html = """
        <html><body>
          <main><p>Main section content.</p></main>
        </body></html>
        """
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()

        with patch("contentfeed.content_feed.httpx.get", return_value=mock_resp):
            result = fetch_full_article("https://example.com/story")

        assert "Main section content" in result

    def test_falls_back_to_body_when_no_article_or_main(self):
        html = "<html><body><p>Body content here.</p></body></html>"
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()

        with patch("contentfeed.content_feed.httpx.get", return_value=mock_resp):
            result = fetch_full_article("https://example.com/page")

        assert "Body content here" in result

    def test_removes_nav_footer_script_noise(self):
        html = """
        <html><body>
          <nav>Navigation bar</nav>
          <article><p>Real content.</p></article>
          <footer>Footer text</footer>
          <script>console.log('js')</script>
        </body></html>
        """
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()

        with patch("contentfeed.content_feed.httpx.get", return_value=mock_resp):
            result = fetch_full_article("https://example.com/news")

        assert "Navigation bar" not in result
        assert "Footer text" not in result
        assert "console.log" not in result
        assert "Real content" in result

    def test_caps_content_at_5000_chars(self):
        long_text = "word " * 2000  # ~10000 chars
        html = f"<html><body><article><p>{long_text}</p></article></body></html>"
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()

        with patch("contentfeed.content_feed.httpx.get", return_value=mock_resp):
            result = fetch_full_article("https://example.com/long-article")

        assert len(result) <= 5000

    # ── Error handling ──────────────────────────────────────────────────────

    def test_returns_empty_string_on_http_error(self):
        with patch("contentfeed.content_feed.httpx.get",
                   side_effect=httpx.HTTPStatusError("404", request=MagicMock(), response=MagicMock())):
            result = fetch_full_article("https://example.com/missing")

        assert result == ""

    def test_returns_empty_string_on_timeout(self):
        with patch("contentfeed.content_feed.httpx.get",
                   side_effect=httpx.TimeoutException("timeout")):
            result = fetch_full_article("https://example.com/slow")

        assert result == ""

    def test_returns_empty_string_on_connection_error(self):
        with patch("contentfeed.content_feed.httpx.get",
                   side_effect=httpx.ConnectError("connection refused")):
            result = fetch_full_article("https://example.com/unreachable")

        assert result == ""

    def test_logs_error_on_exception(self):
        with patch("contentfeed.content_feed.httpx.get",
                   side_effect=Exception("unexpected error")), \
             patch("contentfeed.content_feed.logger") as mock_logger:
            fetch_full_article("https://example.com/fail")
            mock_logger.error.assert_called_once()

    def test_sends_user_agent_header(self):
        mock_resp = MagicMock()
        mock_resp.text = "<html><body><p>ok</p></body></html>"
        mock_resp.raise_for_status = MagicMock()

        with patch("contentfeed.content_feed.httpx.get", return_value=mock_resp) as mock_get:
            fetch_full_article("https://example.com/article")
            _, kwargs = mock_get.call_args
            assert "User-Agent" in kwargs.get("headers", {})

    def test_uses_10s_timeout(self):
        mock_resp = MagicMock()
        mock_resp.text = "<html><body><p>ok</p></body></html>"
        mock_resp.raise_for_status = MagicMock()

        with patch("contentfeed.content_feed.httpx.get", return_value=mock_resp) as mock_get:
            fetch_full_article("https://example.com/article")
            _, kwargs = mock_get.call_args
            assert kwargs.get("timeout") == 10


class TestIgnoreDomains:
    def test_all_expected_domains_present(self):
        for domain in ["youtube.com", "twitter.com", "facebook.com", "instagram.com"]:
            assert domain in IGNORE_DOMAINS