"""
tests/gaps_test.py

Branches the module tests don't reach:
  content_feed  - unsupported scheme, ignored domain (and sub-domain), page without content
                  element, fetch budget exceeded -> summary fallback
  consumer      - retriable Kafka error skipped, fatal error raised, heartbeat OSError
                  logged, a signal stops the loop and the consumer is closed
"""

import signal
import time
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from contentfeed import content_feed


class TestFetchFullArticleGuards:
    @pytest.mark.parametrize(
        "url", ["ftp://example.com/a", "file:///etc/passwd", "javascript:alert(1)"]
    )
    def test_unsupported_scheme_is_not_fetched(self, url):
        with patch.object(content_feed, "_http_client") as client:
            assert content_feed.fetch_full_article(url) == ""
        client.assert_not_called()

    @pytest.mark.parametrize(
        "url",
        [
            "https://www.youtube.com/watch?v=x",
            "https://youtube.com/x",
            "https://m.facebook.com/p",
        ],
    )
    def test_ignored_domains_and_subdomains_are_skipped(self, url):
        with patch.object(content_feed, "_http_client") as client:
            assert content_feed.fetch_full_article(url) == ""
        client.assert_not_called()

    def test_lookalike_domain_is_not_ignored(self):
        response = MagicMock(
            text="<html><body><article>real text</article></body></html>"
        )
        with patch.object(content_feed, "_http_client") as client:
            client.return_value.get.return_value = response
            assert (
                content_feed.fetch_full_article("https://notyoutube.com/a")
                == "real text"
            )

    def test_page_without_content_element_is_empty(self):
        response = MagicMock(text="<html></html>")
        with (
            patch.object(content_feed, "_http_client") as client,
            patch("contentfeed.content_feed.BeautifulSoup") as soup_cls,
        ):
            client.return_value.get.return_value = response
            soup = soup_cls.return_value
            soup.return_value = []  # soup([...noise tags]) -> nothing to remove
            soup.find.return_value = None  # no article/main/div/body
            assert content_feed.fetch_full_article("https://example.com/a") == ""


class TestFetchBudget:
    def test_slow_fetches_fall_back_to_summary(self, monkeypatch):
        cfg = dict(content_feed.get_config("CONTENT_FEED"), fetch_budget_seconds=0.2)
        monkeypatch.setattr(content_feed, "get_config", lambda key: cfg)
        articles = [{"link": "https://example.com/slow", "summary": "summary text"}]

        def slow(url):
            time.sleep(1)
            return "late content"

        with (
            patch.object(content_feed, "_http_client"),
            patch.object(content_feed, "fetch_full_article", side_effect=slow),
            patch.object(content_feed, "logger") as logger,
        ):
            assert content_feed.fetch_article_contents(articles) == ["summary text"]
        logger.warning.assert_called_once()


@pytest.fixture()
def processor(monkeypatch, tmp_path):
    from consumer import bulk_message_processor as bmp

    monkeypatch.setattr(bmp, "HEARTBEAT_FILE", str(tmp_path / "heartbeat"))
    with patch.object(bmp, "Consumer") as consumer_cls:
        yield bmp.BulkMessageProcessor("topic", 10), consumer_cls.return_value, bmp


def _error_msg(retriable: bool, code: int = KafkaError._TRANSPORT):
    error = MagicMock()
    error.code.return_value = code
    error.retriable.return_value = retriable
    msg = MagicMock()
    msg.error.return_value = error
    return msg


class TestConsumerBranches:
    def test_retriable_error_is_skipped(self, processor):
        proc, consumer, _ = processor
        consumer.poll.side_effect = [_error_msg(retriable=True), None]
        assert proc.poll_batch() == [] and proc.last_poll_consumed == 0

    def test_partition_eof_is_skipped(self, processor):
        proc, consumer, _ = processor
        consumer.poll.side_effect = [
            _error_msg(retriable=False, code=KafkaError._PARTITION_EOF),
            None,
        ]
        assert proc.poll_batch() == []

    def test_fatal_error_is_raised(self, processor):
        proc, consumer, _ = processor
        consumer.poll.side_effect = [_error_msg(retriable=False)]
        with pytest.raises(KafkaException):
            proc.poll_batch()

    def test_heartbeat_oserror_is_logged_not_raised(self, processor):
        proc, _, bmp = processor
        with (
            patch.object(bmp, "Path") as path_cls,
            patch.object(bmp, "logger") as logger,
        ):
            path_cls.return_value.touch.side_effect = OSError("read-only")
            proc._touch_heartbeat()
        logger.debug.assert_called_once()

    def test_signal_stops_loop_and_closes_consumer(self, processor):
        proc, consumer, _ = processor

        def poll(timeout):
            proc._request_stop(
                signal.SIGTERM, None
            )  # as if SIGTERM arrived while polling

        consumer.poll.side_effect = poll
        proc.run()  # returns instead of looping forever
        assert proc._stop is True
        consumer.close.assert_called_once()
        consumer.commit.assert_not_called()
