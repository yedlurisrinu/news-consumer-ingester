"""
Unit tests for src/consumer/bulk_message_processor.py
"""
import json
import pytest
from unittest.mock import MagicMock, patch, call

from confluent_kafka import KafkaException

from consumer.bulk_message_processor import BulkMessageProcessor


# ── Helpers ────────────────────────────────────────────────────────────────

def _make_message(key: str, value: dict, offset: int = 0) -> MagicMock:
    msg = MagicMock()
    msg.error.return_value = None
    msg.key.return_value = key.encode("utf-8")
    msg.value.return_value = json.dumps(value).encode("utf-8")
    msg.offset.return_value = offset
    return msg


SAMPLE_ARTICLE = {
    "title": "Test Article",
    "link": "https://example.com/article",
    "summary": "A test summary",
    "published_at": "2026-03-15T14:01:23Z",
    "fetched_at": "2026-03-18T03:51:24Z",
    "source": "TestSource",
}


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture()
def mock_consumer():
    with patch("consumer.bulk_message_processor.Consumer") as mock_cls:
        instance = MagicMock()
        mock_cls.return_value = instance
        yield mock_cls, instance


@pytest.fixture()
def processor(mock_consumer, monkeypatch):
    monkeypatch.setenv("CONFLUENT.bootstrap.servers", "localhost:9092")
    monkeypatch.setenv("CONFLUENT.sasl.username", "user")
    monkeypatch.setenv("CONFLUENT.sasl.password", "pass")
    monkeypatch.setenv("CONFLUENT.client.id", "test-client")

    _, consumer_instance = mock_consumer
    with patch('consumer.bulk_message_processor.get_config') as mock_config:
        mock_config.return_value = {'security.protocol': 'PLAINTEXT', 'bootstrap.servers': 'kafka:9092', 'group.id': 'news_ai_summary_group', 'auto.offset.reset': 'latest', 'enable.auto.commit': 'False', 
                                    'session.timeout.ms': 45000, 'heartbeat.interval.ms': 3000, 'max.poll.interval.ms': 300000, 'socket.keepalive.enable': 'True', 'reconnect.backoff.ms': 1000, 'reconnect.backoff.max.ms': 10000}
        proc = BulkMessageProcessor("test-topic", batch_size=3)
    return proc, consumer_instance


# ── Constructor tests ──────────────────────────────────────────────────────

class TestBulkMessageProcessorInit:
    def test_subscribes_to_topic(self, processor):
        proc, consumer_instance = processor
        consumer_instance.subscribe.assert_called_once_with(["test-topic"])

    def test_sets_batch_size(self, processor):
        proc, _ = processor
        assert proc.batch_size == 3

# ── poll_batch tests ───────────────────────────────────────────────────────

class TestPollBatch:
    def test_returns_batch_of_messages(self, processor):
        proc, consumer_instance = processor
        msgs = [_make_message(f"id-{i}", SAMPLE_ARTICLE, i) for i in range(3)]
        consumer_instance.poll.side_effect = msgs

        batch = proc.poll_batch()

        assert len(batch) == 3
        assert batch[0]["article_id"] == "id-0"
        assert batch[2]["article_id"] == "id-2"

    def test_stops_when_no_message(self, processor):
        proc, consumer_instance = processor
        msg = _make_message("id-0", SAMPLE_ARTICLE)
        consumer_instance.poll.side_effect = [msg, None]

        batch = proc.poll_batch()

        assert len(batch) == 1

    def test_attaches_article_id_from_key(self, processor):
        proc, consumer_instance = processor
        consumer_instance.poll.side_effect = [
            _make_message("article-key-42", SAMPLE_ARTICLE),
            None,
        ]

        batch = proc.poll_batch()

        assert batch[0]["article_id"] == "article-key-42"

    def test_stops_at_batch_size(self, processor):
        proc, consumer_instance = processor
        # 5 messages but batch_size=3
        msgs = [_make_message(f"id-{i}", SAMPLE_ARTICLE, i) for i in range(5)]
        consumer_instance.poll.side_effect = msgs

        batch = proc.poll_batch()

        assert len(batch) == 3

    def test_raises_kafka_exception_on_message_error(self, processor):
        proc, consumer_instance = processor
        error_msg = MagicMock()
        error_msg.error.return_value = MagicMock()  # truthy error
        consumer_instance.poll.return_value = error_msg

        with pytest.raises(KafkaException):
            proc.poll_batch()

    def test_polls_with_timeout_1s(self, processor):
        proc, consumer_instance = processor
        consumer_instance.poll.return_value = None

        proc.poll_batch()

        consumer_instance.poll.assert_called_with(timeout=1.0)


# ── run tests ──────────────────────────────────────────────────────────────

class TestRun:
    def test_processes_and_commits_batch(self, processor):
        proc, consumer_instance = processor
        msgs = [_make_message(f"id-{i}", SAMPLE_ARTICLE, i) for i in range(2)]
        # First call returns batch, second call returns empty to trigger continue, third KeyboardInterrupt
        consumer_instance.poll.side_effect = msgs + [None, KeyboardInterrupt()]

        with patch("consumer.bulk_message_processor.elastic_ingester") as mock_ingester:
            proc.run()

        mock_ingester.bulk_index_articles.assert_called_once()
        consumer_instance.commit.assert_called_with(asynchronous=False)

    def test_skips_empty_batch(self, processor):
        proc, consumer_instance = processor
        # First poll returns None (empty batch), second raises KeyboardInterrupt
        consumer_instance.poll.side_effect = [None, KeyboardInterrupt()]

        with patch("consumer.bulk_message_processor.elastic_ingester") as mock_ingester:
            proc.run()

        mock_ingester.bulk_index_articles.assert_not_called()

    def test_commits_and_closes_on_keyboard_interrupt(self, processor):
        proc, consumer_instance = processor
        consumer_instance.poll.side_effect = KeyboardInterrupt()

        with patch("consumer.bulk_message_processor.elastic_ingester"):
            proc.run()

        consumer_instance.commit.assert_called_with(asynchronous=False)
        consumer_instance.close.assert_called_once()

    def test_closes_consumer_in_finally(self, processor):
        proc, consumer_instance = processor
        consumer_instance.poll.side_effect = [None, KeyboardInterrupt()]

        with patch("consumer.bulk_message_processor.elastic_ingester"):
            proc.run()

        consumer_instance.close.assert_called_once()