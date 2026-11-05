import json
import signal
from pathlib import Path
from types import FrameType
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
import logging
from ingester import elastic_ingester
from config.application_config import get_config

logger = logging.getLogger(__name__)

# Touched every poll loop iteration; the Docker HEALTHCHECK reads its mtime
HEARTBEAT_FILE = "/tmp/heartbeat"

# Fields the ingester reads from every message (the scheduler's Article model). Values may be
# null; a missing key would raise later and the batch would never commit, so skip it here.
REQUIRED_FIELDS = ("title", "summary", "link", "published_at", "fetched_at", "source")

class BulkMessageProcessor:
    def __init__(self, topic: str, batch_size: int = 100) -> None:
        self.batch_size = batch_size
        self.poll_timeout = float(get_config('POLL_TIMEOUT_SECONDS'))
        self._stop = False
        # messages taken off Kafka by the last poll_batch(), including skipped ones
        self.last_poll_consumed = 0
        self.consumer = Consumer(**get_config('KAFKA'))
        self.consumer.subscribe([topic])

    def poll_batch(self) -> list[dict[str, Any]]:
        """Poll up to batch_size messages; stops early when none is waiting. Bad messages are skipped."""
        batch: list[dict[str, Any]] = []
        self.last_poll_consumed = 0
        while len(batch) < self.batch_size:
            msg = self.consumer.poll(timeout=self.poll_timeout)
            if msg is None:
                break  # no more messages available right now
            error = msg.error()
            if error:
                if error.code() == KafkaError._PARTITION_EOF or error.retriable():
                    logger.warning(f"Kafka non-fatal error on partition {msg.partition()}: {error}")
                    continue
                raise KafkaException(error)
            logger.info(f' Processed Offset : {msg.offset()}')
            self.last_poll_consumed += 1
            raw_key, raw_value = msg.key(), msg.value()
            if raw_key is None or raw_value is None:
                logger.error(f"Skipping Kafka message with no key or value at partition {msg.partition()} offset {msg.offset()}")
                continue
            try:
                key = raw_key.decode("utf-8")
                value = raw_value.decode("utf-8")
            except UnicodeDecodeError:
                logger.error(f"Skipping Kafka message that is not valid UTF-8 at partition {msg.partition()} offset {msg.offset()}")
                continue
            try:
                article = json.loads(value)
            except json.JSONDecodeError:
                logger.error(f"Skipping Kafka message with invalid JSON at partition {msg.partition()} offset {msg.offset()}")
                continue
            if not isinstance(article, dict):
                logger.error(f"Skipping Kafka message that is not a JSON object at partition {msg.partition()} offset {msg.offset()}")
                continue
            missing = [field for field in REQUIRED_FIELDS if field not in article]
            if missing:
                logger.error(f"Skipping Kafka message missing fields {missing} at partition {msg.partition()} offset {msg.offset()}")
                continue
            article['article_id'] = key
            batch.append(article)
        return batch

    def _request_stop(self, signum: int, frame: FrameType | None) -> None:
        logger.info(f"Received signal {signal.Signals(signum).name}, stopping consumer")
        self._stop = True

    def _touch_heartbeat(self) -> None:
        try:
            Path(HEARTBEAT_FILE).touch()
        except OSError:
            logger.debug("Unable to update heartbeat file", exc_info=True)

    def run(self) -> None:
        previous_handlers = {
            signum: signal.signal(signum, self._request_stop)
            for signum in (signal.SIGTERM, signal.SIGINT)
        }
        try:
            while not self._stop:
                self._touch_heartbeat()
                # Step 1 — Poll batch
                batch = self.poll_batch()
                if not batch:
                    if self.last_poll_consumed:
                        # every message in this poll was skipped: commit past them so they are not re-read
                        self.consumer.commit(asynchronous=False)
                        logger.info(f"Committed past {self.last_poll_consumed} skipped messages")
                    continue

                # Step 2 — Process batch
                elastic_ingester.bulk_index_articles(batch)

                # Step 3 — Commit same batch (only reached when indexing succeeded)
                self.consumer.commit(asynchronous=False)
                logger.info(f"Polled → Processed → Committed {len(batch)} messages")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            for signum, handler in previous_handlers.items():
                if handler is not None:
                    signal.signal(signum, handler)
