import json
import os

from confluent_kafka import Consumer, KafkaException
import logging
from ingester import elastic_ingester

logger = logging.getLogger(__name__)

class BulkMessageProcessor:
    def __init__(self, config, topic, batch_size=100):
        self.batch_size = batch_size
        self.consumer = Consumer({**config,
                                  'bootstrap.servers': os.getenv('CONFLUENT.bootstrap.servers'),
                                  'sasl.username': os.getenv('CONFLUENT.sasl.username'),
                                  'sasl.password': os.getenv('CONFLUENT.sasl.password'),
                                  'client.id': os.getenv('CONFLUENT.client.id')})

        self.consumer.subscribe([topic])

    def poll_batch(self):
        """Poll exactly batch_size messages"""
        batch = []
        while len(batch) < self.batch_size:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                break  # no more messages available right now
            if msg.error():
                raise KafkaException(msg.error())
            logger.info(f' Processed Offset : {msg.offset()}')
            key = msg.key().decode("utf-8")
            value = msg.value().decode("utf-8")
            article = json.loads(value)
            article['article_id'] = key
            batch.append(article)
        return batch

    def run(self):
        try:
            while True:
                # Step 1 — Poll batch
                batch = self.poll_batch()
                if not batch:
                    continue

                # Step 2 — Process batch
                elastic_ingester.bulk_index_articles(batch)

                # Step 3 — Commit same batch
                self.consumer.commit(asynchronous=False)
                logger.info(f"Polled → Processed → Committed {len(batch)} messages")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.commit(asynchronous=False)
            self.consumer.close()