
"""
The main module that loads config, calling common
 module for setup logging and loading secrets from
 vault server that is running in docker on localhost.
 After successful completion it will submit an async process
 to consume messages from kafka and processing into Elasticsearch.
"""
import asyncio

from pathlib import Path
from py_commons_per.logging_setup import setup_logging
from py_commons_per.vault_secret_loader import load_secrets
from config.confluent_config import read_config
from config import settings
from consumer.bulk_message_processor import BulkMessageProcessor

def main():
    setup_logging()
    load_secrets()
    bulk_message_processor = BulkMessageProcessor(read_config(str(Path(__file__).parent)+"/resources/confluent-client.properties"), settings.TOPIC_NAME)
    asyncio.run(bulk_message_processor.run())

main()