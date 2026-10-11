
"""
The main module that loads config, calling common
 module for setup logging and loading secrets from
 vault server that is running in docker on localhost.
 After successful completion it will submit an async process
 to consume messages from kafka and processing into Elasticsearch.
"""
import asyncio

from pathlib import Path
from config.logging_setup import setup_logging
from config.vault_secrets import load_secrets
from config.application_config import read_config
from config.application_config import get_config
from consumer.bulk_message_processor import BulkMessageProcessor

def main():
    setup_logging()
    load_secrets()
    read_config(str(Path(__file__).parent)+"/resources/application_config.toml")
    bulk_message_processor = BulkMessageProcessor(get_config('TOPIC_NAME'), get_config('INGESTER_BATCH_SIZE'))
    asyncio.run(bulk_message_processor.run())

main()