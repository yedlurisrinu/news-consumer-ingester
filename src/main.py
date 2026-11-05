
"""Entry point: set up logging, load secrets from Vault and the TOML config, then run the
Kafka consumer loop (poll a batch → index into Elasticsearch → commit) until SIGTERM."""
from pathlib import Path
from config.logging_setup import setup_logging
from config.vault_secrets import load_secrets
from config.application_config import read_config
from config.application_config import get_config
from consumer.bulk_message_processor import BulkMessageProcessor

def main() -> None:
    setup_logging()
    load_secrets()
    read_config(str(Path(__file__).parent)+"/resources/application_config.toml")
    bulk_message_processor = BulkMessageProcessor(get_config('TOPIC_NAME'), get_config('INGESTER_BATCH_SIZE'))
    bulk_message_processor.run()

if __name__ == "__main__":
    main()
