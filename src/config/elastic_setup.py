import os

from elasticsearch import Elasticsearch
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
BASE_PATH = str(Path(__file__).resolve().parent.parent)
def get_config():
    resource_path = BASE_PATH+"/resources/elastic-config.properties"
    config = {}
    try:
        with open(resource_path, 'r') as file_handle:
            entries = file_handle.readlines()
            for entry in entries:
                entry = entry.strip()
                if len(entry) > 0 and not entry.startswith("#"):
                    key, value = entry.split(sep="=", maxsplit=1)
                    config[key.strip()] = value.strip()

    except FileNotFoundError as ex:
        logger.error(" Exception while reading elasticsearch config files % ex ",ex, exc_info=True)

    return config

def get_elastic_instance():
    config = get_config()
    return Elasticsearch(
        os.getenv("ELASTIC_URL"),
        api_key=os.getenv('ELASTIC_API_KEY'),
        request_timeout = int(config['request_timeout']),
        retry_on_timeout = bool(config['retry_on_timeout']),
        max_retries = int(config['max_retries']),
        http_compress = bool(config['http_compress']),
        # Connection pooling for cloud,
        connections_per_node = int(config['connections_per_node'])
    )
