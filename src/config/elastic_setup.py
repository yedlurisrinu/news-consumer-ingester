import os

from elasticsearch import Elasticsearch
import logging
from config.application_config import get_config

logger = logging.getLogger(__name__)


def get_elastic_instance():
    config = get_config('ELASTIC')
    return Elasticsearch(
        get_config("URL"),
        api_key=os.getenv('ELASTIC_API_KEY'),
        request_timeout = int(config['request_timeout']),
        retry_on_timeout = bool(config['retry_on_timeout']),
        max_retries = int(config['max_retries']),
        http_compress = bool(config['http_compress']),
        # Connection pooling for cloud,
        connections_per_node = int(config['connections_per_node'])
    )
