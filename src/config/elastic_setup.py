import logging
import os

from elasticsearch import Elasticsearch

from config.application_config import get_config

logger = logging.getLogger(__name__)


def get_elastic_instance() -> Elasticsearch:
    api_key = os.getenv("ELASTIC_API_KEY")
    if not api_key:
        raise RuntimeError("ELASTIC_API_KEY is not set")
    config = get_config("ELASTIC")
    return Elasticsearch(
        get_config("URL"),
        api_key=api_key,
        request_timeout=int(config["request_timeout"]),
        retry_on_timeout=config["retry_on_timeout"],
        max_retries=int(config["max_retries"]),
        http_compress=config["http_compress"],
        # Connection pooling for cloud,
        connections_per_node=int(config["connections_per_node"]),
    )
