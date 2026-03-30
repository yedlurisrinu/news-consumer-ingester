import json
from functools import lru_cache

from elasticsearch import Elasticsearch

from config.elastic_setup import get_elastic_instance
from config.settings import INGESTER_BATCH_SIZE
from pathlib import Path
from dotenv import load_dotenv
import logging
from elasticsearch.helpers import bulk, BulkIndexError

from contentfeed.content_feed import fetch_full_article

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / "config" / ".env")

INDEX_NAME = "news_articles"

logger = logging.getLogger(__name__)

@lru_cache(maxsize=1)
def _es_connection() -> Elasticsearch:
    return get_elastic_instance()

def generate_bulk_actions(articles, index_name):
    """
    Generator function — memory efficient for large batches
    _id is explicitly set from article_id
    """
    for article in articles:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_id": article["article_id"],  # explicit _id prevents duplicates
            "_source": {
                "article_id": article["article_id"],
                "title": article["title"],
                "content": fetch_full_article(article["link"]) or article["summary"],
                "link": article["link"],
                "published_at": article["published_at"],
                "fetched_at": article["fetched_at"],
                "source": article["source"]
            }
        }

def bulk_index(articles, index_name):
    try:
        success, errors = bulk(
            _es_connection(),
            generate_bulk_actions(articles, index_name),
            chunk_size=50,  # matches your Kafka batch size
            max_retries=3,
            raise_on_error=False,  # don't stop on partial failure
            initial_backoff=2,  # wait 2s before first retry
            max_backoff=60,  # max wait between retries
            request_timeout=60,
        )

        logger.info(f"Indexed: {success}, Failed: {len(errors)}")

        if errors:
            for error in errors:
                logger.info(f"Failed to index: {error}")

        return success, errors

    except BulkIndexError as e:
        logger.info(f"Bulk index error: {e}")
        return 0, e.errors

def batch_articles(articles: list[dict], batch_size: int):
    for i in range(0, len(articles), batch_size):
        yield articles[i:i + batch_size]

def bulk_index_articles(articles: list[dict]):
    if not articles:
        logger.info("[Indexer] No articles to index")
        return

    total_success = 0
    total_failed  = 0

    for batch in batch_articles(articles, INGESTER_BATCH_SIZE):
        try:
            success, errors = bulk_index(batch, INDEX_NAME)
            total_success += success
            total_failed  += len(errors)

            if errors:
                for error in errors:
                    logger.info(f"[Indexer] Failed to index: {error}")
            logger.info(f"[Indexer] Batch indexed — success: {success} failed: {len(errors)}")
        except Exception as e:
            logger.info(f"[Indexer] Bulk request failed: {e}")
    logger.info(f"[Indexer] Complete — total success: {total_success} total failed: {total_failed}")

if __name__ == "__main__":
    def load():
        elastic_articles = []
        # TODO replace this file path
        with open("resources/data.json") as file_handle:
            documents = json.load(file_handle)
            for doc in documents:
                elastic_articles.append(doc)
        return elastic_articles

    bulk_index_articles(load())