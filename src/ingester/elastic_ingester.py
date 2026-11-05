import logging
from collections.abc import Iterator
from functools import lru_cache
from typing import Any, cast

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from config.application_config import get_config
from config.elastic_setup import get_elastic_instance
from contentfeed.content_feed import fetch_article_contents

logger = logging.getLogger(__name__)


class BatchRejected(RuntimeError):
    """A document failed for a systematic or transient reason (e.g. a mapping change, a missing index,
    an auth error, 429/5xx): the same failure would hit every document, so stop before committing."""


@lru_cache(maxsize=1)
def _es_connection() -> Elasticsearch:
    return get_elastic_instance()


def generate_bulk_actions(
    articles: list[dict[str, Any]], index_name: str, contents: list[str]
) -> Iterator[dict[str, Any]]:
    """
    Generator function — memory efficient for large batches
    _id is explicitly set from article_id
    contents holds the prefetched article content, one entry per article
    """
    for article, content in zip(articles, contents, strict=True):
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_id": article["article_id"],  # explicit _id prevents duplicates
            "_source": {
                "article_id": article["article_id"],
                "title": article["title"],
                "content": content,
                "link": article["link"],
                "published_at": article["published_at"],
                "fetched_at": article["fetched_at"],
                "source": article["source"],
            },
        }


def _error_info(error: dict[str, Any]) -> tuple[Any, int | None, str]:
    """(_id, HTTP status, error type) of one per-item bulk failure."""
    result: Any = next(iter(error.values()), {})
    if not isinstance(result, dict):
        result = {}
    item_error = result.get("error")
    error_type = (
        item_error.get("type", "unknown") if isinstance(item_error, dict) else "unknown"
    )
    status = result.get("status")
    return (
        result.get("_id"),
        status if isinstance(status, int) else None,
        str(error_type),
    )


def _log_index_error(error: dict[str, Any]) -> None:
    """Log one per-item bulk failure without dumping the whole item"""
    doc_id, status, error_type = _error_info(error)
    logger.error(
        f"[Indexer] Failed to index _id={doc_id} status={status} error_type={error_type}"
    )


def _is_blocking(error: dict[str, Any]) -> bool:
    """True when the failure is systematic or transient (don't commit), False when it is about this document."""
    rules = get_config("INDEX_ERRORS")
    _, status, error_type = _error_info(error)
    return (
        error_type in rules["blocking_types"]
        or status in rules["blocking_statuses"]
        or (status is not None and status >= 500)
    )


def bulk_index(
    articles: list[dict[str, Any]], index_name: str
) -> tuple[int, list[dict[str, Any]]]:
    # Fetch full contents up front (concurrently, time-bounded) so bulk() is not slowed by HTTP fetches
    contents = fetch_article_contents(articles)
    bulk_config = get_config("BULK")
    success, bulk_errors = bulk(
        _es_connection(),
        generate_bulk_actions(articles, index_name, contents),
        chunk_size=bulk_config["chunk_size"],
        max_retries=bulk_config["max_retries"],
        raise_on_error=False,  # don't stop on partial failure
        initial_backoff=bulk_config["initial_backoff"],
        max_backoff=bulk_config["max_backoff"],
        request_timeout=bulk_config["request_timeout"],
    )
    # stats_only=False (default): bulk() returns the list of per-item errors, not a count
    errors = cast(list[dict[str, Any]], bulk_errors)

    logger.info(f"Indexed: {success}, Failed: {len(errors)}")

    for error in errors:
        _log_index_error(error)

    return success, errors


def batch_articles(
    articles: list[dict[str, Any]], batch_size: int
) -> Iterator[list[dict[str, Any]]]:
    for i in range(0, len(articles), batch_size):
        yield articles[i : i + batch_size]


def bulk_index_articles(articles: list[dict[str, Any]]) -> None:
    if not articles:
        logger.info("[Indexer] No articles to index")
        return

    total_success = 0
    total_failed = 0
    index_name = get_config("ES_INDEX")

    for batch in batch_articles(articles, get_config("INGESTER_BATCH_SIZE")):
        try:
            success, errors = bulk_index(batch, index_name)
        except Exception:
            # Re-raise so the consumer does not commit offsets for unindexed messages
            logger.exception("[Indexer] Bulk request failed")
            raise
        blocking = [e for e in errors if _is_blocking(e)]
        if blocking:
            # Document-level errors (one bad document) are logged and committed past. A blocking
            # (systematic or transient, per INDEX_ERRORS) error would hit every document, so raise
            # instead: the batch is not committed, the process restarts and the batch is retried.
            _, status, error_type = _error_info(blocking[0])
            logger.error(
                f"[Indexer] {len(blocking)} of {len(errors)} index errors are systematic/transient "
                f"(e.g. status={status} error_type={error_type}); not committing"
            )
            raise BatchRejected(
                f"{len(blocking)} systematic/transient index errors (e.g. {error_type})"
            )
        total_success += success
        total_failed += len(errors)
        logger.info(
            f"[Indexer] Batch indexed — success: {success} failed: {len(errors)}"
        )
    logger.info(
        f"[Indexer] Complete — total success: {total_success} total failed: {total_failed}"
    )
