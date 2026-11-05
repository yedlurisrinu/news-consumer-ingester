"""
Unit tests for src/ingester/elastic_ingester.py
"""

from unittest.mock import patch

import pytest

import ingester.elastic_ingester as ingester_module
from ingester.elastic_ingester import (
    batch_articles,
    bulk_index,
    bulk_index_articles,
    generate_bulk_actions,
)

INDEX_NAME = "news_articles"


# ── Sample data ────────────────────────────────────────────────────────────


def _make_article(article_id: str = "art-001") -> dict:
    return {
        "article_id": article_id,
        "title": "Test Title",
        "link": "https://example.com/news/test",
        "summary": "A short summary.",
        "published_at": "2026-03-15T14:01:23Z",
        "fetched_at": "2026-03-18T03:51:24Z",
        "source": "TestSource",
    }


# ── generate_bulk_actions ──────────────────────────────────────────────────


class TestGenerateBulkActions:
    def test_yields_one_action_per_article(self):
        articles = [_make_article("a1"), _make_article("a2")]

        actions = list(
            generate_bulk_actions(
                articles, INDEX_NAME, ["full content"] * len(articles)
            )
        )

        assert len(actions) == 2

    def test_action_has_correct_op_type_and_index(self):
        articles = [_make_article("a1")]

        action = next(
            generate_bulk_actions(articles, INDEX_NAME, ["content"] * len(articles))
        )

        assert action["_op_type"] == "index"
        assert action["_index"] == INDEX_NAME

    def test_uses_article_id_as_document_id(self):
        articles = [_make_article("unique-id-99")]

        action = next(
            generate_bulk_actions(articles, INDEX_NAME, ["content"] * len(articles))
        )

        assert action["_id"] == "unique-id-99"

    def test_source_contains_all_required_fields(self):
        articles = [_make_article("a1")]

        action = next(
            generate_bulk_actions(
                articles, INDEX_NAME, ["fetched content"] * len(articles)
            )
        )

        source = action["_source"]
        for field in (
            "article_id",
            "title",
            "content",
            "link",
            "published_at",
            "fetched_at",
            "source",
        ):
            assert field in source

    def test_uses_fetched_content_when_available(self):
        articles = [_make_article()]

        action = next(
            generate_bulk_actions(
                articles, INDEX_NAME, ["fetched full article"] * len(articles)
            )
        )

        assert action["_source"]["content"] == "fetched full article"

    def test_falls_back_to_summary_when_fetch_returns_empty(self):
        article = _make_article()
        article["summary"] = "fallback summary"

        with (
            patch("contentfeed.content_feed.fetch_full_article", return_value=""),
            patch("contentfeed.content_feed._http_client"),
        ):
            contents = ingester_module.fetch_article_contents([article])
        action = next(generate_bulk_actions([article], INDEX_NAME, contents))

        assert action["_source"]["content"] == "fallback summary"

    def test_is_a_generator(self):
        import types

        articles = [_make_article()]

        result = generate_bulk_actions(articles, INDEX_NAME, ["x"] * len(articles))

        assert isinstance(result, types.GeneratorType)


# ── batch_articles ─────────────────────────────────────────────────────────


class TestBatchArticles:
    def test_splits_into_equal_batches(self):
        articles = [_make_article(f"a{i}") for i in range(6)]
        batches = list(batch_articles(articles, 2))
        assert len(batches) == 3
        for b in batches:
            assert len(b) == 2

    def test_last_batch_may_be_smaller(self):
        articles = [_make_article(f"a{i}") for i in range(5)]
        batches = list(batch_articles(articles, 3))
        assert len(batches) == 2
        assert len(batches[-1]) == 2

    def test_single_batch_when_articles_fewer_than_size(self):
        articles = [_make_article(f"a{i}") for i in range(3)]
        batches = list(batch_articles(articles, 10))
        assert len(batches) == 1
        assert len(batches[0]) == 3

    def test_empty_list_yields_nothing(self):
        batches = list(batch_articles([], 5))
        assert batches == []


# ── bulk_index ─────────────────────────────────────────────────────────────


class TestBulkIndex:
    def test_calls_elasticsearch_bulk(self):
        articles = [_make_article()]

        with (
            patch("ingester.elastic_ingester.bulk", return_value=(1, [])) as mock_bulk,
            patch(
                "ingester.elastic_ingester.fetch_article_contents",
                return_value=["content"],
            ),
            patch("ingester.elastic_ingester._es_connection"),
        ):
            bulk_index(articles, INDEX_NAME)

        mock_bulk.assert_called_once()

    def test_returns_success_and_errors(self):
        articles = [_make_article()]

        with (
            patch("ingester.elastic_ingester.bulk", return_value=(1, [])),
            patch(
                "ingester.elastic_ingester.fetch_article_contents",
                return_value=["content"],
            ),
            patch("ingester.elastic_ingester._es_connection"),
        ):
            success, errors = bulk_index(articles, INDEX_NAME)

        assert success == 1
        assert errors == []

    def test_propagates_bulk_request_error(self):
        articles = [_make_article()]

        with (
            patch(
                "ingester.elastic_ingester.bulk",
                side_effect=Exception("ES connection error"),
            ),
            patch(
                "ingester.elastic_ingester.fetch_article_contents",
                return_value=["content"],
            ),
            patch("ingester.elastic_ingester._es_connection"),
            pytest.raises(Exception, match="ES connection error"),
        ):
            bulk_index(articles, INDEX_NAME)

    def test_logs_partial_failures(self):
        articles = [_make_article()]
        partial_errors = [{"index": {"_id": "a1", "error": "conflict"}}]

        with (
            patch("ingester.elastic_ingester.bulk", return_value=(0, partial_errors)),
            patch(
                "ingester.elastic_ingester.fetch_article_contents",
                return_value=["content"],
            ),
            patch("ingester.elastic_ingester._es_connection"),
            patch("ingester.elastic_ingester.logger") as mock_logger,
        ):
            bulk_index(articles, INDEX_NAME)

        assert mock_logger.info.call_count >= 1
        mock_logger.error.assert_called_once()


# ── bulk_index_articles ────────────────────────────────────────────────────


class TestBulkIndexArticles:
    def test_does_nothing_for_empty_list(self):
        with (
            patch("ingester.elastic_ingester.bulk_index") as mock_bulk_index,
            patch("ingester.elastic_ingester.logger") as mock_logger,
        ):
            bulk_index_articles([])

        mock_bulk_index.assert_not_called()
        mock_logger.info.assert_called()

    def test_indexes_single_batch(self):
        articles = [_make_article(f"a{i}") for i in range(5)]
        with (
            patch(
                "ingester.elastic_ingester.bulk_index", return_value=(5, [])
            ) as mock_bulk_index,
            patch("ingester.elastic_ingester.get_config") as mock_config,
        ):
            mock_config.return_value = 50
            bulk_index_articles(articles)

        mock_bulk_index.assert_called_once()

    def test_splits_large_batch_into_chunks(self):
        # INGESTER_BATCH_SIZE is 50; send 120 articles
        articles = [_make_article(f"a{i}") for i in range(120)]

        with (
            patch(
                "ingester.elastic_ingester.bulk_index", return_value=(50, [])
            ) as mock_bulk_index,
            patch("ingester.elastic_ingester.get_config") as mock_config,
        ):
            mock_config.return_value = 50
            bulk_index_articles(articles)

        assert mock_bulk_index.call_count == 3  # 50 + 50 + 20

    def test_accumulates_totals_across_batches(self):
        articles = [_make_article(f"a{i}") for i in range(100)]

        with (
            patch("ingester.elastic_ingester.bulk_index", return_value=(50, [])),
            patch("ingester.elastic_ingester.logger") as mock_logger,
            patch("ingester.elastic_ingester.get_config") as mock_config,
        ):
            mock_config.return_value = 50
            bulk_index_articles(articles)

        # Final summary log contains totals
        last_call_args = mock_logger.info.call_args_list[-1][0][0]
        assert "100" in last_call_args or "total" in last_call_args.lower()

    @staticmethod
    def _err(doc_id, status, error_type):
        return {
            "index": {"_id": doc_id, "status": status, "error": {"type": error_type}}
        }

    def _run_with_errors(self, n_docs, success, errors):
        articles = [_make_article(f"a{i}") for i in range(n_docs)]
        with patch(
            "ingester.elastic_ingester.bulk_index", return_value=(success, errors)
        ):
            bulk_index_articles(articles)

    def test_single_document_level_error_is_committed_past(self):
        self._run_with_errors(
            1, 0, [self._err("a0", 400, "mapper_parsing_exception")]
        )  # no raise

    def test_partial_document_level_errors_are_committed_past(self):
        self._run_with_errors(
            3, 2, [self._err("a0", 400, "document_parsing_exception")]
        )

    @pytest.mark.parametrize(
        "status,error_type",
        [
            (
                400,
                "strict_dynamic_mapping_exception",
            ),  # mapping no longer matches the producer
            (404, "index_not_found_exception"),
            (403, "security_exception"),  # API key lacks write rights
            (
                429,
                "es_rejected_execution_exception",
            ),  # still throttled after bulk's retries
            (503, "unavailable_shards_exception"),
        ],
    )
    def test_systematic_or_transient_error_blocks_commit(self, status, error_type):
        from ingester.elastic_ingester import BatchRejected

        with pytest.raises(BatchRejected):
            self._run_with_errors(1, 0, [self._err("a0", status, error_type)])

    def test_any_blocking_error_in_a_mixed_batch_blocks_commit(self):
        from ingester.elastic_ingester import BatchRejected

        errors = [
            self._err("a0", 400, "mapper_parsing_exception"),
            self._err("a1", 503, "unavailable_shards_exception"),
        ]
        with pytest.raises(BatchRejected):
            self._run_with_errors(5, 3, errors)

    def test_raises_on_batch_exception(self):
        articles = [_make_article(f"a{i}") for i in range(100)]
        call_count = 0

        def side_effect(batch, index):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("ES connection error")
            return (len(batch), [])

        with (
            patch("ingester.elastic_ingester.bulk_index", side_effect=side_effect),
            patch("ingester.elastic_ingester.get_config") as mock_config,
        ):
            mock_config.return_value = 50
            # Must raise so the consumer does not commit offsets
            with pytest.raises(Exception, match="ES connection error"):
                bulk_index_articles(articles)

        assert call_count == 1
