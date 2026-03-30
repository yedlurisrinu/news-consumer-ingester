"""
Unit tests for src/config/settings.py
"""
from config import settings


class TestSettings:
    def test_topic_name_is_defined(self):
        assert hasattr(settings, "TOPIC_NAME")
        assert isinstance(settings.TOPIC_NAME, str)
        assert len(settings.TOPIC_NAME) > 0

    def test_topic_name_value(self):
        assert settings.TOPIC_NAME == "news_ai_pipeline_dev_t_0"

    def test_ingester_batch_size_is_defined(self):
        assert hasattr(settings, "INGESTER_BATCH_SIZE")
        assert isinstance(settings.INGESTER_BATCH_SIZE, int)

    def test_ingester_batch_size_positive(self):
        assert settings.INGESTER_BATCH_SIZE > 0

    def test_ingester_batch_size_value(self):
        assert settings.INGESTER_BATCH_SIZE == 50