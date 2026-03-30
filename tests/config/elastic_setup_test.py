"""
Unit tests for src/config/elastic_setup.py
"""
import pytest
from unittest.mock import patch, MagicMock

from config.elastic_setup import get_config, get_elastic_instance


VALID_PROPERTIES = (
    "request_timeout=60\n"
    "retry_on_timeout=True\n"
    "max_retries=3\n"
    "http_compress=True\n"
    "connections_per_node=10\n"
)


def _write_props(tmp_path, content=VALID_PROPERTIES):
    """Write config to the resources/ subdir that get_config() expects."""
    resources = tmp_path / "resources"
    resources.mkdir()
    (resources / "elastic-config.properties").write_text(content)
    return tmp_path


class TestGetConfig:
    def test_parses_all_properties(self, tmp_path, monkeypatch):
        _write_props(tmp_path)
        monkeypatch.setattr("config.elastic_setup.BASE_PATH", str(tmp_path))

        config = get_config()

        assert config["request_timeout"] == "60"
        assert config["retry_on_timeout"] == "True"
        assert config["max_retries"] == "3"
        assert config["http_compress"] == "True"
        assert config["connections_per_node"] == "10"

    def test_ignores_comment_lines(self, tmp_path, monkeypatch):
        _write_props(tmp_path, "# URL loaded from vault\nrequest_timeout=30\n")
        monkeypatch.setattr("config.elastic_setup.BASE_PATH", str(tmp_path))

        config = get_config()

        assert len(config) == 1
        assert config["request_timeout"] == "30"

    def test_ignores_blank_lines(self, tmp_path, monkeypatch):
        _write_props(tmp_path, "\n\nmax_retries=5\n\n")
        monkeypatch.setattr("config.elastic_setup.BASE_PATH", str(tmp_path))

        config = get_config()

        assert config["max_retries"] == "5"

    def test_returns_empty_dict_when_file_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("config.elastic_setup.BASE_PATH", "/nonexistent/path")

        with patch("config.elastic_setup.logger"):
            config = get_config()

        assert config == {}

    def test_logs_error_when_file_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("config.elastic_setup.BASE_PATH", "/nonexistent/path")

        with patch("config.elastic_setup.logger") as mock_logger:
            get_config()
            mock_logger.error.assert_called_once()


class TestGetElasticInstance:
    def test_creates_elasticsearch_with_env_vars(self, tmp_path, monkeypatch):
        _write_props(tmp_path)
        monkeypatch.setattr("config.elastic_setup.BASE_PATH", str(tmp_path))
        monkeypatch.setenv("ELASTIC_URL", "https://my-cluster.es.io:443")
        monkeypatch.setenv("ELASTIC_API_KEY", "my-secret-api-key")

        with patch("config.elastic_setup.Elasticsearch") as mock_es_cls:
            mock_es_cls.return_value = MagicMock()
            instance = get_elastic_instance()

            mock_es_cls.assert_called_once_with(
                "https://my-cluster.es.io:443",
                api_key="my-secret-api-key",
                request_timeout=60,
                retry_on_timeout=True,
                max_retries=3,
                http_compress=True,
                connections_per_node=10,
            )
            assert instance is mock_es_cls.return_value

    def test_passes_correct_int_and_bool_types(self, tmp_path, monkeypatch):
        _write_props(tmp_path)
        monkeypatch.setattr("config.elastic_setup.BASE_PATH", str(tmp_path))
        monkeypatch.setenv("ELASTIC_URL", "https://cluster.es.io")
        monkeypatch.setenv("ELASTIC_API_KEY", "key")

        with patch("config.elastic_setup.Elasticsearch") as mock_es_cls:
            mock_es_cls.return_value = MagicMock()
            get_elastic_instance()

            _, kwargs = mock_es_cls.call_args
            assert isinstance(kwargs["request_timeout"], int)
            assert isinstance(kwargs["max_retries"], int)
            assert isinstance(kwargs["connections_per_node"], int)