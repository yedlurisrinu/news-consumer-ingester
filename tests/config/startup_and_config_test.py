"""
tests/config/startup_and_config_test.py

Start-up code without live services:
  - load_secrets: required env vars, Vault auth failure, secrets injected, read failure wrapped
  - setup_logging: dictConfig from resources, fallback to basicConfig on a bad file
  - read_config: a missing file is logged and re-raised
  - get_elastic_instance: fails fast without ELASTIC_API_KEY, builds the client from config
  - main: start-up order and the consumer built from TOPIC_NAME / INGESTER_BATCH_SIZE
"""

import logging
import os
from unittest.mock import MagicMock, patch

import pytest

VAULT_ENV = {
    "VAULT_ADDR": "http://vault-server:8200",
    "VAULT_TOKEN": "tok",
    "VAULT_SECRET_PATH": "secret/CREDENTIALS",
}


class TestLoadSecrets:
    def test_missing_addr_or_token_raises(self, monkeypatch):
        from config.vault_secrets import load_secrets

        monkeypatch.delenv("VAULT_ADDR", raising=False)
        monkeypatch.delenv("VAULT_TOKEN", raising=False)
        with pytest.raises(ValueError, match="VAULT_ADDR and VAULT_TOKEN"):
            load_secrets()

    def test_missing_secret_path_raises(self, monkeypatch):
        from config.vault_secrets import load_secrets

        monkeypatch.setenv("VAULT_ADDR", VAULT_ENV["VAULT_ADDR"])
        monkeypatch.setenv("VAULT_TOKEN", "tok")
        monkeypatch.delenv("VAULT_SECRET_PATH", raising=False)
        with pytest.raises(ValueError, match="VAULT_SECRET_PATH"):
            load_secrets()

    def test_auth_failure_raises(self, monkeypatch):
        from config.vault_secrets import load_secrets

        for k, v in VAULT_ENV.items():
            monkeypatch.setenv(k, v)
        client = MagicMock()
        client.is_authenticated.return_value = False
        with (
            patch("config.vault_secrets.Client", return_value=client),
            pytest.raises(ValueError, match="authentication failed"),
        ):
            load_secrets()

    def test_success_injects_env(self, monkeypatch):
        from config.vault_secrets import load_secrets

        for k, v in VAULT_ENV.items():
            monkeypatch.setenv(k, v)
        monkeypatch.delenv("INJECTED_KEY", raising=False)
        client = MagicMock()
        client.is_authenticated.return_value = True
        client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"INJECTED_KEY": "value"}}
        }
        with patch("config.vault_secrets.Client", return_value=client):
            load_secrets()
        assert os.environ["INJECTED_KEY"] == "value"
        assert client.secrets.kv.v2.read_secret_version.call_args.kwargs == {
            "path": "CREDENTIALS",
            "mount_point": "secret",
        }
        monkeypatch.delenv("INJECTED_KEY")

    def test_read_failure_is_wrapped_without_details(self, monkeypatch):
        from config.vault_secrets import load_secrets

        for k, v in VAULT_ENV.items():
            monkeypatch.setenv(k, v)
        client = MagicMock()
        client.is_authenticated.return_value = True
        client.secrets.kv.v2.read_secret_version.side_effect = PermissionError(
            "vault-server:8200 says no"
        )
        with (
            patch("config.vault_secrets.Client", return_value=client),
            pytest.raises(ValueError) as exc,
        ):
            load_secrets()
        assert "PermissionError" in str(exc.value) and "vault-server" not in str(
            exc.value
        )
        assert isinstance(exc.value.__cause__, PermissionError)


class TestSetupLogging:
    def test_loads_dict_config_from_resources(self):
        from config.logging_setup import setup_logging

        with patch("config.logging_setup.logging.config.dictConfig") as dict_config:
            setup_logging()
        (config,), _ = dict_config.call_args
        assert (
            config["handlers"]["file"]["class"]
            == "logging.handlers.TimedRotatingFileHandler"
        )

    def test_bad_file_falls_back_to_basic_config(self):
        from config.logging_setup import setup_logging

        with (
            patch("config.logging_setup.logging.basicConfig") as basic,
            patch("config.logging_setup.logger") as logger,
        ):
            setup_logging("does-not-exist.json")
        basic.assert_called_once_with(level=logging.INFO)
        logger.exception.assert_called_once()


class TestReadConfig:
    def test_missing_file_is_logged_and_raised(self, tmp_path):
        from config import application_config

        with (
            patch.object(application_config, "logger") as logger,
            pytest.raises(FileNotFoundError),
        ):
            application_config.read_config(str(tmp_path / "missing.toml"))
        logger.exception.assert_called_once()


class TestElasticInstance:
    def test_missing_api_key_fails_fast(self, monkeypatch):
        from config.elastic_setup import get_elastic_instance

        monkeypatch.delenv("ELASTIC_API_KEY", raising=False)
        with (
            patch("config.elastic_setup.Elasticsearch") as es_cls,
            pytest.raises(RuntimeError, match="ELASTIC_API_KEY"),
        ):
            get_elastic_instance()
        es_cls.assert_not_called()

    def test_builds_client_from_config(self, monkeypatch):
        from config.application_config import get_config
        from config.elastic_setup import get_elastic_instance

        monkeypatch.setenv("ELASTIC_API_KEY", "write-key")
        with patch("config.elastic_setup.Elasticsearch") as es_cls:
            assert get_elastic_instance() is es_cls.return_value
        args, kwargs = es_cls.call_args
        es = get_config("ELASTIC")
        assert args == (get_config("URL"),) and kwargs["api_key"] == "write-key"
        assert (
            kwargs["request_timeout"] == es["request_timeout"]
            and kwargs["max_retries"] == es["max_retries"]
        )
        assert (
            kwargs["retry_on_timeout"] is es["retry_on_timeout"]
            and kwargs["http_compress"] is es["http_compress"]
        )


class TestMain:
    def test_start_up_order_and_consumer_settings(self):
        import main
        from config.application_config import get_config

        calls = MagicMock()
        with (
            patch.object(main, "setup_logging", calls.setup_logging),
            patch.object(main, "load_secrets", calls.load_secrets),
            patch.object(main, "read_config", calls.read_config),
            patch.object(main, "BulkMessageProcessor", calls.BulkMessageProcessor),
        ):
            main.main()

        names = [
            c[0]
            for c in calls.mock_calls
            if not c[0].startswith("BulkMessageProcessor().")
        ]
        assert names[:3] == ["setup_logging", "load_secrets", "read_config"]
        assert calls.read_config.call_args[0][0].endswith(
            "resources/application_config.toml"
        )
        calls.BulkMessageProcessor.assert_called_once_with(
            get_config("TOPIC_NAME"), get_config("INGESTER_BATCH_SIZE")
        )
        calls.BulkMessageProcessor.return_value.run.assert_called_once_with()
