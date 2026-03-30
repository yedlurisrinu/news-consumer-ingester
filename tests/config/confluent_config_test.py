"""
Unit tests for src/config/confluent_config.py
"""
import pytest
from unittest.mock import mock_open, patch, MagicMock

from config.confluent_config import read_config


class TestReadConfig:
    def test_reads_key_value_pairs(self, tmp_path):
        props = tmp_path / "client.properties"
        props.write_text("security.protocol=SASL_SSL\nsasl.mechanisms=PLAIN\n")

        config = read_config(str(props))

        assert config["security.protocol"] == "SASL_SSL"
        assert config["sasl.mechanisms"] == "PLAIN"

    def test_ignores_comment_lines(self, tmp_path):
        props = tmp_path / "client.properties"
        props.write_text("# this is a comment\ngroup.id=my_group\n")

        config = read_config(str(props))

        assert "# this is a comment" not in config
        assert config["group.id"] == "my_group"

    def test_ignores_blank_lines(self, tmp_path):
        props = tmp_path / "client.properties"
        props.write_text("\n\nauto.offset.reset=latest\n\n")

        config = read_config(str(props))

        assert config["auto.offset.reset"] == "latest"
        assert len(config) == 1

    def test_strips_whitespace_from_values(self, tmp_path):
        props = tmp_path / "client.properties"
        props.write_text("session.timeout.ms=  45000  \n")

        config = read_config(str(props))

        assert config["session.timeout.ms"] == "45000"

    def test_value_with_equals_sign(self, tmp_path):
        """Values containing '=' must be preserved correctly (split on first = only)."""
        props = tmp_path / "client.properties"
        props.write_text("sasl.password=abc=def=ghi\n")

        config = read_config(str(props))

        assert config["sasl.password"] == "abc=def=ghi"

    def test_returns_empty_dict_for_empty_file(self, tmp_path):
        props = tmp_path / "client.properties"
        props.write_text("")

        config = read_config(str(props))

        assert config == {}

    def test_raises_file_not_found_when_missing(self):
        with pytest.raises(FileNotFoundError):
            read_config("/nonexistent/path/client.properties")

    def test_logs_error_on_file_not_found(self):
        with patch("config.confluent_config.logger") as mock_logger:
            with pytest.raises(FileNotFoundError):
                read_config("/no/such/file.properties")
            mock_logger.error.assert_called_once()