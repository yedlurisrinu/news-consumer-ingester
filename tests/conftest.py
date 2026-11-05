"""
Shared fixtures: load the real application config for every test.
"""
from pathlib import Path

import pytest

from config import application_config


@pytest.fixture(autouse=True)
def app_config(monkeypatch):
    monkeypatch.setattr(application_config, "config", {})
    application_config.read_config(
        str(Path(__file__).parent.parent / "src" / "resources" / "application_config.toml")
    )
