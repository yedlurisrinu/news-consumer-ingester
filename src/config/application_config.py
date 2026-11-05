import logging
from tomllib import load
from typing import Any

logger = logging.getLogger(__name__)

config: dict[str, Any] = {}


def get_config(key: str) -> Any:
    return config[key]


def read_config(app_config_path: str) -> None:
    # reads the application configuration TOML file
    # into the module-level config map
    global config
    try:
        with open(app_config_path, mode="rb") as fh:
            config = load(fh)
    except FileNotFoundError:
        logger.exception(" Exception while loading application config")
        raise
