from pathlib import Path

import json
import logging
import logging.config

logger = logging.getLogger(__name__)


def setup_logging(config_file: str = "logging_config.json") -> None:
    """Loads the logging configuration."""
    logger.info("Logging configuration starting.")
    path = str(Path(__file__).parent.parent)
    try:
        with open(path + "/resources/" + config_file) as file_handler:
            config = json.load(file_handler)
        logging.config.dictConfig(config)
    except Exception as fe:
        logging.basicConfig(level=logging.INFO)
        logger.error(f"Failed to load application logging {fe}", exc_info=True)
    logger.info("Logging configured and application starting.")
