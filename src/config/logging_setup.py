from pathlib import Path

# Start scheduler
import json
import logging
import logging.config

logger = logging.getLogger(__name__)


def setup_logging(config_file="logging_config.json"):
    """Loads the logging configuration."""
    logger.info("Logging configuration starting.")
    path = str(Path(__name__).parent.parent)
    try:
        with open(path + "/" + config_file) as file_handler:
            config = json.load(file_handler)
        logging.config.dictConfig(config)
    except Exception as fe:
        logging.basicConfig(level=logging.INFO)
        logger.error(f"Failed to load application logging {fe}", exc_info=True)
    logger.info("Logging configured and application starting.")
