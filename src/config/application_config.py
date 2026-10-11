
import logging
from tomllib import load
logger = logging.getLogger(__name__)

config = {}

def get_config(key: str)  -> any:
  return config[key]

def read_config(app_config_path: str) -> None:
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  global config
  try:
    with open(app_config_path, mode='rb') as fh:
      config = load(fh)
    logger.info(f'config loaded = {config}')
  except FileNotFoundError as ex:
    logger.error(f" Exception while loading application config {ex}",exc_info=True)
    raise ex
