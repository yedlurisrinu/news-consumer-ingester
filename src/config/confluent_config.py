
import logging
logger = logging.getLogger(__name__)

def read_config(client_properties):
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  try:
    with open(client_properties) as fh:
      for line in fh:
        line = line.strip()
        if len(line) != 0 and line[0] != "#":
          parameter, value = line.strip().split('=', 1)
          config[parameter] = value.strip()
  except FileNotFoundError as ex:
    logger.error(f" Exception while loading client properties {ex}",exc_info=True)
    raise ex
  return config

