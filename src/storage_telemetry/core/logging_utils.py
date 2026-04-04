import logging
import logging.config
import yaml
from .config import load_config

def setup_logging():
    config = load_config("logging.yaml")
    logging.config.dictConfig(config)
