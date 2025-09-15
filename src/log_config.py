# Set up repo root path
import os
import sys
repo_root = os.path.abspath(os.getcwd())  
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Configure logging
from src.config import env
import logging

def setup_logger(name=None, level=None):

    if level is None:
        if env == "prod":
            level = logging.INFO
        elif env == "dev":
            level = logging.DEBUG
        else:
            level = logging.DEBUG

    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)

    return logger