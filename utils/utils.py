import logging
import os
from config.config import LOG_FILE

# Ensure logs directory exists
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def log_info(msg, print_to_term=True):
    """Log an INFO message and optionally print to terminal."""
    logging.info(msg)
    if print_to_term:
        print(f"[INFO] {msg}")

def log_error(msg, print_to_term=True):
    """Log an ERROR message and optionally print to terminal."""
    logging.error(msg)
    if print_to_term:
        print(f"[ERROR] {msg}")

def log_debug(msg, print_to_term=False):
    """Log a DEBUG message (mostly for terminal debugging)."""
    logging.debug(msg)
    if print_to_term:
        print(f"[DEBUG] {msg}")