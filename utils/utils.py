import os
import sys
import time
import logging
import requests
from datetime import datetime
from config.config import LOG_FILE, MAX_RETRIES, BACKOFF_FACTOR

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# File handler
_file_handler = logging.FileHandler(LOG_FILE)
_file_handler.setFormatter(logging.Formatter("%(message)s"))

# Stream handler
_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.setFormatter(logging.Formatter("%(message)s"))

_logger = logging.getLogger("pipeline")
_logger.setLevel(logging.DEBUG)
_logger.addHandler(_file_handler)
_logger.addHandler(_stream_handler)
# Prevent double
_logger.propagate = False


def _ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_info(msg):
    _logger.info(f"[INFO] {_ts()} - {msg}")


def log_success(msg):
    _logger.info(f"[SUCCESS] {_ts()} - {msg}")


def log_error(msg):
    _logger.error(f"[ERROR] {_ts()} - {msg}")


# Kept for compatibility
def log_debug(msg):
    _logger.debug(f"[DEBUG] {_ts()} - {msg}")


def print_progress(step, status="In Progress"):
    """Emit a single structured status line; no decorative borders."""
    if status.lower() in ("completed", "success"):
        log_success(f"{step} - {status}")
    elif status.lower() in ("failed", "error"):
        log_error(f"{step} - {status}")
    else:
        log_info(f"{step} - {status}")


def request_with_retry(url, params=None, headers=None, method="GET"):
    backoff = 1
    for attempt in range(MAX_RETRIES):
        try:
            if method.upper() == "GET":
                response = requests.get(url, params=params, headers=headers, timeout=30)
            elif method.upper() == "POST":
                response = requests.post(url, json=params, headers=headers, timeout=30)
            else:
                raise ValueError(f"Unsupported method: {method}")

            if response.status_code == 200:
                return response.json()

            if response.status_code in [429, 500, 502, 503, 504]:
                log_error(f"Transient error {response.status_code}. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= BACKOFF_FACTOR
                continue

            log_error(f"Error {response.status_code}: {response.text}")
            break

        except Exception as e:
            log_error(f"Exception during request: {str(e)}")
            time.sleep(backoff)
            backoff *= BACKOFF_FACTOR

    return None


def validate_dataframe(df):
    """Run basic DQ checks. Returns True if row count > 0."""
    count = df.count()
    log_info(f"Row count: {count}")
    for col_name in df.columns:
        null_count = df.filter(df[col_name].isNull()).count()
        if null_count > 0:
            log_info(f"DQ Alert: Column '{col_name}' has {null_count} records below 0 or null.")
    return count > 0


def print_summary(df, name):
    """Log a concise summary of a DataFrame — no decorative borders, no df.show()."""
    count = df.count()
    cols = ", ".join(df.columns)
    log_info(f"--- SUMMARY: {name} ---")
    log_info(f"Total Records: {count}")
    log_info(f"Columns: {cols}")


def save_local_copy(df, path):
    log_info(f"Saving local copy to: {path}")
    os.makedirs(path, exist_ok=True)
    df.write.mode("overwrite").parquet(f"file:///{path.lstrip('/')}")