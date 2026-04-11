import time
import requests
from utils.utils import log_info, log_error, log_debug
from config.config import MAX_RETRIES, BACKOFF_FACTOR

def request_with_retry(url, params=None, headers=None, method="GET"):
    """
    Generic API requester with exponential backoff and error handling.
    """
    backoff = 1
    
    for attempt in range(MAX_RETRIES):
        log_debug(f"Request URL: {url} (Attempt {attempt + 1})", print_to_term=True)
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, params=params, headers=headers)
            elif method.upper() == "POST":
                response = requests.post(url, json=params, headers=headers)
            else:
                raise ValueError(f"Unsupported method: {method}")

            log_info(f"API Request to {url} - Status: {response.status_code}", print_to_term=True)

            if response.status_code == 200:
                return response.json()
            
            # Handle rate limiting (429) or server errors (5xx)
            if response.status_code in [429, 500, 502, 503, 504]:
                log_error(f"Transient error {response.status_code}. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= BACKOFF_FACTOR
                continue
            
            # Non-transient errors
            log_error(f"Error {response.status_code}: {response.text}")
            break

        except Exception as e:
            log_error(f"Exception during request: {str(e)}")
            time.sleep(backoff)
            backoff *= BACKOFF_FACTOR
            
    return None
