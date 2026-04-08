import logging
import os

# create logs folder
os.makedirs("/workspace/logs", exist_ok=True)

# logging setup
logging.basicConfig(
    filename="/workspace/logs/app.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ✅ FIXED FUNCTIONS (THIS WAS MISSING)
def log_info(msg):
    print(msg)
    logging.info(msg)

def log_error(msg):
    print(msg)
    logging.error(msg)