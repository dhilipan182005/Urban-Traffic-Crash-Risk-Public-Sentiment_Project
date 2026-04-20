import os
import sys
import glob
from pyspark.sql import SparkSession
from config.config import (
    BRONZE_CHICAGO, BRONZE_YOUTUBE,
    SILVER_CHICAGO, SILVER_YOUTUBE,
    GOLD_CHICAGO, GOLD_YOUTUBE
)
from utils.utils import log_info, log_success, log_error, validate_dataframe


def check_layer(spark, path, name, format="parquet"):
    log_info(f"Validating {name}...")

    if not os.path.exists(path):
        log_error(f"Validation failed - {name}: directory does not exist at {path}")
        return False

    try:
        if format == "json":
            files = glob.glob(f"{path}/*.json")
            if not files:
                log_error(f"Validation failed - {name}: no JSON files found")
                return False
            df = spark.read.json(f"{path}/*.json")
        else:
            df = spark.read.parquet(path)

        is_valid = validate_dataframe(df)

        if is_valid:
            log_success(f"{name}: validation passed")
            return True
        else:
            log_error(f"Validation failed - {name}: row count is 0")
            return False

    except Exception as e:
        log_error(f"Validation failed - {name}: {str(e)}")
        return False


def main():
    log_info("Pipeline Validation - Running")

    spark = SparkSession.builder \
        .appName("Pipeline-Validation") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    checks = {
        "Bronze Chicago":        (BRONZE_CHICAGO,                      "json"),
        "Bronze YouTube":        (BRONZE_YOUTUBE,                      "json"),
        "Silver Chicago":        (SILVER_CHICAGO,                      "parquet"),
        "Silver YouTube":        (SILVER_YOUTUBE,                      "parquet"),
        "Gold Chicago (Risk)":   (f"{GOLD_CHICAGO}/location_risk",     "parquet"),
        "Gold YouTube (Channel)":(f"{GOLD_YOUTUBE}/channel_stats",     "parquet"),
    }

    results = {
        name: check_layer(spark, path, name, fmt)
        for name, (path, fmt) in checks.items()
    }

    spark.stop()

    log_info("Pipeline execution summary")
    all_pass = True
    for layer, passed in results.items():
        if passed:
            log_success(f"{layer} - passed")
        else:
            log_error(f"{layer} - FAILED")
            all_pass = False

    if all_pass:
        log_success("All validation checks passed")
    else:
        log_error("Validation failed - one or more layers did not pass")
        sys.exit(1)


if __name__ == "__main__":
    main()
