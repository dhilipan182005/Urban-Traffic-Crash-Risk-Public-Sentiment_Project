from pyspark.sql.functions import col, to_timestamp, year, month, when, desc, row_number
from pyspark.sql.window import Window
from utils.utils import log_info, log_error

def check_nulls(df, column_name):
    null_count = df.filter(col(column_name).isNull()).count()
    if null_count > 0:
        log_info(f"DQ Alert: Column '{column_name}' has {null_count} null records.")
    return null_count

def check_numeric_validity(df, column_name, min_val=0):
    invalid_count = df.filter((col(column_name) < min_val) | col(column_name).isNull()).count()
    if invalid_count > 0:
        log_info(f"DQ Alert: Column '{column_name}' has {invalid_count} records below {min_val} or null.")
    return invalid_count

def transform_chicago_dq(df):
    log_info("Starting DQ validation for Chicago dataset...")
    
    initial_count = df.count()
    
    df = df.filter(col("crash_record_id").isNotNull())
    df = df.filter(col("injuries_total") >= 0)
    
    final_count = df.count()
    dropped = initial_count - final_count
    
    log_info(f"DQ Result: {final_count} valid records kept, {dropped} invalid records dropped.")
    return df

def transform_youtube_dq(df):
    log_info("Starting DQ validation for YouTube dataset...")
    
    initial_count = df.count()
    
    df = df.filter(col("video_id").isNotNull())
    df = df.filter(col("views") >= 0)
    
    final_count = df.count()
    dropped = initial_count - final_count
    
    log_info(f"DQ Result: {final_count} valid records kept, {dropped} invalid records dropped.")
    return df

def transform_chicago(df):
    log_info("Applying transformations to Chicago data...")

    window_spec = Window.partitionBy("crash_record_id").orderBy(desc("crash_date"))
    df = df.withColumn("row_num", row_number().over(window_spec)) \
           .filter(col("row_num") == 1) \
           .drop("row_num")

    df = df.withColumn("crash_date", to_timestamp("crash_date"))
    df = df.withColumn("year", year("crash_date")) \
           .withColumn("month", month("crash_date"))

    df = df.withColumn(
        "severity",
        when((col("injuries_total") >= 5), "HIGH")
        .when(col("injuries_total") >= 1, "MEDIUM")
        .otherwise("LOW")
    )

    df = df.withColumn(
        "time_of_day",
        when(col("lighting_condition").contains("DAYLIGHT"), "DAY")
        .otherwise("NIGHT")
    )

    return df

def transform_youtube(df):
    log_info("Applying transformations to YouTube data...")

    window_spec = Window.partitionBy("video_id").orderBy(desc("views"))
    df = df.withColumn("row_num", row_number().over(window_spec)) \
           .filter(col("row_num") == 1) \
           .drop("row_num")

    df = df.withColumn(
        "engagement_score",
        when(col("views") > 0, (col("likes") + col("comments")) / col("views"))
        .otherwise(0)
    )

    df = df.withColumn(
        "is_viral",
        when(col("views") > 1000000, True).otherwise(False)
    )

    return df