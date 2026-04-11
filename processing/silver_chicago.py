from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from config.config import BRONZE_CHICAGO, SILVER_CHICAGO
from processing.transformations import validate_chicago, transform_chicago
import os

def process():
    spark = SparkSession.builder \
        .appName("Silver-Chicago") \
        .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()
    
    print(f"[INFO] Reading raw data from: {BRONZE_CHICAGO}")
    if not os.path.exists(BRONZE_CHICAGO) or not os.listdir(BRONZE_CHICAGO):
        print("[WARNING] No data found in Bronze Chicago. Exiting.")
        return

    # Read all JSON batches
    df = spark.read.json(BRONZE_CHICAGO)
    
    # Extract records from metadata wrapper
    df = df.select(explode("data").alias("record")).select("record.*")
    initial_count = df.count()
    print(f"[INFO] Records ingested: {initial_count}")

    # Data Quality Validation
    df = validate_chicago(df)
    valid_count = df.count()
    if initial_count > valid_count:
        print(f"[INFO] Dropped {initial_count - valid_count} invalid records.")

    # Deduplication
    df = df.dropDuplicates(["crash_record_id"])
    final_count = df.count()
    print(f"[INFO] Records after deduplication: {final_count}")

    # Transformations & Feature Engineering
    df = transform_chicago(df)

    print("\n[INFO] --- Silver Chicago Schema ---")
    df.printSchema()
    print("\n[INFO] --- Silver Chicago Sample Data ---")
    df.show(5, truncate=False)

    # Save to Silver (Parquet)
    print(f"[INFO] Saving cleaned data to: {SILVER_CHICAGO}")
    df.write.mode("overwrite").partitionBy("year", "month").parquet(SILVER_CHICAGO)
    
    # Save to MySQL (select only flat columns — nested structs like 'location' are not JDBC-compatible)
    print(f"[INFO] Saving cleaned data to MySQL table: chicago_data")
    mysql_columns = [c for c, t in df.dtypes if t in ("string", "int", "bigint", "long", "double", "float", "boolean", "timestamp", "date", "integer")]
    df.select(mysql_columns).write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/data_pipeline") \
        .option("dbtable", "chicago_data") \
        .option("user", "root") \
        .option("password", "182005kavi") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()
    
    print("[INFO] Silver Chicago Processing Complete.")

if __name__ == "__main__":
    process()