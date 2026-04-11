from pyspark.sql.functions import col, to_timestamp, year, month, when, round, explode, lit

def validate_chicago(df):
    # remove records missing the essential crash id
    df = df.filter(col("crash_record_id").isNotNull())
    
    # keep total injuries within valid positive ranges, ignoring nulls
    df = df.filter((col("injuries_total").isNull()) | (col("injuries_total") >= 0))
    
    return df

def transform_chicago(df):
    # explode nested array if raw data hasn't been flattened yet
    if "data" in df.columns:
        df = df.select(explode("data").alias("record")).select("record.*")
        
    # remove records with blank crash IDs
    df = df.filter(col("crash_record_id").isNotNull())
    
    # convert date string to actual timestamp format
    df = df.withColumn("crash_date", to_timestamp("crash_date"))
    
    # extract year and month into separate columns
    df = df.withColumn("year", year("crash_date")) \
           .withColumn("month", month("crash_date"))
    
    # create severity flag based on injury counts
    df = df.withColumn(
        "severity",
        when(col("injuries_total") >= 5, "HIGH")
        .when(col("injuries_total") >= 1, "MEDIUM")
        .otherwise("LOW")
    )
    
    # clear exact duplicate crash events
    df = df.dropDuplicates(["crash_record_id"])
    
    return df

def validate_youtube(df):
    # clear records that failed to parse a video id
    df = df.filter(col("video_id").isNotNull())
    
    # ensure views metric is not a negative number
    if "views" in df.columns:
        df = df.filter(col("views") >= 0)
    
    return df

def transform_youtube(df):
    # explode data array if passing raw bronze json directly
    if "data" in df.columns:
        df = df.select(explode("data").alias("item"))
    elif "items" in df.columns:
        df = df.select(explode("items").alias("item"))
        
    # extract fields into top-level columns if nested in 'item'
    if "item" in df.columns:
        df = df.select(
            col("item.id.videoId").alias("video_id"),
            col("item.snippet.title").alias("title"),
            col("item.statistics.viewCount").alias("views"),
            col("item.statistics.likeCount").alias("likes"),
            col("item.statistics.commentCount").alias("comments")
        )
        
    # safely add dummy metric columns if extraction missed them
    for c in ["views", "likes", "comments"]:
        if c not in df.columns:
            df = df.withColumn(c, lit(0))
            
    # convert text strings to long numeric types
    df = df.withColumn("views", col("views").cast("long")) \
           .withColumn("likes", col("likes").cast("long")) \
           .withColumn("comments", col("comments").cast("long"))

    # clear blank video ids
    df = df.filter(col("video_id").isNotNull())
    
    # remove duplicate videos
    df = df.dropDuplicates(["video_id"])

    # calculate engagement relative to total views
    df = df.withColumn(
        "engagement_score",
        round(when(col("views") > 0, (col("likes") + col("comments")) / col("views")).otherwise(0.0), 4)
    )
    
    # flag viral videos over 1 million views
    df = df.withColumn(
        "is_viral",
        when(col("views") > 1000000, True).otherwise(False)
    )
    
    return df