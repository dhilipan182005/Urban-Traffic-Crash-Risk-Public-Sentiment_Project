set -e
export MSYS_NO_PATHCONV=1

BRONZE_CHICAGO_LOCAL="/workspace/data/bronze/chicago"
BRONZE_YOUTUBE_LOCAL="/workspace/data/bronze/youtube"
HDFS_BRONZE_CHICAGO="/user/hadoop/bronze/chicago"
HDFS_BRONZE_YOUTUBE="/user/hadoop/bronze/youtube"

echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') - Uploading bronze data to HDFS"

docker exec namenode hdfs dfs -mkdir -p "$HDFS_BRONZE_CHICAGO"
docker exec namenode hdfs dfs -mkdir -p "$HDFS_BRONZE_YOUTUBE"

if ls "$BRONZE_CHICAGO_LOCAL"/*.json 1>/dev/null 2>&1; then
    docker exec namenode hdfs dfs -put -f "$BRONZE_CHICAGO_LOCAL"/*.json "$HDFS_BRONZE_CHICAGO/"
    echo "[SUCCESS] $(date '+%Y-%m-%d %H:%M:%S') - Chicago bronze uploaded to HDFS"
else
    echo "[WARN] $(date '+%Y-%m-%d %H:%M:%S') - No Chicago bronze files found at $BRONZE_CHICAGO_LOCAL"
fi

if ls "$BRONZE_YOUTUBE_LOCAL"/*.json 1>/dev/null 2>&1; then
    docker exec namenode hdfs dfs -put -f "$BRONZE_YOUTUBE_LOCAL"/*.json "$HDFS_BRONZE_YOUTUBE/"
    echo "[SUCCESS] $(date '+%Y-%m-%d %H:%M:%S') - YouTube bronze uploaded to HDFS"
else
    echo "[WARN] $(date '+%Y-%m-%d %H:%M:%S') - No YouTube bronze files found at $BRONZE_YOUTUBE_LOCAL"
fi

echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') - Bronze HDFS upload complete"
