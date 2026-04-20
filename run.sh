export MSYS_NO_PATHCONV=1

LOG_FILE="logs/pipeline.log"
mkdir -p logs

CLEAN_PATH=$(echo "$1" | sed 's|.*data_pipeline/||; s|.*workspace/||; s|^/||')

if [ -z "$CLEAN_PATH" ]; then
    echo "[ERROR] $(date +'%Y-%m-%d %H:%M:%S') - No script path provided. Usage: ./run.sh <path_to_script>"
    exit 1
fi

if [[ $CLEAN_PATH == ingestion* ]]; then
    if ! docker exec spark-local python3 -c "import socket; socket.gethostbyname('google.com')" > /dev/null 2>&1; then
        echo "[ERROR] $(date +'%Y-%m-%d %H:%M:%S') - No internet connectivity in container"
        exit 1
    fi
fi

echo "[INFO] $(date +'%Y-%m-%d %H:%M:%S') - Running: $CLEAN_PATH"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] Starting execution: $CLEAN_PATH" >> "$LOG_FILE"

if [[ $CLEAN_PATH == processing/* ]]; then
    CMD="spark-submit --master local[*]"
else
    CMD="python3"
fi

docker exec spark-local $CMD /workspace/$CLEAN_PATH 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -eq 0 ]; then
    echo "[SUCCESS] $(date +'%Y-%m-%d %H:%M:%S') - Completed: $CLEAN_PATH"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Success: $CLEAN_PATH" >> "$LOG_FILE"

    if [[ $CLEAN_PATH == ingestion/* ]]; then
        if [[ $CLEAN_PATH == *chicago* ]]; then
            echo "[INFO] $(date +'%Y-%m-%d %H:%M:%S') - Syncing Chicago data to HDFS..."
            docker exec namenode bash -c "hdfs dfs -put /workspace/data/bronze/chicago/*.json /user/hadoop/bronze/chicago/ 2>/dev/null || true" >> "$LOG_FILE" 2>&1
        elif [[ $CLEAN_PATH == *youtube* ]]; then
            echo "[INFO] $(date +'%Y-%m-%d %H:%M:%S') - Syncing YouTube data to HDFS..."
            docker exec namenode bash -c "hdfs dfs -put /workspace/data/bronze/youtube/*.json /user/hadoop/bronze/youtube/ 2>/dev/null || true" >> "$LOG_FILE" 2>&1
        fi
    fi
else
    echo "[ERROR] $(date +'%Y-%m-%d %H:%M:%S') - Execution failed: $CLEAN_PATH (exit code $EXIT_CODE)"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Failed ($EXIT_CODE): $CLEAN_PATH" >> "$LOG_FILE"
    exit $EXIT_CODE
fi