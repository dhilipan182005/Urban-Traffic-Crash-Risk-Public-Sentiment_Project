export MSYS_NO_PATHCONV=1

docker exec namenode hdfs dfs -mkdir -p /user/hadoop/bronze/chicago
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/bronze/youtube
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/silver/chicago
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/silver/youtube
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/gold/chicago
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/gold/youtube
docker exec namenode hdfs dfs -chmod -R 777 /user/hadoop

echo "[INFO] HDFS directories initialized successfully"

