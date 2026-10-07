#!/bin/bash

export POSTGRES_PASSWORD=admin
DATA_DIR="${DATA_DIR:-./Data}"
# Step 1: Start the containers
docker compose up --scale worker=3 --scale spark-worker=3 --remove-orphans -d

# Step 2: Wait for the namenode container to be ready
echo "Waiting for namenode to start..."
until docker exec namenode ls / >/dev/null 2>&1; do
  sleep 1
done

echo "Waiting for HDFS to be ready..."
until docker exec namenode hdfs dfsadmin -safemode get | grep -q "OFF"; do
  sleep 2
done
docker exec namenode bash -c "mkdir tmpdata"

# Step 3: Copy the local data into the namenode container

if docker exec namenode [ ! -d /tmpdata ] || [ -z "$(docker exec namenode ls -A /tmpdata)" ]; then
  echo "Copying data to namenode container..."
  docker cp "$DATA_DIR/." namenode:/tmpdata
else
  echo "Data already exists in the container."
fi

echo "Copying data to hdfs..."
docker exec namenode hdfs dfs -mkdir -p nader/data
docker exec namenode hdfs dfs -put /tmpdata/. nader/data

echo "adding nano to spark"
docker exec spark-master bin/bash -c "apk update && apk add nano"

echo "adding postgres package to spark"
docker exec spark-master bin/bash -c "mkdir -p /opt/spark/jars"
docker exec spark-master sh -c "curl -o /opt/spark/jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"

echo "Launching ETL process"
docker cp ./sparkcode.py spark-master:/
docker exec spark-master spark-submit --jars /opt/spark/jars/postgresql-42.7.3.jar sparkcode.py
echo "Done."
