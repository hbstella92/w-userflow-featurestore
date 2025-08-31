#!/usr/bin/env bash
set -euo pipefail

# container setting
SPARK_MASTER_CONTAINER="${SPARK_MASTER_CONTAINER:-spark-master}"
SPARK_APP_FILE_IN_CONTAINER="${SPARK_APP_FILE_IN_CONTAINER:-/opt/workspace/src/spark/streaming_sessions.py}"

# path
SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_COMPOSE_FILE="${DOCKER_COMPOSE_FILE:-$ROOT_DIR/docker/docker-compose.yml}"
ENV_FILE="$ROOT_DIR/.env"

# load .env file
if [[ ! -f $ENV_FILE ]]; then
    echo "[ERROR] .env file is not found at $ENV_FILE"
    exit 1
fi

if [[ ! -f $DOCKER_COMPOSE_FILE ]]; then
    echo "[ERROR] docker-compose.yml not found at $DOCKER_COMPOSE_FILE"
    exit 1
fi

if ! docker compose -f "$DOCKER_COMPOSE_FILE" ps "$SPARK_MASTER_CONTAINER" | grep -q Up; then
    echo "[ERROR] container not running: $SPARK_MASTER_CONTAINER"
    echo "      hint) docker compose -f \"$DOCKER_COMPOSE_FILE\" up -d $SPARK_MASTER_CONTAINER spark-worker"
    exit 1
fi

echo "[INFO] docker compose exec -> $SPARK_MASTER_CONTAINER"
echo "[INFO] app: $SPARK_APP_FILE_IN_CONTAINER"

docker compose -f "$DOCKER_COMPOSE_FILE" exec -T "$SPARK_MASTER_CONTAINER" bash -lc "
    set -euo pipefail

    [[ -f /opt/workspace/.env ]] || { echo '[ERROR] /opt/workspace/.env not found'; exit 1; }
    [[ -f \"$SPARK_APP_FILE_IN_CONTAINER\" ]] || { echo \"[ERROR] app not found: $SPARK_APP_FILE_IN_CONTAINER\"; exit 1; }

    set -a
    source /opt/workspace/.env
    set +a

    echo '[INFO] inside container:'
    echo \"  MASTER                     = \$SPARK_MASTER\"
    echo \"  APP                        = \$SPARK_APP_FILE_IN_CONTAINER\"
    echo \"  KAFKA_BOOTSTRAP_SERVERS    = \$KAFKA_BOOTSTRAP_SERVERS\"
    echo \"  KAFKA_TOPIC                = \$KAFKA_TOPIC\"
    echo \"  POSTGRES_JDBC_URL          = \$POSTGRES_JDBC_URL\"
    echo \"  AVSC_PATH                  = \$AVSC_PATH\"
    echo \"  SPARK_CHECKPOINT_DIR       = \$SPARK_CHECKPOINT_DIR\"
    echo \"  SPARK_PACKAGES             = \$SPARK_PACKAGES\"

    \"\$SPARK_SUBMIT\" \
        --conf spark.pyspark.python=python3.11 \
        --conf spark.pyspark.driver=python3.11 \
        --conf spark.jars.ivy=/opt/bitnami/spark/.ivy2 \
        --conf spark.hadoop.fs.defaultFS=file:/// \
        --conf spark.sql.catalogImplementation=in-memory \
        --conf spark.sql.shuffle.partitions=1 \
        --conf spark.driver.extraJavaOptions=\"-Duser.name=spark\" \
        --conf spark.executor.extraJavaOptions=\"-Duser.name=spark\" \
        --master \"\$SPARK_MASTER\" \
        --packages \"\$SPARK_PACKAGES\" \
        \"\$SPARK_APP_FILE_IN_CONTAINER\"
"
