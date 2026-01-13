FROM apache/airflow:2.10.2-python3.10

USER root

# Install necessary packages
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    bash curl procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Spark for spark-submit
COPY spark-3.5.3-bin-hadoop3.tgz /tmp/spark.tgz
RUN tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-3.5.3-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# Spark & Java environment variables
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="/home/airflow/.local/bin:/usr/local/bin:${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

ENV PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip"

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

ENV SPARK_DIST_CLASSPATH="/opt/spark/jars/*"
ENV SPARK_CLASSPATH="/opt/spark/jars/*"

# For avoiding conflict versions
RUN pip uninstall -y pyspark || true
RUN rm -f /home/airflow/.local/bin/spark-submit || true
RUN rm -f /home/airflow/.local/bin/pyspark || true
RUN rm -f /usr/local/bin/spark-submit || true
RUN rm -f /usr/local/bin/pyspark || true

# Install Iceberg Runtime Jar (match with Spark 3.5)
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
      -o /opt/spark/jars/iceberg-runtime.jar

# Install AWS SDK + Iceberg AWS bundle (for S3FileIO)
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar \
      -o /opt/spark/jars/iceberg-aws-bundle.jar

# Install Hadoop-AWS (S3AFileSystem) Jar
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o /opt/spark/jars/hadoop-aws.jar

# Install AWS SDK v1 bundle (S3AFileSystem dependency)
RUN curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -o /opt/spark/jars/aws-java-sdk-bundle.jar

# Install Kafka Structured Streaming Jar
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.5.3/spark-streaming-kafka-0-10_2.12-3.5.3.jar \
    -o /opt/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.3.jar
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    -o /opt/spark/jars/kafka-clients.jar
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar

ENV SPARK_CLASSPATH=/opt/spark/jars/*

RUN groupadd -g 50000 airflow || true && \
    useradd -m -u 50000 -g airflow airflow || true

# Give jar-file permission to airflow
RUN chown -R airflow:airflow /opt/spark /usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Fix Airflow version to be 2.10.2
ARG AIRFLOW_VERSION=2.10.2
ARG PYTHON_VERSION=3.10
ENV AIRFLOW_CONSTRAINT="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install constraint provider to prevent Airflow version to be 3.x
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" --constraint "${AIRFLOW_CONSTRAINT}" && \
    pip install --no-cache-dir apache-airflow-providers-apache-spark --constraint "${AIRFLOW_CONSTRAINT}"

RUN command -v airflow && airflow version
