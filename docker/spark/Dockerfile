FROM eclipse-temurin:17-jdk-jammy

USER root

ARG SPARK_VERSION=3.5.3

# Install necessary packages
RUN apt-get update && \
    apt-get install -y curl bash procps python3 python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Spark
COPY spark-3.5.3-bin-hadoop3.tgz /tmp/spark.tgz
RUN tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-3.5.3-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin

# Install Iceberg-Spark Runtime
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    -o /opt/spark/jars/iceberg-spark-runtime.jar

# Install Iceberg AWS bundle (for S3FileIO)
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

# Kafka runtime dependencies
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -o /opt/spark/jars/commons-pool2.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar \
    -o /opt/spark/jars/lz4-java.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar \
    -o /opt/spark/jars/snappy-java.jar && \
    curl -fsSL https://repo1.maven.org/maven2/io/dropwizard/metrics/metrics-core/4.2.19/metrics-core-4.2.19.jar \
    -o /opt/spark/jars/metrics-core.jar

ENV SPARK_CLASSPATH=/opt/spark/jars/*

RUN groupadd -g 50000 spark || true && \
    useradd -m -u 50000 -g spark spark || true

# Give jar-file permission to spark
RUN chown -R spark:spark /opt/spark

USER spark
