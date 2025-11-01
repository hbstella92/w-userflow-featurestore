FROM openjdk:17-slim

ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3
ARG ICEBERG_VERSION=1.5.0

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl bash procps python3 python3-pip python3-venv ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o /tmp/spark.tgz && \
    mkdir -p /opt/spark && \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN mkdir -p /opt/spark/jars && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar -o /opt/spark/jars/hadoop-common-3.3.6.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.6/hadoop-auth-3.3.6.jar -o /opt/spark/jars/hadoop-auth-3.3.6.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.6/hadoop-hdfs-client-3.3.6.jar -o /opt/spark/jars/hadoop-hdfs-client-3.3.6.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar -o /opt/spark/jars/hadoop-client-api-3.3.6.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar -o /opt/spark/jars/hadoop-client-runtime-3.3.6.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar -o /opt/spark/jars/hadoop-aws-3.3.6.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar

RUN useradd -m -u 1001 -d /opt/spark spark && \
    chown -R 1001:1001 /opt/spark

USER 1001
WORKDIR /opt/spark

ENV JAVA_TOOL_OPTIONS="-Duser.name=spark"
ENV HADOOP_USER_NAME=spark
ENV AWS_REGION=ap-northeast-2
ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

# Thrift JDBC Server port
EXPOSE 10000

# Spark Thrift Server 실행
CMD /opt/spark/bin/spark-submit \
    --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
    --master local[*] \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.endpoint=s3.ap-northeast-2.amazonaws.com \
    --conf spark.hadoop.hive.server2.authentication=NOSASL \
    --conf spark.hadoop.hive.server2.thrift.bind.host=0.0.0.0 \
    --conf spark.hadoop.hive.server2.thrift.port=10000 \
    --conf spark.authenticate=false \
    --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
    --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=hive \
    --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
    --conf spark.sql.warehouse.dir=s3a://w-userflow-featurestore/iceberg/