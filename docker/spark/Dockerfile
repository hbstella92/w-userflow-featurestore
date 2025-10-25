FROM openjdk:17-slim

ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl bash procps python3 python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tar /tmp/
RUN tar -xf /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tar -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tar

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN groupadd -g 1001 spark && \
    useradd -u 1001 -g spark -ms /bin/bash spark && \
    mkdir -p /opt/spark/.ivy2/cache /opt/spark/tmp && \
    chown -R 1001:1001 /opt/spark && \
    chmod -R 777 /opt/spark

USER 1001

WORKDIR /opt/spark
