FROM openjdk:17-slim

ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl bash python3 python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN mkdir -p /opt/spark/.ivy2 /opt/spark/tmp && \
    chown -R root:root /opt/spark && chmod -R 775 /opt/spark

RUN useradd -u 1001 -ms /bin/bash spark
USER spark

WORKDIR /opt/spark
