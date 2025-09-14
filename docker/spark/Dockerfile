FROM bitnami/spark:3.5.6

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3.11 python3.11-venv python3-pip && \
    ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/bitnami/spark/.ivy2 && \
    chown -R 1001:root /opt/bitnami/spark/.ivy2 && \
    chmod -R 775 /opt/bitnami/spark/.ivy2 && \
    mkdir -p /opt/bitnami/spark/tmp && \
    chown -R 1001:root /opt/bitnami/spark/tmp && \
    chmod -R 775 /opt/bitnami/spark/tmp

ENV PYSPARK_PYTHON=python3.11
ENV PYSPARK_DRIVER_PYTHON=python3.11
ENV SPARK_LOCAL_DIRS=/opt/bitnami/spark/tmp
ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf
ENV SPARK_LOG_DIR=/opt/bitnami/spark/logs
ENV SPARK_PID_DIR=/opt/bitnami/spark/tmp

USER 1001
