FROM bitnami/spark:3.5.6

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3.11 python3.11-venv python3-pip && \
    ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python3.11
ENV PYSPARK_DRIVER_PYTHON=python3.11

USER 1001