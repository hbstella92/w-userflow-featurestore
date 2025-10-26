FROM openjdk:17-slim

WORKDIR /opt/dbt

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv curl bash && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core \
    dbt-spark \
    pyspark