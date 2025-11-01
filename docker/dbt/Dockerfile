FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl bash git build-essential g++ libsasl2-dev libkrb5-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && \
    pip install \
    dbt-core==1.7.14 \
    dbt-spark[PyHive]==1.7.1 \
    PyHive==0.7.0 \
    thrift==0.16.0 \
    thrift-sasl==0.4.3 \
    sasl==0.3.1 \
    boto3==1.34.0 \
    pandas==2.2.2

ENV DBT_PROFILES_DIR=/opt/dbt
ENV AWS_REGION=ap-northeast-2
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/dbt

# 기본 명령 (대기)
CMD ["sleep", "infinity"]
