# 베이스 이미지 다운로드
FROM python:3.10.18-slim

# 필수 시스템 도구 설치
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl wget git unzip build-essential lsb-release netcat \
    && apt-get clean

# 환경 변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.5.3
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3

# Spark 설치
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    ln -s ${SPARK_HOME}/bin/* /usr/local/bin/

# 작업 디렉토리 설정
WORKDIR /workspace

# requirements.txt 복사 및 Python 패키지 설치
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt