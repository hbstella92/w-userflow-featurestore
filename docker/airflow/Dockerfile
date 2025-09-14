FROM apache/airflow:2.9.2-python3.11

USER airflow

COPY requirements.txt /
# 공식 airflow 이미지의 entrypoint가 airflow 실행파일을 PATH에 넣어준다는 보장이 없음
# 패키지가 root 레벨 /usr/local/bin에 들어갈 수 있도록
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt"

USER root

# entrypoint가 바라보는 곳에 airflow 실행파일 연결해줌
RUN ln -sf /home/airflow/.local/bin/airflow /usr/local/bin/airflow && \
    ln -sf /home/airflow/.local/bin/pip /usr/local/bin/pip

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    procps \
    netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

COPY spark-3.5.6-bin-hadoop3.tar /tmp/

RUN tar -xf /tmp/spark-3.5.6-bin-hadoop3.tar -C /opt/ && \
    ln -s /opt/spark-3.5.6-bin-hadoop3 /opt/spark && \
    rm /tmp/spark-3.5.6-bin-hadoop3.tar

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="/opt/spark/bin:${PATH}"

USER airflow
