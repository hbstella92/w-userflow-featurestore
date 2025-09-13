FROM apache/airflow:2.9.2

USER airflow

COPY requirements.txt /

# 공식 airflow 이미지의 entrypoint가 airflow 실행파일을 PATH에 넣어준다는 보장이 없음
# 패키지가 root 레벨 /usr/local/bin에 들어갈 수 있도록
RUN pip install --no-cache-dir -r /requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt"

USER root

# entrypoint가 바라보는 곳에 airflow 실행파일 연결해줌
RUN ln -sf /home/airflow/.local/bin/airflow /usr/local/bin/airflow && \
    ln -sf /home/airflow/.local/bin/pip /usr/local/bin/pip

USER airflow