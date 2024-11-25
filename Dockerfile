FROM apache/airflow:2.9.2

USER root

RUN apt-get update

USER airflow

RUN pip install -U pip

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt