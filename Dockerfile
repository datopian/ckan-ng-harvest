FROM puckel/docker-airflow:1.10.4
# based on python:3.7-slim-stretch -> debian:stretch-slim
# https://github.com/docker-library/python/blob/c3233a936f58bee7c6899d3e381f23ed12cfc7a8/3.7/stretch/slim/Dockerfile
USER root

RUN echo "************** Installing dependencies ... **************" && \
    apt-get update && \
        apt-get install -y \
        libxml2 \
        libxml2-dev \
        libxslt-dev \
        python-dev

WORKDIR /app
ADD requirements.txt .

RUN pip install --upgrade pip && \
    pip install pyproj==1.9.6 && \
    pip install -r requirements.txt

ADD . .
# RUN mv /app/automate-tasks/airflow/dags /usr/local/airflow/dags && ls -la /usr/local/airflow/dags
# RUN cp -r ./automate-tasks/airflow/dags /usr/local/airflow/dags && ls -la /usr/local/airflow/dags
# ADD ./automate-tasks/airflow/dags /usr/local/airflow/dags
# RUN ls -la /usr/local/airflow/dags