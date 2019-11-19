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
        python-dev \
        unzip \
        wget


WORKDIR /app
ADD requirements.txt .

RUN pip install --upgrade pip && \
    pip install pyproj==1.9.6 && \
    pip install -r requirements.txt && \
    pip install SQLAlchemy

ADD . .

# install rest api plugin
ARG AIRFLOW_HOME=/usr/local/airflow
RUN wget https://github.com/teamclairvoyant/airflow-rest-api-plugin/archive/v1.0.8.zip && \
    pwd && ls -la && unzip v1.0.8.zip && \
    mkdir -p ${AIRFLOW_HOME}/plugins && \
    cp -r ./airflow-rest-api-plugin-1.0.8/plugins/* ${AIRFLOW_HOME}/plugins

RUN echo ${AIRFLOW_HOME}/plugins && ls -la ${AIRFLOW_HOME}/plugins


