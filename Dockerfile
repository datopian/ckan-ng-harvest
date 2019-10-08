FROM python:3.7

RUN echo "************** Installing dependencies ... **************" && \
    apt-get update && apt-get install -y \
        libxml2 \
        libxml2-dev \
        libxslt-dev \
        python-dev

WORKDIR /app
ADD requirements.txt .

RUN pip install pyproj==1.9.6 \
    && pip install -r requirements.txt

ADD . .
CMD [ "/bin/bash", "-c" ]