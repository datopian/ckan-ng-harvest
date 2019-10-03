FROM python:3.7-alpine

RUN echo "************** Installing dependencies ... **************" && \
    apk add --no-cache \
        bash \
        build-base \
        libxml2 \
        libxml2-dev \
        libxslt-dev \
        linux-headers \
        python-dev

WORKDIR /app
ADD requirements.txt .

RUN apk add --no-cache --virtual .build-deps \
    git \
    build-base \
    && pip install pyproj==1.9.6 \
    && pip install -r requirements.txt \
    && apk del --no-cache .build-deps


# pip install -U pip && \
# pip install --no-cache-dir -r requirements.txt

ADD . .
ENTRYPOINT ["/bin/bash"]