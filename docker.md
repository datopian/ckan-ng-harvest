# Docker usage

## Build image
Build the image
```
docker build -t havesrter_ng:v0.103 .
```

## Run analyze sources
Run commands inside docker
```
docker run -it --rm havesrter_ng:v0.103 python analyze_harvest_sources.py
```