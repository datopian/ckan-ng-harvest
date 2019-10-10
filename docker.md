# Docker usage

## Define environment variables

Copy `env.airflow.sample` to `env.airflow` and `env.harvester.sample` to `env.harvester`

## Run Airflow + Harvester

To start Airflow and load automatically all the harvester jobs just do:

```
docker-compose up -d
```
This will compile and start a scheduler to harvest periodically all you harvest sources.  
You will be able to check airflow status at `http://localhosts:8080`.  

## Internals

### Build the main image

```
docker build -t havesrter_ng:latest .
```

### Run analyze sources

Run commands inside docker

```
docker run -it --rm havesrter_ng:latest python analyze_harvest_sources.py
```

