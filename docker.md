# Docker usage

## Define environment variables

Set you values at the _environment_ section at the _docker-compose.yaml_ file.

At _services.webserver.environment_ you should define CKAN_API_KEY and CKAN_BASE_URL.  
This points to the CKAN instance you need to harvest to.  

At [CKAN Cloud Docker](https://github.com/avdata99/ckan-cloud-docker/blob/7aac946ee7a732379a9c6b7a933ebf5f90b358c7/.docker-compose-harvester_ng.yaml#L41) we use `CKAN_API_KEY=READ_FROM_DB` to read credentials directly from database. Here we need to difine this.  

You can also create a docker-compose-override.yml file in order to override env vars and define you API_KEY outside this repo.

## Run Airflow + Harvester

To start Airflow and load automatically all the harvester jobs just do:

```
# If you need to connect with a local CKAN instance with "nginx" as host you will need this line
export HOST_IP=`ip -4 addr show scope global dev docker0 | grep inet | awk '{print \$2}' | cut -d / -f 1`

docker-compose up -d
# or docker-compose -f docker-compose.yml -f docker-compose-override.yml up -d

docker-compose logs -f
```
This will compile and start a scheduler to harvest periodically all you harvest sources.  
You will be able to check airflow status at `http://localhosts:8081`.  

## Internals

### Build the main image

```
docker build -t ckan-harvest-ng:latest .
```

### Run analyze sources

Run commands inside docker

```
docker run -it --rm ckan-harvest-ng:latest python tools/analyze_harvest_sources.py
```

