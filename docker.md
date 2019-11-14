# Docker usage

## Define environment variables

Set you values at the _environment_ section at the _docker-compose.yaml_ file.

At _services.webserver.environment_ you should define CKAN_API_KEY and CKAN_BASE_URL.  
This points to the CKAN instance you need to harvest to.  

## Run Airflow + Harvester

To start Airflow and load automatically all the harvester jobs just do:

```
# If you need to connect with a local CKAN instance with "nginx" as host you will need this line
export HOST_IP=`ip -4 addr show scope global dev docker0 | grep inet | awk '{print \$2}' | cut -d / -f 1`

docker-compose up -d
docker-compose logs -f
```
This will compile and start a scheduler to harvest periodically all you harvest sources.  
You will be able to check airflow status at `http://localhosts:8081`.  

## Internals

### Build the main image

```
docker build -t viderum/ckan-harvest-ng:latest .
```

### Run analyze sources

Run commands inside docker

```
docker run -it --rm havesrter_ng:latest python tools/analyze_harvest_sources.py
```

