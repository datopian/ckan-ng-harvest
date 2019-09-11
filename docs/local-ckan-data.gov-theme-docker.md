# Start local data.gov local docker env

For testing this tool we will need a CKAN instance (local or remote) with all the harvesting extensions working.  

One way to do that is use this docker image:

## Install local CKAN with harvest

Clone [this repo](https://github.com/ViderumGlobal/ckan-cloud-docker).  
Pay atention to the README file.  
Add nginx 127.0.0.1 to hosts file.  

For just install CKAN with harvesters use:

``` 
./create_secrets.py
docker-compose down -v
docker-compose -f docker-compose.yaml -f .docker-compose-db.yaml -f .docker-compose.datagov-theme.yaml pull
docker-compose -f docker-compose.yaml -f .docker-compose-db.yaml -f .docker-compose.datagov-theme.yaml build
docker-compose -f docker-compose.yaml -f .docker-compose-db.yaml -f .docker-compose.datagov-theme.yaml up -d
``` 
Create a admin user
``` 
docker-compose exec ckan ckan-paster --plugin=ckan \
    sysadmin add -c /etc/ckan/production.ini admin password=12345678 email=admin@localhost
``` 

You will see CKAN working at _nginx:8080_ or _localhost:5000_.  

You can see logs with:
```
docker-compose -f docker-compose.yaml -f .docker-compose-db.yaml -f .docker-compose.datagov-theme.yaml logs -f

# or just logs from CKAN
docker-compose -f docker-compose.yaml -f .docker-compose-db.yaml -f .docker-compose.datagov-theme.yaml logs -f ckan
```