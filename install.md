# Install New Harvester

This tool does not need a CKAN instance to work. It works as a set of independent Python scripts that communicates with different instances of CKAN via WEB APIs.  

## Requirements

Python 3.6
requirement.txt file.
Tested on:
 - Ubuntu 18.04 LTS
 - MacOSX

### Harvester

#### Sources
You need a CKAN instance with harvest sources defined.  
If your CKAN instance needs to import harvest sources from another CKAN instance you can use the _import script_.  

```
# for example import all CSW harvest sources from data.gov
python3 import_harvest_sources.py --import_from_url https://catalog.data.gov --source_type csw --method GET
# --method GET is for older CKAN instances
```

#### Analyze sources

You can count and analyze al harvest sources:

```
python3 analize_harvest_sources.py 
```
This creates a CSV file with all the harvest sources.

### Apache Airflow
To start process you need a queue with all the harvesters. This will be done with the _harvest_with_airflow.py_ [script](/automate-tasks/airflow/harvest_with_airflow.py) at automate-tasks/airflow folder.  
This file need to live at the Airflow [DAG](https://airflow.apache.org/concepts.html#dags) folder (or maybe just a link to this file).  

![af01](/imgs/airflow01.png)
![af02](/imgs/airflow02.png)

#### About Airflow
Airflow is a Python/Flask app.  
Is installed at requirement file but need to be configured (_~/airflow/airflow.cfg_).  
Airflow has a web interface useful for analyzing all harvest process.  

