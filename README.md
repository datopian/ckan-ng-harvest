# Harvesting data.json files

The harvest process includes:
 - Read a [data.json](data.json.md) resource file from an external resource I want to harvest.
 - Validate and save these results.
 - Search the previous datasets harvested for that particular source
 - Compare both resources and list the differences.
 - Update the CKAN instance with these updates 

Current process: [using ckan extensions](harvest-in-ckanext.md).  

Process using [dataflows](https://github.com/datahq/dataflows) and [datapackages](https://github.com/frictionlessdata/datapackage-py).  

## Settings

The files _settings.py_ (empty) and _local_settings.py_ (ignored from repo) are to define your local configuration (url, api key, etc).  

## Harvest sources

The _data.json_ harvest sources are CKAN packages with an URL to this file.  
We can import all the harvest sources from a productive CKAN instance with the command

### Some tools

#### Read harvest sources
You can search via CKAN API the list of packages/harvest sources. 

```
# Get CSW harvest sources at _data.gov_
python3 read_harvest_sources.py --base_url https://catalog.data.gov --source_type csw --method GET  
# CKAN 2.3 fail with POST, current versions works fine with POST

# Get your local data.json harvest sources
python3 read_harvest_sources.py --base_url http://ckan:5000 --source_type datajson --method POST
```

#### Import harvest sources

You can import harvest sources from another CKAN instance.

```
# import all CSW harvest sources from data.gov
python3 import_harvest_sources.py --import_from_url https://catalog.data.gov --source_type csw --method GET
```

### Harvest one source

You need:
 - a _name_ for the harvest source
 - url of the data.json
 - harvest_source_id: the ID of the harvest soure
 - ckan_owner_org_id: name of the organization for all the harvested datasets
 - catalog_url you CKAN instance 
 - ckan_api_key you CKAN API key
  
Harvest source: RRB JSON e058dafa-75db-4480-a90a-c1026e3005e2 
rrb-json datajson https://secure.rrb.gov/data.json

Example using data from _read_harvest_sources_ and a local CKAN instance

```
python3 harvest.py \
  --name rrb \
  --url https://secure.rrb.gov/data.json \
  --harvest_source_id e058dafa-75db-4480-a90a-c1026e3005e2 \
  --ckan_owner_org_id rrb-gov \
  --catalog_url http://ckan:5000 \
  --ckan_api_key 5ce77b38-3556-4a2c-9e44-5a18f53f9862
```

Results

```
**************
Execute: python3 flow.py --name rrb --url https://secure.rrb.gov/data.json --limit_dataset 0
**************
Geting data.json from https://secure.rrb.gov/data.json
/home/hudson/envs/data_json_etl/lib/python3.6/site-packages/urllib3/connectionpool.py:851: InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
  InsecureRequestWarning)
Downloaded OK
JSON OK
Validate OK: 16 datasets
VALID JSON, 16 datasets found
Cleaning duplicates
0 duplicates deleted. 16 OK
Continue to next step with: python3 flow2.py --name rrb 
**************
COMD OK: python3 flow.py --name rrb --url https://secure.rrb.gov/data.json --limit_dataset 0
**************
**************
Execute: python3 flow2.py --name rrb --harvest_source_id e058dafa-75db-4480-a90a-c1026e3005e2 --catalog_url http://ckan:5000
**************
Extracting from harvest source id: e058dafa-75db-4480-a90a-c1026e3005e2
Searching http://ckan:5000/api/3/action/package_search PAGE:1 start:0, rows:1000 with params: {'start': 0, 'rows': 1000, 'fq': '+harvest_ng_source_id:"e058dafa-75db-4480-a90a-c1026e3005e2"'}
0 results
0 total resources in harvest source id: e058dafa-75db-4480-a90a-c1026e3005e2
Rows from resource ckan_results
Total processed: 0.
                0 fail extras.
                0 fail identifier key.
                0 deleted.
                0 datasets found (0 needs update, 0 are the same),
                16 new datasets.
Continue to next step with: python3 flow3.py --name rrb --harvest_source_id e058dafa-75db-4480-a90a-c1026e3005e2
**************
COMD OK: python3 flow2.py --name rrb --harvest_source_id e058dafa-75db-4480-a90a-c1026e3005e2 --catalog_url http://ckan:5000
**************
**************
Execute: python3 flow3.py --name rrb --ckan_owner_org_id rrb-gov --catalog_url http://ckan:5000 --ckan_api_key 5ce77b38-3556-4a2c-9e44-5a18f53f9862
**************
Transforming data.json dataset RRB-460
Dataset transformed RRB-460 OK
POST http://ckan:5000/api/3/action/package_create headers:{'User-Agent': 'ckan-portal-filter 0.01-alpha', 'X-CKAN-API-Key': '5ce77b38-3556-4a2c-9e44-5a18f53f9862', 'Content-Type': 'application/json'} data:{'name': 'total-railroad-employment-by-state-and-county-2014', 'title': 'Total Railroad Employment by State and County, 2014', 'owner_org': 'rrb-gov', 'private': False, 'maintainer': 'Anna Salazar-Bartolon', 'maintainer_email': 'Anna.Salazar-Bartolon@rrb.gov', 'notes': 'A breakdown of Railroad employees by State and County', 'state': 'active', 'resources': [{'url': 'http://www.rrb.gov/sites/default/files/2017-01/StateCounty2014.xls', 'description': 'A breakdown of Railroad employees by State and County', 'format': 'application/xls', 'name': 'Total Railroad Employment by State and County, 2014', 'mimetype': 'application/vnd.ms-excel', 'describedBy': 'https://www.rrb.gov/FinancialReporting/FinancialActuarialStatistical/Annual'}], 'tags': [{'name': 'county'}, {'name': 'demographic'}, {'name': 'railroad'}, {'name': 'railroad-employees'}], 'extras': [{'key': 'resource-type', 'value': 'Dataset'}, {'key': 'modified', 'value': '2016-03-01'}, {'key': 'identifier', 'value': 'RRB-460'}, {'key': 'accessLevel', 'value': 'public'}, {'key': 'bureauCode', 'value': ['446:00']}, {'key': 'programCode', 'value': ['000:000']}, {'key': 'spatial', 'value': 'US'}, {'key': 'accrualPeriodicity', 'value': 'R/P1Y'}, {'key': 'landingPage', 'value': 'http://www.rrb.gov/pdf/act/StateCounty2014.xls'}, {'key': 'issued', 'value': '2016-03-01'}, {'key': 'harvest_source_title', 'value': 'rrb'}, {'key': 'source_schema_version', 'value': '1.1'}, {'key': 'source_hash', 'value': '10f8d1f8f7d01a2defc4eea7d31c304e49a5b905'}, {'key': 'catalog_@context', 'value': 'https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld'}, {'key': 'catalog_conformsTo', 'value': 'https://project-open-data.cio.gov/v1.1/schema'}, {'key': 'catalog_describedBy', 'value': 'https://project-open-data.cio.gov/v1.1/schema/catalog.json'}, {'key': 'source_datajson_identifier', 'value': True}, {'key': 'publisher', 'value': 'Railroad Retirement Board'}]}
Transforming data.json dataset RRB-501
Dataset transformed RRB-501 OK
POST http://ckan:5000/api/3/action/package_create headers:{'User-Agent': 'ckan-portal-filter 0.01-alpha', 'X-CKAN-API-Key': '5ce77b38-3556-4a2c-9e44-5a18f53f9862', 'Content-Type': 'application/json'} data:{'name': 'application-outcomes-for-disability-benefits-2015', 'title': 'Application Outcomes for Disability Benefits, 2015', 'owner_org': 'rrb-gov', 'private': False, 'maintainer': 'Anna Salazar-Bartolon', 'maintainer_email': 'Anna.Salazar-Bartolon@rrb.gov', 'notes': 'Data on the application outcomes for Railroad Retirement employee and survivor disability awards', 'state': 'active', 'resources': [{'url': 'https://www.rrb.gov/sites/default/files/2017-06/Application%20Outcomes%20for%20Disability%20Benefits%20Report%20Final%202017-02.pdf', 'description': 'Data on the application outcomes for Railroad Retirement employee and survivor disability awards', 'format': 'pdf', 'name': 'Application Outcomes for Disability Benefits for Employees and Survivors, 2015', 'mimetype': 'application/pdf', 'describedBy': 'https://www.rrb.gov/sites/default/files/2016-10/TotalEmployment2014.pdf'}], 'tags': [{'name': 'railroad-employee-disability'}, {'name': 'railroad-employees'}, {'name': 'railroad-occupational-disability'}, {'name': 'railroad-survivor-disability'}, {'name': 'railroad-total-disability'}], 'extras': [{'key': 'resource-type', 'value': 'Dataset'}, {'key': 'modified', 'value': '2017-02-28'}, {'key': 'identifier', 'value': 'RRB-501'}, {'key': 'accessLevel', 'value': 'public'}, {'key': 'bureauCode', 'value': ['446:00']}, {'key': 'programCode', 'value': ['000:000']}, {'key': 'spatial', 'value': 'Global'}, {'key': 'accrualPeriodicity', 'value': 'R/P1Y'}, {'key': 'landingPage', 'value': 'https://www.rrb.gov/FinancialReporting/FinancialActuarialStatistical/Annual'}, {'key': 'issued', 'value': '2017-02-28'}, {'key': 'harvest_source_title', 'value': 'rrb'}, {'key': 'source_schema_version', 'value': '1.1'}, {'key': 'source_hash', 'value': '9e63b0ddfef1ba8a9dc87a048f0ed4edda8bedae'}, {'key': 'catalog_@context', 'value': 'https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld'}, {'key': 'catalog_conformsTo', 'value': 'https://project-open-data.cio.gov/v1.1/schema'}, {'key': 'catalog_describedBy', 'value': 'https://project-open-data.cio.gov/v1.1/schema/catalog.json'}, {'key': 'source_datajson_identifier', 'value': True}, {'key': 'publisher', 'value': 'Railroad Retirement Board'}]}

...
....

**************
COMD OK: python3 flow3.py --name rrb --ckan_owner_org_id rrb-gov --catalog_url http://ckan:5000 --ckan_api_key 5ce77b38-3556-4a2c-9e44-5a18f53f9862
**************
```

You can see the harvested datasets at you CKAN instance

![h0](imgs/harvest00.png)
![h0](imgs/harvest01.png)


## Internal scripts

### Usage flow.py script

```
usage: flow.py [-h] [--url URL] [--name NAME]
                  [--harvest_source_id HARVEST_SOURCE_ID]

optional arguments:
  -h, --help            show this help message and exit
  --url URL             URL of the data.json
  --name NAME           Name of the resource (for generate the containing
                        folder)
  --force_download      Force download or just use local data.json prevously
                        downloaded
  --harvest_source_id HARVEST_SOURCE_ID
                        Source ID for filter CKAN API
```

Sample results

```
$ python3 flow.py --url http://www.usda.gov/data.json --name agriculture --harvest_source_id 50ca39af-9ddb-466d-8cf3-84d67a204346
Geting data.json from http://www.usda.gov/data.json
OK http://www.usda.gov/data.json
VALID JSON
1580 datasets finded
--------------------------------
Package processor
Package: <dataflows.base.package_wrapper.PackageWrapper object at 0x7f472dafccf8>
 - Resource: {'name': 'datajson', 'path': 'res_1.csv', 'profile': 'tabular-data-resource', 'fields': {'name': 'identifier', 'type': 'string', 'format': 'default'}}
--------------------------------
Cleaning duplicates
Rows from resource datajson
0 duplicates deleted. 1580 OK
---------------
Second part
---------------
Extracting from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
PAGE 1 from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
--------------------------------
Package processor
Package: <dataflows.base.package_wrapper.PackageWrapper object at 0x7f472d302550>
 - Resource: {'name': 'ckanapi', 'path': 'res_1.csv', 'profile': 'tabular-data-resource', 'fields': {'name': 'license_title', 'type': 'string', 'format': 'default'}}
--------------------------------
No identifier! dataset: e2430c9b-2fe2-40c8-b08e-238eef975992
No identifier! dataset: 0c4888bf-4e8d-4e86-b774-0afda835bb86
PAGE 2 from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
3369 total resources in harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
Total processed: 1570. 
                    0 fail extras. 
                    2 fail identifier key.
                    0 deleted.
                    1568 datasets finded.

```

### Other example
```
$ python3 flow.py --url http://www.energy.gov/data.json --name energy --harvest_source_id 8d4de31c-979c-4b50-be6b-ea3c72453ff6
Geting data.json from http://www.energy.gov/data.json
OK http://www.energy.gov/data.json
VALID JSON
5009 datasets finded
--------------------------------
Package processor
Package: <dataflows.base.package_wrapper.PackageWrapper object at 0x7fb33c4aea58>
 - Resource: {'name': 'datajson', 'path': 'res_1.csv', 'profile': 'tabular-data-resource', 'fields': {'name': 'accessLevel', 'type': 'string', 'format': 'default'}}
--------------------------------
Cleaning duplicates
Rows from resource datajson
Duplicated ncepgfsbrwpprof
Duplicated 10.5439/1027266
Duplicated avhrr12
Duplicated  10.5439/1150252
Duplicated rp
Duplicated 10.5439/1095587
Duplicated 10.5439/1350600
Duplicated 10.5439/1027281
Duplicated  10.5439/1150254
... more duplicates not shown
2502 duplicates deleted. 2507 OK
---------------
Second part
---------------
Extracting from harvest source id: 8d4de31c-979c-4b50-be6b-ea3c72453ff6
PAGE 1 from harvest source id: 8d4de31c-979c-4b50-be6b-ea3c72453ff6
--------------------------------
Package processor
Package: <dataflows.base.package_wrapper.PackageWrapper object at 0x7fb33de46cc0>
 - Resource: {'name': 'ckanapi', 'path': 'res_1.csv', 'profile': 'tabular-data-resource', 'fields': {'name': 'license_title', 'type': 'string', 'format': 'default'}}
--------------------------------
PAGE 2 from harvest source id: 8d4de31c-979c-4b50-be6b-ea3c72453ff6
PAGE 3 from harvest source id: 8d4de31c-979c-4b50-be6b-ea3c72453ff6
3871 total resources in harvest source id: 8d4de31c-979c-4b50-be6b-ea3c72453ff6
Total processed: 2613. 
                    0 fail extras. 
                    0 fail identifier key.
                    0 deleted.
                    2613 datasets finded.

```

### Tests

We have two tests. One requires that a local instance of CKAN running and the others not.
```
python -m pytest -v tests/
python -m pytest -v tests_using_local_ckan_instance/
```