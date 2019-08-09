# Harvesting data.json files

The harvest process includes:
 - Read a [data.json](data.json.md) resource file from an external resource I want to harvest.
 - Validate and save these results.
 - Read the previous resources harvested for that particular source (via the harvest_source_id)
 - Compare both resources and list the differences.
 - Update the CKAN installation with these updates 

Current process: [using ckan extensions](harvest-in-ckanext.md).  

Process using [dataflows](https://github.com/datahq/dataflows) and [datapackages](https://github.com/frictionlessdata/datapackage-py):  

# Full harvest local test

We need a local CKAN instance running ar http://ckan:5000.  
Test for a small version of education data.json

```
python3 harvest.py \
  --name smalleducation \
  --url https://gitlab.com/datopian/ckan-ng-harvest/raw/develop/public/small-education.json \
  --harvest_source_id d6ce0a12-5d48-452e-b97d-14dcb8426899 \
  --ckan_owner_org_id my-local-test-organization-v2 \
  --catalog_url http://ckan:5000 \
  --ckan_api_key 79744bbe-f27b-46c8-a1e0-8f7264746c86 \
  --limit_dataset 10  # just test 10 datasets
```

This process runs 3 parts: flow, flow1 and flow2

Results:

```
Starting full harvest process
Geting data.json from http://www.usda.gov/data.json
Downloaded OK
JSON OK
Validate OK: 1580 datasets
VALID JSON, 1580 datasets found
Cleaning duplicates
0 duplicates deleted. 1580 OK
Continue to next step with: python3 flow2.py --name agriculture2 
Extracting from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
PAGE 1 from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
Rows from resource ckan_results
No identifier (extras[].key.identifier not exists). Dataset.id: e2430c9b-2fe2-40c8-b08e-238eef975992
Dataset: d12a8b1f-f2d3-44ea-b0be-b37b7acbd595 not in DATA.JSON.It was deleted?: data/agriculture2/data-packages/data-json-MjFjZTFhZDEtNWFhNi00NTdkLTk0NDktNzU5NDVkMjFjMmZh.json
Dataset: 18915b7e-9ea4-419e-a3a7-bf62149ba2d0 not in DATA.JSON.It was deleted?: data/agriculture2/data-packages/data-json-OGI1NmY4YzItNmFjYy00YzkyLTg3YTEtMmYzYjM1MzA0NGY3.json
No identifier (extras[].key.identifier not exists). Dataset.id: 0c4888bf-4e8d-4e86-b774-0afda835bb86
PAGE 2 from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
Dataset: c20d553a-8a5e-4d1a-93c6-72f270689ade not in DATA.JSON.It was deleted?: data/agriculture2/data-packages/data-json-OTViMTk0MDItMWMyYy00M2VkLTg4YTUtZjQ3YTViMTAxMTU1.json
3369 total resources in harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
Total processed: 1570.
                0 fail extras.
                2 fail identifier key.
                17 deleted.
                1551 datasets found (1549 needs update, 2 are the same),
                29 new datasets.
Continue to next step with: python3 flow3.py --name agriculture2 --harvest_source_id 50ca39af-9ddb-466d-8cf3-84d67a204346
ERROR creating CKAN package: http://ckan:5000/api/3/action/package_create 
	 Status code: 409 
	 content:b'{"help": "http://ckan:5000/api/3/action/help_show?name=package_create", "success": false, "error": {"__type": "Validation Error", "tags": ["Tag \\"EARTH SCIENCE &gt; ATMOSPHERE &gt; ATMOSPHERIC RADIATION &gt; INCOMING SOLAR RADIATION\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; ATMOSPHERE &gt; ATMOSPHERIC TEMPERATURE &gt; AIR TEMPERATURE\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; ATMOSPHERE &gt; ATMOSPHERIC WATER VAPOR &gt; DEW POINT TEMPERATURE\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; ATMOSPHERE &gt; ATMOSPHERIC WATER VAPOR &gt; HUMIDITY\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; ATMOSPHERE &gt; ATMOSPHERIC WINDS &gt; SURFACE WINDS &gt; WIND SPEED/WIND DIRECTION\\" length is more than maximum 100, Tag \\"EARTH SCIENCE &gt; ATMOSPHERE &gt; ATMOSPHERIC WINDS &gt; SURFACE WINDS &gt; WIND SPEED/WIND DIRECTION\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; ATMOSPHERE &gt; PRECIPITATION &gt; PRECIPITATION AMOUNT\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; ATMOSPHERE &gt; PRECIPITATION &gt; RAIN\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; ATMOSPHERE &gt; PRECIPITATION &gt; SNOW\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; BIOSPHERE &gt; AQUATIC ECOSYSTEMS\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; TERRESTRIAL HYDROSPHERE &gt; SURFACE WATER\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; TERRESTRIAL HYDROSPHERE &gt; SURFACE WATER &gt; DISCHARGE/FLOW\\" must be alphanumeric characters or symbols: -_.", {}, {}, {}, {}, {}, {}, {}], "name": ["That URL is already in use."]}}'
ERROR creating CKAN package: http://ckan:5000/api/3/action/package_create 
	 Status code: 409 
	 content:b'{"help": "http://ckan:5000/api/3/action/help_show?name=package_create", "success": false, "error": {"__type": "Validation Error", "tags": ["Tag \\"EARTH SCIENCE &gt; AGRICULTURE &gt; SOILS\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; LAND SURFACE &gt; EROSION/SEDIMENTATION &gt; SEDIMENT TRANSPORT\\" must be alphanumeric characters or symbols: -_.", "Tag \\"EARTH SCIENCE &gt; TERRESTRIAL HYDROSPHERE &gt; SURFACE WATER &gt; RUNOFF\\" must be alphanumeric characters or symbols: -_.", {}, {}, {}, {}, {}, {}], "name": ["That URL is already in use."]}}'

Actions detected {'update': {'total': 1549, 'success': 0, 'fails': 0}, 'error': {'total': 2, 'success': 0, 'fails': 0}, 'delete': {'total': 17, 'success': 0, 'fails': 0}, 'ignore': {'total': 2, 'success': 0, 'fails': 0}, 'create': {'total': 29, 'success': 0, 'fails': 29}}

```


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