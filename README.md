# Harvesting data.json files

The harvest process includes:
 - Read a [data.json](data.json.md) resource file from an external resource I want to harvest.
 - Validate and save these results.
 - Read the previous resources harvested for that particular source (via the harvest_source_id)
 - Compare both resources and list the differences.
 - Update the CKAN installation with these updates 

Current process: [using ckan extensions](harvest-in-ckanext.md).  

Process using [dataflows](https://github.com/datahq/dataflows) and [datapackages](https://github.com/frictionlessdata/datapackage-py):  

Usage flow_v2 script

```
usage: flow_v2.py [-h] [--url URL] [--name NAME] [--force_download]
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
$ python3 flow_v2.py --url http://www.usda.gov/data.json --name agriculture --harvest_source_id 50ca39af-9ddb-466d-8cf3-84d67a204346
Geting data.json from http://www.usda.gov/data.json
OK http://www.usda.gov/data.json
VALID JSON
1580 datasets finded
--------------------------------
Package processor
Package: <dataflows.base.package_wrapper.PackageWrapper object at 0x7f49e66769b0>
 - Resource: {'name': 'datajson', 'path': 'res_1.csv', 'profile': 'tabular-data-resource', 'fields': {'name': 'identifier', 'type': 'string', 'format': 'default'}}
--------------------------------
Extracting from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
PAGE 1 from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
--------------------------------
Package processor
Package: <dataflows.base.package_wrapper.PackageWrapper object at 0x7f49e4da16a0>
 - Resource: {'name': 'datajson', 'path': 'res_1.csv', 'profile': 'tabular-data-resource', 'fields': {'name': 'identifier', 'type': 'string', 'format': 'default'}}
 - Resource: {'name': 'ckanapi', 'path': 'res_2.csv', 'profile': 'tabular-data-resource', 'fields': {'name': 'license_title', 'type': 'string', 'format': 'default'}}
--------------------------------
Cleaning duplicates
Rows from resource_ datajson
0 duplicates deleted. 1580 OK
PAGE 2 from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
3369 total resources in harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
```

Another way of dealing with the problem: without dataflows
Usage _process_data_json_:

```
python3 process_data_json.py -h
usage: process_data_json.py [-h] [--url URL] [--name NAME] [--force_download]
                            [--request_timeout REQUEST_TIMEOUT]

optional arguments:
  -h, --help            show this help message and exit
  --url URL             URL of the data.json
  --name NAME           Name of the resource (for generate the containing
                        folder)
  --force_download      Force download or just use local data.json prevously
                        downloaded
  --request_timeout REQUEST_TIMEOUT
                        Request data.json URL timeout
```

Energy data.json
```
python3 process_data_json.py --name energy-data --url http://www.energy.gov/data.json

Downloading http://www.energy.gov/data.json
Downloaded OK
JSON OK
Validate OK: 5009 datasets
Readed 5009 datasets including 5425 resources. 1251 duplicated identifiers removed
```

Agiculture data.json

```
python3 process_data_json.py --name agriculture --url http://www.usda.gov/data.json

Downloading http://www.usda.gov/data.json
Downloaded OK
JSON OK
Validate OK: 1580 datasets
Readed 1580 datasets including 3408 resources. 0 duplicated identifiers removed
```

Healt data.json (with errors)

```
python3 process_data_json.py --name healt --url https://healthdata.gov/data.jsonUsing data.json prevously downloaded: data/healt/data.json
JSON OK
1 Errors validating data
Error 1/1 validating data:
	Error validating JsonSchema: 'programCode' is a required property

Fai
Validate FAILED: 1766 datasets
Readed 1766 datasets including 3938 resources. 57 duplicated identifiers removed
```


# Getting CKAN packages via _harvest_source_id_

Usage _process_ckan_api_:

```
python3 process_ckan_api.py -h
usage: process_ckan_api.py [-h] [--ckan_base_url CKAN_BASE_URL] [--name NAME]
                           [--harvest_source_id HARVEST_SOURCE_ID]
                           [--force_download]
                           [--request_timeout REQUEST_TIMEOUT]

optional arguments:
  -h, --help            show this help message and exit
  --ckan_base_url CKAN_BASE_URL
                        URL of the data.json
  --name NAME           Name of the resource (for generate the containing
                        folder)
  --harvest_source_id HARVEST_SOURCE_ID
                        Source ID for filter CKAN API
  --force_download      Force download or just use local data.json prevously
                        downloaded
  --request_timeout REQUEST_TIMEOUT
                        Request data.json URL timeout

```

Example

```
python3 process_ckan_api.py --name treasury --harvest_source_id de90314a-7c7d-4aff-bd84-87b134bba13d
Downloading
Readed 278 datasets including 322 resources. 0 duplicated identifiers removed
```

## Old version using DataFlows

```
python3 flow.py

Downloaded OK
JSON OK
Validate OK: 1580 datasets
 - Dataset: Department of Agriculture Congressional Logs for Fiscal Year 2014
 - Dataset: Department of Agriculture Enterprise Data Inventory
 - Dataset: Department of Agriculture Secretary's Calendar Schedule
 - Dataset: Realized Cost Savings and Avoidance
 - Dataset: USDA Active Purchase Card Holders
 - Dataset: USDA Annual FOIA Report
 - Dataset: USDA Bureau IT Leadership Directory
 - Dataset: USDA Governance Boards
 - Dataset: USDA Help Desk Support Data Asset
0 duplicates deleted. 1580 OK
Extracting from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
PAGE 1 from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
2161 total resources
PAGE 2 from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
3369 total resources
PAGE 3 from harvest source id: 50ca39af-9ddb-466d-8cf3-84d67a204346
3369 total resources

```


### Tests

```
python -m unittest discover -s tests -v

test_load_from_url (test_data_ckan_api.CKANPortalAPITestClass) ... API packages search page 1
ok
test_load_from_url (test_data_json.DataJSONTestClass) ... ok
test_read_json (test_data_json.DataJSONTestClass) ... ok
test_validate_json1 (test_data_json.DataJSONTestClass) ... ok
test_validate_json2 (test_data_json.DataJSONTestClass) ... ok
test_validate_json3 (test_data_json.DataJSONTestClass) ... ok

----------------------------------------------------------------------
Ran 6 tests in 45.506s

OK
```