# Harvesting data.json files

The harvest process includes:
 - Read a [data.json](data.json.md) resource file from an external resource I want to harvest.
 - Validate and save these results.
 - Read the previous resources harvested for that particular source (via the harvest_source_id)
 - Compare both resources and list the differences.
 - Update the CKAN installation with these updates 

Current process: [using ckan extensions](harvest-in-ckanext.md).  

Process using [dataflows](https://github.com/datahq/dataflows) and [datapackages](https://github.com/frictionlessdata/datapackage-py):  

Usage flow script

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