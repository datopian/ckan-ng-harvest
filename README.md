# Harvesting data.json files

Read and process [data.json](data.json.md) resources files.  

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
```