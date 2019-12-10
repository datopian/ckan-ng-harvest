### Tests

We have many tests: 
 - One for the data.json harvester
 - One for the CSW harvester
 - Several (optional) tests that requires a local instance of CKAN running.



```
python -m pytest tests/datajson
***************************
Test data.json
======================== test session starts ========================
platform linux -- Python 3.6.8, pytest-5.0.1, py-1.8.0, pluggy-0.12.0
rootdir: /home/hudson/dev/datopian/harvesting-data-json-v2/harvest/datajson
plugins: cov-2.7.1, celery-4.3.0
collected 53 items

tests/test_clean_duplicates.py ....    [  7%]
tests/test_data_json_validator.py .....[ 86%]
tests/test_functions.py ......         [ 98%]
tests/test_functions2.py .             [100%]

=============================== 53 passed in 98.14 seconds ===

# REQUEIRE UPGRADE 
# python -m pytest tests/csw

```


# Tests with local CKAN instance

The script _test_with_ckan.sh_ contains other test with CKAN running loically.  

```
python -m pytest tests_with_ckan
```
