# Tests

With _pytest_

```
python -m pytest -v tests-using-local-ckan-instance
```

## Measure overage

```
python -m pytest --cov=. tests-using-local-ckan-instance/
================================================================ test session starts ================================================================
platform linux -- Python 3.6.7, pytest-5.0.1, py-1.8.0, pluggy-0.12.0
rootdir: /home/hudson/dev/datopian/harvesting-data-json-v2
plugins: cov-2.7.1
collected 35 items                                                                                                                                  

tests/test_clean_duplicates.py ....                                                                                                           [ 11%]
tests/test_data_ckan_api.py .                                                                                                                 [ 14%]
tests/test_data_json.py .....                                                                                                                 [ 28%]
tests/test_data_json_validator.py ...................                                                                                         [ 82%]
tests/test_functions.py .....                                                                                                                 [ 97%]
tests/test_functions2.py .                                                                                                                    [100%]

----------- coverage: platform linux, python 3.6.7-final-0 -----------
Name                                Stmts   Miss  Cover
-------------------------------------------------------
config.py                              40     12    70%
flow.py                                24     24     0%
flow2.py                               23     23     0%
flow3.py                               16     16     0%
functions.py                           86      9    90%
functions2.py                         107     14    87%
functions3.py                          13     13     0%
libs/data_gov_api.py                  143     96    33%
libs/data_json.py                     132     47    64%
libs/datajsonvalidator.py             214     69    68%
logs.py                                 9      0   100%
tests/test_clean_duplicates.py         37      1    97%
tests/test_data_ckan_api.py            16      0   100%
tests/test_data_json.py                47      0   100%
tests/test_data_json_validator.py     160      0   100%
tests/test_functions.py                35      3    91%
tests/test_functions2.py               34      0   100%
-------------------------------------------------------
TOTAL                                1136    327    71%
```

## Previous with unittest

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

Test one file

```
python -m unittest tests.test_functions -v
```