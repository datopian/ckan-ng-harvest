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


You can start this containers, get inside and run the tests:

```
docker-compose exec webserver bash
pip install -r dev-requirements.txt
python -m pytest -s tests_with_ckan
```

We use [pytest-vcr](https://pytest-vcr.readthedocs.io/en/latest/) based on [VCRpy](https://vcrpy.readthedocs.io/en/latest/), to mock http requests. In this way, we don't need to hit the real internet to run our test (which is very fragile and slow), because there is a mocked version of a each response needed by tests, in vcr's *cassettes* format. 

In order to update these *cassettes* just run as following: 

```
pytest --vcr-record=all
```

To actually hit the internet without use mocks, disable the plugin 

```
pytest --disable-vcr
```

In order to read from these *cassettes* just run as following: 

```
pytest --vcr-record=none
```
