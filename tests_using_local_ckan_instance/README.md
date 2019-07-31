# Tests

With _pytest_

```
python -m pytest -v tests_using_local_ckan_instance/
================================================================ test session starts ================================================================
platform linux -- Python 3.6.7, pytest-5.0.1, py-1.8.0, pluggy-0.12.0 -- /home/hudson/envs/data_json_etl/bin/python
cachedir: .pytest_cache
rootdir: /home/hudson/dev/datopian/harvesting-data-json-v2
plugins: cov-2.7.1
collected 6 items                                                                                                                                   

tests_using_local_ckan_instance/test_data_ckan_api.py::CKANPortalAPITestClass::test_create_harvest_source PASSED                              [ 16%]
tests_using_local_ckan_instance/test_data_ckan_api.py::CKANPortalAPITestClass::test_create_package PASSED                                     [ 33%]
tests_using_local_ckan_instance/test_data_ckan_api.py::CKANPortalAPITestClass::test_create_package_with_tags PASSED                           [ 50%]
tests_using_local_ckan_instance/test_data_ckan_api.py::CKANPortalAPITestClass::test_get_admins PASSED                                         [ 66%]
tests_using_local_ckan_instance/test_data_ckan_api.py::CKANPortalAPITestClass::test_get_user_info PASSED                                      [ 83%]
tests_using_local_ckan_instance/test_data_ckan_api.py::CKANPortalAPITestClass::test_load_from_url PASSED                                      [100%]

============================================================= 6 passed in 1.35 seconds ==============================================================
```
