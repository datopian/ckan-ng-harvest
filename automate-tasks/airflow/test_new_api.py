""" Test Airflow Plugin API
"""

import requests
headers = {'rest_token': '123456'}
report = {

}  # final report

base_url = 'http://nginx:8080/airflow/admin/rest_api/api?api='

# API test version
endpoint = 'version'
results = requests.get(f'{base_url}{endpoint}')
print(f'request: {endpoint}: status code: {results.status_code}')
assert results.status_code == 403

results = requests.get(f'{base_url}{endpoint}', headers=headers)
print(f'request: {endpoint}: status code: {results.status_code}')
assert results.status_code == 200
data = results.json()
assert data['output'] == '1.10.4'


print('-------------------')
print('-------------------')
print('Final Report')
print(report)
print('-------------------')
print('-------------------')







