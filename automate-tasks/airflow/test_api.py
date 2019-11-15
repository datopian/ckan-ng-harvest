import requests

report = {
    'total_latest_runs': 0,
    'total_runs': 0,
    'total_executions': 0,
    'runs_states': {},
    'executions_states': {}
}  # final report
base_url = 'http://nginx:8080/airflow/api/experimental'

# API first test
endpoint = '/test'
results = requests.get(f'{base_url}{endpoint}')
print(f'request: {endpoint}: status code: {results.status_code}')
assert results.status_code == 200
data = results.json()
assert data['status'] == 'OK'

# latest runs
endpoint = '/latest_runs'
results = requests.get(f'{base_url}{endpoint}')
print(f'request: {endpoint}: status code: {results.status_code}')
assert results.status_code == 200
data = results.json()
assert type(data['items']) == list
report['total_latest_runs'] = len(data['items'])
for dag in data['items']:
    dag_id = dag['dag_id']
    print(f'Analyzing DAG {dag_id}')
    execution_date = dag['execution_date']
    start_date = dag['start_date']
    print(f'  - Execution date:{execution_date}')

    # dag runs
    print('  - Runs:')
    endpoint = f'/dags/{dag_id}/dag_runs'
    results = requests.get(f'{base_url}{endpoint}')
    print(f'request: {endpoint}: status code: {results.status_code}')
    assert results.status_code == 200
    dag_runs = results.json()
    assert type(dag_runs) == list
    for run in dag_runs:
        execution_date = run['execution_date']
        start_date = run['start_date']
        rid = run['id']
        run_id = run['run_id']

        state = run['state']
        if state not in report['runs_states']:
            report['runs_states'][state] = 0
        report['runs_states'][state] += 1

        print(f'    - ID:{rid} {run_id}')
        print(f'    - Execution date:{execution_date}')
        print(f'    - State:{state}')

        print('    - Executions')
        endpoint = f'/dags/{dag_id}/dag_runs/{execution_date}'
        results = requests.get(f'{base_url}{endpoint}')
        print(f'request: {endpoint}: status code: {results.status_code} resusts: {results.text}')
        assert results.status_code == 200
        executions = results.json()
        assert 'state' in executions
        state = executions['state']
        print(f'      - State:{state}')

        if state not in report['executions_states']:
            report['executions_states'][state] = 0
        report['executions_states'][state] += 1


print('-------------------')
print('-------------------')
print('Final Report')
print(report)
print('-------------------')
print('-------------------')







