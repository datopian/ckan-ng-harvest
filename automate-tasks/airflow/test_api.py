""" Test Airflow API
Sample results:

{
    'total_latest_runs': 38,
    'total_runs': 38,
    'runs_states': {
        'running': 13,
        'failed': 25
        },
    'tasks_states': {
        'running': 3,
        'scheduled': 10,
        'failed': 25
        },
    'executions_states': {
        'running': 13,
        'failed': 25
        }
    }

"""

import requests
report = {
    'total_latest_runs': 0,
    'total_runs': 0,
    'runs_states': {},
    'tasks_states': {},
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
    task_id = dag_id  # no way to get task with the API
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
        report['total_runs'] += 1
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

        print('    - Execution')
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

        # task in run
        endpoint = f'/dags/{dag_id}/dag_runs/{execution_date}/tasks/{task_id}'
        results = requests.get(f'{base_url}{endpoint}')
        print(f'request: {endpoint}: status code: {results.status_code}')
        assert results.status_code == 200
        task_run = results.json()

        state = task_run['state']
        if state not in report['tasks_states']:
            report['tasks_states'][state] = 0
        report['tasks_states'][state] += 1

        print(f'      - TASK:{task_id}')
        print(f'        - State:{task_run["state"]}')
        print(f'        - Duration:{task_run["duration"]}')
        print(f'        - Start date:{task_run["start_date"]}')
        print(f'        - End date:{task_run["end_date"]}')
        print(f'        - Execution date:{task_run["execution_date"]}')
        print(f'        - Hostname:{task_run["hostname"]}')
        print(f'        - Job ID:{task_run["job_id"]}')
        print(f'        - Operator:{task_run["operator"]}')

print('-------------------')
print('-------------------')
print('Final Report')
print(report)
print('-------------------')
print('-------------------')







