"""
Harvester DAGs
 This file must live at the airflow dags folder
"""

app_path = '/home/hudson/dev/datopian/harvesting-data-json-v2'
env_path = '/home/hudson/envs/data_json_etl'
import sys
sys.path.append(app_path)

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from harvester.data_gov_api import CKANPortalAPI
from harvester.logs import logger


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 8),
    'email': ['devops@datopian.com'],  # TODO check
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2020, 2, 1),
}

valid_frequencies = {'DAILY': {'interval': timedelta(days=1)},
                     'WEEKLY': {'interval': timedelta(weeks=1)},
                     'MONTHLY': {'interval': timedelta(days=30)},
                     'MANUAL': {'interval': timedelta(days=20)},
                     'BIWEEKLY': {'interval': timedelta(weeks=2)}
                     }  # TODO how for manual ?

# we need dags visibles
# https://airflow.readthedocs.io/en/stable/concepts.html#scope
# one dag for each _frequency_
logger.info('CREATING DAGs')
dag_daily = DAG('HARVEST_DAILY',
                default_args=default_args,
                schedule_interval=valid_frequencies['DAILY']['interval'])
dag_weekly = DAG('HARVEST_WEEKLY',
                 default_args=default_args,
                 schedule_interval=valid_frequencies['WEEKLY']['interval'])
dag_monthly = DAG('HARVEST_MONTHLY',
                  default_args=default_args,
                  schedule_interval=valid_frequencies['WEEKLY']['interval'])
dag_manual = DAG('HARVEST_MANUAL',
                 default_args=default_args,
                 schedule_interval=valid_frequencies['MANUAL']['interval'])
dag_biweekly = DAG('HARVEST_BIWEEKLY',
                   default_args=default_args,
                   schedule_interval=valid_frequencies['BIWEEKLY']['interval'])


dags = {'DAILY': {'dag': dag_daily, 'last_task': None},
        'WEEKLY': {'dag': dag_weekly, 'last_task': None},
        'MONTHLY': {'dag': dag_monthly, 'last_task': None},
        'MANUAL': {'dag': dag_manual, 'last_task': None},
        'BIWEEKLY': {'dag': dag_biweekly, 'last_task': None}
        }


catalog_url = 'http://ckan:5000'
catalog_api_key = '5ce77b38-3556-4a2c-9e44-5a18f53f9862'

cpa = CKANPortalAPI(base_url=catalog_url, api_key=catalog_api_key)
urls = []

templated_harvest_command = """
            source {{ params.env_path }}/bin/activate
            cd {{ params.app_path }}
            python harvest.py \
                --name {{ params.name }} \
                --url {{ params.data_json_url }} \
                --harvest_source_id {{ params.harvest_source_id }} \
                --ckan_owner_org_id {{ params.ckan_org_id }} \
                --catalog_url {{ params.catalog_url }} \
                --ckan_api_key {{ params.ckan_api_key }} \
                --limit_dataset 10 # limit for test, remove for production
            """

results = cpa.search_harvest_packages(rows=1000, harvest_type='harvest', source_type='datajson')
for datasets in results:
    for harvest_source in datasets:

        frequency = harvest_source.get('frequency', 'MONTHLY').upper()
        if frequency not in valid_frequencies:
            raise Exception(f'Unknown frequency: {frequency}')

        url = harvest_source['url']
        if url in urls:  # avoid duplicates
            continue
        urls.append(url)

        organization = harvest_source['organization']
        name = harvest_source['name']
        # this is the ID of the organization at the external source
        # we need to get our local organizaion ID
        ckan_org_id = harvest_source['owner_org']
        params = {
            'env_path': env_path,
            'app_path': app_path,
            'name': name,
            'data_json_url': url,
            'harvest_source_id': harvest_source['id'],  # check if this is the rigth ID
            'ckan_org_id': ckan_org_id,
            'catalog_url': catalog_url,
            'ckan_api_key': catalog_api_key
            }

        dag = dags[frequency]['dag']
        task = BashOperator(
            task_id=f'harvest-{name}',
            bash_command=templated_harvest_command,
            params=params,
            trigger_rule="all_done",  # continue regardless of whether it fails. Everything will stop if it fails without this line
            dag=dag  # set actual dag for this task
            )

        logger.info(f'set {dag} dag for the task: {task.bash_command}')

        last_task = dags[frequency]['last_task']
        if last_task is not None:
            task.set_upstream(last_task)

        dags[frequency]['last_task'] = task

        logger.info(f'task added {task}')
