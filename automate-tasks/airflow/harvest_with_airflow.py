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
from libs.data_gov_api import CKANPortalAPI
from logs import logger


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 8),
    'email': ['devops@datopian.com'],  # TODO check
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2020, 2, 1),
}

dags = {}  # une dag for each _frquency_
valid_frequencies = {'DAILY': {'interval': timedelta(days=1)},
                     'WEEKLY': {'interval': timedelta(weeks=1)},
                     'MONTHLY': {'interval': timedelta(days=30)},
                     'MANUAL': {'interval': timedelta(days=20)},
                     'BIWEEKLY': {'interval': timedelta(weeks=2)}
                     }  # TODO how for manual ?

catalog_url = 'http://ckan:5000'
catalog_api_key = '2de6add4-bd1c-4f66-9e2b-37f4bc3ddd0f'

cpa = CKANPortalAPI(base_url=catalog_url, api_key=catalog_api_key)
urls = []

results = cpa.search_harvest_packages(rows=1000, harvest_type='harvest', source_type='datajson')
for datasets in results:
    for harvest_source in datasets:

        frequency = harvest_source.get('frequency', 'MONTHLY').upper()
        if frequency not in valid_frequencies:
            raise Exception(f'Unknown frequency: {frequency}')
        if frequency not in dags:
            logger.info('CREATING DAG')
            interval = valid_frequencies[frequency]['interval']
            dags[frequency] = {'dag': DAG(f'HARVEST_{frequency}',
                                    default_args=default_args,
                                    schedule_interval=interval),
                               'last_task': None}

            logger.info(f'DAG created {frequency}')

        # set actual dag for this task
        dag = dags[frequency]['dag']

        url = harvest_source['url']
        if url in urls:  # avoid duplicates
            continue
        urls.append(url)

        organization = harvest_source['organization']

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

        task = BashOperator(
            task_id=f'harvest-{name}',
            bash_command=templated_harvest_command,
            params=params,
            dag=dag)

        last_task = dags[frequency]['last_task']
        if last_task is not None:
            task.set_upstream(last_task)

        dags[frequency]['last_task'] = task

        logger.info(f'task added {task}')
