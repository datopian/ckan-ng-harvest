"""
Harvester DAGs
 This file must live at the airflow dags folder
"""

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

dag = DAG('harvest_all_data_gov', default_args=default_args,
          schedule_interval=timedelta(weeks=1))

logger.info(f'DAG created {dag}')
catalog_url = 'http://ckan:5000'
catalog_api_key = '79744bbe-f27b-46c8-a1e0-8f7264746c86'

cpa = CKANPortalAPI(base_url=catalog_url)
urls = []
last_task = None

for results in cpa.search_harvest_packages(harvest_type='harvest', source_type='datajson'):
    for harvest_source in results:

        url = harvest_source['url']
        if url in urls:  # avoid duplicates
            continue
        urls.append(url)

        organization = harvest_source['organization']

        templated_harvest_command = """
            python3 harvest.py \
                --name {{ params.name }} \
                --url {{ params.data_json_url }} \
                --harvest_source_id {{ params.harvest_source_id }} \
                --ckan_owner_org_id {{ params.ckan_org_id }} \
                --catalog_url {{ params.catalog_url }} \
                --ckan_api_key {{ params.ckan_api_key }}
            """

        name = harvest_source['name']
        # this is the ID of the organization at the external source
        # we need to get our local organizaion ID
        ckan_org_id = harvest_source['owner_org']
        params = {
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

        if last_task is not None:
            task.set_upstream(last_task)

        last_task = task

        logger.info(f'task added {task}')
