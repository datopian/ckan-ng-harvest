"""
Harvester DAGs

"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from libs.data_gov_api import CKANPortalAPI


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
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

dag = DAG('Harvest all at catalog.data.gov', default_args=default_args,
          schedule_interval=timedelta(weeks=1))

catalog_url = 'http://ckan:5000'
catalog_api_key = '79744bbe-f27b-46c8-a1e0-8f7264746c86'

cpa = CKANPortalAPI(base_url='https://catalog.data.gov')
urls = []
last_task = None

for results in cpa.search_harvest_packages(harvest_type='harvest', source_type='datajson'):
    for harvest_source in results:

        url = harvest_source['url']
        if url in urls:  # avoid duplicates
            continue
        urls.append(url)

        templated_harvest_command = """
            python3 harvest.py \
                --name {{ name }} \
                --url {{ data_json_url }} \
                --harvest_source_id {{ harvest_source_id }} \
                --ckan_owner_org_id {{ ckan_org_id }} \
                --catalog_url {{ catalog_url }} \
                --ckan_api_key {{ ckan_api_key }}
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