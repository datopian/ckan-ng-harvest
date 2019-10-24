"""
Harvester DAGs
 This file must live at the airflow dags folder
"""
import os
import shlex
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils import dates
from datetime import datetime, timedelta
from harvester.data_gov_api import CKANPortalAPI
from harvester.logs import logger

app_path = os.environ.get('HARVESTER_APP_PATH', None)
catalog_url = os.environ.get('CKAN_BASE_URL', None)
catalog_api_key = os.environ.get('CKAN_API_KEY', None)

strs = '************************'
logger.info(f'{strs}\nStarting DAG file for Harvest\n{catalog_url}\n{catalog_api_key}\n{strs}')

# in some environments we need to read the just created CKAN API KEY from databases
api_key_from_db = catalog_api_key == 'READ_FROM_DB'
if api_key_from_db:
    import sqlalchemy as db
    # string connection to CKAN psql, like: postgresql://ckan:123456@db/ckan
    psql_ckan_conn = os.environ.get('PSQL_CKAN_CONN', None)
    engine = db.create_engine(psql_ckan_conn)
    conn = engine.connect()
    query = conn.execute("select apikey from public.user where name='admin'")
    if len(query) == 1:
        os.environ['CKAN_API_KEY'] = query[0]
        logger.info('Read API KEY from database: {}'.format(query[0]))
    else:
        logger.error('Unable to read API KEY from database')


source_types = ['datajson']  # , 'csw']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(7),  # 7 days ago
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

cpa = CKANPortalAPI(base_url=catalog_url, api_key=catalog_api_key)
urls = []

templated_harvest_command = """
            cd {{ params.app_path }}/harvest/{{ params.source_type }}
            python harvest.py \
                --name {{ params.name }} \
                --url {{ params.data_json_url }} \
                --harvest_source_id {{ params.harvest_source_id }} \
                --ckan_owner_org_id {{ params.ckan_org_id }} \
                --catalog_url {{ params.catalog_url }} \
                --ckan_api_key {{ params.ckan_api_key }}
            """

for source_type in source_types:
    results = cpa.search_harvest_packages(rows=1000,
                                          harvest_type='harvest',
                                          source_type=source_type)
    for datasets in results:
        for harvest_source in datasets:

            frequency = harvest_source.get('frequency', 'MONTHLY').upper()
            if frequency not in valid_frequencies:
                # use default
                frequency = 'MONTHLY'
                # raise Exception(f'Unknown frequency: {frequency}')

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
                'app_path': app_path,
                'name': name,
                'source_type': source_type,
                'data_json_url': shlex.quote(url),
                'harvest_source_id': harvest_source['id'],  # check if this is the rigth ID
                'ckan_org_id': ckan_org_id,
                'catalog_url': shlex.quote(catalog_url),
                'ckan_api_key': catalog_api_key
                }

            interval = valid_frequencies[frequency]['interval']
            tp = source_type.upper()
            nm = name.upper()
            dag_id = f'HARVEST_{tp}_{nm}'
            new_dag = DAG(dag_id, default_args=default_args, schedule_interval=interval)
            # http://airflow.apache.org/faq.html#how-can-i-create-dags-dynamically
            globals()[dag_id] = new_dag
            task = BashOperator(
                task_id=f'harvest-{source_type}-{name}',
                bash_command=templated_harvest_command,
                params=params,
                dag=new_dag
                )

            logger.info(f'task added {dag_id} {task}')
