"""
Harvester DAGs
 This file must live at the airflow dags folder
"""
import os
import json
import sys
import shlex
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils import dates
from datetime import datetime, timedelta
from harvester_adapters.ckan.api import CKANPortalAPI
from harvesters.logs import logger

app_path = os.environ.get('HARVESTER_APP_PATH', None)
catalog_url = os.environ.get('CKAN_BASE_URL', None)
catalog_api_key = os.environ.get('CKAN_API_KEY', None)

strs = '************************'
logger.info(f'{strs}\nStarting Harvest DAGs\n{catalog_url}\n{catalog_api_key}\n{strs}')

# in some environments we need to read the just created CKAN API KEY from databases
api_key_from_db = catalog_api_key == 'READ_FROM_DB'
if api_key_from_db:
    import sqlalchemy as db
    # string connection to CKAN psql, like: postgresql://ckan:123456@db/ckan
    # readed from CKAN secrets in CKAN CLOUD DOCKER or defined locally
    psql_ckan_conn = os.environ.get('SQLALCHEMY_URL', None)
    if not psql_ckan_conn:
        try:
            with open('/etc/ckan-conf/secrets/secrets.sh') as secrets_file:
                secrets = secrets_file.readlines()
                psql_ckan_conn = list(filter(lambda l: 'SQLALCHEMY_URL' in l, secrets))[0].split('=')[1].strip()
        except Exception as e:
            logger.error(repr(e))
            raise
    try:
        engine = db.create_engine(psql_ckan_conn)
    except Exception as e:
        error = f'Failed to connect with CKAN DB {psql_ckan_conn}'
        logger.error(error)
        sys.exit(error)
    conn = engine.connect()
    result = conn.execute("select apikey from public.user where name='admin'")
    try:
        row = result.fetchone()
    except Exception as e:
        error = f'Unable to read API KEY from database {psql_ckan_conn}'
        logger.error(error)
        sys.exit(error)

    if row is None:
        error = f'There is no API KEY for user "admin"'
        logger.error(error)
        sys.exit(error)

    os.environ['CKAN_API_KEY'] = row['apikey']
    catalog_api_key = row['apikey']
    logger.info('Read API KEY from database: {} ({})'.format(row['apikey'], psql_ckan_conn))
    result.close()

source_types = ['datajson']  # , 'csw']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(1),
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
            cd {{ params.app_path }}
            python {{ params.python_command }}.py \
                --name {{ params.name }} \
                --url {{ params.data_json_url }} \
                --harvest_source_id {{ params.harvest_source_id }} \
                --ckan_owner_org_id {{ params.ckan_org_id }} \
                --catalog_url {{ params.catalog_url }} \
                --ckan_api_key {{ params.ckan_api_key }} \
                --config {{ params.harverst_source_config }}
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

            harverst_source_config = harvest_source.get('config', {})

            if source_type == 'datajson':
                python_command = 'harvest_datajson'
            elif source_type == 'csw':
                python_command = 'harvest_csw'

            params = {
                'app_path': app_path,
                'python_command': python_command,
                'name': name,
                'source_type': source_type,
                'data_json_url': shlex.quote(url),
                'harvest_source_id': harvest_source['id'],  # check if this is the rigth ID
                'ckan_org_id': ckan_org_id,
                'catalog_url': shlex.quote(catalog_url),
                'ckan_api_key': catalog_api_key,
                'harverst_source_config': json.dumps(harverst_source_config)
                }

            interval = valid_frequencies[frequency]['interval']
            tp = source_type.upper()
            nm = name.upper()
            dag_id = f'HARVEST_{tp}_{nm}'
            new_dag = DAG(dag_id, default_args=default_args, schedule_interval=interval)
            # http://airflow.apache.org/faq.html#how-can-i-create-dags-dynamically
            globals()[dag_id] = new_dag
            task = BashOperator(
                task_id=dag_id,  # f'harvest-{source_type}-{name}',
                bash_command=templated_harvest_command,
                params=params,
                dag=new_dag
                )

            logger.info(f'task added {dag_id} {task}')
