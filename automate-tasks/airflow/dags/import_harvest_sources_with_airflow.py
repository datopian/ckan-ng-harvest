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

source_types = ['datajson', 'csw']

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

logger.info('CREATING Import harvest source DAG')
dag_daily = DAG('IMPORT_HARVEST_SOURCES_DAILY',
                default_args=default_args,
                schedule_interval=timedelta(days=1))


# import harvest sources from another CKAN instance is a custom optional task
import_harvest_url = os.environ.get('CKAN_IMPORT_HARVEST_SOURCES_URL', None)
if import_harvest_url is not None and import_harvest_url != '':
    import_method = os.environ.get('CKAN_IMPORT_REQUEST_METHOD', 'POST')
    last_task = None
    for source_type in source_types:
        tpl_command = """
                cd {{ params.app_path }}
                python import_harvest_sources.py \
                    --import_from_url {{ params.import_harvest_url }} \
                    --source_type {{ params.source_type }} \
                    --method {{ params.import_method }}
                """

        params = {
            'app_path': app_path,
            'import_harvest_url': shlex.quote(import_harvest_url),
            'source_type': source_type,
            'import_method': import_method,
            }

        task = BashOperator(
            task_id=f'import-harvest-sources-{source_type}',
            bash_command=tpl_command,
            params=params,
            trigger_rule="all_done",  # continue regardless of whether it fails. Everything will stop if it fails without this line
            dag=dag_daily  # set actual dag for this task
            )

        logger.info(f'set {dag_daily} dag for the task: {task.bash_command} with params {params}')

        if last_task is not None:
            task.set_upstream(last_task)
        last_task = task

        logger.info(f'task added {task}')
