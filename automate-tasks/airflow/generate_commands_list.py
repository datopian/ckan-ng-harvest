"""
generate a list of the commands to harvest
"""

app_path = '/home/hudson/dev/datopian/harvesting-data-json-v2'
env_path = '/home/hudson/envs/data_json_etl'
import sys
sys.path.append(app_path)

from harvester.data_gov_api import CKANPortalAPI
from harvester.logs import logger
from jinja2 import Template


catalog_url = 'http://ckan:5000'
catalog_api_key = '5ce77b38-3556-4a2c-9e44-5a18f53f9862'

cpa = CKANPortalAPI(base_url=catalog_url, api_key=catalog_api_key)
urls = []

templated_harvest_command = """
            source {{ env_path }}/bin/activate
            cd {{ app_path }}
            python harvest.py \
                --name {{ name }} \
                --url {{ data_json_url }} \
                --harvest_source_id {{ harvest_source_id }} \
                --ckan_owner_org_id {{ ckan_org_id }} \
                --catalog_url {{ catalog_url }} \
                --ckan_api_key {{ ckan_api_key }}
            """

results = cpa.search_harvest_packages(rows=1000, harvest_type='harvest', source_type='datajson')
for datasets in results:
    for harvest_source in datasets:

        url = harvest_source['url']
        if url in urls:  # avoid duplicates
            continue
        urls.append(url)

        organization = harvest_source['organization']
        name = harvest_source['name']
        # this is the ID of the organization at the external source
        # we need to get our local organizaion ID
        ckan_org_id = harvest_source['owner_org']
        context = {
            'env_path': env_path,
            'app_path': app_path,
            'name': name,
            'data_json_url': url,
            'harvest_source_id': harvest_source['id'],  # check if this is the rigth ID
            'ckan_org_id': ckan_org_id,
            'catalog_url': catalog_url,
            'ckan_api_key': catalog_api_key
            }

        template = Template(templated_harvest_command)
        print(template.render(**context))
