"""
generate a list of the commands to harvest
"""
import os
import argparse

import shlex
from harvester_adapters.ckan.api import CKANPortalAPI
from harvesters.logs import logger
from jinja2 import Template


parser = argparse.ArgumentParser()
parser.add_argument("--ckan_url", type=str, help="CKAN instance URL")
parser.add_argument("--ckan_api_key", type=str, help="Valid CKAN API KEY")
parser.add_argument("--source_type", type=str, default='datajson', help="Tipe of harvest source: datajson|csw|waf etc")

args = parser.parse_args()


catalog_url = args.ckan_url
catalog_api_key = args.ckan_api_key
source_type = args.source_type

cpa = CKANPortalAPI(base_url=catalog_url, api_key=catalog_api_key)
urls = []

templated_harvest_command = """
    python harvest_datajson.py \
        --name {{ name }} \
        --url {{ data_json_url }} \
        --harvest_source_id {{ harvest_source_id }} \
        --ckan_owner_org_id {{ ckan_org_id }} \
        --catalog_url {{ catalog_url }} \
        --ckan_api_key {{ ckan_api_key }} \
        --config {{ harverst_source_config }}
    """

results = cpa.search_harvest_packages(rows=1000, harvest_type='harvest', source_type=source_type)
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
        harverst_source_config = harvest_source.get('config', {})

        context = {
            'name': name,
            'data_json_url': shlex.quote(url),
            'harvest_source_id': harvest_source['id'],  # check if this is the rigth ID
            'ckan_org_id': ckan_org_id,
            'catalog_url': shlex.quote(catalog_url),
            'ckan_api_key': catalog_api_key,
            'harverst_source_config': str(harverst_source_config)
            }

        template = Template(templated_harvest_command)
        print(template.render(**context))
