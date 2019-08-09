"""
Full harvest process. Include task in flow, flow2 and flow3
"""
import os
from logs import logger
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--url", type=str, help="URL of the data.json")
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)")
parser.add_argument("--harvest_source_id", type=str, help="Source ID for filter CKAN API")
parser.add_argument("--ckan_owner_org_id", type=str, help="CKAN ORG ID")
parser.add_argument("--catalog_url", type=str, help="URL for write CKAN API")
parser.add_argument("--ckan_api_key", type=str, help="CKAN API KEY")
parser.add_argument("--limit_dataset", type=int, default=0, help="Limit datasets to harvest on each source. Defualt=0 => no limit")

args = parser.parse_args()

logger.info('Starting full harvest process')

commands = [f'python3 flow.py --name {args.name} --url {args.url} --limit_dataset {args.limit_dataset}',
            f'python3 flow2.py --name {args.name} --harvest_source_id {args.harvest_source_id} --catalog_url {args.catalog_url}',
            f'python3 flow3.py --name {args.name} --ckan_owner_org_id {args.ckan_owner_org_id} --catalog_url {args.catalog_url} --ckan_api_key {args.ckan_api_key}']

for cmd in commands:
    logger.info(f'**************\nExecute: {cmd}\n**************')
    os.system(cmd)

""" local example
python3 harvest.py \
    --name agriculture \
    --url http://www.usda.gov/data.json \
    --harvest_source_id 50ca39af-9ddb-466d-8cf3-84d67a204346 \
    --ckan_owner_org_id my-local-test-organization-v2 \
    --catalog_url http://ckan:5000 \
    --ckan_api_key 79744bbe-f27b-46c8-a1e0-8f7264746c86
"""
