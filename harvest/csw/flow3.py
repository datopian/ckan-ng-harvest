"""
continue from flow2.py
"""

import argparse
# from dataflows.join import join_with_self
import json
import os
from dataflows import Flow, load

from harvester import config
from functions3 import (write_results_to_ckan,
                        build_validation_error_email
                        )
from harvesters.logs import logger

parser = argparse.ArgumentParser()
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)")
parser.add_argument("--ckan_owner_org_id", type=str, help="Source ID for filter CKAN API")
parser.add_argument("--catalog_url", type=str, help="URL for write CKAN API")
parser.add_argument("--ckan_api_key", type=str, help="CKAN API KEY")
parser.add_argument("--harvest_source_id", type=str, help="Source ID for filter CKAN API")

args = parser.parse_args()
config.SOURCE_NAME = args.name  # Nice name of the source
config.CKAN_OWNER_ORG = args.ckan_owner_org_id
config.CKAN_CATALOG_URL = args.catalog_url
config.CKAN_API_KEY = args.ckan_api_key
config.SOURCE_ID = args.harvest_source_id

res = Flow(
    load(load_source=config.get_flow2_datasets_result_path()),
    write_results_to_ckan,
).results()

build_validation_error_email(res[0][0])

# save results
# TODO in res[0][0] is an exception wi will fail here
dmp = json.dumps(res[0][0], indent=2)
f = open(config.get_flow2_datasets_result_path(), 'w')
f.write(dmp)
f.close()
