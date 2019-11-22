"""
continue from flow.py
"""

import argparse
# from dataflows.join import join_with_self
import json
import os
from dataflows import (Flow, dump_to_path, load, printer,
                       update_resource,
                       duplicate, add_field
                       )

from harvesters import config
from functions2 import (get_current_ckan_resources_from_api,
                        compare_resources
                        )
from harvesters.logs import logger

parser = argparse.ArgumentParser()
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)")
parser.add_argument("--catalog_url", type=str, help="URL for write CKAN API")
parser.add_argument("--harvest_source_id", type=str, help="Source ID for filter CKAN API")

args = parser.parse_args()

config.SOURCE_NAME = args.name  # Nice name of the source
config.CKAN_CATALOG_URL = args.catalog_url
config.SOURCE_ID = args.harvest_source_id

res = Flow(
    # add other resource to this process. The packages list from data.gov
    get_current_ckan_resources_from_api(harvest_source_id=config.SOURCE_ID),
    update_resource('res_1', name='ckan_results'),
    # new field at this copy for comparasion results
    add_field(name='comparison_results',
              type='object',
              resources='ckan_results'),

    # Compare both resources
    # In CSW source the datasets have the identifier field: "identifier"
    # In CKAN API results the datasets have the same identifier at "extras" list: {"key": "identifier", "value": "xxxxxxxx"}
    compare_resources,
).results()

# save results
# comparison results
dmp = json.dumps(res[0][0], indent=2)
f = open(config.get_flow2_datasets_result_path(), 'w')
f.write(dmp)
f.close()

pkg = res[1]  # package returned
pkg.save(config.get_flow2_data_package_result_path())

logger.info('Continue to next step with: python3 flow3.py '
            f'--name {config.SOURCE_NAME} '
            f'--harvest_source_id {args.harvest_source_id}')