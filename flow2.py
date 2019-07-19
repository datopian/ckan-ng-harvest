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

import config
from functions2 import (get_current_ckan_resources_from_api,
                        compare_resources
                        )
from logs import logger

parser = argparse.ArgumentParser()
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)")
parser.add_argument("--force_download", action='store_true',
                    help="Force download or just use local data.json prevously downloaded")
parser.add_argument("--harvest_source_id", type=str, help="Source ID for filter CKAN API")
parser.add_argument("--data_packages_path", type=str, help="Path of data packages from data.json")

args = parser.parse_args()

config.SOURCE_NAME = args.name  # Nice name of the source
config.SOURCE_ID = args.harvest_source_id
data_packages_path = args.data_packages_path

res = Flow(
    # add other resource to this process. The packages list from data.gov
    get_current_ckan_resources_from_api(harvest_source_id=config.SOURCE_ID),
    update_resource('res_1', name='ckan_results'),
    # new field at this copy for comparasion results
    add_field(name='comparison_results',
              type='object',
              resources='ckan_results'),

    # Compare both resources
    # In data.json the datasets have the identifier field: "identifier": "USDA-ERS-00071"
    # In CKAN API results the datasets have the same identifier at "extras" list: {"key": "identifier", "value": "USDA-ERS-00071"}
    compare_resources,

).results()

# save results (data package and final datasets results)
dmp = json.dumps(res[0][0], indent=2)
f = open(config.get_flow2_datasets_result_path(), 'w')
f.write(dmp)
f.close()

pkg = res[1]  # package returned
pkg.save(config.get_flow2_data_package_result_path())
