"""
continue from flow.py
"""

from dataflows import Flow, printer, dump_to_path, load, update_resource
# from dataflows.join import join_with_self
import json
import os
from logs import logger
from functions import dbg_packages
from functions2 import (get_current_ckan_resources_from_api,
                        # compare_resources,
                        pkg_processor
                       )

import config
import argparse
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

Flow(
    # add other resource to this process. The packages list from data.gov
    get_current_ckan_resources_from_api(harvest_source_id=config.SOURCE_ID,
                                        results_json_path=config.get_ckan_results_cache_path()),

    update_resource('res_1', name='ckan_results'),
    update_resource('res_2', name='comparasion_results'),
    dbg_packages,  # get info about updated packaghes

    # Compare both resources
    # In data.json the datasets have the identifier field: "identifier": "USDA-ERS-00071"
    # In CKAN API results the datasets have the same identifier at "extras" list: {"key": "identifier", "value": "USDA-ERS-00071"},

    # compare_resources(data_packages_path=data_packages_path),
    pkg_processor,

    dump_to_path(config.get_base_path()),
    # printer(num_rows=1), # , tablefmt='html')

).process()[1]
