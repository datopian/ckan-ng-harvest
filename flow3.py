"""
continue from flow2.py
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
from functions3 import write_results_to_ckan, write_final_report
from logs import logger

parser = argparse.ArgumentParser()
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)")
parser.add_argument("--harvest_source_id", type=str, help="Source ID for filter CKAN API")
parser.add_argument("--ckan_owner_org_id", type=str, help="Source ID for filter CKAN API")

args = parser.parse_args()
config.SOURCE_NAME = args.name  # Nice name of the source
config.SOURCE_ID = args.harvest_source_id
config.CKAN_OWNER_ORG = args.ckan_owner_org_id

res = Flow(
    load(load_source=config.get_flow2_datasets_result_path()),
    write_results_to_ckan,

).results()
