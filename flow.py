"""
Get updated list of resources from a data.json file
Get the actual list of resources in data.gov
Compare both and upgrade data.gov

Using:
 - DataFlows: https://github.com/datahq/dataflows/blob/master/TUTORIAL.md
 - DataPackages: https://frictionlessdata.io/data-packages/ + https://github.com/frictionlessdata/datapackage-py
"""

from dataflows import Flow, printer, dump_to_path, load, update_resource
# from dataflows.join import join_with_self
import json
import os
from logs import logger
from functions import (get_data_json_from_url,
                       clean_duplicated_identifiers,
                       dbg_packages,
                       )

import config
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--url", type=str, help="URL of the data.json")
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)")
parser.add_argument("--force_download", action='store_true',
                                        help="Force download or just use local data.json prevously downloaded")
parser.add_argument("--harvest_source_id", type=str, help="Source ID for filter CKAN API")

args = parser.parse_args()

config.SOURCE_NAME = args.name  # Nice name of the source
config.SOURCE_URL = args.url  # data.json final URL

Flow(

    get_data_json_from_url(config.SOURCE_URL, name=config.SOURCE_NAME,
                            data_json_path=config.get_datajson_cache_path()),

    # I like to split _headers_ and ['dataset'] as two different resources. Then valid the headers from one side and process the ['dataset'] as rows independently. It's a good idea?
    update_resource('res_1', name='datajson'),

    clean_duplicated_identifiers,  # remove duplicates
    # also save datapackages at (config.get_data_packages_folder_path) 'data/NAME/data-packages/*.json

    # TODO analyze for replace "clean_duplicated" with "join_with_self" standar processor: join_with_self(source_name, source_key, target_name, fields),
    # https://github.com/datahq/dataflows/blob/master/PROCESSORS.md#joinpy

    dbg_packages,  # debug info about packages

    # if we want a full data package for this list:
    # dump_to_path(config.get_base_path()),
).process()[1]

logger.info('To continue this: python3 flow2.py '
            f'--name {config.SOURCE_NAME} '
            f'--data_packages_path {config.get_data_packages_folder_path()} '
            f'--harvest_source_id {args.harvest_source_id}')
