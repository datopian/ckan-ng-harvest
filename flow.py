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
                       validate_datasets,
                       save_as_data_packages,
                       )
from logs import logger
import config
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--url", type=str, help="URL of the data.json")
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)")
parser.add_argument("--limit_dataset", type=int, default=0, help="Limit datasets to harvest on each source. Defualt=0 => no limit")

args = parser.parse_args()

config.SOURCE_NAME = args.name  # Nice name of the source
config.SOURCE_URL = args.url  # data.json final URL
config.LIMIT_DATASETS = args.limit_dataset

res = Flow(
    # get data.json and yield all datasets
    # validate headers and save the validation errors
    get_data_json_from_url(url=config.SOURCE_URL),
    update_resource('res_1', name='datajson', path='datajson.csv'),

    # remove duplicates
    clean_duplicated_identifiers,

    # validate each dataset
    validate_datasets,

    # save each dataset as data package
    save_as_data_packages,
).results()

logger.info('Continue to next step with: python3 flow2.py '
            f'--name {config.SOURCE_NAME} ')

# save results (data package and final datasets results)
dmp = json.dumps(res[0][0], indent=2)
f = open(config.get_flow1_datasets_result_path(), 'w')
f.write(dmp)
f.close()

pkg = res[1]  # package returned
pkg.save(config.get_flow1_data_package_result_path())
