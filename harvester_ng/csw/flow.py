# flake8: noqa
"""
Get updated list of resources from a CSW source
"""
# use always base project folder as base path for imports
# move libs to a python package to fix this
import json
import os

from dataflows import Flow, update_resource
# from dataflows.join import join_with_self
from harvesters.logs import logger
from functions import (get_csw_from_url,
                       clean_duplicated_identifiers,
                       validate_datasets,
                       save_as_data_packages,
                       )
from harvesters.logs import logger
from harvesters import config
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--url", type=str, help="URL of the data.json")
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)")
parser.add_argument("--prefix", type=str, default='csw', help="Prefix for all names to avoid collitions")
parser.add_argument("--limit_dataset", type=int, default=0, help="Limit datasets to harvest on each source. Defualt=0 => no limit")

args = parser.parse_args()

config.SOURCE_NAME = args.name  # Nice name of the source
config.SOURCE_URL = args.url  # data.json final URL
config.LIMIT_DATASETS = args.limit_dataset

res = Flow(
    get_csw_from_url(url=config.SOURCE_URL),
    update_resource('res_1', name='csw_resource'),

    # remove duplicates
    clean_duplicated_identifiers,

    # validate each dataset
    validate_datasets,

    # save each dataset as data package
    save_as_data_packages,
).results()

logger.info('Continue to next step with: python3 flow2.py')

# save results (data package and final datasets results)
dmp = json.dumps(res[0][0], indent=2)
f = open(config.get_download_result_path(), 'w')
f.write(dmp)
f.close()

pkg = res[1]  # package returned
pkg.save(config.get_data_package_result_path())
