"""
Get updated list of resources from a data.json file
"""

from functions import get_data_json_from_url, get_data_json_from_file
import json
import os
from harvester.logs import logger
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--url", type=str, help="URL of the data.json")
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)")
parser.add_argument("--force_download", action='store_true',
                                        help="Force download or just use local data.json prevously downloaded")
parser.add_argument("--request_timeout", type=int, default=90, help="Request data.json URL timeout")
args = parser.parse_args()

name = args.name  # Nice name of the source
url = args.url  # data.json final URL

base_data_folder = 'data'
local_folder = os.path.join(base_data_folder, args.name)
packages_folder_path = os.path.join(local_folder, 'datapackages')
if not os.path.isdir(packages_folder_path):
    os.makedirs(packages_folder_path)

data_json_path = os.path.join(local_folder, 'data.json')
data_json_errors_path = os.path.join(local_folder, 'data_json_errors.json')
duplicates_path = os.path.join(local_folder, 'duplicates.json')

# ----------------------------------------------------
# Get data.json if not here (or force)
# ----------------------------------------------------
if not os.path.isfile(data_json_path) or args.force_download:
    logger.info(f'Downloading {url}')
    datajson = get_data_json_from_url(url)
    datajson.save_data_json(data_json_path)
else:
    logger.info(f'Using data.json prevously downloaded: {data_json_path}')
    datajson = get_data_json_from_file(data_json_path=data_json_path)

datajson.save_validation_errors(path=data_json_errors_path)
total_datasets = len(datajson.datasets)
total_resources = datajson.count_resources()
logger.info('cleaning datasets')
duplicates = datajson.remove_duplicated_identifiers()
total_duplicates = len(duplicates)
datajson.save_duplicates(path=duplicates_path)

logger.info(f'Readed {total_datasets} datasets including {total_resources} resources. {total_duplicates} duplicated identifiers removed')

logger.info('creating datapackages')
datajson.save_datasets_as_data_packages(folder_path=packages_folder_path)

