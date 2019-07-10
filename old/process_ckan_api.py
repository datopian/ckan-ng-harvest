"""
Get the actual list of resources in data.gov
"""
from libs.data_gov_api import CKANPortalAPI
from functions import get_current_ckan_resources_from_api
import json
import os
from logs import logger
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--ckan_base_url", type=str, default='https://catalog.data.gov', help="URL of the data.json")
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)")
parser.add_argument("--harvest_source_id", type=str, help="Source ID for filter CKAN API")
parser.add_argument("--force_download", action='store_true',
                                        help="Force download or just use local data.json prevously downloaded")
parser.add_argument("--request_timeout", type=int, default=90, help="Request data.json URL timeout")
args = parser.parse_args()

name = args.name  # Nice name of the source

base_data_folder = 'data'
local_folder = os.path.join(base_data_folder, args.name)
packages_folder_path = os.path.join(local_folder, 'datapackages')
if not os.path.isdir(packages_folder_path):
    os.makedirs(packages_folder_path)

api_results_path = os.path.join(local_folder, 'api_results.json')
# api_errors_path = os.path.join(local_folder, 'api_errors.json')
# duplicates_path = os.path.join(local_folder, 'api_duplicates.json')

# ----------------------------------------------------
# Get data.json if not here (or force)
# ----------------------------------------------------
if not os.path.isfile(api_results_path) or args.force_download:
    logger.info('Downloading')
    cpa = CKANPortalAPI(base_url=args.ckan_base_url)
    cpa.get_all_packages(harvest_source_id=args.harvest_source_id)
    cpa.save_packages_list(path=api_results_path)
else:
    logger.info(f'Using data.json prevously downloaded: {api_results_path}')
    cpa = CKANPortalAPI()
    cpa.read_local_packages(path=api_results_path)

packages = cpa.package_list
total_datasets = len(packages)
total_resources = cpa.count_resources()

logger.info('cleaning datasets')
duplicates = cpa.remove_duplicated_identifiers()
total_duplicates = len(duplicates)

logger.info(f'Readed {total_datasets} datasets including {total_resources} resources. {total_duplicates} duplicated identifiers removed')

logger.info('creating datapackages')
cpa.save_datasets_as_data_packages(folder_path=packages_folder_path)
