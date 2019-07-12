import json
from logs import logger
import os
import requests
from libs.data_gov_api import CKANPortalAPI
from libs.data_json import DataJSON
from datapackage import Package, Resource
from slugify import slugify
import config
import base64
from dateutil.parser import parse
import glob
from functions import encode_identifier


def get_current_ckan_resources_from_api(harvest_source_id, results_json_path):
    logger.info('Extracting from harvest source id: {}'.format(harvest_source_id))
    cpa = CKANPortalAPI()
    resources = 0

    page = 0
    for packages in cpa.search_harvest_packages(harvest_source_id=harvest_source_id):
        # getting resources in pages of packages
        page += 1
        logger.info('PAGE {} from harvest source id: {}'.format(page, harvest_source_id))
        for package in packages:
            pkg_resources = len(package['resources'])
            resources += pkg_resources
            yield(package)

            # we don't need to save this
            # save_dict_as_data_packages(data=package, path=config.get_data_packages_folder_path(),
            #                           prefix='ckan-result',
            #                           identifier_field='id')

    logger.info('{} total resources in harvest source id: {}'.format(resources, harvest_source_id))
    cpa.save_packages_list(path=results_json_path)


def compare_resources(data_packages_path):
    # get both resources and compare them using their identifiers.

    def rows_processor(rows):
        # Calculate minimum statistics
        total = 0

        no_extras = 0
        no_identifier_key_found = 0
        deleted = 0
        finded = 0

        results = []

        for row in rows:
            yield(row)  # all row passes

            total += 1
            # check for identifier
            ckan_id = row['id']
            extras = row.get('extras', False)
            if not extras:
                # TODO learn why.
                logger.error(f'No extras! dataset: {ckan_id}')
                no_extras += 1
                continue

            identifier = None
            for extra in extras:
                if extra['key'] == 'identifier':
                    identifier = extra['value']

            if identifier is None:
                logger.error('No identifier '
                             '(extras[].key.identifier not exists). '
                             'Dataset.id: {}'.format(ckan_id))
                no_identifier_key_found += 1
                continue

            encoded_identifier = encode_identifier(identifier)
            expected_filename = f'data-json_{encoded_identifier}.json'
            expected_path = os.path.join(data_packages_path, expected_filename)

            if not os.path.isfile(expected_path):
                logger.info((f'Dataset: {ckan_id} not in DATA.JSON.'
                             f'It was deleted?: {expected_path}'))
                deleted += 1
                task = {'action': 'delete',
                        'ckan_id': ckan_id,
                        'reason': 'It no longer exists in the data.json source'}
                results.append(task)
                continue

            finded += 1
            package = Package(expected_path)
            # logger.info(f'Dataset: {ckan_id}
            # Finded as data package at {expected_path}')

            # TODO analyze this: https://github.com/ckan/ckanext-harvest/blob/master/ckanext/harvest/harvesters/base.py#L229

            # compare dates
            # at data.json: "modified": "2019-06-27 12:41:27",
            # at ckan results: "metadata_modified": "2019-07-02T17:20:58.334748",

            data_json = package.get_resource('inline')
            data_json_data = data_json.source
            data_json_modified = parse(data_json_data['modified'])  # It's a naive date

            ckan_json = row
            ckan_json_modified = parse(ckan_json['metadata_modified'])

            diff_times = data_json_modified - ckan_json_modified
            seconds = diff_times.total_seconds()
            logger.info(f'Seconds: {seconds} data.json:{data_json_modified} ckan:{ckan_json_modified})')

            # TODO analyze this since we have a Naive data we are not sure
            if abs(seconds) > 86400:  # more than a day
                warning = None if seconds > 0 else 'Data.json is older than CKAN'
                task = {'action': 'update',
                        'ckan_id': ckan_id,
                        'new_data': data_json_data,
                        'reason': 'Changed: ~{seconds} seconds difference',
                        'warnings': [warning]}

                results.append(task)
            else:
                task = {'action': 'ignore',
                        'ckan_id': ckan_id,
                        'new_data': None,  # do not need this data_json_data,
                        'reason': 'Changed: ~{seconds} seconds difference'}
                results.append(task)

            # Delete the data.json files
            os.remove(expected_path)

        stats = f"""Total processed: {total}.
                    {no_extras} fail extras.
                    {no_identifier_key_found} fail identifier key.
                    {deleted} deleted.
                    {finded} datasets finded."""

        logger.info(stats)

    return rows_processor


# TODO add the missing json files as new datasets
def add_new_resources(data_packages_path):
    results = []
    # Get all missing (new) data.json datasets
    for name in glob.glob(f'{data_packages_path}/data-json_*.json'):
        print(f'New resource {name}')

        task = {'action': 'add',
                'ckan_id': None,
                'new_data': data_json_data,
                'reason': 'Changed: ~{seconds} difference'}

        results.append(task)