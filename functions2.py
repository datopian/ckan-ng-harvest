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


def get_current_ckan_resources_from_api(harvest_source_id):
    results_json_path = config.get_ckan_results_cache_path()
    logger.info(f'Extracting from harvest source id: {harvest_source_id}')
    cpa = CKANPortalAPI()
    resources = 0

    page = 0
    for datasets in cpa.search_harvest_packages(harvest_source_id=harvest_source_id):
        # getting resources in pages of packages
        page += 1
        logger.info('PAGE {} from harvest source id: {}'.format(page, harvest_source_id))
        for dataset in datasets:
            pkg_resources = len(dataset['resources'])
            resources += pkg_resources
            yield(dataset)

            # we don't need to save this
            # save_dict_as_data_packages(data=package, path=config.get_data_packages_folder_path(),
            #                           prefix='ckan-result',
            #                           identifier_field='id')

    logger.info('{} total resources in harvest source id: {}'.format(resources, harvest_source_id))
    cpa.save_packages_list(path=results_json_path)


def compare_resources(rows):
    """ read the previous resource (CKAN API results)
        Yield any comparison result
        """

    logger.info(f'Rows from resource {rows.res.name}')

    data_packages_path = config.get_data_packages_folder_path()

    # Calculate minimum statistics
    total = 0

    no_extras = 0
    no_identifier_key_found = 0
    deleted = 0
    found = 0

    sample_row = None
    for row in rows:
        total += 1
        # logger.info(f'Row: {total}')
        # check for identifier
        ckan_id = row['id']
        extras = row.get('extras', False)
        if not extras:
            # TODO learn why.
            logger.error(f'No extras! dataset: {ckan_id}')
            result = {'action': 'error',
                        'ckan_id': ckan_id,
                        'reason': 'The CKAN dataset does not '
                                'have the "extras" property'}
            row.update({'comparison_results': result})
            yield row
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
            result = {'action': 'error',
                    'ckan_id': ckan_id,
                    'reason': 'The CKAN dataset does not have an "identifier"'}
            row.update({'comparison_results': result})
            yield row
            continue

        encoded_identifier = encode_identifier(identifier)
        expected_filename = f'data-json-{encoded_identifier}.json'
        expected_path = os.path.join(data_packages_path, expected_filename)

        if not os.path.isfile(expected_path):
            logger.info((f'Dataset: {ckan_id} not in DATA.JSON.'
                        f'It was deleted?: {expected_path}'))
            deleted += 1
            result = {'action': 'delete',
                      'ckan_id': ckan_id,
                      'reason': 'It no longer exists in the data.json source'}
            row.update({'comparison_results': result})
            yield row
            continue

        found += 1
        datajson_package = Package(expected_path)
        # logger.info(f'Dataset: {ckan_id}
        # found as data package at {expected_path}')

        # TODO analyze this: https://github.com/ckan/ckanext-harvest/blob/master/ckanext/harvest/harvesters/base.py#L229

        # compare dates
        # at data.json: "modified": "2019-06-27 12:41:27",
        # at ckan results: "metadata_modified": "2019-07-02T17:20:58.334748",

        data_json = datajson_package.get_resource('inline')
        data_json_data = data_json.source
        data_json_modified = parse(data_json_data['modified'])  # It's a naive date

        ckan_json = row
        ckan_json_modified = parse(ckan_json['metadata_modified'])

        diff_times = data_json_modified - ckan_json_modified
        seconds = diff_times.total_seconds()
        # logger.info(f'Seconds: {seconds} data.json:{data_json_modified} ckan:{ckan_json_modified})')

        # TODO analyze this since we have a Naive date we are not sure
        if abs(seconds) > 86400:  # more than a day
            warning = '' if seconds > 0 else 'Data.json is older than CKAN'
            result = {'action': 'update',
                      'ckan_id': ckan_id,
                      'new_data': data_json_data,
                      'reason': f'Changed: ~{seconds} seconds difference. {warning}'
                    }
        else:
            result = {'action': 'ignore',
                      'ckan_id': ckan_id,
                      'new_data': None,  # do not need this data_json_data
                      'reason': 'Changed: ~{seconds} seconds difference'}
        row.update({'comparison_results': result})
        yield row

        # if sample_row is None:
        #     sample_row = row

        # Delete the data.json file
        os.remove(expected_path)

    news = 0
    for name in glob.glob(f'{data_packages_path}/data-json-*.json'):
        news += 1
        package = Package(name)
        data_json = package.get_resource('inline')
        data_json_data = data_json.source

        result = {'action': 'add',
                  'ckan_id': None,
                  'new_data': data_json_data,
                  'reason': 'Not found in the CKAN results'}

        # there is no real row here

        # row = sample_row.update({'comparison_results': result})
        row = {'comparison_results': result}
        yield row

        # Delete the data.json file
        os.remove(name)

    stats = f"""Total processed: {total}.
                {no_extras} fail extras.
                {no_identifier_key_found} fail identifier key.
                {deleted} deleted.
                {found} datasets found,
                {news} new datasets."""

    logger.info(stats)