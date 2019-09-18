from harvester.logs import logger
import os
import requests
from harvester.data_gov_api import CKANPortalAPI
from harvester.csw import CSWSource
from datapackage import Package, Resource
from slugify import slugify
from harvester import config
import base64
from dateutil.parser import parse
import glob
from functions import encode_identifier
import pytz


def get_current_ckan_resources_from_api(harvest_source_id):
    results_json_path = config.get_ckan_results_cache_path()
    logger.info(f'Extracting from harvest source id: {harvest_source_id}')
    cpa = CKANPortalAPI(base_url=config.CKAN_CATALOG_URL)
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

    res_name = rows.res.name if hasattr(rows, 'res') else 'Fake res testing'
    logger.info(f'Rows from resource {res_name}')

    data_packages_path = config.get_data_packages_folder_path()
    default_tzinfo_for_naives_dates = pytz.UTC

    # Calculate minimum statistics
    total = 0

    no_extras = 0
    no_identifier_key_found = 0
    deleted = 0
    found_update = 0
    found_not_update = 0

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
                      'new_data': None,
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
                      'new_data': None,
                      'reason': 'The CKAN dataset does not have an "identifier"'}
            row.update({'comparison_results': result})
            yield row
            continue

        # was parent in the previous harvest
        # if extras.get('collection_metadata', None) is not None:

        encoded_identifier = encode_identifier(identifier)
        expected_filename = f'csw-{encoded_identifier}.json'
        expected_path = os.path.join(data_packages_path, expected_filename)

        if not os.path.isfile(expected_path):
            logger.info((f'Dataset: {ckan_id} not in CSW Source.'
                        f'It was deleted?: {expected_path}'))
            deleted += 1
            result = {'action': 'delete',
                      'ckan_id': ckan_id,
                      'new_data': None,
                      'reason': 'It no longer exists in the CSW source'}
            row.update({'comparison_results': result})
            yield row
            continue

        # the file (and the identifier) exists

        csw_package = Package(expected_path)
        csw_json = csw_package.get_resource('inline')
        csw_json_data = csw_json.source

        result = {'action': 'update',
                    'ckan_id': ckan_id,
                    'new_data': csw_json_data,
                    'reason': 'Update by default'
                }
        found_update += 1
        row.update({'comparison_results': result})
        yield row

        # remove so next step not detect it as new
        os.remove(expected_path)

    news = 0
    for name in glob.glob(f'{data_packages_path}/csw-*.json'):
        news += 1
        package = Package(name)
        csw_json = package.get_resource('inline')
        csw_json_data = csw_json.source

        result = {'action': 'create',
                  'ckan_id': None,
                  'new_data': csw_json_data,
                  'reason': 'Not found in the CKAN results'}

        # there is no real row here

        # row = sample_row.update({'comparison_results': result})
        row = {'comparison_results': result}
        yield row

        # Delete the csw.json file
        os.remove(name)

    found = found_not_update + found_update

    stats = f"""Total processed: {total}.
                {no_extras} fail extras.
                {no_identifier_key_found} fail identifier key.
                {deleted} deleted.
                {found} datasets found ({found_update} needs update, {found_not_update} are the same),
                {news} new datasets."""

    logger.info(stats)
