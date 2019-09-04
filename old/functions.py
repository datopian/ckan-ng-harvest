from urllib.request import urlopen
import json
from harvester.data_json import JSONSchema, DataJSON
from harvester.data_gov_api import CKANPortalAPI
from harvester.logs import logger
import os


def get_data_json_from_url(url):
    datajson = DataJSON()
    datajson.url = url

    ret, info = datajson.download_data_json(timeout=90)
    if not ret:
        error = 'Error getting data: {}'.format(info)
        logger.error(error)
        raise Exception(error)
    logger.info('Downloaded OK')

    ret, info = datajson.load_data_json()
    if not ret:
        error = 'Error loading JSON data: {}'.format(info)
        logger.error(error)
        raise Exception(error)

    logger.info('JSON OK')
    ret, info = datajson.validate_json()
    if not ret:
        logger.error('Error validating data: {}\n----------------\n'.format(info))
        # continue  # USE invalid too
        logger.info('Validate FAILED: {} datasets'.format(len(datajson.datasets)))
    else:
        logger.info('Validate OK: {} datasets'.format(len(datajson.datasets)))

    # logger.debug('JSONSchema: {}'.format(json.dumps(datajson.schema.json_content, indent=4)))
    return datajson

def get_data_json_from_file(data_json_path):
    datajson = DataJSON()

    ret, info = datajson.read_local_data_json(data_json_path=data_json_path)

    ret, info = datajson.load_data_json()
    if not ret:
        error = 'Error loading JSON data: {}'.format(info)
        logger.error(error)
        raise Exception(error)

    logger.info('JSON OK')
    ret, errors = datajson.validate_json()
    if not ret:
        total_errors = len(errors)
        logger.error('{} Errors validating data'.format(total_errors))
        error = errors[0]
        if len(error) > 70: # too long and vervose errors
            error = error[:70]
        logger.error('Error 1/{} validating data:\n\t{}'.format(total_errors, error))
        # continue  # USE invalid too
        logger.info('Validate FAILED: {} datasets'.format(len(datajson.datasets)))
    else:
        logger.info('Validate OK: {} datasets'.format(len(datajson.datasets)))

    # logger.debug('JSONSchema: {}'.format(json.dumps(datajson.schema.json_content, indent=4)))

    return datajson

def validate_headers():
    pass

def list_parents_and_childs(package):
    # get a list of datasets with "isPartOf" and his childs.
    # https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L145

    package.pkg.add_resource('parent_identifiers')
    yield package.pkg

    package.pkg.add_resource('child_identifiers')
    yield package.pkg

    resources = iter(package)
    # learn how to do this
    """
    if row.get('isPartOf', None):
        parent_identifiers.append(row['isPartOf'])
        child_identifiers.append(row['identifier'])
        logger.debug('{} is part of {}'.fortmat(row['identifier'], row['isPartOf']))
    """
    yield from resources


def clean_duplicated_identifiers(rows):
    unique_identifiers = []
    duplicates = []
    processed = 0
    for row in rows:
        if row['identifier'] not in unique_identifiers:
            unique_identifiers.append(row['identifier'])
            yield(row)
            processed += 1
        else:
            duplicates.append(row['identifier'])
            logger.error('Duplicated {}'.format(row['identifier']))
    logger.info('{} duplicates deleted. {} OK'.format(len(duplicates), processed))



def get_current_ckan_resources_from_api(harvest_source_id=None):
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

        logger.info('{} total resources'.format(resources))
