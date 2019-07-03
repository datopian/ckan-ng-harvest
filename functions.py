from urllib.request import urlopen
import json
from libs.data_json import JSONSchema, DataJSON
from libs.data_gov_api import CKANPortalAPI

import logging
logger = logging.getLogger(__name__)

c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('harvest.log')
logger.addHandler(c_handler)
logger.addHandler(f_handler)
logger.setLevel(logging.DEBUG)


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
    
    logger.debug('JSONSchema: {}'.format(json.dumps(datajson.schema.json_content, indent=4)))

    c = 0
    for dataset in datajson.datasets: 
        yield(dataset)

        c += 1  # just log some datasets
        if c < 10:
            logger.debug(' - Dataset: {}'.format(dataset['title']))

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



def get_actual_ckan_resources_from_api(harvest_source_id=None):
    logger.info('Extracting from harvest source id: {}'.format(harvest_source_id))
    cpa = CKANPortalAPI()
    resources = 0
    
    page = 0
    for packages in cpa.search_packages(harvest_source_id=harvest_source_id):
        # getting resources in pages of packages
        page += 1
        logger.info('PAGE {} from harvest source id: {}'.format(page, harvest_source_id))
        for package in packages:
            pkg_resources = len(package['resources'])
            resources += pkg_resources
            yield(package)

        logger.info('{} total resources'.format(resources))
        