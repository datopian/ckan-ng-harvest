import json
from logs import logger
import os
import requests
from libs.data_gov_api import CKANPortalAPI


def get_data_json_from_url(url):
    logger.info(f'Geting data.json from {url}')
    try:
        req = requests.get(url, timeout=90)
    except Exception as e:
        error = 'ERROR Donwloading data: {} [{}]'.format(self.url, e)
        logger.error(error)
        return None

    if req.status_code >= 400:
        error = '{} HTTP error: {}'.format(url, req.status_code)
        logger.error(error)
        return None

    logger.info(f'OK {url}')

    try:
        data_json = json.loads(req.content) 
    except Exception as e:
        error = 'ERROR parsing JSON data: {}'.format(e)
        logger.error(error)
        return None

    logger.info(f'VALID JSON')

    if not data_json.get('dataset', False):
        logger.error('No dataset key')
        return None

    ret = data_json['dataset']
    logger.info('{} datasets'.format(len(ret)))

    return ret

def clean_duplicated_identifiers(rows):
    logger.info('Cleaning duplicates')
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


def validate_headers():
    pass


def split_headers(package):
    pass

def log_package_info(package):
    logger.info('--------------------------------')
    logger.info('Package processor')

    logger.info(f'Package: {package}')
    resources = iter(package)
    for resource in resources:
        logger.info(f' - Resource: {resource}')

    
    logger.info('--------------------------------')

def dbg_packages(package):
    log_package_info(package)
    
    yield package.pkg
    yield from package
    


def get_actual_ckan_resources_from_api(harvest_source_id=None):
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