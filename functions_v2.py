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


def get_data_json_from_url(url, name, data_json_path):
    logger.info(f'Geting data.json from {url}')
    try:
        req = requests.get(url, timeout=90)
    except Exception as e:
        error = 'ERROR Donwloading data: {} [{}]'.format(url, e)
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

    logger.info('{} datasets finded'.format(len(data_json['dataset'])))

    dmp = json.dumps(data_json, indent=2)
    f = open(data_json_path, 'w')
    f.write(dmp)
    f.close()

    # the real dataset list
    for dataset in data_json['dataset']:
        yield(dataset)

    # is this better in this case?
    # return data_json['dataset']


def clean_duplicated_identifiers(rows):
    logger.info('Cleaning duplicates')
    unique_identifiers = []
    duplicates = []
    processed = 0
    resource = rows.res
    logger.error('Rows from resource_ {}'.format(resource.name))
    
    for row in rows:
        if row['identifier'] not in unique_identifiers:
            unique_identifiers.append(row['identifier'])
            yield(row)
            processed += 1
            # save as data package
            save_dict_as_data_packages(data=row, path=config.get_data_packages_folder_path(), 
                                        prefix='data-json', identifier_field='identifier')
        else:
            duplicates.append(row['identifier'])
            # do not log all duplicates. Sometimes they are too many.
            if len(duplicates) < 10:
                logger.error('Duplicated {}'.format(row['identifier']))
            elif len(duplicates) == 10:
                logger.error('... more duplicates not shown')
    logger.info('{} duplicates deleted. {} OK'.format(len(duplicates), processed))


def log_package_info(package):
    logger.info('--------------------------------')
    logger.info('Package processor')

    logger.info(f'Package: {package}')
    resources = package.pkg.descriptor['resources']
    for resource in resources:
        # nice_resource = json.dumps(resource, indent=4)
        # short vesion
        nice_resource = {'name': resource['name'],
                            'path': resource['path'],
                            'profile': resource['profile'],
                            # 'fields': resource['schema']['fields'][0]
                            }
        logger.info(f' - Resource: {nice_resource}')

    
    logger.info('--------------------------------')

def dbg_packages(package):
    log_package_info(package)

    yield package.pkg
    yield from package
    

def get_actual_ckan_resources_from_api(harvest_source_id, results_json_path):
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
            save_dict_as_data_packages(data=package, path=config.get_data_packages_folder_path(), 
                                        prefix='ckan-result', identifier_field='id')

    logger.info('{} total resources in harvest source id: {}'.format(resources, harvest_source_id))
    cpa.save_packages_list(path=results_json_path)


def save_dict_as_data_packages(data, path, prefix, identifier_field):
    """ save dict resource as data package """
    # logger.info(f'Saving DP at folder {path}. Identifier: {identifier_field}. DATA: {data}')
    package = Package()
    
    #TODO check this, I'm learning datapackages
    resource = Resource({'data': data})
    resource.infer()  #adds "name": "inline"
    if not resource.valid:
        raise Exception('Invalid resource')

    identifier = data[identifier_field]
    bytes_identifier = identifier.encode('utf-8')
    encoded = base64.b64encode(bytes_identifier)
    encoded_identifier = str(encoded, "utf-8")

    # resource_path = os.path.join(path, f'{prefix}_{encoded_identifier}.json')
    # resource.save(resource_path) 

    package.add_resource(descriptor=resource.descriptor)
    package_path = os.path.join(path, f'{prefix}_{encoded_identifier}.json')

    # no not rewrite if exists
    if not os.path.isfile(package_path):
        package.save(target=package_path)
