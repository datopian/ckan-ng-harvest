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


def get_data_json_from_url(url, name, data_json_path):
    logger.info(f'Geting data.json from {url}')
    try:
        req = requests.get(url, timeout=90)
    except Exception as e:
        error = 'ERROR Downloading data: {} [{}]'.format(url, e)
        logger.error(error)
        raise

    if req.status_code >= 400:
        error = '{} HTTP error: {}'.format(url, req.status_code)
        logger.error(error)
        raise Exception('Http Error')

    logger.info(f'OK {url}')

    try:
        data_json = json.loads(req.content)
    except Exception as e:
        error = 'ERROR parsing JSON data: {}'.format(e)
        logger.error(error)
        raise

    # TODO validate with jsonschema as in lib/data_json.py
    # TODO check and re-use a ckanext-datajson validator:
    # https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/datajsonvalidator.py

    # TODO check how ckanext-datajson uses jsonschema.
    #   One example (there are more)
    #   https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L368

    logger.info(f'VALID JSON')

    if not data_json.get('dataset', False):
        logger.error('No dataset key')
        raise Exception('Valid but invalid JSON')

    logger.info('{} datasets found'.format(len(data_json['dataset'])))

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
    """ clean duplicated identifiers on data.json source
        and save as datapackages the unique ones """

    logger.info('Cleaning duplicates')
    unique_identifiers = []
    duplicates = []
    processed = 0
    # resource = rows.res
    # logger.error('Rows from resource {}'.format(resource.name))

    for row in rows:
        if row['identifier'] not in unique_identifiers:
            unique_identifiers.append(row['identifier'])
            yield(row)
            processed += 1
            # save as data package
            save_dict_as_data_packages(data=row,
                                        path=config.get_data_packages_folder_path(),
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

        # some times there are no fields
        fields = [] if len(resource['schema']['fields']) == 0 else resource['schema']['fields'][0]
        nice_resource = {'name': resource['name'],
                            'path': resource['path'],
                            'profile': resource['profile'],
                            'fields': fields
                            }
        logger.info(f' - Resource: {nice_resource}')


    logger.info('--------------------------------')


def dbg_packages(package):
    log_package_info(package)

    yield package.pkg
    yield from package


# we need a way to save as file using an unique identifier
# TODO check if base64 is the best idea
def encode_identifier(identifier):
    bytes_identifier = identifier.encode('utf-8')
    encoded = base64.b64encode(bytes_identifier)
    encoded_identifier = str(encoded, 'utf-8')

    return encoded_identifier


def decode_identifier(encoded_identifier):
    decoded_bytes = base64.b64decode(encoded_identifier)
    decoded_str = str(decoded_bytes, 'utf-8')

    return decoded_str


def save_dict_as_data_packages(data, path, prefix, identifier_field):
    """ save dict resource as data package """
    # TODO check if ckanext-datapackager is useful for import
    # or export resources:
    # https://github.com/frictionlessdata/ckanext-datapackager

    package = Package()

    # TODO check this, I'm learning datapackages.
    resource = Resource({'data': data})
    resource.infer()  # adds "name": "inline"
    if not resource.valid:
        raise Exception('Invalid resource')

    encoded_identifier = encode_identifier(identifier=data[identifier_field])

    # resource_path = os.path.join(path, f'{prefix}_{encoded_identifier}.json')
    # resource.save(resource_path)

    package.add_resource(descriptor=resource.descriptor)
    package_path = os.path.join(path, f'{prefix}_{encoded_identifier}.json')

    # no not rewrite if exists
    if not os.path.isfile(package_path):
        package.save(target=package_path)