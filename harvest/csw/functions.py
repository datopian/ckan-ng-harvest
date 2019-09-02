# use always base project folder as base path for imports
# move libs to a python package to fix this
import sys
from pathlib import Path
FULL_BASE_PROJECT_PATH = str(Path().parent.parent.parent.absolute())
print(FULL_BASE_PROJECT_PATH)
sys.path.append(FULL_BASE_PROJECT_PATH)

import json
from harvester.logs import logger
import os
import requests
from harvester.csw import CSWSource
from datapackage import Package, Resource
from slugify import slugify
from harvester import config
import base64


def get_csw_from_url(url):
    logger.info(f'Geting CSW from {url}')

    csw = CSWSource(url=url)
    if not csw.connect_csw():
        error = f'Fail to connect {csw.errors}'
        raise Exception(error)

    c = 0
    for record in csw.get_records(esn='full'):
        idf = record.get('identifier', None)
        if idf is None:
            continue

        if config.LIMIT_DATASETS > 0 and c > config.LIMIT_DATASETS:
            break
        logger.info(f'Found {idf} at {csw.get_cleaned_url()}')
        yield(record)
        c += 1


def clean_duplicated_identifiers(rows):
    """ clean duplicated datasets identifiers on data.json source """

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
        else:
            duplicates.append(row['identifier'])
            row['is_duplicate'] = 'True'
            yield(row)
            # do not log all duplicates. Sometimes they are too many.
            if len(duplicates) < 10:
                logger.error('Duplicated {}'.format(row['identifier']))
            elif len(duplicates) == 10:
                logger.error('... more duplicates not shown')
    logger.info('{} duplicates deleted. {} OK'.format(len(duplicates), processed))


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


def save_as_data_packages(row):
    """ save dataset from data.json as data package
        We will use this files as a queue to process later """
    # TODO check if ckanext-datapackager is useful for import
    # or export resources:
    # https://github.com/frictionlessdata/ckanext-datapackager

    package = Package()

    # TODO check this, I'm learning datapackages.
    resource = Resource({'data': row})
    resource.infer()  # adds "name": "inline"
    if not resource.valid:
        raise Exception('Invalid resource')

    encoded_identifier = encode_identifier(identifier=row['identifier'])

    # resource_path = os.path.join(path, f'{prefix}_{encoded_identifier}.json')
    # resource.save(resource_path)

    package.add_resource(descriptor=resource.descriptor)
    folder = config.get_data_packages_folder_path()
    filename = f'data-json-{encoded_identifier}.json'
    package_path = os.path.join(folder, filename)

    # no not rewrite if exists
    if not os.path.isfile(package_path):
        package.save(target=package_path)