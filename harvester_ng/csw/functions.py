# flake8: noqa
import json
import os

import requests
from datapackage import Package, Resource
from slugify import slugify

from harvester_ng.helpers import encode_identifier
from harvesters import config
from harvesters.csw.harvester import CSWSource
from harvesters.logs import logger


def get_csw_from_url(url):
    logger.info(f'Geting CSW from {url}')

    csw = CSWSource(url=url)
    try:
        csw.fetch()
        connected = True
    except Exception as e:
        connected = False
    if not connected:
        error = f'Fail to connect {csw.errors}'
        csw.save_errors(path=config.get_errors_path())
        raise Exception(error)

    c = 0
    for record in csw.get_records(esn='full'):
        idf = record.get('identifier', None)
        if idf is None:
            continue

        logger.info(f'{c} Found {idf} at {csw.get_cleaned_url()}')
        # get XML info
        yield(record)

        if config.LIMIT_DATASETS > 0 and c > config.LIMIT_DATASETS:
            break
        c += 1
    csw.save_json(path=config.get_data_cache_path())


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


def validate_datasets(row):
    """ validate one dataset """
    # TODO check what to validate in this resources
    row['validation_errors'] = []


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

    package.add_resource(descriptor=resource.descriptor)
    folder = config.get_data_packages_folder_path()
    filename = f'csw-{encoded_identifier}.json'
    package_path = os.path.join(folder, filename)

    # no not rewrite if exists
    if not os.path.isfile(package_path):
        package.save(target=package_path)
