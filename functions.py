import json
from logs import logger
import os
import requests
from libs.data_gov_api import CKANPortalAPI
from libs.data_json import DataJSON
from libs.data_json import DataJSONDataset
from datapackage import Package, Resource
from functions3 import build_validation_error_email
from slugify import slugify
import config
import base64
from dateutil.parser import parse
import glob


def validate_data_json(row):
    # Taken from https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/datajsonvalidator.py
    errors = []
    try:
        data_validator = DataJSONDataset()
        errors = data_validator.validate_dataset(row)
    except Exception as e:
        errors.append(("Internal Error", ["Something bad happened: " + str(e)]))
    return errors


def get_data_json_from_url(url):
    logger.info(f'Geting data.json from {url}')

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
        datajson.validation_errors = 'Error loading JSON data: {}'.format(info)
        datajson.save_validation_errors(path=config.get_datajson_validation_errors_path())
        logger.error(datajson.validation_errors)
        try:
            build_validation_error_email()
        except Exception as e:
            logger.error('Error sending validation email: {}'.format(e))
        raise Exception(datajson.validation_errors)

    logger.info('JSON OK')
    ret, info = datajson.validate_json()
    if not ret:
        logger.error('Error validating data: {}\n----------------\n'.format(info))
        # continue  # USE invalid too
        logger.info('Validate FAILED: {} datasets'.format(len(datajson.datasets)))
    else:
        logger.info('Validate OK: {} datasets'.format(len(datajson.datasets)))

    # TODO move this as a DataJson function and add it to a validate function validate_data_json(data_json['dataset'])

    logger.info('VALID JSON, {} datasets found'.format(len(datajson.datasets)))

    # save data.json
    datajson.save_data_json(path=config.get_datajson_cache_path())
    # save headers errors
    datajson.save_validation_errors(path=config.get_datajson_headers_validation_errors_path())

    # the real dataset list

    if config.LIMIT_DATASETS > 0:
        datajson.datasets = datajson.datasets[:config.LIMIT_DATASETS]
    for dataset in datajson.datasets:
        # add headers (previously called catalog_values)
        dataset['headers'] = datajson.headers
        yield(dataset)


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
    """ validate dataset row by row """
    errors = validate_data_json(row)
    row['validation_errors'] = errors

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