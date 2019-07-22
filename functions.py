import json
from logs import logger
import os
import requests
from libs.data_gov_api import CKANPortalAPI
from libs.data_json import DataJSON
from libs.data_json import DataJSONDataset
from datapackage import Package, Resource
from slugify import slugify
import config
import base64
from dateutil.parser import parse
import glob


def validate_data_json(data_json):
    # Taken from https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/datajsonvalidator.py
    # TODO send these errors somewhere
    errors = []
    try:
        data_validator = DataJSONDataset()
        data_validator.validate_dataset(data_json, errors)
    except Exception as e:
        errors.append(("Internal Error", ["Something bad happened: " + str(e)]))
    filename = f'data-json-errors.log'
    if os.path.exists(filename):
        append_write = 'a'
    else:
        append_write = 'w'
    file = open(filename,append_write)
    file.write("Dataset {}:".format(str(data_json['identifier'])))
    for e in errors:
        file.write(str(e))
    file.close()
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

    # TODO move this as a DataJson function and add it to a validate function validate_data_json(data_json['dataset'])

    logger.info('VALID JSON, {} datasets found'.format(len(datajson.datasets)))

    # save data.json
    datajson.save_data_json(path=config.get_datajson_cache_path())
    # save headers errors
    datajson.save_validation_errors(path=config.get_datajson_headers_validation_errors_path())

    # the real dataset list
    for dataset in datajson.datasets:
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
            # do not log all duplicates. Sometimes they are too many.
            if len(duplicates) < 10:
                logger.error('Duplicated {}'.format(row['identifier']))
            elif len(duplicates) == 10:
                logger.error('... more duplicates not shown')
    logger.info('{} duplicates deleted. {} OK'.format(len(duplicates), processed))


def validate_datasets(row):
    """ validate dataset row by row """
    # just validate this row dictionary and append a line (if error)
    # in a CSV file defined in config.py
    # do not need to yield anything, a row processor just modify a row
    # example dataflows row processor: https://github.com/datahq/dataflows/blob/master/TUTORIAL.md#learn-how-to-write-your-own-processing-flows
    # if you need to delete some dataset (on big errors) convert this in a "rows" processor (as clean_duplicates)
    validate_data_json(row)
    row = row


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