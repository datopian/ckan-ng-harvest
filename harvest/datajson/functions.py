import os
import json
import base64
from harvesters.logs import logger
from harvester_adapters.ckan.api import CKANPortalAPI
from harvesters.datajson.harvester import DataJSON
from harvesters.datajson.harvester import DataJSONDataset
from datapackage import Package, Resource
from functions3 import build_validation_error_email
from harvester import config


def validate_data_json(row):
    # Taken from https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/datajsonvalidator.py
    errors = []
    try:
        data_validator = DataJSONDataset()
        errors = data_validator.validate_dataset(row)
    except Exception as e:
        # errors.append(("Internal Error", ["Something bad happened: " + str(e)]))
        errors.append({'Internal Error': [f'Something bad happened: {e}']})
    return errors


def get_data_json_from_url(url):
    logger.info(f'Geting data.json from {url}')

    datajson = DataJSON()
    datajson.url = url

    try:
        datajson.fetch(timeout=90)
        ret = True
    except Exception as e:
        ret = False
    if not ret:
        error = 'Error getting data: {}'.format(datajson.errors)
        datajson.save_errors(path=config.get_errors_path())
        logger.error(error)
        raise Exception(error)
    logger.info('Downloaded OK')

    ret = datajson.validate()
    if not ret:
        error = 'Error validating data: {}'.format(datajson.errors)
        logger.error(error)
        raise Exception(error)
    else:
        datajson.post_fetch()
        logger.info('Validate OK: {} datasets'.format(len(datajson.datasets)))

    logger.info('{} datasets found'.format(len(datajson.datasets)))

    # save data.json
    datajson.save_json(path=config.get_data_cache_path())
    # save headers errors
    datajson.save_errors(path=config.get_errors_path())

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
            processed += 1
            unique_identifiers.append(row['identifier'])
            logger.info('Dataset {} not duplicated: {}'.format(processed, row['identifier']))
            yield(row)
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