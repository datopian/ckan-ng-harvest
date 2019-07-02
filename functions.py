from urllib.request import urlopen
import json
from libs.data_json import JSONSchema, DataJSON
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

def list_parents(row):
    # get a list of datasets with "isPartOf" and his childs.
    # https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L145
    if row.get('isPartOf', None):
        dh.parent_identifiers.append(row['isPartOf'])
        dh.child_identifiers.append(row['identifier'])
        logger.debug('{} is part of {}'.fortmat(row['identifier'], row['isPartOf']))


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