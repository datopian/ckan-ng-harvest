import glob
import json
import logging
import os
import pytz

from datapackage import Package, Resource
from dateutil.parser import parse
from harvesters.datajson.harvester import DataJSONDataset
from harvester_ng import helpers
from harvester_ng.logs import logger


logger = logging.getLogger(__name__)


def clean_duplicated_identifiers(rows):
    """ clean duplicated datasets identifiers on data.json source """

    logger.info('Cleaning duplicates')
    unique_identifiers = []
    c = 0
    for row in rows:
        c += 1
        idf = row['identifier']
        logger.info(f'Searching duplicates {c} {idf}')
        if idf not in unique_identifiers:
            unique_identifiers.append(idf)
            yield row
        else:
            row['is_duplicate'] = True
            logger.info(f'{idf} is duplicated')
            yield row


def validate_datasets(row):
    """ validate dataset row by row """
    data_validator = DataJSONDataset(row)
    valid = data_validator.validate(validator_schema=row['validator_schema'])
    errors = data_validator.errors
    row['validation_errors'] = errors
    if not valid:
        logger.error(f'Error validating {row}: {errors}')


def save_as_data_packages(path):
    """ save dataset from data.json as data package
        We will use this files as a queue to process later """

    logger.info(f'Saving as data packages at {path}')
    def f(row):
        package = Package()
        logger.debug(f'Resource {row}')
        # TODO check this, I'm learning datapackages.
        resource = Resource({'data': row})
        resource.infer()  # adds "name": "inline"
        if not resource.valid:
            raise Exception('Invalid resource')

        encoded_identifier = helpers.encode_identifier(identifier=row['identifier'])

        package.add_resource(descriptor=resource.descriptor)
        filename = f'data-json-{encoded_identifier}.json'
        package_path = os.path.join(path, filename)

        # no not rewrite if exists
        if not os.path.isfile(package_path):
            package.save(target=package_path)

    return f


def compare_resources_validate(row):
    """ validate a row while comparing resources.
        Check for extras and identifier.
    Returns: Boolean, Error """

    ckan_id = row['id']
    extras = row.get('extras', False)
    if not extras:
        error = f'The CKAN dataset {ckan_id} does not have the "extras" property'
        logging.error(error)
        return False, error
    
    identifier = None
    for extra in extras:
        if extra['key'] == 'identifier':
            identifier = extra['value']

    if identifier is None:
        error = f'The CKAN dataset {ckan_id} does not have an "identifier"'
        logging.error(error)
        return False, error

    return True, None


def compare_resources_resource_exists(data_packages_path, identifier):
    """ Check if a row is for detele
    Returns: Boolean, expected file path """
    encoded_identifier = helpers.encode_identifier(identifier)
    expected_filename = f'data-json-{encoded_identifier}.json'
    expected_path = os.path.join(data_packages_path, expected_filename)
    logger.debug(f'Expected path {expected_path}')

    file_exists = os.path.isfile(expected_path)
    if not file_exists:
        logger.info(f'Dataset: {identifier} not in DATA.JSON.')

    return file_exists, expected_path


def compare_resource_require_update(data_package_expected_path, row):
    """ Check if a row require update or it's ok to ignore 
    Returns: Boolean (if we need to update), new data readed from package """

    default_tzinfo_for_naives_dates = pytz.UTC
    expected_path = data_package_expected_path
    datajson_package = Package(expected_path)
            
    data_json = datajson_package.get_resource('inline')
    data_json_data = data_json.source
    data_json_modified = parse(data_json_data['modified'])  # It's a naive date
    
    ckan_json = row
    ckan_json_modified = parse(ckan_json['metadata_modified'])

    # un-naive datetimes
    if data_json_modified.tzinfo is None:
        data_json_modified = data_json_modified.replace(tzinfo=default_tzinfo_for_naives_dates)
    if ckan_json_modified.tzinfo is None:
        ckan_json_modified = ckan_json_modified.replace(tzinfo=default_tzinfo_for_naives_dates)

    logger.info(f'data_json_modified: {data_json_modified}. ckan_json_modified: {ckan_json_modified}')

    diff_times = data_json_modified - ckan_json_modified
    seconds = diff_times.total_seconds()

    os.remove(expected_path)

    # requires update if the data.json date is newer.
    # We ask for a day of differente because some dates are naives.
    require_update = seconds > 86400
    logger.info(f'Comparing times: {seconds}>86400 = {require_update}')
    return require_update, data_json_data


def compare_resource_get_new_datasets(data_packages_path):
    """ get new datesets,
    Yield this datasets """
    
    for name in glob.glob(f'{data_packages_path}/data-json-*.json'):
        package = Package(name)
        data_json = package.get_resource('inline')
        data_json_data = data_json.source

        yield data_json_data

        # Delete the data.json file
        os.remove(name)


def compare_resources(data_packages_path):
    """ read the previous resource (CKAN API results)
        and yield any comparison result with current rows
        """
    logger.info(f'Comparing resources at {data_packages_path}')

    def f(rows):

        # Calculate minimum statistics
        total = 0

        errors = []
        deleted = 0
        found_update = 0
        found_not_update = 0

        for row in rows:
            total += 1
            ckan_id = row['id']
            
            valid, error = compare_resources_validate(row)
            if not valid:
                errors.append(error)
                row['comparison_results'] = {'action': 'error', 'ckan_id': ckan_id, 'new_data': None, 'reason': error}
                yield row
                continue
            
            extras = row.get('extras', False)
            identifier = [extra['value'] for extra in extras if extra['key'] == 'identifier'][0]

            file_exists, expected_path = compare_resources_resource_exists(data_packages_path, identifier)
            if not file_exists:
                deleted += 1
                row['comparison_results'] = {
                        'action': 'delete', 
                        'ckan_id': ckan_id, 
                        'new_data': None, 
                        'reason': 'It no longer exists in the data.json source'
                        }
                logger.info(f'Mark for delete: ID {ckan_id}')
                yield row
                continue

            require_update, data_json_data = compare_resource_require_update(expected_path, row)
            if require_update:
                row['comparison_results'] = {
                        'action': 'update',
                        'ckan_id': ckan_id,
                        'new_data': data_json_data,
                        'reason': f'The resource is older'
                        }
                logger.info(f'Mark for update: ID {ckan_id}')
                found_update += 1
            else:
                row['comparison_results'] = {
                        'action': 'ignore',
                        'ckan_id': ckan_id,
                        'new_data': None,  # don't need this
                        'reason': 'The resource is updated'
                        }
                found_not_update += 1
                logger.info(f'Mark for ignore: ID {ckan_id}')
            yield row
            
        # detect new datasets
        news = 0
        for data_json_data in compare_resource_get_new_datasets(data_packages_path):
            total += 1
            news += 1            
            row = {
                'comparison_results': {
                    'action': 'create',
                    'ckan_id': None,
                    'new_data': data_json_data,
                    'reason': 'Not found in the CKAN results'}
                }

            yield row

        found = found_not_update + found_update

        total_errors = len(errors)
        stats = f"""Compare total processed: {total}.
                    {total_errors} errors.
                    {deleted} deleted.
                    {found} datasets found
                    ({found_update} needs update, {found_not_update} are the same).
                    {news} new datasets."""

        logger.info(stats)

    return f
