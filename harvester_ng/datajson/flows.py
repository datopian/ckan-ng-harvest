import glob
import json
import os
import pytz

from datapackage import Package, Resource
from dateutil.parser import parse
from harvesters.datajson.harvester import DataJSONDataset
from harvester_ng import helpers
from harvester_ng.logs import logger


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
    data_validator = DataJSONDataset(row)
    data_validator.validate(validator_schema=row['validator_schema'])
    row['validation_errors'] = data_validator.errors


def save_as_data_packages(path):
    """ save dataset from data.json as data package
        We will use this files as a queue to process later """

    def f(row):
        package = Package()

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

def compare_resources(data_packages_path):
    """ read the previous resource (CKAN API results)
        Yield any comparison result
        """

    def f(rows):
        res_name = rows.res.name if hasattr(rows, 'res') else 'Fake res testing'
        logger.info(f'Rows from resource {res_name}')

        default_tzinfo_for_naives_dates = pytz.UTC

        # Calculate minimum statistics
        total = 0

        no_extras = 0
        no_identifier_key_found = 0
        deleted = 0
        found_update = 0
        found_not_update = 0

        for row in rows:
            total += 1
            # logger.info(f'Row: {total}')
            # check for identifier
            ckan_id = row['id']
            extras = row.get('extras', False)
            if not extras:
                # TODO learn why.
                logger.error(f'No extras! dataset: {ckan_id}')
                result = {'action': 'error',
                        'ckan_id': ckan_id,
                        'new_data': None,
                        'reason': 'The CKAN dataset does not '
                                    'have the "extras" property'}
                row.update({'comparison_results': result})
                yield row
                no_extras += 1
                continue

            identifier = None
            for extra in extras:
                if extra['key'] == 'identifier':
                    identifier = extra['value']

            if identifier is None:
                logger.error('No identifier '
                            '(extras[].key.identifier not exists). '
                            'Dataset.id: {}'.format(ckan_id))

                no_identifier_key_found += 1
                result = {'action': 'error',
                        'ckan_id': ckan_id,
                        'new_data': None,
                        'reason': 'The CKAN dataset does not have an "identifier"'}
                row.update({'comparison_results': result})
                yield row
                continue

            # was parent in the previous harvest
            # if extras.get('collection_metadata', None) is not None:

            encoded_identifier = helpers.encode_identifier(identifier)
            expected_filename = f'data-json-{encoded_identifier}.json'
            expected_path = os.path.join(data_packages_path, expected_filename)

            if not os.path.isfile(expected_path):
                logger.info((f'Dataset: {ckan_id} not in DATA.JSON.'
                            f'It was deleted?: {expected_path}'))
                deleted += 1
                result = {'action': 'delete',
                        'ckan_id': ckan_id,
                        'new_data': None,
                        'reason': 'It no longer exists in the data.json source'}
                row.update({'comparison_results': result})
                yield row
                continue

            datajson_package = Package(expected_path)
            # logger.info(f'Dataset: {ckan_id}
            # found as data package at {expected_path}')

            # TODO analyze this: https://github.com/ckan/ckanext-harvest/blob/master/ckanext/harvest/harvesters/base.py#L229

            # compare dates
            # at data.json: "modified": "2019-06-27 12:41:27",
            # at ckan results: "metadata_modified": "2019-07-02T17:20:58.334748",

            data_json = datajson_package.get_resource('inline')
            data_json_data = data_json.source
            data_json_modified = parse(data_json_data['modified'])  # It's a naive date

            ckan_json = row
            ckan_json_modified = parse(ckan_json['metadata_modified'])

            # un-naive datetimes
            if data_json_modified.tzinfo is None:
                data_json_modified = data_json_modified.replace(tzinfo=default_tzinfo_for_naives_dates)
                # logger.warning('Modified date in data.json is naive: {}'.format(data_json_data['modified']))
            if ckan_json_modified.tzinfo is None:
                ckan_json_modified = ckan_json_modified.replace(tzinfo=default_tzinfo_for_naives_dates)
                # logger.warning('Modified date in CKAN results is naive: {}'.format(ckan_json['metadata_modified']))

            diff_times = data_json_modified - ckan_json_modified

            seconds = diff_times.total_seconds()
            # logger.info(f'Seconds: {seconds} data.json:{data_json_modified} ckan:{ckan_json_modified})')

            # TODO analyze this since we have a Naive date we are not sure
            if abs(seconds) > 86400:  # more than a day
                warning = '' if seconds > 0 else 'Data.json is older than CKAN'
                result = {'action': 'update',
                        'ckan_id': ckan_id,
                        'new_data': data_json_data,
                        'reason': f'Changed: ~{seconds} seconds difference. {warning}'
                        }
                found_update += 1
            else:
                result = {'action': 'ignore',
                        'ckan_id': ckan_id,
                        'new_data': None,  # do not need this data_json_data
                        'reason': 'Changed: ~{seconds} seconds difference'}
                found_not_update += 1
            row.update({'comparison_results': result})
            yield row

            
            # Delete the data.json file
            os.remove(expected_path)

        news = 0
        for name in glob.glob(f'{data_packages_path}/data-json-*.json'):
            total += 1
            news += 1
            package = Package(name)
            data_json = package.get_resource('inline')
            data_json_data = data_json.source

            result = {'action': 'create',
                    'ckan_id': None,
                    'new_data': data_json_data,
                    'reason': 'Not found in the CKAN results'}

            row = {'comparison_results': result}
            yield row

            # Delete the data.json file
            os.remove(name)

        found = found_not_update + found_update

        stats = f"""Total processed: {total}.
                    {no_extras} fail extras.
                    {no_identifier_key_found} fail identifier key.
                    {deleted} deleted.
                    {found} datasets found
                    ({found_update} needs update,
                    {found_not_update} are the same),
                    {news} new datasets."""

        logger.info(stats)

    return f
    
