import glob
import logging
import os
import pytz

from datapackage import Package, Resource
from dateutil.parser import parse
from harvesters.datajson.harvester import DataJSONDataset
from harvester_ng import helpers


logger = logging.getLogger(__name__)


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

def compare_resources(rows):
    """ read the previous resource (CKAN API results)
        Yield any comparison result
        """

    res_name = rows.res.name if hasattr(rows, 'res') else 'Fake res testing'
    logger.info(f'Rows from resource {res_name}')

    data_packages_path = self.get_data_packages_folder_path()
    default_tzinfo_for_naives_dates = pytz.UTC

    # Calculate minimum statistics
    total = 0

    no_extras = 0
    no_identifier_key_found = 0
    deleted = 0
    found_update = 0
    found_not_update = 0

    sample_row = None
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

        # if sample_row is None:
        #     sample_row = row

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

        # there is no real row here

        # row = sample_row.update({'comparison_results': result})
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


def assing_collection_pkg_id(rows):
    """ detect new CKAN ids for collections.
        The IDs are at different rows so we need to iterate all rows
        """
    # create a list of datajson identifiers -> CKAN indetifiers
    # to detect collection IDs
    if rows is None:
        raise Exception('NADA')
    related_ids = {}
    need_update_rows = []  # need to save the collection_pkg_id
    for row in rows:
        comparison_results = row['comparison_results']
        action = comparison_results['action']
        if action not in ['update', 'create']:
            yield row
        else:
            datajson_dataset = comparison_results['new_data']
            old_identifier = datajson_dataset['identifier']  # ID at data.json
            # If I'm creating a new resource that not exists at CKAN then I have no ID
            new_identifier = row.get('id', None)  # ID at CKAN
            related_ids[old_identifier] = new_identifier

            # if is part of a collection, get the CKAN ID
            is_part_of = datajson_dataset.get('isPartOf', None)
            if is_part_of is None:
                yield row
            else:
                need_update_rows.append(row)

    cpa = CKANPortalAPI(base_url=config.CKAN_CATALOG_URL,
                        api_key=config.CKAN_API_KEY)

    for row in need_update_rows:
        comparison_results = row['comparison_results']
        datajson_dataset = comparison_results['new_data']
        old_identifier = datajson_dataset['isPartOf']  # ID at data.json
        new_ckan_identifier = related_ids.get(old_identifier, None)
        if new_ckan_identifier is not None:

            res3 = cpa.show_package(ckan_package_id_or_name=row['id'])
            if res3['success'] != True:
                error = 'Unable to read package for update collection_pkg_id'
                comparison_results['action_results']['errors'].append(error)
            else:
                # update ckan package
                ckan_dataset = res3['result']
                ckan_dataset = set_extra(ckan_dataset=ckan_dataset,
                                         key='collection_package_id',
                                         value=new_ckan_identifier)

                try:
                    ckan_response = cpa.update_package(ckan_package=ckan_dataset)
                    # save for not ask package_show again in tests
                    row['extras'] = ckan_response['result']['extras']
                except Exception as e:
                    error = f'Error updating collection_package_id at {ckan_dataset}: {e}'
                    comparison_results['action_results']['errors'].append(error)

        else:
            error = f'Unable to detect the collection_pkg_id at {row}'
            comparison_results['action_results']['errors'].append(error)

        yield row


def set_extra(ckan_dataset, key, value):
    found = False
    for extra in ckan_dataset['extras']:
        if extra['key'] == key:
            extra['value'] = value
            found = True
    if not found:
        ckan_dataset['extras'].append({'key': key, 'value': value})
    return ckan_dataset


def write_results_to_ckan(rows):
    """ each row it's a dataset to delete/update/create """

    actions = {}
    c = 0
    for row in rows:
        c += 1
        if 'is_duplicate' in row:
            continue

        comparison_results = row['comparison_results']
        action = comparison_results['action']
        if action not in actions.keys():
            actions[action] = {'total': 0, 'success': 0, 'fails': 0}
        actions[action]['total'] += 1

        dump_comp_res = json.dumps(comparison_results, indent=4)
        # logger.info(f'Previous results {dump_comp_res}')
        """ comparison_results is something like this
        row['comparison_results'] {
            "action": "update" | "delete" | "create",
            "ckan_id": "1bfc8520-17b0-46b9-9940-a6646615436c",
            "new_data": {data json dataset format},
            "reason": "Some reason for the action"
            }
        """

        results = {'success': False, 'warnings': [], 'errors': []}
        comparison_results['action_results'] = results

        if action == 'error':
            results['errors'].append(comparison_results['reason'])
            actions[action]['fails'] += 1
            yield row
            continue

        # if it's an update we need to merge internal resources
        if action == 'update':
            existing_resources = row['resources']
        elif action == 'create':
            existing_resources = None

        if action in ['update', 'create']:
            datajson_dataset = comparison_results['new_data']

            # add required extras
            # set catalog extras
            for key, value in datajson_dataset['headers'].items():
                if key in ['@context', '@id', 'conformsTo', 'describedBy']:
                    datajson_dataset[f'catalog_{key}'] = value

            schema_version = datajson_dataset['headers']['schema_version']  # 1.1 or 1.0
            assert schema_version in ['1.0', '1.1']  # main error
            datajson_dataset['source_schema_version'] = schema_version
            datajson_dataset['source_hash'] = hash_dataset(datasetdict=datajson_dataset)

            # harvest extras
            # check if a local harvest source is required
            # https://github.com/ckan/ckanext-harvest/blob/master/ckanext/harvest/logic/action/create.py#L27
            datajson_dataset['harvest_ng_source_title'] = config.SOURCE_NAME
            datajson_dataset['harvest_ng_source_id'] = config.SOURCE_ID

            # CKAN hides this extras if we not define as harvest type
            # if https://github.com/ckan/ckanext-harvest/blob/3a72337f1e619bf9ea3221037ca86615ec22ae2f/ckanext/harvest/plugin.py#L125
            datajson_dataset['harvest_source_title'] = config.SOURCE_NAME
            datajson_dataset['harvest_source_id'] = config.SOURCE_ID

            if schema_version == '1.1':
                djss = DataJSONSchema1_1(original_dataset=datajson_dataset)
            else:
                results['errors'].append('We only harvest 1.1 schema datasets')
                actions[action]['fails'] += 1
                yield row
                continue
                # raise Exception('We are not ready to harvest 1.0 schema datasets. Check if this kind of dataset still exists')

            #  ORG is required!
            djss.ckan_owner_org_id = config.CKAN_OWNER_ORG
            ckan_dataset = djss.transform_to_ckan_dataset(existing_resources=existing_resources)

            # check errors
            results['errors'] += djss.errors
            if ckan_dataset is None:
                error = 'Package skipped with errors: {}'.format(results['errors'])
                logger.error(error)
                actions[action]['fails'] += 1
                yield row
                continue

        if action == 'create':
            cpa = CKANPortalAPI(base_url=config.CKAN_CATALOG_URL,
                                api_key=config.CKAN_API_KEY)

            try:
                ckan_response = cpa.create_package(ckan_package=ckan_dataset, on_duplicated='DELETE')
            except Exception as e:
                ckan_response = {'success': False, 'error': str(e)}

            results['success'] = ckan_response['success']
            results['ckan_response'] = ckan_response

            if ckan_response['success']:
                actions[action]['success'] += 1
                # add this new CKAN ID in the case we need as collection_pkg_id
                row['id'] = ckan_response['result']['id']
                row['extras'] = ckan_response['result'].get('extras', [])
                comparison_results['ckan_id'] = ckan_response['result']['id']
            else:
                actions[action]['fails'] += 1
                error = 'Error creating dataset: {}'.format(ckan_response['error'])
                results['errors'].append(error)

        elif action == 'update':
            cpa = CKANPortalAPI(base_url=config.CKAN_CATALOG_URL,
                                api_key=config.CKAN_API_KEY)

            try:
                ckan_response = cpa.update_package(ckan_package=ckan_dataset)
            except Exception as e:
                ckan_response = {'success': False, 'error': str(e)}

            results['success'] = ckan_response['success']
            results['ckan_response'] = ckan_response
            # row['id'] = comparison_results['ckan_id']

            if ckan_response['success']:
                actions[action]['success'] += 1
                row['extras'] = ckan_response['result'].get('extras', [])
            else:
                actions[action]['fails'] += 1
                error = 'Error updating dataset: {}'.format(ckan_response['error'])
                results['errors'].append(error)

        elif action == 'delete':
            ckan_id = row['comparison_results']['ckan_id']
            cpa = CKANPortalAPI(base_url=config.CKAN_CATALOG_URL, api_key=config.CKAN_API_KEY)

            try:
                ckan_response = cpa.delete_package(ckan_package_id_or_name=ckan_id)
            except Exception as e:
                ckan_response = {'success': False, 'error': str(e)}

            results['success'] = ckan_response['success']
            results['ckan_response'] = ckan_response
            error = 'Error updating dataset: {}'.format(ckan_response['error'])
            results['errors'].append(error)

            if ckan_response['success']:
                actions[action]['success'] += 1
            else:
                actions[action]['fails'] += 1


        elif action == 'ignore':
            continue
            results = {'success': True}

        else:
            error = 'Unexpected action for this dataset: {}'.format(action)
            results = {'success': False, 'error': error}

        results['timestamp'] = datetime.now(pytz.utc).isoformat()  # iso format move as string to save to disk
        yield row

    logger.info(f'Actions detected {actions}')
