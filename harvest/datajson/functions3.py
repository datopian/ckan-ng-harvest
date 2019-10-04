from harvester.logs import logger
from harvester import config
from harvester.adapters.datasets.data_json import DataJSONSchema1_1
from harvester.data_gov_api import CKANPortalAPI
import json
from datetime import datetime
import pytz
import hashlib
import os.path


def hash_dataset(datasetdict):
    # hash the dataset.
    # We change the way that previous harvester do this
    #  https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L730
    dmp_dataset = json.dumps(datasetdict, sort_keys=True)
    str_to_hash = dmp_dataset.encode('utf-8')
    return hashlib.sha1(str_to_hash).hexdigest()


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
                results['errors'].append('We are not ready to harvest 1.0 schema datasets. Add it to harvester')
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


def write_final_report():
    """ take all generated data and write a final report """
    pass


def build_validation_error_email(error_items=[]):
    # json errors
    errors = {}
    json_errors_path = config.get_errors_path()
    f = open(json_errors_path, "r")
    json_validation_errors = f.read()
    errors['json_validation_errors'] = json_validation_errors

    # header errors
    errors = {}

    #dataset errors
    errors['dataset_errors'] = []
    for item in error_items:
        new_data = item['comparison_results']['new_data']
        if new_data is None:
            continue
        if 'validation_errors' not in new_data or new_data['validation_errors'] is None:
            continue
        if len(new_data['validation_errors']) > 0:
            errors['dataset_errors'].append(new_data['validation_errors'])

    #duplicate errors
    flow_1_results_path = config.get_flow1_datasets_result_path()
    f = open(flow_1_results_path, "r")
    flow_1_results = f.read()
    if flow_1_results:
        flow_1_json_results = json.loads(flow_1_results)
        errors['dataset_duplicates'] = []
        for item in flow_1_json_results:
            if 'is_duplicate' in item:
                errors['dataset_duplicates'].append(item)

    #send validation email
    send_validation_error_email(errors)


def send_validation_error_email(errors):
    """ take all errors and send to organization admins """
    if len(errors) > 0:
        msg = errors
        admin_users = get_admin_users()
        recipients = []
        for user in admin_users:
            member_details = get_user_info(user[0])
            if member_details.get('email', None) is not None:
                recipients.append({
                    'name': member_details['name'],
                    'email': member_details['email']
                })

            for recipient in recipients:
                email = {'recipient_name': recipient['name'],
                         'recipient_email': recipient['email'],
                         'subject': 'Harvesting Job - Error Notification',
                         'body': msg}
                # TODO find email server to send email
                # try:
                #     mail_recipient(**email)
                # except MailerException:
                #     logger.error('Sending Harvest-Notification-Mail failed. Message: ' + msg)
                # except Exception as e:
                #     logger.error(e)


def get_admin_users():
    """ fetch admin users from an organization """
    owner_org = config.CKAN_OWNER_ORG
    cpa = CKANPortalAPI(base_url=config.CKAN_CATALOG_URL,
                        api_key=config.CKAN_API_KEY)
    res = cpa.get_admin_users(organization_id=owner_org)
    return res['result']


def get_user_info(user_id):
    """ fetch admin users from an organization """
    cpa = CKANPortalAPI(base_url=config.CKAN_CATALOG_URL,
                        api_key=config.CKAN_API_KEY)
    res = cpa.get_user_info(user_id=user_id)
    return res['result']
