import base64
import json
import pytz
from abc import ABC, abstractmethod
from datetime import datetime
from harvester_adapters.ckan.api import CKANPortalAPI
from harvesters.datajson.ckan.dataset import DataJSONSchema1_1
from harvester_ng import helpers
from harvester_ng.logs import logger


class HarvestDestination(ABC):
    """ main harvest destination class to inherit """
    def __init__(self, *args, **kwargs):
        self.source = None  # class who call use this class as destination
        # configuration (e.g: CKAN uses validator_schema)
        config = kwargs.get('config', {})  # configuration (e.g validation_schema)
        if type(config) == str:
            self.config = json.loads(config)

    @abstractmethod
    def yield_datasets(self):
        """ get datasets to compare and analyze differences """
        pass

    @abstractmethod
    def write_results(self):
        """ save final dataset to destination """
        pass

    @abstractmethod
    def destination_type(self):
        """ class name """
        pass

    def __str__(self):
        return self.destination_type()



class CKANHarvestDestination(HarvestDestination):
    """ CKAN destination for harvested data """
    def __init__(self, catalog_url, api_key, organization_id, harvest_source_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.catalog_url = catalog_url
        self.api_key = api_key
        self.organization_id = organization_id
        self.harvest_source_id = harvest_source_id
        logger.info(f'Harvest destination set: {catalog_url}')
    
    def destination_type(self):
        return "CKAN"

    def yield_datasets(self, harvest_source_id, save_results_json_path=None):
        
        logger.info(f'Extracting from harvest source id: {harvest_source_id}')
        cpa = CKANPortalAPI(base_url=self.catalog_url)
        resources = 0

        page = 0
        for datasets in cpa.search_harvest_packages(harvest_source_id=harvest_source_id):
            # getting resources in pages of packages
            page += 1
            logger.info('PAGE {} from harvest source id: {}'.format(page, harvest_source_id))
            for dataset in datasets:
                pkg_resources = len(dataset['resources'])
                resources += pkg_resources
                yield(dataset)

        logger.info('{} total resources in harvest source id: {}'.format(resources, harvest_source_id))
        if save_results_json_path is not None:
            cpa.save_packages_list(path=save_results_json_path)

    def write_results(self):
        """ save results to destination. Yield results to continue flow process """
        logger.info('Writting results')
        def f(rows):
            actions = {}
            c = 0
            for row in results:
                c += 1
                if 'is_duplicate' in row:
                    logger.info(f'Duplicated writting results: {row}')
                    continue

                comparison_results = row['comparison_results']
                action = comparison_results['action']
                if action not in actions.keys():
                    actions[action] = {'total': 0, 'success': 0, 'fails': 0}
                actions[action]['total'] += 1

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
                    logger.info(f'Ignored for error: {row}')
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

                    schema_version = datajson_dataset['headers']['schema_version'] 
                    if schema_version not in ['1.1']:  # main error
                        raise Exception(f'Unknown schema version: "{schema_version}"')
                    
                    datajson_dataset['source_schema_version'] = schema_version
                    datajson_dataset['source_hash'] = helpers.hash_dataset(dataset=datajson_dataset)

                    # harvest extras
                    # check if a local harvest source is required
                    # https://github.com/ckan/ckanext-harvest/blob/master/ckanext/harvest/logic/action/create.py#L27
                    datajson_dataset['harvest_ng_source_title'] = self.source.name
                    datajson_dataset['harvest_ng_source_id'] = self.harvest_source_id

                    # CKAN hides this extras if we not define as harvest type
                    # if https://github.com/ckan/ckanext-harvest/blob/3a72337f1e619bf9ea3221037ca86615ec22ae2f/ckanext/harvest/plugin.py#L125
                    datajson_dataset['harvest_source_title'] = self.source.name
                    datajson_dataset['harvest_source_id'] = self.harvest_source_id

                    if schema_version == '1.1':
                        djss = DataJSONSchema1_1(original_dataset=datajson_dataset)

                    #  ORG is required!
                    djss.ckan_owner_org_id = self.organization_id
                    ckan_dataset = djss.transform_to_ckan_dataset(existing_resources=existing_resources)
                    logger.info(f'Transformed to CKAN dataset: {ckan_dataset}')

                    # check errors
                    results['errors'] += djss.errors
                    if ckan_dataset is None:
                        error = 'Package skipped with errors: {}'.format(results['errors'])
                        logger.error(error)
                        actions[action]['fails'] += 1
                        yield row
                        continue

                if action == 'create':
                    cpa = CKANPortalAPI(base_url=self.catalog_url,
                                        api_key=self.api_key)

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
                    cpa = CKANPortalAPI(base_url=self.catalog_url,
                                        api_key=self.api_key)

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
                    cpa = CKANPortalAPI(base_url=self.catalog_url, api_key=self.api_key)

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

                else:
                    error = 'Unexpected action for this dataset: {}'.format(action)
                    results = {'success': False, 'error': error}

                results['timestamp'] = datetime.now(pytz.utc).isoformat()  # iso format move as string to save to disk
                yield row

            logger.info(f'Actions detected {actions}')
        return f
        
    def assing_collection_pkg_id(self):
        """ detect new CKAN ids for collections.
            The IDs are at different rows so we need to iterate all rows
            """
        logger.info('Assignment of collection identifiers')
        def f(rows):
            
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
                        ckan_dataset = helpers.set_extra(ckan_dataset=ckan_dataset,
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
        return f