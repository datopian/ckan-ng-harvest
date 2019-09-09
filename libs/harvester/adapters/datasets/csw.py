from harvester.adapters.ckan_dataset_adapters import CKANDatasetAdapter
from harvester.logs import logger
from slugify import slugify
import json
from harvester.adapters.resources.data_json import DataJSONDistribution
from harvester.settings import ckan_settings


class CSWDataset(CKANDatasetAdapter):
    ''' CSW dataset '''

    # check the get_package_dict function
    # https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/base.py#L169

    ckan_owner_org_id = None  # required, the client must inform which existing org

    MAPPING = {
        'name': 'name',
        'title': 'title',
        'tags': 'tags',
        'abstract': 'notes',
        'progress': 'extras__progress',
        'resource-type': 'extras__resource-type',

        # Essentials
        'spatial-reference-system': 'extras__spatial-reference-system',
        'guid': 'extras__guid',
        # Usefuls
        'dataset-reference-date': 'extras__dataset-reference-date',
        'metadata-language': 'extras__metadata-language',  # Language
        'metadata-date': 'extras__metadata-date',  # Released
        'coupled-resource': 'extras__coupled-resource',
        'contact-email': 'extras__contact-email',
        'frequency-of-update': 'extras__frequency-of-update',
        'spatial-data-service-type': 'extras__spatial-data-service-type',

        'harvest_ng_source_title': 'extras__harvest_source_title',
        'harvest_ng_source_id': 'extras__harvest_source_id',
        'harvest_source_title': 'extras__harvest_source_title',
        'harvest_source_id': 'extras__harvest_source_id',
        'source_hash': 'extras__source_hash',
    }

    def fix_fields(self, field, value):
        # some fields requires extra work
        if field == 'tags':
            return self.build_tags(value)
        elif field == 'extras__progress':
            if type(value) == list and len(value) > 0:
                return value[0]
            else:
                return ''
        elif field == 'extras__resource-type':
            if type(value) == list and len(value) > 0:
                return value[0]
            else:
                return ''
        else:
            return value

    def validate_origin_dataset(self):
        # check required https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create

        if self.ckan_owner_org_id is None:
            return False, 'Owner organization ID is required'

        return True, None

    def transform_to_ckan_dataset(self, existing_resources=None):

        valid, error = self.validate_origin_dataset()
        if not valid:
            raise Exception(f'Error validating origin dataset: {error}')

        dataset = self.original_dataset

        # previous transformations at origin
        for old_field, field_ckan in self.MAPPING.items():
            logger.debug(f'Connecting fields "{old_field}", "{field_ckan}"')
            # identify origin and set value to destination
            origin = self.identify_origin_element(raw_field=old_field)
            if origin is None:
                logger.debug(f'No data in origin for "{old_field}"')
            else:
                self.ckan_dataset = self.set_destination_element(raw_field=field_ckan, new_value=origin)
                logger.debug(f'Connected OK fields "{old_field}"="{origin}"')

        # TODO find and adapt resources
        # self.ckan_dataset['resources'] = self.transform_resources ?

        # define name (are uniques in CKAN instance)
        if 'name' not in self.ckan_dataset or self.ckan_dataset['name'] == '':
            self.ckan_dataset['name'] = self.generate_name(title=self.ckan_dataset['title'])

        # mandatory
        self.ckan_dataset['owner_org'] = self.ckan_owner_org_id

        valid, error = self.validate_final_dataset()
        if not valid:
            raise Exception(f'Error validating final dataset: {error}')

        logger.info('Dataset transformed {} OK'.format(self.original_dataset.get('identifier', '')))
        return self.ckan_dataset
