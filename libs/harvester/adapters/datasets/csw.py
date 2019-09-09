from harvester.adapters.ckan_dataset_adapters import CKANDatasetAdapter
from harvester.logs import logger
from slugify import slugify
from urllib.parse import urlparse
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

        'use-constraints': 'extras__licence',
    }

    def fix_fields(self, field, value):
        # some fields requires extra work
        if field == 'tags':
            return self.build_tags(value)
        elif field == 'extras__progress':  # previous harvester take just the first one
            if type(value) == list and len(value) > 0:
                return value[0]
            else:
                return ''
        elif field == 'extras__resource-type':  # previous harvester take just the first one
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

        # custom changes
        self.fix_licence_url()
        self.set_browse_graphic()
        self.set_temporal_extent()
        self.set_responsible_party()

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

    def set_responsible_party(self):
        ro = self.original_dataset.get('responsible-organisation', None)
        if ro is None:
            return

        parties = {}
        for party in ro:
            if party['organisation-name'] in parties:
                if not party['role'] in parties[party['organisation-name']]:
                    parties[party['organisation-name']].append(party['role'])
            else:
                parties[party['organisation-name']] = [party['role']]

        rp = [{'name': k, 'roles': v} for k, v in parties.iteritems()]
        self.set_extra('responsible-party', rp)

    def fix_licence_url(self):
        # https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/base.py#L278
        licences = self.get_extra('licence')
        if licences != '' and licences is not None:
            if type(licences) == list:
                for licence in licences:
                    u = urlparse(licence)
                    if u.scheme and u.netloc:
                        self.set_extra(key='licence_url', value=licence)

    def set_browse_graphic(self):
        browse_graphic = self.original_dataset.get('browse-graphic', None)
        if browse_graphic is None:
            return

        if type(browse_graphic) != list or len(browse_graphic) == 0:
            return

        browse_graphic = browse_graphic[0]
        pf = browse_graphic.get('file', None)
        if pf is not None:
            self.set_extra('graphic-preview-file', pf)

        descr = browse_graphic.get('description', None)
        if descr is not None:
            self.set_extra('graphic-preview-description', descr)

        pt = browse_graphic.get('type', None)
        if pt is not None:
            self.set_extra('graphic-preview-type', pt)

    def set_temporal_extent(self):
        for key in ['temporal-extent-begin', 'temporal-extent-end']:
            te = self.original_dataset.get(key, None)
            if te is not None:
                if type(te) == list and len(te) > 0:
                    self.set_extra(key, te[0])
