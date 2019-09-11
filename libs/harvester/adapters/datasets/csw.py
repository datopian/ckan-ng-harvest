from harvester.adapters.ckan_dataset_adapters import CKANDatasetAdapter
from harvester.logs import logger
from slugify import slugify
from urllib.parse import urlparse
import json
from harvester.adapters.resources.csw import CSWResource
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

        dataset = self.original_dataset['iso_values']

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
        self.set_bbox()

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

    def infer_resources(self):
        # TODO move to the CSWResource adapter since there is no one-to-one resource relationship
        # extract info about internal resources
        self.resources = []

        # the way that previous harvester work is complex
        # https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/base.py#L350-L411
        # That plugin uses these four elements

        resource_locator_groups = self.original_dataset.get('resource-locator-group', [])
        distributor_data_format = self.original_dataset.get('distributor-data-format', '')
        distribution_data_formats = self.original_dataset.get('distribution-data-format', [])

        total_data_formats = len(distribution_data_formats)

        zipit = False
        if distributor_data_format != '':
            universal_format = distributor_data_format
        elif total_data_formats == 1:
            universal_format = distribution_data_formats[0]
        elif total_data_formats == 0:
            universal_format = None
        elif total_data_formats != len(resource_locator_groups):
            universal_format = None
        else:
            zipit = True

        if zipit:
            resource_locator_group_data_format = zip(resource_locator_groups, distribution_data_formats)
        else:
            # rlg: resource_locator_group
            rldf = [(rlg, universal_format) for rlg in resource_locator_groups]
            resource_locator_group_data_format = rldf

        # we need to read more but we have two kind of resources
        for resource in resource_locator_group_data_format:
            res = {'type': 'resource_locator_group_data_format', 'data': resource}
            self.resources.append(resource)

        resource_locators = self.original_dataset.get('resource-locator-identification', [])

        for resource in resource_locators:
            res = {'type': 'resource_locator', 'data': resource}
            self.resources.append(resource)

        return self.resources

    def transform_resources(self):
        ''' Transform this resources in list of resources '''

        for original_resource in self.resources:
            cra = CSWResource(original_resource=original_resource)
            resource_transformed = cra.transform_to_ckan_resource()
            resources.append(resource_transformed)

        return resources

    def set_bbox(self):
        bbx = self.original_dataset.get('bbox', None)
        if bbx is None:
            self.set_extra('spatial', None)
            return

        if type(bbx) != list or len(bbx) == 0:
            self.set_extra('spatial', None)
            return

        bbox = bbx[0]
        self.set_extra('bbox-east-long', bbox['east'])
        self.set_extra('bbox-north-lat', bbox['north'])
        self.set_extra('bbox-south-lat', bbox['south'])
        self.set_extra('bbox-west-long', bbox['west'])

        try:
            xmin = float(bbox['west'])
            xmax = float(bbox['east'])
            ymin = float(bbox['south'])
            ymax = float(bbox['north'])
        except ValueError as e:
            self.set_extra('spatial', None)
        else:
            # Construct a GeoJSON extent so ckanext-spatial can register the extent geometry

            # Some publishers define the same two corners for the bbox (ie a point),
            # that causes problems in the search if stored as polygon
            if xmin == xmax or ymin == ymax:
                extent_string = '{"type": "Point", "coordinates": [{}, {}]}'.format(xmin, ymin)
            else:
                coords = '[[[{xmin}, {ymin}], [{xmax}, {ymin}], [{xmax}, {ymax}], [{xmin}, {ymax}], [{xmin}, {ymin}]]]'.format(xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)
                extent_string = '{{"type": "Polygon", "coordinates": {coords}}}'.format(coords=coords)

            self.set_extra('spatial', extent_string.strip())

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

        rp = [{'name': k, 'roles': v} for k, v in parties.items()]
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
