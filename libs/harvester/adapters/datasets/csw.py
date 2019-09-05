from harvester.adapters.ckan_dataset_adapters import CKANDatasetAdapter
from harvester.logs import logger
from slugify import slugify
import json
from harvester.adapters.resources.data_json import DataJSONDistribution
from harvester.settings import ckan_settings


class CSWDataset(CKANDatasetAdapter):
    ''' CSW dataset '''

    # https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L478
    # value at data.json -> value at CKAN dataset

    ckan_owner_org_id = None  # required, the client must inform which existing org

    MAPPING = {
        # FILL THIS
    }

    def __identify_origin_element(self, raw_field, in_dict):
        # get the value in csw dict to put in CKAN dataset.
        # Consider the __ separator
        # in 'contactPoint__hasEmail' gets in_dict['contactPoint']['hasEmail'] if exists
        # in 'licence' gets in_dict['licence'] if exists

        parts = raw_field.split('__')
        if parts[0] not in in_dict:
            return None
        origin = in_dict[parts[0]]
        if len(parts) > 1:
            for part in parts[1:]:
                if part in origin:
                    origin = origin[part]
                else:  # drop
                    return None
        return origin

    def __fix_fields(self, field, value):
        # some fields requires extra work
        return value

    def validate_origin_dataset(self):
        # check required https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create
        datajson_dataset = self.original_dataset

        if self.ckan_owner_org_id is None:
            return False, 'Owner organization ID is required'

        return True, None

    def validate_final_dataset(self, ckan_dataset):
        # check required https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create

        if 'private' not in ckan_dataset:
            return False, 'private is a required field'
        if 'name' not in ckan_dataset:
            return False, 'name is a required field'

        return True, None

    def transform_to_ckan_dataset(self, existing_resources=None):

        ckan_dataset = self.get_base_ckan_dataset()
        datajson_dataset = self.original_dataset

        # previous transformations at origin
        for field_data_json, field_ckan in self.MAPPING.items():
            logger.debug(f'Connecting fields "{field_data_json}", "{field_ckan}"')
            # identify origin and set value to destination
            origin = self.__identify_origin_element(raw_field=field_data_json, in_dict=datajson_dataset)
            if origin is None:
                logger.debug(f'No data in origin for "{field_data_json}"')
            else:
                ckan_dataset = self.__set_destination_element(raw_field=field_ckan, to_dict=ckan_dataset, new_value=origin)
                logger.debug(f'Connected OK fields "{field_data_json}"="{origin}"')

        ckan_dataset['resources'] = self.__transform_resources(distribution)

        # define name (are uniques in CKAN instance)
        if 'name' not in ckan_dataset or ckan_dataset['name'] == '':
            ckan_dataset['name'] = self.generate_name(title=ckan_dataset['title'])

        # mandatory
        ckan_dataset['owner_org'] = self.ckan_owner_org_id

        valid, error = self.validate_final_dataset(ckan_dataset=ckan_dataset)
        if not valid:
            raise Exception(f'Error validating final dataset: {error}')

        logger.info('Dataset transformed {} OK'.format(self.original_dataset.get('identifier', '')))
        return ckan_dataset

    def __find_extra(self, ckan_dataset, key, default=None):
        for extra in ckan_dataset["extras"]:
            if extra["key"] == key:
                return extra["value"]
        return default

    def __set_extra(self, ckan_dataset, key, value):
        found = False
        for extra in ckan_dataset['extras']:
            if extra['key'] == key:
                extra['value'] = value
                found = True
        if not found:
            ckan_dataset['extras'].append({'key': key, 'value': value})
        return ckan_dataset

    def generate_name(self, title):
        # names are unique in CKAN
        # old harvester do like this: https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L747

        name = slugify(title)
        cut_at = ckan_settings.MAX_NAME_LENGTH - 5  # max length is 100
        if len(name) > cut_at:
            name = name[:cut_at]

        # TODO check if the name MUST be a new unexisting one
        # TODO check if it's an existing resource and we need to read previos name using the identifier

        return name