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

    def __build_tags(self, tags):
        # create a CKAN tag
        # Help https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.tag_create
        ret = []
        for tag in tags:
            tag = tag.strip()
            if tag != '':
                tag = slugify(tag[:ckan_settings.MAX_TAG_NAME_LENGTH])
                ret.append({"name": tag})
        return ret

    def __identify_origin_element(self, raw_field, in_dict):
        # get the value in original dict to put in CKAN dataset.
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
        if field == 'tags':
            return self.__build_tags(value)
        else:
            return value

    def validate_origin_dataset(self):
        # check required https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create
        dataset = self.original_dataset

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

    def __set_destination_element(self, raw_field, to_dict, new_value):
        # in 'extras__issued' gets or creates to_dict[extras][key][issued] and assing new_value to to_dict[extras][value]
        # in 'title' assing new_value to to_dict[title]
        # return to_dict modified

        parts = raw_field.split('__')
        if parts[0] not in to_dict:
            raise Exception(f'Not found field "{parts[0]}" at CKAN destination dict')
        if len(parts) == 1:
            to_dict[raw_field] = self.__fix_fields(field=raw_field,
                                                   value=new_value)
            return to_dict
        elif len(parts) == 2:
            if parts[0] != 'extras':
                raise Exception(f'Unknown field estructure: "{raw_field}" at CKAN destination dict')

            # check if extra already exists
            for extra in to_dict['extras']:

                if extra['key'] == parts[1]:
                    extra['value'] = new_value
                    return to_dict

            # this extra do not exists already
            new_extra = {'key': parts[1], 'value': None}
            new_extra['value'] = new_value
            to_dict['extras'].append(new_extra)
            return to_dict
        else:
            raise Exception(f'Unknown fields length estructure for "{raw_field}" at CKAN destination dict')

    def transform_to_ckan_dataset(self, existing_resources=None):

        ckan_dataset = self.get_base_ckan_dataset()
        dataset = self.original_dataset

        # previous transformations at origin
        for old_field, field_ckan in self.MAPPING.items():
            logger.debug(f'Connecting fields "{old_field}", "{field_ckan}"')
            # identify origin and set value to destination
            origin = self.__identify_origin_element(raw_field=old_field, in_dict=dataset)
            if origin is None:
                logger.debug(f'No data in origin for "{old_field}"')
            else:
                ckan_dataset = self.__set_destination_element(raw_field=field_ckan, to_dict=ckan_dataset, new_value=origin)
                logger.debug(f'Connected OK fields "{old_field}"="{origin}"')

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