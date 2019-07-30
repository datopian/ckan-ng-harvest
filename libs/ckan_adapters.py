''' transform datasets to CKAN datasets '''
from abc import ABC, abstractmethod
from logs import logger
from slugify import slugify
import json


class CKANDatasetAdapter(ABC):
    ''' transform other datasets objects into CKAN datasets '''

    def __init__(self, original_dataset):
        self.original_dataset = original_dataset

    def get_base_ckan_dataset(self):
        # base results
        # Check for required fields: https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create

        pkg = {
            'name': '',  # no spaces, just lowercases, - and _
            'title': '',
            'owner_org': '',  # (string) – the id of the dataset’s owning organization, see organization_list() or organization_list_for_user() for available values. This parameter can be made optional if the config option ckan.auth.create_unowned_dataset is set to True.
            'private': False,
            'author': None,  # (string) – the name of the dataset’s author (optional)
            'author_email': None,  # (string) – the email address of the dataset’s author (optional)
            'maintainer': None,  # (string) – the name of the dataset’s maintainer (optional)
            'maintainer_email': None,  # (string) – the email address of the dataset’s maintainer (optional)
            'license_id': None,  # (license id string) – the id of the dataset’s license, see license_list() for available values (optional)
            'notes':  None,  # (string) – a description of the dataset (optional)
            'url': None,  # (string) – a URL for the dataset’s source (optional)
            'version': None,  # (string, no longer than 100 characters) – (optional)
            'state': 'active',  # (string) – the current state of the dataset, e.g. 'active' or 'deleted'
            'type': None,  # (string) – the type of the dataset (optional), IDatasetForm plugins associate themselves with different dataset types and provide custom dataset handling behaviour for these types
            'resources': None,  # (list of resource dictionaries) – the dataset’s resources, see resource_create() for the format of resource dictionaries (optional)
            'tags': None,  # (list of tag dictionaries) – the dataset’s tags, see tag_create() for the format of tag dictionaries (optional)
            'extras': [  # (list of dataset extra dictionaries) – the dataset’s extras (optional), extras are arbitrary (key: value) metadata items that can be added to datasets, each extra dictionary should have keys 'key' (a string), 'value' (a string)
                {'key': 'resource-type', 'value': 'Dataset'}
            ],
            'relationships_as_object': None,  # (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
            'relationships_as_subject': None,  # (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
            'groups': None,  # (list of dictionaries) – the groups to which the dataset belongs (optional), each group dictionary should have one or more of the following keys which identify an existing group: 'id' (the id of the group, string), or 'name' (the name of the group, string), to see which groups exist call group_list()
        }

        return pkg

    @abstractmethod
    def transform_to_ckan_dataset(self):
        pass


class DataJSONSchema1_1(CKANDatasetAdapter):
    ''' Data.json dataset from Schema 1.0'''

    # https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L478
    # value at data.json -> value at CKAN dataset

    ckan_owner_org_id = None  # required, the client must inform which existing org

    MAPPING = {
        'title': 'title',
        'description': 'notes',
        'keyword': 'tags',
        'modified': 'extras__modified',  # ! revision_timestamp
        'publisher': 'extras__publisher',  # !owner_org
        'contactPoint__fn': 'maintainer',
        'contactPoint__hasEmail': 'maintainer_email',
        'identifier': 'extras__identifier',  # !id
        'accessLevel': 'extras__accessLevel',
        'bureauCode': 'extras__bureauCode',
        'programCode': 'extras__programCode',
        'rights': 'extras__rights',
        'license': 'extras__license',  # !license_id
        'spatial': 'extras__spatial',  # Geometry not valid GeoJSON, not indexing
        'temporal': 'extras__temporal',
        'theme': 'extras__theme',
        'dataDictionary': 'extras__dataDictionary',  # !data_dict
        'dataQuality': 'extras__dataQuality',
        'accrualPeriodicity':'extras__accrualPeriodicity',
        'landingPage': 'extras__landingPage',
        'language': 'extras__language',
        'primaryITInvestmentUII': 'extras__primaryITInvestmentUII',  # !PrimaryITInvestmentUII
        'references': 'extras__references',
        'issued': 'extras__issued',
        'systemOfRecords': 'extras__systemOfRecords',
        # 'distribution': 'resources'

        'harvest_ng_source_title': 'extras__harvest_source_title',
        'harvest_ng_source_id': 'extras__harvest_source_id',

        'harvest_source_title': 'extras__harvest_source_title',
        'harvest_source_id': 'extras__harvest_source_id',
        'source_schema_version': 'extras__source_schema_version',  # 1.1 or 1.0
        'source_hash': 'extras__source_hash',

    }

    def __identify_origin_element(self, raw_field, in_dict):
        # in 'contactPoint__hasEmail' gets in_dict[contactPoint][hasEmail] if exists
        # in 'licence' gets in_dict[licence] if exists

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
        elif field == 'maintainer_email':
            if value.startswith('mailto:'):
                value = value.replace('mailto:', '')
            return value

        else:
            return value

    def __fix_extras(self, key, value):
        # modify extras when need it
        logger.debug(f'fix extras {key} {value}')
        # some fields requires extra work
        if key == 'publisher':
            return value['name']
        else:
            return value

    def __build_tags(self, tags):
        # create a CKAN tag
        # Help https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.tag_create
        ret = []
        for tag in tags:
            # ret.append({"id": None, "name": tag})
            ret.append({"name": tag})
        return ret

    def __set_destination_element(self, raw_field, to_dict, new_value):
        # in 'extras__issued' gets or creates to_dict[extras][key][issued] and assing new_value to in_dict[extras][value]
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
                    new_value = self.__fix_extras(key=extra['key'], value=new_value)
                    extra['value'] = new_value
                    return to_dict

            # the extra do not exists already
            new_extra = {'key': parts[1], 'value': None}
            new_value = self.__fix_extras(key=parts[1], value=new_value)
            new_extra['value'] = new_value
            to_dict['extras'].append(new_extra)
            return to_dict
        else:
            raise Exception(f'Unknown fields length estructure for "{raw_field}" at CKAN destination dict')

    def transform_to_ckan_dataset(self):
        ckan_dataset = self.get_base_ckan_dataset()
        datajson_dataset = self.original_dataset

        for field_data_json, field_ckan in self.MAPPING.items():
            logger.debug(f'Connecting fields "{field_data_json}", "{field_ckan}"')
            # identify origin and set value to destination
            origin = self.__identify_origin_element(raw_field=field_data_json, in_dict=datajson_dataset)
            if origin is None:
                logger.debug(f'No data in origin for "{field_data_json}"')
            else:
                ckan_dataset = self.__set_destination_element(raw_field=field_ckan, to_dict=ckan_dataset, new_value=origin)
                logger.debug(f'Connected OK fields "{field_data_json}"="{origin}"')

        # add custom extras
        # add source_datajson_identifier = {"key": "source_datajson_identifier", "value": True}
        ckan_dataset = self.__set_destination_element(raw_field='extras__source_datajson_identifier', to_dict=ckan_dataset, new_value=True)

        # define name (are uniques in CKAN instance)
        ckan_dataset['name'] = self.generate_name(title=ckan_dataset['title'])

        # mandatory
        ckan_dataset['owner_org'] = self.ckan_owner_org_id

        # clean all empty unused values (can't pop keys while iterating)
        ckan_dataset_copy = ckan_dataset.copy()
        for k, v in ckan_dataset.items():
            if v is None:
                ckan_dataset_copy.pop(k)
        return ckan_dataset_copy

    def generate_name(self, title):
        # names are unique in CKAN
        # old harvester do like this: https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L747

        name = slugify(title)
        if len(name) > 99:  # max length is 100
            name = name[:95]

        # TODO check if the name MUST be a new unexisting one
        # TODO check if it's an existing resource and we need to read previos name using the identifier

        return name


class DataJSONSchema1_0(CKANDatasetAdapter):
    ''' Data.json dataset from Schema 1.0
        NOT IN USE '''

    # https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L443
    MAPPING = {
        'title': 'title',
        'description': 'notes',
        'keyword': 'tags',
        'modified': 'extras__modified', # ! revision_timestamp
        'publisher': 'extras__publisher', # !owner_org
        'contactPoint': 'maintainer',
        'mbox': 'maintainer_email',
        'identifier': 'extras__identifier', # !id
        'accessLevel': 'extras__accessLevel',
        'bureauCode': 'extras__bureauCode',
        'programCode': 'extras__programCode',
        'accessLevelComment': 'extras__accessLevelComment',
        'license': 'extras__license', # !license_id
        'spatial': 'extras__spatial', # Geometry not valid GeoJSON, not indexing
        'temporal': 'extras__temporal',
        'theme': 'extras__theme',
        'dataDictionary': 'extras__dataDictionary', # !data_dict
        'dataQuality': 'extras__dataQuality',
        'accrualPeriodicity':'extras__accrualPeriodicity',
        'landingPage': 'extras__landingPage',
        'language': 'extras__language',
        'primaryITInvestmentUII': 'extras__primaryITInvestmentUII', # !PrimaryITInvestmentUII
        'references': 'extras__references',
        'issued': 'extras__issued',
        'systemOfRecords': 'extras__systemOfRecords'
    }

    def transform_to_ckan_dataset(self):
        return self.original_dataset


