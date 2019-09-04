from harvester.adapters.ckan_dataset_adapters import CKANDatasetAdapter
from harvester.logs import logger
from slugify import slugify
import json
from harvester.adapters.resources.data_json import DataJSONDistribution
from harvester.settings import ckan_settings


class DataJSONSchema1_1(CKANDatasetAdapter):
    ''' Data.json dataset from Schema 1.1'''

    # https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L478
    # value at data.json -> value at CKAN dataset

    ckan_owner_org_id = None  # required, the client must inform which existing org

    MAPPING = {
        'name': 'name',
        'title': 'title',
        'description': 'notes',
        'keyword': 'tags',
        'modified': 'extras__modified',  # ! revision_timestamp
        # requires extra work 'publisher': 'extras__publisher',  # !owner_org
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
        # 'distribution': 'resources'  # transformed with a custom adapter

        'harvest_ng_source_title': 'extras__harvest_source_title',
        'harvest_ng_source_id': 'extras__harvest_source_id',

        'harvest_source_title': 'extras__harvest_source_title',
        'harvest_source_id': 'extras__harvest_source_id',
        'source_schema_version': 'extras__source_schema_version',  # 1.1 or 1.0
        'source_hash': 'extras__source_hash',

        'catalog_@context': 'extras__catalog_@context',
        'catalog_@id': 'extras__catalog_@id',
        'catalog_conformsTo': 'extras__catalog_conformsTo',
        'catalog_describedBy': 'extras__catalog_describedBy',

        'is_collection': 'extras__is_collection',
        'collection_pkg_id': 'extras__collection_package_id',  # don't like pkg vs package
    }

    def __identify_origin_element(self, raw_field, in_dict):
        # get the value in data.json to put in CKAN dataset.
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
        elif field == 'maintainer_email':
            if value.startswith('mailto:'):
                value = value.replace('mailto:', '')
            return value

        else:
            return value

    def __build_tags(self, tags):
        # create a CKAN tag
        # Help https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.tag_create
        ret = []
        for tag in tags:
            # CKAN exts uses a more ugly an complex function
            # https://github.com/ckan/ckan/blob/30ca7aae2f2aca6a19a2e6ed29148f8428e25c86/ckan/lib/munge.py#L26
            tag = tag.strip()
            if tag != '':
                tag = slugify(tag[:ckan_settings.MAX_TAG_NAME_LENGTH])
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
                    extra['value'] = new_value
                    return to_dict

            # this extra do not exists already
            new_extra = {'key': parts[1], 'value': None}
            new_extra['value'] = new_value
            to_dict['extras'].append(new_extra)
            return to_dict
        else:
            raise Exception(f'Unknown fields length estructure for "{raw_field}" at CKAN destination dict')

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

    def __infer_distribution(self):
        # if _distribution_ is empty then we try to create them from "accessURL" or "webService" URLs
        datajson_dataset = self.original_dataset
        distribution = []
        for field in ['accessURL', 'webService']:
            url = datajson_dataset.get(field, '').strip()

            if url != '':
                fmt = datajson_dataset.get('format', '')
                distribution.append({field: url, 'format': fmt, 'mimetype': fmt})

        return distribution

    def __transform_resources(self, distribution):
        ''' Transform the distribution list in list of resources '''
        if type(distribution) == dict:
            distribution = [distribution]

        resources = []
        for original_resource in distribution:
            cra = DataJSONDistribution(original_resource=original_resource)
            resource_transformed = cra.transform_to_ckan_resource()
            resources.append(resource_transformed)

        return resources

    def transform_to_ckan_dataset(self, existing_resources=None):
        # check how to parse
        # https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/parse_datajson.py#L5
        # if we are updating existing dataset we need to merge resources

        logger.info('Transforming data.json dataset {}'.format(self.original_dataset.get('identifier', '')))
        valid, error = self.validate_origin_dataset()
        if not valid:
            raise Exception(f'Error validating origin dataset: {error}')

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

        # transform distribution into resources
        distribution = datajson_dataset['distribution'] if 'distribution' in datajson_dataset else []
        # if _distribution_ is empty then we try to create them from "accessURL" or "webService" URLs
        if distribution is None or distribution == []:
            distribution = self.__infer_distribution()
        ckan_dataset['resources'] = self.__transform_resources(distribution)
        if existing_resources is not None:
            ckan_dataset['resources'] = self.__merge_resources(existing_resources=existing_resources,
                                                               new_resources=ckan_dataset['resources'])

        # add custom extras
        # add source_datajson_identifier = {"key": "source_datajson_identifier", "value": True}
        ckan_dataset = self.__set_destination_element(raw_field='extras__source_datajson_identifier',
                                                      to_dict=ckan_dataset,
                                                      new_value=True)

        # define name (are uniques in CKAN instance)
        if 'name' not in ckan_dataset or ckan_dataset['name'] == '':
            ckan_dataset['name'] = self.generate_name(title=ckan_dataset['title'])

        # mandatory
        ckan_dataset['owner_org'] = self.ckan_owner_org_id

        # check for license
        if datajson_dataset.get('license', None) not in [None, '']:
            original_license = datajson_dataset['license']
            original_license = original_license.replace('http://', '')
            original_license = original_license.replace('https://', '')
            original_license = original_license.rstrip('/')
            license_id = ckan_settings.LICENCES.get(original_license, "other-license-specified")
            ckan_dataset['license_id'] = license_id

        # define publisher as extras as we expect
        publisher = datajson_dataset.get('publisher', None)
        if publisher is not None:
            publisher_name = publisher.get('name', '')
            ckan_dataset = self.__set_extra(ckan_dataset, 'publisher', publisher_name)
            parent_publisher = publisher.get('subOrganizationOf', None)
            if parent_publisher is not None:
                publisher_hierarchy = [publisher_name]
                while parent_publisher:
                    parent_name = parent_publisher.get('name', '')
                    parent_publisher = parent_publisher.get('subOrganizationOf', None)
                    publisher_hierarchy.append(parent_name)

                publisher_hierarchy.reverse()
                publisher_hierarchy = " > ".join(publisher_hierarchy)
                ckan_dataset = self.__set_extra(ckan_dataset, 'publisher_hierarchy', publisher_hierarchy)

        # clean all empty unused values (can't pop keys while iterating)
        ckan_dataset_copy = ckan_dataset.copy()
        for k, v in ckan_dataset.items():
            if v is None:
                ckan_dataset_copy.pop(k)

        valid, error = self.validate_final_dataset(ckan_dataset=ckan_dataset_copy)
        if not valid:
            raise Exception(f'Error validating final dataset: {error}')

        logger.info('Dataset transformed {} OK'.format(self.original_dataset.get('identifier', '')))
        return ckan_dataset_copy

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

    def __merge_resources(self, existing_resources, new_resources):
        # if we are updating datasets we need to check if the resources exists and merge them
        # https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L681

        merged_resources = []

        for res in new_resources:
            for existing_res in existing_resources:
                if res["url"] == existing_res["url"]:
                    # in CKAN exts maybe the have an ID because a local show
                    res["id"] = existing_res["id"]
            merged_resources.append(res)

        return merged_resources

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