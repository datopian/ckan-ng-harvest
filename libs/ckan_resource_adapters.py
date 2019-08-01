''' transform datasets to CKAN datasets '''
from abc import ABC, abstractmethod
from logs import logger
from slugify import slugify
import json


class CKANResourceAdapter(ABC):
    ''' transform other resource objects into CKAN resource '''

    def __init__(self, original_resource):
        self.original_resource = original_resource

    def get_base_ckan_resource(self):
        # base results
        # Check for required fields: https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.resource_create

        resource = {
            'package_id': None,  # (string) – id of package that the resource should be added to.
            'url': None,  # (string) – url of resource
            'revision_id': None,  # (string) – (optional)
            'description': None,  # (string) – (optional)
            'format': None,  # (string) – (optional)
            'hash': None,  # (string) – (optional)
            'name': None,  # (string) – (optional)
            'resource_type': None,  # (string) – (optional)
            'mimetype': None,  # (string) – (optional)
            'mimetype_inner': None,  # (string) – (optional)
            'cache_url': None,  # (string) – (optional)
            'size': None,  # (int) – (optional)
            'created': None,  # (iso date string) – (optional)
            'last_modified': None,  # (iso date string) – (optional)
            'cache_last_updated': None,  # (iso date string) – (optional)
            'upload': None,  # (FieldStorage (optional) needs multipart/form-data) – (optional)
        }

        return resource

    @abstractmethod
    def transform_to_ckan_resource(self):
        pass


class DataJSONDistribution(CKANResourceAdapter):
    ''' Data.json resource
        Transformed in CKAN extension here:
        https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/parse_datajson.py#L84'''

    def validate_origin_distribution(self):
        original_resource = self.original_resource
        download_url = original_resource.get('downloadURL', '').strip()
        access_url = original_resource.get('accessURL', '').strip()
        url = download_url or access_url

        if url == '':
            return False, 'You need "downloadURL" or "accessURL" to conform a final url'

        return True, None

    def validate_final_resource(self, ckan_resource):
        if 'url' not in ckan_resource:
            return False, 'url is a required field'

        return True, None

    def transform_to_ckan_resource(self):

        valid, error = self.validate_origin_distribution()
        if not valid:
            raise Exception(f'Error validating origin resource/distribution: {error}')

        original_resource = self.original_resource
        download_url = original_resource.get('downloadURL', '').strip()
        access_url = original_resource.get('accessURL', '').strip()
        url = download_url or access_url

        ckan_resource = self.get_base_ckan_resource()
        fmt = original_resource.get('format', original_resource.get('mediaType', ''))
        ckan_resource['url'] = url
        ckan_resource['format'] = fmt
        ckan_resource['mimetype'] = original_resource.get('mediaType', '')
        ckan_resource['description'] = original_resource.get('description', '')
        ckan_resource['name'] = original_resource.get('title', None)

        resource_extras = ['conformsTo', 'describedBy', 'describedByType']
        for resource_extra_key in resource_extras:
            resource_extra_value = original_resource.get(resource_extra_key)
            if resource_extra_value:
                ckan_resource[resource_extra_key] = resource_extra_value

        if download_url and access_url:
            ckan_resource['accessURL'] = access_url

        # remove empty fields
        ckan_resource_copy = ckan_resource.copy()
        for k, v in ckan_resource.items():
            if v is None:
                ckan_resource_copy.pop(k)

        valid, error = self.validate_final_resource(ckan_resource_copy)
        if not valid:
            raise Exception(f'Error validating final resource/distribution: {error}')

        return ckan_resource_copy
