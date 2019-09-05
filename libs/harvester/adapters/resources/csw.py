from harvester.adapters.ckan_resource_adapters import CKANResourceAdapter
from harvester.logs import logger
from slugify import slugify
import json


class CSWRecord(CKANResourceAdapter):
    ''' CSW resource '''

    def validate_origin_distribution(self):
        return True, None

    def validate_final_resource(self, ckan_resource):
        return True, None

    def transform_to_ckan_resource(self):

        valid, error = self.validate_origin_distribution()
        if not valid:
            raise Exception(f'Error validating origin resource/record: {error}')

        ckan_resource = self.get_base_ckan_resource()

        valid, error = self.validate_final_resource(ckan_resource)
        if not valid:
            raise Exception(f'Error validating final resource/distribution: {error}')

        return ckan_resource
