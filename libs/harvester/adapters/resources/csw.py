from harvester.adapters.ckan_resource_adapters import CKANResourceAdapter
from harvester.logs import logger
from slugify import slugify
import json


class CSWResource(CKANResourceAdapter):
    ''' CSW resource '''

    def validate_origin_distribution(self):
        return True, None

    def validate_final_resource(self, ckan_resource):
        return True, None

    def transform_to_ckan_resource(self):

        valid, error = self.validate_origin_distribution()
        if not valid:
            raise Exception(f'Error validating origin resource/record: {error}')

        original_resource = self.original_resource
        ckan_resource = self.get_base_ckan_resource()

        # FIX THIS
        if original_resource['type'] == 'resource_locator':
            url = resource_locator.get('url', '').strip()
            if url:
                resource = {}
                format_from_url = guess_resource_format(url)
                resource['format'] = format_from_url
                if resource['format'] == 'wms' and config.get('ckanext.spatial.harvest.validate_wms', False):
                    # Check if the service is a view service
                    test_url = url.split('?')[0] if '?' in url else url
                    if self._is_wms(test_url):
                        resource['verified'] = True
                        resource['verified_date'] = datetime.now().isoformat()

                resource.update(
                    {
                        'url': url,
                        'name': resource_locator.get('name') or p.toolkit._('Unnamed resource'),
                        'description': resource_locator.get('description') or  '',
                        'resource_locator_protocol': resource_locator.get('protocol') or '',
                        'resource_locator_function': resource_locator.get('function') or '',
                    })
                package_dict['resources'].append(resource)

        elif original_resource['type'] == 'resource_locator_group_data_format':
            for resource_locator_group, data_format in resource_locator_group_data_format:
                for resource_locator in resource_locator_group['resource-locator']:
                    url = resource_locator.get('url', None)
                    if url is not None:
                        resource = {}
                        format_from_url = self.guess_resource_format(url)
                        resource['format'] = format_from_url if format_from_url else data_format
                        if resource['format'] == 'wms' and config.get('ckanext.spatial.harvest.validate_wms', False):
                            # Check if the service is a view service
                            test_url = url.split('?')[0] if '?' in url else url
                            if self._is_wms(test_url):
                                resource['verified'] = True
                                resource['verified_date'] = datetime.now().isoformat()

                        resource.update(
                            {
                                'url': url,
                                'name': resource_locator.get('name') or p.toolkit._('Unnamed resource'),
                                'description': resource_locator.get('description') or  '',
                                'resource_locator_protocol': resource_locator.get('protocol') or '',
                                'resource_locator_function': resource_locator.get('function') or '',
                            })
                        package_dict['resources'].append(resource)

        valid, error = self.validate_final_resource(ckan_resource)
        if not valid:
            raise Exception(f'Error validating final resource/distribution: {error}')

        return ckan_resource
