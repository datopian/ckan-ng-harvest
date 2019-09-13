import requests
from owslib import wms
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
            resource_locator = original_resource['data']
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
                return resource

        elif original_resource['type'] == 'resource_locator_group_data_format':
            resource_locator_group_data_format = original_resource['data']
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
                        return resource

        valid, error = self.validate_final_resource(ckan_resource)
        if not valid:
            raise Exception(f'Error validating final resource/distribution: {error}')

        return ckan_resource

    def _is_wms(self, url):
        '''
        Checks if the provided URL actually points to a Web Map Service.
        Uses owslib WMS reader to parse the response.
        '''

        try:
            capabilities_url = wms.WMSCapabilitiesReader().capabilities_url(url)
            res = requests.get(capabilities_url, timeout=10)
            xml = res.text

            s = wms.WebMapService(url, xml=xml)
            raise Exception('is_wms: {}'.format(s.contents))
            return isinstance(s.contents, dict) and s.contents != {}
        except Exception as e:
            logger.error('WMS check for %s failed with exception: %s' % (url, str(e)))
        return False

    # TODO copie from previous plugin. UPGRADE required
    def guess_resource_format(url, use_mimetypes=True):
        '''
        Given a URL try to guess the best format to assign to the resource

        The function looks for common patterns in popular geospatial services and
        file extensions, so it may not be 100% accurate. It just looks at the
        provided URL, it does not attempt to perform any remote check.

        if 'use_mimetypes' is True (default value), the mimetypes module will be
        used if no match was found before.

        Returns None if no format could be guessed.

        '''
        url = url.lower().strip()

        resource_types = {
            # OGC
            'wms': ('service=wms', 'geoserver/wms', 'mapserver/wmsserver', 'com.esri.wms.Esrimap', 'service/wms'),
            'wfs': ('service=wfs', 'geoserver/wfs', 'mapserver/wfsserver', 'com.esri.wfs.Esrimap'),
            'wcs': ('service=wcs', 'geoserver/wcs', 'imageserver/wcsserver', 'mapserver/wcsserver'),
            'sos': ('service=sos',),
            'csw': ('service=csw',),
            # ESRI
            'kml': ('mapserver/generatekml',),
            'arcims': ('com.esri.esrimap.esrimap',),
            'arcgis_rest': ('arcgis/rest/services',),
        }

        for resource_type, parts in resource_types.iteritems():
            if any(part in url for part in parts):
                return resource_type

        file_types = {
            'kml' : ('kml',),
            'kmz': ('kmz',),
            'gml': ('gml',),
        }

        for file_type, extensions in file_types.iteritems():
            if any(url.endswith(extension) for extension in extensions):
                return file_type

        resource_format, encoding = mimetypes.guess_type(url)
        if resource_format:
            return resource_format

        return None
