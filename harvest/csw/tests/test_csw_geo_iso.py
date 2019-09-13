import pytest
from harvester.csw import CSWSource


class TestCSWISOClass(object):

    xml_sample_2 = 'samples/sample2.xml'
    iso_values_sample_2 = 'samples/iso_values_sample2.json'

    def test_transform_ISO(self):
        # read XML and transform to ISO values

        csw = CSWSource(url='http://fake.url')
        f = open(self.xml_sample_2, 'r')
        xml_data = f.read()
        f.close()

        iso_values = csw.read_values_from_xml(xml_data=xml_data)

        assert iso_values['guid'] == '{F7AD63D2-2770-4D9E-B6C2-3A53E77D0CE8}'
        assert iso_values['metadata-language'] == 'en'
        assert iso_values['metadata-standard-name'] == 'ISO 19139/19119 Metadata for Web Services'
        assert iso_values['metadata-standard-version'] == '2018'
        assert iso_values['resource-type'] == ['service']
        assert iso_values['metadata-point-of-contact'] == []
        assert iso_values['metadata-date'] == '2018-12-11'
        assert iso_values['spatial-reference-system'] == ''
        assert iso_values['title'] == 'NAIP 2018 Imagery for North Carolina - USDA'
        assert iso_values['format'] == ''
        assert iso_values['alternative-title'] == []
        assert iso_values[''] == ''
        assert iso_values[''] == ''
        assert iso_values[''] == ''
        assert iso_values[''] == ''
        assert iso_values[''] == ''
        assert iso_values[''] == ''
        assert iso_values[''] == ''
        assert iso_values[''] == ''

        iv = {
            'guid': '{F7AD63D2-2770-4D9E-B6C2-3A53E77D0CE8}',
            'metadata-language': '',
            'metadata-standard-name': 'ISO 19139/19119 Metadata for Web Services',
            'metadata-standard-version': '2018',
            'resource-type': ['service'],
            'metadata-point-of-contact': [],
            'metadata-date': '2018-12-11',
            'spatial-reference-system': '',
            'title': 'NAIP 2018 Imagery for North Carolina - USDA',
            'format': '',
            'alternative-title': [],
            'dataset-reference-date': [{
                'type': 'publication',
                'value': '2018'
            }],
            'unique-resource-identifier': '',
            'presentation-form': [],
            'abstract': 'The National Agriculture Imagery Program (NAIP) acquires aerial imagery during the agricultural growing seasons in the continental US A primary goal of the NAIP program is to make digital ortho photography available to governmental agencies and the public within a year of acquisition.\n\n    NAIP is administered by the USDA\'s Farm Service Agency (FSA) through the Aerial Photography Field Office in Salt Lake City. This "leaf-on" imagery is used as a base layer for GIS programs in FSA\'s County Service Centers, and is used to maintain the Common Land Unit (CLU) boundaries.\n\n    For more information please see: http://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery',
            'purpose': '',
            'responsible-organisation': [{
                'individual-name': '',
                'organisation-name': 'US Department of Agriculture, Farm Service Agency',
                'position-name': '',
                'contact-info': '',
                'role': 'custodian'
            }],
            'frequency-of-update': '',
            'maintenance-note': '',
            'progress': [],
            'keywords': [{
                'keyword': ['USDA', 'NAIP', 'imagery', 'leaf-on', 'aerial photography', 'aerials'],
                'type': ''
            }],
            'keyword-inspire-theme': ['USDA', 'NAIP', 'imagery', 'leaf-on', 'aerial photography', 'aerials'],
            'keyword-controlled-other': [],
            'usage': [],
            'limitations-on-public-access': [],
            'access-constraints': [],
            'use-constraints': [],
            'aggregation-info': [],
            'spatial-data-service-type': 'urn:x-esri:specification:ServiceType:ArcGIS',
            'spatial-resolution': '',
            'spatial-resolution-units': '',
            'equivalent-scale': [],
            'dataset-language': [],
            'topic-category': [],
            'extent-controlled': [],
            'extent-free-text': [],
            'bbox': [{
                'west': '-84.4524',
                'east': '-75.298',
                'north': '36.717',
                'south': '33.7651'
            }],
            'temporal-extent-begin': [],
            'temporal-extent-end': [],
            'vertical-extent': [],
            'coupled-resource': [],
            'additional-information-source': '',
            'distributor-data-format': '',
            'distribution-data-format': [],
            'distributor': [],
            'resource-locator-group': [{
                'resource-locator': [{
                    'url': 'https://gis.apfo.usda.gov/arcgis/rest/services/NAIP/USDA_CONUS_PRIME/ImageServer',
                    'function': '',
                    'name': '',
                    'description': '',
                    'protocol': ''
                }]
            }],
            'resource-locator-identification': [{
                'url': 'https://gis.apfo.usda.gov/arcgis/rest/services/NAIP/USDA_CONUS_PRIME/ImageServer',
                'function': '',
                'name': '',
                'description': '',
                'protocol': ''
            }],
            'conformity-specification': '',
            'conformity-pass': '',
            'conformity-explanation': '',
            'lineage': '',
            'browse-graphic': [],
            'date-released': '2018',
            'date-updated': '',
            'date-created': '',
            'url': '',
            'tags': ['USDA', 'NAIP', 'imagery', 'leaf-on', 'aerial photography', 'aerials'],
            'publisher': '',
            'contact': 'US Department of Agriculture, Farm Service Agency',
            'contact-email': ''
        }


