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
        assert iso_values['dataset-reference-date'] == [{
                'type': 'publication',
                'value': '2018'
            }]
        assert iso_values['unique-resource-identifier'] == ''
        assert iso_values['presentation-form'] == []
        # assert iso_values['abstract'] == 'The National Agriculture Imagery Program (NAIP) acquires aerial imagery during the agricultural growing seasons in the continental US A primary goal of the NAIP program is to make digital ortho photography available to governmental agencies and the public within a year of acquisition.\n\n    NAIP is administered by the USDA\'s Farm Service Agency (FSA) through the Aerial Photography Field Office in Salt Lake City. This "leaf-on" imagery is used as a base layer for GIS programs in FSA\'s County Service Centers, and is used to maintain the Common Land Unit (CLU) boundaries.\n\n    For more information please see: http://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery'
        assert iso_values['purpose'] == ''
        assert iso_values['responsible-organisation'] == [{
                'individual-name': '',
                'organisation-name': 'US Department of Agriculture, Farm Service Agency',
                'position-name': '',
                'contact-info': '',
                'role': 'custodian'
            }]
        assert iso_values['frequency-of-update'] == ''
        assert iso_values['maintenance-note'] == ''
        assert iso_values['progress'] == []
        assert iso_values['keywords'] == [{
                'keyword': ['USDA', 'NAIP', 'imagery', 'leaf-on', 'aerial photography', 'aerials'],
                'type': ''
            }]
        assert iso_values['keyword-inspire-theme'] == ['USDA', 'NAIP', 'imagery', 'leaf-on', 'aerial photography', 'aerials']
        assert iso_values['keyword-controlled-other'] == []
        assert iso_values['usage'] == []
        assert iso_values['limitations-on-public-access'] == []
        assert iso_values['access-constraints'] == []
        assert iso_values['use-constraints'] == []
        assert iso_values['aggregation-info'] == []
        assert iso_values['spatial-data-service-type'] == 'urn:x-esri:specification:ServiceType:ArcGIS'
        assert iso_values['spatial-resolution'] == ''
        assert iso_values['spatial-resolution-units'] == ''
        assert iso_values['equivalent-scale'] == []
        assert iso_values['dataset-language'] == []
        assert iso_values['topic-category'] == []
        assert iso_values['extent-controlled'] == []
        assert iso_values['extent-free-text'] == []
        assert iso_values['bbox'] == [{
                'west': '-84.4524',
                'east': '-75.298',
                'north': '36.717',
                'south': '33.7651'
            }]
        assert iso_values['temporal-extent-begin'] == []
        assert iso_values['temporal-extent-end'] == []
        assert iso_values['vertical-extent'] == []
        assert iso_values['coupled-resource'] == []
        assert iso_values['additional-information-source'] == ''
        assert iso_values['distributor-data-format'] == ''
        assert iso_values['distribution-data-format'] == []
        assert iso_values['distributor'] == []
        assert iso_values['resource-locator-group'] == [{
                'resource-locator': [{
                    'url': 'https://gis.apfo.usda.gov/arcgis/rest/services/NAIP/USDA_CONUS_PRIME/ImageServer',
                    'function': '',
                    'name': '',
                    'description': '',
                    'protocol': ''
                }]
            }]
        assert iso_values['resource-locator-identification'] == [{
                'url': 'https://gis.apfo.usda.gov/arcgis/rest/services/NAIP/USDA_CONUS_PRIME/ImageServer',
                'function': '',
                'name': '',
                'description': '',
                'protocol': ''
            }]
        assert iso_values['conformity-specification'] == ''
        assert iso_values['conformity-pass'] == ''
        assert iso_values['conformity-explanation'] == ''
        assert iso_values['lineage'] == ''
        assert iso_values['browse-graphic'] == []
        assert iso_values['date-released'] == '2018'
        assert iso_values['date-updated'] == ''
        assert iso_values['date-created'] == ''
        assert iso_values['url'] == ''
        assert iso_values['tags'] == ['USDA', 'NAIP', 'imagery', 'leaf-on', 'aerial photography', 'aerials']
        assert iso_values['publisher'] == ''
        assert iso_values['contact'] == 'US Department of Agriculture, Farm Service Agency'
        assert iso_values['contact-email'] == ''
