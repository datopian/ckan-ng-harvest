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
