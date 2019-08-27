import pytest
from libs.csw import CSWSource
from owslib.csw import CatalogueServiceWeb


class TestCSWClass(object):

    base_url = 'https://datopian.gitlab.io/ckan-ng-harvest'

    def test_url(self):
        csw = CSWSource(url='http://cswtest.com/test?p=10')
        url = csw.get_original_url(harvest_id=99)

        # TODO what about the p=10?
        new_url = 'http://cswtest.com/test?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetRecordById&OUTPUTSCHEMA=http%3A%2F%2Fwww.isotc211.org%2F2005%2Fgmd&OUTPUTFORMAT=application%2Fxml&ID=99'
        assert url == new_url

    def test_base_service_csw(self):
        url_services = [
            'http://metadata.arcticlcc.org/csw',
            # 'http://data.nconemap.com/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2',
            # 'http://metadata.usace.army.mil/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2',
            # 'https://meta.geo.census.gov/data/existing/decennial/GEO/GPMB/TIGERline/TIGER2017/arealm/',
            # 'http://geonode.state.gov/catalogue/csw?service=CSW&version=2.0.2&request=GetRecords&typenames=csw:Record&elementsetname=brief',
            'https://portal.opentopography.org/geoportal/csw'
        ]
        for url in url_services:
            csw = CSWSource(url=url)
            csw.connect_csw()
            service = csw.csw
            # Check each service instance conforms to OWSLib interface
            service.alias = 'CSW'
            isinstance(service, CatalogueServiceWeb)
            # URL attribute
            assert service.url == url
            # version attribute
            assert service.version == '2.0.2'
            # Identification object
            assert hasattr(service, 'identification')
            # Check all ServiceIdentification attributes
            assert service.identification.type == 'CSW'
            for attribute in ['type', 'version', 'title', 'abstract', 'keywords', 'accessconstraints', 'fees']:
                assert hasattr(service.identification, attribute)
            # Check all ServiceProvider attributes
            for attribute in ['name', 'url', 'contact']:
                assert hasattr(service.provider, attribute)
            # Check all operations implement IOperationMetadata
            for op in service.operations:
                for attribute in ['name', 'formatOptions', 'methods']:
                    assert hasattr(op, attribute)
            # Check all contents implement IContentMetadata as a dictionary
            # CSW does not work in this way so use dummy
            service.contents = {'dummy': '1'}
            isinstance(service.contents, dict)
            # Check any item (WCS coverage, WMS layer etc) from the contents of each service
            # Check it conforms to IContentMetadata interface
            # CSW does not conform to this
