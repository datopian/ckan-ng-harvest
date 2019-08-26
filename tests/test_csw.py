import pytest
from libs.csw import CSWSource


class TestCSWClass(object):

    base_url = 'https://datopian.gitlab.io/ckan-ng-harvest'

    def test_url(self):
        csw = CSWSource(url='http://cswtest.com/test?p=10')
        url = csw.get_original_url(harvest_id=99)

        # TODO what about the p=10?
        new_url = 'http://cswtest.com/test?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetRecordById&OUTPUTSCHEMA=http%3A%2F%2Fwww.isotc211.org%2F2005%2Fgmd&OUTPUTFORMAT=application%2Fxml&ID=99'
        assert url == new_url
